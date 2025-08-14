// File: src/cache.rs - Optimized version with parallel I/O, compression, and memory mapping
use std::path::{Path, PathBuf};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Write, Read};
use bincode;
use std::time::{SystemTime, Instant};
use std::sync::Arc;
use rayon::prelude::*;
use lz4::{Decoder, EncoderBuilder};
use zstd::stream::{encode_all, decode_all};
use memmap2::{Mmap, MmapOptions};
use dashmap::DashMap;
use crossbeam_channel::{bounded, Sender, Receiver};
use serde::{Serialize, Deserialize};

use crate::utils::{TimsTOFRawData, IndexedTimsTOFData};

// Constants for optimization
const DEFAULT_COMPRESSION_LEVEL: i32 = 3;
const SHARD_SIZE_THRESHOLD: usize = 1024 * 1024; // 1MB threshold for sharding
const BUFFER_SIZE: usize = 1024 * 1024 * 16; // 16MB buffer

// Compression types
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum CompressionType {
    None,
    Lz4,
    Zstd,
    Hybrid, // Use different compression for different data types
}

// Cache metadata structure
#[derive(Debug, Serialize, Deserialize)]
struct CacheMetadata {
    version: u32,
    compression_type: CompressionType,
    shard_count: usize,
    ms2_window_count: usize,
    created_at: SystemTime,
    source_modified: SystemTime,
    parallel_threads: usize,
}

// Shard information
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ShardInfo {
    shard_id: usize,
    mz_range: (f32, f32),
    data_points: usize,
    compressed_size: u64,
    uncompressed_size: u64,
}

// Data shard for parallel processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataShard {
    pub rt_values_min: Vec<f32>,
    pub mobility_values: Vec<f32>,
    pub mz_values: Vec<f32>,
    pub intensity_values: Vec<u32>,
    pub frame_indices: Vec<u32>,
    pub scan_indices: Vec<u32>,
    pub mz_range: (f32, f32),
}

impl DataShard {
    fn from_indexed_slice(
        data: &IndexedTimsTOFData,
        start: usize,
        end: usize,
    ) -> Self {
        let mz_min = data.mz_values[start];
        let mz_max = data.mz_values[end - 1];
        
        Self {
            rt_values_min: data.rt_values_min[start..end].to_vec(),
            mobility_values: data.mobility_values[start..end].to_vec(),
            mz_values: data.mz_values[start..end].to_vec(),
            intensity_values: data.intensity_values[start..end].to_vec(),
            frame_indices: data.frame_indices[start..end].to_vec(),
            scan_indices: data.scan_indices[start..end].to_vec(),
            mz_range: (mz_min, mz_max),
        }
    }
    
    fn point_count(&self) -> usize {
        self.mz_values.len()
    }
}

pub struct CacheManager {
    cache_dir: PathBuf,
    compression_type: CompressionType,
    parallel_threads: usize,
}

impl CacheManager {
    pub fn new() -> Self {
        Self::with_threads(num_cpus::get())
    }
    
    pub fn with_threads(parallel_threads: usize) -> Self {
        let cache_dir = PathBuf::from(".timstof_cache_optimized");
        fs::create_dir_all(&cache_dir).unwrap();
        Self { 
            cache_dir,
            compression_type: CompressionType::Hybrid,
            parallel_threads,
        }
    }
    
    fn get_cache_path(&self, source_path: &Path, cache_type: &str) -> PathBuf {
        let source_name = source_path.file_name().unwrap().to_str().unwrap();
        let cache_name = format!("{}.{}.cache", source_name, cache_type);
        self.cache_dir.join(cache_name)
    }
    
    fn get_metadata_path(&self, source_path: &Path) -> PathBuf {
        let source_name = source_path.file_name().unwrap().to_str().unwrap();
        let meta_name = format!("{}.meta.json", source_name);
        self.cache_dir.join(meta_name)
    }
    
    fn get_shard_path(&self, source_path: &Path, cache_type: &str, shard_id: usize) -> PathBuf {
        let source_name = source_path.file_name().unwrap().to_str().unwrap();
        let shard_name = format!("{}.{}.shard_{}.cache", source_name, cache_type, shard_id);
        self.cache_dir.join(shard_name)
    }
    
    pub fn is_cache_valid(&self, source_path: &Path) -> bool {
        let meta_path = self.get_metadata_path(source_path);
        
        if !meta_path.exists() {
            return false;
        }
        
        // Check metadata
        let metadata: CacheMetadata = match self.load_metadata(&meta_path) {
            Ok(m) => m,
            Err(_) => return false,
        };
        
        // Check source folder modification time
        let source_modified = fs::metadata(source_path)
            .and_then(|m| m.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH);
            
        metadata.source_modified >= source_modified
    }
    
    // Split IndexedTimsTOFData into shards for parallel processing
    fn split_into_shards(&self, data: &IndexedTimsTOFData, num_shards: usize) -> Vec<DataShard> {
        let total_points = data.mz_values.len();
        if total_points == 0 {
            return vec![];
        }
        
        let points_per_shard = (total_points + num_shards - 1) / num_shards;
        let mut shards = Vec::with_capacity(num_shards);
        
        for i in 0..num_shards {
            let start = i * points_per_shard;
            let end = ((i + 1) * points_per_shard).min(total_points);
            
            if start < total_points {
                shards.push(DataShard::from_indexed_slice(data, start, end));
            }
        }
        
        shards
    }
    
    // Merge shards back into IndexedTimsTOFData
    fn merge_shards(&self, shards: Vec<DataShard>) -> IndexedTimsTOFData {
        let total_size: usize = shards.iter().map(|s| s.point_count()).sum();
        
        let mut result = IndexedTimsTOFData {
            rt_values_min: Vec::with_capacity(total_size),
            mobility_values: Vec::with_capacity(total_size),
            mz_values: Vec::with_capacity(total_size),
            intensity_values: Vec::with_capacity(total_size),
            frame_indices: Vec::with_capacity(total_size),
            scan_indices: Vec::with_capacity(total_size),
        };
        
        for shard in shards {
            result.rt_values_min.extend(shard.rt_values_min);
            result.mobility_values.extend(shard.mobility_values);
            result.mz_values.extend(shard.mz_values);
            result.intensity_values.extend(shard.intensity_values);
            result.frame_indices.extend(shard.frame_indices);
            result.scan_indices.extend(shard.scan_indices);
        }
        
        result
    }
    
    // Compress data based on type
    fn compress_data(&self, data: &[u8], compression: CompressionType) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        match compression {
            CompressionType::None => Ok(data.to_vec()),
            CompressionType::Lz4 => {
                let mut encoder = EncoderBuilder::new()
                    .level(4)
                    .build(Vec::new())?;
                encoder.write_all(data)?;
                let (compressed, _) = encoder.finish();
                Ok(compressed)
            },
            CompressionType::Zstd => {
                Ok(encode_all(data, DEFAULT_COMPRESSION_LEVEL)?)
            },
            CompressionType::Hybrid => {
                // For hybrid, we use Lz4 by default (caller should handle type-specific compression)
                self.compress_data(data, CompressionType::Lz4)
            }
        }
    }
    
    // Decompress data
    fn decompress_data(&self, data: &[u8], compression: CompressionType) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        match compression {
            CompressionType::None => Ok(data.to_vec()),
            CompressionType::Lz4 => {
                let mut decoder = Decoder::new(data)?;
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed)?;
                Ok(decompressed)
            },
            CompressionType::Zstd => {
                Ok(decode_all(data)?)
            },
            CompressionType::Hybrid => {
                // For hybrid, try Lz4 first
                self.decompress_data(data, CompressionType::Lz4)
            }
        }
    }
    
    // Save a single shard with compression
    fn save_shard(&self, shard: &DataShard, path: &PathBuf, compression: CompressionType) -> Result<u64, Box<dyn std::error::Error>> {
        let serialized = bincode::serialize(shard)?;
        let compressed = self.compress_data(&serialized, compression)?;
        
        let file = File::create(path)?;
        let mut writer = BufWriter::with_capacity(BUFFER_SIZE, file);
        writer.write_all(&compressed)?;
        writer.flush()?;
        
        Ok(compressed.len() as u64)
    }
    
    // Load a single shard with decompression
    fn load_shard(&self, path: &PathBuf, compression: CompressionType) -> Result<DataShard, Box<dyn std::error::Error + Send + Sync>> {
        let file = File::open(path)?;
        let mut reader = BufReader::with_capacity(BUFFER_SIZE, file);
        let mut compressed = Vec::new();
        reader.read_to_end(&mut compressed)?;
        
        let decompressed = self.decompress_data(&compressed, compression)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { 
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())) 
            })?;
        let shard: DataShard = bincode::deserialize(&decompressed)?;
        Ok(shard)
    }
    
    // Load a shard using memory mapping (for large files)
    fn load_shard_mmap(&self, path: &PathBuf, compression: CompressionType) -> Result<DataShard, Box<dyn std::error::Error + Send + Sync>> {
        let file = File::open(path)?;
        let mmap = unsafe { MmapOptions::new().map(&file)? };
        
        let decompressed = self.decompress_data(&mmap[..], compression)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { 
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())) 
            })?;
        let shard: DataShard = bincode::deserialize(&decompressed)?;
        Ok(shard)
    }
    
    // Save metadata
    fn save_metadata(&self, metadata: &CacheMetadata, path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
        let json = serde_json::to_string_pretty(metadata)?;
        fs::write(path, json)?;
        Ok(())
    }
    
    // Load metadata
    fn load_metadata(&self, path: &PathBuf) -> Result<CacheMetadata, Box<dyn std::error::Error>> {
        let json = fs::read_to_string(path)?;
        let metadata: CacheMetadata = serde_json::from_str(&json)?;
        Ok(metadata)
    }
    
    // Main save function with parallel processing
    pub fn save_indexed_data(
        &self, 
        source_path: &Path, 
        ms1_indexed: &IndexedTimsTOFData,
        ms2_indexed_pairs: &Vec<((f32, f32), IndexedTimsTOFData)>
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        println!("Saving indexed data to optimized cache...");
        let start_time = Instant::now();
        
        // Get source modification time
        let source_modified = fs::metadata(source_path)
            .and_then(|m| m.modified())
            .unwrap_or(SystemTime::now());
        
        // Split MS1 data into shards
        let ms1_shards = self.split_into_shards(ms1_indexed, self.parallel_threads);
        let num_shards = ms1_shards.len();
        
        // Parallel save MS1 shards
        let ms1_sizes: Vec<_> = ms1_shards
            .par_iter()
            .enumerate()
            .map(|(i, shard)| {
                let shard_path = self.get_shard_path(source_path, "ms1", i);
                self.save_shard(shard, &shard_path, self.compression_type)
                    .unwrap_or_else(|e| {
                        eprintln!("Error saving MS1 shard {}: {}", i, e);
                        0
                    })
            })
            .collect();
        
        // Parallel save MS2 windows
        let ms2_sizes: Vec<_> = ms2_indexed_pairs
            .par_iter()
            .enumerate()
            .map(|(i, (_range, data))| {
                let window_path = self.get_shard_path(source_path, "ms2_window", i);
                
                // Create a temporary shard from MS2 data
                let shard = DataShard {
                    rt_values_min: data.rt_values_min.clone(),
                    mobility_values: data.mobility_values.clone(),
                    mz_values: data.mz_values.clone(),
                    intensity_values: data.intensity_values.clone(),
                    frame_indices: data.frame_indices.clone(),
                    scan_indices: data.scan_indices.clone(),
                    mz_range: _range.clone(),
                };
                
                self.save_shard(&shard, &window_path, self.compression_type)
                    .unwrap_or_else(|e| {
                        eprintln!("Error saving MS2 window {}: {}", i, e);
                        0
                    })
            })
            .collect();
        
        // Save metadata
        let metadata = CacheMetadata {
            version: 2, // Version 2 for optimized cache
            compression_type: self.compression_type,
            shard_count: num_shards,
            ms2_window_count: ms2_indexed_pairs.len(),
            created_at: SystemTime::now(),
            source_modified,
            parallel_threads: self.parallel_threads,
        };
        
        let meta_path = self.get_metadata_path(source_path);
        self.save_metadata(&metadata, &meta_path)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;
        
        let elapsed = start_time.elapsed();
        let total_ms1_size: u64 = ms1_sizes.iter().sum();
        let total_ms2_size: u64 = ms2_sizes.iter().sum();
        let total_size_mb = (total_ms1_size + total_ms2_size) as f32 / 1024.0 / 1024.0;
        
        println!("Optimized cache saved:");
        println!("  - MS1 shards: {} ({:.2} MB)", num_shards, total_ms1_size as f32 / 1024.0 / 1024.0);
        println!("  - MS2 windows: {} ({:.2} MB)", ms2_indexed_pairs.len(), total_ms2_size as f32 / 1024.0 / 1024.0);
        println!("  - Total: {:.2} MB", total_size_mb);
        println!("  - Time: {:.2}s", elapsed.as_secs_f32());
        println!("  - Throughput: {:.2} MB/s", total_size_mb / elapsed.as_secs_f32());
        
        Ok(())
    }
    
    // Main load function with parallel processing
    pub fn load_indexed_data(
        &self, 
        source_path: &Path
    ) -> Result<(IndexedTimsTOFData, Vec<((f32, f32), IndexedTimsTOFData)>), Box<dyn std::error::Error + Send + Sync>> {
        println!("Loading indexed data from optimized cache...");
        let start_time = Instant::now();
        
        // Load metadata
        let meta_path = self.get_metadata_path(source_path);
        let metadata = self.load_metadata(&meta_path)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?;
        
        // Decide whether to use memory mapping based on file sizes
        let use_mmap = metadata.shard_count > 4; // Use mmap for larger datasets
        
        // Parallel load MS1 shards - use try_fold and reduce pattern
        let ms1_shards_results: Vec<Result<DataShard, _>> = (0..metadata.shard_count)
            .into_par_iter()
            .map(|i| {
                let shard_path = self.get_shard_path(source_path, "ms1", i);
                if use_mmap && shard_path.metadata().map(|m| m.len() > 10_000_000).unwrap_or(false) {
                    self.load_shard_mmap(&shard_path, metadata.compression_type)
                } else {
                    self.load_shard(&shard_path, metadata.compression_type)
                }
            })
            .collect();
        
        // Convert results
        let mut ms1_shards = Vec::new();
        for result in ms1_shards_results {
            ms1_shards.push(result.map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?);
        }
        
        // Merge MS1 shards
        let ms1_indexed = self.merge_shards(ms1_shards);
        
        // Parallel load MS2 windows - use similar pattern
        let ms2_results: Vec<Result<((f32, f32), IndexedTimsTOFData), _>> = (0..metadata.ms2_window_count)
            .into_par_iter()
            .map(|i| {
                let window_path = self.get_shard_path(source_path, "ms2_window", i);
                let shard = if use_mmap && window_path.metadata().map(|m| m.len() > 10_000_000).unwrap_or(false) {
                    self.load_shard_mmap(&window_path, metadata.compression_type)
                } else {
                    self.load_shard(&window_path, metadata.compression_type)
                };
                
                shard.map(|s| {
                    // Convert shard back to IndexedTimsTOFData
                    let data = IndexedTimsTOFData {
                        rt_values_min: s.rt_values_min,
                        mobility_values: s.mobility_values,
                        mz_values: s.mz_values,
                        intensity_values: s.intensity_values,
                        frame_indices: s.frame_indices,
                        scan_indices: s.scan_indices,
                    };
                    (s.mz_range, data)
                })
            })
            .collect();
        
        // Convert results
        let mut ms2_indexed_pairs = Vec::new();
        for result in ms2_results {
            ms2_indexed_pairs.push(result.map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
            })?);
        }
        
        let elapsed = start_time.elapsed();
        println!("Optimized cache loaded:");
        println!("  - MS1 shards: {}", metadata.shard_count);
        println!("  - MS2 windows: {}", metadata.ms2_window_count);
        println!("  - Time: {:.2}s", elapsed.as_secs_f32());
        println!("  - Used {} threads", self.parallel_threads);
        
        Ok((ms1_indexed, ms2_indexed_pairs))
    }
    
    pub fn clear_cache(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.cache_dir.exists() {
            fs::remove_dir_all(&self.cache_dir)?;
            println!("Optimized cache cleared");
        }
        Ok(())
    }
    
    pub fn get_cache_info(&self) -> Result<Vec<(String, u32, String)>, Box<dyn std::error::Error>> {
        let mut info = Vec::new();
        
        if self.cache_dir.exists() {
            // Group files by source
            let mut source_sizes: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
            
            for entry in fs::read_dir(&self.cache_dir)? {
                let entry = entry?;
                let path = entry.path();
                
                if let Some(extension) = path.extension() {
                    if extension == "cache" {
                        let metadata = fs::metadata(&path)?;
                        let size = metadata.len();
                        
                        // Extract source name from filename
                        if let Some(filename) = path.file_name().and_then(|s| s.to_str()) {
                            let parts: Vec<&str> = filename.split('.').collect();
                            if parts.len() > 0 {
                                let source_name = parts[0].to_string();
                                *source_sizes.entry(source_name).or_insert(0) += size;
                            }
                        }
                    }
                }
            }
            
            // Convert to output format
            for (name, size) in source_sizes {
                let size_mb = size as f32 / 1024.0 / 1024.0;
                let size_gb = size as f32 / 1024.0 / 1024.0 / 1024.0;
                
                let size_str = if size_gb >= 1.0 {
                    format!("{:.2} GB", size_gb)
                } else {
                    format!("{:.2} MB", size_mb)
                };
                
                info.push((name, size as u32, size_str));
            }
        }
        
        Ok(info)
    }
    
    // Advanced: Async save for very large datasets
    pub async fn save_indexed_data_async(
        &self,
        source_path: &Path,
        ms1_indexed: &IndexedTimsTOFData,
        ms2_indexed_pairs: &Vec<((f32, f32), IndexedTimsTOFData)>
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use tokio::task;
        
        let source_path = source_path.to_path_buf();
        let ms1_indexed = ms1_indexed.clone();
        let ms2_indexed_pairs = ms2_indexed_pairs.clone();
        let cache_manager = self.clone();
        
        let result = task::spawn_blocking(move || {
            cache_manager.save_indexed_data(&source_path, &ms1_indexed, &ms2_indexed_pairs)
        }).await?;
        
        result.map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
            Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))
        })
    }
}

// Make CacheManager cloneable for async operations
impl Clone for CacheManager {
    fn clone(&self) -> Self {
        Self {
            cache_dir: self.cache_dir.clone(),
            compression_type: self.compression_type,
            parallel_threads: self.parallel_threads,
        }
    }
}

