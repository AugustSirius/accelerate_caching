// File: src/cache.rs
use std::path::{Path, PathBuf};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use bincode;
use std::time::SystemTime;

use crate::utils::{TimsTOFRawData, IndexedTimsTOFData};

#[derive(Clone)]
pub struct CacheConfig {
    pub enable_compression: bool,
    pub buffer_size: usize,
    pub auto_compression: bool, // Automatically decide based on file size
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enable_compression: false,  // Disabled by default for speed
            buffer_size: 1024 * 1024 * 32, // Smaller, more efficient buffer
            auto_compression: true,     // Smart compression decisions
        }
    }
}

pub struct CacheManager {
    cache_dir: PathBuf,
    config: CacheConfig,
}

impl CacheManager {
    pub fn new() -> Self {
        Self::with_config(CacheConfig::default())
    }
    
    pub fn with_config(config: CacheConfig) -> Self {
        let cache_dir = PathBuf::from(".timstof_cache");
        fs::create_dir_all(&cache_dir).unwrap();
        Self { cache_dir, config }
    }
    
    fn get_cache_path(&self, source_path: &Path, cache_type: &str) -> PathBuf {
        let source_name = source_path.file_name().unwrap().to_str().unwrap();
        let extension = if self.should_compress_file(cache_type) { "cache.lz4" } else { "cache.bin" };
        let cache_name = format!("{}.{}.{}", source_name, cache_type, extension);
        self.cache_dir.join(cache_name)
    }
    
    fn get_metadata_path(&self, source_path: &Path) -> PathBuf {
        let source_name = source_path.file_name().unwrap().to_str().unwrap();
        let meta_name = format!("{}.meta", source_name);
        self.cache_dir.join(meta_name)
    }
    
    // Smart compression decision based on file type and size
    fn should_compress_file(&self, cache_type: &str) -> bool {
        if !self.config.auto_compression {
            return self.config.enable_compression;
        }
        
        // Only compress larger files where the CPU overhead is worth it
        // MS2 data is typically much larger and benefits from compression
        match cache_type {
            "ms2_indexed" => true,  // Large, repetitive data - good compression ratio
            "ms1_indexed" => false, // Smaller, less compressible - not worth the CPU cost
            _ => false,
        }
    }
    
    pub fn is_cache_valid(&self, source_path: &Path) -> bool {
        let ms1_cache_path = self.get_cache_path(source_path, "ms1_indexed");
        let ms2_cache_path = self.get_cache_path(source_path, "ms2_indexed");
        let meta_path = self.get_metadata_path(source_path);
        
        if !ms1_cache_path.exists() || !ms2_cache_path.exists() || !meta_path.exists() {
            return false;
        }
        
        // Check source folder modification time
        let source_modified = fs::metadata(source_path)
            .and_then(|m| m.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH);
            
        let cache_modified = fs::metadata(&ms1_cache_path)
            .and_then(|m| m.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH);
            
        cache_modified > source_modified
    }
    
    // OPTIMIZED: Sequential save with smart compression
    pub fn save_indexed_data(
        &self, 
        source_path: &Path, 
        ms1_indexed: &IndexedTimsTOFData,
        ms2_indexed_pairs: &Vec<((f32, f32), IndexedTimsTOFData)>
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!("Saving indexed data to optimized cache...");
        let start_time = std::time::Instant::now();
        
        // Save MS1 data (fast, no compression)
        let ms1_start = std::time::Instant::now();
        let ms1_cache_path = self.get_cache_path(source_path, "ms1_indexed");
        Self::save_data_to_file(&ms1_cache_path, ms1_indexed, &self.config, false)?;
        let ms1_time = ms1_start.elapsed();
        
        // Save MS2 data (with smart compression)
        let ms2_start = std::time::Instant::now();
        let ms2_cache_path = self.get_cache_path(source_path, "ms2_indexed");
        let use_compression = self.should_compress_file("ms2_indexed");
        Self::save_data_to_file(&ms2_cache_path, ms2_indexed_pairs, &self.config, use_compression)?;
        let ms2_time = ms2_start.elapsed();
        
        // Save metadata
        let meta_path = self.get_metadata_path(source_path);
        let metadata = format!(
            "cached at: {:?}\nms2_windows: {}\ntype: indexed\nms1_compression: false\nms2_compression: {}\nversion: 2.0\n",
            SystemTime::now(),
            ms2_indexed_pairs.len(),
            use_compression
        );
        fs::write(meta_path, metadata)?;
        
        let elapsed = start_time.elapsed();
        let ms1_size = fs::metadata(&ms1_cache_path)?.len();
        let ms2_size = fs::metadata(&ms2_cache_path)?.len();
        let total_size_mb = (ms1_size + ms2_size) as f32 / 1024.0 / 1024.0;
        
        println!("✅ Optimized cache saved: {:.2} MB total", total_size_mb);
        println!("   ├── MS1: {:.3}s ({:.1} MB)", ms1_time.as_secs_f32(), ms1_size as f32 / 1024.0 / 1024.0);
        println!("   ├── MS2: {:.3}s ({:.1} MB, compressed: {})", ms2_time.as_secs_f32(), ms2_size as f32 / 1024.0 / 1024.0, use_compression);
        println!("   └── Total time: {:.3}s", elapsed.as_secs_f32());
        
        Ok(())
    }
    
    // OPTIMIZED: Sequential load with smart compression
    pub fn load_indexed_data(
        &self, 
        source_path: &Path
    ) -> Result<(IndexedTimsTOFData, Vec<((f32, f32), IndexedTimsTOFData)>), Box<dyn std::error::Error>> {
        println!("Loading indexed data from optimized cache...");
        let start_time = std::time::Instant::now();
        
        // Load MS1 data (fast, no compression)
        let ms1_start = std::time::Instant::now();
        let ms1_cache_path = self.get_cache_path(source_path, "ms1_indexed");
        let ms1_indexed = Self::load_data_from_file(&ms1_cache_path, &self.config, false)?;
        let ms1_time = ms1_start.elapsed();
        
        // Load MS2 data (with smart compression detection)
        let ms2_start = std::time::Instant::now();
        let ms2_cache_path = self.get_cache_path(source_path, "ms2_indexed");
        let use_compression = ms2_cache_path.extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext == "lz4")
            .unwrap_or(false);
        let ms2_indexed_pairs = Self::load_data_from_file(&ms2_cache_path, &self.config, use_compression)?;
        let ms2_time = ms2_start.elapsed();
        
        let elapsed = start_time.elapsed();
        println!("✅ Optimized cache loaded");
        println!("   ├── MS1: {:.3}s", ms1_time.as_secs_f32());
        println!("   ├── MS2: {:.3}s (compressed: {})", ms2_time.as_secs_f32(), use_compression);
        println!("   └── Total time: {:.3}s", elapsed.as_secs_f32());
        
        Ok((ms1_indexed, ms2_indexed_pairs))
    }
    
    // OPTIMIZED: Single-threaded save with optional compression
    fn save_data_to_file<T>(
        path: &Path,
        data: &T,
        config: &CacheConfig,
        use_compression: bool,
    ) -> Result<(), std::io::Error>
    where
        T: serde::Serialize + ?Sized,
    {
        let file = File::create(path)?;
        let writer = BufWriter::with_capacity(config.buffer_size, file);
        
        if use_compression {
            // Use LZ4 compression only when beneficial
            let encoder = lz4_flex::frame::FrameEncoder::new(writer);
            bincode::serialize_into(encoder, data)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        } else {
            // Direct binary serialization (fastest)
            bincode::serialize_into(writer, data)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        }
        
        Ok(())
    }
    
    // OPTIMIZED: Single-threaded load with optional compression
    fn load_data_from_file<T>(
        path: &Path,
        config: &CacheConfig,
        use_compression: bool,
    ) -> Result<T, std::io::Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let file = File::open(path)?;
        let reader = BufReader::with_capacity(config.buffer_size, file);
        
        if use_compression {
            // Use LZ4 decompression
            let decoder = lz4_flex::frame::FrameDecoder::new(reader);
            bincode::deserialize_from(decoder)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        } else {
            // Direct binary deserialization (fastest)
            bincode::deserialize_from(reader)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        }
    }
    
    pub fn clear_cache(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.cache_dir.exists() {
            fs::remove_dir_all(&self.cache_dir)?;
            println!("Cache cleared");
        }
        Ok(())
    }
    
    pub fn get_cache_info(&self) -> Result<Vec<(String, u32, String)>, Box<dyn std::error::Error>> {
        let mut info = Vec::new();
        
        if self.cache_dir.exists() {
            for entry in fs::read_dir(&self.cache_dir)? {
                let entry = entry?;
                let path = entry.path();
                let file_name = path.file_name().unwrap().to_str().unwrap();
                
                // Check for cache files
                if file_name.ends_with(".cache.bin") || file_name.ends_with(".cache.lz4") || file_name.ends_with(".cache") {
                    let metadata = fs::metadata(&path)?;
                    let size = metadata.len() as u32;
                    let name = file_name.to_string();
                    let size_mb = size as f32 / 1024.0 / 1024.0;
                    let size_gb = size as f32 / 1024.0 / 1024.0 / 1024.0;
                    
                    let size_str = if size_gb >= 1.0 {
                        format!("{:.2} GB", size_gb)
                    } else {
                        format!("{:.2} MB", size_mb)
                    };
                    
                    info.push((name, size, size_str));
                }
            }
        }
        
        Ok(info)
    }
    
    // Smart configuration based on system and data characteristics
    pub fn configure_for_threads(mut self, thread_count: usize) -> Self {
        // Optimize buffer size based on available threads (for CPU-bound operations elsewhere)
        // But keep I/O sequential for maximum disk performance
        self.config.buffer_size = match thread_count {
            1 => 1024 * 1024 * 16,     // 16MB for single-threaded
            2..=4 => 1024 * 1024 * 32, // 32MB for multi-threaded
            _ => 1024 * 1024 * 64,     // 64MB for high-thread systems
        };
        
        // Enable smart compression for systems with more CPU power
        self.config.auto_compression = thread_count > 1;
        
        self
    }
    
    // Benchmark cache performance
    pub fn benchmark_cache(&self, test_data_size: usize) -> Result<(), Box<dyn std::error::Error>> {
        println!("🔬 Benchmarking cache performance...");
        
        // Create test data
        let test_data: Vec<u8> = (0..test_data_size).map(|i| (i % 256) as u8).collect();
        let test_path = self.cache_dir.join("benchmark.test");
        
        // Test without compression
        let start = std::time::Instant::now();
        Self::save_data_to_file(&test_path, &test_data, &self.config, false)?;
        let save_time_uncompressed = start.elapsed();
        
        let start = std::time::Instant::now();
        let _: Vec<u8> = Self::load_data_from_file(&test_path, &self.config, false)?;
        let load_time_uncompressed = start.elapsed();
        let uncompressed_size = fs::metadata(&test_path)?.len();
        
        // Test with compression
        let start = std::time::Instant::now();
        Self::save_data_to_file(&test_path, &test_data, &self.config, true)?;
        let save_time_compressed = start.elapsed();
        
        let start = std::time::Instant::now();
        let _: Vec<u8> = Self::load_data_from_file(&test_path, &self.config, true)?;
        let load_time_compressed = start.elapsed();
        let compressed_size = fs::metadata(&test_path)?.len();
        
        // Cleanup
        let _ = fs::remove_file(&test_path);
        
        println!("📊 Cache Benchmark Results:");
        println!("   ├── Uncompressed: Save {:.3}s, Load {:.3}s, Size {:.1}MB", 
                 save_time_uncompressed.as_secs_f32(),
                 load_time_uncompressed.as_secs_f32(),
                 uncompressed_size as f32 / 1024.0 / 1024.0);
        println!("   └── Compressed:   Save {:.3}s, Load {:.3}s, Size {:.1}MB ({:.1}% of original)", 
                 save_time_compressed.as_secs_f32(),
                 load_time_compressed.as_secs_f32(),
                 compressed_size as f32 / 1024.0 / 1024.0,
                 compressed_size as f32 / uncompressed_size as f32 * 100.0);
        
        Ok(())
    }
}