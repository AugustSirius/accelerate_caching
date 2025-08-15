// File: src/cache.rs
use std::path::{Path, PathBuf};
use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use bincode;
use std::time::SystemTime;
use rayon::prelude::*;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::utils::{TimsTOFRawData, IndexedTimsTOFData};

#[derive(Clone)]
pub struct CacheConfig {
    pub enable_compression: bool,
    pub compression_level: u32,
    pub buffer_size: usize,
    pub parallel_io: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enable_compression: true,
            compression_level: 4, // Fast compression
            buffer_size: 1024 * 1024 * 128, // 128MB buffer
            parallel_io: true,
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
        let extension = if self.config.enable_compression { "cache.lz4" } else { "cache" };
        let cache_name = format!("{}.{}.{}", source_name, cache_type, extension);
        self.cache_dir.join(cache_name)
    }
    
    fn get_metadata_path(&self, source_path: &Path) -> PathBuf {
        let source_name = source_path.file_name().unwrap().to_str().unwrap();
        let meta_name = format!("{}.meta", source_name);
        self.cache_dir.join(meta_name)
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
    
    // Optimized parallel save function
    pub fn save_indexed_data(
        &self, 
        source_path: &Path, 
        ms1_indexed: &IndexedTimsTOFData,
        ms2_indexed_pairs: &Vec<((f32, f32), IndexedTimsTOFData)>
    ) -> Result<(), Box<dyn std::error::Error>> {
        println!("Saving indexed data to cache with optimizations...");
        let start_time = std::time::Instant::now();
        
        if self.config.parallel_io {
            // Parallel save using scoped threads to avoid lifetime issues
            thread::scope(|s| -> Result<(), Box<dyn std::error::Error>> {
                let ms1_result: Arc<Mutex<Option<Result<(), std::io::Error>>>> = Arc::new(Mutex::new(None));
                let ms2_result: Arc<Mutex<Option<Result<(), std::io::Error>>>> = Arc::new(Mutex::new(None));
                let meta_result: Arc<Mutex<Option<Result<(), std::io::Error>>>> = Arc::new(Mutex::new(None));
                
                let ms1_result_clone = Arc::clone(&ms1_result);
                let ms2_result_clone = Arc::clone(&ms2_result);
                let meta_result_clone = Arc::clone(&meta_result);
                
                // MS1 save thread
                let ms1_path = self.get_cache_path(source_path, "ms1_indexed");
                let ms1_config = self.config.clone();
                let ms1_handle = s.spawn(move || {
                    let result = Self::save_data_to_file(&ms1_path, ms1_indexed, &ms1_config);
                    *ms1_result_clone.lock().unwrap() = Some(result);
                });
                
                // MS2 save thread
                let ms2_path = self.get_cache_path(source_path, "ms2_indexed");
                let ms2_config = self.config.clone();
                let ms2_handle = s.spawn(move || {
                    let result = Self::save_data_to_file(&ms2_path, ms2_indexed_pairs, &ms2_config);
                    *ms2_result_clone.lock().unwrap() = Some(result);
                });
                
                // Metadata save thread
                let meta_path = self.get_metadata_path(source_path);
                let meta_config = self.config.clone();
                let ms2_len = ms2_indexed_pairs.len();
                let meta_handle = s.spawn(move || {
                    let metadata = format!(
                        "cached at: {:?}\nms2_windows: {}\ntype: indexed\ncompression: {}\n",
                        SystemTime::now(),
                        ms2_len,
                        meta_config.enable_compression
                    );
                    let result = fs::write(meta_path, metadata);
                    *meta_result_clone.lock().unwrap() = Some(result);
                });
                
                // Wait for all threads to complete
                let _ = ms1_handle.join();
                let _ = ms2_handle.join();
                let _ = meta_handle.join();
                
                // Check results
                if let Some(result) = ms1_result.lock().unwrap().take() {
                    result?;
                }
                if let Some(result) = ms2_result.lock().unwrap().take() {
                    result?;
                }
                if let Some(result) = meta_result.lock().unwrap().take() {
                    result?;
                }
                
                Ok(())
            })?;
        } else {
            // Sequential save (fallback)
            let ms1_cache_path = self.get_cache_path(source_path, "ms1_indexed");
            let ms2_cache_path = self.get_cache_path(source_path, "ms2_indexed");
            
            Self::save_data_to_file(&ms1_cache_path, ms1_indexed, &self.config)?;
            Self::save_data_to_file(&ms2_cache_path, ms2_indexed_pairs, &self.config)?;
            
            // Save metadata
            let meta_path = self.get_metadata_path(source_path);
            let metadata = format!(
                "cached at: {:?}\nms2_windows: {}\ntype: indexed\ncompression: {}\n",
                SystemTime::now(),
                ms2_indexed_pairs.len(),
                self.config.enable_compression
            );
            fs::write(meta_path, metadata)?;
        }
        
        let elapsed = start_time.elapsed();
        let ms1_size = fs::metadata(self.get_cache_path(source_path, "ms1_indexed"))?.len();
        let ms2_size = fs::metadata(self.get_cache_path(source_path, "ms2_indexed"))?.len();
        let total_size_mb = (ms1_size + ms2_size) as f32 / 1024.0 / 1024.0;
        
        println!("Indexed cache saved: {:.2} MB total, time: {:.3}s (parallel: {})", 
                 total_size_mb, elapsed.as_secs_f32(), self.config.parallel_io);
        Ok(())
    }
    
    // Optimized parallel load function
    pub fn load_indexed_data(
        &self, 
        source_path: &Path
    ) -> Result<(IndexedTimsTOFData, Vec<((f32, f32), IndexedTimsTOFData)>), Box<dyn std::error::Error>> {
        println!("Loading indexed data from cache with optimizations...");
        let start_time = std::time::Instant::now();
        
        if self.config.parallel_io {
            // Parallel load using scoped threads
            let (ms1_indexed, ms2_indexed_pairs) = thread::scope(|s| -> Result<(IndexedTimsTOFData, Vec<((f32, f32), IndexedTimsTOFData)>), Box<dyn std::error::Error>> {
                let ms1_result: Arc<Mutex<Option<Result<IndexedTimsTOFData, std::io::Error>>>> = Arc::new(Mutex::new(None));
                let ms2_result: Arc<Mutex<Option<Result<Vec<((f32, f32), IndexedTimsTOFData)>, std::io::Error>>>> = Arc::new(Mutex::new(None));
                
                let ms1_result_clone = Arc::clone(&ms1_result);
                let ms2_result_clone = Arc::clone(&ms2_result);
                
                // MS1 load thread
                let ms1_path = self.get_cache_path(source_path, "ms1_indexed");
                let ms1_config = self.config.clone();
                let ms1_handle = s.spawn(move || {
                    let result = Self::load_data_from_file(&ms1_path, &ms1_config);
                    *ms1_result_clone.lock().unwrap() = Some(result);
                });
                
                // MS2 load thread
                let ms2_path = self.get_cache_path(source_path, "ms2_indexed");
                let ms2_config = self.config.clone();
                let ms2_handle = s.spawn(move || {
                    let result = Self::load_data_from_file(&ms2_path, &ms2_config);
                    *ms2_result_clone.lock().unwrap() = Some(result);
                });
                
                // Wait for both threads to complete
                let _ = ms1_handle.join();
                let _ = ms2_handle.join();
                
                // Extract results
                let ms1_indexed = ms1_result.lock().unwrap().take().unwrap()?;
                let ms2_indexed_pairs = ms2_result.lock().unwrap().take().unwrap()?;
                
                Ok((ms1_indexed, ms2_indexed_pairs))
            })?;
            
            let elapsed = start_time.elapsed();
            println!("Indexed cache loaded (time: {:.3}s, parallel: true)", elapsed.as_secs_f32());
            Ok((ms1_indexed, ms2_indexed_pairs))
        } else {
            // Sequential load (fallback)
            let ms1_cache_path = self.get_cache_path(source_path, "ms1_indexed");
            let ms2_cache_path = self.get_cache_path(source_path, "ms2_indexed");
            
            let ms1_indexed = Self::load_data_from_file(&ms1_cache_path, &self.config)?;
            let ms2_indexed_pairs = Self::load_data_from_file(&ms2_cache_path, &self.config)?;
            
            let elapsed = start_time.elapsed();
            println!("Indexed cache loaded (time: {:.3}s, parallel: false)", elapsed.as_secs_f32());
            Ok((ms1_indexed, ms2_indexed_pairs))
        }
    }
    
    // Generic save function with compression support
    fn save_data_to_file<T>(
        path: &Path,
        data: &T,
        config: &CacheConfig,
    ) -> Result<(), std::io::Error>
    where
        T: serde::Serialize + ?Sized,
    {
        let file = File::create(path)?;
        let writer = BufWriter::with_capacity(config.buffer_size, file);
        
        if config.enable_compression {
            // Use LZ4 compression for faster I/O
            let mut encoder = lz4_flex::frame::FrameEncoder::new(writer);
            bincode::serialize_into(&mut encoder, data)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            encoder.finish()
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        } else {
            bincode::serialize_into(writer, data)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        }
        
        Ok(())
    }
    
    // Generic load function with compression support
    fn load_data_from_file<T>(
        path: &Path,
        config: &CacheConfig,
    ) -> Result<T, std::io::Error>
    where
        T: serde::de::DeserializeOwned,
    {
        let file = File::open(path)?;
        let reader = BufReader::with_capacity(config.buffer_size, file);
        
        if config.enable_compression {
            // Use LZ4 decompression
            let decoder = lz4_flex::frame::FrameDecoder::new(reader);
            let data = bincode::deserialize_from(decoder)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            Ok(data)
        } else {
            let data = bincode::deserialize_from(reader)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
            Ok(data)
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
                
                // Check for both .cache and .cache.lz4 extensions
                if file_name.ends_with(".cache") || file_name.ends_with(".cache.lz4") {
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
    
    // Method to configure cache settings based on available threads
    pub fn configure_for_threads(mut self, thread_count: usize) -> Self {
        // Adjust configuration based on thread count
        if thread_count == 1 {
            self.config.parallel_io = false;
        } else {
            self.config.parallel_io = true;
            // Increase buffer size for parallel processing
            self.config.buffer_size = (1024 * 1024 * 64 * thread_count.min(4)).max(1024 * 1024 * 64);
        }
        self
    }
}