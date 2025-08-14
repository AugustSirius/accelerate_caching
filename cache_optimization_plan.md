# Cache.rs 优化方案文档

## 现状分析

### 当前实现的问题
1. **单线程串行I/O**: 当前使用单一线程进行cache的读写操作，未利用`parallel_threads`参数
2. **无压缩**: 直接使用bincode序列化，数据体积大，I/O传输时间长
3. **单文件存储**: MS1和MS2数据分别存储为单个大文件，无法并行读取部分数据
4. **缓冲区固定**: 使用固定64MB缓冲区，可能不适合所有数据大小
5. **无内存映射**: 使用传统文件I/O，而非更高效的内存映射

## 综合优化方案

### 1. 并行化策略

#### 1.1 数据分片存储
```rust
// 将数据按照mz范围分成多个片段
// MS1: 按mz值分成N个区间（N = parallel_threads）
// MS2: 按window分组，每组可独立存储
```

**实现细节**:
- 将`IndexedTimsTOFData`按mz范围分成`parallel_threads`个片段
- 每个片段独立序列化到不同文件
- MS2数据按窗口分组，每个窗口组可以独立处理

#### 1.2 并行I/O操作
```rust
// 使用rayon并行读写多个cache片段
// 利用main.rs中配置的parallel_threads数量
```

**实现方式**:
- 使用`rayon::par_iter()`并行处理每个数据片段
- 每个线程负责一个片段的序列化/反序列化
- 使用`Arc<DashMap>`协调并发访问

### 2. 压缩优化

#### 2.1 多级压缩策略
```rust
// 根据数据特征选择不同压缩算法
// - 整数数组(frame_indices, scan_indices): 使用LZ4
// - 浮点数组(mz_values, rt_values): 使用Zstd
// - 强度值(intensity_values): 使用差分编码+LZ4
```

**压缩库选择**:
- 添加依赖: `lz4 = "1.24"`, `zstd = "0.13"`
- 压缩级别动态调整: 根据数据大小和CPU核心数选择

#### 2.2 智能压缩决策
```rust
// 根据数据大小决定是否压缩
// 小于1MB的数据不压缩（避免压缩开销）
// 大于1MB的数据使用并行压缩
```

### 3. 内存映射优化

#### 3.1 使用mmap进行快速加载
```rust
// 使用memmap2库实现内存映射
// 避免数据拷贝，直接从磁盘映射到内存
```

**实现细节**:
- 添加依赖: `memmap2 = "0.9"`
- 对于只读操作使用`MmapOptions::map()`
- 支持延迟加载，按需访问数据

### 4. 缓存结构优化

#### 4.1 索引文件设计
```rust
struct CacheIndex {
    version: u32,
    compression_type: CompressionType,
    shard_count: usize,
    shard_info: Vec<ShardInfo>,
}

struct ShardInfo {
    mz_range: (f32, f32),
    file_offset: u64,
    compressed_size: u64,
    uncompressed_size: u64,
}
```

#### 4.2 增量更新支持
- 记录数据版本和时间戳
- 支持部分数据更新而不重写整个cache
- 使用WAL（Write-Ahead Logging）确保数据一致性

### 5. 异步I/O优化

#### 5.1 使用tokio异步运行时
```rust
// 对于大文件操作使用异步I/O
// 避免阻塞主线程
```

**实现方式**:
- 添加依赖: `tokio = { version = "1", features = ["fs", "rt-multi-thread"] }`
- 使用`tokio::fs`进行异步文件操作
- 结合`futures::stream`实现流式处理

### 6. 智能缓冲区管理

#### 6.1 动态缓冲区大小
```rust
fn calculate_buffer_size(data_size: usize, available_memory: usize) -> usize {
    // 根据数据大小和可用内存动态调整
    let ideal_size = data_size / parallel_threads;
    let max_size = available_memory / 4; // 使用1/4可用内存
    ideal_size.min(max_size).max(1024 * 1024) // 最小1MB
}
```

#### 6.2 缓冲池复用
```rust
// 使用对象池复用缓冲区
// 避免频繁分配/释放内存
```

### 7. 数据布局优化

#### 7.1 列式存储
```rust
// 将相同类型的数据连续存储
// 提高缓存局部性和压缩率
struct ColumnStore {
    rt_column: Vec<f32>,
    mobility_column: Vec<f32>,
    mz_column: Vec<f32>,
    intensity_column: Vec<u32>,
    frame_column: Vec<u32>,
    scan_column: Vec<u32>,
}
```

#### 7.2 数据对齐
- 确保数据按64字节边界对齐（缓存行大小）
- 使用SIMD友好的数据布局

### 8. 预处理优化

#### 8.1 预计算统计信息
```rust
struct CacheStats {
    total_points: usize,
    mz_range: (f32, f32),
    rt_range: (f32, f32),
    im_range: (f32, f32),
    intensity_histogram: Vec<u32>,
}
```

#### 8.2 构建快速查找索引
- 预构建B+树索引加速mz范围查询
- 使用布隆过滤器快速判断数据存在性

## 实施步骤

### 第一阶段: 基础并行化（影响最小）
1. 实现数据分片
2. 添加rayon并行读写
3. 保持与现有接口兼容

### 第二阶段: 压缩优化
1. 集成LZ4/Zstd压缩
2. 实现智能压缩决策
3. 添加压缩性能监控

### 第三阶段: 高级优化
1. 实现内存映射
2. 添加异步I/O支持
3. 优化数据布局

## 性能预期

基于以上优化方案，预计可以实现:

1. **加载速度提升**: 3-5倍
   - 并行读取: 2-3倍提升
   - 内存映射: 额外20-30%提升
   
2. **保存速度提升**: 2-4倍
   - 并行写入: 2-3倍提升
   - 智能压缩: 减少I/O量30-50%

3. **内存使用优化**: 减少20-30%
   - 列式存储提高压缩率
   - 缓冲池复用减少分配

4. **CPU利用率**: 提升至80-90%
   - 充分利用所有`parallel_threads`
   - 并行压缩/解压缩

## 兼容性保证

1. **向后兼容**: 保留原有API接口
2. **渐进式迁移**: 新旧cache格式共存
3. **自动转换**: 检测旧格式自动转换为新格式
4. **配置可选**: 通过环境变量控制优化级别

## 推荐的具体实现顺序

1. **立即实施**（影响小，收益大）:
   - 数据分片 + rayon并行化
   - 动态缓冲区大小调整

2. **短期实施**（需要测试）:
   - LZ4压缩集成
   - 列式存储布局

3. **长期优化**（需要充分测试）:
   - 内存映射
   - 异步I/O
   - 增量更新

## 代码示例

### 并行保存示例
```rust
pub fn save_indexed_data_parallel(
    &self,
    source_path: &Path,
    ms1_indexed: &IndexedTimsTOFData,
    ms2_indexed_pairs: &Vec<((f32, f32), IndexedTimsTOFData)>,
    parallel_threads: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    use rayon::prelude::*;
    
    // 分片MS1数据
    let ms1_shards = split_into_shards(ms1_indexed, parallel_threads);
    
    // 并行保存MS1分片
    ms1_shards.par_iter().enumerate().try_for_each(|(i, shard)| {
        let shard_path = self.get_cache_path(source_path, &format!("ms1_shard_{}", i));
        save_shard_compressed(shard, &shard_path)
    })?;
    
    // 并行保存MS2窗口
    ms2_indexed_pairs.par_iter().enumerate().try_for_each(|(i, (range, data))| {
        let window_path = self.get_cache_path(source_path, &format!("ms2_window_{}", i));
        save_compressed(data, &window_path)
    })?;
    
    Ok(())
}
```

### 并行加载示例
```rust
pub fn load_indexed_data_parallel(
    &self,
    source_path: &Path,
    parallel_threads: usize,
) -> Result<(IndexedTimsTOFData, Vec<((f32, f32), IndexedTimsTOFData)>), Box<dyn std::error::Error>> {
    use rayon::prelude::*;
    
    // 并行加载MS1分片
    let ms1_shards: Vec<_> = (0..parallel_threads)
        .into_par_iter()
        .map(|i| {
            let shard_path = self.get_cache_path(source_path, &format!("ms1_shard_{}", i));
            load_shard_compressed(&shard_path)
        })
        .collect::<Result<Vec<_>, _>>()?;
    
    // 合并MS1分片
    let ms1_indexed = merge_shards(ms1_shards);
    
    // 并行加载MS2窗口
    let ms2_indexed_pairs = load_ms2_windows_parallel(source_path)?;
    
    Ok((ms1_indexed, ms2_indexed_pairs))
}
```

## 总结

通过以上优化方案的实施，cache.rs的性能将得到显著提升，充分利用多核CPU的并行处理能力，同时通过压缩和内存映射等技术减少I/O开销。整个优化过程可以分阶段进行，确保系统稳定性的同时逐步提升性能。