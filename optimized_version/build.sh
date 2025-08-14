#!/bin/bash

# Build script for optimized cache version

echo "Building optimized cache version..."
echo "=================================="

# Clean previous builds
echo "Cleaning previous builds..."
cargo clean

# Build with maximum performance optimizations
echo "Building with max-perf profile..."
cargo build --profile max-perf

# Create a symlink for easy access
if [ -f "target/max-perf/read_bruker_data" ]; then
    ln -sf target/max-perf/read_bruker_data read_bruker_data_optimized
    echo "Build successful! Executable linked as: read_bruker_data_optimized"
else
    # Fallback to release build if max-perf profile doesn't exist
    echo "Max-perf profile not found, building with release profile..."
    cargo build --release
    if [ -f "target/release/read_bruker_data" ]; then
        ln -sf target/release/read_bruker_data read_bruker_data_optimized
        echo "Build successful! Executable linked as: read_bruker_data_optimized"
    else
        echo "Build failed!"
        exit 1
    fi
fi

echo ""
echo "To run the optimized version:"
echo "  ./read_bruker_data_optimized"
echo ""
echo "Or with cargo:"
echo "  cargo run --release"
echo ""
echo "Cache will be stored in: .timstof_cache_optimized/"