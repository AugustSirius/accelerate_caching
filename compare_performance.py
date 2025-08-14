#!/usr/bin/env python3
"""
Performance comparison script for original vs optimized cache versions
"""

import subprocess
import time
import os
import json
import sys
from pathlib import Path

class PerformanceComparison:
    def __init__(self):
        self.original_dir = Path("原始版本")
        self.optimized_dir = Path("optimized_version")
        self.results = {
            "original": {},
            "optimized": {}
        }
    
    def clear_caches(self):
        """Clear both original and optimized caches"""
        print("Clearing caches...")
        
        # Clear original cache
        original_cache = Path(".timstof_cache")
        if original_cache.exists():
            import shutil
            shutil.rmtree(original_cache)
            print("  - Original cache cleared")
        
        # Clear optimized cache
        optimized_cache = Path(".timstof_cache_optimized")
        if optimized_cache.exists():
            import shutil
            shutil.rmtree(optimized_cache)
            print("  - Optimized cache cleared")
    
    def run_version(self, version_dir, version_name):
        """Run a specific version and capture timing information"""
        print(f"\nRunning {version_name} version...")
        print("=" * 50)
        
        # Build the project
        print(f"Building {version_name}...")
        build_result = subprocess.run(
            ["cargo", "build", "--release"],
            cwd=version_dir,
            capture_output=True,
            text=True
        )
        
        if build_result.returncode != 0:
            print(f"Build failed for {version_name}:")
            print(build_result.stderr)
            return None
        
        # Run the program and capture output
        start_time = time.time()
        result = subprocess.run(
            ["cargo", "run", "--release"],
            cwd=version_dir,
            capture_output=True,
            text=True,
            timeout=600  # 10 minute timeout
        )
        total_time = time.time() - start_time
        
        if result.returncode != 0:
            print(f"Execution failed for {version_name}:")
            print(result.stderr)
            return None
        
        # Parse output for timing information
        output_lines = result.stdout.split('\n')
        timings = {}
        
        for line in output_lines:
            if "cache loading time" in line.lower():
                try:
                    time_str = line.split(':')[-1].strip().split()[0]
                    timings["cache_loading"] = float(time_str)
                except:
                    pass
            elif "cache saving time" in line.lower():
                try:
                    time_str = line.split(':')[-1].strip().split()[0]
                    timings["cache_saving"] = float(time_str)
                except:
                    pass
            elif "raw data reading time" in line.lower():
                try:
                    time_str = line.split(':')[-1].strip().split()[0]
                    timings["raw_reading"] = float(time_str)
                except:
                    pass
            elif "total data preparation time" in line.lower():
                try:
                    time_str = line.split(':')[-1].strip().split()[0]
                    timings["total_preparation"] = float(time_str)
                except:
                    pass
        
        timings["total_execution"] = total_time
        
        # Check cache size
        if version_name == "original":
            cache_dir = Path(".timstof_cache")
        else:
            cache_dir = Path(".timstof_cache_optimized")
        
        if cache_dir.exists():
            cache_size = sum(f.stat().st_size for f in cache_dir.glob('**/*') if f.is_file())
            timings["cache_size_mb"] = cache_size / (1024 * 1024)
        
        return timings
    
    def compare_results(self):
        """Compare and display results"""
        print("\n" + "=" * 60)
        print("PERFORMANCE COMPARISON RESULTS")
        print("=" * 60)
        
        metrics = [
            ("Cache Loading", "cache_loading"),
            ("Cache Saving", "cache_saving"),
            ("Raw Data Reading", "raw_reading"),
            ("Total Preparation", "total_preparation"),
            ("Total Execution", "total_execution"),
            ("Cache Size (MB)", "cache_size_mb")
        ]
        
        print(f"\n{'Metric':<20} {'Original':<15} {'Optimized':<15} {'Speedup':<10}")
        print("-" * 60)
        
        for display_name, key in metrics:
            orig_val = self.results["original"].get(key, 0)
            opt_val = self.results["optimized"].get(key, 0)
            
            if orig_val > 0 and opt_val > 0:
                if key == "cache_size_mb":
                    speedup = orig_val / opt_val  # For size, smaller is better
                    speedup_str = f"{speedup:.2f}x smaller"
                else:
                    speedup = orig_val / opt_val
                    speedup_str = f"{speedup:.2f}x faster"
                
                print(f"{display_name:<20} {orig_val:<15.3f} {opt_val:<15.3f} {speedup_str:<10}")
            elif orig_val > 0:
                print(f"{display_name:<20} {orig_val:<15.3f} {'N/A':<15} {'N/A':<10}")
            elif opt_val > 0:
                print(f"{display_name:<20} {'N/A':<15} {opt_val:<15.3f} {'N/A':<10}")
    
    def run_comparison(self, clear_cache_first=True):
        """Run the full comparison"""
        print("Starting Performance Comparison")
        print("=" * 60)
        
        if clear_cache_first:
            self.clear_caches()
        
        # Run original version
        if self.original_dir.exists():
            original_results = self.run_version(self.original_dir, "original")
            if original_results:
                self.results["original"] = original_results
        else:
            print(f"Warning: Original version directory not found: {self.original_dir}")
        
        # Clear cache between runs for fair comparison
        if clear_cache_first:
            self.clear_caches()
        
        # Run optimized version
        if self.optimized_dir.exists():
            optimized_results = self.run_version(self.optimized_dir, "optimized")
            if optimized_results:
                self.results["optimized"] = optimized_results
        else:
            print(f"Warning: Optimized version directory not found: {self.optimized_dir}")
        
        # Compare results
        if self.results["original"] and self.results["optimized"]:
            self.compare_results()
        
        # Save results to JSON
        with open("performance_comparison.json", "w") as f:
            json.dump(self.results, f, indent=2)
        print(f"\nDetailed results saved to: performance_comparison.json")

def main():
    # Check if we're in the right directory
    if not Path("原始版本").exists() or not Path("optimized_version").exists():
        print("Error: This script must be run from the accelerate_caching directory")
        print("Current directory:", os.getcwd())
        sys.exit(1)
    
    comparison = PerformanceComparison()
    
    # Parse command line arguments
    clear_cache = "--no-clear" not in sys.argv
    
    if "--help" in sys.argv:
        print("Usage: python compare_performance.py [--no-clear]")
        print("  --no-clear: Don't clear caches before running (use existing caches)")
        sys.exit(0)
    
    comparison.run_comparison(clear_cache_first=clear_cache)

if __name__ == "__main__":
    main()