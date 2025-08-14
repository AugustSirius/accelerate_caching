#!/bin/bash
#slurm options
#SBATCH -p amd-ep2,intel-sc3,amd-ep2-short
#SBATCH -q normal
#SBATCH -J rust
#SBATCH -c 1
#SBATCH -n 32
#SBATCH --mem 200G
########################## MSConvert run #####################
# module
module load gcc
cd /storage/guotiannanLab/wangshuaiyao/006.DIABERT_TimsTOF_Rust/accelerate_caching/original_version
cargo run --release


