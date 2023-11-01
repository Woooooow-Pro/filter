# Oasis

Code release of "Oasis: An Optimal Disjoint Segmented Learned Range Filter"


## Directory Layout

- `Oasis/` - standalone Oasis code base.
  - `oasis/` - contains all the files necessary to the implementation of the Oasis filter.
  - `src/` - contains all the files necessary to the implementation of the OasisPlus filter.
  - `benchmark/` - contains all the files used for standalone filter benchmarks.
    - `workloads/` - contains all the files used for generating synthetic workloads and downloading real-world datasets.`
- `range_filter_exp/` - contains files used for system evaluations.

## Dependency

- `jq` (to download Internet domains dataset)
- `CMake`, `gcc9`, `g++9`, `gtest`, `lz4`, `gflags`, `zstandard`

```
# Linux
sudo apt-get install jq build-essential cmake libgtest.dev liblz4-dev libzstd-dev libgflags-dev
```

## Dataset
```
cd Oasis/benchmark/workloads
./setup.sh
```

`books_800M_uint64` and `fb_200M_uint64` from https://github.com/learnedsystems/SOSD


## Standalone Filter Benchmarks

```
cd Oasis
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER="/usr/bin/gcc-9" -DCMAKE_CXX_COMPILER="/usr/bin/g++-9" ..
make -j$(nproc) all

cd ../benchmark
./in_mem_bench.sh test > test.txt&
```


## System Evaulations 

### Build RocksDB

```sh
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release -DWITH_LZ4=ON -DWITH_ZSTD=ON ..
make -j$(nproc) workload_gen filter_experiment
```

### Run Experiments

```sh
cd range_filter_exp/bench
./rocksdb_exp.sh rocks_exp_result
```
