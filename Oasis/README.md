# Oasis

Code release of "Oasis: An Optimal Disjoint Segmented Learned Range Filter"

## Directory Layout

- `oasis/` - contains all the files necessary to the implementation of the Oasis filter.

- `src/` - contains all the files necessary to the implementation of the OasisPlus filter.

- `benchmark/` - contains all the files used for standalone filter benchmarks.
  - `workloads/` - contains all the files used for generating synthetic workloads and downloading real-world datasets.`

## Dependency

- `jq` (to download Internet domains dataset)
- `CMake`, `gcc9`, `g++9`

```
# Linux
sudo apt-get install jq cmake
```

## Dataset

```
cd benchmark/workloads
./setup.sh
```

`books_800M_uint64` and `fb_200M_uint64` from https://github.com/learnedsystems/SOSD


## Standalone Filter Benchmarks

```
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make all

cd ../benchmark
./in_mem_bench.sh test > test.txt&
```