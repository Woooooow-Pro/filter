#include <algorithm>
#include <fstream>
#include <iostream>
#include <random>
#include <vector>

#include "filter_exp_util.h"
#include "filter_test_util.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/table.h"
#include "rocksdb/write_batch.h"

#define INIT_EXP_TIMER auto start = std::chrono::high_resolution_clock::now();
#define START_EXP_TIMER start = std::chrono::high_resolution_clock::now();
#define STOP_EXP_TIMER(name)                                          \
  std::cout << "RUNTIME of " << name << ": "                          \
            << std::chrono::duration_cast<std::chrono::microseconds>( \
                   std::chrono::high_resolution_clock::now() - start) \
                   .count()                                           \
            << " us " << std::endl;
#define SAVE_EXP_TIMER                                             \
  rescsv << std::chrono::duration_cast<std::chrono::microseconds>( \
                std::chrono::high_resolution_clock::now() - start) \
                .count()                                           \
         << ",";
#define SAVE_TO_RESCSV(val) rescsv << val << ",";

std::ofstream rescsv;
bool is_int_bench;
double bpk;
size_t block_sz = 150;
size_t max_qlen = 10;

bool use_Oasis = false;
bool use_OasisPlus = false;

void init(rocksdb::DB** db, rocksdb::Options* options,
          rocksdb::BlockBasedTableOptions* table_options) {
  // Create the corresponding filter policies
  if (use_Oasis) {
    table_options->filter_policy.reset(
        rocksdb::NewOasisFilterPolicy(bpk, block_sz));
  } else if (use_OasisPlus) {
    table_options->filter_policy.reset(
        rocksdb::NewOasisPlusFilterPolicy(bpk, block_sz, max_qlen));
  }

  std::cout << "Using " << table_options->filter_policy->Name() << "\n";

  options->create_if_missing = true;
  options->statistics = rocksdb::CreateDBStatistics();

  options->write_buffer_size =
      64 * 1048576;  // Size of memtable = Size of SST file (64 MB)
  options->max_bytes_for_level_base = 4 * 64 * 1048576;  // 4 SST files at L1
  options->target_file_size_base = 64 * 1048576;  // Each SST file is 64 MB

  // Force L0 to be empty for consistent LSM tree shape
  options->level0_file_num_compaction_trigger = 1;

  table_options->pin_l0_filter_and_index_blocks_in_cache = true;
  table_options->cache_index_and_filter_blocks = true;
  table_options->cache_index_and_filter_blocks_with_high_priority = true;

  table_options->block_cache =
      rocksdb::NewLRUCache(1024 * 1024 * 1024);  // 1 GB Block Cache

  // higher read-ahead generally recommended for disks,
  // for flash/ssd generally 0 is ok, as can unnecessarily
  // cause extra read-amp on smaller compactions
  options->compaction_readahead_size = 0;

  table_options->partition_filters = false;

  // no mmap for reads nor writes
  options->allow_mmap_reads = false;
  options->allow_mmap_writes = false;

  // direct I/O usage points
  options->use_direct_reads = true;
  options->use_direct_io_for_flush_and_compaction = true;

  // Enable compression -> more keys per SST file = more valid sample queries
  // per filter, bigger filters Don't compress first few levels and use better
  // but slower compression at deeper levels
  options->num_levels = 4;
  options->compression_per_level.resize(options->num_levels);
  for (int i = 0; i < options->num_levels; ++i) {
    if (i < 2) {
      options->compression_per_level[i] =
          rocksdb::CompressionType::kNoCompression;
    } else if (i == 2) {
      options->compression_per_level[i] =
          rocksdb::CompressionType::kLZ4Compression;
    } else {
      options->compression_per_level[i] = rocksdb::CompressionType::kZSTD;
    }
  }

  /*
   *  By default, RocksDB uses only one background thread for flush and
   *  compaction. Calling this function will set it up such that total of
   *  `total_threads` is used. Good value for `total_threads` is the number
   *  of cores. You almost definitely want to call this function if your system
   *  is bottlenecked by RocksDB.
   */
  options->IncreaseParallelism(60);

  // Default setting - pre-load indexes and filters
  options->max_open_files = -1;

  options->table_factory.reset(
      rocksdb::NewBlockBasedTableFactory(*table_options));

  // Open database
  const std::string db_path = "./db/";
  rocksdb::Status status = rocksdb::DB::Open(*options, db_path, db);
  assert(status.ok());
}

void loadInitialKeysIntoDB(rocksdb::DB* db,
                           const std::vector<std::string>& keys,
                           const std::vector<rocksdb::Slice>& vals) {
  rocksdb::WriteOptions write_options = rocksdb::WriteOptions();
  rocksdb::Status s;

  // Use RocksDB Put to get "normal" LSM tree shape (all levels populated
  // somewhat)
  for (size_t i = 0; i < keys.size(); i++) {
    s = db->Put(write_options, rocksdb::Slice(keys[i]), vals[i]);
    if (!s.ok()) {
      std::cout << s.ToString().c_str() << "\n";
      assert(false);
    }
  }
}

void runQuery(rocksdb::DB* db, const std::pair<std::string, std::string>& q) {
  std::string found_key;
  std::string found_value;
  rocksdb::Slice lower_key(q.first);
  rocksdb::Slice upper_key(q.second);

  // Do a Get if it is a point query
  // Else do a Seek for the range query
  if (isPointQuery(q.first, q.second)) {
    rocksdb::Status s =
        db->Get(rocksdb::ReadOptions(), lower_key, &found_value);
    assert(s.IsNotFound() || s.ok());
  } else {
    rocksdb::ReadOptions read_options = rocksdb::ReadOptions();
    read_options.iterate_upper_bound = &upper_key;
    rocksdb::Iterator* it = db->NewIterator(read_options);

    for (it->Seek(lower_key); it->Valid(); it->Next()) {
      assert(it->value().size() == VAL_SZ);
      found_key = it->key().data();
      found_value = it->value().data();
      (void)found_key;
      (void)found_value;
    }

    if (!it->status().ok()) {
      std::cout << "ERROR in RocksDB Iterator: " << it->status().ToString()
                << std::endl;
      exit(1);
    }

    delete it;
  }
}

void runInitialReadWorkload(
    rocksdb::DB* db,
    const std::vector<std::pair<std::string, std::string>>& queries) {
  for (size_t i = 0; i < queries.size(); ++i) {
    runQuery(db, queries[i]);
  }
}

void runExperiment(
    std::vector<std::vector<std::string>>& keys,
    std::vector<std::vector<rocksdb::Slice>> vals,
    std::vector<std::vector<std::pair<std::string, std::string>>>& queries) {
  INIT_EXP_TIMER

  // Configure and initialize database
  rocksdb::DB* db;
  rocksdb::Options options;
  rocksdb::BlockBasedTableOptions table_options;
  init(&db, &options, &table_options);

  START_EXP_TIMER
  loadInitialKeysIntoDB(db, keys[0], vals[0]);
  STOP_EXP_TIMER("Load Keys into DB")

  START_EXP_TIMER
  flushMemTable(db);
  STOP_EXP_TIMER("Flush MemTable")

  START_EXP_TIMER
  waitForBGCompactions(db);
  STOP_EXP_TIMER("Wait for Background Compactions")

  printCompactionAndDBStats(db);

  // Reset performance stats
  rocksdb::SetPerfLevel(
      rocksdb::PerfLevel::kEnableTimeAndCPUTimeExceptForMutex);
  rocksdb::get_perf_context()->Reset();
  rocksdb::get_perf_context()->ClearPerLevelPerfContext();
  rocksdb::get_perf_context()->EnablePerLevelPerfContext();
  rocksdb::get_iostats_context()->Reset();

  START_EXP_TIMER
  runInitialReadWorkload(db, queries[0]);
  STOP_EXP_TIMER("Initial Read Workload")
  SAVE_EXP_TIMER

  // Print Initial Read Workload FPR
  printFPR(&options, rescsv);
  printCompactionAndDBStats(db);

  printStats(db, &options, rescsv);

  // Finish result line in result csv and close file stream
  rescsv << std::endl;
  rescsv.close();

  // Close database
  rocksdb::Status s = db->Close();
  assert(s.ok());
  delete db;
}

int main(int argc, char** argv) {
  /****************************************
      Arguments from filter_experiment.sh
      ==================================

      Common arguments:
          $is_int_bench
          {"Proteus, "SuRF"}
          $RES_CSV

      Oasis:
          $membudg
          $block_sz

      OasisPlus:
          $membudg
          $block_sz
          $max_qlen

  ****************************************/

  use_Oasis = (strcmp(argv[1], "Oasis") == 0);
  use_OasisPlus = (strcmp(argv[1], "OasisPlus") == 0);

  printf("%s\t", argv[1]);

  // Open result csv to append results to file
  rescsv.open(argv[2], std::ios_base::app);

  if (use_Oasis) {
    bpk = strtod(argv[3], nullptr);
    block_sz = strtoull(argv[4], nullptr, 10);
  } else if (use_OasisPlus) {
    bpk = strtod(argv[3], nullptr);
    block_sz = strtoull(argv[4], nullptr, 10);
    max_qlen = std::strtoull(argv[5], nullptr, 10);
    max_qlen = 64ULL - __builtin_clzll(max_qlen) - 1;
  }

  auto kvps = intLoadKeysValues();
  std::vector<std::vector<std::pair<std::string, std::string>>> queries =
      intLoadQueries();

  runExperiment(kvps.first, kvps.second, queries);

  return 0;
}
