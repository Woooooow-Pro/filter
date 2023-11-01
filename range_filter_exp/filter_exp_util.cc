#include "filter_exp_util.h"

#include <unistd.h>

#include <filesystem>
#include <iostream>
#include <random>

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/options.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/sst_file_writer.h"
#include "rocksdb/table.h"
#include "rocksdb/write_batch.h"

auto uint64ToString(const uint64_t word) -> std::string {
  uint64_t endian_swapped_word = __builtin_bswap64(word);
  return std::string(reinterpret_cast<const char*>(&endian_swapped_word), 8);
}

auto stringToUint64(const std::string& str_word) -> uint64_t {
  uint64_t int_word = 0;
  memcpy(reinterpret_cast<char*>(&int_word), str_word.data(), 8);
  return __builtin_bswap64(int_word);
}

// assume compression ratio = 0.5
void setValueBuffer(char* value_buf, int size, std::mt19937_64& e,
                    std::uniform_int_distribution<unsigned long long>& dist) {
  memset(value_buf, 0, size);
  int pos = size / 2;
  while (pos < size) {
    uint64_t num = dist(e);
    char* num_bytes = reinterpret_cast<char*>(&num);
    memcpy(value_buf + pos, num_bytes, 8);
    pos += 8;
  }
}

/*
 * Generate values for multiple workloads except for the initial read workload
 */
template <typename T>
auto generateValues(const std::vector<std::vector<T>>& keys)
    -> std::vector<std::vector<rocksdb::Slice>> {
  char value_buf[VAL_SZ];
  std::mt19937_64 e(2017);
  std::uniform_int_distribution<unsigned long long> dist(0, ULLONG_MAX);
  std::vector<std::vector<rocksdb::Slice>> vals(keys.size(),
                                                std::vector<rocksdb::Slice>());

  for (size_t i = 0; i < keys.size(); i++) {
    for (size_t j = 0; j < keys[i].size(); j++) {
      setValueBuffer(value_buf, VAL_SZ, e, dist);
      vals[i].push_back(rocksdb::Slice(value_buf, VAL_SZ));
    }
  }

  return vals;
}

auto intLoadKeysValues()
    -> std::pair<std::vector<std::vector<std::string>>,
                 std::vector<std::vector<rocksdb::Slice>>> {
  std::ifstream keyFile;
  std::vector<std::vector<std::string>> keys;
  uint64_t key;
  size_t idx = 0;

  // Iterate over all key files
  while (std::filesystem::exists(dataPath + "data" + std::to_string(idx) +
                                 ".txt")) {
    keys.push_back(std::vector<std::string>());
    keyFile.open(dataPath + "data" + std::to_string(idx) + ".txt");
    while (keyFile >> key) {
      keys.back().push_back(uint64ToString(key));
    }

    keyFile.close();
    idx++;
  }

  return std::make_pair(keys, generateValues(keys));
}

auto intLoadQueries()
    -> std::vector<std::vector<std::pair<std::string, std::string>>> {
  std::ifstream lQueryFile, uQueryFile;
  std::vector<std::vector<std::pair<std::string, std::string>>> queries;
  uint64_t lq, uq;
  size_t idx = 0;

  // Iterate over all query files
  while (std::filesystem::exists(dataPath + "txn" + std::to_string(idx) +
                                 ".txt") &&
         std::filesystem::exists(dataPath + "upper_bound" +
                                 std::to_string(idx) + ".txt")) {
    queries.push_back(std::vector<std::pair<std::string, std::string>>());
    lQueryFile.open(dataPath + "txn" + std::to_string(idx) + ".txt");
    uQueryFile.open(dataPath + "upper_bound" + std::to_string(idx) + ".txt");
    while ((lQueryFile >> lq) && (uQueryFile >> uq)) {
      assert(lq <= uq);
      queries.back().push_back(
          std::make_pair(uint64ToString(lq), uint64ToString(uq)));
    }

    lQueryFile.close();
    uQueryFile.close();
    idx++;
  }

  return queries;
}

void printCompactionAndDBStats(rocksdb::DB* db) {
  std::string stats;
  db->GetProperty("rocksdb.stats", &stats);
  printf("%s", stats.c_str());
}

void printLSM(rocksdb::DB* db) {
  std::cout << "Print LSM" << std::endl;
  rocksdb::ColumnFamilyMetaData cf_meta;
  db->GetColumnFamilyMetaData(&cf_meta);

  std::cout << "Total Size (bytes): " << cf_meta.size << std::endl;
  std::cout << "Total File Count: " << cf_meta.file_count << std::endl;

  int largest_used_level = -1;
  for (auto level : cf_meta.levels) {
    if (level.files.size() > 0) {
      largest_used_level = level.level;
    }
  }

  std::cout << "Largest Level: " << largest_used_level << std::endl;
  for (auto level : cf_meta.levels) {
    long level_size = 0;
    for (auto file : level.files) {
      level_size += file.size;
    }
    std::cout << "level " << level.level << ".  Size " << level_size << " bytes"
              << std::endl;
    std::cout << std::endl;
    for (auto file : level.files) {
      std::cout << " \t " << file.size << " bytes \t " << file.name
                << std::endl;
    }
    if (level.level == largest_used_level) {
      break;
    }
  }

  std::cout << std::endl;
}

void flushMemTable(rocksdb::DB* db) {
  rocksdb::FlushOptions flush_opt;
  flush_opt.wait = true;
  rocksdb::Status s = db->Flush(flush_opt);
  assert(s.ok());
}

void waitForBGCompactions(rocksdb::DB* db) {
  bool double_checked = false;
  uint64_t prop;
  while (true) {
    // Check stats every 10s
    sleep(10);

    if (!(db->GetIntProperty("rocksdb.num-running-flushes", &prop))) continue;
    if (prop > 0) continue;
    if (!(db->GetIntProperty("rocksdb.num-running-compactions", &prop)))
      continue;
    if (prop > 0) continue;
    if (!(db->GetIntProperty("rocksdb.mem-table-flush-pending", &prop)))
      continue;
    if (prop == 1) continue;
    if (!(db->GetIntProperty("rocksdb.compaction-pending", &prop))) continue;
    if (prop == 1) continue;

    if (double_checked) {
      break;
    } else {
      double_checked = true;
      continue;
    }
  }

  // Print out initial LSM state
  printLSM(db);
}

void printFPR(rocksdb::Options* options, std::ofstream& stream) {
  uint32_t hits =
               options->statistics->getTickerCount(rocksdb::RANGE_FILTER_HIT),
           misses =
               options->statistics->getTickerCount(rocksdb::RANGE_FILTER_MISS),
           uses =
               options->statistics->getTickerCount(rocksdb::RANGE_FILTER_USE);
  printf("Uses: %u, Misses: %u, Hits: %u\n", uses, misses, hits);
  printf("Overall False Positive Rate: %Lf\n",
         (long double)misses / (uses - hits));
  stream << (long double)misses / (uses - hits) << ",";
}

void printStats(rocksdb::DB* db, rocksdb::Options* options,
                std::ofstream& stream) {
  sleep(10);

  // STOP ROCKS PROFILE
  rocksdb::SetPerfLevel(rocksdb::PerfLevel::kDisable);

  std::cout << "RocksDB Perf Context : " << std::endl;

  std::cout << rocksdb::get_perf_context()->ToString() << std::endl;

  std::cout << "RocksDB Iostats Context : " << std::endl;

  std::cout << rocksdb::get_iostats_context()->ToString() << std::endl;
  // END ROCKS PROFILE

  // Print Full RocksDB stats
  std::cout << "RocksDB Statistics : " << std::endl;
  std::cout << options->statistics->ToString() << std::endl;

  std::cout << "----------------------------------------" << std::endl;

  printLSM(db);

  std::string tr_mem;
  db->GetProperty("rocksdb.estimate-table-readers-mem", &tr_mem);
  std::cout << "RocksDB Estimated Table Readers Memory (index, filters) : "
            << tr_mem << std::endl;

  printFPR(options, stream);
}
