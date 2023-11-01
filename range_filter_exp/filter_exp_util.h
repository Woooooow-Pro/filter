#pragma once
#include <fstream>

#include "rocksdb/db.h"

static const size_t VAL_SZ = 512;
static const std::string dataPath = "./my_data/";

inline auto isPointQuery(const uint64_t& a, const uint64_t& b) -> bool {
  return b == (a + 1);
}

inline auto isPointQuery(const std::string& a, const std::string& b) -> bool {
  return b == a;
}

auto intLoadKeysValues() -> std::pair<std::vector<std::vector<std::string>>,
                                      std::vector<std::vector<rocksdb::Slice>>>;
auto intLoadQueries()
    -> std::vector<std::vector<std::pair<std::string, std::string>>>;

void printCompactionAndDBStats(rocksdb::DB* db);
void printLSM(rocksdb::DB* db);
void flushMemTable(rocksdb::DB* db);
void waitForBGCompactions(rocksdb::DB* db);
void printFPR(rocksdb::Options* options, std::ofstream& stream);
void printStats(rocksdb::DB* db, rocksdb::Options* options,
                std::ofstream& stream);