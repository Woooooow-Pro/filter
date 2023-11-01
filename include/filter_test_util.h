#pragma once
#include <algorithm>
#include <cassert>
#include <cinttypes>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <vector>

namespace rocksdb {

// uint64_t sliceToUint64(const char* data);

// For RocksDB
inline uint64_t sliceToUint64(const char* data) {
  uint64_t out = 0ULL;
  memcpy(&out, data, 8);
  return __builtin_bswap64(out);
}

inline std::string util_uint64ToString(const uint64_t& word) {
  uint64_t endian_swapped_word = __builtin_bswap64(word);
  return std::string(reinterpret_cast<const char*>(&endian_swapped_word), 8);
}

inline uint64_t util_stringToUint64(const std::string& str_word) {
  uint64_t int_word = 0;
  memcpy(reinterpret_cast<char*>(&int_word), str_word.data(), 8);
  return __builtin_bswap64(int_word);
}
}  // namespace rocksdb