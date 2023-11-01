#include <algorithm>
#include <atomic>
#include <iostream>
#include <map>

#include "filter_test_util.h"
#include "oasis/oasis.hpp"
#include "rocksdb/filter_policy.h"
#include "rocksdb/slice.h"

namespace rocksdb {
static std::map<uint64_t, oasis::Oasis*> cache;
static std::atomic<uint64_t> timestamp;

class OasisFilterBitsBuilder : public FilterBitsBuilder {
 private:
  double bpk_;
  size_t block_sz_;
  std::vector<uint64_t> keys_;

 public:
  OasisFilterBitsBuilder(double bpk, uint16_t block_sz)
      : bpk_(bpk), block_sz_(block_sz) {}

  ~OasisFilterBitsBuilder() { keys_.clear(); }

  void AddKey(const Slice& key) { keys_.push_back(sliceToUint64(key.data())); }

  Slice Finish(std::unique_ptr<const char[]>* buf) {
    oasis::Oasis* filter = new oasis::Oasis(bpk_, block_sz_, keys_);

    uint64_t key = ++timestamp;
    cache[key] = filter;

    uint8_t* data = new uint8_t[sizeof(uint64_t)];
    memcpy(data, &key, sizeof(uint64_t));

    buf->reset((const char*)data);
    Slice out((const char*)data, sizeof(uint64_t));

    return out;
  }
};

class OasisFilterBitsReader : public FilterBitsReader {
 protected:
  oasis::Oasis* filter_ = nullptr;

 public:
  explicit OasisFilterBitsReader(const Slice& contents) {
    uint8_t* ser = (uint8_t*)contents.data();
    uint64_t key = ((uint64_t*)ser)[0];

    auto iter = cache.find(key);
    filter_ = reinterpret_cast<oasis::Oasis*>(iter->second);
  }

  // ~OasisFilterBitsReader() { printf("delete %lu\n", ++delete_cnt); }

  using FilterBitsReader::MayMatch;
  void MayMatch(int num_keys, Slice** keys, bool* may_match) override {
    (void)num_keys;
    (void)keys;
    (void)may_match;
    return;
  }

  bool MayMatch(const Slice& entry) override {
    return filter_->query(sliceToUint64(entry.data()));
  }

  bool RangeQuery(const Slice& left, const Slice& right) override {
    return filter_->query(sliceToUint64(left.data()),
                          sliceToUint64(right.data()) - 1);
  }
};

class OasisFilterPolicy : public FilterPolicy {
 public:
  explicit OasisFilterPolicy(double bpk, size_t block_sz)
      : bpk_(bpk), block_sz_(block_sz) {}

  ~OasisFilterPolicy() {}

  const char* Name() const { return "Oasis"; }

  void CreateFilter(const Slice* keys, int n, std::string* dst) const override {
    (void)keys;
    (void)n;
    (void)dst;
    assert(false);
  }

  bool KeyMayMatch(const Slice& key, const Slice& filter) const override {
    (void)key;
    (void)filter;

    assert(false);
    return true;
  }

  OasisFilterBitsBuilder* GetFilterBitsBuilder() const override {
    return new OasisFilterBitsBuilder(bpk_, block_sz_);
  }

  OasisFilterBitsReader* GetFilterBitsReader(
      const Slice& contents) const override {
    return new OasisFilterBitsReader(contents);
  }

 private:
  double bpk_;
  size_t block_sz_;
};

const FilterPolicy* NewOasisFilterPolicy(double bpk, size_t block_sz) {
  return new OasisFilterPolicy(bpk, block_sz);
}

}  // namespace rocksdb