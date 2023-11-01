#include <algorithm>
#include <atomic>
#include <iostream>
#include <map>

#include "filter_test_util.h"
#include "oasis_plus.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/slice.h"

namespace rocksdb {

static std::map<uint64_t, oasis_plus::OasisPlus*> cache;
static std::atomic<uint64_t> timestamp;

class OasisPlusFilterBitsBuilder : public FilterBitsBuilder {
 private:
  double bpk_;
  size_t block_sz_;
  size_t max_qlen_;
  std::vector<uint64_t> keys_;

 public:
  OasisPlusFilterBitsBuilder(double bpk, uint16_t block_sz,
                             size_t max_qlen = 10)
      : bpk_(bpk), block_sz_(block_sz), max_qlen_(max_qlen) {}

  ~OasisPlusFilterBitsBuilder() { keys_.clear(); }

  void AddKey(const Slice& key) { keys_.push_back(sliceToUint64(key.data())); }
  Slice Finish(std::unique_ptr<const char[]>* buf) {
    oasis_plus::OasisPlus* filter =
        new oasis_plus::OasisPlus(bpk_, block_sz_, keys_, max_qlen_);

    uint64_t key = ++timestamp;
    cache[key] = filter;

    uint8_t* data = new uint8_t[sizeof(uint64_t)];
    memcpy(data, &key, sizeof(uint64_t));

    buf->reset((const char*)data);
    Slice out((const char*)data, sizeof(uint64_t));

    return out;
  }
};

class OasisPlusFilterBitsReader : public FilterBitsReader {
 protected:
  oasis_plus::OasisPlus* filter_;

 public:
  explicit OasisPlusFilterBitsReader(const Slice& contents) {
    uint8_t* ser = (uint8_t*)contents.data();
    uint64_t key = ((uint64_t*)ser)[0];

    auto iter = cache.find(key);

    filter_ = iter->second;
  }

  // ~OasisPlusFilterBitsReader() { delete filter_; }

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

class OasisPlusFilterPolicy : public FilterPolicy {
 public:
  explicit OasisPlusFilterPolicy(double bpk, size_t block_sz,
                                 size_t max_qlen = 10)
      : bpk_(bpk), block_sz_(block_sz), max_qlen_(max_qlen) {}

  ~OasisPlusFilterPolicy() {}

  const char* Name() const { return "OasisPlus"; }

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

  FilterBitsBuilder* GetFilterBitsBuilder() const override {
    return new OasisPlusFilterBitsBuilder(bpk_, block_sz_, max_qlen_);
  }

  FilterBitsReader* GetFilterBitsReader(const Slice& contents) const override {
    return new OasisPlusFilterBitsReader(contents);
  }

 private:
  double bpk_;
  size_t block_sz_;
  size_t max_qlen_;
};

const FilterPolicy* NewOasisPlusFilterPolicy(double bpk, size_t block_sz,
                                             size_t max_qlen = 10) {
  return new OasisPlusFilterPolicy(bpk, block_sz, max_qlen);
}

}  // namespace rocksdb