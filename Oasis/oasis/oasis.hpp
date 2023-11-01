#pragma once

#include "bitset.hpp"
#include "cdf_model.hpp"

namespace oasis {

class Oasis {
 public:
  Oasis(double bit_per_key, size_t elements_per_block,
        const std::vector<uint64_t> &keys);

  Oasis(size_t bitmap_sz, uint16_t block_sz, uint16_t last_block_sz,
        CDFModel *cdf_model, uint8_t *bitmap_ptr,
        std::vector<uint64_t> &block_bias, std::vector<BitSet> &block_list)
      : cdf_model_(cdf_model),
        bitmap_ptr_(bitmap_ptr),
        block_bias_(std::move(block_bias)),
        block_list_(std::move(block_list)) {
    bitmap_sz_ = bitmap_sz;
    block_sz_ = block_sz;
    last_block_sz_ = last_block_sz;
  }

  ~Oasis() {
    delete cdf_model_;
    delete[] bitmap_ptr_;
  }

  auto query(uint64_t query_key) -> bool;
  /* [left, right] */
  auto query(uint64_t left, uint64_t right) -> bool;

  auto serialize() const -> std::pair<uint8_t *, size_t>;
  static auto deserialize(uint8_t *ser) -> Oasis *;

  auto size() const -> size_t;

 private:
  inline void build_block_list(const std::vector<uint64_t> &keys);

 private:
  size_t bitmap_sz_ = 0;
  uint16_t block_sz_;
  uint16_t last_block_sz_;

  CDFModel *cdf_model_ = nullptr;
  uint8_t *bitmap_ptr_ = nullptr;
  std::vector<uint64_t> block_bias_;

  /* Do not need serialize */
  std::vector<BitSet> block_list_;
};

Oasis::Oasis(double bit_per_key, size_t elements_per_block,
             const std::vector<uint64_t> &keys) {
  assert(elements_per_block != 0);
  assert(elements_per_block <= UINT16_MAX);
  block_sz_ = elements_per_block;
  cdf_model_ = new CDFModel(bit_per_key, block_sz_, keys);
  build_block_list(keys);

  double bpk = size() * 8.0 / keys.size();
  bpk = bit_per_key - bpk;
  if (bpk < 0.2) {
    return;
  }
  bit_per_key += bpk;
  delete cdf_model_;
  delete[] bitmap_ptr_;

  block_bias_.clear();
  block_list_.clear();
  cdf_model_ = new CDFModel(bit_per_key, elements_per_block, keys);
  build_block_list(keys);
}

void Oasis::build_block_list(const std::vector<uint64_t> &keys) {
  std::vector<uint64_t> keys_pos = cdf_model_->get_locations(keys);
  last_block_sz_ = keys_pos.size() % block_sz_;

  std::vector<uint8_t> compressed_bitmap;

  uint64_t low_bound = keys_pos[0];
  block_bias_.emplace_back(low_bound);
  for (size_t idx = 1; idx < keys_pos.size();) {
    std::vector<uint64_t> cur_batch;
    cur_batch.emplace_back(0);
    for (size_t cnt = 1; cnt < block_sz_ && idx < keys_pos.size(); ++cnt) {
      cur_batch.emplace_back(keys_pos[idx++] - low_bound);
    }
    if (idx < keys_pos.size()) {
      block_bias_.emplace_back(keys_pos[idx++]);
    } else {
      block_bias_.emplace_back(keys_pos.back());
    }
    std::vector<uint8_t> batch_block =
        BitSet::build(cur_batch, block_bias_.back() - low_bound);
    compressed_bitmap.insert(compressed_bitmap.end(), batch_block.begin(),
                             batch_block.end());
    low_bound = block_bias_.back();
  }

  bitmap_sz_ = compressed_bitmap.size();
  bitmap_ptr_ = new uint8_t[bitmap_sz_];
  memcpy(bitmap_ptr_, compressed_bitmap.data(), bitmap_sz_ * sizeof(uint8_t));

  size_t nbatches = block_bias_.size() - 1;
  uint8_t *pos = bitmap_ptr_;
  for (size_t i = 0; i < nbatches; ++i) {
    block_list_.emplace_back(i == nbatches - 1 ? last_block_sz_ : block_sz_,
                             block_bias_[i + 1] - block_bias_[i], pos);
    pos += block_list_.back().size();
  }
}

auto Oasis::query(uint64_t query_key) -> bool {
  size_t pos;
  CDFModel::QueryPosStatus status = cdf_model_->query(query_key, pos);
  switch (status) {
    case CDFModel::EXIST:
      return true;
    case CDFModel::OUT_OF_SCOPE:
      return false;
    default:
      break;
  }

  if (pos < block_bias_[0] || pos > block_bias_.back()) {
    return false;
  }
  auto iter = std::upper_bound(block_bias_.begin(), block_bias_.end(), pos) - 1;
  if (*iter == pos) {
    return true;
  }

  size_t block_idx = iter - block_bias_.begin();

  return block_list_[block_idx].query(static_cast<uint32_t>(pos - *iter));
}

auto Oasis::query(uint64_t left, uint64_t right) -> bool {
  std::pair<size_t, size_t> pos;
  CDFModel::QueryPosStatus status = cdf_model_->query(left, right, pos);
  switch (status) {
    case CDFModel::EXIST:
      return true;
    case CDFModel::OUT_OF_SCOPE:
      return false;
    default:
      break;
  }

  if (pos.second < block_bias_[0] || pos.first > block_bias_.back()) {
    return false;
  }

  auto iter =
      std::upper_bound(block_bias_.begin(), block_bias_.end(), pos.second);
  if (iter == block_bias_.end() || *(--iter) == pos.second ||
      pos.first <= *iter) {
    return true;
  }

  size_t block_idx = iter - block_bias_.begin();
  return block_list_[block_idx].query(pos.first - *iter, pos.second - *iter);
}

auto Oasis::serialize() const -> std::pair<uint8_t *, size_t> {
  size_t nbatches = block_list_.size();
  size_t bias_list_sz = (nbatches + 1) * sizeof(uint64_t);

  size_t meta_sz = sizeof(size_t)          /* block_list_ size */
                   + sizeof(size_t)        /* bitmap_sz_ */
                   + sizeof(uint16_t) * 2; /* block_sz */
  sizeAlign(meta_sz);

  std::pair<uint8_t *, size_t> cdf_ser = cdf_model_->serialize();

  size_t size = meta_sz + bias_list_sz /* block_bias_ */
                + cdf_ser.second       /* cdf model */
                + bitmap_sz_;          /* block_list_ */

  uint8_t *ser = new uint8_t[size];
  uint8_t *pos = ser;

  memcpy(pos, &nbatches, sizeof(size_t));
  pos += sizeof(size_t);

  memcpy(pos, &bitmap_sz_, sizeof(size_t));
  pos += sizeof(size_t);

  memcpy(pos, &block_sz_, sizeof(uint16_t));
  pos += sizeof(uint16_t);

  memcpy(pos, &last_block_sz_, sizeof(uint16_t));
  pos += sizeof(uint16_t);

  align(pos);

  memcpy(pos, block_bias_.data(), bias_list_sz);
  pos += bias_list_sz;

  memcpy(pos, cdf_ser.first, cdf_ser.second);
  pos += cdf_ser.second;
  delete[] cdf_ser.first;

  memcpy(pos, bitmap_ptr_, bitmap_sz_);

  return {ser, size};
}

auto Oasis::deserialize(uint8_t *ser) -> Oasis * {
  size_t nbatches;
  memcpy(&nbatches, ser, sizeof(size_t));
  ser += sizeof(size_t);

  size_t bitmap_sz;
  memcpy(&bitmap_sz, ser, sizeof(size_t));
  ser += sizeof(size_t);

  uint16_t block_sz;
  memcpy(&block_sz, ser, sizeof(uint16_t));
  ser += sizeof(uint16_t);

  uint16_t last_block_sz;
  memcpy(&last_block_sz, ser, sizeof(uint16_t));
  ser += sizeof(uint16_t);

  align(ser);

  std::vector<uint64_t> block_bias(nbatches + 1);
  size_t bias_sz = block_bias.size() * sizeof(uint64_t);
  memcpy(block_bias.data(), ser, bias_sz);
  ser += bias_sz;

  CDFModel *model = CDFModel::deserialize(ser);
  ser += model->size();

  uint8_t *bitmap_ptr = new uint8_t[bitmap_sz];
  memcpy(bitmap_ptr, ser, bitmap_sz);

  std::vector<BitSet> block_list;
  block_list.reserve(nbatches);

  uint8_t *pos = bitmap_ptr;
  for (size_t i = 0; i < nbatches; ++i) {
    block_list.emplace_back(i == nbatches - 1 ? last_block_sz : block_sz,
                            block_bias[i + 1] - block_bias[i], pos);
    pos += block_list.back().size();
  }

  return {new Oasis(bitmap_sz, block_sz, last_block_sz, model, bitmap_ptr,
                    block_bias, block_list)};
}

auto Oasis::size() const -> size_t {
  size_t meta_sz = sizeof(size_t)          /* block_list_ size */
                   + sizeof(size_t)        /* bitmap_sz_ */
                   + sizeof(uint16_t) * 2; /* block_sz */
  sizeAlign(meta_sz);

  size_t cdf_sz = cdf_model_->size();
  return meta_sz + cdf_sz                        /* cdf model */
         + block_bias_.size() * sizeof(uint64_t) /* bias size */
         + bitmap_sz_;                           /* block_list_ */
}

}  // namespace oasis
