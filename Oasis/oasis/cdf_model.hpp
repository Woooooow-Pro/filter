#pragma once

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <queue>
#include <tuple>
#include <vector>

namespace oasis {

class CDFModel {
 private:
  static const uint64_t kCost = 3 * sizeof(uint64_t) * 8;

 public:
  enum QueryPosStatus { OUT_OF_SCOPE, EXIST, NO_IDEA };

 public:
  CDFModel(double bpk, size_t elem_per_block,
           const std::vector<uint64_t> &keys);

  CDFModel(std::vector<uint64_t> &begins, std::vector<uint64_t> &ends,
           std::vector<uint64_t> &accumulate_nkeys)
      : begins_(std::move(begins)),
        ends_(std::move(ends)),
        accumulate_nkeys_(std::move(accumulate_nkeys)) {
    assert(begins_.size() == ends_.size());
    assert(accumulate_nkeys_.size() == begins_.size() + 1);
  }

  /* return the estimate distribution of all the key */
  auto get_locations(const std::vector<uint64_t> &keys) -> std::vector<size_t>;

  /**
   * For given query (point/range), return the (slope, bias) of the model
   * return (-1, 0): query crosses the models
   * return (0, 2): query out of scop;
   */
  auto query(const uint64_t &key, size_t &result) -> QueryPosStatus;
  /* [l_key, r_key], and return [l_pos, r_pos] */
  auto query(const uint64_t &l_key, const uint64_t &r_key,
             std::pair<size_t, size_t> &result) -> QueryPosStatus;

  auto size() const -> size_t;

  auto serialize() const -> std::pair<uint8_t *, size_t>;

  static auto deserialize(uint8_t *ser) -> CDFModel *;

 private:
  inline void build_indices(const uint64_t threshold, const double bpk,
                            const std::vector<uint64_t> &keys);
  /* idx range in [0, index_.size() - 1] */
  inline auto get_params(size_t idx)
      -> std::tuple<uint64_t, uint64_t, long double>;
  /* idx range in [0, index_.size() - 2] */
  inline auto get_location(
      const uint64_t &key,
      const std::tuple<uint64_t, uint64_t, long double> &params) -> uint64_t;

  auto get_threshold(double bpk, uint64_t delta_sum, size_t nkeys,
                     std::queue<uint64_t> &threshold_set) -> uint64_t;

 private:
  std::vector<uint64_t> begins_;
  std::vector<uint64_t> ends_;

  std::vector<uint64_t> accumulate_nkeys_;
};

CDFModel::CDFModel(double bpk, size_t elem_per_block,
                   const std::vector<uint64_t> &keys) {
  size_t nkeys = keys.size();
  double kMemBudget = bpk * nkeys;
  size_t M = static_cast<size_t>(kMemBudget / kCost);

  std::priority_queue<uint64_t, std::vector<uint64_t>, std::greater<uint64_t>>
      min_heap;
  for (size_t i = 0; i < nkeys - 1; ++i) {
    uint64_t diff = keys[i + 1] - keys[i];
    if (min_heap.size() >= M) {
      if (min_heap.top() > diff) {
        continue;
      }
      min_heap.pop();
    }
    min_heap.push(diff);
  }

  uint64_t threshold = min_heap.top();
  while (!min_heap.empty() && min_heap.top() == threshold) {
    min_heap.pop();
  }

  std::queue<uint64_t> threshold_set;
  uint64_t delta_sum = 0;
  while (!min_heap.empty()) {
    uint64_t tmp = min_heap.top();
    min_heap.pop();

    threshold_set.push(tmp);
    delta_sum += tmp;
  }

  delta_sum = keys.back() - keys[0] - delta_sum;
  double remain_bpk = bpk - 2 - 64.0L / elem_per_block;

  threshold = get_threshold(remain_bpk, delta_sum, nkeys, threshold_set);

  build_indices(threshold, remain_bpk, keys);
}

auto CDFModel::get_locations(const std::vector<uint64_t> &keys)
    -> std::vector<size_t> {
  size_t nkeys = keys.size();

  std::vector<size_t> positions;

  std::tuple<uint64_t, uint64_t, long double> params = get_params(0);
  size_t idx_iter = 0;
  for (size_t i = 1; i < nkeys - 1; ++i) {
    if (keys[i] >= ends_[idx_iter]) {
      params = get_params(++idx_iter);
    } else if (keys[i] > begins_[idx_iter]) {
      positions.emplace_back(get_location(keys[i], params));
    }
  }
  return positions;
}

auto CDFModel::query(const uint64_t &key, size_t &result)
    -> CDFModel::QueryPosStatus {
  if (key < begins_[0] || key > ends_.back()) {
    return QueryPosStatus::OUT_OF_SCOPE;
  }

  size_t idx =
      std::distance(begins_.begin(),
                    std::upper_bound(begins_.begin(), begins_.end(), key)) -
      1;
  if (ends_[idx] < key) {
    return QueryPosStatus::OUT_OF_SCOPE;
  }
  if (begins_[idx] == key || ends_[idx] == key) {
    return QueryPosStatus::EXIST;
  }

  auto params = get_params(idx);
  if (std::get<1>(params) == 0) {
    return QueryPosStatus::OUT_OF_SCOPE;
  }

  result = get_location(key, params);
  return QueryPosStatus::NO_IDEA;
}

auto CDFModel::query(const uint64_t &l_key, const uint64_t &r_key,
                     std::pair<size_t, size_t> &result)
    -> CDFModel::QueryPosStatus {
  assert(l_key < r_key);

  if (l_key > ends_.back() || r_key < begins_[0]) {
    return QueryPosStatus::OUT_OF_SCOPE;
  }

  size_t idx =
      std::distance(begins_.begin(),
                    std::upper_bound(begins_.begin(), begins_.end(), l_key)) -
      1;

  if (r_key < begins_[idx + 1] && l_key > ends_[idx]) {
    return QueryPosStatus::OUT_OF_SCOPE;
  }

  if (!(l_key > begins_[idx] && r_key < ends_[idx])) {
    return QueryPosStatus::EXIST;
  }

  auto params = get_params(idx);
  if (std::get<1>(params) == 0) {
    return QueryPosStatus::OUT_OF_SCOPE;
  }

  result.first = get_location(l_key, params);
  result.second = get_location(r_key, params);
  return QueryPosStatus::NO_IDEA;
}

auto CDFModel::size() const -> size_t {
  size_t meta_sz = sizeof(size_t); /* # elements in index_ */
  sizeAlign(meta_sz);
  return meta_sz +
         3 * sizeof(uint64_t) *
             begins_.size(); /* begins_ + ends_ + # keys in each interval*/
}

auto CDFModel::serialize() const -> std::pair<uint8_t *, size_t> {
  size_t meta_sz = sizeof(size_t); /* # elements in index_ */
  sizeAlign(meta_sz);

  size_t size =
      meta_sz +
      3 * sizeof(uint64_t) *
          begins_.size(); /* begins_ + ends_ + # keys in each interval*/

  uint8_t *out = new uint8_t[size];
  uint8_t *pos = out;

  size_t idx_sz = begins_.size();
  memcpy(pos, &idx_sz, sizeof(size_t));
  pos += sizeof(size_t);

  align(pos);

  memcpy(pos, begins_.data(), sizeof(uint64_t) * idx_sz);
  pos += sizeof(uint64_t) * idx_sz;

  memcpy(pos, ends_.data(), sizeof(uint64_t) * idx_sz);
  pos += sizeof(uint64_t) * idx_sz;

  memcpy(pos, accumulate_nkeys_.data() + 1, sizeof(uint64_t) * idx_sz);

  return {out, size};
}

auto CDFModel::deserialize(uint8_t *ser) -> CDFModel * {
  assert(ser != nullptr);

  size_t idx_len;
  memcpy(&idx_len, ser, sizeof(size_t));
  ser += sizeof(size_t);

  align(ser);

  size_t index_sz = sizeof(uint64_t) * idx_len;
  std::vector<uint64_t> begins(idx_len);
  memcpy(begins.data(), ser, index_sz);
  ser += index_sz;

  std::vector<uint64_t> ends(idx_len);
  memcpy(ends.data(), ser, index_sz);
  ser += index_sz;

  std::vector<uint64_t> accumulate_nkeys(idx_len + 1, 0);
  memcpy(accumulate_nkeys.data() + 1, ser, index_sz);

  return {new CDFModel(begins, ends, accumulate_nkeys)};
}

/** Helping Function */
void CDFModel::build_indices(const uint64_t threshold, const double bpk,
                             const std::vector<uint64_t> &keys) {
  size_t nkeys = keys.size();
  uint64_t avg_range = 0;

  begins_.clear();
  ends_.clear();

  std::vector<bool> interval_sz;
  uint32_t cnt = 0;
  /** build the indices */
  begins_.emplace_back(keys[0]);
  for (size_t i = 0; i < nkeys - 1; ++i) {
    if (keys[i + 1] - keys[i] >= threshold) {
      ends_.emplace_back(keys[i++]);

      interval_sz.emplace_back(cnt == 0);
      avg_range += cnt == 0 ? 0 : ends_.back() - begins_.back();
      cnt = 0;
      begins_.emplace_back(keys[i]);
    } else {
      ++cnt;
    }
  }

  ends_.emplace_back(keys.back());
  interval_sz.emplace_back(cnt == 0);
  avg_range += cnt == 0 ? 0 : ends_.back() - begins_.back();

  accumulate_nkeys_.clear();
  accumulate_nkeys_.emplace_back(0);

  /** Build the alpha array */

  uint64_t bit_array_range =
      pow(2, bpk - kCost * 1.0L / nkeys * ends_.size()) * nkeys;
  for (size_t i = 0; i < begins_.size(); ++i) {
    if (interval_sz[i]) {
      accumulate_nkeys_.emplace_back(accumulate_nkeys_.back());
      continue;
    }
    uint64_t alpha = std::ceil(static_cast<double>(ends_[i] - begins_[i]) /
                               avg_range * bit_array_range);
    if (alpha == 0) {
      alpha = 1;
    }
    accumulate_nkeys_.emplace_back(accumulate_nkeys_.back() + alpha);
  }
}

auto CDFModel::get_params(size_t idx)
    -> std::tuple<uint64_t, uint64_t, long double> {
  uint64_t begin = begins_[idx];
  uint64_t end = ends_[idx];
  uint64_t low_location = accumulate_nkeys_[idx];
  uint64_t high_location = accumulate_nkeys_[idx + 1];
  return {end - begin, high_location - low_location,
          (end * 1.0L) * low_location - (begin * 1.0L) * high_location};
}

auto CDFModel::get_location(
    const uint64_t &key,
    const std::tuple<uint64_t, uint64_t, long double> &params) -> uint64_t {
  return static_cast<uint64_t>(
      (static_cast<double>(std::get<1>(params)) * key + std::get<2>(params)) /
      std::get<0>(params));
}

auto CDFModel::get_threshold(double bpk, uint64_t delta_sum, size_t nkeys,
                             std::queue<uint64_t> &threshold_set) -> uint64_t {
  double param = kCost * 1.0L / nkeys;
  double min_rho = std::numeric_limits<double>::max();
  uint64_t m = threshold_set.size();
  uint64_t best_threshold = threshold_set.front();
  while (!threshold_set.empty()) {
    uint64_t mp_sz = std::ceil(std::pow(2, bpk - param * (m + 1)) * nkeys);
    double rho = static_cast<double>(delta_sum) * delta_sum / mp_sz;
    if (rho <= min_rho) {
      min_rho = rho;
      best_threshold = threshold_set.front();
    }

    uint64_t cnt = threshold_set.size();
    uint64_t cur_threshold = threshold_set.front();
    while (!threshold_set.empty() && threshold_set.front() == cur_threshold) {
      threshold_set.pop();
    }
    m = threshold_set.size();
    delta_sum += (cnt - m) * cur_threshold;
  }
  return best_threshold;
}

}  // namespace oasis
