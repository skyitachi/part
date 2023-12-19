//
// Created by Shiping Yao on 2023/12/19.
//

#ifndef PART_UTIL_H
#define PART_UTIL_H
#include <memory>
#include <random>
#include <unordered_set>
#include <vector>

#include "arena_allocator.h"
#include "art_key.h"

namespace part {

class Random {
 public:
  template <class T>
  using Vector = std::vector<T>;
  using Int64pair = std::pair<int64_t, int64_t>;
  using ARTKeyInt64Pair = std::pair<ARTKey, int64_t>;

  Random() {
    gen_ = std::make_unique<std::mt19937_64>(rd_());
    dist_ = std::make_unique<std::uniform_int_distribution<int64_t>>(0, std::numeric_limits<int64_t>::max());
  }

  inline int64_t NextLong() { return (*dist_)(*gen_); }

  Vector<ARTKeyInt64Pair> GenKvPairs(int32_t limit) {
    Allocator &allocator = Allocator::DefaultAllocator();
    ArenaAllocator arena_allocator(allocator, 16384);
    Vector<ARTKeyInt64Pair> kv_pairs;
    std::unordered_set<int64_t> key_sets;
    for (int i = 0; i < limit; i++) {
      int64_t rk = 0;
      do {
        rk = NextLong();
      } while (key_sets.contains(rk));
      auto art_key = ARTKey::CreateARTKey<int64_t>(arena_allocator, rk);
      kv_pairs.emplace_back(art_key, i);
      key_sets.insert(rk);
    }
    return kv_pairs;
  }

  std::string GenStrings(int length) {
    std::string randomString;
    randomString.resize(length);
    for (int i = 0; i < length; ++i) {
      randomString[i] = charset[NextLong() % length];
    }
    return randomString;
  }

 private:
  std::string charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

  std::random_device rd_;
  std::unique_ptr<std::mt19937_64> gen_;
  std::unique_ptr<std::uniform_int_distribution<int64_t>> dist_;
};
}  // namespace part
#endif  // PART_UTIL_H
