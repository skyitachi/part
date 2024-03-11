//
// Created by skyitachi on 24-3-9.
//
#include <fmt/core.h>
#include <gtest/gtest.h>

#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <random>

#include "art.h"
#include "concurrent_art.h"
#include "fixed_size_allocator.h"

using json = nlohmann::json;

using namespace part;

class ConcurrentARTSerializeTest : public testing::Test {
 protected:
  template <class T>
  using Vector = std::vector<T>;
  using Int64pair = std::pair<int64_t, int64_t>;

  Vector<Int64pair> genRandomKvPairs(int32_t limit) {
    Vector<Int64pair> kv_pairs;
    std::unordered_set<int64_t> key_sets;
    for (int i = 0; i < limit; i++) {
      int64_t rk = 0;
      do {
        rk = (*dist_)(*gen_);
      } while (key_sets.contains(rk));
      kv_pairs.emplace_back(rk, i);
      key_sets.insert(rk);
    }
    return kv_pairs;
  }

  static void removeFiles(const std::vector<std::string> &files) {
    for (auto &file : files) {
      ::unlink(file.c_str());
    }
  }

  static Vector<Int64pair> readKVPairsFromFile(const std::string &path) {
    std::ifstream f(path);
    json array_obj = json::array();
    f >> array_obj;
    Vector<Int64pair> kv_pairs;
    for (const auto &kv : array_obj) {
      kv_pairs.emplace_back(kv["key"], kv["value"]);
    }
    return kv_pairs;
  }

  static void kvPairToJsonFile(const Vector<Int64pair> &pairs, const std::string &output) {
    std::ofstream f(output);

    json array_obj = json::array();
    for (const auto &kv : pairs) {
      array_obj.emplace_back(json::object({{"key", kv.first}, {"value", kv.second}}));
    }

    f << std::setw(4) << array_obj << std::endl;
  }

  Vector<Int64pair> PrepareData(int32_t limit, const std::string &output) {
    auto pairs = genRandomKvPairs(limit);
    kvPairToJsonFile(pairs, output);
    return pairs;
  }

  void SetUpFiles(const std::string &index_path) {
    std::lock_guard<std::mutex> lock(mu_);
    index_path_ = index_path;
  }

  std::string GetFiles() {
    std::lock_guard<std::mutex> lock(mu_);
    assert(!index_path_.empty());
    return index_path_;
  }

  void SetUp() override {
    gen_ = std::make_unique<std::mt19937_64>(rd_());
    dist_ = std::make_unique<std::uniform_int_distribution<int64_t>>(0, std::numeric_limits<int64_t>::max());
    keep_file = false;
  }

  void TearDown() override {
    gen_.reset();
    if (keep_file) {
      return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    if (!index_path_.empty()) {
      removeFiles({index_path_});
    }
  }

  std::random_device rd_;
  std::unique_ptr<std::mt19937_64> gen_;
  std::unique_ptr<std::uniform_int_distribution<int64_t>> dist_;

  std::string index_path_;

  std::mutex mu_;

 public:
  std::atomic<bool> keep_file;
};

TEST_F(ConcurrentARTSerializeTest, Debug) {
  Allocator &allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  ConcurrentART cart("cart.data");

  auto k1 = ARTKey::CreateARTKey<int32_t>(arena_allocator, 1);
  auto k2 = ARTKey::CreateARTKey<int32_t>(arena_allocator, 2);

  std::vector<idx_t> result_ids;
  cart.Get(k1, result_ids);

  ASSERT_EQ(result_ids.size(), 1);
  ASSERT_EQ(result_ids[0], 1);

  result_ids.clear();
  cart.Get(k2, result_ids);
  ASSERT_EQ(result_ids.size(), 1);
  ASSERT_EQ(result_ids[0], 2);
}

TEST_F(ConcurrentARTSerializeTest, DebugForComparsion) {
  Allocator &allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  ART cart("cart.data");

  auto k1 = ARTKey::CreateARTKey<int32_t>(arena_allocator, 1);

  std::vector<idx_t> result_ids;
  cart.Get(k1, result_ids);

  ASSERT_EQ(result_ids.size(), 1);
  ASSERT_EQ(result_ids[0], 1);
}

TEST_F(ConcurrentARTSerializeTest, SerializeTest) {
  Allocator &allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  SetUpFiles("cart_serialize.idx");

  keep_file = false;
  auto index_path = GetFiles();
  ConcurrentART cart(index_path);

  idx_t limit = 10000;

  std::vector<ARTKey> keys;

  for (idx_t i = 0; i < limit; i++) {
    keys.push_back(ARTKey::CreateARTKey<int32_t>(arena_allocator, i));
    cart.Put(keys[i], i);
  }

  cart.Serialize();

  {
    ART art(index_path);

    //    art.Draw("cart_serialize.dot");

    for (idx_t i = 0; i < limit; i++) {
      std::vector<idx_t> result_ids;
      art.Get(keys[i], result_ids);
      ASSERT_EQ(result_ids.size(), 1);
      ASSERT_EQ(result_ids[0], i);
    }
  }
}

TEST_F(ConcurrentARTSerializeTest, SerializeRandomTest) {
  Allocator &allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  SetUpFiles("cart_serialize.idx");

  auto kv_pairs = PrepareData(10000, "medium_kv_pairs.json");

  auto index_path = GetFiles();

  ConcurrentART cart(index_path);

  idx_t limit = 10000;

  for (const auto &kv : kv_pairs) {
    auto art_key = ARTKey::CreateARTKey<int64_t>(arena_allocator, kv.first);
    cart.Put(art_key, kv.second);
  }

  cart.Serialize();

  ART art(index_path);

  //    art.Draw("cart_serialize.dot");
  for (const auto &kv : kv_pairs) {
    auto art_key = ARTKey::CreateARTKey<int64_t>(arena_allocator, kv.first);
    std::vector<idx_t> results;
    art.Get(art_key, results);
    ASSERT_EQ(1, results.size());
    ASSERT_EQ(kv.second, results[0]);
  }
}

TEST_F(ConcurrentARTSerializeTest, DeserializeTest) {
  Allocator &allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  SetUpFiles("cart_deserialized.idx");
  auto index_path = GetFiles();

  ART art(index_path);

  idx_t limit = 10000;
  std::vector<ARTKey> keys;

  for (idx_t i = 0; i < limit; i++) {
    keys.push_back(ARTKey::CreateARTKey<int32_t>(arena_allocator, i));
  }

  for (idx_t i = 0; i < limit; i++) {
    art.Put(keys[i], i);
  }

  art.Serialize();

  ConcurrentART cart(index_path);

  for (idx_t i = 0; i < limit; i++) {
    std::vector<idx_t> result_ids;
    cart.Get(keys[i], result_ids);
    ASSERT_EQ(result_ids.size(), 1);
    ASSERT_EQ(result_ids[0], i);
  }
}

TEST_F(ConcurrentARTSerializeTest, DeserializeRandomTest) {
  Allocator &allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  SetUpFiles("cart_deserialized_random.idx");

  auto kv_pairs = PrepareData(10000, "medium_kv_pairs.json");

  auto index_path = GetFiles();

  ART art(index_path);

  std::vector<ARTKey> keys;

  for (const auto &kv : kv_pairs) {
    auto art_key = ARTKey::CreateARTKey<int64_t>(arena_allocator, kv.first);
    art.Put(art_key, kv.second);
  }

  art.Serialize();

  ConcurrentART cart(index_path);

  for (const auto &kv : kv_pairs) {
    auto art_key = ARTKey::CreateARTKey<int64_t>(arena_allocator, kv.first);
    std::vector<idx_t> results;
    cart.Get(art_key, results);

    ASSERT_EQ(1, results.size());
    ASSERT_EQ(kv.second, results[0]);
  }
}

TEST_F(ConcurrentARTSerializeTest, HybridSerializeTest) {
  Allocator &allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  SetUpFiles("cart_serialized_hybrid.idx");

  auto index_path = GetFiles();

  idx_t limit = 10000;
  std::vector<ARTKey> keys;

  for (idx_t i = 0; i < limit; i++) {
    keys.push_back(ARTKey::CreateARTKey<int32_t>(arena_allocator, i));
  }

  // first round
  {
    ConcurrentART cart(index_path);

    for (idx_t i = 0; i < limit / 2; i++) {
      cart.Put(keys[i], i);
    }
    cart.Serialize();
  }

  {
    ConcurrentART cart(index_path);
    for (idx_t i = limit / 2; i < limit; i++) {
      cart.Put(keys[i], i);
    }

    for (idx_t i = 0; i < limit; i++) {
      std::vector<idx_t> result_ids;
      cart.Get(keys[i], result_ids);
      ASSERT_EQ(result_ids.size(), 1);
      ASSERT_EQ(result_ids[0], i);
    }
  }
}
