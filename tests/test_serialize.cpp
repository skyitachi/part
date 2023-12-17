//
// Created by skyitachi on 12/17/23.
//
#include <random>
#include <unordered_set>
#include <iostream>
#include <fstream>
#include <mutex>

#include <nlohmann/json.hpp>
#include <gtest/gtest.h>

#include "art.h"
#include "allocator.h"
#include "arena_allocator.h"
#include "art_key.h"

using json = nlohmann::json;
using namespace part;

class ARTSerializeTest : public testing::Test {
protected:
  template<class T>
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
    for(auto &file: files) {
      ::unlink(file.c_str());
    }
  }

  static Vector<Int64pair> readKVPairsFromFile(const std::string &path) {
    std::ifstream f(path);
    json array_obj = json::array();
    f >> array_obj;
    Vector<Int64pair> kv_pairs;
    for (const auto &kv: array_obj) {
      kv_pairs.emplace_back(kv["key"], kv["value"]);
    }
    return kv_pairs;
  }

  static void kvPairToJsonFile(const Vector<Int64pair> &pairs, const std::string &output) {
    std::ofstream f(output);

    json array_obj = json::array();
    for (const auto &kv: pairs) {
      array_obj.emplace_back(json::object({{"key",   kv.first},
                                           {"value", kv.second}}));
    }

    f << std::setw(4) << array_obj << std::endl;
  }

  Vector<Int64pair> PrepareData(int32_t limit, const std::string &output) {
    auto pairs = genRandomKvPairs(limit);
    kvPairToJsonFile(pairs, output);
    return pairs;
  }

  void SetUpFiles(const std::string &meta_path, const std::string &index_path) {
    std::lock_guard<std::mutex> lock(mu_);
    meta_path_ = meta_path;
    index_path_ = index_path;
  }

  std::pair<std::string, std::string> GetFiles() {
    std::lock_guard<std::mutex> lock(mu_);
    assert(!meta_path_.empty() && !index_path_.empty());
    return {meta_path_, index_path_};
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
    if (!meta_path_.empty() && !index_path_.empty()) {
      removeFiles({index_path_, meta_path_});
    }
  }

  std::random_device rd_;
  std::unique_ptr<std::mt19937_64> gen_;
  std::unique_ptr<std::uniform_int_distribution<int64_t>> dist_;

  std::string meta_path_;
  std::string index_path_;

  std::mutex mu_;

public:
  std::atomic<bool> keep_file;
};


TEST_F(ARTSerializeTest, JsonFileTest) {
  auto kv_pairs = genRandomKvPairs(10);

  ARTSerializeTest::kvPairToJsonFile(kv_pairs, "keys_int64.json");

  auto kv_pairs_from_file = ARTSerializeTest::readKVPairsFromFile("keys_int64.json");

  EXPECT_EQ(kv_pairs, kv_pairs_from_file);
}

TEST_F(ARTSerializeTest, ARTSerializeBasicTest) {
  Allocator &allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);
  SetUpFiles("i64_art.meta", "i64_art.data");

  auto [meta_path, index_path] = GetFiles();

    auto kv_pairs = ARTSerializeTest::readKVPairsFromFile("keys_int64.json");

    ART art(meta_path, index_path);

    for (const auto &kv: kv_pairs) {
      auto art_key = ARTKey::CreateARTKey<int64_t>(arena_allocator, kv.first);
      art.Put(art_key, kv.second);
    }

    art.Serialize();

    ART art2("i64_art.meta", "i64_art.data");

    for (const auto &kv: kv_pairs) {
      auto art_key = ARTKey::CreateARTKey<int64_t>(arena_allocator, kv.first);
      std::vector<idx_t> results;
      bool success = art2.Get(art_key, results);

      EXPECT_TRUE(success);
      EXPECT_EQ(1, results.size());
      EXPECT_EQ(kv.second, results[0]);
    }
}

TEST(ARTTest, ARTDebugTEST) {
  ART art("i64_art.meta", "i64_art.data");
}

TEST_F(ARTSerializeTest, MediumARTTest) {
  Allocator &allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);
  SetUpFiles("medium_i64_art.meta", "medium_i64_art.data");
  auto kv_pairs = PrepareData(10000, "medium_kv_pairs.json");
  keep_file = true;

  auto [meta_path, index_path] = GetFiles();

  ART art(meta_path, index_path);

  for (const auto &kv: kv_pairs) {
    auto art_key = ARTKey::CreateARTKey<int64_t>(arena_allocator, kv.first);
    art.Put(art_key, kv.second);
  }

  art.Serialize();

  ART art2(meta_path, index_path);

  for (const auto &kv: kv_pairs) {
    auto art_key = ARTKey::CreateARTKey<int64_t>(arena_allocator, kv.first);
    std::vector<idx_t> results;
    bool success = art2.Get(art_key, results);

    EXPECT_TRUE(success);
    EXPECT_EQ(1, results.size());
    EXPECT_EQ(kv.second, results[0]);
  }
}

