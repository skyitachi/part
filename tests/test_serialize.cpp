//
// Created by skyitachi on 12/17/23.
//
#include <fmt/core.h>
#include <gtest/gtest.h>

#include <fstream>
#include <iostream>
#include <mutex>
#include <nlohmann/json.hpp>
#include <random>
#include <unordered_set>

#include "allocator.h"
#include "arena_allocator.h"
#include "art.h"
#include "art_key.h"

using json = nlohmann::json;
using namespace part;

std::string generateRandomString(int length) {
  std::string charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  std::random_device rd;
  std::mt19937 generator(rd());
  std::uniform_int_distribution<int> distribution(0, charset.length() - 1);

  std::string randomString;
  for (int i = 0; i < length; ++i) {
    randomString += charset[distribution(generator)];
  }

  return randomString;
}

class ARTSerializeTest : public testing::Test {
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

TEST_F(ARTSerializeTest, JsonFileTest) {
  auto kv_pairs = genRandomKvPairs(1000);

  ARTSerializeTest::kvPairToJsonFile(kv_pairs, "keys_int64.json");

  auto kv_pairs_from_file = ARTSerializeTest::readKVPairsFromFile("keys_int64.json");

  EXPECT_EQ(kv_pairs, kv_pairs_from_file);
}

TEST_F(ARTSerializeTest, ARTSerializeBasicTest) {
  Allocator &allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);
  SetUpFiles("i64_art.data");

  auto index_path = GetFiles();

  auto kv_pairs = ARTSerializeTest::readKVPairsFromFile("keys_int64.json");

  ART art(index_path);

  for (const auto &kv : kv_pairs) {
    auto art_key = ARTKey::CreateARTKey<int64_t>(arena_allocator, kv.first);
    art.Put(art_key, kv.second);
  }

  art.Serialize();

  ART art2(index_path);

  for (const auto &kv : kv_pairs) {
    auto art_key = ARTKey::CreateARTKey<int64_t>(arena_allocator, kv.first);
    std::vector<idx_t> results;
    bool success = art2.Get(art_key, results);

    EXPECT_TRUE(success);
    EXPECT_EQ(1, results.size());
    EXPECT_EQ(kv.second, results[0]);
  }
}

TEST(ARTTest, ARTDebugTEST) { ART art("i64_art.data"); }

TEST_F(ARTSerializeTest, MediumARTTest) {
  Allocator &allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  SetUpFiles("medium_i64_art.data");
  auto kv_pairs = PrepareData(10000, "medium_kv_pairs.json");

  kv_pairs = readKVPairsFromFile("medium_kv_pairs.json");

  keep_file = false;

  auto index_path = GetFiles();

  ART art(index_path);

  for (const auto &kv : kv_pairs) {
    auto art_key = ARTKey::CreateARTKey<int64_t>(arena_allocator, kv.first);
    art.Put(art_key, kv.second);
  }

  art.Serialize();

  ART art2(index_path);

  for (const auto &kv : kv_pairs) {
    auto art_key = ARTKey::CreateARTKey<int64_t>(arena_allocator, kv.first);
    std::vector<idx_t> results;
    bool success = art2.Get(art_key, results);

    EXPECT_TRUE(success);
    EXPECT_EQ(1, results.size());
    EXPECT_EQ(kv.second, results[0]);
  }
}

TEST_F(ARTSerializeTest, BigARTTest) {
  Allocator &allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);
  SetUpFiles("big_i64_art.data");
  auto kv_pairs = PrepareData(1000000, "big_kv_pairs.json");

  kv_pairs = readKVPairsFromFile("big_kv_pairs.json");

  keep_file = true;

  auto index_path = GetFiles();

  ART art(index_path);

  for (const auto &kv : kv_pairs) {
    auto art_key = ARTKey::CreateARTKey<int64_t>(arena_allocator, kv.first);
    art.Put(art_key, kv.second);
  }

  art.Serialize();

  ART art2(index_path);

  for (const auto &kv : kv_pairs) {
    auto art_key = ARTKey::CreateARTKey<int64_t>(arena_allocator, kv.first);
    std::vector<idx_t> results;
    bool success = art2.Get(art_key, results);

    EXPECT_TRUE(success);
    EXPECT_EQ(1, results.size());
    EXPECT_EQ(kv.second, results[0]);
  }
}

TEST(SerializerTest, Basic) {
  Allocator &allocator = Allocator::DefaultAllocator();
  SequentialSerializer serializer("serialize_test.data");
  std::vector<int> ints;
  for (int i = 0; i < 2023; i++) {
    ints.emplace_back(i);
    serializer.Write(i);
  }
  std::vector<std::string> strs;
  for (int i = 0; i < 2023; i++) {
    std::string str = fmt::format("value_{}", i);
    strs.emplace_back(str);
    serializer.WriteData(reinterpret_cast<const_data_ptr_t>(str.data()), str.size());
  }
  serializer.Flush();

  BlockPointer pointer(0, 0);
  BlockDeserializer reader("serialize_test.data", pointer);

  for (const auto &v : ints) {
    auto rv = reader.Read<int>();
    EXPECT_EQ(rv, v);
  }

  for (const auto &str : strs) {
    auto data = allocator.AllocateData(str.size());
    reader.ReadData(data, str.size());
    std::string rv(reinterpret_cast<const char *>(data), str.size());
    EXPECT_EQ(rv, str);
    allocator.FreeData(data, str.size());
  }

  std::string r_str = generateRandomString(10000);

  serializer.WriteData(reinterpret_cast<const_data_ptr_t>(r_str.data()), r_str.size());

  std::string r_str2 = generateRandomString(2023 * 3);

  serializer.WriteData(reinterpret_cast<const_data_ptr_t>(r_str2.data()), r_str2.size());

  serializer.Flush();

  {
    auto data = allocator.AllocateData(r_str.size());
    reader.ReadData(data, r_str.size());

    std::string rv(reinterpret_cast<const char *>(data), r_str.size());

    EXPECT_EQ(rv, r_str);
    allocator.FreeData(data, r_str.size());
  }
  {
    auto data = allocator.AllocateData(r_str2.size());
    reader.ReadData(data, r_str2.size());

    std::string rv(reinterpret_cast<const char *>(data), r_str2.size());

    EXPECT_EQ(rv, r_str2);
    allocator.FreeData(data, r_str2.size());
  }
}

TEST(UIDIndexTest, Serialize) {
  ART art("uid.idx");
  Allocator &allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);
  ARTKey key = ARTKey::CreateARTKey<int64_t>(arena_allocator, 1);
  art.Put(key, 1);

  art.Serialize();
}

TEST(UIDIndexTest, Deserialize) {
  ART art("uid.idx");

  Allocator &allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);
  ARTKey key = ARTKey::CreateARTKey<int64_t>(arena_allocator, 1);
  std::vector<idx_t> results;
  bool success = art.Get(key, results);
  EXPECT_TRUE(success);
  EXPECT_EQ(1, results.size());
  EXPECT_EQ(1, results[0]);
}
