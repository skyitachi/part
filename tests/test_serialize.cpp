//
// Created by skyitachi on 12/17/23.
//
#include <random>
#include <unordered_set>
#include <iostream>
#include <fstream>

#include <nlohmann/json.hpp>
#include <gtest/gtest.h>

using json = nlohmann::json;

class ARTSerializeTest: public testing::Test {
protected:
  template<class T>
  using Vector = std::vector<T>;
  using Int64pair = std::pair<int64_t, int64_t>;

  Vector<Int64pair> genRandomKvPairs(int32_t limit) {
      Vector<Int64pair> kv_pairs;
      std::unordered_set<int64_t> key_sets;
      for(int i = 0; i < limit; i++) {
        int64_t rk = 0;
        do {
          rk = (*dist_)(*gen_);
        } while (key_sets.contains(rk));
        kv_pairs.emplace_back(rk, i);
        key_sets.insert(rk);
      }
      return kv_pairs;
  }

  int64_t getRandom() {
      return (*dist_)(*gen_);
  }

  static void kvPairToJsonFile(const Vector<Int64pair>& pairs, const std::string &output) {
      std::ofstream f(output);

      json array_obj = json::array();
      for (const auto &kv: pairs) {
          array_obj.emplace_back(json::object({{"key", kv.first}, {"value", kv.second}}));
      }

      f << std::setw(4) << array_obj << std::endl;
  }

  void SetUp() override {
      std::cout << "in the setup " << std::endl;
      gen_ = std::make_unique<std::mt19937_64>(rd_());
      dist_ = std::make_unique<std::uniform_int_distribution<int64_t>>(0, std::numeric_limits<int64_t>::max());
  }

  std::random_device rd_;
  std::unique_ptr<std::mt19937_64> gen_;
  std::unique_ptr<std::uniform_int_distribution<int64_t>> dist_;
};


TEST_F(ARTSerializeTest, Basic) {
    auto kv_pairs = genRandomKvPairs(10);
    std::cout << "kv_pairs: " << kv_pairs.size() << std::endl;

    ARTSerializeTest::kvPairToJsonFile(kv_pairs, "keys_int64.json");
}