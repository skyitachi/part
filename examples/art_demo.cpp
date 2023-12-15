//
// Created by Shiping Yao on 2023/11/30.
//
#include <iostream>
#include <arena_allocator.h>
#include <art_key.h>
#include <art.h>
#include <node.h>
#include <fmt/core.h>
#include <random>
#include <unordered_set>
#include <unordered_map>
#include <chrono>
#include <absl/container/flat_hash_map.h>

using namespace part;

void benchHashMap(std::unordered_map<int64_t, int64_t> &map, std::vector<std::pair<int64_t, int64_t>> &values) {
  auto start = std::chrono::high_resolution_clock::now();
  for (const auto &pair : values) {
    map.insert(pair);
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto consumes = std::chrono::duration_cast<std::chrono::milliseconds >(end - start).count();

  int64_t estimated_size =
      map.size() * (sizeof(int64_t) + sizeof(void *)) + map.bucket_count() * (sizeof(void *) + sizeof(size_t)) * 1.5;

  fmt::print("unordered_map: {} keys consumes: {} bytes, {} ms\n", values.size(), estimated_size, consumes);
}

void benchAbslFlatHashMap(absl::flat_hash_map<int64_t, int64_t> &map, std::vector<std::pair<int64_t, int64_t>> &values) {
  auto start = std::chrono::high_resolution_clock::now();
  for (const auto &pair : values) {
    map.insert(pair);
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto consumes = std::chrono::duration_cast<std::chrono::milliseconds >(end - start).count();

  int64_t estimated_size =
      map.size() * (sizeof(int64_t) + sizeof(void *)) + map.bucket_count() * (sizeof(void *) + sizeof(size_t)) * 1.5;

  fmt::print("flat_hash_map: {} keys consumes: {} bytes, {} ms\n", values.size(), estimated_size, consumes);
}

void benchARTRead(ART &art,  std::vector<std::pair<ARTKey, idx_t>> &art_keys) {
  auto start = std::chrono::high_resolution_clock::now();
  std::vector<idx_t> results = {};
  for(int i = 0; i < art_keys.size(); i++) {
    results.clear();
    bool success = art.Get(art_keys[i].first, results);
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto consumes = std::chrono::duration_cast<std::chrono::milliseconds >(end - start).count();

  fmt::print("ART read: {} keys consumes: {} ms\n", art_keys.size(), consumes);

}

void benchHashMapRead(std::unordered_map<int64_t, int64_t> &map, std::vector<std::pair<int64_t, int64_t>> &values) {
  auto start = std::chrono::high_resolution_clock::now();
  for (const auto &pair : values) {
    map.find(pair.first);
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto consumes = std::chrono::duration_cast<std::chrono::milliseconds >(end - start).count();

  fmt::print("unordered_map read: {} keys consumes {} ms\n", values.size(), consumes);
}

void benchAbslFlatHashMapRead(absl::flat_hash_map<int64_t, int64_t> &map, std::vector<std::pair<int64_t, int64_t>> &values) {
  auto start = std::chrono::high_resolution_clock::now();
  for (const auto &pair : values) {
    map.find(pair.first);
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto consumes = std::chrono::duration_cast<std::chrono::milliseconds >(end - start).count();

  fmt::print("absl flat_hash_map read: {} keys consumes {} ms\n", values.size(), consumes);
}

void benchBuildART(ART &art, std::vector<std::pair<ARTKey, idx_t>> &values) {
  auto start = std::chrono::high_resolution_clock::now();
  for (const auto &[key, value]: values) {
    art.Put(key, value);
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto consumes = std::chrono::duration_cast<std::chrono::milliseconds >(end - start).count();

  fmt::print("ART consumes memory: {}, {} ms\n", art.GetMemoryUsage(), consumes);
}

void testInsertDuplicateKey(ArenaAllocator& arena_allocator) {
  ART art;
  ARTKey k1 = ARTKey::CreateARTKey<std::string>(arena_allocator, "k1");
  ARTKey k2 = ARTKey::CreateARTKey<std::string>(arena_allocator, "k1");

  art.Put(k1, 1);
  art.Put(k2, 2);

  std::vector<idx_t> results;

  art.Get(k1, results);

  assert(results.size() == 2);
  assert(results[0] == 1 && results[1] == 2);
}

int main() {
  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  assert(arena_allocator.IsEmpty());


  data_ptr_t data = arena_allocator.Allocate(10);
  assert(data);

//  assert(arena_allocator.SizeInBytes() == 10);
  std::cout << arena_allocator.SizeInBytes() << std::endl;

  ARTKey k1 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 10);
  ARTKey k2 = ARTKey::CreateARTKey<int64_t>(arena_allocator, -1);

  assert(k1 > k2);

  ARTKey k3 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 10);
  assert(k1 == k3);

  ARTKey k4 = ARTKey::CreateARTKey<int64_t >(arena_allocator, 100);
  assert(k4 > k1);

  ARTKey sk1 = ARTKey::CreateARTKey<std::string_view>(arena_allocator, "hello");
  std::cout << sk1.len << std::endl;

  std::string raw_sk2 = "hello2";
  ARTKey sk2 = ARTKey::CreateARTKey<std::string_view>(arena_allocator, raw_sk2);
  std::cout << sk2.len << std::endl;

  assert(sk2 > sk1);

  std::string raw_sk3 = "hello2";
  ARTKey sk3 = ARTKey::CreateARTKey<std::string_view>(arena_allocator, raw_sk3);
  assert(sk2 == sk3);

  testInsertDuplicateKey(arena_allocator);

  ART art;
  std::vector<idx_t> results = {};

  std::random_device rd;
  std::mt19937_64 gen(rd());
  std::uniform_int_distribution<int64_t> dist(0, std::numeric_limits<int64_t>::max());

  int limit = 0;

  // 需要保证所有的key都不一样
//  std::vector<std::string> raw_keys = {};
//  std::unordered_set<int64_t> key_sets = {};
//  std::unordered_map<int64_t, int64_t> hash_map;
//  std::vector<std::pair<int64_t, int64_t>> values;
//
//  for(int i = 0; i < limit; i++) {
//    int64_t rk;
//    do {
//      rk = dist(gen);
//    } while (key_sets.contains(rk));
//    values.emplace_back(rk, i);
//    key_sets.insert(rk);
//    raw_keys.emplace_back(fmt::format("{}",rk));
//  }
//
//  benchHashMap(hash_map, values);
//
//  absl::flat_hash_map<int64_t, int64_t> fmap;
//  benchAbslFlatHashMap(fmap, values);
//
//
//  std::vector<std::pair<ARTKey, idx_t>> art_keys = {};
//  for(auto& value: values) {
//    art_keys.emplace_back(ARTKey::CreateARTKey<int64_t >(arena_allocator, value.first), value.second);
//  }

  {
    ART art2;
    art2.Put(k1, 1000);

    SequentialSerializer writer("art.index");

    auto block_pointer = art2.Serialize(writer);


    fmt::println("root set: {}, block_id: {}, offset_: {}", art2.root->IsSet(), block_pointer.block_id, block_pointer.offset);


  }

//  benchBuildART(art, art_keys);
//  benchHashMapRead(hash_map, values);
//  benchARTRead(art, art_keys);
//  benchAbslFlatHashMapRead(fmap, values);

}

