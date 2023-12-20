//
// Created by Shiping Yao on 2023/12/8.
//
#include <fmt/core.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "util.h"
#include <node.h>
#include <allocator.h>
#include <arena_allocator.h>
#include <art.h>
#include <art_key.h>

using namespace part;

TEST(ArtTest, basic) {
  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  assert(arena_allocator.IsEmpty());
}

TEST(ARTSerializeTest, basic) {
  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  ART art;

  ARTKey k1 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 10);

  art.Put(k1, 123);

  art.Put(k1, 124);

  art.Put(k1, 123);

  std::vector<idx_t> results;
  bool success = art.Get(k1, results);
  EXPECT_TRUE(success);
  EXPECT_EQ(3, results.size());
  EXPECT_EQ(123, results[0]);
  EXPECT_EQ(124, results[1]);
  EXPECT_EQ(123, results[2]);

  SequentialSerializer writer("art.index");

  auto block_pointer = art.Serialize(writer);

  fmt::println("block_id: {}, offset_: {}", block_pointer.block_id, block_pointer.offset);

  SequentialSerializer meta_writer("art.meta");

  art.UpdateMetadata(block_pointer, meta_writer);
}

TEST(ARTDeserializeTest, Basic) {
  ART art("art.meta", "art.index");
  art.Deserialize();
  EXPECT_FALSE(art.root->IsSerialized());

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  ARTKey k1 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 10);

  std::vector<idx_t> results;
  bool success = art.Get(k1, results);

  EXPECT_TRUE(success);
  EXPECT_EQ(3, results.size());
  EXPECT_EQ(123, results[0]);
  EXPECT_EQ(124, results[1]);
  EXPECT_EQ(123, results[2]);
}

TEST(ARTTest, NodeCount) {
  ART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  ARTKey k1 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 10);
  art.Put(k1, 1);

  EXPECT_EQ(art.NoneLeafCount(), 1);
  EXPECT_EQ(art.LeafCount(), 1);

  // same prefix
  ARTKey k2 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 10);
  art.Put(k2, 2);

  EXPECT_EQ(art.NoneLeafCount(), 1);
  EXPECT_EQ(art.LeafCount(), 1);


  ARTKey k3 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 11);
  art.Put(k3, 2);

  EXPECT_EQ(art.NoneLeafCount(), 2);
  EXPECT_EQ(art.LeafCount(), 2);

}

TEST(ARTNodeCountTest, BigARTNodeCount) {
  ART art;
  Random random;

  auto kv_pairs = random.GenKvPairs(100000);
  for(const auto& [k, v]: kv_pairs) {
    art.Put(k, v);
  }

  fmt::println("non_leaf_count: {}, leaf count: {}", art.NoneLeafCount(), art.LeafCount());
}

TEST(ARTNodeCountTest, SequenceBigARTNodeCount) {
  ART art;
  Random random;

  auto kv_pairs = random.GenOrderedKvPairs(100000);
  for(const auto& [k, v]: kv_pairs) {
    art.Put(k, v);
  }

  fmt::println("non_leaf_count: {}, leaf count: {}", art.NoneLeafCount(), art.LeafCount());
}

TEST(ARTTest, ARTKeyMemoryLeak) {
  Random random;
  auto kv_pairs = random.GenKvPairs(10);
  for(auto &pair: kv_pairs) {
    fmt::println("key ken: {}", pair.first.len);
  }
}

