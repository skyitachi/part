//
// Created by Shiping Yao on 2023/12/8.
//
#include <fmt/core.h>
#include <gtest/gtest.h>

#include <memory>

#include "art.h"
#include "leaf.h"
#include "node.h"
#include "node16.h"
#include "node256.h"
#include "node4.h"
#include "node48.h"
#include "prefix.h"
#include "util.h"

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

  SequentialSerializer writer("art.index", META_OFFSET);

  auto block_pointer = art.Serialize(writer);

  fmt::println("block_id: {}, offset_: {}", block_pointer.block_id, block_pointer.offset);

  SequentialSerializer meta_writer("art.index");

  art.UpdateMetadata(block_pointer, meta_writer);

  meta_writer.Flush();
}

TEST(ARTDeserializeTest, Basic) {
  ART art("art.index");
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
  ArenaAllocator arena_allocator(Allocator::DefaultAllocator(), 16384);
  ART art;
  Random random;

  auto kv_pairs = random.GenKvPairs(100000, arena_allocator);
  for (const auto& [k, v] : kv_pairs) {
    art.Put(k, v);
  }

  fmt::println("non_leaf_count: {}, leaf count: {}", art.NoneLeafCount(), art.LeafCount());
}

TEST(ARTNodeCountTest, SequenceBigARTNodeCount) {
  ArenaAllocator arena_allocator(Allocator::DefaultAllocator(), 16384);

  ART art;
  Random random;

  auto kv_pairs = random.GenOrderedKvPairs(100000, arena_allocator);
  for (const auto& [k, v] : kv_pairs) {
    art.Put(k, v);
  }

  fmt::println("non_leaf_count: {}, leaf count: {}", art.NoneLeafCount(), art.LeafCount());
}

TEST(ARTTest, ARTKeyMemoryLeak) {
  ArenaAllocator arena_allocator(Allocator::DefaultAllocator(), 16384);
  Random random;
  auto kv_pairs = random.GenKvPairs(10, arena_allocator);
  for (auto& pair : kv_pairs) {
    fmt::println("key ken: {}", pair.first.len);
  }
}

TEST(ARTTest, DeleteTest) {
  ART art;

  ArenaAllocator arena_allocator(Allocator::DefaultAllocator(), 16384);
  Random random;

  auto kv_pairs = random.GenKvPairs(10000, arena_allocator);

  for (auto& pair : kv_pairs) {
    art.Put(pair.first, pair.second);
  }

  for (auto& pair : kv_pairs) {
    std::vector<idx_t> results;
    EXPECT_TRUE(art.Get(pair.first, results));
    EXPECT_EQ(results[0], pair.second);
  }

  for (auto& pair : kv_pairs) {
    art.Delete(pair.first, pair.second);
    std::vector<idx_t> results;
    //    bool success = art.Get(pair.first, results);
    EXPECT_FALSE(art.Get(pair.first, results));
  }

  //  ARTKey k1 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 1);
  //  art.Put(k1, 1);
  //
  //  art.Delete(k1, 1);
  //
  //  std::vector<idx_t> results;
  //  art.Get(k1, results);
  //  EXPECT_EQ(results.size(), 0);
  //
  //  ARTKey k2 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 2);
  //  art.Put(k2, 2);
  //
  //  EXPECT_EQ(results[0], 1);
}

TEST(ARTTest, MergeTest) {
  // important
  auto allocators = std::make_shared<std::vector<FixedSizeAllocator>>();
  allocators->emplace_back(sizeof(Prefix), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Leaf), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Node4), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Node16), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Node48), Allocator::DefaultAllocator());
  allocators->emplace_back(sizeof(Node256), Allocator::DefaultAllocator());

  ART left(allocators);
  // TODO: 不支持32位integer
  ArenaAllocator arena_allocator(Allocator::DefaultAllocator(), 16384);
  auto k1 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 10);
  auto k2 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 11);
  left.Put(k1, 1000);

  std::vector<idx_t> results;
  EXPECT_TRUE(left.Get(k1, results));
  EXPECT_EQ(1, results.size());
  EXPECT_EQ(1000, results[0]);
  results.clear();

  ART right(allocators);
  right.Put(k2, 1001);

  EXPECT_TRUE(right.Get(k2, results));
  EXPECT_EQ(1, results.size());
  EXPECT_EQ(1001, results[0]);
  results.clear();

  left.Merge(right);

  EXPECT_TRUE(left.Get(k2, results));
  EXPECT_EQ(1, results.size());
  EXPECT_EQ(1001, results[0]);
}
