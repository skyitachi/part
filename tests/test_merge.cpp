//
// Created by skyitachi on 24-2-27.
//
#include <fmt/core.h>
#include <gtest/gtest.h>

#include <memory>
#include <type_traits>

#include "art.h"
#include "concurrent_art.h"
#include "concurrent_node.h"
#include "leaf.h"
#include "node.h"
#include "node16.h"
#include "node256.h"
#include "node4.h"
#include "node48.h"
#include "prefix.h"
#include "util.h"

using namespace part;

TEST(ConcurrentARTMergeTest, MemoryMergeBasic) {
  ConcurrentART cart;
  ART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  ARTKey k1 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 1);
  art.Put(k1, 1);

  ARTKey k2 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 2);
  art.Put(k2, 2);

  cart.Merge(art);

  std::vector<idx_t> result_ids;
  cart.Get(k1, result_ids);
  ASSERT_EQ(result_ids.size(), 1);
  ASSERT_EQ(result_ids[0], 1);

  result_ids.clear();
  cart.Get(k2, result_ids);
  ASSERT_EQ(result_ids.size(), 1);
  ASSERT_EQ(result_ids[0], 2);
}

TEST(ConcurrentARTMergeTest, MemoryMergeWithTwoART) {
  ConcurrentART cart;
  ART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  std::vector<ARTKey> keys;
  idx_t limit = 4;
  for (idx_t i = 0; i < limit; i++) {
    keys.push_back(ARTKey::CreateARTKey<int32_t>(arena_allocator, i));
  }

  cart.Put(keys[0], 1);
  cart.Put(keys[1], 2);

  art.Put(keys[2], 3);
  art.Put(keys[3], 4);

  cart.Merge(art);

  //  cart.Draw("merged_cart.dot");

  for (idx_t i = 0; i < limit; i++) {
    std::vector<idx_t> result_ids;
    cart.Get(keys[i], result_ids);
    ASSERT_EQ(result_ids.size(), 1);
    ASSERT_EQ(result_ids[0], i + 1);
  }
}

TEST(ConcurrentARTMergeTest, MergeUpdateBigTest) {
  ConcurrentART cart;
  ART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  std::vector<ARTKey> keys;
  idx_t limit = 100000;
  for (idx_t i = 0; i < limit; i++) {
    keys.push_back(ARTKey::CreateARTKey<int32_t>(arena_allocator, i));
  }

  for (idx_t i = 0; i < limit; i++) {
    art.Put(keys[i], i);
  }

  cart.Merge(art);

  for (idx_t i = 0; i < limit; i++) {
    std::vector<idx_t> result_ids;
    cart.Get(keys[i], result_ids);
    ASSERT_EQ(result_ids.size(), 1);
    ASSERT_EQ(result_ids[0], i);
  }
}

TEST(ConcurrentARTMergeTest, MemoryMergeWithTwoARTMedium) {
  ConcurrentART cart;
  ART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  std::vector<ARTKey> keys;
  idx_t limit = 20;
  for (idx_t i = 0; i < limit; i++) {
    keys.push_back(ARTKey::CreateARTKey<int32_t>(arena_allocator, i));
  }

  for (idx_t i = 0; i < limit / 2; i++) {
    cart.Put(keys[i], i);
  }

  for (idx_t i = limit / 2; i < limit; i++) {
    art.Put(keys[i], i);
  }

  cart.Merge(art);

  cart.Draw("merged_cart.dot");

  for (idx_t i = 0; i < limit; i++) {
    std::vector<idx_t> result_ids;
    cart.Get(keys[i], result_ids);
    ASSERT_EQ(result_ids.size(), 1);
    ASSERT_EQ(result_ids[0], i);
  }
}

TEST(ConcurrentARTMergeTest, MergeDifferPrefix) {
  ConcurrentART cart;
  ART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  auto k1 = ARTKey::CreateARTKey<std::string_view>(arena_allocator, "abc");
  auto k2 = ARTKey::CreateARTKey<std::string_view>(arena_allocator, "acd");

  cart.Put(k1, 1);
  art.Put(k2, 2);

  cart.Merge(art);

  cart.Draw("merge_diff_prefix.dot");

  std::vector<idx_t> result_ids;
  cart.Get(k1, result_ids);
  ASSERT_EQ(result_ids.size(), 1);
  ASSERT_EQ(result_ids[0], 1);

  result_ids.clear();
  cart.Get(k2, result_ids);
  ASSERT_EQ(result_ids.size(), 1);
  ASSERT_EQ(result_ids[0], 2);
}

TEST(ConcurrentARTMergeTest, MergeDifferPrefix1) {
  ConcurrentART cart;
  ART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  auto k1 = ARTKey::CreateARTKey<std::string_view>(arena_allocator, "bc");
  auto k2 = ARTKey::CreateARTKey<std::string_view>(arena_allocator, "cde");

  cart.Put(k1, 1);
  art.Put(k2, 2);

  cart.Merge(art);

  cart.Draw("merge_diff_prefix1.dot");

  std::vector<idx_t> result_ids;
  cart.Get(k1, result_ids);
  ASSERT_EQ(result_ids.size(), 1);
  ASSERT_EQ(result_ids[0], 1);

  result_ids.clear();
  cart.Get(k2, result_ids);
  ASSERT_EQ(result_ids.size(), 1);
  ASSERT_EQ(result_ids[0], 2);
}

TEST(ConcurrentARTMergeTest, Node4MergePrefix) {
  ConcurrentART cart;
  ART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  auto k1 = ARTKey::CreateARTKey<std::string_view>(arena_allocator, "bc");
  auto k2 = ARTKey::CreateARTKey<std::string_view>(arena_allocator, "cde");
  auto k3 = ARTKey::CreateARTKey<std::string_view>(arena_allocator, "ba");

  cart.Put(k1, 1);
  cart.Put(k2, 2);
  art.Put(k3, 3);

  cart.Merge(art);

  cart.Draw("merge_diff_prefix1.dot");

  std::vector<idx_t> result_ids;
  cart.Get(k1, result_ids);
  ASSERT_EQ(result_ids.size(), 1);
  ASSERT_EQ(result_ids[0], 1);

  result_ids.clear();
  cart.Get(k2, result_ids);
  ASSERT_EQ(result_ids.size(), 1);
  ASSERT_EQ(result_ids[0], 2);

  result_ids.clear();
  cart.Get(k3, result_ids);
  ASSERT_EQ(result_ids.size(), 1);
  ASSERT_EQ(result_ids[0], 3);
}

TEST(ConcurrentARTTEST, MergeNonePrefixByPrefix) {
  ConcurrentART cart;
  ART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  auto k1 = ARTKey::CreateARTKey<std::string_view>(arena_allocator, "bc");
  auto k2 = ARTKey::CreateARTKey<std::string_view>(arena_allocator, "cde");
  auto k3 = ARTKey::CreateARTKey<std::string_view>(arena_allocator, "ba");

  cart.Put(k1, 1);
  art.Put(k2, 2);
  art.Put(k3, 3);

  cart.Merge(art);

  cart.Draw("MergeNonePrefixByPrefix.dot");

  std::vector<idx_t> result_ids;
  cart.Get(k1, result_ids);
  ASSERT_EQ(result_ids.size(), 1);
  ASSERT_EQ(result_ids[0], 1);

  result_ids.clear();
  cart.Get(k2, result_ids);
  ASSERT_EQ(result_ids.size(), 1);
  ASSERT_EQ(result_ids[0], 2);

  result_ids.clear();
  cart.Get(k3, result_ids);
  ASSERT_EQ(result_ids.size(), 1);
  ASSERT_EQ(result_ids[0], 3);
}

TEST(ConcurrentARTTEST, LeafMergeInternal) {
  ConcurrentART cart;
  ART art;
  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  idx_t limit = 10;
  std::vector<ARTKey> keys;

  for (idx_t i = 0; i < limit; i++) {
    keys.push_back(ARTKey::CreateARTKey<std::string_view>(arena_allocator, "abc"));
  }
  for (idx_t i = 0; i < limit / 2; i++) {
    cart.Put(keys[i], i);
  }

  for (idx_t i = limit / 2; i < limit; i++) {
    art.Put(keys[i], i);
  }

  cart.Merge(art);

  cart.Draw("LeafMergeInternal.dot");

  std::vector<idx_t> result_ids;
  cart.Get(keys[0], result_ids);
  ASSERT_EQ(result_ids.size(), limit);
  for (idx_t i = 0; i < limit; i++) {
    ASSERT_EQ(result_ids[i], i);
  }
}

TEST(ConcurrentARTTest, MergeMedium) {
  ConcurrentART cart;
  ART art;
  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  idx_t limit = 1000;
  std::vector<ARTKey> keys;

  for (idx_t i = 0; i < limit; i++) {
    keys.push_back(ARTKey::CreateARTKey<int32_t>(arena_allocator, i));
  }
  for (idx_t i = 0; i < limit / 2; i++) {
    cart.Put(keys[i], i);
  }

  for (idx_t i = limit / 2; i < limit; i++) {
    art.Put(keys[i], i);
  }

  cart.Merge(art);

  for (idx_t i = 0; i < limit; i++) {
    std::vector<idx_t> result_ids;
    cart.Get(keys[i], result_ids);
    ASSERT_EQ(result_ids.size(), 1);
    ASSERT_EQ(result_ids[0], i);
  }
}

TEST(ConcurrentARTTest, MergeMediumNotEqualSize) {
  ConcurrentART cart;
  ART art;
  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  idx_t limit = 1000;
  idx_t left = 10;
  std::vector<ARTKey> keys;

  for (idx_t i = 0; i < limit; i++) {
    keys.push_back(ARTKey::CreateARTKey<int32_t>(arena_allocator, i));
  }
  for (idx_t i = 0; i < left; i++) {
    cart.Put(keys[i], i);
  }

  for (idx_t i = left; i < limit; i++) {
    art.Put(keys[i], i);
  }

  cart.Merge(art);

  for (idx_t i = 0; i < limit; i++) {
    std::vector<idx_t> result_ids;
    cart.Get(keys[i], result_ids);
    ASSERT_EQ(result_ids.size(), 1);
    ASSERT_EQ(result_ids[0], i);
  }
}
