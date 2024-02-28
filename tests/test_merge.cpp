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
  for(idx_t i = 0; i < limit; i++) {
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