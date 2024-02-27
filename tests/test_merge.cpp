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

  cart.Merge(art);

  std::vector<idx_t> result_ids;
  cart.Get(k1, result_ids);
  ASSERT_EQ(result_ids.size(), 1);
  ASSERT_EQ(result_ids[0], 1);
}
