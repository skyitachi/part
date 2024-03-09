//
// Created by skyitachi on 24-3-9.
//
#include <fmt/core.h>
#include <gtest/gtest.h>

#include <memory>

#include "art.h"
#include "concurrent_art.h"
#include "fixed_size_allocator.h"

using namespace part;

TEST(ConcurrentARTSerializeTest, Debug) {
  Allocator &allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  ConcurrentART cart("cart.data");

  auto k1 = ARTKey::CreateARTKey<int32_t>(arena_allocator, 1);

  std::vector<idx_t> result_ids;
  cart.Get(k1, result_ids);

  ASSERT_EQ(result_ids.size(), 1);
  ASSERT_EQ(result_ids[0], 1);
}

TEST(ConcurrentARTSerializeTest, Basic) {
  Allocator &allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  ConcurrentART cart("cart.data");

  auto k1 = ARTKey::CreateARTKey<int32_t>(arena_allocator, 1);
  auto k2 = ARTKey::CreateARTKey<int32_t>(arena_allocator, 2);

  cart.Put(k1, 1);
  cart.Put(k2, 2);

  cart.Serialize();

  {
    ART art("cart.data");
    std::vector<idx_t> result_ids;
    art.Get(k1, result_ids);
    ASSERT_EQ(result_ids.size(), 1);
    ASSERT_EQ(result_ids[0], 1);

    result_ids.clear();
    art.Get(k2, result_ids);
    ASSERT_EQ(result_ids.size(), 1);
    ASSERT_EQ(result_ids[0], 2);
  }
}
