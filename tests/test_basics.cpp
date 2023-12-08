//
// Created by Shiping Yao on 2023/12/8.
//
#include <gtest/gtest.h>

#include <art.h>
#include <allocator.h>
#include <arena_allocator.h>

using namespace part;

TEST(ArtTest, basic) {
  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  assert(arena_allocator.IsEmpty());

}
