//
// Created by skyitachi on 24-1-31.
//
#include <fmt/core.h>
#include <gtest/gtest.h>

#include <chrono>

#include "concurrent_art.h"
#include "leaf.h"
#include "prefix.h"

using namespace part;

TEST(ConcurrentARTTest, Basic) {
  ConcurrentART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  ARTKey k1 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 10);

  art.Put(k1, 1);

  std::vector<idx_t> results_ids;

  art.Get(k1, results_ids);
  EXPECT_EQ(results_ids.size(), 1);
  EXPECT_EQ(results_ids[0], 1);
}

TEST(ConcurrentARTTest, ConcurrentTest) {
  ConcurrentART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  ARTKey k1 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 10);
  idx_t doc_id = 1;

  std::thread t1([&] {
    std::this_thread::yield();
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(1ms);

    art.Put(k1, doc_id);
  });

  std::thread t2([&] {
    std::vector<idx_t> results_ids;

    while (!art.Get(k1, results_ids)) {
    }
    EXPECT_EQ(results_ids.size(), 1);
    EXPECT_EQ(results_ids[0], 1);
  });
  t1.join();
  t2.join();
}

TEST(ConcurrentARTTest, Memory) {
  fmt::println("CPrefix size {}, CLeaf size {}", sizeof(CPrefix), sizeof(CLeaf));
  fmt::println("Prefix size {}, Leaf size {}", sizeof(Prefix), sizeof(Leaf));
}
