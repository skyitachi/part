//
// Created by skyitachi on 24-1-31.
//
#include <fmt/core.h>
#include <gtest/gtest.h>

#include <chrono>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <type_traits>

#include "concurrent_art.h"
#include "leaf.h"
#include "prefix.h"
using namespace part;

TEST(ConcurrentARTTest, Basic) {
  ConcurrentART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  ARTKey k1 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 10);

  fmt::println("before first put success");
  art.Put(k1, 1);
  fmt::println("first put success");

  std::vector<idx_t> results_ids;

  art.Get(k1, results_ids);
  EXPECT_EQ(results_ids.size(), 1);
  EXPECT_EQ(results_ids[0], 1);

  //  art.Put(k1, 2);
  //
  //  results_ids.clear();
  //  art.Get(k1, results_ids);
  //  EXPECT_EQ(results_ids.size(), 2);
  //  EXPECT_EQ(results_ids[0], 1);
  //  EXPECT_EQ(results_ids[1], 2);
}

TEST(ConcurrentARTTest, LeafExpand) {
  ConcurrentART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  ARTKey k1 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 10);

  std::vector<idx_t> result_ids;
  std::vector<idx_t> doc_ids = {1, 2, 3, 4, 5};
  for (int i = 0; i < doc_ids.size(); i++) {
    result_ids.clear();
    art.Put(k1, doc_ids[i]);
    art.Get(k1, result_ids);
    EXPECT_EQ(result_ids.size(), i + 1);
    for (int k = 0; k < result_ids.size(); k++) {
      EXPECT_EQ(result_ids[k], doc_ids[k]);
    }
  }
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
  fmt::println("sizeof mutex: {}, shared_mutex: {}", sizeof(std::mutex), sizeof(std::shared_mutex));
}

TEST(ConcurrentARTTest, PointerBasedTest) {
  ConcurrentART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  ARTKey k1 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 10);

  fmt::println("before first put success");
  art.Put(k1, 1);
  fmt::println("first put success");
}

TEST(ConcurrentARTTest, ConcurrentNode) {
  auto ptr = std::make_unique<ConcurrentNode>();
  ptr->Lock();
  ptr->Unlock();

  //  char *prefix_addr = (char *)malloc(sizeof(CPrefix));
  //  auto *cprefix = new (prefix_addr)CPrefix();
  CPrefix* cprefix = new CPrefix;
  cprefix->ptr = new ConcurrentNode();
  cprefix->ptr->Lock();
  cprefix->ptr->Unlock();

  //  delete prefix_addr;
  //  free(prefix_addr);
  delete cprefix;

  fmt::println("CPrefix is trivial = {}", std::is_trivial<CPrefix>::value);

  CPrefix* cprefix2 = (CPrefix*)malloc(sizeof(CPrefix));
  cprefix2->ptr = new ConcurrentNode();
  cprefix2->ptr->Lock();
  cprefix2->ptr->Unlock();
}
