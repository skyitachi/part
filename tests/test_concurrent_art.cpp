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

  art.Put(k1, 1);

  std::vector<idx_t> results_ids;

  art.Get(k1, results_ids);
  EXPECT_EQ(results_ids.size(), 1);
  EXPECT_EQ(results_ids[0], 1);

  art.Put(k1, 2);

  results_ids.clear();
  art.Get(k1, results_ids);
  EXPECT_EQ(results_ids.size(), 2);
  EXPECT_EQ(results_ids[0], 1);
  EXPECT_EQ(results_ids[1], 2);
}

TEST(ConcurrentARTTest, LeafExpand) {
  ConcurrentART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  ARTKey k1 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 10);

  std::vector<idx_t> result_ids;
  std::vector<idx_t> doc_ids = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
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

TEST(ConcurrentARTTest, PrefixSpiltTest) {
  ConcurrentART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  std::vector<idx_t> results_ids;

  ARTKey k1 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 10);

  art.Put(k1, 1);

  art.Get(k1, results_ids);
  EXPECT_EQ(results_ids.size(), 1);
  EXPECT_EQ(results_ids[0], 1);

  results_ids.clear();
  fmt::println("put k1 success");

  ARTKey k2 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 11);
  art.Put(k2, 2);

  art.Get(k1, results_ids);
  EXPECT_EQ(results_ids.size(), 1);
  EXPECT_EQ(results_ids[0], 1);

  results_ids.clear();
  art.Get(k2, results_ids);
  EXPECT_EQ(results_ids.size(), 1);
  EXPECT_EQ(results_ids[0], 2);
}

TEST(ConcurrentARTTest, PrefixSplitAndLeafExpand) {
  ConcurrentART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  std::vector<idx_t> results_ids;

  ARTKey k1 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 10);
  art.Put(k1, 0);

  ARTKey k2 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 11);
  art.Put(k2, 2);

  std::vector<idx_t> result_ids;

  std::vector<idx_t> doc_ids = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
  for (int i = 0; i < doc_ids.size(); i++) {
    result_ids.clear();
    art.Put(k1, doc_ids[i]);
    art.Get(k1, result_ids);
    EXPECT_EQ(result_ids.size(), i + 2);
    for (int k = 1; k < result_ids.size(); k++) {
      EXPECT_EQ(result_ids[k], doc_ids[k - 1]);
    }
  }

  art.Draw("cart.dot");
}

TEST(ConcurrentARTTest, CNode4Insert) {
  ConcurrentART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  std::vector<idx_t> results_ids;

  ARTKey k1 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 1);
  art.Put(k1, 1);

  ARTKey k2 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 2);
  art.Put(k2, 2);

  ARTKey k3 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 3);

  art.Put(k3, 3);

  std::vector<idx_t> result_ids;
  art.Get(k3, results_ids);
  EXPECT_EQ(results_ids.size(), 1);
  EXPECT_EQ(results_ids[0], 3);
  art.Draw("prefix.dot");
}

TEST(ConcurrentARTTest, CNode16Insert) {
  ConcurrentART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  for (idx_t i = 0; i < 10; i++) {
    ARTKey k = ARTKey::CreateARTKey<int64_t>(arena_allocator, i);
    art.Put(k, i);
  }

  art.Draw("cnode16.dot");
}

TEST(ConcurrentARTTest, BigARTInsert) {
  ConcurrentART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);
  idx_t limit = 10000;

  for (idx_t i = 0; i < limit; i++) {
    ARTKey k = ARTKey::CreateARTKey<int64_t>(arena_allocator, i);
    art.Put(k, i);
  }

  for (idx_t i = 0; i < limit; i++) {
    ARTKey k = ARTKey::CreateARTKey<int64_t>(arena_allocator, i);
    std::vector<idx_t> result_ids;
    art.Get(k, result_ids);
    EXPECT_EQ(result_ids.size(), 1);
    EXPECT_EQ(result_ids[0], i);
  }
}

TEST(ConcurrentARTTest, SmallMultiThreadTest) {
  ConcurrentART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);
  idx_t limit = 10;

  std::thread writer([&] {
    for (idx_t i = 0; i < limit; i++) {
      ARTKey k = ARTKey::CreateARTKey<int64_t>(arena_allocator, i);
      art.Put(k, i);
    }
    fmt::println("finish put all data");
  });

  std::thread reader0([&] {
    for (idx_t i = 0; i < limit; i++) {
      ARTKey k = ARTKey::CreateARTKey<int64_t>(arena_allocator, i);
      std::vector<idx_t> result_ids;
      while (!art.Get(k, result_ids)) {
        result_ids.clear();
        std::this_thread::yield();
      }
      EXPECT_EQ(result_ids.size(), 1);
      EXPECT_EQ(result_ids[0], i);
    }
  });

  std::thread reader1([&] {
    for (idx_t i = 0; i < limit; i++) {
      ARTKey k = ARTKey::CreateARTKey<int64_t>(arena_allocator, i);
      std::vector<idx_t> result_ids;
      while (!art.Get(k, result_ids)) {
        result_ids.clear();
        std::this_thread::yield();
      }
      EXPECT_EQ(result_ids.size(), 1);
      EXPECT_EQ(result_ids[0], i);
    }
  });

  writer.join();
  reader0.join();
  reader1.join();
}

TEST(ConcurrentNodeTest, MultiThreadTest) {
  ConcurrentNode node;
  int counter = 0;
  std::vector<std::thread> ths;
  for (int i = 0; i < 10; i++) {
    ths.emplace_back([&] {
      for (int k = 0; k < 100000; k++) {
        node.Lock();
        counter++;
        node.Unlock();
      }
    });
  }

  ths.emplace_back([&] {
    for (int k = 0; k < 10000; k++) {
      node.RLock();
      node.RUnlock();
    }
  });

  for (auto& th : ths) {
    th.join();
  }

  EXPECT_EQ(counter, 10 * 100000);
}

TEST(ConcurrentARTTest, BigMultiThreadTest) {
  ConcurrentART art;

  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);
  std::vector<ARTKey> keys;
  idx_t limit = 1000;

  for (idx_t i = 0; i < limit; i++) {
    keys.push_back(ARTKey::CreateARTKey<int64_t>(arena_allocator, i));
  }

  std::thread writer([&] {
    for (idx_t i = 0; i < limit; i++) {
      art.Put(keys[i], i);
    }
    art.Draw("multi_thread_debug256.dot");
    fmt::println("finish put all data");
  });

  std::thread reader0([&] {
    for (idx_t i = 0; i < limit; i++) {
      std::vector<idx_t> result_ids;
      auto start = std::chrono::high_resolution_clock::now();
      while (!art.Get(keys[i], result_ids)) {
        result_ids.clear();
        std::this_thread::yield();
        auto end = std::chrono::high_resolution_clock::now();

        // 计算耗时
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        if (duration > 1000) {
          fmt::println("debug point, key failed {}", i);
          //          break;
        }
      }
      EXPECT_EQ(result_ids.size(), 1);
      if (result_ids.size() > 0) {
        EXPECT_EQ(result_ids[0], i);
      }
    }
  });

  std::thread reader1([&] {
    for (idx_t i = 0; i < limit; i++) {
      std::vector<idx_t> result_ids;
      while (!art.Get(keys[i], result_ids)) {
        result_ids.clear();
        std::this_thread::yield();
      }
      EXPECT_EQ(result_ids.size(), 1);
      EXPECT_EQ(result_ids[0], i);
    }
  });

  writer.join();
  reader0.join();
  reader1.join();
}
