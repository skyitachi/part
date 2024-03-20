//
// Created by skyitachi on 24-3-20.
//
#include <chrono>

#include "concurrent_art.h"
#include "util.h"
#include "art.h"

using namespace part;

int main() {
  int limit = 1000000;

//  ConcurrentART cart("item_id.idx");
  ART cart("art.idx");

  Allocator &allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  std::vector<ARTKey> keys;

  for (idx_t i = 0; i < limit; i++) {
    keys.push_back(ARTKey::CreateARTKey<int32_t>(arena_allocator, i));
  }

  for (idx_t i = 0; i < limit; i++) {
    cart.Put(keys[i], i);
  }

  auto start = std::chrono::high_resolution_clock::now();

  cart.Serialize();

  auto end = std::chrono::high_resolution_clock::now();

  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  // 输出计时结果
  std::cout << "耗时: " << duration.count() << " 毫秒" << std::endl;

  return 0;
}