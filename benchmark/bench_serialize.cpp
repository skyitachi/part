//
// Created by skyitachi on 24-3-20.
//
#include <chrono>

#include "concurrent_art.h"
#include "util.h"
#include "art.h"

using namespace part;

void bench_art_serialize(ART &art) {
  auto start = std::chrono::high_resolution_clock::now();

  art.Serialize();

  auto end = std::chrono::high_resolution_clock::now();

  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  // 输出计时结果
  std::cout << "serialize: " << duration.count() << " 毫秒" << std::endl;
}

void bench_art_fast_serialize(ART &art) {
  auto start = std::chrono::high_resolution_clock::now();

  art.FastSerialize();

  auto end = std::chrono::high_resolution_clock::now();

  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

  // 输出计时结果
  std::cout << "fast_serialize: " << duration.count() << " 毫秒" << std::endl;
}

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

  bench_art_serialize(cart);

  ART fastART("fast_art.idx");

  for (idx_t i = 0; i < limit; i++) {
    fastART.Put(keys[i], i);
  }

  bench_art_fast_serialize(fastART);
  return 0;
}