//
// Created by Shiping Yao on 2023/12/20.
//
#include <iostream>

#include "art.h"

#include "util.h"

using namespace part;

int main() {
  Random random;

  ArenaAllocator arena_allocator(Allocator::DefaultAllocator(), 16384);

  auto kv_pairs = random.GenKvPairs(20, arena_allocator);

  std::sort(kv_pairs.begin(), kv_pairs.end(), [](auto a, auto b) {
    return a.first < b.first;
  });

  std::cout << kv_pairs.size() << std::endl;

  auto kv_pairs2 = random.GenKvPairs(100000, arena_allocator);

}