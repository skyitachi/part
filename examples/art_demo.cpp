//
// Created by Shiping Yao on 2023/11/30.
//
#include <iostream>
#include <arena_allocator.h>
using namespace part;

int main() {
  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arenaAllocator(allocator, 16384);

  assert(arenaAllocator.IsEmpty());

  arenaAllocator.Allocate(10);

  assert(arenaAllocator.SizeInBytes() == 10);
}
