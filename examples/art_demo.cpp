//
// Created by Shiping Yao on 2023/11/30.
//
#include <iostream>
#include <arena_allocator.h>
using namespace part;

int main() {
  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  assert(arena_allocator.IsEmpty());

  arena_allocator.Allocate(10);

  assert(arena_allocator.SizeInBytes() == 10);
}
