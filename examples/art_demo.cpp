//
// Created by Shiping Yao on 2023/11/30.
//
#include <iostream>
#include <arena_allocator.h>
#include <art_key.h>

using namespace part;

int main() {
  Allocator& allocator = Allocator::DefaultAllocator();
  ArenaAllocator arena_allocator(allocator, 16384);

  assert(arena_allocator.IsEmpty());

  data_ptr_t data = arena_allocator.Allocate(10);
  assert(data);

  assert(arena_allocator.SizeInBytes() == 10);


  ARTKey k1 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 10);
  ARTKey k2 = ARTKey::CreateARTKey<int64_t>(arena_allocator, -1);

  assert(k1 > k2);

  ARTKey k3 = ARTKey::CreateARTKey<int64_t>(arena_allocator, 10);
  assert(k1 == k3);

  ARTKey k4 = ARTKey::CreateARTKey<int64_t >(arena_allocator, 100);
  assert(k4 > k1);

  ARTKey sk1 = ARTKey::CreateARTKey<std::string_view>(arena_allocator, "hello");
  std::cout << sk1.len << std::endl;

  std::string raw_sk2 = "hello2";
  ARTKey sk2 = ARTKey::CreateARTKey<std::string_view>(arena_allocator, raw_sk2);
  std::cout << sk2.len << std::endl;

  assert(sk2 > sk1);

  std::string raw_sk3 = "hello2";
  ARTKey sk3 = ARTKey::CreateARTKey<std::string_view>(arena_allocator, raw_sk3);
  assert(sk2 == sk3);

}

