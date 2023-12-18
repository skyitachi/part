//
// Created by Shiping Yao on 2023/12/4.
//
#include "arena_allocator.h"

#include <cassert>

namespace part {

//===--------------------------------------------------------------------===//
// Arena Chunk
//===--------------------------------------------------------------------===//
ArenaChunk::ArenaChunk(Allocator &allocator, idx_t size) : current_position(0), maximum_size(size), prev(nullptr) {
  assert(size > 0);
  data = allocator.Allocate(size);
}

ArenaChunk::~ArenaChunk() {
  if (next) {
    auto current_next = std::move(next);
    while (current_next) {
      current_next = std::move(current_next->next);
    }
  }
}
//===--------------------------------------------------------------------===//
// Allocator Wrapper
//===--------------------------------------------------------------------===//
struct ArenaAllocatorData : public PrivateAllocatorData {
  explicit ArenaAllocatorData(ArenaAllocator &allocator) : allocator(allocator) {}

  ArenaAllocator &allocator;
};

static data_ptr_t ArenaAllocatorAllocate(PrivateAllocatorData *private_data, idx_t size) {
  auto &allocator_data = private_data->Cast<ArenaAllocatorData>();
  return allocator_data.allocator.Allocate(size);
}

static void ArenaAllocatorFree(PrivateAllocatorData *, data_ptr_t, idx_t) {
  // nop
}

static data_ptr_t ArenaAllocateReallocate(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t old_size,
                                          idx_t size) {
  auto &allocator_data = private_data->Cast<ArenaAllocatorData>();
  return allocator_data.allocator.Reallocate(pointer, old_size, size);
}

ArenaAllocator::ArenaAllocator(Allocator &allocator, idx_t initial_capacity)
    : allocator(allocator),
      arena_allocator(ArenaAllocatorAllocate, ArenaAllocatorFree, ArenaAllocateReallocate,
                      std::make_unique<ArenaAllocatorData>(*this)) {
  head = nullptr;
  tail = nullptr;
  current_capacity = initial_capacity;
}

ArenaAllocator::~ArenaAllocator() {}

data_ptr_t ArenaAllocator::Allocate(idx_t len) {
  assert(!head || head->current_position <= head->maximum_size);
  if (!head || head->current_position + len > head->maximum_size) {
    do {
      current_capacity *= 2;
    } while (current_capacity < len);
    auto new_chunk = std::make_unique<ArenaChunk>(allocator, current_capacity);
    if (head) {
      head->prev = new_chunk.get();
      new_chunk->next = std::move(head);
    } else {
      tail = new_chunk.get();
    }
    head = std::move(new_chunk);
  }
  assert(head->current_position + len <= head->maximum_size);
  auto result = head->data.get() + head->current_position;
  head->current_position += len;
  return result;
}

data_ptr_t ArenaAllocator::Reallocate(data_ptr_t pointer, idx_t old_size, idx_t new_size) {
  assert(head);
  if (old_size == new_size) {
    return pointer;
  }
  auto head_ptr = head->data.get() + head->current_position;
  int64_t diff = new_size - old_size;
  if (pointer == head_ptr && (new_size < old_size || head->current_position + diff <= head->maximum_size)) {
    head->current_position += diff;
    return pointer;
  } else {
    auto result = Allocate(new_size);
    memcpy(result, pointer, old_size);
    return result;
  }
}

data_ptr_t ArenaAllocator::AllocateAligned(idx_t size) { return Allocate(AlignValue<idx_t>(size)); }

data_ptr_t ArenaAllocator::ReallocateAligned(data_ptr_t pointer, idx_t old_size, idx_t size) {
  return Reallocate(pointer, old_size, AlignValue<idx_t>(size));
}

void ArenaAllocator::Reset() {
  if (head) {
    // destroy all chunks except the current one
    if (head->next) {
      auto current_next = std::move(head->next);
      while (current_next) {
        current_next = std::move(current_next->next);
      }
    }
    tail = head.get();

    // reset the head
    head->current_position = 0;
    head->prev = nullptr;
  }
}

void ArenaAllocator::Destroy() {
  head = nullptr;
  tail = nullptr;
  current_capacity = ARENA_ALLOCATOR_INITIAL_CAPACITY;
}

void ArenaAllocator::Move(ArenaAllocator &other) {
  assert(!other.head);
  other.tail = tail;
  other.head = std::move(head);
  other.current_capacity = current_capacity;
  Destroy();
}

ArenaChunk *ArenaAllocator::GetHead() { return head.get(); }

ArenaChunk *ArenaAllocator::GetTail() { return tail; }

bool ArenaAllocator::IsEmpty() const { return head == nullptr; }

idx_t ArenaAllocator::SizeInBytes() const {
  idx_t total_size = 0;
  if (!IsEmpty()) {
    auto current = head.get();
    while (current != nullptr) {
      total_size += current->current_position;
      current = current->next.get();
    }
  }
  return total_size;
}

}  // namespace part