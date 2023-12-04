//
// Created by Shiping Yao on 2023/12/4.
//

#ifndef PART_FIXED_SIZE_ALLOCATOR_H
#define PART_FIXED_SIZE_ALLOCATOR_H
#include <vector>
#include <unordered_set>

#include "allocator.h"
#include "node.h"

namespace part {

struct BufferEntry {
  BufferEntry(const data_ptr_t& ptr, const idx_t& allocation_count)
    : ptr(ptr), allocation_count(allocation_count) {}

  data_ptr_t ptr;
  idx_t allocation_count;
};

class FixedSizeAllocator {
public:
  //! Fixed size of the buffers
  static constexpr idx_t BUFFER_ALLOC_SIZE = 262144;

  //! We can vacuum 10% or more of the total memory usage of the allocator
  static constexpr uint8_t VACUUM_THRESHOLD = 10;

  //! Constants for fast offset calculations in the bitmask
  static constexpr idx_t BASE[] = {0x00000000FFFFFFFF, 0x0000FFFF, 0x00FF, 0x0F, 0x3, 0x1};
  static constexpr uint8_t SHIFT[] = {32, 16, 8, 4, 2, 1};

public:
  explicit FixedSizeAllocator(const idx_t allocation_size, Allocator& allocator);
  ~FixedSizeAllocator();

  idx_t allocation_size;
  idx_t total_allocations;
  idx_t bitmask_count;
  idx_t allocation_offset;
  idx_t allocations_per_buffer;

  std::vector<BufferEntry> buffers;
  std::unordered_set<idx_t> buffers_with_free_space;
  Allocator& allocator;
public:
  Node New();

  void Free(const Node ptr);

  template<class T>
  inline T *Get(const Node ptr) const {
    return (T *)Get(ptr);
  }

  void Reset();

  inline idx_t GetMemoryUsage() const {
    return buffers.size() * BUFFER_ALLOC_SIZE;
  }

private:
  inline data_ptr_t Get(const Node ptr) const {
    assert(ptr.GetBufferId() < buffers.size());
    assert(ptr.GetOffset() < allocations_per_buffer);
    return buffers[ptr.GetBufferId()].ptr + ptr.GetOffset() * allocation_size + allocation_offset;
  }
};
}
#endif //PART_FIXED_SIZE_ALLOCATOR_H
