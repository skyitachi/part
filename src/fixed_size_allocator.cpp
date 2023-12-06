//
// Created by Shiping Yao on 2023/12/4.
//
#include "fixed_size_allocator.h"

namespace part {

constexpr idx_t FixedSizeAllocator::BASE[];
constexpr uint8_t FixedSizeAllocator::SHIFT[];

FixedSizeAllocator::FixedSizeAllocator(const idx_t allocation_size,
                                       Allocator &allocator)
    : allocation_size(allocation_size), total_allocations(0),
      allocator(allocator) {

  idx_t bits_per_value = sizeof(validity_t) * 8;
  idx_t curr_alloc_size = 0;

  bitmask_count = 0;
  allocations_per_buffer = 0;
  while (curr_alloc_size < BUFFER_ALLOC_SIZE) {
    if (!bitmask_count ||
        (bitmask_count * bits_per_value) % allocations_per_buffer == 0) {
      bitmask_count++;
      curr_alloc_size += sizeof(validity_t);
    }

    auto remaining_alloc_size = BUFFER_ALLOC_SIZE - curr_alloc_size;
    auto remaining_allocations =
        std::min(remaining_alloc_size / allocation_size, bits_per_value);

    if (remaining_allocations == 0) {
      break;
    }
    allocations_per_buffer += remaining_allocations;
    curr_alloc_size += remaining_allocations * allocation_size;
  }
  allocation_offset = bitmask_count * sizeof(validity_t);
}

FixedSizeAllocator::~FixedSizeAllocator() {
  for (auto &buffer : buffers) {
    allocator.FreeData(buffer.ptr, BUFFER_ALLOC_SIZE);
  }
}

uint32_t FixedSizeAllocator::GetOffset(ValidityMask &mask,
                                       const idx_t allocation_count) {

  auto data = mask.GetData();

  // fills up a buffer sequentially before searching for free bits
  if (mask.RowIsValid(allocation_count)) {
    mask.SetInvalid(allocation_count);
    return allocation_count;
  }

  // get an entry with free bits
  for (idx_t entry_idx = 0; entry_idx < bitmask_count; entry_idx++) {
    if (data[entry_idx] != 0) {

      // find the position of the free bit
      auto entry = data[entry_idx];
      idx_t first_valid_bit = 0;

      // this loop finds the position of the rightmost set bit in entry and
      // stores it in first_valid_bit
      for (idx_t i = 0; i < 6; i++) {
        // set the left half of the bits of this level to zero and test if the
        // entry is still not zero
        if (entry & BASE[i]) {
          // first valid bit is in the rightmost s[i] bits
          // permanently set the left half of the bits to zero
          entry &= BASE[i];
        } else {
          // first valid bit is in the leftmost s[i] bits
          // shift by s[i] for the next iteration and add s[i] to the position
          // of the rightmost set bit
          entry >>= SHIFT[i];
          first_valid_bit += SHIFT[i];
        }
      }
      assert(entry);

      auto prev_bits = entry_idx * sizeof(validity_t) * 8;
      assert(mask.RowIsValid(prev_bits + first_valid_bit));
      mask.SetInvalid(prev_bits + first_valid_bit);
      return (prev_bits + first_valid_bit);
    }
  }

  throw std::invalid_argument("Invalid bitmask of FixedSizeAllocator");
}

Node FixedSizeAllocator::New() {
  if (buffers_with_free_space.empty()) {
    idx_t buffer_id = buffers.size();
    auto buffer = allocator.AllocateData(BUFFER_ALLOC_SIZE);
    buffers.emplace_back(buffer, 0);
    buffers_with_free_space.insert(buffer_id);

    ValidityMask mask(reinterpret_cast<validity_t *>(buffer));
    mask.SetAllValid(allocations_per_buffer);
  }
  assert(!buffers_with_free_space.empty());
  auto buffer_id = (uint32_t)*buffers_with_free_space.begin();

  auto bitmask_ptr = reinterpret_cast<validity_t *>(buffers[buffer_id].ptr);
  ValidityMask mask(bitmask_ptr);
  auto offset = GetOffset(mask, buffers[buffer_id].allocation_count);

  buffers[buffer_id].allocation_count++;
  total_allocations++;
  if (buffers[buffer_id].allocation_count == allocations_per_buffer) {
    buffers_with_free_space.erase(buffer_id);
  }
  return Node(buffer_id, offset);
}

} // namespace part
