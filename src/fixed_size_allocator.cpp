//
// Created by Shiping Yao on 2023/12/4.
//
#include "fixed_size_allocator.h"

#include <fmt/core.h>
#include <fmt/printf.h>

#include "leaf.h"
#include "node16.h"
#include "node256.h"
#include "node4.h"
#include "node48.h"
#include "prefix.h"
#include "serializer.h"

namespace part {

constexpr idx_t FixedSizeAllocator::BASE[];
constexpr uint8_t FixedSizeAllocator::SHIFT[];

FixedSizeAllocator::FixedSizeAllocator(const idx_t allocation_size, Allocator &allocator)
    : allocation_size(allocation_size), total_allocations(0), allocator(allocator) {
  initMaskData();
}

void FixedSizeAllocator::initMaskData() {
  idx_t bits_per_value = sizeof(validity_t) * 8;
  idx_t curr_alloc_size = 0;
  bitmask_count = 0;
  allocations_per_buffer = 0;
  while (curr_alloc_size < BUFFER_ALLOC_SIZE) {
    if (!bitmask_count || (bitmask_count * bits_per_value) % allocations_per_buffer == 0) {
      bitmask_count++;
      curr_alloc_size += sizeof(validity_t);
    }

    auto remaining_alloc_size = BUFFER_ALLOC_SIZE - curr_alloc_size;
    auto remaining_allocations = std::min(remaining_alloc_size / allocation_size, bits_per_value);

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

uint32_t FixedSizeAllocator::GetOffset(ValidityMask &mask, idx_t allocation_count) {
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

ConcurrentNode FixedSizeAllocator::ConcNew() {
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
  return ConcurrentNode(buffer_id, offset);
}

// not reclaim memory
void FixedSizeAllocator::Free(const Node ptr) {
  auto buffer_id = ptr.GetBufferId();
  auto offset = ptr.GetOffset();

  assert(buffer_id < buffers.size());
  auto &buffer = buffers[buffer_id];

  auto bitmask_ptr = reinterpret_cast<validity_t *>(buffer.ptr);
  ValidityMask mask(bitmask_ptr);

  assert(!mask.RowIsValid(offset));
  mask.SetValid(offset);

  buffer.allocation_count--;
  total_allocations--;
  buffers_with_free_space.insert(buffer_id);
}

void FixedSizeAllocator::ConcFree(const ConcurrentNode *ptr) {
  assert(ptr->Locked());
  auto buffer_id = ptr->GetBufferId();
  auto offset = ptr->GetOffset();

  assert(buffer_id < buffers.size());
  auto &buffer = buffers[buffer_id];

  auto bitmask_ptr = reinterpret_cast<validity_t *>(buffer.ptr);
  ValidityMask mask(bitmask_ptr);

  assert(!mask.RowIsValid(offset));
  mask.SetValid(offset);

  buffer.allocation_count--;
  total_allocations--;
  buffers_with_free_space.insert(buffer_id);
}

void FixedSizeAllocator::SerializeBuffers(SequentialSerializer &writer) {
  auto buf_size = buffers.size();
  writer.WriteData(const_data_ptr_cast(&buf_size), sizeof(buf_size));
  writer.WriteData(const_data_ptr_cast(&allocation_size), sizeof(allocation_size));

  for (auto &buffer : buffers) {
    // NOTE: mask and data are need to write files???
    // and allocation_size, allocation_count are needed to write to files
    //    ValidityMask mask(bitmask_ptr);
    //    auto p_data = buffer.ptr + allocation_offset;
    writer.WriteData(const_data_ptr_cast(&buffer.allocation_count), sizeof(buffer.allocation_count));
    //    writer.WriteData(p_data, buffer.allocation_count * allocation_size);
    // include mask data
    writer.WriteData(buffer.ptr, BUFFER_ALLOC_SIZE);
  }
}

FixedSizeAllocator::FixedSizeAllocator(Deserializer &reader, Allocator &allocator)
    : total_allocations(0), allocator(allocator) {

  size_t buf_size = 0;

  reader.ReadData(data_ptr_cast(&buf_size), sizeof(buf_size));
  reader.ReadData(data_ptr_cast(&allocation_size), sizeof(allocation_size));
  initMaskData();

  for (idx_t i = 0; i < buf_size; i++) {
    idx_t allocation_count = 0;
    reader.ReadData(data_ptr_cast(&allocation_count), sizeof(allocation_count));
    total_allocations += allocation_count;
    auto ptr = allocator.AllocateData(BUFFER_ALLOC_SIZE);
    reader.ReadData(ptr, BUFFER_ALLOC_SIZE);
    BufferEntry entry(ptr, allocation_count);
    buffers.emplace_back(std::move(entry));
  }
}

// only need read lock
void FixedSizeAllocator::SerializeBuffers(SequentialSerializer &writer, NType node_type) {
  auto buf_size = buffers.size();
  writer.WriteData(const_data_ptr_cast(&buf_size), sizeof(buf_size));
  writer.WriteData(const_data_ptr_cast(&allocation_size), sizeof(allocation_size));

  auto block_pointer = writer.GetBlockPointer();

//  fmt::println("serialize allocator: {}, buf_size: {}, block_id: {}, block_offset: {}", allocation_size, buf_size,
//               block_pointer.block_id, block_pointer.offset);
  //  // Node: different from ART
  //  writer.Write<uint8_t>(static_cast<uint8_t>(node_type));
  idx_t nsz[] = {sizeof(CPrefix), sizeof(CLeaf), sizeof(CNode4), sizeof(CNode16), sizeof(CNode48), sizeof(CNode256)};

  data_t tmp_buf[4096];

  for (auto &buffer : buffers) {
    // allocation_count
    writer.Write<idx_t>(buffer.allocation_count);
    auto bitmask_ptr = reinterpret_cast<validity_t *>(buffer.ptr);
    ValidityMask mask(bitmask_ptr);
    idx_t cnt = 0;

    // copy mask data
    writer.WriteData(buffer.ptr, allocation_offset);

    cnt += allocation_offset;

    idx_t padding = BUFFER_ALLOC_SIZE - allocations_per_buffer * allocation_size - allocation_offset;

//    fmt::println("allocation_offset: {}, allocations_per_buffer: {}, padding: {}", allocation_offset,
//                 allocations_per_buffer, padding);

    auto sz = nsz[uint8_t(node_type) - 1];

    for (idx_t i = 0; i < allocations_per_buffer; i++) {
      auto ptr = buffer.ptr + allocation_offset + i * allocation_size;
      cnt += sz;

      block_pointer = writer.GetBlockPointer();
      if (mask.RowIsValid(i)) {
        writer.WriteData(tmp_buf, sz);
        auto after_block_pointer = writer.GetBlockPointer();
        auto written = after_block_pointer.block_id * BLOCK_SIZE + after_block_pointer.offset -
                       block_pointer.block_id * BLOCK_SIZE - block_pointer.offset;
        P_ASSERT(written == sz);
        continue;
      }

      std::memcpy(tmp_buf, ptr, sz);

      switch (node_type) {
        case NType::LEAF: {
          // need acquire write lock for node points to this node
          auto *cleaf = Get<CLeaf>(tmp_buf);
          if (cleaf->next != 0) {
            auto *cnode = cleaf->ptr;
            if (cleaf->ptr) {
              cleaf->next = cnode->GetData();
              Node::SetSerialized(cleaf->next);
            }
          }
          break;
        }
        case NType::PREFIX: {
          auto *cprefix = Get<CPrefix>(tmp_buf);
//          fmt::println("fast_serialize prefix count: {}, offset: {}", cprefix->data[Node::PREFIX_SIZE], i);
          auto *cnode = cprefix->ptr;
          if (cprefix->ptr) {
            cprefix->node = cnode->GetData();
            Node::SetSerialized(cprefix->node);
          }
          break;
        }
        case NType::NODE_4: {
          auto *cnode4 = Get<CNode4>(tmp_buf);
//          fmt::println("cnode4 count: {}", cnode4->count);
          ::fflush(stdout);
          for (idx_t k = 0; k < cnode4->count; k++) {
            auto child_node = cnode4->children[k].ptr->GetData();
            cnode4->children[k].node = child_node;
            Node::SetSerialized(cnode4->children[k].node);
          }
          break;
        }
        case NType::NODE_16: {
          auto *cn16 = Get<CNode16>(tmp_buf);
//          fmt::println("cnode16 count: {}", cn16->count);
          for (idx_t k = 0; k < cn16->count; k++) {
            auto child_node = cn16->children[k].ptr->GetData();
            cn16->children[k].node = child_node;
            Node::SetSerialized(cn16->children[k].node);
          }
          break;
        }
        case NType::NODE_48: {
          auto *cn48 = Get<CNode48>(tmp_buf);
//          fmt::println("cnode48 count: {}", cn48->count);

          for (idx_t k = 0; k < Node::NODE_256_CAPACITY; k++) {
            auto idx = cn48->child_index[k];
            if (idx != Node::EMPTY_MARKER) {
              auto child_node = cn48->children[idx].ptr->GetData();
              cn48->children[idx].node = child_node;
              Node::SetSerialized(cn48->children[idx].node);
            }
          }
          break;
        }
        case NType::NODE_256: {
          auto *cn256 = Get<CNode256>(tmp_buf);
//          fmt::println("cnode256 count: {}", cn256->count);
          for (idx_t k = 0; k < Node::NODE_256_CAPACITY; k++) {
            if (cn256->children[k].data != 0) {
              auto child_node = cn256->children[k].ptr->GetData();
              cn256->children[k].data = child_node;
              Node::SetSerialized(cn256->children[k].data);
            }
          }
          break;
        }
        default:
          throw std::invalid_argument("unsupported node type in SerializeBuffers");
      }
      block_pointer = writer.GetBlockPointer();
      writer.WriteData(tmp_buf, sz);
      auto after_block_pointer = writer.GetBlockPointer();

      auto written = after_block_pointer.block_id * BLOCK_SIZE + after_block_pointer.offset -
                     block_pointer.block_id * BLOCK_SIZE - block_pointer.offset;
      P_ASSERT(written == sz);
    }

    if (padding > 0) {
      block_pointer = writer.GetBlockPointer();
      writer.WriteData(tmp_buf, padding);
      auto after_block_pointer = writer.GetBlockPointer();

      auto written = after_block_pointer.block_id * BLOCK_SIZE + after_block_pointer.offset -
                     block_pointer.block_id * BLOCK_SIZE - block_pointer.offset;
      cnt += padding;
      P_ASSERT(written == padding);
    }

    block_pointer = writer.GetBlockPointer();

//    fmt::println(
//        "[SerializeBuffer] write data: {}, buf allocated size: {}, "
//        "block_id: {}, block_offset: {}",
//        cnt, BUFFER_ALLOC_SIZE, block_pointer.block_id, block_pointer.offset);
  }
}

}  // namespace part
