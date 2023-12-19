//
// Created by skyitachi on 23-12-13.
//

#ifndef PART_BLOCK_H
#define PART_BLOCK_H

#include "types.h"

namespace part {
struct BlockPointer {
  BlockPointer(block_id_t block_id_p, uint32_t offset_p) : block_id(block_id_p), offset(offset_p) {}

  BlockPointer() : block_id(INVALID_BLOCK), offset(0) {}

  block_id_t block_id;
  uint32_t offset;

  bool IsValid() { return block_id != INVALID_BLOCK; }
};
}  // namespace part
#endif  // PART_BLOCK_H
