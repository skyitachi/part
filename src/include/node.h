//
// Created by Shiping Yao on 2023/11/30.
//

#ifndef PART_NODE_H
#define PART_NODE_H

#include <inttypes.h>

namespace part {

enum class NType : uint8_t {
  PREFIX = 1,
  LEAF = 2,
  NODE_4 = 3,
  NODE_16 = 4,
  NODE_48 = 5,
  NODE_256 = 6,
  LEAF_INLINED = 7,
};

class Node {
public:
  //! Node thresholds
  static constexpr uint8_t NODE_48_SHRINK_THRESHOLD = 12;
  static constexpr uint8_t NODE_256_SHRINK_THRESHOLD = 36;
  //! Node sizes
  static constexpr uint8_t NODE_4_CAPACITY = 4;
  static constexpr uint8_t NODE_16_CAPACITY = 16;
  static constexpr uint8_t NODE_48_CAPACITY = 48;
  static constexpr uint16_t NODE_256_CAPACITY = 256;
  //! Bit-shifting
  static constexpr uint64_t SHIFT_OFFSET = 32;
  static constexpr uint64_t SHIFT_TYPE = 56;
  static constexpr uint64_t SHIFT_SERIALIZED_FLAG = 63;
  //! AND operations
  static constexpr uint64_t AND_OFFSET = 0x0000000000FFFFFF;
  static constexpr uint64_t AND_BUFFER_ID = 0x00000000FFFFFFFF;
  static constexpr uint64_t AND_IS_SET = 0xFF00000000000000;
  static constexpr uint64_t AND_RESET = 0x00FFFFFFFFFFFFFF;
  //! OR operations
  static constexpr uint64_t SET_SERIALIZED_FLAG = 0x8000000000000000;
  //! Other constants
  static constexpr uint8_t EMPTY_MARKER = 48;
  static constexpr uint8_t LEAF_SIZE = 4;
  static constexpr uint8_t PREFIX_SIZE = 15;

public:
  Node() : data(0) {}

private:
  uint64_t data;
};
} // namespace part

#endif // PART_NODE_H
