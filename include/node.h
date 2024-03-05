//
// Created by Shiping Yao on 2023/11/30.
//

#ifndef PART_NODE_H
#define PART_NODE_H
#include <cassert>
#include <cstring>
#include <optional>

#include "helper.h"
#include "serializer.h"
#include "types.h"

namespace part {

class FixedSizeAllocator;
class ART;

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
  static constexpr uint64_t SHIFT_DELETED_FLAG = 62;
  //! AND operations
  static constexpr uint64_t AND_OFFSET = 0x0000000000FFFFFF;
  static constexpr uint64_t AND_BUFFER_ID = 0x00000000FFFFFFFF;
  static constexpr uint64_t AND_IS_SET = 0xFF00000000000000;
  static constexpr uint64_t AND_RESET = 0x00FFFFFFFFFFFFFF;
  //! OR operations
  static constexpr uint64_t SET_SERIALIZED_FLAG = 0x8000000000000000;

  static constexpr uint64_t SET_DELETED_FLAG = 0x4000000000000000;

  //! Other constants
  static constexpr uint8_t EMPTY_MARKER = 48;
  static constexpr uint8_t LEAF_SIZE = 4;
  static constexpr uint8_t PREFIX_SIZE = 15;

  Node() : data(0) {}
  Node(const uint32_t buffer_id, const uint32_t offset) : data(0) { SetPtr(buffer_id, offset); };

  Node(Deserializer &reader);

  Node(ART &art, Deserializer &reader);

  static void Free(ART &art, Node &node);

  inline void Reset() { data = 0; }

  inline bool operator==(const Node &node) const { return data == node.data; }

  inline bool IsSet() const { return data & AND_IS_SET; }

  //! Returns whether the node is serialized or not (zero bit)
  inline bool IsSerialized() const { return data >> Node::SHIFT_SERIALIZED_FLAG; }

  inline bool IsDeleted() const { return (data >> Node::SHIFT_DELETED_FLAG) & (0x1); }

  //! Get the type (1st to 7th bit)
  inline NType GetType() const {
    assert(!IsSerialized());
    auto type = data >> Node::SHIFT_TYPE;
    assert(type >= (uint8_t)NType::PREFIX);
    P_ASSERT(type <= (uint8_t)NType::LEAF_INLINED);
    return NType(type);
  }

  inline uint8_t UnsafeGetType() const { return data >> Node::SHIFT_TYPE; }

  //! Get the doc id
  inline idx_t GetDocId() const { return data & Node::AND_RESET; }

  inline uint64_t GetData() const { return data; }

  inline void SetData(uint64_t other) { data = other; }

  //! Get the offset (8th to 23rd bit)
  inline idx_t GetOffset() const {
    auto offset = data >> Node::SHIFT_OFFSET;
    return offset & Node::AND_OFFSET;
  }

  inline idx_t GetBufferId() const { return data & Node::AND_BUFFER_ID; }

  //! Set the serialized flag (zero bit)
  inline void SetSerialized() {
    data &= Node::AND_RESET;
    data |= Node::SET_SERIALIZED_FLAG;
  }

  inline void SetDeleted() {
    data &= Node::AND_RESET;
    data |= Node::SET_DELETED_FLAG;
  }

  //! Set the type (1st to 7th bit)
  inline void SetType(const uint8_t type) {
    P_ASSERT(!IsSerialized());
    data += (uint64_t)type << Node::SHIFT_TYPE;
  }
  //! Set the block/buffer ID (24th to 63rd bit) and offset (8th to 23rd bit)
  inline void SetPtr(const uint32_t buffer_id, const uint32_t offset) {
    assert(!(data & Node::AND_RESET));
    auto shifted_offset = ((uint64_t)offset) << Node::SHIFT_OFFSET;
    data += shifted_offset;
    data += buffer_id;
  }

  //! Set the row ID (8th to 63rd bit)
  inline void SetDocID(const idx_t doc_id) {
    P_ASSERT(!(data & Node::AND_RESET));
    data += doc_id;
  }

  static FixedSizeAllocator &GetAllocator(const ART &art, NType type);

  std::optional<Node *> GetChild(ART &art, const uint8_t byte) const;

  std::optional<Node *> GetNextChild(ART &art, uint8_t &byte) const;

  static void InsertChild(ART &art, Node &node, const uint8_t byte, const Node child);

  static void DeleteChild(ART &art, Node &node, Node &prefix, const uint8_t byte);

  void Merge(ART &art, Node &other);

  bool ResolvePrefixes(ART &art, Node &other);

  bool MergeInternal(ART &art, Node &other);

  static void MergePrefixesDiffer(ART &art, reference<Node> &l_node, reference<Node> &r_node,
                                  idx_t &mismatched_position);

  static bool MergePrefixContainsOtherPrefix(ART &art, reference<Node> &l_node, reference<Node> &r_node,
                                             idx_t &mismatch_position);

  // duckdb api design is bad too...
  void ReplaceChild(const ART &art, const uint8_t byte, const Node child);

  BlockPointer Serialize(ART &art, Serializer &serializer);

  void Deserialize(ART &art);

  void ToGraph(ART &art, std::ofstream &out, idx_t &id, std::string parent_id = "");

 private:
  uint64_t data;
};
}  // namespace part

#endif  // PART_NODE_H
