//
// Created by skyitachi on 24-1-25.
//

#ifndef PART_CONCURRENT_NODE_H
#define PART_CONCURRENT_NODE_H
#include <fmt/core.h>

#include <atomic>

#include "node.h"

namespace part {
class ConcurrentART;
class Prefix;

// NOTE: 1. node cannot be serialized when accessed, different from Node (currently for simplicity)
class ConcurrentNode : public Node {
 public:
  ConcurrentNode(const uint32_t buffer_id, const uint32_t offset) : lock_(0) {
    SetPtr(buffer_id, offset);
    // important
  }

  explicit ConcurrentNode(Deserializer &reader) : Node(reader) {}

  ConcurrentNode(ConcurrentART &art, Deserializer &reader) : Node(reader) {
    if (IsSerialized() && IsSet()) {
      Deserialize(art);
    }
  }

  ConcurrentNode() = default;

  // NOTE: copy data
  ConcurrentNode(const ConcurrentNode &other) : lock_(0) { SetData(other.GetData()); }

  // just used in Serialize and Deserialize
  ConcurrentNode &operator=(const ConcurrentNode &other) {
    SetData(other.GetData());
    return *this;
  }

  // keep lock_ states
  void Update(const ConcurrentNode &&other) {
    Reset();
    SetPtr(other.GetBufferId(), other.GetOffset());
  }

  void Update(ConcurrentNode *other) {
    Reset();
    SetPtr(other->GetBufferId(), other->GetOffset());
  }

  static FixedSizeAllocator &GetAllocator(const ConcurrentART &art, NType type);

  void ToGraph(ConcurrentART &art, std::ofstream &out, idx_t &id, const std::string& parent_id = "");

  void Lock();
  void Unlock();
  void RLock();
  void RUnlock();
  void Downgrade();
  void Upgrade();
  int64_t Readers();

  bool RLocked() const;
  bool Locked() const;

  inline void ResetAll() {
    Reset();
    lock_ = 0;
  }

  static void Free(ConcurrentART &art, ConcurrentNode *node);

  std::optional<ConcurrentNode *> GetChild(ConcurrentART &art, uint8_t byte) const;

  static void InsertChild(ConcurrentART &art, ConcurrentNode *node, uint8_t byte, ConcurrentNode *child);

  BlockPointer Serialize(ConcurrentART &art, Serializer &serializer);

  void Deserialize(ConcurrentART &art);

  bool Deserialize(ConcurrentART &art, Deserializer &reader);

  void DeserializeInternal(ConcurrentART &art, Deserializer &reader);

  void FastDeserialize(ConcurrentART &art);

  void Merge(ConcurrentART &cart, ART &art, Node &other);

  void MergeUpdate(ConcurrentART &cart, ART &art, Node &other);

  bool ResolvePrefixes(ConcurrentART &cart, ART &art, Node &other);

  bool MergeInternal(ConcurrentART &cart, ART &art, Node &other);

  bool MergePrefix(ConcurrentART &cart, ART &art, Node &other);

  static bool TraversePrefix(ConcurrentART &cart, ART &art, ConcurrentNode *node, reference<Node> &prefix, idx_t &pos);

  static void MergePrefixesDiffer(ConcurrentART &cart, ART &art, ConcurrentNode *l_node, reference<Node> &r_node,
                                  idx_t &left_pos, idx_t &right_pos);
  // Note: prefix node merge none prefix node
  static void MergeNonePrefixByPrefix(ConcurrentART &cart, ART &art, ConcurrentNode *l_node, Node &other,
                                      idx_t l_pos = 0);

  static void ConvertToNode(ConcurrentART &cart, ART &art, ConcurrentNode *src, Node &dst, idx_t pos = 0);

  static void InsertForMerge(ConcurrentART &cart, ART &art, ConcurrentNode *node, Prefix &other, idx_t pos);

 private:
  // NOTE: 如何传递锁状态是个问题
  std::atomic<uint64_t> lock_ = {0};
};

}  // namespace part
#endif  // PART_CONCURRENT_NODE_H
