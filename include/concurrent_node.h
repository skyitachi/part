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
  ConcurrentNode(const uint32_t buffer_id, const uint32_t offset) {
    SetPtr(buffer_id, offset);
    // important
    lock_ = 0;
  }

  ConcurrentNode(Deserializer &reader) : Node(reader) {}

  ConcurrentNode(ConcurrentART &art, Deserializer &reader) : Node(reader) {
    if (IsSerialized() && IsSet()) {
      Deserialize(art);
    }
  }

  ConcurrentNode(){};

  // NOTE: copy data
  ConcurrentNode(const ConcurrentNode &other) : lock_(0) { SetData(other.GetData()); }

  // just used in Serialize and Deserialize
  ConcurrentNode &operator=(const ConcurrentNode &other) {
    SetData(other.GetData());
    lock_ = 0;
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

  void ToGraph(ConcurrentART &art, std::ofstream &out, idx_t &id, std::string parent_id = "");

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

  void Merge(ConcurrentART &cart, ART &art, Node &other);

  void MergeUpdate(ConcurrentART &cart, ART &art, Node &other);

  bool ResolvePrefixes(ConcurrentART &cart, ART &art, Node &other);

  bool MergeInternal(ConcurrentART &cart, ART &art, Node &other);

  bool MergePrefix(ConcurrentART &cart, ART &art, Node &other);

  static bool TraversePrefix(ConcurrentART &cart, ART &art, ConcurrentNode *&node, Prefix &prefix, idx_t &pos);

 private:
  // NOTE: 如何传递锁状态是个问题
  std::atomic<uint64_t> lock_ = {0};
};

}  // namespace part
#endif  // PART_CONCURRENT_NODE_H
