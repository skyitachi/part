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
  ConcurrentNode(const ConcurrentNode &other) {
    SetData(other.GetData());
    lock_ = 0;
  }

  // just used in Serialize and Deserialize
  // TODO: copy data
  ConcurrentNode &operator=(const ConcurrentNode &other) {
    SetData(other.GetData());
    // NOTE: is this ok???
    //    lock_ = other.lock_.load();
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

 private:
  // NOTE: 如何传递锁状态是个问题
  std::atomic<uint64_t> lock_ = {0};
};

}  // namespace part
#endif  // PART_CONCURRENT_NODE_H
