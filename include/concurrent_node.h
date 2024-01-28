//
// Created by skyitachi on 24-1-25.
//

#ifndef PART_CONCURRENT_NODE_H
#define PART_CONCURRENT_NODE_H
#include <atomic>

#include "node.h"

namespace part {
class ConcurrentART;

// NOTE: 1. node cannot be serialized when accessed, different from Node (currently for simplicity)
class ConcurrentNode : public Node {
 public:
  ConcurrentNode(const uint32_t buffer_id, const uint32_t offset) { SetPtr(buffer_id, offset); }

  ConcurrentNode(Deserializer &reader) : Node(reader) {}

  ConcurrentNode(ART &art, Deserializer &reader) : Node(art, reader) {}

  ConcurrentNode(){};

  static FixedSizeAllocator &GetAllocator(const ConcurrentART &art, NType type);

  void Lock();
  void Unlock();
  void RLock();
  void RUnlock();
  void Downgrade();
  void Upgrade();

  bool RLocked() const;
  bool Locked() const;

 private:
  std::atomic<uint64_t> lock_;
};

}  // namespace part
#endif  // PART_CONCURRENT_NODE_H
