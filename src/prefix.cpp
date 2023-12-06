//
// Created by Shiping Yao on 2023/12/4.
//

#include "prefix.h"
#include "art_key.h"

namespace part {

void Prefix::New(ART &art, std::reference_wrapper<Node> &node,
                 const ARTKey &key, const uint32_t depth, uint32_t count) {
  if (count == 0) {
    return;
  }
  idx_t copy_count = 0;

  while (count > 0) {
    node.get() = Node::GetAllocator(art, NType::PREFIX).New();
    node.get().SetType((uint8_t)NType::PREFIX);
    auto &prefix = Prefix::Get(art, node);

    auto this_count = std::min((uint32_t)Node::PREFIX_SIZE, count);
    prefix.data[Node::PREFIX_SIZE] = (uint8_t)this_count;
    memcpy(prefix.data, key.data + depth + copy_count, this_count);

    node = prefix.ptr;
    copy_count += this_count;
    count -= this_count;
  }
}

idx_t Prefix::Traverse(ART &art, std::reference_wrapper<Node> &prefix_node, const ARTKey &key, idx_t &depth) {
  assert(prefix_node.get().IsSet() && !prefix_node.get().IsSerialized());
  assert(prefix_node.get().GetType() == NType::PREFIX);

  while(prefix_node.get().GetType() == NType::PREFIX) {
    auto& prefix = Prefix::Get(art, prefix_node);
    for(idx_t i = 0; i < prefix.data[Node::PREFIX_SIZE]; i++) {
      if (prefix.data[i] != key[depth]) {
        return i;
      }
      depth++;
    }
    prefix_node = prefix.ptr;
    assert(prefix_node.get().IsSet());
    // TODO: serialized
  }

  return INVALID_INDEX;
}
} // namespace part
