//
// Created by Shiping Yao on 2023/12/4.
//

#include "art.h"
#include "art_key.h"
#include "node.h"
#include "prefix.h"

namespace part {

ART::ART(const std::shared_ptr<std::vector<FixedSizeAllocator>> &allocators_ptr)
     : allocators(allocators_ptr), owns_data(false) {
  if (!allocators) {
    owns_data = true;
  }

  root = std::make_unique<Node>();

}

ART::~ART() {
  root->Reset();
}

void ART::insert(Node &node, const ARTKey &key, idx_t depth, const int64_t &value) {
  if (!node.IsSet()) {
    assert(depth <= key.len);
    std::reference_wrapper<Node> ref_node(node);
    Prefix::New(*this, ref_node, key, depth, value);
    return;
  }

}
}
