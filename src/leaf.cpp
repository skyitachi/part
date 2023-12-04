//
// Created by Shiping Yao on 2023/12/4.
//

#include "leaf.h"

namespace part {

void Leaf::New(Node& node, const idx_t value) {
  node.Reset();
  node.SetType((uint8_t)NType::LEAF_INLINED);
}

}