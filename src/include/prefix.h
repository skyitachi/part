//
// Created by Shiping Yao on 2023/12/4.
//

#ifndef PART_PREFIX_H
#define PART_PREFIX_H

#include "node.h"
#include "art.h"

namespace part {
class ARTKey;

class Prefix {
public:
  uint8_t data[Node::PREFIX_SIZE + 1];

  Node ptr;

public:
  static Prefix &New(ART &art, Node &node);
  static void New(ART& art, std::reference_wrapper<Node>& node, const ARTKey& key, const uint32_t depth, uint32_t count);
};
}
#endif //PART_PREFIX_H
