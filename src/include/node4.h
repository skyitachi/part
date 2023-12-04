//
// Created by Shiping Yao on 2023/11/30.
//

#ifndef PART_NODE4_H
#define PART_NODE4_H
#include "node.h"

namespace part {

class Node4 {

public:
  //! Number of non-null children
  uint8_t count;
  //! Array containing all partial key bytes
  uint8_t key[Node::NODE_4_CAPACITY];
  //! ART node pointers to the child nodes
  Node children[Node::NODE_4_CAPACITY];
};
}

#endif //PART_NODE4_H
