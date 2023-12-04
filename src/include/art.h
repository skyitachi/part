//
// Created by Shiping Yao on 2023/12/1.
//

#ifndef PART_ART_H
#define PART_ART_H
#include <memory>

namespace part {
class Node;

class ART {
public:
  std::unique_ptr<Node> root;
};

} // namespace part

#endif // PART_ART_H
