//
// Created by Shiping Yao on 2023/12/20.
//
#include <iostream>

#include <art_key.h>
#include "art.h"

#include "util.h"

using namespace part;

int main() {
  Random random;

  auto kv_pairs = random.GenKvPairs(100000);

  std::cout << kv_pairs.size() << std::endl;

  auto kv_pairs2 = random.GenKvPairs(100000);

}