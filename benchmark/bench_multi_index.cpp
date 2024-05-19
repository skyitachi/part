//
// Created by skyitachi on 24-5-11.
//

#include <iostream>

//#include <art.h>
extern "C" {
  #include <hamt/hamt.h>
  #include <hamt/murmur3.h>
}

static uint32_t my_keyhash_int(const void *key, const size_t gen)
{
  uint32_t hash = murmur3_32((uint8_t *)key, sizeof(int), gen);
  return hash;
}

static int my_keycmp_int(const void *lhs, const void *rhs)
{
  /* expects lhs and rhs to be pointers to 0-terminated strings */
  const int *l = (const int *)lhs;
  const int *r = (const int *)rhs;

  if (*l > *r)
    return 1;
  return *l == *r ? 0 : -1;
}

int main() {
  {
    hamt *t = hamt_create(my_keyhash_int, my_keycmp_int, &hamt_allocator_default);
    int32_t key1 = 100;
    int32_t value1 = 1;
    hamt_set(t, &key1, &value1);

    auto *v = hamt_get(t, &key1);
    if (v) {
      std::cout << "found value: " << *(int32_t*)v << std::endl;
    }

    int32_t key2 = 200;
    v = hamt_get(t, &key2);
    if (!v) {
      std::cout << "count found key: " << key2 << std::endl;
    }
    hamt_delete(t);
  }


  return 0;
}
