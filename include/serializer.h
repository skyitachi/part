//
// Created by skyitachi on 23-12-13.
//

#ifndef PART_SERIALIZER_H
#define PART_SERIALIZER_H
#include "types.h"
#include "block.h"

namespace part {
class Serializer {

private:
  uint64_t version = 0L;

public:
  virtual ~Serializer() {
  }

  void SetVersion(uint64_t v) {
    this->version = v;
  }

  virtual void WriteData(const_data_ptr_t buffer, idx_t write_size) = 0;

  template <class T>
  void Write(T element) {
    static_assert(std::is_trivially_destructible<T>(), "write element must be trivially destructible");
    WriteData(reinterpret_cast<const_data_ptr_t>(&element), sizeof(T));
  }

  virtual BlockPointer GetBlockPointer() = 0;
};

class Deserializer {
private:
  uint64_t version = 0L;

public:
  virtual ~Deserializer() {
  }

  virtual void ReadData(data_ptr_t buffer, idx_t read_size) = 0;

  template <class T>
  T Read() {
    T value;
    ReadData(reinterpret_cast<data_ptr_t>(&value), sizeof(T));
    return value;
  }
};
}
#endif //PART_SERIALIZER_H
