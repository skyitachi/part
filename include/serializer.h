//
// Created by skyitachi on 23-12-13.
//

#ifndef PART_SERIALIZER_H
#define PART_SERIALIZER_H
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>

#include <fmt/core.h>

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

class SequentialSerializer: public Serializer {
public:
    explicit SequentialSerializer(std::string path): block_id_(0), offset_(0), capacity_(4096) {
        fd_ = ::open(path.c_str(), O_CREAT | O_RDWR);
        if (fd_ == -1) {
            throw std::invalid_argument(fmt::format("cannot open file {}, due to {}", path, strerror(errno)));
        }
    }
    virtual void WriteData(const_data_ptr_t buffer, idx_t write_size) override;

    virtual BlockPointer GetBlockPointer() override;

    ~SequentialSerializer() {
        if (fd_ != -1) {
            close(fd_);
        }
    }

private:
    std::string path_;
    idx_t block_id_;
    uint32_t offset_;
    int fd_;
    uint32_t capacity_;
};
}
#endif //PART_SERIALIZER_H
