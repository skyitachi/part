//
// Created by skyitachi on 23-12-13.
//

#ifndef PART_SERIALIZER_H
#define PART_SERIALIZER_H

#include <fcntl.h>
#include <fmt/core.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "allocator.h"
#include "block.h"
#include "types.h"

namespace part {

class Serializer {
 private:
  uint64_t version = 0L;

 public:
  virtual ~Serializer() {}

  void SetVersion(uint64_t v) { this->version = v; }

  virtual void WriteData(const_data_ptr_t buffer, idx_t write_size) = 0;

  template <class T>
  void Write(T element) {
    static_assert(std::is_trivially_destructible<T>(), "write element must be trivially destructible");
    WriteData(reinterpret_cast<const_data_ptr_t>(&element), sizeof(T));
  }

  virtual BlockPointer GetBlockPointer() = 0;

  virtual void Flush() = 0;
};

class Deserializer {
 private:
  uint64_t version = 0L;

 public:
  virtual ~Deserializer() {}

  virtual BlockPointer GetBlockPointer() = 0;

  virtual void ReadData(data_ptr_t buffer, idx_t read_size) = 0;

  template <class T>
  T Read() {
    T value;
    ReadData(reinterpret_cast<data_ptr_t>(&value), sizeof(T));
    return value;
  }
};

class SequentialSerializer : public Serializer {
 public:
  explicit SequentialSerializer(std::string path, Allocator &allocator = Allocator::DefaultAllocator())
      : block_id_(0), offset_(0), capacity_(BLOCK_SIZE), allocator(allocator), buf_offset_(0) {
    fd_ = ::open(path.c_str(), O_CREAT | O_RDWR, 0644);
    if (fd_ == -1) {
      throw std::invalid_argument(fmt::format("cannot open file {}, due to {}", path, strerror(errno)));
    }
    buffer_ = allocator.AllocateData(capacity_);
  }

  virtual void WriteData(const_data_ptr_t buffer, idx_t write_size) override;

  virtual BlockPointer GetBlockPointer() override;

  Allocator &allocator;

  ~SequentialSerializer() {
    if (fd_ != -1) {
      ::close(fd_);
    }
  }

  virtual void Flush() override;

 private:
  std::string path_;
  idx_t block_id_;
  uint32_t offset_;
  int fd_;
  uint32_t capacity_;
  data_ptr_t buffer_;
  uint32_t buf_offset_;

  idx_t write(const_data_ptr_t data, idx_t size);
};

// not Sequential read
class BlockDeserializer : public Deserializer {
 public:
  explicit BlockDeserializer(const std::string &path, const BlockPointer &pointer,
                             Allocator &allocator = Allocator::DefaultAllocator())
      : allocator(allocator) {
    fd_ = ::open(path.c_str(), O_RDONLY, 0644);
    if (fd_ == -1) {
      throw std::invalid_argument(fmt::format("cannot open file {}, error: {}", path, strerror(errno)));
    }

    block_id_ = pointer.block_id;
    offset_ = pointer.offset;
  }

  BlockDeserializer(int fd, const BlockPointer &pointer, Allocator &allocator = Allocator::DefaultAllocator())
      : fd_(fd), block_id_(pointer.block_id), offset_(pointer.offset), allocator(allocator) {}

  virtual void ReadData(data_ptr_t buffer, idx_t read_size) override;

  virtual BlockPointer GetBlockPointer() override;

  Allocator &allocator;

 private:
  idx_t block_id_;
  int fd_;
  uint32_t offset_;
};
}  // namespace part
#endif  // PART_SERIALIZER_H
