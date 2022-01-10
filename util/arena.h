// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_ARENA_H_
#define STORAGE_LEVELDB_UTIL_ARENA_H_

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

namespace leveldb {
//内存池。对于基础组件来说只要是频繁创建和销毁内存的地方就有
//内存池，leveldb也有，目的是避免频繁创建和销毁内存。
//每分配一块内存的最小单位是4K
class Arena {
 public:
  Arena();

  Arena(const Arena&) = delete;
  Arena& operator=(const Arena&) = delete;
  //析构函数，当Memtable结束生命周期时，析构函数会释放内存池
  ~Arena();

  // Return a pointer to a newly allocated memory block of "bytes" bytes.
  char* Allocate(size_t bytes);

  // Allocate memory with the normal alignment guarantees provided by malloc.
  char* AllocateAligned(size_t bytes);

  // Returns an estimate of the total memory usage of data allocated
  // by the arena.
  size_t MemoryUsage() const {
    return memory_usage_.load(std::memory_order_relaxed);
  }

 private:
  //按需的方式分配内存，可能会造成内存的浪费
  char* AllocateFallback(size_t bytes);
  //对其的方式分配内存，CPU在寻址的过程中是按偶数的方式寻址的，奇数要寻址两次。从而提升寻址性能
  char* AllocateNewBlock(size_t block_bytes);

  // Allocation state
  // 当前已使用内存的指针,内存池中已经用到多少内存的位置
  char* alloc_ptr_;
  // 当前内存池剩余多少字节数
  size_t alloc_bytes_remaining_;

  // Array of new[] allocated memory blocks
  // 实际分配的内存池
  std::vector<char*> blocks_;

  // Total memory usage of the arena.
  // 已使用内存的使用情况，统计功能
  // TODO(costan): This member is accessed via atomics, but the others are
  //               accessed without any locking. Is this OK?
  std::atomic<size_t> memory_usage_;
};

inline char* Arena::Allocate(size_t bytes) {
  // The semantics of what to return are a bit messy if we allow
  // 0-byte allocations, so we disallow them here (we don't need
  // them for our internal use).
  assert(bytes > 0);
  //先判断当前容量是否够用
  //够用就直接分配
  if (bytes <= alloc_bytes_remaining_) {
    char* result = alloc_ptr_;
    alloc_ptr_ += bytes;
    alloc_bytes_remaining_ -= bytes;
    return result;
  }
  //不够用就再申请block
  return AllocateFallback(bytes);
}

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_ARENA_H_
