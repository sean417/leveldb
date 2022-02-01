// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"

namespace leveldb {
// 默认块大小4096 Byte
static const int kBlockSize = 4096;
// 构造时初始化一些值，

Arena::Arena()
    : alloc_ptr_(nullptr), alloc_bytes_remaining_(0), memory_usage_(0) {}
//  析构时释放内存块
Arena::~Arena() {
  for (size_t i = 0; i < blocks_.size(); i++) {
    delete[] blocks_[i];
  }
}
/*
  1、申请的内存大于1024时，直接从内存申请返回，避免对现有剩下的内存的浪费。
  2、申请内存小于等于1024时，直接向系统申请4096内存大小
     并重新赋值alloc_ptr、alloc_bytes_remaining_，这样的话
     如果之前的block中还剩余内存，则直接浪费掉了。

*/
char* Arena::AllocateFallback(size_t bytes) {
  if (bytes > kBlockSize / 4) {
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    char* result = AllocateNewBlock(bytes);
    return result;
  }

  // We waste the remaining space in the current block.
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize;

  char* result = alloc_ptr_;
  alloc_ptr_ += bytes;
  alloc_bytes_remaining_ -= bytes;
  return result;
}
// 申请内存对齐的内存
char* Arena::AllocateAligned(size_t bytes) {
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
  // 对齐必须是2的倍数
  static_assert((align & (align - 1)) == 0,
                "Pointer size should be a power of 2");
  // 求出当前地址与对齐大小相与之后的值
  size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr_) & (align - 1);
  // 如果current_mod不是0，就求出与对齐大小的差值
  size_t slop = (current_mod == 0 ? 0 : align - current_mod);
  // 为了对齐，申请的字节大小加上需要偏移的大小。
  // 将对应的值进行赋值。
  size_t needed = bytes + slop;
  char* result;
  //补齐操作
  if (needed <= alloc_bytes_remaining_) {
    result = alloc_ptr_ + slop;
    alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
  } else {
    // AllocateFallback always returned aligned memory
    result = AllocateFallback(bytes);
  }
  // 最后检测下返回的地址是否对齐的。
  assert((reinterpret_cast<uintptr_t>(result) & (align - 1)) == 0);
  return result;
}
// new一个内存，并计数下来。
char* Arena::AllocateNewBlock(size_t block_bytes) {
  char* result = new char[block_bytes];
  blocks_.push_back(result);
  memory_usage_.fetch_add(block_bytes + sizeof(char*),
                          std::memory_order_relaxed);
  return result;
}

}  // namespace leveldb
