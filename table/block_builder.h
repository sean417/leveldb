// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
#define STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_

#include <cstdint>
#include <vector>

#include "leveldb/slice.h"

namespace leveldb {

struct Options;
//用于index_block,data_block,meta_index_block的构建。
class BlockBuilder {
 public:
  explicit BlockBuilder(const Options* options);

  BlockBuilder(const BlockBuilder&) = delete;
  BlockBuilder& operator=(const BlockBuilder&) = delete;

  // Reset the contents as if the BlockBuilder was just constructed.
  // Block构建结束了，清空状态。
  void Reset();

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  void Add(const Slice& key, const Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  // 计算当前block的大小，如果超过了设定的大小，就要开启新的block。
  size_t CurrentSizeEstimate() const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const { return buffer_.empty(); }

 private:
  //设置重启点等数据
  const Options* options_;
  //序列化之后的数据
  std::string buffer_;              // Destination buffer
  /*
    内部重启点具体数值，需要注意的是这是偏移量，每个重启点的偏移量
    重启点主要用于防止数据损坏导致的雪崩效应和作为索引加速数据的搜索;
    data block 中的数据项依赖于其前面的数据，如果 data block 中某个位置的数据发生损坏，
    则其后的所有数据都无法正常解析出来，形成雪崩效应。因此，为了避免单个数据损坏导致整个 data block 的数据不可用，
    leveldb 中每隔若干个数据项会强制取消共享 key 机制，这些位置的数据项会存储完整的 key/value 信息，
    这些位置也会保存在 data block 中，称为重启点。
    Restart point 的另一个作用是作为 Data Entry 的索引，从而在 Data Entry 中搜索指定 key 的时候可以使用二分搜索（binary search）加速搜索。
  */
  std::vector<uint32_t> restarts_;  // Restart points
  //重启点恢复的key数量，比如，设置10个key作为重启点的恢复
  int counter_;                     // Number of entries emitted since restart,如10个key做重启点的恢复
  bool finished_;                   // Has Finish() been called?
  std::string last_key_;
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_BLOCK_BUILDER_H_
