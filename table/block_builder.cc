// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.
// BlockBuilder主要是用于index_block/data_block/meta_index_block的构建。

#include "table/block_builder.h"

#include <algorithm>
#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/options.h"
#include "util/coding.h"

namespace leveldb {

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options), restarts_(), counter_(0), finished_(false) {
  assert(options->block_restart_interval >= 1);
  restarts_.push_back(0);  // First restart point is at offset 0
}

void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);  // First restart point is at offset 0
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

size_t BlockBuilder::CurrentSizeEstimate() const {
  return (buffer_.size() +                       // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +  // Restart array
          sizeof(uint32_t));                     // Restart array length
}

Slice BlockBuilder::Finish() {
  // Append restart array 保存重启点到磁盘
  for (size_t i = 0; i < restarts_.size(); i++) {
    //保存每个重启点
    PutFixed32(&buffer_, restarts_[i]);
  }
  //保存重启点的个数
  PutFixed32(&buffer_, restarts_.size());
  finished_ = true;
  return Slice(buffer_);
}
//共享前缀存储法
void BlockBuilder::Add(const Slice& key, const Slice& value) {
  //表示上一个key
  Slice last_key_piece(last_key_);
  assert(!finished_);
  //block_restart_internal控制着重启点之间的距离
  assert(counter_ <= options_->block_restart_interval);
  //这里验证一下，首先有数据，其次当前的key肯定比上一个key大，因为它是有序的
  assert(buffer_.empty()  // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);
  size_t shared = 0;
  //block_restart_internal控制着重启点之间的距离。
  //如果不到block_restart_interval的话，就继续计算
  if (counter_ < options_->block_restart_interval) {
    // See how much sharing to do with previous string
    // 计算和前一个key共享的部分。
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      //计算当前key与上一个key之间共享的部分
      shared++;
    }
  } else {
    // Restart compression  保存重启点的位置，对后面的影响是，保证完整的key了就不能用前缀匹配了
    // 保存buffer的size()，恢复的时候我们就可以读取每个重启点保存的size，从而找到offset,把数据反序列化出来
    restarts_.push_back(buffer_.size());
    counter_ = 0;
  }
  const size_t non_shared = key.size() - shared;

  // Add "<shared><non_shared><value_size>" to buffer_
  PutVarint32(&buffer_, shared);//共享前缀长度
  PutVarint32(&buffer_, non_shared);//非共享长度
  PutVarint32(&buffer_, value.size());//value的大小

  // Add string delta to buffer_ followed by value
  buffer_.append(key.data() + shared, non_shared);//非共享的key的数据
  buffer_.append(value.data(), value.size());//value的大小

  // Update state 更新
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared);
  assert(Slice(last_key_) == key);
  counter_++;
}

}  // namespace leveldb
