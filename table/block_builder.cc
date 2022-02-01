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
// 构建一个组织DataBlock格式方法类
BlockBuilder::BlockBuilder(const Options* options)
    : options_(options), restarts_(), counter_(0), finished_(false) {
  assert(options->block_restart_interval >= 1);
  restarts_.push_back(0);  // First restart point is at offset 0
}
// 重置，主要把用于生产DataBlock的一些临时变量重置
void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);  // First restart point is at offset 0
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}
/*
  获取DataBlock的大致大小，DataBlock由三部分组成：
  1、存储KV数据的buffer_;
  2、存储重启点的数组restart_[]，每个重启点类型占4Byte；
  3、表示重启点个数的num_restarts_，占4Byte。

*/
size_t BlockBuilder::CurrentSizeEstimate() const {
  return (buffer_.size() +                       // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +  // Restart array
          sizeof(uint32_t));                     // Restart array length
}
/*
  表示完成一个 DataBlock，这里就是在已存有KV的buffer_尾部追加
  重启点restart[]和表示重启点个数的restart.size()。
*/
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
//这里就是往DataBlock中添加一个k-v。上层已保证添加的Key都是有序而且是递增的。
void BlockBuilder::Add(const Slice& key, const Slice& value) {
  //表示上一个key
  Slice last_key_piece(last_key_);
  assert(!finished_);
  /*block_restart_internal控制着重启点之间的距离
  这里要说下重启点间隔参数options_->block_restart_interval，
  我们知道DataBlock为压缩空间，先将一个Entry的完整key保存下来，
  后续先添加的Entry的key只保存与前一个key不同的部分。而多少个key之后
  再保存一个完整的key，则就是靠重启点间隔参数来控制的。
  两个重启点间隔直接的KV个数conter_肯定是 <= 重启点间隔
  */
  assert(counter_ <= options_->block_restart_interval);
  // 这里验证一下，首先有数据，其次当前的key肯定比上一个key大，因为它是有序的
  // 第一次进来，或者后续进来的key都是大于前一个key的
  assert(buffer_.empty()  // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);
  size_t shared = 0;
  /* 
  block_restart_internal 控制着重启点之间的距离。如果不到 block_restart_interval 的话，就继续计算
  若存储KV个数还未达到重启点个数要求,则找出新Key与上一个Key的相同部分个数,用shared来统计
  */
  if (counter_ < options_->block_restart_interval) {
    // See how much sharing to do with previous string
    // 计算和前一个key共享的部分。
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      //计算当前key与上一个key之间共享的部分
      shared++;
    }
  } else {
    /* Restart compression  保存重启点的位置，对后面的影响是，保证完整的key了就不能用前缀匹配了
     保存buffer的size()，恢复的时候我们就可以读取每个重启点保存的size，从而找到offset,把数据反序列化出来
     表示设置一个重启点，其实就是将buffer_中的接下来要存的
     完整key的offset写入到restart_中，BlockBuilder类构造或者
     调用Reset()方法时，已默认往restarts_中存入了重启点偏移位0。
    */
    restarts_.push_back(buffer_.size());
    counter_ = 0;
  }
  // 这里求出前后两个key不相同的部分，如果是新设置的重启点，shared值为0，也就是说保存一个完整的key。
  const size_t non_shared = key.size() - shared;

  // Add "<shared><non_shared><value_size>" to buffer_
  // 下面流程很好懂了，写各个字段值
  PutVarint32(&buffer_, shared);//共享前缀长度
  PutVarint32(&buffer_, non_shared);//非共享长度
  PutVarint32(&buffer_, value.size());//value的大小

  // Add string delta to buffer_ followed by value
  buffer_.append(key.data() + shared, non_shared);//非共享的key的数据
  buffer_.append(value.data(), value.size());//value的大小

  // Update state 更新
  /*
    将当前新写入的 key 保存为 last_key，
    同将 counter_++，用于重启点间隔判断。
  */
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared);
  assert(Slice(last_key_) == key);
  counter_++;
}

}  // namespace leveldb
