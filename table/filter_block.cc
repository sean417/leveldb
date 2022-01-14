// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"
//FilterBlock用于提高sstable的读取效率，目前leveldb中使用的Filter算法是布隆过滤器。
namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
/*
  1.FilterBlock受data block的更新而更新，当data block开启新的block的时候，
  filter block也会开启新的，内部会遵循2kb一个filter来进行构建。
  2.filter这部分数据是 最后批量写到sst的。
*/


// 每2k的数据生成一个filter
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  //2kb一个filter
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  //是否需要构建新的布隆过滤器
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

Slice FilterBlockBuilder::Finish() {
  if (!start_.empty()) {
    GenerateFilter();
  }

  // Append array of per-filter offsets 每个过滤器偏移量数组大小
  const uint32_t array_offset = result_.size();
  //保存每个filter的偏移量
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }
  //保存所有filter的size
  PutFixed32(&result_, array_offset);
  //filter内部拆分的阈值
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result 保存kFilterBaseLg,默认11。占一个字节
  return Slice(result_);
}
//生成布隆过滤器的过程
void FilterBlockBuilder::GenerateFilter() {
  //key个数，当前新添加的key的个数，如果没有，则记录上一次的result大小。
  const size_t num_keys = start_.size();
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  start_.push_back(keys_.size());  // Simplify length computation
  tmp_keys_.resize(num_keys);
  //这里是根据start_和keys两个数据结构，恢复出来真实的 key
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  // 构建布隆过滤器
  // 针对当前已有的数据，构建布隆过滤器，注意每次都是新加的操作
  filter_offsets_.push_back(result_.size());
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);//

  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}
//读取流程
FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
      //整个filter block的大小,即多少个byte
  size_t n = contents.size();
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n - 1];
  //解析filter offset总长度的数据 ，解析出offset_array,因为它只会取4个字节，取出来的数据就是整个filter offset的长度
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  if (last_word > n - 5) return;
  data_ = contents.data();
  // offset_计算对应的就是每2k所对应的filter的偏移量和大小，也就是filter offset。
  offset_ = data_ + last_word;
  //num_表示总共有几个filter offset,也就是有多少个filter
  num_ = (n - 5 - last_word) / 4;
}
//KeyMayMatch对应的是反解析出对应的数据，然后进行bf判断。
bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    //filter offset固定是4个字节，所以这里直接加index*4,跳转到该offset的起始位置。
    uint32_t start = DecodeFixed32(offset_ + index * 4);
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      // 反解出对应的bf数据，over
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
