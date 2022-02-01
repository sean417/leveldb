// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.
/*
Filter Block中存的都是Data Block中的key（key是经过处理再存入到FilterBlock中），
其作用就是提高SSTable的读取效率。当查询一个key时，可以快速定位这个key是否在当前SSTable中，
其流程是当进行一个key查询时，先通过index block中的二分法确定key在哪个Data Block中，
取出这个Data Block的offset，凭借这个Offset和要查询的key去Filter Block中去查找确定，
若判断不存在，则无需对这个DataBlock进行数据查找。
*/
#ifndef STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
#define STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "leveldb/slice.h"
#include "util/hash.h"

namespace leveldb {
//过滤策略基类
class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock AddKey*)* Finish
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy*);

  FilterBlockBuilder(const FilterBlockBuilder&) = delete;
  FilterBlockBuilder& operator=(const FilterBlockBuilder&) = delete;
  // 是否要新起一个 Fileter
  void StartBlock(uint64_t block_offset);
  // 往 Filter 中添加 key
  void AddKey(const Slice& key);
  // 完成当前 Filter Block 的数据状态
  Slice Finish();

 private:
  //产生一个Filter
  void GenerateFilter();
  //过滤器策略：默认是布隆过滤器
  const FilterPolicy* policy_; 
  //追加保存用于产生Filter的key
  std::string keys_;             // Flattened key contents
  // 存储keys_中每个key的offset
  std::vector<size_t> start_;    // Starting index in keys_ of each key
  //1.布隆过滤器的二进制数据，构建的filter二进制数据。
  //2.每2k数据构建一个布隆过滤器,所以可能存在多个filter，并且都保存在result_里。
  std::string result_;           // Filter data computed so far 
  //1.每2k数据构建一个布隆过滤器,所以可能存在多个filter，而该变量记录的是filter的偏移量。
  //2.
  std::vector<Slice> tmp_keys_;  // policy_->CreateFilter() argument
  //保存每个Filter中当前Filter Block中的偏移位
  std::vector<uint32_t> filter_offsets_;
};
//读取FilterBlock的类
class FilterBlockReader {
 public:
  // REQUIRES: "contents" and *policy must stay live while *this is live.
  // 创建一个用于读取Filter Block的实体
  FilterBlockReader(const FilterPolicy* policy, const Slice& contents);
  // 判断当前Filter Block是否存在Key
  bool KeyMayMatch(uint64_t block_offset, const Slice& key);

 private:
  const FilterPolicy* policy_;//过滤器策略：默认是布隆过滤器
  const char* data_;    // Pointer to filter data (at block-start) 指向Filter Block的数据首地址
  const char* offset_;  // Pointer to beginning of offset array (at block-end) Filter Offset数组的起始偏移
  size_t num_;          // Number of entries in offset array Filter Offset个数
  size_t base_lg_;      // Encoding parameter (see kFilterBaseLg in .cc file) Filter Block结束标志，也用于表示每个Filter的大小
};

}  // namespace leveldb

#endif  // STORAGE_LEVELDB_TABLE_FILTER_BLOCK_H_
