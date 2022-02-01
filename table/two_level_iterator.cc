// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/two_level_iterator.h"

#include "leveldb/table.h"
#include "table/block.h"
#include "table/format.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {
/*
二级迭代器的存在便于对SSTable的DataBlock数据进行访问。
对于SSTable来说：
Level_1是Index Block迭代器；
Level_2是指向Data Block迭代器。
通过这种设计，可以对SSTable的所有key进行向前扫描，向后扫描这种批量查询工作。
*/


//设置二级迭代器时传入的回调函数
typedef Iterator* (*BlockFunction)(void*, const ReadOptions&, const Slice&);

class TwoLevelIterator : public Iterator {
 public:
  //构造。对于SSTable来说：
  //1、index_iter是指向index block的迭代器；
  //2、block_function是Table::BlockReader,即读取一个block;
  //3、arg是指向一个SSTable;
  //4、options 读选项。
  TwoLevelIterator(Iterator* index_iter, BlockFunction block_function,
                   void* arg, const ReadOptions& options);

  ~TwoLevelIterator() override;
  // 针对一级迭代器，target 是一个 index block元素，
  // 这里就是seek到 index block对应元素位置
  void Seek(const Slice& target) override;
  //针对一级迭代器操作
  void SeekToFirst() override;
  //针对一级迭代器操作
  void SeekToLast() override;
  //针对二级迭代器,DataBlock中的下一个Entry
  void Next() override;
  //针对二级迭代器,DataBlock中的前一个Entry
  void Prev() override;
  //针对二级迭代器,指向DataBlock的迭代器是否有效
  bool Valid() const override { return data_iter_.Valid(); }
  //针对二级迭代器,DataBlock中的一个Entry的Key
  Slice key() const override {
    assert(Valid());
    return data_iter_.key();
  }
  //针对二级迭代器,DataBlock中的一个Entry的Value
  Slice value() const override {
    assert(Valid());
    return data_iter_.value();
  }
  //当前二级迭代器的操作状态
  Status status() const override {
    // It'd be nice if status() returned a const Status& instead of a Status
    if (!index_iter_.status().ok()) {
      return index_iter_.status();
    } else if (data_iter_.iter() != nullptr && !data_iter_.status().ok()) {
      return data_iter_.status();
    } else {
      return status_;
    }
  }

 private:
  //保存错误状态，如果最近一次状态是非ok状态，
  //则不保存
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) status_ = s;
  }
  //跳过当前空的DataBlock,转到下一个DataBlock
  void SkipEmptyDataBlocksForward();
  //跳过当前空的DataBlock,转到前一个DataBlock
  void SkipEmptyDataBlocksBackward();
  //设置二级迭代器data_iter
  void SetDataIterator(Iterator* data_iter);
  //初始化DataBlock的二级迭代器
  void InitDataBlock();

  BlockFunction block_function_;
  void* arg_;
  const ReadOptions options_;
  Status status_;
  //一级迭代器，对于SSTable来说就是指向index block
  IteratorWrapper index_iter_;
  //二级迭代器，对于SSTable来说就是指向DataBlock
  IteratorWrapper data_iter_;  // May be nullptr
  // If data_iter_ is non-null, then "data_block_handle_" holds the
  // "index_value" passed to block_function_ to create the data_iter_.
  std::string data_block_handle_;
};
//构造二级迭代器。
//一级迭代器赋值为index_iner,
//二级迭代器赋值为nullptr。
TwoLevelIterator::TwoLevelIterator(Iterator* index_iter,
                                   BlockFunction block_function, void* arg,
                                   const ReadOptions& options)
    : block_function_(block_function),
      arg_(arg),
      options_(options),
      index_iter_(index_iter),
      data_iter_(nullptr) {}

TwoLevelIterator::~TwoLevelIterator() = default;
// target是我们输入的key,  流程同Seek()
void TwoLevelIterator::Seek(const Slice& target) {
  //一级索引找到target在从哪个sst文件开始查找
  index_iter_.Seek(target);
  //初始化data_block，创建第二级迭代器
  InitDataBlock();
  //二级索引中查找数据，在一级索引中已经创建了二级索引，直接在二级索引查找key就可以了
  if (data_iter_.iter() != nullptr) data_iter_.Seek(target);
  //跳过背后为空的data_block索引
  SkipEmptyDataBlocksForward();
}
//流程同Seek()
void TwoLevelIterator::SeekToFirst() {
  index_iter_.SeekToFirst();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  SkipEmptyDataBlocksForward();
}

void TwoLevelIterator::SeekToLast() {
  index_iter_.SeekToLast();
  InitDataBlock();
  if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  SkipEmptyDataBlocksBackward();
}
//二级迭代器的下一个元素，
//对SSTable来说就是DataBlock中的下一个元素。
//需要检查跳过空的DataBlock。
void TwoLevelIterator::Next() {
  assert(Valid());
  data_iter_.Next();
  SkipEmptyDataBlocksForward();
}
//二级迭代器的前一个元素，
//对SSTable来说就是DataBlock中的前一个元素。
//需要检查跳过空的DataBlock。
void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_.Prev();
  SkipEmptyDataBlocksBackward();
}
//针对二级迭代器。
//如果当前二级迭代器指向为空或者非法;
//那就向后跳到下一个非空的DataBlock。
void TwoLevelIterator::SkipEmptyDataBlocksForward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // 移动到下一个block.Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    // 移动到下一个 一级索引index_iter 所对应的二级索引。
    index_iter_.Next();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToFirst();
  }
}
//针对二级迭代器。
//如果当前二级迭代器指向为空或者非法;
//那就向前跳到下一个非空的DataBlock。
void TwoLevelIterator::SkipEmptyDataBlocksBackward() {
  while (data_iter_.iter() == nullptr || !data_iter_.Valid()) {
    // Move to next block
    if (!index_iter_.Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    index_iter_.Prev();
    InitDataBlock();
    if (data_iter_.iter() != nullptr) data_iter_.SeekToLast();
  }
}
//设置二级迭代器
void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  if (data_iter_.iter() != nullptr) SaveError(data_iter_.status());
  data_iter_.Set(data_iter);
}
//初始化二级迭代器指向。
//对SSTable来说就是获取DataBlock的迭代器赋值给二级迭代器。
void TwoLevelIterator::InitDataBlock() {
  // 最外层都显示无效了，内部也直接设置无效。
  if (!index_iter_.Valid()) {
    SetDataIterator(nullptr);
  } else {
    // 否则取出内部的值。
    // LevelFileNumIterator对应的value是文件编号和文件的大小
    Slice handle = index_iter_.value();
    // 第一次为null，走下面的分支，如果一级索引对应下的二级索引已经构建，那就不需要再构建了
    if (data_iter_.iter() != nullptr &&
        handle.compare(data_block_handle_) == 0) {
      // data_iter_ is already constructed with this iterator, so
      // no need to change anything
    } else {
      //构建二级迭代。 回调函数是 table_cache->NewIterator。
      Iterator* iter = (*block_function_)(arg_, options_, handle);
      data_block_handle_.assign(handle.data(), handle.size());
      //设置二级索引的值
      SetDataIterator(iter);
    }
  }
}

}  // namespace
//第一级是 index_iter
//第二级是 block_function 回调函数 
//构造结构
Iterator* NewTwoLevelIterator(Iterator* index_iter,
                              BlockFunction block_function, void* arg,
                              const ReadOptions& options) {
  return new TwoLevelIterator(index_iter, block_function, arg, options);
}

}  // namespace leveldb
