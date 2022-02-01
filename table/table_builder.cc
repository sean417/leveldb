// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        pending_index_entry(false) {
        //block_restart_interval = 1: index block数据少不需要任何压缩，
        //所以block_restart_interval = 1
    index_block_options.block_restart_interval = 1;
  }
  //Data Block写选项
  Options options;
  //index Block写选项，主要参数是block_restart_interval，
  //主要是多久写一个Data Block重启点。
  Options index_block_options;
  //<!写文件操作>
  WritableFile* file;
  uint64_t offset;
  Status status;
  BlockBuilder data_block;
  BlockBuilder index_block;
  // <!Index Block中的key>
  std::string last_key;
  //<!整个SSTable的KV个数>
  int64_t num_entries;
  bool closed;  // Either Finish() or Abandon() has been called.
  FilterBlockBuilder* filter_block;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  // true表示当前data_block是空的，需要往indexBlock中写入一条记录
  bool pending_index_entry;
  // 存储offset和size，用于写index block
  BlockHandle pending_handle;  // Handle to add to index block
  // 临时存储压缩数据
  std::string compressed_output;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    //sst文件才开始，所以偏移量是0
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}
/*
  修改下Option，影响到了IndexBlock，注意：
	如果有Optinon中有新增的参数选项，在已启动SSTable创建之后，
	不能在修改了。
	下文中，检测到Key的比较方式发生变化，则直接报错，
	因为Key的排序规则都变了，那之前排序的数据则是异常的了。
*/
Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

//数据写入流程：例如把immemtable（不可变的内存数据表）中的数据dump下来
void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  /*
    如果当前num_entries > 0,表示已存储了key。
    上层传过来的key已保证从小到达的顺序，
    所以新加入的key肯定大于已存在key数据的最后一个key。

  */
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }
  //表示上一个数据block刚刷新到磁盘。说明这是新的block的第一个record,要保存到index_block里。
  //而flush()会把pending_index_entry改为true，这样flush()后的新block的第一个record的处理是
  //构建上一个block的index entry的时机
  /*
    pending_index_entry为ture则构建索引
    pending_index_entry为true表示需要往index_block中写入一条DataBlock记录，
    数据格式就是| key | offset | size|。一般在写入了一个DataBlock
    之后，后续来了新key就要写一条DataBlock记录。
  */
  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    //找到最短距离最近的key存储下来，所以不一定是原始的key
    /*
      新写入到IndexBlock中的记录，对Key的要求是
	    大于等于当前DataBlock数据块的最大，同时要小于接下来要写入的DataBlock数据块的最小Key；
	    而当前这个key就是新的接下来要写入的DataBlock第一个key，也是其最小key。
	    这个通过FindShortestSeparator()方法找到满足上述条件的key。
    */
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    // 将上一个即当前已存在的DataBlock的Offset和Size，Var编码到handle_encoding
    std::string handle_encoding;
    //作为index的部分。
    r->pending_handle.EncodeTo(&handle_encoding);
    //保存到index block里。构建上一个block的index entry的时机,所以用last_key
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    // 表示接下来不用再往IndexBlock中写入一条记录了
    r->pending_index_entry = false;
  }
  //如果开启了过滤策略，则会有filter_block，则添加用于过滤的key.data block写一个key,filter block也要写一个key。
  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }
  //更新下最新的最后一个key
  r->last_key.assign(key.data(), key.size());
  // KV次数累加
  r->num_entries++;
  //将key和value写入data block中
  r->data_block.Add(key, value);
  // 每个datablock有4k的限制 当DataBlock达到指定大小block_size，一般是4KB，用户可设置。
  // 就要将DataBlock数据落地到磁盘，即SSTable文件中。
  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  //如果超过4k的限制就要把数据刷新到磁盘，并且开启新的block
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}
// 这个方法主要是将数据刷到磁盘
void TableBuilder::Flush() {
  // 开始是一些条件检测
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  /*
  pending_index_entry应为false，
  可以说明之前已一一对应写了一个
  DataBlock对应的记录到IndexBlock中
  */
  assert(!r->pending_index_entry);
  //写block。内部实现回刷数据
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    // 下次添加新的kv时，要对这次的block创建index entry。
    // 如果写成功了，将pending_index_entry置位true，表示接下来要写一条记录到indexBlock
    r->pending_index_entry = true;
    //刷新到磁盘上,执行性flush，保证写的数据都正确落地到磁盘
    r->status = r->file->Flush();
  }
  //filter_block随之也会更新新的block,这里检测下要不要在FilterBlock中生成一个新的Filter
  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);
  }
}
// 写DataBlock方法
void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:
  //    block_data: uint8[n]
  //    type: uint8
  //    crc: uint32
  assert(ok());
  Rep* r = rep_;
  // 将DataBlock中的数据按DataBlock格式封装好存于raw中,将重启点和重启点的个数放入buffer中。
  Slice raw = block->Finish();
  // 将数据压缩存储，作者是建议将数据压缩的，一方面可以减少写放大，另一方面存入的数据更少。
  // 如果开启了压缩，但是压缩率小于12.5%，则直接存储未压缩的格式数据。
  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    case kSnappyCompression: {
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }
  //追加，刷盘  真正写元数据方法
  WriteRawBlock(block_contents, type, handle);
  // 将辅助字段清空初始化
  r->compressed_output.clear();
  //写完后，重置
  block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  // 保存偏移量  记下DataBlock在整个SSTable中的偏移位offset，
  // 以及DataBlock的大小size，用于上层写IndexBlock用。
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());// 不包括crc32和compressType,因为rc32和compressType都是固定大小，要想获取骗字节就行了。
  // 将DataBlock数据写入文件中
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    // 生成crc
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    // 刷盘
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      // 写成功之后，更新下SSTable接下来写新的DataBlock的偏移位offset
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}
// 获取当前操作的状态
Status TableBuilder::status() const { return rep_->status; }
//构造并写footer
/*
这个Finish方法就是按SSTable文件结构组装数据，
一般在要完成真个SSTable时，调用此方法。
数据格式就是：
| DataBlock | MetaBlcok(FilterBlock) | MetaBlcok Index | Index Block | Footer |
————————————————

*/
Status TableBuilder::Finish() {
  Rep* r = rep_;
  //
  Flush();
  assert(!r->closed);
  r->closed = true;
  //三种BlockHandle
  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // Write filter block
  if (ok() && r->filter_block != nullptr) {
    //调用filter_block的Finish()来写filter block
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // Write metaindex block
  /*
    写Metaindex Block，数据记录也是如下格式：
    | key | offset | size |，这里的key格式是"filer." + 策略名字。

  */
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      //filter_block_handle所在的offset和文件大小，通过这个可以找到上述的filterBlock对象
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // Write index block,最后一次构造最后一个block在index_block上对应的entry。
  // 将IndexBlock写入SSTable
  if (ok()) {
    if (r->pending_index_entry) {
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      //
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    WriteBlock(&r->index_block, &index_block_handle);
  }
  /*
    写Footer，整个大小是48Byte，组成如下：
  | metaindex_block_handle | index_block_handle | pendding | magic |
  */
  // Write footer
  // 写footer。
  if (ok()) {
    Footer footer;
    //写入对应的handle
    footer.set_metaindex_handle(metaindex_block_handle);
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    //编码，就是序列化
    footer.EncodeTo(&footer_encoding);
    //追加到文件的最后
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      //累加偏移量offset。
      r->offset += footer_encoding.size();
    }
  }
  // 返回操作状态，外层会根据情况将数据刷到磁盘
  return r->status;
}

void TableBuilder::Abandon() {
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}
// 获取SSTable写了多少个key
uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }
// 获取SSTable的大小
uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
