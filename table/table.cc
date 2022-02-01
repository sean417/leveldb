// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"
#include "leveldb/cache.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"

namespace leveldb {

struct Table::Rep {
  ~Rep() {
    delete filter;
    delete[] filter_data;
    delete index_block;
  }
  //Table相关的参数信息
  Options options;
  //Table相关的状态信息
  Status status;
  //Table持有的文件
  RandomAccessFile* file;//ldb文件句柄
  //Table对应的缓存id
  uint64_t cache_id;//Cache缓存分配给当前DataBlock的唯一id
  //filter block块的读取
  FilterBlockReader* filter;//读取FilterBlock实例
  //保存对应的filter数据
  const char* filter_data;//指向filter 数据
  //解析保存metaindex_handler //index block在ldb文件中的位置信息
  BlockHandle metaindex_handle;  // Handle to metaindex_block: saved from footer
  //index block数据 //index_block的操作实例
  Block* index_block;
};
/* 打开一个sst的操作。打开一个sst和写一个sst的逻辑是反的。
   写一个sst是先写Data Block然后再写File Footer，然后
   再根据得到的元数据解析Data Block
   1.静态函数，而且唯一是公共的接口。
   2.主要负责解析出基本数据，如Footer，然后解析出meta index block+index block+
   filter block+data block。
   3.BlockContents对象到Block的转换，其中主要是计算出restart_offset_;而且Block是可以被遍历的。


//打开SSTable时，首先将index block读取出来，
//用于后期查询key时，先通过内存中的index block来
//判断key在不在这个SSTable，然后再决定是否去读取对应的data block。
//这样明显可减少I/O操作。
*/ 
Status Table::Open(const Options& options, RandomAccessFile* file,
                   uint64_t size, Table** table) {
  *table = nullptr;
  //1.先解析footer:footer部分就是48个字节，如果整个sst文件小于48个字节，那么肯定是不正常的
  // //SSTable的Footer就是48Byte
  if (size < Footer::kEncodedLength) {
    return Status::Corruption("file is too short to be an sstable");
  }
  //1.1 从尾部解析出，固定长度的Footer对应的二进制数据。
  char footer_space[Footer::kEncodedLength];
  Slice footer_input;
  //因为Footer位于文件末尾，所以减去Footer本身的长度，
  //然后再开始读取Footer具体内容，这也是为啥Footer是定长的，便于区分文件内容而已


  //将footer读出来，用于解析其中的metaindex_block_handle和
  //index_block_handle。

   //1、解析出metaindex_block_handle；
   //2、解析出index_block_handle。
  Status s = file->Read(size - Footer::kEncodedLength, Footer::kEncodedLength,
                        &footer_input, footer_space);
  if (!s.ok()) return s;
  // 二进制数据反序列回 footer
   //1、解析出metaindex_block_handle；
  //2、解析出index_block_handle。
  Footer footer;
  //解码，反序列化
  s = footer.DecodeFrom(&footer_input);
  if (!s.ok()) return s;
  //2.BlockContents对象到Block的转换，其中主要是计算出restart_offset_;而且Block是可以被遍历的。
  // Read the index block
  
  BlockContents index_block_contents;
  ReadOptions opt;
  //是否开启严格检查数据完整性，默认false
	//开启之后可能会因为部分数据异常导致整个数据库无法读。
  if (options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  //将index_block读出。
	//1、安装offset去sstable位置读取数据；
	//2、若开启校验则校验；
	//3、若数据压缩则解压。
  s = ReadBlock(file, opt, footer.index_handle(), &index_block_contents);

  if (s.ok()) {
    // We've successfully read the footer and the index block: we're
    // ready to serve requests.
    Block* index_block = new Block(index_block_contents);
    Rep* rep = new Table::Rep;
    rep->options = options;
    rep->file = file;
    rep->metaindex_handle = footer.metaindex_handle();
    rep->index_block = index_block;
    rep->cache_id = (options.block_cache ? options.block_cache->NewId() : 0);
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    //实例一个table，用于对sstable读取解析
    *table = new Table(rep);

    //读取filter block
    (*table)->ReadMeta(footer);
  }

  return s;
}
//读取元数据，主要是Footer,然后是metaIndexHandle,filter handle
void Table::ReadMeta(const Footer& footer) {
  //过滤策略都没有，那就可以确定没必要读filter block了
  if (rep_->options.filter_policy == nullptr) {
    return;  // Do not need any metadata
  }

  // TODO(sanjay): Skip this if footer.metaindex_handle() size indicates
  // it is an empty block.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  BlockContents contents;
  // 根据metaindex_handle读取metaindex block
  if (!ReadBlock(rep_->file, opt, footer.metaindex_handle(), &contents).ok()) {
    // Do not propagate errors since meta info is not needed for operation
    return;
  }

  //这里是疑惑的地方！！！！！！
  Block* meta = new Block(contents);

  Iterator* iter = meta->NewIterator(BytewiseComparator());
  std::string key = "filter.";
  key.append(rep_->options.filter_policy->Name());
  iter->Seek(key);
  if (iter->Valid() && iter->key() == Slice(key)) {
    //根据metaindex的offset+size去读取filter block
    ReadFilter(iter->value());
  }
  delete iter;
  delete meta;
}
// 根据filter handler还原filter block数据。
void Table::ReadFilter(const Slice& filter_handle_value) {
  Slice v = filter_handle_value;
  BlockHandle filter_handle;
  if (!filter_handle.DecodeFrom(&v).ok()) {
    return;
  }

  // We might want to unify with ReadBlock() if we start
  // requiring checksum verification in Table::Open.
  ReadOptions opt;
  if (rep_->options.paranoid_checks) {
    opt.verify_checksums = true;
  }
  //读取filter block 数据
  BlockContents block;
  if (!ReadBlock(rep_->file, opt, filter_handle, &block).ok()) {
    return;
  }
  // 如果heap_allocated为true表示读取
  // filter block的时候new了内存，后续需要删除
  if (block.heap_allocated) {
    rep_->filter_data = block.data.data();  // Will need to delete later
  }
  //构造一个读取filter block的实例
  rep_->filter = new FilterBlockReader(rep_->options.filter_policy, block.data);
}

Table::~Table() { delete rep_; }

static void DeleteBlock(void* arg, void* ignored) {
  delete reinterpret_cast<Block*>(arg);
}

static void DeleteCachedBlock(const Slice& key, void* value) {
  Block* block = reinterpret_cast<Block*>(value);
  delete block;
}

static void ReleaseBlock(void* arg, void* h) {
  Cache* cache = reinterpret_cast<Cache*>(arg);
  Cache::Handle* handle = reinterpret_cast<Cache::Handle*>(h);
  cache->Release(handle);
}

// Convert an index iterator value (i.e., an encoded BlockHandle)
// into an iterator over the contents of the corresponding block.
// block的读取
// 根据 index_value (即offset+size)，读取对应的block。
Iterator* Table::BlockReader(void* arg, const ReadOptions& options,
                             const Slice& index_value) {
  Table* table = reinterpret_cast<Table*>(arg);
  Cache* block_cache = table->rep_->options.block_cache;
  Block* block = nullptr;
  Cache::Handle* cache_handle = nullptr;

  BlockHandle handle;
  Slice input = index_value;
  Status s = handle.DecodeFrom(&input);
  // We intentionally allow extra stuff in index_value so that we
  // can add more features in the future.

  if (s.ok()) {
    BlockContents contents;
    if (block_cache != nullptr) {
      //1.block cache 插入变量的初始化。并进行缓存的读取
      //如果开启了block_cache，则先去此cache中查找
	    //key就是id+DataBlock的offset。（此处暂时不解读Cache相关实现）
      char cache_key_buffer[16];
      //2.缓存的读取：先拼装key
      EncodeFixed64(cache_key_buffer, table->rep_->cache_id);
      EncodeFixed64(cache_key_buffer + 8, handle.offset());
      Slice key(cache_key_buffer, sizeof(cache_key_buffer));
      //3.根据key先到block cache里找。
      //1)、若在cache中查找到了直接将地址赋值给block;
	    //2)、若未找到，则去SSTable文件中去查找
      cache_handle = block_cache->Lookup(key);
      //1、若在cache中查找到了直接将地址赋值给block;
	    //2、若为找到，则去SSTable文件中去查找
      if (cache_handle != nullptr) {
        block = reinterpret_cast<Block*>(block_cache->Value(cache_handle));
      } else {
        //4.block cache里找不到，就到磁盘里找，找到后放到block_cache里。
        s = ReadBlock(table->rep_->file, options, handle, &contents);
        if (s.ok()) {
          block = new Block(contents);
          // contents.cachable和options.fill_cache会控制是否插入到缓存里面去。
          // 若读取的Block是直接new的，且fill_cache,则将这个Block缓存起来。
          if (contents.cachable && options.fill_cache) {
            //5.把block插入到block_cache里。同时注册一个回调函数DeleteCachedBlock。
            //回调函数DeleteCachedBlock目的是内存释放。
            cache_handle = block_cache->Insert(key, block, block->size(),
                                               &DeleteCachedBlock);
          }
        }
      }
    } else {
      //3、若为使用block_cache，则直接去SSTable中去读数据。
      s = ReadBlock(table->rep_->file, options, handle, &contents);
      if (s.ok()) {
        block = new Block(contents);
      }
    }
  }

  Iterator* iter;
  if (block != nullptr) {
    iter = block->NewIterator(table->rep_->options.comparator);
    //1、cache_handle 为null,表示block不在缓存中，在迭代器iter析构时，
	  //   直接删除这个block。
	  //2、cache_handle非null,表示block在缓存中，在迭代器iter析构时,
	  //   通过ReleaseBlock，减少其一次引用计数。
    if (cache_handle == nullptr) {
      iter->RegisterCleanup(&DeleteBlock, block, nullptr);
    } else {
      iter->RegisterCleanup(&ReleaseBlock, block_cache, cache_handle);
    }
  } else {
    //若未获取到block，则直接生存一个错误迭代器返回。
    iter = NewErrorIterator(s);
  }
  return iter;
}

// 返回index block的迭代器，目的是为了确定当前key位于哪个data block中
// SSTable二层迭代器迭代器。
Iterator* Table::NewIterator(const ReadOptions& options) const {
  return NewTwoLevelIterator(
      rep_->index_block->NewIterator(rep_->options.comparator),
      &Table::BlockReader, const_cast<Table*>(this), options);
}

//查找指定的key,内部先bf判断，然后缓存获取，最后再读取文件。
Status Table::InternalGet(const ReadOptions& options, const Slice& k, void* arg,
                          void (*handle_result)(void*, const Slice&,
                                                const Slice&)) {
  Status s;
  //通过key，找到 index block 中的一条对应DataBlock的记录
  Iterator* iiter = rep_->index_block->NewIterator(rep_->options.comparator);
  iiter->Seek(k);
  //Seek到
  if (iiter->Valid()) {
    //  handle_value 就是返回的DataBlock的offset+size。   
    Slice handle_value = iiter->value();
    FilterBlockReader* filter = rep_->filter;
    BlockHandle handle;
    // KeyMayMatch（）：根据偏移量找到对应布隆过滤器，并把 key 传到布隆过滤器里去
    // 如果过滤策略非空，则通过DataBlock的offset,去Filter中去查找是否有此key
    if (filter != nullptr && handle.DecodeFrom(&handle_value).ok() &&
        !filter->KeyMayMatch(handle.offset(), k)) {
      // Not found  key不存在
    } else {
      //key可能存在（因为布隆过滤器存在一定误差。）：这是需要进一步的判断
      //先把block找到。
      /*
        如果在Filte Block中查找到了（不一定真的查找到），那就去DataBlock中去查找。
	      通过DataBlock的offset+size去创建一个读取DataBlock的迭代器
      */
      Iterator* block_iter = BlockReader(this, options, iiter->value());
      //在block上迭代查找。//Seek要查找的key
      block_iter->Seek(k);
      if (block_iter->Valid()) {

        //查找到key之后，执行传入的方法函数
        (*handle_result)(arg, block_iter->key(), block_iter->value());
      }
      s = block_iter->status();
      delete block_iter;
    }
  }
  if (s.ok()) {
    s = iiter->status();
  }
  delete iiter;
  return s;
}

//预估key的大致偏移位。
//1、在index_block中查找到了就返回index_block中对应的DataBlock的offset。
//2、如果在index_block中查找到了但是无法解码出offset+size,就默认给metaindex_block的offset。
//3、Seek是查到大于等于这个key的值，若未找到，说明这个key比较大，默认给metaindex_block的offset。
uint64_t Table::ApproximateOffsetOf(const Slice& key) const {
  Iterator* index_iter =
      rep_->index_block->NewIterator(rep_->options.comparator);
  index_iter->Seek(key);
  uint64_t result;
  if (index_iter->Valid()) {
    BlockHandle handle;
    Slice input = index_iter->value();
    Status s = handle.DecodeFrom(&input);
    if (s.ok()) {
      result = handle.offset();
    } else {
      // Strange: we can't decode the block handle in the index block.
      // We'll just return the offset of the metaindex block, which is
      // close to the whole file size for this case.
      result = rep_->metaindex_handle.offset();
    }
  } else {
    // key is past the last key in the file.  Approximate the offset
    // by returning the offset of the metaindex block (which is
    // right near the end of the file).
    result = rep_->metaindex_handle.offset();
  }
  delete index_iter;
  return result;
}

}  // namespace leveldb
