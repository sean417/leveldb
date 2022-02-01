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


// 每2k的数据生成一个filter  每个Filter是2KByte左右
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;
/*
  构造一个FilterBlockBuilder，用于按照FilterBlock格式去生存FilterBlock，
  这里要传一个过滤策略进去，用于生存Filter和过滤检查Key时使用。
*/
FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}
/*
  根据Data Block在SSTable中的偏移位block_offset来判定要不要新生存一个Filter，
  若要生存Filter，则由GenerateFilter()去生成。
*/
void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  //2kb一个filter
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  //是否需要构建新的布隆过滤器
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}
/*
往FilterBlock中添加一个key，
  1.start_是个vector类型，其主要存储每个key的大小，其实就是偏移位，根据这个偏移位
    就可以去keys_中找到这个key。
  2.keys_是个string类型，所有的key都追加添加进去。
  当用start_和keys_生成完一个Filter之后，就将二者清空。 
*/
void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}
/*
当SSTable要写FilterBlock时，则调用此Finish()，
 Finish()就是完成FilterBlock格式的封装。
 参照FilterBlock的格式，就很容易明白此方法的流程了。 
*/
Slice FilterBlockBuilder::Finish() {
  // 当前start_存有key size，则需要新生存一个Filter
  if (!start_.empty()) {
    GenerateFilter();
  }
  // 此刻restlt中存的都是Filter，然后将filter offset追加到后面
  // Append array of per-filter offsets 每个过滤器偏移量数组大小
  /*
    此刻result中存的是 filter、filter offset，
    此时将filter offset这个数组的起始偏移位array_offset
    写入result。
  */
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
//生成布隆过滤器的过程,生成一个Filter
void FilterBlockBuilder::GenerateFilter() {
  //key个数，当前新添加的key的个数，如果没有，则记录上一次的result大小。获取当前有多少key用于Filter
  const size_t num_keys = start_.size();
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    /*
      如果key个数为0，那新添加的filter_offset指向当前
      最后一个Filter尾部，也是下一个Filter的起始处
     （不一定有下一个Filter了）
    */
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  /*
    下文在将key放到tmp_keys_中时，求key的长度都是后一起i+1  - 前一个i,
    为了能计算最后一个key的长度，所以这里要把整个key的长度再次放入到
    vector类型start_中。

  */
  start_.push_back(keys_.size());  // Simplify length computation
  //这里就是将key封装为Slice，放入到tmp_keys_中用于计算Filter
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
  /*
    新Filter的起始偏移位就是当前已存在Filter大小，
    起始这里往filter_offset中存的就是
    新Filter的起始偏移位。
  */
  filter_offsets_.push_back(result_.size());
  // 按照过滤策略生存Filter，并存于result_中
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);//
  // 清空用于生存Filter的临时变量
  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}
//读取流程:构造一个解析FilterBlock的类，其实就是按FilterBlock的格式去解析
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
//判断key是否在对应的布隆过滤器里
//KeyMayMatch对应的是反解析出对应的数据，然后进行bf判断。匹配key是否在block_offset对应的Filter中
bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  // 通过block_offset算出整个key应该在哪个Filter中去查找
  uint64_t index = block_offset >> base_lg_;
  /*
    这里如果index 大于最大的Filter offset个数，
    这里还是会返回true，默认匹配。实际去DataBlock中去定位查找。
  */
  if (index < num_) {
    //filter offset固定是4个字节，所以这里直接加index*4,跳转到该offset的起始位置。
    /*
      Filter Offset都是4Byte大小的，所以这里都是*4。
      start是这个Filter在FilterBlock中的起始偏移位，
      limit就是这个Filter的大小。
    */
    uint32_t start = DecodeFixed32(offset_ + index * 4);
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      // 反解出对应的bf数据，over   将Filter封装到Slice中，通过过滤策略内部实现去查找确定是否有对应的key
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys。如果start == limit，表示不存在这个Filter，所以肯定不存在匹配
      return false;
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
