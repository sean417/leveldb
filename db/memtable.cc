// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/memtable.h"
#include "db/dbformat.h"
#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/coding.h"

namespace leveldb {
// 获取InternalKey
static Slice GetLengthPrefixedSlice(const char* data) {
  uint32_t len;
  const char* p = data;
  //因为Varint32最多占用5Byte，这里默认+5，
  //保证内部地址判断合法，取到正确的长度。
  p = GetVarint32Ptr(p, p + 5, &len);  // +5: we assume "p" is not corrupted
  return Slice(p, len);
}

MemTable::MemTable(const InternalKeyComparator& comparator)
    : comparator_(comparator), refs_(0), table_(comparator_, &arena_) {}
// 析构是由Unref()调用，此时refs_ == 0
MemTable::~MemTable() { assert(refs_ == 0); }
// 获取此时MemTable消耗的内存大小
size_t MemTable::ApproximateMemoryUsage() { return arena_.MemoryUsage(); }
/*
Key比较的二次封装，取出aptr和bptr对应的
 InternalKey，然后进行比较。此处使用的SkipList
 内部都是比较InternalKey来排序的。
*/
int MemTable::KeyComparator::operator()(const char* aptr,
                                        const char* bptr) const {
  // Internal keys are encoded as length-prefixed strings.
  Slice a = GetLengthPrefixedSlice(aptr);
  Slice b = GetLengthPrefixedSlice(bptr);
  return comparator.Compare(a, b);
}

// Encode a suitable internal key target for "target" and return it.
// Uses *scratch as scratch space, and the returned pointer will point
// into this scratch space.此处就是编码一个LookUPKey格式的数据，存于临时内存*scratch中
static const char* EncodeKey(std::string* scratch, const Slice& target) {
  scratch->clear();
  PutVarint32(scratch, target.size());
  scratch->append(target.data(), target.size());
  return scratch->data();
}
// MemTable的迭代器，基本上使用的是SkipList中iterator
class MemTableIterator : public Iterator {
 public:
  //外部传入table后，对table进行迭代
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) {}

  MemTableIterator(const MemTableIterator&) = delete;
  MemTableIterator& operator=(const MemTableIterator&) = delete;
   /*
   指定编译器合成一个析构函数。使用default作用：
   1. 可不用程序员自己再去实现方法。
   2. 编译器实现的效率会高些。
   */
  ~MemTableIterator() override = default;

  bool Valid() const override { return iter_.Valid(); }
  // 此处通过EncodeKey，编码一个LookUPKey用于定位
  void Seek(const Slice& k) override { iter_.Seek(EncodeKey(&tmp_, k)); }
  void SeekToFirst() override { iter_.SeekToFirst(); }
  void SeekToLast() override { iter_.SeekToLast(); }
  void Next() override { iter_.Next(); }
  void Prev() override { iter_.Prev(); }
  // 此处返回一个InternalKey
  Slice key() const override { return GetLengthPrefixedSlice(iter_.key()); }
  Slice value() const override {
    Slice key_slice = GetLengthPrefixedSlice(iter_.key());
    return GetLengthPrefixedSlice(key_slice.data() + key_slice.size());
  }

  Status status() const override { return Status::OK(); }

 private:
  //对MemTable的迭代实际是在迭代skiplist
  MemTable::Table::Iterator iter_;
  std::string tmp_;  // For passing to EncodeKey
};

Iterator* MemTable::NewIterator() { return new MemTableIterator(&table_); }

/*
  编码一个Memtable entry存入SkipList数据结构中。
 看懂了Memtable entry结构就很好理解下面的过程了。
 最后的buf中存的就是一个Memtable entry。

*/
void MemTable::Add(SequenceNumber s, ValueType type, const Slice& key,
                   const Slice& value) {
  // Format of an entry is concatenation of:
  //  key_size     : varint32 of internal_key.size()
  //  key bytes    : char[internal_key.size()]
  //  value_size   : varint32 of value.size()
  //  value bytes  : char[value.size()]
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  const size_t encoded_len = VarintLength(internal_key_size) +
                             internal_key_size + VarintLength(val_size) +
                             val_size;
  char* buf = arena_.Allocate(encoded_len);
  char* p = EncodeVarint32(buf, internal_key_size);
  std::memcpy(p, key.data(), key_size);
  p += key_size;
  //左移8位留一个字节充当type
  EncodeFixed64(p, (s << 8) | type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  std::memcpy(p, value.data(), val_size);
  assert(p + val_size == buf + encoded_len);
  table_.Insert(buf);
}
// 根据LookupKey查询出对应的Value
bool MemTable::Get(const LookupKey& key, std::string* value, Status* s) {
  // 就是LookupKey
  Slice memkey = key.memtable_key();
  Table::Iterator iter(&table_);
  iter.Seek(memkey.data());
  // 根据LookupKey进行查找，找到了则
  // iter.Valid()为true，否则false。
  if (iter.Valid()) {
    // entry format is:
    //    klength  varint32
    //    userkey  char[klength]
    //    tag      uint64
    //    vlength  varint32
    //    value    char[vlength]
    // Check that it belongs to same user key.  We do not check the
    // sequence number since the Seek() call above should have skipped
    // all entries with overly large sequence numbers.
    const char* entry = iter.key();
    uint32_t key_length;
    const char* key_ptr = GetVarint32Ptr(entry, entry + 5, &key_length);
    /*
      因为上述Seek是找的大于等于key的值，所以此处还需要直接和UserKey比较下。
      这里的Compare内部仅仅是比较UserKey，不是涉及到其他的。
    */
    if (comparator_.comparator.user_comparator()->Compare(
            Slice(key_ptr, key_length - 8), key.user_key()) == 0) {
      // Correct user key
      /*
       如果确实找到了UserKey，则通过TypeValue来判断下是否是被删除的。
       删除的话就在*s中返回NotFound状态。
      */
      const uint64_t tag = DecodeFixed64(key_ptr + key_length - 8);
      switch (static_cast<ValueType>(tag & 0xff)) {
        case kTypeValue: {
          Slice v = GetLengthPrefixedSlice(key_ptr + key_length);
          value->assign(v.data(), v.size());
          return true;
        }
        case kTypeDeletion:
          *s = Status::NotFound(Slice());
          return true;
      }
    }
  }
  return false;
}

}  // namespace leveldb

/*
总结：
1.Memtable的Get中对取得的Value值都是直接拷贝的，如果Value越大，消耗则越大。
2.Memtable是利用Lookupkey来作为key使用。
3.SkipList第0层是从小到大排序的。

*/
