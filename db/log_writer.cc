// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <cstdint>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {
// 计算RecordType的CRC32值
static void InitTypeCrc(uint32_t* type_crc) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc[i] = crc32c::Value(&t, 1);
  }
}

Writer::Writer(WritableFile* dest) : dest_(dest), block_offset_(0) {
  InitTypeCrc(type_crc_);
}

Writer::Writer(WritableFile* dest, uint64_t dest_length)
    : dest_(dest), block_offset_(dest_length % kBlockSize) {
  InitTypeCrc(type_crc_);
}
// 指定默认析构函数
Writer::~Writer() = default;
// 写Record流程
Status Writer::AddRecord(const Slice& slice) {
  //ptr为指向数据的指针。
  const char* ptr = slice.data();
  size_t left = slice.size();
  /*
   1、有必要的情况下，需要record进行分片写入；
   2、如果slice数据为空，仍然会写一次，只是长度为0，
      读取的时候会对此种情况进行处理。
  >
  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  <!写文件是以一个Block(32KB)为单元写入的，而写入到Block的是一个个Record，
    每个Record的头长度为7Byte。假设这个Block剩余可写的长度为L，
    要写入的数据为N，则分以下情况进行处理：
    1、L >= N+7，说明Block空间足以容纳下一个Record和7Byte的头，
       则这个数据被定义为一个Type为kFullType的Record。
    2、N + 7 > L >= 7，即当前Block空间大于等于7Byte，但不足以保存全部内容，
       则在当前页生存一个Type为kFirstType的Record，Payload（Block剩余空间）保存
       数据前面L-7字节的内容（可以为0，那就直说一个头），如果数据剩余的长度小于32KB，
       则在下一个页中生成一个Type为kLastType的Record，否则在下一个Block中生成一个
       Type为kMiddleType的Record，依次类推，直至数据被完全保存下来。
    3、L < 7，当前Block的剩余长度小于7Byte，则填充0。      
    以上流程就是整个写流程了。
 */

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;
  do {
    //检查还剩多少空间可以写
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    //如果小于 kHeaderSize,就需要开启新的block，因为kHeaderSize必须要写在一个block
    //上。
    if (leftover < kHeaderSize) {
      // Switch to a new block
      // 转换到新的block之后，如果上个block还有剩余的空间那么就用0来填充。
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        static_assert(kHeaderSize == 7, "");
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    //计算是否刚好填满这个block
    const bool end = (left == fragment_length);
    if (begin && end) {//新block,又刚好装下，完美啊
      type = kFullType;
    } else if (begin) {//新block但是一个又装不下
      type = kFirstType;
    } else if (end) {//数据在上一个block保存了一部分，同时需要新block保存而且保存完了。
      type = kLastType;
    } else {
      type = kMiddleType;//其他场景，数据比较长会跨多个block，本block还结束不了。
    }

    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
  return s;
}
//把WAL数据保存先到内存中

/*
实际写实现：
  1、格式化打包头；
  2、CRC校验计算；
  3、先写头、再写Payload，写成功之后flush下；
  4、将block_offset_位置重新计算下。
>
*/
Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr,
                                  size_t length) {
  assert(length <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + length <= kBlockSize);

  // Format the header
  char buf[kHeaderSize];
  //序列化长度和recordtype信息
  buf[4] = static_cast<char>(length & 0xff);
  buf[5] = static_cast<char>(length >> 8);
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.c
  // crc数据
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, length);
  crc = crc32c::Mask(crc);  // Adjust for storage
  EncodeFixed32(buf, crc);

  // Write the header and the payload 写头和数据
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
    s = dest_->Append(Slice(ptr, length));
    if (s.ok()) {
      s = dest_->Flush();
    }
  }
  block_offset_ += kHeaderSize + length;
  return s;
}

}  // namespace log
}  // namespace leveldb
