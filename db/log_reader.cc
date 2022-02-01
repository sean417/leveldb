// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_reader.h"

#include <cstdio>

#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

Reader::Reporter::~Reporter() = default;
/*
实例化时，做如下事情：
 1、赋值下读取文件、异常上报程序;
 2、是否执行数据校验（checksum_为true,则校验）;
 3、申请一块32KB大小的内存用于读取block;
 4、Slice(buffer_)初始化;
 5、上次读取的record偏移位为0;
 6、读取的一个buffer尾部偏移位为0;
 7、初始化读取Record位置。
 8、重读取标志(resyncing_
*/
Reader::Reader(SequentialFile* file, Reporter* reporter, bool checksum,
               uint64_t initial_offset)
    : file_(file),
      reporter_(reporter),
      checksum_(checksum),
      backing_store_(new char[kBlockSize]),
      buffer_(),
      eof_(false),
      last_record_offset_(0),
      end_of_buffer_offset_(0),
      initial_offset_(initial_offset),
      resyncing_(initial_offset > 0) {}
//析构时，释放内存
Reader::~Reader() { delete[] backing_store_; }
//根据initial_offset跳转到第一个Block处
bool Reader::SkipToInitialBlock() {
  // 先计算offset_in_block长度超出N个block多少字节。
  const size_t offset_in_block = initial_offset_ % kBlockSize;
  // offset_in_block长度最后一个字节会落在某个block上，那么这个block的起始位置的offset是多少。
  uint64_t block_start_location = initial_offset_ - offset_in_block;
  /*
    写数据时，会有个最后6字节的0x00填充位，也就是trailer
    如果最后求到的余的位置落在这6字节范围内，直接跳过一个
    32768个字节的Block，进行读取。
  */
  // Don't search a block if we'd be in the trailer
  if (offset_in_block > kBlockSize - 6) {
    block_start_location += kBlockSize;
  }
  //跳转到的开始读取位置指定为Buffer的尾部偏移位
  end_of_buffer_offset_ = block_start_location;

  // Skip to start of first block that can contain the initial record
  // 跳转到第一个包含初始Record的Block处，如果异常就报错
  if (block_start_location > 0) {
    Status skip_status = file_->Skip(block_start_location);
    if (!skip_status.ok()) {
      ReportDrop(block_start_location, skip_status);
      return false;
    }
  }

  return true;
}
// 读取Record实现
bool Reader::ReadRecord(Slice* record, std::string* scratch) {
  // 如果上一次读取record位置小于当前起始读取位置则跳过中间部分，直接到开始读取数据处
  if (last_record_offset_ < initial_offset_) {
    if (!SkipToInitialBlock()) {
      return false;
    }
  }
  /*
    1、初始化值；
    2、首次进来，肯定不在一个record片段中，所以 in_fragmented_record 为false。
  */
  scratch->clear();
  record->clear();
  bool in_fragmented_record = false;
  // Record offset of the logical record that we're reading
  // 0 is a dummy value to make compilers happy
  // 正在读取Record的偏移位，初始化为0
  uint64_t prospective_record_offset = 0;

  Slice fragment;
  while (true) {
    // 读取一个Record，并返回Record的Type，实现及注释看下文。
    const unsigned int record_type = ReadPhysicalRecord(&fragment);

    // ReadPhysicalRecord may have only had an empty trailer remaining in its
    // internal buffer. Calculate the offset of the next physical record now
    // that it has returned, properly accounting for its header size.
    // 这里就是计算出当前读取的Record的开始位置偏移位。
    uint64_t physical_record_offset =
        end_of_buffer_offset_ - buffer_.size() - kHeaderSize - fragment.size();
    /*
      如果initial_offset > 0，则resyncing_为true:
      1、如果读取到的record_type是kMiddleType，则少了kFirstType，重新读。
      2、如果读取到的record_type是kLastType，则少了kFirstType和kMiddleType，重新读，
        同时要把resyncing_置位false。
      
    */
    if (resyncing_) {
      if (record_type == kMiddleType) {
        continue;
      } else if (record_type == kLastType) {
        resyncing_ = false;
        continue;
      } else {
        resyncing_ = false;
      }
    }

    switch (record_type) {
      case kFullType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(1)");
          }
        }

        // 1、记录下当前Record起始地址，
        // 2、返回读取到的record。
        prospective_record_offset = physical_record_offset;
        scratch->clear();
        *record = fragment;
        last_record_offset_ = prospective_record_offset;
        return true;

      case kFirstType:
        if (in_fragmented_record) {
          // Handle bug in earlier versions of log::Writer where
          // it could emit an empty kFirstType record at the tail end
          // of a block followed by a kFullType or kFirstType record
          // at the beginning of the next block.

          /*
            早期版本有BUG，在下一个block之前会存在一个kFirstType，
            这样如果读取到下一个block有kFirstType，而之前已经读了一个kFirstType，
            则in_fragmented_record置位true了，如此则进入此流程
          */
          if (!scratch->empty()) {
            ReportCorruption(scratch->size(), "partial record without end(2)");
          }
        }

        /*
            进入此流程表示一个完整的record由first、middle、last组成
            剩下的就是组装数据。
        */
        prospective_record_offset = physical_record_offset;
        scratch->assign(fragment.data(), fragment.size());
        in_fragmented_record = true;
        break;

      case kMiddleType:
        //理论下如果record是kMiddleType，则in_fragmented_record为true，否则报错
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(1)");
        } else {
          scratch->append(fragment.data(), fragment.size());
        }
        break;

      case kLastType:
        if (!in_fragmented_record) {
          ReportCorruption(fragment.size(),
                           "missing start of fragmented record(2)");
        } else {
          /*
            最后一个type，Record，读完则组成一个完整的record，
            同时赋值下当前完整record的起始位置。
          */
          scratch->append(fragment.data(), fragment.size());
          *record = Slice(*scratch);
          last_record_offset_ = prospective_record_offset;
          return true;
        }
        break;
      //余下的都是错误处理，很容易看懂，就不注释了
      case kEof:
        if (in_fragmented_record) {
          // This can be caused by the writer dying immediately after
          // writing a physical record but before completing the next; don't
          // treat it as a corruption, just ignore the entire logical record.
          scratch->clear();
        }
        return false;

      case kBadRecord:
        if (in_fragmented_record) {
          ReportCorruption(scratch->size(), "error in middle of record");
          in_fragmented_record = false;
          scratch->clear();
        }
        break;

      default: {
        char buf[40];
        std::snprintf(buf, sizeof(buf), "unknown record type %u", record_type);
        ReportCorruption(
            (fragment.size() + (in_fragmented_record ? scratch->size() : 0)),
            buf);
        in_fragmented_record = false;
        scratch->clear();
        break;
      }
    }
  }
  return false;
}
//返回最近读取Record的偏移位
uint64_t Reader::LastRecordOffset() { return last_record_offset_; }

void Reader::ReportCorruption(uint64_t bytes, const char* reason) {
  ReportDrop(bytes, Status::Corruption(reason));
}

void Reader::ReportDrop(uint64_t bytes, const Status& reason) {
  if (reporter_ != nullptr &&
      end_of_buffer_offset_ - buffer_.size() - bytes >= initial_offset_) {
    reporter_->Corruption(static_cast<size_t>(bytes), reason);
  }
}

unsigned int Reader::ReadPhysicalRecord(Slice* result) {
  // while true的目的就是读取一个完整的Record
  while (true) {
    if (buffer_.size() < kHeaderSize) {
      /*
        kHeaderSize为7，如果buffer剩余大小小于
        7Byte，分两组情况：
        1、还未读取到一个文件尾部；
        2、已经读取到文件尾部。
      */
      if (!eof_) {
        // Last read was a full read, so this is a trailer to skip
        /*
          如果buffer_剩余大小小于7Byte且文件未读取到尾，那上一次读是读取了一个完整的Record,
          剩余的大小只是 6B 的填充trailer，所以只需跳过这个trailer,清空即可。
        */
        buffer_.clear();
        /*
          1、读取32KB大小数据；
          2、将end_of_buffer_offset_偏移一下位置。
        */
        Status status = file_->Read(kBlockSize, &buffer_, backing_store_);
        end_of_buffer_offset_ += buffer_.size();
        if (!status.ok()) {
          // 读取失败，直接报错并返回读到文件尾
          buffer_.clear();
          ReportDrop(kBlockSize, status);
          eof_ = true;
          return kEof;
        } else if (buffer_.size() < kBlockSize) {
          // 读取数据大小小于32KB，认为读取到整个wal文件尾了，
          // 通过continue，由上文判断下是不是小于7Byte的大小。
          eof_ = true;
        }
        continue;
      } else {
        // Note that if buffer_ is non-empty, we have a truncated header at the
        // end of the file, which can be caused by the writer crashing in the
        // middle of writing the header. Instead of considering this an error,
        // just report EOF.
        /*
          如果buffer_是大于0，小于7（头大小）且到文件尾了，
          很可能是正在写头的时候，写流程崩溃了导致截断的头，
          这里我们只需要返回到达文件尾即可，不会影响数据。
        */
        buffer_.clear();
        return kEof;
      }
    }

    // Parse the header
    // 准备解析数据，先解析header
    const char* header = buffer_.data();
    const uint32_t a = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t b = static_cast<uint32_t>(header[5]) & 0xff;
    // record的type
    const unsigned int type = header[6];
    const uint32_t length = a | (b << 8);
    // 解析出的数据长度大于实际读取的数据，则是异常的，返回
    if (kHeaderSize + length > buffer_.size()) {
      size_t drop_size = buffer_.size();
      buffer_.clear();
      if (!eof_) {
        ReportCorruption(drop_size, "bad record length");
        return kBadRecord;
      }
      // If the end of the file has been reached without reading |length| bytes
      // of payload, assume the writer died in the middle of writing the record.
      // Don't report a corruption.
      return kEof;
    }

    // 在env_posix.cc环境下写文件时存在预分配的情况会导致此类型type,
    // 返回异常即可，不用上报
    if (type == kZeroType && length == 0) {
      // Skip zero length record without reporting any drops since
      // such records are produced by the mmap based writing code in
      // env_posix.cc that preallocates file regions.
      buffer_.clear();
      return kBadRecord;
    }

    // Check crc
    // 主要是校验type+data数据，校验失败这要上报数据异常，并返回
    if (checksum_) {
      uint32_t expected_crc = crc32c::Unmask(DecodeFixed32(header));
      uint32_t actual_crc = crc32c::Value(header + 6, 1 + length);
      if (actual_crc != expected_crc) {
        // Drop the rest of the buffer since "length" itself may have
        // been corrupted and if we trust it, we could find some
        // fragment of a real log record that just happens to look
        // like a valid log record.
        size_t drop_size = buffer_.size();
        buffer_.clear();
        ReportCorruption(drop_size, "checksum mismatch");
        return kBadRecord;
      }
    }
    // 从buffer_中移除读取到的Record数据指向和大小
    buffer_.remove_prefix(kHeaderSize + length);

    // Skip physical record that started before initial_offset_
    // 就是读取 Record 的开始位置，也就是说读取Record的开始位置在initial_offset之前，则丢弃这个Record。
    if (end_of_buffer_offset_ - buffer_.size() - kHeaderSize - length <
        initial_offset_) {
      result->clear();
      return kBadRecord;
    }
    // 返回一个完整Record
    *result = Slice(header + kHeaderSize, length);
    return type;
  }
}

}  // namespace log
}  // namespace leveldb
