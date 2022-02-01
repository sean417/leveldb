// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_DB_LOG_READER_H_
#define STORAGE_LEVELDB_DB_LOG_READER_H_

#include <cstdint>

#include "db/log_format.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"

namespace leveldb {
// 顺序读取文件的抽象封装类
class SequentialFile;

namespace log {

class Reader {
 public:
  // Interface for reporting errors.
  // 负责上报错误类
  class Reporter {
   public:
    virtual ~Reporter();

    // Some corruption was detected.  "size" is the approximate number
    // of bytes dropped due to the corruption.
    virtual void Corruption(size_t bytes, const Status& status) = 0;
  };

  // Create a reader that will return log records from "*file".
  // "*file" must remain live while this Reader is in use.
  // 创建一个读对象，这个读对象将会从 "*file"文件中返回log record。
  // If "reporter" is non-null, it is notified whenever some data is
  // dropped due to a detected corruption.  "*reporter" must remain
  // live while this Reader is in use.
  // 如果reporter是非空的，无论什么时候一些数据由于一个破坏造成丢弃的错误会报错。如果reader在使用中，那么"*reporter" 必须保持活跃的，
  // If "checksum" is true, verify checksums if available.
  //
  // The Reader will start reading at the first record located at physical
  // position >= initial_offset within the file.
  // 在物理偏移量大于初始offest的第一个record开始读。
  // 1.file: 要读取的Log文件封装。
  // 2.reporter: 错误上报类。
  // 3.checksum: 是否check校验。
  // 4.initial_offset：开始读取数据偏移位置。
  Reader(SequentialFile* file, Reporter* reporter, bool checksum,
         uint64_t initial_offset);
  // 禁止拷贝构造和赋值构造
  Reader(const Reader&) = delete;
  Reader& operator=(const Reader&) = delete;

  ~Reader();

  // Read the next record into *record.  Returns true if read
  // successfully, false if we hit end of the input.  May use
  // "*scratch" as temporary storage.  The contents filled in *record
  // will only be valid until the next mutating operation on this
  // reader or the next mutation to *scratch.
  /*
    1.读取一个Record记录，成功返回true，失败返回false。
    2.读取的数据在*record参数中，传入的*scratch用于临时内部临时存储使用。
  */
  bool ReadRecord(Slice* record, std::string* scratch);

  // Returns the physical offset of the last record returned by ReadRecord.
  //
  // Undefined before the first call to ReadRecord.
  // 返回最近一次读取Record的偏移位，也就是这个Record的起始位
  uint64_t LastRecordOffset();

 private:
  // Extend record types with the following special values

  /*
    扩展两种类型用于错误表示。
    1.kEof表示到达文件尾。
    2.kBadRecord表示以下三种错误：
    1)CRC校验失败、
    2)读取长度为0、
    3)读取的内存在 initial_offset 之外，比方说从64位置开始读而Record在31~63之间。
  */
  enum {
    kEof = kMaxRecordType + 1,
    // Returned whenever we find an invalid physical record.
    // Currently there are three situations in which this happens:
    // * The record has an invalid CRC (ReadPhysicalRecord reports a drop)
    // * The record is a 0-length record (No drop is reported)
    // * The record is below constructor's initial_offset (No drop is reported)
    kBadRecord = kMaxRecordType + 2
  };

  // Skips all blocks that are completely before "initial_offset_".
  // 跳到起始位置initial_offset处开始读取
  // Returns true on success. Handles reporting.
  bool SkipToInitialBlock();

  // Return type, or one of the preceding special values
  unsigned int ReadPhysicalRecord(Slice* result);

  // Reports dropped bytes to the reporter.
  // buffer_ must be updated to remove the dropped bytes prior to invocation.
  void ReportCorruption(uint64_t bytes, const char* reason);
  void ReportDrop(uint64_t bytes, const Status& reason);

  SequentialFile* const file_;
  Reporter* const reporter_;
  bool const checksum_;
  //32kb大小数据存储空间，用于从文件中读取一个 Block
  char* const backing_store_;
  // 将从文件读取到的数据封装为一个Slice，用buffer_来表示
  Slice buffer_;
  // 当读取的文件数据大小小于kBlockSize，表示读取到文件尾，将eof_置位true
  bool eof_;  // Last Read() indicated EOF by returning < kBlockSize

  // Offset of the last record returned by ReadRecord.
  // 最近一次读取Record的偏移位，也就是这个Record的起始位
  uint64_t last_record_offset_;
  // Offset of the first location past the end of buffer_.
  // 读取的Buffer尾部的偏移位
  uint64_t end_of_buffer_offset_;

  // Offset at which to start looking for the first record to return
  // 开始读取数据位置
  uint64_t const initial_offset_;

  // True if we are resynchronizing after a seek (initial_offset_ > 0). In
  // particular, a run of kMiddleType and kLastType records can be silently
  // skipped in this mode

  /*
    是否重新开始读取 Record
    在初始读取位置 initial_offset > 0的情况下，resyncing_才为true，
    因为初始位置如果不是从0开始，首次读取到的Record的type是kMiddleType和
    kLastType的话，则不是一个完整的record，所以要丢弃重新读取。
  */
  bool resyncing_;
};

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_READER_H_
