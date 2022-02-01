// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Log format information shared by reader and writer.
// See ../doc/log_format.md for more detail.

#ifndef STORAGE_LEVELDB_DB_LOG_FORMAT_H_
#define STORAGE_LEVELDB_DB_LOG_FORMAT_H_

namespace leveldb {
/*WAL日志
所有的写操作都是先成功的append到Log日志中，然后在更新内存memtable的。
这样做有如下优点：
1) 可以将随机的写IO变成append，极大的提高写磁盘速度；
2) 防止在节点down机导致内存数据丢失，造成数据丢失，这对系统来说是个灾难。
日志文件的切换是在写KV记录之前会进行MakeRoomForWrite来决定是否切换新的日志文件，所以在写入的过程中是不需要关注文件切换的。
接下来介绍Log模块的读写流程及结构。
*/
namespace log {

enum RecordType {
  // Zero is reserved for preallocated files
  kZeroType = 0,

  kFullType = 1,

  // For fragments
  kFirstType = 2,
  kMiddleType = 3,
  kLastType = 4
};
static const int kMaxRecordType = kLastType;
//WAL日志一个block文件大小32k
static const int kBlockSize = 32768;
// WAL日志record的header的大小及组成
// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
static const int kHeaderSize = 4 + 2 + 1;

}  // namespace log
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_DB_LOG_FORMAT_H_
