// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/cache.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>

#include "port/port.h"
#include "port/thread_annotations.h"
#include "util/hash.h"
#include "util/mutexlock.h"

namespace leveldb {

Cache::~Cache() {}

namespace {

// LRU cache implementation
//
// Cache entries have an "in_cache" boolean indicating whether the cache has a
// reference on the entry.  The only ways that this can become false without the
// entry being passed to its "deleter" are via Erase(), via Insert() when
// an element with a duplicate key is inserted, or on destruction of the cache.
//
// The cache keeps two linked lists of items in the cache.  All items in the
// cache are in one list or the other, and never both.  Items still referenced
// by clients but erased from the cache are in neither list.  The lists are:
// - in-use:  contains the items currently referenced by clients, in no
//   particular order.  (This list is used for invariant checking.  If we
//   removed the check, elements that would otherwise be on this list could be
//   left as disconnected singleton lists.)
// - LRU:  contains the items not currently referenced by clients, in LRU order
// Elements are moved between these lists by the Ref() and Unref() methods,
// when they detect an element in the cache acquiring or losing its only
// external reference.

// An entry is a variable length heap-allocated structure.  Entries
// are kept in a circular doubly linked list ordered by access time.
// LRUHandle是一个节点。LRUHandle不仅仅在LRUCache里用，而且还在 自定义哈希表 里用。
struct LRUHandle {
  void* value;//具体的值，指针类型
  void (*deleter)(const Slice&, void* value);//自定义回收节点的回调函数
  LRUHandle* next_hash;//用在哈希表里的，用于发生hash冲突的时候指向下一个值。
  LRUHandle* next;//用在LRU缓存里面的，LRU中双向链表中的下一个节点
  LRUHandle* prev;//用在LRU缓存里面的，LRU中双向链表中的上一个节点
  //为什么用两个指针呢？因为我需要快速定位前面一个节点并且快速定位后面一个节点。比如快速删除当前节点
  size_t charge;  // TODO(opt): Only allow uint32_t? 当前entry缓存占据了多少容量。
  size_t key_length; //key的长度
  bool in_cache;     // Whether entry is in the cache.   Entry是否在缓存中
  // References, including cache reference, if present.引用计数。有多少使用者引用。
  // 作用：比如LRUHandle节点缓存在cache里了，同时被外面组件引用了，这时LRU认为这个节点用的不多要从cache中删除，
  // 这时如果有外部引用那么如果删除了就会造成异常，所以要使用计数器保证为零时，再删除。
  uint32_t refs;
  uint32_t hash;     // Hash of key(); used for fast sharding and comparisons 记录key的hash值。
  char key_data[1];  // Beginning of key

  Slice key() const {
    // next_ is only equal to this if the LRU handle is the list head of an
    // empty list. List heads never have meaningful keys.
    assert(next != this);

    return Slice(key_data, key_length);
  }
};
 
// leveldb内部实现了一个hash表，相对于标准库而言，去除了移植性，而且比gcc4.4.3中内置的随机快了5%
//  
// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class HandleTable {
 public:
 //length_:哈希表总长度。
 //elems_:当前哈希表实际元素个数。
 //list:底层存储数据的数据结构，这里用的是链表
  HandleTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }
  ~HandleTable() { delete[] list_; }

  LRUHandle* Lookup(const Slice& key, uint32_t hash) {
    return *FindPointer(key, hash);
  }

  LRUHandle* Insert(LRUHandle* h) {
    LRUHandle** ptr = FindPointer(h->key(), h->hash);
    LRUHandle* old = *ptr;
    h->next_hash = (old == nullptr ? nullptr : old->next_hash);
    *ptr = h;
    if (old == nullptr) {
      ++elems_;
      //当元素个数比哈希表长度大时，才会触发rehash。
      if (elems_ > length_) {
        // Since each cache entry is fairly large, we aim for a small
        // average linked list length (<= 1).
        Resize();
      }
    }
    return old;
  }

  LRUHandle* Remove(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = FindPointer(key, hash);
    LRUHandle* result = *ptr;
    if (result != nullptr) {
      *ptr = result->next_hash;
      --elems_;
    }
    return result;
  }

 private:
  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;
  uint32_t elems_;
  LRUHandle** list_;

  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const Slice& key, uint32_t hash) {
    LRUHandle** ptr = &list_[hash & (length_ - 1)];
    while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }
  //扩容，扩容的目的是为了防止数据过多造成频繁的hash冲突，
  //从而造成性能下降。
  void Resize() {
    uint32_t new_length = 4;
    //当元素的个数大于4时，按照2的倍数扩容。
    while (new_length < elems_) {
      new_length *= 2;
    }
    LRUHandle** new_list = new LRUHandle*[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    //扩容后如何把原来哈希表的数据移动到新的哈希表中
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      while (h != nullptr) {
        LRUHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        //把原hash表中的链表放入插入新的hash表中。
        LRUHandle** ptr = &new_list[hash & (new_length - 1)];
        h->next_hash = *ptr;//尾插法
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
  }
};

/* A single shard of sharded cache.
  1. LRU中的元素可能不仅仅只在cache中，有可能被外部所引用，因此我们不能直接删除节点。
  2. 如果某个节点被修改/被其他线程引用，在空间不足的时候，也不能参与lru。
    比如如果被修改了但是没有放到磁盘里，那么修改可能会丢失
  3. in_use表示既在缓存中，也在外部被引用。
  4. lru_.next表示仅仅在缓存中而已。
  5. table_是为了记录key和节点的映射关系，通过key可以快速定位到某个节点
  6.调用Insert/LookUp之后，一定要使用Release来释放句柄，因为如果不释放句柄，
  就会让引用计数始终为2的状态，这样就无法删除这个节点了。造成内存暴涨
*/

class LRUCache {
 public:
  LRUCache();
  ~LRUCache();

  // Separate from constructor so caller can easily make an array of LRUCache
  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  // Like Cache methods, but with an extra "hash" parameter.
  Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value,
                        size_t charge,
                        void (*deleter)(const Slice& key, void* value));
  Cache::Handle* Lookup(const Slice& key, uint32_t hash);
  void Release(Cache::Handle* handle);
  void Erase(const Slice& key, uint32_t hash);
  void Prune();
  size_t TotalCharge() const {
    MutexLock l(&mutex_);
    return usage_;
  }

 private:
  void LRU_Remove(LRUHandle* e);
  void LRU_Append(LRUHandle* list, LRUHandle* e);
  void Ref(LRUHandle* e);
  void Unref(LRUHandle* e);
  bool FinishErase(LRUHandle* e) EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Initialized before use.
  // LRU cache的容量
  size_t capacity_;

  // mutex_ protects the following state.
  // 需要互斥锁来保证多个线程同时访问cache
  mutable port::Mutex mutex_;
  // 获取当前LRUCache已经使用的内存空间。
  size_t usage_ GUARDED_BY(mutex_);

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // Entries have refs==1 and in_cache==true.
  // 表示缓存中的entry节点。entry节点的特点是refs==1且in_cache=true
  //lru.prev 是最新的节点, lru.next 是最老的节点.
  LRUHandle lru_ GUARDED_BY(mutex_);

  // Dummy head of in-use list.
  // 即在缓存中又被外部引用的节点。
  // Entries are in use by clients, and have refs >= 2 and in_cache==true.
  // 
  LRUHandle in_use_ GUARDED_BY(mutex_);
  //根据key快速获取某个节点.
  HandleTable table_ GUARDED_BY(mutex_);
};
//构造
LRUCache::LRUCache() : capacity_(0), usage_(0) {
  // Make empty circular linked lists.
  lru_.next = &lru_;
  lru_.prev = &lru_;
  in_use_.next = &in_use_;
  in_use_.prev = &in_use_;
}

/*
  析构函数:
  析构只在缓存中的节点，真正是否释放需要看Unref函数，
  由于lru_.next记录的是仅仅只在cache中的，因此直接从这个next节点开始拿数据就可以了
*/
LRUCache::~LRUCache() {
  assert(in_use_.next == &in_use_);  // Error if caller has an unreleased handle
  for (LRUHandle* e = lru_.next; e != &lru_;) {
    LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);  // Invariant of lru_ list.
    Unref(e);
    e = next;
  }
}

void LRUCache::Ref(LRUHandle* e) {
  //如果引用是1,而且在缓存中。如果引用计数再加一的话，说明外部有引用，这是就要把节点从lru_ list移动到in_use_ list里
  if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
    LRU_Remove(e);
    LRU_Append(&in_use_, e);
  }
  e->refs++;
}

void LRUCache::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  //只有refs为零的时候，才会真正的去释放节点，free()是真正的释放节点空间。
  if (e->refs == 0) {  // Deallocate.
    assert(!e->in_cache);
    (*e->deleter)(e->key(), e->value);
    free(e);
  } else if (e->in_cache && e->refs == 1) {
    // No longer in use; move to lru_ list.
    //如果不是零,那么就仅仅把节点保存在缓存中，此时需要将其从
    //in_use中删除,然后放到lru中
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  }
}

void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
  // Make "e" newest entry by inserting just before *list
  e->next = list;
  e->prev = list->prev;
  e->prev->next = e;
  e->next->prev = e;
}

Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != nullptr) {
    Ref(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

void LRUCache::Release(Cache::Handle* handle) {
  MutexLock l(&mutex_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

Cache::Handle* LRUCache::Insert(const Slice& key, uint32_t hash, void* value,
                                size_t charge,
                                void (*deleter)(const Slice& key,
                                                void* value)) {
  //需要加锁                                                   
  MutexLock l(&mutex_);
  //创建节点对象
  LRUHandle* e =
      reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));
  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->in_cache = false;
  //这里是1，因为该节点会返回给调用者
  e->refs = 1;  // for the returned handle.
  std::memcpy(e->key_data, key.data(), key.size());
  //容量大于0，所以开启缓存模式
  if (capacity_ > 0) {
    //这里加一的原因是：该节点会放入到缓存中，所以会放到两个地方
    e->refs++;  // for the cache's reference.
    e->in_cache = true;
    //该节点会被外部使用，所以节点应该在in_use链上。
    LRU_Append(&in_use_, e);
    //加上新增的字节数。
    usage_ += charge;
    //这里为什么有FinishErase呢？是因为如果已经存在了这个节点，那么需要将老的节点给释放掉。
    //FinishErase并不是直接删除掉，而是把引用计数变为0。
    FinishErase(table_.Insert(e));
  } else {  // don't cache. (capacity_==0 is supported and turns off caching.)
    // next is read by key() in an assert, so it must be initialized
    e->next = nullptr;
  }
  //当cache容量不够，而且有空余的节点时候需要进行lru策略进行淘汰
  //这里需要注意的是对于那些in_use中的节点是不能被淘汰的，因为他们有外界使用。
  while (usage_ > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;//lru_.next是老节点，轮询释放
    assert(old->refs == 1);
    bool erased = FinishErase(table_.Remove(old->key(), old->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

// If e != nullptr, finish removing *e from the cache; it has already been
// removed from the hash table.  Return whether e != nullptr.
bool LRUCache::FinishErase(LRUHandle* e) {
  if (e != nullptr) {
    assert(e->in_cache);
    //从 in-use list里面把e移除掉。
    LRU_Remove(e);
    e->in_cache = false;//标记不在缓存里。
    usage_ -= e->charge;//减轻e的大小。
    //减少引用
    Unref(e);
  }
  return e != nullptr;
}

void LRUCache::Erase(const Slice& key, uint32_t hash) {
  MutexLock l(&mutex_);
  FinishErase(table_.Remove(key, hash));
}
//手动检测是否有需要删除的节点，发生在节点超过容量以后
void LRUCache::Prune() {
  MutexLock l(&mutex_);
  while (lru_.next != &lru_) {
    LRUHandle* e = lru_.next;
    assert(e->refs == 1);
    bool erased = FinishErase(table_.Remove(e->key(), e->hash));
    if (!erased) {  // to avoid unused variable when compiled NDEBUG
      assert(erased);
    }
  }
}

static const int kNumShardBits = 4;
static const int kNumShards = 1 << kNumShardBits;
/* 对 LRUCache 的进一步封装和优化。
  LRUCache的锁竞争太激烈了。为了降低锁的竞争和提升性能。
  ShardedLRUCache通过分片，在片的内部进行互斥锁。
*/
class ShardedLRUCache : public Cache {
 private:
  LRUCache shard_[kNumShards];//默认16个shard，16个LRUCache组成的数组。
  port::Mutex id_mutex_;//锁id
  uint64_t last_id_;//

  static inline uint32_t HashSlice(const Slice& s) {
    return Hash(s.data(), s.size(), 0);
  }
  //通过hash值算出entry在哪个shard
  static uint32_t Shard(uint32_t hash) { return hash >> (32 - kNumShardBits); }

 public:
  explicit ShardedLRUCache(size_t capacity) : last_id_(0) {
    const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].SetCapacity(per_shard);
    }
  }
  ~ShardedLRUCache() override {}
  // 把一个sst的缓存插入到table cache里
  Handle* Insert(const Slice& key, void* value, size_t charge,
                 void (*deleter)(const Slice& key, void* value)) override {
    //通过hash计算哪个分片
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }
  Handle* Lookup(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }
  void Release(Handle* handle) override {
    LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
    shard_[Shard(h->hash)].Release(handle);
  }
  void Erase(const Slice& key) override {
    const uint32_t hash = HashSlice(key);
    shard_[Shard(hash)].Erase(key, hash);
  }
  void* Value(Handle* handle) override {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }
  uint64_t NewId() override {
    MutexLock l(&id_mutex_);
    return ++(last_id_);
  }
  void Prune() override {
    for (int s = 0; s < kNumShards; s++) {
      shard_[s].Prune();
    }
  }
  size_t TotalCharge() const override {
    size_t total = 0;
    for (int s = 0; s < kNumShards; s++) {
      total += shard_[s].TotalCharge();
    }
    return total;
  }
};

}  // end anonymous namespace

Cache* NewLRUCache(size_t capacity) { return new ShardedLRUCache(capacity); }

}  // namespace leveldb
