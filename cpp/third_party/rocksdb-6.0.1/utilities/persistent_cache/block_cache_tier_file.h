//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#pragma once

#ifndef ROCKSDB_LITE

#include <list>
#include <memory>
#include <string>
#include <vector>

#include "rocksdb/comparator.h"
#include "rocksdb/env.h"

#include "utilities/persistent_cache/block_cache_tier_file_buffer.h"
#include "utilities/persistent_cache/lrulist.h"
#include "utilities/persistent_cache/persistent_cache_tier.h"
#include "utilities/persistent_cache/persistent_cache_util.h"

#include "port/port.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"
#include "util/mutexlock.h"

// The io code path of persistent cache uses pipelined architecture
//
// client -> In Queue <-- BlockCacheTier --> Out Queue <-- Writer <--> Kernel
//
// This would enable the system to scale for GB/s of throughput which is
// expected with modern devies like NVM.
//
// The file level operations are encapsulated in the following abstractions
//
// BlockCacheFile
//       ^
//       |
//       |
// RandomAccessCacheFile (For reading)
//       ^
//       |
//       |
// WriteableCacheFile (For writing)
//
// Write IO code path :
//
namespace rocksdb {

class WriteableCacheFile;
struct BlockInfo;

// Represents a logical record on device
//
// (L)ogical (B)lock (Address = { cache-file-id, offset, size }
struct LogicalBlockAddress {
  LogicalBlockAddress() {}
  explicit LogicalBlockAddress(const uint32_t cache_id, const uint32_t off,
                               const uint16_t size)
      : cache_id_(cache_id), off_(off), size_(size) {}

  uint32_t cache_id_ = 0;
  uint32_t off_ = 0;
  uint32_t size_ = 0;
};

typedef LogicalBlockAddress LBA;

// class Writer
//
// Writer is the abstraction used for writing data to file. The component can be
// multithreaded. It is the last step of write pipeline
class Writer {
 public:
  explicit Writer(PersistentCacheTier* const cache) : cache_(cache) {}
  virtual ~Writer() {}

  // write buffer to file at the given offset
  virtual void Write(WritableFile* const file, CacheWriteBuffer* buf,
                     const uint64_t file_off,
                     const std::function<void()> callback) = 0;
  // stop the writer
  virtual void Stop() = 0;

  PersistentCacheTier* const cache_;
};

// class BlockCacheFile
//
// Generic interface to support building file specialized for read/writing
class BlockCacheFile : public LRUElement<BlockCacheFile> {
 public:
  explicit BlockCacheFile(const uint32_t cache_id)
      : LRUElement<BlockCacheFile>(), cache_id_(cache_id) {}

  explicit BlockCacheFile(Env* const env, const std::string& dir,
                          const uint32_t cache_id)
      : LRUElement<BlockCacheFile>(),
        env_(env),
        dir_(dir),
        cache_id_(cache_id) {}

  virtual ~BlockCacheFile() {}

  // append key/value to file and return LBA locator to user
  virtual bool Append(const Slice& /*key*/, const Slice& /*val*/,
                      LBA* const /*lba*/) {
    assert(!"not implemented");
    return false;
  }

  // read from the record locator (LBA) and return key, value and status
  virtual bool Read(const LBA& /*lba*/, Slice* /*key*/, Slice* /*block*/,
                    char* /*scratch*/) {
    assert(!"not implemented");
    return false;
  }

  // get file path
  std::string Path() const {
    return dir_ + "/" + std::to_string(cache_id_) + ".rc";
  }
  // get cache ID
  uint32_t cacheid() const { return cache_id_; }
  // Add block information to file data
  // Block information is the list of index reference for this file
  virtual void Add(BlockInfo* binfo) {
    WriteLock _(&rwlock_);
    block_infos_.push_back(binfo);
  }
  // get block information
  std::list<BlockInfo*>& block_infos() { return block_infos_; }
  // delete file and return the size of the file
  virtual Status Delete(uint64_t* size);

 protected:
  port::RWMutex rwlock_;               // synchronization mutex
  Env* const env_ = nullptr;           // Env for IO
  const std::string dir_;              // Directory name
  const uint32_t cache_id_;            // Cache id for the file
  std::list<BlockInfo*> block_infos_;  // List of index entries mapping to the
                                       // file content
};

// class RandomAccessFile
//
// Thread safe implementation for reading random data from file
class RandomAccessCacheFile : public BlockCacheFile {
 public:
  explicit RandomAccessCacheFile(Env* const env, const std::string& dir,
                                 const uint32_t cache_id,
                                 const std::shared_ptr<Logger>& log)
      : BlockCacheFile(env, dir, cache_id), log_(log) {}

  virtual ~RandomAccessCacheFile() {}

  // open file for reading
  bool Open(const bool enable_direct_reads);
  // read data from the disk
  bool Read(const LBA& lba, Slice* key, Slice* block, char* scratch) override;

 private:
  std::unique_ptr<RandomAccessFileReader> freader_;

 protected:
  bool OpenImpl(const bool enable_direct_reads);
  bool ParseRec(const LBA& lba, Slice* key, Slice* val, char* scratch);

  std::shared_ptr<Logger> log_;  // log file
};

// class WriteableCacheFile
//
// All writes to the files are cached in buffers. The buffers are flushed to
// disk as they get filled up. When file size reaches a certain size, a new file
// will be created provided there is free space
class WriteableCacheFile : public RandomAccessCacheFile {
 public:
  explicit WriteableCacheFile(Env* const env, CacheWriteBufferAllocator* alloc,
                              Writer* writer, const std::string& dir,
                              const uint32_t cache_id, const uint32_t max_size,
                              const std::shared_ptr<Logger>& log)
      : RandomAccessCacheFile(env, dir, cache_id, log),
        alloc_(alloc),
        writer_(writer),
        max_size_(max_size) {}

  virtual ~WriteableCacheFile();

  // create file on disk
  bool Create(const bool enable_direct_writes, const bool enable_direct_reads);

  // read data from logical file
  bool Read(const LBA& lba, Slice* key, Slice* block, char* scratch) override {
    ReadLock _(&rwlock_);
    const bool closed = eof_ && bufs_.empty();
    if (closed) {
      // the file is closed, read from disk
      return RandomAccessCacheFile::Read(lba, key, block, scratch);
    }
    // file is still being written, read from buffers
    return ReadBuffer(lba, key, block, scratch);
  }

  // append data to end of file
  bool Append(const Slice&, const Slice&, LBA* const) override;
  // End-of-file
  bool Eof() const { return eof_; }

 private:
  friend class ThreadedWriter;

  static const size_t kFileAlignmentSize = 4 * 1024;  // align file size

  bool ReadBuffer(const LBA& lba, Slice* key, Slice* block, char* scratch);
  bool ReadBuffer(const LBA& lba, char* data);
  bool ExpandBuffer(const size_t size);
  void DispatchBuffer();
  void BufferWriteDone();
  void CloseAndOpenForReading();
  void ClearBuffers();
  void Close();

  // File layout in memory
  //
  // +------+------+------+------+------+------+
  // | b0   | b1   | b2   | b3   | b4   | b5   |
  // +------+------+------+------+------+------+
  //        ^                           ^
  //        |                           |
  //      buf_doff_                   buf_woff_
  //   (next buffer to           (next buffer to fill)
  //   flush to disk)
  //
  //  The buffers are flushed to disk serially for a given file

  CacheWriteBufferAllocator* const alloc_ = nullptr;  // Buffer provider
  Writer* const writer_ = nullptr;                    // File writer thread
  std::unique_ptr<WritableFile> file_;   // RocksDB Env file abstraction
  std::vector<CacheWriteBuffer*> bufs_;  // Written buffers
  uint32_t size_ = 0;                    // Size of the file
  const uint32_t max_size_;              // Max size of the file
  bool eof_ = false;                     // End of file
  uint32_t disk_woff_ = 0;               // Offset to write on disk
  size_t buf_woff_ = 0;                  // off into bufs_ to write
  size_t buf_doff_ = 0;                  // off into bufs_ to dispatch
  size_t pending_ios_ = 0;               // Number of ios to disk in-progress
  bool enable_direct_reads_ = false;     // Should we enable direct reads
                                         // when reading from disk
};

//
// Abstraction to do writing to device. It is part of pipelined architecture.
//
class ThreadedWriter : public Writer {
 public:
  // Representation of IO to device
  struct IO {
    explicit IO(const bool signal) : signal_(signal) {}
    explicit IO(WritableFile* const file, CacheWriteBuffer* const buf,
                const uint64_t file_off, const std::function<void()> callback)
        : file_(file), buf_(buf), file_off_(file_off), callback_(callback) {}

    IO(const IO&) = default;
    IO& operator=(const IO&) = default;
    size_t Size() const { return sizeof(IO); }

    WritableFile* file_ = nullptr;           // File to write to
    CacheWriteBuffer* const buf_ = nullptr;  // buffer to write
    uint64_t file_off_ = 0;                  // file offset
    bool signal_ = false;                    // signal to exit thread loop
    std::function<void()> callback_;         // Callback on completion
  };

  explicit ThreadedWriter(PersistentCacheTier* const cache, const size_t qdepth,
                          const size_t io_size);
  virtual ~ThreadedWriter() { assert(threads_.empty()); }

  void Stop() override;
  void Write(WritableFile* const file, CacheWriteBuffer* buf,
             const uint64_t file_off,
             const std::function<void()> callback) override;

 private:
  void ThreadMain();
  void DispatchIO(const IO& io);

  const size_t io_size_ = 0;
  BoundedQueue<IO> q_;
  std::vector<port::Thread> threads_;
};

}  // namespace rocksdb

#endif
