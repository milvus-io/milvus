//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
#ifndef ROCKSDB_LITE

#include "utilities/persistent_cache/block_cache_tier_file.h"

#ifndef OS_WIN
#include <unistd.h>
#endif
#include <functional>
#include <memory>
#include <vector>

#include "port/port.h"
#include "util/crc32c.h"
#include "util/logging.h"

namespace rocksdb {

//
// File creation factories
//
Status NewWritableCacheFile(Env* const env, const std::string& filepath,
                            std::unique_ptr<WritableFile>* file,
                            const bool use_direct_writes = false) {
  EnvOptions opt;
  opt.use_direct_writes = use_direct_writes;
  Status s = env->NewWritableFile(filepath, file, opt);
  return s;
}

Status NewRandomAccessCacheFile(Env* const env, const std::string& filepath,
                                std::unique_ptr<RandomAccessFile>* file,
                                const bool use_direct_reads = true) {
  EnvOptions opt;
  opt.use_direct_reads = use_direct_reads;
  Status s = env->NewRandomAccessFile(filepath, file, opt);
  return s;
}

//
// BlockCacheFile
//
Status BlockCacheFile::Delete(uint64_t* size) {
  Status status = env_->GetFileSize(Path(), size);
  if (!status.ok()) {
    return status;
  }
  return env_->DeleteFile(Path());
}

//
// CacheRecord
//
// Cache record represents the record on disk
//
// +--------+---------+----------+------------+---------------+-------------+
// | magic  | crc     | key size | value size | key data      | value data  |
// +--------+---------+----------+------------+---------------+-------------+
// <-- 4 --><-- 4  --><-- 4   --><-- 4     --><-- key size  --><-- v-size -->
//
struct CacheRecordHeader {
  CacheRecordHeader()
    : magic_(0), crc_(0), key_size_(0), val_size_(0) {}
  CacheRecordHeader(const uint32_t magic, const uint32_t key_size,
                    const uint32_t val_size)
      : magic_(magic), crc_(0), key_size_(key_size), val_size_(val_size) {}

  uint32_t magic_;
  uint32_t crc_;
  uint32_t key_size_;
  uint32_t val_size_;
};

struct CacheRecord {
  CacheRecord() {}
  CacheRecord(const Slice& key, const Slice& val)
      : hdr_(MAGIC, static_cast<uint32_t>(key.size()),
             static_cast<uint32_t>(val.size())),
        key_(key),
        val_(val) {
    hdr_.crc_ = ComputeCRC();
  }

  uint32_t ComputeCRC() const;
  bool Serialize(std::vector<CacheWriteBuffer*>* bufs, size_t* woff);
  bool Deserialize(const Slice& buf);

  static uint32_t CalcSize(const Slice& key, const Slice& val) {
    return static_cast<uint32_t>(sizeof(CacheRecordHeader) + key.size() +
                                 val.size());
  }

  static const uint32_t MAGIC = 0xfefa;

  bool Append(std::vector<CacheWriteBuffer*>* bufs, size_t* woff,
              const char* data, const size_t size);

  CacheRecordHeader hdr_;
  Slice key_;
  Slice val_;
};

static_assert(sizeof(CacheRecordHeader) == 16, "DataHeader is not aligned");

uint32_t CacheRecord::ComputeCRC() const {
  uint32_t crc = 0;
  CacheRecordHeader tmp = hdr_;
  tmp.crc_ = 0;
  crc = crc32c::Extend(crc, reinterpret_cast<const char*>(&tmp), sizeof(tmp));
  crc = crc32c::Extend(crc, reinterpret_cast<const char*>(key_.data()),
                       key_.size());
  crc = crc32c::Extend(crc, reinterpret_cast<const char*>(val_.data()),
                       val_.size());
  return crc;
}

bool CacheRecord::Serialize(std::vector<CacheWriteBuffer*>* bufs,
                            size_t* woff) {
  assert(bufs->size());
  return Append(bufs, woff, reinterpret_cast<const char*>(&hdr_),
                sizeof(hdr_)) &&
         Append(bufs, woff, reinterpret_cast<const char*>(key_.data()),
                key_.size()) &&
         Append(bufs, woff, reinterpret_cast<const char*>(val_.data()),
                val_.size());
}

bool CacheRecord::Append(std::vector<CacheWriteBuffer*>* bufs, size_t* woff,
                         const char* data, const size_t data_size) {
  assert(*woff < bufs->size());

  const char* p = data;
  size_t size = data_size;

  while (size && *woff < bufs->size()) {
    CacheWriteBuffer* buf = (*bufs)[*woff];
    const size_t free = buf->Free();
    if (size <= free) {
      buf->Append(p, size);
      size = 0;
    } else {
      buf->Append(p, free);
      p += free;
      size -= free;
      assert(!buf->Free());
      assert(buf->Used() == buf->Capacity());
    }

    if (!buf->Free()) {
      *woff += 1;
    }
  }

  assert(!size);

  return !size;
}

bool CacheRecord::Deserialize(const Slice& data) {
  assert(data.size() >= sizeof(CacheRecordHeader));
  if (data.size() < sizeof(CacheRecordHeader)) {
    return false;
  }

  memcpy(&hdr_, data.data(), sizeof(hdr_));

  assert(hdr_.key_size_ + hdr_.val_size_ + sizeof(hdr_) == data.size());
  if (hdr_.key_size_ + hdr_.val_size_ + sizeof(hdr_) != data.size()) {
    return false;
  }

  key_ = Slice(data.data_ + sizeof(hdr_), hdr_.key_size_);
  val_ = Slice(key_.data_ + hdr_.key_size_, hdr_.val_size_);

  if (!(hdr_.magic_ == MAGIC && ComputeCRC() == hdr_.crc_)) {
    fprintf(stderr, "** magic %d ** \n", hdr_.magic_);
    fprintf(stderr, "** key_size %d ** \n", hdr_.key_size_);
    fprintf(stderr, "** val_size %d ** \n", hdr_.val_size_);
    fprintf(stderr, "** key %s ** \n", key_.ToString().c_str());
    fprintf(stderr, "** val %s ** \n", val_.ToString().c_str());
    for (size_t i = 0; i < hdr_.val_size_; ++i) {
      fprintf(stderr, "%d.", (uint8_t)val_.data()[i]);
    }
    fprintf(stderr, "\n** cksum %d != %d **", hdr_.crc_, ComputeCRC());
  }

  assert(hdr_.magic_ == MAGIC && ComputeCRC() == hdr_.crc_);
  return hdr_.magic_ == MAGIC && ComputeCRC() == hdr_.crc_;
}

//
// RandomAccessFile
//

bool RandomAccessCacheFile::Open(const bool enable_direct_reads) {
  WriteLock _(&rwlock_);
  return OpenImpl(enable_direct_reads);
}

bool RandomAccessCacheFile::OpenImpl(const bool enable_direct_reads) {
  rwlock_.AssertHeld();

  ROCKS_LOG_DEBUG(log_, "Opening cache file %s", Path().c_str());

  std::unique_ptr<RandomAccessFile> file;
  Status status =
      NewRandomAccessCacheFile(env_, Path(), &file, enable_direct_reads);
  if (!status.ok()) {
    Error(log_, "Error opening random access file %s. %s", Path().c_str(),
          status.ToString().c_str());
    return false;
  }
  freader_.reset(new RandomAccessFileReader(std::move(file), Path(), env_));

  return true;
}

bool RandomAccessCacheFile::Read(const LBA& lba, Slice* key, Slice* val,
                                 char* scratch) {
  ReadLock _(&rwlock_);

  assert(lba.cache_id_ == cache_id_);

  if (!freader_) {
    return false;
  }

  Slice result;
  Status s = freader_->Read(lba.off_, lba.size_, &result, scratch);
  if (!s.ok()) {
    Error(log_, "Error reading from file %s. %s", Path().c_str(),
          s.ToString().c_str());
    return false;
  }

  assert(result.data() == scratch);

  return ParseRec(lba, key, val, scratch);
}

bool RandomAccessCacheFile::ParseRec(const LBA& lba, Slice* key, Slice* val,
                                     char* scratch) {
  Slice data(scratch, lba.size_);

  CacheRecord rec;
  if (!rec.Deserialize(data)) {
    assert(!"Error deserializing data");
    Error(log_, "Error de-serializing record from file %s off %d",
          Path().c_str(), lba.off_);
    return false;
  }

  *key = Slice(rec.key_);
  *val = Slice(rec.val_);

  return true;
}

//
// WriteableCacheFile
//

WriteableCacheFile::~WriteableCacheFile() {
  WriteLock _(&rwlock_);
  if (!eof_) {
    // This file never flushed. We give priority to shutdown since this is a
    // cache
    // TODO(krad): Figure a way to flush the pending data
    if (file_) {
      assert(refs_ == 1);
      --refs_;
    }
  }
  assert(!refs_);
  ClearBuffers();
}

bool WriteableCacheFile::Create(const bool /*enable_direct_writes*/,
                                const bool enable_direct_reads) {
  WriteLock _(&rwlock_);

  enable_direct_reads_ = enable_direct_reads;

  ROCKS_LOG_DEBUG(log_, "Creating new cache %s (max size is %d B)",
                  Path().c_str(), max_size_);

  Status s = env_->FileExists(Path());
  if (s.ok()) {
    ROCKS_LOG_WARN(log_, "File %s already exists. %s", Path().c_str(),
                   s.ToString().c_str());
  }

  s = NewWritableCacheFile(env_, Path(), &file_);
  if (!s.ok()) {
    ROCKS_LOG_WARN(log_, "Unable to create file %s. %s", Path().c_str(),
                   s.ToString().c_str());
    return false;
  }

  assert(!refs_);
  ++refs_;

  return true;
}

bool WriteableCacheFile::Append(const Slice& key, const Slice& val, LBA* lba) {
  WriteLock _(&rwlock_);

  if (eof_) {
    // We can't append since the file is full
    return false;
  }

  // estimate the space required to store the (key, val)
  uint32_t rec_size = CacheRecord::CalcSize(key, val);

  if (!ExpandBuffer(rec_size)) {
    // unable to expand the buffer
    ROCKS_LOG_DEBUG(log_, "Error expanding buffers. size=%d", rec_size);
    return false;
  }

  lba->cache_id_ = cache_id_;
  lba->off_ = disk_woff_;
  lba->size_ = rec_size;

  CacheRecord rec(key, val);
  if (!rec.Serialize(&bufs_, &buf_woff_)) {
    // unexpected error: unable to serialize the data
    assert(!"Error serializing record");
    return false;
  }

  disk_woff_ += rec_size;
  eof_ = disk_woff_ >= max_size_;

  // dispatch buffer for flush
  DispatchBuffer();

  return true;
}

bool WriteableCacheFile::ExpandBuffer(const size_t size) {
  rwlock_.AssertHeld();
  assert(!eof_);

  // determine if there is enough space
  size_t free = 0;  // compute the free space left in buffer
  for (size_t i = buf_woff_; i < bufs_.size(); ++i) {
    free += bufs_[i]->Free();
    if (size <= free) {
      // we have enough space in the buffer
      return true;
    }
  }

  // expand the buffer until there is enough space to write `size` bytes
  assert(free < size);
  while (free < size) {
    CacheWriteBuffer* const buf = alloc_->Allocate();
    if (!buf) {
      ROCKS_LOG_DEBUG(log_, "Unable to allocate buffers");
      return false;
    }

    size_ += static_cast<uint32_t>(buf->Free());
    free += buf->Free();
    bufs_.push_back(buf);
  }

  assert(free >= size);
  return true;
}

void WriteableCacheFile::DispatchBuffer() {
  rwlock_.AssertHeld();

  assert(bufs_.size());
  assert(buf_doff_ <= buf_woff_);
  assert(buf_woff_ <= bufs_.size());

  if (pending_ios_) {
    return;
  }

  if (!eof_ && buf_doff_ == buf_woff_) {
    // dispatch buffer is pointing to write buffer and we haven't hit eof
    return;
  }

  assert(eof_ || buf_doff_ < buf_woff_);
  assert(buf_doff_ < bufs_.size());
  assert(file_);

  auto* buf = bufs_[buf_doff_];
  const uint64_t file_off = buf_doff_ * alloc_->BufferSize();

  assert(!buf->Free() ||
         (eof_ && buf_doff_ == buf_woff_ && buf_woff_ < bufs_.size()));
  // we have reached end of file, and there is space in the last buffer
  // pad it with zero for direct IO
  buf->FillTrailingZeros();

  assert(buf->Used() % kFileAlignmentSize == 0);

  writer_->Write(file_.get(), buf, file_off,
                 std::bind(&WriteableCacheFile::BufferWriteDone, this));
  pending_ios_++;
  buf_doff_++;
}

void WriteableCacheFile::BufferWriteDone() {
  WriteLock _(&rwlock_);

  assert(bufs_.size());

  pending_ios_--;

  if (buf_doff_ < bufs_.size()) {
    DispatchBuffer();
  }

  if (eof_ && buf_doff_ >= bufs_.size() && !pending_ios_) {
    // end-of-file reached, move to read mode
    CloseAndOpenForReading();
  }
}

void WriteableCacheFile::CloseAndOpenForReading() {
  // Our env abstraction do not allow reading from a file opened for appending
  // We need close the file and re-open it for reading
  Close();
  RandomAccessCacheFile::OpenImpl(enable_direct_reads_);
}

bool WriteableCacheFile::ReadBuffer(const LBA& lba, Slice* key, Slice* block,
                                    char* scratch) {
  rwlock_.AssertHeld();

  if (!ReadBuffer(lba, scratch)) {
    Error(log_, "Error reading from buffer. cache=%d off=%d", cache_id_,
          lba.off_);
    return false;
  }

  return ParseRec(lba, key, block, scratch);
}

bool WriteableCacheFile::ReadBuffer(const LBA& lba, char* data) {
  rwlock_.AssertHeld();

  assert(lba.off_ < disk_woff_);

  // we read from the buffers like reading from a flat file. The list of buffers
  // are treated as contiguous stream of data

  char* tmp = data;
  size_t pending_nbytes = lba.size_;
  // start buffer
  size_t start_idx = lba.off_ / alloc_->BufferSize();
  // offset into the start buffer
  size_t start_off = lba.off_ % alloc_->BufferSize();

  assert(start_idx <= buf_woff_);

  for (size_t i = start_idx; pending_nbytes && i < bufs_.size(); ++i) {
    assert(i <= buf_woff_);
    auto* buf = bufs_[i];
    assert(i == buf_woff_ || !buf->Free());
    // bytes to write to the buffer
    size_t nbytes = pending_nbytes > (buf->Used() - start_off)
                        ? (buf->Used() - start_off)
                        : pending_nbytes;
    memcpy(tmp, buf->Data() + start_off, nbytes);

    // left over to be written
    pending_nbytes -= nbytes;
    start_off = 0;
    tmp += nbytes;
  }

  assert(!pending_nbytes);
  if (pending_nbytes) {
    return false;
  }

  assert(tmp == data + lba.size_);
  return true;
}

void WriteableCacheFile::Close() {
  rwlock_.AssertHeld();

  assert(size_ >= max_size_);
  assert(disk_woff_ >= max_size_);
  assert(buf_doff_ == bufs_.size());
  assert(bufs_.size() - buf_woff_ <= 1);
  assert(!pending_ios_);

  Info(log_, "Closing file %s. size=%d written=%d", Path().c_str(), size_,
       disk_woff_);

  ClearBuffers();
  file_.reset();

  assert(refs_);
  --refs_;
}

void WriteableCacheFile::ClearBuffers() {
  for (size_t i = 0; i < bufs_.size(); ++i) {
    alloc_->Deallocate(bufs_[i]);
  }

  bufs_.clear();
}

//
// ThreadedFileWriter implementation
//
ThreadedWriter::ThreadedWriter(PersistentCacheTier* const cache,
                               const size_t qdepth, const size_t io_size)
    : Writer(cache), io_size_(io_size) {
  for (size_t i = 0; i < qdepth; ++i) {
    port::Thread th(&ThreadedWriter::ThreadMain, this);
    threads_.push_back(std::move(th));
  }
}

void ThreadedWriter::Stop() {
  // notify all threads to exit
  for (size_t i = 0; i < threads_.size(); ++i) {
    q_.Push(IO(/*signal=*/true));
  }

  // wait for all threads to exit
  for (auto& th : threads_) {
    th.join();
    assert(!th.joinable());
  }
  threads_.clear();
}

void ThreadedWriter::Write(WritableFile* const file, CacheWriteBuffer* buf,
                           const uint64_t file_off,
                           const std::function<void()> callback) {
  q_.Push(IO(file, buf, file_off, callback));
}

void ThreadedWriter::ThreadMain() {
  while (true) {
    // Fetch the IO to process
    IO io(q_.Pop());
    if (io.signal_) {
      // that's secret signal to exit
      break;
    }

    // Reserve space for writing the buffer
    while (!cache_->Reserve(io.buf_->Used())) {
      // We can fail to reserve space if every file in the system
      // is being currently accessed
      /* sleep override */
      Env::Default()->SleepForMicroseconds(1000000);
    }

    DispatchIO(io);

    io.callback_();
  }
}

void ThreadedWriter::DispatchIO(const IO& io) {
  size_t written = 0;
  while (written < io.buf_->Used()) {
    Slice data(io.buf_->Data() + written, io_size_);
    Status s = io.file_->Append(data);
    assert(s.ok());
    if (!s.ok()) {
      // That is definite IO error to device. There is not much we can
      // do but ignore the failure. This can lead to corruption of data on
      // disk, but the cache will skip while reading
      fprintf(stderr, "Error writing data to file. %s\n", s.ToString().c_str());
    }
    written += io_size_;
  }
}

}  // namespace rocksdb

#endif
