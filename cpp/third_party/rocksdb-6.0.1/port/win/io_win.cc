//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "port/win/io_win.h"

#include "monitoring/iostats_context_imp.h"
#include "util/aligned_buffer.h"
#include "util/coding.h"
#include "util/sync_point.h"

namespace rocksdb {
namespace port {

/*
* DirectIOHelper
*/
namespace {

const size_t kSectorSize = 512;

inline
bool IsPowerOfTwo(const size_t alignment) {
  return ((alignment) & (alignment - 1)) == 0;
}

inline
bool IsSectorAligned(const size_t off) {
  return (off & (kSectorSize - 1)) == 0;
}

inline
bool IsAligned(size_t alignment, const void* ptr) {
  return ((uintptr_t(ptr)) & (alignment - 1)) == 0;
}
}


std::string GetWindowsErrSz(DWORD err) {
  LPSTR lpMsgBuf;
  FormatMessageA(FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM |
    FORMAT_MESSAGE_IGNORE_INSERTS,
    NULL, err,
    0,  // Default language
    reinterpret_cast<LPSTR>(&lpMsgBuf), 0, NULL);

  std::string Err = lpMsgBuf;
  LocalFree(lpMsgBuf);
  return Err;
}

// We preserve the original name of this interface to denote the original idea
// behind it.
// All reads happen by a specified offset and pwrite interface does not change
// the position of the file pointer. Judging from the man page and errno it does
// execute
// lseek atomically to return the position of the file back where it was.
// WriteFile() does not
// have this capability. Therefore, for both pread and pwrite the pointer is
// advanced to the next position
// which is fine for writes because they are (should be) sequential.
// Because all the reads/writes happen by the specified offset, the caller in
// theory should not
// rely on the current file offset.
Status pwrite(const WinFileData* file_data, const Slice& data,
  uint64_t offset, size_t& bytes_written) {

  Status s;
  bytes_written = 0;

  size_t num_bytes = data.size();
  if (num_bytes > std::numeric_limits<DWORD>::max()) {
    // May happen in 64-bit builds where size_t is 64-bits but
    // long is still 32-bit, but that's the API here at the moment
    return Status::InvalidArgument("num_bytes is too large for a single write: " +
          file_data->GetName());
  }

  OVERLAPPED overlapped = { 0 };
  ULARGE_INTEGER offsetUnion;
  offsetUnion.QuadPart = offset;

  overlapped.Offset = offsetUnion.LowPart;
  overlapped.OffsetHigh = offsetUnion.HighPart;

  DWORD bytesWritten = 0;

  if (FALSE == WriteFile(file_data->GetFileHandle(), data.data(), static_cast<DWORD>(num_bytes),
    &bytesWritten, &overlapped)) {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError("WriteFile failed: " + file_data->GetName(),
      lastError);
  } else {
    bytes_written = bytesWritten;
  }

  return s;
}

// See comments for pwrite above
Status pread(const WinFileData* file_data, char* src, size_t num_bytes,
  uint64_t offset, size_t& bytes_read) {

  Status s;
  bytes_read = 0;

  if (num_bytes > std::numeric_limits<DWORD>::max()) {
    return Status::InvalidArgument("num_bytes is too large for a single read: " +
      file_data->GetName());
  }

  OVERLAPPED overlapped = { 0 };
  ULARGE_INTEGER offsetUnion;
  offsetUnion.QuadPart = offset;

  overlapped.Offset = offsetUnion.LowPart;
  overlapped.OffsetHigh = offsetUnion.HighPart;

  DWORD bytesRead = 0;

  if (FALSE == ReadFile(file_data->GetFileHandle(), src, static_cast<DWORD>(num_bytes),
    &bytesRead, &overlapped)) {
    auto lastError = GetLastError();
    // EOF is OK with zero bytes read
    if (lastError != ERROR_HANDLE_EOF) {
      s = IOErrorFromWindowsError("ReadFile failed: " + file_data->GetName(),
        lastError);
    }
  } else {
    bytes_read = bytesRead;
  }

  return s;
}

// SetFileInformationByHandle() is capable of fast pre-allocates.
// However, this does not change the file end position unless the file is
// truncated and the pre-allocated space is not considered filled with zeros.
Status fallocate(const std::string& filename, HANDLE hFile,
  uint64_t to_size) {
  Status status;

  FILE_ALLOCATION_INFO alloc_info;
  alloc_info.AllocationSize.QuadPart = to_size;

  if (!SetFileInformationByHandle(hFile, FileAllocationInfo, &alloc_info,
    sizeof(FILE_ALLOCATION_INFO))) {
    auto lastError = GetLastError();
    status = IOErrorFromWindowsError(
      "Failed to pre-allocate space: " + filename, lastError);
  }

  return status;
}

Status ftruncate(const std::string& filename, HANDLE hFile,
  uint64_t toSize) {
  Status status;

  FILE_END_OF_FILE_INFO end_of_file;
  end_of_file.EndOfFile.QuadPart = toSize;

  if (!SetFileInformationByHandle(hFile, FileEndOfFileInfo, &end_of_file,
    sizeof(FILE_END_OF_FILE_INFO))) {
    auto lastError = GetLastError();
    status = IOErrorFromWindowsError("Failed to Set end of file: " + filename,
      lastError);
  }

  return status;
}

size_t GetUniqueIdFromFile(HANDLE hFile, char* id, size_t max_size) {

  if (max_size < kMaxVarint64Length * 3) {
    return 0;
  }
#if (_WIN32_WINNT == _WIN32_WINNT_VISTA)
  // MINGGW as defined by CMake file.
  // yuslepukhin: I hate the guts of the above macros.
  // This impl does not guarantee uniqueness everywhere
  // is reasonably good
  BY_HANDLE_FILE_INFORMATION FileInfo;

  BOOL result = GetFileInformationByHandle(hFile, &FileInfo);

  TEST_SYNC_POINT_CALLBACK("GetUniqueIdFromFile:FS_IOC_GETVERSION", &result);

  if (!result) {
    return 0;
  }

  char* rid = id;
  rid = EncodeVarint64(rid, uint64_t(FileInfo.dwVolumeSerialNumber));
  rid = EncodeVarint64(rid, uint64_t(FileInfo.nFileIndexHigh));
  rid = EncodeVarint64(rid, uint64_t(FileInfo.nFileIndexLow));

  assert(rid >= id);
  return static_cast<size_t>(rid - id);
#else
  FILE_ID_INFO FileInfo;
  BOOL result = GetFileInformationByHandleEx(hFile, FileIdInfo, &FileInfo,
    sizeof(FileInfo));

  TEST_SYNC_POINT_CALLBACK("GetUniqueIdFromFile:FS_IOC_GETVERSION", &result);

  if (!result) {
    return 0;
  }

  static_assert(sizeof(uint64_t) == sizeof(FileInfo.VolumeSerialNumber),
    "Wrong sizeof expectations");
  // FileId.Identifier is an array of 16 BYTEs, we encode them as two uint64_t
  static_assert(sizeof(uint64_t) * 2 == sizeof(FileInfo.FileId.Identifier),
    "Wrong sizeof expectations");

  char* rid = id;
  rid = EncodeVarint64(rid, uint64_t(FileInfo.VolumeSerialNumber));
  uint64_t* file_id = reinterpret_cast<uint64_t*>(&FileInfo.FileId.Identifier[0]);
  rid = EncodeVarint64(rid, *file_id);
  ++file_id;
  rid = EncodeVarint64(rid, *file_id);

  assert(rid >= id);
  return static_cast<size_t>(rid - id);
#endif
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// WinMmapReadableFile

WinMmapReadableFile::WinMmapReadableFile(const std::string& fileName,
                                         HANDLE hFile, HANDLE hMap,
                                         const void* mapped_region,
                                         size_t length)
    : WinFileData(fileName, hFile, false /* use_direct_io */),
      hMap_(hMap),
      mapped_region_(mapped_region),
      length_(length) {}

WinMmapReadableFile::~WinMmapReadableFile() {
  BOOL ret __attribute__((__unused__));
  ret = ::UnmapViewOfFile(mapped_region_);
  assert(ret);

  ret = ::CloseHandle(hMap_);
  assert(ret);
}

Status WinMmapReadableFile::Read(uint64_t offset, size_t n, Slice* result,
  char* scratch) const {
  Status s;

  if (offset > length_) {
    *result = Slice();
    return IOError(filename_, EINVAL);
  } else if (offset + n > length_) {
    n = length_ - static_cast<size_t>(offset);
  }
  *result =
    Slice(reinterpret_cast<const char*>(mapped_region_)+offset, n);
  return s;
}

Status WinMmapReadableFile::InvalidateCache(size_t offset, size_t length) {
  return Status::OK();
}

size_t WinMmapReadableFile::GetUniqueId(char* id, size_t max_size) const {
  return GetUniqueIdFromFile(hFile_, id, max_size);
}

///////////////////////////////////////////////////////////////////////////////
/// WinMmapFile


// Can only truncate or reserve to a sector size aligned if
// used on files that are opened with Unbuffered I/O
Status WinMmapFile::TruncateFile(uint64_t toSize) {
  return ftruncate(filename_, hFile_, toSize);
}

Status WinMmapFile::UnmapCurrentRegion() {
  Status status;

  if (mapped_begin_ != nullptr) {
    if (!::UnmapViewOfFile(mapped_begin_)) {
      status = IOErrorFromWindowsError(
        "Failed to unmap file view: " + filename_, GetLastError());
    }

    // Move on to the next portion of the file
    file_offset_ += view_size_;

    // UnmapView automatically sends data to disk but not the metadata
    // which is good and provides some equivalent of fdatasync() on Linux
    // therefore, we donot need separate flag for metadata
    mapped_begin_ = nullptr;
    mapped_end_ = nullptr;
    dst_ = nullptr;

    last_sync_ = nullptr;
    pending_sync_ = false;
  }

  return status;
}

Status WinMmapFile::MapNewRegion() {

  Status status;

  assert(mapped_begin_ == nullptr);

  size_t minDiskSize = static_cast<size_t>(file_offset_) + view_size_;

  if (minDiskSize > reserved_size_) {
    status = Allocate(file_offset_, view_size_);
    if (!status.ok()) {
      return status;
    }
  }

  // Need to remap
  if (hMap_ == NULL || reserved_size_ > mapping_size_) {

    if (hMap_ != NULL) {
      // Unmap the previous one
      BOOL ret __attribute__((__unused__));
      ret = ::CloseHandle(hMap_);
      assert(ret);
      hMap_ = NULL;
    }

    ULARGE_INTEGER mappingSize;
    mappingSize.QuadPart = reserved_size_;

    hMap_ = CreateFileMappingA(
      hFile_,
      NULL,                  // Security attributes
      PAGE_READWRITE,        // There is not a write only mode for mapping
      mappingSize.HighPart,  // Enable mapping the whole file but the actual
      // amount mapped is determined by MapViewOfFile
      mappingSize.LowPart,
      NULL);  // Mapping name

    if (NULL == hMap_) {
      return IOErrorFromWindowsError(
        "WindowsMmapFile failed to create file mapping for: " + filename_,
        GetLastError());
    }

    mapping_size_ = reserved_size_;
  }

  ULARGE_INTEGER offset;
  offset.QuadPart = file_offset_;

  // View must begin at the granularity aligned offset
  mapped_begin_ = reinterpret_cast<char*>(
    MapViewOfFileEx(hMap_, FILE_MAP_WRITE, offset.HighPart, offset.LowPart,
    view_size_, NULL));

  if (!mapped_begin_) {
    status = IOErrorFromWindowsError(
      "WindowsMmapFile failed to map file view: " + filename_,
      GetLastError());
  } else {
    mapped_end_ = mapped_begin_ + view_size_;
    dst_ = mapped_begin_;
    last_sync_ = mapped_begin_;
    pending_sync_ = false;
  }
  return status;
}

Status WinMmapFile::PreallocateInternal(uint64_t spaceToReserve) {
  return fallocate(filename_, hFile_, spaceToReserve);
}

WinMmapFile::WinMmapFile(const std::string& fname, HANDLE hFile, size_t page_size,
  size_t allocation_granularity, const EnvOptions& options)
  : WinFileData(fname, hFile, false),
  hMap_(NULL),
  page_size_(page_size),
  allocation_granularity_(allocation_granularity),
  reserved_size_(0),
  mapping_size_(0),
  view_size_(0),
  mapped_begin_(nullptr),
  mapped_end_(nullptr),
  dst_(nullptr),
  last_sync_(nullptr),
  file_offset_(0),
  pending_sync_(false) {
  // Allocation granularity must be obtained from GetSystemInfo() and must be
  // a power of two.
  assert(allocation_granularity > 0);
  assert((allocation_granularity & (allocation_granularity - 1)) == 0);

  assert(page_size > 0);
  assert((page_size & (page_size - 1)) == 0);

  // Only for memory mapped writes
  assert(options.use_mmap_writes);

  // View size must be both the multiple of allocation_granularity AND the
  // page size and the granularity is usually a multiple of a page size.
  const size_t viewSize = 32 * 1024; // 32Kb similar to the Windows File Cache in buffered mode
  view_size_ = Roundup(viewSize, allocation_granularity_);
}

WinMmapFile::~WinMmapFile() {
  if (hFile_) {
    this->Close();
  }
}

Status WinMmapFile::Append(const Slice& data) {
  const char* src = data.data();
  size_t left = data.size();

  while (left > 0) {
    assert(mapped_begin_ <= dst_);
    size_t avail = mapped_end_ - dst_;

    if (avail == 0) {
      Status s = UnmapCurrentRegion();
      if (s.ok()) {
        s = MapNewRegion();
      }

      if (!s.ok()) {
        return s;
      }
    } else {
      size_t n = std::min(left, avail);
      memcpy(dst_, src, n);
      dst_ += n;
      src += n;
      left -= n;
      pending_sync_ = true;
    }
  }

  // Now make sure that the last partial page is padded with zeros if needed
  size_t bytesToPad = Roundup(size_t(dst_), page_size_) - size_t(dst_);
  if (bytesToPad > 0) {
    memset(dst_, 0, bytesToPad);
  }

  return Status::OK();
}

// Means Close() will properly take care of truncate
// and it does not need any additional information
Status WinMmapFile::Truncate(uint64_t size) {
  return Status::OK();
}

Status WinMmapFile::Close() {
  Status s;

  assert(NULL != hFile_);

  // We truncate to the precise size so no
  // uninitialized data at the end. SetEndOfFile
  // which we use does not write zeros and it is good.
  uint64_t targetSize = GetFileSize();

  if (mapped_begin_ != nullptr) {
    // Sync before unmapping to make sure everything
    // is on disk and there is not a lazy writing
    // so we are deterministic with the tests
    Sync();
    s = UnmapCurrentRegion();
  }

  if (NULL != hMap_) {
    BOOL ret = ::CloseHandle(hMap_);
    if (!ret && s.ok()) {
      auto lastError = GetLastError();
      s = IOErrorFromWindowsError(
        "Failed to Close mapping for file: " + filename_, lastError);
    }

    hMap_ = NULL;
  }

  if (hFile_ != NULL) {

    TruncateFile(targetSize);

    BOOL ret = ::CloseHandle(hFile_);
    hFile_ = NULL;

    if (!ret && s.ok()) {
      auto lastError = GetLastError();
      s = IOErrorFromWindowsError(
        "Failed to close file map handle: " + filename_, lastError);
    }
  }

  return s;
}

Status WinMmapFile::Flush() { return Status::OK(); }

// Flush only data
Status WinMmapFile::Sync() {
  Status s;

  // Some writes occurred since last sync
  if (dst_ > last_sync_) {
    assert(mapped_begin_);
    assert(dst_);
    assert(dst_ > mapped_begin_);
    assert(dst_ < mapped_end_);

    size_t page_begin =
      TruncateToPageBoundary(page_size_, last_sync_ - mapped_begin_);
    size_t page_end =
      TruncateToPageBoundary(page_size_, dst_ - mapped_begin_ - 1);

    // Flush only the amount of that is a multiple of pages
    if (!::FlushViewOfFile(mapped_begin_ + page_begin,
      (page_end - page_begin) + page_size_)) {
      s = IOErrorFromWindowsError("Failed to FlushViewOfFile: " + filename_,
        GetLastError());
    } else {
      last_sync_ = dst_;
    }
  }

  return s;
}

/**
* Flush data as well as metadata to stable storage.
*/
Status WinMmapFile::Fsync() {
  Status s = Sync();

  // Flush metadata
  if (s.ok() && pending_sync_) {
    if (!::FlushFileBuffers(hFile_)) {
      s = IOErrorFromWindowsError("Failed to FlushFileBuffers: " + filename_,
        GetLastError());
    }
    pending_sync_ = false;
  }

  return s;
}

/**
* Get the size of valid data in the file. This will not match the
* size that is returned from the filesystem because we use mmap
* to extend file by map_size every time.
*/
uint64_t WinMmapFile::GetFileSize() {
  size_t used = dst_ - mapped_begin_;
  return file_offset_ + used;
}

Status WinMmapFile::InvalidateCache(size_t offset, size_t length) {
  return Status::OK();
}

Status WinMmapFile::Allocate(uint64_t offset, uint64_t len) {
  Status status;
  TEST_KILL_RANDOM("WinMmapFile::Allocate", rocksdb_kill_odds);

  // Make sure that we reserve an aligned amount of space
  // since the reservation block size is driven outside so we want
  // to check if we are ok with reservation here
  size_t spaceToReserve = Roundup(static_cast<size_t>(offset + len), view_size_);
  // Nothing to do
  if (spaceToReserve <= reserved_size_) {
    return status;
  }

  IOSTATS_TIMER_GUARD(allocate_nanos);
  status = PreallocateInternal(spaceToReserve);
  if (status.ok()) {
    reserved_size_ = spaceToReserve;
  }
  return status;
}

size_t WinMmapFile::GetUniqueId(char* id, size_t max_size) const {
  return GetUniqueIdFromFile(hFile_, id, max_size);
}

//////////////////////////////////////////////////////////////////////////////////
// WinSequentialFile

WinSequentialFile::WinSequentialFile(const std::string& fname, HANDLE f,
                                     const EnvOptions& options)
    : WinFileData(fname, f, options.use_direct_reads) {}

WinSequentialFile::~WinSequentialFile() {
  assert(hFile_ != INVALID_HANDLE_VALUE);
}

Status WinSequentialFile::Read(size_t n, Slice* result, char* scratch) {
  Status s;
  size_t r = 0;

  assert(result != nullptr);
  if (WinFileData::use_direct_io()) {
    return Status::NotSupported("Read() does not support direct_io");
  }

  // Windows ReadFile API accepts a DWORD.
  // While it is possible to read in a loop if n is too big
  // it is an unlikely case.
  if (n > std::numeric_limits<DWORD>::max()) {
    return Status::InvalidArgument("n is too big for a single ReadFile: "
      + filename_);
  }

  DWORD bytesToRead = static_cast<DWORD>(n); //cast is safe due to the check above
  DWORD bytesRead = 0;
  BOOL ret = ReadFile(hFile_, scratch, bytesToRead, &bytesRead, NULL);
  if (ret != FALSE) {
    r = bytesRead;
  } else {
    auto lastError = GetLastError();
    if (lastError != ERROR_HANDLE_EOF) {
      s = IOErrorFromWindowsError("ReadFile failed: " + filename_,
        lastError);
    }
  }

  *result = Slice(scratch, r);
  return s;
}

Status WinSequentialFile::PositionedReadInternal(char* src, size_t numBytes,
  uint64_t offset, size_t& bytes_read) const {
  return pread(this, src, numBytes, offset, bytes_read);
}

Status WinSequentialFile::PositionedRead(uint64_t offset, size_t n, Slice* result,
  char* scratch) {

  Status s;

  if (!WinFileData::use_direct_io()) {
    return Status::NotSupported("This function is only used for direct_io");
  }

  if (!IsSectorAligned(static_cast<size_t>(offset)) ||
      !IsSectorAligned(n)) {
      return Status::InvalidArgument(
        "WinSequentialFile::PositionedRead: offset is not properly aligned");
  }

  size_t bytes_read = 0; // out param
  s = PositionedReadInternal(scratch, static_cast<size_t>(n), offset, bytes_read);
  *result = Slice(scratch, bytes_read);
  return s;
}


Status WinSequentialFile::Skip(uint64_t n) {
  // Can't handle more than signed max as SetFilePointerEx accepts a signed 64-bit
  // integer. As such it is a highly unlikley case to have n so large.
  if (n > static_cast<uint64_t>(std::numeric_limits<LONGLONG>::max())) {
    return Status::InvalidArgument("n is too large for a single SetFilePointerEx() call" +
      filename_);
  }

  LARGE_INTEGER li;
  li.QuadPart = static_cast<LONGLONG>(n); //cast is safe due to the check above
  BOOL ret = SetFilePointerEx(hFile_, li, NULL, FILE_CURRENT);
  if (ret == FALSE) {
    auto lastError = GetLastError();
    return IOErrorFromWindowsError("Skip SetFilePointerEx():" + filename_, 
      lastError);
  }
  return Status::OK();
}

Status WinSequentialFile::InvalidateCache(size_t offset, size_t length) {
  return Status::OK();
}

//////////////////////////////////////////////////////////////////////////////////////////////////
/// WinRandomAccessBase

inline
Status WinRandomAccessImpl::PositionedReadInternal(char* src,
  size_t numBytes,
  uint64_t offset,
  size_t& bytes_read) const {
  return pread(file_base_, src, numBytes, offset, bytes_read);
}

inline
WinRandomAccessImpl::WinRandomAccessImpl(WinFileData* file_base,
  size_t alignment,
  const EnvOptions& options) :
    file_base_(file_base),
    alignment_(alignment) {

  assert(!options.use_mmap_reads);
}

inline
Status WinRandomAccessImpl::ReadImpl(uint64_t offset, size_t n, Slice* result,
  char* scratch) const {

  Status s;

  // Check buffer alignment
  if (file_base_->use_direct_io()) {
    if (!IsSectorAligned(static_cast<size_t>(offset)) ||
        !IsAligned(alignment_, scratch)) {
      return Status::InvalidArgument(
        "WinRandomAccessImpl::ReadImpl: offset or scratch is not properly aligned");
    }
  }

  if (n == 0) {
    *result = Slice(scratch, 0);
    return s;
  }

  size_t bytes_read = 0;
  s = PositionedReadInternal(scratch, n, offset, bytes_read);
  *result = Slice(scratch, bytes_read);
  return s;
}

///////////////////////////////////////////////////////////////////////////////////////////////////
/// WinRandomAccessFile

WinRandomAccessFile::WinRandomAccessFile(const std::string& fname, HANDLE hFile,
                                         size_t alignment,
                                         const EnvOptions& options)
    : WinFileData(fname, hFile, options.use_direct_reads),
      WinRandomAccessImpl(this, alignment, options) {}

WinRandomAccessFile::~WinRandomAccessFile() {
}

Status WinRandomAccessFile::Read(uint64_t offset, size_t n, Slice* result,
  char* scratch) const {
  return ReadImpl(offset, n, result, scratch);
}

Status WinRandomAccessFile::InvalidateCache(size_t offset, size_t length) {
  return Status::OK();
}

size_t WinRandomAccessFile::GetUniqueId(char* id, size_t max_size) const {
  return GetUniqueIdFromFile(GetFileHandle(), id, max_size);
}

size_t WinRandomAccessFile::GetRequiredBufferAlignment() const {
  return GetAlignment();
}

/////////////////////////////////////////////////////////////////////////////
// WinWritableImpl
//

inline
Status WinWritableImpl::PreallocateInternal(uint64_t spaceToReserve) {
  return fallocate(file_data_->GetName(), file_data_->GetFileHandle(), spaceToReserve);
}

inline
WinWritableImpl::WinWritableImpl(WinFileData* file_data, size_t alignment)
  : file_data_(file_data),
  alignment_(alignment),
  next_write_offset_(0),
  reservedsize_(0) {

  // Query current position in case ReopenWritableFile is called
  // This position is only important for buffered writes
  // for unbuffered writes we explicitely specify the position.
  LARGE_INTEGER zero_move;
  zero_move.QuadPart = 0; // Do not move
  LARGE_INTEGER pos;
  pos.QuadPart = 0;
  BOOL ret = SetFilePointerEx(file_data_->GetFileHandle(), zero_move, &pos,
      FILE_CURRENT);
  // Querying no supped to fail
  if (ret != 0) {
    next_write_offset_ = pos.QuadPart;
  } else {
    assert(false);
  }
}

inline
Status WinWritableImpl::AppendImpl(const Slice& data) {

  Status s;

  if (data.size() > std::numeric_limits<DWORD>::max()) {
    return Status::InvalidArgument("data is too long for a single write" + 
      file_data_->GetName());
  }

  size_t bytes_written = 0; // out param

  if (file_data_->use_direct_io()) {
    // With no offset specified we are appending
    // to the end of the file
    assert(IsSectorAligned(next_write_offset_));
    if (!IsSectorAligned(data.size()) ||
        !IsAligned(static_cast<size_t>(GetAlignement()), data.data())) {
      s = Status::InvalidArgument(
        "WriteData must be page aligned, size must be sector aligned");
    } else {
      s = pwrite(file_data_, data, next_write_offset_, bytes_written);
    }
  } else {

    DWORD bytesWritten = 0;
    if (!WriteFile(file_data_->GetFileHandle(), data.data(),
      static_cast<DWORD>(data.size()), &bytesWritten, NULL)) {
      auto lastError = GetLastError();
      s = IOErrorFromWindowsError(
        "Failed to WriteFile: " + file_data_->GetName(),
        lastError);
    } else {
      bytes_written = bytesWritten;
    }
  }

  if(s.ok()) {
    if (bytes_written == data.size()) {
      // This matters for direct_io cases where
      // we rely on the fact that next_write_offset_
      // is sector aligned
      next_write_offset_ += bytes_written;
    } else {
      s = Status::IOError("Failed to write all bytes: " + 
        file_data_->GetName());
    }
  }

  return s;
}

inline
Status WinWritableImpl::PositionedAppendImpl(const Slice& data, uint64_t offset) {

  if(file_data_->use_direct_io()) {
    if (!IsSectorAligned(static_cast<size_t>(offset)) ||
        !IsSectorAligned(data.size()) ||
        !IsAligned(static_cast<size_t>(GetAlignement()), data.data())) {
      return Status::InvalidArgument(
        "Data and offset must be page aligned, size must be sector aligned");
    }
  }

  size_t bytes_written = 0;
  Status s = pwrite(file_data_, data, offset, bytes_written);

  if(s.ok()) {
    if (bytes_written == data.size()) {
      // For sequential write this would be simple
      // size extension by data.size()
      uint64_t write_end = offset + bytes_written;
      if (write_end >= next_write_offset_) {
        next_write_offset_ = write_end;
      }
    } else {
      s = Status::IOError("Failed to write all of the requested data: " +
        file_data_->GetName());
    }
  }
  return s;
}

inline
Status WinWritableImpl::TruncateImpl(uint64_t size) {

  // It is tempting to check for the size for sector alignment
  // but truncation may come at the end and there is not a requirement
  // for this to be sector aligned so long as we do not attempt to write
  // after that. The interface docs state that the behavior is undefined
  // in that case.
  Status s = ftruncate(file_data_->GetName(), file_data_->GetFileHandle(),
    size);

  if (s.ok()) {
    next_write_offset_ = size;
  }
  return s;
}

inline
Status WinWritableImpl::CloseImpl() {

  Status s;

  auto hFile = file_data_->GetFileHandle();
  assert(INVALID_HANDLE_VALUE != hFile);

  if (!::FlushFileBuffers(hFile)) {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError("FlushFileBuffers failed at Close() for: " +
      file_data_->GetName(),
      lastError);
  }

  if(!file_data_->CloseFile() && s.ok()) {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError("CloseHandle failed for: " + file_data_->GetName(),
      lastError);
  }
  return s;
}

inline
Status WinWritableImpl::SyncImpl() {
  Status s;
  if (!::FlushFileBuffers (file_data_->GetFileHandle())) {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError(
        "FlushFileBuffers failed at Sync() for: " + file_data_->GetName(), lastError);
  }
  return s;
}


inline
Status WinWritableImpl::AllocateImpl(uint64_t offset, uint64_t len) {
  Status status;
  TEST_KILL_RANDOM("WinWritableFile::Allocate", rocksdb_kill_odds);

  // Make sure that we reserve an aligned amount of space
  // since the reservation block size is driven outside so we want
  // to check if we are ok with reservation here
  size_t spaceToReserve = Roundup(static_cast<size_t>(offset + len), static_cast<size_t>(alignment_));
  // Nothing to do
  if (spaceToReserve <= reservedsize_) {
    return status;
  }

  IOSTATS_TIMER_GUARD(allocate_nanos);
  status = PreallocateInternal(spaceToReserve);
  if (status.ok()) {
    reservedsize_ = spaceToReserve;
  }
  return status;
}


////////////////////////////////////////////////////////////////////////////////
/// WinWritableFile

WinWritableFile::WinWritableFile(const std::string& fname, HANDLE hFile,
                                 size_t alignment, size_t /* capacity */,
                                 const EnvOptions& options)
    : WinFileData(fname, hFile, options.use_direct_writes),
      WinWritableImpl(this, alignment) {
  assert(!options.use_mmap_writes);
}

WinWritableFile::~WinWritableFile() {
}

// Indicates if the class makes use of direct I/O
bool WinWritableFile::use_direct_io() const { return WinFileData::use_direct_io(); }

size_t WinWritableFile::GetRequiredBufferAlignment() const {
  return static_cast<size_t>(GetAlignement());
}

Status WinWritableFile::Append(const Slice& data) {
  return AppendImpl(data);
}

Status WinWritableFile::PositionedAppend(const Slice& data, uint64_t offset) {
  return PositionedAppendImpl(data, offset);
}

// Need to implement this so the file is truncated correctly
// when buffered and unbuffered mode
Status WinWritableFile::Truncate(uint64_t size) {
  return TruncateImpl(size);
}

Status WinWritableFile::Close() {
  return CloseImpl();
}

  // write out the cached data to the OS cache
  // This is now taken care of the WritableFileWriter
Status WinWritableFile::Flush() {
  return Status::OK();
}

Status WinWritableFile::Sync() {
  return SyncImpl();
}

Status WinWritableFile::Fsync() { return SyncImpl(); }

bool WinWritableFile::IsSyncThreadSafe() const { return true; }

uint64_t WinWritableFile::GetFileSize() {
  return GetFileNextWriteOffset();
}

Status WinWritableFile::Allocate(uint64_t offset, uint64_t len) {
  return AllocateImpl(offset, len);
}

size_t WinWritableFile::GetUniqueId(char* id, size_t max_size) const {
  return GetUniqueIdFromFile(GetFileHandle(), id, max_size);
}

/////////////////////////////////////////////////////////////////////////
/// WinRandomRWFile

WinRandomRWFile::WinRandomRWFile(const std::string& fname, HANDLE hFile,
                                 size_t alignment, const EnvOptions& options)
    : WinFileData(fname, hFile,
                  options.use_direct_reads && options.use_direct_writes),
      WinRandomAccessImpl(this, alignment, options),
      WinWritableImpl(this, alignment) {}

bool WinRandomRWFile::use_direct_io() const { return WinFileData::use_direct_io(); }

size_t WinRandomRWFile::GetRequiredBufferAlignment() const {
  return static_cast<size_t>(GetAlignement());
}

Status WinRandomRWFile::Write(uint64_t offset, const Slice & data) {
  return PositionedAppendImpl(data, offset);
}

Status WinRandomRWFile::Read(uint64_t offset, size_t n, Slice* result,
                             char* scratch) const {
  return ReadImpl(offset, n, result, scratch);
}

Status WinRandomRWFile::Flush() {
  return Status::OK();
}

Status WinRandomRWFile::Sync() {
  return SyncImpl();
}

Status WinRandomRWFile::Close() {
  return CloseImpl();
}

//////////////////////////////////////////////////////////////////////////
/// WinMemoryMappedBufer
WinMemoryMappedBuffer::~WinMemoryMappedBuffer() {
  BOOL ret = FALSE;
  if (base_ != nullptr) {
    ret = ::UnmapViewOfFile(base_);
    assert(ret);
    base_ = nullptr;
  }
  if (map_handle_ != NULL && map_handle_ != INVALID_HANDLE_VALUE) {
    ret = ::CloseHandle(map_handle_);
    assert(ret);
    map_handle_ = NULL;
  }
  if (file_handle_ != NULL && file_handle_ != INVALID_HANDLE_VALUE) {
    ret = ::CloseHandle(file_handle_);
    assert(ret);
    file_handle_ = NULL;
  }
}

//////////////////////////////////////////////////////////////////////////
/// WinDirectory

Status WinDirectory::Fsync() { return Status::OK(); }

size_t WinDirectory::GetUniqueId(char* id, size_t max_size) const {
  return GetUniqueIdFromFile(handle_, id, max_size);
}
//////////////////////////////////////////////////////////////////////////
/// WinFileLock

WinFileLock::~WinFileLock() {
  BOOL ret __attribute__((__unused__));
  ret = ::CloseHandle(hFile_);
  assert(ret);
}

}
}
