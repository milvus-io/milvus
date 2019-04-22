//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "port/win/env_win.h"
#include "port/win/win_thread.h"
#include <algorithm>
#include <ctime>
#include <thread>

#include <errno.h>
#include <process.h> // _getpid
#include <io.h> // _access
#include <direct.h> // _rmdir, _mkdir, _getcwd
#include <sys/types.h>
#include <sys/stat.h>

#include "rocksdb/env.h"
#include "rocksdb/slice.h"

#include "port/port.h"
#include "port/dirent.h"
#include "port/win/win_logger.h"
#include "port/win/io_win.h"

#include "monitoring/iostats_context_imp.h"

#include "monitoring/thread_status_updater.h"
#include "monitoring/thread_status_util.h"

#include <rpc.h>  // for uuid generation
#include <windows.h>
#include <shlwapi.h>
#include "strsafe.h"

#include <algorithm>

namespace rocksdb {

ThreadStatusUpdater* CreateThreadStatusUpdater() {
  return new ThreadStatusUpdater();
}

namespace {

static const size_t kSectorSize = 512; // Sector size used when physical sector size could not be obtained from device.

// RAII helpers for HANDLEs
const auto CloseHandleFunc = [](HANDLE h) { ::CloseHandle(h); };
typedef std::unique_ptr<void, decltype(CloseHandleFunc)> UniqueCloseHandlePtr;

const auto FindCloseFunc = [](HANDLE h) { ::FindClose(h); };
typedef std::unique_ptr<void, decltype(FindCloseFunc)> UniqueFindClosePtr;

void WinthreadCall(const char* label, std::error_code result) {
  if (0 != result.value()) {
    fprintf(stderr, "pthread %s: %s\n", label, strerror(result.value()));
    abort();
  }
}

}

namespace port {

WinEnvIO::WinEnvIO(Env* hosted_env)
  :   hosted_env_(hosted_env),
      page_size_(4 * 1024),
      allocation_granularity_(page_size_),
      perf_counter_frequency_(0),
      GetSystemTimePreciseAsFileTime_(NULL) {

  SYSTEM_INFO sinfo;
  GetSystemInfo(&sinfo);

  page_size_ = sinfo.dwPageSize;
  allocation_granularity_ = sinfo.dwAllocationGranularity;

  {
    LARGE_INTEGER qpf;
    BOOL ret __attribute__((__unused__));
    ret = QueryPerformanceFrequency(&qpf);
    assert(ret == TRUE);
    perf_counter_frequency_ = qpf.QuadPart;
  }

  HMODULE module = GetModuleHandle("kernel32.dll");
  if (module != NULL) {
    GetSystemTimePreciseAsFileTime_ = (FnGetSystemTimePreciseAsFileTime)GetProcAddress(
      module, "GetSystemTimePreciseAsFileTime");
  }
}

WinEnvIO::~WinEnvIO() {
}

Status WinEnvIO::DeleteFile(const std::string& fname) {
  Status result;

  BOOL ret = RX_DeleteFile(RX_FN(fname).c_str());

  if(!ret) {
    auto lastError = GetLastError();
    result = IOErrorFromWindowsError("Failed to delete: " + fname,
      lastError);
  }

  return result;
}

Status WinEnvIO::Truncate(const std::string& fname, size_t size) {
  Status s;
  int result = rocksdb::port::Truncate(fname, size);
  if (result != 0) {
    s = IOError("Failed to truncate: " + fname, errno);
  }
  return s;
}

Status WinEnvIO::GetCurrentTime(int64_t* unix_time) {
  time_t time = std::time(nullptr);
  if (time == (time_t)(-1)) {
    return Status::NotSupported("Failed to get time");
  }

  *unix_time = time;
  return Status::OK();
}

Status WinEnvIO::NewSequentialFile(const std::string& fname,
  std::unique_ptr<SequentialFile>* result,
  const EnvOptions& options) {
  Status s;

  result->reset();

  // Corruption test needs to rename and delete files of these kind
  // while they are still open with another handle. For that reason we
  // allow share_write and delete(allows rename).
  HANDLE hFile = INVALID_HANDLE_VALUE;

  DWORD fileFlags = FILE_ATTRIBUTE_READONLY;

  if (options.use_direct_reads && !options.use_mmap_reads) {
    fileFlags |= FILE_FLAG_NO_BUFFERING;
  }

  {
    IOSTATS_TIMER_GUARD(open_nanos);
    hFile = RX_CreateFile(
      RX_FN(fname).c_str(), GENERIC_READ,
      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
      OPEN_EXISTING,  // Original fopen mode is "rb"
      fileFlags, NULL);
  }

  if (INVALID_HANDLE_VALUE == hFile) {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError("Failed to open NewSequentialFile" + fname,
      lastError);
  } else {
    result->reset(new WinSequentialFile(fname, hFile, options));
  }
  return s;
}

Status WinEnvIO::NewRandomAccessFile(const std::string& fname,
  std::unique_ptr<RandomAccessFile>* result,
  const EnvOptions& options) {
  result->reset();
  Status s;

  // Open the file for read-only random access
  // Random access is to disable read-ahead as the system reads too much data
  DWORD fileFlags = FILE_ATTRIBUTE_READONLY;

  if (options.use_direct_reads && !options.use_mmap_reads) {
    fileFlags |= FILE_FLAG_NO_BUFFERING;
  } else {
    fileFlags |= FILE_FLAG_RANDOM_ACCESS;
  }

  /// Shared access is necessary for corruption test to pass
  // almost all tests would work with a possible exception of fault_injection
  HANDLE hFile = 0;
  {
    IOSTATS_TIMER_GUARD(open_nanos);
    hFile =
      RX_CreateFile(RX_FN(fname).c_str(), GENERIC_READ,
      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
      NULL, OPEN_EXISTING, fileFlags, NULL);
  }

  if (INVALID_HANDLE_VALUE == hFile) {
    auto lastError = GetLastError();
    return IOErrorFromWindowsError(
      "NewRandomAccessFile failed to Create/Open: " + fname, lastError);
  }

  UniqueCloseHandlePtr fileGuard(hFile, CloseHandleFunc);

  // CAUTION! This will map the entire file into the process address space
  if (options.use_mmap_reads && sizeof(void*) >= 8) {
    // Use mmap when virtual address-space is plentiful.
    uint64_t fileSize;

    s = GetFileSize(fname, &fileSize);

    if (s.ok()) {
      // Will not map empty files
      if (fileSize == 0) {
        return IOError(
          "NewRandomAccessFile failed to map empty file: " + fname, EINVAL);
      }

      HANDLE hMap = RX_CreateFileMapping(hFile, NULL, PAGE_READONLY,
        0,  // Whole file at its present length
        0,
        NULL);  // Mapping name

      if (!hMap) {
        auto lastError = GetLastError();
        return IOErrorFromWindowsError(
          "Failed to create file mapping for NewRandomAccessFile: " + fname,
          lastError);
      }

      UniqueCloseHandlePtr mapGuard(hMap, CloseHandleFunc);

      const void* mapped_region =
        MapViewOfFileEx(hMap, FILE_MAP_READ,
        0,  // High DWORD of access start
        0,  // Low DWORD
        static_cast<SIZE_T>(fileSize),
        NULL);  // Let the OS choose the mapping

      if (!mapped_region) {
        auto lastError = GetLastError();
        return IOErrorFromWindowsError(
          "Failed to MapViewOfFile for NewRandomAccessFile: " + fname,
          lastError);
      }

      result->reset(new WinMmapReadableFile(fname, hFile, hMap, mapped_region,
				static_cast<size_t>(fileSize)));

      mapGuard.release();
      fileGuard.release();
    }
  } else {
    result->reset(new WinRandomAccessFile(fname, hFile, 
      std::max(GetSectorSize(fname), page_size_), options));
    fileGuard.release();
  }
  return s;
}

Status WinEnvIO::OpenWritableFile(const std::string& fname,
  std::unique_ptr<WritableFile>* result,
  const EnvOptions& options,
  bool reopen) {

  const size_t c_BufferCapacity = 64 * 1024;

  EnvOptions local_options(options);

  result->reset();
  Status s;

  DWORD fileFlags = FILE_ATTRIBUTE_NORMAL;

  if (local_options.use_direct_writes && !local_options.use_mmap_writes) {
    fileFlags = FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH;
  }

  // Desired access. We are want to write only here but if we want to memory
  // map
  // the file then there is no write only mode so we have to create it
  // Read/Write
  // However, MapViewOfFile specifies only Write only
  DWORD desired_access = GENERIC_WRITE;
  DWORD shared_mode = FILE_SHARE_READ;

  if (local_options.use_mmap_writes) {
    desired_access |= GENERIC_READ;
  } else {
    // Adding this solely for tests to pass (fault_injection_test,
    // wal_manager_test).
    shared_mode |= (FILE_SHARE_WRITE | FILE_SHARE_DELETE);
  }

  // This will always truncate the file
  DWORD creation_disposition = CREATE_ALWAYS;
  if (reopen) {
    creation_disposition = OPEN_ALWAYS;
  }

  HANDLE hFile = 0;
  {
    IOSTATS_TIMER_GUARD(open_nanos);
    hFile = RX_CreateFile(
      RX_FN(fname).c_str(),
      desired_access,  // Access desired
      shared_mode,
      NULL,           // Security attributes
      creation_disposition,  // Posix env says (reopen) ? (O_CREATE | O_APPEND) : O_CREAT | O_TRUNC
      fileFlags,      // Flags
      NULL);          // Template File
  }

  if (INVALID_HANDLE_VALUE == hFile) {
    auto lastError = GetLastError();
    return IOErrorFromWindowsError(
      "Failed to create a NewWriteableFile: " + fname, lastError);
  }

  // We will start writing at the end, appending
  if (reopen) {
    LARGE_INTEGER zero_move;
    zero_move.QuadPart = 0;
    BOOL ret = SetFilePointerEx(hFile, zero_move, NULL, FILE_END);
    if (!ret) {
      auto lastError = GetLastError();
      return IOErrorFromWindowsError(
        "Failed to create a ReopenWritableFile move to the end: " + fname, lastError);
    }
  }

  if (options.use_mmap_writes) {
    // We usually do not use mmmapping on SSD and thus we pass memory
    // page_size
    result->reset(new WinMmapFile(fname, hFile, page_size_,
      allocation_granularity_, local_options));
  } else {
    // Here we want the buffer allocation to be aligned by the SSD page size
    // and to be a multiple of it
    result->reset(new WinWritableFile(fname, hFile, std::max(GetSectorSize(fname), GetPageSize()),
      c_BufferCapacity, local_options));
  }
  return s;
}

Status WinEnvIO::NewRandomRWFile(const std::string & fname,
  std::unique_ptr<RandomRWFile>* result, const EnvOptions & options) {

  Status s;

  // Open the file for read-only random access
  // Random access is to disable read-ahead as the system reads too much data
  DWORD desired_access = GENERIC_READ | GENERIC_WRITE;
  DWORD shared_mode = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;
  DWORD creation_disposition = OPEN_EXISTING; // Fail if file does not exist
  DWORD file_flags = FILE_FLAG_RANDOM_ACCESS;

  if (options.use_direct_reads && options.use_direct_writes) {
    file_flags |= FILE_FLAG_NO_BUFFERING;
  }

  /// Shared access is necessary for corruption test to pass
  // almost all tests would work with a possible exception of fault_injection
  HANDLE hFile = 0;
  {
    IOSTATS_TIMER_GUARD(open_nanos);
    hFile =
      RX_CreateFile(RX_FN(fname).c_str(),
        desired_access,
        shared_mode,
        NULL, // Security attributes
        creation_disposition,
        file_flags,
        NULL);
  }

  if (INVALID_HANDLE_VALUE == hFile) {
    auto lastError = GetLastError();
    return IOErrorFromWindowsError(
      "NewRandomRWFile failed to Create/Open: " + fname, lastError);
  }

  UniqueCloseHandlePtr fileGuard(hFile, CloseHandleFunc);
  result->reset(new WinRandomRWFile(fname, hFile, std::max(GetSectorSize(fname), GetPageSize()),
   options));
  fileGuard.release();

  return s;
}

Status WinEnvIO::NewMemoryMappedFileBuffer(const std::string & fname,
  std::unique_ptr<MemoryMappedFileBuffer>* result) {
  Status s;
  result->reset();

  DWORD fileFlags = FILE_ATTRIBUTE_READONLY;

  HANDLE hFile = INVALID_HANDLE_VALUE;
  {
    IOSTATS_TIMER_GUARD(open_nanos);
    hFile = RX_CreateFile(
      RX_FN(fname).c_str(), GENERIC_READ | GENERIC_WRITE,
      FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
      NULL,
      OPEN_EXISTING,  // Open only if it exists
      fileFlags,
      NULL);
  }

  if (INVALID_HANDLE_VALUE == hFile) {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError("Failed to open NewMemoryMappedFileBuffer: " + fname,
      lastError);
    return s;
  }
  UniqueCloseHandlePtr fileGuard(hFile, CloseHandleFunc);

  uint64_t fileSize = 0;
  s = GetFileSize(fname, &fileSize);
  if (!s.ok()) {
    return s;
  }
  // Will not map empty files
  if (fileSize == 0) {
    return Status::NotSupported("NewMemoryMappedFileBuffer can not map zero length files: " + fname);
  }

  // size_t is 32-bit with 32-bit builds
  if (fileSize > std::numeric_limits<size_t>::max()) {
    return Status::NotSupported(
      "The specified file size does not fit into 32-bit memory addressing: " + fname);
  }

  HANDLE hMap = RX_CreateFileMapping(hFile, NULL, PAGE_READWRITE,
      0,  // Whole file at its present length
      0,
      NULL);  // Mapping name

  if (!hMap) {
    auto lastError = GetLastError();
    return IOErrorFromWindowsError(
      "Failed to create file mapping for NewMemoryMappedFileBuffer: " + fname,
      lastError);
  }
  UniqueCloseHandlePtr mapGuard(hMap, CloseHandleFunc);

  void* base = MapViewOfFileEx(hMap, FILE_MAP_WRITE,
    0,  // High DWORD of access start
    0,  // Low DWORD
		static_cast<SIZE_T>(fileSize),
    NULL);  // Let the OS choose the mapping

  if (!base) {
    auto lastError = GetLastError();
    return IOErrorFromWindowsError(
      "Failed to MapViewOfFile for NewMemoryMappedFileBuffer: " + fname,
      lastError);
  }

  result->reset(new WinMemoryMappedBuffer(hFile, hMap, 
    base, static_cast<size_t>(fileSize)));

  mapGuard.release();
  fileGuard.release();

  return s;
}

Status WinEnvIO::NewDirectory(const std::string& name,
  std::unique_ptr<Directory>* result) {
  Status s;
  // Must be nullptr on failure
  result->reset();

  if (!DirExists(name)) {
    s = IOErrorFromWindowsError(
      "open folder: " + name, ERROR_DIRECTORY);
    return s;
  }

  HANDLE handle = INVALID_HANDLE_VALUE;
  // 0 - for access means read metadata
  {
    IOSTATS_TIMER_GUARD(open_nanos);
    handle = RX_CreateFile(RX_FN(name).c_str(), 0,
      FILE_SHARE_DELETE | FILE_SHARE_READ | FILE_SHARE_WRITE,
      NULL,
      OPEN_EXISTING,
      FILE_FLAG_BACKUP_SEMANTICS, // make opening folders possible
      NULL);
  }

  if (INVALID_HANDLE_VALUE == handle) {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError(
      "open folder: " + name, lastError);
    return s;
  }

  result->reset(new WinDirectory(handle));

  return s;
}

Status WinEnvIO::FileExists(const std::string& fname) {
  Status s;
  // TODO: This does not follow symbolic links at this point
  // which is consistent with _access() impl on windows
  // but can be added
  WIN32_FILE_ATTRIBUTE_DATA attrs;
  if (FALSE == RX_GetFileAttributesEx(RX_FN(fname).c_str(), GetFileExInfoStandard, &attrs)) {
    auto lastError = GetLastError();
    switch (lastError) {
    case ERROR_ACCESS_DENIED:
    case ERROR_NOT_FOUND:
    case ERROR_FILE_NOT_FOUND:
    case ERROR_PATH_NOT_FOUND:
      s = Status::NotFound();
      break;
    default:
      s = IOErrorFromWindowsError("Unexpected error for: " + fname,
        lastError);
      break;
    }
  }
  return s;
}

Status WinEnvIO::GetChildren(const std::string& dir,
  std::vector<std::string>* result) {

  Status status;
  result->clear();
  std::vector<std::string> output;

  RX_WIN32_FIND_DATA data;
  memset(&data, 0, sizeof(data));
  std::string pattern(dir);
  pattern.append("\\").append("*");

  HANDLE handle = RX_FindFirstFileEx(RX_FN(pattern).c_str(),
    FindExInfoBasic, // Do not want alternative name
    &data,
    FindExSearchNameMatch,
    NULL, // lpSearchFilter
    0);

  if (handle == INVALID_HANDLE_VALUE) {
    auto lastError = GetLastError();
    switch (lastError) {
    case ERROR_NOT_FOUND:
    case ERROR_ACCESS_DENIED:
    case ERROR_FILE_NOT_FOUND:
    case ERROR_PATH_NOT_FOUND:
      status = Status::NotFound();
      break;
    default:
      status = IOErrorFromWindowsError(
        "Failed to GetChhildren for: " + dir, lastError);
    }
    return status;
  }

  UniqueFindClosePtr fc(handle, FindCloseFunc);

  if (result->capacity() > 0) {
    output.reserve(result->capacity());
  }

  // For safety
  data.cFileName[MAX_PATH - 1] = 0;

  while (true) {
    auto x = RX_FILESTRING(data.cFileName, RX_FNLEN(data.cFileName));
    output.emplace_back(FN_TO_RX(x));
    BOOL ret =- RX_FindNextFile(handle, &data);
    // If the function fails the return value is zero
    // and non-zero otherwise. Not TRUE or FALSE.
    if (ret == FALSE) {
      // Posix does not care why we stopped
      break;
    }
    data.cFileName[MAX_PATH - 1] = 0;
  }
  output.swap(*result);
  return status;
}

Status WinEnvIO::CreateDir(const std::string& name) {
  Status result;
  BOOL ret = RX_CreateDirectory(RX_FN(name).c_str(), NULL);
  if (!ret) {
    auto lastError = GetLastError();
    result = IOErrorFromWindowsError(
        "Failed to create a directory: " + name, lastError);
  }

  return result;
}

Status  WinEnvIO::CreateDirIfMissing(const std::string& name) {
  Status result;

  if (DirExists(name)) {
    return result;
  }

  BOOL ret = RX_CreateDirectory(RX_FN(name).c_str(), NULL);
  if (!ret) {
    auto lastError = GetLastError();
    if (lastError != ERROR_ALREADY_EXISTS) {
      result = IOErrorFromWindowsError(
        "Failed to create a directory: " + name, lastError);
    } else {
      result =
        Status::IOError(name + ": exists but is not a directory");
    }
  }
  return result;
}

Status WinEnvIO::DeleteDir(const std::string& name) {
  Status result;
  BOOL ret = RX_RemoveDirectory(RX_FN(name).c_str());
  if (!ret) {
    auto lastError = GetLastError();
    result = IOErrorFromWindowsError("Failed to remove dir: " + name, lastError);
  }
  return result;
}

Status WinEnvIO::GetFileSize(const std::string& fname,
  uint64_t* size) {
  Status s;

  WIN32_FILE_ATTRIBUTE_DATA attrs;
  if (RX_GetFileAttributesEx(RX_FN(fname).c_str(), GetFileExInfoStandard, &attrs)) {
    ULARGE_INTEGER file_size;
    file_size.HighPart = attrs.nFileSizeHigh;
    file_size.LowPart = attrs.nFileSizeLow;
    *size = file_size.QuadPart;
  } else {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError("Can not get size for: " + fname, lastError);
  }
  return s;
}

uint64_t WinEnvIO::FileTimeToUnixTime(const FILETIME& ftTime) {
  const uint64_t c_FileTimePerSecond = 10000000U;
  // UNIX epoch starts on 1970-01-01T00:00:00Z
  // Windows FILETIME starts on 1601-01-01T00:00:00Z
  // Therefore, we need to subtract the below number of seconds from
  // the seconds that we obtain from FILETIME with an obvious loss of
  // precision
  const uint64_t c_SecondBeforeUnixEpoch = 11644473600U;

  ULARGE_INTEGER li;
  li.HighPart = ftTime.dwHighDateTime;
  li.LowPart = ftTime.dwLowDateTime;

  uint64_t result =
    (li.QuadPart / c_FileTimePerSecond) - c_SecondBeforeUnixEpoch;
  return result;
}

Status WinEnvIO::GetFileModificationTime(const std::string& fname,
  uint64_t* file_mtime) {
  Status s;

  WIN32_FILE_ATTRIBUTE_DATA attrs;
  if (RX_GetFileAttributesEx(RX_FN(fname).c_str(), GetFileExInfoStandard, &attrs)) {
    *file_mtime = FileTimeToUnixTime(attrs.ftLastWriteTime);
  } else {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError(
      "Can not get file modification time for: " + fname, lastError);
    *file_mtime = 0;
  }

  return s;
}

Status WinEnvIO::RenameFile(const std::string& src,
  const std::string& target) {
  Status result;

  // rename() is not capable of replacing the existing file as on Linux
  // so use OS API directly
  if (!RX_MoveFileEx(RX_FN(src).c_str(), RX_FN(target).c_str(), MOVEFILE_REPLACE_EXISTING)) {
    DWORD lastError = GetLastError();

    std::string text("Failed to rename: ");
    text.append(src).append(" to: ").append(target);

    result = IOErrorFromWindowsError(text, lastError);
  }

  return result;
}

Status WinEnvIO::LinkFile(const std::string& src,
  const std::string& target) {
  Status result;

  if (!RX_CreateHardLink(RX_FN(target).c_str(), RX_FN(src).c_str(),  NULL)) {
    DWORD lastError = GetLastError();
    if (lastError == ERROR_NOT_SAME_DEVICE) {
      return Status::NotSupported("No cross FS links allowed");
    }

    std::string text("Failed to link: ");
    text.append(src).append(" to: ").append(target);

    result = IOErrorFromWindowsError(text, lastError);
  }

  return result;
}

Status WinEnvIO::NumFileLinks(const std::string& fname, uint64_t* count) {
  Status s;
  HANDLE handle = RX_CreateFile(
      RX_FN(fname).c_str(), 0,
      FILE_SHARE_DELETE | FILE_SHARE_READ | FILE_SHARE_WRITE,
      NULL, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS, NULL);

  if (INVALID_HANDLE_VALUE == handle) {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError("NumFileLinks: " + fname, lastError);
    return s;
  }
  UniqueCloseHandlePtr handle_guard(handle, CloseHandleFunc);
  FILE_STANDARD_INFO standard_info;
  if (0 != GetFileInformationByHandleEx(handle, FileStandardInfo,
                                        &standard_info,
                                        sizeof(standard_info))) {
    *count = standard_info.NumberOfLinks;
  } else {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError("GetFileInformationByHandleEx: " + fname,
                                lastError);
  }
  return s;
}

Status WinEnvIO::AreFilesSame(const std::string& first,
  const std::string& second, bool* res) {
// For MinGW builds
#if (_WIN32_WINNT == _WIN32_WINNT_VISTA)
  Status s = Status::NotSupported();
#else
  assert(res != nullptr);
  Status s;
  if (res == nullptr) {
    s = Status::InvalidArgument("res");
    return s;
  }

  // 0 - for access means read metadata
  HANDLE file_1 = RX_CreateFile(RX_FN(first).c_str(), 0, 
                      FILE_SHARE_DELETE | FILE_SHARE_READ | FILE_SHARE_WRITE,
                      NULL, 
                      OPEN_EXISTING,
                      FILE_FLAG_BACKUP_SEMANTICS, // make opening folders possible
                      NULL);

  if (INVALID_HANDLE_VALUE == file_1) {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError(
      "open file: " + first, lastError);
    return s;
  }
  UniqueCloseHandlePtr g_1(file_1, CloseHandleFunc);

  HANDLE file_2 = RX_CreateFile(RX_FN(second).c_str(), 0,
                     FILE_SHARE_DELETE | FILE_SHARE_READ | FILE_SHARE_WRITE,
                     NULL, OPEN_EXISTING,
                     FILE_FLAG_BACKUP_SEMANTICS, // make opening folders possible
                     NULL);

  if (INVALID_HANDLE_VALUE == file_2) {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError(
      "open file: " + second, lastError);
    return s;
  }
  UniqueCloseHandlePtr g_2(file_2, CloseHandleFunc);

  FILE_ID_INFO FileInfo_1;
  BOOL result = GetFileInformationByHandleEx(file_1, FileIdInfo, &FileInfo_1,
    sizeof(FileInfo_1));

  if (!result) {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError(
      "stat file: " + first, lastError);
    return s;
  }

   FILE_ID_INFO FileInfo_2;
   result = GetFileInformationByHandleEx(file_2, FileIdInfo, &FileInfo_2,
    sizeof(FileInfo_2));

  if (!result) {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError(
      "stat file: " + second, lastError);
    return s;
  }

  if (FileInfo_1.VolumeSerialNumber == FileInfo_2.VolumeSerialNumber) {
    *res = (0 == memcmp(FileInfo_1.FileId.Identifier, FileInfo_2.FileId.Identifier, 
      sizeof(FileInfo_1.FileId.Identifier)));
  } else {
    *res = false;
  }
#endif
  return s;
}

Status  WinEnvIO::LockFile(const std::string& lockFname,
  FileLock** lock) {
  assert(lock != nullptr);

  *lock = NULL;
  Status result;

  // No-sharing, this is a LOCK file
  const DWORD ExclusiveAccessON = 0;

  // Obtain exclusive access to the LOCK file
  // Previously, instead of NORMAL attr we set DELETE on close and that worked
  // well except with fault_injection test that insists on deleting it.
  HANDLE hFile = 0;
  {
    IOSTATS_TIMER_GUARD(open_nanos);
    hFile = RX_CreateFile(RX_FN(lockFname).c_str(), (GENERIC_READ | GENERIC_WRITE),
      ExclusiveAccessON, NULL, CREATE_ALWAYS,
      FILE_ATTRIBUTE_NORMAL, NULL);
  }

  if (INVALID_HANDLE_VALUE == hFile) {
    auto lastError = GetLastError();
    result = IOErrorFromWindowsError(
      "Failed to create lock file: " + lockFname, lastError);
  } else {
    *lock = new WinFileLock(hFile);
  }

  return result;
}

Status WinEnvIO::UnlockFile(FileLock* lock) {
  Status result;

  assert(lock != nullptr);

  delete lock;

  return result;
}

Status WinEnvIO::GetTestDirectory(std::string* result) {

  std::string output;

  const char* env = getenv("TEST_TMPDIR");
  if (env && env[0] != '\0') {
    output = env;
  } else {
    env = getenv("TMP");

    if (env && env[0] != '\0') {
      output = env;
    } else {
      output = "c:\\tmp";
    }
  }
  CreateDir(output);

  output.append("\\testrocksdb-");
  output.append(std::to_string(_getpid()));

  CreateDir(output);

  output.swap(*result);

  return Status::OK();
}

Status WinEnvIO::NewLogger(const std::string& fname,
  std::shared_ptr<Logger>* result) {
  Status s;

  result->reset();

  HANDLE hFile = 0;
  {
    IOSTATS_TIMER_GUARD(open_nanos);
    hFile = RX_CreateFile(
      RX_FN(fname).c_str(), GENERIC_WRITE,
      FILE_SHARE_READ | FILE_SHARE_DELETE,  // In RocksDb log files are
      // renamed and deleted before
      // they are closed. This enables
      // doing so.
      NULL,
      CREATE_ALWAYS,  // Original fopen mode is "w"
      FILE_ATTRIBUTE_NORMAL, NULL);
  }

  if (INVALID_HANDLE_VALUE == hFile) {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError("Failed to open LogFile" + fname, lastError);
  } else {
    {
      // With log files we want to set the true creation time as of now
      // because the system
      // for some reason caches the attributes of the previous file that just
      // been renamed from
      // this name so auto_roll_logger_test fails
      FILETIME ft;
      GetSystemTimeAsFileTime(&ft);
      // Set creation, last access and last write time to the same value
      SetFileTime(hFile, &ft, &ft, &ft);
    }
    result->reset(new WinLogger(&WinEnvThreads::gettid, hosted_env_, hFile));
  }
  return s;
}

uint64_t WinEnvIO::NowMicros() {

  if (GetSystemTimePreciseAsFileTime_ != NULL) {
    // all std::chrono clocks on windows proved to return
    // values that may repeat that is not good enough for some uses.
    const int64_t c_UnixEpochStartTicks = 116444736000000000LL;
    const int64_t c_FtToMicroSec = 10;

    // This interface needs to return system time and not
    // just any microseconds because it is often used as an argument
    // to TimedWait() on condition variable
    FILETIME ftSystemTime;
    GetSystemTimePreciseAsFileTime_(&ftSystemTime);

    LARGE_INTEGER li;
    li.LowPart = ftSystemTime.dwLowDateTime;
    li.HighPart = ftSystemTime.dwHighDateTime;
    // Subtract unix epoch start
    li.QuadPart -= c_UnixEpochStartTicks;
    // Convert to microsecs
    li.QuadPart /= c_FtToMicroSec;
    return li.QuadPart;
  }
  using namespace std::chrono;
  return duration_cast<microseconds>(system_clock::now().time_since_epoch()).count();
}

uint64_t WinEnvIO::NowNanos() {
  // all std::chrono clocks on windows have the same resolution that is only
  // good enough for microseconds but not nanoseconds
  // On Windows 8 and Windows 2012 Server
  // GetSystemTimePreciseAsFileTime(&current_time) can be used
  LARGE_INTEGER li;
  QueryPerformanceCounter(&li);
  // Convert to nanoseconds first to avoid loss of precision
  // and divide by frequency
  li.QuadPart *= std::nano::den;
  li.QuadPart /= perf_counter_frequency_;
  return li.QuadPart;
}

Status WinEnvIO::GetHostName(char* name, uint64_t len) {
  Status s;
  DWORD nSize = static_cast<DWORD>(
    std::min<uint64_t>(len, std::numeric_limits<DWORD>::max()));

  if (!::GetComputerNameA(name, &nSize)) {
    auto lastError = GetLastError();
    s = IOErrorFromWindowsError("GetHostName", lastError);
  } else {
    name[nSize] = 0;
  }

  return s;
}

Status WinEnvIO::GetAbsolutePath(const std::string& db_path,
  std::string* output_path) {

  // Check if we already have an absolute path
  // For test compatibility we will consider starting slash as an
  // absolute path
  if ((!db_path.empty() && (db_path[0] == '\\' || db_path[0] == '/')) ||
    !RX_PathIsRelative(RX_FN(db_path).c_str())) {
    *output_path = db_path;
    return Status::OK();
  }

  RX_FILESTRING result;
  result.resize(MAX_PATH);

  // Hopefully no changes the current directory while we do this
  // however _getcwd also suffers from the same limitation
  DWORD len = RX_GetCurrentDirectory(MAX_PATH, &result[0]);
  if (len == 0) {
    auto lastError = GetLastError();
    return IOErrorFromWindowsError("Failed to get current working directory",
      lastError);
  }

  result.resize(len);
  std::string res = FN_TO_RX(result);
  
  res.swap(*output_path);
  return Status::OK();
}

std::string WinEnvIO::TimeToString(uint64_t secondsSince1970) {
  std::string result;

  const time_t seconds = secondsSince1970;
  const int maxsize = 64;

  struct tm t;
  errno_t ret = localtime_s(&t, &seconds);

  if (ret) {
    result = std::to_string(seconds);
  } else {
    result.resize(maxsize);
    char* p = &result[0];

    int len = snprintf(p, maxsize, "%04d/%02d/%02d-%02d:%02d:%02d ",
      t.tm_year + 1900, t.tm_mon + 1, t.tm_mday, t.tm_hour,
      t.tm_min, t.tm_sec);
    assert(len > 0);

    result.resize(len);
  }

  return result;
}

EnvOptions WinEnvIO::OptimizeForLogWrite(const EnvOptions& env_options,
  const DBOptions& db_options) const {
  EnvOptions optimized(env_options);
  // These two the same as default optimizations
  optimized.bytes_per_sync = db_options.wal_bytes_per_sync;
  optimized.writable_file_max_buffer_size =
      db_options.writable_file_max_buffer_size;

  // This adversely affects %999 on windows
  optimized.use_mmap_writes = false;
  // Direct writes will produce a huge perf impact on
  // Windows. Pre-allocate space for WAL.
  optimized.use_direct_writes = false;
  return optimized;
}

EnvOptions WinEnvIO::OptimizeForManifestWrite(
  const EnvOptions& env_options) const {
  EnvOptions optimized(env_options);
  optimized.use_mmap_writes = false;
  optimized.use_direct_reads = false;
  return optimized;
}

EnvOptions WinEnvIO::OptimizeForManifestRead(
  const EnvOptions& env_options) const {
  EnvOptions optimized(env_options);
  optimized.use_mmap_writes = false;
  optimized.use_direct_reads = false;
  return optimized;
}

// Returns true iff the named directory exists and is a directory.
bool WinEnvIO::DirExists(const std::string& dname) {
  WIN32_FILE_ATTRIBUTE_DATA attrs;
  if (RX_GetFileAttributesEx(RX_FN(dname).c_str(), GetFileExInfoStandard, &attrs)) {
    return 0 != (attrs.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY);
  }
  return false;
}

size_t WinEnvIO::GetSectorSize(const std::string& fname) {
  size_t sector_size = kSectorSize;

  if (RX_PathIsRelative(RX_FN(fname).c_str())) {
    return sector_size;
  }

  // obtain device handle
  char devicename[7] = "\\\\.\\";
  int erresult = strncat_s(devicename, sizeof(devicename), fname.c_str(), 2);

  if (erresult) {
    assert(false);
    return sector_size;
  }

  HANDLE hDevice = CreateFile(devicename, 0, 0,
                    nullptr, OPEN_EXISTING,
                    FILE_ATTRIBUTE_NORMAL, nullptr);

  if (hDevice == INVALID_HANDLE_VALUE) {
    return sector_size;
  }

  STORAGE_PROPERTY_QUERY spropertyquery;
  spropertyquery.PropertyId = StorageAccessAlignmentProperty;
  spropertyquery.QueryType = PropertyStandardQuery;

  BYTE output_buffer[sizeof(STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR)];
  DWORD output_bytes = 0;

  BOOL ret = DeviceIoControl(hDevice, IOCTL_STORAGE_QUERY_PROPERTY,
              &spropertyquery, sizeof(spropertyquery), output_buffer,
              sizeof(STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR), &output_bytes, nullptr);

  if (ret) {
    sector_size = ((STORAGE_ACCESS_ALIGNMENT_DESCRIPTOR *)output_buffer)->BytesPerLogicalSector;
  } else {
    // many devices do not support StorageProcessAlignmentProperty. Any failure here and we
    // fall back to logical alignment

    DISK_GEOMETRY_EX geometry = { 0 };
    ret = DeviceIoControl(hDevice, IOCTL_DISK_GET_DRIVE_GEOMETRY,
           nullptr, 0, &geometry, sizeof(geometry), &output_bytes, nullptr);
    if (ret) {
      sector_size = geometry.Geometry.BytesPerSector;
    }
  }

  if (hDevice != INVALID_HANDLE_VALUE) {
    CloseHandle(hDevice);
  }

  return sector_size;
}

////////////////////////////////////////////////////////////////////////
// WinEnvThreads

WinEnvThreads::WinEnvThreads(Env* hosted_env) : hosted_env_(hosted_env), thread_pools_(Env::Priority::TOTAL) {

  for (int pool_id = 0; pool_id < Env::Priority::TOTAL; ++pool_id) {
    thread_pools_[pool_id].SetThreadPriority(
      static_cast<Env::Priority>(pool_id));
    // This allows later initializing the thread-local-env of each thread.
    thread_pools_[pool_id].SetHostEnv(hosted_env);
  }
}

WinEnvThreads::~WinEnvThreads() {

  WaitForJoin();

  for (auto& thpool : thread_pools_) {
    thpool.JoinAllThreads();
  }
}

void WinEnvThreads::Schedule(void(*function)(void*), void* arg, Env::Priority pri,
  void* tag, void(*unschedFunction)(void* arg)) {
  assert(pri >= Env::Priority::BOTTOM && pri <= Env::Priority::HIGH);
  thread_pools_[pri].Schedule(function, arg, tag, unschedFunction);
}

int WinEnvThreads::UnSchedule(void* arg, Env::Priority pri) {
  return thread_pools_[pri].UnSchedule(arg);
}

namespace {

  struct StartThreadState {
    void(*user_function)(void*);
    void* arg;
  };

  void* StartThreadWrapper(void* arg) {
    std::unique_ptr<StartThreadState> state(
      reinterpret_cast<StartThreadState*>(arg));
    state->user_function(state->arg);
    return nullptr;
  }

}

void WinEnvThreads::StartThread(void(*function)(void* arg), void* arg) {
  std::unique_ptr<StartThreadState> state(new StartThreadState);
  state->user_function = function;
  state->arg = arg;
  try {

    rocksdb::port::WindowsThread th(&StartThreadWrapper, state.get());
    state.release();

    std::lock_guard<std::mutex> lg(mu_);
    threads_to_join_.push_back(std::move(th));

  } catch (const std::system_error& ex) {
    WinthreadCall("start thread", ex.code());
  }
}

void WinEnvThreads::WaitForJoin() {
  for (auto& th : threads_to_join_) {
    th.join();
  }
  threads_to_join_.clear();
}

unsigned int WinEnvThreads::GetThreadPoolQueueLen(Env::Priority pri) const {
  assert(pri >= Env::Priority::BOTTOM && pri <= Env::Priority::HIGH);
  return thread_pools_[pri].GetQueueLen();
}

uint64_t WinEnvThreads::gettid() {
  uint64_t thread_id = GetCurrentThreadId();
  return thread_id;
}

uint64_t WinEnvThreads::GetThreadID() const { return gettid(); }

void  WinEnvThreads::SleepForMicroseconds(int micros) {
  std::this_thread::sleep_for(std::chrono::microseconds(micros));
}

void WinEnvThreads::SetBackgroundThreads(int num, Env::Priority pri) {
  assert(pri >= Env::Priority::BOTTOM && pri <= Env::Priority::HIGH);
  thread_pools_[pri].SetBackgroundThreads(num);
}

int WinEnvThreads::GetBackgroundThreads(Env::Priority pri) {
  assert(pri >= Env::Priority::BOTTOM && pri <= Env::Priority::HIGH);
  return thread_pools_[pri].GetBackgroundThreads();
}

void WinEnvThreads::IncBackgroundThreadsIfNeeded(int num, Env::Priority pri) {
  assert(pri >= Env::Priority::BOTTOM && pri <= Env::Priority::HIGH);
  thread_pools_[pri].IncBackgroundThreadsIfNeeded(num);
}

/////////////////////////////////////////////////////////////////////////
// WinEnv

WinEnv::WinEnv() : winenv_io_(this), winenv_threads_(this) {
  // Protected member of the base class
  thread_status_updater_ = CreateThreadStatusUpdater();
}


WinEnv::~WinEnv() {
  // All threads must be joined before the deletion of
  // thread_status_updater_.
  delete thread_status_updater_;
}

Status WinEnv::GetThreadList(
  std::vector<ThreadStatus>* thread_list) {
  assert(thread_status_updater_);
  return thread_status_updater_->GetThreadList(thread_list);
}

Status WinEnv::DeleteFile(const std::string& fname) {
  return winenv_io_.DeleteFile(fname);
}

Status WinEnv::Truncate(const std::string& fname, size_t size) {
  return winenv_io_.Truncate(fname, size);
}

Status WinEnv::GetCurrentTime(int64_t* unix_time) {
  return winenv_io_.GetCurrentTime(unix_time);
}

Status  WinEnv::NewSequentialFile(const std::string& fname,
  std::unique_ptr<SequentialFile>* result,
  const EnvOptions& options) {
  return winenv_io_.NewSequentialFile(fname, result, options);
}

Status WinEnv::NewRandomAccessFile(const std::string& fname,
  std::unique_ptr<RandomAccessFile>* result,
  const EnvOptions& options) {
  return winenv_io_.NewRandomAccessFile(fname, result, options);
}

Status WinEnv::NewWritableFile(const std::string& fname,
                               std::unique_ptr<WritableFile>* result,
                               const EnvOptions& options) {
  return winenv_io_.OpenWritableFile(fname, result, options, false);
}

Status WinEnv::ReopenWritableFile(const std::string& fname,
    std::unique_ptr<WritableFile>* result, const EnvOptions& options) {
  return winenv_io_.OpenWritableFile(fname, result, options, true);
}

Status WinEnv::NewRandomRWFile(const std::string & fname,
  std::unique_ptr<RandomRWFile>* result, const EnvOptions & options) {
  return winenv_io_.NewRandomRWFile(fname, result, options);
}

Status WinEnv::NewMemoryMappedFileBuffer(const std::string& fname,
  std::unique_ptr<MemoryMappedFileBuffer>* result) {
  return winenv_io_.NewMemoryMappedFileBuffer(fname, result);
}

Status WinEnv::NewDirectory(const std::string& name,
  std::unique_ptr<Directory>* result) {
  return winenv_io_.NewDirectory(name, result);
}

Status WinEnv::FileExists(const std::string& fname) {
  return winenv_io_.FileExists(fname);
}

Status WinEnv::GetChildren(const std::string& dir,
  std::vector<std::string>* result) {
  return winenv_io_.GetChildren(dir, result);
}

Status WinEnv::CreateDir(const std::string& name) {
  return winenv_io_.CreateDir(name);
}

Status WinEnv::CreateDirIfMissing(const std::string& name) {
  return winenv_io_.CreateDirIfMissing(name);
}

Status WinEnv::DeleteDir(const std::string& name) {
  return winenv_io_.DeleteDir(name);
}

Status WinEnv::GetFileSize(const std::string& fname,
  uint64_t* size) {
  return winenv_io_.GetFileSize(fname, size);
}

Status  WinEnv::GetFileModificationTime(const std::string& fname,
  uint64_t* file_mtime) {
  return winenv_io_.GetFileModificationTime(fname, file_mtime);
}

Status WinEnv::RenameFile(const std::string& src,
  const std::string& target) {
  return winenv_io_.RenameFile(src, target);
}

Status WinEnv::LinkFile(const std::string& src,
  const std::string& target) {
  return winenv_io_.LinkFile(src, target);
}

Status WinEnv::NumFileLinks(const std::string& fname, uint64_t* count) {
  return winenv_io_.NumFileLinks(fname, count);
}

Status WinEnv::AreFilesSame(const std::string& first,
  const std::string& second, bool* res) {
  return winenv_io_.AreFilesSame(first, second, res);
}

Status WinEnv::LockFile(const std::string& lockFname,
  FileLock** lock) {
  return winenv_io_.LockFile(lockFname, lock);
}

Status WinEnv::UnlockFile(FileLock* lock) {
  return winenv_io_.UnlockFile(lock);
}

Status  WinEnv::GetTestDirectory(std::string* result) {
  return winenv_io_.GetTestDirectory(result);
}

Status WinEnv::NewLogger(const std::string& fname,
  std::shared_ptr<Logger>* result) {
  return winenv_io_.NewLogger(fname, result);
}

uint64_t WinEnv::NowMicros() {
  return winenv_io_.NowMicros();
}

uint64_t  WinEnv::NowNanos() {
  return winenv_io_.NowNanos();
}

Status WinEnv::GetHostName(char* name, uint64_t len) {
  return winenv_io_.GetHostName(name, len);
}

Status WinEnv::GetAbsolutePath(const std::string& db_path,
  std::string* output_path) {
  return winenv_io_.GetAbsolutePath(db_path, output_path);
}

std::string WinEnv::TimeToString(uint64_t secondsSince1970) {
  return winenv_io_.TimeToString(secondsSince1970);
}

void  WinEnv::Schedule(void(*function)(void*), void* arg, Env::Priority pri,
  void* tag,
  void(*unschedFunction)(void* arg)) {
  return winenv_threads_.Schedule(function, arg, pri, tag, unschedFunction);
}

int WinEnv::UnSchedule(void* arg, Env::Priority pri) {
  return winenv_threads_.UnSchedule(arg, pri);
}

void WinEnv::StartThread(void(*function)(void* arg), void* arg) {
  return winenv_threads_.StartThread(function, arg);
}

void WinEnv::WaitForJoin() {
  return winenv_threads_.WaitForJoin();
}

unsigned int  WinEnv::GetThreadPoolQueueLen(Env::Priority pri) const {
  return winenv_threads_.GetThreadPoolQueueLen(pri);
}

uint64_t WinEnv::GetThreadID() const {
  return winenv_threads_.GetThreadID();
}

void WinEnv::SleepForMicroseconds(int micros) {
  return winenv_threads_.SleepForMicroseconds(micros);
}

// Allow increasing the number of worker threads.
void  WinEnv::SetBackgroundThreads(int num, Env::Priority pri) {
  return winenv_threads_.SetBackgroundThreads(num, pri);
}

int WinEnv::GetBackgroundThreads(Env::Priority pri) {
  return winenv_threads_.GetBackgroundThreads(pri);
}

void  WinEnv::IncBackgroundThreadsIfNeeded(int num, Env::Priority pri) {
  return winenv_threads_.IncBackgroundThreadsIfNeeded(num, pri);
}

EnvOptions WinEnv::OptimizeForManifestRead(
  const EnvOptions& env_options) const {
  return winenv_io_.OptimizeForManifestRead(env_options);
}

EnvOptions WinEnv::OptimizeForLogWrite(const EnvOptions& env_options,
  const DBOptions& db_options) const {
  return winenv_io_.OptimizeForLogWrite(env_options, db_options);
}

EnvOptions WinEnv::OptimizeForManifestWrite(
  const EnvOptions& env_options) const {
  return winenv_io_.OptimizeForManifestWrite(env_options);
}

}  // namespace port

std::string Env::GenerateUniqueId() {
  std::string result;

  UUID uuid;
  UuidCreateSequential(&uuid);

  RPC_CSTR rpc_str;
  auto status = UuidToStringA(&uuid, &rpc_str);
  (void)status;
  assert(status == RPC_S_OK);

  result = reinterpret_cast<char*>(rpc_str);

  status = RpcStringFreeA(&rpc_str);
  assert(status == RPC_S_OK);

  return result;
}

}  // namespace rocksdb
