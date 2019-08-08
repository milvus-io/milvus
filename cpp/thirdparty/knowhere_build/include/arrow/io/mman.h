// Copyright https://code.google.com/p/mman-win32/
//
// Licensed under the MIT License;
// You may obtain a copy of the License at
//
// https://opensource.org/licenses/MIT

#ifndef _MMAN_WIN32_H
#define _MMAN_WIN32_H

#include "arrow/util/windows_compatibility.h"

#include <errno.h>
#include <io.h>
#include <sys/types.h>

#define PROT_NONE 0
#define PROT_READ 1
#define PROT_WRITE 2
#define PROT_EXEC 4

#define MAP_FILE 0
#define MAP_SHARED 1
#define MAP_PRIVATE 2
#define MAP_TYPE 0xf
#define MAP_FIXED 0x10
#define MAP_ANONYMOUS 0x20
#define MAP_ANON MAP_ANONYMOUS

#define MAP_FAILED ((void*)-1)

/* Flags for msync. */
#define MS_ASYNC 1
#define MS_SYNC 2
#define MS_INVALIDATE 4

#ifndef FILE_MAP_EXECUTE
#define FILE_MAP_EXECUTE 0x0020
#endif

static inline int __map_mman_error(const DWORD err, const int deferr) {
  if (err == 0) return 0;
  // TODO: implement
  return err;
}

static inline DWORD __map_mmap_prot_page(const int prot) {
  DWORD protect = 0;

  if (prot == PROT_NONE) return protect;

  if ((prot & PROT_EXEC) != 0) {
    protect = ((prot & PROT_WRITE) != 0) ? PAGE_EXECUTE_READWRITE : PAGE_EXECUTE_READ;
  } else {
    protect = ((prot & PROT_WRITE) != 0) ? PAGE_READWRITE : PAGE_READONLY;
  }

  return protect;
}

static inline DWORD __map_mmap_prot_file(const int prot) {
  DWORD desiredAccess = 0;

  if (prot == PROT_NONE) return desiredAccess;

  if ((prot & PROT_READ) != 0) desiredAccess |= FILE_MAP_READ;
  if ((prot & PROT_WRITE) != 0) desiredAccess |= FILE_MAP_WRITE;
  if ((prot & PROT_EXEC) != 0) desiredAccess |= FILE_MAP_EXECUTE;

  return desiredAccess;
}

static inline void* mmap(void* addr, size_t len, int prot, int flags, int fildes,
                         off_t off) {
  HANDLE fm, h;

  void* map = MAP_FAILED;

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4293)
#endif

  const DWORD dwFileOffsetLow =
      (sizeof(off_t) <= sizeof(DWORD)) ? (DWORD)off : (DWORD)(off & 0xFFFFFFFFL);
  const DWORD dwFileOffsetHigh =
      (sizeof(off_t) <= sizeof(DWORD)) ? (DWORD)0 : (DWORD)((off >> 32) & 0xFFFFFFFFL);
  const DWORD protect = __map_mmap_prot_page(prot);
  const DWORD desiredAccess = __map_mmap_prot_file(prot);

  const size_t maxSize = off + len;

  const DWORD dwMaxSizeLow = static_cast<DWORD>(maxSize & 0xFFFFFFFFL);
  const DWORD dwMaxSizeHigh = static_cast<DWORD>((maxSize >> 32) & 0xFFFFFFFFL);

#ifdef _MSC_VER
#pragma warning(pop)
#endif

  errno = 0;

  if (len == 0
      /* Unsupported flag combinations */
      || (flags & MAP_FIXED) != 0
      /* Usupported protection combinations */
      || prot == PROT_EXEC) {
    errno = EINVAL;
    return MAP_FAILED;
  }

  h = ((flags & MAP_ANONYMOUS) == 0) ? (HANDLE)_get_osfhandle(fildes)
                                     : INVALID_HANDLE_VALUE;

  if ((flags & MAP_ANONYMOUS) == 0 && h == INVALID_HANDLE_VALUE) {
    errno = EBADF;
    return MAP_FAILED;
  }

  fm = CreateFileMapping(h, NULL, protect, dwMaxSizeHigh, dwMaxSizeLow, NULL);

  if (fm == NULL) {
    errno = __map_mman_error(GetLastError(), EPERM);
    return MAP_FAILED;
  }

  map = MapViewOfFile(fm, desiredAccess, dwFileOffsetHigh, dwFileOffsetLow, len);

  CloseHandle(fm);

  if (map == NULL) {
    errno = __map_mman_error(GetLastError(), EPERM);
    return MAP_FAILED;
  }

  return map;
}

static inline int munmap(void* addr, size_t len) {
  if (UnmapViewOfFile(addr)) return 0;

  errno = __map_mman_error(GetLastError(), EPERM);

  return -1;
}

static inline int mprotect(void* addr, size_t len, int prot) {
  DWORD newProtect = __map_mmap_prot_page(prot);
  DWORD oldProtect = 0;

  if (VirtualProtect(addr, len, newProtect, &oldProtect)) return 0;

  errno = __map_mman_error(GetLastError(), EPERM);

  return -1;
}

static inline int msync(void* addr, size_t len, int flags) {
  if (FlushViewOfFile(addr, len)) return 0;

  errno = __map_mman_error(GetLastError(), EPERM);

  return -1;
}

static inline int mlock(const void* addr, size_t len) {
  if (VirtualLock((LPVOID)addr, len)) return 0;

  errno = __map_mman_error(GetLastError(), EPERM);

  return -1;
}

static inline int munlock(const void* addr, size_t len) {
  if (VirtualUnlock((LPVOID)addr, len)) return 0;

  errno = __map_mman_error(GetLastError(), EPERM);

  return -1;
}

#endif
