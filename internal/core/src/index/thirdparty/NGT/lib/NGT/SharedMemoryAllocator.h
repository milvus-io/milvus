//
// Copyright (C) 2015-2020 Yahoo Japan Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#pragma once

#include	"NGT/defines.h"
#include	"NGT/MmapManagerDefs.h"
#include	"NGT/MmapManager.h"

#include	<unistd.h>
#include	<cstdlib>
#include	<cstring>
#include	<string>
#include	<iostream>
#include	<vector>
#include	<exception>
#include	<cassert>

#define		MMAP_MANAGER



///////////////////////////////////////////////////////////////////////
class SharedMemoryAllocator {
 public:
  enum GetMemorySizeType {
    GetTotalMemorySize		= 0,
    GetAllocatedMemorySize	= 1,
    GetFreedMemorySize		= 2
  };

  SharedMemoryAllocator():isValid(false) { 
#ifdef SMA_TRACE
    std::cerr << "SharedMemoryAllocatorSiglton::constructor" << std::endl; 
#endif
  }
  SharedMemoryAllocator(const SharedMemoryAllocator& a){}
  SharedMemoryAllocator& operator=(const SharedMemoryAllocator& a){ return *this; }  
 public:
  void* allocate(size_t size) {
    if (isValid == false) {
      std::cerr << "SharedMemoryAllocator::allocate: Fatal error! " << std::endl;
      assert(isValid);
    }
#ifdef SMA_TRACE
    std::cerr << "SharedMemoryAllocator::allocate: size=" << size << std::endl;
    std::cerr << "SharedMemoryAllocator::allocate: before " << getTotalSize() << ":" << getAllocatedSize() << ":" << getFreedSize() << std::endl;
#endif
#if defined(MMAP_MANAGER) && !defined(NOT_USE_MMAP_ALLOCATOR)
    if(!isValid){
      return NULL;
    }
    off_t file_offset = mmanager->alloc(size, true);	
    if (file_offset == -1) {
      std::cerr << "Fatal Error: Allocating memory size is too big for this settings." << std::endl;
      std::cerr << "             Max allocation size should be enlarged." << std::endl;
      abort();
    }
    void *p = mmanager->getAbsAddr(file_offset);
    std::memset(p, 0, size);
#ifdef SMA_TRACE
    std::cerr << "SharedMemoryAllocator::allocate: end" <<std::endl;
#endif
    return p;
#else
    void *ptr = std::malloc(size);
    std::memset(ptr, 0, size);
    return ptr;
#endif
  }
  void free(void *ptr) {
#ifdef SMA_TRACE
    std::cerr << "SharedMemoryAllocator::free: ptr=" << ptr << std::endl;
#endif
    if (ptr == 0) {
      std::cerr << "SharedMemoryAllocator::free: ptr is invalid! ptr=" << ptr << std::endl;
    }
    if (ptr == 0) {
      return;
    }
#if defined(MMAP_MANAGER) && !defined(NOT_USE_MMAP_ALLOCATOR)
    off_t file_offset = mmanager->getRelAddr(ptr);
    mmanager->free(file_offset);
#else
    std::free(ptr);
#endif
  }

  void *construct(const std::string &filePath, size_t memorysize = 0) {
    file = filePath;	// debug
#ifdef SMA_TRACE
    std::cerr << "ObjectSharedMemoryAllocator::construct: file " << filePath << std::endl;
#endif
    void *hook = 0;
#ifdef MMAP_MANAGER
    mmanager = new MemoryManager::MmapManager();
    // msize is the maximum allocated size (M byte) at once.
    size_t msize = memorysize;
    if (msize == 0) {
      msize = NGT_SHARED_MEMORY_MAX_SIZE;
    }
    size_t bsize = msize * 1048576 / MemoryManager::MMAP_CNTL_PAGE_SIZE + 1; // 1048576=1M
    uint64_t size = bsize * MemoryManager::MMAP_CNTL_PAGE_SIZE;
    MemoryManager::init_option_st option;
    MemoryManager::MmapManager::setDefaultOptionValue(option);
    option.use_expand = true;
    option.reuse_type = MemoryManager::REUSE_DATA_CLASSIFY;
    bool create = true;
    if(!mmanager->init(filePath, size, &option)){
#ifdef SMA_TRACE
      std::cerr << "SMA: info. already existed." << std::endl;
#endif
      create = false;
    } else {
#ifdef SMA_TRACE
      std::cerr << "SMA::construct: msize=" << msize << ":" << memorysize << std::endl;
#endif
    }
    if(!mmanager->openMemory(filePath)){
      std::cerr << "SMA: open error" << std::endl;
      return 0;
    }
    if (!create) {
#ifdef SMA_TRACE
      std::cerr << "SMA: get hook to initialize data structure" << std::endl;
#endif
      hook = mmanager->getEntryHook();
      assert(hook != 0);
    }
#endif
    isValid = true;
#ifdef SMA_TRACE
    std::cerr << "SharedMemoryAllocator::construct: " << filePath << " total=" 
	      << getTotalSize() << " allocated=" << getAllocatedSize() << " freed=" 
	      << getFreedSize() << " (" << (double)getFreedSize() / (double)getTotalSize() << ") " << std::endl;
#endif
    return hook;
  }
  void destruct() {
    if (!isValid) {
      return;
    }
    isValid = false;
#ifdef MMAP_MANAGER
    mmanager->closeMemory();
    delete mmanager;
#endif
  };
  void setEntry(void *entry) {
#ifdef MMAP_MANAGER
    mmanager->setEntryHook(entry);
#endif
  }
  void *getAddr(off_t oft) { 
    if (oft == 0) {
      return 0;
    }
    assert(oft > 0);
#if defined(MMAP_MANAGER) && !defined(NOT_USE_MMAP_ALLOCATOR)
    return mmanager->getAbsAddr(oft); 
#else
    return reinterpret_cast<void*>(oft);
#endif
  }
  off_t getOffset(void *adr) { 
    if (adr == 0) {
      return 0;
    }
#if defined(MMAP_MANAGER) && !defined(NOT_USE_MMAP_ALLOCATOR)
    return mmanager->getRelAddr(adr); 
#else
    return static_cast<off_t>(reinterpret_cast<int64_t>(adr));
#endif
  }
  size_t getMemorySize(GetMemorySizeType t) {
    switch (t) {
    case GetTotalMemorySize : 	  return getTotalSize();
    case GetAllocatedMemorySize : return getAllocatedSize();
    case GetFreedMemorySize :	  return getFreedSize();
    }
    return getTotalSize();
  }
  size_t getTotalSize() { return mmanager->getTotalSize(); }
  size_t getAllocatedSize() { return mmanager->getUseSize(); }
  size_t getFreedSize() { return mmanager->getFreeSize(); }

  bool isValid;
  std::string file;
#ifdef MMAP_MANAGER
  MemoryManager::MmapManager *mmanager;
#endif
};

/////////////////////////////////////////////////////////////////////////

void* operator new(size_t size, SharedMemoryAllocator &allocator);
void* operator new[](size_t size, SharedMemoryAllocator &allocator);
