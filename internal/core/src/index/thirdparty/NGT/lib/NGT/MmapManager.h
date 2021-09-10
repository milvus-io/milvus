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

#include <sys/types.h>
#include <stdint.h>
#include <string>
#include <memory>

#define ABS_ADDR(x, y) (void *)(x + (char *)y);

#define USE_MMAP_MANAGER

namespace MemoryManager{
  
  typedef enum _option_reuse_t{
    REUSE_DATA_CLASSIFY,
    REUSE_DATA_QUEUE,
    REUSE_DATA_QUEUE_PLUS,    
  }option_reuse_t;

  typedef enum _reuse_state_t{
    REUSE_STATE_OK, 
    REUSE_STATE_FALSE, 
    REUSE_STATE_ALLOC, 
  }reuse_state_t;

  typedef enum _check_statistics_t{
    CHECK_STATS_USE_SIZE,  
    CHECK_STATS_USE_NUM,   
    CHECK_STATS_FREE_SIZE, 
    CHECK_STATS_FREE_NUM,  
  }check_statistics_t;

  typedef struct _init_option_st{
    bool use_expand;  
    option_reuse_t reuse_type; 
  }init_option_st;


  class MmapManager{
  public:
    MmapManager();
    ~MmapManager();
        
    bool init(const std::string &filePath, size_t size, const init_option_st *optionst = NULL) const;
    bool openMemory(const std::string &filePath);
    void closeMemory(const bool force = false);
    off_t alloc(const size_t size, const bool not_reuse_flag = false);
    void free(const off_t p);
    off_t reuse(const size_t size, reuse_state_t &reuse_state);
    void *getAbsAddr(off_t p) const;
    off_t getRelAddr(const void *p) const;

    size_t getTotalSize() const; 
    size_t getUseSize() const;   
    uint64_t getUseNum() const;  
    size_t getFreeSize() const;  
    uint64_t getFreeNum() const; 
    uint16_t getUnitNum() const; 
    size_t getQueueCapacity() const; 
    uint64_t getQueueNum() const; 
    uint64_t getLargeListNum() const;    

    void dumpHeap() const;

    bool isOpen() const;
    void *getEntryHook() const;
    void setEntryHook(const void *entry_p);

    // static method --- 
    static void setDefaultOptionValue(init_option_st &optionst);
    static size_t getAlignSize(size_t size);

  private:
    class Impl;
    std::unique_ptr<Impl> _impl;
  };

  std::string getErrorStr(int32_t err_num);
}
