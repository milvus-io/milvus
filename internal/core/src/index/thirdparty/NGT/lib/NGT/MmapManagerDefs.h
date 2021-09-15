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

#include "MmapManager.h"

#include <unistd.h>

namespace MemoryManager{
  const uint64_t MMAP_MANAGER_VERSION = 5;
  
  const bool MMAP_DEFAULT_ALLOW_EXPAND = false; 
  const uint64_t MMAP_CNTL_FILE_RANGE = 16; 
  #ifdef WIN32
  const size_t MMAP_CNTL_PAGE_SIZE = 65536;
  #else
  const size_t MMAP_CNTL_PAGE_SIZE = sysconf(_SC_PAGESIZE);
  #endif
  const size_t MMAP_CNTL_FILE_SIZE = MMAP_CNTL_FILE_RANGE * MMAP_CNTL_PAGE_SIZE;
  const uint64_t MMAP_MAX_FILE_NAME_LENGTH = 1024; 
  const std::string MMAP_CNTL_FILE_SUFFIX = "c"; 
  
  const size_t MMAP_LOWER_SIZE = 1; 
  const size_t MMAP_MEMORY_ALIGN = 8;   
  const size_t MMAP_MEMORY_ALIGN_EXP = 3;

#ifndef MMANAGER_TEST_MODE
  const uint64_t MMAP_MAX_UNIT_NUM = 1024; 
#else
  const uint64_t MMAP_MAX_UNIT_NUM = 8; 
#endif
  
  const uint64_t MMAP_FREE_QUEUE_SIZE = 1024;
  
  const uint64_t MMAP_FREE_LIST_NUM = 64;    

  typedef struct _boot_st{
    uint32_t version; 
    uint64_t reserve; 
    size_t size;      
  }boot_st;

  typedef struct _head_st{
    off_t break_p;    
    uint64_t chunk_num; 
    uint64_t reserve; 
  }head_st;

  
  typedef struct _free_list_st{
    off_t free_p;
    off_t free_last_p;
  }free_list_st;

  
  typedef struct _free_st{
    free_list_st large_list;
    free_list_st free_lists[MMAP_FREE_LIST_NUM];
  }free_st;

  
  typedef struct _free_queue_st{
    off_t data;
    size_t capacity;
    uint64_t tail;
  }free_queue_st;

  
  
  typedef struct _control_st{
    bool use_expand;   
    uint16_t unit_num;   
    uint16_t active_unit; 
    uint64_t reserve;   
    size_t base_size;   
    off_t entry_p;      
    option_reuse_t reuse_type;  
    free_st free_data;   
    free_queue_st free_queue; 
    head_st data_headers[MMAP_MAX_UNIT_NUM]; 
  }control_st;

  typedef struct _chunk_head_st{
    bool delete_flg; 
    uint16_t unit_id; 
    off_t free_next;  
    size_t size; 
  }chunk_head_st;
}
