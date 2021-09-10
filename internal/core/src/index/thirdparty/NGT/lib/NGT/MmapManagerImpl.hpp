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

#include "defines.h"
#include "MmapManagerDefs.h"
#include "MmapManagerException.h"

#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <sstream>
#include <cstring>
#include <cassert>

namespace MemoryManager{
  
  class MmapManager::Impl{
  public:
    Impl() = delete;
    Impl(MmapManager &ommanager);
    virtual ~Impl(){}

    MmapManager &mmanager;
    bool isOpen;
    void *mmapCntlAddr;
    control_st *mmapCntlHead;
    std::string filePath;         
    void *mmapDataAddr[MMAP_MAX_UNIT_NUM];

    void initBootStruct(boot_st &bst, size_t size) const;
    void initFreeStruct(free_st &fst) const;
    void initFreeQueue(free_queue_st &fqst) const;
    void initControlStruct(control_st &cntlst, size_t size) const;

    void setupChunkHead(chunk_head_st *chunk_head, const bool delete_flg, const uint16_t unit_id, const off_t free_next, const size_t size) const;
    bool expandMemory();
    int32_t formatFile(const std::string &targetFile, size_t size) const;
    void clearChunk(const off_t chunk_off) const;

    void free_data_classify(const off_t p, const bool force_large_list = false) const;    
    off_t reuse_data_classify(const size_t size, reuse_state_t &reuse_state, const bool force_large_list = false) const;
    void free_data_queue(const off_t p);
    off_t reuse_data_queue(const size_t size, reuse_state_t &reuse_state);
    void free_data_queue_plus(const off_t p);
    off_t reuse_data_queue_plus(const size_t size, reuse_state_t &reuse_state);
    
    bool scanAllData(void *target, const check_statistics_t stats_type) const;

    void upHeap(free_queue_st *free_queue, uint64_t index) const;
    void downHeap(free_queue_st *free_queue)const;
    bool insertHeap(free_queue_st *free_queue, const off_t p) const;
    bool getHeap(free_queue_st *free_queue, off_t *p) const;
    size_t getMaxHeapValue(free_queue_st *free_queue) const;
    void dumpHeap() const;
    
    void divChunk(const off_t chunk_offset, const size_t size);
  };


  MmapManager::Impl::Impl(MmapManager &ommanager):mmanager(ommanager), isOpen(false), mmapCntlAddr(NULL), mmapCntlHead(NULL){}
  
  
  void MmapManager::Impl::initBootStruct(boot_st &bst, size_t size) const 
  {
    bst.version = MMAP_MANAGER_VERSION;
    bst.reserve = 0;
    bst.size = size;
  }
  
  void MmapManager::Impl::initFreeStruct(free_st &fst) const
  {
    fst.large_list.free_p = -1;
    fst.large_list.free_last_p = -1;	
    for(uint32_t i = 0; i < MMAP_FREE_LIST_NUM; ++i){
      fst.free_lists[i].free_p = -1;
      fst.free_lists[i].free_last_p = -1;	
    }
  }
  
  void MmapManager::Impl::initFreeQueue(free_queue_st &fqst) const
  {
    fqst.data = -1;
    fqst.capacity = MMAP_FREE_QUEUE_SIZE;
    fqst.tail = 1;
  }
  
  void MmapManager::Impl::initControlStruct(control_st &cntlst, size_t size) const
  {
    cntlst.use_expand = MMAP_DEFAULT_ALLOW_EXPAND;
    cntlst.unit_num = 1;
    cntlst.active_unit = 0;
    cntlst.reserve = 0;
    cntlst.base_size = size;
    cntlst.entry_p = 0;
    cntlst.reuse_type = REUSE_DATA_CLASSIFY;
    initFreeStruct(cntlst.free_data);
    initFreeQueue(cntlst.free_queue);
    memset(cntlst.data_headers, 0, sizeof(head_st) * MMAP_MAX_UNIT_NUM);
  }
  
  void MmapManager::Impl::setupChunkHead(chunk_head_st *chunk_head, const bool delete_flg, const uint16_t unit_id, const off_t free_next, const size_t size) const
  {
      chunk_head_st chunk_buffer;
      chunk_buffer.delete_flg = delete_flg;
      chunk_buffer.unit_id    = unit_id;
      chunk_buffer.free_next  = free_next;
      chunk_buffer.size       = size;
      
      memcpy(chunk_head, &chunk_buffer, sizeof(chunk_head_st));
  }

  bool MmapManager::Impl::expandMemory()
  {
    const uint16_t new_unit_num = mmapCntlHead->unit_num + 1;
    const size_t new_file_size = mmapCntlHead->base_size * new_unit_num;
    const off_t old_file_size = mmapCntlHead->base_size * mmapCntlHead->unit_num;

    if(new_unit_num >= MMAP_MAX_UNIT_NUM){
        if (NGT_LOG_DEBUG_)
          (*NGT_LOG_DEBUG_)("over max unit num");
//      std::cerr << "over max unit num" << std::endl;
      return false;
    }

    int32_t fd = formatFile(filePath, new_file_size);
    assert(fd >= 0);

    const off_t offset = mmapCntlHead->base_size * mmapCntlHead->unit_num;
    errno = 0;
    void *new_area = mmap(NULL, mmapCntlHead->base_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, offset);
    if(new_area == MAP_FAILED){
      const std::string err_str = getErrorStr(errno);
      
      errno = 0;
      if(ftruncate(fd, old_file_size) == -1){
        const std::string err_str = getErrorStr(errno);
        throw MmapManagerException("truncate error" + err_str);
      }
      
      if(close(fd) == -1 && NGT_LOG_DEBUG_)
        (*NGT_LOG_DEBUG_)(filePath + "[WARN] : filedescript cannot close");
//        std::cerr << filePath << "[WARN] : filedescript cannot close" << std::endl;
      throw MmapManagerException("mmap error" + err_str);
    }
    if(close(fd) == -1 && NGT_LOG_DEBUG_)
      (*NGT_LOG_DEBUG_)(filePath + "[WARN] : filedescript cannot close");
//      std::cerr << filePath << "[WARN] : filedescript cannot close" << std::endl;
    
    mmapDataAddr[mmapCntlHead->unit_num] = new_area;
    
    mmapCntlHead->unit_num = new_unit_num;
    mmapCntlHead->active_unit++;
    
    return true;
  }

  int32_t MmapManager::Impl::formatFile(const std::string &targetFile, size_t size) const
  {
    const char *c = "";
    int32_t fd;
    
    errno = 0;
    if((fd = open(targetFile.c_str(), O_RDWR|O_CREAT, 0666)) == -1){
      std::stringstream ss;
      ss << "[ERR] Cannot open the file. " << targetFile << " " << getErrorStr(errno);
      throw MmapManagerException(ss.str());
    }
    errno = 0;
    if(lseek(fd, (off_t)size-1, SEEK_SET) < 0){
      std::stringstream ss;
      ss << "[ERR] Cannot seek the file. " << targetFile << " " << getErrorStr(errno);
      if(close(fd) == -1 && NGT_LOG_DEBUG_)
        (*NGT_LOG_DEBUG_)(targetFile + "[WARN] : filedescript cannot close");
//        std::cerr << targetFile << "[WARN] : filedescript cannot close" << std::endl;
      throw MmapManagerException(ss.str());
    }
    errno = 0;
    if(write(fd, &c, sizeof(char)) == -1){
      std::stringstream ss;
      ss << "[ERR] Cannot write the file. Check the disk space. " << targetFile << " " << getErrorStr(errno);
      if(close(fd) == -1 && NGT_LOG_DEBUG_)
        (*NGT_LOG_DEBUG_)(targetFile + "[WARN] : filedescript cannot close");
//        std::cerr << targetFile << "[WARN] : filedescript cannot close" << std::endl;
      throw MmapManagerException(ss.str());
    }
    
    return fd;
  }

  void MmapManager::Impl::clearChunk(const off_t chunk_off) const
  {
    chunk_head_st *chunk_head = (chunk_head_st *)mmanager.getAbsAddr(chunk_off);
    const off_t payload_off = chunk_off + sizeof(chunk_head_st);
    
    chunk_head->delete_flg = false;
    chunk_head->free_next = -1;
    char *payload_addr = (char *)mmanager.getAbsAddr(payload_off);
    memset(payload_addr, 0, chunk_head->size);
  }

  void MmapManager::Impl::free_data_classify(const off_t p, const bool force_large_list) const
  {
    const off_t chunk_offset = p - sizeof(chunk_head_st);
    chunk_head_st *chunk_head = (chunk_head_st *)mmanager.getAbsAddr(chunk_offset);
    const size_t p_size = chunk_head->size;

    
    
    const size_t border_size = MMAP_MEMORY_ALIGN * MMAP_FREE_LIST_NUM;

    free_list_st *free_list;
    if(p_size <= border_size && force_large_list == false){
      uint32_t index = (p_size / MMAP_MEMORY_ALIGN) - 1; 
      free_list = &mmapCntlHead->free_data.free_lists[index];
    }else{
      free_list = &mmapCntlHead->free_data.large_list;
    }
    
    if(free_list->free_p == -1){
      free_list->free_p = free_list->free_last_p = chunk_offset;
    }else{
      off_t last_off = free_list->free_last_p;
      chunk_head_st *tmp_chunk_head = (chunk_head_st *)mmanager.getAbsAddr(last_off);
      free_list->free_last_p = tmp_chunk_head->free_next = chunk_offset;
    }
    chunk_head->delete_flg = true;
  }

  off_t MmapManager::Impl::reuse_data_classify(const size_t size, reuse_state_t &reuse_state, const bool force_large_list) const
  {
    
    
    const size_t border_size = MMAP_MEMORY_ALIGN * MMAP_FREE_LIST_NUM;

    free_list_st *free_list;
    if(size <= border_size && force_large_list == false){
      uint32_t index = (size / MMAP_MEMORY_ALIGN) - 1; 
      free_list = &mmapCntlHead->free_data.free_lists[index];
    }else{
      free_list = &mmapCntlHead->free_data.large_list;
    }
    
    if(free_list->free_p == -1){
      reuse_state = REUSE_STATE_ALLOC;
      return -1;
    }

    off_t current_off = free_list->free_p;
    off_t ret_off = 0;
    chunk_head_st *current_chunk_head = (chunk_head_st *)mmanager.getAbsAddr(current_off);
    chunk_head_st *ret_chunk_head = NULL;

    if( (size <= border_size) && (free_list->free_last_p == free_list->free_p) ){
      ret_off = current_off;
      ret_chunk_head = current_chunk_head;
      free_list->free_p = free_list->free_last_p = -1;
    }else{
      off_t ret_before_off = -1, before_off = -1;
      bool found_candidate_flag = false;

      
      while(current_chunk_head != NULL){
	if( current_chunk_head->size >= size ) found_candidate_flag = true;

	if(found_candidate_flag){
	  ret_off = current_off;
	  ret_chunk_head = current_chunk_head;
	  ret_before_off = before_off;
	  break;
	}
	before_off = current_off;
	current_off = current_chunk_head->free_next;
	current_chunk_head = (chunk_head_st *)mmanager.getAbsAddr(current_off);
      }

      if(!found_candidate_flag){
	reuse_state = REUSE_STATE_ALLOC;
	return -1;
      }

      const off_t free_next = ret_chunk_head->free_next;
      if(free_list->free_p == ret_off){
	free_list->free_p = free_next;
      }else{
	chunk_head_st *before_chunk = (chunk_head_st *)mmanager.getAbsAddr(ret_before_off);
	before_chunk->free_next = free_next;
      }

      if(free_list->free_last_p == ret_off){
	free_list->free_last_p = ret_before_off;
      }
    }

    clearChunk(ret_off);

    ret_off = ret_off + sizeof(chunk_head_st);
    return ret_off;
  }

  void MmapManager::Impl::free_data_queue(const off_t p)
  {
    free_queue_st *free_queue = &mmapCntlHead->free_queue;
    if(free_queue->data == -1){
      
      const size_t queue_size = sizeof(off_t) * free_queue->capacity;
      const off_t alloc_offset = mmanager.alloc(queue_size);
      if(alloc_offset == -1){
	
	return free_data_classify(p, true);
      }
      free_queue->data = alloc_offset;
    }else if(free_queue->tail >= free_queue->capacity){
      
      const off_t tmp_old_queue = free_queue->data;
      const size_t old_size = sizeof(off_t) * free_queue->capacity;
      const size_t new_capacity = free_queue->capacity * 2;
      const size_t new_size = sizeof(off_t) * new_capacity;

      if(new_size > mmapCntlHead->base_size){
	
	
	return free_data_classify(p, true);
      }else{
	const off_t alloc_offset = mmanager.alloc(new_size);	
	if(alloc_offset == -1){
	  
	  return free_data_classify(p, true);
	}
	free_queue->data = alloc_offset;
	const off_t *old_data = (off_t *)mmanager.getAbsAddr(tmp_old_queue);
	off_t *new_data = (off_t *)mmanager.getAbsAddr(free_queue->data);
	memcpy(new_data, old_data, old_size);
      
	free_queue->capacity = new_capacity;
	mmanager.free(tmp_old_queue);
      }
    }

    const off_t chunk_offset = p - sizeof(chunk_head_st);
    if(!insertHeap(free_queue, chunk_offset)){
      
      return;
    }

    chunk_head_st *chunk_head = (chunk_head_st*)mmanager.getAbsAddr(chunk_offset);
    chunk_head->delete_flg = 1;
    
    return;
  }
  
  off_t MmapManager::Impl::reuse_data_queue(const size_t size, reuse_state_t &reuse_state)
  {
    free_queue_st *free_queue = &mmapCntlHead->free_queue;    
    if(free_queue->data == -1){    
      
      reuse_state = REUSE_STATE_ALLOC;
      return -1;
    }

    if(getMaxHeapValue(free_queue) < size){
      reuse_state = REUSE_STATE_ALLOC;
      return -1;
    }

    off_t ret_off;
    if(!getHeap(free_queue, &ret_off)){
      
      reuse_state = REUSE_STATE_ALLOC;
      return -1;
    }

    
    reuse_state_t list_state = REUSE_STATE_OK;
    
    off_t candidate_off = reuse_data_classify(MMAP_MEMORY_ALIGN, list_state, true);
    if(list_state == REUSE_STATE_OK){
      
      mmanager.free(candidate_off);
    }
    
    const off_t c_ret_off = ret_off;
    divChunk(c_ret_off, size);

    clearChunk(ret_off);

    ret_off = ret_off + sizeof(chunk_head_st);

    return ret_off;
  }

  void MmapManager::Impl::free_data_queue_plus(const off_t p)
  {
    const off_t chunk_offset = p - sizeof(chunk_head_st);
    chunk_head_st *chunk_head = (chunk_head_st *)mmanager.getAbsAddr(chunk_offset);
    const size_t p_size = chunk_head->size;

    
    
    const size_t border_size = MMAP_MEMORY_ALIGN * MMAP_FREE_LIST_NUM;

    if(p_size <= border_size){
      free_data_classify(p);
    }else{
      free_data_queue(p);
    }
  }

  off_t MmapManager::Impl::reuse_data_queue_plus(const size_t size, reuse_state_t &reuse_state)
  {
    
    
    const size_t border_size = MMAP_MEMORY_ALIGN * MMAP_FREE_LIST_NUM;

    off_t ret_off;
    if(size <= border_size){
      ret_off = reuse_data_classify(size, reuse_state);
      if(reuse_state == REUSE_STATE_ALLOC){
	
	reuse_state = REUSE_STATE_OK;
	ret_off = reuse_data_queue(size, reuse_state);	
      }
    }else{
      ret_off = reuse_data_queue(size, reuse_state);
    }
    
    return ret_off;
  }
  

  bool MmapManager::Impl::scanAllData(void *target, const check_statistics_t stats_type) const
  {
    const uint16_t unit_num = mmapCntlHead->unit_num;
    size_t total_size = 0;
    uint64_t total_chunk_num = 0;

    for(int i = 0; i < unit_num; i++){
      const head_st *target_unit_head = &mmapCntlHead->data_headers[i];
      const uint64_t chunk_num = target_unit_head->chunk_num;
      const off_t base_offset = i * mmapCntlHead->base_size;
      off_t target_offset = base_offset;
      chunk_head_st *target_chunk;

      for(uint64_t j = 0; j < chunk_num; j++){
        target_chunk = (chunk_head_st*)mmanager.getAbsAddr(target_offset);

        if(stats_type == CHECK_STATS_USE_SIZE){
          if(target_chunk->delete_flg == false){
            total_size += target_chunk->size;
          }
        }else if(stats_type == CHECK_STATS_USE_NUM){
          if(target_chunk->delete_flg == false){
            total_chunk_num++;
          }
        }else if(stats_type == CHECK_STATS_FREE_SIZE){
          if(target_chunk->delete_flg == true){
            total_size += target_chunk->size;
          }
        }else if(stats_type == CHECK_STATS_FREE_NUM){
          if(target_chunk->delete_flg == true){
            total_chunk_num++;
          }
        }

        const size_t chunk_size = sizeof(chunk_head_st) + target_chunk->size;
        target_offset += chunk_size;
      }
    }

    if(stats_type == CHECK_STATS_USE_SIZE || stats_type == CHECK_STATS_FREE_SIZE){
      size_t *tmp_size = (size_t *)target;
      *tmp_size = total_size;
    }else if(stats_type == CHECK_STATS_USE_NUM || stats_type == CHECK_STATS_FREE_NUM){
      uint64_t *tmp_chunk_num = (uint64_t *)target;
      *tmp_chunk_num = total_chunk_num;
    }
      
    return true;
  }

  void MmapManager::Impl::upHeap(free_queue_st *free_queue, uint64_t index) const 
  {
    off_t *queue = (off_t *)mmanager.getAbsAddr(free_queue->data);

    while(index > 1){
      uint64_t parent = index / 2;
      
      const off_t parent_chunk_offset = queue[parent];
      const off_t index_chunk_offset = queue[index];
      const chunk_head_st *parent_chunk_head = (chunk_head_st *)mmanager.getAbsAddr(parent_chunk_offset);
      const chunk_head_st *index_chunk_head = (chunk_head_st *)mmanager.getAbsAddr(index_chunk_offset);    
      
      if(parent_chunk_head->size < index_chunk_head->size){
	
	const off_t tmp = queue[parent];
	queue[parent] = queue[index];
	queue[index] = tmp;
      }
      index = parent;
    }
  }

  void MmapManager::Impl::downHeap(free_queue_st *free_queue)const
  {
    off_t *queue = (off_t *)mmanager.getAbsAddr(free_queue->data);
    uint64_t index = 1;    

    while(index * 2 <= free_queue->tail){
      uint64_t child = index * 2;

      const off_t index_chunk_offset = queue[index];
      const chunk_head_st *index_chunk_head = (chunk_head_st *)mmanager.getAbsAddr(index_chunk_offset);  

      if(child + 1 < free_queue->tail){
	const off_t left_chunk_offset = queue[child];
	const off_t right_chunk_offset = queue[child+1];
	const chunk_head_st *left_chunk_head = (chunk_head_st *)mmanager.getAbsAddr(left_chunk_offset);
	const chunk_head_st *right_chunk_head = (chunk_head_st *)mmanager.getAbsAddr(right_chunk_offset);

	
	if(left_chunk_head->size < right_chunk_head->size){
	  child = child + 1;
	}
      }

      
      const off_t child_chunk_offset = queue[child];
      const chunk_head_st *child_chunk_head = (chunk_head_st *)mmanager.getAbsAddr(child_chunk_offset);

      if(child_chunk_head->size > index_chunk_head->size){
	
	const off_t tmp = queue[child];
	queue[child] = queue[index];
	queue[index] = tmp;
	index = child;
      }else{
	break;
      }
    }
  }
  
  bool MmapManager::Impl::insertHeap(free_queue_st *free_queue, const off_t p) const 
  {
    off_t *queue = (off_t *)mmanager.getAbsAddr(free_queue->data);    
    uint64_t index;
    if(free_queue->capacity < free_queue->tail){
      return false;
    }

    index = free_queue->tail;
    queue[index] = p;
    free_queue->tail += 1;

    upHeap(free_queue, index);
    
    return true;
  }

  bool MmapManager::Impl::getHeap(free_queue_st *free_queue, off_t *p) const
  {
    
    if( (free_queue->tail - 1) <= 0){     
      return false;
    }

    off_t *queue = (off_t *)mmanager.getAbsAddr(free_queue->data);        
    *p = queue[1];
    free_queue->tail -= 1;
    queue[1] = queue[free_queue->tail];
    downHeap(free_queue);

    return true;
  }

  size_t MmapManager::Impl::getMaxHeapValue(free_queue_st *free_queue) const
  {
    if(free_queue->data == -1){
      return 0;
    }
    const off_t *queue = (off_t *)mmanager.getAbsAddr(free_queue->data);
    const chunk_head_st *chunk_head = (chunk_head_st *)mmanager.getAbsAddr(queue[1]);
    
    return chunk_head->size;
  }

  void MmapManager::Impl::dumpHeap() const
  {
    free_queue_st *free_queue = &mmapCntlHead->free_queue;
    if(free_queue->data == -1){
      std::cout << "heap unused" << std::endl;
      return;
    }
    
    off_t *queue = (off_t *)mmanager.getAbsAddr(free_queue->data);        
    for(uint32_t i = 1; i < free_queue->tail; ++i){
      const off_t chunk_offset = queue[i];
      const off_t payload_offset = chunk_offset + sizeof(chunk_head_st);
      const chunk_head_st *chunk_head = (chunk_head_st *)mmanager.getAbsAddr(chunk_offset);
      const size_t size = chunk_head->size;
      std::cout << "[" << chunk_offset << "(" << payload_offset << "), " << size << "] ";
    }
    std::cout << std::endl;
  }

  void MmapManager::Impl::divChunk(const off_t chunk_offset, const size_t size)
  {
    if((mmapCntlHead->reuse_type != REUSE_DATA_QUEUE)
       && (mmapCntlHead->reuse_type != REUSE_DATA_QUEUE_PLUS)){
      return;
    }
    
    chunk_head_st *chunk_head = (chunk_head_st *)mmanager.getAbsAddr(chunk_offset);
    const size_t border_size = sizeof(chunk_head_st) + MMAP_MEMORY_ALIGN;
    const size_t align_size = getAlignSize(size);
    const size_t rest_size = chunk_head->size - align_size;

    if(rest_size < border_size){
      return;
    }

    
    chunk_head->size = align_size;

    const off_t new_chunk_offset = chunk_offset + sizeof(chunk_head_st) + align_size;
    chunk_head_st *new_chunk_head = (chunk_head_st *)mmanager.getAbsAddr(new_chunk_offset);
    const size_t new_size = rest_size - sizeof(chunk_head_st);
    setupChunkHead(new_chunk_head, true, chunk_head->unit_id, -1, new_size);
    
    
    head_st *unit_header = &mmapCntlHead->data_headers[mmapCntlHead->active_unit];    
    unit_header->chunk_num++;    

    
    const off_t payload_offset = new_chunk_offset + sizeof(chunk_head_st);
    mmanager.free(payload_offset);
    
    return;
  }
}
