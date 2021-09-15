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

#include "MmapManagerImpl.hpp"

namespace MemoryManager{
  // static method --- 
  void MmapManager::setDefaultOptionValue(init_option_st &optionst)
  {
    optionst.use_expand = MMAP_DEFAULT_ALLOW_EXPAND;
    optionst.reuse_type = REUSE_DATA_CLASSIFY;
  }
  
  size_t MmapManager::getAlignSize(size_t size){
    if((size % MMAP_MEMORY_ALIGN) == 0){
      return size;
    }else{
      return ( (size >> MMAP_MEMORY_ALIGN_EXP ) + 1 ) * MMAP_MEMORY_ALIGN;	
    }
  }
  // static method --- 
  

  MmapManager::MmapManager():_impl(new MmapManager::Impl(*this))
  {
    for(uint64_t i = 0; i < MMAP_MAX_UNIT_NUM; ++i){
      _impl->mmapDataAddr[i] = NULL;
    }
  }

  MmapManager::~MmapManager() = default;

  void MmapManager::dumpHeap() const
  {
    _impl->dumpHeap();
  }
  
  bool MmapManager::isOpen() const
  {
    return _impl->isOpen;
  }

  void *MmapManager::getEntryHook() const {
    return getAbsAddr(_impl->mmapCntlHead->entry_p);
  }
    
  void MmapManager::setEntryHook(const void *entry_p){
    _impl->mmapCntlHead->entry_p = getRelAddr(entry_p);
  }
  
  
  bool MmapManager::init(const std::string &filePath, size_t size, const init_option_st *optionst) const
  {
    try{
      const std::string controlFile = filePath + MMAP_CNTL_FILE_SUFFIX;
      
      struct stat st;
      if(stat(controlFile.c_str(), &st) == 0){
        return false;
      }
      if(filePath.length() > MMAP_MAX_FILE_NAME_LENGTH){
        std::cerr << "too long filepath" << std::endl;
        return false;
      }
      if((size % MMAP_CNTL_PAGE_SIZE != 0) || ( size < MMAP_LOWER_SIZE )){
        std::cerr << "input size error" << std::endl;
        return false;
      }
       
      int32_t fd = _impl->formatFile(controlFile, MMAP_CNTL_FILE_SIZE);
      assert(fd >= 0);
      
      errno = 0;
      char *cntl_p = (char *)mmap(NULL, MMAP_CNTL_FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      if(cntl_p == MAP_FAILED){
        const std::string err_str = getErrorStr(errno);
        if(close(fd) == -1) std::cerr << controlFile << "[WARN] : filedescript cannot close" << std::endl;
        throw MmapManagerException(controlFile + " " + err_str);
      }
      if(close(fd) == -1) std::cerr << controlFile << "[WARN] : filedescript cannot close" << std::endl;
      
      try {
	fd = _impl->formatFile(filePath, size);
      } catch (MmapManagerException &err) {
        if(munmap(cntl_p, MMAP_CNTL_FILE_SIZE) == -1) {
	  throw MmapManagerException("[ERR] : munmap error : " + getErrorStr(errno) +
				     " : Through the exception : " + err.what());
	}
        throw err;
      }
      if(close(fd) == -1) std::cerr << controlFile << "[WARN] : filedescript cannot close" << std::endl;
      
      boot_st bootStruct = {0};
      control_st controlStruct = {0};
      _impl->initBootStruct(bootStruct, size);  
      _impl->initControlStruct(controlStruct, size); 
      
      char *cntl_head = cntl_p;
      cntl_head += sizeof(boot_st);

      if(optionst != NULL){
        controlStruct.use_expand = optionst->use_expand;
        controlStruct.reuse_type = optionst->reuse_type;
      }
      
      memcpy(cntl_p, (char *)&bootStruct, sizeof(boot_st));
      memcpy(cntl_head, (char *)&controlStruct, sizeof(control_st));
      
      errno = 0;
      if(munmap(cntl_p, MMAP_CNTL_FILE_SIZE) == -1) throw MmapManagerException("munmap error : " + getErrorStr(errno));
      
      return true;
    }catch(MmapManagerException &e){
      std::cerr << "init error. " << e.what() << std::endl;
      throw e;
    }
  }

  bool MmapManager::openMemory(const std::string &filePath)
  {
    try{
      if(_impl->isOpen == true){
        std::string err_str = "[ERROR] : openMemory error (double open).";
        throw MmapManagerException(err_str);
      }
      
      const std::string controlFile = filePath + MMAP_CNTL_FILE_SUFFIX;
      _impl->filePath = filePath; 
      
      int32_t fd;
      
      errno = 0;
      if((fd = open(controlFile.c_str(), O_RDWR, 0666)) == -1){
        const std::string err_str = getErrorStr(errno);
        throw MmapManagerException("file open error" + err_str);
      }
      
      errno = 0;
      boot_st *boot_p = (boot_st*)mmap(NULL, MMAP_CNTL_FILE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
      if(boot_p == MAP_FAILED){
        const std::string err_str = getErrorStr(errno);
        if(close(fd) == -1) std::cerr << controlFile << "[WARN] : filedescript cannot close" << std::endl;
        throw MmapManagerException(controlFile + " " + err_str);
      }
      if(close(fd) == -1) std::cerr << controlFile << "[WARN] : filedescript cannot close" << std::endl;
      
      if(boot_p->version != MMAP_MANAGER_VERSION){
        std::cerr << "[WARN] : version error" << std::endl;
        errno = 0;
        if(munmap(boot_p, MMAP_CNTL_FILE_SIZE) == -1) throw MmapManagerException("munmap error : " + getErrorStr(errno));
        throw MmapManagerException("MemoryManager version error");	
      }
      
      errno = 0;
      if((fd = open(filePath.c_str(), O_RDWR, 0666)) == -1){
        const std::string err_str = getErrorStr(errno);
        errno = 0;
        if(munmap(boot_p, MMAP_CNTL_FILE_SIZE) == -1) throw MmapManagerException("munmap error : " + getErrorStr(errno));
        throw MmapManagerException("file open error = " + std::string(filePath.c_str())  + err_str);
      }
      
      _impl->mmapCntlHead = (control_st*)( (char *)boot_p + sizeof(boot_st)); 
      _impl->mmapCntlAddr = (void *)boot_p;

      for(uint64_t i = 0; i < _impl->mmapCntlHead->unit_num; i++){
        off_t offset = _impl->mmapCntlHead->base_size * i;
        errno = 0;
        _impl->mmapDataAddr[i] = mmap(NULL, _impl->mmapCntlHead->base_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, offset);
        if(_impl->mmapDataAddr[i] == MAP_FAILED){
	  if (errno == EINVAL) {
	    std::cerr << "MmapManager::openMemory: Fatal error. EINVAL" << std::endl
		      << "  If you use valgrind, this error might occur when the DB is created." << std::endl
		      << "  In the case of that, reduce bsize in SharedMemoryAllocator." << std::endl;
	    assert(errno != EINVAL);
	  }
          const std::string err_str = getErrorStr(errno);
          if(close(fd) == -1) std::cerr << controlFile << "[WARN] : filedescript cannot close" << std::endl;
          closeMemory(true); 
          throw MmapManagerException(err_str);
        }
      }
      if(close(fd) == -1) std::cerr << controlFile << "[WARN] : filedescript cannot close" << std::endl;
      
      _impl->isOpen = true;
      return true;
    }catch(MmapManagerException &e){
      std::cerr << "open error" << std::endl;
      throw e;
    }
  }

  void MmapManager::closeMemory(const bool force)
  {
    try{
      if(force || _impl->isOpen){
        uint16_t count = 0;
        void *error_ids[MMAP_MAX_UNIT_NUM] = {0};
        for(uint16_t i = 0; i < _impl->mmapCntlHead->unit_num; i++){
          if(_impl->mmapDataAddr[i] != NULL){
            if(munmap(_impl->mmapDataAddr[i], _impl->mmapCntlHead->base_size) == -1){
              error_ids[i] = _impl->mmapDataAddr[i];;
              count++;
            }
            _impl->mmapDataAddr[i] = NULL;
          }
        }
        
        if(count > 0){
          std::string msg = "";
          
          for(uint16_t i = 0; i < count; i++){
            std::stringstream ss;
            ss <<  error_ids[i];
            msg += ss.str() + ", ";
          }
          throw MmapManagerException("unmap error : ids = " + msg);
        }
        
        if(_impl->mmapCntlAddr != NULL){
          if(munmap(_impl->mmapCntlAddr, MMAP_CNTL_FILE_SIZE) == -1) throw MmapManagerException("munmap error : " + getErrorStr(errno));
          _impl->mmapCntlAddr = NULL;
        }
        _impl->isOpen = false;
      }
    }catch(MmapManagerException &e){
      std::cerr << "close error" << std::endl;
      throw e;
    }
  }

  off_t MmapManager::alloc(const size_t size, const bool not_reuse_flag)
  {
    try{
      if(!_impl->isOpen){
        std::cerr << "not open this file" << std::endl;
        return -1;
      }

      size_t alloc_size = getAlignSize(size);

      if( (alloc_size + sizeof(chunk_head_st)) >= _impl->mmapCntlHead->base_size ){
        std::cerr << "alloc size over. size=" << size << "." << std::endl;
        return -1;
      }

      if(!not_reuse_flag){
        if(  _impl->mmapCntlHead->reuse_type == REUSE_DATA_CLASSIFY 
	    || _impl->mmapCntlHead->reuse_type == REUSE_DATA_QUEUE
	    || _impl->mmapCntlHead->reuse_type == REUSE_DATA_QUEUE_PLUS){	  
          off_t ret_offset;
          reuse_state_t reuse_state = REUSE_STATE_OK;
          ret_offset = reuse(alloc_size, reuse_state);
          if(reuse_state != REUSE_STATE_ALLOC){
            return ret_offset;
          }
        }
      }
      
      head_st *unit_header = &_impl->mmapCntlHead->data_headers[_impl->mmapCntlHead->active_unit];
      if((unit_header->break_p + sizeof(chunk_head_st) + alloc_size) >= _impl->mmapCntlHead->base_size){
        if(_impl->mmapCntlHead->use_expand == true){
          if(_impl->expandMemory() == false){
            std::cerr << __func__ << ": cannot expand" << std::endl;
            return -1;
          }
          unit_header = &_impl->mmapCntlHead->data_headers[_impl->mmapCntlHead->active_unit];
        }else{
          std::cerr << __func__ << ": total size over" << std::endl;
          return -1;
        }
      }
      
      const off_t file_offset = _impl->mmapCntlHead->active_unit * _impl->mmapCntlHead->base_size;
      const off_t ret_p = file_offset + ( unit_header->break_p + sizeof(chunk_head_st) );

      chunk_head_st *chunk_head = (chunk_head_st*)(unit_header->break_p + (char *)_impl->mmapDataAddr[_impl->mmapCntlHead->active_unit]);
      _impl->setupChunkHead(chunk_head, false, _impl->mmapCntlHead->active_unit, -1, alloc_size);
      unit_header->break_p += alloc_size + sizeof(chunk_head_st);
      unit_header->chunk_num++;
      
      return ret_p;
    }catch(MmapManagerException &e){
      std::cerr << "allocation error" << std::endl;
      throw e;
    }
  }

  void MmapManager::free(const off_t p)
  {
    switch(_impl->mmapCntlHead->reuse_type){
    case REUSE_DATA_CLASSIFY:
      _impl->free_data_classify(p);
      break;
    case REUSE_DATA_QUEUE:
      _impl->free_data_queue(p);
      break;
    case REUSE_DATA_QUEUE_PLUS:
      _impl->free_data_queue_plus(p);
      break;
    default:
      _impl->free_data_classify(p);
      break;
    }
  }

  off_t MmapManager::reuse(const size_t size, reuse_state_t &reuse_state)
  {
    off_t ret_off;

    switch(_impl->mmapCntlHead->reuse_type){
    case REUSE_DATA_CLASSIFY:
      ret_off = _impl->reuse_data_classify(size, reuse_state);
      break;
    case REUSE_DATA_QUEUE:
      ret_off = _impl->reuse_data_queue(size, reuse_state);
      break;
    case REUSE_DATA_QUEUE_PLUS:
      ret_off = _impl->reuse_data_queue_plus(size, reuse_state);
      break;
    default:
      ret_off = _impl->reuse_data_classify(size, reuse_state);
      break;
    }

    return ret_off;
  }

  void *MmapManager::getAbsAddr(off_t p) const 
  {
    if(p < 0){
      return NULL;
    }
    const uint16_t unit_id = p / _impl->mmapCntlHead->base_size;
    const off_t file_offset = unit_id * _impl->mmapCntlHead->base_size;
    const off_t ret_p = p - file_offset;

    return ABS_ADDR(ret_p, _impl->mmapDataAddr[unit_id]);
  }
  
  off_t MmapManager::getRelAddr(const void *p) const 
  {
    const chunk_head_st *chunk_head = (chunk_head_st *)((char *)p - sizeof(chunk_head_st));
    const uint16_t unit_id = chunk_head->unit_id;

    const off_t file_offset = unit_id * _impl->mmapCntlHead->base_size;
    off_t ret_p = (off_t)((char *)p - (char *)_impl->mmapDataAddr[unit_id]);
    ret_p += file_offset;

    return ret_p;
  }

  std::string getErrorStr(int32_t err_num){
    char err_msg[256];
#ifdef WIN32
    #define strerror_r(errno,buf,len) strerror_s(buf,len,errno)
#endif
#ifdef _GNU_SOURCE
    char *msg = strerror_r(err_num, err_msg, 256);
    return std::string(msg);
#else
    strerror_r(err_num, err_msg, 256);
    return std::string(err_msg);
#endif
  }

  size_t MmapManager::getTotalSize() const
  {
    const uint16_t active_unit = _impl->mmapCntlHead->active_unit;
    const size_t ret_size = ((_impl->mmapCntlHead->unit_num - 1) * _impl->mmapCntlHead->base_size) + _impl->mmapCntlHead->data_headers[active_unit].break_p;

    return ret_size;
  }

  size_t MmapManager::getUseSize() const
  {
    size_t total_size = 0;
    void *ref_addr = (void *)&total_size;
    _impl->scanAllData(ref_addr, CHECK_STATS_USE_SIZE);

    return total_size;
  }

  uint64_t MmapManager::getUseNum() const
  {
    uint64_t total_chunk_num = 0;
    void *ref_addr = (void *)&total_chunk_num;
    _impl->scanAllData(ref_addr, CHECK_STATS_USE_NUM);

    return total_chunk_num;
  }
  
  size_t MmapManager::getFreeSize() const
  {
    size_t total_size = 0;
    void *ref_addr = (void *)&total_size;
    _impl->scanAllData(ref_addr, CHECK_STATS_FREE_SIZE);

    return total_size;
  }

  uint64_t MmapManager::getFreeNum() const
  {
    uint64_t total_chunk_num = 0;
    void *ref_addr = (void *)&total_chunk_num;
    _impl->scanAllData(ref_addr, CHECK_STATS_FREE_NUM);

    return total_chunk_num;
  }

  uint16_t MmapManager::getUnitNum() const
  {
    return _impl->mmapCntlHead->unit_num;
  }

  size_t MmapManager::getQueueCapacity() const
  {
    free_queue_st *free_queue = &_impl->mmapCntlHead->free_queue;
    return free_queue->capacity;
  }

  uint64_t MmapManager::getQueueNum() const
  {
    free_queue_st *free_queue = &_impl->mmapCntlHead->free_queue;
    return free_queue->tail;
  }

  uint64_t MmapManager::getLargeListNum() const
  {
    uint64_t count = 0;
    free_list_st *free_list = &_impl->mmapCntlHead->free_data.large_list;

    if(free_list->free_p == -1){
      return count;
    }

    off_t current_off = free_list->free_p;
    chunk_head_st *current_chunk_head = (chunk_head_st *)getAbsAddr(current_off);

    while(current_chunk_head != NULL){
      count++;
      current_off = current_chunk_head->free_next;
      current_chunk_head = (chunk_head_st *)getAbsAddr(current_off);
    }

    return count;
  }
}
