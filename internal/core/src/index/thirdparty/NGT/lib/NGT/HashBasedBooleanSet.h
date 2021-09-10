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
#include <iostream>
#include <cstring>
#include <stdint.h>
#include <climits>
#include <unordered_set>

class HashBasedBooleanSet{
 private:
  uint32_t *_table;
  uint32_t _tableSize;
  uint32_t _mask;
  
  std::unordered_set<uint32_t> _stlHash;
  
  
  inline uint32_t _hash1(const uint32_t value){
    return value & _mask;
  }
  
 public:
 HashBasedBooleanSet():_table(NULL), _tableSize(0), _mask(0) {}

 HashBasedBooleanSet(const uint64_t size):_table(NULL), _tableSize(0), _mask(0) {
   size_t bitSize = 0;
   size_t bit = size;
   while (bit != 0) {
     bitSize++;
     bit >>= 1;
   }
   size_t bucketSize = 0x1 << ((bitSize + 4) / 2 + 3);
   initialize(bucketSize);
 }
 void initialize(const uint32_t tableSize) {
    _tableSize = tableSize;
    _mask = _tableSize - 1;
    const uint32_t checkValue = _hash1(tableSize);
    if(checkValue != 0){
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("[WARN] table size is not 2^N :  " +  std::to_string(tableSize));
//      std::cerr << "[WARN] table size is not 2^N :  " <<  tableSize << std::endl;
    }
    
    _table = new uint32_t[tableSize];
    memset(_table, 0, tableSize * sizeof(uint32_t));
  }
  
  ~HashBasedBooleanSet(){
    delete[] _table;
    _stlHash.clear();
  }
  
  inline bool operator[](const uint32_t num){
    const uint32_t hashValue = _hash1(num);
    
    auto v = _table[hashValue];
    if (v == num){
      return true;
    }
    if (v == 0){
      return false;
    }
    if (_stlHash.count(num) <= 0) {    
      return false;
    }
    return true;
  }
  
  inline void set(const uint32_t num){
    uint32_t &value = _table[_hash1(num)];
    if(value == 0){
      value = num;
    }else{
      if(value != num){
	_stlHash.insert(num);
      }
    }
  }
  
  inline void insert(const uint32_t num){
    set(num);
  }

  inline void reset(const uint32_t num){
    const uint32_t hashValue = _hash1(num);
    if(_table[hashValue] != 0){
      if(_table[hashValue] != num){
	_stlHash.erase(num);
      }else{
	_table[hashValue] = UINT_MAX;
      }
    }
  }
};

