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

#include <fstream>
#include <string>
#include <cstddef>
#include <stdint.h>
#include <iostream>
#include <stdexcept>
#include <cerrno>
#include <cstring>

namespace NGT {
  class ObjectSpace;
};

template <class TYPE>
class ArrayFile {
 private:
  struct FileHeadStruct {
    size_t recordSize;
    uint64_t extraData; // reserve
  };

  struct RecordStruct {
    bool deleteFlag;
    uint64_t extraData; // reserve    
  };

  bool _isOpen;  
  std::fstream _stream;
  FileHeadStruct _fileHead;

  bool _readFileHead();
  pthread_mutex_t _mutex;
  
 public:
  ArrayFile();
  ~ArrayFile();
  bool create(const std::string &file, size_t recordSize);
  bool open(const std::string &file);
  void close();
  size_t insert(TYPE &data, NGT::ObjectSpace *objectSpace = 0);
  void put(const size_t id, TYPE &data, NGT::ObjectSpace *objectSpace = 0);
  bool get(const size_t id, TYPE &data, NGT::ObjectSpace *objectSpace = 0);
  void remove(const size_t id);
  bool isOpen() const;
  size_t size();
  size_t getRecordSize() { return _fileHead.recordSize; }
};


// constructor 
template <class TYPE>
ArrayFile<TYPE>::ArrayFile()
  : _isOpen(false), _mutex((pthread_mutex_t)PTHREAD_MUTEX_INITIALIZER){
    if(pthread_mutex_init(&_mutex, NULL) < 0) throw std::runtime_error("pthread init error.");
}

// destructor
template <class TYPE>
ArrayFile<TYPE>::~ArrayFile() {
  pthread_mutex_destroy(&_mutex);
  close();
}

template <class TYPE>
bool ArrayFile<TYPE>::create(const std::string &file, size_t recordSize) {
  std::fstream tmpstream;
  tmpstream.open(file.c_str());  
  if(tmpstream){
    return false;
  }
  
  tmpstream.open(file.c_str(), std::ios::out);
  tmpstream.seekp(0, std::ios::beg);
  FileHeadStruct fileHead = {recordSize, 0};
  tmpstream.write((char *)(&fileHead), sizeof(FileHeadStruct));
  tmpstream.close();
  
  return true;
}

template <class TYPE>
bool ArrayFile<TYPE>::open(const std::string &file) {
  _stream.open(file.c_str(), std::ios::in | std::ios::out);  
  if(!_stream){
    _isOpen = false;    
    return false;
  }
  _isOpen = true;

  bool ret = _readFileHead();
  return ret;
}

template <class TYPE>
void ArrayFile<TYPE>::close(){
  _stream.close();
  _isOpen = false;  
}

template <class TYPE>
size_t ArrayFile<TYPE>::insert(TYPE &data, NGT::ObjectSpace *objectSpace) {
  _stream.seekp(sizeof(RecordStruct), std::ios::end);
  int64_t write_pos = _stream.tellg();
  for(size_t i = 0; i < _fileHead.recordSize; i++) { _stream.write("", 1); }
  _stream.seekp(write_pos, std::ios::beg);
  data.serialize(_stream, objectSpace);
  
  int64_t offset_pos = _stream.tellg();
  offset_pos -= sizeof(FileHeadStruct);
  size_t id = offset_pos / (sizeof(RecordStruct) + _fileHead.recordSize);
  if(offset_pos % (sizeof(RecordStruct) + _fileHead.recordSize) == 0){
    id -= 1;
  }
  
  return id;
}

template <class TYPE>
void ArrayFile<TYPE>::put(const size_t id, TYPE &data, NGT::ObjectSpace *objectSpace) {
  uint64_t offset_pos = (id * (sizeof(RecordStruct) + _fileHead.recordSize)) + sizeof(FileHeadStruct);
  offset_pos += sizeof(RecordStruct);
  _stream.seekp(offset_pos, std::ios::beg);
  
  for(size_t i = 0; i < _fileHead.recordSize; i++) { _stream.write("", 1); }
  _stream.seekp(offset_pos, std::ios::beg); 
  data.serialize(_stream, objectSpace);
}

template <class TYPE>
bool ArrayFile<TYPE>::get(const size_t id, TYPE &data, NGT::ObjectSpace *objectSpace) {
  pthread_mutex_lock(&_mutex);

  if( size() <= id ){
    pthread_mutex_unlock(&_mutex);    
    return false;
  }
  
  uint64_t offset_pos = (id * (sizeof(RecordStruct) + _fileHead.recordSize)) + sizeof(FileHeadStruct);
  offset_pos += sizeof(RecordStruct);  
  _stream.seekg(offset_pos, std::ios::beg);
  if (!_stream.fail()) {
    data.deserialize(_stream, objectSpace);
  }
  if (_stream.fail()) {
    const int trialCount = 10;
    for (int tc = 0; tc < trialCount; tc++) {
      _stream.clear();
      _stream.seekg(offset_pos, std::ios::beg);
      if (_stream.fail()) {
	continue;
      }
      data.deserialize(_stream, objectSpace);
      if (_stream.fail()) {
	continue;
      } else {
	break;
      }
    }
    if (_stream.fail()) {
      throw std::runtime_error("ArrayFile::get: Error!");
    }
  }

  pthread_mutex_unlock(&_mutex);
  return true;
}

template <class TYPE>
void ArrayFile<TYPE>::remove(const size_t id) {
  uint64_t offset_pos = (id * (sizeof(RecordStruct) + _fileHead.recordSize)) + sizeof(FileHeadStruct);  
  _stream.seekp(offset_pos, std::ios::beg);
  RecordStruct recordHead = {1, 0};
  _stream.write((char *)(&recordHead), sizeof(RecordStruct));
}

template <class TYPE>
bool ArrayFile<TYPE>::isOpen() const
{
  return _isOpen;
}

template <class TYPE>
size_t ArrayFile<TYPE>::size()
{
  _stream.seekp(0, std::ios::end);
  int64_t offset_pos = _stream.tellg();
  offset_pos -= sizeof(FileHeadStruct);
  size_t num = offset_pos / (sizeof(RecordStruct) + _fileHead.recordSize);

  return num; 
}

template <class TYPE>
bool ArrayFile<TYPE>::_readFileHead() {
  _stream.seekp(0, std::ios::beg);
  _stream.read((char *)(&_fileHead), sizeof(FileHeadStruct));
  if(_stream.bad()){
    return false;
  }
  return true;
}

