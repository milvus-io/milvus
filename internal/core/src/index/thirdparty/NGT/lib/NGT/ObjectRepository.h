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
#include <sstream>
#include "defines.h"

namespace NGT {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  class ObjectRepository : 
  public PersistentRepository<PersistentObject> {
  public:
    typedef PersistentRepository<PersistentObject>	Parent;
    void open(const std::string &smfile, size_t sharedMemorySize) { 
      std::string file = smfile;
      file.append("po");
      Parent::open(file, sharedMemorySize);
    }
#else
  class ObjectRepository : public Repository<Object> {
  public:
    typedef Repository<Object>	Parent;
#endif
    ObjectRepository(size_t dim, const std::type_info &ot):dimension(dim), type(ot), sparse(false) { }

    void initialize() {
      deleteAll();
      Parent::push_back((PersistentObject*)0);
    }

    // for milvus
    void serialize(std::stringstream & obj, ObjectSpace * ospace) { Parent::serialize(obj, ospace); }

    void serialize(const std::string &ofile, ObjectSpace *ospace) { 
      std::ofstream objs(ofile);
      if (!objs.is_open()) {
	std::stringstream msg;
	msg << "NGT::ObjectSpace: Cannot open the specified file " << ofile << ".";
	NGTThrowException(msg);
      }
      Parent::serialize(objs, ospace); 
    }

    void deserialize(std::stringstream & obj, ObjectSpace * ospace)
    {
        assert(ospace != 0);
        Parent::deserialize(obj, ospace);
    }

    void deserialize(const std::string &ifile, ObjectSpace *ospace) { 
      assert(ospace != 0);
      std::ifstream objs(ifile);
      if (!objs.is_open()) {
	std::stringstream msg;
	msg << "NGT::ObjectSpace: Cannot open the specified file " << ifile << ".";
	NGTThrowException(msg);
      }
      Parent::deserialize(objs, ospace);
    }

    void serializeAsText(const std::string &ofile, ObjectSpace *ospace) { 
      std::ofstream objs(ofile);
      if (!objs.is_open()) {
	std::stringstream msg;
	msg << "NGT::ObjectSpace: Cannot open the specified file " << ofile << ".";
	NGTThrowException(msg);
      }
      Parent::serializeAsText(objs, ospace); 
    }

    void deserializeAsText(const std::string &ifile, ObjectSpace *ospace) { 
      std::ifstream objs(ifile);
      if (!objs.is_open()) {
	std::stringstream msg;
	msg << "NGT::ObjectSpace: Cannot open the specified file " << ifile << ".";
	NGTThrowException(msg);
      }
      Parent::deserializeAsText(objs, ospace); 
    }

    void readText(std::istream &is, size_t dataSize = 0) {
      initialize();
      appendText(is, dataSize);
    }

    // For milvus
    template <typename T>
    void readRawData(const T * raw_data, size_t dataSize)
    {
        initialize();
        append(raw_data, dataSize);
    }

    virtual PersistentObject *allocateNormalizedPersistentObject(const std::vector<double> &obj) {
//      std::cerr << "ObjectRepository::allocateNormalizedPersistentObject(double): Fatal error! Something wrong!" << std::endl;
      if (NGT_LOG_DEBUG_)
          (*NGT_LOG_DEBUG_)("ObjectRepository::allocateNormalizedPersistentObject(double): Fatal error! Something wrong!");
      abort();
    }

    virtual PersistentObject *allocateNormalizedPersistentObject(const std::vector<float> &obj) {
//      std::cerr << "ObjectRepository::allocateNormalizedPersistentObject(float): Fatal error! Something wrong!" << std::endl;
      if (NGT_LOG_DEBUG_)
          (*NGT_LOG_DEBUG_)("ObjectRepository::allocateNormalizedPersistentObject(float): Fatal error! Something wrong!");
      abort();
    }

    virtual PersistentObject *allocateNormalizedPersistentObject(const std::vector<uint8_t> &obj) {
//      std::cerr << "ObjectRepository::allocateNormalizedPersistentObject(uint8_t): Fatal error! Something wrong!" << std::endl;
      if (NGT_LOG_DEBUG_)
          (*NGT_LOG_DEBUG_)("ObjectRepository::allocateNormalizedPersistentObject(uint8_t): Fatal error! Something wrong!");
      abort();
    }

    virtual PersistentObject *allocateNormalizedPersistentObject(const float *obj, size_t size) {
//      std::cerr << "ObjectRepository::allocateNormalizedPersistentObject: Fatal error! Something wrong!" << std::endl;
      if (NGT_LOG_DEBUG_)
          (*NGT_LOG_DEBUG_)("ObjectRepository::allocateNormalizedPersistentObject: Fatal error! Something wrong!");
      abort();
    }

    void appendText(std::istream &is, size_t dataSize = 0) {
      if (dimension == 0) {
	NGTThrowException("ObjectSpace::readText: Dimension is not specified.");
      }
      if (size() == 0) {
	// First entry should be always a dummy entry.
	// If it is empty, the dummy entry should be inserted.
	push_back((PersistentObject*)0);
      }
      size_t prevDataSize = size();
      if (dataSize > 0) {
	reserve(size() + dataSize);
      }
      std::string line;
      size_t lineNo = 0;
      while (getline(is, line)) {
	lineNo++;
	if (dataSize > 0 && (dataSize <= size() - prevDataSize)) {
//	  std::cerr << "The size of data reached the specified size. The remaining data in the file are not inserted. "
//	       << dataSize << std::endl;
	  if (NGT_LOG_DEBUG_)
          (*NGT_LOG_DEBUG_)("The size of data reached the specified size. The remaining data in the file are not inserted. "
	       + std::to_string(dataSize));
	  break;
	}
	std::vector<double> object;
	try {
	  extractObjectFromText(line, "\t ", object);
	  PersistentObject *obj = 0;
	  try {
	    obj = allocateNormalizedPersistentObject(object);
	  } catch (Exception &err) {
//	    std::cerr << err.what() << " continue..." << std::endl;
	    if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)(std::string(err.what()) + " continue...");
	    obj = allocatePersistentObject(object);
	  }
	  push_back(obj);
	} catch (Exception &err) {
//	  std::cerr << "ObjectSpace::readText: Warning! Invalid line. [" << line << "] Skip the line " << lineNo << " and continue." << std::endl;
	  if (NGT_LOG_DEBUG_)
          (*NGT_LOG_DEBUG_)("ObjectSpace::readText: Warning! Invalid line. [" + line + "] Skip the line " + std::to_string(lineNo) + " and continue.");
	}
      }
    }

    template <typename T>
    void append(T *data, size_t objectCount) {
      if (dimension == 0) {
	NGTThrowException("ObjectSpace::readText: Dimension is not specified.");
      }
      if (size() == 0) {
	// First entry should be always a dummy entry.
	// If it is empty, the dummy entry should be inserted.
	push_back((PersistentObject*)0);
      }
      if (objectCount > 0) {
	reserve(size() + objectCount);
      }
      for (size_t idx = 0; idx < objectCount; idx++, data += dimension) {
	std::vector<double> object;
	object.reserve(dimension);
	for (size_t dataidx = 0; dataidx < dimension; dataidx++) {
	  object.push_back(data[dataidx]);
	}
	try {
	  PersistentObject *obj = 0;
	  try {
	    obj = allocateNormalizedPersistentObject(object);
	  } catch (Exception &err) {
//	    std::cerr << err.what() << " continue..." << std::endl;
	    if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)(std::string(err.what()) + " continue...");
	    obj = allocatePersistentObject(object);
	  }
	  push_back(obj);

	} catch (Exception &err) {
//	  std::cerr << "ObjectSpace::readText: Warning! Invalid data. Skip the data no. " << idx << " and continue." << std::endl;
	  if (NGT_LOG_DEBUG_)
          (*NGT_LOG_DEBUG_)("ObjectSpace::readText: Warning! Invalid data. Skip the data no. " + std::to_string(idx) + " and continue.");
	}
      }
    }

    Object *allocateObject() {
      return (Object*) new Object(paddedByteSize);
    }

    // This method is called during search to generate query.
    // Therefore the object is not persistent.
    Object *allocateObject(const std::string &textLine, const std::string &sep) {
      std::vector<double> object;
      extractObjectFromText(textLine, sep, object);
      Object *po = (Object*)allocateObject(object);
      return (Object*)po;
    }

    void extractObjectFromText(const std::string &textLine, const std::string &sep, std::vector<double> &object) {
      object.resize(dimension);
      std::vector<std::string> tokens;
      NGT::Common::tokenize(textLine, tokens, sep);
      if (dimension > tokens.size()) {
	std::stringstream msg;
	msg << "ObjectSpace::allocate: too few dimension. " << tokens.size() << ":" << dimension << ". " 
	    << textLine;
	NGTThrowException(msg);
      }
      size_t idx;
      for (idx = 0; idx < dimension; idx++) {
	if (tokens[idx].size() == 0) {
	  std::stringstream msg;
	  msg << "ObjectSpace::allocate: too few dimension. " << tokens.size() << ":" 
	      << dimension << ". "  << textLine;
	  NGTThrowException(msg);
        }
	char *e;
	object[idx] = strtod(tokens[idx].c_str(), &e);
	if (*e != 0) {
//	  std::cerr << "ObjectSpace::readText: Warning! Not numerical value. [" << e << "]" << std::endl;
	  if (NGT_LOG_DEBUG_)
          (*NGT_LOG_DEBUG_)("ObjectSpace::readText: Warning! Not numerical value. [" + std::string(e) + "]");
	  break;
	}
      }
    }

    template <typename T>
      Object *allocateObject(T *o, size_t size) {
      size_t osize = paddedByteSize;
      if (sparse) {
	size_t vsize = size * (type == typeid(float) ? 4 : 1);
	osize = osize < vsize ? vsize : osize;
      } else {
	if (dimension != size) {
//	  std::cerr << "ObjectSpace::allocateObject: Fatal error! dimension is invalid. The indexed objects="
//		    << dimension << " The specified object=" << size << std::endl;
	  if (NGT_LOG_DEBUG_)
          (*NGT_LOG_DEBUG_)("ObjectSpace::allocateObject: Fatal error! dimension is invalid. The indexed objects="
		    + std::to_string(dimension) + " The specified object=" + std::to_string(size));
	  assert(dimension == size);
	}
      }
      Object *po = new Object(osize);
      void *object = static_cast<void*>(&(*po)[0]);
      if (type == typeid(uint8_t)) {
	uint8_t *obj = static_cast<uint8_t*>(object);
	for (size_t i = 0; i < size; i++) {
	  obj[i] = static_cast<uint8_t>(o[i]);
	}
      } else if (type == typeid(float)) {
	float *obj = static_cast<float*>(object);
	for (size_t i = 0; i < size; i++) {
	  obj[i] = static_cast<float>(o[i]);
	}
      } else {
//	std::cerr << "ObjectSpace::allocate: Fatal error: unsupported type!" << std::endl;
	    if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("ObjectSpace::allocate: Fatal error: unsupported type!");
	abort();
      }
      return po;
    }

    template <typename T>
      Object *allocateObject(const std::vector<T> &o) {
      return allocateObject(o.data(), o.size());
    }

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    PersistentObject *allocatePersistentObject(Object &o) {
      SharedMemoryAllocator &objectAllocator = getAllocator();
      size_t cpsize = dimension;
      if (type == typeid(uint8_t)) {
	cpsize *= sizeof(uint8_t);
      } else if (type == typeid(float)) {
	cpsize *= sizeof(float);
      } else {
	    if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("ObjectSpace::allocate: Fatal error: unsupported type!");
//	std::cerr << "ObjectSpace::allocate: Fatal error: unsupported type!" << std::endl;
	abort();
      }
      PersistentObject *po = new (objectAllocator) PersistentObject(objectAllocator, paddedByteSize);
      void *dsto = &(*po).at(0, allocator);
      void *srco = &o[0];
      memcpy(dsto, srco, cpsize);
      return po;
    }

    template <typename T>
      PersistentObject *allocatePersistentObject(T *o, size_t size) {
      SharedMemoryAllocator &objectAllocator = getAllocator();
      PersistentObject *po = new (objectAllocator) PersistentObject(objectAllocator, paddedByteSize);
      if (size != 0 && dimension != size) {
	std::stringstream msg;
	msg << "ObjectSpace::allocatePersistentObject: Fatal error! The dimensionality is invalid. The specified dimensionality=" 
	    << (sparse ? dimension - 1 : dimension) << ". The specified object=" << (sparse ? size - 1 : size) << ".";
	NGTThrowException(msg);
      }
      void *object = static_cast<void*>(&(*po).at(0, allocator));
      if (type == typeid(uint8_t)) {
	uint8_t *obj = static_cast<uint8_t*>(object);
	for (size_t i = 0; i < dimension; i++) {
	  obj[i] = static_cast<uint8_t>(o[i]);
	}
      } else if (type == typeid(float)) {
	float *obj = static_cast<float*>(object);
	for (size_t i = 0; i < dimension; i++) {
	  obj[i] = static_cast<float>(o[i]);
	}
      } else {
	    if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("ObjectSpace::allocate: Fatal error: unsupported type!");
//	std::cerr << "ObjectSpace::allocate: Fatal error: unsupported type!" << std::endl;
	abort();
      }
      return po;
    }

    template <typename T>
    PersistentObject *allocatePersistentObject(const std::vector<T> &o) {
      return allocatePersistentObject(o.data(), o.size());
    }

#else
    template <typename T>
      PersistentObject *allocatePersistentObject(T *o, size_t size) {
      if (size != 0 && dimension != size) {
	std::stringstream msg;
	msg << "ObjectSpace::allocatePersistentObject: Fatal error! The dimensionality is invalid. The specified dimensionality=" 
	    << (sparse ? dimension - 1 : dimension) << ". The specified object=" << (sparse ? size - 1 : size) << ".";
	NGTThrowException(msg);
      }
      return allocateObject(o, size);
    }

    template <typename T>
    PersistentObject *allocatePersistentObject(const std::vector<T> &o) {
      return allocatePersistentObject(o.data(), o.size());
    }
#endif

    void deleteObject(Object *po) {
      delete po;
    }

    private:
    void extractObject(void *object, std::vector<double> &d) {
      if (type == typeid(uint8_t)) {
	uint8_t *obj = (uint8_t*)object;
	for (size_t i = 0; i < dimension; i++) {
	  d.push_back(obj[i]);
	}
      } else if (type == typeid(float)) {
	float *obj = (float*)object;
	for (size_t i = 0; i < dimension; i++) {
	  d.push_back(obj[i]);
	}
      } else {
//	std::cerr << "ObjectSpace::allocate: Fatal error: unsupported type!" << std::endl;
	    if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("ObjectSpace::allocate: Fatal error: unsupported type!");
	abort();
      }
    }
    public:
    void extractObject(Object *o, std::vector<double> &d) {
      void *object = (void*)(&(*o)[0]);
      extractObject(object, d);
    }

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    void extractObject(PersistentObject *o, std::vector<double> &d) {
      SharedMemoryAllocator &objectAllocator = getAllocator();
      void *object = (void*)(&(*o).at(0, objectAllocator));
      extractObject(object, d);
    }
#endif

    void setLength(size_t l) { byteSize = l; }
    void setPaddedLength(size_t l) { paddedByteSize = l; }
    void setSparse() { sparse = true; }
    size_t getByteSize() { return byteSize; }
    size_t insert(PersistentObject *obj) { return Parent::insert(obj); }
    const size_t dimension;
    const std::type_info &type;
   protected:
    size_t byteSize;		// the length of all of elements.
    size_t paddedByteSize;
    bool sparse;		// sparse data format
  };

} // namespace NGT
