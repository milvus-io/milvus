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

#include <cstring>
#include "PrimitiveComparator.h"

class ObjectSpace;

namespace NGT {

  class PersistentObjectDistances;
  class ObjectDistances : public std::vector<ObjectDistance> {
  public:
    ObjectDistances(NGT::ObjectSpace *os = 0) {}
    // for milvus
    void serialize(std::stringstream & os, ObjectSpace * objspace = 0) { NGT::Serializer::write(os, (std::vector<ObjectDistance> &)*this); }
    void serialize(std::ofstream &os, ObjectSpace *objspace = 0) { NGT::Serializer::write(os, (std::vector<ObjectDistance>&)*this);}
    // for milvus
    void deserialize(std::stringstream & is, ObjectSpace * objspace = 0)
    {
        NGT::Serializer::read(is, (std::vector<ObjectDistance> &)*this);
    }
    void deserialize(std::ifstream &is, ObjectSpace *objspace = 0) { NGT::Serializer::read(is, (std::vector<ObjectDistance>&)*this);}

    void serializeAsText(std::ofstream &os, ObjectSpace *objspace = 0) { 
      NGT::Serializer::writeAsText(os, size());
      os << " ";
      for (size_t i = 0; i < size(); i++) {
	(*this)[i].serializeAsText(os);
	os << " ";
      }
    }
    void deserializeAsText(std::ifstream &is, ObjectSpace *objspace = 0) {
      size_t s;
      NGT::Serializer::readAsText(is, s);     
      resize(s);
      for (size_t i = 0; i < size(); i++) {
	(*this)[i].deserializeAsText(is);
      }
    }

    void moveFrom(std::priority_queue<ObjectDistance, std::vector<ObjectDistance>, std::less<ObjectDistance> > &pq) {
      this->clear();
      this->resize(pq.size());
      for (int i = pq.size() - 1; i >= 0; i--) {
	(*this)[i] = pq.top();
	pq.pop();
      }
      assert(pq.size() == 0);
    }

    void moveFrom(std::priority_queue<ObjectDistance, std::vector<ObjectDistance>, std::less<ObjectDistance> > &pq, double (&f)(double)) {
      this->clear();
      this->resize(pq.size());
      for (int i = pq.size() - 1; i >= 0; i--) {
	(*this)[i] = pq.top();
	(*this)[i].distance = f((*this)[i].distance);
	pq.pop();
      }
      assert(pq.size() == 0);
    }

    void moveFrom(std::priority_queue<ObjectDistance, std::vector<ObjectDistance>, std::less<ObjectDistance> > &pq, unsigned int id) {
      this->clear();
      if (pq.size() == 0) {
	return;
      }
      this->resize(id == 0 ? pq.size() : pq.size() - 1);
      int i = this->size() - 1;
      while (pq.size() != 0 && i >= 0) {
	if (pq.top().id != id) {
	  (*this)[i] = pq.top();
	  i--;
	}
	pq.pop();
      }
      if (pq.size() != 0 && pq.top().id != id) {
	std::cerr << "moveFrom: Fatal error: somethig wrong! " << pq.size() << ":" << this->size() << ":" << id << ":" << pq.top().id << std::endl;
	assert(pq.size() == 0 || pq.top().id == id);
      }
    }

      int64_t memSize() const {
//          auto obj = (std::vector<ObjectDistance>)(*this);
          if (this->size() == 0)
              return 0;
          else {
              return (*this)[0].memSize() * this->size();
          }
//        return this->size() == 0 ? 0 : (*this)[0].memSize() * (this->size());
      }
    ObjectDistances &operator=(PersistentObjectDistances &objs);
  };

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  class PersistentObjectDistances : public Vector<ObjectDistance> {
  public:
    PersistentObjectDistances(SharedMemoryAllocator &allocator, NGT::ObjectSpace *os = 0) {}
    void serialize(std::ofstream &os, ObjectSpace *objectspace = 0) { NGT::Serializer::write(os, (Vector<ObjectDistance>&)*this); }
    void deserialize(std::ifstream &is, ObjectSpace *objectspace = 0) { NGT::Serializer::read(is, (Vector<ObjectDistance>&)*this); }
    void serializeAsText(std::ofstream &os, SharedMemoryAllocator &allocator, ObjectSpace *objspace = 0) { 
      NGT::Serializer::writeAsText(os, size());
      os << " ";
      for (size_t i = 0; i < size(); i++) {
	(*this).at(i, allocator).serializeAsText(os);
	os << " ";
      }
    }
    void deserializeAsText(std::ifstream &is, SharedMemoryAllocator &allocator, ObjectSpace *objspace = 0) {
      size_t s;
      is >> s;
      resize(s, allocator);
      for (size_t i = 0; i < size(); i++) {
	(*this).at(i, allocator).deserializeAsText(is);
      }
    }
    PersistentObjectDistances &copy(ObjectDistances &objs, SharedMemoryAllocator &allocator) {
      clear(allocator);
      reserve(objs.size(), allocator);
      for (ObjectDistances::iterator i = objs.begin(); i != objs.end(); i++) {
	push_back(*i, allocator);
      }
      return *this;
    }
  };
  typedef PersistentObjectDistances	GraphNode;

  inline ObjectDistances &ObjectDistances::operator=(PersistentObjectDistances &objs)
    {
      clear();
      reserve(objs.size());
      std::cerr << "not implemented" << std::endl;
      assert(0);
      return *this;
    }
#else // NGT_SHARED_MEMORY_ALLOCATOR
  typedef ObjectDistances	GraphNode;
#endif // NGT_SHARED_MEMORY_ALLOCATOR

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  class PersistentObject;
#else
  typedef Object	PersistentObject;
#endif

  class ObjectRepository;

  class ObjectSpace {
  public:
    class Comparator {
    public:
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    Comparator(size_t d, SharedMemoryAllocator &a) : dimension(d), allocator(a) {}
#else
    Comparator(size_t d) : dimension(d) {}
#endif
      virtual double operator()(Object &objecta, Object &objectb) = 0;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      virtual double operator()(Object &objecta, PersistentObject &objectb) = 0;
      virtual double operator()(PersistentObject &objecta, PersistentObject &objectb) = 0;
#endif
      size_t dimension;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      SharedMemoryAllocator &allocator;
#endif
      virtual ~Comparator(){}
      int64_t memSize() { return sizeof(size_t); }
    };
    enum DistanceType {
      DistanceTypeNone			= -1,
      DistanceTypeL1			= 0,
      DistanceTypeL2			= 1,
      DistanceTypeHamming		= 2,
      DistanceTypeAngle			= 3,
      DistanceTypeCosine		= 4,
      DistanceTypeNormalizedAngle	= 5,
      DistanceTypeNormalizedCosine	= 6,
      DistanceTypeJaccard		= 7,
      DistanceTypeSparseJaccard		= 8,
      DistanceTypeIP		    = 9
    };

    enum ObjectType {
      ObjectTypeNone	= 0,
      Uint8		= 1,
      Float		= 2
    };


    typedef std::priority_queue<ObjectDistance, std::vector<ObjectDistance>, std::less<ObjectDistance> > ResultSet;
    ObjectSpace(size_t d):dimension(d), distanceType(DistanceTypeNone), comparator(0), normalization(false) {}
    virtual ~ObjectSpace() { if (comparator != 0) { delete comparator; } }
    
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    virtual void open(const std::string &f, size_t shareMemorySize) = 0;
    virtual Object *allocateObject(Object &o) = 0;
    virtual Object *allocateObject(PersistentObject &o) = 0;
    virtual PersistentObject *allocatePersistentObject(Object &obj) = 0;
    virtual void deleteObject(PersistentObject *) = 0;
    virtual void copy(PersistentObject &objecta, PersistentObject &objectb) = 0;
    virtual void show(std::ostream &os, PersistentObject &object) = 0;
    virtual size_t insert(PersistentObject *obj) = 0;
#else
    virtual size_t insert(Object *obj) = 0;
#endif

    Comparator &getComparator() { return *comparator; }

    virtual void serialize(const std::string &of) = 0;
    // for milvus
    virtual void serialize(std::stringstream & obj) = 0;
    // for milvus
    virtual void deserialize(std::stringstream & obj) = 0;
    virtual void deserialize(const std::string &ifile) = 0;
    virtual void serializeAsText(const std::string &of) = 0;
    virtual void deserializeAsText(const std::string &of) = 0;
    //for milvus
    virtual void readRawData(const float * raw_data, size_t dataSize) = 0;
    virtual void readText(std::istream &is, size_t dataSize) = 0;
    virtual void appendText(std::istream &is, size_t dataSize) = 0;
    virtual void append(const float *data, size_t dataSize) = 0;
    virtual void append(const double *data, size_t dataSize) = 0;

    virtual void copy(Object &objecta, Object &objectb) = 0;

    virtual void linearSearch(Object &query, double radius, size_t size,  
			      ObjectSpace::ResultSet &results) = 0;

    virtual const std::type_info &getObjectType() = 0;
    virtual void show(std::ostream &os, Object &object) = 0;
    virtual size_t getSize() = 0;
    virtual size_t getSizeOfElement() = 0;
    virtual size_t getByteSizeOfObject() = 0;
    virtual Object *allocateNormalizedObject(const std::string &textLine, const std::string &sep) = 0;
    virtual Object *allocateNormalizedObject(const std::vector<double> &obj) = 0;
    virtual Object *allocateNormalizedObject(const std::vector<float> &obj) = 0;
    virtual Object *allocateNormalizedObject(const std::vector<uint8_t> &obj) = 0;
    virtual Object *allocateNormalizedObject(const float *obj, size_t size) = 0;
    virtual PersistentObject *allocateNormalizedPersistentObject(const std::vector<double> &obj) = 0;
    virtual PersistentObject *allocateNormalizedPersistentObject(const std::vector<float> &obj) = 0;
    virtual void deleteObject(Object *po) = 0;
    virtual Object *allocateObject() = 0;
    virtual void remove(size_t id) = 0;

    virtual ObjectRepository &getRepository() = 0;

    virtual void setDistanceType(DistanceType t) = 0;
    virtual DistanceType getDistanceType() = 0;

    virtual void *getObject(size_t idx) = 0;
    virtual void getObject(size_t idx, std::vector<float> &v) = 0;
    virtual void getObjects(const std::vector<size_t> &idxs, std::vector<std::vector<float>> &vs) = 0;

    size_t getDimension() { return dimension; }
    size_t getPaddedDimension() { return ((dimension - 1) / 16 + 1) * 16; }
    virtual int64_t memSize() { return sizeof(dimension) + sizeof(distanceType) + sizeof(prefetchOffset) * 2 + sizeof(normalization) + comparator->memSize(); };

    template <typename T>
    void normalize(T *data, size_t dim) {
      double sum = 0.0;
      for (size_t i = 0; i < dim; i++) {
	sum += (double)data[i] * (double)data[i];
      }
      if (sum == 0.0) {
	std::stringstream msg;
	msg << "ObjectSpace::normalize: Error! the object is an invalid zero vector for the cosine similarity or angle distance.";
	NGTThrowException(msg);
      }
      sum = sqrt(sum);
      for (size_t i = 0; i < dim; i++) {
	data[i] = (double)data[i] / sum;
      }
    }
    uint32_t getPrefetchOffset() { return prefetchOffset; }
    uint32_t setPrefetchOffset(size_t offset) {
      if (offset == 0) {
	prefetchOffset = floor(300.0 / (static_cast<float>(getPaddedDimension()) + 30.0) + 1.0);
      } else {
	prefetchOffset = offset;
      }
      return prefetchOffset;
    }
    uint32_t getPrefetchSize() { return prefetchSize; }
    uint32_t setPrefetchSize(size_t size) {
      if (size == 0) {
	prefetchSize = getByteSizeOfObject();
      } else {
	prefetchSize = size;
      }
      return prefetchSize;
    }
  protected:
    const size_t	dimension;
    DistanceType	distanceType;
    Comparator		*comparator;
    bool		normalization;
    uint32_t		prefetchOffset;
    uint32_t		prefetchSize;
  };

  class BaseObject {
  public:
    virtual uint8_t &operator[](size_t idx) const = 0;
    void serialize(std::ostream &os, ObjectSpace *objectspace = 0) { 
      assert(objectspace != 0);
      if(objectspace == 0) return; // make compiler happy;
      size_t byteSize = objectspace->getByteSizeOfObject();
      NGT::Serializer::write(os, (uint8_t*)&(*this)[0], byteSize); 
    }
    void deserialize(std::istream &is, ObjectSpace *objectspace = 0) { 
      assert(objectspace != 0);
      if(objectspace == 0) return; // make compiler happy;
      size_t byteSize = objectspace->getByteSizeOfObject();
      assert(&(*this)[0] != 0);
      NGT::Serializer::read(is, (uint8_t*)&(*this)[0], byteSize); 
    }
    void serializeAsText(std::ostream &os, ObjectSpace *objectspace = 0) { 
      assert(objectspace != 0);
      if(objectspace == 0) return; // make compiler happy;
      const std::type_info &t = objectspace->getObjectType();
      size_t dimension = objectspace->getDimension();
      void *ref = (void*)&(*this)[0];
      if (t == typeid(uint8_t)) {
	NGT::Serializer::writeAsText(os, (uint8_t*)ref, dimension); 
      } else if (t == typeid(float)) {
	NGT::Serializer::writeAsText(os, (float*)ref, dimension); 
      } else if (t == typeid(double)) {
	NGT::Serializer::writeAsText(os, (double*)ref, dimension); 
      } else if (t == typeid(uint16_t)) {
	NGT::Serializer::writeAsText(os, (uint16_t*)ref, dimension); 
      } else if (t == typeid(uint32_t)) {
	NGT::Serializer::writeAsText(os, (uint32_t*)ref, dimension); 
      } else {
	std::cerr << "Object::serializeAsText: not supported data type. [" << t.name() << "]" << std::endl;
	assert(0);
      }
    }
    void deserializeAsText(std::ifstream &is, ObjectSpace *objectspace = 0) {
      assert(objectspace != 0);
      if(objectspace == 0) return;
      const std::type_info &t = objectspace->getObjectType();
      size_t dimension = objectspace->getDimension();
      void *ref = (void*)&(*this)[0];
      assert(ref != 0);
      if (t == typeid(uint8_t)) {
	NGT::Serializer::readAsText(is, (uint8_t*)ref, dimension); 
      } else if (t == typeid(float)) {
	NGT::Serializer::readAsText(is, (float*)ref, dimension); 
      } else if (t == typeid(double)) {
	NGT::Serializer::readAsText(is, (double*)ref, dimension); 
      } else if (t == typeid(uint16_t)) {
	NGT::Serializer::readAsText(is, (uint16_t*)ref, dimension); 
      } else if (t == typeid(uint32_t)) {
	NGT::Serializer::readAsText(is, (uint32_t*)ref, dimension); 
      } else {
	std::cerr << "Object::deserializeAsText: not supported data type. [" << t.name() << "]" << std::endl;
	assert(0);
      }
    }

  };

  class Object : public BaseObject {
  public:
    Object(NGT::ObjectSpace *os = 0):vector(0) {
      assert(os != 0);
      if(os == 0) return;
      size_t s = os->getByteSizeOfObject();
      construct(s);
    }

    Object(size_t s):vector(0) {
      assert(s != 0);
      construct(s);
    }

    void copy(Object &o, size_t s) {
      assert(vector != 0);
      for (size_t i = 0; i < s; i++) {
	vector[i] = o[i];
      }
    }

    virtual ~Object() { clear(); }

    uint8_t &operator[](size_t idx) const { return vector[idx]; }

    void *getPointer(size_t idx = 0) const { return vector + idx; }

    static Object *allocate(ObjectSpace &objectspace) { return new Object(&objectspace); }

    virtual int64_t memSize() { return std::strlen((char*)vector); }
  private:
    void clear() {
      if (vector != 0) {
	MemoryCache::alignedFree(vector);
      }
      vector = 0;
    }

    void construct(size_t s) {
      assert(vector == 0);
      size_t allocsize = ((s - 1) / 64 + 1) * 64;	
      vector = static_cast<uint8_t*>(MemoryCache::alignedAlloc(allocsize));
      memset(vector, 0, allocsize);
    }

    uint8_t* vector;
  };


#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  class PersistentObject : public BaseObject {
  public:
    PersistentObject(SharedMemoryAllocator &allocator, NGT::ObjectSpace *os = 0):array(0) {
      assert(os != 0);
      size_t s = os->getByteSizeOfObject();
      construct(s, allocator);
    }
    PersistentObject(SharedMemoryAllocator &allocator, size_t s):array(0) {
      assert(s != 0);
      construct(s, allocator);
    }

    ~PersistentObject() {}

    uint8_t &at(size_t idx, SharedMemoryAllocator &allocator) const { 
      uint8_t *a = (uint8_t *)allocator.getAddr(array);
      return a[idx];
    }
    uint8_t &operator[](size_t idx) const {
      std::cerr << "not implemented" << std::endl;
      assert(0);
      uint8_t *a = 0;
      return a[idx];
    }

    void *getPointer(size_t idx, SharedMemoryAllocator &allocator) {
      uint8_t *a = (uint8_t *)allocator.getAddr(array);
      return a + idx; 
    }

    // set v in objectspace to this object using allocator.
    void set(PersistentObject &po, ObjectSpace &objectspace);

    static off_t allocate(ObjectSpace &objectspace);

    void serializeAsText(std::ostream &os, SharedMemoryAllocator &allocator, 
			 ObjectSpace *objectspace = 0) { 
      serializeAsText(os, objectspace);
    }

    void serializeAsText(std::ostream &os, ObjectSpace *objectspace = 0);

    void deserializeAsText(std::ifstream &is, SharedMemoryAllocator &allocator, 
			   ObjectSpace *objectspace = 0) {
      deserializeAsText(is, objectspace);
    }

    void deserializeAsText(std::ifstream &is, ObjectSpace *objectspace = 0);

    void serialize(std::ostream &os, SharedMemoryAllocator &allocator, 
		   ObjectSpace *objectspace = 0) { 
      std::cerr << "serialize is not implemented" << std::endl;
      assert(0);
    }

  private:
    void construct(size_t s, SharedMemoryAllocator &allocator) {
      assert(array == 0);
      assert(s != 0);
      size_t allocsize = ((s - 1) / 64 + 1) * 64;	
      array = allocator.getOffset(new(allocator) uint8_t[allocsize]);
      memset(getPointer(0, allocator), 0, allocsize);
    }
    off_t array;
  };
#endif // NGT_SHARED_MEMORY_ALLOCATOR

}

