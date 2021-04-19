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

#include <algorithm>
#include <sstream>
#include "NGT/Common.h"
#include "NGT/ObjectSpaceRepository.h"
#include "NGT/defines.h"

namespace NGT {
  class DVPTree;
  class InternalNode;
  class LeafNode;
  class Node {
  public:
    typedef unsigned int	NodeID;
    class ID {
    public:
      enum Type {
	Leaf		= 1,
	Internal	= 0
      };
    ID():id(0) {}
      ID &operator=(const ID &n) {
	id = n.id;
	return *this;
      }
      ID &operator=(int i) {
	setID(i);
	return *this;
      }
      bool operator==(ID &n) { return id == n.id; }
      bool operator<(ID &n) { return id < n.id; }
      Type getType() { return (Type)((0x80000000 & id) >> 31); }
      NodeID getID() { return 0x7fffffff & id; }
      NodeID get() { return id; }
      void setID(NodeID i) { id = (0x80000000 & id) | i; }
      void setType(Type t) { id = (t << 31) | getID(); }
      void setRaw(NodeID i) { id = i; }
      void setNull() { id = 0; }
      // for milvus
      void serialize(std::stringstream & os) { NGT::Serializer::write(os, id); }
      void serialize(std::ofstream &os) { NGT::Serializer::write(os, id); }
      void deserialize(std::ifstream &is) { NGT::Serializer::read(is, id); }
      // for milvus
      void deserialize(std::stringstream & is) { NGT::Serializer::read(is, id); }
      void serializeAsText(std::ofstream &os) { NGT::Serializer::writeAsText(os, id);	}
      void deserializeAsText(std::ifstream &is) { NGT::Serializer::readAsText(is, id); }
      virtual int64_t memSize() { return sizeof(id); }
    protected:
      NodeID id;
    };

    class Object {
    public:
      Object():object(0) {}
      bool operator<(const Object &o) const { return distance < o.distance; }
      virtual int64_t memSize() { return sizeof(*this) + object->memSize(); } // size of object cannot be decided accurately
      static const double	Pivot;
      ObjectID		id;
      PersistentObject	*object;
      Distance		distance;
      Distance		leafDistance;
      int		clusterID;
    };

    typedef std::vector<Object>	Objects;

    Node() {
      parent.setNull();
      id.setNull();
    }

    virtual ~Node() {}

    Node &operator=(const Node &n) {
      id = n.id;
      parent = n.parent;
      return *this;
    }

    // for milvus
    void serialize(std::stringstream & os)
    {
        id.serialize(os);
        parent.serialize(os);
    }

    void serialize(std::ofstream &os) {
      id.serialize(os);
      parent.serialize(os);
    }

    void deserialize(std::ifstream &is) {
      id.deserialize(is);
      parent.deserialize(is);
    }

    void deserialize(std::stringstream & is)
    {
        id.deserialize(is);
        parent.deserialize(is);
    }

    void serializeAsText(std::ofstream &os) {
      id.serializeAsText(os);
      os << " ";
      parent.serializeAsText(os);
    }

    void deserializeAsText(std::ifstream &is) {
      id.deserializeAsText(is);
      parent.deserializeAsText(is);
    }

    virtual int64_t memSize() { return id.memSize() * 2 + pivot->memSize(); }

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    void setPivot(PersistentObject &f, ObjectSpace &os, SharedMemoryAllocator &allocator) {
      if (pivot == 0) {
	pivot = NGT::PersistentObject::allocate(os);
      }
      getPivot(os).set(f, os);
    }
    PersistentObject &getPivot(ObjectSpace &os) { 
      return *(PersistentObject*)os.getRepository().getAllocator().getAddr(pivot); 
    }
    void deletePivot(ObjectSpace &os, SharedMemoryAllocator &allocator) {
      os.deleteObject(&getPivot(os));
    }
#else // NGT_SHARED_MEMORY_ALLOCATOR
    void setPivot(NGT::Object &f, ObjectSpace &os) {
      if (pivot == 0) {
	pivot = NGT::Object::allocate(os);
      }
      os.copy(getPivot(), f);
    }
    NGT::Object &getPivot() { return *pivot; }
    void deletePivot(ObjectSpace &os) {
      os.deleteObject(pivot);
    }
#endif // NGT_SHARED_MEMORY_ALLOCATOR

    bool pivotIsEmpty() {
      return pivot == 0;
    }

    ID		id;
    ID		parent;

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    off_t		pivot;
#else
    NGT::Object		*pivot;
#endif

  };

  
  class InternalNode : public Node {
  public:
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    InternalNode(size_t csize, SharedMemoryAllocator &allocator) : childrenSize(csize) { initialize(allocator); }
    InternalNode(SharedMemoryAllocator &allocator, NGT::ObjectSpace *os = 0) : childrenSize(5) { initialize(allocator); }
#else
    InternalNode(size_t csize) : childrenSize(csize) { initialize(); }
    InternalNode(NGT::ObjectSpace *os = 0) : childrenSize(5) { initialize(); }
#endif

    ~InternalNode() {
#ifndef NGT_SHARED_MEMORY_ALLOCATOR
      if (children != 0) {
        delete[] children;
      }
      if (borders != 0) {
        delete[] borders;
      }
#endif
    }
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    void initialize(SharedMemoryAllocator &allocator) {
#else
    void initialize() {
#endif
      id = 0;
      id.setType(ID::Internal);
      pivot = 0;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      children = allocator.getOffset(new(allocator) ID[childrenSize]);
#else
      children = new ID[childrenSize];
#endif
      for (size_t i = 0; i < childrenSize; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
  	getChildren(allocator)[i] = 0;
#else
  	getChildren()[i] = 0;
#endif
      }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      borders = allocator.getOffset(new(allocator) Distance[childrenSize - 1]);
#else
      borders = new Distance[childrenSize - 1];
#endif
      for (size_t i = 0; i < childrenSize - 1; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
  	getBorders(allocator)[i] = 0;
#else
  	getBorders()[i] = 0;
#endif
      }
    }

#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    void updateChild(DVPTree &dvptree, ID src, ID dst, SharedMemoryAllocator &allocator);
#else
    void updateChild(DVPTree &dvptree, ID src, ID dst);
#endif

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    ID *getChildren(SharedMemoryAllocator &allocator) { return (ID*)allocator.getAddr(children); }
    Distance *getBorders(SharedMemoryAllocator &allocator) { return (Distance*)allocator.getAddr(borders); }
#else // NGT_SHARED_MEMORY_ALLOCATOR
    ID *getChildren() { return children; }
    Distance *getBorders() { return borders; }
#endif // NGT_SHARED_MEMORY_ALLOCATOR

    // for milvus
    void serialize(std::stringstream & os, ObjectSpace * objectspace = 0)
    {
        Node::serialize(os);
        if (pivot == 0)
        {
            NGTThrowException("Node::write: pivot is null!");
        }
        assert(objectspace != 0);
        getPivot().serialize(os, objectspace);
        NGT::Serializer::write(os, childrenSize);
        for (size_t i = 0; i < childrenSize; i++)
        {
            getChildren()[i].serialize(os);
        }
        for (size_t i = 0; i < childrenSize - 1; i++)
        {
            NGT::Serializer::write(os, getBorders()[i]);
        }
    }
#if defined(NGT_SHARED_MEMORY_ALLOCATOR) 
    void serialize(std::ofstream &os, SharedMemoryAllocator &allocator, ObjectSpace *objectspace = 0) {
#else
    void serialize(std::ofstream &os, ObjectSpace *objectspace = 0) {
#endif
      Node::serialize(os);
      if (pivot == 0) {
        NGTThrowException("Node::write: pivot is null!");
      }
      assert(objectspace != 0);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      getPivot(*objectspace).serialize(os, allocator, objectspace);
#else
      getPivot().serialize(os, objectspace);
#endif
      NGT::Serializer::write(os, childrenSize);
      for (size_t i = 0; i < childrenSize; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	getChildren(allocator)[i].serialize(os);
#else
	getChildren()[i].serialize(os);
#endif
      }
      for (size_t i = 0; i < childrenSize - 1; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	NGT::Serializer::write(os, getBorders(allocator)[i]);	
#else
	NGT::Serializer::write(os, getBorders()[i]);	
#endif
      }
    }
    void deserialize(std::ifstream &is, ObjectSpace *objectspace = 0) {
      Node::deserialize(is);
      if (pivot == 0) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	pivot = PersistentObject::allocate(*objectspace);
#else
	pivot = PersistentObject::allocate(*objectspace);
#endif
      }
      assert(objectspace != 0);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      std::cerr << "not implemented" << std::endl;
      assert(0);
#else
      getPivot().deserialize(is, objectspace);
#endif
      NGT::Serializer::read(is, childrenSize);
      assert(children != 0);
      for (size_t i = 0; i < childrenSize; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	assert(0);
#else
	getChildren()[i].deserialize(is);
#endif
      }
      assert(borders != 0);
      for (size_t i = 0; i < childrenSize - 1; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	assert(0);
#else
	NGT::Serializer::read(is, getBorders()[i]);
#endif
      }
    }
    // for milvus
    void deserialize(std::stringstream & is, ObjectSpace * objectspace = 0)
    {
        Node::deserialize(is);
        if (pivot == 0)
        {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	pivot = PersistentObject::allocate(*objectspace);
#else
	pivot = PersistentObject::allocate(*objectspace);
#endif
      }
      assert(objectspace != 0);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      std::cerr << "not implemented" << std::endl;
      assert(0);
#else
      getPivot().deserialize(is, objectspace);
#endif
      NGT::Serializer::read(is, childrenSize);
      assert(children != 0);
      for (size_t i = 0; i < childrenSize; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	assert(0);
#else
	getChildren()[i].deserialize(is);
#endif
      }
      assert(borders != 0);
      for (size_t i = 0; i < childrenSize - 1; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	assert(0);
#else
	NGT::Serializer::read(is, getBorders()[i]);
#endif
      }
    }

#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    void serializeAsText(std::ofstream &os, SharedMemoryAllocator &allocator, ObjectSpace *objectspace = 0) {
#else
    void serializeAsText(std::ofstream &os, ObjectSpace *objectspace = 0) {
#endif
      Node::serializeAsText(os);
      if (pivot == 0) {
        NGTThrowException("Node::write: pivot is null!");
      }
      os << " ";
      assert(objectspace != 0);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      getPivot(*objectspace).serializeAsText(os, objectspace);
#else
      getPivot().serializeAsText(os, objectspace);
#endif
      os << " ";
      NGT::Serializer::writeAsText(os, childrenSize);
      os << " ";
      for (size_t i = 0; i < childrenSize; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	getChildren(allocator)[i].serializeAsText(os);
#else
	getChildren()[i].serializeAsText(os);
#endif
	os << " ";
      }
      for (size_t i = 0; i < childrenSize - 1; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	NGT::Serializer::writeAsText(os, getBorders(allocator)[i]);	
#else
	NGT::Serializer::writeAsText(os, getBorders()[i]);	
#endif
	os << " ";
      }
    }
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    void deserializeAsText(std::ifstream &is, SharedMemoryAllocator &allocator, ObjectSpace *objectspace = 0) {
#else
    void deserializeAsText(std::ifstream &is, ObjectSpace *objectspace = 0) {
#endif
      Node::deserializeAsText(is);
      if (pivot == 0) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	pivot = PersistentObject::allocate(*objectspace);
#else
	pivot = PersistentObject::allocate(*objectspace);
#endif
      }
      assert(objectspace != 0);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      getPivot(*objectspace).deserializeAsText(is, objectspace);
#else
      getPivot().deserializeAsText(is, objectspace);
#endif
      size_t csize;
      NGT::Serializer::readAsText(is, csize);
      assert(children != 0);
      assert(childrenSize == csize);
      for (size_t i = 0; i < childrenSize; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	getChildren(allocator)[i].deserializeAsText(is);
#else
	getChildren()[i].deserializeAsText(is);
#endif
      }
      assert(borders != 0);
      for (size_t i = 0; i < childrenSize - 1; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	NGT::Serializer::readAsText(is, getBorders(allocator)[i]);
#else
	NGT::Serializer::readAsText(is, getBorders()[i]);
#endif
      }
    }

    virtual int64_t memSize() { return sizeof(childrenSize) + children->memSize() + childrenSize * sizeof(Distance) + Node::memSize(); }

    void show() {
      std::cout << "Show internal node " << childrenSize << ":";
      for (size_t i = 0; i < childrenSize; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	assert(0);
#else
	std::cout << getChildren()[i].getID() << " ";
#endif
      }
      std::cout << std::endl;
    }

#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    bool verify(PersistentRepository<InternalNode> &internalNodes, PersistentRepository<LeafNode> &leafNodes, 
		SharedMemoryAllocator &allocator);
#else
    bool verify(Repository<InternalNode> &internalNodes, Repository<LeafNode> &leafNodes);
#endif

    static const int InternalChildrenSizeMax	= 5;
    const size_t	childrenSize;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    off_t		children;
    off_t		borders;
#else
    ID			*children;
    Distance		*borders;
#endif
  };

  
  class LeafNode : public Node {
  public:
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    LeafNode(SharedMemoryAllocator &allocator, NGT::ObjectSpace *os = 0) {
#else
    LeafNode(NGT::ObjectSpace *os = 0) {
#endif
      id = 0;
      id.setType(ID::Leaf);
      pivot = 0;
#ifdef NGT_NODE_USE_VECTOR
      objectIDs.reserve(LeafObjectsSizeMax);
#else
      objectSize = 0;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      objectIDs = allocator.getOffset(new(allocator) Object[LeafObjectsSizeMax]);
#else
      objectIDs = new NGT::ObjectDistance[LeafObjectsSizeMax];
#endif
#endif
    }

    ~LeafNode() {
#ifndef NGT_SHARED_MEMORY_ALLOCATOR
#ifndef NGT_NODE_USE_VECTOR
      if (objectIDs != 0) {
        delete[] objectIDs;
      }
#endif
#endif
    }

    static int
      selectPivotByMaxDistance(Container &iobj, Node::Objects &fs);

    static int
      selectPivotByMaxVariance(Container &iobj, Node::Objects &fs);

    static void
      splitObjects(Container &insertedObject, Objects &splitObjectSet, int pivot);

#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    void removeObject(size_t id, size_t replaceId, SharedMemoryAllocator &allocator);
#else
    void removeObject(size_t id, size_t replaceId);
#endif

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
#ifndef NGT_NODE_USE_VECTOR
    NGT::ObjectDistance *getObjectIDs(SharedMemoryAllocator &allocator) {
      return (NGT::ObjectDistance *)allocator.getAddr(objectIDs); 
    }
#endif
#else // NGT_SHARED_MEMORY_ALLOCATOR
    NGT::ObjectDistance *getObjectIDs() { return objectIDs; }
#endif // NGT_SHARED_MEMORY_ALLOCATOR

    // for milvus
    void serialize(std::stringstream & os, ObjectSpace * objectspace = 0)
    {
        Node::serialize(os);
        NGT::Serializer::write(os, objectSize);
        for (int i = 0; i < objectSize; i++)
        {
            objectIDs[i].serialize(os);
        }
        if (pivot == 0)
        {
            // Before insertion, parent ID == 0 and object size == 0, that indicates an empty index
            if (parent.getID() != 0 || objectSize != 0)
            {
                NGTThrowException("Node::write: pivot is null!");
            }
        }
        else
        {
            assert(objectspace != 0);
            pivot->serialize(os, objectspace);
        }
    }
    void serialize(std::ofstream &os, ObjectSpace *objectspace = 0) {
      Node::serialize(os);
#ifdef NGT_NODE_USE_VECTOR
      NGT::Serializer::write(os, objectIDs);
#else
      NGT::Serializer::write(os, objectSize);
      for (int i = 0; i < objectSize; i++) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	std::cerr << "not implemented" << std::endl;
	assert(0);
#else
	objectIDs[i].serialize(os);
#endif
      }
#endif // NGT_NODE_USE_VECTOR
      if (pivot == 0) {
	// Before insertion, parent ID == 0 and object size == 0, that indicates an empty index
	if (parent.getID() != 0 || objectSize != 0) {
	  NGTThrowException("Node::write: pivot is null!");
	}
      } else {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	std::cerr << "not implemented" << std::endl;
	assert(0);
#else
	assert(objectspace != 0);
	pivot->serialize(os, objectspace);
#endif
      }
    }
    void deserialize(std::ifstream &is, ObjectSpace *objectspace = 0) {
      Node::deserialize(is);

#ifdef NGT_NODE_USE_VECTOR
      objectIDs.clear();
      NGT::Serializer::read(is, objectIDs);
#else
      assert(objectIDs != 0);
      NGT::Serializer::read(is, objectSize);
      for (int i = 0; i < objectSize; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	std::cerr << "not implemented" << std::endl;
	assert(0);
#else
	getObjectIDs()[i].deserialize(is);
#endif
      }
#endif
      if (parent.getID() == 0 && objectSize == 0) {
	// The index is empty
	return;
      }
      if (pivot == 0) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	pivot = PersistentObject::allocate(*objectspace);
#else
	pivot = PersistentObject::allocate(*objectspace);
	assert(pivot != 0);
#endif
      }
      assert(objectspace != 0);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      std::cerr << "not implemented" << std::endl;
      assert(0);
#else
      getPivot().deserialize(is, objectspace);
#endif
    }

    // for milvus
    void deserialize(std::stringstream & is, ObjectSpace * objectspace = 0)
    {
        Node::deserialize(is);

#ifdef NGT_NODE_USE_VECTOR
      objectIDs.clear();
      NGT::Serializer::read(is, objectIDs);
#else
      assert(objectIDs != 0);
      NGT::Serializer::read(is, objectSize);
      for (int i = 0; i < objectSize; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	std::cerr << "not implemented" << std::endl;
	assert(0);
#else
	getObjectIDs()[i].deserialize(is);
#endif
      }
#endif
      if (parent.getID() == 0 && objectSize == 0) {
	// The index is empty
	return;
      }
      if (pivot == 0) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	pivot = PersistentObject::allocate(*objectspace);
#else
	pivot = PersistentObject::allocate(*objectspace);
	assert(pivot != 0);
#endif
      }
      assert(objectspace != 0);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      std::cerr << "not implemented" << std::endl;
      assert(0);
#else
      getPivot().deserialize(is, objectspace);
#endif
    }

#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    void serializeAsText(std::ofstream &os, SharedMemoryAllocator &allocator, ObjectSpace *objectspace = 0) {
#else
    void serializeAsText(std::ofstream &os, ObjectSpace *objectspace = 0) {
#endif
      Node::serializeAsText(os);
      os << " ";
      if (pivot == 0) {
        NGTThrowException("Node::write: pivot is null!");
      }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      getPivot(*objectspace).serializeAsText(os, objectspace);
#else
      assert(pivot != 0);
      assert(objectspace != 0);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      pivot->serializeAsText(os, allocator, objectspace);
#else
      pivot->serializeAsText(os, objectspace);
#endif
#endif
      os << " ";
#ifdef NGT_NODE_USE_VECTOR
      NGT::Serializer::writeAsText(os, objectIDs);
#else
      NGT::Serializer::writeAsText(os, objectSize);
      for (int i = 0; i < objectSize; i++) {
	os << " ";
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	getObjectIDs(allocator)[i].serializeAsText(os);
#else
	objectIDs[i].serializeAsText(os);
#endif
      }
#endif
    }

#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    void deserializeAsText(std::ifstream &is, SharedMemoryAllocator &allocator, ObjectSpace *objectspace = 0) {
#else
    void deserializeAsText(std::ifstream &is, ObjectSpace *objectspace = 0) {
#endif
      Node::deserializeAsText(is);
      if (pivot == 0) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	pivot = PersistentObject::allocate(*objectspace);
#else
	pivot = PersistentObject::allocate(*objectspace);
#endif
      }
      assert(objectspace != 0);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      getPivot(*objectspace).deserializeAsText(is, objectspace);
#else
      getPivot().deserializeAsText(is, objectspace);
#endif
#ifdef NGT_NODE_USE_VECTOR
      objectIDs.clear();
      NGT::Serializer::readAsText(is, objectIDs);
#else
      assert(objectIDs != 0);
      NGT::Serializer::readAsText(is, objectSize);
      for (int i = 0; i < objectSize; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	getObjectIDs(allocator)[i].deserializeAsText(is);
#else
	getObjectIDs()[i].deserializeAsText(is);
#endif
      }
#endif
    }

    void show() {
      std::cout << "Show leaf node " << objectSize << ":";
      for (int i = 0; i < objectSize; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	std::cerr << "not implemented" << std::endl;
	assert(0);
#else
	std::cout << getObjectIDs()[i].id << "," << getObjectIDs()[i].distance << " ";
#endif
      }
      std::cout << std::endl;
    }

#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    bool verify(size_t nobjs, std::vector<uint8_t> &status, SharedMemoryAllocator &allocator);
#else
    bool verify(size_t nobjs, std::vector<uint8_t> &status);
#endif

    virtual int64_t memSize() { return sizeof(objectSize) + objectIDs->memSize() * objectSize + Node::memSize(); }

#ifdef NGT_NODE_USE_VECTOR
    size_t getObjectSize() { return objectIDs.size(); }
#else
    size_t getObjectSize() { return objectSize; }
#endif

    static const size_t LeafObjectsSizeMax		= 100;

#ifdef NGT_NODE_USE_VECTOR
    std::vector<Object>	objectIDs;
#else
    unsigned short	objectSize;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    off_t		objectIDs;
#else
    ObjectDistance	*objectIDs;
#endif
#endif
  };


}
