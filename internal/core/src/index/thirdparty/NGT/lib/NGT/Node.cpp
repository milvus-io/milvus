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

#include	"NGT/defines.h"

#include	"NGT/Node.h"
#include	"NGT/Tree.h"

#include	<algorithm>

using namespace std;

const double NGT::Node::Object::Pivot = -1.0;

using namespace NGT;

void
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
InternalNode::updateChild(DVPTree &dvptree, Node::ID src, Node::ID dst,
			  SharedMemoryAllocator &allocator) {
#else
InternalNode::updateChild(DVPTree &dvptree, Node::ID src, Node::ID dst) {
#endif
  int cs = dvptree.internalChildrenSize;
  for (int i = 0; i < cs; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    if (getChildren(allocator)[i] == src) {
      getChildren(allocator)[i] = dst;
#else
    if (getChildren()[i] == src) {
      getChildren()[i] = dst;
#endif
      return;
    }
  }
}

int
LeafNode::selectPivotByMaxDistance(Container &c, Node::Objects &fs)
{
  DVPTree::InsertContainer &iobj = (DVPTree::InsertContainer&)c;
  int fsize = fs.size();
  Distance maxd = 0.0;
  int maxid = 0;
  for (int i = 1; i < fsize; i++) {
    Distance d = iobj.vptree->objectSpace->getComparator()(*fs[0].object, *fs[i].object);
    if (d >= maxd) {
      maxd = d;
      maxid = i;
    }
  }

  int aid = maxid;
  maxd = 0.0;
  maxid = 0;
  for (int i = 0; i < fsize; i++) {
    Distance d = iobj.vptree->objectSpace->getComparator()(*fs[aid].object, *fs[i].object);
    if (i == aid) {
      continue;
    }
    if (d >= maxd) {
      maxd = d;
      maxid = i;
    }
  }

  int bid = maxid;
  maxd = 0.0;
  maxid = 0;
  for (int i = 0; i < fsize; i++) {
    Distance d = iobj.vptree->objectSpace->getComparator()(*fs[bid].object, *fs[i].object);
    if (i == bid) {
      continue;
    }
    if (d >= maxd) {
      maxd = d;
      maxid = i;
    }
  }
  return maxid;
}

int
LeafNode::selectPivotByMaxVariance(Container &c, Node::Objects &fs)
{
  DVPTree::InsertContainer &iobj = (DVPTree::InsertContainer&)c;

  int fsize = fs.size();
  Distance *distance = new Distance[fsize * fsize];

  for (int i = 0; i < fsize; i++) {
    distance[i * fsize + i] = 0;
  }

  for (int i = 0; i < fsize; i++) {
    for (int j = i + 1; j < fsize; j++) {
      Distance d = iobj.vptree->objectSpace->getComparator()(*fs[i].object, *fs[j].object);
      distance[i * fsize + j] = d;
      distance[j * fsize + i] = d;
    }
  }

  double *variance = new double[fsize];
  for (int i = 0; i < fsize; i++) {
    double avg = 0.0;
    for (int j = 0; j < fsize; j++) {
      avg += distance[i * fsize + j];
    }
    avg /= (double)fsize;

    double v = 0.0;
    for (int j = 0; j < fsize; j++) {
      v += pow(distance[i * fsize + j] - avg, 2.0);
    }
    variance[i] = v / (double)fsize;
  }

  double maxv = variance[0];
  int maxid = 0;
  for (int i = 0; i < fsize; i++) {
    if (variance[i] > maxv) {
      maxv = variance[i];
      maxid = i;
    }
  }
  delete [] variance;
  delete [] distance;

  return maxid;
}

void
LeafNode::splitObjects(Container &c, Objects &fs, int pv)
{
  DVPTree::InsertContainer &iobj = (DVPTree::InsertContainer&)c;

  // sort the objects by distance
  int fsize = fs.size();
  for (int i = 0; i < fsize; i++) {
    if (i == pv) {
      fs[i].distance = 0;
    } else {
      Distance d = iobj.vptree->objectSpace->getComparator()(*fs[pv].object, *fs[i].object);
      fs[i].distance = d;
    }
  }

  sort(fs.begin(), fs.end());

  int childrenSize = iobj.vptree->internalChildrenSize;
  int cid = childrenSize - 1;
  int cms = (fsize * cid) / childrenSize;

  // divide the objects into child clusters.
  fs[fsize - 1].clusterID = cid;
  for (int i = fsize - 2; i >= 0; i--) {
    if (i < cms && cid > 0) {
      if (fs[i].distance != fs[i + 1].distance) {
        cid--;
        cms = (fsize * cid) / childrenSize;
      }
    }
    fs[i].clusterID = cid;
  }

  if (cid != 0) {
    // the required number of child nodes could not be acquired
    stringstream msg;
    msg << "LeafNode::splitObjects: Too many same distances. Reduce internal children size for the tree index or not use the tree index." << endl;
    msg << "  internalChildrenSize=" << childrenSize << endl;
    msg << "  # of the children=" << (childrenSize - cid) << endl;
    msg << "  Size=" << fsize << endl;
    msg << "  pivot=" << pv << endl;
    msg << "  cluster id=" << cid << endl;
    msg << "  Show distances for debug." << endl;
    for (int i = 0; i < fsize; i++) {
      msg << "  " << fs[i].id << ":" << fs[i].distance << endl;
      msg << "  ";
      PersistentObject &po = *fs[i].object;
      iobj.vptree->objectSpace->show(msg, po);
      msg << endl;
    }
    if (fs[fsize - 1].clusterID == cid) {
      msg << "LeafNode::splitObjects: All of the object distances are the same!" << endl;;
      NGTThrowException(msg.str());
    } else {
      cerr << msg.str() << endl;
      cerr << "LeafNode::splitObjects: Anyway, continue..." << endl;
      // sift the cluster IDs to start from 0 to continue.
      for (int i = 0; i < fsize; i++) {
	fs[i].clusterID -= cid;
      }
    }
  }

  long long	*pivots = new long long[childrenSize];
  for (int i = 0; i < childrenSize; i++) {
    pivots[i] = -1;
  }

  // find the boundaries for the subspaces
  for (int i = 0; i < fsize; i++) {
    if (pivots[fs[i].clusterID] == -1) {
      pivots[fs[i].clusterID] = i;
      fs[i].leafDistance = Object::Pivot;
    } else {
      Distance d = iobj.vptree->objectSpace->getComparator()(*fs[pivots[fs[i].clusterID]].object, *fs[i].object);
      fs[i].leafDistance = d;
    }
  }
  delete[] pivots;

  return;
}

void
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
LeafNode::removeObject(size_t id, size_t replaceId, SharedMemoryAllocator &allocator) {
#else
LeafNode::removeObject(size_t id, size_t replaceId) {
#endif

  size_t fsize = getObjectSize();
  size_t idx;
  for (idx = 0; idx < fsize; idx++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    if (getObjectIDs(allocator)[idx].id == id) {
      if (replaceId != 0) {
	getObjectIDs(allocator)[idx].id = replaceId;
#else
    if (getObjectIDs()[idx].id == id) {
      if (replaceId != 0) {
	getObjectIDs()[idx].id = replaceId;
#endif
	return;
      } else {
	break;
      }
    }
  }
  if (idx == fsize) {
    if (pivot == 0) {
      NGTThrowException("LeafNode::removeObject: Internal error!. the pivot is illegal.");
    }
    stringstream msg;
    msg << "VpTree::Leaf::remove: Warning. Cannot find the specified object. ID=" << id << "," << replaceId << " idx=" << idx << " If the same objects were inserted into the index, ignore this message.";
    NGTThrowException(msg.str());
  }

#ifdef NGT_NODE_USE_VECTOR
  for (; idx < objectIDs.size() - 1; idx++) {
    getObjectIDs()[idx] = getObjectIDs()[idx + 1];
  }
  objectIDs.pop_back();
#else
  objectSize--;
  for (; idx < objectSize; idx++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    getObjectIDs(allocator)[idx] = getObjectIDs(allocator)[idx + 1];
#else
    getObjectIDs()[idx] = getObjectIDs()[idx + 1];
#endif
  }
#endif

  return;
}

#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
bool InternalNode::verify(PersistentRepository<InternalNode> &internalNodes, PersistentRepository<LeafNode> &leafNodes, 
			  SharedMemoryAllocator &allocator) {
#else
bool InternalNode::verify(Repository<InternalNode> &internalNodes, Repository<LeafNode> &leafNodes) {
#endif
  size_t isize = internalNodes.size();
  size_t lsize = leafNodes.size();
  bool valid = true;
  for (size_t i = 0; i < childrenSize; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    size_t nid = getChildren(allocator)[i].getID();
    ID::Type type = getChildren(allocator)[i].getType();
#else
    size_t nid = getChildren()[i].getID();
    ID::Type type = getChildren()[i].getType();
#endif
    size_t size = type == ID::Leaf ? lsize : isize;
    if (nid >= size) {
      cerr << "Error! Internal children node id is too big." << nid << ":" << size << endl;
      valid = false;
    }
    try {
      if (type == ID::Leaf) {
	leafNodes.get(nid);
      } else {
	internalNodes.get(nid);
      }
    } catch (...) {
      cerr << "Error! Cannot get the node. " << ((type == ID::Leaf) ? "Leaf" : "Internal") << endl;
      valid = false;
    }
  }
  return valid;
}

#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
bool LeafNode::verify(size_t nobjs, vector<uint8_t> &status, SharedMemoryAllocator &allocator) {
#else
bool LeafNode::verify(size_t nobjs, vector<uint8_t> &status) {
#endif
  bool valid = true;
  for (size_t i = 0; i < objectSize; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    size_t nid = getObjectIDs(allocator)[i].id;
#else
    size_t nid = getObjectIDs()[i].id;
#endif
    if (nid > nobjs) {
      cerr << "Error! Object id is too big. " << nid << ":" << nobjs << endl;
      valid =false;
      continue;
    }
    status[nid] |= 0x04;
  }
  return valid;
}
