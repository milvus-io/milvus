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

#include	"NGT/Tree.h"
#include	"NGT/Node.h"

#include	<vector>

using namespace std;
using namespace NGT;

void
DVPTree::insert(InsertContainer &iobj) {
  SearchContainer q(iobj.object);
  q.mode = SearchContainer::SearchLeaf;
  q.vptree = this;
  q.radius = 0.0;

  search(q);

  iobj.vptree = this;

  assert(q.nodeID.getType() == Node::ID::Leaf);
  LeafNode *ln = (LeafNode*)getNode(q.nodeID);
  insert(iobj, ln);

  return;
}

void
DVPTree::insert(InsertContainer &iobj,  LeafNode *leafNode) 
{
  LeafNode &leaf = *leafNode;
  size_t fsize = leaf.getObjectSize();
  if (fsize != 0) {
    NGT::ObjectSpace::Comparator &comparator = objectSpace->getComparator();
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    Distance d = comparator(iobj.object, leaf.getPivot(*objectSpace));
#else
    Distance d = comparator(iobj.object, leaf.getPivot());
#endif

#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    NGT::ObjectDistance *objects = leaf.getObjectIDs(leafNodes.allocator);
#else
    NGT::ObjectDistance *objects = leaf.getObjectIDs();
#endif

    for (size_t i = 0; i < fsize; i++) {
      if (objects[i].distance == d) {
	Distance idd = 0.0;
	ObjectID loid;
        try {
	  loid = objects[i].id;
	  idd = comparator(iobj.object, *getObjectRepository().get(loid));
        } catch (Exception &e) {
          stringstream msg;
          msg << "LeafNode::insert: Cannot find object which belongs to a leaf node. id="
              << objects[i].id << ":" << e.what() << endl;
          NGTThrowException(msg.str());
        }
        if (idd == 0.0) {
	  if (loid == iobj.id) {
	    stringstream msg;
	    msg << "DVPTree::insert:already existed. " << iobj.id;
	    NGTThrowException(msg);
	  }
	  return;
        }
      }
    }
  }

  if (leaf.getObjectSize() >= leafObjectsSize) {
    split(iobj, leaf);
  } else {
    insertObject(iobj, leaf);
  }

  return;
}
Node::ID 
DVPTree::split(InsertContainer &iobj, LeafNode &leaf)
{
  Node::Objects *fs = getObjects(leaf, iobj);
  int pv = DVPTree::MaxVariance;
  switch (splitMode) {
  case DVPTree::MaxVariance:
    pv = LeafNode::selectPivotByMaxVariance(iobj, *fs);
    break;
  case DVPTree::MaxDistance:
    pv = LeafNode::selectPivotByMaxDistance(iobj, *fs);
    break;
  }

  LeafNode::splitObjects(iobj, *fs, pv);

  Node::ID nid = recombineNodes(iobj, *fs, leaf);
  delete fs;

  return nid;
}

Node::ID
DVPTree::recombineNodes(InsertContainer &ic, Node::Objects &fs, LeafNode &leaf)
{
  LeafNode *ln[internalChildrenSize];
  Node::ID targetParent = leaf.parent;
  Node::ID targetId = leaf.id;
  ln[0] = &leaf;
  ln[0]->objectSize = 0;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  for (size_t i = 1; i < internalChildrenSize; i++) {
    ln[i] = new(leafNodes.allocator) LeafNode(leafNodes.allocator);
  }
#else
  for (size_t i = 1; i < internalChildrenSize; i++) {
    ln[i] = new LeafNode;
  }
#endif
  InternalNode *in = createInternalNode();
  Node::ID inid = in->id;
  try {
    if (targetParent.getID() != 0) {
      InternalNode &pnode = *(InternalNode*)getNode(targetParent);
      for (size_t i = 0; i < internalChildrenSize; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	if (pnode.getChildren(internalNodes.allocator)[i] == targetId) {
	  pnode.getChildren(internalNodes.allocator)[i] = inid;
#else
	if (pnode.getChildren()[i] == targetId) {
	  pnode.getChildren()[i] = inid;
#endif
	  break;
	}
      }
    }
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    in->setPivot(*getObjectRepository().get(fs[0].id), *objectSpace, internalNodes.allocator);
#else
    in->setPivot(*getObjectRepository().get(fs[0].id), *objectSpace);
#endif

    in->parent = targetParent;

    int fsize = fs.size();
    int cid = fs[0].clusterID;
#ifdef NGT_NODE_USE_VECTOR
    LeafNode::ObjectIDs fid;
    fid.id = fs[0].id;
    fid.distance = 0.0;
    ln[cid]->objectIDs.push_back(fid);
#else
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    ln[cid]->getObjectIDs(leafNodes.allocator)[ln[cid]->objectSize].id = fs[0].id;
    ln[cid]->getObjectIDs(leafNodes.allocator)[ln[cid]->objectSize++].distance = 0.0;
#else
    ln[cid]->getObjectIDs()[ln[cid]->objectSize].id = fs[0].id;
    ln[cid]->getObjectIDs()[ln[cid]->objectSize++].distance = 0.0;
#endif
#endif
    if (fs[0].leafDistance == Node::Object::Pivot) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      ln[cid]->setPivot(*getObjectRepository().get(fs[0].id), *objectSpace, leafNodes.allocator);
#else
      ln[cid]->setPivot(*getObjectRepository().get(fs[0].id), *objectSpace);
#endif
    } else {
      NGTThrowException("recombineNodes: internal error : illegal pivot.");
    }
    ln[cid]->parent = inid;
    int maxClusterID = cid;
    for (int i = 1; i < fsize; i++) {
      int clusterID = fs[i].clusterID;
      if (clusterID > maxClusterID) {
	maxClusterID = clusterID;
      }
      Distance ld;
      if (fs[i].leafDistance == Node::Object::Pivot) {
        // pivot
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	ln[clusterID]->setPivot(*getObjectRepository().get(fs[i].id), *objectSpace, leafNodes.allocator);
#else
	ln[clusterID]->setPivot(*getObjectRepository().get(fs[i].id), *objectSpace);
#endif
        ld = 0.0;
      } else {
        ld = fs[i].leafDistance;
      }

#ifdef NGT_NODE_USE_VECTOR
      fid.id = fs[i].id;
      fid.distance = ld;
      ln[clusterID]->objectIDs.push_back(fid);
#else
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      ln[clusterID]->getObjectIDs(leafNodes.allocator)[ln[clusterID]->objectSize].id = fs[i].id;
      ln[clusterID]->getObjectIDs(leafNodes.allocator)[ln[clusterID]->objectSize++].distance = ld;
#else
      ln[clusterID]->getObjectIDs()[ln[clusterID]->objectSize].id = fs[i].id;
      ln[clusterID]->getObjectIDs()[ln[clusterID]->objectSize++].distance = ld;
#endif
#endif
      ln[clusterID]->parent = inid;
      if (clusterID != cid) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
        in->getBorders(internalNodes.allocator)[cid] = fs[i].distance;
#else
        in->getBorders()[cid] = fs[i].distance;
#endif
        cid = fs[i].clusterID;
      }
    }
    // When the number of the children is less than the expected,
    // proper values are set to the empty children.
    for (size_t i = maxClusterID + 1; i < internalChildrenSize; i++) {
      ln[i]->parent = inid;
      // dummy
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      ln[i]->setPivot(*getObjectRepository().get(fs[0].id), *objectSpace, leafNodes.allocator);
#else
      ln[i]->setPivot(*getObjectRepository().get(fs[0].id), *objectSpace);
#endif
      if (i < (internalChildrenSize - 1)) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	in->getBorders(internalNodes.allocator)[i] = FLT_MAX;
#else
	in->getBorders()[i] = FLT_MAX;
#endif
      }
    }

#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    in->getChildren(internalNodes.allocator)[0] = targetId;
#else
    in->getChildren()[0] = targetId;
#endif
    for (size_t i = 1; i < internalChildrenSize; i++) {
      insertNode(ln[i]);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      in->getChildren(internalNodes.allocator)[i] = ln[i]->id;
#else
      in->getChildren()[i] = ln[i]->id;
#endif
    }
  } catch(Exception &e) {
    throw e;
  }
  return inid;
}

void
DVPTree::insertObject(InsertContainer &ic, LeafNode &leaf) {
  if (leaf.getObjectSize() == 0) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    leaf.setPivot(*getObjectRepository().get(ic.id), *objectSpace, leafNodes.allocator);
#else
    leaf.setPivot(*getObjectRepository().get(ic.id), *objectSpace);
#endif
#ifdef NGT_NODE_USE_VECTOR
    LeafNode::ObjectIDs fid;
    fid.id = ic.id;
    fid.distance = 0;
    leaf.objectIDs.push_back(fid);
#else
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    leaf.getObjectIDs(leafNodes.allocator)[leaf.objectSize].id = ic.id;
    leaf.getObjectIDs(leafNodes.allocator)[leaf.objectSize++].distance = 0;
#else
    leaf.getObjectIDs()[leaf.objectSize].id = ic.id;
    leaf.getObjectIDs()[leaf.objectSize++].distance = 0;
#endif
#endif
  } else {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    Distance d = objectSpace->getComparator()(ic.object, leaf.getPivot(*objectSpace));
#else
    Distance d = objectSpace->getComparator()(ic.object, leaf.getPivot());
#endif

#ifdef NGT_NODE_USE_VECTOR
    LeafNode::ObjectIDs fid;
    fid.id = ic.id;
    fid.distance = d;
    leaf.objectIDs.push_back(fid);
    std::sort(leaf.objectIDs.begin(), leaf.objectIDs.end(), LeafNode::ObjectIDs());
#else
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    leaf.getObjectIDs(leafNodes.allocator)[leaf.objectSize].id = ic.id;
    leaf.getObjectIDs(leafNodes.allocator)[leaf.objectSize++].distance = d;
#else
    leaf.getObjectIDs()[leaf.objectSize].id = ic.id;
    leaf.getObjectIDs()[leaf.objectSize++].distance = d;
#endif
#endif
  }
}

Node::Objects *
DVPTree::getObjects(LeafNode &n, Container &iobj)
{
  int size = n.getObjectSize() + 1;

  Node::Objects *fs = new Node::Objects(size);
  for (size_t i = 0; i < n.getObjectSize(); i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    (*fs)[i].object = getObjectRepository().get(n.getObjectIDs(leafNodes.allocator)[i].id);
    (*fs)[i].id = n.getObjectIDs(leafNodes.allocator)[i].id;
#else
    (*fs)[i].object = getObjectRepository().get(n.getObjectIDs()[i].id);
    (*fs)[i].id = n.getObjectIDs()[i].id;
#endif
  }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  (*fs)[n.getObjectSize()].object = getObjectRepository().get(iobj.id);
#else
  (*fs)[n.getObjectSize()].object = &iobj.object;
#endif
  (*fs)[n.getObjectSize()].id = iobj.id;
  return fs;
}

void
DVPTree::removeEmptyNodes(InternalNode &inode) {

  int csize = internalChildrenSize;


  InternalNode *target = &inode;
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
  Node::ID *children = target->getChildren(internalNodes.allocator);
#else
  Node::ID *children = target->getChildren();
#endif
  for(;;) {
    for (int i = 0; i < csize; i++) {
      if (children[i].getType() == Node::ID::Internal) {
	return;
      }
      LeafNode &ln = *static_cast<LeafNode*>(getNode(children[i]));
      if (ln.getObjectSize() != 0) {
	return;
      }
    }

    for (int i = 0; i < csize; i++) {
      removeNode(children[i]);
    }
    if (target->parent.getID() == 0) {
      removeNode(target->id);
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
      LeafNode *root = new(leafNodes.allocator) LeafNode(leafNodes.allocator);
#else
      LeafNode *root = new LeafNode;
#endif
      insertNode(root);
      if (root->id.getID() != 1) {
	NGTThrowException("Root id Error");
      }
      return;
    }

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    LeafNode *ln = new(leafNodes.allocator) LeafNode(leafNodes.allocator);
#else
    LeafNode *ln = new LeafNode;
#endif
    ln->parent = target->parent;
    insertNode(ln);

    InternalNode &in = *(InternalNode*)getNode(ln->parent);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    in.updateChild(*this, target->id, ln->id, internalNodes.allocator);
#else
    in.updateChild(*this, target->id, ln->id);
#endif
    removeNode(target->id);
    target = &in;
  }

  return;
}


void
DVPTree::search(SearchContainer &sc, InternalNode &node, UncheckedNode &uncheckedNode)
{
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
  Distance d = objectSpace->getComparator()(sc.object, node.getPivot(*objectSpace));
#else
  Distance d = objectSpace->getComparator()(sc.object, node.getPivot());
#endif
#ifdef NGT_DISTANCE_COMPUTATION_COUNT
  sc.distanceComputationCount++;
#endif

  int bsize = internalChildrenSize - 1;

  vector<ObjectDistance> regions;
  regions.reserve(internalChildrenSize);

  ObjectDistance child;
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
  Distance *borders = node.getBorders(internalNodes.allocator);
#else
  Distance *borders = node.getBorders();
#endif
  int mid;
  for (mid = 0; mid < bsize; mid++) {
    if (d < borders[mid]) {
        child.id = mid;
        child.distance = 0.0;
        regions.push_back(child);
      if (d + sc.radius < borders[mid]) {
        break;
      } else {
        continue;
      }
    } else {
      if (d < borders[mid] + sc.radius) {
        child.id = mid;
        child.distance = d - borders[mid];
        regions.push_back(child);
        continue;
      } else {
        continue;
      }
    }
  }

  if (mid == bsize) {
    if (d >= borders[mid - 1]) {
      child.id = mid;
      child.distance = 0.0;
      regions.push_back(child);
    } else {
      child.id = mid;
      child.distance = borders[mid - 1] - d;
      regions.push_back(child);
    }
  }

  sort(regions.begin(), regions.end());

#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
  Node::ID *children = node.getChildren(internalNodes.allocator);
#else
  Node::ID *children = node.getChildren();
#endif

  vector<ObjectDistance>::iterator i;
  if (sc.mode == DVPTree::SearchContainer::SearchLeaf) {
    if (children[regions.front().id].getType() == Node::ID::Leaf) {
      sc.nodeID.setRaw(children[regions.front().id].get());
      assert(uncheckedNode.empty());
    } else {
      uncheckedNode.push(children[regions.front().id]);
    }
  } else {
    for (i = regions.begin(); i != regions.end(); i++) {
      uncheckedNode.push(children[i->id]);
    }
  }
  
}

void
DVPTree::search(SearchContainer &so, LeafNode &node, UncheckedNode &uncheckedNode)
{
  DVPTree::SearchContainer &q = (DVPTree::SearchContainer&)so;

  if (node.getObjectSize() == 0) {
    return;
  }
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
  Distance pq = objectSpace->getComparator()(q.object, node.getPivot(*objectSpace));
#else
  Distance pq = objectSpace->getComparator()(q.object, node.getPivot());
#endif
#ifdef NGT_DISTANCE_COMPUTATION_COUNT
  so.distanceComputationCount++;
#endif

  ObjectDistance r;
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
  NGT::ObjectDistance *objects = node.getObjectIDs(leafNodes.allocator);
#else
  NGT::ObjectDistance *objects = node.getObjectIDs();
#endif

  for (size_t i = 0; i < node.getObjectSize(); i++) {
    if ((objects[i].distance <= pq + q.radius) &&
        (objects[i].distance >= pq - q.radius)) {
      Distance d = 0;
      try {
	d = objectSpace->getComparator()(q.object, *q.vptree->getObjectRepository().get(objects[i].id));
#ifdef NGT_DISTANCE_COMPUTATION_COUNT
	so.distanceComputationCount++;
#endif
      } catch(...) {
        NGTThrowException("VpTree::LeafNode::search: Internal fatal error : Cannot get object");
      }
      if (d <= q.radius) {
        r.id = objects[i].id;
        r.distance = d;
	so.getResult().push_back(r);
	std::sort(so.getResult().begin(), so.getResult().end());
	if (so.getResult().size() > q.size) {
	  so.getResult().resize(q.size);
	}
      }
    }
  }
}

void 
DVPTree::search(SearchContainer &sc) {
  ((SearchContainer&)sc).vptree = this;
  Node *root = getRootNode();
  assert(root != 0);
  if (sc.mode == DVPTree::SearchContainer::SearchLeaf) {
    if (root->id.getType() == Node::ID::Leaf) {
      sc.nodeID.setRaw(root->id.get());
      return;
    }
  }

  UncheckedNode uncheckedNode;
  uncheckedNode.push(root->id);

  while (!uncheckedNode.empty()) {
    Node::ID nodeid = uncheckedNode.top();
    uncheckedNode.pop();
    Node *cnode = getNode(nodeid);
    if (cnode == 0) {
      cerr << "Error! child node is null. but continue." << endl;
      continue;
    }
    if (cnode->id.getType() == Node::ID::Internal) {
      search(sc, (InternalNode&)*cnode, uncheckedNode);
    } else if (cnode->id.getType() == Node::ID::Leaf) {
      search(sc, (LeafNode&)*cnode, uncheckedNode);
    } else {
      cerr << "Tree: Inner fatal error!: Node type error!" << endl;
      abort();
    }
  }
}

