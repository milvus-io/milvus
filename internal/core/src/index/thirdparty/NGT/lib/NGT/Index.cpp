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

#include <sstream>
#include	"NGT/defines.h"
#include	"NGT/Common.h"
#include	"NGT/ObjectSpaceRepository.h"
#include	"NGT/Index.h"
#include	"NGT/Thread.h"
#include	"NGT/GraphReconstructor.h"
#include	"NGT/Version.h"

using namespace std;
using namespace NGT;


void 
Index::version(ostream &os) 
{
  os << "libngt:" << endl;
  Version::get(os);
}

string
Index::getVersion()
{
  return Version::getVersion();
}

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
NGT::Index::Index(NGT::Property &prop, const string &database) {
  if (prop.dimension == 0) {
    NGTThrowException("Index::Index. Dimension is not specified.");
  }
  Index* idx = 0;
  mkdir(database);
  if (prop.indexType == NGT::Index::Property::GraphAndTree) {
    idx = new NGT::GraphAndTreeIndex(database, prop);
  } else if (prop.indexType == NGT::Index::Property::Graph) {
    idx = new NGT::GraphIndex(database, prop);
  } else {
    NGTThrowException("Index::Index: Not found IndexType in property file.");
  }
  if (idx == 0) {
    stringstream msg;
    msg << "Index::Index: Cannot construct. ";
    NGTThrowException(msg);
  }
  index = idx;
  path = "";
}
#else
NGT::Index::Index(NGT::Property &prop) {
  if (prop.dimension == 0) {
    NGTThrowException("Index::Index. Dimension is not specified.");
  }
  Index* idx = 0;
  if (prop.indexType == NGT::Index::Property::GraphAndTree) {
    idx = new NGT::GraphAndTreeIndex(prop);
  } else if (prop.indexType == NGT::Index::Property::Graph) {
    idx = new NGT::GraphIndex(prop);
  } else {
    NGTThrowException("Index::Index: Not found IndexType in property file.");
  }
  if (idx == 0) {
    stringstream msg;
    msg << "Index::Index: Cannot construct. ";
    NGTThrowException(msg);
  }
  index = idx;
  path = "";
}
#endif

float 
NGT::Index::getEpsilonFromExpectedAccuracy(double accuracy) { 
   return static_cast<NGT::GraphIndex&>(getIndex()).getEpsilonFromExpectedAccuracy(accuracy);
 }

void 
NGT::Index::open(const string &database, bool rdOnly) {
  NGT::Property prop;
  prop.load(database);
  Index* idx = 0;
  if (prop.indexType == NGT::Index::Property::GraphAndTree) {
    idx = new NGT::GraphAndTreeIndex(database, rdOnly);
  } else if (prop.indexType == NGT::Index::Property::Graph) {
    idx = new NGT::GraphIndex(database, rdOnly);
  } else {
    NGTThrowException("Index::Open: Not found IndexType in property file.");
  }
  if (idx == 0) {
    stringstream msg;
    msg << "Index::open: Cannot open. " << database;
    NGTThrowException(msg);
  }
  index = idx;
  path = database;
}

// for milvus
NGT::Index * NGT::Index::loadIndex(std::stringstream & obj, std::stringstream & grp, std::stringstream & prf, std::stringstream & tre)
{
    NGT::Property prop;
    prop.load(prf);
    if (prop.databaseType != NGT::Index::Property::DatabaseType::Memory)
    {
        NGTThrowException("GraphIndex: Cannot open. Not memory type.");
    }
    assert(prop.dimension != 0);
    NGT::Index * idx = new NGT::Index();
    if (prop.indexType == NGT::Index::Property::GraphAndTree)
    {
        auto iidx = new NGT::GraphAndTreeIndex(prop);
        idx->index = iidx;
    }
    else if (prop.indexType == NGT::Index::Property::Graph)
    {
        auto iidx = new NGT::GraphIndex(prop);
        idx->index = iidx;
    }
    else
    {
        NGTThrowException("Index::Open: Not found IndexType in property file.");
    }
    idx->index->loadIndexFromStream(obj, grp, tre);
    return idx;
}

//For milvus
NGT::Index * NGT::Index::createGraphAndTree(const float * row_data, NGT::Property & prop, size_t dataSize)
{
    //TODO
    if (prop.dimension == 0)
    {
        NGTThrowException("Index::createGraphAndTree. Dimension is not specified.");
    }
    NGT::Index * res = new NGT::Index();
    prop.indexType = NGT::Index::Property::IndexType::GraphAndTree;
    NGT::Index * idx = new NGT::GraphAndTreeIndex(prop);
    assert(idx != 0);
    try
    {
        loadRawDataAndCreateIndex(idx, row_data, prop.threadPoolSize, dataSize);
        res->index = idx;
        return res;
    }
    catch (Exception & err)
    {
        delete idx;
        delete res;
        throw err;
    }
}

void 
NGT::Index::createGraphAndTree(const string &database, NGT::Property &prop, const string &dataFile,
			       size_t dataSize, bool redirect) {
  if (prop.dimension == 0) {
    NGTThrowException("Index::createGraphAndTree. Dimension is not specified.");
  }
  prop.indexType = NGT::Index::Property::IndexType::GraphAndTree;
  Index *idx = 0;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  mkdir(database);
  idx = new NGT::GraphAndTreeIndex(database, prop);
#else
  idx = new NGT::GraphAndTreeIndex(prop);
#endif
  assert(idx != 0);
  StdOstreamRedirector redirector(redirect);
  redirector.begin();
  try {
    loadAndCreateIndex(*idx, database, dataFile, prop.threadPoolSize, dataSize);
  } catch(Exception &err) {
    delete idx;
    redirector.end();
    throw err;
  }
  delete idx;
  redirector.end();
}

// For milvus
NGT::Index * NGT::Index::createGraph(const float * row_data, NGT::Property & prop, size_t dataSize)
{
    //TODO
    if (prop.dimension == 0)
    {
        NGTThrowException("Index::createGraphAndTree. Dimension is not specified.");
    }
    prop.indexType = NGT::Index::Property::IndexType::Graph;
    NGT::Index * res = new NGT::Index();
    NGT::Index * idx = new NGT::GraphAndTreeIndex(prop);
    assert(idx != 0);
    try
    {
        loadRawDataAndCreateIndex(idx, row_data, prop.threadPoolSize, dataSize);
        res->index = idx;
        return res;
    }
    catch (Exception & err)
    {
        delete idx;
        delete res;
        throw err;
    }
}

void 
NGT::Index::createGraph(const string &database, NGT::Property &prop, const string &dataFile, size_t dataSize, bool redirect) {
  if (prop.dimension == 0) {
    NGTThrowException("Index::createGraphAndTree. Dimension is not specified.");
  }
  prop.indexType = NGT::Index::Property::IndexType::Graph;
  Index *idx = 0;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  mkdir(database);
  idx = new NGT::GraphIndex(database, prop);
#else
  idx = new NGT::GraphIndex(prop);
#endif
  assert(idx != 0);
  StdOstreamRedirector redirector(redirect);
  redirector.begin();
  try {
    loadAndCreateIndex(*idx, database, dataFile, prop.threadPoolSize, dataSize);
  } catch(Exception &err) {
    delete idx;
    redirector.end();
    throw err;
  }
  delete idx;
  redirector.end();
}

// For milvus
void NGT::Index::loadRawDataAndCreateIndex(NGT::Index * index_, const float * row_data, size_t threadSize, size_t dataSize)
{
    if (dataSize)
    {
        index_->loadRawData(row_data, dataSize);
    }
    else
    {
        return;
    }
    if (index_->getObjectRepositorySize() == 0)
    {
        NGTThrowException("Index::create: Data file is empty.");
    }
    NGT::Timer timer;
    timer.start();
    index_->createIndex(threadSize);
    timer.stop();
    if (NGT_LOG_DEBUG_)
        (*NGT_LOG_DEBUG_)("Index creation time=" + std::to_string(timer.time) + " (sec) " + std::to_string(timer.time * 1000.0) + " (msec)");
//    cerr << "Index creation time=" << timer.time << " (sec) " << timer.time * 1000.0 << " (msec)" << endl;
}

void 
NGT::Index::loadAndCreateIndex(Index &index, const string &database, const string &dataFile, size_t threadSize, size_t dataSize) {
  NGT::Timer timer;
  timer.start();
  if (dataFile.size() != 0) {
    index.load(dataFile, dataSize);
  } else {
    index.saveIndex(database);
    return;
  }
  timer.stop();
    if (NGT_LOG_DEBUG_)
        (*NGT_LOG_DEBUG_)("Data loading time=" + std::to_string(timer.time) + " (sec) " + std::to_string(timer.time * 1000.0) + " (msec)");
//  cerr << "Data loading time=" << timer.time << " (sec) " << timer.time * 1000.0 << " (msec)" << endl;
  if (index.getObjectRepositorySize() == 0) {
    NGTThrowException("Index::create: Data file is empty.");
  }
    if (NGT_LOG_DEBUG_)
        (*NGT_LOG_DEBUG_)("# of objects=" + std::to_string(index.getObjectRepositorySize() - 1));
//  cerr << "# of objects=" << index.getObjectRepositorySize() - 1 << endl;
  timer.reset();
  timer.start();
  index.createIndex(threadSize);
  timer.stop();
  index.saveIndex(database);
//  cerr << "Index creation time=" << timer.time << " (sec) " << timer.time * 1000.0 << " (msec)" << endl;
    if (NGT_LOG_DEBUG_)
        (*NGT_LOG_DEBUG_)("Index creation time=" + std::to_string(timer.time) + " (sec) " + std::to_string(timer.time * 1000.0) + " (msec)");
}

// For milvus
void NGT::Index::append(NGT::Index * index_, const float * data, size_t dataSize, size_t threadSize)
{
    NGT::Timer timer;
    timer.start();
    if (data != 0 && dataSize != 0)
    {
        index_->append(data, dataSize);
    }
    else
    {
        NGTThrowException("Index::append: No data.");
    }
    timer.stop();
//    cerr << "Data loading time=" << timer.time << " (sec) " << timer.time * 1000.0 << " (msec)" << endl;
//    cerr << "# of objects=" << index_->getObjectRepositorySize() - 1 << endl;
    if (NGT_LOG_DEBUG_) {
        (*NGT_LOG_DEBUG_)(
            "Data loading time=" + std::to_string(timer.time) + " (sec) " + std::to_string(timer.time * 1000.0)
            + " (msec)");
        (*NGT_LOG_DEBUG_)("# of objects=" + std::to_string(index_->getObjectRepositorySize() - 1));
    }
    timer.reset();
    timer.start();
    index_->createIndex(threadSize);
    timer.stop();
//    cerr << "Index creation time=" << timer.time << " (sec) " << timer.time * 1000.0 << " (msec)" << endl;
    if (NGT_LOG_DEBUG_)
        (*NGT_LOG_DEBUG_)("Index creation time=" + std::to_string(timer.time) + " (sec) " + std::to_string(timer.time * 1000.0) + " (msec)");
    return;
}

void 
NGT::Index::append(const string &database, const string &dataFile, size_t threadSize, size_t dataSize) {
  NGT::Index	index(database);
  NGT::Timer	timer;
  timer.start();
  if (dataFile.size() != 0) {
    index.append(dataFile, dataSize);
  }
  timer.stop();
  if (NGT_LOG_DEBUG_) {
      (*NGT_LOG_DEBUG_)("Data loading time=" + std::to_string(timer.time) + " (sec) " + std::to_string(timer.time * 1000.0) + " (msec)");
      (*NGT_LOG_DEBUG_)("# of objects=" + std::to_string(index.getObjectRepositorySize() - 1));
  }
//  cerr << "Data loading time=" << timer.time << " (sec) " << timer.time * 1000.0 << " (msec)" << endl;
//  cerr << "# of objects=" << index.getObjectRepositorySize() - 1 << endl;
  timer.reset();
  timer.start();
  index.createIndex(threadSize);
  timer.stop();
  index.saveIndex(database);
//  cerr << "Index creation time=" << timer.time << " (sec) " << timer.time * 1000.0 << " (msec)" << endl;
    if (NGT_LOG_DEBUG_)
        (*NGT_LOG_DEBUG_)("Index creation time=" + std::to_string(timer.time) + " (sec) " + std::to_string(timer.time * 1000.0) + " (msec)");
  return;
}

void 
NGT::Index::append(const string &database, const float *data, size_t dataSize, size_t threadSize) {
  NGT::Index	index(database);
  NGT::Timer	timer;
  timer.start();
  if (data != 0 && dataSize != 0) {
    index.append(data, dataSize);
  } else {
    NGTThrowException("Index::append: No data.");
  }
  timer.stop();
//  cerr << "Data loading time=" << timer.time << " (sec) " << timer.time * 1000.0 << " (msec)" << endl;
//  cerr << "# of objects=" << index.getObjectRepositorySize() - 1 << endl;
    if (NGT_LOG_DEBUG_) {
        (*NGT_LOG_DEBUG_)("Data loading time=" + std::to_string(timer.time) + " (sec) " + std::to_string(timer.time * 1000.0) + " (msec)");
        (*NGT_LOG_DEBUG_)("# of objects=" + std::to_string(index.getObjectRepositorySize() - 1));
    }
  timer.reset();
  timer.start();
  index.createIndex(threadSize);
  timer.stop();
  index.saveIndex(database);
//  cerr << "Index creation time=" << timer.time << " (sec) " << timer.time * 1000.0 << " (msec)" << endl;
    if (NGT_LOG_DEBUG_)
        (*NGT_LOG_DEBUG_)("Index creation time=" + std::to_string(timer.time) + " (sec) " + std::to_string(timer.time * 1000.0) + " (msec)");
  return;
}

void 
NGT::Index::remove(const string &database, vector<ObjectID> &objects, bool force) {
  NGT::Index	index(database);
  NGT::Timer	timer;
  timer.start();
  for (vector<ObjectID>::iterator i = objects.begin(); i != objects.end(); i++) {
    try {
      index.remove(*i, force);
    } catch (Exception &err) {
//      cerr << "Warning: Cannot remove the node. ID=" << *i << " : " << err.what() << endl;
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("Warning: Cannot remove the node. ID=" + std::to_string(*i) + " : " + err.what());
      continue;
    }
  }
  timer.stop();
//  cerr << "Data removing time=" << timer.time << " (sec) " << timer.time * 1000.0 << " (msec)" << endl;
//  cerr << "# of objects=" << index.getObjectRepositorySize() - 1 << endl;
    if (NGT_LOG_DEBUG_) {
        (*NGT_LOG_DEBUG_)("Data removing time=" + std::to_string(timer.time) + " (sec) " + std::to_string(timer.time * 1000.0) + " (msec)");
        (*NGT_LOG_DEBUG_)("# of objects=" + std::to_string(index.getObjectRepositorySize() - 1));
    }
  index.saveIndex(database);
  return;
}

void 
NGT::Index::importIndex(const string &database, const string &file) {
  Index *idx = 0;
  NGT::Property property;
  property.importProperty(file);
  NGT::Timer	timer;
  timer.start();
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  property.databaseType = NGT::Index::Property::DatabaseType::MemoryMappedFile;
  mkdir(database);
#else
  property.databaseType = NGT::Index::Property::DatabaseType::Memory;
#endif
  if (property.indexType == NGT::Index::Property::IndexType::GraphAndTree) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    idx = new NGT::GraphAndTreeIndex(database, property);
#else
    idx = new NGT::GraphAndTreeIndex(property);
#endif
    assert(idx != 0);
  } else if (property.indexType == NGT::Index::Property::IndexType::Graph) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    idx = new NGT::GraphIndex(database, property);
#else
    idx = new NGT::GraphIndex(property);
#endif
    assert(idx != 0);
  } else {
    NGTThrowException("Index::Open: Not found IndexType in property file.");
  }
  idx->importIndex(file);
  timer.stop();
//  cerr << "Data importing time=" << timer.time << " (sec) " << timer.time * 1000.0 << " (msec)" << endl;
//  cerr << "# of objects=" << idx->getObjectRepositorySize() - 1 << endl;
    if (NGT_LOG_DEBUG_) {
        (*NGT_LOG_DEBUG_)("Data importing time=" + std::to_string(timer.time) + " (sec) " + std::to_string(timer.time * 1000.0) + " (msec)");
        (*NGT_LOG_DEBUG_)("# of objects=" + std::to_string(idx->getObjectRepositorySize() - 1));
    }
  idx->saveIndex(database);
  delete idx;
}

void 
NGT::Index::exportIndex(const string &database, const string &file) {
  NGT::Index	idx(database);
  NGT::Timer	timer;
  timer.start();
  idx.exportIndex(file);
  timer.stop();
//  cerr << "Data exporting time=" << timer.time << " (sec) " << timer.time * 1000.0 << " (msec)" << endl;
//  cerr << "# of objects=" << idx.getObjectRepositorySize() - 1 << endl;
    if (NGT_LOG_DEBUG_) {
        (*NGT_LOG_DEBUG_)("Data exporting time=" + std::to_string(timer.time) + " (sec) " + std::to_string(timer.time * 1000.0) + " (msec)");
        (*NGT_LOG_DEBUG_)("# of objects=" + std::to_string(idx.getObjectRepositorySize() - 1));
    }
}

std::vector<float>
NGT::Index::makeSparseObject(std::vector<uint32_t> &object)
{
  if (static_cast<NGT::GraphIndex&>(getIndex()).getProperty().distanceType != NGT::ObjectSpace::DistanceType::DistanceTypeSparseJaccard) {
    NGTThrowException("NGT::Index::makeSparseObject: Not sparse jaccard.");
  }
  size_t dimension = getObjectSpace().getDimension();
  if (object.size() + 1 > dimension) {
    std::stringstream msg;
    dimension = object.size() + 1;
  }
  std::vector<float> obj(dimension, 0.0);
  for (size_t i = 0; i < object.size(); i++) {
    float fv = *reinterpret_cast<float*>(&object[i]);
    obj[i] = fv;
  }
  return obj;
}

void 
NGT::Index::Property::set(NGT::Property &prop) {
  if (prop.dimension != -1) dimension = prop.dimension;
  if (prop.threadPoolSize != -1) threadPoolSize = prop.threadPoolSize;
  if (prop.objectType != ObjectSpace::ObjectTypeNone) objectType = prop.objectType;
  if (prop.distanceType != DistanceType::DistanceTypeNone) distanceType = prop.distanceType;
  if (prop.indexType != IndexTypeNone) indexType = prop.indexType;
  if (prop.databaseType != DatabaseTypeNone) databaseType = prop.databaseType;
  if (prop.objectAlignment != ObjectAlignmentNone) objectAlignment = prop.objectAlignment;
  if (prop.pathAdjustmentInterval != -1) pathAdjustmentInterval = prop.pathAdjustmentInterval;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  if (prop.graphSharedMemorySize != -1) graphSharedMemorySize = prop.graphSharedMemorySize;
  if (prop.treeSharedMemorySize != -1) treeSharedMemorySize = prop.treeSharedMemorySize;
  if (prop.objectSharedMemorySize != -1) objectSharedMemorySize = prop.objectSharedMemorySize;
#endif
  if (prop.prefetchOffset != -1) prefetchOffset = prop.prefetchOffset;
  if (prop.prefetchSize != -1) prefetchSize = prop.prefetchSize;
  if (prop.accuracyTable != "") accuracyTable = prop.accuracyTable;
}

void 
NGT::Index::Property::get(NGT::Property &prop) {
  prop.dimension = dimension;
  prop.threadPoolSize = threadPoolSize;
  prop.objectType = objectType;
  prop.distanceType = distanceType;
  prop.indexType = indexType;
  prop.databaseType = databaseType;
  prop.pathAdjustmentInterval = pathAdjustmentInterval;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  prop.graphSharedMemorySize = graphSharedMemorySize;
  prop.treeSharedMemorySize = treeSharedMemorySize;
  prop.objectSharedMemorySize = objectSharedMemorySize;
#endif
  prop.prefetchOffset = prefetchOffset;
  prop.prefetchSize = prefetchSize;
  prop.accuracyTable = accuracyTable;
}

class CreateIndexJob {
public:
  CreateIndexJob() {}
  CreateIndexJob &operator=(const CreateIndexJob &d) {
    id = d.id;
    results = d.results;
    object = d.object;
    batchIdx = d.batchIdx;
    return *this;
  }
  friend bool operator<(const CreateIndexJob &ja, const CreateIndexJob &jb) { return ja.batchIdx < jb.batchIdx; }
  NGT::ObjectID		id;
  NGT::Object		*object;	// this will be a node of the graph later.
  NGT::ObjectDistances	*results;
  size_t		batchIdx;
};

class CreateIndexSharedData {
public:
  CreateIndexSharedData(NGT::GraphIndex &nngt) : graphIndex(nngt) {}
  NGT::GraphIndex &graphIndex;
};

class CreateIndexThread : public NGT::Thread {
public:
  CreateIndexThread() {}
  virtual ~CreateIndexThread() {}
  virtual int run();

};

 typedef NGT::ThreadPool<CreateIndexJob, CreateIndexSharedData*, CreateIndexThread> CreateIndexThreadPool;

int
CreateIndexThread::run() {

  NGT::ThreadPool<CreateIndexJob, CreateIndexSharedData*, CreateIndexThread>::Thread &poolThread =
    (NGT::ThreadPool<CreateIndexJob, CreateIndexSharedData*, CreateIndexThread>::Thread&)*this;

  CreateIndexSharedData &sd = *poolThread.getSharedData();
  NGT::GraphIndex &graphIndex = sd.graphIndex;

  for(;;) {
    CreateIndexJob job;
    try {
      poolThread.getInputJobQueue().popFront(job);
    } catch(NGT::ThreadTerminationException &err) {
      break;
    } catch(NGT::Exception &err) {
//      cerr << "CreateIndex::search:Error! popFront " << err.what() << endl;
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("CreateIndex::search:Error! popFront " + std::string(err.what()));
      break;
    }
    ObjectDistances *rs = new ObjectDistances;
    Object &obj = *job.object;
    try {
      if (graphIndex.NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeKNNG) {
	graphIndex.searchForKNNGInsertion(obj, job.id, *rs);	// linear search
      } else {
	graphIndex.searchForNNGInsertion(obj, *rs);
      }
    } catch(NGT::Exception &err) {
//      cerr << "CreateIndex::search:Fatal error! ID=" << job.id << " " << err.what() << endl;
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("CreateIndex::search:Fatal error! ID=" + std::to_string(job.id) + " " + err.what());
      abort();
    } 
    job.results = rs;
    poolThread.getOutputJobQueue().pushBack(job);
  }

  return 0;

}

class BuildTimeController {
public:
  BuildTimeController(GraphIndex &graph, NeighborhoodGraph::Property &prop):property(prop) {
    noOfInsertedObjects = graph.objectSpace->getRepository().size() - graph.repository.size();
    interval = 10000;
    count = interval;
    edgeSizeSave = property.edgeSizeForCreation;
    insertionRadiusCoefficientSave = property.insertionRadiusCoefficient;
    buildTimeLimit = property.buildTimeLimit;
    time = 0.0;
    timer.start();
  }
  ~BuildTimeController() {
    property.edgeSizeForCreation = edgeSizeSave;
    property.insertionRadiusCoefficient = insertionRadiusCoefficientSave;
  }
  void adjustEdgeSize(size_t c) {
    if (buildTimeLimit > 0.0 && count <= c) {
      timer.stop();
      double estimatedTime = time + timer.time / interval * (noOfInsertedObjects - count);
      estimatedTime /= 60 * 60;	// hour
      const size_t edgeInterval = 5;
      const int minimumEdge = 5;
      const float radiusInterval = 0.02;
      if (estimatedTime > buildTimeLimit) {
	if (property.insertionRadiusCoefficient - radiusInterval >= 1.0) {
	  property.insertionRadiusCoefficient -= radiusInterval;
	} else {
	  property.edgeSizeForCreation -= edgeInterval;
	  if (property.edgeSizeForCreation < minimumEdge) {
	    property.edgeSizeForCreation = minimumEdge;
	  }
	}
      }
      time += timer.time;
      count += interval;
      timer.start();
    }
  }

  size_t	noOfInsertedObjects;
  size_t	interval;
  size_t	count ;
  size_t	edgeSizeSave;
  double	insertionRadiusCoefficientSave;
  Timer		timer;
  double	time;
  double	buildTimeLimit;
  NeighborhoodGraph::Property &property;
};

void 
NGT::GraphIndex::constructObjectSpace(NGT::Property &prop) {
  assert(prop.dimension != 0);
  size_t dimension = prop.dimension;
  if (prop.distanceType == NGT::ObjectSpace::DistanceType::DistanceTypeSparseJaccard) {
    dimension++;
  }

  switch (prop.objectType) {
  case NGT::ObjectSpace::ObjectType::Float :
    objectSpace = new ObjectSpaceRepository<float, double>(dimension, typeid(float), prop.distanceType);
    break;
  case NGT::ObjectSpace::ObjectType::Uint8 :
    objectSpace = new ObjectSpaceRepository<unsigned char, int>(dimension, typeid(uint8_t), prop.distanceType);
    break;
  default:
    stringstream msg;
    msg << "Invalid Object Type in the property. " << prop.objectType;
    NGTThrowException(msg);	
  }
}

void 
NGT::GraphIndex::loadIndex(const string &ifile, bool readOnly) {
  objectSpace->deserialize(ifile + "/obj");
#ifdef NGT_GRAPH_READ_ONLY_GRAPH
  if (readOnly && property.indexType == NGT::Index::Property::IndexType::Graph) {
    GraphIndex::NeighborhoodGraph::loadSearchGraph(ifile);
  } else {
    ifstream isg(ifile + "/grp");
    repository.deserialize(isg);
  }
#else
  ifstream isg(ifile + "/grp");
  repository.deserialize(isg);
#endif
}

// for milvus
void NGT::GraphIndex::saveProperty(std::stringstream & prf) { NGT::Property::save(*this, prf); }

void 
NGT::GraphIndex::saveProperty(const std::string &file) {
  NGT::Property::save(*this, file);
}

void 
NGT::GraphIndex::exportProperty(const std::string &file) {
  NGT::Property::exportProperty(*this, file);
}

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
NGT::GraphIndex::GraphIndex(const string &allocator, bool rdonly):readOnly(rdonly) {
  NGT::Property prop;
  prop.load(allocator);
  if (prop.databaseType != NGT::Index::Property::DatabaseType::MemoryMappedFile) {
    NGTThrowException("GraphIndex: Cannot open. Not memory mapped file type.");
  }
  initialize(allocator, prop);
#ifdef NGT_GRAPH_READ_ONLY_GRAPH
  searchUnupdatableGraph = NeighborhoodGraph::Search::getMethod(prop.distanceType, prop.objectType,
								objectSpace->getRepository().size());
#endif
}

NGT::GraphAndTreeIndex::GraphAndTreeIndex(const string &allocator, NGT::Property &prop):GraphIndex(allocator, prop) {
  initialize(allocator, prop.treeSharedMemorySize);
}

void 
GraphAndTreeIndex::createTreeIndex() 
{
  ObjectRepository &fr = GraphIndex::objectSpace->getRepository();
  for (size_t id = 0; id < fr.size(); id++){
    if (id % 100000 == 0) {
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)(" Processed id=" + std::to_string(id));
//      cerr << " Processed id=" << id << endl;
    }
    if (fr.isEmpty(id)) {
      continue;
    }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    Object *f = GraphIndex::objectSpace->allocateObject(*fr[id]);
    DVPTree::InsertContainer tiobj(*f, id);
#else
    DVPTree::InsertContainer tiobj(*fr[id], id);
#endif
    try {
      DVPTree::insert(tiobj);
    } catch (Exception &err) {
//      cerr << "GraphAndTreeIndex::createTreeIndex: Warning. ID=" << id << ":";
//      cerr << err.what() << " continue.." << endl;
        if (NGT_LOG_DEBUG_) {
            (*NGT_LOG_DEBUG_)("GraphAndTreeIndex::createTreeIndex: Warning. ID=" + std::to_string(id) + ":");
            (*NGT_LOG_DEBUG_)(std::string(err.what()) + " continue..");
        }
    }
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    GraphIndex::objectSpace->deleteObject(f);
#endif
  }
}

void 
NGT::GraphIndex::initialize(const string &allocator, NGT::Property &prop) {
  constructObjectSpace(prop);
  repository.open(allocator + "/grp", prop.graphSharedMemorySize);
  objectSpace->open(allocator + "/obj", prop.objectSharedMemorySize);
  setProperty(prop);
}
#else // NGT_SHARED_MEMORY_ALLOCATOR
NGT::GraphIndex::GraphIndex(const string &database, bool rdOnly):readOnly(rdOnly) {
  NGT::Property prop;
  prop.load(database);
  if (prop.databaseType != NGT::Index::Property::DatabaseType::Memory) {
    NGTThrowException("GraphIndex: Cannot open. Not memory type.");
  }
  assert(prop.dimension != 0);
  initialize(prop);
  loadIndex(database, readOnly);
#ifdef NGT_GRAPH_READ_ONLY_GRAPH
  if (prop.searchType == "Large") {
    searchUnupdatableGraph = NeighborhoodGraph::Search::getMethod(prop.distanceType, prop.objectType, 10000000);
  } else if (prop.searchType == "Small") {
    searchUnupdatableGraph = NeighborhoodGraph::Search::getMethod(prop.distanceType, prop.objectType, 0);
  } else {
    searchUnupdatableGraph = NeighborhoodGraph::Search::getMethod(prop.distanceType, prop.objectType,
                                                                  objectSpace->getRepository().size());
  }
#endif
}
#endif

void
GraphIndex::createIndex()
{
  GraphRepository &anngRepo = repository;
  ObjectRepository &fr = objectSpace->getRepository();
  size_t	pathAdjustCount = property.pathAdjustmentInterval;
  NGT::ObjectID id = 1;
  size_t count = 0;
  BuildTimeController buildTimeController(*this, NeighborhoodGraph::property);
  for (; id < fr.size(); id++) {
    if (id < anngRepo.size() && anngRepo[id] != 0) {
      continue;
    }
    insert(id);
    buildTimeController.adjustEdgeSize(++count);
    if (pathAdjustCount > 0 && pathAdjustCount <= id) {
      GraphReconstructor::adjustPathsEffectively(static_cast<GraphIndex&>(*this));
      pathAdjustCount += property.pathAdjustmentInterval;
    }
  }
}

static size_t
searchMultipleQueryForCreation(GraphIndex &neighborhoodGraph, 
			       NGT::ObjectID &id, 
			       CreateIndexJob &job, 
			       CreateIndexThreadPool &threads,
			       size_t sizeOfRepository)
{
  ObjectRepository &repo = neighborhoodGraph.objectSpace->getRepository();
  GraphRepository &anngRepo = neighborhoodGraph.repository;
  size_t cnt = 0;
  for (; id < repo.size(); id++) {
    if (sizeOfRepository > 0 && id >= sizeOfRepository) {
      break;
    }
    if (repo[id] == 0) {
      continue;
    }
    if (neighborhoodGraph.NeighborhoodGraph::property.graphType != NeighborhoodGraph::GraphTypeBKNNG) {
      if (id < anngRepo.size() && anngRepo[id] != 0) {
	continue;
      }
    }
    job.id = id;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    job.object = neighborhoodGraph.objectSpace->allocateObject(*repo[id]);
#else
    job.object = repo[id];
#endif
    job.batchIdx = cnt;
    threads.pushInputQueue(job);
    cnt++;
    if (cnt >= (size_t)neighborhoodGraph.NeighborhoodGraph::property.batchSizeForCreation) {
      id++;
      break;
    }
  } // for
  return cnt;
}

static void
insertMultipleSearchResults(GraphIndex &neighborhoodGraph, 
			    CreateIndexThreadPool::OutputJobQueue &output, 
			    size_t dataSize)
{
  // compute distances among all of the resultant objects
  if (neighborhoodGraph.NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeANNG ||
      neighborhoodGraph.NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeIANNG ||
      neighborhoodGraph.NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeONNG ||
      neighborhoodGraph.NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeDNNG) {
    // This processing occupies about 30% of total indexing time when batch size is 200.
    // Only initial batch objects should be connected for each other.
    // The number of nodes in the graph is checked to know whether the batch is initial.
    //size_t size = NeighborhoodGraph::property.edgeSizeForCreation;
    size_t size = neighborhoodGraph.NeighborhoodGraph::property.edgeSizeForCreation;
    // add distances from a current object to subsequence objects to imitate of sequential insertion.

    sort(output.begin(), output.end());	// sort by batchIdx

    for (size_t idxi = 0; idxi < dataSize; idxi++) {
      // add distances
      ObjectDistances &objs = *output[idxi].results;
      for (size_t idxj = 0; idxj < idxi; idxj++) {
	ObjectDistance	r;
	r.distance = neighborhoodGraph.objectSpace->getComparator()(*output[idxi].object, *output[idxj].object);
	r.id = output[idxj].id;
	objs.push_back(r);
      }
      // sort and cut excess edges	    
      std::sort(objs.begin(), objs.end());
      if (objs.size() > size) {
	objs.resize(size);
      }
    } // for (size_t idxi ....
  } // if (neighborhoodGraph.graphType == NeighborhoodGraph::GraphTypeUDNNG)
  // insert resultant objects into the graph as edges
  for (size_t i = 0; i < dataSize; i++) {
    CreateIndexJob &gr = output[i];
    if ((*gr.results).size() == 0) {
    }
    if (static_cast<int>(gr.id) > neighborhoodGraph.NeighborhoodGraph::property.edgeSizeForCreation &&
	static_cast<int>(gr.results->size()) < neighborhoodGraph.NeighborhoodGraph::property.edgeSizeForCreation) {
//      cerr << "createIndex: Warning. The specified number of edges could not be acquired, because the pruned parameter [-S] might be set." << endl;
//      cerr << "  The node id=" << gr.id << endl;
//      cerr << "  The number of edges for the node=" << gr.results->size() << endl;
//      cerr << "  The pruned parameter (edgeSizeForSearch [-S])=" << neighborhoodGraph.NeighborhoodGraph::property.edgeSizeForSearch << endl;
      if (NGT_LOG_DEBUG_) {
          (*NGT_LOG_DEBUG_)("createIndex: Warning. The specified number of edges could not be acquired, because the pruned parameter [-S] might be set.");
          (*NGT_LOG_DEBUG_)("  The node id=" + std::to_string(gr.id));
          (*NGT_LOG_DEBUG_)("  The number of edges for the node=" + std::to_string(gr.results->size()));
          (*NGT_LOG_DEBUG_)("  The pruned parameter (edgeSizeForSearch [-S])=" + std::to_string(neighborhoodGraph.NeighborhoodGraph::property.edgeSizeForSearch));
      }
    }
    neighborhoodGraph.insertNode(gr.id, *gr.results);
  }
}

void 
GraphIndex::createIndex(size_t threadPoolSize, size_t sizeOfRepository) 
{
  if (NeighborhoodGraph::property.edgeSizeForCreation == 0) {
    return;
  }
  if (threadPoolSize <= 1) {
    createIndex();
  } else {
    Timer	timer;
    size_t	timerInterval = 100000;
    size_t	timerCount = timerInterval;
    size_t	count = 0;
    timer.start();

    size_t	pathAdjustCount = property.pathAdjustmentInterval;
    CreateIndexThreadPool threads(threadPoolSize);
    CreateIndexSharedData sd(*this);

    threads.setSharedData(&sd);
    threads.create();
    CreateIndexThreadPool::OutputJobQueue &output = threads.getOutputJobQueue();

    BuildTimeController buildTimeController(*this, NeighborhoodGraph::property);

    try {
      CreateIndexJob job;
      NGT::ObjectID id = 1;
      for (;;) {
	// search for the nearest neighbors
	size_t cnt = searchMultipleQueryForCreation(*this, id, job, threads, sizeOfRepository);
	if (cnt == 0) {
	  break;
	}
	// wait for the completion of the search
	threads.waitForFinish();
	if (output.size() != cnt) {
//	  cerr << "NNTGIndex::insertGraphIndexByThread: Warning!! Thread response size is wrong." << endl;
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("NNTGIndex::insertGraphIndexByThread: Warning!! Thread response size is wrong.");
	  cnt = output.size();
	}
	// insertion
	insertMultipleSearchResults(*this, output, cnt);

	while (!output.empty()) {
	  delete output.front().results;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	  GraphIndex::objectSpace->deleteObject(output.front().object);
#endif
	  output.pop_front();
	}

	count += cnt;
	if (timerCount <= count) {
	  timer.stop();
//	  cerr << "Processed " << timerCount << " time= " << timer << endl;
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("Processed " + std::to_string(timerCount )+ " time= " + std::to_string(timer.time));
	  timerCount += timerInterval;
	  timer.start();
	}
	buildTimeController.adjustEdgeSize(count);
	if (pathAdjustCount > 0 && pathAdjustCount <= count) {
	  GraphReconstructor::adjustPathsEffectively(static_cast<GraphIndex&>(*this));
	  pathAdjustCount += property.pathAdjustmentInterval;
	}
      }
    } catch(Exception &err) {
      threads.terminate();
      throw err;
    }
    threads.terminate();
  }

}

void GraphIndex::setupPrefetch(NGT::Property &prop) {
  assert(GraphIndex::objectSpace != 0);
  prop.prefetchOffset = GraphIndex::objectSpace->setPrefetchOffset(prop.prefetchOffset);
  prop.prefetchSize = GraphIndex::objectSpace->setPrefetchSize(prop.prefetchSize);
}

bool 
NGT::GraphIndex::showStatisticsOfGraph(NGT::GraphIndex &outGraph, char mode, size_t edgeSize)
{
  long double distance = 0.0;
  size_t numberOfNodes = 0;
  size_t numberOfOutdegree = 0;
  size_t numberOfNodesWithoutEdges = 0;
  size_t maxNumberOfOutdegree = 0;
  size_t minNumberOfOutdegree = SIZE_MAX;
  std::vector<int64_t> indegreeCount;
  std::vector<size_t> outdegreeHistogram;
  std::vector<size_t> indegreeHistogram;
  std::vector<std::vector<float> > indegree;
  NGT::GraphRepository &graph = outGraph.repository;
  NGT::ObjectRepository &repo = outGraph.objectSpace->getRepository();
  indegreeCount.resize(graph.size(), 0);
  indegree.resize(graph.size());
  size_t removedObjectCount = 0;
  bool valid = true;
  for (size_t id = 1; id < graph.size(); id++) {
    if (repo[id] == 0) {
      removedObjectCount++;
      continue;
    }
    NGT::GraphNode *node = 0;
    try {
      node = outGraph.getNode(id);
    } catch(NGT::Exception &err) {
//      std::cerr << "ngt info: Error. Cannot get the node. ID=" << id << ":" << err.what() << std::endl;
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("ngt info: Error. Cannot get the node. ID=" + std::to_string(id) + ":" + err.what());
      valid = false;
      continue;
    }
    numberOfNodes++;
    if (numberOfNodes % 1000000 == 0) {
//      std::cerr << "Processed " << numberOfNodes << std::endl;
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("Processed " + std::to_string(numberOfNodes));
    }
    size_t esize = node->size() > edgeSize ? edgeSize : node->size();
    if (esize == 0) {
      numberOfNodesWithoutEdges++;
    }
    if (esize > maxNumberOfOutdegree) {
      maxNumberOfOutdegree = esize;
    }
    if (esize < minNumberOfOutdegree) {
      minNumberOfOutdegree = esize;
    }
    if (outdegreeHistogram.size() <= esize) {
      outdegreeHistogram.resize(esize + 1);
    }
    outdegreeHistogram[esize]++;
    if (mode == 'e') {
      std::cout << id << "," << esize << ": ";
    }
    for (size_t i = 0; i < esize; i++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      NGT::ObjectDistance &n = (*node).at(i, graph.allocator);
#else
      NGT::ObjectDistance &n = (*node)[i];
#endif
      if (n.id == 0) {
//	std::cerr << "ngt info: Warning. id is zero." << std::endl;
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("ngt info: Warning. id is zero.");
	valid = false;
      }
      indegreeCount[n.id]++;
      indegree[n.id].push_back(n.distance);
      numberOfOutdegree++;
      double d = n.distance;
      if (mode == 'e') {
	std::cout << n.id << ":" << d << " ";
      }
      distance += d;
    }
    if (mode == 'e') {
      std::cout << std::endl;
    }
  }

  if (mode == 'a') {
    size_t count = 0;
    for (size_t id = 1; id < graph.size(); id++) {
      if (repo[id] == 0) {
	continue;
      }
      NGT::GraphNode *n = 0;
      try {
	n = outGraph.getNode(id);
      } catch(NGT::Exception &err) {
	continue;
      }
      NGT::GraphNode &node = *n;
      for (size_t i = 0; i < node.size(); i++) {  
	NGT::GraphNode *nn = 0;
	try {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	  nn = outGraph.getNode(node.at(i, graph.allocator).id);
#else
	  nn = outGraph.getNode(node[i].id);
#endif
	} catch(NGT::Exception &err) {
	  count++;
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
//	  std::cerr << "Directed edge! " << id << "->" << node.at(i, graph.allocator).id << " no object. "
//		    << node.at(i, graph.allocator).id << std::endl;
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("Directed edge! " + std::to_string(id) + "->" + std::to_string(node.at(i, graph.allocator).id) + " no object. "
		    + std::to_string(node.at(i, graph.allocator).id));
#else
//	  std::cerr << "Directed edge! " << id << "->" << node[i].id << " no object. " << node[i].id << std::endl;
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("Directed edge! " + std::to_string(id) + "->" + std::to_string(node[i].id) + " no object. " + std::to_string(node[i].id));
#endif
	  continue;
	}
	NGT::GraphNode &nnode = *nn;
	bool found = false;
	for (size_t i = 0; i < nnode.size(); i++) {  
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	  if (nnode.at(i, graph.allocator).id == id) {
#else
	  if (nnode[i].id == id) {
#endif
	    found = true;
	    break;
	  }
	}
	if (!found) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
//	    std::cerr << "Directed edge! " << id << "->" << node.at(i, graph.allocator).id << " no edge. "
//		      << node.at(i, graph.allocator).id << "->" << id << std::endl;
	    if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("Directed edge! " + std::to_string(id) + "->" + std::to_string(node.at(i, graph.allocator).id) + " no edge. "
		      + std::to_string(node.at(i, graph.allocator).id) + "->" + std::to_string(id));
#else
	    if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("Directed edge! " + std::to_string(id) + "->" + std::to_string(node[i].id) + " no edge. " + std::to_string(node[i].id) + "->" + std::to_string(id));
//	    std::cerr << "Directed edge! " << id << "->" << node[i].id << " no edge. " << node[i].id << "->" << id << std::endl;
#endif
	    count++;
	}
      }
    }
//    std::cerr << "The number of directed edges=" << count << std::endl;
    if (NGT_LOG_DEBUG_)
        (*NGT_LOG_DEBUG_)("The number of directed edges=" + std::to_string(count));
  }

  // calculate outdegree distance 10
  size_t d10count = 0;
  long double distance10 = 0.0;
  size_t d10SkipCount = 0;
  const size_t dcsize = 10;
  for (size_t id = 1; id < graph.size(); id++) {
    if (repo[id] == 0) {
      continue;
    }
    NGT::GraphNode *n = 0;
    try {
      n = outGraph.getNode(id);
    } catch(NGT::Exception &err) {
//      std::cerr << "ngt info: Warning. Cannot get the node. ID=" << id << ":" << err.what() << std::endl;
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("ngt info: Warning. Cannot get the node. ID=" + std::to_string(id) + ":" + err.what());
      continue;
    }
    NGT::GraphNode &node = *n;
    if (node.size() < dcsize - 1) {
      d10SkipCount++;
      continue;
    }
    for (size_t i = 0; i < node.size(); i++) {  
      if (i >= dcsize) {
	break;
      }
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      distance10 += node.at(i, graph.allocator).distance;
#else
      distance10 += node[i].distance;
#endif
      d10count++;
    }
  }
  distance10 /= (long double)d10count;

  // calculate indegree distance 10
  size_t ind10count = 0;
  long double indegreeDistance10 = 0.0;
  size_t ind10SkipCount = 0;
  for (size_t id = 1; id < indegree.size(); id++) {
    std::vector<float> &node = indegree[id];
    if (node.size() < dcsize - 1) {
      ind10SkipCount++;
      continue;
    }
    std::sort(node.begin(), node.end());
    for (size_t i = 0; i < node.size(); i++) {  
      assert(i == 0 || node[i - 1] <= node[i]);
      if (i >= dcsize) {
	break;
      }
      indegreeDistance10 += node[i];
      ind10count++;
    }
  }
  indegreeDistance10 /= (long double)ind10count;

  // calculate variance
  double averageNumberOfOutdegree = (double)numberOfOutdegree / (double)numberOfNodes;
  double sumOfSquareOfOutdegree = 0;
  double sumOfSquareOfIndegree = 0;
  for (size_t id = 1; id < graph.size(); id++) {
    if (repo[id] == 0) {
      continue;
    }
    NGT::GraphNode *node = 0;
    try {
      node = outGraph.getNode(id);
    } catch(NGT::Exception &err) {
//      std::cerr << "ngt info: Warning. Cannot get the node. ID=" << id << ":" << err.what() << std::endl;
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("ngt info: Warning. Cannot get the node. ID=" + std::to_string(id) + ":" + err.what());
      continue;
    }
    size_t esize = node->size();
    sumOfSquareOfOutdegree += ((double)esize - averageNumberOfOutdegree) * ((double)esize - averageNumberOfOutdegree);
    sumOfSquareOfIndegree += ((double)indegreeCount[id] - averageNumberOfOutdegree) * ((double)indegreeCount[id] - averageNumberOfOutdegree);	
  }

  size_t numberOfNodesWithoutIndegree = 0;
  size_t maxNumberOfIndegree = 0;
  size_t minNumberOfIndegree = INT64_MAX;
  for (size_t id = 1; id < graph.size(); id++) {
    if (graph[id] == 0) {
      continue;
    }
    if (indegreeCount[id] == 0) {
      numberOfNodesWithoutIndegree++;
//      std::cerr << "Error! The node without incoming edges. " << id << std::endl;
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("Error! The node without incoming edges. " + std::to_string(id));
      valid = false;
    }
    if (indegreeCount[id] > static_cast<int>(maxNumberOfIndegree)) {
      maxNumberOfIndegree = indegreeCount[id];
    }
    if (indegreeCount[id] < static_cast<int64_t>(minNumberOfIndegree)) {
      minNumberOfIndegree = indegreeCount[id];
    }
    if (static_cast<int>(indegreeHistogram.size()) <= indegreeCount[id]) {
      indegreeHistogram.resize(indegreeCount[id] + 1);
    }
    indegreeHistogram[indegreeCount[id]]++;
  }

  size_t count = 0;
  int medianOutdegree = -1;
  size_t modeOutdegree = 0;
  size_t max = 0;
  double c95 = 0.0;
  double c99 = 0.0;
  for (size_t i = 0; i < outdegreeHistogram.size(); i++) {
    count += outdegreeHistogram[i];
    if (medianOutdegree == -1 && count >= numberOfNodes / 2) {
      medianOutdegree = i;
    }
    if (max < outdegreeHistogram[i]) {
      max = outdegreeHistogram[i];
      modeOutdegree = i;
    }
    if (count > numberOfNodes * 0.95) {
      if (c95 == 0.0) {
	c95 += i * (count - numberOfNodes * 0.95);
      } else {
	c95 += i * outdegreeHistogram[i];
      }
    }
    if (count > numberOfNodes * 0.99) {
      if (c99 == 0.0) {
	c99 += i * (count - numberOfNodes * 0.99);
      } else {
	c99 += i * outdegreeHistogram[i];
      }
    }
  }
  c95 /= (double)numberOfNodes * 0.05;
  c99 /= (double)numberOfNodes * 0.01;

  count = 0;
  int medianIndegree = -1;
  size_t modeIndegree = 0;
  max = 0;
  double c5 = 0.0;
  double c1 = 0.0;
  for (size_t i = 0; i < indegreeHistogram.size(); i++) {
    if (count < numberOfNodes * 0.05) {
      if (count + indegreeHistogram[i] >= numberOfNodes * 0.05) {
	c5 += i * (numberOfNodes * 0.05 - count);
      } else {
	c5 += i * indegreeHistogram[i];
      }
    }
    if (count < numberOfNodes * 0.01) {
      if (count + indegreeHistogram[i] >= numberOfNodes * 0.01) {
	c1 += i * (numberOfNodes * 0.01 - count);
      } else {
	c1 += i * indegreeHistogram[i];
      }
    }
    count += indegreeHistogram[i];
    if (medianIndegree == -1 && count >= numberOfNodes / 2) {
      medianIndegree = i;
    }
    if (max < indegreeHistogram[i]) {
      max = indegreeHistogram[i];
      modeIndegree = i;
    }
  }
  c5 /= (double)numberOfNodes * 0.05;
  c1 /= (double)numberOfNodes * 0.01;

  if (NGT_LOG_DEBUG_) {
      (*NGT_LOG_DEBUG_)("The size of the object repository (not the number of the objects):\t" + std::to_string(repo.size() - 1));
      (*NGT_LOG_DEBUG_)("The number of the removed objects:\t" + std::to_string(removedObjectCount) + "/" + std::to_string(repo.size() - 1));
      (*NGT_LOG_DEBUG_)("The number of the nodes:\t" + std::to_string(numberOfNodes));
      (*NGT_LOG_DEBUG_)("The number of the edges:\t" + std::to_string(numberOfOutdegree));
      (*NGT_LOG_DEBUG_)("The mean of the edge lengths:\t" + std::to_string(distance / (double)numberOfOutdegree));
      (*NGT_LOG_DEBUG_)("The mean of the number of the edges per node:\t" + std::to_string((double)numberOfOutdegree / (double)numberOfNodes));
      (*NGT_LOG_DEBUG_)("The number of the nodes without edges:\t" + std::to_string(numberOfNodesWithoutEdges));
      (*NGT_LOG_DEBUG_)("The maximum of the outdegrees:\t" + std::to_string(maxNumberOfOutdegree));
      if (minNumberOfOutdegree == SIZE_MAX) {
          (*NGT_LOG_DEBUG_)("The minimum of the outdegrees:\t-NA-");
      } else {
          (*NGT_LOG_DEBUG_)("The minimum of the outdegrees:\t" + std::to_string(minNumberOfOutdegree));
      }
      (*NGT_LOG_DEBUG_)("The number of the nodes where indegree is 0:\t" + std::to_string(numberOfNodesWithoutIndegree));
      (*NGT_LOG_DEBUG_)("The maximum of the indegrees:\t" + std::to_string(maxNumberOfIndegree));
      if (minNumberOfIndegree == INT64_MAX) {
          (*NGT_LOG_DEBUG_)("The minimum of the indegrees:\t-NA-");
      } else {
          (*NGT_LOG_DEBUG_)("The minimum of the indegrees:\t" + std::to_string(minNumberOfIndegree));
      }
      (*NGT_LOG_DEBUG_)("#-nodes,#-edges,#-no-indegree,avg-edges,avg-dist,max-out,min-out,v-out,max-in,min-in,v-in,med-out,"
                   "med-in,mode-out,mode-in,c95,c5,o-distance(10),o-skip,i-distance(10),i-skip:"
                + std::to_string(numberOfNodes) + ":" + std::to_string(numberOfOutdegree) + ":" + std::to_string(numberOfNodesWithoutIndegree) + ":"
                + std::to_string((double)numberOfOutdegree / (double)numberOfNodes) + ":"
                + std::to_string(distance / (double)numberOfOutdegree) + ":"
                + std::to_string(maxNumberOfOutdegree) + ":" + std::to_string(minNumberOfOutdegree) + ":" + std::to_string(sumOfSquareOfOutdegree / (double)numberOfOutdegree) + ":"
                + std::to_string(maxNumberOfIndegree) + ":" + std::to_string(minNumberOfIndegree) + ":" + std::to_string(sumOfSquareOfIndegree / (double)numberOfOutdegree) + ":"
                + std::to_string(medianOutdegree) + ":" + std::to_string(medianIndegree) + ":" + std::to_string(modeOutdegree) + ":" + std::to_string(modeIndegree)
                + ":" + std::to_string(c95) + ":" + std::to_string(c5) + ":" + std::to_string(c99) + ":" + std::to_string(c1) + ":" + std::to_string(distance10) + ":" + std::to_string(d10SkipCount) + ":"
                + std::to_string(indegreeDistance10) + ":" + std::to_string(ind10SkipCount));
      if (mode == 'h') {
          (*NGT_LOG_DEBUG_)("#\tout\tin");
          for (size_t i = 0; i < outdegreeHistogram.size() || i < indegreeHistogram.size(); i++) {
              size_t out = outdegreeHistogram.size() <= i ? 0 : outdegreeHistogram[i];
              size_t in = indegreeHistogram.size() <= i ? 0 : indegreeHistogram[i];
              (*NGT_LOG_DEBUG_)(std::to_string(i) + "\t" + std::to_string(out) + "\t" + std::to_string(in));
          }
      }
  }
  /*
  std::cerr << "The size of the object repository (not the number of the objects):\t" << repo.size() - 1 << std::endl;
  std::cerr << "The number of the removed objects:\t" << removedObjectCount << "/" << repo.size() - 1 << std::endl;
  std::cerr << "The number of the nodes:\t" << numberOfNodes << std::endl;
  std::cerr << "The number of the edges:\t" << numberOfOutdegree << std::endl;
  std::cerr << "The mean of the edge lengths:\t" << std::setprecision(10) << distance / (double)numberOfOutdegree << std::endl;
  std::cerr << "The mean of the number of the edges per node:\t" << (double)numberOfOutdegree / (double)numberOfNodes << std::endl;
  std::cerr << "The number of the nodes without edges:\t" << numberOfNodesWithoutEdges << std::endl;
  std::cerr << "The maximum of the outdegrees:\t" << maxNumberOfOutdegree << std::endl;
  if (minNumberOfOutdegree == SIZE_MAX) {
    std::cerr << "The minimum of the outdegrees:\t-NA-" << std::endl;
  } else {
    std::cerr << "The minimum of the outdegrees:\t" << minNumberOfOutdegree << std::endl;
  }
  std::cerr << "The number of the nodes where indegree is 0:\t" << numberOfNodesWithoutIndegree << std::endl;
  std::cerr << "The maximum of the indegrees:\t" << maxNumberOfIndegree << std::endl;
  if (minNumberOfIndegree == INT64_MAX) {
    std::cerr << "The minimum of the indegrees:\t-NA-" << std::endl;
  } else {
    std::cerr << "The minimum of the indegrees:\t" << minNumberOfIndegree << std::endl;
  }
  std::cerr << "#-nodes,#-edges,#-no-indegree,avg-edges,avg-dist,max-out,min-out,v-out,max-in,min-in,v-in,med-out,"
    "med-in,mode-out,mode-in,c95,c5,o-distance(10),o-skip,i-distance(10),i-skip:"
	    << numberOfNodes << ":" << numberOfOutdegree << ":" << numberOfNodesWithoutIndegree << ":"
	    << std::setprecision(10) << (double)numberOfOutdegree / (double)numberOfNodes << ":"
	    << distance / (double)numberOfOutdegree << ":"
	    << maxNumberOfOutdegree << ":" << minNumberOfOutdegree << ":" << sumOfSquareOfOutdegree / (double)numberOfOutdegree<< ":"
	    << maxNumberOfIndegree << ":" << minNumberOfIndegree << ":" << sumOfSquareOfIndegree / (double)numberOfOutdegree << ":"
	    << medianOutdegree << ":" << medianIndegree << ":" << modeOutdegree << ":" << modeIndegree
	    << ":" << c95 << ":" << c5 << ":" << c99 << ":" << c1 << ":" << distance10 << ":" << d10SkipCount << ":"
	    << indegreeDistance10 << ":" << ind10SkipCount << std::endl;
  if (mode == 'h') {
    std::cerr << "#\tout\tin" << std::endl;
    for (size_t i = 0; i < outdegreeHistogram.size() || i < indegreeHistogram.size(); i++) {
      size_t out = outdegreeHistogram.size() <= i ? 0 : outdegreeHistogram[i];
      size_t in = indegreeHistogram.size() <= i ? 0 : indegreeHistogram[i];
      std::cerr << i << "\t" << out << "\t" << in << std::endl;
    }
  }
  */
  return valid;
}


void 
GraphAndTreeIndex::createIndex(size_t threadPoolSize, size_t sizeOfRepository) 
{
  assert(threadPoolSize > 0);

  if (NeighborhoodGraph::property.edgeSizeForCreation == 0) {
    return;
  }

  Timer	timer;
  size_t	timerInterval = 100000;
  size_t	timerCount = timerInterval;
  size_t	count = 0;
  timer.start();

  size_t	pathAdjustCount = property.pathAdjustmentInterval;
  CreateIndexThreadPool threads(threadPoolSize);

  CreateIndexSharedData sd(*this);

  threads.setSharedData(&sd);
  threads.create();
  CreateIndexThreadPool::OutputJobQueue &output = threads.getOutputJobQueue();

  BuildTimeController buildTimeController(*this, NeighborhoodGraph::property);

  try {
    CreateIndexJob job;
    NGT::ObjectID id = 1;
    for (;;) {
      size_t cnt = searchMultipleQueryForCreation(*this, id, job, threads, sizeOfRepository);

      if (cnt == 0) {
	break;
      }
      threads.waitForFinish();

      if (output.size() != cnt) {
//	cerr << "NNTGIndex::insertGraphIndexByThread: Warning!! Thread response size is wrong." << endl;
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("NNTGIndex::insertGraphIndexByThread: Warning!! Thread response size is wrong.");
	cnt = output.size();
      }

      insertMultipleSearchResults(*this, output, cnt);

      for (size_t i = 0; i < cnt; i++) {
	CreateIndexJob &job = output[i];
	if (((job.results->size() > 0) && ((*job.results)[0].distance != 0.0)) ||
	    (job.results->size() == 0)) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	  Object *f = GraphIndex::objectSpace->allocateObject(*job.object);
	  DVPTree::InsertContainer tiobj(*f, job.id);
#else
	  DVPTree::InsertContainer tiobj(*job.object, job.id);
#endif
	  try {
	    DVPTree::insert(tiobj);
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	    GraphIndex::objectSpace->deleteObject(f);
#endif
	  } catch (Exception &err) {
//	    cerr << "NGT::createIndex: Fatal error. ID=" << job.id << ":";
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("NGT::createIndex: Fatal error. ID=" + std::to_string(job.id) + ":");
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	    GraphIndex::objectSpace->deleteObject(f);
#endif
	    if (NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeKNNG) {
//	      cerr << err.what() << " continue.." << endl;
            if (NGT_LOG_DEBUG_)
                (*NGT_LOG_DEBUG_)(std::string(err.what()) + " continue..");
	    } else {
	      throw err;
	    }
	  }
	}
      } // for

      while (!output.empty()) {
	delete output.front().results;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	GraphIndex::objectSpace->deleteObject(output.front().object);
#endif
	output.pop_front();
      }

      count += cnt;
      if (timerCount <= count) {
	    timer.stop();
//	cerr << "Processed " << timerCount << " objects. time= " << timer << endl;
        if (NGT_LOG_DEBUG_) {
            (*NGT_LOG_DEBUG_)(
                "Processed " + std::to_string(timerCount) + " objects. time= " + std::to_string(timer.time));
        }
	    timerCount += timerInterval;
	    timer.start();
      }
      buildTimeController.adjustEdgeSize(count);
      if (pathAdjustCount > 0 && pathAdjustCount <= count) {
	GraphReconstructor::adjustPathsEffectively(static_cast<GraphIndex&>(*this));
	pathAdjustCount += property.pathAdjustmentInterval;
      }
    }
  } catch(Exception &err) {
    threads.terminate();
    throw err;
  }
  threads.terminate();
}


void 
GraphAndTreeIndex::createIndex(const vector<pair<NGT::Object*, size_t> > &objects, 
			       vector<InsertionResult> &ids, 
			       double range, size_t threadPoolSize)
{
  Timer		timer;
  size_t	timerInterval = 100000;
  size_t	timerCount = timerInterval;
  size_t	count = 0;
  timer.start();
  if (threadPoolSize <= 0) {
//    cerr << "Not implemented!!" << endl;
    if (NGT_LOG_DEBUG_)
        (*NGT_LOG_DEBUG_)("Not implemented!!");
    abort();
  } else {
    CreateIndexThreadPool threads(threadPoolSize);
    CreateIndexSharedData sd(*this);
    threads.setSharedData(&sd);
    threads.create();
    CreateIndexThreadPool::OutputJobQueue &output = threads.getOutputJobQueue();
    try {
      CreateIndexJob job;
      size_t	idx = 0;
      for (;;) {
	size_t	cnt = 0;
	{
	  for (; idx < objects.size(); idx++) {
	    if (objects[idx].first == 0) {
	      ids.push_back(InsertionResult());
	      continue;
	    }
	    job.id = 0;
	    job.results = 0;
	    job.object = objects[idx].first;
	    job.batchIdx = ids.size();
	    // insert an empty entry to prepare.
	    ids.push_back(InsertionResult(job.id, false, 0.0));
	    threads.pushInputQueue(job);
	    cnt++;
	    if (cnt >= (size_t)NeighborhoodGraph::property.batchSizeForCreation) {
	      idx++;
	      break;
	    }
	  } 
	}
	if (cnt == 0) {
	  break;
	}
	threads.waitForFinish();
	if (output.size() != cnt) {
//	  cerr << "NNTGIndex::insertGraphIndexByThread: Warning!! Thread response size is wrong." << endl;
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("NNTGIndex::insertGraphIndexByThread: Warning!! Thread response size is wrong.");
	  cnt = output.size();
	}
	{
	  // This processing occupies about 30% of total indexing time when batch size is 200.
	  // Only initial batch objects should be connected for each other.
	  // The number of nodes in the graph is checked to know whether the batch is initial.
	  size_t size = NeighborhoodGraph::property.edgeSizeForCreation;
	  // add distances from a current object to subsequence objects to imitate of sequential insertion.

	  sort(output.begin(), output.end());	
	  for (size_t idxi = 0; idxi < cnt; idxi++) {
	    // add distances
	    ObjectDistances &objs = *output[idxi].results;
	    for (size_t idxj = 0; idxj < idxi; idxj++) {
	      if (output[idxj].id == 0) {
		// unregistered object
		continue;
	      }
	      ObjectDistance	r;
	      r.distance = GraphIndex::objectSpace->getComparator()(*output[idxi].object, *output[idxj].object);
	      r.id = output[idxj].id;
	      objs.push_back(r);
	    }
	    // sort and cut excess edges	    
	    std::sort(objs.begin(), objs.end());
	    if (objs.size() > size) {
	      objs.resize(size);
	    }
	    if ((objs.size() > 0) && (range < 0.0 || ((double)objs[0].distance <= range + FLT_EPSILON))) {
	      // The line below was replaced by the line above to consider EPSILON for float comparison. 170702
	      // if ((objs.size() > 0) && (range < 0.0 || (objs[0].distance <= range))) {
	      // An identical or similar object already exits
	      ids[output[idxi].batchIdx].identical = true;
	      ids[output[idxi].batchIdx].id = objs[0].id;
	      ids[output[idxi].batchIdx].distance = objs[0].distance;
	      output[idxi].id = 0;
	    } else {
	      assert(output[idxi].id == 0);
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	      PersistentObject *obj = GraphIndex::objectSpace->allocatePersistentObject(*output[idxi].object);
	      output[idxi].id = GraphIndex::objectSpace->insert(obj);
#else
	      output[idxi].id = GraphIndex::objectSpace->insert(output[idxi].object);
#endif
	      ids[output[idxi].batchIdx].id = output[idxi].id;
	    }
	  } 
	}
	// insert resultant objects into the graph as edges
	for (size_t i = 0; i < cnt; i++) {
	  CreateIndexJob &job = output.front();
	  if (job.id != 0) {
	    if (property.indexType == NGT::Property::GraphAndTree) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	      Object *f = GraphIndex::objectSpace->allocateObject(*job.object);
	      DVPTree::InsertContainer tiobj(*f, job.id);
#else
	      DVPTree::InsertContainer tiobj(*job.object, job.id);
#endif
	      try {
		DVPTree::insert(tiobj);
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
		GraphIndex::objectSpace->deleteObject(f);
#endif
	      } catch (Exception &err) {
//		cerr << "NGT::createIndex: Fatal error. ID=" << job.id << ":" << err.what();
            if (NGT_LOG_DEBUG_)
                (*NGT_LOG_DEBUG_)("NGT::createIndex: Fatal error. ID=" + std::to_string(job.id) + ":" + err.what());
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
		GraphIndex::objectSpace->deleteObject(f);
#endif
		if (NeighborhoodGraph::property.graphType == NeighborhoodGraph::GraphTypeKNNG) {
//		  cerr << err.what() << " continue.." << endl;
            if (NGT_LOG_DEBUG_)
                (*NGT_LOG_DEBUG_)(std::string(err.what()) + " continue..");
		} else {
		  throw err;
		}
	      }
	    }
	    if (((*job.results).size() == 0) && (job.id != 1)) {
//	      cerr  << "insert warning!! No searched nodes!. If the first time, no problem. " << job.id << endl;
            if (NGT_LOG_DEBUG_)
                (*NGT_LOG_DEBUG_)("insert warning!! No searched nodes!. If the first time, no problem. " + std::to_string(job.id));
	    }
	    GraphIndex::insertNode(job.id, *job.results);
	  } 
	  if (job.results != 0) {
	    delete job.results;
	  }
	  output.pop_front();
	}
	
	count += cnt;
	if (timerCount <= count) {
	  timer.stop();
//	  cerr << "Processed " << timerCount << " time= " << timer << endl;
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("Processed " + std::to_string(timerCount) + " time= " + std::to_string(timer.time));
	  timerCount += timerInterval;
	  timer.start();
	}
      }
    } catch(Exception &err) {
//      cerr << "thread terminate!" << endl;
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("thread terminate!");
      threads.terminate();
      throw err;
    }
    threads.terminate();
  }
}

static bool 
findPathAmongIdenticalObjects(GraphAndTreeIndex &graph, size_t srcid, size_t dstid) {
  stack<size_t> nodes;
  unordered_set<size_t> done;
  nodes.push(srcid);
  while (!nodes.empty()) {
    auto tid = nodes.top();
    nodes.pop();
    done.insert(tid);
    GraphNode &node = *graph.GraphIndex::getNode(tid);
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    for (auto i = node.begin(graph.repository.allocator); i != node.end(graph.GraphIndex::repository.allocator); ++i) {
#else
    for (auto i = node.begin(); i != node.end(); ++i) {
#endif
      if ((*i).distance != 0.0) {
	break;
      }
      if ((*i).id == dstid) {
	return true;
      }
      if (done.count((*i).id) == 0) {
        nodes.push((*i).id);
      }
    }
  }
  return false;
}

bool 
GraphAndTreeIndex::verify(vector<uint8_t> &status, bool info, char mode) {
  bool valid = GraphIndex::verify(status, info);
  if (!valid) {
//    cerr << "The graph or object is invalid!" << endl;
    if (NGT_LOG_DEBUG_)
        (*NGT_LOG_DEBUG_)("The graph or object is invalid!");
  }
  bool treeValid = DVPTree::verify(GraphIndex::objectSpace->getRepository().size(), status);
  if (!treeValid) {
//    cerr << "The tree is invalid" << endl;
    if (NGT_LOG_DEBUG_)
        (*NGT_LOG_DEBUG_)("The tree is invalid");
  }
  valid = valid && treeValid;
  // status: tree|graph|object
//  cerr << "Started checking consistency..." << endl;
    if (NGT_LOG_DEBUG_)
        (*NGT_LOG_DEBUG_)("Started checking consistency...");
  for (size_t id = 1; id < status.size(); id++) {
    if (id % 100000 == 0) {
//      cerr << "The number of processed objects=" << id << endl;
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("The number of processed objects=" + std::to_string(id));
    }
    if (status[id] != 0x00 && status[id] != 0x07) {
      if (status[id] == 0x03) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	NGT::Object *po = GraphIndex::objectSpace->allocateObject(*GraphIndex::getObjectRepository().get(id));
	NGT::SearchContainer sc(*po);
#else
	NGT::SearchContainer sc(*GraphIndex::getObjectRepository().get(id));
#endif
	NGT::ObjectDistances objects;
	sc.setResults(&objects);
	sc.id = 0;
	sc.radius = 0.0;
	sc.explorationCoefficient = 1.1;
	sc.edgeSize = 0;
	ObjectDistances	seeds;
	seeds.push_back(ObjectDistance(id, 0.0));
	objects.clear();
	try {
	  GraphIndex::search(sc, seeds);
	} catch(Exception &err) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	  GraphIndex::objectSpace->deleteObject(po);
#endif
//	  cerr << "Fatal Error!: Cannot search! " << err.what() << endl;
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("Fatal Error!: Cannot search! " + std::string(err.what()));
	  objects.clear();
	}
	size_t n = 0;
	bool registeredIdenticalObject = false;
	for (; n < objects.size(); n++) {
	  if (objects[n].id != id && status[objects[n].id] == 0x07) {
	    registeredIdenticalObject = true;
	    break;
	  }
	}
	if (!registeredIdenticalObject) {
	  if (info) {
//	    cerr << "info: not found the registered same objects. id=" << id << " size=" << objects.size() << endl;
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("info: not found the registered same objects. id=" + std::to_string(id) + " size=" + std::to_string(objects.size()));
	  }
	  sc.id = 0;
	  sc.radius = FLT_MAX;
	  sc.explorationCoefficient = 1.2;
	  sc.edgeSize = 0;
	  sc.size = objects.size() < 100 ? 100 : objects.size() * 2;
	  ObjectDistances	seeds;
	  seeds.push_back(ObjectDistance(id, 0.0));
	  objects.clear();
	  try {
	    GraphIndex::search(sc, seeds);
	  } catch(Exception &err) {
//	    cerr << "Fatal Error!: Cannot search! " << err.what() << endl;
        if (NGT_LOG_DEBUG_) {
            (*NGT_LOG_DEBUG_)("Fatal Error!: Cannot search! " + std::string(err.what()));
        }
	    objects.clear();
	  }
	  registeredIdenticalObject = false;
	  for (n = 0; n < objects.size(); n++) {
	    if (objects[n].distance != 0.0) break;
	    if (objects[n].id != id && status[objects[n].id] == 0x07) {
	      registeredIdenticalObject = true;
	      if (info) {
//		cerr << "info: found by using mode accurate search. " << objects[n].id << endl;
                if (NGT_LOG_DEBUG_)
                    (*NGT_LOG_DEBUG_)("info: found by using mode accurate search. " + std::to_string(objects[n].id));
	      }
	      break;
	    }
	  }
	}
	if (!registeredIdenticalObject && mode != 's') {
	  if (info) {
//	    cerr << "info: not found by using more accurate search." << endl;
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("info: not found by using more accurate search.");
	  }
	  sc.id = 0;
	  sc.radius = 0.0;
	  sc.explorationCoefficient = 1.1;
	  sc.edgeSize = 0;
	  sc.size = SIZE_MAX;
	  objects.clear();
	  linearSearch(sc);
	  n = 0;
	  registeredIdenticalObject = false;
	  for (; n < objects.size(); n++) {
	    if (objects[n].distance != 0.0) break;
	    if (objects[n].id != id && status[objects[n].id] == 0x07) {
	      registeredIdenticalObject = true;
	      if (info) {
//		cerr << "info: found by using linear search. " << objects[n].id << endl;
            if (NGT_LOG_DEBUG_)
                (*NGT_LOG_DEBUG_)("info: found by using linear search. " + std::to_string(objects[n].id));
	      }
	      break;
	    }
	  }
	}
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	GraphIndex::objectSpace->deleteObject(po);
#endif
	if (registeredIdenticalObject) {
	  if (info) {
//	    cerr << "Info ID=" << id << ":" << static_cast<int>(status[id]) << endl;
//	    cerr << "  found the valid same objects. " << objects[n].id << endl;
        if (NGT_LOG_DEBUG_){
            (*NGT_LOG_DEBUG_)("Info ID=" + std::to_string(id) + ":" + std::to_string(static_cast<int>(status[id])));
            (*NGT_LOG_DEBUG_)("  found the valid same objects. " + std::to_string(objects[n].id));
        }
	  }
	  GraphNode &fromNode = *GraphIndex::getNode(id);
	  bool fromFound = false;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	  for (auto i = fromNode.begin(GraphIndex::repository.allocator); i != fromNode.end(GraphIndex::repository.allocator); ++i) {
#else
	  for (auto i = fromNode.begin(); i != fromNode.end(); ++i) {
#endif
	    if ((*i).id == objects[n].id) {
	      fromFound = true;
	    }
	  }
	  GraphNode &toNode = *GraphIndex::getNode(objects[n].id);
	  bool toFound = false;
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	  for (auto i = toNode.begin(GraphIndex::repository.allocator); i != toNode.end(GraphIndex::repository.allocator); ++i) {
#else
	  for (auto i = toNode.begin(); i != toNode.end(); ++i) {
#endif
	    if ((*i).id == id) {
	      toFound = true;
	    }
	  }
	  if (!fromFound || !toFound) {
	    if (info) {
	      if (!fromFound && !toFound) {
//		cerr << "Warning no undirected edge between " << id << "(" << fromNode.size() << ") and "
//		     << objects[n].id << "(" << toNode.size() << ")." << endl;
		    if (NGT_LOG_DEBUG_)
                (*NGT_LOG_DEBUG_)("Warning no undirected edge between " + std::to_string(id) + "(" + std::to_string(fromNode.size()) + ") and "
		     + std::to_string(objects[n].id) + "(" + std::to_string(toNode.size()) + ").");
	      } else if (!fromFound) {
//		cerr << "Warning no directed edge from " << id << "(" << fromNode.size() << ") to "
//		     << objects[n].id << "(" << toNode.size() << ")." << endl;
		    if (NGT_LOG_DEBUG_)
                (*NGT_LOG_DEBUG_)("Warning no directed edge from " + std::to_string(id) + "(" + std::to_string(fromNode.size()) + ") to "
		     + std::to_string(objects[n].id) + "(" + std::to_string(toNode.size()) + ").");
	      } else if (!toFound) {
//		cerr << "Warning no reverse directed edge from " << id << "(" << fromNode.size() << ") to "
//		     << objects[n].id << "(" << toNode.size() << ")." << endl;
		    if (NGT_LOG_DEBUG_)
                (*NGT_LOG_DEBUG_)("Warning no reverse directed edge from " + std::to_string(id) + "(" + std::to_string(fromNode.size()) + ") to "
		     + std::to_string(objects[n].id) + "(" + std::to_string(toNode.size()) + ").");
	      }
	    }
	    if (!findPathAmongIdenticalObjects(*this, id, objects[n].id)) {
//	      cerr << "Warning no path from " << id << " to " << objects[n].id << endl;
            if (NGT_LOG_DEBUG_)
                (*NGT_LOG_DEBUG_)("Warning no path from " + std::to_string(id) + " to " + std::to_string(objects[n].id));
	    }
	    if (!findPathAmongIdenticalObjects(*this, objects[n].id, id)) {
//	      cerr << "Warning no reverse path from " << id << " to " << objects[n].id << endl;
	        if (NGT_LOG_DEBUG_)
                (*NGT_LOG_DEBUG_)("Warning no reverse path from " + std::to_string(id) + " to " + std::to_string(objects[n].id));
	    }
	  }
	} else {
	  if (mode == 's') {
//	    cerr << "Warning: not found the valid same object, but not try to use linear search." << endl;
//	    cerr << "Error! ID=" << id << ":" << static_cast<int>(status[id]) << endl;
	    if (NGT_LOG_DEBUG_) {
            (*NGT_LOG_DEBUG_)("Warning: not found the valid same object, but not try to use linear search.");
            (*NGT_LOG_DEBUG_)("Error! ID=" + std::to_string(id) + ":" + std::to_string(static_cast<int>(status[id])));
	    }
	  } else {
//	    cerr << "Warning: not found the valid same object even by using linear search." << endl;
//	    cerr << "Error! ID=" << id << ":" << static_cast<int>(status[id]) << endl;
          if (NGT_LOG_DEBUG_) {
              (*NGT_LOG_DEBUG_)("Warning: not found the valid same object even by using linear search.");
              (*NGT_LOG_DEBUG_)("Error! ID=" + std::to_string(id) + ":" + std::to_string(static_cast<int>(status[id])));
          }
	    valid = false;
	  }
	}
      } else if (status[id] == 0x01) {
	  if (info) {
//	    cerr << "Warning! ID=" << id << ":" << static_cast<int>(status[id]) << endl;
//	    cerr << "  not inserted into the indexes" << endl;
	    if (NGT_LOG_DEBUG_) {
            (*NGT_LOG_DEBUG_)("Warning! ID=" + std::to_string(id) + ":" + std::to_string(static_cast<int>(status[id])));
            (*NGT_LOG_DEBUG_)("  not inserted into the indexes");
	    }
	  }
      } else {
//	  cerr << "Error! ID=" << id << ":" << static_cast<int>(status[id]) << endl;
        if (NGT_LOG_DEBUG_)
            (*NGT_LOG_DEBUG_)("Error! ID=" + std::to_string(id) + ":" + std::to_string(static_cast<int>(status[id])));
	  valid = false;
      }
    }
  }
  return valid;
}



