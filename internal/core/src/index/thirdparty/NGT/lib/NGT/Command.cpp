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

#include	"NGT/Command.h"
#include	"NGT/GraphReconstructor.h"
#include	"NGT/Optimizer.h"
#include	"NGT/GraphOptimizer.h"


using namespace std;


  void 
  NGT::Command::create(Args &args)
  {
    const string usage = "Usage: ngt create "
      "-d dimension [-p #-of-thread] [-i index-type(t|g)] [-g graph-type(a|k|b|o|i)] "
      "[-t truncation-edge-limit] [-E edge-size] [-S edge-size-for-search] [-L edge-size-limit] "
      "[-e epsilon] [-o object-type(f|c)] [-D distance-function(1|2|a|A|h|j|c|C)] [-n #-of-inserted-objects] "
      "[-P path-adjustment-interval] [-B dynamic-edge-size-base] [-A object-alignment(t|f)] "
      "[-T build-time-limit] [-O outgoing x incoming] "
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
      "[-N maximum-#-of-inserted-objects] "
#endif
      "index(output) [data.tsv(input)]";
    string database;
    try {
      database = args.get("#1");
    } catch (...) {
      cerr << "ngt: Error: DB is not specified." << endl;
      cerr << usage << endl;
      return;
    }
    string data;
    try {
      data = args.get("#2");
    } catch (...) {}

    NGT::Property property;

    property.edgeSizeForCreation = args.getl("E", 10);
    property.edgeSizeForSearch = args.getl("S", 40);
    property.batchSizeForCreation = args.getl("b", 200);
    property.insertionRadiusCoefficient = args.getf("e", 0.1) + 1.0;
    property.truncationThreshold = args.getl("t", 0);
    property.dimension = args.getl("d", 0);
    property.threadPoolSize = args.getl("p", 24);
    property.pathAdjustmentInterval = args.getl("P", 0);
    property.dynamicEdgeSizeBase = args.getl("B", 30);
    property.buildTimeLimit = args.getf("T", 0.0);

    if (property.dimension <= 0) {
      cerr << "ngt: Error: Specify greater than 0 for # of your data dimension by a parameter -d." << endl;
      cerr << usage << endl;
      return;
    }

    property.objectAlignment = args.getChar("A", 'f') == 't' ? NGT::Property::ObjectAlignmentTrue : NGT::Property::ObjectAlignmentFalse;

    char graphType = args.getChar("g", 'a');
    switch(graphType) {
    case 'a': property.graphType = NGT::Property::GraphType::GraphTypeANNG; break;
    case 'k': property.graphType = NGT::Property::GraphType::GraphTypeKNNG; break;
    case 'b': property.graphType = NGT::Property::GraphType::GraphTypeBKNNG; break;
    case 'd': property.graphType = NGT::Property::GraphType::GraphTypeDNNG; break;
    case 'o': property.graphType = NGT::Property::GraphType::GraphTypeONNG; break;
    case 'i': property.graphType = NGT::Property::GraphType::GraphTypeIANNG; break;
    default:
      cerr << "ngt: Error: Invalid graph type. " << graphType << endl;
      cerr << usage << endl;
      return;
    }    

    if (property.graphType == NGT::Property::GraphType::GraphTypeONNG) {
      property.outgoingEdge = 10;
      property.incomingEdge = 80;
      string str = args.getString("O", "-");
      if (str != "-") {
	vector<string> tokens;
	NGT::Common::tokenize(str, tokens, "x");
	if (str != "-" && tokens.size() != 2) {
	  cerr << "ngt: Error: outgoing/incoming edge size specification is invalid. (out)x(in) " << str << endl;
	  cerr << usage << endl;
	  return;
	}
	property.outgoingEdge = NGT::Common::strtod(tokens[0]);
	property.incomingEdge = NGT::Common::strtod(tokens[1]);
	cerr << "ngt: ONNG out x in=" << property.outgoingEdge << "x" << property.incomingEdge << endl;
      }
    }

    char seedType = args.getChar("s", '-');
    switch(seedType) {
    case 'f': property.seedType = NGT::Property::SeedType::SeedTypeFixedNodes; break;
    case '1': property.seedType = NGT::Property::SeedType::SeedTypeFirstNode; break;
    case 'r': property.seedType = NGT::Property::SeedType::SeedTypeRandomNodes; break;
    case 'l': property.seedType = NGT::Property::SeedType::SeedTypeAllLeafNodes; break;
    default:
    case '-': property.seedType = NGT::Property::SeedType::SeedTypeNone; break;
    }

    char objectType = args.getChar("o", 'f');
    char distanceType = args.getChar("D", '2');

    size_t dataSize = args.getl("n", 0);
    char indexType = args.getChar("i", 't');

    if (debugLevel >= 1) {
      cerr << "edgeSizeForCreation=" << property.edgeSizeForCreation << endl;
      cerr << "edgeSizeForSearch=" << property.edgeSizeForSearch << endl;
      cerr << "edgeSizeLimit=" << property.edgeSizeLimitForCreation << endl;
      cerr << "batch size=" << property.batchSizeForCreation << endl;
      cerr << "graphType=" << property.graphType << endl;
      cerr << "epsilon=" << property.insertionRadiusCoefficient - 1.0 << endl;
      cerr << "thread size=" << property.threadPoolSize << endl;
      cerr << "dimension=" << property.dimension << endl;
      cerr << "indexType=" << indexType << endl;
    }

    switch (objectType) {
    case 'f': 
      property.objectType = NGT::Index::Property::ObjectType::Float;
      break;
    case 'c':
      property.objectType = NGT::Index::Property::ObjectType::Uint8;
      break;
    default:
      cerr << "ngt: Error: Invalid object type. " << objectType << endl;
      cerr << usage << endl;
      return;
    }

    switch (distanceType) {
    case '1': 
      property.distanceType = NGT::Index::Property::DistanceType::DistanceTypeL1;
      break;
    case '2':
      property.distanceType = NGT::Index::Property::DistanceType::DistanceTypeL2;
      break;
    case 'a':
      property.distanceType = NGT::Index::Property::DistanceType::DistanceTypeAngle;
      break;
    case 'A':
      property.distanceType = NGT::Index::Property::DistanceType::DistanceTypeNormalizedAngle;
      break;
    case 'h':
      property.distanceType = NGT::Index::Property::DistanceType::DistanceTypeHamming;
      break;
    case 'j':
      property.distanceType = NGT::Index::Property::DistanceType::DistanceTypeJaccard;
      break;
    case 'J':
      property.distanceType = NGT::Index::Property::DistanceType::DistanceTypeSparseJaccard;
      break;
    case 'c':
      property.distanceType = NGT::Index::Property::DistanceType::DistanceTypeCosine;
      break;
    case 'C':
      property.distanceType = NGT::Index::Property::DistanceType::DistanceTypeNormalizedCosine;
      break;
    default:
      cerr << "ngt: Error: Invalid distance type. " << distanceType << endl;
      cerr << usage << endl;
      return;
    }

#ifdef NGT_SHARED_MEMORY_ALLOCATOR
    size_t maxNoOfObjects = args.getl("N", 0);
    if (maxNoOfObjects > 0) {
      property.graphSharedMemorySize 
	= property.treeSharedMemorySize
	= property.objectSharedMemorySize = 512 * ceil(maxNoOfObjects / 50000000);
    }
#endif

    switch (indexType) {
    case 't':
      NGT::Index::createGraphAndTree(database, property, data, dataSize);
      break;
    case 'g':
      NGT::Index::createGraph(database, property, data, dataSize);	
      break;
    }
  }

  void 
  NGT::Command::append(Args &args)
  {
    const string usage = "Usage: ngt append [-p #-of-thread] [-d dimension] [-n data-size] "
      "index(output) [data.tsv(input)]";
    string database;
    try {
      database = args.get("#1");
    } catch (...) {
      cerr << "ngt: Error: DB is not specified." << endl;
      cerr << usage << endl;
      return;
    }
    string data;
    try {
      data = args.get("#2");
    } catch (...) {
      cerr << "ngt: Warning: No specified object file. Just build an index for the existing objects." << endl;
    }

    int threadSize = args.getl("p", 50);
    size_t dimension = args.getl("d", 0);
    size_t dataSize = args.getl("n", 0);

    if (debugLevel >= 1) {
      cerr << "thread size=" << threadSize << endl;
      cerr << "dimension=" << dimension << endl;
    }


    try {
      NGT::Index::append(database, data, threadSize, dataSize);	
    } catch (NGT::Exception &err) {
      cerr << "ngt: Error " << err.what() << endl;
      cerr << usage << endl;
    } catch (...) {
      cerr << "ngt: Error" << endl;
      cerr << usage << endl;
    }
  }


  void
  NGT::Command::search(NGT::Index &index, NGT::Command::SearchParameter &searchParameter, istream &is, ostream &stream)
  {

    if (searchParameter.outputMode[0] == 'e') { 
      stream << "# Beginning of Evaluation" << endl; 
    }

    string line;
    double totalTime	= 0;
    size_t queryCount	= 0;
    while(getline(is, line)) {
      if (searchParameter.querySize > 0 && queryCount >= searchParameter.querySize) {
	break;
      }
      NGT::Object *object = index.allocateObject(line, " \t");
      queryCount++;
      size_t step = searchParameter.step == 0 ? UINT_MAX : searchParameter.step;
      for (size_t n = 0; n <= step; n++) {
	NGT::SearchContainer sc(*object);
	double epsilon;
	if (searchParameter.step != 0) {
	  epsilon = searchParameter.beginOfEpsilon + (searchParameter.endOfEpsilon - searchParameter.beginOfEpsilon) * n / step; 
	} else {
	  epsilon = searchParameter.beginOfEpsilon + searchParameter.stepOfEpsilon * n;
	  if (epsilon > searchParameter.endOfEpsilon) {
	    break;
	  }
	}
	NGT::ObjectDistances objects;
	sc.setResults(&objects);
	sc.setSize(searchParameter.size);
	sc.setRadius(searchParameter.radius);
	if (searchParameter.accuracy > 0.0) {
	  sc.setExpectedAccuracy(searchParameter.accuracy);
	} else {
	  sc.setEpsilon(epsilon);
	}
 	sc.setEdgeSize(searchParameter.edgeSize);
	NGT::Timer timer;
	try {
	  if (searchParameter.outputMode[0] == 'e') {
	    double time = 0.0;
	    uint64_t ntime = 0;
	    double minTime = DBL_MAX;
	    size_t trial = searchParameter.trial <= 1 ? 2 : searchParameter.trial;
	    for (size_t t = 0; t < trial; t++) {
	      switch (searchParameter.indexType) {
	      case 't': timer.start(); index.search(sc); timer.stop(); break;
	      case 'g': timer.start(); index.searchUsingOnlyGraph(sc); timer.stop(); break;
	      case 's': timer.start(); index.linearSearch(sc); timer.stop(); break;
	      }
	      if (minTime > timer.time) {
		minTime = timer.time;
	      }
	      time += timer.time;
	      ntime += timer.ntime;
	    }
	    time /= (double)searchParameter.trial;
	    ntime /= searchParameter.trial;
	    timer.time = minTime;
	    timer.ntime = ntime;
	  } else {
	    switch (searchParameter.indexType) {
	    case 't': timer.start(); index.search(sc); timer.stop(); break;
	    case 'g': timer.start(); index.searchUsingOnlyGraph(sc); timer.stop(); break;
	    case 's': timer.start(); index.linearSearch(sc); timer.stop(); break;
	    }
	  }
	} catch (NGT::Exception &err) {
	  if (searchParameter.outputMode != "ei") {
	    // not ignore exceptions
	    throw err;
	  }
	}
	totalTime += timer.time;
	if (searchParameter.outputMode[0] == 'e') {
	  stream << "# Query No.=" << queryCount << endl;
	  stream << "# Query=" << line.substr(0, 20) + " ..." << endl;
	  stream << "# Index Type=" << searchParameter.indexType << endl;
	  stream << "# Size=" << searchParameter.size << endl;
	  stream << "# Radius=" << searchParameter.radius << endl;
	  stream << "# Epsilon=" << epsilon << endl;
	  stream << "# Query Time (msec)=" << timer.time * 1000.0 << endl;
	  stream << "# Distance Computation=" << sc.distanceComputationCount << endl;
	  stream << "# Visit Count=" << sc.visitCount << endl;
	} else {
	  stream << "Query No." << queryCount << endl;
	  stream << "Rank\tID\tDistance" << endl;
	}
	for (size_t i = 0; i < objects.size(); i++) {
	  stream << i + 1 << "\t" << objects[i].id << "\t";
	  stream << objects[i].distance << endl;
	}
	if (searchParameter.outputMode[0] == 'e') {
	  stream << "# End of Search" << endl;
	} else {
	  stream << "Query Time= " << timer.time << " (sec), " << timer.time * 1000.0 << " (msec)" << endl;
	}
      } // for
      index.deleteObject(object);
      if (searchParameter.outputMode[0] == 'e') {
	stream << "# End of Query" << endl;
      }
    } // while
    if (searchParameter.outputMode[0] == 'e') {
      stream << "# Average Query Time (msec)=" << totalTime * 1000.0 / (double)queryCount << endl;
      stream << "# Number of queries=" << queryCount << endl;
      stream << "# End of Evaluation" << endl;

      if (searchParameter.outputMode == "e+") {
	// show graph information
	size_t esize = searchParameter.edgeSize;
	long double distance = 0.0;
	size_t numberOfNodes = 0;
	size_t numberOfEdges = 0;

	NGT::GraphIndex	&graph = (NGT::GraphIndex&)index.getIndex();
	for (size_t id = 1; id < graph.repository.size(); id++) {
	  NGT::GraphNode *node = 0;
	  try {
	    node = graph.getNode(id);
	  } catch(NGT::Exception &err) {
	    cerr << "Graph::search: Warning. Cannot get the node. ID=" << id << ":" << err.what() << " If the node was removed, no problem." << endl;
	    continue;
	  }
	  numberOfNodes++;
	  if (numberOfNodes % 1000000 == 0) {
	    cerr << "Processed " << numberOfNodes << endl;
	  }
	  for (size_t i = 0; i < node->size(); i++) {
	    if (esize != 0 && i >= esize) {
	      break;
	    }
	    numberOfEdges++;
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	    distance += (*node).at(i, graph.repository.allocator).distance;
#else
	    distance += (*node)[i].distance;
#endif
	  }
	}

	stream << "# # of nodes=" << numberOfNodes << endl;
	stream << "# # of edges=" << numberOfEdges << endl;
	stream << "# Average number of edges=" << (double)numberOfEdges / (double)numberOfNodes << endl;
	stream << "# Average distance of edges=" << setprecision(10) << distance / (double)numberOfEdges << endl;
      }
    } else {
      stream << "Average Query Time= " << totalTime / (double)queryCount  << " (sec), " 
	   << totalTime * 1000.0 / (double)queryCount << " (msec), (" 
	   << totalTime << "/" << queryCount << ")" << endl;
    }
  }


  void
  NGT::Command::search(Args &args) {
    const string usage = "Usage: ngt search [-i index-type(g|t|s)] [-n result-size] [-e epsilon] [-E edge-size] "
      "[-m open-mode(r|w)] [-o output-mode] index(input) query.tsv(input)";

    string database;
    try {
      database = args.get("#1");
    } catch (...) {
      cerr << "ngt: Error: DB is not specified" << endl;
      cerr << usage << endl;
      return;
    }

    SearchParameter searchParameter(args);

    if (debugLevel >= 1) {
      cerr << "indexType=" << searchParameter.indexType << endl;
      cerr << "size=" << searchParameter.size << endl;
      cerr << "edgeSize=" << searchParameter.edgeSize << endl;
      cerr << "epsilon=" << searchParameter.beginOfEpsilon << "<->" << searchParameter.endOfEpsilon << "," 
	   << searchParameter.stepOfEpsilon << endl;
    }

    try {
      NGT::Index	index(database, searchParameter.openMode == 'r');
      search(index, searchParameter, cout);
    } catch (NGT::Exception &err) {
      cerr << "ngt: Error " << err.what() << endl;
      cerr << usage << endl;
    } catch (...) {
      cerr << "ngt: Error" << endl;
      cerr << usage << endl;
    }

  }


  void
  NGT::Command::remove(Args &args)
  {
    const string usage = "Usage: ngt remove [-d object-ID-type(f|d)] [-m f] index(input) object-ID(input)";
    string database;
    try {
      database = args.get("#1");
    } catch (...) {
      cerr << "ngt: Error: DB is not specified" << endl;
      cerr << usage << endl;
      return;
    }
    try {
      args.get("#2");
    } catch (...) {
      cerr << "ngt: Error: ID is not specified" << endl;
      cerr << usage << endl;
      return;
    }
    char dataType = args.getChar("d", 'f');
    char mode = args.getChar("m", '-');
    bool force = false;
    if (mode == 'f') {
      force = true;
    }
    if (debugLevel >= 1) {
      cerr << "dataType=" << dataType << endl;
    }

    try {
      vector<NGT::ObjectID> objects;
      if (dataType == 'f') {
	string ids;
	try {
	  ids = args.get("#2");
	} catch (...) {
	  cerr << "ngt: Error: Data file is not specified" << endl;
	  cerr << usage << endl;
	  return;
	}
	ifstream is(ids);
	if (!is) {
	  cerr << "ngt: Error: Cannot open the specified file. " << ids << endl;
	  cerr << usage << endl;
	  return;
	}
	string line;
	int count = 0;
	while(getline(is, line)) {
	  count++;
	  vector<string> tokens;
	  NGT::Common::tokenize(line, tokens, "\t ");
	  if (tokens.size() == 0 || tokens[0].size() == 0) {
	    continue;
	  }
	  char *e;
	  size_t id;
	  try {
	    id = strtol(tokens[0].c_str(), &e, 10);
	    objects.push_back(id);
	  } catch (...) {
	    cerr << "Illegal data. " << tokens[0] << endl;
	  }
	  if (*e != 0) {
	    cerr << "Illegal data. " << e << endl;
	  }
	  cerr << "removed ID=" << id << endl;	
	}
      } else {
	size_t id = args.getl("#2", 0);
	cerr << "removed ID=" << id << endl;
	objects.push_back(id);
      }
      NGT::Index::remove(database, objects, force);
    } catch (NGT::Exception &err) {
      cerr << "ngt: Error " << err.what() << endl;
      cerr << usage << endl;
    } catch (...) {
      cerr << "ngt: Error" << endl;
      cerr << usage << endl;
    }
  }

  void
  NGT::Command::exportIndex(Args &args)
  {
    const string usage = "Usage: ngt export index(input) export-file(output)";
    string database;
    try {
      database = args.get("#1");
    } catch (...) {
      cerr << "ngt: Error: DB is not specified" << endl;
      cerr << usage << endl;
      return;
    }
    string exportFile;
    try {
      exportFile = args.get("#2");
    } catch (...) {
      cerr << "ngt: Error: ID is not specified" << endl;
      cerr << usage << endl;
      return;
    }
    try {
      NGT::Index::exportIndex(database, exportFile);
    } catch (NGT::Exception &err) {
      cerr << "ngt: Error " << err.what() << endl;
      cerr << usage << endl;
    } catch (...) {
      cerr << "ngt: Error" << endl;
      cerr << usage << endl;
    }
  }

  void
  NGT::Command::importIndex(Args &args)
  {
    const string usage = "Usage: ngt import index(output) import-file(input)";
    string database;
    try {
      database = args.get("#1");
    } catch (...) {
      cerr << "ngt: Error: DB is not specified" << endl;
      cerr << usage << endl;
      return;
    }
    string importFile;
    try {
      importFile = args.get("#2");
    } catch (...) {
      cerr << "ngt: Error: ID is not specified" << endl;
      cerr << usage << endl;
      return;
    }

    try {
      NGT::Index::importIndex(database, importFile);
    } catch (NGT::Exception &err) {
      cerr << "ngt: Error " << err.what() << endl;
      cerr << usage << endl;
    } catch (...) {
      cerr << "ngt: Error" << endl;
      cerr << usage << endl;
    }

  }

  void
  NGT::Command::prune(Args &args)
  {
    const string usage = "Usage: ngt prune -e #-of-forcedly-pruned-edges -s #-of-selecively-pruned-edge index(in/out)";
    string indexName;
    try {
      indexName = args.get("#1");
    } catch (...) {
      cerr << "Index is not specified" << endl;
      cerr << usage << endl;
      return;
    }

    // the number of forcedly pruned edges
    size_t forcedlyPrunedEdgeSize	= args.getl("e", 0);
    // the number of selectively pruned edges
    size_t selectivelyPrunedEdgeSize	= args.getl("s", 0);

    cerr << "forcedly pruned edge size=" << forcedlyPrunedEdgeSize << endl;
    cerr << "selectively pruned edge size=" << selectivelyPrunedEdgeSize << endl;

    if (selectivelyPrunedEdgeSize == 0 && forcedlyPrunedEdgeSize == 0) {
      cerr << "prune: Error! Either of selective edge size or remaining edge size should be specified." << endl;
      cerr << usage << endl;
      return;
    }

    if (forcedlyPrunedEdgeSize != 0 && selectivelyPrunedEdgeSize != 0 && selectivelyPrunedEdgeSize >= forcedlyPrunedEdgeSize) {
      cerr << "prune: Error! selective edge size is less than remaining edge size." << endl;
      cerr << usage << endl;
      return;
    }

    NGT::Index	index(indexName);
    cerr << "loaded the input index." << endl;

    NGT::GraphIndex	&graph = (NGT::GraphIndex&)index.getIndex();

    for (size_t id = 1; id < graph.repository.size(); id++) {
      try {
	NGT::GraphNode &node = *graph.getNode(id);
	if (id % 1000000 == 0) {
	  cerr << "Processed " << id << endl;
	}
	if (forcedlyPrunedEdgeSize > 0 && node.size() >= forcedlyPrunedEdgeSize) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	  node.resize(forcedlyPrunedEdgeSize, graph.repository.allocator);
#else
	  node.resize(forcedlyPrunedEdgeSize);
#endif
	}
	if (selectivelyPrunedEdgeSize > 0 && node.size() >= selectivelyPrunedEdgeSize) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
	  cerr << "not implemented" << endl;
	  abort();
#else
	  size_t rank = 0;
	  for (NGT::GraphNode::iterator i = node.begin(); i != node.end(); ++rank) {
	    if (rank >= selectivelyPrunedEdgeSize) {
	      bool found = false;
	      for (size_t t1 = 0; t1 < node.size() && found == false; ++t1) {
		if (t1 >= selectivelyPrunedEdgeSize) {
		  break;
		}
		if (rank == t1) {
		  continue;
		}
		NGT::GraphNode &node2 = *graph.getNode(node[t1].id);
		for (size_t t2 = 0; t2 < node2.size(); ++t2) {		
		  if (t2 >= selectivelyPrunedEdgeSize) {
		    break;
		  }
		  if (node2[t2].id == (*i).id) {
		    found = true;
		    break;
		  }
		} // for
	      } // for
	      if (found) {
		//remove
		i = node.erase(i);
		continue;
	      }
	    }
	    i++;
	  } // for
#endif
	}
	  
      } catch(NGT::Exception &err) {
	cerr << "Graph::search: Warning. Cannot get the node. ID=" << id << ":" << err.what() << endl;
	continue;
      }
    }

    graph.saveIndex(indexName);

  }

  void
  NGT::Command::reconstructGraph(Args &args)
  {
    const string usage = "Usage: ngt reconstruct-graph [-m mode] [-P path-adjustment-mode] -o #-of-outgoing-edges -i #-of-incoming(reversed)-edges [-q #-of-queries] [-n #-of-results] index(input) index(output)\n"
      "\t-m mode\n"
      "\t\ts: Edge adjustment. (default)\n"
      "\t\tS: Edge adjustment and path adjustment.\n"
      "\t\tc: Edge adjustment with the constraint.\n"
      "\t\tC: Edge adjustment with the constraint and path adjustment.\n"
      "\t\tP: Path adjustment.\n"
      "\t-P path-adjustment-mode\n"
      "\t\ta: Advanced method. High-speed. Not guarantee the paper's method. (default)\n"
      "\t\tothers: Slow and less memory usage, but guarantee the paper's method.\n";

    string inIndexPath;
    try {
      inIndexPath = args.get("#1");
    } catch (...) {
      cerr << "ngt::reconstructGraph: Input index is not specified." << endl;
      cerr << usage << endl;
      return;
    }
    string outIndexPath;
    try {
      outIndexPath = args.get("#2");
    } catch (...) {
      cerr << "ngt::reconstructGraph: Output index is not specified." << endl;
      cerr << usage << endl;
      return;
    }

    char mode = args.getChar("m", 'S');
    size_t nOfQueries = args.getl("q", 100);		// # of query objects
    size_t nOfResults = args.getl("n", 20);		// # of resultant objects
    double gtEpsilon = args.getf("e", 0.1);
    double margin = args.getf("M", 0.2);
    char smode = args.getChar("s", '-');

    // the number (rank) of original edges
    int numOfOutgoingEdges	= args.getl("o", -1);
    // the number (rank) of reverse edges
    int numOfIncomingEdges		= args.getl("i", -1);

    NGT::GraphOptimizer graphOptimizer(false);

    if (mode == 'P') {
      numOfOutgoingEdges = 0;
      numOfIncomingEdges = 0;
      std::cerr << "ngt::reconstructGraph: Warning. \'-m P\' and not zero for # of in/out edges are specified at the same time." << std::endl;
    }
    graphOptimizer.shortcutReduction = (mode == 'S' || mode == 'C' || mode == 'P') ? true : false;
    graphOptimizer.searchParameterOptimization = (smode == '-' || smode == 's') ? true : false;
    graphOptimizer.prefetchParameterOptimization = (smode == '-' || smode == 'p') ? true : false;
    graphOptimizer.accuracyTableGeneration = (smode == '-' || smode == 'a') ? true : false;
    graphOptimizer.margin = margin;
    graphOptimizer.gtEpsilon = gtEpsilon;

    graphOptimizer.set(numOfOutgoingEdges, numOfIncomingEdges, nOfQueries, nOfResults);
    graphOptimizer.execute(inIndexPath, outIndexPath);

    std::cout << "Successfully completed." << std::endl;
  }

  void
  NGT::Command::optimizeSearchParameters(Args &args)
  {
    const string usage = "Usage: ngt optimize-search-parameters [-m optimization-target(s|p|a)] [-q #-of-queries] [-n #-of-results] index\n"
      "\t-m mode\n"
      "\t\ts: optimize search parameters (the number of explored edges).\n"
      "\t\tp: optimize prefetch prameters.\n"
      "\t\ta: generate an accuracy table to specify an expected accuracy instead of an epsilon for search.\n";
    
    string indexPath;
    try {
      indexPath = args.get("#1");
    } catch (...) {
      cerr << "Index is not specified" << endl;
      cerr << usage << endl;
      return;
    }

    char mode = args.getChar("m", '-');

    size_t nOfQueries = args.getl("q", 100);		// # of query objects
    size_t nOfResults = args.getl("n", 20);		// # of resultant objects


    try {
      NGT::GraphOptimizer graphOptimizer(false);

      graphOptimizer.searchParameterOptimization = (mode == '-' || mode == 's') ? true : false;
      graphOptimizer.prefetchParameterOptimization = (mode == '-' || mode == 'p') ? true : false;
      graphOptimizer.accuracyTableGeneration = (mode == '-' || mode == 'a') ? true : false;
      graphOptimizer.numOfQueries = nOfQueries;
      graphOptimizer.numOfResults = nOfResults;

      graphOptimizer.set(0, 0, nOfQueries, nOfResults);
      graphOptimizer.optimizeSearchParameters(indexPath);

      std::cout << "Successfully completed." << std::endl;
    } catch (NGT::Exception &err) {
      cerr << "ngt: Error " << err.what() << endl;
      cerr << usage << endl;
    }

  }

  void
  NGT::Command::refineANNG(Args &args)
  {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    std::cerr << "refineANNG. Not implemented." << std::endl;
    abort();
#else
    const string usage = "Usage: ngt refine-anng [-e epsilon] [-a expected-accuracy] anng-index refined-anng-index";

    string inIndexPath;
    try {
      inIndexPath = args.get("#1");
    } catch (...) {
      cerr << "Input index is not specified" << endl;
      cerr << usage << endl;
      return;
    }

    string outIndexPath;
    try {
      outIndexPath = args.get("#2");
    } catch (...) {
      cerr << "Output index is not specified" << endl;
      cerr << usage << endl;
      return;
    }

    NGT::Index	index(inIndexPath);

    float  epsilon		= args.getf("e", 0.1);
    float  expectedAccuracy	= args.getf("a", 0.0);
    int    noOfEdges		= args.getl("k", 0);	// to reconstruct kNNG
    int    exploreEdgeSize	= args.getf("E", INT_MIN);
    size_t batchSize		= args.getl("b", 10000);

    try {
      GraphReconstructor::refineANNG(index, epsilon, expectedAccuracy, noOfEdges, exploreEdgeSize, batchSize);
    } catch (NGT::Exception &err) {
      std::cerr << "Error!! Cannot refine the index. " << err.what() << std::endl;
      return;
    }
    index.saveIndex(outIndexPath);
#endif
  }

  void
  NGT::Command::repair(Args &args)
  {
    const string usage = "Usage: ng[ [-m c|r|R] repair index \n"
      "\t-m mode\n"
      "\t\tc: Check. (default)\n"
      "\t\tr: Repair and save it as [index].repair.\n"
      "\t\tR: Repair and overwrite into the specified index.\n";

    string indexPath;
    try {
      indexPath = args.get("#1");
    } catch (...) {
      cerr << "Index is not specified" << endl;
      cerr << usage << endl;
      return;
    }
    
    char mode = args.getChar("m", 'c');

    bool repair = false;
    if (mode == 'r' || mode == 'R') {
      repair = true;
    }
    string path = indexPath;
    if (mode == 'r') {
      path = indexPath + ".repair";
      const string com = "cp -r " + indexPath + " " + path;
      int stat = system(com.c_str());
      if (stat != 0) {
	std::cerr << "ngt::repair: Cannot create the specified index. " << path << std::endl;
	cerr << usage << endl;
	return;
      }
    }

    NGT::Index	index(path);

    NGT::ObjectRepository &objectRepository = index.getObjectSpace().getRepository();
    NGT::GraphIndex &graphIndex = static_cast<GraphIndex&>(index.getIndex());
    NGT::GraphAndTreeIndex &graphAndTreeIndex = static_cast<GraphAndTreeIndex&>(index.getIndex());
    size_t objSize = objectRepository.size();
    std::cerr << "aggregate removed objects from the repository." << std::endl;
    std::set<ObjectID> removedIDs;
    for (ObjectID id = 1; id < objSize; id++) {
      if (objectRepository.isEmpty(id)) {
	removedIDs.insert(id);
      }
    }

    std::cerr << "aggregate objects from the tree." << std::endl;
    std::set<ObjectID> ids;
    graphAndTreeIndex.DVPTree::getAllObjectIDs(ids);
    size_t idsSize = ids.size() == 0 ? 0 : (*ids.rbegin()) + 1;
    if (objSize < idsSize) {
      std::cerr << "The sizes of the repository and tree are inconsistent. " << objSize << ":" << idsSize << std::endl;
    }
    size_t invalidTreeObjectCount = 0;
    size_t uninsertedTreeObjectCount = 0;
    std::cerr << "remove invalid objects from the tree." << std::endl;
    size_t size = objSize > idsSize ? objSize : idsSize;
    for (size_t id = 1; id < size; id++) {    
      if (ids.find(id) != ids.end()) {
	if (removedIDs.find(id) != removedIDs.end() || id >= objSize) {
	  if (repair) {
	    graphAndTreeIndex.DVPTree::removeNaively(id);
	    std::cerr << "Found the removed object in the tree. Removed it from the tree. " << id << std::endl;
	  } else {
	    std::cerr << "Found the removed object in the tree. " << id << std::endl;
	  }
	  invalidTreeObjectCount++;
	}
      } else {
	if (removedIDs.find(id) == removedIDs.end() && id < objSize) {
          std::cerr << "Not found an object in the tree. However, it might be a duplicated object. " << id << std::endl;
	  uninsertedTreeObjectCount++;
	  try {
	    graphIndex.repository.remove(id);
	  } catch(...) {}
	}
      }
    }

    if (objSize != graphIndex.repository.size()) {
      std::cerr << "The sizes of the repository and graph are inconsistent. " << objSize << ":" << graphIndex.repository.size() << std::endl;
    }
    size_t invalidGraphObjectCount = 0;
    size_t uninsertedGraphObjectCount = 0;
    size = objSize > graphIndex.repository.size() ? objSize : graphIndex.repository.size();
    std::cerr << "remove invalid objects from the graph." << std::endl;
    for (size_t id = 1; id < size; id++) {
      try {
	graphIndex.getNode(id);
	if (removedIDs.find(id) != removedIDs.end() || id >= objSize) {
	  if (repair) {
	    graphAndTreeIndex.DVPTree::removeNaively(id);
	    try {
	      graphIndex.repository.remove(id);
	    } catch(...) {}
	    std::cerr << "Found the removed object in the graph. Removed it from the graph. " << id << std::endl;
	  } else {
	    std::cerr << "Found the removed object in the graph. " << id << std::endl;
	  }
	  invalidGraphObjectCount++;
	}
      } catch (...) {
        if (removedIDs.find(id) == removedIDs.end() && id < objSize) {
          std::cerr << "Not found an object in the graph. It should be inserted into the graph. " << id << std::endl;
	  uninsertedGraphObjectCount++;
	  try {
	    graphAndTreeIndex.DVPTree::removeNaively(id);
	  } catch(...) {}
	}
      }
    }

    size_t invalidEdgeCount = 0;
//#pragma omp parallel for
    for (size_t id = 1; id < graphIndex.repository.size(); id++) {
      try {
        NGT::GraphNode &node = *graphIndex.getNode(id);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	for (auto n = node.begin(graphIndex.repository.allocator); n != node.end(graphIndex.repository.allocator);) {
#else
	for (auto n = node.begin(); n != node.end();) {
#endif
	  if (removedIDs.find((*n).id) != removedIDs.end() || (*n).id >= objSize) {

	    std::cerr << "Not found the destination object of the edge. " << id << ":" << (*n).id << std::endl;
	    invalidEdgeCount++;
            if (repair) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	      n = node.erase(n, graphIndex.repository.allocator);
#else
	      n = node.erase(n);
#endif
	      continue;
	    }
	  }
	  ++n;
	}
      } catch(...) {}
    }

    if (repair) {
      if (objSize < graphIndex.repository.size()) {
	graphIndex.repository.resize(objSize);
      }
    }

    std::cerr << "The number of invalid tree objects=" << invalidTreeObjectCount << std::endl;
    std::cerr << "The number of invalid graph objects=" << invalidGraphObjectCount << std::endl;
    std::cerr << "The number of uninserted tree objects (Can be ignored)=" << uninsertedTreeObjectCount << std::endl;
    std::cerr << "The number of uninserted graph objects=" << uninsertedGraphObjectCount << std::endl;
    std::cerr << "The number of invalid edges=" << invalidEdgeCount << std::endl;

    if (repair) {
      try {
	if (uninsertedGraphObjectCount > 0) {
	  std::cerr << "Building index." << std::endl;
	  index.createIndex(16);
	}
	std::cerr << "Saving index." << std::endl;
	index.saveIndex(path);
      } catch (NGT::Exception &err) {
	cerr << "ngt: Error " << err.what() << endl;
	cerr << usage << endl;
	return;
      }
    }
  }


  void
  NGT::Command::optimizeNumberOfEdgesForANNG(Args &args)
  {
    const string usage = "Usage: ngt optimize-#-of-edges [-q #-of-queries] [-k #-of-retrieved-objects] "
      "[-p #-of-threads] [-a target-accuracy] [-o target-#-of-objects] [-s #-of-sampe-objects] "
      "[-e maximum-#-of-edges] anng-index";

    string indexPath;
    try {
      indexPath = args.get("#1");
    } catch (...) {
      cerr << "Index is not specified" << endl;
      cerr << usage << endl;
      return;
    }

    GraphOptimizer::ANNGEdgeOptimizationParameter parameter;

    parameter.noOfQueries	= args.getl("q", 200);
    parameter.noOfResults	= args.getl("k", 50);
    parameter.noOfThreads	= args.getl("p", 16);
    parameter.targetAccuracy	= args.getf("a", 0.9);
    parameter.targetNoOfObjects	= args.getl("o", 0);	// zero will replaced # of the repository size.
    parameter.noOfSampleObjects	= args.getl("s", 100000);
    parameter.maxNoOfEdges	= args.getl("e", 100);

    NGT::GraphOptimizer graphOptimizer(false); // false=log
    auto optimizedEdge = graphOptimizer.optimizeNumberOfEdgesForANNG(indexPath, parameter);
    std::cout << "The optimized # of edges=" << optimizedEdge.first << "(" << optimizedEdge.second << ")" << std::endl;
    std::cout << "Successfully completed." << std::endl;
  }



  void
  NGT::Command::info(Args &args)
  {
    const string usage = "Usage: ngt info [-E #-of-edges] [-m h|e] index";

    cerr << "NGT version: " << NGT::Index::getVersion() << endl;

    string database;
    try {
      database = args.get("#1");
    } catch (...) {
      cerr << "ngt: Error: DB is not specified" << endl;
      cerr << usage << endl;
      return;
    }

    size_t edgeSize = args.getl("E", UINT_MAX);
    char mode = args.getChar("m", '-');

    try {
      NGT::Index	index(database);
      NGT::GraphIndex::showStatisticsOfGraph(static_cast<NGT::GraphIndex&>(index.getIndex()), mode, edgeSize);
      if (mode == 'v') {
	vector<uint8_t> status;
	index.verify(status);
      }
    } catch (NGT::Exception &err) {
      cerr << "ngt: Error " << err.what() << endl;
      cerr << usage << endl;
    } catch (...) {
      cerr << "ngt: Error" << endl;
      cerr << usage << endl;
    }
  }

