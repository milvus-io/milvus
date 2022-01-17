//
// Copyright (C) 2016-2020 Yahoo Japan Corporation
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

#include	"NGT/NGTQ/Quantizer.h"

#define NGTQ_SEARCH_CODEBOOK_SIZE_FLUCTUATION	

namespace NGTQ {

class Command {
public:
  Command():debugLevel(0) {}

  void 
  create(NGT::Args &args)
  {
    const string usage = "Usage: ngtq create "
      "[-o object-type (f:float|c:unsigned char)] [-D distance-function] [-n data-size] "
      "[-p #-of-thread] [-d dimension] [-R global-codebook-range] [-r local-codebook-range] "
      "[-C global-codebook-size-limit] [-c local-codebook-size-limit] [-N local-division-no] "
      "[-T single-local-centroid (t|f)] [-e epsilon] [-i index-type (t:Tree|g:Graph)] "
      "[-M global-centroid-creation-mode (d|s)] [-L global-centroid-creation-mode (d|k|s)] "
      "[-S local-sample-coefficient] "
      "index(output) data.tsv(input)";
    string database;
    try {
      database = args.get("#1");
    } catch (...) {
      cerr << "DB is not specified." << endl;
      cerr << usage << endl;
      return;
    }
    string data;
    try {
      data = args.get("#2");
    } catch (...) {
      cerr << "Data is not specified." << endl;
    }

    char objectType = args.getChar("o", 'f');
    char distanceType = args.getChar("D", '2');
    size_t dataSize = args.getl("n", 0);

    NGTQ::Property property;
    property.threadSize = args.getl("p", 24);
    property.dimension = args.getl("d", 0);
    property.globalRange = args.getf("R", 0);
    property.localRange = args.getf("r", 0);
    property.globalCentroidLimit = args.getl("C", 1000000);
    property.localCentroidLimit = args.getl("c", 65000);
    property.localDivisionNo = args.getl("N", 8);
    property.batchSize = args.getl("b", 1000);
    property.localClusteringSampleCoefficient = args.getl("S", 10);
    {
      char localCentroidType = args.getChar("T", 'f');
      property.singleLocalCodebook = localCentroidType == 't' ? true : false;
    }
    {
      char centroidCreationMode = args.getChar("M", 'd');
      switch(centroidCreationMode) {
      case 'd': property.centroidCreationMode = NGTQ::CentroidCreationModeDynamic; break;
      case 's': property.centroidCreationMode = NGTQ::CentroidCreationModeStatic; break;
      default:
	cerr << "ngt: Invalid centroid creation mode. " << centroidCreationMode << endl;
	cerr << usage << endl;
	return;
      }
    }
    {
      char localCentroidCreationMode = args.getChar("L", 'd');
      switch(localCentroidCreationMode) {
      case 'd': property.localCentroidCreationMode = NGTQ::CentroidCreationModeDynamic; break;
      case 's': property.localCentroidCreationMode = NGTQ::CentroidCreationModeStatic; break;
      case 'k': property.localCentroidCreationMode = NGTQ::CentroidCreationModeDynamicKmeans; break;
      default:
	cerr << "ngt: Invalid centroid creation mode. " << localCentroidCreationMode << endl;
	cerr << usage << endl;
	return;
      }
    }

    NGT::Property globalProperty;
    NGT::Property localProperty;

    {
      char indexType = args.getChar("i", 't');
      globalProperty.indexType = indexType == 't' ? NGT::Property::GraphAndTree : NGT::Property::Graph;
      localProperty.indexType = globalProperty.indexType;
    }
    globalProperty.insertionRadiusCoefficient = args.getf("e", 0.1) + 1.0;
    localProperty.insertionRadiusCoefficient = globalProperty.insertionRadiusCoefficient;

    if (debugLevel >= 1) {
      cerr << "epsilon=" << globalProperty.insertionRadiusCoefficient << endl;
      cerr << "data size=" << dataSize << endl;
      cerr << "dimension=" << property.dimension << endl;
      cerr << "thread size=" << property.threadSize << endl;
      cerr << "batch size=" << localProperty.batchSizeForCreation << endl;;
      cerr << "index type=" << globalProperty.indexType << endl;
    }


    switch (objectType) {
    case 'f': property.dataType = NGTQ::DataTypeFloat; break;
    case 'c': property.dataType = NGTQ::DataTypeUint8; break;
    default:
      cerr << "ngt: Invalid object type. " << objectType << endl;
      cerr << usage << endl;
      return;
    }

    switch (distanceType) {
    case '2': property.distanceType = NGTQ::DistanceTypeL2; break;
    case '1': property.distanceType = NGTQ::DistanceTypeL1; break;
    case 'a': property.distanceType = NGTQ::DistanceTypeAngle; break;
    default:
      cerr << "ngt: Invalid distance type. " << distanceType << endl;
      cerr << usage << endl;
      return;
    }

    cerr << "ngtq: Create" << endl;
    NGTQ::Index::create(database, property, globalProperty, localProperty);

    cerr << "ngtq: Append" << endl;
    NGTQ::Index::append(database, data, dataSize);
  }

  void 
  rebuild(NGT::Args &args)
  {
    const string usage = "Usage: ngtq rebuild "
      "[-o object-type (f:float|c:unsigned char)] [-D distance-function] [-n data-size] "
      "[-p #-of-thread] [-d dimension] [-R global-codebook-range] [-r local-codebook-range] "
      "[-C global-codebook-size-limit] [-c local-codebook-size-limit] [-N local-division-no] "
      "[-T single-local-centroid (t|f)] [-e epsilon] [-i index-type (t:Tree|g:Graph)] "
      "[-M centroid-creation_mode (d|s)] "
      "index(output) data.tsv(input)";
    string srcIndex;
    try {
      srcIndex = args.get("#1");
    } catch (...) {
      cerr << "DB is not specified." << endl;
      cerr << usage << endl;
      return;
    }
    string rebuiltIndex = srcIndex + ".tmp";


    NGTQ::Property property;
    NGT::Property globalProperty;
    NGT::Property localProperty;

    {
      NGTQ::Index index(srcIndex);
      property = index.getQuantizer().property;
      index.getQuantizer().globalCodebook.getProperty(globalProperty);
      index.getQuantizer().getLocalCodebook(0).getProperty(localProperty);
    }

    property.globalRange = args.getf("R", property.globalRange);
    property.localRange = args.getf("r", property.localRange);
    property.globalCentroidLimit = args.getl("C", property.globalCentroidLimit);
    property.localCentroidLimit = args.getl("c", property.localCentroidLimit);
    property.localDivisionNo = args.getl("N", property.localDivisionNo);
    {
      char localCentroidType = args.getChar("T", '-');
      if (localCentroidType != '-') {
	property.singleLocalCodebook = localCentroidType == 't' ? true : false;
      }
    }
    {
      char centroidCreationMode = args.getChar("M", '-');
      if (centroidCreationMode != '-') {
	property.centroidCreationMode = centroidCreationMode == 'd' ? 
	  NGTQ::CentroidCreationModeDynamic : NGTQ::CentroidCreationModeStatic;
      }
    }

    cerr << "global range=" << property.globalRange << endl;
    cerr << "local range=" << property.localRange << endl;
    cerr << "global centroid limit=" << property.globalCentroidLimit << endl;
    cerr << "local centroid limit=" << property.localCentroidLimit << endl;
    cerr << "local division no=" << property.localDivisionNo << endl;

    NGTQ::Index::create(rebuiltIndex, property, globalProperty, localProperty);
    cerr << "created a new db" << endl;
    cerr << "start rebuilding..." << endl;
    NGTQ::Index::rebuild(srcIndex, rebuiltIndex);
    {
      string src = srcIndex;
      string dst = srcIndex + ".org";
      if (std::rename(src.c_str(), dst.c_str()) != 0) {
        stringstream msg;
        msg << "ngtq::rebuild: Cannot rename. " << src << "=>" << dst ;
        NGTThrowException(msg);
      }
    }
    {
      string src = rebuiltIndex;
      string dst = srcIndex;
      if (std::rename(src.c_str(), dst.c_str()) != 0) {
        stringstream msg;
        msg << "ngtq::rebuild: Cannot rename. " << src << "=>" << dst ;
        NGTThrowException(msg);
      }
    }
  }


  void 
  append(NGT::Args &args)
  {
    const string usage = "Usage: ngtq append [-n data-size] "
      "index(output) data.tsv(input)";
    string index;
    try {
      index = args.get("#1");
    } catch (...) {
      cerr << "DB is not specified." << endl;
      cerr << usage << endl;
      return;
    }
    string data;
    try {
      data = args.get("#2");
    } catch (...) {
      cerr << "Data is not specified." << endl;
    }

    size_t dataSize = args.getl("n", 0);

    if (debugLevel >= 1) {
      cerr << "data size=" << dataSize << endl;
    }

    NGTQ::Index::append(index, data, dataSize);

  }

  void
  search(NGT::Args &args)
  {
    const string usage = "Usage: ngtq search [-i g|t|s] [-n result-size] [-e epsilon] [-m mode(r|l|c|a)] "
      "[-E edge-size] [-o output-mode] [-b result expansion(begin:end:[x]step)] "
      "index(input) query.tsv(input)";
    string database;
    try {
      database = args.get("#1");
    } catch (...) {
      cerr << "DB is not specified" << endl;
      cerr << usage << endl;
      return;
    }

    string query;
    try {
      query = args.get("#2");
    } catch (...) {
      cerr << "Query is not specified" << endl;
      cerr << usage << endl;
      return;
    }

    int size		= args.getl("n", 20);
    char outputMode	= args.getChar("o", '-');
    float epsilon	= 0.1;

    char mode		= args.getChar("m", '-');
    NGTQ::AggregationMode aggregationMode;
    switch (mode) {
    case 'r': aggregationMode = NGTQ::AggregationModeExactDistanceThroughApproximateDistance; break; // refine
    case 'e': aggregationMode = NGTQ::AggregationModeExactDistance; break; // refine
    case 'l': aggregationMode = NGTQ::AggregationModeApproximateDistanceWithLookupTable; break; // lookup
    case 'c': aggregationMode = NGTQ::AggregationModeApproximateDistanceWithCache; break; // cache
    case '-':
    case 'a': aggregationMode = NGTQ::AggregationModeApproximateDistance; break; // cache
    default: 
      cerr << "Invalid aggregation mode. " << mode << endl;
      cerr << usage << endl;
      return;
    }

    if (args.getString("e", "none") == "-") {
      // linear search
      epsilon = FLT_MAX;
    } else {
      epsilon = args.getf("e", 0.1);
    }

    size_t beginOfResultExpansion, endOfResultExpansion, stepOfResultExpansion;
    bool mulStep = false;
    {
      beginOfResultExpansion = stepOfResultExpansion = 1;
      endOfResultExpansion = 0;
      string str = args.getString("b", "16");
      vector<string> tokens;
      NGT::Common::tokenize(str, tokens, ":");
      if (tokens.size() >= 1) { beginOfResultExpansion = NGT::Common::strtod(tokens[0]); }
      if (tokens.size() >= 2) { endOfResultExpansion = NGT::Common::strtod(tokens[1]); }
      if (tokens.size() >= 3) { 
	if (tokens[2][0] == 'x') {
	  mulStep = true;
	  stepOfResultExpansion = NGT::Common::strtod(tokens[2].substr(1)); 
	} else {
	  stepOfResultExpansion = NGT::Common::strtod(tokens[2]); 
	}
      }
    }
    if (debugLevel >= 1) {
      cerr << "size=" << size << endl;
      cerr << "result expansion=" << beginOfResultExpansion << "->" << endOfResultExpansion << "," << stepOfResultExpansion << endl;
    }

    NGTQ::Index index(database);
    try {
      ifstream		is(query);
      if (!is) {
	cerr << "Cannot open the specified file. " << query << endl;
	return;
      }
      if (outputMode == 's') { cout << "# Beginning of Evaluation" << endl; }
      string line;
      double totalTime = 0;
      int queryCount = 0;
      while(getline(is, line)) {
	NGT::Object *query = index.allocateObject(line, " \t", 0);
	queryCount++;
	size_t resultExpansion = 0;
	for (size_t base = beginOfResultExpansion; 
	     resultExpansion <= endOfResultExpansion; 
	     base = mulStep ? base * stepOfResultExpansion : base + stepOfResultExpansion) {
	  resultExpansion = base;
	  NGT::ObjectDistances objects;

	  if (outputMode == 'e') {
	    index.search(query, objects, size, resultExpansion, aggregationMode, epsilon);
	    objects.clear();
	  }

	  NGT::Timer timer;
	  timer.start();
	  // size : # of final resultant objects 
	  // resultExpansion : # of resultant objects by using codebook search
	  index.search(query, objects, size, resultExpansion, aggregationMode, epsilon);
	  timer.stop();

	  totalTime += timer.time;
	  if (outputMode == 'e') {
	    cout << "# Query No.=" << queryCount << endl;
	    cout << "# Query=" << line.substr(0, 20) + " ..." << endl;
	    cout << "# Index Type=" << "----" << endl;
	    cout << "# Size=" << size << endl;
	    cout << "# Epsilon=" << epsilon << endl;
	    cout << "# Result expansion=" << resultExpansion << endl;
	    cout << "# Distance Computation=" << index.getQuantizer().distanceComputationCount << endl;
	    cout << "# Query Time (msec)=" << timer.time * 1000.0 << endl;
	  } else {
	    cout << "Query No." << queryCount << endl;
	    cout << "Rank\tIN-ID\tID\tDistance" << endl;
	  }

	  for (size_t i = 0; i < objects.size(); i++) {
	    cout << i + 1 << "\t" << objects[i].id << "\t";
	    cout << objects[i].distance << endl;
	  }

	  if (outputMode == 'e') {
	    cout << "# End of Search" << endl;
	  } else {
	    cout << "Query Time= " << timer.time << " (sec), " << timer.time * 1000.0 << " (msec)" << endl;
	  }
	} 
	if (outputMode == 'e') {
	  cout << "# End of Query" << endl;
	}
	index.deleteObject(query);
      } 
      if (outputMode == 'e') {
	cout << "# Average Query Time (msec)=" << totalTime * 1000.0 / (double)queryCount << endl;
	cout << "# Number of queries=" << queryCount << endl;
	cout << "# End of Evaluation" << endl;
      } else {
	cout << "Average Query Time= " << totalTime / (double)queryCount  << " (sec), " 
	     << totalTime * 1000.0 / (double)queryCount << " (msec), (" 
	     << totalTime << "/" << queryCount << ")" << endl;
      }
    } catch (NGT::Exception &err) {
      cerr << "Error " << err.what() << endl;
      cerr << usage << endl;
    } catch (...) {
      cerr << "Error" << endl;
      cerr << usage << endl;
    }
    index.close();
  }

  void
  remove(NGT::Args &args)
  {
    const string usage = "Usage: ngtq remove [-d object-ID-type(f|d)] index(input) object-ID(input)";
    string database;
    try {
      database = args.get("#1");
    } catch (...) {
      cerr << "DB is not specified" << endl;
      cerr << usage << endl;
      return;
    }
    try {
      args.get("#2");
    } catch (...) {
      cerr << "ID is not specified" << endl;
      cerr << usage << endl;
      return;
    }
    char dataType = args.getChar("d", 'f');
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
	  cerr << "Data file is not specified" << endl;
	  cerr << usage << endl;
	  return;
	}
	ifstream is(ids);
	if (!is) {
	  cerr << "Cannot open the specified file. " << ids << endl;
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
      NGT::Index::remove(database, objects);
    } catch (NGT::Exception &err) {
      cerr << "Error " << err.what() << endl;
      cerr << usage << endl;
    } catch (...) {
      cerr << "Error" << endl;
      cerr << usage << endl;
    }
  }


  void
  info(NGT::Args &args)
  {
    const string usage = "Usage: ngtq info index";
    string database;
    try {
      database = args.get("#1");
    } catch (...) {
      cerr << "DB is not specified" << endl;
      cerr << usage << endl;
      return;
    }
    NGTQ::Index index(database);
    index.info(cout);

  }

  void
  validate(NGT::Args &args)
  {
    const string usage = "parameter";
    string database;
    try {
      database = args.get("#1");
    } catch (...) {
      cerr << "DB is not specified" << endl;
      cerr << usage << endl;
      return;
    }
    NGTQ::Index index(database);

    index.getQuantizer().validate();

  }
  


#ifdef NGTQ_SHARED_INVERTED_INDEX
  void
  compress(NGT::Args &args)
  {
    const string usage = "Usage: ngtq compress index)";
    string database;
    try {
      database = args.get("#1");
    } catch (...) {
      cerr << "DB is not specified" << endl;
      cerr << usage << endl;
      return;
    }
    try {
      NGTQ::Index::compress(database);
    } catch (NGT::Exception &err) {
      cerr << "Error " << err.what() << endl;
      cerr << usage << endl;
    } catch (...) {
      cerr << "Error" << endl;
      cerr << usage << endl;
    }
  }
#endif

  void help() {
    cerr << "Usage : ngtq command database data" << endl;
    cerr << "           command : create search remove append export import" << endl;
  }

  void execute(NGT::Args args) {
    string command;
    try {
      command = args.get("#0");
    } catch(...) {
      help();
      return;
    }

    debugLevel = args.getl("X", 0);

    try {
      if (debugLevel >= 1) {
	cerr << "ngt::command=" << command << endl;
      }
      if (command == "search") {
	search(args);
      } else if (command == "create") {
	create(args);
      } else if (command == "append") {
	append(args);
      } else if (command == "remove") {
	remove(args);
      } else if (command == "info") {
	info(args);
      } else if (command == "validate") {
	validate(args);
      } else if (command == "rebuild") {
	rebuild(args);
#ifdef NGTQ_SHARED_INVERTED_INDEX
      } else if (command == "compress") {
	compress(args);
#endif
      } else {
	cerr << "Illegal command. " << command << endl;
      }
    } catch(NGT::Exception &err) {
      cerr << "ngt: Fatal error: " << err.what() << endl;
    }
  }

  int debugLevel;

};

};

