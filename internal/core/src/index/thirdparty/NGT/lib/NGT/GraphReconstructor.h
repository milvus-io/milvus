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

#include	<unordered_map>
#include	<unordered_set>
#include	<list>
#include "defines.h"

#ifdef _OPENMP
#include	<omp.h>
#else
#warning "*** OMP is *NOT* available! ***"
#endif

namespace NGT {

class GraphReconstructor {
 public:
  static void extractGraph(std::vector<NGT::ObjectDistances> &graph, NGT::GraphIndex &graphIndex) {
    graph.reserve(graphIndex.repository.size());
    for (size_t id = 1; id < graphIndex.repository.size(); id++) {
      if (id % 1000000 == 0) {
      	if (NGT_LOG_DEBUG_)
			(*NGT_LOG_DEBUG_)("GraphReconstructor::extractGraph: Processed " + std::to_string(id) + " objects.");
//      	std::cerr << "GraphReconstructor::extractGraph: Processed " << id << " objects." << std::endl;
      }
      try {
	NGT::GraphNode &node = *graphIndex.getNode(id);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	NGT::ObjectDistances nd;
	nd.reserve(node.size());
	for (auto n = node.begin(graphIndex.repository.allocator); n != node.end(graphIndex.repository.allocator); ++n) {
	  nd.push_back(ObjectDistance((*n).id, (*n).distance));
        }
	graph.push_back(nd);
#else
	graph.push_back(node);
#endif
	if (graph.back().size() != graph.back().capacity()) {
		if (NGT_LOG_DEBUG_)
			(*NGT_LOG_DEBUG_)("GraphReconstructor::extractGraph: Warning! The graph size must be the same as the capacity. " + std::to_string(id));
//		std::cerr << "GraphReconstructor::extractGraph: Warning! The graph size must be the same as the capacity. " << id << std::endl;
	}
      } catch(NGT::Exception &err) {
	graph.push_back(NGT::ObjectDistances());
	continue;
      }
    }

  }




  static void 
    adjustPaths(NGT::Index &outIndex)
  {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	  if (NGT_LOG_DEBUG_)
			(*NGT_LOG_DEBUG_)("construct index is not implemented.");
//    std::cerr << "construct index is not implemented." << std::endl;
    exit(1);
#else
    NGT::GraphIndex	&outGraph = dynamic_cast<NGT::GraphIndex&>(outIndex.getIndex());
    size_t rStartRank = 0; 
    std::list<std::pair<size_t, NGT::GraphNode> > tmpGraph;
    for (size_t id = 1; id < outGraph.repository.size(); id++) {
      NGT::GraphNode &node = *outGraph.getNode(id);
      tmpGraph.push_back(std::pair<size_t, NGT::GraphNode>(id, node));
      if (node.size() > rStartRank) {
	node.resize(rStartRank);
      }
    }
    size_t removeCount = 0;
    for (size_t rank = rStartRank; ; rank++) {
      bool edge = false;
      Timer timer;
      for (auto it = tmpGraph.begin(); it != tmpGraph.end();) {
	size_t id = (*it).first;
	try {
	  NGT::GraphNode &node = (*it).second;
	  if (rank >= node.size()) {
	    it = tmpGraph.erase(it);
	    continue;
	  }
	  edge = true;
	  if (rank >= 1 && node[rank - 1].distance > node[rank].distance) {
//	  	std::cerr << "distance order is wrong!" << std::endl;
//	    std::cerr << id << ":" << rank << ":" << node[rank - 1].id << ":" << node[rank].id << std::endl;
	    if (NGT_LOG_DEBUG_) {
			(*NGT_LOG_DEBUG_)("distance order is wrong!");
			(*NGT_LOG_DEBUG_)(std::to_string(id) + ":" + std::to_string(rank) + ":" + std::to_string(node[rank - 1].id) + ":" + std::to_string(node[rank].id));
	    }
	  }
	  NGT::GraphNode &tn = *outGraph.getNode(id);
	  volatile bool found = false;
	  if (rank < 1000) {
	    for (size_t tni = 0; tni < tn.size() && !found; tni++) {
	      if (tn[tni].id == node[rank].id) {
		continue;
	      }
	      NGT::GraphNode &dstNode = *outGraph.getNode(tn[tni].id);
	      for (size_t dni = 0; dni < dstNode.size(); dni++) {
		if ((dstNode[dni].id == node[rank].id) && (dstNode[dni].distance < node[rank].distance)) {
		  found = true;
		  break;
		}
	      } 
	    }  
	  } else {
#ifdef _OPENMP
#pragma omp parallel for num_threads(10)
#endif
	    for (size_t tni = 0; tni < tn.size(); tni++) {
	      if (found) {
		continue;
	      }
	      if (tn[tni].id == node[rank].id) {
		continue;
	      }
	      NGT::GraphNode &dstNode = *outGraph.getNode(tn[tni].id);
	      for (size_t dni = 0; dni < dstNode.size(); dni++) {
		if ((dstNode[dni].id == node[rank].id) && (dstNode[dni].distance < node[rank].distance)) {
		  found = true;
		}
	      } 
	    } 
	  } 
	  if (!found) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	    outGraph.addEdge(id, node.at(i, outGraph.repository.allocator).id,
			     node.at(i, outGraph.repository.allocator).distance, true);
#else
	    tn.push_back(NGT::ObjectDistance(node[rank].id, node[rank].distance));
#endif
	  } else {
	    removeCount++;
	  }
	} catch(NGT::Exception &err) {
//	  std::cerr << "GraphReconstructor: Warning. Cannot get the node. ID=" << id << ":" << err.what() << std::endl;
	  if (NGT_LOG_DEBUG_)
		  (*NGT_LOG_DEBUG_)("GraphReconstructor: Warning. Cannot get the node. ID=" + std::to_string(id) + ":" + err.what());
	  it++;
	  continue;
	}
	it++;
      } 
      if (edge == false) {
	break;
      }
    } 
#endif // NGT_SHARED_MEMORY_ALLOCATOR
  }

  static void 
    adjustPathsEffectively(NGT::Index &outIndex)
  {
    NGT::GraphIndex	&outGraph = dynamic_cast<NGT::GraphIndex&>(outIndex.getIndex());
    adjustPathsEffectively(outGraph);
  }

  static bool edgeComp(NGT::ObjectDistance a, NGT::ObjectDistance b) {
    return a.id < b.id;
  }

#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
  static void insert(NGT::GraphNode &node, size_t edgeID, NGT::Distance edgeDistance, NGT::GraphIndex &graph) {
    NGT::ObjectDistance edge(edgeID, edgeDistance);
    GraphNode::iterator ni = std::lower_bound(node.begin(graph.repository.allocator), node.end(graph.repository.allocator), edge, edgeComp);
    node.insert(ni, edge, graph.repository.allocator);
  }

  static bool hasEdge(NGT::GraphIndex &graph, size_t srcNodeID, size_t dstNodeID) 
  {
     NGT::GraphNode &srcNode = *graph.getNode(srcNodeID);
     GraphNode::iterator ni = std::lower_bound(srcNode.begin(graph.repository.allocator), srcNode.end(graph.repository.allocator), ObjectDistance(dstNodeID, 0.0), edgeComp);
     return (ni != srcNode.end(graph.repository.allocator)) && ((*ni).id == dstNodeID);
  }
#else
  static void insert(NGT::GraphNode &node, size_t edgeID, NGT::Distance edgeDistance) {
    NGT::ObjectDistance edge(edgeID, edgeDistance);
    GraphNode::iterator ni = std::lower_bound(node.begin(), node.end(), edge, edgeComp);
    node.insert(ni, edge);
  }

  static bool hasEdge(NGT::GraphIndex &graph, size_t srcNodeID, size_t dstNodeID) 
  {
     NGT::GraphNode &srcNode = *graph.getNode(srcNodeID);
     GraphNode::iterator ni = std::lower_bound(srcNode.begin(), srcNode.end(), ObjectDistance(dstNodeID, 0.0), edgeComp);
     return (ni != srcNode.end()) && ((*ni).id == dstNodeID);
  }
#endif


  static void 
    adjustPathsEffectively(NGT::GraphIndex &outGraph)
  {
    Timer timer;
    timer.start();
    std::vector<NGT::GraphNode> tmpGraph;
    for (size_t id = 1; id < outGraph.repository.size(); id++) {
      try {
	NGT::GraphNode &node = *outGraph.getNode(id);
	tmpGraph.push_back(node);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	node.clear(outGraph.repository.allocator);
#else
	node.clear();
#endif
      } catch(NGT::Exception &err) {
//	std::cerr << "GraphReconstructor: Warning. Cannot get the node. ID=" << id << ":" << err.what() << std::endl;
		if (NGT_LOG_DEBUG_)
			(*NGT_LOG_DEBUG_)("GraphReconstructor: Warning. Cannot get the node. ID=" + std::to_string(id) + ":" + err.what());
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	tmpGraph.push_back(NGT::GraphNode(outGraph.repository.allocator));
#else
	tmpGraph.push_back(NGT::GraphNode());
#endif
      }
    }
    if (outGraph.repository.size() != tmpGraph.size() + 1) {
      std::stringstream msg;
      msg << "GraphReconstructor: Fatal inner error. " << outGraph.repository.size() << ":" << tmpGraph.size();
      NGTThrowException(msg);
    }
    timer.stop();
//    std::cerr << "GraphReconstructor::adjustPaths: graph preparing time=" << timer << std::endl;
	if (NGT_LOG_DEBUG_)
		(*NGT_LOG_DEBUG_)("GraphReconstructor::adjustPaths: graph preparing time=" + std::to_string(timer.time));
    timer.reset();
    timer.start();

    std::vector<std::vector<std::pair<uint32_t, uint32_t> > > removeCandidates(tmpGraph.size());
    int removeCandidateCount = 0;
#ifdef _OPENMP
#pragma omp parallel for
#endif
    for (size_t idx = 0; idx < tmpGraph.size(); ++idx) {
      auto it = tmpGraph.begin() + idx;
      size_t id = idx + 1;
      try {
	NGT::GraphNode &srcNode = *it;	
	std::unordered_map<uint32_t, std::pair<size_t, double> > neighbors;
	for (size_t sni = 0; sni < srcNode.size(); ++sni) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	  neighbors[srcNode.at(sni, outGraph.repository.allocator).id] = std::pair<size_t, double>(sni, srcNode.at(sni, outGraph.repository.allocator).distance);
#else
	  neighbors[srcNode[sni].id] = std::pair<size_t, double>(sni, srcNode[sni].distance);
#endif
	}

	std::vector<std::pair<int, std::pair<uint32_t, uint32_t> > > candidates;	
	for (size_t sni = 0; sni < srcNode.size(); sni++) { 
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	  NGT::GraphNode &pathNode = tmpGraph[srcNode.at(sni, outGraph.repository.allocator).id - 1];
#else
	  NGT::GraphNode &pathNode = tmpGraph[srcNode[sni].id - 1];
#endif
	  for (size_t pni = 0; pni < pathNode.size(); pni++) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	    auto dstNodeID = pathNode.at(pni, outGraph.repository.allocator).id;
#else
	    auto dstNodeID = pathNode[pni].id;
#endif
	    auto dstNode = neighbors.find(dstNodeID);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	    if (dstNode != neighbors.end() 
		&& srcNode.at(sni, outGraph.repository.allocator).distance < (*dstNode).second.second 
		&& pathNode.at(pni, outGraph.repository.allocator).distance < (*dstNode).second.second  
		) {
#else
	    if (dstNode != neighbors.end() 
		&& srcNode[sni].distance < (*dstNode).second.second 
		&& pathNode[pni].distance < (*dstNode).second.second  
		) {
#endif
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	      candidates.push_back(std::pair<int, std::pair<uint32_t, uint32_t> >((*dstNode).second.first, std::pair<uint32_t, uint32_t>(srcNode.at(sni, outGraph.repository.allocator).id, dstNodeID)));  
#else
	      candidates.push_back(std::pair<int, std::pair<uint32_t, uint32_t> >((*dstNode).second.first, std::pair<uint32_t, uint32_t>(srcNode[sni].id, dstNodeID)));  
#endif
	      removeCandidateCount++;
	    }
	  }
	}
	sort(candidates.begin(), candidates.end(), std::greater<std::pair<int, std::pair<uint32_t, uint32_t>>>());
	removeCandidates[id - 1].reserve(candidates.size());
	for (size_t i = 0; i < candidates.size(); i++) {
	  removeCandidates[id - 1].push_back(candidates[i].second);
	}
      } catch(NGT::Exception &err) {
//	std::cerr << "GraphReconstructor: Warning. Cannot get the node. ID=" << id << ":" << err.what() << std::endl;
		if (NGT_LOG_DEBUG_)
			(*NGT_LOG_DEBUG_)("GraphReconstructor: Warning. Cannot get the node. ID=" + std::to_string(id) + ":" + err.what());
	continue;
      }
    }
    timer.stop();
//    std::cerr << "GraphReconstructor::adjustPaths: extracting removed edge candidates time=" << timer << std::endl;
	if (NGT_LOG_DEBUG_)
		(*NGT_LOG_DEBUG_)("GraphReconstructor::adjustPaths: extracting removed edge candidates time=" + std::to_string(timer.time));
    timer.reset();
    timer.start();

    std::list<size_t> ids;
    for (size_t idx = 0; idx < tmpGraph.size(); ++idx) {
      ids.push_back(idx + 1);
    }

    int removeCount = 0;
    removeCandidateCount = 0;
    for (size_t rank = 0; ids.size() != 0; rank++) {
      for (auto it = ids.begin(); it != ids.end(); ) {
	size_t id = *it;
	size_t idx = id - 1;
	try {
	  NGT::GraphNode &srcNode = tmpGraph[idx];
	  if (rank >= srcNode.size()) {
	    if (!removeCandidates[idx].empty()) {
//	      std::cerr << "Something wrong! ID=" << id << " # of remaining candidates=" << removeCandidates[idx].size() << std::endl;
	      if (NGT_LOG_DEBUG_)
			  (*NGT_LOG_DEBUG_)("Something wrong! ID=" + std::to_string(id) + " # of remaining candidates=" + std::to_string(removeCandidates[idx].size()));
	      abort();
	    }
#if !defined(NGT_SHARED_MEMORY_ALLOCATOR)
	    NGT::GraphNode empty;
            tmpGraph[idx] = empty;
#endif
	    it = ids.erase(it);
	    continue;
	  }
	  if (removeCandidates[idx].size() > 0) {
	    removeCandidateCount++;
	    bool pathExist = false;
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
            while (!removeCandidates[idx].empty() && (removeCandidates[idx].back().second == srcNode.at(rank, outGraph.repository.allocator).id)) {
#else
	    while (!removeCandidates[idx].empty() && (removeCandidates[idx].back().second == srcNode[rank].id)) {
#endif
	      size_t path = removeCandidates[idx].back().first;
	      size_t dst = removeCandidates[idx].back().second;
	      removeCandidates[idx].pop_back();
 	      if (removeCandidates[idx].empty()) {
 	        std::vector<std::pair<uint32_t, uint32_t>> empty;
 		removeCandidates[idx] = empty;
 	      }
              if ((hasEdge(outGraph, id, path)) && (hasEdge(outGraph, path, dst))) {
		pathExist = true;
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	        while (!removeCandidates[idx].empty() && (removeCandidates[idx].back().second == srcNode.at(rank, outGraph.repository.allocator).id)) {
#else
		while (!removeCandidates[idx].empty() && (removeCandidates[idx].back().second == srcNode[rank].id)) {
#endif
	          removeCandidates[idx].pop_back();
 	          if (removeCandidates[idx].empty()) {
 	            std::vector<std::pair<uint32_t, uint32_t>> empty;
 		    removeCandidates[idx] = empty;
 	          }
		}
		break;
	      }
	    }
	    if (pathExist) {
	      removeCount++;
              it++;
	      continue;
	    }
	  }
	  NGT::GraphNode &outSrcNode = *outGraph.getNode(id);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	  insert(outSrcNode, srcNode.at(rank, outGraph.repository.allocator).id, srcNode.at(rank, outGraph.repository.allocator).distance, outGraph);
#else
	  insert(outSrcNode, srcNode[rank].id, srcNode[rank].distance);
#endif
	} catch(NGT::Exception &err) {
//	  std::cerr << "GraphReconstructor: Warning. Cannot get the node. ID=" << id << ":" << err.what() << std::endl;
	  if (NGT_LOG_DEBUG_)
		  (*NGT_LOG_DEBUG_)("GraphReconstructor: Warning. Cannot get the node. ID=" + std::to_string(id) + ":" + err.what());
          it++;
	  continue;
	}
        it++;
      }
    }
    for (size_t id = 1; id < outGraph.repository.size(); id++) {
      try {
	NGT::GraphNode &node = *outGraph.getNode(id);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	std::sort(node.begin(outGraph.repository.allocator), node.end(outGraph.repository.allocator));
#else
	std::sort(node.begin(), node.end());
#endif
      } catch(...) {}
    }
  }


  static 
    void convertToANNG(std::vector<NGT::ObjectDistances> &graph)
  {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	  if (NGT_LOG_DEBUG_)
		  (*NGT_LOG_DEBUG_)("convertToANNG is not implemented for shared memory.");
//    std::cerr << "convertToANNG is not implemented for shared memory." << std::endl;
    return;
#else
//    std::cerr << "convertToANNG begin" << std::endl;
    if (NGT_LOG_DEBUG_)
		(*NGT_LOG_DEBUG_)("convertToANNG begin");
    for (size_t idx = 0; idx < graph.size(); idx++) {
      NGT::GraphNode &node = graph[idx];
      for (auto ni = node.begin(); ni != node.end(); ++ni) {
	graph[(*ni).id - 1].push_back(NGT::ObjectDistance(idx + 1, (*ni).distance));
      }
    }
    for (size_t idx = 0; idx < graph.size(); idx++) {
      NGT::GraphNode &node = graph[idx];
      if (node.size() == 0) {
	continue;
      }
      std::sort(node.begin(), node.end());
      NGT::ObjectID prev = 0;
      for (auto it = node.begin(); it != node.end();) {
	if (prev == (*it).id) {
	  it = node.erase(it);
	  continue;
	}
	prev = (*it).id;
	it++;
      }
      NGT::GraphNode tmp = node;
      node.swap(tmp);
    }
    if (NGT_LOG_DEBUG_)
		(*NGT_LOG_DEBUG_)("convertToANNG end");
//    std::cerr << "convertToANNG end" << std::endl;
#endif
  }

  static 
    void reconstructGraph(std::vector<NGT::ObjectDistances> &graph, NGT::GraphIndex &outGraph, size_t originalEdgeSize, size_t reverseEdgeSize) 
  {
    if (reverseEdgeSize > 10000) {
//      std::cerr << "something wrong. Edge size=" << reverseEdgeSize << std::endl;
      if (NGT_LOG_DEBUG_)
		  (*NGT_LOG_DEBUG_)("something wrong. Edge size=" + std::to_string(reverseEdgeSize));
      exit(1);
    }

    NGT::Timer	originalEdgeTimer, reverseEdgeTimer, normalizeEdgeTimer;
    originalEdgeTimer.start();

    for (size_t id = 1; id < outGraph.repository.size(); id++) {
      try {
	NGT::GraphNode &node = *outGraph.getNode(id);
	if (originalEdgeSize == 0) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	  node.clear(outGraph.repository.allocator);
#else
	  NGT::GraphNode empty;
	  node.swap(empty);
#endif
	} else {
	  NGT::ObjectDistances n = graph[id - 1];
	  if (n.size() < originalEdgeSize) {
//	    std::cerr << "GraphReconstructor: Warning. The edges are too few. " << n.size() << ":" << originalEdgeSize << " for " << id << std::endl;
	    if (NGT_LOG_DEBUG_)
			(*NGT_LOG_DEBUG_)("GraphReconstructor: Warning. The edges are too few. " + std::to_string(n.size()) + ":" + std::to_string(originalEdgeSize) + " for " + std::to_string(id));
	    continue;
	  }
	  n.resize(originalEdgeSize);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	  node.copy(n, outGraph.repository.allocator);
#else
	  node.swap(n);
#endif
	}
      } catch(NGT::Exception &err) {
//	std::cerr << "GraphReconstructor: Warning. Cannot get the node. ID=" << id << ":" << err.what() << std::endl;
		if (NGT_LOG_DEBUG_)
			(*NGT_LOG_DEBUG_)("GraphReconstructor: Warning. Cannot get the node. ID=" + std::to_string(id) + ":" + err.what());
	continue;
      }
    }
    originalEdgeTimer.stop();

    reverseEdgeTimer.start();
    int insufficientNodeCount = 0;
    for (size_t id = 1; id <= graph.size(); ++id) {
      try {
	NGT::ObjectDistances &node = graph[id - 1];
	size_t rsize = reverseEdgeSize;
	if (rsize > node.size()) {
	  insufficientNodeCount++;
	  rsize = node.size();
	}
	for (size_t i = 0; i < rsize; ++i) {
	  NGT::Distance distance = node[i].distance;
	  size_t nodeID = node[i].id;
	  try {
	    NGT::GraphNode &n = *outGraph.getNode(nodeID);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	    n.push_back(NGT::ObjectDistance(id, distance), outGraph.repository.allocator);
#else
	    n.push_back(NGT::ObjectDistance(id, distance));
#endif
	  } catch(...) {}
	}
      } catch(NGT::Exception &err) {
//	std::cerr << "GraphReconstructor: Warning. Cannot get the node. ID=" << id << ":" << err.what() << std::endl;
		if (NGT_LOG_DEBUG_)
			(*NGT_LOG_DEBUG_)("GraphReconstructor: Warning. Cannot get the node. ID=" + std::to_string(id) + ":" + err.what());
	continue;
      }
    } 
    reverseEdgeTimer.stop();    
    if (insufficientNodeCount != 0) {
//      std::cerr << "# of the nodes edges of which are in short = " << insufficientNodeCount << std::endl;
      	if (NGT_LOG_DEBUG_)
			(*NGT_LOG_DEBUG_)("# of the nodes edges of which are in short = " + std::to_string(insufficientNodeCount));
    }

    normalizeEdgeTimer.start();    
    for (size_t id = 1; id < outGraph.repository.size(); id++) {
      try {
	NGT::GraphNode &n = *outGraph.getNode(id);
	if (id % 100000 == 0) {
//	  std::cerr << "Processed " << id << " nodes" << std::endl;
	  	if (NGT_LOG_DEBUG_)
			(*NGT_LOG_DEBUG_)("Processed " + std::to_string(id) + " nodes");
	}
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	std::sort(n.begin(outGraph.repository.allocator), n.end(outGraph.repository.allocator));
#else
	std::sort(n.begin(), n.end());
#endif
	NGT::ObjectID prev = 0;
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	for (auto it = n.begin(outGraph.repository.allocator); it != n.end(outGraph.repository.allocator);) {
#else
	for (auto it = n.begin(); it != n.end();) {
#endif
	  if (prev == (*it).id) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	    it = n.erase(it, outGraph.repository.allocator);
#else
	    it = n.erase(it);
#endif
	    continue;
	  }
	  prev = (*it).id;
	  it++;
	}
#if !defined(NGT_SHARED_MEMORY_ALLOCATOR)
	NGT::GraphNode tmp = n;
	n.swap(tmp);
#endif
      } catch(NGT::Exception &err) {
//	std::cerr << "GraphReconstructor: Warning. Cannot get the node. ID=" << id << ":" << err.what() << std::endl;
		if (NGT_LOG_DEBUG_)
			(*NGT_LOG_DEBUG_)("GraphReconstructor: Warning. Cannot get the node. ID=" + std::to_string(id) + ":" + err.what());
	continue;
      }
    }
    normalizeEdgeTimer.stop();
//    std::cerr << "Reconstruction time=" << originalEdgeTimer.time << ":" << reverseEdgeTimer.time
//	 << ":" << normalizeEdgeTimer.time << std::endl;
    if (NGT_LOG_DEBUG_)
		(*NGT_LOG_DEBUG_)("Reconstruction time=" + std::to_string(originalEdgeTimer.time) + ":" + std::to_string(reverseEdgeTimer.time)
	 + ":" + std::to_string(normalizeEdgeTimer.time));

    NGT::Property prop;
    outGraph.getProperty().get(prop);
    prop.graphType = NGT::NeighborhoodGraph::GraphTypeONNG;
    outGraph.getProperty().set(prop);
  }



  static 
    void reconstructGraphWithConstraint(std::vector<NGT::ObjectDistances> &graph, NGT::GraphIndex &outGraph, 
					size_t originalEdgeSize, size_t reverseEdgeSize,
					char mode = 'a') 
  {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	  if (NGT_LOG_DEBUG_)
		(*NGT_LOG_DEBUG_)("reconstructGraphWithConstraint is not implemented.");
//    std::cerr << "reconstructGraphWithConstraint is not implemented." << std::endl;
    abort();
#else 

    NGT::Timer	originalEdgeTimer, reverseEdgeTimer, normalizeEdgeTimer;

    if (reverseEdgeSize > 10000) {
//      std::cerr << "something wrong. Edge size=" << reverseEdgeSize << std::endl;
      if (NGT_LOG_DEBUG_)
		  (*NGT_LOG_DEBUG_)("something wrong. Edge size=" + std::to_string(reverseEdgeSize));
      exit(1);
    }

    for (size_t id = 1; id < outGraph.repository.size(); id++) {
      if (id % 1000000 == 0) {
//	std::cerr << "Processed " << id << std::endl;
		if (NGT_LOG_DEBUG_)
			(*NGT_LOG_DEBUG_)("Processed " + std::to_string(id));
      }
      try {
	NGT::GraphNode &node = *outGraph.getNode(id);
	if (node.size() == 0) {
	  continue;
	}
	node.clear();
	NGT::GraphNode empty;
	node.swap(empty);
      } catch(NGT::Exception &err) {
//	std::cerr << "GraphReconstructor: Warning. Cannot get the node. ID=" << id << ":" << err.what() << std::endl;
		if (NGT_LOG_DEBUG_)
			(*NGT_LOG_DEBUG_)("GraphReconstructor: Warning. Cannot get the node. ID=" + std::to_string(id) + ":" + err.what());
	continue;
      }
    }
    NGT::GraphIndex::showStatisticsOfGraph(outGraph);

    std::vector<ObjectDistances> reverse(graph.size() + 1);	
    for (size_t id = 1; id <= graph.size(); ++id) {
      try {
	NGT::GraphNode &node = graph[id - 1];
	if (id % 100000 == 0) {
//	  std::cerr << "Processed (summing up) " << id << std::endl;
	  	if (NGT_LOG_DEBUG_)
			(*NGT_LOG_DEBUG_)("Processed (summing up) " + std::to_string(id));
	}
	for (size_t rank = 0; rank < node.size(); rank++) {
	  reverse[node[rank].id].push_back(ObjectDistance(id, node[rank].distance));
	}
      } catch(NGT::Exception &err) {
//	std::cerr << "GraphReconstructor: Warning. Cannot get the node. ID=" << id << ":" << err.what() << std::endl;
		if (NGT_LOG_DEBUG_)
			(*NGT_LOG_DEBUG_)("GraphReconstructor: Warning. Cannot get the node. ID=" + std::to_string(id) + ":" + err.what());
	continue;
      }
    }

    std::vector<std::pair<size_t, size_t> > reverseSize(graph.size() + 1);	
    reverseSize[0] = std::pair<size_t, size_t>(0, 0);
    for (size_t rid = 1; rid <= graph.size(); ++rid) {
      reverseSize[rid] = std::pair<size_t, size_t>(reverse[rid].size(), rid);
    }
    std::sort(reverseSize.begin(), reverseSize.end());		


    std::vector<uint32_t> indegreeCount(graph.size(), 0);	
    size_t zeroCount = 0;
    for (size_t sizerank = 0; sizerank <= reverseSize.size(); sizerank++) {
      
      if (reverseSize[sizerank].first == 0) {
	zeroCount++;
	continue;
      }
      size_t rid = reverseSize[sizerank].second;	
      ObjectDistances &rnode = reverse[rid];		
      for (auto rni = rnode.begin(); rni != rnode.end(); ++rni) {
	if (indegreeCount[(*rni).id] >= reverseEdgeSize) {	
	  continue;
	}
	NGT::GraphNode &node = *outGraph.getNode(rid);	
	if (indegreeCount[(*rni).id] > 0 && node.size() >= originalEdgeSize) {
	  continue;
	}
	
	node.push_back(NGT::ObjectDistance((*rni).id, (*rni).distance));
	indegreeCount[(*rni).id]++;
      }
    }
    reverseEdgeTimer.stop();    
//    std::cerr << "The number of nodes with zero outdegree by reverse edges=" << zeroCount << std::endl;
    if (NGT_LOG_DEBUG_)
		(*NGT_LOG_DEBUG_)("The number of nodes with zero outdegree by reverse edges=" + std::to_string(zeroCount));
    NGT::GraphIndex::showStatisticsOfGraph(outGraph);

    normalizeEdgeTimer.start();    
    for (size_t id = 1; id < outGraph.repository.size(); id++) {
      try {
	NGT::GraphNode &n = *outGraph.getNode(id);
	if (id % 100000 == 0) {
//	  std::cerr << "Processed " << id << std::endl;
	  	if (NGT_LOG_DEBUG_)
			(*NGT_LOG_DEBUG_)("Processed " + std::to_string(id));
	}
	std::sort(n.begin(), n.end());
	NGT::ObjectID prev = 0;
	for (auto it = n.begin(); it != n.end();) {
	  if (prev == (*it).id) {
	    it = n.erase(it);
	    continue;
	  }
	  prev = (*it).id;
	  it++;
	}
	NGT::GraphNode tmp = n;
	n.swap(tmp);
      } catch(NGT::Exception &err) {
//	std::cerr << "GraphReconstructor: Warning. Cannot get the node. ID=" << id << ":" << err.what() << std::endl;
		if (NGT_LOG_DEBUG_)
			(*NGT_LOG_DEBUG_)("GraphReconstructor: Warning. Cannot get the node. ID=" + std::to_string(id) + ":" + err.what());
	continue;
      }
    }
    normalizeEdgeTimer.stop();
    NGT::GraphIndex::showStatisticsOfGraph(outGraph);

    originalEdgeTimer.start();
    for (size_t id = 1; id < outGraph.repository.size(); id++) {
      if (id % 1000000 == 0) {
//	std::cerr << "Processed " << id << std::endl;
		if (NGT_LOG_DEBUG_)
			(*NGT_LOG_DEBUG_)("Processed " + std::to_string(id));
      }
      NGT::GraphNode &node = graph[id - 1];
      try {
	NGT::GraphNode &onode = *outGraph.getNode(id);
	bool stop = false;
	for (size_t rank = 0; (rank < node.size() && rank < originalEdgeSize) && stop == false; rank++) {
	  switch (mode) {
	  case 'a':
	    if (onode.size() >= originalEdgeSize) {
	      stop = true;
	      continue;
	    }
	    break;
	  case 'c':
	    break;
	  }
	  NGT::Distance distance = node[rank].distance;
	  size_t nodeID = node[rank].id;
	  outGraph.addEdge(id, nodeID, distance, false);
	}
      } catch(NGT::Exception &err) {
//	std::cerr << "GraphReconstructor: Warning. Cannot get the node. ID=" << id << ":" << err.what() << std::endl;
		if (NGT_LOG_DEBUG_)
			(*NGT_LOG_DEBUG_)("GraphReconstructor: Warning. Cannot get the node. ID=" + std::to_string(id) + ":" + err.what());
	continue;
      }
    }
    originalEdgeTimer.stop();
    NGT::GraphIndex::showStatisticsOfGraph(outGraph);

//    std::cerr << "Reconstruction time=" << originalEdgeTimer.time << ":" << reverseEdgeTimer.time
//	 << ":" << normalizeEdgeTimer.time << std::endl;
    if (NGT_LOG_DEBUG_)
		(*NGT_LOG_DEBUG_)("Reconstruction time=" + std::to_string(originalEdgeTimer.time) + ":" + std::to_string(reverseEdgeTimer.time)
	 + ":" + std::to_string(normalizeEdgeTimer.time));

#endif
  }

  // reconstruct a pseudo ANNG with a fewer edges from an actual ANNG with more edges.
  // graph is a source ANNG
  // index is an index with a reconstructed ANNG
  static 
    void reconstructANNGFromANNG(std::vector<NGT::ObjectDistances> &graph, NGT::Index &index, size_t edgeSize) 
  {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	  if (NGT_LOG_DEBUG_)
		(*NGT_LOG_DEBUG_)("reconstructANNGFromANNG is not implemented.");
//    std::cerr << "reconstructANNGFromANNG is not implemented." << std::endl;
    abort();
#else 

    NGT::GraphIndex	&outGraph = dynamic_cast<NGT::GraphIndex&>(index.getIndex());

    // remove all edges in the index.
    for (size_t id = 1; id < outGraph.repository.size(); id++) {
      if (id % 1000000 == 0) {
//	std::cerr << "Processed " << id << " nodes." << std::endl;
		if (NGT_LOG_DEBUG_)
			(*NGT_LOG_DEBUG_)("Processed " + std::to_string(id) + " nodes.");
      }
      try {
	NGT::GraphNode &node = *outGraph.getNode(id);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	node.clear(outGraph.repository.allocator);
#else
	NGT::GraphNode empty;
	node.swap(empty);
#endif
      } catch(NGT::Exception &err) {
      }
    }

    for (size_t id = 1; id <= graph.size(); ++id) {
      size_t edgeCount = 0;
      try {
	NGT::ObjectDistances &node = graph[id - 1];
	NGT::GraphNode &n = *outGraph.getNode(id);
	NGT::Distance prevDistance = 0.0;
	assert(n.size() == 0);
	for (size_t i = 0; i < node.size(); ++i) {
	  NGT::Distance distance = node[i].distance;
	  if (prevDistance > distance) {
	    NGTThrowException("Edge distance order is invalid");
	  }
	  prevDistance = distance;
	  size_t nodeID = node[i].id;
	  if (node[i].id < id) {
	    try {
	      NGT::GraphNode &dn = *outGraph.getNode(nodeID);
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
	      n.push_back(NGT::ObjectDistance(nodeID, distance), outGraph.repository.allocator);
	      dn.push_back(NGT::ObjectDistance(id, distance), outGraph.repository.allocator);
#else
	      n.push_back(NGT::ObjectDistance(nodeID, distance));
	      dn.push_back(NGT::ObjectDistance(id, distance));
#endif
	    } catch(...) {}
	    edgeCount++;
	  }
	  if (edgeCount >= edgeSize) {
	    break;
	  }
	}
      } catch(NGT::Exception &err) {
      }
    } 

    for (size_t id = 1; id < outGraph.repository.size(); id++) {
      try {
	NGT::GraphNode &n = *outGraph.getNode(id);
	std::sort(n.begin(), n.end());
	NGT::ObjectID prev = 0;
	for (auto it = n.begin(); it != n.end();) {
	  if (prev == (*it).id) {
	    it = n.erase(it);
	    continue;
	  }
	  prev = (*it).id;
	  it++;
	}
	NGT::GraphNode tmp = n;
	n.swap(tmp);
      } catch (...) {
      }
    }
#endif
  }

  static void refineANNG(NGT::Index &index, bool unlog, float epsilon = 0.1, float accuracy = 0.0, int noOfEdges = 0, int exploreEdgeSize = INT_MIN, size_t batchSize = 10000) {
    NGT::StdOstreamRedirector redirector(unlog);
    redirector.begin();
    try {
      refineANNG(index, epsilon, accuracy, noOfEdges, exploreEdgeSize, batchSize);
    } catch (NGT::Exception &err) {
      redirector.end();
      throw(err);
    }
  }

  static void refineANNG(NGT::Index &index, float epsilon = 0.1, float accuracy = 0.0, int noOfEdges = 0, int exploreEdgeSize = INT_MIN, size_t batchSize = 10000) {
#if defined(NGT_SHARED_MEMORY_ALLOCATOR)
    NGTThrowException("GraphReconstructor::refineANNG: Not implemented for the shared memory option.");
#else
    auto prop = static_cast<GraphIndex&>(index.getIndex()).getGraphProperty();
    NGT::ObjectRepository &objectRepository = index.getObjectSpace().getRepository();
    NGT::GraphIndex &graphIndex = static_cast<GraphIndex&>(index.getIndex());
    size_t nOfObjects = objectRepository.size();
    bool error = false;
    std::string errorMessage;
    for (size_t bid = 1; bid < nOfObjects; bid += batchSize) {
      NGT::ObjectDistances results[batchSize];
      // search
#pragma omp parallel for
      for (size_t idx = 0; idx < batchSize; idx++) {
	size_t id = bid + idx;
	if (id % 100000 == 0) {
//	  std::cerr << "# of processed objects=" << id << std::endl;
	  	if (NGT_LOG_DEBUG_)
			(*NGT_LOG_DEBUG_)("# of processed objects=" + std::to_string(id));
	}
	if (objectRepository.isEmpty(id)) {
	  continue;
	}
	NGT::SearchContainer searchContainer(*objectRepository.get(id));
	searchContainer.setResults(&results[idx]);
	assert(prop.edgeSizeForCreation > 0);
	searchContainer.setSize(noOfEdges > prop.edgeSizeForCreation ? noOfEdges : prop.edgeSizeForCreation);
	if (accuracy > 0.0) {
          searchContainer.setExpectedAccuracy(accuracy);
        } else {
	  searchContainer.setEpsilon(epsilon);
        }
	if (exploreEdgeSize != INT_MIN) {
          searchContainer.setEdgeSize(exploreEdgeSize);
        }
	if (!error) {
          try {
            index.search(searchContainer);
          } catch (NGT::Exception &err) {
#pragma omp critical
            {
      	      error = true;
	      errorMessage = err.what();
            }
          }
        }
      }
      if (error) {
        std::stringstream msg;
	msg << "GraphReconstructor::refineANNG: " << errorMessage;
        NGTThrowException(msg);
      }
      // outgoing edges
#pragma omp parallel for
      for (size_t idx = 0; idx < batchSize; idx++) {
	size_t id = bid + idx;
	if (objectRepository.isEmpty(id)) {
	  continue;
	}
	NGT::GraphNode &node = *graphIndex.getNode(id);
	for (auto i = results[idx].begin(); i != results[idx].end(); ++i) {
	  if ((*i).id != id) {
	    node.push_back(*i);
	  }
	}
	std::sort(node.begin(), node.end());
	// dedupe
	ObjectID prev = 0;
	for (GraphNode::iterator ni = node.begin(); ni != node.end();) {
	  if (prev == (*ni).id) {
	    ni = node.erase(ni);
	    continue;
	  }
	  prev = (*ni).id;
	  ni++;
	}
      }
      // incomming edges
      if (noOfEdges != 0) {
	continue;
      }
      for (size_t idx = 0; idx < batchSize; idx++) {
	size_t id = bid + idx;
	if (id % 10000 == 0) {
//	  std::cerr << "# of processed objects=" << id << std::endl;
	  	if (NGT_LOG_DEBUG_)
			(*NGT_LOG_DEBUG_)("# of processed objects=" + std::to_string(id));
	}
	for (auto i = results[idx].begin(); i != results[idx].end(); ++i) {
	  if ((*i).id != id) {
	    NGT::GraphNode &node = *graphIndex.getNode((*i).id);
	    graphIndex.addEdge(node, id, (*i).distance, false);
	  }
	}
      }
    }
    if (noOfEdges != 0) {
      // prune to build knng
      size_t  nedges = noOfEdges < 0 ? -noOfEdges : noOfEdges;
#pragma omp parallel for
      for (ObjectID id = 1; id < nOfObjects; ++id) {
        if (objectRepository.isEmpty(id)) {
	  continue;
        }
	NGT::GraphNode &node = *graphIndex.getNode(id);
	if (node.size() > nedges) {
	  node.resize(nedges);
        }
      }
    }
#endif // defined(NGT_SHARED_MEMORY_ALLOCATOR)
  }
};

}; // NGT
