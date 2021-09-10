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

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>

typedef unsigned int ObjectID;
typedef void* NGTIndex;
typedef void* NGTProperty;
typedef void* NGTObjectSpace;
typedef void* NGTObjectDistances;
typedef void* NGTError;
typedef void* NGTOptimizer;

typedef struct {
  ObjectID id;
  float distance;
} NGTObjectDistance;

typedef struct {
  float		*query;
  size_t	size;		// # of returned objects
  float		epsilon;
  float		accuracy;	// expected accuracy
  float		radius;
  size_t	edge_size;	// # of edges to explore for each node
} NGTQuery;

typedef struct {
  size_t	no_of_queries;
  size_t	no_of_results;
  size_t	no_of_threads;
  float		target_accuracy;
  size_t	target_no_of_objects;
  size_t	no_of_sample_objects;
  size_t	max_of_no_of_edges;
  bool		log;
} NGTAnngEdgeOptimizationParameter;

NGTIndex ngt_open_index(const char *, NGTError);

NGTIndex ngt_create_graph_and_tree(const char *, NGTProperty, NGTError);

NGTIndex ngt_create_graph_and_tree_in_memory(NGTProperty, NGTError);

NGTProperty ngt_create_property(NGTError);

bool ngt_save_index(const NGTIndex, const char *, NGTError);

bool ngt_get_property(const NGTIndex, NGTProperty, NGTError);

int32_t ngt_get_property_dimension(NGTProperty, NGTError);

bool ngt_set_property_dimension(NGTProperty, int32_t, NGTError);

bool ngt_set_property_edge_size_for_creation(NGTProperty, int16_t, NGTError);

bool ngt_set_property_edge_size_for_search(NGTProperty, int16_t, NGTError);

int32_t ngt_get_property_object_type(NGTProperty, NGTError);

bool ngt_is_property_object_type_float(int32_t);

bool ngt_is_property_object_type_integer(int32_t);

bool ngt_set_property_object_type_float(NGTProperty, NGTError);

bool ngt_set_property_object_type_integer(NGTProperty, NGTError);

bool ngt_set_property_distance_type_l1(NGTProperty, NGTError);

bool ngt_set_property_distance_type_l2(NGTProperty, NGTError);

bool ngt_set_property_distance_type_angle(NGTProperty, NGTError);

bool ngt_set_property_distance_type_hamming(NGTProperty, NGTError);

bool ngt_set_property_distance_type_jaccard(NGTProperty, NGTError);

bool ngt_set_property_distance_type_cosine(NGTProperty, NGTError);

bool ngt_set_property_distance_type_normalized_angle(NGTProperty, NGTError);

bool ngt_set_property_distance_type_normalized_cosine(NGTProperty, NGTError);  
  
NGTObjectDistances ngt_create_empty_results(NGTError);

bool ngt_search_index(NGTIndex, double*, int32_t, size_t, float, float, NGTObjectDistances, NGTError);

bool ngt_search_index_as_float(NGTIndex, float*, int32_t, size_t, float, float, NGTObjectDistances, NGTError);

bool ngt_search_index_with_query(NGTIndex, NGTQuery, NGTObjectDistances, NGTError);

int32_t ngt_get_size(NGTObjectDistances, NGTError); // deprecated
  
uint32_t ngt_get_result_size(NGTObjectDistances, NGTError); 

NGTObjectDistance ngt_get_result(const NGTObjectDistances, const uint32_t, NGTError);

ObjectID ngt_insert_index(NGTIndex, double*, uint32_t, NGTError);

ObjectID ngt_append_index(NGTIndex, double*, uint32_t, NGTError);

ObjectID ngt_insert_index_as_float(NGTIndex, float*, uint32_t, NGTError);

ObjectID ngt_append_index_as_float(NGTIndex, float*, uint32_t, NGTError);

bool ngt_batch_append_index(NGTIndex, float*, uint32_t, NGTError);

bool ngt_batch_insert_index(NGTIndex, float*, uint32_t, uint32_t *, NGTError);

bool ngt_create_index(NGTIndex, uint32_t, NGTError);

bool ngt_remove_index(NGTIndex, ObjectID, NGTError);

NGTObjectSpace ngt_get_object_space(NGTIndex, NGTError);

float* ngt_get_object_as_float(NGTObjectSpace, ObjectID, NGTError);

uint8_t* ngt_get_object_as_integer(NGTObjectSpace, ObjectID, NGTError);

void ngt_destroy_results(NGTObjectDistances);

void ngt_destroy_property(NGTProperty);

void ngt_close_index(NGTIndex);

int16_t ngt_get_property_edge_size_for_creation(NGTProperty, NGTError);

int16_t ngt_get_property_edge_size_for_search(NGTProperty, NGTError);

int32_t ngt_get_property_distance_type(NGTProperty, NGTError);

NGTError ngt_create_error_object();

const char *ngt_get_error_string(const NGTError);

void ngt_clear_error_string(NGTError);
  
void ngt_destroy_error_object(NGTError);

NGTOptimizer ngt_create_optimizer(bool logDisabled, NGTError);

bool ngt_optimizer_adjust_search_coefficients(NGTOptimizer, const char *, NGTError);

bool ngt_optimizer_execute(NGTOptimizer, const char *, const char *, NGTError);

bool ngt_optimizer_set(NGTOptimizer optimizer, int outgoing, int incoming, int nofqs, 
		       float baseAccuracyFrom, float baseAccuracyTo,
		       float rateAccuracyFrom, float rateAccuracyTo,
		       double gte, double m, NGTError error);

bool ngt_optimizer_set_minimum(NGTOptimizer optimizer, int outgoing, int incoming, 
			       int nofqs, int nofrs, NGTError error);

bool ngt_optimizer_set_extension(NGTOptimizer optimizer,
				 float baseAccuracyFrom, float baseAccuracyTo,
				 float rateAccuracyFrom, float rateAccuracyTo,
				 double gte, double m, NGTError error);

bool ngt_optimizer_set_processing_modes(NGTOptimizer optimizer, bool searchParameter, 
					bool prefetchParameter, bool accuracyTable, NGTError error);

void ngt_destroy_optimizer(NGTOptimizer);

// refine: the specified index by searching each node.
// epsilon, exepectedAccuracy and edgeSize: the same as the prameters for search. but if edgeSize is INT_MIN, default is used.
// noOfEdges: if this is not 0, kNNG with k = noOfEdges is build 
// batchSize: batch size for parallelism.
bool ngt_refine_anng(NGTIndex index, float epsilon, float expectedAccuracy, 
		     int noOfEdges, int edgeSize, size_t batchSize, NGTError error);

// get edges of the node that is specified with id.
bool ngt_get_edges(NGTIndex index, ObjectID id, NGTObjectDistances edges, NGTError error);

// get the size of the specified object repository.
// Since the size includes empty objects, the size is not the number of objects.
// The size is mostly the largest ID of the objects - 1;
uint32_t ngt_get_object_repository_size(NGTIndex index, NGTError error);

// return parameters for ngt_optimize_number_of_edges. You can customize them before calling ngt_optimize_number_of_edges.
NGTAnngEdgeOptimizationParameter ngt_get_anng_edge_optimization_parameter();

// optimize the number of initial edges for ANNG that is specified with indexPath.
// The parameter should be a struct which is returned by nt_get_optimization_parameter.
bool ngt_optimize_number_of_edges(const char *indexPath, NGTAnngEdgeOptimizationParameter parameter, NGTError error);

#ifdef __cplusplus
}
#endif
