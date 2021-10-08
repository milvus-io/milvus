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

#include <string>
#include <iostream>
#include <sstream>

#include "NGT/Index.h"
#include "NGT/GraphOptimizer.h"
#include "Capi.h"

static bool operate_error_string_(const std::stringstream &ss, NGTError error){
  if(error != NULL){  
    try{
      std::string *error_str = static_cast<std::string*>(error);
      *error_str = ss.str();
    }catch(std::exception &err){
      std::cerr << ss.str() << " > " << err.what() << std::endl;
      return false;
    }
  }else{
    std::cerr << ss.str() << std::endl;
  }
  return true;
}

NGTIndex ngt_open_index(const char *index_path, NGTError error) {
  try{
    std::string index_path_str(index_path);
    NGT::Index *index = new NGT::Index(index_path_str);
    index->disableLog();
    return static_cast<NGTIndex>(index);
  }catch(std::exception &err){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);
    return NULL;
  }
}
    
NGTIndex ngt_create_graph_and_tree(const char *database, NGTProperty prop, NGTError error) {
  NGT::Index *index = NULL;
  try{
    std::string database_str(database);
    NGT::Property prop_i = *(static_cast<NGT::Property*>(prop));    
    NGT::Index::createGraphAndTree(database_str, prop_i, true);
    index = new NGT::Index(database_str);
    index->disableLog();
    return static_cast<NGTIndex>(index);
  }catch(std::exception &err){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);    
    delete index;
    return NULL;
  }
}

NGTIndex ngt_create_graph_and_tree_in_memory(NGTProperty prop, NGTError error) {
#ifdef NGT_SHARED_MEMORY_ALLOCATOR
  std::stringstream ss;
  ss << "Capi : " << __FUNCTION__ << "() : Error: " << __FUNCTION__ << " is unavailable for shared-memory-type NGT.";
  operate_error_string_(ss, error);        
  return NULL;
#else
  try{
    NGT::Index *index = new NGT::GraphAndTreeIndex(*(static_cast<NGT::Property*>(prop)));
    index->disableLog();
    return static_cast<NGTIndex>(index);
  }catch(std::exception &err){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);        
    return NULL;
  }
#endif
}

NGTProperty ngt_create_property(NGTError error) {
  try{
    return static_cast<NGTProperty>(new NGT::Property());
  }catch(std::exception &err){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);        
    return NULL;
  }
}

bool ngt_save_index(const NGTIndex index, const char *database, NGTError error) {
  try{
    std::string database_str(database);
    (static_cast<NGT::Index*>(index))->saveIndex(database_str);
  }catch(std::exception &err) {
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);    
    return false;
  }
  return true;
}

bool ngt_get_property(NGTIndex index, NGTProperty prop, NGTError error) {
  if(index == NULL || prop == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: index = " << index << " prop = " << prop;
    operate_error_string_(ss, error);        
    return false;
  }
  
  try{
    (static_cast<NGT::Index*>(index))->getProperty(*(static_cast<NGT::Property*>(prop)));
  }catch(std::exception &err) {
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);
    return false;
  }
  return true;
}

int32_t ngt_get_property_dimension(NGTProperty prop, NGTError error) {
  if(prop == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: prop = " << prop;
    operate_error_string_(ss, error);    
    return -1;
  }
  return (*static_cast<NGT::Property*>(prop)).dimension;      
}

bool ngt_set_property_dimension(NGTProperty prop, int32_t value, NGTError error) {
  if(prop == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: prop = " << prop;            
    operate_error_string_(ss, error);
    return false;
  }
  (*static_cast<NGT::Property*>(prop)).dimension = value;
  return true;
}

bool ngt_set_property_edge_size_for_creation(NGTProperty prop, int16_t value, NGTError error) {
  if(prop == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: prop = " << prop;
    operate_error_string_(ss, error);    
    return false;
  }
  (*static_cast<NGT::Property*>(prop)).edgeSizeForCreation = value;
  return true;
}

bool ngt_set_property_edge_size_for_search(NGTProperty prop, int16_t value, NGTError error) {
  if(prop == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: prop = " << prop;            
    operate_error_string_(ss, error);        
    return false;
  }
  (*static_cast<NGT::Property*>(prop)).edgeSizeForSearch = value;
  return true;
}

int32_t ngt_get_property_object_type(NGTProperty prop, NGTError error) {
  if(prop == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: prop = " << prop;
    operate_error_string_(ss, error);              
    return -1;
  }
  return (*static_cast<NGT::Property*>(prop)).objectType;
}

bool ngt_is_property_object_type_float(int32_t object_type) {
    return (object_type == NGT::ObjectSpace::ObjectType::Float);
}

bool ngt_is_property_object_type_integer(int32_t object_type) {
    return (object_type == NGT::ObjectSpace::ObjectType::Uint8);
}

bool ngt_set_property_object_type_float(NGTProperty prop, NGTError error) {
  if(prop == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: prop = " << prop;
    operate_error_string_(ss, error);                    
    return false;
  }
  
  (*static_cast<NGT::Property*>(prop)).objectType = NGT::ObjectSpace::ObjectType::Float;
  return true;
}

bool ngt_set_property_object_type_integer(NGTProperty prop, NGTError error) {
  if(prop == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: prop = " << prop;
    operate_error_string_(ss, error);
    return false;
  }
  
  (*static_cast<NGT::Property*>(prop)).objectType = NGT::ObjectSpace::ObjectType::Uint8;
  return true;
}

bool ngt_set_property_distance_type_l1(NGTProperty prop, NGTError error) {
  if(prop == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: prop = " << prop;
    operate_error_string_(ss, error);      
    return false;
  }
  
  (*static_cast<NGT::Property*>(prop)).distanceType = NGT::Index::Property::DistanceType::DistanceTypeL1;
  return true;
}

bool ngt_set_property_distance_type_l2(NGTProperty prop, NGTError error) {
  if(prop == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: prop = " << prop;
    operate_error_string_(ss, error);            
    return false;
  }
  
  (*static_cast<NGT::Property*>(prop)).distanceType = NGT::Index::Property::DistanceType::DistanceTypeL2;
  return true;
}

bool ngt_set_property_distance_type_angle(NGTProperty prop, NGTError error) {
  if(prop == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: prop = " << prop;
    operate_error_string_(ss, error);                  
    return false;
  }
  
  (*static_cast<NGT::Property*>(prop)).distanceType = NGT::Index::Property::DistanceType::DistanceTypeAngle;
  return true;
}

bool ngt_set_property_distance_type_hamming(NGTProperty prop, NGTError error) {
  if(prop == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: prop = " << prop;
    operate_error_string_(ss, error);                        
    return false;
  }
  
  (*static_cast<NGT::Property*>(prop)).distanceType = NGT::Index::Property::DistanceType::DistanceTypeHamming;
  return true;
}

bool ngt_set_property_distance_type_jaccard(NGTProperty prop, NGTError error) {
  if(prop == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: prop = " << prop;
    operate_error_string_(ss, error);                        
    return false;
  }
  
  (*static_cast<NGT::Property*>(prop)).distanceType = NGT::Index::Property::DistanceType::DistanceTypeJaccard;
  return true;
}

bool ngt_set_property_distance_type_cosine(NGTProperty prop, NGTError error) {
  if(prop == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: prop = " << prop;
    operate_error_string_(ss, error);
    return false;
  }
 
  (*static_cast<NGT::Property*>(prop)).distanceType = NGT::Index::Property::DistanceType::DistanceTypeCosine;
  return true;
}

bool ngt_set_property_distance_type_normalized_angle(NGTProperty prop, NGTError error) {
  if(prop == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: prop = " << prop;
    operate_error_string_(ss, error);
    return false;
  }
 
  (*static_cast<NGT::Property*>(prop)).distanceType = NGT::Index::Property::DistanceType::DistanceTypeNormalizedAngle;
  return true;
}

bool ngt_set_property_distance_type_normalized_cosine(NGTProperty prop, NGTError error) {
  if(prop == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: prop = " << prop;
    operate_error_string_(ss, error);
    return false;
  }
 
  (*static_cast<NGT::Property*>(prop)).distanceType = NGT::Index::Property::DistanceType::DistanceTypeNormalizedCosine;
  return true;
}

NGTObjectDistances ngt_create_empty_results(NGTError error) {
  try{
    return static_cast<NGTObjectDistances>(new NGT::ObjectDistances());
  }catch(std::exception &err) {
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);                              
    return NULL;
  }
}

static bool ngt_search_index_(NGT::Index* pindex, NGT::Object *ngtquery, size_t size, float epsilon, float radius, NGTObjectDistances results, int edge_size = INT_MIN) {
  // set search prameters.
  NGT::SearchContainer sc(*ngtquery);      // search parametera container.
  
  sc.setResults(static_cast<NGT::ObjectDistances*>(results));          // set the result set.
  sc.setSize(size);                        // the number of resultant objects.
  sc.setRadius(radius);                    // search radius.
  sc.setEpsilon(epsilon);                  // set exploration coefficient.
  if (edge_size != INT_MIN) {
    sc.setEdgeSize(edge_size);// set # of edges for each node
  }

  pindex->search(sc);
  
  // delete the query object.
  pindex->deleteObject(ngtquery);
  return true;
}

bool ngt_search_index(NGTIndex index, double *query, int32_t query_dim, size_t size, float epsilon, float radius, NGTObjectDistances results, NGTError error) {
  if(index == NULL || query == NULL || results == NULL || query_dim <= 0){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: index = " << index << " query = " << query << " results = " << results << " query_dim = " << query_dim;
    operate_error_string_(ss, error);
    return false;
  }
  
  NGT::Index* pindex = static_cast<NGT::Index*>(index);    
  NGT::Object *ngtquery = NULL;
  
  if(radius < 0.0){
    radius = FLT_MAX;
  }
  
  try{	  
    std::vector<double> vquery(&query[0], &query[query_dim]);
    ngtquery = pindex->allocateObject(vquery);
    ngt_search_index_(pindex, ngtquery, size, epsilon, radius, results);
  }catch(std::exception &err) {
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);      
    if(ngtquery != NULL){
      pindex->deleteObject(ngtquery);
    }
    return false;
  }
  return true;
}

bool ngt_search_index_as_float(NGTIndex index, float *query, int32_t query_dim, size_t size, float epsilon, float radius, NGTObjectDistances results, NGTError error) {
  if(index == NULL || query == NULL || results == NULL || query_dim <= 0){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: index = " << index << " query = " << query << " results = " << results << " query_dim = " << query_dim;
    operate_error_string_(ss, error);
    return false;
  }
  
  NGT::Index* pindex = static_cast<NGT::Index*>(index);    
  NGT::Object *ngtquery = NULL;

  if(radius < 0.0){
    radius = FLT_MAX;
  }

  try{	  
    std::vector<float> vquery(&query[0], &query[query_dim]);
    ngtquery = pindex->allocateObject(vquery);
    ngt_search_index_(pindex, ngtquery, size, epsilon, radius, results);
  }catch(std::exception &err) {
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);      
    if(ngtquery != NULL){
      pindex->deleteObject(ngtquery);
    }
    return false;
  }
  return true;
}

bool ngt_search_index_with_query(NGTIndex index,  NGTQuery query, NGTObjectDistances results, NGTError error) {
  if(index == NULL || query.query == NULL || results == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: index = " << index << " query = " << query.query << " results = " << results;
    operate_error_string_(ss, error);
    return false;
  }
  
  NGT::Index* pindex = static_cast<NGT::Index*>(index);   
  int32_t dim = pindex->getObjectSpace().getDimension();

  NGT::Object *ngtquery = NULL;

  if(query.radius < 0.0){
    query.radius = FLT_MAX;
  }

  try{
    std::vector<float> vquery(&query.query[0], &query.query[dim]);
    ngtquery = pindex->allocateObject(vquery);
    ngt_search_index_(pindex, ngtquery, query.size, query.epsilon, query.radius, results, query.edge_size);
  }catch(std::exception &err) {
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);
    if(ngtquery != NULL){
      pindex->deleteObject(ngtquery);
    }
    return false;
  }
  return true;
}


// * deprecated *
int32_t ngt_get_size(NGTObjectDistances results, NGTError error) {
  if(results == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: results = " << results;
    operate_error_string_(ss, error);            
    return -1;
  }
  
  return (static_cast<NGT::ObjectDistances*>(results))->size();
}

uint32_t ngt_get_result_size(NGTObjectDistances results, NGTError error) {
  if(results == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: results = " << results;
    operate_error_string_(ss, error);            
    return 0;
  }
  
  return (static_cast<NGT::ObjectDistances*>(results))->size();
}

NGTObjectDistance ngt_get_result(const NGTObjectDistances results, const uint32_t i, NGTError error) {
  try{
    NGT::ObjectDistances objects = *(static_cast<NGT::ObjectDistances*>(results));
    NGTObjectDistance ret_val = {objects[i].id, objects[i].distance};
    return ret_val;
  }catch(std::exception &err) {
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);
    
    NGTObjectDistance err_val = {0};
    return err_val;
  }
}

ObjectID ngt_insert_index(NGTIndex index, double *obj, uint32_t obj_dim, NGTError error) {
  if(index == NULL || obj == NULL || obj_dim == 0){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: index = " << index << " obj = " << obj << " obj_dim = " << obj_dim;
    operate_error_string_(ss, error);
    return 0;
  }

  try{
    NGT::Index* pindex = static_cast<NGT::Index*>(index);
    std::vector<double> vobj(&obj[0], &obj[obj_dim]);
    return pindex->insert(vobj);    
  }catch(std::exception &err) {
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);      
    return 0;
  }
}

ObjectID ngt_append_index(NGTIndex index, double *obj, uint32_t obj_dim, NGTError error) {
  if(index == NULL || obj == NULL || obj_dim == 0){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: index = " << index << " obj = " << obj << " obj_dim = " << obj_dim;
    operate_error_string_(ss, error);
    return 0;
  }

  try{
    NGT::Index* pindex = static_cast<NGT::Index*>(index);
    std::vector<double> vobj(&obj[0], &obj[obj_dim]);
    return pindex->append(vobj);    
  }catch(std::exception &err) {
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);      
    return 0;
  }
}

ObjectID ngt_insert_index_as_float(NGTIndex index, float *obj, uint32_t obj_dim, NGTError error) {
  if(index == NULL || obj == NULL || obj_dim == 0){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: index = " << index << " obj = " << obj << " obj_dim = " << obj_dim;
    operate_error_string_(ss, error);
    return 0;
  }

  try{
    NGT::Index* pindex = static_cast<NGT::Index*>(index);
    std::vector<float> vobj(&obj[0], &obj[obj_dim]);
    return pindex->insert(vobj);    
  }catch(std::exception &err) {
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);      
    return 0;
  }
}

ObjectID ngt_append_index_as_float(NGTIndex index, float *obj, uint32_t obj_dim, NGTError error) {
  if(index == NULL || obj == NULL || obj_dim == 0){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: index = " << index << " obj = " << obj << " obj_dim = " << obj_dim;
    operate_error_string_(ss, error);
    return 0;
  }

  try{
    NGT::Index* pindex = static_cast<NGT::Index*>(index);
    std::vector<float> vobj(&obj[0], &obj[obj_dim]);
    return pindex->append(vobj);    
  }catch(std::exception &err) {
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);      
    return 0;
  }
}

bool ngt_batch_append_index(NGTIndex index, float *obj, uint32_t data_count, NGTError error) {
  try{
    NGT::Index* pindex = static_cast<NGT::Index*>(index);
    pindex->append(obj, data_count);
    return true;
  }catch(std::exception &err) {
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);      
    return false;
  }
}

bool ngt_batch_insert_index(NGTIndex index, float *obj, uint32_t data_count, uint32_t *ids, NGTError error) {
  NGT::Index* pindex = static_cast<NGT::Index*>(index);
  int32_t dim = pindex->getObjectSpace().getDimension();

  bool status = true;
  float *objptr = obj;
  for (size_t idx = 0; idx < data_count; idx++, objptr += dim) {
    try{
      std::vector<double> vobj(objptr, objptr + dim);
      ids[idx] = pindex->insert(vobj);
    }catch(std::exception &err) {
      status = false;
      ids[idx] = 0;
      std::stringstream ss;
      ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
      operate_error_string_(ss, error);      
    }
  }
  return status;
}

bool ngt_create_index(NGTIndex index, uint32_t pool_size, NGTError error) {
  if(index == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: idnex = " << index;
    operate_error_string_(ss, error);            
    return false;
  }
  
  try{
    (static_cast<NGT::Index*>(index))->createIndex(pool_size);
  }catch(std::exception &err) {
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);                  
    return false;
  }
  return true;
}

bool ngt_remove_index(NGTIndex index, ObjectID id, NGTError error) {
  if(index == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: idnex = " << index;
    operate_error_string_(ss, error);                        
    return false;
  }
  
  try{
    (static_cast<NGT::Index*>(index))->remove(id);
  }catch(std::exception &err) {
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);                              
    return false;
  }
  return true;
}

NGTObjectSpace ngt_get_object_space(NGTIndex index, NGTError error) {
  if(index == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: idnex = " << index;
    operate_error_string_(ss, error);                                    
    return NULL;
  }
  
  try{
    return static_cast<NGTObjectSpace>(&(static_cast<NGT::Index*>(index))->getObjectSpace());
  }catch(std::exception &err) {
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);
    return NULL;
  }
}

float* ngt_get_object_as_float(NGTObjectSpace object_space, ObjectID id, NGTError error) {
  if(object_space == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: object_space = " << object_space;
    operate_error_string_(ss, error);      
    return NULL;
  }
  try{
    return static_cast<float*>((static_cast<NGT::ObjectSpace*>(object_space))->getObject(id));
  }catch(std::exception &err) {
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);
    return NULL;
  }
}

uint8_t* ngt_get_object_as_integer(NGTObjectSpace object_space, ObjectID id, NGTError error) {
  if(object_space == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: object_space = " << object_space;
    operate_error_string_(ss, error);      
    return NULL;
  }
  try{
    return static_cast<uint8_t*>((static_cast<NGT::ObjectSpace*>(object_space))->getObject(id));
  }catch(std::exception &err) {
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);
    return NULL;
  }
}

void ngt_destroy_results(NGTObjectDistances results) {
    if(results == NULL) return;
    delete(static_cast<NGT::ObjectDistances*>(results));
}

void ngt_destroy_property(NGTProperty prop) {
    if(prop == NULL) return;  
    delete(static_cast<NGT::Property*>(prop));
}

void ngt_close_index(NGTIndex index) {
    if(index == NULL) return;
    (static_cast<NGT::Index*>(index))->close();
    delete(static_cast<NGT::Index*>(index));
}

int16_t ngt_get_property_edge_size_for_creation(NGTProperty prop, NGTError error) {
  if(prop == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: prop = " << prop;
    operate_error_string_(ss, error);      
    return -1;
  }
  return (*static_cast<NGT::Property*>(prop)).edgeSizeForCreation;
}

int16_t ngt_get_property_edge_size_for_search(NGTProperty prop, NGTError error) {
  if(prop == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: prop = " << prop;
    operate_error_string_(ss, error);            
    return -1;
  }
  return (*static_cast<NGT::Property*>(prop)).edgeSizeForSearch;
}

int32_t ngt_get_property_distance_type(NGTProperty prop, NGTError error){
  if(prop == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: prop = " << prop;
    operate_error_string_(ss, error);                  
    return -1;
  }
  return (*static_cast<NGT::Property*>(prop)).distanceType; 
}

NGTError ngt_create_error_object()
{
  try{
    std::string *error_str = new std::string();
    return static_cast<NGTError>(error_str);
  }catch(std::exception &err){
    std::cerr << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    return NULL;
  }    
}

const char *ngt_get_error_string(const NGTError error)
{
  std::string *error_str = static_cast<std::string*>(error);
  return error_str->c_str();
}

void ngt_clear_error_string(NGTError error)
{
  std::string *error_str = static_cast<std::string*>(error);
  *error_str = "";
}

void ngt_destroy_error_object(NGTError error)
{
  std::string *error_str = static_cast<std::string*>(error);
  delete error_str;
}

NGTOptimizer ngt_create_optimizer(bool logDisabled, NGTError error)
{
  try{
    return static_cast<NGTOptimizer>(new NGT::GraphOptimizer(logDisabled));
  }catch(std::exception &err){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);
    return NULL;
  }
}

bool ngt_optimizer_adjust_search_coefficients(NGTOptimizer optimizer, const char *index, NGTError error) {
  if(optimizer == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: optimizer = " << optimizer;
    operate_error_string_(ss, error);      
    return false;
  }
  try{
    (static_cast<NGT::GraphOptimizer*>(optimizer))->adjustSearchCoefficients(std::string(index));
  }catch(std::exception &err) {
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);                  
    return false;
  }
  return true;
}

bool ngt_optimizer_execute(NGTOptimizer optimizer, const char *inIndex, const char *outIndex, NGTError error) {
  if(optimizer == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: optimizer = " << optimizer;
    operate_error_string_(ss, error);      
    return false;
  }
  try{
    (static_cast<NGT::GraphOptimizer*>(optimizer))->execute(std::string(inIndex), std::string(outIndex));
  }catch(std::exception &err) {
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);                  
    return false;
  }
  return true;
}

// obsolute because of a lack of a parameter
bool ngt_optimizer_set(NGTOptimizer optimizer, int outgoing, int incoming, int nofqs, 
		       float baseAccuracyFrom, float baseAccuracyTo,
		       float rateAccuracyFrom, float rateAccuracyTo,
		       double gte, double m, NGTError error) {
  if(optimizer == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: optimizer = " << optimizer;
    operate_error_string_(ss, error);      
    return false;
  }
  try{
    (static_cast<NGT::GraphOptimizer*>(optimizer))->set(outgoing, incoming, nofqs, baseAccuracyFrom, baseAccuracyTo,
    							rateAccuracyFrom, rateAccuracyTo, gte, m);
  }catch(std::exception &err) {
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);                  
    return false;
  }
  return true;
}

bool ngt_optimizer_set_minimum(NGTOptimizer optimizer, int outgoing, int incoming,
			       int nofqs, int nofrs, NGTError error) {
  if(optimizer == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: optimizer = " << optimizer;
    operate_error_string_(ss, error);      
    return false;
  }
  try{
    (static_cast<NGT::GraphOptimizer*>(optimizer))->set(outgoing, incoming, nofqs, nofrs);
  }catch(std::exception &err) {
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);                  
    return false;
  }
  return true;
}

bool ngt_optimizer_set_extension(NGTOptimizer optimizer,
				 float baseAccuracyFrom, float baseAccuracyTo,
				 float rateAccuracyFrom, float rateAccuracyTo,
				 double gte, double m, NGTError error) {
  if(optimizer == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: optimizer = " << optimizer;
    operate_error_string_(ss, error);      
    return false;
  }
  try{
    (static_cast<NGT::GraphOptimizer*>(optimizer))->setExtension(baseAccuracyFrom, baseAccuracyTo,
								  rateAccuracyFrom, rateAccuracyTo, gte, m);
  }catch(std::exception &err) {
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);                  
    return false;
  }
  return true;
}

bool ngt_optimizer_set_processing_modes(NGTOptimizer optimizer, bool searchParameter, 
					bool prefetchParameter, bool accuracyTable, NGTError error)
{
  if(optimizer == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: optimizer = " << optimizer;
    operate_error_string_(ss, error);      
    return false;
  }

  (static_cast<NGT::GraphOptimizer*>(optimizer))->setProcessingModes(searchParameter, prefetchParameter,
								     accuracyTable);
  return true;
}

void ngt_destroy_optimizer(NGTOptimizer optimizer)
{
    if(optimizer == NULL) return;  
    delete(static_cast<NGT::GraphOptimizer*>(optimizer));
}

bool ngt_refine_anng(NGTIndex index, float epsilon, float accuracy, int noOfEdges, int exploreEdgeSize, size_t batchSize, NGTError error)
{
    NGT::Index* pindex = static_cast<NGT::Index*>(index);    
    try {
      NGT::GraphReconstructor::refineANNG(*pindex, true, epsilon, accuracy, noOfEdges, exploreEdgeSize, batchSize);
    } catch(std::exception &err) {
      std::stringstream ss;
      ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
      operate_error_string_(ss, error);      
      return false;
    }
    return true;
}

bool ngt_get_edges(NGTIndex index, ObjectID id, NGTObjectDistances edges, NGTError error) 
{
  if(index == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: index = " << index;
    operate_error_string_(ss, error);        
    return false;
  }

  NGT::Index*		pindex = static_cast<NGT::Index*>(index);
  NGT::GraphIndex	&graph = static_cast<NGT::GraphIndex&>(pindex->getIndex());

  try {
    NGT::ObjectDistances &objects = *static_cast<NGT::ObjectDistances*>(edges);
    objects = *graph.getNode(id);
  }catch(std::exception &err){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);
    return false;
  }

  return true;
}

uint32_t ngt_get_object_repository_size(NGTIndex index, NGTError error)
{
  if(index == NULL){
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : parametor error: index = " << index;
    operate_error_string_(ss, error);        
    return false;
  }
  NGT::Index&		pindex = *static_cast<NGT::Index*>(index);
  return pindex.getObjectRepositorySize();
}

NGTAnngEdgeOptimizationParameter ngt_get_anng_edge_optimization_parameter()
{
  NGT::GraphOptimizer::ANNGEdgeOptimizationParameter gp;
  NGTAnngEdgeOptimizationParameter parameter;  

  parameter.no_of_queries		= gp.noOfQueries;
  parameter.no_of_results		= gp.noOfResults;
  parameter.no_of_threads		= gp.noOfThreads;
  parameter.target_accuracy		= gp.targetAccuracy;
  parameter.target_no_of_objects	= gp.targetNoOfObjects;
  parameter.no_of_sample_objects	= gp.noOfSampleObjects;
  parameter.max_of_no_of_edges		= gp.maxNoOfEdges;
  parameter.log = false;

  return parameter;
}

bool ngt_optimize_number_of_edges(const char *indexPath, NGTAnngEdgeOptimizationParameter parameter, NGTError error)
{
  
  NGT::GraphOptimizer::ANNGEdgeOptimizationParameter p;

  p.noOfQueries	= parameter.no_of_queries;
  p.noOfResults	= parameter.no_of_results;
  p.noOfThreads	= parameter.no_of_threads;
  p.targetAccuracy	= parameter.target_accuracy;
  p.targetNoOfObjects	= parameter.target_no_of_objects;
  p.noOfSampleObjects	= parameter.no_of_sample_objects;
  p.maxNoOfEdges	= parameter.max_of_no_of_edges;

  try {
    NGT::GraphOptimizer graphOptimizer(!parameter.log); // false=log
    std::string path(indexPath);
    auto edge = graphOptimizer.optimizeNumberOfEdgesForANNG(path, p);
    if (parameter.log) {
      std::cerr << "the optimized number of edges is" << edge.first << "(" << edge.second << ")" << std::endl;
    }
  }catch(std::exception &err) {
    std::stringstream ss;
    ss << "Capi : " << __FUNCTION__ << "() : Error: " << err.what();
    operate_error_string_(ss, error);                  
    return false;
  }
  return true;

}
