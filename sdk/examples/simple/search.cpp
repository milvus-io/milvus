// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.
#include <thirdparty/nlohmann/json.hpp>
#include "interface/ConnectionImpl.h"
#include "include/MilvusApi.h"
#include "grpc/ClientProxy.h"
#include "interface/ConnectionImpl.h"
#include "utils/TimeRecorder.h"
#include "utils/Utils.h"
#include <random>

const int TOP_K = 10;
const int LOOP = 1000;

const int DIM = 128;

std::default_random_engine eng(42);

const milvus::VectorParam 
get_vector_param() {

  milvus::VectorParam vectorParam;
  std::vector<milvus::VectorData> vector_records;

  std::normal_distribution<float> dis(0, 1);
 
  for (int j = 0; j < 1; ++j) {
    milvus::VectorData vectorData;
    std::vector<float> float_data;
    for (int i = 0; i < DIM; ++i) {
      float_data.emplace_back(dis(eng));
    }

    vectorData.float_data = float_data;
    vector_records.emplace_back(vectorData);
  }

  nlohmann::json vector_param_json;
  vector_param_json["num_queries"] = 1;
  vector_param_json["topK"] = TOP_K;
  vector_param_json["field_name"] = "field_vec";
  std::string vector_param_json_string = vector_param_json.dump();

  vectorParam.json_param = vector_param_json_string;
  vectorParam.vector_records = vector_records;

  return vectorParam;
}

bool check_field(milvus::FieldPtr left, milvus::FieldPtr right){

    if (left->field_name != right->field_name){
	std::cout<<"filed_name not match! want "<< left->field_name << " but get "<<right->field_name << std::endl;
	return false;
    }

    if (left->field_type != right->field_type){
	std::cout<<"filed_type not match! want "<< int(left->field_type) << " but get "<< int(right->field_type) << std::endl;
	return false;
    }


    if (left->dim != right->dim){
	std::cout<<"dim not match! want "<< left->dim << " but get "<<right->dim << std::endl;
	return false;
    }

    return true;	
}


bool check_schema(const milvus::Mapping & map){
    // Get Collection info
    bool ret = false;

    milvus::FieldPtr field_ptr1 = std::make_shared<milvus::Field>();
    milvus::FieldPtr field_ptr2 = std::make_shared<milvus::Field>();

    field_ptr1->field_name = "age";
    field_ptr1->field_type = milvus::DataType::INT32;
    field_ptr1->dim = 1;

    field_ptr2->field_name = "field_vec";
    field_ptr2->field_type = milvus::DataType::VECTOR_FLOAT;
    field_ptr2->dim = DIM;

    std::vector<milvus::FieldPtr> fields{field_ptr1, field_ptr2};

    auto size_ = map.fields.size();
    for ( int i =0; i != size_; ++ i){
	auto ret = check_field(fields[i], map.fields[i]);
	if (!ret){
		return false;
	}
    }

    for (auto &f : map.fields) {
      std::cout << f->field_name << ":" << int(f->field_type) << ":" << f->dim << "DIM" << std::endl;
    }

    return true;
}

int main(int argc , char**argv) {
  TestParameters parameters = milvus_sdk::Utils::ParseTestParameters(argc, argv);
  if (!parameters.is_valid){
    return 0;
   }

  if (parameters.collection_name_.empty()){
	std::cout<< "should specify collection name!" << std::endl;
	milvus_sdk::Utils::PrintHelp(argc, argv);
	return 0;
  }


  const std::string collection_name = parameters.collection_name_;
  auto client = milvus::ConnectionImpl();
  milvus::ConnectParam connect_param;
  connect_param.ip_address = parameters.address_.empty() ? "127.0.0.1":parameters.address_;
  connect_param.port = parameters.port_.empty() ? "19530":parameters.port_ ;
  client.Connect(connect_param);


  milvus::Mapping map;
  client.GetCollectionInfo(collection_name, map);
  auto check_ret = check_schema(map);
  if (!check_ret){
	std::cout<<" Schema is not right!"<< std::endl;
	return 0;
  }


  std::vector<std::string> partition_list;
  partition_list.emplace_back("default");

  auto vectorParam = get_vector_param();

  milvus::TopKQueryResult result;
  milvus_sdk::TimeRecorder test_search("search");
  for (int k = 0; k < LOOP; ++k) {
      test_search.Start();
      auto status = client.Search(collection_name, partition_list, "dsl", vectorParam, result);
      test_search.End();
  }
  test_search.Print(LOOP);

  return 0;
}
