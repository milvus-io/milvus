#include <Status.h>
#include <Field.h>
#include <MilvusApi.h>
#include <interface/ConnectionImpl.h>
#include "utils/Utils.h"

const int DIM = 128;

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
  if (!parameters.is_valid) {
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
  connect_param.ip_address = parameters.address_.empty() ? "127.0.0.1" : parameters.address_;
  connect_param.port = parameters.port_.empty() ? "19530" : parameters.port_;
  client.Connect(connect_param);

  milvus::Mapping map;
  client.GetCollectionInfo(collection_name, map);
  auto check_ret = check_schema(map);
  if (!check_ret){
	std::cout<<" Schema is not right!"<< std::endl;
	return 0;
  }


  milvus::Status stat;
  int64_t count = 0;
  stat = client.CountEntities(collection_name, count);
  if (!stat.ok()){
    std::cerr << "Error: " << stat.message() << std::endl;
  }
  std::cout << "Collection " <<collection_name<< " rows: " << count << std::endl;
  return 0;
}
