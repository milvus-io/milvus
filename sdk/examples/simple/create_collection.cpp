
#include <Status.h>
#include <Field.h>
#include <MilvusApi.h>
#include <interface/ConnectionImpl.h>
#include "utils/Utils.h"

const int DIM = 128;

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

  auto client = milvus::ConnectionImpl();
  milvus::ConnectParam connect_param;
  connect_param.ip_address = parameters.address_.empty() ? "127.0.0.1":parameters.address_;
  connect_param.port = parameters.port_.empty() ? "19530":parameters.port_ ;
  client.Connect(connect_param);

  const std::string collection_name = parameters.collection_name_;

  // Create
  milvus::FieldPtr field_ptr1 = std::make_shared<milvus::Field>();
  milvus::FieldPtr field_ptr2 = std::make_shared<milvus::Field>();

  field_ptr1->field_name = "age";
  field_ptr1->field_type = milvus::DataType::INT32;
  field_ptr1->dim = 1;

  field_ptr2->field_name = "field_vec";
  field_ptr2->field_type = milvus::DataType::VECTOR_FLOAT;
  field_ptr2->dim = DIM;

  milvus::Mapping mapping = {collection_name, {field_ptr1, field_ptr2}};

  milvus::Status stat;
  stat = client.CreateCollection(mapping, "extra_params");
  if (!stat.ok()){
  	std::cout << "create collection failed!" << std::endl;
	return 0;
  }

  std::cout << "create collection done!" << std::endl;

  // Get Collection info
  milvus::Mapping map;
  client.GetCollectionInfo(collection_name, map);
  for (auto &f : map.fields) {
    std::cout << f->field_name << ":" << int(f->field_type) << ":" << f->dim << "DIM" << std::endl;
  }

  return 0;

}
