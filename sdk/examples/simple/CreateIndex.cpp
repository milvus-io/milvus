#include <string>
#include "interface/ConnectionImpl.h"
#include "utils/Utils.h"

const std::string COLLECTION = "collection_0";

int main(int argc, char *argv[]) {
  TestParameters parameters = milvus_sdk::Utils::ParseTestParameters(argc, argv);
  if (!parameters.is_valid){
    return 0;
  }
  auto client = milvus::ConnectionImpl();
  milvus::ConnectParam connect_param;
  connect_param.ip_address = parameters.address_.empty() ? "127.0.0.1":parameters.address_;
  connect_param.port = parameters.port_.empty() ? "19530":parameters.port_ ;

  client.Connect(connect_param);

  JSON json_params = {{"index_type", "IVF_FLAT"}, {"metric_type", "L2"}, {"params", {{"nlist", 100}}}};
  milvus::IndexParam index1 = {COLLECTION, "field_vec", json_params.dump()};
  milvus_sdk::Utils::PrintIndexParam(index1);
  milvus::Status stat = client.CreateIndex(index1);
  std::cout << "CreateIndex function call status: " << stat.message() << std::endl;

  return 0;
}