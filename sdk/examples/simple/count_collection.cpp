#include <Status.h>
#include <Field.h>
#include <MilvusApi.h>
#include <interface/ConnectionImpl.h>
#include "utils/Utils.h"

int main(int argc , char**argv) {

  TestParameters parameters = milvus_sdk::Utils::ParseTestParameters(argc, argv);
  if (!parameters.is_valid) {
    return 0;
  }
  auto client = milvus::ConnectionImpl();
  milvus::ConnectParam connect_param;
  connect_param.ip_address = parameters.address_.empty() ? "127.0.0.1" : parameters.address_;
  connect_param.port = parameters.port_.empty() ? "19530" : parameters.port_;
  client.Connect(connect_param);

  milvus::Status stat;
  const std::string collectin_name = "collection1";

  int64_t count = 0;
  stat = client.CountEntities(collectin_name, count);
  if (!stat.ok()){
    std::cerr << "Error: " << stat.message() << std::endl;
  }
  std::cout << "Collection " << collectin_name << " rows: " << count << std::endl;

}