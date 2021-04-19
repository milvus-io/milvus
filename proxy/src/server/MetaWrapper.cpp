#include "MetaWrapper.h"
#include "config/ServerConfig.h"
namespace milvus{
namespace server {

MetaWrapper& MetaWrapper::GetInstance() {
  static MetaWrapper wrapper;
  return wrapper;
}

Status MetaWrapper::Init() {
  auto addr = config.master.address() + ":" + std::to_string(config.master.port());
  client_ = std::make_shared<milvus::master::GrpcClient>(addr);
}

std::shared_ptr<milvus::master::GrpcClient> MetaWrapper::MetaClient() {
  return client_;
}

}
}