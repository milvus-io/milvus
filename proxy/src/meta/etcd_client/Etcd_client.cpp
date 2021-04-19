#include "Etcd_client.h"
#include "grpc++/grpc++.h"

namespace milvus{
namespace master{

EtcdClient::EtcdClient(const std::string &addr) {
  auto channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
  stub_ = etcdserverpb::KV::NewStub(channel);
}

Status
EtcdClient::Range(const etcdserverpb::RangeRequest& request, etcdserverpb::RangeResponse& response){
  ::grpc::ClientContext context;
  auto status = stub_->Range(&context, request, &response);
  if (!status.ok()){
    return Status(DB_ERROR, status.error_message());
  }
  return Status::OK();
}

}
}