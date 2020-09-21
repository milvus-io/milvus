#include "GrpcClient.h"
#include "grpc++/grpc++.h"

using grpc::ClientContext;

namespace milvus {
namespace master {
GrpcClient::GrpcClient(const std::string &addr) {
  auto channel = ::grpc::CreateChannel(addr, ::grpc::InsecureChannelCredentials());
  stub_ = masterpb::Master::NewStub(channel);
}

GrpcClient::GrpcClient(std::shared_ptr<::grpc::Channel> &channel)
    : stub_(masterpb::Master::NewStub(channel)) {
}

Status GrpcClient::CreateCollection(const milvus::grpc::Mapping &mapping) {
  ClientContext context;
  ::milvus::grpc::Status response;
  ::grpc::Status grpc_status = stub_->CreateCollection(&context, mapping, &response);

  if (!grpc_status.ok()) {
    std::cerr << "CreateHybridCollection gRPC failed!" << grpc_status.error_message() << std::endl;
    return Status(grpc_status.error_code(), grpc_status.error_message());
  }

  if (response.error_code() != grpc::SUCCESS) {
    // TODO: LOG
    return Status(response.error_code(), response.reason());
  }
  return Status::OK();
}

}
}