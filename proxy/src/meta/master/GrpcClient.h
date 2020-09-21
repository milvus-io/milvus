#pragma once
#include "grpc/master.grpc.pb.h"
#include "grpc/message.pb.h"
#include "grpc++/grpc++.h"
#include "utils/Status.h"

namespace milvus {
namespace master {
class GrpcClient {
 public:
  explicit GrpcClient(const std::string& addr);
  explicit GrpcClient(std::shared_ptr<::grpc::Channel>& channel);
  ~GrpcClient() = default;

 public:
  Status
  CreateCollection(const milvus::grpc::Mapping& mapping);

  Status
  CreateIndex(const milvus::grpc::IndexParam& request);

 private:
  std::unique_ptr<masterpb::Master::Stub> stub_;

};


}
}

