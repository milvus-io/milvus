#include "server/grpc_impl/interceptor/GrpcInterceptorHookHandler.h"

namespace milvus {
namespace server {
namespace grpc {

void
GrpcInterceptorHookHandler::OnPostRecvInitialMetaData(
    ::grpc::experimental::ServerRpcInfo* server_rpc_info,
    ::grpc::experimental::InterceptorBatchMethods* interceptor_batch_methods) {
}

void
GrpcInterceptorHookHandler::OnPreSendMessage(::grpc::experimental::ServerRpcInfo* server_rpc_info,
                                             ::grpc::experimental::InterceptorBatchMethods* interceptor_batch_methods) {
}

}  // namespace grpc
}  // namespace server
}  // namespace milvus
