#include "GrpcInterceptorHookHandler.h"

void
GrpcInterceptorHookHandler::OnPostRecvInitialMetaData(
    ::grpc::experimental::ServerRpcInfo* server_rpc_info,
    ::grpc::experimental::InterceptorBatchMethods* interceptor_batch_methods) {
}

void
GrpcInterceptorHookHandler::OnPreSendMessage(::grpc::experimental::ServerRpcInfo* server_rpc_info,
                                             ::grpc::experimental::InterceptorBatchMethods* interceptor_batch_methods) {
}
