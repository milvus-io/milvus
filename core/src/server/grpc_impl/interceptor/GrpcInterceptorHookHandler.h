#pragma once

#include <grpcpp/impl/codegen/interceptor.h>
#include <grpcpp/impl/codegen/server_interceptor.h>

class GrpcInterceptorHookHandler {
 public:
    virtual void
    OnPostRecvInitialMetaData(::grpc::experimental::ServerRpcInfo* server_rpc_info,
                              ::grpc::experimental::InterceptorBatchMethods* interceptor_batch_methods);

    virtual void
    OnPreSendMessage(::grpc::experimental::ServerRpcInfo* server_rpc_info,
                     ::grpc::experimental::InterceptorBatchMethods* interceptor_batch_methods);
};