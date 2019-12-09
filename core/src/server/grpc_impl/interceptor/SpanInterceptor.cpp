#include "SpanInterceptor.h"

#include "tracing/TracerUtil.h"

namespace milvus {
namespace server {
namespace grpc {

SpanInterceptor::SpanInterceptor(::grpc::experimental::ServerRpcInfo* info, GrpcInterceptorHookHandler* hook_handler)
    : info_(info), hook_handler_(hook_handler) {
}

void
SpanInterceptor::Intercept(::grpc::experimental::InterceptorBatchMethods* methods) {
    if (methods->QueryInterceptionHookPoint(::grpc::experimental::InterceptionHookPoints::POST_RECV_INITIAL_METADATA)) {
        hook_handler_->OnPostRecvInitialMetaData(info_, methods);

    } else if (methods->QueryInterceptionHookPoint(::grpc::experimental::InterceptionHookPoints::PRE_SEND_MESSAGE)) {
        hook_handler_->OnPreSendMessage(info_, methods);
    }

    methods->Proceed();
}

::grpc::experimental::Interceptor*
SpanInterceptorFactory::CreateServerInterceptor(::grpc::experimental::ServerRpcInfo* info) {
    return new SpanInterceptor(info, hook_handler_);
}

}  // namespace grpc
}  // namespace server
}  // namespace milvus
