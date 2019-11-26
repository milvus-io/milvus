#pragma once

#include <grpcpp/impl/codegen/interceptor.h>
#include <grpcpp/impl/codegen/server_interceptor.h>
#include <opentracing/tracer.h>
#include <memory>

#include "GrpcInterceptorHookHandler.h"

class SpanInterceptor : public grpc::experimental::Interceptor {
 public:
    SpanInterceptor(grpc::experimental::ServerRpcInfo* info, GrpcInterceptorHookHandler* hook_handler);

    void
    Intercept(grpc::experimental::InterceptorBatchMethods* methods) override;

 private:
    grpc::experimental::ServerRpcInfo* info_;
    GrpcInterceptorHookHandler* hook_handler_;
    //    std::shared_ptr<opentracing::Tracer> tracer_;
    //    std::unique_ptr<opentracing::Span> span_;
};

class SpanInterceptorFactory : public grpc::experimental::ServerInterceptorFactoryInterface {
 public:
    explicit SpanInterceptorFactory(GrpcInterceptorHookHandler* hook_handler) : hook_handler_(hook_handler) {
    }

    grpc::experimental::Interceptor*
    CreateServerInterceptor(grpc::experimental::ServerRpcInfo* info) override;

 private:
    GrpcInterceptorHookHandler* hook_handler_;
};
