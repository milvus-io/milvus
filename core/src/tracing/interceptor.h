#pragma once

#include <grpcpp/impl/codegen/interceptor.h>
#include <grpcpp/impl/codegen/server_interceptor.h>
#include <opentracing/tracer.h>
#include <memory>

class SpanInterceptor : public grpc::experimental::Interceptor {
 public:
    SpanInterceptor(grpc::experimental::ServerRpcInfo* info, std::shared_ptr<opentracing::Tracer> tracer);

    void
    Intercept(grpc::experimental::InterceptorBatchMethods* methods) override;

 private:
    grpc::experimental::ServerRpcInfo* info_;
    std::shared_ptr<opentracing::Tracer> tracer_;
    std::unique_ptr<opentracing::Span> span_;
};

class SpanInterceptorFactory : public grpc::experimental::ServerInterceptorFactoryInterface {
 public:
    explicit SpanInterceptorFactory(std::shared_ptr<opentracing::Tracer> tracer) : tracer_(tracer) {
    }

    grpc::experimental::Interceptor*
    CreateServerInterceptor(grpc::experimental::ServerRpcInfo* info) override;

 private:
    std::shared_ptr<opentracing::Tracer> tracer_;
};
