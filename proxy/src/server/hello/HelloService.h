#ifndef MILVUS_HELLOSERVICE_H
#define MILVUS_HELLOSERVICE_H

#include "grpc++/grpc++.h"
#include <src/grpc/hello.grpc.pb.h>

class HelloService final : public ::milvus::grpc::HelloService::Service{
    ::grpc::Status SayHello
            (::grpc::ServerContext* context, const ::milvus::grpc::HelloRequest* request, ::milvus::grpc::HelloReply* response){
        const auto& name = request->name();
        response->mutable_msg()->append( "hello " +  name);
        return ::grpc::Status::OK;
    }
};


#endif //MILVUS_HELLOSERVICE_H
