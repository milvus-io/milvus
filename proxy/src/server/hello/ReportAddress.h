#pragma once

#include "grpc++/grpc++.h"
#include "src/grpc/master.grpc.pb.h"

class ReportClient {
public:
    ReportClient(const std::shared_ptr<::grpc::ChannelInterface>& channel)
            : stub_(master::Greeter::NewStub(channel)) {}

    milvus::Status
    ReportAddress(){
        grpc::ClientContext context;
        master::Request req;
        master::Reply rsp;

        req.set_address("192.168.2.18:19530");
        auto status =  stub_->ReportAddress(&context, req, &rsp);
        if (status.ok() && rsp.status()){
            std::cout << "Report address to master Succeed" << std::endl;
            return milvus::Status::OK();
        } else{
            std::cout << "Error occur when report address to master " << status.error_message() << std::endl;
            return milvus::Status(milvus::SERVER_ERROR_CODE_BASE, "");
        }
    }

private:
    std::unique_ptr<master::Greeter::Stub> stub_;
};