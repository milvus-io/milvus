//#include "S3ClientWrapper.h"
//
//#include <aws/s3/model/CreateBucketRequest.h>
//#include <aws/s3/model/DeleteBucketRequest.h>
//#include <aws/s3/model/PutObjectRequest.h>
//#include <aws/s3/model/GetObjectRequest.h>
//#include <aws/s3/model/DeleteObjectRequest.h>
//
//#include <iostream>
//#include <fstream>
//
//
//namespace zilliz {
//namespace milvus {
//namespace engine {
//namespace storage {
//
//Status
//S3ClientWrapper::Create(const std::string &ip_address,
//                        const std::string &port,
//                        const std::string &access_key,
//                        const std::string &secret_key) {
//    Aws::InitAPI(options_);
//    Aws::Client::ClientConfiguration cfg;
//
//    // TODO: ip_address need to be validated.
//
//    cfg.endpointOverride = ip_address + ":" + port; // S3 server ip address and port
//    cfg.scheme = Aws::Http::Scheme::HTTP;
//    cfg.verifySSL =
//        false; //Aws::Auth::AWSCredentials cred("RPW421T9GSIO4A45Y9ZR", "2owKYy9emSS90Q0pXuyqpX1OxBCyEDYodsiBemcq"); // 认证的Key
//    client_ =
//        new S3Client(Aws::Auth::AWSCredentials(access_key, secret_key),
//                     cfg,
//                     Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Always,
//                     false);
//    if (client_ == nullptr) {
//        std::string error = "Can't connect server.";
//        return Status::Error(error);
//    } else {
//        return Status::OK();
//    }
//}
//
//
//Status
//S3ClientWrapper::Close() {
//    if (client_ != nullptr) {
//        delete client_;
//        client_ = nullptr;
//    }
//    Aws::ShutdownAPI(options_);
//    return Status::OK();
//}
//
//Status
//S3ClientWrapper::CreateBucket(std::string& bucket_name) {
//    Aws::S3::Model::CreateBucketRequest request;
//    request.SetBucket(bucket_name);
//
//    auto outcome = client_->CreateBucket(request);
//
//    if (outcome.IsSuccess())
//    {
//        return Status::OK();
//    }
//    else
//    {
//        std::cout << "CreateBucket error: "
//                  << outcome.GetError().GetExceptionName() << std::endl
//                  << outcome.GetError().GetMessage() << std::endl;
//        switch(outcome.GetError().GetErrorType()) {
//            case Aws::S3::S3Errors::BUCKET_ALREADY_EXISTS:
//            case Aws::S3::S3Errors::BUCKET_ALREADY_OWNED_BY_YOU:
//                return Status::AlreadyExist(outcome.GetError().GetMessage());
//            default:
//                return Status::Error(outcome.GetError().GetMessage());
//        }
//    }
//}
//
//Status
//S3ClientWrapper::DeleteBucket(std::string& bucket_name) {
//    Aws::S3::Model::DeleteBucketRequest bucket_request;
//    bucket_request.SetBucket(bucket_name);
//
//    auto outcome = client_->DeleteBucket(bucket_request);
//
//    if (outcome.IsSuccess())
//    {
//        return Status::OK();
//    }
//    else
//    {
//        std::cout << "DeleteBucket error: "
//                  << outcome.GetError().GetExceptionName() << " - "
//                  << outcome.GetError().GetMessage() << std::endl;
//        return Status::Error(outcome.GetError().GetMessage());
//    }
//}
//
//Status
//S3ClientWrapper::UploadFile(std::string &BucketName, std::string &objectKey, std::string &pathkey) {
//
//    PutObjectRequest putObjectRequest;
//    putObjectRequest.WithBucket(BucketName.c_str()).WithKey(objectKey.c_str());
//
//    auto input_data = Aws::MakeShared<Aws::FStream>("PutObjectInputStream",
//                                                    pathkey.c_str(),
//                                                    std::ios_base::in | std::ios_base::binary);
//    putObjectRequest.SetBody(input_data);
//    auto put_object_result = client_->PutObject(putObjectRequest);
//    if (put_object_result.IsSuccess()) {
//        return Status::OK();
//    } else {
//        std::cout << "PutObject error: " << put_object_result.GetError().GetExceptionName() << " "
//                  << put_object_result.GetError().GetMessage() << std::endl;
//        return Status::Error(put_object_result.GetError().GetMessage());
//    }
//}
//
//Status
//S3ClientWrapper::DownloadFile(std::string &BucketName, std::string &objectKey, std::string &pathkey) {
//    GetObjectRequest object_request;
//    object_request.WithBucket(BucketName.c_str()).WithKey(objectKey.c_str());
//    auto get_object_outcome = client_->GetObject(object_request);
//    if (get_object_outcome.IsSuccess()) {
//        Aws::OFStream local_file(pathkey.c_str(), std::ios::out | std::ios::binary);
//        local_file << get_object_outcome.GetResult().GetBody().rdbuf();
//        return Status::OK();
//    } else {
//        std::cout << "GetObject error: " << get_object_outcome.GetError().GetExceptionName() << " "
//                  << get_object_outcome.GetError().GetMessage() << std::endl;
//        return Status::Error(get_object_outcome.GetError().GetMessage());
//    }
//}
//
//Status
//S3ClientWrapper::DeleteFile(std::string &bucket_name, std::string &object_key) {
//    Aws::S3::Model::DeleteObjectRequest object_request;
//    object_request.WithBucket(bucket_name).WithKey(object_key);
//
//    auto delete_object_outcome = client_->DeleteObject(object_request);
//
//    if (delete_object_outcome.IsSuccess()) {
//        return Status::OK();
//    } else {
//        std::cout << "DeleteObject error: " <<
//                  delete_object_outcome.GetError().GetExceptionName() << " " <<
//                  delete_object_outcome.GetError().GetMessage() << std::endl;
//
//        return Status::Error(delete_object_outcome.GetError().GetMessage());
//    }
//}
//
//}
//}
//}
//}