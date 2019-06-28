//#pragma once
//
//#include "storage/IStorage.h"
//
//
//#include <aws/s3/S3Client.h>
//#include <aws/core/Aws.h>
//#include <aws/core/auth/AWSCredentialsProvider.h>
//
//
//using namespace Aws::S3;
//using namespace Aws::S3::Model;
//
//namespace zilliz {
//namespace milvus {
//namespace engine {
//namespace storage {
//
//class S3ClientWrapper : public IStorage {
// public:
//
//    S3ClientWrapper() = default;
//    ~S3ClientWrapper() = default;
//
//    Status Create(const std::string &ip_address,
//                const std::string &port,
//                const std::string &access_key,
//                const std::string &secret_key) override;
//    Status Close() override;
//
//    Status CreateBucket(std::string& bucket_name) override;
//    Status DeleteBucket(std::string& bucket_name) override;
//    Status UploadFile(std::string &BucketName, std::string &objectKey, std::string &pathkey) override;
//    Status DownloadFile(std::string &BucketName, std::string &objectKey, std::string &pathkey) override;
//    Status DeleteFile(std::string &bucket_name, std::string &object_key) override;
//
// private:
//    S3Client *client_ = nullptr;
//    Aws::SDKOptions options_;
//};
//
//}
//}
//}
//}