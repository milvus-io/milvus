#pragma once

#include "db/Status.h"

#include <string>


namespace zilliz {
namespace milvus {
namespace engine {
namespace storage {

class IStorage {
 public:
    virtual Status Create(const std::string &ip_address,
                          const std::string &port,
                          const std::string &access_key,
                          const std::string &secret_key) = 0;

    virtual Status Close() = 0;

    virtual Status CreateBucket(std::string& bucket_name) = 0;
    virtual Status DeleteBucket(std::string& bucket_name) = 0;
    virtual Status UploadFile(std::string &bucket_name, std::string &object_key, std::string &path_key) = 0;
    virtual Status DownloadFile(std::string &bucket_name, std::string &object_key, std::string &path_key) = 0;
    virtual Status DeleteFile(std::string &bucket_name, std::string &object_key) = 0;
};

}
}
}
}