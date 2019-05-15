/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include <boost/serialization/access.hpp>
#include <string>
#include <map>


class GPUInfoFile {
 public:
    GPUInfoFile() = default;

    GPUInfoFile(const int &device_count, const std::map<int, std::string> &uuid_encryption_map)
        : device_count_(device_count), uuid_encryption_map_(uuid_encryption_map) {}

    int get_device_count() {
        return device_count_;
    }
    std::map<int, std::string> &get_uuid_encryption_map() {
        return uuid_encryption_map_;
    }


 public:
    friend class boost::serialization::access;

    template<typename Archive>
    void serialize(Archive &ar, const unsigned int version) {
        ar & device_count_;
        ar & uuid_encryption_map_;
    }

 public:
    int device_count_ = 0;
    std::map<int, std::string> uuid_encryption_map_;
};

class SerializedGPUInfoFile {
 public:
    ~SerializedGPUInfoFile() {
        if (gpu_info_file_ != nullptr) {
            delete (gpu_info_file_);
            gpu_info_file_ = nullptr;
        }
    }

    void
    set_gpu_info_file(GPUInfoFile *gpu_info_file) {
        gpu_info_file_ = gpu_info_file;
    }

    GPUInfoFile *get_gpu_info_file() {
        return gpu_info_file_;
    }
 private:
    friend class boost::serialization::access;

    template<typename Archive>
    void serialize(Archive &ar, const unsigned int version) {
        ar & gpu_info_file_;
    }

 private:
    GPUInfoFile *gpu_info_file_ = nullptr;
};
