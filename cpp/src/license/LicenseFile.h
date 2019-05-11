/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once


#include <boost/serialization/access.hpp>
#include <string>
#include <map>

class LicenseFile {
 public:
    LicenseFile() = default;

    LicenseFile(const int &device_count, const std::map<int, std::string> &uuid_encryption_map, const int64_t &remaining_hour)
        : device_count_(device_count), uuid_encryption_map_(uuid_encryption_map), remaining_hour_(remaining_hour) {}

    int get_device_count() {
        return device_count_;
    }
    std::map<int, std::string> &get_uuid_encryption_map() {
        return uuid_encryption_map_;
    }
    int64_t get_remaining_hour() {
        return remaining_hour_;
    }

 public:
    friend class boost::serialization::access;

    template<typename Archive>
    void serialize(Archive &ar, const unsigned int version) {
        ar & device_count_;
        ar & uuid_encryption_map_;
        ar & remaining_hour_;
    }

 public:
    int device_count_ = 0;
    std::map<int, std::string> uuid_encryption_map_;
    int64_t remaining_hour_ = 0;
};

class SerializedLicenseFile {
 public:
    ~SerializedLicenseFile() {
        if(license_file_ != nullptr) {
            delete(license_file_);
            license_file_ = nullptr;
        }
    }

    void
    set_license_file(LicenseFile* license_file) {
        license_file_ = license_file;
    }

    LicenseFile* get_license_file() {
        return license_file_;
    }
 private:
    friend class boost::serialization::access;

    template<typename Archive>
    void serialize(Archive &ar, const unsigned int version) {
        ar & license_file_;
    }

 private:
    LicenseFile *license_file_ = nullptr;
};

