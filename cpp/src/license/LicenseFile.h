///*******************************************************************************
// * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// * Unauthorized copying of this file, via any medium is strictly prohibited.
// * Proprietary and confidential.
// ******************************************************************************/
//#pragma once
//
//
//#include <boost/serialization/access.hpp>
//#include <string>
//#include <map>
//
//
//class LicenseFile {
// public:
//    LicenseFile() = default;
//
//    LicenseFile(const int &device_count,
//                const std::map<int, std::string> &uuid_encryption_map,
//                const time_t &starting_time,
//                const time_t &end_time)
//        : device_count_(device_count),
//          uuid_encryption_map_(uuid_encryption_map),
//          starting_time_(starting_time),
//          end_time_(end_time) {}
//
//    int get_device_count() {
//        return device_count_;
//    }
//    std::map<int, std::string> &get_uuid_encryption_map() {
//        return uuid_encryption_map_;
//    }
//    time_t get_starting_time() {
//        return starting_time_;
//    }
//    time_t get_end_time() {
//        return end_time_;
//    }
//
// public:
//    friend class boost::serialization::access;
//
//    template<typename Archive>
//    void serialize(Archive &ar, const unsigned int version) {
//        ar & device_count_;
//        ar & uuid_encryption_map_;
//        ar & starting_time_;
//        ar & end_time_;
//    }
//
// public:
//    int device_count_ = 0;
//    std::map<int, std::string> uuid_encryption_map_;
//    time_t starting_time_ = 0;
//    time_t end_time_ = 0;
//};
//
//class SerializedLicenseFile {
// public:
//    ~SerializedLicenseFile() {
//        if (license_file_ != nullptr) {
//            delete (license_file_);
//            license_file_ = nullptr;
//        }
//    }
//
//    void
//    set_license_file(LicenseFile *license_file) {
//        license_file_ = license_file;
//    }
//
//    LicenseFile *get_license_file() {
//        return license_file_;
//    }
// private:
//    friend class boost::serialization::access;
//
//    template<typename Archive>
//    void serialize(Archive &ar, const unsigned int version) {
//        ar & license_file_;
//    }
//
// private:
//    LicenseFile *license_file_ = nullptr;
//};
//
