//#include "LicenseCheck.h"
//#include <iostream>
//#include <thread>
//
//#include <boost/archive/binary_oarchive.hpp>
//#include <boost/archive/binary_iarchive.hpp>
////#include <boost/foreach.hpp>
////#include <boost/serialization/vector.hpp>
//#include <boost/filesystem/path.hpp>
//#include <boost/serialization/map.hpp>
//#include <boost/filesystem/operations.hpp>
//#include <boost/thread.hpp>
//#include <boost/date_time/posix_time/posix_time.hpp>
//
//
//namespace zilliz {
//namespace milvus {
//namespace server {
//
//LicenseCheck::LicenseCheck() {
//
//}
//
//LicenseCheck::~LicenseCheck() {
//    StopCountingDown();
//}
//
//ServerError
//LicenseCheck::LegalityCheck(const std::string &license_file_path) {
//
//    int device_count;
//    LicenseLibrary::GetDeviceCount(device_count);
//    std::vector<std::string> uuid_array;
//    LicenseLibrary::GetUUID(device_count, uuid_array);
//
//    std::vector<std::string> sha_array;
//    LicenseLibrary::GetUUIDSHA256(device_count, uuid_array, sha_array);
//
//    int output_device_count;
//    std::map<int, std::string> uuid_encryption_map;
//    time_t starting_time;
//    time_t end_time;
//    ServerError err = LicenseLibrary::LicenseFileDeserialization(license_file_path,
//                                               output_device_count,
//                                               uuid_encryption_map,
//                                               starting_time,
//                                               end_time);
//    if(err !=SERVER_SUCCESS)
//    {
//        std::cout << "License check error: 01" << std::endl;
//        return SERVER_UNEXPECTED_ERROR;
//    }
//    time_t system_time;
//    LicenseLibrary::GetSystemTime(system_time);
//
//    if (device_count != output_device_count) {
//        std::cout << "License check error: 02" << std::endl;
//        return SERVER_UNEXPECTED_ERROR;
//    }
//    for (int i = 0; i < device_count; ++i) {
//        if (sha_array[i] != uuid_encryption_map[i]) {
//            std::cout << "License check error: 03" << std::endl;
//            return SERVER_UNEXPECTED_ERROR;
//        }
//    }
//    if (system_time < starting_time || system_time > end_time) {
//        std::cout << "License check error: 04" << std::endl;
//        return SERVER_UNEXPECTED_ERROR;
//    }
//    std::cout << "Legality Check Success" << std::endl;
//    return SERVER_SUCCESS;
//}
//
//// Part 2: Timing check license
//
//ServerError
//LicenseCheck::AlterFile(const std::string &license_file_path,
//                        const boost::system::error_code &ec,
//                        boost::asio::deadline_timer *pt) {
//
//    ServerError err = LicenseCheck::LegalityCheck(license_file_path);
//    if(err!=SERVER_SUCCESS) {
//        std::cout << "license file check error" << std::endl;
//        exit(1);
//    }
//
//    std::cout << "---runing---" << std::endl;
//    pt->expires_at(pt->expires_at() + boost::posix_time::hours(1));
//    pt->async_wait(boost::bind(LicenseCheck::AlterFile, license_file_path, boost::asio::placeholders::error, pt));
//
//    return SERVER_SUCCESS;
//
//}
//
//ServerError
//LicenseCheck::StartCountingDown(const std::string &license_file_path) {
//
//    if (!LicenseLibrary::IsFileExistent(license_file_path)) {
//        std::cout << "license file not exist" << std::endl;
//        exit(1);
//    }
//
//    //create a thread to run AlterFile
//    if(counting_thread_ == nullptr) {
//        counting_thread_ = std::make_shared<std::thread>([&]() {
//            boost::asio::deadline_timer t(io_service_, boost::posix_time::hours(1));
//            t.async_wait(boost::bind(LicenseCheck::AlterFile, license_file_path, boost::asio::placeholders::error, &t));
//            io_service_.run();//this thread will block here
//        });
//    }
//
//    return SERVER_SUCCESS;
//}
//
//ServerError
//LicenseCheck::StopCountingDown() {
//    if(!io_service_.stopped()) {
//        io_service_.stop();
//    }
//
//    if(counting_thread_ != nullptr) {
//        counting_thread_->join();
//        counting_thread_ = nullptr;
//    }
//
//    return SERVER_SUCCESS;
//}
//
//}
//}
//}