#include "LicenseCheck.h"
#include <iostream>
#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
//#include <boost/foreach.hpp>
//#include <boost/serialization/vector.hpp>
#include <boost/filesystem/path.hpp>
#include <boost/serialization/map.hpp>
#include <boost/filesystem/operations.hpp>


namespace zilliz {
namespace vecwise {
namespace server {


//ServerError
//LicenseCheck::IntegrityCheck(const std::string &license_file_path, const std::string &secret_file_path) {
//
//    std::string file_md5;
//    LicenseLibrary::GetFileMD5(license_file_path, file_md5);
//    time_t update_time;
//    off_t file_size;
//    LicenseLibrary::GetFileUpdateTimeAndSize(license_file_path, update_time, file_size);
//    time_t system_time;
//    LicenseLibrary::GetSystemTime(system_time);
//
//    std::string output_file_md5;
//    time_t output_update_time;
//    off_t output_file_size;
//    time_t output_starting_time;
//    time_t output_end_time;
//    LicenseLibrary::SecretFileDeserialization(secret_file_path,
//                                              output_update_time,
//                                              output_file_size,
//                                              output_starting_time,
//                                              output_end_time,
//                                              output_file_md5);
//    if (file_md5 != output_file_md5) {
//        printf("License file has been modified\n");
//        return SERVER_UNEXPECTED_ERROR;
//    }
//    if (update_time != output_update_time) {
//        printf("update_time is wrong\n");
//        return SERVER_UNEXPECTED_ERROR;
//    }
//    if (file_size != output_file_size) {
//        printf("file_size is wrong\n");
//        return SERVER_UNEXPECTED_ERROR;
//    }
//    if (system_time < output_starting_time || system_time > output_end_time) {
//        printf("License expired\n");
//        return SERVER_UNEXPECTED_ERROR;
//    }
//    return SERVER_SUCCESS;
//}

// Part 1:  Legality check

ServerError
LicenseCheck::LegalityCheck(const std::string &license_file_path) {

    int device_count;
    LicenseLibrary::GetDeviceCount(device_count);
    std::vector<std::string> uuid_array;
    LicenseLibrary::GetUUID(device_count, uuid_array);

    std::vector<std::string> sha_array;
    LicenseLibrary::GetUUIDSHA256(device_count, uuid_array, sha_array);

    int output_device_count;
    std::map<int, std::string> uuid_encryption_map;
    time_t starting_time;
    time_t end_time;
    ServerError err = LicenseLibrary::LicenseFileDeserialization(license_file_path,
                                               output_device_count,
                                               uuid_encryption_map,
                                               starting_time,
                                               end_time);
    if(err !=SERVER_SUCCESS)
    {
        printf("License check error: 01\n");
        return SERVER_UNEXPECTED_ERROR;
    }
    time_t system_time;
    LicenseLibrary::GetSystemTime(system_time);

    if (device_count != output_device_count) {
        printf("License check error: 02\n");
        return SERVER_UNEXPECTED_ERROR;
    }
    for (int i = 0; i < device_count; ++i) {
        if (sha_array[i] != uuid_encryption_map[i]) {
            printf("License check error: 03\n");
            return SERVER_UNEXPECTED_ERROR;
        }
    }
    if (system_time < starting_time || system_time > end_time) {
        printf("License check error: 04\n");
        return SERVER_UNEXPECTED_ERROR;
    }
    printf("Legality Check Success\n");
    return SERVER_SUCCESS;
}

// Part 2: Timing check license

ServerError
LicenseCheck::AlterFile(const std::string &license_file_path,
                        const boost::system::error_code &ec,
                        boost::asio::deadline_timer *pt) {

    ServerError err = LegalityCheck(license_file_path);
    if(err!=SERVER_SUCCESS)
    {
        exit(1);
    }
    printf("---runing---\n");
    pt->expires_at(pt->expires_at() + boost::posix_time::hours(1));
    pt->async_wait(boost::bind(AlterFile, license_file_path, boost::asio::placeholders::error, pt));
    return SERVER_SUCCESS;

}

ServerError
LicenseCheck::StartCountingDown(const std::string &license_file_path) {

    if (!LicenseLibrary::IsFileExistent(license_file_path)) return SERVER_LICENSE_FILE_NOT_EXIST;
    boost::asio::io_service io;
    boost::asio::deadline_timer t(io, boost::posix_time::hours(1));
    t.async_wait(boost::bind(AlterFile, license_file_path, boost::asio::placeholders::error, &t));
    io.run();
    return SERVER_SUCCESS;
}

}
}
}