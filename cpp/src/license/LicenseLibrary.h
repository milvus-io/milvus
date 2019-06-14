#pragma once

#include "LicenseFile.h"
#include "GPUInfoFile.h"

#include "utils/Error.h"

#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include <vector>
#include <map>
#include <time.h>


namespace zilliz {
namespace milvus {
namespace server {

class LicenseLibrary {
 public:
    // Part 0: File check
    static bool
    IsFileExistent(const std::string &path);

    // Part 1: Get GPU Info
    static ServerError
    GetDeviceCount(int &device_count);

    static ServerError
    GetUUID(int device_count, std::vector<std::string> &uuid_array);

    static ServerError
    GetUUIDMD5(int device_count, std::vector<std::string> &uuid_array, std::vector<std::string> &md5_array);


    static ServerError
    GetUUIDSHA256(const int &device_count,
                  std::vector<std::string> &uuid_array,
                  std::vector<std::string> &sha_array);

    static ServerError
    GetSystemTime(time_t &system_time);

    // Part 2: Handle License File
    static ServerError
    LicenseFileSerialization(const std::string &path,
                             int device_count,
                             const std::map<int, std::string> &uuid_encrption_map,
                             time_t starting_time,
                             time_t end_time);

    static ServerError
    LicenseFileDeserialization(const std::string &path,
                               int &device_count,
                               std::map<int, std::string> &uuid_encrption_map,
                               time_t &starting_time,
                               time_t &end_time);

//    static ServerError
//    SecretFileSerialization(const std::string &path,
//                            const time_t &update_time,
//                            const off_t &file_size,
//                            const time_t &starting_time,
//                            const time_t &end_time,
//                            const std::string &file_md5);
//
//    static ServerError
//    SecretFileDeserialization(const std::string &path,
//                              time_t &update_time,
//                              off_t &file_size,
//                              time_t &starting_time,
//                              time_t &end_time,
//                              std::string &file_md5);

    // Part 3: File attribute: UpdateTime Time/ Size/ MD5
    static ServerError
    GetFileUpdateTimeAndSize(const std::string &path, time_t &update_time, off_t &file_size);

    static ServerError
    GetFileMD5(const std::string &path, std::string &filemd5);

    // Part 4: GPU Info File Serialization/Deserialization
    static ServerError
    GPUinfoFileSerialization(const std::string &path,
                             int device_count,
                             const std::map<int, std::string> &uuid_encrption_map);
    static ServerError
    GPUinfoFileDeserialization(const std::string &path,
                               int &device_count,
                               std::map<int, std::string> &uuid_encrption_map);

    static ServerError
    GetDateTime(const char *cha, time_t &data_time);


 private:
    static constexpr int sha256_length_ = 32;
};


}
}
}
