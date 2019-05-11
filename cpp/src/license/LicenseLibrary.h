#pragma once

#include "LicenseFile.h"
#include "SecretFile.h"

#include "utils/Error.h"

#include <vector>
#include <map>


namespace zilliz {
namespace vecwise {
namespace server {

class LicenseLibrary {
 public:
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

    // Part 2: Handle License File
    static ServerError
    LicenseFileSerialization(const std::string &path,
                             int device_count,
                             const std::map<int, std::string> &uuid_encrption_map, int64_t remaining_hour);

    static ServerError
    LicenseFileDeserialization(const std::string &path,
                               int &device_count,
                               std::map<int, std::string> &uuid_encrption_map,
                               int64_t &remaining_hour);

    static ServerError
    SecretFileSerialization(const std::string &path,
                            const time_t &update_time,
                            const off_t &file_size,
                            const std::string &file_md5);

    static ServerError
    SecretFileDeserialization(const std::string &path, time_t &update_time, off_t &file_size, std::string &file_md5);

    // Part 3: File attribute: UpdateTime Time/ Size/ MD5
    static ServerError
    GetFileUpdateTimeAndSize(const std::string &path, time_t &update_time, off_t &file_size);

    static ServerError
    GetFileMD5(const std::string &path, std::string &filemd5);

    // Part 4: GPU Info File Serialization/Deserialization

 private:
    static constexpr int sha256_length_ = 32;
};


}
}
}
