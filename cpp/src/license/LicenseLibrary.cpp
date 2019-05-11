////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "LicenseLibrary.h"
#include "utils/Log.h"
#include <cuda_runtime.h>
#include <nvml.h>
#include <openssl/md5.h>
#include <openssl/sha.h>

#include <boost/archive/binary_oarchive.hpp>
#include <boost/archive/binary_iarchive.hpp>
//#include <boost/foreach.hpp>
//#include <boost/serialization/vector.hpp>
#include <boost/serialization/map.hpp>


namespace zilliz {
namespace vecwise {
namespace server {

constexpr int LicenseLibrary::sha256_length_;

ServerError
LicenseLibrary::GetDeviceCount(int &device_count) {
    nvmlReturn_t result = nvmlInit();
    if (NVML_SUCCESS != result) {
        printf("Failed to initialize NVML: %s\n", nvmlErrorString(result));
        return SERVER_UNEXPECTED_ERROR;
    }
    cudaError_t error_id = cudaGetDeviceCount(&device_count);
    if (error_id != cudaSuccess) {
        printf("cudaGetDeviceCount returned %d\n-> %s\n", (int) error_id, cudaGetErrorString(error_id));
        printf("Result = FAIL\n");
        return SERVER_UNEXPECTED_ERROR;
    }
    return SERVER_SUCCESS;
}

ServerError
LicenseLibrary::GetUUID(int device_count, std::vector<std::string> &uuid_array) {
    if (device_count == 0) {
        printf("There are no available device(s) that support CUDA\n");
        return SERVER_UNEXPECTED_ERROR;
    }

    for (int dev = 0; dev < device_count; ++dev) {
        nvmlDevice_t device;
        nvmlReturn_t result = nvmlDeviceGetHandleByIndex(dev, &device);
        if (NVML_SUCCESS != result) {
            printf("Failed to get handle for device %i: %s\n", dev, nvmlErrorString(result));
            return SERVER_UNEXPECTED_ERROR;
        }

        char uuid[80];
        unsigned int length = 80;
        nvmlReturn_t err = nvmlDeviceGetUUID(device, uuid, length);
        if (err != NVML_SUCCESS) {
            printf("nvmlDeviceGetUUID error: %d\n", err);
            return SERVER_UNEXPECTED_ERROR;
        }

        uuid_array.emplace_back(uuid);
    }
    return SERVER_SUCCESS;
}

ServerError
LicenseLibrary::GetUUIDMD5(int device_count,
                           std::vector<std::string> &uuid_array,
                           std::vector<std::string> &md5_array) {
    MD5_CTX ctx;
    unsigned char outmd[16];
    char temp[2];
    std::string md5;
    for (int dev = 0; dev < device_count; ++dev) {
        md5.clear();
        memset(outmd, 0, sizeof(outmd));
        MD5_Init(&ctx);
        MD5_Update(&ctx, uuid_array[dev].c_str(), uuid_array[dev].size());
        MD5_Final(outmd, &ctx);
        for (int i = 0; i < 16; ++i) {
            std::snprintf(temp, 2, "%02X", outmd[i]);
            md5 += temp;
        }
        md5_array.push_back(md5);
    }
    return SERVER_SUCCESS;
}

ServerError
LicenseLibrary::GetUUIDSHA256(const int &device_count,
                              std::vector<std::string> &uuid_array,
                              std::vector<std::string> &sha_array) {
    SHA256_CTX ctx;
    unsigned char outmd[sha256_length_];
    char temp[2];
    std::string sha;
    for (int dev = 0; dev < device_count; ++dev) {
        sha.clear();
        memset(outmd, 0, sizeof(outmd));
        SHA256_Init(&ctx);
        SHA256_Update(&ctx, uuid_array[dev].c_str(), uuid_array[dev].size());
        SHA256_Final(outmd, &ctx);
        for (int i = 0; i < sha256_length_; ++i) {
            std::snprintf(temp, 2, "%02X", outmd[i]);
            sha += temp;
        }
        sha_array.push_back(sha);
    }
    return SERVER_SUCCESS;
}

// Part 2: Handle License File
ServerError
LicenseLibrary::LicenseFileSerialization(const std::string &path,
                                         int device_count,
                                         const std::map<int, std::string> &uuid_encrption_map,
                                         int64_t remaining_hour) {

    std::ofstream file(path);
    boost::archive::binary_oarchive oa(file);
    oa.register_type<LicenseFile>();

    SerializedLicenseFile serialized_license_file;

    serialized_license_file.set_license_file(new LicenseFile(device_count, uuid_encrption_map, remaining_hour));
    oa << serialized_license_file;

    file.close();
    return SERVER_SUCCESS;
}

ServerError
LicenseLibrary::LicenseFileDeserialization(const std::string &path,
                                           int &device_count,
                                           std::map<int, std::string> &uuid_encrption_map,
                                           int64_t &remaining_hour) {
    std::ifstream file(path);
    boost::archive::binary_iarchive ia(file);
    ia.register_type<LicenseFile>();

    SerializedLicenseFile serialized_license_file;
    ia >> serialized_license_file;

    device_count = serialized_license_file.get_license_file()->get_device_count();
    uuid_encrption_map = serialized_license_file.get_license_file()->get_uuid_encryption_map();
    remaining_hour = serialized_license_file.get_license_file()->get_remaining_hour();

    file.close();
    return SERVER_SUCCESS;
}

ServerError
LicenseLibrary::SecretFileSerialization(const std::string &path,
                                        const time_t &update_time,
                                        const off_t &file_size,
                                        const std::string &file_md5) {
    std::ofstream file(path);
    boost::archive::binary_oarchive oa(file);
    oa.register_type<SecretFile>();

    SerializedSecretFile serialized_secret_file;

    serialized_secret_file.set_secret_file(new SecretFile(update_time, file_size, file_md5));
    oa << serialized_secret_file;

    file.close();
    return SERVER_SUCCESS;
}

ServerError
LicenseLibrary::SecretFileDeserialization(const std::string &path,
                                          time_t &update_time,
                                          off_t &file_size,
                                          std::string &file_md5) {
    std::ifstream file(path);
    boost::archive::binary_iarchive ia(file);
    ia.register_type<SecretFile>();
    SerializedSecretFile serialized_secret_file;

    ia >> serialized_secret_file;
    update_time = serialized_secret_file.get_secret_file()->get_update_time();
    file_size = serialized_secret_file.get_secret_file()->get_file_size();
    file_md5 = serialized_secret_file.get_secret_file()->get_file_md5();
    file.close();
    return SERVER_SUCCESS;
}

// Part 3: File attribute: UpdateTime Time/ Size/ MD5
ServerError
LicenseLibrary::GetFileUpdateTimeAndSize(const std::string& path, time_t& update_time, off_t& file_size) {
    struct stat buf;
    int err_no = stat(path.c_str(), &buf);
    if(err_no != 0)
    {
        std::cout << strerror(err_no) << std::endl;
        return  SERVER_UNEXPECTED_ERROR;
    }

    update_time =buf.st_mtime;
    file_size =buf.st_size;

    return SERVER_SUCCESS;
}

ServerError
LicenseLibrary::GetFileMD5(const std::string &path, std::string &filemd5) {
    filemd5.clear();

    std::ifstream file(path.c_str(), std::ifstream::binary);
    if (!file) {
        return -1;
    }

    MD5_CTX md5Context;
    MD5_Init(&md5Context);

    char buf[1024 * 16];
    while (file.good()) {
        file.read(buf, sizeof(buf));
        MD5_Update(&md5Context, buf, file.gcount());
    }

    unsigned char result[MD5_DIGEST_LENGTH];
    MD5_Final(result, &md5Context);

    char hex[35];
    memset(hex, 0, sizeof(hex));
    for (int i = 0; i < MD5_DIGEST_LENGTH; ++i) {
        sprintf(hex + i * 2, "%02X", result[i]);
    }
    hex[32] = '\0';
    filemd5 = std::string(hex);

    return SERVER_SUCCESS;
}

}
}
}