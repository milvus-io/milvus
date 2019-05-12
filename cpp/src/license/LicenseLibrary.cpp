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
LicenseLibrary::OpenFile(const std::string &path) {
    std::ifstream fileread;
    fileread.open(path, std::ios::in);
    if (!fileread) {
        std::cout << path << "  does not exist" << std::endl;
        return SERVER_UNEXPECTED_ERROR;
    }
    fileread.close();
    return SERVER_SUCCESS;
}

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
    OpenFile(path);
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
    OpenFile(path);
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
LicenseLibrary::GetFileUpdateTimeAndSize(const std::string &path, time_t &update_time, off_t &file_size) {

    OpenFile(path);
    struct stat buf;
    int err_no = stat(path.c_str(), &buf);
    if (err_no != 0) {
        std::cout << strerror(err_no) << std::endl;
        return SERVER_UNEXPECTED_ERROR;
    }

    update_time = buf.st_mtime;
    file_size = buf.st_size;

    return SERVER_SUCCESS;
}

ServerError
LicenseLibrary::GetFileMD5(const std::string &path, std::string &filemd5) {

    OpenFile(path);
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
// Part 4: GPU Info File Serialization/Deserialization
ServerError
LicenseLibrary::GPUinfoFileSerialization(const std::string &path,
                                         int device_count,
                                         const std::map<int, std::string> &uuid_encrption_map) {
    std::ofstream file(path);
    boost::archive::binary_oarchive oa(file);
    oa.register_type<GPUInfoFile>();

    SerializedGPUInfoFile serialized_gpu_info_file;

    serialized_gpu_info_file.set_gpu_info_file(new GPUInfoFile(device_count, uuid_encrption_map));
    oa << serialized_gpu_info_file;

    file.close();
    return SERVER_SUCCESS;
}
ServerError
LicenseLibrary::GPUinfoFileDeserialization(const std::string &path,
                                           int &device_count,
                                           std::map<int, std::string> &uuid_encrption_map) {
    OpenFile(path);
    std::ifstream file(path);
    boost::archive::binary_iarchive ia(file);
    ia.register_type<GPUInfoFile>();

    SerializedGPUInfoFile serialized_gpu_info_file;
    ia >> serialized_gpu_info_file;

    device_count = serialized_gpu_info_file.get_gpu_info_file()->get_device_count();
    uuid_encrption_map = serialized_gpu_info_file.get_gpu_info_file()->get_uuid_encryption_map();

    file.close();
    return SERVER_SUCCESS;
}

// Part 5: Integrity check and Legality check
ServerError
LicenseLibrary::IntegrityCheck(const std::string &license_file_path, const std::string &secret_file_path) {

    std::string file_md5;
    GetFileMD5(license_file_path, file_md5);
    time_t update_time;
    off_t file_size;
    GetFileUpdateTimeAndSize(license_file_path, update_time, file_size);

    std::string output_file_md5;
    time_t output_update_time;
    off_t output_file_size;
    SecretFileDeserialization(secret_file_path, output_update_time, output_file_size, output_file_md5);
    if (file_md5 != output_file_md5) {
        printf("License file has been modified\n");
        return SERVER_UNEXPECTED_ERROR;
    }
    if (update_time != output_update_time) {
        printf("update_time is wrong\n");
        return SERVER_UNEXPECTED_ERROR;
    }
    if (file_size != output_file_size) {
        printf("file_size is wrong\n");
        return SERVER_UNEXPECTED_ERROR;
    }
    std::cout << "Integrity Check Success" << std::endl;
    return SERVER_SUCCESS;
}

ServerError
LicenseLibrary::LegalityCheck(const std::string &license_file_path) {

    int device_count;
    GetDeviceCount(device_count);
    std::vector<std::string> uuid_array;
    GetUUID(device_count, uuid_array);

    std::vector<std::string> sha_array;
    GetUUIDSHA256(device_count, uuid_array, sha_array);

    int output_device_count;
    std::map<int, std::string> uuid_encryption_map;
    int64_t remaining_time;
    LicenseFileDeserialization(license_file_path, output_device_count, uuid_encryption_map, remaining_time);

    if (device_count != output_device_count) {
        printf("device_count is wrong\n");
        return SERVER_UNEXPECTED_ERROR;
    }
    for (int i = 0; i < device_count; ++i) {
        if (sha_array[i] != uuid_encryption_map[i]) {
            printf("uuid_encryption_map %d is wrong\n", i);
            return SERVER_UNEXPECTED_ERROR;
        }
    }
    if (remaining_time <= 0) {
        printf("License expired\n");
        return SERVER_UNEXPECTED_ERROR;
    }
    std::cout << "Legality Check Success" << std::endl;
    return SERVER_SUCCESS;
}

// Part 6: Timer
ServerError
LicenseLibrary::AlterFile(const std::string &license_file_path,
                          const std::string &secret_file_path,
                          const boost::system::error_code &ec,
                          boost::asio::deadline_timer *pt) {
    int device_count;
    std::map<int, std::string> uuid_encryption_map;
    int64_t remaining_time;
    LicenseFileDeserialization(license_file_path, device_count, uuid_encryption_map, remaining_time);

    std::cout << "remaining_time: " << remaining_time << std::endl;

    if (remaining_time <= 0) {
        std::cout << "License expired" << std::endl;
        exit(1);
    }
    --remaining_time;
    LicenseFileSerialization(license_file_path, device_count, uuid_encryption_map, remaining_time);

    time_t update_time;
    off_t file_size;
    GetFileUpdateTimeAndSize(license_file_path, update_time, file_size);
    std::string file_md5;
    GetFileMD5(license_file_path, file_md5);
    SecretFileSerialization(secret_file_path, update_time, file_size, file_md5);

    pt->expires_at(pt->expires_at() + boost::posix_time::hours(1));
    pt->async_wait(boost::bind(AlterFile, license_file_path, secret_file_path, boost::asio::placeholders::error, pt));
    return SERVER_SUCCESS;
}

ServerError
LicenseLibrary::StartCountingDown(const std::string &license_file_path, const std::string &secret_file_path) {

    OpenFile(license_file_path);
    OpenFile(secret_file_path);
    boost::asio::io_service io;
    boost::asio::deadline_timer t(io, boost::posix_time::hours(1));
    t.async_wait(boost::bind(AlterFile, license_file_path, secret_file_path, boost::asio::placeholders::error, &t));
    std::cout << "Timing begins" << std::endl;
    io.run();
    return SERVER_SUCCESS;
}

}
}
}