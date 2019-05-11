/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/


#include "utils/Log.h"
#include "license/LicenseLibrary.h"
#include "utils/Error.h"

#include <gtest/gtest.h>


using namespace zilliz::vecwise;

TEST(LicenseLibraryTest, GPU_INFO_TEST) {

    int device_count = 0;
    server::ServerError err = server::LicenseLibrary::GetDeviceCount(device_count);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
    std::cout << "Device Count: " << device_count << std::endl;

    std::vector<std::string> uuid_array;
    err = server::LicenseLibrary::GetUUID(device_count, uuid_array);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
    for (long i = 0; i < device_count; ++i) {
        std::cout << "Device Id: " << i << ", UUID: " << uuid_array[i] << std::endl;
    }

    std::vector<std::string> uuid_md5_array;
    err = server::LicenseLibrary::GetUUIDMD5(device_count, uuid_array, uuid_md5_array);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
    for (long i = 0; i < device_count; ++i) {
        std::cout << "Device Id: " << i << ", UUID: " << uuid_array[i] << ", UUID_MD5: " << uuid_md5_array[i]
                  << std::endl;
    }

    std::vector<std::string> uuid_sha256_array;
    err = server::LicenseLibrary::GetUUIDSHA256(device_count, uuid_array, uuid_sha256_array);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
    for (long i = 0; i < device_count; ++i) {
        std::cout << "Device Id: " << i << ", UUID: " << uuid_array[i] << ", UUID_SHA256: "
                  << uuid_sha256_array[i] << std::endl;
    }
}

TEST(LicenseLibraryTest, LICENSE_FILE_TEST) {

    // 1. Get Device Count
    int device_count = 0;
    server::ServerError err = server::LicenseLibrary::GetDeviceCount(device_count);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    // 2. Get All GPU UUID
    std::vector<std::string> uuid_array;
    err = server::LicenseLibrary::GetUUID(device_count, uuid_array);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    // 3. Get UUID SHA256
    std::vector<std::string> uuid_sha256_array;
    err = server::LicenseLibrary::GetUUIDSHA256(device_count, uuid_array, uuid_sha256_array);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    // 4. Set a file name
    std::string license_file_path("/tmp/megasearch.license");

    // 5. Provide remaining hour
    int64_t remaining_hour = 2 * 365 * 24;

    // 6. Generate GPU ID map with GPU UUID
    std::map<int, std::string> uuid_encrption_map;
    for (int i = 0; i < device_count; ++i) {
        uuid_encrption_map[i] = uuid_sha256_array[i];
    }

    // 7. Generate License File
    err = server::LicenseLibrary::LicenseFileSerialization(license_file_path,
                                                           device_count,
                                                           uuid_encrption_map,
                                                           remaining_hour);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    // 8. Define output var
    int output_device_count = 0;
    std::map<int, std::string> output_uuid_encrption_map;
    int64_t output_remaining_hour = 0;

    // 9. Read License File
    err = server::LicenseLibrary::LicenseFileDeserialization(license_file_path,
                                                             output_device_count,
                                                             output_uuid_encrption_map,
                                                             output_remaining_hour);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    ASSERT_EQ(device_count, output_device_count);
    ASSERT_EQ(remaining_hour, output_remaining_hour);
    for (int i = 0; i < device_count; ++i) {
        ASSERT_EQ(uuid_encrption_map[i], output_uuid_encrption_map[i]);
    }

    // 10. Get License File Attribute
    time_t update_time;
    off_t file_size;
    err = server::LicenseLibrary::GetFileUpdateTimeAndSize(license_file_path, update_time, file_size);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    // 11. Get License File MD5
    std::string file_md5;
    err = server::LicenseLibrary::GetFileMD5(license_file_path, file_md5);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    // 12. Generate Secret File
    std::string secret_file_path("/tmp/megasearch.secret");
    err = server::LicenseLibrary::SecretFileSerialization(secret_file_path, update_time, file_size, file_md5);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    // 13. Define output var
    time_t output_update_time;
    off_t output_file_size;
    std::string output_file_md5;

    // 14. Read License File
    err = server::LicenseLibrary::SecretFileDeserialization(secret_file_path,
                                                            output_update_time,
                                                            output_file_size,
                                                            output_file_md5);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    ASSERT_EQ(update_time, output_update_time);
    ASSERT_EQ(file_size, output_file_size);
    ASSERT_EQ(file_md5, output_file_md5);

}