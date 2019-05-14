/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/


#include "utils/Log.h"
#include "license/LicenseLibrary.h"
#include "utils/Error.h"

#include <gtest/gtest.h>
#include <iostream>


using namespace zilliz::vecwise;

TEST(LicenseLibraryTest, FILE_EXISTENT_TEST) {

    std::string hosts_file = "/etc/hosts";
    ASSERT_EQ(server::LicenseLibrary::IsFileExistent(hosts_file), true);

    std::string no_exist_file = "/temp/asdaasd";
    ASSERT_EQ(server::LicenseLibrary::IsFileExistent(no_exist_file), false);

    std::string directory = "/tmp";
    ASSERT_EQ(server::LicenseLibrary::IsFileExistent(directory), false);
}

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

    time_t systemtime;
    err = server::LicenseLibrary::GetSystemTime(systemtime);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
    std::cout << "System Time: " << systemtime << std::endl;

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

    // 7.GPU_info File
    std::string GPU_info_file_path("/tmp/megasearch.info");


    // 8. Generate GPU_info File
    err = server::LicenseLibrary::GPUinfoFileSerialization(GPU_info_file_path,
                                                           device_count,
                                                           uuid_encrption_map);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    // 9. Define output var
    int output_info_device_count = 0;
    std::map<int, std::string> output_info_uuid_encrption_map;

    // 10. Read GPU_info File
    err = server::LicenseLibrary::GPUinfoFileDeserialization(GPU_info_file_path,
                                                             output_info_device_count,
                                                             output_info_uuid_encrption_map);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    ASSERT_EQ(device_count, output_info_device_count);
    for (int i = 0; i < device_count; ++i) {
        ASSERT_EQ(uuid_encrption_map[i], output_info_uuid_encrption_map[i]);
    }


    // 16. Get System Time/starting_time ans End Time
    time_t starting_time;
    err = server::LicenseLibrary::GetSystemTime(starting_time);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
    time_t end_time;
    end_time = starting_time + (long) (60 * 60 * 24 * 7);

    // 11. Generate License File
    err = server::LicenseLibrary::LicenseFileSerialization(license_file_path,
                                                           device_count,
                                                           uuid_encrption_map,
                                                           starting_time,
                                                           end_time);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    // 12. Define output var
    int output_device_count = 0;
    std::map<int, std::string> output_uuid_encrption_map;
    time_t output_starting_time;
    time_t output_end_time;

    // 13. Read License File
    err = server::LicenseLibrary::LicenseFileDeserialization(license_file_path,
                                                             output_device_count,
                                                             output_uuid_encrption_map,
                                                             output_starting_time,
                                                             output_end_time);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    ASSERT_EQ(device_count, output_device_count);
    ASSERT_EQ(starting_time, output_starting_time);
    ASSERT_EQ(end_time, output_end_time);

    for (int i = 0; i < device_count; ++i) {
        ASSERT_EQ(uuid_encrption_map[i], output_uuid_encrption_map[i]);
    }

    // 14. Get License File Attribute
    time_t update_time;
    off_t file_size;
    err = server::LicenseLibrary::GetFileUpdateTimeAndSize(license_file_path, update_time, file_size);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    // 15. Get License File MD5
    std::string file_md5;
    err = server::LicenseLibrary::GetFileMD5(license_file_path, file_md5);
    ASSERT_EQ(err, server::SERVER_SUCCESS);



    // 16. Generate Secret File
//    std::string secret_file_path("/tmp/megasearch.secret");
//    err = server::LicenseLibrary::SecretFileSerialization(secret_file_path,
//                                                          update_time,
//                                                          file_size,
//                                                          starting_time,
//                                                          end_time,
//                                                          file_md5);
//    ASSERT_EQ(err, server::SERVER_SUCCESS);

    // 17. Define output var
//    time_t output_update_time;
//    off_t output_file_size;
//    time_t output_starting_time;
//    time_t output_end_time;
//    std::string output_file_md5;

    // 18. Read License File
//    err = server::LicenseLibrary::SecretFileDeserialization(secret_file_path,
//                                                            output_update_time,
//                                                            output_file_size,
//                                                            output_starting_time,
//                                                            output_end_time,
//                                                            output_file_md5);
//    ASSERT_EQ(err, server::SERVER_SUCCESS);
//
//    ASSERT_EQ(update_time, output_update_time);
//    ASSERT_EQ(file_size, output_file_size);
//    ASSERT_EQ(starting_time, output_starting_time);
//    ASSERT_EQ(end_time, output_end_time);
//    ASSERT_EQ(file_md5, output_file_md5);

    // 19. Integrity check and Legality check
//    err = server::LicenseLibrary::IntegrityCheck(license_file_path, secret_file_path);
//    ASSERT_EQ(err, server::SERVER_SUCCESS);
//
//    err = server::LicenseLibrary::LegalityCheck(license_file_path);
//    ASSERT_EQ(err, server::SERVER_SUCCESS);

}


TEST(LicenseLibraryTest, GET_GPU_INFO_FILE) {

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

    // 4. Generate GPU ID map with GPU UUID
    std::map<int, std::string> uuid_encrption_map;
    for (int i = 0; i < device_count; ++i) {
        uuid_encrption_map[i] = uuid_sha256_array[i];
    }

    // 5.GPU_info File
    std::string GPU_info_file_path("/tmp/megasearch.info");

    // 6. Generate GPU_info File
    err = server::LicenseLibrary::GPUinfoFileSerialization(GPU_info_file_path,
                                                           device_count,
                                                           uuid_encrption_map);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    std::cout << "Generate GPU_info File Success" << std::endl;
}

TEST(LicenseLibraryTest, GET_LICENSE_FILE) {

    server::ServerError err;
    // 1.GPU_info File
    std::string GPU_info_file_path("/tmp/megasearch.info");

    // 2. License File
    std::string license_file_path("/tmp/megasearch.license");

    // 3. Define output var
    int output_info_device_count = 0;
    std::map<int, std::string> output_info_uuid_encrption_map;

    // 4. Read GPU_info File
    err = server::LicenseLibrary::GPUinfoFileDeserialization(GPU_info_file_path,
                                                             output_info_device_count,
                                                             output_info_uuid_encrption_map);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    time_t system_time;
    err = server::LicenseLibrary::GetSystemTime(system_time);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
    time_t starting_time = system_time;
    time_t end_time = system_time + (long) (60 * 60 * 24 * 7);

    err = server::LicenseLibrary::LicenseFileSerialization(license_file_path,
                                                           output_info_device_count,
                                                           output_info_uuid_encrption_map,
                                                           starting_time,
                                                           end_time);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
    std::cout << "Generate License File Success" << std::endl;

}

TEST(LicenseLibraryTest, GET_DATA_TIME) {
    server::ServerError err;
    char * a ="2018-03-06";
    time_t pp;
    err = server::LicenseLibrary::GetDateTime(a,pp);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
    std::cout << pp <<std::endl;

}

//TEST(LicenseLibraryTest, GET_SECRET_FILE) {
//
//    server::ServerError err;
//    std::string license_file_path("/tmp/megasearch.license");
//    std::string secret_file_path("/tmp/megasearch.secret");
//
//    // 14. Get License File Attribute
//    time_t update_time;
//    off_t file_size;
//    err = server::LicenseLibrary::GetFileUpdateTimeAndSize(license_file_path, update_time, file_size);
//    ASSERT_EQ(err, server::SERVER_SUCCESS);
//
//    // 15. Get License File MD5
//    std::string file_md5;
//    err = server::LicenseLibrary::GetFileMD5(license_file_path, file_md5);
//    ASSERT_EQ(err, server::SERVER_SUCCESS);
//
//    // 16. Get System Time/starting_time ans End Time
//    time_t starting_time;
//    err = server::LicenseLibrary::GetSystemTime(starting_time);
//    ASSERT_EQ(err, server::SERVER_SUCCESS);
//    time_t end_time;
//    end_time = starting_time + (long) (60 * 60 * 24 * 7);
//
//    // 16. Generate Secret File
//    err = server::LicenseLibrary::SecretFileSerialization(secret_file_path,
//                                                          update_time,
//                                                          file_size,
//                                                          starting_time,
//                                                          end_time,
//                                                          file_md5);
//    ASSERT_EQ(err, server::SERVER_SUCCESS);
//
//    std::cout << "Generate Secret File Success" << std::endl;
//}
