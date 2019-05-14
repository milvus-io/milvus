//
// Created by zilliz on 19-5-13.
//

#include "utils/Log.h"
#include "license/LicenseCheck.h"
#include "utils/Error.h"

#include <gtest/gtest.h>
#include <iostream>


using namespace zilliz::vecwise;

TEST(LicenseLibraryTest, CHECK_TEST) {

    server::ServerError err;

    std::string license_file_path("/tmp/megasearch/abc.license");


    int device_count;
    std::map<int, std::string> uuid_encryption_map;
//
//    err = server::LicenseLibrary::GPUinfoFileDeserialization(sys_info_file_path, device_count, uuid_encryption_map);
//    ASSERT_EQ(err, server::SERVER_SUCCESS);
//    std::cout << " device_count: " << device_count << std::endl;
//    for (int i = 0; i < device_count; i++) {
//        std::cout << " uuid_encryption_map: " << i << "  " << uuid_encryption_map[i] << std::endl;
//    }
//
    time_t starting_time;
    time_t end_time;
    err = server::LicenseLibrary::LicenseFileDeserialization(license_file_path,
                                                             device_count,
                                                             uuid_encryption_map,
                                                             starting_time,
                                                             end_time);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
//
//    std::cout << "device_count: " << device_count << std::endl;
//    for (int i = 0; i < device_count; i++) {
//        std::cout << " uuid_encryption_map: " << i << "  " << uuid_encryption_map[i] << std::endl;
//    }
//    std::cout << "starting_time: " << starting_time << std::endl;
//    std::cout << "end_time: " << end_time << std::endl;


    time_t system_time;
    err = server::LicenseLibrary::GetSystemTime(system_time);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    system_time+=111100;
    err = server::LicenseLibrary::LicenseFileSerialization(license_file_path,
                                                           device_count,
                                                           uuid_encryption_map,
                                                           system_time,
                                                           end_time);
//    ASSERT_EQ(err, server::SERVER_SUCCESS);
    // 19. Legality check
    err = server::LicenseCheck::LegalityCheck(license_file_path);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    // 20. Start counting down

//   err = server::LicenseCheck::StartCountingDown(license_file_path);
//   ASSERT_EQ(err, server::SERVER_SUCCESS);
}
