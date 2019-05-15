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
    // 1. Set license file name
    std::string license_file_path("/tmp/megasearch/abc.license");

    // 2. Legality check
    err = server::LicenseCheck::LegalityCheck(license_file_path);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

}

TEST(LicenseLibraryTest, CHECK_ERROR1_TEST){

    server::ServerError err;
    // 1. Set license file name
    std::string license_file_path("/tmp/megasearch/abc");

    // 2. Legality check
    err = server::LicenseCheck::LegalityCheck(license_file_path);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
}

TEST(LicenseLibraryTest, CHECK_ERROR2_TEST){

    server::ServerError err;
    // 1. Set license file name
    std::string license_file_path("/tmp/megasearch/abc.license");

    // 2. Define output var
    int device_count;
    std::map<int, std::string> uuid_encryption_map;
    time_t starting_time;
    time_t end_time;

    // 3. Read License File
    err = server::LicenseLibrary::LicenseFileDeserialization(license_file_path,
                                                             device_count,
                                                             uuid_encryption_map,
                                                             starting_time,
                                                             end_time);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    // 4. Change device count
    ++device_count;
    err = server::LicenseLibrary::LicenseFileSerialization(license_file_path,
                                                           device_count,
                                                           uuid_encryption_map,
                                                           starting_time,
                                                           end_time);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    // 5. Legality check
    err = server::LicenseCheck::LegalityCheck(license_file_path);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
}

TEST(LicenseLibraryTest, CHECK_ERROR3_TEST){

    server::ServerError err;
    // 1. Set license file name
    std::string license_file_path("/tmp/megasearch/abc.license");

    // 2. Define output var
    int device_count;
    std::map<int, std::string> uuid_encryption_map;
    time_t starting_time;
    time_t end_time;

    // 3. Read License File
    err = server::LicenseLibrary::LicenseFileDeserialization(license_file_path,
                                                             device_count,
                                                             uuid_encryption_map,
                                                             starting_time,
                                                             end_time);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    // 4. Change device count
    if(device_count) uuid_encryption_map[0]+="u";
    err = server::LicenseLibrary::LicenseFileSerialization(license_file_path,
                                                           device_count,
                                                           uuid_encryption_map,
                                                           starting_time,
                                                           end_time);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    // 5. Legality check
    err = server::LicenseCheck::LegalityCheck(license_file_path);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
}

TEST(LicenseLibraryTest, CHECK_ERROR4_1_TEST){

    server::ServerError err;
    // 1. Set license file name
    std::string license_file_path("/tmp/megasearch/abc.license");

    // 2. Define output var
    int device_count;
    std::map<int, std::string> uuid_encryption_map;
    time_t starting_time;
    time_t end_time;

    // 3. Read License File
    err = server::LicenseLibrary::LicenseFileDeserialization(license_file_path,
                                                             device_count,
                                                             uuid_encryption_map,
                                                             starting_time,
                                                             end_time);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    // 4. Change starting time
    time_t system_time;
    err = server::LicenseLibrary::GetSystemTime(system_time);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    system_time+=60*60*24;

    err = server::LicenseLibrary::LicenseFileSerialization(license_file_path,
                                                           device_count,
                                                           uuid_encryption_map,
                                                           system_time,
                                                           end_time);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    // 5. Legality check
    err = server::LicenseCheck::LegalityCheck(license_file_path);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
}

TEST(LicenseLibraryTest, CHECK_ERROR4_2_TEST){

    server::ServerError err;
    // 1. Set license file name
    std::string license_file_path("/tmp/megasearch/abc.license");

    // 2. Define output var
    int device_count;
    std::map<int, std::string> uuid_encryption_map;
    time_t starting_time;
    time_t end_time;

    // 3. Read License File
    err = server::LicenseLibrary::LicenseFileDeserialization(license_file_path,
                                                             device_count,
                                                             uuid_encryption_map,
                                                             starting_time,
                                                             end_time);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    // 4. Change end time
    time_t system_time;
    err = server::LicenseLibrary::GetSystemTime(system_time);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    system_time-=100;

    err = server::LicenseLibrary::LicenseFileSerialization(license_file_path,
                                                           device_count,
                                                           uuid_encryption_map,
                                                           starting_time,
                                                           system_time);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    // 5. Legality check
    err = server::LicenseCheck::LegalityCheck(license_file_path);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
}

