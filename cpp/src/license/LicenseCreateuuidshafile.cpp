//
// Created by zilliz on 19-5-11.
//
#include <gtest/gtest.h>
#include <iostream>

#include "utils/Error.h"
#include "license/LicenseCheck.h"


using namespace zilliz::vecwise;

//
//TEST(LicenseTest, LICENSE_TEST) {
//
//    std::string path1 = "/tmp/vecwise_engine.sha";
//    std::cout << "This is create  uuidshafile " << std::endl;
//
//    server::ServerError err;
//    int deviceCount=0;
//    std::vector<std::string> uuids;
//
//    err = server::LicenseGetuuid(deviceCount,uuids);
//    ASSERT_EQ(err, server::SERVER_SUCCESS);
//
//    std::vector<std::string> shas;
//    err = server::LicenseGetuuidsha(deviceCount,uuids,shas);
//    ASSERT_EQ(err, server::SERVER_SUCCESS);
//
//    err = server::LicenseSave(path1,deviceCount,shas);
//    ASSERT_EQ(err, server::SERVER_SUCCESS);
//
//}