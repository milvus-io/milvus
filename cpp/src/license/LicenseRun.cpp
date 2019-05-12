//
// Created by zilliz on 19-5-11.
//
#include <gtest/gtest.h>
#include <iostream>
#include <map>

#include "utils/Error.h"
#include "license/LicenseCheck.h"
#include "license/LicensePublic.h"


using namespace zilliz::vecwise;

//TEST(LicenseTest, LICENSE_TEST) {
//
//    std::string path1 = "/tmp/vecwise_engine.license";
//    std::string path2 = "/tmp/vecwise_engine2.license";
//    std::cout << "This is run " << std::endl;
//
//    server::ServerError err;
//
//    err = server::Licensefileread(path1);
//    if(err!=server::SERVER_SUCCESS)
//    {
//        exit(1);
//    }
//    err = server::LicenseIntegrity_check(path1,path2);
//    if(err!=server::SERVER_SUCCESS)
//    {
//        std::cout << "Integrity_check is wrong " << std::endl;
//        exit(1);
//    }
//    err = server::LicenseLegality_check(path1);
//    if(err!=server::SERVER_SUCCESS)
//    {
//        std::cout << "Legality_check is wrong " << std::endl;
//        exit(1);
//    }
//    std::cout << " runing " << std::endl;
//    server::Runtime(path1,path2);
//}