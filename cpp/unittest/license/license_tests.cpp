////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>
#include <license/LicenseLibrary.h>
#include "license/LicenseCheck.h"
#include "license/LicensePublic.h"
#include "utils/Error.h"


using namespace zilliz::vecwise;

//TEST(LicenseTest, LICENSE_TEST) {

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





TEST(LicenseTest, LICENSE_TEST) {
    std::string path1 = "/tmp/vecwise_engine.license";
    std::string path2 = "/tmp/vecwise_engine2.license";

    server::ServerError err;
    server::Runtime(path1,path2);


    time_t  update_time;
    off_t file_size;
    std::string filemd5;

    time_t  update_timecheck;
    off_t file_sizecheck;
    std::string filemd5check;

    err = server::LicenseGetfiletime(path1,update_time);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    err = server::LicenseGetfilesize(path1,file_size);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    err = server::LicenseGetfilemd5(path1,filemd5);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    err = server::LifileSave(path2,update_time,file_size,filemd5);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    err = server::LifileLoad(path2,update_timecheck,file_sizecheck,filemd5check);
    ASSERT_EQ(err, server::SERVER_SUCCESS);


    std::cout<< "update_time : " << update_time <<std::endl;
    std::cout<< "update_timecheck : " << update_timecheck <<std::endl;

    std::cout<< "file_size : " << file_size<<std::endl;
    std::cout<< "file_sizecheck : " << file_sizecheck <<std::endl;

    std::cout<< "filemd5 : " << filemd5 <<std::endl;
    std::cout<< "filemd5check : " << filemd5check <<std::endl;


    err = server::LicenseValidate(path1);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    int deviceCount = 0;
    std::vector<std::string> uuids;

    deviceCount = 2;
    uuids.push_back("121");
    uuids.push_back("324");
    err = server::LicenseGetuuid(deviceCount,uuids);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
    printf("\n  deviceCount  = %d\n",deviceCount);

    std::vector<std::string> uuidmd5s;
    err = server::LicenseGetuuidmd5(deviceCount,uuids,uuidmd5s);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    printf("  md5s  \n");
    for(int i=0;i<deviceCount;i++)
        std::cout<< uuidmd5s[i] << std::endl;

    std::vector<std::string> uuidshas;
    err = server::LicenseGetuuidsha(deviceCount,uuids,uuidshas);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    std::map<int,std::string> uuidEncryption;
    for(int i=0;i<deviceCount;i++)
    {
        uuidEncryption.insert(std::make_pair(i,uuidshas[i]));
    }
    for(int i=0;i<deviceCount;i++)
    {
        std::cout<< "uuidshas : " << uuidshas[i] << std::endl;
    }

    err = server::LiSave(path1,deviceCount,uuidEncryption);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    int64_t  RemainingTime;
    int deviceCountcheck;
    std::map<int,std::string> uuidEncryptioncheck;
//    err = server::LicenseLibrary::LicenseFileDeserialization(path1, deviceCountcheck, uuidEncryptioncheck, RemainingTime);
    err = server::LiLoad(path1,deviceCountcheck,uuidEncryptioncheck,RemainingTime);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    printf("----- checking ----\n");
    printf("\n  deviceCount  = %d\n",deviceCountcheck);
    for(auto it : uuidEncryptioncheck)
    {
        std::cout<< "uuidshas : " << it.second << std::endl;
    }
    std::cout<< "RemainingTime :" << RemainingTime << std::endl;

    printf("   shas  \n");
    for(int i=0;i<deviceCount;i++)
        std::cout<< uuidshas[i] << std::endl;



    err = server::LicenseSave(path1,deviceCount,uuidshas);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    std::cout<<" file save success " << std::endl;



    err = server::LicenseLegalitycheck(path1);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
    std::cout<<" Legality check success " << std::endl;

    std::vector<std::string> uuidshascheck;

    err = server::LicenseLoad(path1,deviceCountcheck,uuidshascheck);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    std::cout<<" deviceCountcheck ：" <<  deviceCountcheck << std::endl;
    std::cout<<" uuidshascheck ：" <<  uuidshascheck[0] << std::endl;


    err = server::LicenseGetfilemd5(path1,filemd5);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
    std::cout<<" filemd5 ：" <<  filemd5 << std::endl;

    err= server::LicensefileSave(path1,path2);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    time_t last_timecheck;
    err= server::LicensefileLoad(path2,filemd5check,last_timecheck);
    ASSERT_EQ(err, server::SERVER_SUCCESS);
    std::cout<<" filemd5check ：" <<  filemd5check << std::endl;

    time_t last_time;
    err = server::LicenseGetfiletime(path1,last_time);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    std::cout<<"  last_time :  " << last_time << std::endl;
    std::cout<<" last_timecheck ：" <<  last_timecheck << std::endl;

    err = server::LicenseIntegritycheck(path1,path2);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

}
