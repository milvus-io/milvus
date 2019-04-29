////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <easylogging++.h>

#include "server/ServerConfig.h"
#include "utils/CommonUtil.h"

INITIALIZE_EASYLOGGINGPP

using namespace zilliz::vecwise;

int main(int argc, char **argv) {
    std::string exe_path = server::CommonUtil::GetExePath();
    std::string config_filename = exe_path + "../../../conf/server_config.yaml";
    zilliz::vecwise::server::ServerConfig& config = zilliz::vecwise::server::ServerConfig::GetInstance();
    config.LoadConfigFile(config_filename);
    std::cout << "Load config file form: " << config_filename << std::endl;

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
