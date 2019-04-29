////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <easylogging++.h>

#include "server/ServerConfig.h"

INITIALIZE_EASYLOGGINGPP

int main(int argc, char **argv) {
    std::string config_filename = "../../../conf/server_config.yaml";
    zilliz::vecwise::server::ServerConfig& config = zilliz::vecwise::server::ServerConfig::GetInstance();
    config.LoadConfigFile(config_filename);

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
