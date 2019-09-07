/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "scheduler/SchedInst.h"
#include "server/ServerConfig.h"
#include <boost/filesystem.hpp>
#include <gtest/gtest.h>


namespace zilliz {
namespace milvus {
namespace engine {


class SchedInstTest : public testing::Test {

protected:
    void
    SetUp() override {
        boost::filesystem::create_directory(TMP_DIR);
        std::stringstream ss;
        ss << "resource_config: " << std::endl;
        ss << "  resources: " << std::endl;
        ss << "    ssda: " << std::endl;
        ss << "      type: DISK" << std::endl;
        ss << "      device_id: 0" << std::endl;
        ss << "      enable_loader: true" << std::endl;
        ss << "      enable_executor: false" << std::endl;
        ss << " " << std::endl;
        ss << "    cpu: " << std::endl;
        ss << "      type: CPU" << std::endl;
        ss << "      device_id: 0" << std::endl;
        ss << "      enable_loader: true" << std::endl;
        ss << "      enable_executor: false" << std::endl;
        ss << " " << std::endl;
        ss << "    gpu0: " << std::endl;
        ss << "      type: GPU" << std::endl;
        ss << "      device_id: 0" << std::endl;
        ss << "      enable_loader: true" << std::endl;
        ss << "      enable_executor: true" << std::endl;
        ss << " " << std::endl;
        ss << "  connections: " << std::endl;
        ss << "    io: " << std::endl;
        ss << "      speed: 500" << std::endl;
        ss << "      endpoint: ssda===cpu" << std::endl;
        ss << "    pcie: " << std::endl;
        ss << "      speed: 11000" << std::endl;
        ss << "      endpoint: cpu===gpu0" << std::endl;

        boost::filesystem::path fpath(CONFIG_FILE);
        boost::filesystem::fstream fstream(fpath, std::ios_base::out);
        fstream << ss.str();
        fstream.close();

        server::ServerConfig::GetInstance().LoadConfigFile(CONFIG_FILE);
    }

    void
    TearDown() override {
        StopSchedulerService();
        boost::filesystem::remove_all(TMP_DIR);
    }

    const std::string TMP_DIR = "/tmp/milvus_sched_test";
    const std::string CONFIG_FILE = "/tmp/milvus_sched_test/config.yaml";
};

TEST_F(SchedInstTest, simple_gpu) {
    StartSchedulerService();
}

}
}
}
