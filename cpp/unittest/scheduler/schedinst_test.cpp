// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


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

TEST_F(SchedInstTest, SIMPLE_GPU) {
    StartSchedulerService();
}

}
}
}
