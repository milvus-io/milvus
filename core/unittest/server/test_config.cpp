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

#include <gtest/gtest.h>
#include <gtest/gtest-death-test.h>

#include "config/YamlConfigMgr.h"
#include "utils/CommonUtil.h"
#include "utils/ValidationUtil.h"
#include "server/Config.h"
#include "server/utils.h"

namespace {

static constexpr uint64_t KB = 1024;
static constexpr uint64_t MB = KB * 1024;
static constexpr uint64_t GB = MB * 1024;

} // namespace

TEST_F(ConfigTest, CONFIG_TEST) {
    milvus::server::ConfigMgr* config_mgr = milvus::server::YamlConfigMgr::GetInstance();

    milvus::Status s = config_mgr->LoadConfigFile("");
    ASSERT_FALSE(s.ok());

    std::string config_path(CONFIG_PATH);
    s = config_mgr->LoadConfigFile(config_path + INVALID_CONFIG_FILE);
    ASSERT_FALSE(s.ok());

    s = config_mgr->LoadConfigFile(config_path + VALID_CONFIG_FILE);
    ASSERT_TRUE(s.ok());

    config_mgr->Print();

    milvus::server::ConfigNode& root_config = config_mgr->GetRootNode();
    milvus::server::ConfigNode& server_config = root_config.GetChild("server_config");
    milvus::server::ConfigNode& db_config = root_config.GetChild("db_config");
    milvus::server::ConfigNode& metric_config = root_config.GetChild("metric_config");
    milvus::server::ConfigNode& cache_config = root_config.GetChild("cache_config");
    milvus::server::ConfigNode invalid_config = root_config.GetChild("invalid_config");
    auto valus = invalid_config.GetSequence("not_exist");
    float ff = invalid_config.GetFloatValue("not_exist", 3.0);
    ASSERT_EQ(ff, 3.0);

    std::string address = server_config.GetValue("address");
    ASSERT_TRUE(!address.empty());
    int64_t port = server_config.GetInt64Value("port");
    ASSERT_NE(port, 0);

    server_config.SetValue("test", "2.5");
    double test = server_config.GetDoubleValue("test");
    ASSERT_EQ(test, 2.5);

    milvus::server::ConfigNode fake;
    server_config.AddChild("fake", fake);
    fake = server_config.GetChild("fake");
    milvus::server::ConfigNodeArr arr;
    server_config.GetChildren(arr);
    ASSERT_EQ(arr.size(), 1UL);

    server_config.ClearChildren();
    auto children = server_config.GetChildren();
    ASSERT_TRUE(children.empty());

    server_config.ClearConfig();
    auto configs = server_config.GetConfig();
    ASSERT_TRUE(configs.empty());

    server_config.AddSequenceItem("seq", "aaa");
    server_config.AddSequenceItem("seq", "bbb");
    auto seq = server_config.GetSequence("seq");
    ASSERT_EQ(seq.size(), 2UL);

    milvus::server::ConfigNode combine;
    combine.Combine(server_config);

    combine.PrintAll();
    std::string all = combine.DumpString();
    ASSERT_TRUE(!all.empty());

    server_config.ClearSequences();
    auto seqs = server_config.GetSequences();
    ASSERT_TRUE(seqs.empty());
}

TEST_F(ConfigTest, SERVER_CONFIG_TEST) {
    std::string config_path(CONFIG_PATH);
    milvus::server::Config& config = milvus::server::Config::GetInstance();
    milvus::Status s = config.LoadConfigFile(config_path + VALID_CONFIG_FILE);
    ASSERT_TRUE(s.ok());

    s = config.ValidateConfig();
    ASSERT_TRUE(s.ok());

    config.PrintAll();

    s = config.ResetDefaultConfig();
    ASSERT_TRUE(s.ok());
}

TEST_F(ConfigTest, SERVER_CONFIG_INVALID_TEST) {
    std::string config_path(CONFIG_PATH);
    milvus::server::Config& config = milvus::server::Config::GetInstance();
    milvus::Status s;

    s = config.LoadConfigFile("");
    ASSERT_FALSE(s.ok());

    s = config.LoadConfigFile(config_path + INVALID_CONFIG_FILE);
    ASSERT_FALSE(s.ok());
    s = config.LoadConfigFile(config_path + "dummy.yaml");
    ASSERT_FALSE(s.ok());

    /* server config */
    s = config.SetServerConfigAddress("0.0.0");
    ASSERT_FALSE(s.ok());
    s = config.SetServerConfigAddress("0.0.0.256");
    ASSERT_FALSE(s.ok());

    s = config.SetServerConfigPort("a");
    ASSERT_FALSE(s.ok());
    s = config.SetServerConfigPort("99999");
    ASSERT_FALSE(s.ok());

    s = config.SetServerConfigDeployMode("cluster");
    ASSERT_FALSE(s.ok());

    s = config.SetServerConfigTimeZone("GM");
    ASSERT_FALSE(s.ok());
    s = config.SetServerConfigTimeZone("GMT8");
    ASSERT_FALSE(s.ok());
    s = config.SetServerConfigTimeZone("UTCA");
    ASSERT_FALSE(s.ok());

    /* db config */
    s = config.SetDBConfigPrimaryPath("");
    ASSERT_FALSE(s.ok());

//    s = config.SetDBConfigSecondaryPath("");
//    ASSERT_FALSE(s.ok());

    s = config.SetDBConfigBackendUrl("http://www.google.com");
    ASSERT_FALSE(s.ok());
    s = config.SetDBConfigBackendUrl("sqlite://:@:");
    ASSERT_FALSE(s.ok());
    s = config.SetDBConfigBackendUrl("mysql://root:123456@127.0.0.1/milvus");
    ASSERT_FALSE(s.ok());

    s = config.SetDBConfigArchiveDiskThreshold("0x10");
    ASSERT_FALSE(s.ok());

    s = config.SetDBConfigArchiveDaysThreshold("0x10");
    ASSERT_FALSE(s.ok());

    s = config.SetDBConfigInsertBufferSize("a");
    ASSERT_FALSE(s.ok());
    s = config.SetDBConfigInsertBufferSize("0");
    ASSERT_FALSE(s.ok());
    s = config.SetDBConfigInsertBufferSize("2048");
    ASSERT_FALSE(s.ok());

    /* metric config */
    s = config.SetMetricConfigEnableMonitor("Y");
    ASSERT_FALSE(s.ok());

    s = config.SetMetricConfigCollector("zilliz");
    ASSERT_FALSE(s.ok());

    s = config.SetMetricConfigPrometheusPort("0xff");
    ASSERT_FALSE(s.ok());

    /* cache config */
    s = config.SetCacheConfigCpuCacheCapacity("a");
    ASSERT_FALSE(s.ok());
    s = config.SetCacheConfigCpuCacheCapacity("0");
    ASSERT_FALSE(s.ok());
    s = config.SetCacheConfigCpuCacheCapacity("2048");
    ASSERT_FALSE(s.ok());

    s = config.SetCacheConfigCpuCacheThreshold("a");
    ASSERT_FALSE(s.ok());
    s = config.SetCacheConfigCpuCacheThreshold("1.0");
    ASSERT_FALSE(s.ok());

    s = config.SetCacheConfigGpuCacheCapacity("a");
    ASSERT_FALSE(s.ok());
    s = config.SetCacheConfigGpuCacheCapacity("128");
    ASSERT_FALSE(s.ok());

    s = config.SetCacheConfigGpuCacheThreshold("a");
    ASSERT_FALSE(s.ok());
    s = config.SetCacheConfigGpuCacheThreshold("1.0");
    ASSERT_FALSE(s.ok());

    s = config.SetCacheConfigCacheInsertData("N");
    ASSERT_FALSE(s.ok());

    /* engine config */
    s = config.SetEngineConfigUseBlasThreshold("0xff");
    ASSERT_FALSE(s.ok());

    s = config.SetEngineConfigOmpThreadNum("a");
    ASSERT_FALSE(s.ok());
    s = config.SetEngineConfigOmpThreadNum("10000");
    ASSERT_FALSE(s.ok());

    s = config.SetEngineConfigGpuSearchThreshold("-1");
    ASSERT_FALSE(s.ok());

    /* resource config */
    s = config.SetResourceConfigMode("default");
    ASSERT_FALSE(s.ok());

    s = config.SetResourceConfigIndexBuildDevice("gup2");
    ASSERT_FALSE(s.ok());
    s = config.SetResourceConfigIndexBuildDevice("gpu16");
    ASSERT_FALSE(s.ok());
}

