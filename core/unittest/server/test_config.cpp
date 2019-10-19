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
    milvus::server::ConfigMgr *config_mgr = milvus::server::YamlConfigMgr::GetInstance();

    milvus::Status s = config_mgr->LoadConfigFile("");
    ASSERT_FALSE(s.ok());

    s = config_mgr->LoadConfigFile(INVALID_CONFIG_PATH);
    ASSERT_FALSE(s.ok());

    s = config_mgr->LoadConfigFile(VALID_CONFIG_PATH);
    ASSERT_TRUE(s.ok());

    config_mgr->Print();

    milvus::server::ConfigNode &root_config = config_mgr->GetRootNode();
    milvus::server::ConfigNode &server_config = root_config.GetChild("server_config");
    milvus::server::ConfigNode &db_config = root_config.GetChild("db_config");
    milvus::server::ConfigNode &metric_config = root_config.GetChild("metric_config");
    milvus::server::ConfigNode &cache_config = root_config.GetChild("cache_config");
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
    milvus::server::Config &config = milvus::server::Config::GetInstance();
    milvus::Status s = config.LoadConfigFile(VALID_CONFIG_PATH);
    ASSERT_TRUE(s.ok());

    s = config.ValidateConfig();
    ASSERT_TRUE(s.ok());

    config.PrintAll();

    s = config.ResetDefaultConfig();
    ASSERT_TRUE(s.ok());
}
