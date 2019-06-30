////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <gtest/gtest.h>

#include "config/IConfigMgr.h"
#include "server/ServerConfig.h"

using namespace zilliz::milvus;

namespace {

static const std::string CONFIG_FILE_PATH = "./milvus/conf/server_config.yaml";
static const std::string LOG_FILE_PATH = "./milvus/conf/log_config.conf";

}

TEST(ConfigTest, CONFIG_TEST) {
    server::IConfigMgr* config_mgr = server::IConfigMgr::GetInstance();

    server::ServerError err = config_mgr->LoadConfigFile("");
    ASSERT_EQ(err, server::SERVER_UNEXPECTED_ERROR);

    err = config_mgr->LoadConfigFile(LOG_FILE_PATH);
    ASSERT_EQ(err, server::SERVER_UNEXPECTED_ERROR);

    err = config_mgr->LoadConfigFile(CONFIG_FILE_PATH);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    config_mgr->Print();

    server::ConfigNode& root_config = config_mgr->GetRootNode();
    server::ConfigNode& server_config = root_config.GetChild("server_config");
    server::ConfigNode& db_config = root_config.GetChild("db_config");
    server::ConfigNode& metric_config = root_config.GetChild("metric_config");
    server::ConfigNode& cache_config = root_config.GetChild("cache_config");

    std::string address = server_config.GetValue("address");
    ASSERT_TRUE(!address.empty());
    int64_t port = server_config.GetInt64Value("port");
    ASSERT_TRUE(port != 0);

    server_config.SetValue("test", "2.5");
    double test = server_config.GetDoubleValue("test");
    ASSERT_EQ(test, 2.5);

    server::ConfigNode fake;
    server_config.AddChild("fake", fake);
    fake = server_config.GetChild("fake");
    server::ConfigNodeArr arr;
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

    server::ConfigNode combine;
    combine.Combine(server_config);

    combine.PrintAll();
    std::string all = combine.DumpString();
    ASSERT_TRUE(!all.empty());

    server_config.ClearSequences();
    auto seqs = server_config.GetSequences();
    ASSERT_TRUE(seqs.empty());

    const server::ConfigNode const_node = root_config.GetChild("cache_config");
    float flt = const_node.GetFloatValue("cpu_cache_capacity");
    ASSERT_GT(flt, 0.0);
}

TEST(ConfigTest, SERVER_CONFIG_TEST) {
    server::ServerConfig& config = server::ServerConfig::GetInstance();
    server::ServerError err = config.LoadConfigFile(CONFIG_FILE_PATH);
    ASSERT_EQ(err, server::SERVER_SUCCESS);

    server::ConfigNode node1 = config.GetConfig("server_config");
    server::ConfigNode& node2 = config.GetConfig("cache_config");
    node1.Combine(node2);

    int32_t cap = node1.GetInt32Value("cpu_cache_capacity");
    ASSERT_GT(cap, 0);

    node1.SetValue("bool", "true");
    bool bt = node1.GetBoolValue("bool");
    ASSERT_TRUE(bt);

    config.PrintAll();

    const server::ServerConfig const_config = config;
    server::ConfigNode node = const_config.GetConfig("aaa");
}