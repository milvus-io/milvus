// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include <fiu-control.h>
#include <fiu-local.h>
#include <gtest/gtest-death-test.h>
#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <thread>

#include "config/Config.h"
#include "config/YamlConfigMgr.h"
#include "config/handler/CacheConfigHandler.h"
#include "server/utils.h"
#include "utils/CommonUtil.h"
#include "utils/StringHelpFunctions.h"
#include "utils/ValidationUtil.h"

namespace {

static constexpr uint64_t KB = 1024;
static constexpr uint64_t MB = KB * 1024;
static constexpr uint64_t GB = MB * 1024;

class TestConfigHandler : public milvus::server::CacheConfigHandler {
 public:
    TestConfigHandler() {
        SetIdentity("MemTableFile");
        AddInsertBufferSizeListener();
    }
};

}  // namespace

namespace ms = milvus::server;

TEST_F(ConfigTest, CONFIG_HANDLER_TEST) {
    auto test_func = [&]() {
        uint64_t count = 10000, index = 0;
        while (true) {
            if (index++ == count) {
                break;
            }

            // register callback
            TestConfigHandler ttt;

            // trigger callback
            auto& config = milvus::server::Config::GetInstance();
            config.SetCacheConfigInsertBufferSize("1GB");
        }
    };

    using ThreadPtr = std::shared_ptr<std::thread>;
    ThreadPtr thread_1 = std::make_shared<std::thread>(test_func);
    ThreadPtr thread_2 = std::make_shared<std::thread>(test_func);
    thread_1->join();
    thread_2->join();
}

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
    config_mgr->DumpString();

    milvus::server::ConfigNode& root_config = config_mgr->GetRootNode();
    milvus::server::ConfigNode& server_config = root_config.GetChild("network");
    milvus::server::ConfigNode invalid_config = root_config.GetChild("invalid_config");

    const auto& im_config_mgr = *static_cast<milvus::server::YamlConfigMgr*>(config_mgr);
    const milvus::server::ConfigNode& im_root_config = im_config_mgr.GetRootNode();
    const milvus::server::ConfigNode& im_invalid_config = im_root_config.GetChild("invalid_config");
    const milvus::server::ConfigNode& im_not_exit_config = im_root_config.GetChild("not_exit_config");
    ASSERT_EQ(im_not_exit_config.GetConfig().size(), 0);
    ASSERT_EQ(im_root_config.DumpString(), root_config.DumpString());
    ASSERT_EQ(im_invalid_config.DumpString(), invalid_config.DumpString());

    auto valus = invalid_config.GetSequence("not_exist");
    float ff = invalid_config.GetFloatValue("not_exist", 3.0);
    ASSERT_EQ(ff, 3.0);
    double not_exit_double = server_config.GetDoubleValue("not_exit", 3.0);
    ASSERT_EQ(not_exit_double, 3.0);
    int64_t not_exit_int64 = server_config.GetInt64Value("not_exit", 3);
    ASSERT_EQ(not_exit_int64, 3);
    int64_t not_exit_int32 = server_config.GetInt32Value("not_exit", 3);
    ASSERT_EQ(not_exit_int32, 3);
    bool not_exit_bool = server_config.GetBoolValue("not_exit", false);
    ASSERT_FALSE(not_exit_bool);

    std::string address = server_config.GetValue("bind.address");
    ASSERT_TRUE(!address.empty());
    int64_t port = server_config.GetInt64Value("bind.port");
    ASSERT_NE(port, 0);

    server_config.SetValue("float_test", "2.5");
    double dbl = server_config.GetDoubleValue("float_test");
    ASSERT_LE(std::fabs(dbl - 2.5), std::numeric_limits<double>::epsilon());
    float flt = server_config.GetFloatValue("float_test");
    ASSERT_LE(std::fabs(flt - 2.5), std::numeric_limits<float>::epsilon());

    server_config.SetValue("bool_test", "true");
    bool blt = server_config.GetBoolValue("bool_test");
    ASSERT_TRUE(blt);

    server_config.SetValue("int_test", "34");
    int32_t it32 = server_config.GetInt32Value("int_test");
    ASSERT_EQ(it32, 34);
    int64_t it64 = server_config.GetInt64Value("int_test");
    ASSERT_EQ(it64, 34);

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

    server_config.SetValue("fake", "fake");
    server_config.AddChild("fake", fake);
    milvus::server::ConfigNode combine;
    combine.Combine(server_config);

    combine.PrintAll();
    std::string all = combine.DumpString();
    ASSERT_TRUE(!all.empty());

    server_config.ClearSequences();
    auto seqs = server_config.GetSequences();
    ASSERT_TRUE(seqs.empty());
}

TEST_F(ConfigTest, SERVER_CONFIG_VALID_TEST) {
    milvus::server::Config& config = milvus::server::Config::GetInstance();
    std::string str_val;
    int64_t int64_val;
    float float_val;
    bool bool_val;

    /* server config */
    std::string server_addr = "192.168.1.155";
    ASSERT_TRUE(config.SetNetworkConfigBindAddress(server_addr).ok());
    ASSERT_TRUE(config.GetNetworkConfigBindAddress(str_val).ok());
    ASSERT_TRUE(str_val == server_addr);

    std::string server_port = "12345";
    ASSERT_TRUE(config.SetNetworkConfigBindPort(server_port).ok());
    ASSERT_TRUE(config.GetNetworkConfigBindPort(str_val).ok());
    ASSERT_TRUE(str_val == server_port);

    std::string web_port = "19999";
    ASSERT_TRUE(config.SetNetworkConfigHTTPPort(web_port).ok());
    ASSERT_TRUE(config.GetNetworkConfigHTTPPort(str_val).ok());
    ASSERT_TRUE(str_val == web_port);

    std::string server_mode = "ro";
    ASSERT_TRUE(config.SetClusterConfigRole(server_mode).ok());
    ASSERT_TRUE(config.GetClusterConfigRole(str_val).ok());
    ASSERT_TRUE(str_val == server_mode);

    std::string server_time_zone = "UTC+6";
    ASSERT_TRUE(config.SetGeneralConfigTimezone(server_time_zone).ok());
    ASSERT_TRUE(config.GetGeneralConfigTimezone(str_val).ok());
    ASSERT_TRUE(str_val == server_time_zone);

    /* db config */
    std::string db_backend_url = "mysql://root:123456@127.0.0.1:19530/milvus";
    ASSERT_TRUE(config.SetGeneralConfigMetaURI(db_backend_url).ok());
    ASSERT_TRUE(config.GetGeneralConfigMetaURI(str_val).ok());
    ASSERT_TRUE(str_val == db_backend_url);

    int64_t db_archive_disk_threshold = 100;
    ASSERT_TRUE(config.SetDBConfigArchiveDiskThreshold(std::to_string(db_archive_disk_threshold)).ok());
    ASSERT_TRUE(config.GetDBConfigArchiveDiskThreshold(int64_val).ok());
    ASSERT_TRUE(int64_val == db_archive_disk_threshold);

    int64_t db_archive_days_threshold = 365;
    ASSERT_TRUE(config.SetDBConfigArchiveDaysThreshold(std::to_string(db_archive_days_threshold)).ok());
    ASSERT_TRUE(config.GetDBConfigArchiveDaysThreshold(int64_val).ok());
    ASSERT_TRUE(int64_val == db_archive_days_threshold);

    int64_t db_auto_flush_interval = 1;
    ASSERT_TRUE(config.SetStorageConfigAutoFlushInterval(std::to_string(db_auto_flush_interval)).ok());
    ASSERT_TRUE(config.GetStorageConfigAutoFlushInterval(int64_val).ok());
    ASSERT_TRUE(int64_val == db_auto_flush_interval);

    /* storage config */
    std::string storage_primary_path = "/home/zilliz";
    ASSERT_TRUE(config.SetStorageConfigPath(storage_primary_path).ok());
    ASSERT_TRUE(config.GetStorageConfigPath(str_val).ok());
    ASSERT_TRUE(str_val == storage_primary_path);

    //    bool storage_s3_enable = true;
    //    ASSERT_TRUE(config.SetStorageConfigS3Enable(std::to_string(storage_s3_enable)).ok());
    //    ASSERT_TRUE(config.GetStorageConfigS3Enable(bool_val).ok());
    //    ASSERT_TRUE(bool_val == storage_s3_enable);
    //
    //    std::string storage_s3_addr = "192.168.1.100";
    //    ASSERT_TRUE(config.SetStorageConfigS3Address(storage_s3_addr).ok());
    //    ASSERT_TRUE(config.GetStorageConfigS3Address(str_val).ok());
    //    ASSERT_TRUE(str_val == storage_s3_addr);
    //
    //    std::string storage_s3_port = "12345";
    //    ASSERT_TRUE(config.SetStorageConfigS3Port(storage_s3_port).ok());
    //    ASSERT_TRUE(config.GetStorageConfigS3Port(str_val).ok());
    //    ASSERT_TRUE(str_val == storage_s3_port);
    //
    //    std::string storage_s3_access_key = "minioadmin";
    //    ASSERT_TRUE(config.SetStorageConfigS3AccessKey(storage_s3_access_key).ok());
    //    ASSERT_TRUE(config.GetStorageConfigS3AccessKey(str_val).ok());
    //    ASSERT_TRUE(str_val == storage_s3_access_key);
    //
    //    std::string storage_s3_secret_key = "minioadmin";
    //    ASSERT_TRUE(config.SetStorageConfigS3SecretKey(storage_s3_secret_key).ok());
    //    ASSERT_TRUE(config.GetStorageConfigS3SecretKey(str_val).ok());
    //    ASSERT_TRUE(str_val == storage_s3_secret_key);
    //
    //    std::string storage_s3_bucket = "s3bucket";
    //    ASSERT_TRUE(config.SetStorageConfigS3Bucket(storage_s3_bucket).ok());
    //    ASSERT_TRUE(config.GetStorageConfigS3Bucket(str_val).ok());
    //    ASSERT_TRUE(str_val == storage_s3_bucket);

    /* metric config */
    bool metric_enable_monitor = false;
    ASSERT_TRUE(config.SetMetricConfigEnableMonitor(std::to_string(metric_enable_monitor)).ok());
    ASSERT_TRUE(config.GetMetricConfigEnableMonitor(bool_val).ok());
    ASSERT_TRUE(bool_val == metric_enable_monitor);

    std::string metric_address = "192.168.0.2";
    ASSERT_TRUE(config.SetMetricConfigAddress(metric_address).ok());
    ASSERT_TRUE(config.GetMetricConfigAddress(str_val).ok());
    ASSERT_TRUE(str_val == metric_address);

    std::string metric_port = "2222";
    ASSERT_TRUE(config.SetMetricConfigPort(metric_port).ok());
    ASSERT_TRUE(config.GetMetricConfigPort(str_val).ok());
    ASSERT_TRUE(str_val == metric_port);

    /* cache config */
    int64_t cache_cpu_cache_capacity = 1;
    ASSERT_TRUE(config.SetCacheConfigCpuCacheCapacity(std::to_string(cache_cpu_cache_capacity)).ok());
    ASSERT_TRUE(config.GetCacheConfigCpuCacheCapacity(int64_val).ok());
    ASSERT_TRUE(int64_val == cache_cpu_cache_capacity);

    float cache_cpu_cache_threshold = 0.1;
    ASSERT_TRUE(config.SetCacheConfigCpuCacheThreshold(std::to_string(cache_cpu_cache_threshold)).ok());
    ASSERT_TRUE(config.GetCacheConfigCpuCacheThreshold(float_val).ok());
    ASSERT_TRUE(float_val == cache_cpu_cache_threshold);

    int64_t cache_insert_buffer_size = 2;
    ASSERT_TRUE(config.SetCacheConfigInsertBufferSize(std::to_string(cache_insert_buffer_size)).ok());
    ASSERT_TRUE(config.GetCacheConfigInsertBufferSize(int64_val).ok());
    ASSERT_TRUE(int64_val == cache_insert_buffer_size);

    bool cache_insert_data = true;
    ASSERT_TRUE(config.SetCacheConfigCacheInsertData(std::to_string(cache_insert_data)).ok());
    ASSERT_TRUE(config.GetCacheConfigCacheInsertData(bool_val).ok());
    ASSERT_TRUE(bool_val == cache_insert_data);

    {
        // #2564
        int64_t total_mem = 0, free_mem = 0;
        milvus::server::CommonUtil::GetSystemMemInfo(total_mem, free_mem);
        int64_t cgroup_limit_size = 0;
        milvus::server::CommonUtil::GetSysCgroupMemLimit(cgroup_limit_size);
        ASSERT_TRUE(config.SetCacheConfigInsertBufferSize("1GB").ok());
        int64_t cache_cpu_cache_size = 0;
        if (cgroup_limit_size < total_mem) {
            cache_cpu_cache_size = cgroup_limit_size / 2;
        } else {
            cache_cpu_cache_size = total_mem / 2;
        }
        float cache_cpu_cache_threshold = 0.7;
        ASSERT_TRUE(config.SetCacheConfigCpuCacheThreshold(std::to_string(cache_cpu_cache_threshold)).ok());
        ASSERT_TRUE(config.SetCacheConfigCpuCacheCapacity(std::to_string(cache_cpu_cache_size)).ok());
        ASSERT_TRUE(config.GetCacheConfigCpuCacheCapacity(int64_val).ok());
        ASSERT_TRUE(int64_val == cache_cpu_cache_size);
    }

    {
        int64_t total_mem = 0, free_mem = 0;
        milvus::server::CommonUtil::GetSystemMemInfo(total_mem, free_mem);
        int64_t cgroup_limit_size = 0;
        milvus::server::CommonUtil::GetSysCgroupMemLimit(cgroup_limit_size);
        ASSERT_TRUE(config.SetCacheConfigInsertBufferSize("1GB").ok());
        int64_t cache_cpu_cache_size = 0;
        if (cgroup_limit_size < total_mem) {
            cache_cpu_cache_size = cgroup_limit_size - 1073741824 - 1;
        } else {
            cache_cpu_cache_size = total_mem - 1073741824 - 1;  // total_size - 1GB - 1
        }
        ASSERT_TRUE(config.SetCacheConfigCpuCacheCapacity(std::to_string(cache_cpu_cache_size)).ok());
        ASSERT_TRUE(config.GetCacheConfigCpuCacheCapacity(int64_val).ok());
        ASSERT_TRUE(int64_val == cache_cpu_cache_size);
    }

    /* engine config */
    int64_t engine_use_blas_threshold = 50;
    ASSERT_TRUE(config.SetEngineConfigUseBlasThreshold(std::to_string(engine_use_blas_threshold)).ok());
    ASSERT_TRUE(config.GetEngineConfigUseBlasThreshold(int64_val).ok());
    ASSERT_TRUE(int64_val == engine_use_blas_threshold);

    int64_t engine_omp_thread_num = 1;
    ASSERT_TRUE(config.SetEngineConfigOmpThreadNum(std::to_string(engine_omp_thread_num)).ok());
    ASSERT_TRUE(config.GetEngineConfigOmpThreadNum(int64_val).ok());
    ASSERT_TRUE(int64_val == engine_omp_thread_num);

    std::string engine_simd_type = "sse";
    ASSERT_TRUE(config.SetEngineConfigSimdType(engine_simd_type).ok());
    ASSERT_TRUE(config.GetEngineConfigSimdType(str_val).ok());
    ASSERT_TRUE(str_val == engine_simd_type);

    int64_t max_partition = 1;
    ASSERT_TRUE(config.SetEngineConfigMaxPartitionNum(std::to_string(max_partition)).ok());
    ASSERT_TRUE(config.GetEngineConfigMaxPartitionNum(int64_val).ok());
    ASSERT_TRUE(int64_val == max_partition);

#ifdef MILVUS_GPU_VERSION
    int64_t engine_gpu_search_threshold = 800;
    auto status = config.SetGpuResourceConfigGpuSearchThreshold(std::to_string(engine_gpu_search_threshold));
    ASSERT_TRUE(status.ok()) << status.message();
    ASSERT_TRUE(config.GetGpuResourceConfigGpuSearchThreshold(int64_val).ok());
    ASSERT_TRUE(int64_val == engine_gpu_search_threshold);
#endif

    /* gpu resource config */
#ifdef MILVUS_GPU_VERSION
    bool resource_enable_gpu = true;
    ASSERT_TRUE(config.SetGpuResourceConfigEnable(std::to_string(resource_enable_gpu)).ok());
    ASSERT_TRUE(config.GetGpuResourceConfigEnable(bool_val).ok());
    ASSERT_TRUE(bool_val == resource_enable_gpu);

    int64_t gpu_cache_capacity = 1;
    ASSERT_TRUE(config.SetGpuResourceConfigCacheCapacity(std::to_string(gpu_cache_capacity)).ok());
    ASSERT_TRUE(config.GetGpuResourceConfigCacheCapacity(int64_val).ok());
    ASSERT_TRUE(int64_val == gpu_cache_capacity);

    float gpu_cache_threshold = 0.2;
    ASSERT_TRUE(config.SetGpuResourceConfigCacheThreshold(std::to_string(gpu_cache_threshold)).ok());
    ASSERT_TRUE(config.GetGpuResourceConfigCacheThreshold(float_val).ok());
    ASSERT_TRUE(float_val == gpu_cache_threshold);

    std::vector<std::string> search_resources = {"gpu0"};
    std::vector<int64_t> search_res_vec;
    std::string search_res_str;
    milvus::server::StringHelpFunctions::MergeStringWithDelimeter(
        search_resources, milvus::server::CONFIG_GPU_RESOURCE_DELIMITER, search_res_str);
    ASSERT_TRUE(config.SetGpuResourceConfigSearchResources(search_res_str).ok());
    ASSERT_TRUE(config.GetGpuResourceConfigSearchResources(search_res_vec).ok());
    for (size_t i = 0; i < search_resources.size(); i++) {
        ASSERT_TRUE(std::stoll(search_resources[i].substr(3)) == search_res_vec[i]);
    }

    std::vector<std::string> build_index_resources = {"gpu0"};
    std::vector<int64_t> build_index_res_vec;
    std::string build_index_res_str;
    milvus::server::StringHelpFunctions::MergeStringWithDelimeter(
        build_index_resources, milvus::server::CONFIG_GPU_RESOURCE_DELIMITER, build_index_res_str);
    ASSERT_TRUE(config.SetGpuResourceConfigBuildIndexResources(build_index_res_str).ok());
    ASSERT_TRUE(config.GetGpuResourceConfigBuildIndexResources(build_index_res_vec).ok());
    for (size_t i = 0; i < build_index_resources.size(); i++) {
        ASSERT_TRUE(std::stoll(build_index_resources[i].substr(3)) == build_index_res_vec[i]);
    }
#endif

    /* wal config */
    bool wal_enable = false;
    ASSERT_TRUE(config.SetWalConfigEnable(std::to_string(wal_enable)).ok());
    ASSERT_TRUE(config.GetWalConfigEnable(bool_val).ok());
    ASSERT_TRUE(bool_val == wal_enable);

    bool wal_recovery_ignore = true;
    ASSERT_TRUE(config.SetWalConfigRecoveryErrorIgnore(std::to_string(wal_recovery_ignore)).ok());
    ASSERT_TRUE(config.GetWalConfigRecoveryErrorIgnore(bool_val).ok());
    ASSERT_TRUE(bool_val == wal_recovery_ignore);

    int64_t wal_buffer_size = 128 * 1024 * 1024;  // 128 M
    ASSERT_TRUE(config.SetWalConfigBufferSize(std::to_string(wal_buffer_size)).ok());
    ASSERT_TRUE(config.GetWalConfigBufferSize(int64_val).ok());
    ASSERT_TRUE(int64_val == wal_buffer_size);

    std::string wal_path = "/tmp/aaa/wal";
    ASSERT_TRUE(config.SetWalConfigWalPath(wal_path).ok());
    ASSERT_TRUE(config.GetWalConfigWalPath(str_val).ok());
    ASSERT_TRUE(str_val == wal_path);

    /* logs config */
    std::string logs_level = "debug";
    ASSERT_TRUE(config.SetLogsLevel(logs_level).ok());
    ASSERT_TRUE(config.GetLogsLevel(str_val).ok());
    ASSERT_TRUE(str_val == logs_level);

    bool logs_trace_enable = false;
    ASSERT_TRUE(config.SetLogsTraceEnable(std::to_string(logs_trace_enable)).ok());
    ASSERT_TRUE(config.GetLogsTraceEnable(bool_val).ok());
    ASSERT_TRUE(bool_val == logs_trace_enable);

    std::string logs_path = "/tmp/aaa/logs";
    ASSERT_TRUE(config.SetLogsPath(logs_path).ok());
    ASSERT_TRUE(config.GetLogsPath(str_val).ok());
    ASSERT_TRUE(str_val == logs_path);

    std::string logs_max_log_file_size = "1000MB";
    auto s = config.SetLogsMaxLogFileSize(logs_max_log_file_size);
    ASSERT_TRUE(s.ok()) << s.message();
    ASSERT_TRUE(config.GetLogsMaxLogFileSize(int64_val).ok());
    ASSERT_TRUE(int64_val == 1000 * 1024 * 1024);  // 1000MB

    int64_t logs_log_rotate_num = 100;
    ASSERT_TRUE(config.SetLogsLogRotateNum(std::to_string(logs_log_rotate_num)).ok());
    ASSERT_TRUE(config.GetLogsLogRotateNum(int64_val).ok());
    ASSERT_TRUE(int64_val == logs_log_rotate_num);

    bool log_to_stdout = true;
    ASSERT_TRUE(config.SetLogsLogToStdout(std::to_string(log_to_stdout)).ok());
    ASSERT_TRUE(config.GetLogsLogToStdout(bool_val).ok());
    ASSERT_TRUE(bool_val == log_to_stdout);

    bool log_to_file = false;
    ASSERT_TRUE(config.SetLogsLogToFile(std::to_string(log_to_file)).ok());
    ASSERT_TRUE(config.GetLogsLogToFile(bool_val).ok());
    ASSERT_TRUE(bool_val == log_to_file);
}

std::string
gen_get_command(const std::string& parent_node, const std::string& child_node) {
    std::string cmd = "get_config " + parent_node + ms::CONFIG_NODE_DELIMITER + child_node;
    return cmd;
}

std::string
gen_set_command(const std::string& parent_node, const std::string& child_node, const std::string& value) {
    std::string cmd = "set_config " + parent_node + ms::CONFIG_NODE_DELIMITER + child_node + " " + value;
    return cmd;
}

TEST_F(ConfigTest, SERVER_CONFIG_CLI_TEST) {
    std::string conf_file = std::string(CONFIG_PATH) + VALID_CONFIG_FILE;
    milvus::server::Config& config = milvus::server::Config::GetInstance();

    auto s = config.LoadConfigFile(conf_file);
    ASSERT_TRUE(s.ok()) << s.message();
    s = config.ResetDefaultConfig();
    ASSERT_TRUE(s.ok()) << s.message();

    std::string get_cmd, set_cmd;
    std::string result, dummy;

    s = config.ProcessConfigCli(result, "get_config *");
    ASSERT_TRUE(s.ok());

    /* server config */
    std::string server_addr = "192.168.1.155";
    get_cmd = gen_get_command(ms::CONFIG_NETWORK, ms::CONFIG_NETWORK_BIND_ADDRESS);
    set_cmd = gen_set_command(ms::CONFIG_NETWORK, ms::CONFIG_NETWORK_BIND_ADDRESS, server_addr);
    s = config.ProcessConfigCli(dummy, set_cmd);
    ASSERT_TRUE(s.ok()) << s.message();
    s = config.ProcessConfigCli(result, get_cmd);
    ASSERT_TRUE(s.ok()) << s.message();

    /* db config */
    std::string db_backend_url = "sqlite://milvus:zilliz@:/";
    get_cmd = gen_get_command(ms::CONFIG_GENERAL, ms::CONFIG_GENERAL_METAURI);
    set_cmd = gen_set_command(ms::CONFIG_GENERAL, ms::CONFIG_GENERAL_METAURI, db_backend_url);
    s = config.ProcessConfigCli(dummy, set_cmd);
    ASSERT_TRUE(s.ok());
    s = config.ProcessConfigCli(result, get_cmd);
    ASSERT_TRUE(s.ok());

    /* metric config */
    std::string metric_enable_monitor = "false";
    get_cmd = gen_get_command(ms::CONFIG_METRIC, ms::CONFIG_METRIC_ENABLE_MONITOR);
    set_cmd = gen_set_command(ms::CONFIG_METRIC, ms::CONFIG_METRIC_ENABLE_MONITOR, metric_enable_monitor);
    s = config.ProcessConfigCli(dummy, set_cmd);
    ASSERT_TRUE(s.ok());
    s = config.ProcessConfigCli(result, get_cmd);
    ASSERT_TRUE(s.ok());

    /* storage config */
    std::string storage_primary_path = "/tmp/milvus1";
    get_cmd = gen_get_command(ms::CONFIG_STORAGE, ms::CONFIG_STORAGE_PATH);
    set_cmd = gen_set_command(ms::CONFIG_STORAGE, ms::CONFIG_STORAGE_PATH, storage_primary_path);
    s = config.ProcessConfigCli(dummy, set_cmd);
    ASSERT_TRUE(s.ok());
    s = config.ProcessConfigCli(result, get_cmd);
    ASSERT_TRUE(s.ok());

    std::string storage_auto_flush_interval = "42";
    get_cmd = gen_get_command(ms::CONFIG_STORAGE, ms::CONFIG_STORAGE_AUTO_FLUSH_INTERVAL);
    set_cmd = gen_set_command(ms::CONFIG_STORAGE, ms::CONFIG_STORAGE_AUTO_FLUSH_INTERVAL, storage_auto_flush_interval);
    s = config.ProcessConfigCli(dummy, set_cmd);
    ASSERT_TRUE(s.ok());
    s = config.ProcessConfigCli(result, get_cmd);
    ASSERT_TRUE(s.ok());

    /* cache config */
    std::string cache_cpu_cache_capacity = "1";
    get_cmd = gen_get_command(ms::CONFIG_CACHE, ms::CONFIG_CACHE_CPU_CACHE_CAPACITY);
    set_cmd = gen_set_command(ms::CONFIG_CACHE, ms::CONFIG_CACHE_CPU_CACHE_CAPACITY, cache_cpu_cache_capacity);
    s = config.ProcessConfigCli(dummy, set_cmd);
    ASSERT_TRUE(s.ok());
    s = config.ProcessConfigCli(result, get_cmd);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(result == cache_cpu_cache_capacity);

    std::string cache_cpu_cache_threshold = "0.1";
    get_cmd = gen_get_command(ms::CONFIG_CACHE, ms::CONFIG_CACHE_CPU_CACHE_THRESHOLD);
    set_cmd = gen_set_command(ms::CONFIG_CACHE, ms::CONFIG_CACHE_CPU_CACHE_THRESHOLD, cache_cpu_cache_threshold);
    s = config.ProcessConfigCli(dummy, set_cmd);
    ASSERT_TRUE(s.ok());
    s = config.ProcessConfigCli(result, get_cmd);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(result == cache_cpu_cache_threshold);

    std::string cache_insert_buffer_size = "1";
    get_cmd = gen_get_command(ms::CONFIG_CACHE, ms::CONFIG_CACHE_INSERT_BUFFER_SIZE);
    set_cmd = gen_set_command(ms::CONFIG_CACHE, ms::CONFIG_CACHE_INSERT_BUFFER_SIZE, cache_insert_buffer_size);
    s = config.ProcessConfigCli(dummy, set_cmd);
    ASSERT_TRUE(s.ok());
    s = config.ProcessConfigCli(result, get_cmd);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(result == cache_insert_buffer_size);

    std::string cache_insert_data = "true";
    get_cmd = gen_get_command(ms::CONFIG_CACHE, ms::CONFIG_CACHE_CACHE_INSERT_DATA);
    set_cmd = gen_set_command(ms::CONFIG_CACHE, ms::CONFIG_CACHE_CACHE_INSERT_DATA, cache_insert_data);
    s = config.ProcessConfigCli(dummy, set_cmd);
    ASSERT_TRUE(s.ok());
    s = config.ProcessConfigCli(result, get_cmd);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(result == cache_insert_data);

    /* engine config */
    std::string engine_use_blas_threshold = "50";
    get_cmd = gen_get_command(ms::CONFIG_ENGINE, ms::CONFIG_ENGINE_USE_BLAS_THRESHOLD);
    set_cmd = gen_set_command(ms::CONFIG_ENGINE, ms::CONFIG_ENGINE_USE_BLAS_THRESHOLD, engine_use_blas_threshold);
    s = config.ProcessConfigCli(dummy, set_cmd);
    ASSERT_TRUE(s.ok());
    s = config.ProcessConfigCli(result, get_cmd);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(result == engine_use_blas_threshold);

    std::string engine_omp_thread_num = "1";
    get_cmd = gen_get_command(ms::CONFIG_ENGINE, ms::CONFIG_ENGINE_OMP_THREAD_NUM);
    set_cmd = gen_set_command(ms::CONFIG_ENGINE, ms::CONFIG_ENGINE_OMP_THREAD_NUM, engine_omp_thread_num);
    s = config.ProcessConfigCli(dummy, set_cmd);
    ASSERT_TRUE(s.ok());
    s = config.ProcessConfigCli(result, get_cmd);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(result == engine_omp_thread_num);

    std::string engine_simd_type = "sse";
    get_cmd = gen_get_command(ms::CONFIG_ENGINE, ms::CONFIG_ENGINE_SIMD_TYPE);
    set_cmd = gen_set_command(ms::CONFIG_ENGINE, ms::CONFIG_ENGINE_SIMD_TYPE, engine_simd_type);
    s = config.ProcessConfigCli(dummy, set_cmd);
    ASSERT_TRUE(s.ok());
    s = config.ProcessConfigCli(result, get_cmd);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(result == engine_simd_type);

#ifdef MILVUS_GPU_VERSION
    std::string engine_gpu_search_threshold = "800";
    get_cmd = gen_get_command(ms::CONFIG_GPU_RESOURCE, ms::CONFIG_GPU_RESOURCE_GPU_SEARCH_THRESHOLD);
    set_cmd = gen_set_command(ms::CONFIG_GPU_RESOURCE, ms::CONFIG_GPU_RESOURCE_GPU_SEARCH_THRESHOLD,
                              engine_gpu_search_threshold);
    s = config.ProcessConfigCli(dummy, set_cmd);
    ASSERT_TRUE(s.ok());
    s = config.ProcessConfigCli(result, get_cmd);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(result == engine_gpu_search_threshold);
#endif

    /* gpu resource config */
#ifdef MILVUS_GPU_VERSION
    std::string resource_enable_gpu = "true";
    get_cmd = gen_get_command(ms::CONFIG_GPU_RESOURCE, ms::CONFIG_GPU_RESOURCE_ENABLE);
    set_cmd = gen_set_command(ms::CONFIG_GPU_RESOURCE, ms::CONFIG_GPU_RESOURCE_ENABLE, resource_enable_gpu);
    s = config.ProcessConfigCli(dummy, set_cmd);
    ASSERT_TRUE(s.ok());
    s = config.ProcessConfigCli(result, get_cmd);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(result == resource_enable_gpu);

    std::string gpu_cache_capacity = "1";
    get_cmd = gen_get_command(ms::CONFIG_GPU_RESOURCE, ms::CONFIG_GPU_RESOURCE_CACHE_CAPACITY);
    set_cmd = gen_set_command(ms::CONFIG_GPU_RESOURCE, ms::CONFIG_GPU_RESOURCE_CACHE_CAPACITY, gpu_cache_capacity);
    s = config.ProcessConfigCli(dummy, set_cmd);
    ASSERT_TRUE(s.ok());
    s = config.ProcessConfigCli(result, get_cmd);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(result == gpu_cache_capacity);

    std::string gpu_cache_threshold = "0.2";
    get_cmd = gen_get_command(ms::CONFIG_GPU_RESOURCE, ms::CONFIG_GPU_RESOURCE_CACHE_THRESHOLD);
    set_cmd = gen_set_command(ms::CONFIG_GPU_RESOURCE, ms::CONFIG_GPU_RESOURCE_CACHE_THRESHOLD, gpu_cache_threshold);
    s = config.ProcessConfigCli(dummy, set_cmd);
    ASSERT_TRUE(s.ok());
    s = config.ProcessConfigCli(result, get_cmd);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(result == gpu_cache_threshold);

    std::string search_resources = "gpu0";
    get_cmd = gen_get_command(ms::CONFIG_GPU_RESOURCE, ms::CONFIG_GPU_RESOURCE_SEARCH_RESOURCES);
    set_cmd = gen_set_command(ms::CONFIG_GPU_RESOURCE, ms::CONFIG_GPU_RESOURCE_SEARCH_RESOURCES, search_resources);
    s = config.ProcessConfigCli(dummy, set_cmd);
    ASSERT_TRUE(s.ok());
    s = config.ProcessConfigCli(result, get_cmd);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(result == search_resources);

    std::string build_index_resources = "gpu0";
    get_cmd = gen_get_command(ms::CONFIG_GPU_RESOURCE, ms::CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES);
    set_cmd =
        gen_set_command(ms::CONFIG_GPU_RESOURCE, ms::CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES, build_index_resources);
    s = config.ProcessConfigCli(dummy, set_cmd);
    ASSERT_TRUE(s.ok());
    s = config.ProcessConfigCli(result, get_cmd);
    ASSERT_TRUE(s.ok());
    ASSERT_TRUE(result == build_index_resources);
#endif

    /* wal config */
    std::string wal_path = "/tmp/aaa/wal";
    get_cmd = gen_get_command(ms::CONFIG_WAL, ms::CONFIG_WAL_WAL_PATH);
    set_cmd = gen_set_command(ms::CONFIG_WAL, ms::CONFIG_WAL_WAL_PATH, wal_path);
    s = config.ProcessConfigCli(dummy, set_cmd);
    ASSERT_TRUE(s.ok());
    s = config.ProcessConfigCli(result, get_cmd);
    ASSERT_TRUE(s.ok());

    /* logs config */
    std::string logs_path = "/tmp/aaa/logs";
    get_cmd = gen_get_command(ms::CONFIG_LOGS, ms::CONFIG_LOGS_PATH);
    set_cmd = gen_set_command(ms::CONFIG_LOGS, ms::CONFIG_LOGS_PATH, logs_path);
    s = config.ProcessConfigCli(dummy, set_cmd);
    ASSERT_TRUE(s.ok());
    s = config.ProcessConfigCli(result, get_cmd);
    ASSERT_TRUE(s.ok());
}

TEST_F(ConfigTest, SERVER_CONFIG_INVALID_TEST) {
    std::string config_path(CONFIG_PATH);
    milvus::server::Config& config = milvus::server::Config::GetInstance();
    milvus::Status s;

    ASSERT_FALSE(config.LoadConfigFile("").ok());

    ASSERT_FALSE(config.LoadConfigFile(config_path + INVALID_CONFIG_FILE).ok());
    ASSERT_FALSE(config.LoadConfigFile(config_path + "dummy.yaml").ok());

    /* server config */
    ASSERT_FALSE(config.SetNetworkConfigBindAddress("0.0.0").ok());
    ASSERT_FALSE(config.SetNetworkConfigBindAddress("0.0.0.256").ok());

    ASSERT_FALSE(config.SetNetworkConfigBindPort("a").ok());
    ASSERT_FALSE(config.SetNetworkConfigBindPort("99999").ok());

    ASSERT_FALSE(config.SetNetworkConfigHTTPPort("a").ok());
    ASSERT_FALSE(config.SetNetworkConfigHTTPPort("99999").ok());
    ASSERT_FALSE(config.SetNetworkConfigHTTPPort("-1").ok());

    ASSERT_FALSE(config.SetClusterConfigRole("cluster").ok());

    ASSERT_FALSE(config.SetGeneralConfigTimezone("GM").ok());
    ASSERT_FALSE(config.SetGeneralConfigTimezone("GMT8").ok());
    ASSERT_FALSE(config.SetGeneralConfigTimezone("UTCA").ok());

    /* db config */
    ASSERT_FALSE(config.SetGeneralConfigMetaURI("http://www.google.com").ok());
    ASSERT_FALSE(config.SetGeneralConfigMetaURI("sqlite://:@:").ok());
    ASSERT_FALSE(config.SetGeneralConfigMetaURI("mysql://root:123456@127.0.0.1/milvus").ok());

    ASSERT_FALSE(config.SetDBConfigArchiveDiskThreshold("0x10").ok());

    ASSERT_FALSE(config.SetDBConfigArchiveDaysThreshold("0x10").ok());

    /* storage config */
    ASSERT_FALSE(config.SetStorageConfigPath("").ok());
    ASSERT_FALSE(config.SetStorageConfigPath("./milvus").ok());
    ASSERT_FALSE(config.SetStorageConfigPath("../milvus").ok());
    ASSERT_FALSE(config.SetStorageConfigPath("/**milvus").ok());
    ASSERT_FALSE(config.SetStorageConfigPath("/milvus--/path").ok());

    ASSERT_FALSE(config.SetStorageConfigAutoFlushInterval("0.1").ok());

    //    ASSERT_FALSE(config.SetStorageConfigS3Enable("10").ok());
    //
    //    ASSERT_FALSE(config.SetStorageConfigS3Address("127.0.0").ok());
    //
    //    ASSERT_FALSE(config.SetStorageConfigS3Port("100").ok());
    //    ASSERT_FALSE(config.SetStorageConfigS3Port("100000").ok());
    //
    //    ASSERT_FALSE(config.SetStorageConfigS3AccessKey("").ok());
    //
    //    ASSERT_FALSE(config.SetStorageConfigS3SecretKey("").ok());
    //
    //    ASSERT_FALSE(config.SetStorageConfigS3Bucket("").ok());

    /* metric config */
    ASSERT_FALSE(config.SetMetricConfigEnableMonitor("Y").ok());

    ASSERT_FALSE(config.SetMetricConfigAddress("127.0.0").ok());

    ASSERT_FALSE(config.SetMetricConfigPort("0xff").ok());

    /* cache config */
    ASSERT_FALSE(config.SetCacheConfigCpuCacheCapacity("a").ok());
    ASSERT_FALSE(config.SetCacheConfigCpuCacheCapacity("0").ok());
    ASSERT_FALSE(config.SetCacheConfigCpuCacheCapacity("2048G").ok());
    ASSERT_FALSE(config.SetCacheConfigCpuCacheCapacity("-1").ok());

    ASSERT_FALSE(config.SetCacheConfigCpuCacheThreshold("a").ok());
    ASSERT_FALSE(config.SetCacheConfigCpuCacheThreshold("1.0").ok());
    ASSERT_FALSE(config.SetCacheConfigCpuCacheThreshold("-0.1").ok());

    ASSERT_FALSE(config.SetCacheConfigInsertBufferSize("a").ok());
    ASSERT_FALSE(config.SetCacheConfigInsertBufferSize("0").ok());
    ASSERT_FALSE(config.SetCacheConfigInsertBufferSize("2048GB").ok());
    ASSERT_FALSE(config.SetCacheConfigInsertBufferSize("-1").ok());

    ASSERT_FALSE(config.SetCacheConfigCacheInsertData("N").ok());

    /* engine config */
    ASSERT_FALSE(config.SetEngineConfigUseBlasThreshold("0xff").ok());

    ASSERT_FALSE(config.SetEngineConfigOmpThreadNum("a").ok());
    ASSERT_FALSE(config.SetEngineConfigOmpThreadNum("10000").ok());
    ASSERT_FALSE(config.SetEngineConfigOmpThreadNum("-10").ok());

    ASSERT_FALSE(config.SetEngineConfigSimdType("None").ok());

#ifdef MILVUS_GPU_VERSION
    ASSERT_FALSE(config.SetGpuResourceConfigGpuSearchThreshold("-1").ok());
#endif

    /* gpu resource config */
#ifdef MILVUS_GPU_VERSION
    ASSERT_FALSE(config.SetGpuResourceConfigEnable("ok").ok());

    ASSERT_FALSE(config.SetGpuResourceConfigCacheCapacity("a").ok());
    ASSERT_FALSE(config.SetGpuResourceConfigCacheCapacity("128GB").ok());
    ASSERT_FALSE(config.SetGpuResourceConfigCacheCapacity("-1").ok());

    ASSERT_FALSE(config.SetGpuResourceConfigCacheThreshold("a").ok());
    ASSERT_FALSE(config.SetGpuResourceConfigCacheThreshold("1.0").ok());
    ASSERT_FALSE(config.SetGpuResourceConfigCacheThreshold("-0.1").ok());

    ASSERT_FALSE(config.SetGpuResourceConfigSearchResources("gpu10").ok());
    ASSERT_FALSE(config.SetGpuResourceConfigSearchResources("gpu0, gpu0").ok());

    ASSERT_FALSE(config.SetGpuResourceConfigBuildIndexResources("gup2").ok());
    ASSERT_FALSE(config.SetGpuResourceConfigBuildIndexResources("gpu16").ok());
    ASSERT_FALSE(config.SetGpuResourceConfigBuildIndexResources("gpu0, gpu0, gpu1").ok());
#endif

    /* wal config */
    ASSERT_FALSE(config.SetWalConfigWalPath("hello/world").ok());
    ASSERT_FALSE(config.SetWalConfigWalPath("").ok());
    ASSERT_FALSE(config.SetWalConfigBufferSize("-1").ok());
    ASSERT_FALSE(config.SetWalConfigBufferSize("a").ok());

    /* log config */
    ASSERT_FALSE(config.SetLogsLevel("invalid").ok());
    ASSERT_FALSE(config.SetLogsTraceEnable("invalid").ok());
    ASSERT_FALSE(config.SetLogsPath("").ok());
    ASSERT_FALSE(config.SetLogsMaxLogFileSize("-1").ok());
    ASSERT_FALSE(config.SetLogsMaxLogFileSize("511MB").ok());
    ASSERT_FALSE(config.SetLogsLogRotateNum("-1").ok());
    ASSERT_FALSE(config.SetLogsLogRotateNum("1025").ok());
    ASSERT_FALSE(config.SetLogsLogToStdout("invalid").ok());
    ASSERT_FALSE(config.SetLogsLogToFile("invalid").ok());
}

TEST_F(ConfigTest, SERVER_CONFIG_TEST) {
    std::string config_path(CONFIG_PATH);
    milvus::server::Config& config = milvus::server::Config::GetInstance();

    ASSERT_TRUE(config.LoadConfigFile(config_path + VALID_CONFIG_FILE).ok());

    ASSERT_TRUE(config.ValidateConfig().ok());

    std::string config_json_str;
    config.GetConfigJsonStr(config_json_str);
    std::cout << config_json_str << std::endl;

    auto s = config.ResetDefaultConfig();
    ASSERT_TRUE(s.ok()) << s.message();
}

TEST_F(ConfigTest, SERVER_CONFIG_VALID_FAIL_TEST) {
    fiu_init(0);

    std::string config_path(CONFIG_PATH);
    milvus::server::Config& config = milvus::server::Config::GetInstance();
    milvus::Status s = config.LoadConfigFile(config_path + VALID_CONFIG_FILE);
    ASSERT_TRUE(s.ok());

    fiu_enable("check_config_version_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_version_fail");

    /* server config */
    fiu_enable("check_config_bind_address_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_bind_address_fail");

    fiu_enable("check_config_bind_port_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_bind_port_fail");

    fiu_enable("check_config_cluster_role_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_cluster_role_fail");

    fiu_enable("check_config_timezone_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_timezone_fail");

    /* db config */
    fiu_enable("check_config_path_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_path_fail");

    fiu_enable("check_config_meta_uri_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_meta_uri_fail");

    fiu_enable("check_config_archive_disk_threshold_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_archive_disk_threshold_fail");

    fiu_enable("check_config_archive_days_threshold_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_archive_days_threshold_fail");

    fiu_enable("check_config_insert_buffer_size_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_insert_buffer_size_fail");

    /* metric config */

    fiu_enable("check_config_enable_monitor_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_enable_monitor_fail");

    /* cache config */
    fiu_enable("check_config_cache_size_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_cache_size_fail");

    fiu_enable("check_config_cpu_cache_threshold_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_cpu_cache_threshold_fail");

    fiu_enable("check_config_cache_insert_data_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_cache_insert_data_fail");

    /* engine config */
    fiu_enable("check_config_use_blas_threshold_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_use_blas_threshold_fail");

    fiu_enable("check_config_omp_thread_num_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_omp_thread_num_fail");

    fiu_enable("check_config_simd_type_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_simd_type_fail");

#ifdef MILVUS_GPU_VERSION
    fiu_enable("check_config_gpu_search_threshold_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_gpu_search_threshold_fail");

    fiu_enable("check_config_gpu_resource_enable_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_gpu_resource_enable_fail");

    fiu_enable("check_gpu_cache_size_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_gpu_cache_size_fail");

    fiu_enable("check_config_gpu_resource_cache_threshold_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_gpu_resource_cache_threshold_fail");

    fiu_enable("check_gpu_search_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_gpu_search_fail");

    fiu_enable("check_gpu_build_index_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_gpu_build_index_fail");
#endif

    fiu_enable("get_config_json_config_path_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("get_config_json_config_path_fail");

    s = config.ValidateConfig();
    ASSERT_TRUE(s.ok());

#ifdef MILVUS_GPU_VERSION
    std::vector<int64_t> empty_value;
    fiu_enable("check_config_gpu_resource_enable_fail", 1, NULL, 0);
    empty_value.clear();
    s = config.GetGpuResourceConfigBuildIndexResources(empty_value);
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_gpu_resource_enable_fail");

    fiu_enable("get_gpu_config_build_index_resources.disable_gpu_resource_fail", 1, NULL, 0);
    empty_value.clear();
    s = config.GetGpuResourceConfigBuildIndexResources(empty_value);
    ASSERT_FALSE(s.ok());
    fiu_disable("get_gpu_config_build_index_resources.disable_gpu_resource_fail");

    fiu_enable("get_gpu_config_search_resources.disable_gpu_resource_fail", 1, NULL, 0);
    empty_value.clear();
    s = config.GetGpuResourceConfigSearchResources(empty_value);
    ASSERT_FALSE(s.ok());
    fiu_disable("get_gpu_config_search_resources.disable_gpu_resource_fail");

    fiu_enable("check_config_gpu_resource_enable_fail", 1, NULL, 0);
    empty_value.clear();
    s = config.GetGpuResourceConfigSearchResources(empty_value);
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_gpu_resource_enable_fail");

    int64_t value;
    fiu_enable("check_config_gpu_resource_enable_fail", 1, NULL, 0);
    s = config.GetGpuResourceConfigCacheCapacity(value);
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_gpu_resource_enable_fail");

    fiu_enable("Config.GetGpuResourceConfigCacheCapacity.diable_gpu_resource", 1, NULL, 0);
    s = config.GetGpuResourceConfigCacheCapacity(value);
    ASSERT_FALSE(s.ok());
    fiu_disable("Config.GetGpuResourceConfigCacheCapacity.diable_gpu_resource");

    fiu_enable("ValidationUtil.GetGpuMemory.return_error", 1, NULL, 0);
    s = config.GetGpuResourceConfigCacheCapacity(value);
    ASSERT_FALSE(s.ok());
    fiu_disable("ValidationUtil.GetGpuMemory.return_error");

    // fiu_enable("check_config_insert_buffer_size_fail", 1, NULL, 0);
    // s = config.GetCacheConfigCpuCacheCapacity(value);
    // ASSERT_FALSE(s.ok());
    // fiu_disable("check_config_insert_buffer_size_fail");

    fiu_enable("Config.CheckCacheConfigCpuCacheCapacity.large_insert_buffer", 1, NULL, 0);
    s = config.GetCacheConfigCpuCacheCapacity(value);
    ASSERT_FALSE(s.ok());
    fiu_disable("Config.CheckCacheConfigCpuCacheCapacity.large_insert_buffer");

    float f_value;
    fiu_enable("check_config_gpu_resource_enable_fail", 1, NULL, 0);
    s = config.GetGpuResourceConfigCacheThreshold(f_value);
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_gpu_resource_enable_fail");

    fiu_enable("Config.GetGpuResourceConfigCacheThreshold.diable_gpu_resource", 1, NULL, 0);
    s = config.GetGpuResourceConfigCacheThreshold(f_value);
    ASSERT_FALSE(s.ok());
    fiu_disable("Config.GetGpuResourceConfigCacheThreshold.diable_gpu_resource");
#endif

    /* wal config */
    fiu_enable("check_config_wal_enable_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_wal_enable_fail");

    fiu_enable("check_config_wal_recovery_error_ignore_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_wal_recovery_error_ignore_fail");

    fiu_enable("check_config_wal_buffer_size_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_wal_buffer_size_fail");

    fiu_enable("check_wal_path_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_wal_path_fail");

    /* logs config */
    fiu_enable("check_logs_level_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_logs_level_fail");

    fiu_enable("check_logs_trace_enable_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_logs_trace_enable_fail");

    fiu_enable("check_logs_path_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_logs_path_fail");

    fiu_enable("check_logs_max_log_file_size_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_logs_max_log_file_size_fail");

    fiu_enable("check_logs_log_rotate_num_fail", 1, NULL, 0);
    s = config.ValidateConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_logs_log_rotate_num_fail");
}

TEST_F(ConfigTest, SERVER_CONFIG_RESET_DEFAULT_CONFIG_FAIL_TEST) {
    fiu_init(0);

    std::string config_path(CONFIG_PATH);
    milvus::server::Config& config = milvus::server::Config::GetInstance();
    milvus::Status s = config.LoadConfigFile(config_path + VALID_CONFIG_FILE);
    ASSERT_TRUE(s.ok());

    s = config.ValidateConfig();
    ASSERT_TRUE(s.ok());

    /* server config */
    fiu_enable("check_config_bind_address_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_bind_address_fail");

    fiu_enable("check_config_bind_port_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_bind_port_fail");

    fiu_enable("check_config_cluster_role_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_cluster_role_fail");

    fiu_enable("check_config_timezone_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_timezone_fail");

    /* db config */
    fiu_enable("check_config_path_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_path_fail");

    fiu_enable("check_config_meta_uri_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_meta_uri_fail");

    fiu_enable("check_config_preload_collection_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_preload_collection_fail");

    fiu_enable("check_config_archive_disk_threshold_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_archive_disk_threshold_fail");

    fiu_enable("check_config_archive_days_threshold_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_archive_days_threshold_fail");

    fiu_enable("check_config_auto_flush_interval_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_auto_flush_interval_fail");

    fiu_enable("check_config_insert_buffer_size_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_insert_buffer_size_fail");

    /* metric config */

    fiu_enable("check_config_enable_monitor_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_enable_monitor_fail");

    /* cache config */
    fiu_enable("check_config_cache_size_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_cache_size_fail");

    fiu_enable("check_config_cpu_cache_threshold_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_cpu_cache_threshold_fail");

    fiu_enable("check_config_cache_insert_data_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_cache_insert_data_fail");

    /* engine config */
    fiu_enable("check_config_use_blas_threshold_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_use_blas_threshold_fail");

    fiu_enable("check_config_omp_thread_num_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_omp_thread_num_fail");

    fiu_enable("check_config_simd_type_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_simd_type_fail");

#ifdef MILVUS_GPU_VERSION
    fiu_enable("check_config_gpu_search_threshold_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_gpu_search_threshold_fail");

    fiu_enable("check_config_gpu_resource_enable_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_gpu_resource_enable_fail");

    fiu_enable("check_gpu_cache_size_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_gpu_cache_size_fail");

    fiu_enable("check_config_gpu_resource_cache_threshold_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_gpu_resource_cache_threshold_fail");

    fiu_enable("check_gpu_search_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_gpu_search_fail");

    fiu_enable("check_gpu_build_index_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_gpu_build_index_fail");
#endif

    s = config.ResetDefaultConfig();
    ASSERT_TRUE(s.ok());

    /* wal config */
    fiu_enable("check_config_wal_enable_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_wal_enable_fail");

    fiu_enable("check_config_wal_recovery_error_ignore_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_wal_recovery_error_ignore_fail");

    fiu_enable("check_config_wal_buffer_size_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_config_wal_buffer_size_fail");

    fiu_enable("check_wal_path_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_wal_path_fail");

    /* logs config */
    fiu_enable("check_logs_level_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_logs_level_fail");

    fiu_enable("check_logs_trace_enable_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_logs_trace_enable_fail");

    fiu_enable("check_logs_path_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_logs_path_fail");

    fiu_enable("check_logs_max_log_file_size_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_logs_max_log_file_size_fail");

    fiu_enable("check_logs_log_rotate_num_fail", 1, NULL, 0);
    s = config.ResetDefaultConfig();
    ASSERT_FALSE(s.ok());
    fiu_disable("check_logs_log_rotate_num_fail");
}

TEST_F(ConfigTest, SERVER_CONFIG_OTHER_CONFIGS_FAIL_TEST) {
    fiu_init(0);

    std::string config_path(CONFIG_PATH);
    milvus::server::Config& config = milvus::server::Config::GetInstance();
    milvus::Status s;

    std::string get_cmd, set_cmd;
    std::string result, dummy;

    s = config.ProcessConfigCli(result, "get_config");
    ASSERT_FALSE(s.ok());

    s = config.ProcessConfigCli(result, "get_config");
    ASSERT_FALSE(s.ok());

    s = config.ProcessConfigCli(result, "get_config 1");
    ASSERT_FALSE(s.ok());

    s = config.ProcessConfigCli(result, "get_config 1.1");
    ASSERT_FALSE(s.ok());

    std::string build_index_resources = "gpu0";

    set_cmd =
        gen_set_command(ms::CONFIG_GPU_RESOURCE, ms::CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES, build_index_resources);
    std::cout << set_cmd << std::endl;

    s = config.ProcessConfigCli(dummy, set_cmd + " invalid");
    ASSERT_FALSE(s.ok());

    s = config.ProcessConfigCli(dummy, "set_config gpu_resource_config.build_index_resources.sss gpu0");
    ASSERT_FALSE(s.ok());

    s = config.ProcessConfigCli(dummy, "set_config gpu_resource_config_invalid.build_index_resources_invalid gpu0");
    ASSERT_FALSE(s.ok());

    s = config.ProcessConfigCli(dummy, "invalid_config gpu_resource_config.build_index_resources.sss gpu0");
    ASSERT_FALSE(s.ok());

#ifndef MILVUS_GPU_VERSION
    s = config.ProcessConfigCli(
        dummy,
        gen_set_command(ms::CONFIG_TRACING, ms::CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES, build_index_resources));
    ASSERT_FALSE(s.ok());
#endif
}

TEST_F(ConfigTest, SEARCH_ROW_CONFIG_TEST) {
    std::string conf_file = std::string(CONFIG_PATH) + VALID_CONFIG_FILE;
    milvus::server::Config& config = milvus::server::Config::GetInstance();

    config.ClearCache();
    auto status = config.LoadConfigFile(conf_file);
    ASSERT_TRUE(status.ok()) << status.message();

    std::string config_json_str;
    config.GetConfigJsonStr(config_json_str);
    std::cout << config_json_str << std::endl;

    bool is_search_row;
    config.GetGeneralConfigSearchRawEnable(is_search_row);
    ASSERT_FALSE(is_search_row);

    config.SetGeneralConfigSearchRawEnable("true");
    config.GetGeneralConfigSearchRawEnable(is_search_row);
    ASSERT_TRUE(is_search_row);

    status = config.ResetDefaultConfig();
    ASSERT_TRUE(status.ok()) << status.message();
    config.GetGeneralConfigSearchRawEnable(is_search_row);
    ASSERT_TRUE(is_search_row);
}

TEST_F(ConfigTest, SERVER_CONFIG_UPDATE_TEST) {
    std::string conf_file = std::string(CONFIG_PATH) + VALID_CONFIG_FILE;
    std::string yaml_value;
    std::string reply_set, reply_get;
    std::string cmd_set, cmd_get;

    auto lambda = [&conf_file](const std::string& key, const std::string& child_key, const std::string& default_value,
                               std::string& value) {
        auto* ymgr = milvus::server::YamlConfigMgr::GetInstance();
        auto status = ymgr->LoadConfigFile(conf_file);

        if (status.ok())
            value = ymgr->GetRootNode().GetChild(key).GetValue(child_key, default_value);

        return status;
    };

    milvus::server::Config& config = milvus::server::Config::GetInstance();

    auto status = config.LoadConfigFile(conf_file);
    ASSERT_TRUE(status.ok()) << status.message();

    /* validate if setting config store in files */
    // test numeric config value
    cmd_set = gen_set_command(ms::CONFIG_CACHE, ms::CONFIG_CACHE_INSERT_BUFFER_SIZE, "2");
    ASSERT_TRUE(config.ProcessConfigCli(reply_set, cmd_set).ok());

    ASSERT_TRUE(lambda(ms::CONFIG_CACHE, ms::CONFIG_CACHE_INSERT_BUFFER_SIZE,
                       ms::CONFIG_CACHE_INSERT_BUFFER_SIZE_DEFAULT, yaml_value)
                    .ok());
    ASSERT_EQ("2", yaml_value);

    // test boolean config value
    cmd_set = gen_set_command(ms::CONFIG_METRIC, ms::CONFIG_METRIC_ENABLE_MONITOR, "True");
    ASSERT_TRUE(config.ProcessConfigCli(reply_set, cmd_set).ok());
    ASSERT_TRUE(lambda(ms::CONFIG_METRIC, ms::CONFIG_METRIC_ENABLE_MONITOR, ms::CONFIG_METRIC_ENABLE_MONITOR_DEFAULT,
                       yaml_value)
                    .ok());
    ASSERT_EQ("true", yaml_value);

    cmd_set = gen_set_command(ms::CONFIG_METRIC, ms::CONFIG_METRIC_ENABLE_MONITOR, "On");
    ASSERT_TRUE(config.ProcessConfigCli(reply_set, cmd_set).ok());
    ASSERT_TRUE(lambda(ms::CONFIG_METRIC, ms::CONFIG_METRIC_ENABLE_MONITOR, ms::CONFIG_METRIC_ENABLE_MONITOR_DEFAULT,
                       yaml_value)
                    .ok());
    ASSERT_EQ("true", yaml_value);

    cmd_set = gen_set_command(ms::CONFIG_METRIC, ms::CONFIG_METRIC_ENABLE_MONITOR, "False");
    ASSERT_TRUE(config.ProcessConfigCli(reply_set, cmd_set).ok());
    ASSERT_TRUE(lambda(ms::CONFIG_METRIC, ms::CONFIG_METRIC_ENABLE_MONITOR, ms::CONFIG_METRIC_ENABLE_MONITOR_DEFAULT,
                       yaml_value)
                    .ok());
    ASSERT_EQ("false", yaml_value);

    cmd_set = gen_set_command(ms::CONFIG_METRIC, ms::CONFIG_METRIC_ENABLE_MONITOR, "Off");
    ASSERT_TRUE(config.ProcessConfigCli(reply_set, cmd_set).ok());
    ASSERT_TRUE(lambda(ms::CONFIG_METRIC, ms::CONFIG_METRIC_ENABLE_MONITOR, ms::CONFIG_METRIC_ENABLE_MONITOR_DEFAULT,
                       yaml_value)
                    .ok());
    ASSERT_EQ("false", yaml_value);

    // test path
    cmd_set = gen_set_command(ms::CONFIG_STORAGE, ms::CONFIG_STORAGE_PATH, "/tmp/milvus_config_unittest");
    ASSERT_TRUE(config.ProcessConfigCli(reply_set, cmd_set).ok());
    ASSERT_TRUE(lambda(ms::CONFIG_STORAGE, ms::CONFIG_STORAGE_PATH, ms::CONFIG_STORAGE_PATH_DEFAULT, yaml_value).ok());
    ASSERT_EQ("/tmp/milvus_config_unittest", yaml_value);

#ifdef MILVUS_GPU_VERSION
    cmd_set = gen_set_command(ms::CONFIG_GPU_RESOURCE, ms::CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES, "gpu0");
    ASSERT_TRUE(config.ProcessConfigCli(reply_set, cmd_set).ok());
    ASSERT_TRUE(lambda(ms::CONFIG_GPU_RESOURCE, ms::CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES,
                       ms::CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES_DEFAULT, yaml_value)
                    .ok());
    ASSERT_EQ("gpu0", yaml_value);

    cmd_set = gen_set_command(ms::CONFIG_GPU_RESOURCE, ms::CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES, "GPU0");
    ASSERT_TRUE(config.ProcessConfigCli(reply_set, cmd_set).ok());
    ASSERT_TRUE(lambda(ms::CONFIG_GPU_RESOURCE, ms::CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES,
                       ms::CONFIG_GPU_RESOURCE_BUILD_INDEX_RESOURCES_DEFAULT, yaml_value)
                    .ok());
    ASSERT_EQ("gpu0", yaml_value);
#endif
}
