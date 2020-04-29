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

#include <boost/filesystem.hpp>

#include <fiu-control.h>
#include <fiu-local.h>
#include <gtest/gtest.h>

#include "config/Config.h"
#include "server/init/CpuChecker.h"
#ifdef MILVUS_GPU_VERSION
#include "server/init/GpuChecker.h"
#endif
#include "server/init/StorageChecker.h"

namespace ms = milvus::server;

class ServerCheckerTest : public testing::Test {
 protected:
    void
    SetUp() override {
        auto& config = ms::Config::GetInstance();

        logs_path = "/tmp/milvus-test/logs";
        boost::filesystem::create_directories(logs_path);
        config.SetLogsPath(logs_path);

        db_primary_path = "/tmp/milvus-test/db";
        boost::filesystem::create_directories(db_primary_path);
        config.SetStorageConfigPrimaryPath(db_primary_path);

        db_secondary_path = "/tmp/milvus-test/db-secondary";
        boost::filesystem::create_directories(db_secondary_path);
        config.SetStorageConfigSecondaryPath(db_secondary_path);

        wal_path = "/tmp/milvus-test/wal";
        boost::filesystem::create_directories(wal_path);
        config.SetWalConfigWalPath(wal_path);
    }

    void
    TearDown() override {
        boost::filesystem::remove_all(logs_path);
        boost::filesystem::remove_all(db_primary_path);
        boost::filesystem::remove_all(db_secondary_path);
        boost::filesystem::remove_all(wal_path);
    }

 protected:
    std::string logs_path;
    std::string db_primary_path;
    std::string db_secondary_path;
    std::string wal_path;
};

TEST_F(ServerCheckerTest, STORAGE_TEST) {
    auto status = ms::StorageChecker::CheckStoragePermission();
    ASSERT_TRUE(status.ok()) << status.message();
}

TEST_F(ServerCheckerTest, STORAGE_FAIL_TEST) {
    fiu_enable("StorageChecker.CheckStoragePermission.logs_path_access_fail", 1, NULL, 0);
    ASSERT_FALSE(ms::StorageChecker::CheckStoragePermission().ok());
    fiu_disable("StorageChecker.CheckStoragePermission.logs_path_access_fail");

    fiu_enable("StorageChecker.CheckStoragePermission.db_primary_path_access_fail", 1, NULL, 0);
    ASSERT_FALSE(ms::StorageChecker::CheckStoragePermission().ok());
    fiu_disable("StorageChecker.CheckStoragePermission.db_primary_path_access_fail");

    auto& config = ms::Config::GetInstance();
    std::string storage_secondary_path;
    ASSERT_TRUE(config.GetStorageConfigSecondaryPath(storage_secondary_path).ok());
    ASSERT_TRUE(config.SetStorageConfigSecondaryPath("/tmp/milvus-test01,/tmp/milvus-test02").ok());
    fiu_enable("StorageChecker.CheckStoragePermission.db_secondary_path_access_fail", 1, NULL, 0);
    ASSERT_FALSE(ms::StorageChecker::CheckStoragePermission().ok());
    fiu_disable("StorageChecker.CheckStoragePermission.db_secondary_path_access_fail");
    ASSERT_TRUE(config.SetStorageConfigSecondaryPath(storage_secondary_path).ok());

    fiu_enable("StorageChecker.CheckStoragePermission.wal_path_access_fail", 1, NULL, 0);
    ASSERT_FALSE(ms::StorageChecker::CheckStoragePermission().ok());
    fiu_disable("StorageChecker.CheckStoragePermission.wal_path_access_fail");
}

TEST_F(ServerCheckerTest, CPU_TEST) {
    auto status = ms::CpuChecker::CheckCpuInstructionSet();
    ASSERT_TRUE(status.ok()) << status.message();
}

TEST_F(ServerCheckerTest, CPU_FAIL_TEST) {
    fiu_enable("CpuChecker.CheckCpuInstructionSet.instruction_sets_empty", 1, NULL, 0);
    ASSERT_FALSE(ms::CpuChecker::CheckCpuInstructionSet().ok());
    fiu_disable("CpuChecker.CheckCpuInstructionSet.instruction_sets_empty");

    fiu_enable("CpuChecker.CheckCpuInstructionSet.not_support_avx512", 1, NULL, 0);
    // CPU not support avx512, but avx2 and sse4_2 support
    ASSERT_TRUE(ms::CpuChecker::CheckCpuInstructionSet().ok());

    // CPU only support sse4_2
    fiu_enable("CpuChecker.CheckCpuInstructionSet.not_support_avx2", 1, NULL, 0);
    ASSERT_TRUE(ms::CpuChecker::CheckCpuInstructionSet().ok());

    // CPU not support one of sse4_2, avx2, avx512
    fiu_enable("CpuChecker.CheckCpuInstructionSet.not_support_sse4_2", 1, NULL, 0);
    ASSERT_FALSE(ms::CpuChecker::CheckCpuInstructionSet().ok());

    fiu_disable("CpuChecker.CheckCpuInstructionSet.not_support_sse4_2");
    fiu_disable("CpuChecker.CheckCpuInstructionSet.not_support_avx2");
    fiu_disable("CpuChecker.CheckCpuInstructionSet.not_support_avx512");
}

#ifdef MILVUS_GPU_VERSION
TEST_F(ServerCheckerTest, GPU_TEST) {
    auto& config = ms::Config::GetInstance();
    auto status = config.SetGpuResourceConfigEnable("true");
    ASSERT_TRUE(status.ok()) << status.message();

    status = ms::GpuChecker::CheckGpuEnvironment();
    ASSERT_TRUE(status.ok()) << status.message();

    status = config.SetGpuResourceConfigEnable("false");
    ASSERT_TRUE(status.ok()) << status.message();

    status = ms::GpuChecker::CheckGpuEnvironment();
    ASSERT_TRUE(status.ok()) << status.message();
}

TEST_F(ServerCheckerTest, GPU_FAIL_TEST) {
    auto& config = ms::Config::GetInstance();
    auto status = config.SetGpuResourceConfigEnable("true");
    ASSERT_TRUE(status.ok()) << status.message();

    fiu_enable("GpuChecker.CheckGpuEnvironment.nvml_init_fail", 1, NULL, 0);
    ASSERT_FALSE(ms::GpuChecker::CheckGpuEnvironment().ok());
    fiu_disable("GpuChecker.CheckGpuEnvironment.nvml_init_fail");

    fiu_enable("GpuChecker.CheckGpuEnvironment.get_nvidia_driver_fail", 1, NULL, 0);
    ASSERT_FALSE(ms::GpuChecker::CheckGpuEnvironment().ok());
    fiu_disable("GpuChecker.CheckGpuEnvironment.get_nvidia_driver_fail");

    fiu_enable("GpuChecker.CheckGpuEnvironment.nvidia_driver_too_slow", 1, NULL, 0);
    ASSERT_FALSE(ms::GpuChecker::CheckGpuEnvironment().ok());
    fiu_disable("GpuChecker.CheckGpuEnvironment.nvidia_driver_too_slow");

    fiu_enable("GpuChecker.CheckGpuEnvironment.cuda_driver_fail", 1, NULL, 0);
    ASSERT_FALSE(ms::GpuChecker::CheckGpuEnvironment().ok());
    fiu_disable("GpuChecker.CheckGpuEnvironment.cuda_driver_fail");

    fiu_enable("GpuChecker.CheckGpuEnvironment.cuda_driver_too_slow", 1, NULL, 0);
    ASSERT_FALSE(ms::GpuChecker::CheckGpuEnvironment().ok());
    fiu_disable("GpuChecker.CheckGpuEnvironment.cuda_driver_too_slow");

    fiu_enable("GpuChecker.CheckGpuEnvironment.cuda_runtime_driver_fail", 1, NULL, 0);
    ASSERT_FALSE(ms::GpuChecker::CheckGpuEnvironment().ok());
    fiu_disable("GpuChecker.CheckGpuEnvironment.cuda_runtime_driver_fail");

    fiu_enable("GpuChecker.CheckGpuEnvironment.cuda_runtime_driver_too_slow", 1, NULL, 0);
    ASSERT_FALSE(ms::GpuChecker::CheckGpuEnvironment().ok());
    fiu_disable("GpuChecker.CheckGpuEnvironment.cuda_runtime_driver_too_slow");

    fiu_enable("GpuChecker.CheckGpuEnvironment.nvml_get_device_count_fail", 1, NULL, 0);
    ASSERT_FALSE(ms::GpuChecker::CheckGpuEnvironment().ok());
    fiu_disable("GpuChecker.CheckGpuEnvironment.nvml_get_device_count_fail");

    fiu_enable("GpuChecker.CheckGpuEnvironment.nvml_device_count_zero", 1, NULL, 0);
    ASSERT_FALSE(ms::GpuChecker::CheckGpuEnvironment().ok());
    fiu_disable("GpuChecker.CheckGpuEnvironment.nvml_device_count_zero");

    fiu_enable("GpuChecker.CheckGpuEnvironment.nvml_get_device_handle_fail", 1, NULL, 0);
    ASSERT_FALSE(ms::GpuChecker::CheckGpuEnvironment().ok());
    fiu_disable("GpuChecker.CheckGpuEnvironment.nvml_get_device_handle_fail");

    fiu_enable("GpuChecker.CheckGpuEnvironment.nvml_get_device_name_fail", 1, NULL, 0);
    ASSERT_FALSE(ms::GpuChecker::CheckGpuEnvironment().ok());
    fiu_disable("GpuChecker.CheckGpuEnvironment.nvml_get_device_name_fail");

    fiu_enable("GpuChecker.CheckGpuEnvironment.device_compute_capacity_fail", 1, NULL, 0);
    ASSERT_FALSE(ms::GpuChecker::CheckGpuEnvironment().ok());
    fiu_disable("GpuChecker.CheckGpuEnvironment.device_compute_capacity_fail");

    fiu_enable("GpuChecker.CheckGpuEnvironment.device_compute_capacity_too_weak", 1, NULL, 0);
    ASSERT_FALSE(ms::GpuChecker::CheckGpuEnvironment().ok());
    fiu_disable("GpuChecker.CheckGpuEnvironment.device_compute_capacity_too_weak");

    fiu_enable("GpuChecker.CheckGpuEnvironment.nvml_shutdown_fail", 1, NULL, 0);
    ASSERT_FALSE(ms::GpuChecker::CheckGpuEnvironment().ok());
    fiu_disable("GpuChecker.CheckGpuEnvironment.nvml_shutdown_fail");
}

#endif
