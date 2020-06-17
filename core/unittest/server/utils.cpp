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

#include "server/utils.h"
#include "utils/CommonUtil.h"

#include <fstream>
#include <iostream>
#include <string>
#include <thread>

namespace {


static const char* VALID_CONFIG_STR =
    "# Default values are used when you make no changes to the following parameters.\n"
    "\n"
    "version: 0.5\n"
    "\n"
    "cluster:\n"
    "  enable: false\n"
    "  role: rw\n"
    "\n"
    "general:\n"
    "  timezone: UTC+8\n"
    "  meta_uri: sqlite://:@:/\n"
    "\n"
    "network:\n"
    "  bind.address: 0.0.0.0\n"
    "  bind.port: 19530\n"
    "  http.enable: true\n"
    "  http.port: 19121\n"
    "\n"
    "storage:\n"
    "  path: /tmp/milvus\n"
    "  auto_flush_interval: 1\n"
    "\n"
    "wal:\n"
    "  enable: true\n"
    "  recovery_error_ignore: false\n"
    "  buffer_size: 256MB\n"
    "  path: /tmp/milvus/wal\n"
    "\n"
    "cache:\n"
    "  cache_size: 4GB\n"
    "  insert_buffer_size: 1GB\n"
    "  preload_collection:\n"
    "\n"
    "gpu:\n"
    "  enable: true\n"
    "  cache_size: 1GB\n"
    "  gpu_search_threshold: 1000\n"
    "  search_devices:\n"
    "    - gpu0\n"
    "  build_index_devices:\n"
    "    - gpu0\n"
    "\n"
    "logs:\n"
    "  level: debug\n"
    "  trace.enable: true\n"
    "  path: /tmp/milvus/logs\n"
    "  max_log_file_size: 1024MB\n"
    "  log_rotate_num: 0\n"
    "\n"
    "metric:\n"
    "  enable: false\n"
    "  address: 127.0.0.1\n"
    "  port: 9091\n"
    "\n";

static const char* INVALID_CONFIG_STR = "*INVALID*";

void
WriteToFile(const std::string& file_path, const char* content) {
    std::fstream fs(file_path.c_str(), std::ios_base::out);

    // write data to file
    fs << content;
    fs.close();
}

}  // namespace

void
ConfigTest::SetUp() {
    std::string config_path(CONFIG_PATH);
    milvus::server::CommonUtil::CreateDirectory(config_path);
    WriteToFile(config_path + VALID_CONFIG_FILE, VALID_CONFIG_STR);
    WriteToFile(config_path + INVALID_CONFIG_FILE, INVALID_CONFIG_STR);
}

void
ConfigTest::TearDown() {
    std::string config_path(CONFIG_PATH);
    milvus::server::CommonUtil::DeleteDirectory(config_path);
}
