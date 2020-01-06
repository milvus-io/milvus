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

#include <faiss/IndexFlat.h>
#include <gtest/gtest.h>
#include <string>

#include "utils/CommonUtil.h"
#include "wrapper/utils.h"

namespace {
static const char* CONFIG_STR =
    "# All the following configurations are default values.\n"
    "\n"
    "server_config:\n"
    "  address: 0.0.0.0                  # milvus server ip address (IPv4)\n"
    "  port: 19530                       # port range: 1025 ~ 65534\n"
    "  deploy_mode: single               \n"
    "  time_zone: UTC+8\n"
    "\n"
    "db_config:\n"
    "  backend_url: sqlite://:@:/        # URI format: dialect://username:password@host:port/database\n"
    "                                    \n"
    "                                    # Replace 'dialect' with 'mysql' or 'sqlite'\n"
    "\n"
    "  insert_buffer_size: 4             # GB, maximum insert buffer size allowed\n"
    "\n"
    "storage_config:\n"
    "  primary_path: /tmp/milvus         # path used to store data and meta\n"
    "  secondary_path:                   # path used to store data only, split by semicolon\n"
    "\n"
    "metric_config:\n"
    "  enable_monitor: false             # enable monitoring or not\n"
    "  collector: prometheus             # prometheus\n"
    "  prometheus_config:\n"
    "    port: 8080                      # port prometheus used to fetch metrics\n"
    "\n"
    "cache_config:\n"
    "  cpu_mem_capacity: 16              # GB, CPU memory used for cache\n"
    "  cpu_mem_threshold: 0.85           # percentage of data kept when cache cleanup triggered\n"
    "  cache_insert_data: false          # whether load inserted data into cache\n"
    "\n"
    "engine_config:\n"
    "  blas_threshold: 20\n"
    "\n"
#ifdef MILVUS_GPU_VERSION
    "gpu_resource_config:\n"
    "  enable: true                      # whether to enable GPU resources\n"
    "  cache_capacity: 4                 # GB, size of GPU memory per card used for cache, must be a positive integer\n"
    "  search_resources:                 # define the GPU devices used for search computation, must be in format gpux\n"
    "    - gpu0\n"
    "  build_index_resources:            # define the GPU devices used for index building, must be in format gpux\n"
    "    - gpu0\n"
#endif
    "\n";

void
WriteToFile(const std::string& file_path, const char* content) {
    std::fstream fs(file_path.c_str(), std::ios_base::out);

    // write data to file
    fs << content;
    fs.close();
}

}  // namespace

void
KnowhereTest::SetUp() {
    std::string config_path(CONFIG_PATH);
    milvus::server::CommonUtil::CreateDirectory(config_path);
    WriteToFile(config_path + CONFIG_FILE, CONFIG_STR);
}

void
KnowhereTest::TearDown() {
    std::string config_path(CONFIG_PATH);
    milvus::server::CommonUtil::DeleteDirectory(config_path);
}

void
DataGenBase::GenData(const int& dim, const int& nb, const int& nq, float* xb, float* xq, int64_t* ids, const int& k,
                     int64_t* gt_ids, float* gt_dis) {
    for (auto i = 0; i < nb; ++i) {
        for (auto j = 0; j < dim; ++j) {
            // p_data[i * d + j] = float(base + i);
            xb[i * dim + j] = drand48();
        }
        xb[dim * i] += i / 1000.;
        ids[i] = i;
    }
    for (size_t i = 0; i < nq * dim; ++i) {
        xq[i] = xb[i];
    }

    faiss::IndexFlatL2 index(dim);
    // index.add_with_ids(nb, xb, ids);
    index.add(nb, xb);
    index.search(nq, xq, k, gt_dis, gt_ids);
}

void
DataGenBase::GenData(const int& dim, const int& nb, const int& nq, std::vector<float>& xb, std::vector<float>& xq,
                     std::vector<int64_t>& ids, const int& k, std::vector<int64_t>& gt_ids,
                     std::vector<float>& gt_dis) {
    xb.clear();
    xq.clear();
    ids.clear();
    gt_ids.clear();
    gt_dis.clear();
    xb.resize(nb * dim);
    xq.resize(nq * dim);
    ids.resize(nb);
    gt_ids.resize(nq * k);
    gt_dis.resize(nq * k);
    GenData(dim, nb, nq, xb.data(), xq.data(), ids.data(), k, gt_ids.data(), gt_dis.data());
}

void
DataGenBase::AssertResult(const std::vector<int64_t>& ids, const std::vector<float>& dis) {
    EXPECT_EQ(ids.size(), nq * k);
    EXPECT_EQ(dis.size(), nq * k);

    for (auto i = 0; i < nq; i++) {
        EXPECT_EQ(ids[i * k], gt_ids[i * k]);
        // EXPECT_EQ(dis[i * k], gt_dis[i * k]);
    }

    int match = 0;
    for (int i = 0; i < nq; ++i) {
        for (int j = 0; j < k; ++j) {
            for (int l = 0; l < k; ++l) {
                if (ids[i * nq + j] == gt_ids[i * nq + l])
                    match++;
            }
        }
    }

    auto precision = float(match) / (nq * k);
    EXPECT_GT(precision, 0.5);
    std::cout << std::endl << "Precision: " << precision << ", match: " << match << ", total: " << nq * k << std::endl;
}
