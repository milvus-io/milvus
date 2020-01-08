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

#pragma once

#include <atomic>
#include <thread>
#include <condition_variable>
//#include <src/sdk/include/MilvusApi.h>
#include "WalDefinations.h"
#include "WalFileHandler.h"
#include "WalMetaHandler.h"
#include "WalBuffer.h"

namespace milvus {
namespace engine {
namespace wal {

class WalManager {
 public:
    WalManager(const meta::MetaPtr& meta);
    ~WalManager();

    void Recovery();
    void Start();
    void Stop();
    //todo: return error code
    bool
    Insert(const std::string &table_id,
           const std::vector<float> &vectors,
           const IDNumbers &vector_ids);
    bool
    Insert(const std::string &table_id,
           const std::vector<uint8_t> vectors,
           const IDNumbers &vector_ids);
    bool
    DeleteById(const std::string& table_id,
               const IDNumbers &vector_ids);
    bool Flush(const std::string& table_id = "");
    void Apply(const uint64_t& apply_lsn);
    void Dispatch(std::string &table_id,
                  MXLogType& mxl_type,
                  size_t &n,
                  size_t &dim,
                  float *vectors,
                  milvus::engine::IDNumbers &vector_ids,
                  const uint64_t& last_applied_lsn,
                  uint64_t &lsn);

    uint64_t GetCurrentLsn();

 private:
    WalManager operator = (WalManager&);

    const meta::MetaPtr& meta_;

    bool is_running_;
    MXLogConfiguration mxlog_config_;
    uint64_t last_applied_lsn_;
    MXLogBufferPtr p_buffer_;
    MXLogMetaHandlerPtr p_meta_handler_;

    std::thread reader_;

};
} // wal
} // engine
} // milvus

