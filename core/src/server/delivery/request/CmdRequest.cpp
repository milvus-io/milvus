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

#include "server/delivery/request/CmdRequest.h"
#include "metrics/SystemInfo.h"
#include "scheduler/SchedInst.h"
#include "utils/Log.h"
#include "utils/TimeRecorder.h"

#include <memory>

namespace milvus {
namespace server {

CmdRequest::CmdRequest(const std::shared_ptr<Context>& context, const std::string& cmd, std::string& result)
    : BaseRequest(context, INFO_REQUEST_GROUP), cmd_(cmd), result_(result) {
}

BaseRequestPtr
CmdRequest::Create(const std::shared_ptr<Context>& context, const std::string& cmd, std::string& result) {
    return std::shared_ptr<BaseRequest>(new CmdRequest(context, cmd, result));
}

Status
CmdRequest::OnExecute() {
    std::string hdr = "CmdRequest(cmd=" + cmd_ + ")";
    TimeRecorderAuto rc(hdr);
    Status stat = Status::OK();

    if (cmd_ == "version") {
        result_ = MILVUS_VERSION;
    } else if (cmd_ == "status") {
        result_ = "OK";
    } else if (cmd_ == "tasktable") {
        result_ = scheduler::ResMgrInst::GetInstance()->DumpTaskTables();
    } else if (cmd_ == "mode") {
#ifdef MILVUS_GPU_VERSION
        result_ = "GPU";
#else
        result_ = "CPU";
#endif
    } else if (cmd_ == "get_system_info") {
        server::SystemInfo& sys_info_inst = server::SystemInfo::GetInstance();
        sys_info_inst.GetSysInfoJsonStr(result_);
    } else if (cmd_ == "build_commit_id") {
        result_ = LAST_COMMIT_ID;
    } else if (cmd_.substr(0, 10) == "set_config" || cmd_.substr(0, 10) == "get_config") {
        server::Config& config = server::Config::GetInstance();
        stat = config.ProcessConfigCli(result_, cmd_);
    } else {
        result_ = "Unknown command";
    }

    return stat;
}

}  // namespace server
}  // namespace milvus
