// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <string>

namespace milvus::ChunkMangerConfig {

void
SetAddress(const std::string& address);

std::string
GetAddress();

void
SetAccessKey(const std::string& access_key);

std::string
GetAccessKey();

void
SetAccessValue(const std::string& access_value);

std::string
GetAccessValue();

void
SetUseSSL(bool use_ssl);

bool
GetUseSSL();

void
SetUseIAM(bool use_iam);

bool
GetUseIAM();

void
SetBucketName(const std::string& bucket_name);

std::string
GetBucketName();

void
SetRemoteRootPath(const std::string& path_prefix);

std::string
GetRemoteRootPath();

void
SetLocalRootPath(const std::string& path_prefix);

std::string
GetLocalRootPath();

}  // namespace milvus::ChunkMangerConfig
