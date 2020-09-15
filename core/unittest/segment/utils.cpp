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

#include <fstream>
#include <string>

#include "segment/utils.h"
#include "config/ServerConfig.h"
#include "storage/s3/S3ClientWrapper.h"

void
SegmentTest::SetUp() {
    bool s3_enable = milvus::config.storage.s3_enable();
    if (s3_enable) {
        ASSERT_TRUE(milvus::storage::S3ClientWrapper::GetInstance().StartService().ok());
    }
}

void
SegmentTest::TearDown() {
    bool s3_enable = milvus::config.storage.s3_enable();
    if (s3_enable) {
        milvus::storage::S3ClientWrapper::GetInstance().StopService();
    }
}
