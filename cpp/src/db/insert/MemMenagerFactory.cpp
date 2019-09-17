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

#include "MemMenagerFactory.h"
#include "MemManagerImpl.h"
#include "utils/Log.h"
#include "utils/Exception.h"

#include <stdlib.h>
#include <time.h>
#include <sstream>
#include <cstdlib>
#include <string>
#include <regex>

namespace zilliz {
namespace milvus {
namespace engine {

MemManagerPtr MemManagerFactory::Build(const std::shared_ptr<meta::Meta>& meta,
                                        const Options& options) {
    return std::make_shared<MemManagerImpl>(meta, options);
}

} // namespace engine
} // namespace milvus
} // namespace zilliz
