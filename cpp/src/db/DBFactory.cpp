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


#include "db/DBFactory.h"
#include "DBImpl.h"
#include "utils/Exception.h"
#include "meta/MetaFactory.h"
#include "meta/SqliteMetaImpl.h"
#include "meta/MySQLMetaImpl.h"

#include <stdlib.h>
#include <time.h>
#include <sstream>
#include <cstdlib>
#include <string>

namespace zilliz {
namespace milvus {
namespace engine {

DBOptions
DBFactory::BuildOption() {
    auto meta = MetaFactory::BuildOption();
    DBOptions options;
    options.meta_ = meta;
    return options;
}

DBPtr
DBFactory::Build(const DBOptions &options) {
    return std::make_shared<DBImpl>(options);
}

} // namespace engine
} // namespace milvus
} // namespace zilliz
