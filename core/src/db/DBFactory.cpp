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

#include "db/DBFactory.h"
#include "DBImpl.h"
#include "SSDBImpl.h"
#include "meta/MetaFactory.h"
#include "meta/MySQLMetaImpl.h"
#include "meta/SqliteMetaImpl.h"
#include "utils/Exception.h"

#include <stdlib.h>
#include <time.h>
#include <cstdlib>
#include <sstream>
#include <string>

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
DBFactory::Build(const DBOptions& options) {
    return std::make_shared<DBImpl>(options);
}

SSDBPtr
DBFactory::BuildSSDB(const DBOptions& options) {
    return std::make_shared<SSDBImpl>(options);
}

}  // namespace engine
}  // namespace milvus
