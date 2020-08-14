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
#include "db/DBImpl.h"
#include "db/transcript/Transcript.h"
#include "db/wal/WriteAheadLog.h"

namespace milvus {
namespace engine {

DBPtr
DBFactory::BuildDB(const DBOptions& options) {
    DBPtr db = std::make_shared<DBImpl>(options);

    // need wal? wal must be after db
    if (options.wal_enable_) {
        db = std::make_shared<WriteAheadLog>(db, options);
    }

    // need transcript? transcript must be after wal
    if (options.transcript_enable_) {
        db = std::make_shared<Transcript>(db, options);
    }

    return db;
}

}  // namespace engine
}  // namespace milvus
