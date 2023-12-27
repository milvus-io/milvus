// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <unordered_map>
#include <shared_mutex>

#include "segcore/DeletedRecord.h"

namespace milvus::segcore {

class OrderedDeletedRecord {
 public:
    OrderedDeletedRecord() = default;

 public:
    DeletedRecord&
    get_or_generate();

    int64_t
    get_deleted_count() const;

    void
    load(const std::vector<PkType>& pks, const Timestamp* timestamps);

    void
    push(const std::vector<PkType>& pks, const Timestamp* timestamps);

 private:
    void
    generate_unsafe();

    void
    load_unsafe(const std::vector<PkType>& pks, const Timestamp* timestamps);

    void
    push_unsafe(const std::vector<PkType>& pks, const Timestamp* timestamps);

 private:
    mutable std::shared_mutex mtx_;
    DeletedRecord final_record_;
    std::unordered_map<PkType, Timestamp> buffer_;
    bool switched_ = false;
};

}  // namespace milvus::segcore
