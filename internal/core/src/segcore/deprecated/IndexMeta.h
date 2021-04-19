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

#pragma once
//
//#include <shared_mutex>
//
//#include "common/Schema.h"
// #include "segcore/SegmentBase.h"
#if 0
#include "common/Schema.h"
#include "knowhere/index/IndexType.h"
#include "knowhere/common/Config.h"
#include <string>
#include <map>
#include <memory>
namespace milvus::segcore {
// TODO: this is
class IndexMeta {
 public:
    explicit IndexMeta(SchemaPtr schema) : schema_(schema) {
    }
    using IndexType = knowhere::IndexType;
    using IndexMode = knowhere::IndexMode;
    using IndexConfig = knowhere::Config;

    struct Entry {
        std::string index_name;
        FieldName field_name;
        IndexType type;
        IndexMode mode;
        IndexConfig config;
    };

    Status
    AddEntry(const std::string& index_name,
             const std::string& field_name,
             IndexType type,
             IndexMode mode,
             IndexConfig config);

    Status
    DropEntry(const std::string& index_name);

    const std::map<std::string, Entry>&
    get_entries() {
        return entries_;
    }

    const Entry&
    lookup_by_field(const FieldName& field_name) {
        AssertInfo(lookups_.count(field_name), field_name.get());
        auto index_name = lookups_.at(field_name);
        AssertInfo(entries_.count(index_name), index_name);
        return entries_.at(index_name);
    }

 private:
    void
    VerifyEntry(const Entry& entry);

 private:
    SchemaPtr schema_;
    std::map<std::string, Entry> entries_;        // index_name => Entry
    std::map<FieldName, std::string> lookups_;  // field_name => index_name
};

using IndexMetaPtr = std::shared_ptr<IndexMeta>;
}  // namespace milvus::segcore

#endif
