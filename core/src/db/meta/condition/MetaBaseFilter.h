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

#pragma once

#include <memory>
#include <string>

#include "db/meta/condition/MetaBaseCondition.h"
#include "db/meta/condition/MetaFinder.h"

namespace milvus::engine::meta {

class MetaBaseFilter : public MetaBaseCondition, public Finder {
 public:
    explicit MetaBaseFilter(const std::string& field) : field_(field) {

    }

    ~MetaBaseFilter() override = default;

    std::string
    Field() const {
        return field_;
    }

    virtual bool
    FieldFind(const std::string& field, const std::string& v) const {
        return (field_ == field) && StrFind(v);
    }

 private:
    std::string field_;
};

using MetaFilterPtr = std::shared_ptr<MetaBaseFilter>;

}  // namespace milvus::engine::meta
