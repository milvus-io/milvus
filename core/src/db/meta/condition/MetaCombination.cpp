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

#include "db/meta/condition/MetaCombination.h"

#include <utility>

namespace milvus::engine::meta {

std::string
MetaFilterCombination::Dump() const {
    return filter_->Dump();
}

MetaRelationCombination::MetaRelationCombination(Comb comb, MetaConditionPtr lcond, MetaConditionPtr rcond)
    : MetaBaseCombination(comb), lcond_(std::move(lcond)), rcond_(std::move(rcond)) {
    if (comb != and_ && comb != or_) {
        throw std::runtime_error("Invalid combination relation");
    }
}

std::string
MetaRelationCombination::Dump() const {
    std::string l_dump_str = lcond_->Dump();
    if (std::dynamic_pointer_cast<MetaRelationCombination>(lcond_)) {
        l_dump_str = "(" + l_dump_str + ")";
    }

    std::string r_dump_str = rcond_->Dump();
    if (std::dynamic_pointer_cast<MetaRelationCombination>(rcond_)) {
        r_dump_str = "(" + r_dump_str + ")";
    }
    return l_dump_str + " " + Relation() + " " + r_dump_str;
}

}  // namespace milvus::engine::meta
