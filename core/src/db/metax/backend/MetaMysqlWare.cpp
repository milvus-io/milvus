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

#include "db/metax/backend/MetaMysqlWare.h"

#include <string>
#include <tuple>
#include <utility>

#include "db/metax/MetaResField.h"
#include "db/metax/MetaResFieldHelper.h"
#include "db/metax/backend/MetaSqlContext.h"

namespace milvus::engine::metax {

// inner namespace
namespace {

template <typename T>
Status
value2sqlstring(const T& t, std::string& out, MetaConvertorPtr convertor) {
    if constexpr (decay_equal_v<T, int64_t>) {
        out = convertor->int2str(t);
        return Status::OK();
    } else if constexpr (decay_equal_v<T, uint64_t>) {
        out = convertor->uint2str(t);
        return Status::OK();
    } else if constexpr (decay_equal_v<T, std::string>) {
        out = convertor->str2str(t);
        return Status::OK();
    } else if constexpr (decay_equal_v<T, json>) {
        out = convertor->json2str(t);
        return Status::OK();
    } else {
        return Status(DB_ERROR, "Unknown value type");
    }
}

template <typename F>
void
convert2raw(const F& f, Raw& raw, MetaConvertorPtr convertor) {
    if (f.Filled()) {
        std::string out;
        auto status = value2sqlstring<typename F::VType>(f.Get(), out, convertor);
        raw.insert(std::make_pair(std::string(F::Name), out));
    }
}

}  // namespace

Status
MetaMysqlWare::Ser2InsertContext(const MetaResFieldTuple& fields, MetaSqlCUDContext& context) {
    context.op_ = SqlOperation::sAdd_;
    auto& raw = context.raw_;
    std::apply([this, &raw](auto&... field) { ((convert2raw(field, raw, this->shared_from_this())), ...); }, fields);
    context.table_ = std::get<0>(fields).Table();
    return Status::OK();
}

Status
MetaMysqlWare::Insert(const MetaResFieldTuple& fields, snapshot::ID_TYPE& result_id) {
    MetaSqlCUDContext context;
    auto status = Ser2InsertContext(fields, context);

    if (!status.ok()) {
        return status;
    }

    return engine_->Insert(context, result_id);
}

}  // namespace milvus::engine::metax
