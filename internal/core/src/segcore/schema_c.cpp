// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "segcore/schema_handle.h"

#include <exception>
#include <limits>
#include <memory>
#include <vector>

#include "common/EasyAssert.h"
#include "monitor/scope_metric.h"
#include "pb/schema.pb.h"
#include "segcore/SchemaCache.h"

namespace milvus::segcore {

const SchemaPtr&
BorrowSchemaPtrFromC(CSchemaHandle schema_handle) {
    AssertInfo(schema_handle != nullptr, "schema handle is null");
    const auto* schema = static_cast<const SchemaPtr*>(schema_handle);
    AssertInfo(*schema != nullptr, "schema handle is empty");
    return *schema;
}

SchemaPtr
CloneSchemaPtrFromC(CSchemaHandle schema_handle) {
    return BorrowSchemaPtrFromC(schema_handle);
}

}  // namespace milvus::segcore

CStatus
AcquireSchemaHandle(const int64_t collection_id,
                    const void* schema_proto_blob,
                    const int64_t length,
                    CSchemaHandle* schema_handle) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(schema_handle != nullptr,
                   "schema handle output pointer is null");
        *schema_handle = nullptr;
        AssertInfo(schema_proto_blob != nullptr, "schema proto is null");
        AssertInfo(length > 0, "schema proto length must be positive");
        AssertInfo(length <= std::numeric_limits<int>::max(),
                   "schema proto length is too large: {}",
                   length);

        milvus::proto::schema::CollectionSchema schema_proto;
        const auto parsed = schema_proto.ParseFromArray(
            schema_proto_blob, static_cast<int>(length));
        AssertInfo(parsed, "parse schema proto failed");

        auto schema = std::make_unique<milvus::SchemaPtr>(
            milvus::segcore::GetGlobalSchemaCache().GetOrCreate(collection_id,
                                                                schema_proto));
        *schema_handle = schema.release();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
AcquireLoadSchemaHandle(const void* schema_proto_blob,
                        const int64_t length,
                        const int64_t* load_fields,
                        const int64_t load_field_count,
                        CSchemaHandle* schema_handle) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(schema_handle != nullptr,
                   "schema handle output pointer is null");
        *schema_handle = nullptr;
        AssertInfo(schema_proto_blob != nullptr, "schema proto is null");
        AssertInfo(length > 0, "schema proto length must be positive");
        AssertInfo(length <= std::numeric_limits<int>::max(),
                   "schema proto length is too large: {}",
                   length);
        AssertInfo(load_field_count >= 0,
                   "load field count must not be negative: {}",
                   load_field_count);
        AssertInfo(load_field_count == 0 || load_fields != nullptr,
                   "load fields are null");

        milvus::proto::schema::CollectionSchema schema_proto;
        const auto parsed = schema_proto.ParseFromArray(
            schema_proto_blob, static_cast<int>(length));
        AssertInfo(parsed, "parse schema proto failed");

        auto schema = milvus::Schema::ParseFrom(schema_proto);
        schema->set_schema_version(schema_proto.version());
        std::vector<int64_t> fields;
        if (load_field_count > 0) {
            fields.assign(load_fields, load_fields + load_field_count);
        }
        schema->UpdateLoadFields(fields);
        *schema_handle = new milvus::SchemaPtr(std::move(schema));
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
CloneSchemaHandle(CSchemaHandle schema_handle,
                  CSchemaHandle* cloned_schema_handle) {
    SCOPE_CGO_CALL_METRIC();

    try {
        AssertInfo(cloned_schema_handle != nullptr,
                   "cloned schema handle output pointer is null");
        *cloned_schema_handle = nullptr;
        auto clone = std::make_unique<milvus::SchemaPtr>(
            milvus::segcore::CloneSchemaPtrFromC(schema_handle));
        *cloned_schema_handle = clone.release();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

void
ReleaseSchemaHandle(CSchemaHandle schema_handle) {
    SCOPE_CGO_CALL_METRIC();
    delete static_cast<milvus::SchemaPtr*>(schema_handle);
}
