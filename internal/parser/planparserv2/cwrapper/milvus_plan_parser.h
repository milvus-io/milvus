// Copyright (C) 2019-2025 Zilliz. All rights reserved.
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

#include <cstdint>
#include <string>
#include <vector>

namespace milvus {
namespace planparserv2 {

// SchemaHandle is an opaque handle to a registered schema.
// Valid handles are > 0. A handle of 0 indicates an invalid/unregistered schema.
using SchemaHandle = int64_t;

constexpr SchemaHandle kInvalidSchemaHandle = 0;

// Thread-safe wrapper for the Go plan parser.
//
// Thread safety guarantees:
// - RegisterSchema: Can be called concurrently. Each call returns a unique handle.
// - UnregisterSchema: Can be called concurrently. Returns error if schema is in use or already unregistered.
// - Parse: Can be called concurrently. Uses lock-free reference counting internally.
//
// Usage:
//   auto handle = PlanParser::RegisterSchema(schema_proto);
//   auto plan = PlanParser::Parse(handle, "field > 10");
//   PlanParser::UnregisterSchema(handle);
class PlanParser {
 public:
    /**
     * @brief Register a schema to the plan parser.
     *
     * Thread-safe. Each call returns a unique handle, even for identical schemas.
     * The same schema can be registered multiple times, each with a different handle.
     *
     * @param schema_proto The serialized CollectionSchema protobuf.
     * @return SchemaHandle A unique handle for the registered schema (> 0).
     * @throws std::runtime_error if registration fails (e.g., invalid protobuf).
     */
    static SchemaHandle
    RegisterSchema(const std::vector<uint8_t>& schema_proto);

    /**
     * @brief Unregister a schema from the plan parser.
     *
     * Thread-safe. Fails if the schema is currently being used by Parse() or already unregistered.
     *
     * @param handle The handle returned by RegisterSchema.
     * @return Empty string on success, error message on failure.
     */
    static std::string
    UnregisterSchema(SchemaHandle handle);

    /**
     * @brief Parse an expression string into a serialized PlanNode protobuf (RetrievePlan).
     *
     * Thread-safe and lock-free. Multiple threads can call Parse() concurrently
     * with the same or different handles.
     *
     * @param handle The handle returned by RegisterSchema.
     * @param expr The expression string to parse.
     * @return std::vector<uint8_t> The serialized PlanNode protobuf.
     * @throws std::runtime_error if:
     *   - handle is invalid or not found
     *   - schema was unregistered
     *   - parsing fails
     */
    static std::vector<uint8_t>
    Parse(SchemaHandle handle, const std::string& expr);

    /**
     * @brief Parse an expression string into a serialized SearchPlan PlanNode protobuf.
     *
     * Thread-safe and lock-free. Creates a VectorANNS plan node for search operations.
     *
     * @param handle The handle returned by RegisterSchema.
     * @param expr The filter expression string to parse (can be empty).
     * @param vector_field_name The name of the vector field to search.
     * @param query_info_proto The serialized QueryInfo protobuf containing topk, metric_type, etc.
     * @return std::vector<uint8_t> The serialized PlanNode protobuf.
     * @throws std::runtime_error if:
     *   - handle is invalid or not found
     *   - schema was unregistered
     *   - vector field not found
     *   - parsing fails
     */
    static std::vector<uint8_t>
    ParseSearch(SchemaHandle handle,
                const std::string& expr,
                const std::string& vector_field_name,
                const std::vector<uint8_t>& query_info_proto);
};

}  // namespace planparserv2
}  // namespace milvus
