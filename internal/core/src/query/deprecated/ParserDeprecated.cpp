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

#include <iostream>
#include <random>
#include "ParserDeprecated.h"

namespace milvus::wtf {
using google::protobuf::RepeatedField;
using google::protobuf::RepeatedPtrField;
#if 1
#if 0
void
CopyRowRecords(const RepeatedPtrField<proto::service::PlaceholderValue>& grpc_records,
               const RepeatedField<int64_t>& grpc_id_array,
               engine::VectorsData& vectors
               ) {
    // step 1: copy vector data
    int64_t float_data_size = 0, binary_data_size = 0;

    for (auto& record : grpc_records) {
        float_data_size += record.float_data_size();
        binary_data_size += record.binary_data().size();
    }

    std::vector<float> float_array(float_data_size, 0.0f);
    std::vector<uint8_t> binary_array(binary_data_size, 0);
    int64_t offset = 0;
    if (float_data_size > 0) {
        for (auto& record : grpc_records) {
            memcpy(&float_array[offset], record.float_data().data(), record.float_data_size() * sizeof(float));
            offset += record.float_data_size();
        }
    } else if (binary_data_size > 0) {
        for (auto& record : grpc_records) {
            memcpy(&binary_array[offset], record.binary_data().data(), record.binary_data().size());
            offset += record.binary_data().size();
        }
    }

    // step 2: copy id array
    std::vector<int64_t> id_array;
    if (grpc_id_array.size() > 0) {
        id_array.resize(grpc_id_array.size());
        memcpy(id_array.data(), grpc_id_array.data(), grpc_id_array.size() * sizeof(int64_t));
    }

    // step 3: contruct vectors
    vectors.vector_count_ = grpc_records.size();
    vectors.float_data_.swap(float_array);
    vectors.binary_data_.swap(binary_array);
    vectors.id_array_.swap(id_array);
}
#endif

Status
ProcessLeafQueryJson(const milvus::json& query_json, query_old::BooleanQueryPtr& query, std::string& field_name) {
#if 1
    if (query_json.contains("term")) {
        auto leaf_query = std::make_shared<query_old::LeafQuery>();
        auto term_query = std::make_shared<query_old::TermQuery>();
        milvus::json json_obj = query_json["term"];
        JSON_NULL_CHECK(json_obj);
        JSON_OBJECT_CHECK(json_obj);
        term_query->json_obj = json_obj;
        milvus::json::iterator json_it = json_obj.begin();
        field_name = json_it.key();
        leaf_query->term_query = term_query;
        query->AddLeafQuery(leaf_query);
    } else if (query_json.contains("range")) {
        auto leaf_query = std::make_shared<query_old::LeafQuery>();
        auto range_query = std::make_shared<query_old::RangeQuery>();
        milvus::json json_obj = query_json["range"];
        JSON_NULL_CHECK(json_obj);
        JSON_OBJECT_CHECK(json_obj);
        range_query->json_obj = json_obj;
        milvus::json::iterator json_it = json_obj.begin();
        field_name = json_it.key();

        leaf_query->range_query = range_query;
        query->AddLeafQuery(leaf_query);
    } else if (query_json.contains("vector")) {
        auto leaf_query = std::make_shared<query_old::LeafQuery>();
        auto vector_json = query_json["vector"];
        JSON_NULL_CHECK(vector_json);

        leaf_query->vector_placeholder = vector_json.get<std::string>();
        query->AddLeafQuery(leaf_query);
    } else {
        return Status{SERVER_INVALID_ARGUMENT, "Leaf query get wrong key"};
    }
#endif
    return Status::OK();
}

Status
ProcessBooleanQueryJson(const milvus::json& query_json,
                        query_old::BooleanQueryPtr& boolean_query,
                        query_old::QueryPtr& query_ptr) {
#if 1
    if (query_json.empty()) {
        return Status{SERVER_INVALID_ARGUMENT, "BoolQuery is null"};
    }
    for (auto& el : query_json.items()) {
        if (el.key() == "must") {
            boolean_query->SetOccur(query_old::Occur::MUST);
            auto must_json = el.value();
            if (!must_json.is_array()) {
                std::string msg = "Must json string is not an array";
                return Status{SERVER_INVALID_DSL_PARAMETER, msg};
            }

            for (auto& json : must_json) {
                auto must_query = std::make_shared<query_old::BooleanQuery>();
                if (json.contains("must") || json.contains("should") || json.contains("must_not")) {
                    STATUS_CHECK(ProcessBooleanQueryJson(json, must_query, query_ptr));
                    boolean_query->AddBooleanQuery(must_query);
                } else {
                    std::string field_name;
                    STATUS_CHECK(ProcessLeafQueryJson(json, boolean_query, field_name));
                    if (!field_name.empty()) {
                        query_ptr->index_fields.insert(field_name);
                    }
                }
            }
        } else if (el.key() == "should") {
            boolean_query->SetOccur(query_old::Occur::SHOULD);
            auto should_json = el.value();
            if (!should_json.is_array()) {
                std::string msg = "Should json string is not an array";
                return Status{SERVER_INVALID_DSL_PARAMETER, msg};
            }

            for (auto& json : should_json) {
                auto should_query = std::make_shared<query_old::BooleanQuery>();
                if (json.contains("must") || json.contains("should") || json.contains("must_not")) {
                    STATUS_CHECK(ProcessBooleanQueryJson(json, should_query, query_ptr));
                    boolean_query->AddBooleanQuery(should_query);
                } else {
                    std::string field_name;
                    STATUS_CHECK(ProcessLeafQueryJson(json, boolean_query, field_name));
                    if (!field_name.empty()) {
                        query_ptr->index_fields.insert(field_name);
                    }
                }
            }
        } else if (el.key() == "must_not") {
            boolean_query->SetOccur(query_old::Occur::MUST_NOT);
            auto should_json = el.value();
            if (!should_json.is_array()) {
                std::string msg = "Must_not json string is not an array";
                return Status{SERVER_INVALID_DSL_PARAMETER, msg};
            }

            for (auto& json : should_json) {
                if (json.contains("must") || json.contains("should") || json.contains("must_not")) {
                    auto must_not_query = std::make_shared<query_old::BooleanQuery>();
                    STATUS_CHECK(ProcessBooleanQueryJson(json, must_not_query, query_ptr));
                    boolean_query->AddBooleanQuery(must_not_query);
                } else {
                    std::string field_name;
                    STATUS_CHECK(ProcessLeafQueryJson(json, boolean_query, field_name));
                    if (!field_name.empty()) {
                        query_ptr->index_fields.insert(field_name);
                    }
                }
            }
        } else {
            std::string msg = "BoolQuery json string does not include bool query";
            return Status{SERVER_INVALID_DSL_PARAMETER, msg};
        }
    }
#endif
    return Status::OK();
}

Status
DeserializeJsonToBoolQuery(const google::protobuf::RepeatedPtrField<::milvus::grpc::VectorParam>& vector_params,
                           const std::string& dsl_string,
                           query_old::BooleanQueryPtr& boolean_query,
                           query_old::QueryPtr& query_ptr) {
#if 1
    try {
        milvus::json dsl_json = Json::parse(dsl_string);

        if (dsl_json.empty()) {
            return Status{SERVER_INVALID_ARGUMENT, "Query dsl is null"};
        }
        auto status = Status::OK();
        if (vector_params.empty()) {
            return Status(SERVER_INVALID_DSL_PARAMETER, "DSL must include vector query");
        }
        for (const auto& vector_param : vector_params) {
            const std::string& vector_string = vector_param.json();
            milvus::json vector_json = Json::parse(vector_string);
            milvus::json::iterator it = vector_json.begin();
            std::string placeholder = it.key();

            auto vector_query = std::make_shared<query_old::VectorQuery>();
            milvus::json::iterator vector_param_it = it.value().begin();
            if (vector_param_it != it.value().end()) {
                const std::string& field_name = vector_param_it.key();
                vector_query->field_name = field_name;
                milvus::json param_json = vector_param_it.value();
                int64_t topk = param_json["topk"];
                // STATUS_CHECK(server::ValidateSearchTopk(topk));
                vector_query->topk = topk;
                if (param_json.contains("metric_type")) {
                    std::string metric_type = param_json["metric_type"];
                    vector_query->metric_type = metric_type;
                    query_ptr->metric_types.insert({field_name, param_json["metric_type"]});
                }
                if (!vector_param_it.value()["params"].empty()) {
                    vector_query->extra_params = vector_param_it.value()["params"];
                }
                query_ptr->index_fields.insert(field_name);
            }

            engine::VectorsData vector_data;
            CopyRowRecords(vector_param.row_record().records(),
                           google::protobuf::RepeatedField<google::protobuf::int64>(), vector_data);
            vector_query->query_vector.vector_count = vector_data.vector_count_;
            vector_query->query_vector.binary_data.swap(vector_data.binary_data_);
            vector_query->query_vector.float_data.swap(vector_data.float_data_);

            query_ptr->vectors.insert(std::make_pair(placeholder, vector_query));
        }
        if (dsl_json.contains("bool")) {
            auto boolean_query_json = dsl_json["bool"];
            JSON_NULL_CHECK(boolean_query_json);
            status = ProcessBooleanQueryJson(boolean_query_json, boolean_query, query_ptr);
            if (!status.ok()) {
                return Status(SERVER_INVALID_DSL_PARAMETER, "DSL does not include bool");
            }
        } else {
            return Status(SERVER_INVALID_DSL_PARAMETER, "DSL does not include bool query");
        }
        return Status::OK();
    } catch (std::exception& e) {
        return Status(SERVER_INVALID_DSL_PARAMETER, e.what());
    }
#endif
    return Status::OK();
}

#endif
query_old::QueryPtr
Transformer(proto::service::Query* request) {
    query_old::BooleanQueryPtr boolean_query = std::make_shared<query_old::BooleanQuery>();
    query_old::QueryPtr query_ptr = std::make_shared<query_old::Query>();
#if 0
    query_ptr->collection_id = request->collection_name();
    auto status = DeserializeJsonToBoolQuery(request->placeholders(), request->dsl(), boolean_query, query_ptr);
    status = query_old::ValidateBooleanQuery(boolean_query);
    query_old::GeneralQueryPtr general_query = std::make_shared<query_old::GeneralQuery>();
    query_old::GenBinaryQuery(boolean_query, general_query->bin);
    query_ptr->root = general_query;
#endif
    return query_ptr;
}

}  // namespace milvus::wtf
