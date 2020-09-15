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

#include "db/transcript/ScriptCodec.h"

#include <memory>
#include <set>
#include <utility>

namespace milvus {
namespace engine {

// action names
const char* ActionCreateCollection = "CreateCollection";
const char* ActionDropCollection = "DropCollection";
const char* ActionHasCollection = "HasCollection";
const char* ActionListCollections = "ListCollections";
const char* ActionGetCollectionInfo = "GetCollectionInfo";
const char* ActionGetCollectionStats = "GetCollectionStats";
const char* ActionCountEntities = "CountEntities";
const char* ActionCreatePartition = "CreatePartition";
const char* ActionDropPartition = "DropPartition";
const char* ActionHasPartition = "HasPartition";
const char* ActionListPartitions = "ListPartitions";
const char* ActionCreateIndex = "CreateIndex";
const char* ActionDropIndex = "DropIndex";
const char* ActionDescribeIndex = "DescribeIndex";
const char* ActionInsert = "Insert";
const char* ActionGetEntityByID = "GetEntityByID";
const char* ActionDeleteEntityByID = "DeleteEntityByID";
const char* ActionListIDInSegment = "ListIDInSegment";
const char* ActionQuery = "Query";
const char* ActionLoadCollection = "LoadCollection";
const char* ActionFlush = "Flush";
const char* ActionCompact = "Compact";

// json keys
const char* J_ACTION_TYPE = "action";
const char* J_ACTION_TS = "time";  // action timestamp
const char* J_COLLECTION_NAME = "col_name";
const char* J_PARTITION_NAME = "part_name";
const char* J_FIELD_NAME = "field_name";
const char* J_FIELD_NAMES = "field_names";
const char* J_FIELD_TYPE = "field_type";
const char* J_MAPPINGS = "mappings";
const char* J_PARAMS = "params";
const char* J_ID_ARRAY = "id_array";
const char* J_SEGMENT_ID = "segment_id";
const char* J_THRESHOLD = "threshold";
const char* J_FORCE = "force";
const char* J_INDEX_NAME = "index_name";
const char* J_INDEX_TYPE = "index_type";
const char* J_METRIC_TYPE = "metric_type";
const char* J_CHUNK_COUNT = "count";
const char* J_FIXED_FIELDS = "fixed_fields";
const char* J_VARIABLE_FIELDS = "variable_fields";
const char* J_CHUNK_DATA = "data";
const char* J_CHUNK_OFFSETS = "offsets";

const char* J_PARTITIONS = "partitions";
const char* J_FIELDS = "fields";
const char* J_METRIC_TYPES = "metric_types";
const char* J_KEY = "key";
const char* J_VECTOR_QUERIES = "vector_queries";
const char* J_TOPK = "topk";
const char* J_NQ = "nq";
const char* J_BOOST = "boost";
const char* J_FLOAT_DATA = "float_data";
const char* J_BIN_DATA = "bin_data";
const char* J_GENERAL_QUERY = "general_query";
const char* J_QUERY_LEAF = "leaf";
const char* J_QUERY_BIN = "bin";
const char* J_QUERY_RELATION = "relation";
const char* J_QUERY_LEFT = "left";
const char* J_QUERY_RIGHT = "right";
const char* J_QUERY_TERM = "term";
const char* J_QUERY_RANGE = "range";
const char* J_QUERY_PLACEHOLDER = "placeholder";

// encode methods
Status
ScriptCodec::EncodeAction(milvus::json& json_obj, const std::string& action_type) {
    json_obj[J_ACTION_TYPE] = action_type;
    json_obj[J_ACTION_TS] = utils::GetMicroSecTimeStamp();
    return Status::OK();
}

Status
ScriptCodec::Encode(milvus::json& json_obj, const snapshot::CreateCollectionContext& context) {
    if (context.collection == nullptr) {
        return Status::OK();
    }

    json_obj[J_COLLECTION_NAME] = context.collection->GetName();

    // params
    json_obj[J_PARAMS] = context.collection->GetParams();

    // mappings
    milvus::json json_fields;
    for (const auto& pair : context.fields_schema) {
        auto& field = pair.first;
        milvus::json json_field;
        json_field[J_FIELD_NAME] = field->GetName();
        json_field[J_FIELD_TYPE] = field->GetFtype();
        json_field[J_PARAMS] = field->GetParams();
        json_fields.push_back(json_field);
    }
    json_obj[J_MAPPINGS] = json_fields;

    return Status::OK();
}

Status
ScriptCodec::EncodeCollectionName(milvus::json& json_obj, const std::string& collection_name) {
    json_obj[J_COLLECTION_NAME] = collection_name;
    return Status::OK();
}

Status
ScriptCodec::EncodePartitionName(milvus::json& json_obj, const std::string& partition_name) {
    json_obj[J_PARTITION_NAME] = partition_name;
    return Status::OK();
}

Status
ScriptCodec::EncodeFieldName(milvus::json& json_obj, const std::string& field_name) {
    json_obj[J_FIELD_NAME] = field_name;
    return Status::OK();
}

Status
ScriptCodec::EncodeFieldNames(milvus::json& json_obj, const std::vector<std::string>& field_names) {
    json_obj[J_FIELD_NAMES] = field_names;
    return Status::OK();
}

Status
ScriptCodec::Encode(milvus::json& json_obj, const CollectionIndex& index) {
    json_obj[J_INDEX_NAME] = index.index_name_;
    json_obj[J_INDEX_TYPE] = index.index_type_;
    json_obj[J_METRIC_TYPE] = index.metric_name_;
    json_obj[J_PARAMS] = index.extra_params_;

    return Status::OK();
}

Status
ScriptCodec::Encode(milvus::json& json_obj, const DataChunkPtr& data_chunk) {
    if (data_chunk == nullptr) {
        return Status::OK();
    }

    json_obj[J_CHUNK_COUNT] = data_chunk->count_;

    // fixed fields
    {
        milvus::json json_fields;
        for (const auto& pair : data_chunk->fixed_fields_) {
            auto& data = pair.second;
            if (data == nullptr) {
                continue;
            }

            milvus::json json_field;
            json_field[J_FIELD_NAME] = pair.first;
            json_field[J_CHUNK_DATA] = data->data_;
            json_fields.push_back(json_field);
        }
        json_obj[J_FIXED_FIELDS] = json_fields;
    }

    // variable fields
    {
        milvus::json json_fields;
        for (const auto& pair : data_chunk->variable_fields_) {
            auto& data = pair.second;
            if (data == nullptr) {
                continue;
            }

            milvus::json json_field;
            json_field[J_FIELD_NAME] = pair.first;
            json_field[J_CHUNK_DATA] = data->data_;
            json_field[J_CHUNK_OFFSETS] = data->offset_;

            json_fields.push_back(json_field);
        }
        json_obj[J_VARIABLE_FIELDS] = json_fields;
    }

    return Status::OK();
}

Status
ScriptCodec::Encode(milvus::json& json_obj, const IDNumbers& id_array) {
    json_obj[J_ID_ARRAY] = id_array;
    return Status::OK();
}

Status
ScriptCodec::EncodeSegmentID(milvus::json& json_obj, int64_t segment_id) {
    json_obj[J_SEGMENT_ID] = segment_id;
    return Status::OK();
}

Status
ScriptCodec::Encode(milvus::json& json_obj, const query::QueryPtr& query_ptr) {
    if (query_ptr == nullptr) {
        return Status::OK();
    }

    EncodeCollectionName(json_obj, query_ptr->collection_id);
    json_obj[J_PARTITIONS] = query_ptr->partitions;
    json_obj[J_FIELD_NAMES] = query_ptr->field_names;
    json_obj[J_FIELDS] = query_ptr->index_fields;
    milvus::json metrics;
    for (auto& pair : query_ptr->metric_types) {
        milvus::json metric;
        metric[J_FIELD_NAME] = pair.first;
        metric[J_METRIC_TYPE] = pair.second;
        metrics.push_back(metric);
    }
    json_obj[J_METRIC_TYPES] = metrics;

    // vector query
    milvus::json vector_queries;
    for (auto& pair : query_ptr->vectors) {
        milvus::query::VectorQueryPtr& query = pair.second;
        if (query == nullptr) {
            continue;
        }

        milvus::json vector_query;
        vector_query[J_KEY] = pair.first;
        vector_query[J_FIELD_NAME] = query->field_name;
        vector_query[J_PARAMS] = query->extra_params;
        vector_query[J_TOPK] = query->topk;
        vector_query[J_NQ] = query->nq;
        vector_query[J_METRIC_TYPE] = query->metric_type;
        vector_query[J_BOOST] = query->boost;
        vector_query[J_FLOAT_DATA] = query->query_vector.float_data;
        vector_query[J_BIN_DATA] = query->query_vector.binary_data;

        vector_queries.push_back(vector_query);
    }
    json_obj[J_VECTOR_QUERIES] = vector_queries;

    // general query
    if (query_ptr->root) {
        milvus::json general_query;
        EncodeGeneralQuery(general_query, query_ptr->root);
        json_obj[J_GENERAL_QUERY] = general_query;
    }

    return Status::OK();
}

Status
ScriptCodec::EncodeGeneralQuery(milvus::json& json_obj, query::GeneralQueryPtr& query) {
    if (query == nullptr) {
        return Status::OK();
    }

    if (query->leaf) {
        milvus::json json_leaf;
        json_leaf[J_QUERY_PLACEHOLDER] = query->leaf->vector_placeholder;
        json_leaf[J_BOOST] = query->leaf->query_boost;
        if (query->leaf->term_query) {
            milvus::json json_term;
            json_term[J_PARAMS] = query->leaf->term_query->json_obj;
            json_leaf[J_QUERY_TERM] = json_term;
        }
        if (query->leaf->range_query) {
            milvus::json json_range;
            json_range[J_PARAMS] = query->leaf->range_query->json_obj;
            json_leaf[J_QUERY_RANGE] = json_range;
        }

        json_obj[J_QUERY_LEAF] = json_leaf;
    }
    if (query->bin) {
        milvus::json json_bin;
        json_bin[J_QUERY_RELATION] = query->bin->relation;
        json_bin[J_BOOST] = query->bin->query_boost;

        if (query->bin->left_query) {
            milvus::json json_left;
            EncodeGeneralQuery(json_left, query->bin->left_query);
            json_bin[J_QUERY_LEFT] = json_left;
        }
        if (query->bin->right_query) {
            milvus::json json_right;
            EncodeGeneralQuery(json_right, query->bin->right_query);
            json_bin[J_QUERY_RIGHT] = json_right;
        }

        json_obj[J_QUERY_BIN] = json_bin;
    }
    return Status::OK();
}

Status
ScriptCodec::EncodeThreshold(milvus::json& json_obj, double threshold) {
    json_obj[J_THRESHOLD] = threshold;
    return Status::OK();
}

Status
ScriptCodec::EncodeForce(milvus::json& json_obj, bool force) {
    json_obj[J_FORCE] = force;
    return Status::OK();
}

// decode methods
Status
ScriptCodec::DecodeAction(milvus::json& json_obj, std::string& action_type, int64_t& action_ts) {
    action_type = "";
    action_ts = 0;
    if (json_obj.find(J_ACTION_TYPE) != json_obj.end()) {
        action_type = json_obj[J_ACTION_TYPE].get<std::string>();
    } else {
        return Status(DB_ERROR, "element doesn't exist");
    }

    if (json_obj.find(J_ACTION_TS) != json_obj.end()) {
        action_ts = json_obj[J_ACTION_TS].get<int64_t>();
    } else {
        return Status(DB_ERROR, "element doesn't exist");
    }

    return Status::OK();
}

Status
ScriptCodec::Decode(milvus::json& json_obj, snapshot::CreateCollectionContext& context) {
    std::string collection_name;
    ScriptCodec::DecodeCollectionName(json_obj, collection_name);

    milvus::json params;
    if (json_obj.find(J_PARAMS) != json_obj.end()) {
        params = json_obj[J_PARAMS];
    }
    context.collection = std::make_shared<snapshot::Collection>(collection_name, params);

    // mappings
    if (json_obj.find(J_MAPPINGS) != json_obj.end()) {
        milvus::json& fields = json_obj[J_MAPPINGS];
        for (auto& field : fields) {
            auto field_name = field[J_FIELD_NAME].get<std::string>();
            auto field_type = static_cast<DataType>(field[J_FIELD_TYPE].get<int32_t>());
            milvus::json& field_params = field[J_PARAMS];
            auto field_ptr = std::make_shared<engine::snapshot::Field>(field_name, 0, field_type, field_params);
            context.fields_schema[field_ptr] = {};
        }
    }

    return Status::OK();
}

Status
ScriptCodec::DecodeCollectionName(milvus::json& json_obj, std::string& collection_name) {
    if (json_obj.find(J_COLLECTION_NAME) != json_obj.end()) {
        collection_name = json_obj[J_COLLECTION_NAME].get<std::string>();
        return Status::OK();
    }

    return Status(DB_ERROR, "element doesn't exist");
}

Status
ScriptCodec::DecodePartitionName(milvus::json& json_obj, std::string& partition_name) {
    if (json_obj.find(J_PARTITION_NAME) != json_obj.end()) {
        partition_name = json_obj[J_PARTITION_NAME].get<std::string>();
        return Status::OK();
    }

    return Status(DB_ERROR, "element doesn't exist");
}

Status
ScriptCodec::DecodeFieldName(milvus::json& json_obj, std::string& field_name) {
    if (json_obj.find(J_FIELD_NAME) != json_obj.end()) {
        field_name = json_obj[J_FIELD_NAME].get<std::string>();
        return Status::OK();
    }

    return Status(DB_ERROR, "element doesn't exist");
}

Status
ScriptCodec::DecodeFieldNames(milvus::json& json_obj, std::vector<std::string>& field_names) {
    if (json_obj.find(J_FIELD_NAMES) != json_obj.end()) {
        field_names = json_obj[J_FIELD_NAMES].get<std::vector<std::string>>();
        return Status::OK();
    }

    return Status(DB_ERROR, "element doesn't exist");
}

Status
ScriptCodec::Decode(milvus::json& json_obj, CollectionIndex& index) {
    if (json_obj.find(J_INDEX_NAME) != json_obj.end()) {
        index.index_name_ = json_obj[J_INDEX_NAME].get<std::string>();
    }
    if (json_obj.find(J_INDEX_TYPE) != json_obj.end()) {
        index.index_type_ = json_obj[J_INDEX_TYPE].get<std::string>();
    }
    if (json_obj.find(J_METRIC_TYPE) != json_obj.end()) {
        index.metric_name_ = json_obj[J_METRIC_TYPE].get<std::string>();
    }
    if (json_obj.find(J_PARAMS) != json_obj.end()) {
        index.extra_params_ = json_obj[J_PARAMS];
    }

    return Status::OK();
}

Status
ScriptCodec::Decode(milvus::json& json_obj, DataChunkPtr& data_chunk) {
    data_chunk = std::make_shared<DataChunk>();
    if (json_obj.find(J_CHUNK_COUNT) != json_obj.end()) {
        data_chunk->count_ = json_obj[J_CHUNK_COUNT].get<int64_t>();
    }

    // fixed fields
    if (json_obj.find(J_FIXED_FIELDS) != json_obj.end()) {
        auto& fields = json_obj[J_FIXED_FIELDS];
        for (auto& field : fields) {
            auto name = field[J_FIELD_NAME].get<std::string>();
            BinaryDataPtr bin = std::make_shared<BinaryData>();
            bin->data_ = field[J_CHUNK_DATA].get<std::vector<uint8_t>>();
            data_chunk->fixed_fields_.insert(std::make_pair(name, bin));
        }
    }

    // variable fields
    if (json_obj.find(J_VARIABLE_FIELDS) != json_obj.end()) {
        auto& fields = json_obj[J_VARIABLE_FIELDS];
        for (auto& field : fields) {
            auto name = field[J_FIELD_NAME].get<std::string>();
            VaribleDataPtr bin = std::make_shared<VaribleData>();
            bin->data_ = field[J_CHUNK_DATA].get<std::vector<uint8_t>>();
            bin->offset_ = field[J_CHUNK_OFFSETS].get<std::vector<int64_t>>();

            data_chunk->variable_fields_.insert(std::make_pair(name, bin));
        }
    }

    return Status::OK();
}

Status
ScriptCodec::Decode(milvus::json& json_obj, IDNumbers& id_array) {
    if (json_obj.find(J_ID_ARRAY) != json_obj.end()) {
        id_array = json_obj[J_ID_ARRAY].get<IDNumbers>();
        return Status::OK();
    }

    return Status(DB_ERROR, "element doesn't exist");
}

Status
ScriptCodec::DecodeSegmentID(milvus::json& json_obj, int64_t& segment_id) {
    if (json_obj.find(J_SEGMENT_ID) != json_obj.end()) {
        segment_id = json_obj[J_SEGMENT_ID].get<int64_t>();
        return Status::OK();
    }

    return Status(DB_ERROR, "element doesn't exist");
}

Status
ScriptCodec::Decode(milvus::json& json_obj, query::QueryPtr& query_ptr) {
    query_ptr = std::make_shared<query::Query>();
    DecodeCollectionName(json_obj, query_ptr->collection_id);
    DecodeFieldNames(json_obj, query_ptr->field_names);
    if (json_obj.find(J_PARTITIONS) != json_obj.end()) {
        query_ptr->partitions = json_obj[J_PARTITIONS].get<std::vector<std::string>>();
    }
    if (json_obj.find(J_FIELDS) != json_obj.end()) {
        query_ptr->index_fields = json_obj[J_FIELDS].get<std::set<std::string>>();
    }

    if (json_obj.find(J_METRIC_TYPES) != json_obj.end()) {
        milvus::json& metrics = json_obj[J_METRIC_TYPES];
        for (auto& metric : metrics) {
            std::string field_name = metric[J_FIELD_NAME];
            std::string metric_type = metric[J_METRIC_TYPE];
            query_ptr->metric_types.insert(std::make_pair(field_name, metric_type));
        }
    }

    // vector queries
    if (json_obj.find(J_VECTOR_QUERIES) != json_obj.end()) {
        milvus::json& vector_queries = json_obj[J_VECTOR_QUERIES];
        for (auto& vector_query : vector_queries) {
            std::string key = vector_query[J_KEY];

            milvus::query::VectorQueryPtr query = std::make_shared<milvus::query::VectorQuery>();
            query->field_name = vector_query[J_FIELD_NAME];
            query->extra_params = vector_query[J_PARAMS];
            query->topk = vector_query[J_TOPK];
            query->nq = vector_query[J_NQ];
            query->metric_type = vector_query[J_METRIC_TYPE];
            query->boost = vector_query[J_BOOST];
            query->query_vector.float_data = vector_query[J_FLOAT_DATA].get<std::vector<float>>();
            query->query_vector.binary_data = vector_query[J_BIN_DATA].get<std::vector<uint8_t>>();

            query_ptr->vectors.insert(std::make_pair(key, query));
        }
    }

    // general query
    if (json_obj.find(J_GENERAL_QUERY) != json_obj.end()) {
        milvus::json& json_query = json_obj[J_GENERAL_QUERY];
        query_ptr->root = std::make_shared<query::GeneralQuery>();
        DecodeGeneralQuery(json_query, query_ptr->root);
    }

    return Status::OK();
}

Status
ScriptCodec::DecodeGeneralQuery(milvus::json& json_obj, query::GeneralQueryPtr& query) {
    if (query == nullptr) {
        return Status::OK();
    }

    if (json_obj.find(J_QUERY_LEAF) != json_obj.end()) {
        milvus::json& json_leaf = json_obj[J_QUERY_LEAF];
        query->leaf = std::make_shared<query::LeafQuery>();
        query->leaf->vector_placeholder = json_leaf[J_QUERY_PLACEHOLDER];
        query->leaf->query_boost = json_leaf[J_BOOST];

        if (json_leaf.find(J_QUERY_TERM) != json_leaf.end()) {
            milvus::json& json_term = json_leaf[J_QUERY_TERM];
            query->leaf->term_query = std::make_shared<query::TermQuery>();
            query->leaf->term_query->json_obj = json_term[J_PARAMS];
        }
        if (json_leaf.find(J_QUERY_RANGE) != json_leaf.end()) {
            milvus::json& json_range = json_leaf[J_QUERY_RANGE];
            query->leaf->range_query = std::make_shared<query::RangeQuery>();
            query->leaf->range_query->json_obj = json_range[J_PARAMS];
        }
    }

    if (json_obj.find(J_QUERY_BIN) != json_obj.end()) {
        milvus::json& json_bin = json_obj[J_QUERY_BIN];
        query->bin = std::make_shared<query::BinaryQuery>();
        query->bin->relation = json_bin[J_QUERY_RELATION];
        query->bin->query_boost = json_bin[J_BOOST];

        if (json_bin.find(J_QUERY_LEFT) != json_bin.end()) {
            milvus::json& json_left = json_bin[J_QUERY_LEFT];
            query->bin->left_query = std::make_shared<query::GeneralQuery>();
            DecodeGeneralQuery(json_left, query->bin->left_query);
        }

        if (json_bin.find(J_QUERY_RIGHT) != json_bin.end()) {
            milvus::json& json_right = json_bin[J_QUERY_RIGHT];
            query->bin->right_query = std::make_shared<query::GeneralQuery>();
            DecodeGeneralQuery(json_right, query->bin->right_query);
        }
    }

    return Status::OK();
}

Status
ScriptCodec::DecodeThreshold(milvus::json& json_obj, double& threshold) {
    if (json_obj.find(J_THRESHOLD) != json_obj.end()) {
        threshold = json_obj[J_THRESHOLD].get<double>();
        return Status::OK();
    }

    return Status(DB_ERROR, "element doesn't exist");
}

Status
ScriptCodec::DecodeForce(milvus::json& json_obj, bool& force) {
    if (json_obj.find(J_FORCE) != json_obj.end()) {
        force = json_obj[J_FORCE].get<bool>();
        return Status::OK();
    }

    return Status(DB_ERROR, "element doesn't exist");
}

}  // namespace engine
}  // namespace milvus
