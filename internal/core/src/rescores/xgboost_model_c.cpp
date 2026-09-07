// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "rescores/xgboost_model_c.h"

#include <arrow/c/abi.h>

#include <algorithm>
#include <climits>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "common/EasyAssert.h"
#include "nlohmann/json.hpp"

namespace {

using Json = nlohmann::json;

constexpr int32_t kMaxXGBoostUBJDepth = 128;

struct XGBoostTree {
    std::vector<int32_t> left_children;
    std::vector<int32_t> right_children;
    std::vector<int32_t> split_indices;
    std::vector<float> split_conditions;
    std::vector<uint8_t> default_left;
};

struct ArrowFeatureColumn;

using ArrowFeatureGetter = float (*)(const ArrowFeatureColumn&, int64_t);

struct ArrowFeatureColumn {
    const void* values = nullptr;
    const uint8_t* validity = nullptr;
    int64_t offset = 0;
    int64_t null_count = 0;
    char format = '\0';
    ArrowFeatureGetter getter = nullptr;

    float
    Get(int64_t row) const;
};

class XGBoostModel {
 public:
    XGBoostModel(int32_t num_features,
                 std::string objective,
                 float base_score,
                 std::vector<XGBoostTree> trees);

    static std::unique_ptr<XGBoostModel>
    LoadUBJ(const char* path);

    int32_t
    NumFeatures() const {
        return num_features_;
    }

    void
    Predict(const CXGBoostPredictRequest& request) const;

 private:
    static std::string
    GetObjectiveName(const Json& learner);

    static void
    ValidateObjective(const std::string& objective);

    static const Json&
    GetGradientBooster(const Json& learner);

    static float
    ParseBaseScore(const Json& learner_param, const std::string& objective);

    static void
    ValidateBoosterModel(const Json& booster_model);

    static void
    CheckArraySize(const Json& array, size_t expected, const char* key);

    static XGBoostTree
    ParseTree(const Json& tree, int32_t model_num_features);

    static std::vector<XGBoostTree>
    ParseTrees(const Json& booster_model, int32_t model_num_features);

    static void
    AddTreeContributionBatch(const XGBoostTree& tree,
                             const std::vector<ArrowFeatureColumn>& columns,
                             int64_t num_rows,
                             std::vector<int32_t>& nodes,
                             float* output);

    void
    TransformOutputBatch(int64_t num_rows,
                         bool output_default,
                         float* output) const;

    int32_t num_features_ = 0;
    std::string objective_;
    float base_score_ = 0.0f;
    std::vector<XGBoostTree> trees_;
};

std::string
JsonString(const Json& object,
           const char* key,
           const std::string& default_value = "") {
    auto it = object.find(key);
    if (it == object.end() || it->is_null()) {
        return default_value;
    }
    if (it->is_string()) {
        return it->get<std::string>();
    }
    if (it->is_number_integer()) {
        return std::to_string(it->get<int64_t>());
    }
    if (it->is_number_unsigned()) {
        return std::to_string(it->get<uint64_t>());
    }
    if (it->is_number_float()) {
        return std::to_string(it->get<double>());
    }
    return default_value;
}

const Json&
RequiredObject(const Json& parent, const char* key) {
    auto it = parent.find(key);
    if (it == parent.end() || !it->is_object()) {
        ThrowInfo(milvus::InvalidParameter,
                  "xgboost: UBJ model missing object '{}'",
                  key);
    }
    return *it;
}

const Json&
RequiredArray(const Json& parent, const char* key) {
    auto it = parent.find(key);
    if (it == parent.end() || !it->is_array()) {
        ThrowInfo(milvus::InvalidParameter,
                  "xgboost: UBJ model missing array '{}'",
                  key);
    }
    return *it;
}

int32_t
ParseNonNegativeInt32(const std::string& value, const char* key) {
    try {
        size_t pos = 0;
        auto parsed = std::stoll(value, &pos, 10);
        if (pos != value.size() || parsed < 0 || parsed > INT32_MAX) {
            ThrowInfo(milvus::InvalidParameter,
                      "xgboost: invalid {} '{}'",
                      key,
                      value);
        }
        return static_cast<int32_t>(parsed);
    } catch (std::exception&) {
        ThrowInfo(
            milvus::InvalidParameter, "xgboost: invalid {} '{}'", key, value);
    }
}

std::string
NormalizeFloatString(std::string value) {
    if (value.size() >= 2 && value.front() == '[' && value.back() == ']') {
        value = value.substr(1, value.size() - 2);
    }
    return value;
}

float
ParseFloatValue(const std::string& value, const char* key) {
    try {
        size_t pos = 0;
        auto normalized = NormalizeFloatString(value);
        auto parsed = std::stof(normalized, &pos);
        if (pos != normalized.size() || !std::isfinite(parsed)) {
            ThrowInfo(milvus::InvalidParameter,
                      "xgboost: invalid {} '{}'",
                      key,
                      value);
        }
        return parsed;
    } catch (std::exception&) {
        ThrowInfo(
            milvus::InvalidParameter, "xgboost: invalid {} '{}'", key, value);
    }
}

int32_t
JsonInt32(const Json& value, const char* key) {
    if (value.is_number_integer()) {
        auto parsed = value.get<int64_t>();
        if (parsed < INT32_MIN || parsed > INT32_MAX) {
            ThrowInfo(milvus::InvalidParameter,
                      "xgboost: invalid {} '{}'",
                      key,
                      parsed);
        }
        return static_cast<int32_t>(parsed);
    }
    if (value.is_number_unsigned()) {
        auto parsed = value.get<uint64_t>();
        if (parsed > static_cast<uint64_t>(INT32_MAX)) {
            ThrowInfo(milvus::InvalidParameter,
                      "xgboost: invalid {} '{}'",
                      key,
                      parsed);
        }
        return static_cast<int32_t>(parsed);
    }
    if (value.is_string()) {
        try {
            size_t pos = 0;
            const auto& string_value = value.get<std::string>();
            auto parsed = std::stoll(string_value, &pos, 10);
            if (pos != string_value.size() || parsed < INT32_MIN ||
                parsed > INT32_MAX) {
                ThrowInfo(milvus::InvalidParameter,
                          "xgboost: invalid {} '{}'",
                          key,
                          string_value);
            }
            return static_cast<int32_t>(parsed);
        } catch (std::exception&) {
            ThrowInfo(milvus::InvalidParameter, "xgboost: invalid {}", key);
        }
    }
    ThrowInfo(milvus::InvalidParameter, "xgboost: invalid {}", key);
}

float
JsonFloat(const Json& value, const char* key) {
    if (value.is_number()) {
        auto parsed = value.get<float>();
        if (!std::isfinite(parsed)) {
            ThrowInfo(milvus::InvalidParameter, "xgboost: invalid {}", key);
        }
        return parsed;
    }
    if (value.is_string()) {
        return ParseFloatValue(value.get<std::string>(), key);
    }
    ThrowInfo(milvus::InvalidParameter, "xgboost: invalid {}", key);
}

bool
JsonBool(const Json& value, const char* key) {
    if (value.is_boolean()) {
        return value.get<bool>();
    }
    if (value.is_number_integer()) {
        return value.get<int64_t>() != 0;
    }
    if (value.is_number_unsigned()) {
        return value.get<uint64_t>() != 0;
    }
    if (value.is_string()) {
        auto string_value = value.get<std::string>();
        if (string_value == "1" || string_value == "true") {
            return true;
        }
        if (string_value == "0" || string_value == "false") {
            return false;
        }
    }
    ThrowInfo(milvus::InvalidParameter, "xgboost: invalid {}", key);
}

class UBJDepthGuardSax : public nlohmann::json_sax<Json> {
 public:
    bool
    null() override {
        return true;
    }

    bool
    boolean(bool) override {
        return true;
    }

    bool
    number_integer(number_integer_t) override {
        return true;
    }

    bool
    number_unsigned(number_unsigned_t) override {
        return true;
    }

    bool
    number_float(number_float_t, const string_t&) override {
        return true;
    }

    bool
    string(string_t&) override {
        return true;
    }

    bool
    binary(binary_t&) override {
        return true;
    }

    bool
    start_object(std::size_t) override {
        return PushContainer();
    }

    bool
    key(string_t&) override {
        return true;
    }

    bool
    end_object() override {
        PopContainer();
        return true;
    }

    bool
    start_array(std::size_t) override {
        return PushContainer();
    }

    bool
    end_array() override {
        PopContainer();
        return true;
    }

    bool
    parse_error(std::size_t,
                const std::string&,
                const nlohmann::detail::exception& ex) override {
        parse_error_ = ex.what();
        return false;
    }

    bool
    ExceededDepth() const {
        return exceeded_depth_;
    }

    const std::string&
    ParseError() const {
        return parse_error_;
    }

 private:
    bool
    PushContainer() {
        depth_++;
        if (depth_ > kMaxXGBoostUBJDepth) {
            exceeded_depth_ = true;
            return false;
        }
        return true;
    }

    void
    PopContainer() {
        if (depth_ > 0) {
            depth_--;
        }
    }

    int32_t depth_ = 0;
    bool exceeded_depth_ = false;
    std::string parse_error_;
};

void
ValidateUBJDepth(const std::vector<uint8_t>& data) {
    UBJDepthGuardSax sax;
    if (Json::sax_parse(data.begin(),
                        data.end(),
                        &sax,
                        Json::input_format_t::ubjson,
                        true)) {
        return;
    }
    if (sax.ExceededDepth()) {
        ThrowInfo(milvus::InvalidParameter,
                  "xgboost: UBJ model nesting depth exceeds limit {}",
                  kMaxXGBoostUBJDepth);
    }
    ThrowInfo(milvus::InvalidParameter,
              "xgboost: failed to parse UBJ model: {}",
              sax.ParseError());
}

std::string
XGBoostModel::GetObjectiveName(const Json& learner) {
    auto objective_it = learner.find("objective");
    if (objective_it == learner.end() || !objective_it->is_object()) {
        ThrowInfo(milvus::InvalidParameter,
                  "xgboost: UBJ model missing objective metadata");
    }
    auto name = JsonString(*objective_it, "name");
    if (name.empty()) {
        ThrowInfo(milvus::InvalidParameter,
                  "xgboost: UBJ model missing objective name");
    }
    return name;
}

void
XGBoostModel::ValidateObjective(const std::string& objective) {
    if (objective != "reg:squarederror" && objective != "binary:logistic") {
        ThrowInfo(milvus::InvalidParameter,
                  "xgboost: unsupported objective '{}'",
                  objective);
    }
}

const Json&
XGBoostModel::GetGradientBooster(const Json& learner) {
    auto booster_it = learner.find("gradient_booster");
    if (booster_it == learner.end() || !booster_it->is_object()) {
        ThrowInfo(milvus::InvalidParameter,
                  "xgboost: UBJ model missing gradient_booster metadata");
    }

    auto name = JsonString(*booster_it, "name");
    if (name.empty()) {
        auto model_it = booster_it->find("model");
        if (model_it != booster_it->end() && model_it->is_object()) {
            name = JsonString(*model_it, "name");
        }
    }
    if (!name.empty() && name != "gbtree") {
        ThrowInfo(milvus::InvalidParameter,
                  "xgboost: unsupported booster '{}'",
                  name);
    }

    auto model_it = booster_it->find("model");
    if (model_it != booster_it->end() && model_it->is_object()) {
        return *model_it;
    }
    return *booster_it;
}

float
XGBoostModel::ParseBaseScore(const Json& learner_param,
                             const std::string& objective) {
    auto base_score = JsonString(learner_param, "base_score", "0");
    auto parsed = ParseFloatValue(base_score, "base_score");
    if (objective == "binary:logistic") {
        if (parsed <= 0.0f || parsed >= 1.0f) {
            if (parsed == 0.0f) {
                return 0.0f;
            }
            ThrowInfo(milvus::InvalidParameter,
                      "xgboost: invalid binary logistic base_score '{}'",
                      base_score);
        }
        return std::log(parsed / (1.0f - parsed));
    }
    return parsed;
}

void
XGBoostModel::ValidateBoosterModel(const Json& booster_model) {
    auto gbtree_param_it = booster_model.find("gbtree_model_param");
    if (gbtree_param_it != booster_model.end() &&
        gbtree_param_it->is_object()) {
        auto num_parallel_tree =
            JsonString(*gbtree_param_it, "num_parallel_tree", "1");
        if (num_parallel_tree != "1") {
            ThrowInfo(milvus::InvalidParameter,
                      "xgboost: unsupported num_parallel_tree '{}'",
                      num_parallel_tree);
        }
    }

    auto trees_it = booster_model.find("trees");
    if (trees_it == booster_model.end() || !trees_it->is_array()) {
        ThrowInfo(milvus::InvalidParameter,
                  "xgboost: UBJ model missing gbtree trees");
    }
    for (const auto& tree : *trees_it) {
        auto categories_it = tree.find("categories");
        if (categories_it != tree.end() && categories_it->is_array() &&
            !categories_it->empty()) {
            ThrowInfo(milvus::InvalidParameter,
                      "xgboost: categorical split is not supported");
        }
        auto split_type_it = tree.find("split_type");
        if (split_type_it != tree.end() && split_type_it->is_array()) {
            for (const auto& split_type : *split_type_it) {
                if (JsonInt32(split_type, "split_type") != 0) {
                    ThrowInfo(milvus::InvalidParameter,
                              "xgboost: categorical split is not supported");
                }
            }
        }
    }
}

void
XGBoostModel::CheckArraySize(const Json& array,
                             size_t expected,
                             const char* key) {
    if (array.size() != expected) {
        ThrowInfo(milvus::InvalidParameter,
                  "xgboost: tree array '{}' has {} entries, expected {}",
                  key,
                  array.size(),
                  expected);
    }
}

XGBoostTree
XGBoostModel::ParseTree(const Json& tree, int32_t model_num_features) {
    const auto& left_children = RequiredArray(tree, "left_children");
    const auto& right_children = RequiredArray(tree, "right_children");
    const auto& split_indices = RequiredArray(tree, "split_indices");
    const auto& split_conditions = RequiredArray(tree, "split_conditions");
    const auto& default_left = RequiredArray(tree, "default_left");

    auto num_nodes = left_children.size();
    if (num_nodes == 0) {
        ThrowInfo(milvus::InvalidParameter, "xgboost: tree has no nodes");
    }

    auto tree_param_it = tree.find("tree_param");
    if (tree_param_it != tree.end() && tree_param_it->is_object()) {
        auto expected_num_nodes =
            JsonString(*tree_param_it, "num_nodes", std::to_string(num_nodes));
        auto parsed_num_nodes =
            ParseNonNegativeInt32(expected_num_nodes, "tree_param.num_nodes");
        if (static_cast<size_t>(parsed_num_nodes) != num_nodes) {
            ThrowInfo(
                milvus::InvalidParameter,
                "xgboost: tree_param.num_nodes {} does not match {} nodes",
                parsed_num_nodes,
                num_nodes);
        }
        auto size_leaf_vector =
            JsonString(*tree_param_it, "size_leaf_vector", "0");
        auto parsed_size_leaf_vector = ParseNonNegativeInt32(
            size_leaf_vector, "tree_param.size_leaf_vector");
        if (parsed_size_leaf_vector > 1) {
            ThrowInfo(
                milvus::InvalidParameter,
                "xgboost: multi-target leaf vector model is not supported");
        }
    }

    CheckArraySize(right_children, num_nodes, "right_children");
    CheckArraySize(split_indices, num_nodes, "split_indices");
    CheckArraySize(split_conditions, num_nodes, "split_conditions");
    CheckArraySize(default_left, num_nodes, "default_left");

    XGBoostTree parsed_tree;
    parsed_tree.left_children.resize(num_nodes);
    parsed_tree.right_children.resize(num_nodes);
    parsed_tree.split_indices.resize(num_nodes);
    parsed_tree.split_conditions.resize(num_nodes);
    parsed_tree.default_left.resize(num_nodes);
    for (size_t node_id = 0; node_id < num_nodes; node_id++) {
        auto left_child = JsonInt32(left_children[node_id], "left_children");
        auto right_child = JsonInt32(right_children[node_id], "right_children");
        auto is_leaf = left_child < 0 && right_child < 0;
        if ((left_child < 0) != (right_child < 0)) {
            ThrowInfo(milvus::InvalidParameter,
                      "xgboost: node {} has only one child",
                      node_id);
        }
        if (!is_leaf) {
            if (left_child >= static_cast<int32_t>(num_nodes) ||
                right_child >= static_cast<int32_t>(num_nodes)) {
                ThrowInfo(milvus::InvalidParameter,
                          "xgboost: node {} child is out of range",
                          node_id);
            }
        }

        auto split_index = JsonInt32(split_indices[node_id], "split_indices");
        if (!is_leaf &&
            (split_index < 0 || split_index >= model_num_features)) {
            ThrowInfo(milvus::InvalidParameter,
                      "xgboost: node {} split index {} exceeds num_feature {}",
                      node_id,
                      split_index,
                      model_num_features);
        }

        parsed_tree.left_children[node_id] = left_child;
        parsed_tree.right_children[node_id] = right_child;
        parsed_tree.split_indices[node_id] = split_index;
        parsed_tree.split_conditions[node_id] =
            JsonFloat(split_conditions[node_id], "split_conditions");
        parsed_tree.default_left[node_id] =
            JsonBool(default_left[node_id], "default_left") ? 1 : 0;
    }
    return parsed_tree;
}

std::vector<XGBoostTree>
XGBoostModel::ParseTrees(const Json& booster_model,
                         int32_t model_num_features) {
    const auto& trees = RequiredArray(booster_model, "trees");
    std::vector<XGBoostTree> parsed_trees;
    parsed_trees.reserve(trees.size());
    for (const auto& tree : trees) {
        if (!tree.is_object()) {
            ThrowInfo(milvus::InvalidParameter,
                      "xgboost: tree entry must be object");
        }
        parsed_trees.push_back(ParseTree(tree, model_num_features));
    }
    return parsed_trees;
}

float
Sigmoid(float value) {
    if (value >= 0.0f) {
        auto z = std::exp(-value);
        return 1.0f / (1.0f + z);
    }
    auto z = std::exp(value);
    return z / (1.0f + z);
}

bool
IsArrowNull(const ArrowFeatureColumn& column, int64_t row) {
    if (column.null_count == 0 || column.validity == nullptr) {
        return false;
    }
    auto index = row + column.offset;
    auto byte = column.validity[index / 8];
    auto mask = static_cast<uint8_t>(1U << (index % 8));
    return (byte & mask) == 0;
}

template <typename T>
float
GetArrowFeatureValue(const ArrowFeatureColumn& column, int64_t row) {
    auto index = row + column.offset;
    return static_cast<float>(static_cast<const T*>(column.values)[index]);
}

template <>
float
GetArrowFeatureValue<float>(const ArrowFeatureColumn& column, int64_t row) {
    auto index = row + column.offset;
    return static_cast<const float*>(column.values)[index];
}

float
ArrowFeatureColumn::Get(int64_t row) const {
    if (IsArrowNull(*this, row)) {
        return std::numeric_limits<float>::quiet_NaN();
    }
    if (getter == nullptr) {
        ThrowInfo(milvus::InvalidParameter,
                  "xgboost: Arrow feature getter is nil");
    }
    return getter(*this, row);
}

ArrowFeatureColumn
CompileFeatureColumn(const ArrowArray& array,
                     const ArrowSchema& schema,
                     int32_t col) {
    if (array.length < 0) {
        ThrowInfo(milvus::InvalidParameter,
                  "xgboost: feature column {} length must be non-negative",
                  col);
    }
    if (array.offset < 0) {
        ThrowInfo(milvus::InvalidParameter,
                  "xgboost: feature column {} offset must be non-negative",
                  col);
    }
    if (array.n_buffers < 2 || array.buffers == nullptr ||
        array.buffers[1] == nullptr) {
        ThrowInfo(milvus::InvalidParameter,
                  "xgboost: feature column {} missing value buffer",
                  col);
    }
    if (schema.format == nullptr || schema.format[0] == '\0' ||
        schema.format[1] != '\0') {
        ThrowInfo(milvus::InvalidParameter,
                  "xgboost: feature column {} unsupported Arrow format '{}'; "
                  "expected numeric scalar",
                  col,
                  schema.format == nullptr ? "" : schema.format);
    }

    ArrowFeatureColumn column;
    column.values = array.buffers[1];
    column.validity = array.buffers[0] == nullptr
                          ? nullptr
                          : static_cast<const uint8_t*>(array.buffers[0]);
    column.offset = array.offset;
    column.null_count = array.null_count;
    column.format = schema.format[0];
    switch (column.format) {
        case 'c':
            column.getter = GetArrowFeatureValue<int8_t>;
            break;
        case 's':
            column.getter = GetArrowFeatureValue<int16_t>;
            break;
        case 'i':
            column.getter = GetArrowFeatureValue<int32_t>;
            break;
        case 'l':
            column.getter = GetArrowFeatureValue<int64_t>;
            break;
        case 'f':
            column.getter = GetArrowFeatureValue<float>;
            break;
        case 'g':
            column.getter = GetArrowFeatureValue<double>;
            break;
        default:
            ThrowInfo(
                milvus::InvalidParameter,
                "xgboost: feature column {} unsupported Arrow format '{}'",
                col,
                schema.format);
    }
    return column;
}

std::vector<ArrowFeatureColumn>
CompileFeatureColumns(const ArrowArray* arrays,
                      const ArrowSchema* schemas,
                      int32_t num_features,
                      int64_t expected_rows) {
    std::vector<ArrowFeatureColumn> columns;
    columns.reserve(num_features);
    for (int32_t col = 0; col < num_features; col++) {
        const auto& array = arrays[col];
        if (array.length != expected_rows) {
            ThrowInfo(milvus::InvalidParameter,
                      "xgboost: feature column {} has {} rows, expected {}",
                      col,
                      array.length,
                      expected_rows);
        }
        columns.push_back(CompileFeatureColumn(array, schemas[col], col));
    }
    return columns;
}

void
XGBoostModel::AddTreeContributionBatch(
    const XGBoostTree& tree,
    const std::vector<ArrowFeatureColumn>& columns,
    int64_t num_rows,
    std::vector<int32_t>& nodes,
    float* output) {
    std::fill(nodes.begin(), nodes.end(), 0);
    const auto num_nodes = static_cast<int32_t>(tree.left_children.size());

    for (int32_t step = 0; step <= num_nodes; step++) {
        bool all_leaf = true;
        for (int64_t row = 0; row < num_rows; row++) {
            auto node_id = nodes[row];
            if (node_id < 0 || node_id >= num_nodes) {
                ThrowInfo(milvus::InvalidParameter,
                          "xgboost: tree traversal reached invalid node {}",
                          node_id);
            }
            if (tree.left_children[node_id] >= 0) {
                all_leaf = false;
                break;
            }
        }
        if (all_leaf) {
            for (int64_t row = 0; row < num_rows; row++) {
                output[row] += tree.split_conditions[nodes[row]];
            }
            return;
        }
        if (step == num_nodes) {
            ThrowInfo(milvus::InvalidParameter,
                      "xgboost: tree traversal exceeded node count");
        }

        for (int64_t row = 0; row < num_rows; row++) {
            auto node_id = nodes[row];
            if (tree.left_children[node_id] < 0) {
                continue;
            }
            auto split_index = tree.split_indices[node_id];
            auto value = columns[split_index].Get(row);
            bool go_left;
            if (std::isnan(value)) {
                go_left = tree.default_left[node_id] != 0;
            } else {
                go_left = value < tree.split_conditions[node_id];
            }
            nodes[row] = go_left ? tree.left_children[node_id]
                                 : tree.right_children[node_id];
        }
    }
}

XGBoostModel::XGBoostModel(int32_t num_features,
                           std::string objective,
                           float base_score,
                           std::vector<XGBoostTree> trees)
    : num_features_(num_features),
      objective_(std::move(objective)),
      base_score_(base_score),
      trees_(std::move(trees)) {
}

std::unique_ptr<XGBoostModel>
XGBoostModel::LoadUBJ(const char* path) {
    if (path == nullptr || path[0] == '\0') {
        ThrowInfo(milvus::InvalidParameter, "xgboost: model path is empty");
    }

    std::ifstream input(path, std::ios::binary);
    if (!input.is_open()) {
        ThrowInfo(milvus::FileOpenFailed,
                  "xgboost: failed to open UBJ model file '{}'",
                  path);
    }
    std::vector<uint8_t> data((std::istreambuf_iterator<char>(input)),
                              std::istreambuf_iterator<char>());
    if (input.bad()) {
        ThrowInfo(milvus::FileReadFailed,
                  "xgboost: failed to read UBJ model file '{}'",
                  path);
    }
    if (data.empty()) {
        ThrowInfo(milvus::InvalidParameter, "xgboost: UBJ model file is empty");
    }

    ValidateUBJDepth(data);

    Json root;
    try {
        root = Json::from_ubjson(data);
    } catch (std::exception& e) {
        ThrowInfo(milvus::InvalidParameter,
                  "xgboost: failed to parse UBJ model: {}",
                  e.what());
    }
    if (!root.is_object()) {
        ThrowInfo(milvus::InvalidParameter,
                  "xgboost: UBJ model top-level value must be object");
    }

    const auto& learner = RequiredObject(root, "learner");
    const auto& learner_param = RequiredObject(learner, "learner_model_param");
    auto num_feature_value = JsonString(learner_param, "num_feature");
    if (num_feature_value.empty()) {
        ThrowInfo(milvus::InvalidParameter,
                  "xgboost: UBJ model missing num_feature");
    }
    auto num_features = ParseNonNegativeInt32(num_feature_value, "num_feature");

    auto num_class_value = JsonString(learner_param, "num_class", "0");
    auto num_class = ParseNonNegativeInt32(num_class_value, "num_class");
    if (num_class > 1) {
        ThrowInfo(milvus::InvalidParameter,
                  "xgboost: multiclass model is not supported");
    }

    auto objective = GetObjectiveName(learner);
    ValidateObjective(objective);
    const auto& booster_model = GetGradientBooster(learner);
    ValidateBoosterModel(booster_model);

    auto base_score = ParseBaseScore(learner_param, objective);
    auto trees = ParseTrees(booster_model, num_features);
    return std::make_unique<XGBoostModel>(
        num_features, std::move(objective), base_score, std::move(trees));
}

void
XGBoostModel::Predict(const CXGBoostPredictRequest& request) const {
    if (request.num_features < 0) {
        ThrowInfo(milvus::InvalidParameter,
                  "xgboost: num_features must be non-negative");
    }
    if (num_features_ > 0 && request.num_features != num_features_) {
        ThrowInfo(milvus::InvalidParameter,
                  "xgboost: expected {} feature columns, got {}",
                  num_features_,
                  request.num_features);
    }
    if (request.num_features > 0 && request.feature_arrays == nullptr) {
        ThrowInfo(milvus::InvalidParameter,
                  "xgboost: feature ArrowArrays is nil");
    }
    if (request.num_features > 0 && request.feature_schemas == nullptr) {
        ThrowInfo(milvus::InvalidParameter,
                  "xgboost: feature ArrowSchemas is nil");
    }

    int64_t num_rows = 0;
    if (request.num_features > 0) {
        num_rows = request.feature_arrays[0].length;
        if (num_rows < 0) {
            ThrowInfo(milvus::InvalidParameter,
                      "xgboost: ArrowArray length must be non-negative");
        }
    }
    if (num_rows > 0 && request.output == nullptr) {
        ThrowInfo(milvus::InvalidParameter, "xgboost: output is nil");
    }
    if (num_rows == 0) {
        return;
    }

    auto columns = CompileFeatureColumns(request.feature_arrays,
                                         request.feature_schemas,
                                         request.num_features,
                                         num_rows);
    std::fill(request.output, request.output + num_rows, base_score_);
    std::vector<int32_t> nodes(static_cast<size_t>(num_rows));
    for (const auto& tree : trees_) {
        AddTreeContributionBatch(
            tree, columns, num_rows, nodes, request.output);
    }
    TransformOutputBatch(num_rows, request.output_default, request.output);
}

void
XGBoostModel::TransformOutputBatch(int64_t num_rows,
                                   bool output_default,
                                   float* output) const {
    if (!output_default || objective_ != "binary:logistic") {
        return;
    }
    for (int64_t row = 0; row < num_rows; row++) {
        output[row] = Sigmoid(output[row]);
    }
}

}  // namespace

CXGBoostLoadModelResult
LoadXGBoostUBJModel(const char* path) {
    CXGBoostLoadModelResult result{};
    try {
        auto model = XGBoostModel::LoadUBJ(path);
        result.num_features = model->NumFeatures();
        result.model = static_cast<CXGBoostModel>(model.release());
        result.status = milvus::SuccessCStatus();
        return result;
    } catch (std::exception& e) {
        result.status = milvus::FailureCStatus(&e);
        result.model = nullptr;
        result.num_features = 0;
        return result;
    }
}

CStatus
PredictXGBoost(CXGBoostPredictRequest request) {
    try {
        auto model = static_cast<XGBoostModel*>(request.model);
        if (model == nullptr) {
            ThrowInfo(milvus::InvalidParameter, "xgboost: model is nil");
        }
        model->Predict(request);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
DeleteXGBoostModel(CXGBoostModel model) {
    try {
        if (model == nullptr) {
            return milvus::SuccessCStatus();
        }
        delete static_cast<XGBoostModel*>(model);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}
