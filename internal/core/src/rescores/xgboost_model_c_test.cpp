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
#include <gtest/gtest.h>

#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "nlohmann/json.hpp"

namespace {

using Json = nlohmann::json;

std::filesystem::path
TempModelPath(const std::string& name) {
    auto dir =
        std::filesystem::temp_directory_path() / "milvus_xgboost_model_test";
    std::filesystem::create_directories(dir);
    return dir / name;
}

Json
SingleTree() {
    return Json{
        {"base_weights", Json::array({0.0, 1.5, -0.5})},
        {"categories", Json::array()},
        {"default_left", Json::array({1, 0, 0})},
        {"left_children", Json::array({1, -1, -1})},
        {"right_children", Json::array({2, -1, -1})},
        {"split_conditions", Json::array({0.5, 1.5, -0.5})},
        {"split_indices", Json::array({0, 0, 0})},
        {"split_type", Json::array({0, 0, 0})},
        {"tree_param", Json{{"num_nodes", "3"}, {"size_leaf_vector", "0"}}}};
}

Json
MinimalModel(const std::string& objective = "binary:logistic",
             const std::string& booster = "gbtree",
             const std::string& num_feature = "3") {
    Json model;
    model["learner"]["learner_model_param"] = Json{{"base_score", "0.5"},
                                                   {"num_feature", num_feature},
                                                   {"num_class", "0"}};
    model["learner"]["objective"] = Json{{"name", objective}};
    model["learner"]["gradient_booster"]["name"] = booster;
    model["learner"]["gradient_booster"]["model"]["gbtree_model_param"] =
        Json{{"num_parallel_tree", "1"}};
    model["learner"]["gradient_booster"]["model"]["trees"] =
        Json::array({SingleTree()});
    return model;
}

void
WriteUBJ(const std::filesystem::path& path, const Json& model) {
    auto data = Json::to_ubjson(model);
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output.write(reinterpret_cast<const char*>(data.data()), data.size());
}

void
WriteText(const std::filesystem::path& path, const std::string& value) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output << value;
}

void
FreeStatus(CStatus* status) {
    if (status->error_code != 0 && status->error_msg != nullptr) {
        free(const_cast<char*>(status->error_msg));
    }
}

void
ExpectLoadFails(const std::filesystem::path& path) {
    auto result = LoadXGBoostUBJModel(path.c_str());
    EXPECT_NE(result.status.error_code, 0);
    EXPECT_EQ(result.model, nullptr);
    FreeStatus(&result.status);
}

void
ExpectLoadFailsWithMessage(const std::filesystem::path& path,
                           const std::string& message) {
    auto result = LoadXGBoostUBJModel(path.c_str());
    EXPECT_NE(result.status.error_code, 0);
    EXPECT_EQ(result.model, nullptr);
    ASSERT_NE(result.status.error_msg, nullptr);
    EXPECT_NE(std::string(result.status.error_msg).find(message),
              std::string::npos)
        << result.status.error_msg;
    FreeStatus(&result.status);
}

Json
DeeplyNestedArray(int depth) {
    Json value = nullptr;
    for (int i = 0; i < depth; i++) {
        value = Json::array({value});
    }
    return value;
}

CXGBoostLoadModelResult
LoadModelForTest(const std::filesystem::path& path) {
    auto result = LoadXGBoostUBJModel(path.c_str());
    EXPECT_EQ(result.status.error_code, 0) << result.status.error_msg;
    EXPECT_NE(result.model, nullptr);
    FreeStatus(&result.status);
    return result;
}

void
DeleteModelForTest(CXGBoostModel model) {
    auto status = DeleteXGBoostModel(model);
    EXPECT_EQ(status.error_code, 0) << status.error_msg;
    FreeStatus(&status);
}

ArrowSchema
Float32Schema() {
    ArrowSchema schema{};
    schema.format = "f";
    return schema;
}

ArrowArray
Float32Array(const std::vector<float>& values,
             const uint8_t* validity = nullptr) {
    ArrowArray array{};
    array.length = values.size();
    array.null_count = validity == nullptr ? 0 : -1;
    array.offset = 0;
    array.n_buffers = 2;
    auto buffers = new const void*[2];
    buffers[0] = validity;
    buffers[1] = values.data();
    array.buffers = buffers;
    return array;
}

void
FreeArrowArrayBuffers(ArrowArray* array) {
    delete[] array->buffers;
    array->buffers = nullptr;
}

TEST(XGBoostModelCTest, LoadUBJModelMetadata) {
    auto path = TempModelPath("load_metadata.ubj");
    WriteUBJ(path, MinimalModel());

    auto result = LoadModelForTest(path);
    EXPECT_EQ(result.num_features, 3);
    DeleteModelForTest(result.model);
}

TEST(XGBoostModelCTest, PredictRawScore) {
    auto path = TempModelPath("predict_raw.ubj");
    WriteUBJ(path, MinimalModel("reg:squarederror"));
    auto result = LoadModelForTest(path);

    std::vector<float> f0{0.1f, 0.8f};
    std::vector<float> f1{0.0f, 0.0f};
    std::vector<float> f2{0.0f, 0.0f};
    ArrowSchema schemas[] = {Float32Schema(), Float32Schema(), Float32Schema()};
    ArrowArray arrays[] = {
        Float32Array(f0), Float32Array(f1), Float32Array(f2)};
    std::vector<float> output(2);
    auto status = PredictXGBoost(CXGBoostPredictRequest{
        result.model, arrays, schemas, 3, true, output.data()});
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    FreeStatus(&status);
    EXPECT_FLOAT_EQ(output[0], 2.0f);
    EXPECT_FLOAT_EQ(output[1], 0.0f);
    for (auto& array : arrays) {
        FreeArrowArrayBuffers(&array);
    }
    DeleteModelForTest(result.model);
}

TEST(XGBoostModelCTest, PredictBinaryLogisticDefaultOutput) {
    auto path = TempModelPath("predict_logistic.ubj");
    WriteUBJ(path, MinimalModel("binary:logistic"));
    auto result = LoadModelForTest(path);

    std::vector<float> f0{0.8f};
    std::vector<float> f1{0.0f};
    std::vector<float> f2{0.0f};
    ArrowSchema schemas[] = {Float32Schema(), Float32Schema(), Float32Schema()};
    ArrowArray arrays[] = {
        Float32Array(f0), Float32Array(f1), Float32Array(f2)};
    std::vector<float> output(1);
    auto status = PredictXGBoost(CXGBoostPredictRequest{
        result.model, arrays, schemas, 3, true, output.data()});
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    FreeStatus(&status);
    EXPECT_NEAR(output[0], 1.0f / (1.0f + std::exp(0.5f)), 1e-6);
    for (auto& array : arrays) {
        FreeArrowArrayBuffers(&array);
    }
    DeleteModelForTest(result.model);
}

TEST(XGBoostModelCTest, PredictMissingValueUsesDefaultDirection) {
    auto path = TempModelPath("predict_missing.ubj");
    WriteUBJ(path, MinimalModel("reg:squarederror"));
    auto result = LoadModelForTest(path);

    uint8_t validity[] = {0b00000110};
    std::vector<float> f0{0.0f};
    std::vector<float> f1{0.0f};
    std::vector<float> f2{0.0f};
    ArrowSchema schemas[] = {Float32Schema(), Float32Schema(), Float32Schema()};
    ArrowArray arrays[] = {
        Float32Array(f0, validity), Float32Array(f1), Float32Array(f2)};
    std::vector<float> output(1);
    auto status = PredictXGBoost(CXGBoostPredictRequest{
        result.model, arrays, schemas, 3, true, output.data()});
    ASSERT_EQ(status.error_code, 0) << status.error_msg;
    FreeStatus(&status);
    EXPECT_FLOAT_EQ(output[0], 2.0f);
    for (auto& array : arrays) {
        FreeArrowArrayBuffers(&array);
    }
    DeleteModelForTest(result.model);
}

TEST(XGBoostModelCTest, RejectsNonUBJContent) {
    auto path = TempModelPath("not_ubj.ubj");
    WriteText(path, R"({"learner":{}})");
    ExpectLoadFails(path);
}

TEST(XGBoostModelCTest, RejectsDeeplyNestedUBJ) {
    auto path = TempModelPath("deeply_nested.ubj");
    WriteUBJ(path, DeeplyNestedArray(129));
    ExpectLoadFailsWithMessage(path, "nesting depth exceeds limit 128");
}

TEST(XGBoostModelCTest, RejectsUnsupportedObjective) {
    auto path = TempModelPath("unsupported_objective.ubj");
    WriteUBJ(path, MinimalModel("rank:pairwise"));
    ExpectLoadFails(path);
}

TEST(XGBoostModelCTest, RejectsUnsupportedBooster) {
    auto path = TempModelPath("unsupported_booster.ubj");
    WriteUBJ(path, MinimalModel("binary:logistic", "gblinear"));
    ExpectLoadFails(path);
}

TEST(XGBoostModelCTest, AcceptsSingleTargetLeafVector) {
    auto model = MinimalModel();
    model["learner"]["gradient_booster"]["model"]["trees"][0]["tree_param"]
         ["size_leaf_vector"] = "1";
    auto path = TempModelPath("single_target_leaf_vector.ubj");
    WriteUBJ(path, model);

    auto result = LoadModelForTest(path);
    DeleteModelForTest(result.model);
}

TEST(XGBoostModelCTest, RejectsMultiTargetLeafVector) {
    auto model = MinimalModel();
    model["learner"]["gradient_booster"]["model"]["trees"][0]["tree_param"]
         ["size_leaf_vector"] = "2";
    auto path = TempModelPath("multi_target_leaf_vector.ubj");
    WriteUBJ(path, model);
    ExpectLoadFails(path);
}

TEST(XGBoostModelCTest, RejectsMulticlass) {
    auto model = MinimalModel();
    model["learner"]["learner_model_param"]["num_class"] = "3";
    auto path = TempModelPath("multiclass.ubj");
    WriteUBJ(path, model);
    ExpectLoadFails(path);
}

}  // namespace
