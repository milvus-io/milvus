// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the License.

#include <string>

#include "common/QueryInfo.h"
#include "common/Types.h"
#include "gtest/gtest.h"
#include "knowhere/config.h"
#include "knowhere/index/index_static.h"
#include "knowhere/operands.h"
#include "knowhere/version.h"
#include "nlohmann/json.hpp"
#include "segcore/SearchParamsValidator.h"

using namespace milvus;

namespace {
// Returns true iff ValidateVectorSearchParams threw whose what() contains
// substr. Catches std::exception so it is robust to the exact exception type.
bool
ThrewWith(const std::string& index_type,
          DataType dt,
          const std::string& params_json,
          const std::string& substr) {
    SearchInfo info;
    info.metric_type_ = knowhere::metric::L2;
    info.topk_ = 10;
    info.search_params_ = knowhere::Json::parse(params_json);
    try {
        segcore::ValidateVectorSearchParams(info, index_type, dt);
        return false;
    } catch (const std::exception& e) {
        return std::string(e.what()).find(substr) != std::string::npos;
    }
}

void
NoThrow(const std::string& index_type,
        DataType dt,
        const std::string& params_json) {
    SearchInfo info;
    info.metric_type_ = knowhere::metric::L2;
    info.topk_ = 10;
    info.search_params_ = knowhere::Json::parse(params_json);
    EXPECT_NO_THROW(segcore::ValidateVectorSearchParams(info, index_type, dt))
        << "index_type=" << index_type << " params=" << params_json;
}
}  // namespace

// Sweep: an empty/minimal param set must still load for every common index
// family. This is what makes routing every search through knowhere Config::Load
// safe — no family regresses.
TEST(ValidateVectorSearchParams, EmptyParamsLoadAllIndexTypes) {
    const DataType dt = DataType::VECTOR_FLOAT;
    for (const auto* index_type :
         {"IVF_FLAT", "IVF_PQ", "IVF_SQ8", "SCANN", "HNSW", "DISKANN"}) {
        NoThrow(index_type, dt, R"({})");
    }
}

// IVF owns nprobe (knowhere ivf_config.h); invalid values are rejected with
// knowhere's own message, identical to the indexed path.
TEST(ValidateVectorSearchParams, IvfRejectsInvalidNprobe) {
    const DataType dt = DataType::VECTOR_FLOAT;
    // out of range [1, 65536] — including the 0 case from issue #47729
    EXPECT_TRUE(ThrewWith("IVF_FLAT",
                          dt,
                          R"({"nprobe": 0})",
                          "Out of range in json: param 'nprobe' (0)"));
    EXPECT_TRUE(ThrewWith("IVF_FLAT",
                          dt,
                          R"({"nprobe": -1})",
                          "Out of range in json: param 'nprobe' (-1)"));
    EXPECT_TRUE(ThrewWith("IVF_FLAT",
                          dt,
                          R"({"nprobe": 65537})",
                          "Out of range in json: param 'nprobe' (65537)"));
    // string "0" → FormatAndCheck stolls → 0 → out of range, matching the
    // indexed path (config.cc string-to-int handling).
    EXPECT_TRUE(ThrewWith("IVF_FLAT",
                          dt,
                          R"({"nprobe": "0"})",
                          "Out of range in json: param 'nprobe' (0)"));
    // wrong JSON types → type conflict
    EXPECT_TRUE(ThrewWith("IVF_FLAT",
                          dt,
                          R"({"nprobe": null})",
                          "Type conflict in json: param 'nprobe' (null)"));
    EXPECT_TRUE(ThrewWith("IVF_FLAT",
                          dt,
                          R"({"nprobe": true})",
                          "Type conflict in json: param 'nprobe' (true)"));
    EXPECT_TRUE(ThrewWith("IVF_FLAT",
                          dt,
                          R"({"nprobe": 32.0})",
                          "Type conflict in json: param 'nprobe' (32.0)"));
}

// Valid IVF nprobe passes (including string-form, which knowhere normalizes).
TEST(ValidateVectorSearchParams, IvfAcceptsValidNprobe) {
    const DataType dt = DataType::VECTOR_FLOAT;
    NoThrow("IVF_FLAT", dt, R"({})");
    NoThrow("IVF_FLAT", dt, R"({"nprobe": 1})");
    NoThrow("IVF_FLAT", dt, R"({"nprobe": 8})");
    NoThrow("IVF_FLAT", dt, R"({"nprobe": 65536})");
    NoThrow("IVF_FLAT", dt, R"({"nprobe": "32"})");
}

// Non-IVF families do not declare nprobe; a stray nprobe is ignored exactly
// as today (no collateral damage from routing through knowhere).
TEST(ValidateVectorSearchParams, HnswIgnoresStrayNprobe) {
    const DataType dt = DataType::VECTOR_FLOAT;
    NoThrow("HNSW", dt, R"({})");
    NoThrow("HNSW", dt, R"({"nprobe": 0})");
    NoThrow("HNSW", dt, R"({"nprobe": -1})");
    NoThrow("HNSW", dt, R"({"nprobe": 32})");
    NoThrow("HNSW", dt, R"({"ef": 64})");
}
