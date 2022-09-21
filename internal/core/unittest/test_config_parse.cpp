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

#include <gtest/gtest.h>
#include <knowhere/index/vector_index/helpers/IndexParameter.h>
#include "indexbuilder/parse.h"

TEST(ConfigParse, Parse) {
    using namespace milvus::indexbuilder;
    auto stoi_closure = [](const std::string& s) -> int { return std::stoi(s); };

    knowhere::Config config;
    ASSERT_TRUE(!config.contains(knowhere::meta::SLICE_SIZE));

    ParseConfig<int>(config, knowhere::meta::SLICE_SIZE, stoi_closure, std::nullopt);
    ASSERT_TRUE(!config.contains(knowhere::meta::SLICE_SIZE));

    ParseConfig<int>(config, knowhere::meta::SLICE_SIZE, stoi_closure, std::optional{4});
    ASSERT_EQ(4, config[knowhere::meta::SLICE_SIZE].get<int>());

    config.clear();
    config[knowhere::meta::SLICE_SIZE] = "16";
    ParseConfig<int>(config, knowhere::meta::SLICE_SIZE, stoi_closure, std::optional{4});
    ASSERT_EQ(16, config[knowhere::meta::SLICE_SIZE].get<int>());
}

TEST(ConfigParse, Add) {
    using namespace milvus::indexbuilder;
    using namespace milvus::proto::indexcgo;

    IndexParams indexParams;
    TypeParams typeParams;

    auto indexParam = indexParams.add_params();
    indexParam->set_key("k1");
    indexParam->set_value("v1");

    auto typeParam = typeParams.add_params();
    typeParam->set_key("k2");
    typeParam->set_value("v2");

    knowhere::Config config;
    AddToConfig(config, indexParams);
    AddToConfig(config, typeParams);

    ASSERT_EQ("v1", config["k1"].get<std::string>());
    ASSERT_EQ("v2", config["k2"].get<std::string>());
}
