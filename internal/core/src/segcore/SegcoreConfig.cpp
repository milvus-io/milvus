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

#include "common/Schema.h"
#include "SegcoreConfig.h"
#include "utils/Json.h"
#include "yaml-cpp/yaml.h"

namespace milvus::segcore {

static YAML::Node
subnode(const YAML::Node& parent, const std::string& key) {
    AssertInfo(parent.IsMap(), "wrong type node when getting key[" + key + "]");
    auto& node = parent[key];
    AssertInfo(node.IsDefined(), "key[" + key + "] not found in sub-node");
    return node;
}

template <typename T, typename Func>
std::vector<T>
apply_parser(const YAML::Node& node, Func func) {
    std::vector<T> results;
    Assert(node.IsDefined());
    if (node.IsScalar()) {
        results.emplace_back(func(node));
    } else if (node.IsSequence()) {
        for (auto& element : node) {
            Assert(element.IsScalar());
            results.emplace_back(func(element));
        }
    } else {
        PanicInfo("node should be scalar or sequence");
    }
    return results;
}

void
SegcoreConfig::parse_from(const std::string& config_path) {
    try {
        YAML::Node top_config = YAML::LoadFile(config_path);
        Assert(top_config.IsMap());
        auto seg_config = subnode(top_config, "segcore");
        auto chunk_size = subnode(seg_config, "chunk_size").as<int64_t>();
        this->size_per_chunk_ = chunk_size;

#if 0
        auto index_list = subnode(seg_config, "small_index");

        Assert(index_list.IsSequence());
        for (auto index : index_list) {
            Assert(index.IsMap());
            auto metric_types = apply_parser<MetricType>(subnode(index, "metric_type"), [](const YAML::Node& node) {
                return GetMetricType(node.as<std::string>());
            });

            {
                std::sort(metric_types.begin(), metric_types.end());
                auto end_iter = std::unique(metric_types.begin(), metric_types.end());
                metric_types.resize(end_iter - metric_types.begin());
            }

            auto index_type = index["index_type"].as<std::string>();
            AssertInfo(index_type == "IVF", "only ivf is supported now");

            SmallIndexConf conf;
            conf.index_type = index_type;

            // parse build config
            for (auto node : index["build_params"]) {
                // TODO: currently support IVF only
                auto key = node.first.as<std::string>();
                Assert(key == "nlist");
                auto value = node.second.as<int64_t>();
                conf.build_params[key] = value;
            }

            // parse search config
            for (auto node : index["search_params"]) {
                // TODO: currently support IVF only
                auto key = node.first.as<std::string>();
                Assert(key == "nprobe");
                auto value = node.second.as<int64_t>();
                conf.search_params[key] = value;
            }

            for (auto metric_type : metric_types) {
                Assert(result.table_.count(metric_type) == 0);
                result.table_[metric_type] = conf;
            }
        }
#endif
    } catch (const SegcoreError& e) {
        throw e;
    } catch (const std::exception& e) {
        std::string str = std::string("Invalid Yaml: ") + config_path + ", err: " + e.what();
        PanicInfo(str);
    }
}

}  // namespace milvus::segcore
