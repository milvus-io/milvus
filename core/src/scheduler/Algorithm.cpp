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

#include "scheduler/Algorithm.h"

#include <limits>
#include <unordered_map>
#include <utility>

namespace milvus {
namespace scheduler {

constexpr uint64_t MAXINT = std::numeric_limits<uint32_t>::max();

uint64_t
ShortestPath(const ResourcePtr& src, const ResourcePtr& dest, const ResourceMgrPtr& res_mgr,
             std::vector<std::string>& path) {
    uint64_t num_of_resources = res_mgr->GetAllResources().size();
    std::unordered_map<uint64_t, std::string> id_name_map;
    std::unordered_map<std::string, uint64_t> name_id_map;
    for (uint64_t i = 0; i < num_of_resources; ++i) {
        id_name_map.insert(std::make_pair(i, res_mgr->GetAllResources().at(i)->name()));
        name_id_map.insert(std::make_pair(res_mgr->GetAllResources().at(i)->name(), i));
    }

    std::vector<std::vector<uint64_t>> dis_matrix;
    dis_matrix.resize(num_of_resources);
    for (uint64_t i = 0; i < num_of_resources; ++i) {
        dis_matrix[i].resize(num_of_resources);
        for (uint64_t j = 0; j < num_of_resources; ++j) {
            dis_matrix[i][j] = MAXINT;
        }
        dis_matrix[i][i] = 0;
    }

    std::vector<bool> vis(num_of_resources, false);
    std::vector<uint64_t> dis(num_of_resources, MAXINT);
    for (auto& res : res_mgr->GetAllResources()) {
        auto cur_node = std::static_pointer_cast<Node>(res);
        auto cur_neighbours = cur_node->GetNeighbours();

        for (auto& neighbour : cur_neighbours) {
            auto neighbour_res = std::static_pointer_cast<Resource>(neighbour.neighbour_node);
            dis_matrix[name_id_map.at(res->name())][name_id_map.at(neighbour_res->name())] =
                neighbour.connection.transport_cost();
        }
    }

    for (uint64_t i = 0; i < num_of_resources; ++i) {
        dis[i] = dis_matrix[name_id_map.at(src->name())][i];
    }

    vis[name_id_map.at(src->name())] = true;
    std::vector<int64_t> parent(num_of_resources, -1);

    for (uint64_t i = 0; i < num_of_resources; ++i) {
        uint64_t minn = MAXINT;
        uint64_t temp = 0;
        for (uint64_t j = 0; j < num_of_resources; ++j) {
            if (!vis[j] && dis[j] < minn) {
                minn = dis[j];
                temp = j;
            }
        }
        vis[temp] = true;

        if (i == 0) {
            parent[temp] = name_id_map.at(src->name());
        }

        for (uint64_t j = 0; j < num_of_resources; ++j) {
            if (!vis[j] && dis_matrix[temp][j] != MAXINT && dis_matrix[temp][j] + dis[temp] < dis[j]) {
                dis[j] = dis_matrix[temp][j] + dis[temp];
                parent[j] = temp;
            }
        }
    }

    int64_t parent_idx = parent[name_id_map.at(dest->name())];
    if (parent_idx != -1) {
        path.push_back(dest->name());
    }
    while (parent_idx != -1) {
        path.push_back(id_name_map.at(parent_idx));
        parent_idx = parent[parent_idx];
    }
    return dis[name_id_map.at(dest->name())];
}

}  // namespace scheduler
}  // namespace milvus
