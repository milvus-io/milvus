/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "Algorithm.h"

namespace zilliz {
namespace milvus {
namespace engine {

constexpr uint64_t MAXINT = 99999;

uint64_t
ShortestPath(const ResourcePtr &src,
             const ResourcePtr &dest,
             const ResourceMgrPtr &res_mgr,
             std::vector<std::string> &path) {

    std::vector<std::vector<std::string>> paths;

    uint64_t num_of_resources = res_mgr->GetAllResouces().size();
    std::unordered_map<uint64_t, std::string> id_name_map;
    std::unordered_map<std::string, uint64_t> name_id_map;
    for (uint64_t i = 0; i < num_of_resources; ++i) {
        id_name_map.insert(std::make_pair(i, res_mgr->GetAllResouces().at(i)->Name()));
        name_id_map.insert(std::make_pair(res_mgr->GetAllResouces().at(i)->Name(), i));
    }

    std::vector<std::vector<uint64_t> > dis_matrix;
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
    for (auto &res : res_mgr->GetAllResouces()) {

        auto cur_node = std::static_pointer_cast<Node>(res);
        auto cur_neighbours = cur_node->GetNeighbours();

        for (auto &neighbour : cur_neighbours) {
            auto neighbour_res = std::static_pointer_cast<Resource>(neighbour.neighbour_node.lock());
            dis_matrix[name_id_map.at(res->Name())][name_id_map.at(neighbour_res->Name())] =
                neighbour.connection.transport_cost();
        }
    }

    for (uint64_t i = 0; i < num_of_resources; ++i) {
        dis[i] = dis_matrix[name_id_map.at(src->Name())][i];
    }

    vis[name_id_map.at(src->Name())] = true;
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
            parent[temp] = name_id_map.at(src->Name());
        }

        for (uint64_t j = 0; j < num_of_resources; ++j) {
            if (!vis[j] && dis_matrix[temp][j] != MAXINT && dis_matrix[temp][j] + dis[temp] < dis[j]) {
                dis[j] = dis_matrix[temp][j] + dis[temp];
                parent[j] = temp;
            }
        }
    }

    int64_t parent_idx = parent[name_id_map.at(dest->Name())];
    if (parent_idx != -1) {
        path.push_back(dest->Name());
    }
    while (parent_idx != -1) {
        path.push_back(id_name_map.at(parent_idx));
        parent_idx = parent[parent_idx];
    }
    return dis[name_id_map.at(dest->Name())];
}

}
}
}