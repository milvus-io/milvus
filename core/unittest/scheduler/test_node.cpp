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

#include <gtest/gtest.h>
#include "scheduler/resource/Node.h"

namespace {

namespace ms = milvus::scheduler;

}  // namespace

class NodeTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        node1_ = std::make_shared<ms::Node>();
        node2_ = std::make_shared<ms::Node>();
        node3_ = std::make_shared<ms::Node>();
        isolated_node1_ = std::make_shared<ms::Node>();
        isolated_node2_ = std::make_shared<ms::Node>();

        auto pcie = ms::Connection("PCIe", 11.0);

        node1_->AddNeighbour(node2_, pcie);
        node1_->AddNeighbour(node3_, pcie);
        node2_->AddNeighbour(node1_, pcie);
    }

    ms::NodePtr node1_;
    ms::NodePtr node2_;
    ms::NodePtr node3_;
    ms::NodePtr isolated_node1_;
    ms::NodePtr isolated_node2_;
};

TEST_F(NodeTest, ADD_NEIGHBOUR) {
    ASSERT_EQ(isolated_node1_->GetNeighbours().size(), 0);
    ASSERT_EQ(isolated_node2_->GetNeighbours().size(), 0);
    auto pcie = ms::Connection("PCIe", 11.0);
    isolated_node1_->AddNeighbour(isolated_node2_, pcie);
    ASSERT_EQ(isolated_node1_->GetNeighbours().size(), 1);
    ASSERT_EQ(isolated_node2_->GetNeighbours().size(), 0);
}

TEST_F(NodeTest, REPEAT_ADD_NEIGHBOUR) {
    ASSERT_EQ(isolated_node1_->GetNeighbours().size(), 0);
    ASSERT_EQ(isolated_node2_->GetNeighbours().size(), 0);
    auto pcie = ms::Connection("PCIe", 11.0);
    isolated_node1_->AddNeighbour(isolated_node2_, pcie);
    isolated_node1_->AddNeighbour(isolated_node2_, pcie);
    ASSERT_EQ(isolated_node1_->GetNeighbours().size(), 1);
    ASSERT_EQ(isolated_node2_->GetNeighbours().size(), 0);
}

TEST_F(NodeTest, GET_NEIGHBOURS) {
    {
        bool n2 = false, n3 = false;
        auto node1_neighbours = node1_->GetNeighbours();
        ASSERT_EQ(node1_neighbours.size(), 2);
        for (auto& n : node1_neighbours) {
            if (n.neighbour_node == node2_)
                n2 = true;
            if (n.neighbour_node == node3_)
                n3 = true;
        }
        ASSERT_TRUE(n2);
        ASSERT_TRUE(n3);
    }

    {
        auto node2_neighbours = node2_->GetNeighbours();
        ASSERT_EQ(node2_neighbours.size(), 1);
        ASSERT_EQ(node2_neighbours[0].neighbour_node, node1_);
    }

    {
        auto node3_neighbours = node3_->GetNeighbours();
        ASSERT_EQ(node3_neighbours.size(), 0);
    }
}

TEST_F(NodeTest, DUMP) {
    std::cout << node1_->Dump();
    ASSERT_FALSE(node1_->Dump().empty());

    std::cout << node2_->Dump();
    ASSERT_FALSE(node2_->Dump().empty());
}
