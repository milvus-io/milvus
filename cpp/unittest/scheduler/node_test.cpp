#include "scheduler/resource/Node.h"
#include <gtest/gtest.h>


using namespace zilliz::milvus::engine;

class NodeTest : public ::testing::Test {
protected:
    void
    SetUp() override {
        node1_ = std::make_shared<Node>();
        node2_ = std::make_shared<Node>();
        node3_ = std::make_shared<Node>();
        isolated_node1_ = std::make_shared<Node>();
        isolated_node2_ = std::make_shared<Node>();

        auto pcie = Connection("PCIe", 11.0);

        node1_->AddNeighbour(node2_, pcie);
        node1_->AddNeighbour(node3_, pcie);
        node2_->AddNeighbour(node1_, pcie);
    }

    NodePtr node1_;
    NodePtr node2_;
    NodePtr node3_;
    NodePtr isolated_node1_;
    NodePtr isolated_node2_;
};

TEST_F(NodeTest, add_neighbour) {
    ASSERT_EQ(isolated_node1_->GetNeighbours().size(), 0);
    ASSERT_EQ(isolated_node2_->GetNeighbours().size(), 0);
    auto pcie = Connection("PCIe", 11.0);
    isolated_node1_->AddNeighbour(isolated_node2_, pcie);
    ASSERT_EQ(isolated_node1_->GetNeighbours().size(), 1);
    ASSERT_EQ(isolated_node2_->GetNeighbours().size(), 0);
}

TEST_F(NodeTest, repeat_add_neighbour) {
    ASSERT_EQ(isolated_node1_->GetNeighbours().size(), 0);
    ASSERT_EQ(isolated_node2_->GetNeighbours().size(), 0);
    auto pcie = Connection("PCIe", 11.0);
    isolated_node1_->AddNeighbour(isolated_node2_, pcie);
    isolated_node1_->AddNeighbour(isolated_node2_, pcie);
    ASSERT_EQ(isolated_node1_->GetNeighbours().size(), 1);
    ASSERT_EQ(isolated_node2_->GetNeighbours().size(), 0);
}

TEST_F(NodeTest, get_neighbours) {
    {
        bool n2 = false, n3 = false;
        auto node1_neighbours = node1_->GetNeighbours();
        ASSERT_EQ(node1_neighbours.size(), 2);
        for (auto &n : node1_neighbours) {
            if (n.neighbour_node.lock() == node2_) n2 = true;
            if (n.neighbour_node.lock() == node3_) n3 = true;
        }
        ASSERT_TRUE(n2);
        ASSERT_TRUE(n3);
    }

    {
        auto node2_neighbours = node2_->GetNeighbours();
        ASSERT_EQ(node2_neighbours.size(), 1);
        ASSERT_EQ(node2_neighbours[0].neighbour_node.lock(), node1_);
    }

    {
        auto node3_neighbours = node3_->GetNeighbours();
        ASSERT_EQ(node3_neighbours.size(), 0);
    }
}

TEST_F(NodeTest, dump) {
    std::cout << node1_->Dump();
    ASSERT_FALSE(node1_->Dump().empty());

    std::cout << node2_->Dump();
    ASSERT_FALSE(node2_->Dump().empty());
}
