#include "scheduler/resource/Node.h"
#include <gtest/gtest.h>


using namespace zilliz::milvus::engine;

class NodeTest : public ::testing::Test {
protected:
    void
    SetUp() override {
        node1_ = std::make_shared<Node>();
        node2_ = std::make_shared<Node>();

        auto pcie = Connection("PCIe", 11.0);

        node1_->AddNeighbour(node2_, pcie);
        node2_->AddNeighbour(node1_, pcie);
    }

    NodePtr node1_;
    NodePtr node2_;
    NodePtr node3_;
    NodePtr node4_;
};

TEST_F(NodeTest, add_neighbour) {
    ASSERT_EQ(node3_->GetNeighbours().size(), 0);
    ASSERT_EQ(node4_->GetNeighbours().size(), 0);
    auto pcie = Connection("PCIe", 11.0);
    node3_->AddNeighbour(node4_, pcie);
    node4_->AddNeighbour(node3_, pcie);
    ASSERT_EQ(node3_->GetNeighbours().size(), 1);
    ASSERT_EQ(node4_->GetNeighbours().size(), 1);
}

TEST_F(NodeTest, del_neighbour) {
    ASSERT_EQ(node1_->GetNeighbours().size(), 1);
    ASSERT_EQ(node2_->GetNeighbours().size(), 1);
    ASSERT_EQ(node3_->GetNeighbours().size(), 0);
    node1_->DelNeighbour(node2_);
    node2_->DelNeighbour(node2_);
    node3_->DelNeighbour(node2_);
    ASSERT_EQ(node1_->GetNeighbours().size(), 0);
    ASSERT_EQ(node2_->GetNeighbours().size(), 1);
    ASSERT_EQ(node3_->GetNeighbours().size(), 0);
}

TEST_F(NodeTest, is_neighbour) {
    ASSERT_TRUE(node1_->IsNeighbour(node2_));
    ASSERT_TRUE(node2_->IsNeighbour(node1_));

    ASSERT_FALSE(node1_->IsNeighbour(node3_));
    ASSERT_FALSE(node2_->IsNeighbour(node3_));
    ASSERT_FALSE(node3_->IsNeighbour(node1_));
    ASSERT_FALSE(node3_->IsNeighbour(node2_));
}

TEST_F(NodeTest, get_neighbours) {
    auto node1_neighbours = node1_->GetNeighbours();
    ASSERT_EQ(node1_neighbours.size(), 1);
    ASSERT_EQ(node1_neighbours[0].neighbour_node.lock(), node2_);

    auto node2_neighbours = node2_->GetNeighbours();
    ASSERT_EQ(node2_neighbours.size(), 1);
    ASSERT_EQ(node2_neighbours[0].neighbour_node.lock(), node1_);
}
