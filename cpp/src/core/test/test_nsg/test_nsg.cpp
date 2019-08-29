////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include <gtest/gtest.h>
#include <memory>

#include "knowhere/index/vector_index/nsg_index.h"
#include "knowhere/index/vector_index/nsg/nsg_io.h"

#include "../utils.h"


using namespace zilliz::knowhere;
using ::testing::TestWithParam;
using ::testing::Values;
using ::testing::Combine;

class NSGInterfaceTest : public DataGen, public TestWithParam<::std::tuple<Config, Config>> {
 protected:
    void SetUp() override {
        //Init_with_default();
        Generate(256, 1000000, 1);
        index_ = std::make_shared<NSG>();
        std::tie(train_cfg, search_cfg) = GetParam();
    }

 protected:
    std::shared_ptr<NSG> index_;
    Config train_cfg;
    Config search_cfg;
};

INSTANTIATE_TEST_CASE_P(NSGparameters, NSGInterfaceTest,
                        Values(std::make_tuple(
                            // search length > out_degree
                            Config::object{{"nlist", 16384}, {"nprobe", 50}, {"knng", 100}, {"metric_type", "L2"},
                                           {"search_length", 60}, {"out_degree", 70}, {"candidate_pool_size", 500}},
                            Config::object{{"k", 20}, {"search_length", 30}}))
);

void AssertAnns(const DatasetPtr &result,
                const int &nq,
                const int &k) {
    auto ids = result->array()[0];
    for (auto i = 0; i < nq; i++) {
        EXPECT_EQ(i, *(ids->data()->GetValues<int64_t>(1, i * k)));
    }
}

TEST_P(NSGInterfaceTest, basic_test) {
    assert(!xb.empty());

    auto model = index_->Train(base_dataset, train_cfg);
    auto result = index_->Search(query_dataset, search_cfg);
    AssertAnns(result, nq, k);

    auto binaryset = index_->Serialize();
    auto new_index = std::make_shared<NSG>();
    new_index->Load(binaryset);
    auto new_result = new_index->Search(query_dataset, Config::object{{"k", k}});
    AssertAnns(result, nq, k);

    {
        //std::cout << "k = 1" << std::endl;
        //new_index->Search(GenQuery(1), Config::object{{"k", 1}});
        //new_index->Search(GenQuery(10), Config::object{{"k", 1}});
        //new_index->Search(GenQuery(100), Config::object{{"k", 1}});
        //new_index->Search(GenQuery(1000), Config::object{{"k", 1}});
        //new_index->Search(GenQuery(10000), Config::object{{"k", 1}});

        //std::cout << "k = 5" << std::endl;
        //new_index->Search(GenQuery(1), Config::object{{"k", 5}});
        //new_index->Search(GenQuery(20), Config::object{{"k", 5}});
        //new_index->Search(GenQuery(100), Config::object{{"k", 5}});
        //new_index->Search(GenQuery(300), Config::object{{"k", 5}});
        //new_index->Search(GenQuery(500), Config::object{{"k", 5}});
    }
}

