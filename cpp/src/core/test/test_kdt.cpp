////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include <gtest/gtest.h>

#include <iostream>
#include <sstream>

#include "knowhere/index/vector_index/cpu_kdt_rng.h"
#include "knowhere/index/vector_index/definitions.h"
#include "knowhere/adapter/sptag.h"
#include "knowhere/adapter/structure.h"

#include "utils.h"


using namespace zilliz::knowhere;

using ::testing::TestWithParam;
using ::testing::Values;
using ::testing::Combine;


class KDTTest
    : public DataGen, public TestWithParam<::std::tuple<Config, Config, Config, Config>> {
 protected:
    void SetUp() override {
        std::tie(preprocess_cfg, train_cfg, add_cfg, search_cfg) = GetParam();
        index_ = std::make_shared<CPUKDTRNG>();
        Init_with_default();
    }

 protected:
    Config preprocess_cfg;
    Config train_cfg;
    Config add_cfg;
    Config search_cfg;
    std::shared_ptr<CPUKDTRNG> index_ = nullptr;
};

INSTANTIATE_TEST_CASE_P(KDTParameters, KDTTest,
                        Values(
                            std::make_tuple(Config(),
                                            Config::object{{"TPTNumber", 1}},
                                            Config(),
                                            Config::object{{"k", 10}})
                        )
);

void AssertAnns(const DatasetPtr &result,
                const int &nq,
                const int &k) {
    auto ids = result->array()[0];
    for (auto i = 0; i < nq; i++) {
        EXPECT_EQ(i, *(ids->data()->GetValues<int64_t>(1, i * k)));
    }
}

void PrintResult(const DatasetPtr &result,
                 const int &nq,
                 const int &k) {
    auto ids = result->array()[0];
    auto dists = result->array()[1];

    std::stringstream ss_id;
    std::stringstream ss_dist;
    for (auto i = 0; i < 10; i++) {
        for (auto j = 0; j < k; ++j) {
            ss_id << *(ids->data()->GetValues<int64_t>(1, i * k + j)) << " ";
            ss_dist << *(dists->data()->GetValues<float>(1, i * k + j)) << " ";
        }
        ss_id << std::endl;
        ss_dist << std::endl;
    }
    std::cout << "id\n" << ss_id.str() << std::endl;
    std::cout << "dist\n" << ss_dist.str() << std::endl;
}

// TODO(linxj): add test about count() and dimension()
TEST_P(KDTTest, kdt_basic) {
    assert(!xb.empty());

    auto preprocessor = index_->BuildPreprocessor(base_dataset, preprocess_cfg);
    index_->set_preprocessor(preprocessor);

    auto model = index_->Train(base_dataset, train_cfg);
    index_->set_index_model(model);
    index_->Add(base_dataset, add_cfg);
    auto result = index_->Search(query_dataset, search_cfg);
    AssertAnns(result, nq, k);

    {
        auto ids = result->array()[0];
        auto dists = result->array()[1];

        std::stringstream ss_id;
        std::stringstream ss_dist;
        for (auto i = 0; i < nq; i++) {
            for (auto j = 0; j < k; ++j) {
                ss_id << *ids->data()->GetValues<int64_t>(1, i * k + j) << " ";
                ss_dist << *dists->data()->GetValues<float>(1, i * k + j) << " ";
            }
            ss_id << std::endl;
            ss_dist << std::endl;
        }
        std::cout << "id\n" << ss_id.str() << std::endl;
        std::cout << "dist\n" << ss_dist.str() << std::endl;
    }
}

TEST_P(KDTTest, kdt_serialize) {
    assert(!xb.empty());

    auto preprocessor = index_->BuildPreprocessor(base_dataset, preprocess_cfg);
    index_->set_preprocessor(preprocessor);

    auto model = index_->Train(base_dataset, train_cfg);
    //index_->Add(base_dataset, add_cfg);
    auto binaryset = index_->Serialize();
    auto new_index = std::make_shared<CPUKDTRNG>();
    new_index->Load(binaryset);
    auto result = new_index->Search(query_dataset, search_cfg);
    AssertAnns(result, nq, k);
    PrintResult(result, nq, k);

    {
        int fileno = 0;
        const std::string &base_name = "/tmp/kdt_serialize_test_bin_";
        std::vector<std::string> filename_list;
        std::vector<std::pair<std::string, size_t >> meta_list;
        for (auto &iter: binaryset.binary_map_) {
            const std::string &filename = base_name + std::to_string(fileno);
            FileIOWriter writer(filename);
            writer(iter.second->data.get(), iter.second->size);

            meta_list.emplace_back(std::make_pair(iter.first, iter.second->size));
            filename_list.push_back(filename);
            ++fileno;
        }

        BinarySet load_data_list;
        for (int i = 0; i < filename_list.size() && i < meta_list.size(); ++i) {
            auto bin_size = meta_list[i].second;
            FileIOReader reader(filename_list[i]);

            auto load_data = new uint8_t[bin_size];
            reader(load_data, bin_size);
            auto data = std::make_shared<uint8_t>();
            data.reset(load_data);
            load_data_list.Append(meta_list[i].first, data, bin_size);
        }

        auto new_index = std::make_shared<CPUKDTRNG>();
        new_index->Load(load_data_list);
        auto result = new_index->Search(query_dataset, search_cfg);
        AssertAnns(result, nq, k);
        PrintResult(result, nq, k);
    }
}
