// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gtest/gtest.h>

#include <iostream>
#include <sstream>
#include "knowhere/adapter/SptagAdapter.h"
#include "knowhere/adapter/VectorAdapter.h"
#include "knowhere/common/Exception.h"
#include "knowhere/index/vector_index/IndexSPTAG.h"
#include "knowhere/index/vector_index/helpers/Definitions.h"

#include "unittest/utils.h"

using ::testing::Combine;
using ::testing::TestWithParam;
using ::testing::Values;

class SPTAGTest : public DataGen, public TestWithParam<std::string> {
 protected:
    void
    SetUp() override {
        IndexType = GetParam();
        Generate(128, 100, 5);
        index_ = std::make_shared<knowhere::CPUSPTAGRNG>(IndexType);
        if (IndexType == "KDT") {
            auto tempconf = std::make_shared<knowhere::KDTCfg>();
            tempconf->tptnumber = 1;
            tempconf->k = 10;
            conf = tempconf;
        } else {
            auto tempconf = std::make_shared<knowhere::BKTCfg>();
            tempconf->tptnumber = 1;
            tempconf->k = 10;
            conf = tempconf;
        }

        Init_with_default();
    }

 protected:
    knowhere::Config conf;
    std::shared_ptr<knowhere::CPUSPTAGRNG> index_ = nullptr;
    std::string IndexType;
};

INSTANTIATE_TEST_CASE_P(SPTAGParameters, SPTAGTest, Values("KDT", "BKT"));

// TODO(lxj): add test about count() and dimension()
TEST_P(SPTAGTest, sptag_basic) {
    assert(!xb.empty());

    auto preprocessor = index_->BuildPreprocessor(base_dataset, conf);
    index_->set_preprocessor(preprocessor);

    auto model = index_->Train(base_dataset, conf);
    index_->set_index_model(model);
    index_->Add(base_dataset, conf);
    auto result = index_->Search(query_dataset, conf);
    AssertAnns(result, nq, k);

    {
        auto ids = result->Get<int64_t*>(knowhere::meta::IDS);
        auto dist = result->Get<float*>(knowhere::meta::DISTANCE);

        std::stringstream ss_id;
        std::stringstream ss_dist;
        for (auto i = 0; i < nq; i++) {
            for (auto j = 0; j < k; ++j) {
                // ss_id << *ids->data()->GetValues<int64_t>(1, i * k + j) << " ";
                // ss_dist << *dists->data()->GetValues<float>(1, i * k + j) << " ";
                ss_id << *((int64_t*)(ids) + i * k + j) << " ";
                ss_dist << *((float*)(dist) + i * k + j) << " ";
            }
            ss_id << std::endl;
            ss_dist << std::endl;
        }
        std::cout << "id\n" << ss_id.str() << std::endl;
        std::cout << "dist\n" << ss_dist.str() << std::endl;
    }
}

TEST_P(SPTAGTest, sptag_serialize) {
    assert(!xb.empty());

    auto preprocessor = index_->BuildPreprocessor(base_dataset, conf);
    index_->set_preprocessor(preprocessor);

    auto model = index_->Train(base_dataset, conf);

    index_->Add(base_dataset, conf);
    auto binaryset = index_->Serialize();
    auto new_index = std::make_shared<knowhere::CPUSPTAGRNG>(IndexType);
    new_index->Load(binaryset);
    auto result = new_index->Search(query_dataset, conf);
    AssertAnns(result, nq, k);
    PrintResult(result, nq, k);
    ASSERT_EQ(new_index->Count(), nb);
    ASSERT_EQ(new_index->Dimension(), dim);
    //        ASSERT_THROW({ new_index->Clone(); }, knowhere::KnowhereException);
    //        ASSERT_NO_THROW({ new_index->Seal(); });

    {
        int fileno = 0;
        const std::string& base_name = "/tmp/sptag_serialize_test_bin_";
        std::vector<std::string> filename_list;
        std::vector<std::pair<std::string, size_t>> meta_list;
        for (auto& iter : binaryset.binary_map_) {
            const std::string& filename = base_name + std::to_string(fileno);
            FileIOWriter writer(filename);
            writer(iter.second->data.get(), iter.second->size);

            meta_list.emplace_back(std::make_pair(iter.first, iter.second->size));
            filename_list.push_back(filename);
            ++fileno;
        }

        knowhere::BinarySet load_data_list;
        for (int i = 0; i < filename_list.size() && i < meta_list.size(); ++i) {
            auto bin_size = meta_list[i].second;
            FileIOReader reader(filename_list[i]);

            auto load_data = new uint8_t[bin_size];
            reader(load_data, bin_size);
            auto data = std::make_shared<uint8_t>();
            data.reset(load_data);
            load_data_list.Append(meta_list[i].first, data, bin_size);
        }

        auto new_index = std::make_shared<knowhere::CPUSPTAGRNG>(IndexType);
        new_index->Load(load_data_list);
        auto result = new_index->Search(query_dataset, conf);
        AssertAnns(result, nq, k);
        PrintResult(result, nq, k);
    }
}
