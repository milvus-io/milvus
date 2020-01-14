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
#include <thread>

#include "knowhere/adapter/VectorAdapter.h"
#include "knowhere/common/Exception.h"
#include "knowhere/common/Timer.h"

#include "knowhere/index/vector_index/IndexBinaryIVF.h"

#include "unittest/Helper.h"
#include "unittest/utils.h"

using ::testing::Combine;
using ::testing::TestWithParam;
using ::testing::Values;

class BinaryIVFTest : public BinaryDataGen, public TestWithParam<knowhere::METRICTYPE> {
 protected:
    void
    SetUp() override {
        knowhere::METRICTYPE MetricType = GetParam();
        Init_with_binary_default();
        //        nb = 1000000;
        //        nq = 1000;
        //        k = 1000;
        //        Generate(DIM, NB, NQ);
        index_ = std::make_shared<knowhere::BinaryIVF>();
        auto x_conf = std::make_shared<knowhere::IVFBinCfg>();
        x_conf->d = dim;
        x_conf->k = k;
        x_conf->metric_type = MetricType;
        x_conf->nlist = 100;
        x_conf->nprobe = 10;
        conf = x_conf;
        conf->Dump();
    }

    void
    TearDown() override {
    }

 protected:
    std::string index_type;
    knowhere::Config conf;
    knowhere::BinaryIVFIndexPtr index_ = nullptr;
};

INSTANTIATE_TEST_CASE_P(METRICParameters, BinaryIVFTest,
                        Values(knowhere::METRICTYPE::JACCARD, knowhere::METRICTYPE::TANIMOTO,
                               knowhere::METRICTYPE::HAMMING));

TEST_P(BinaryIVFTest, binaryivf_basic) {
    assert(!xb.empty());

    //    auto preprocessor = index_->BuildPreprocessor(base_dataset, conf);
    //    index_->set_preprocessor(preprocessor);

    index_->Train(base_dataset, conf);
    //    index_->set_index_model(model);
    //    index_->Add(base_dataset, conf);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dimension(), dim);

    auto result = index_->Search(query_dataset, conf);
    AssertAnns(result, nq, conf->k);
    // PrintResult(result, nq, k);
}

TEST_P(BinaryIVFTest, binaryivf_serialize) {
    auto serialize = [](const std::string& filename, knowhere::BinaryPtr& bin, uint8_t* ret) {
        FileIOWriter writer(filename);
        writer(static_cast<void*>(bin->data.get()), bin->size);

        FileIOReader reader(filename);
        reader(ret, bin->size);
    };

    //    {
    //        // serialize index-model
    //        auto model = index_->Train(base_dataset, conf);
    //        auto binaryset = model->Serialize();
    //        auto bin = binaryset.GetByName("BinaryIVF");
    //
    //        std::string filename = "/tmp/binaryivf_test_model_serialize.bin";
    //        auto load_data = new uint8_t[bin->size];
    //        serialize(filename, bin, load_data);
    //
    //        binaryset.clear();
    //        auto data = std::make_shared<uint8_t>();
    //        data.reset(load_data);
    //        binaryset.Append("BinaryIVF", data, bin->size);
    //
    //        model->Load(binaryset);
    //
    //        index_->set_index_model(model);
    //        index_->Add(base_dataset, conf);
    //        auto result = index_->Search(query_dataset, conf);
    //        AssertAnns(result, nq, conf->k);
    //    }

    {
        // serialize index
        index_->Train(base_dataset, conf);
        //        index_->set_index_model(model);
        //        index_->Add(base_dataset, conf);
        auto binaryset = index_->Serialize();
        auto bin = binaryset.GetByName("BinaryIVF");

        std::string filename = "/tmp/binaryivf_test_serialize.bin";
        auto load_data = new uint8_t[bin->size];
        serialize(filename, bin, load_data);

        binaryset.clear();
        auto data = std::make_shared<uint8_t>();
        data.reset(load_data);
        binaryset.Append("BinaryIVF", data, bin->size);

        index_->Load(binaryset);
        EXPECT_EQ(index_->Count(), nb);
        EXPECT_EQ(index_->Dimension(), dim);
        auto result = index_->Search(query_dataset, conf);
        AssertAnns(result, nq, conf->k);
        // PrintResult(result, nq, k);
    }
}
