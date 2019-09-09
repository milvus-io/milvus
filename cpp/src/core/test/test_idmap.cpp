////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include <gtest/gtest.h>

#include <iostream>
#include <sstream>

#include "knowhere/index/vector_index/idmap.h"
#include "knowhere/adapter/structure.h"
#include "knowhere/index/vector_index/cloner.h"

#include "utils.h"


using namespace zilliz::knowhere;

static int device_id = 0;
class IDMAPTest : public DataGen, public ::testing::Test {
 protected:
    void SetUp() override {
        FaissGpuResourceMgr::GetInstance().InitDevice(device_id, 1024*1024*200, 1024*1024*300, 2);
        Init_with_default();
        index_ = std::make_shared<IDMAP>();
    }

    void TearDown() override {
        FaissGpuResourceMgr::GetInstance().Free();
    }

 protected:
    IDMAPPtr index_ = nullptr;
};

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

TEST_F(IDMAPTest, idmap_basic) {
    assert(!xb.empty());
    Config Default_cfg;

    index_->Train(Config::object{{"dim", dim}, {"metric_type", "L2"}});
    index_->Add(base_dataset, Default_cfg);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dimension(), dim);
    assert(index_->GetRawVectors() != nullptr);
    assert(index_->GetRawIds() != nullptr);
    auto result = index_->Search(query_dataset, Config::object{{"k", k}});
    AssertAnns(result, nq, k);
    PrintResult(result, nq, k);

    auto binaryset = index_->Serialize();
    auto new_index = std::make_shared<IDMAP>();
    new_index->Load(binaryset);
    auto re_result = index_->Search(query_dataset, Config::object{{"k", k}});
    AssertAnns(re_result, nq, k);
    PrintResult(re_result, nq, k);
}

TEST_F(IDMAPTest, idmap_serialize) {
    auto serialize = [](const std::string &filename, BinaryPtr &bin, uint8_t *ret) {
        FileIOWriter writer(filename);
        writer(static_cast<void *>(bin->data.get()), bin->size);

        FileIOReader reader(filename);
        reader(ret, bin->size);
    };

    {
        // serialize index
        index_->Train(Config::object{{"dim", dim}, {"metric_type", "L2"}});
        index_->Add(base_dataset, Config());
        auto re_result = index_->Search(query_dataset, Config::object{{"k", k}});
        AssertAnns(re_result, nq, k);
        PrintResult(re_result, nq, k);
        EXPECT_EQ(index_->Count(), nb);
        EXPECT_EQ(index_->Dimension(), dim);
        auto binaryset = index_->Serialize();
        auto bin = binaryset.GetByName("IVF");

        std::string filename = "/tmp/idmap_test_serialize.bin";
        auto load_data = new uint8_t[bin->size];
        serialize(filename, bin, load_data);

        binaryset.clear();
        auto data = std::make_shared<uint8_t>();
        data.reset(load_data);
        binaryset.Append("IVF", data, bin->size);

        index_->Load(binaryset);
        EXPECT_EQ(index_->Count(), nb);
        EXPECT_EQ(index_->Dimension(), dim);
        auto result = index_->Search(query_dataset, Config::object{{"k", k}});
        AssertAnns(result, nq, k);
        PrintResult(result, nq, k);
    }
}

TEST_F(IDMAPTest, copy_test) {
    assert(!xb.empty());
    Config Default_cfg;

    index_->Train(Config::object{{"dim", dim}, {"metric_type", "L2"}});
    index_->Add(base_dataset, Default_cfg);
    EXPECT_EQ(index_->Count(), nb);
    EXPECT_EQ(index_->Dimension(), dim);
    assert(index_->GetRawVectors() != nullptr);
    assert(index_->GetRawIds() != nullptr);
    auto result = index_->Search(query_dataset, Config::object{{"k", k}});
    AssertAnns(result, nq, k);
    //PrintResult(result, nq, k);

    {
        // clone
        auto clone_index = index_->Clone();
        auto clone_result = clone_index->Search(query_dataset, Config::object{{"k", k}});
        AssertAnns(clone_result, nq, k);
    }

    {
        // cpu to gpu
        auto clone_index = CopyCpuToGpu(index_, device_id, Config());
        auto clone_result = clone_index->Search(query_dataset, Config::object{{"k", k}});
        AssertAnns(clone_result, nq, k);
        //assert(std::static_pointer_cast<GPUIDMAP>(clone_index)->GetRawVectors() != nullptr);
        //assert(std::static_pointer_cast<GPUIDMAP>(clone_index)->GetRawIds() != nullptr);
        auto clone_gpu_idx = clone_index->Clone();
        auto clone_gpu_res = clone_gpu_idx->Search(query_dataset, Config::object{{"k", k}});
        AssertAnns(clone_gpu_res, nq, k);

        // gpu to cpu
        auto host_index = CopyGpuToCpu(clone_index, Config());
        auto host_result = host_index->Search(query_dataset, Config::object{{"k", k}});
        AssertAnns(host_result, nq, k);
        assert(std::static_pointer_cast<IDMAP>(host_index)->GetRawVectors() != nullptr);
        assert(std::static_pointer_cast<IDMAP>(host_index)->GetRawIds() != nullptr);

        // gpu to gpu
        auto device_index = CopyCpuToGpu(index_, device_id, Config());
        auto device_result = device_index->Search(query_dataset, Config::object{{"k", k}});
        AssertAnns(device_result, nq, k);
        //assert(std::static_pointer_cast<GPUIDMAP>(device_index)->GetRawVectors() != nullptr);
        //assert(std::static_pointer_cast<GPUIDMAP>(device_index)->GetRawIds() != nullptr);
    }
}
