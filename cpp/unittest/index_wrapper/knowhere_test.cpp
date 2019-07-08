////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include <gtest/gtest.h>

#include <wrapper/knowhere/vec_index.h>

#include "utils.h"


using namespace zilliz::milvus::engine;
using namespace zilliz::knowhere;

using ::testing::TestWithParam;
using ::testing::Values;
using ::testing::Combine;


class KnowhereWrapperTest
    : public TestWithParam<::std::tuple<IndexType, std::string, int, int, int, int, Config, Config>> {
 protected:
    void SetUp() override {
        std::string generator_type;
        std::tie(index_type, generator_type, dim, nb, nq, k, train_cfg, search_cfg) = GetParam();

        //auto generator = GetGenerateFactory(generator_type);
        auto generator = std::make_shared<DataGenBase>();
        generator->GenData(dim, nb, nq, xb, xq, ids, k, gt_ids);

        index_ = GetVecIndexFactory(index_type);
    }

 protected:
    IndexType index_type;
    Config train_cfg;
    Config search_cfg;

    int dim = 64;
    int nb = 10000;
    int nq = 10;
    int k = 10;
    std::vector<float> xb;
    std::vector<float> xq;
    std::vector<long> ids;

    VecIndexPtr index_ = nullptr;

    // Ground Truth
    std::vector<long> gt_ids;
};

INSTANTIATE_TEST_CASE_P(WrapperParam, KnowhereWrapperTest,
                        Values(
                            // ["Index type", "Generator type", "dim", "nb", "nq", "k", "build config", "search config"]
                            std::make_tuple(IndexType::FAISS_IVFFLAT_CPU, "Default",
                                            64, 10000, 10, 10,
                                            Config::object{{"nlist", 100}, {"dim", 64}},
                                            Config::object{{"dim", 64}, {"k", 10}, {"nprobe", 20}}
                            ),
                            std::make_tuple(IndexType::SPTAG_KDT_RNT_CPU, "Default",
                                            64, 10000, 10, 10,
                                            Config::object{{"TPTNumber", 1}, {"dim", 64}},
                                            Config::object{{"dim", 64}, {"k", 10}}
                            )
                        )
);

void AssertAnns(const std::vector<long> &gt,
                const std::vector<long> &res,
                const int &nq,
                const int &k) {
    EXPECT_EQ(res.size(), nq * k);

    for (auto i = 0; i < nq; i++) {
        EXPECT_EQ(gt[i * k], res[i * k]);
    }

    int match = 0;
    for (int i = 0; i < nq; ++i) {
        for (int j = 0; j < k; ++j) {
            for (int l = 0; l < k; ++l) {
                if (gt[i * nq + j] == res[i * nq + l]) match++;
            }
        }
    }

    // TODO(linxj): percision check
    EXPECT_GT(float(match/nq*k), 0.5);
}

TEST_P(KnowhereWrapperTest, base_test) {
    std::vector<long> res_ids;
    float *D = new float[k * nq];
    res_ids.resize(nq * k);

    index_->BuildAll(nb, xb.data(), ids.data(), train_cfg);
    index_->Search(nq, xq.data(), D, res_ids.data(), search_cfg);
    AssertAnns(gt_ids, res_ids, nq, k);
    delete[] D;
}

TEST_P(KnowhereWrapperTest, serialize_test) {
    std::vector<long> res_ids;
    float *D = new float[k * nq];
    res_ids.resize(nq * k);

    index_->BuildAll(nb, xb.data(), ids.data(), train_cfg);
    index_->Search(nq, xq.data(), D, res_ids.data(), search_cfg);
    AssertAnns(gt_ids, res_ids, nq, k);

    {
        auto binaryset = index_->Serialize();
        //int fileno = 0;
        //const std::string &base_name = "/tmp/wrapper_serialize_test_bin_";
        //std::vector<std::string> filename_list;
        //std::vector<std::pair<std::string, size_t >> meta_list;
        //for (auto &iter: binaryset.binary_map_) {
        //    const std::string &filename = base_name + std::to_string(fileno);
        //    FileIOWriter writer(filename);
        //    writer(iter.second->data.get(), iter.second->size);
        //
        //    meta_list.push_back(std::make_pair(iter.first, iter.second.size));
        //    filename_list.push_back(filename);
        //    ++fileno;
        //}
        //
        //BinarySet load_data_list;
        //for (int i = 0; i < filename_list.size() && i < meta_list.size(); ++i) {
        //    auto bin_size = meta_list[i].second;
        //    FileIOReader reader(filename_list[i]);
        //    std::vector<uint8_t> load_data(bin_size);
        //    reader(load_data.data(), bin_size);
        //    load_data_list.Append(meta_list[i].first, load_data);
        //}

        int fileno = 0;
        std::vector<std::string> filename_list;
        const std::string &base_name = "/tmp/wrapper_serialize_test_bin_";
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


        res_ids.clear();
        res_ids.resize(nq * k);
        auto new_index = GetVecIndexFactory(index_type);
        new_index->Load(load_data_list);
        new_index->Search(nq, xq.data(), D, res_ids.data(), search_cfg);
        AssertAnns(gt_ids, res_ids, nq, k);
    }

    delete[] D;
}
