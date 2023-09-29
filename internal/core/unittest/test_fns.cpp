#include <iostream>
#include <random>
#include <gtest/gtest.h>

#include "test_utils/Distance.h"
#include "fns/fns.hpp"

using namespace milvus;

namespace{

    void GenRandom(float *addr, unsigned size) {
        for (unsigned i = 0; i < size; ++i) {
            addr[i] = static_cast<float>(rand()) / static_cast<float>(RAND_MAX);
        }
    }

    void GenGT(float * query, float * data, size_t query_size, size_t data_size, size_t dim, std::vector<std::vector<unsigned>> & gt){
        std::vector<std::vector<unsigned>>(query_size).swap(gt); 
        size_t gt_size = 100; 
        for (unsigned i = 0; i < query_size; ++i){
            auto q = query + i * dim; 
            std::vector<std::pair<float, unsigned>> candidates;
            for (unsigned j = 0; j < data_size; ++j){
                auto dist = L2(q, data + j * dim, (int)dim);
                candidates.emplace_back(-dist, j); 
            }
            std::sort(candidates.begin(), candidates.end()); 
            for (size_t j = 0; j < gt_size; ++j){
                gt[i].emplace_back(candidates[j].second); 
            }
        }    
    }
} // namespace



class FurthestNeighborSearchTest : public testing::Test {
protected:
    static void SetUpTestCase() {
        data_size   = 10000; 
        data_dim    = 128; 
        query_size  = 100; 
        query_dim   = data_dim; 
        base_data   = new float[data_size  * data_dim]; 
        query       = new float[query_size * query_dim];
        for (size_t i = 0; i < data_size; ++i){
            GenRandom(base_data + i * data_dim, data_dim); 
        }
        for (size_t i = 0; i < query_size; ++i){
            GenRandom(query + i * query_dim, query_dim); 
        }
        GenGT(query, base_data, query_size, data_size, query_dim, fns_gt); 
        fns_ptr_ = new FNS<float>(base_data, data_size, data_dim);

    }


    static void TearDownTestCase() {
        delete fns_ptr_;
        fns_ptr_ = nullptr;
        if (base_data) {
            delete [] base_data;
            base_data = nullptr;
        }
        if (query){
            delete [] query;
            query = nullptr; 
        } 
    }

    void SetUp() override { 
    }

    void TearDown() override {  
    }


    // Some expensive resource shared by all tests.
    static  FNS<float> * fns_ptr_;
    static  size_t query_size, query_dim; 
    static float * base_data;
    static float * query;
    static  std::vector<std::vector<unsigned>> fns_gt;
    static   size_t data_size, data_dim;
};

FNS<float> *    FurthestNeighborSearchTest::fns_ptr_    = nullptr;
size_t          FurthestNeighborSearchTest::query_size  = 0;
size_t          FurthestNeighborSearchTest::query_dim   = 0; 
float *         FurthestNeighborSearchTest::base_data   = nullptr;
float *         FurthestNeighborSearchTest::query       = nullptr;;
size_t          FurthestNeighborSearchTest::data_size   = 0;
size_t          FurthestNeighborSearchTest::data_dim    = 0;
std::vector<std::vector<unsigned>> FurthestNeighborSearchTest::fns_gt = std::vector<std::vector<unsigned>>();





TEST_F(FurthestNeighborSearchTest, BuildKmeansZeroK) {
    BuildParam bp;
    bp.kms_cluster_num = 0;
    fns_ptr_->setUpBuildPara(bp);
    EXPECT_EQ(fns_ptr_->runKmeans(), -1);
}


TEST_F(FurthestNeighborSearchTest, BuildKmeansHugeK) {
    BuildParam bp;
    bp.kms_cluster_num  = FurthestNeighborSearchTest::data_size;
    fns_ptr_->setUpBuildPara(bp);
    EXPECT_EQ(fns_ptr_->runKmeans(), -1);
}

TEST_F(FurthestNeighborSearchTest, BuildKmeansNormalK) {
    BuildParam bp;
    bp.kms_cluster_num  = (size_t)sqrt(FurthestNeighborSearchTest::data_size);
    fns_ptr_->setUpBuildPara(bp);
    EXPECT_GT(fns_ptr_->runKmeans(), 0);
}

TEST_F(FurthestNeighborSearchTest, NNDescentHugeS){
    NNDIndexParam nnd_para;
    nnd_para.S = 32;
    nnd_para.R = 32;
    nnd_para.K = 20;
    nnd_para.L = 20; 
    fns_ptr_->setUpNNDPara(nnd_para);
    EXPECT_EQ(fns_ptr_->runNNDescent(), -1);
}

TEST_F(FurthestNeighborSearchTest, NNDescentNormalS){
    NNDIndexParam nnd_para;
    fns_ptr_->setUpNNDPara(nnd_para);
    EXPECT_EQ(fns_ptr_->runNNDescent(), 0);
}


TEST_F(FurthestNeighborSearchTest, BuildIndexing) {
    BuildParam bp;
    bp.kms_cluster_num  = (size_t)sqrt(FurthestNeighborSearchTest::data_size);
    bp.filter_ctrl_size = 100;                  //// num to expand FNSs
    bp.filter_pos       = 3;                    //// TOP `pos` neighbors to calculate RVS density
    bp.filter_density   = 1;                    //// max-density to judge whether a point is a low-density point.  
    bp.filter_max_edges = 2048;                 //// max edges to re-ranking ("brute-force" to get top-furthest points. )
    fns_ptr_->setUpBuildPara(bp);
    NNDIndexParam nnd_para;
    fns_ptr_->setUpNNDPara(nnd_para);
    EXPECT_EQ(fns_ptr_->build(), 0); 
}

TEST_F(FurthestNeighborSearchTest, SearchFurthestNeighborsMinusOne) {
    fns_ptr_->setSearchK(-1);
#pragma omp parallel for  
    for (auto iter = 0; iter < query_size; ++iter){
        auto res = fns_ptr_->searchFNS(query + iter * query_dim);
        EXPECT_EQ(res.size(), 0); 
    }
}

TEST_F(FurthestNeighborSearchTest, SearchFurthestNeighborsZero) {
    fns_ptr_->setSearchK(0);
#pragma omp parallel for  
    for (auto iter = 0; iter < query_size; ++iter){
        auto res = fns_ptr_->searchFNS(query + iter * query_dim);
        EXPECT_EQ(res.size(), 0); 
    }
}

TEST_F(FurthestNeighborSearchTest, SearchFurthestNeighborsTop100) {
    fns_ptr_->setSearchK(100);
#pragma omp parallel for  
    for (auto iter = 0; iter < query_size; ++iter){
        auto res = fns_ptr_->searchFNS(query + iter * query_dim);
        EXPECT_EQ(res.size(), 100); 
    }
}

TEST_F(FurthestNeighborSearchTest, SearchFurthestNeighborsHuge) {
    fns_ptr_->setSearchK(1E9);
#pragma omp parallel for  
    for (auto iter = 0; iter < query_size; ++iter){
        auto res = fns_ptr_->searchFNS(query + iter * query_dim);
        EXPECT_EQ(res.size(), 0); 
    }
}






TEST_F(FurthestNeighborSearchTest, SearchFurthestNeighborsBenchMark) {
    fns_ptr_->setSearchK(100);
    graph_t FNS_res(query_size);
    auto start = std::chrono::steady_clock::now();
#pragma omp parallel for  
    for (auto iter = 0; iter < query_size; ++iter){
        auto res = fns_ptr_->searchFNS(query + iter * query_dim);
        vector<idx_t> tmp;
        while (res.size()){
            tmp.emplace_back(res.top().second);
            res.pop();
        }
        reverse(tmp.begin(), tmp.end());
        FNS_res[iter] = tmp;
    }
    auto end = std::chrono::steady_clock::now();
    auto duration = (double) 1.0 * std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 1000.0; // seconds
    std::cout << "duration = " << duration << " s\n";
    std::cout << "QPS = " << (1.0 * query_size / duration) << std::endl;
    std::cout << "avg_ratio@1 = " << (fns_ptr_->evaluateRatio(fns_gt, FNS_res, query_size, query, 1)) << std::endl;
    std::cout << "avg_ratio@10 = " << (fns_ptr_->evaluateRatio(fns_gt, FNS_res, query_size, query, 10)) << std::endl;
    std::cout << "avg_ratio@100 = " << (fns_ptr_->evaluateRatio(fns_gt, FNS_res, query_size, query, 100)) << std::endl;
    std::cout << "recall@1 = " << (fns_ptr_->evaluateRecall(fns_gt, FNS_res, 1)) << std::endl;
    std::cout << "recall@10 = " << (fns_ptr_->evaluateRecall(fns_gt, FNS_res, 10)) << std::endl;
    std::cout << "recall@100 = " << (fns_ptr_->evaluateRecall(fns_gt, FNS_res, 100)) << std::endl;
}