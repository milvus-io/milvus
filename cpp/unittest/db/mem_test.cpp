#include "gtest/gtest.h"

#include "db/VectorSource.h"
#include "db/MemTableFile.h"
#include "db/MemTable.h"
#include "utils.h"
#include "db/Factories.h"
#include "db/Constants.h"
#include "db/EngineFactory.h"
#include "metrics/Metrics.h"

#include <thread>
#include <fstream>
#include <iostream>

using namespace zilliz::milvus;

namespace {

    static const std::string TABLE_NAME = "test_group";
    static constexpr int64_t TABLE_DIM = 256;
    static constexpr int64_t VECTOR_COUNT = 250000;
    static constexpr int64_t INSERT_LOOP = 10000;

    engine::meta::TableSchema BuildTableSchema() {
        engine::meta::TableSchema table_info;
        table_info.dimension_ = TABLE_DIM;
        table_info.table_id_ = TABLE_NAME;
        table_info.engine_type_ = (int)engine::EngineType::FAISS_IDMAP;
        return table_info;
    }

    void BuildVectors(int64_t n, std::vector<float>& vectors) {
        vectors.clear();
        vectors.resize(n*TABLE_DIM);
        float* data = vectors.data();
//        std::random_device rd;
//        std::mt19937 gen(rd());
//        std::uniform_real_distribution<> dis(0.0, 1.0);
        for(int i = 0; i < n; i++) {
            for(int j = 0; j < TABLE_DIM; j++) data[TABLE_DIM * i + j] = drand48();
            data[TABLE_DIM * i] += i / 2000.;
        }
    }
}

TEST(MEM_TEST, VECTOR_SOURCE_TEST) {

    std::shared_ptr<engine::meta::DBMetaImpl> impl_ = engine::DBMetaImplFactory::Build();

    engine::meta::TableSchema table_schema = BuildTableSchema();
    auto status = impl_->CreateTable(table_schema);
    ASSERT_TRUE(status.ok());

    engine::meta::TableFileSchema table_file_schema;
    table_file_schema.table_id_ = TABLE_NAME;
    status = impl_->CreateTableFile(table_file_schema);
    ASSERT_TRUE(status.ok());

    int64_t n = 100;
    std::vector<float> vectors;
    BuildVectors(n, vectors);

    engine::VectorSource source(n, vectors.data());

    size_t num_vectors_added;
    engine::ExecutionEnginePtr execution_engine_ = engine::EngineFactory::Build(table_file_schema.dimension_,
                                                                                table_file_schema.location_,
                                                                                (engine::EngineType)table_file_schema.engine_type_);
    status = source.Add(execution_engine_, table_file_schema, 50, num_vectors_added);
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(num_vectors_added, 50);

    engine::IDNumbers vector_ids = source.GetVectorIds();
    ASSERT_EQ(vector_ids.size(), 50);

    status = source.Add(execution_engine_, table_file_schema, 60, num_vectors_added);
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(num_vectors_added, 50);

    vector_ids = source.GetVectorIds();
    ASSERT_EQ(vector_ids.size(), 100);

//    for (auto& id : vector_ids) {
//        std::cout << id << std::endl;
//    }

    status = impl_->DropAll();
    ASSERT_TRUE(status.ok());
}

TEST(MEM_TEST, MEM_TABLE_FILE_TEST) {

    std::shared_ptr<engine::meta::DBMetaImpl> impl_ = engine::DBMetaImplFactory::Build();
    auto options = engine::OptionsFactory::Build();

    engine::meta::TableSchema table_schema = BuildTableSchema();
    auto status = impl_->CreateTable(table_schema);
    ASSERT_TRUE(status.ok());

    engine::MemTableFile memTableFile(TABLE_NAME, impl_, options);

    int64_t n_100 = 100;
    std::vector<float> vectors_100;
    BuildVectors(n_100, vectors_100);

    engine::VectorSource::Ptr source = std::make_shared<engine::VectorSource>(n_100, vectors_100.data());

    status = memTableFile.Add(source);
    ASSERT_TRUE(status.ok());

//    std::cout << memTableFile.GetCurrentMem() << " " << memTableFile.GetMemLeft() << std::endl;

    engine::IDNumbers vector_ids = source->GetVectorIds();
    ASSERT_EQ(vector_ids.size(), 100);

    size_t singleVectorMem = sizeof(float) * TABLE_DIM;
    ASSERT_EQ(memTableFile.GetCurrentMem(), n_100 * singleVectorMem);

    int64_t n_max = engine::MAX_TABLE_FILE_MEM / singleVectorMem;
    std::vector<float> vectors_128M;
    BuildVectors(n_max, vectors_128M);

    engine::VectorSource::Ptr source_128M = std::make_shared<engine::VectorSource>(n_max, vectors_128M.data());
    status = memTableFile.Add(source_128M);

    vector_ids = source_128M->GetVectorIds();
    ASSERT_EQ(vector_ids.size(), n_max - n_100);

    ASSERT_TRUE(memTableFile.IsFull());

    status = impl_->DropAll();
    ASSERT_TRUE(status.ok());
}

TEST(MEM_TEST, MEM_TABLE_TEST) {

    std::shared_ptr<engine::meta::DBMetaImpl> impl_ = engine::DBMetaImplFactory::Build();
    auto options = engine::OptionsFactory::Build();

    engine::meta::TableSchema table_schema = BuildTableSchema();
    auto status = impl_->CreateTable(table_schema);
    ASSERT_TRUE(status.ok());

    int64_t n_100 = 100;
    std::vector<float> vectors_100;
    BuildVectors(n_100, vectors_100);

    engine::VectorSource::Ptr source_100 = std::make_shared<engine::VectorSource>(n_100, vectors_100.data());

    engine::MemTable memTable(TABLE_NAME, impl_, options);

    status = memTable.Add(source_100);
    ASSERT_TRUE(status.ok());

    engine::IDNumbers vector_ids = source_100->GetVectorIds();
    ASSERT_EQ(vector_ids.size(), 100);

    engine::MemTableFile::Ptr memTableFile;
    memTable.GetCurrentMemTableFile(memTableFile);
    size_t singleVectorMem = sizeof(float) * TABLE_DIM;
    ASSERT_EQ(memTableFile->GetCurrentMem(), n_100 * singleVectorMem);

    int64_t n_max = engine::MAX_TABLE_FILE_MEM / singleVectorMem;
    std::vector<float> vectors_128M;
    BuildVectors(n_max, vectors_128M);

    engine::VectorSource::Ptr source_128M = std::make_shared<engine::VectorSource>(n_max, vectors_128M.data());
    status = memTable.Add(source_128M);
    ASSERT_TRUE(status.ok());

    vector_ids = source_128M->GetVectorIds();
    ASSERT_EQ(vector_ids.size(), n_max);

    memTable.GetCurrentMemTableFile(memTableFile);
    ASSERT_EQ(memTableFile->GetCurrentMem(), n_100 * singleVectorMem);

    ASSERT_EQ(memTable.GetTableFileCount(), 2);

    int64_t n_1G = 1024000;
    std::vector<float> vectors_1G;
    BuildVectors(n_1G, vectors_1G);

    engine::VectorSource::Ptr source_1G = std::make_shared<engine::VectorSource>(n_1G, vectors_1G.data());

    status = memTable.Add(source_1G);
    ASSERT_TRUE(status.ok());

    vector_ids = source_1G->GetVectorIds();
    ASSERT_EQ(vector_ids.size(), n_1G);

    int expectedTableFileCount = 2 + std::ceil((n_1G - n_100) * singleVectorMem / engine::MAX_TABLE_FILE_MEM);
    ASSERT_EQ(memTable.GetTableFileCount(), expectedTableFileCount);

    status = memTable.Serialize();
    ASSERT_TRUE(status.ok());

    status = impl_->DropAll();
    ASSERT_TRUE(status.ok());
}

TEST(MEM_TEST, MEM_MANAGER_TEST) {

    auto options = engine::OptionsFactory::Build();
    options.meta.path = "/tmp/milvus_test";
    options.meta.backend_uri = "sqlite://:@:/";
    auto db_ = engine::DBFactory::Build(options);

    engine::meta::TableSchema table_info = BuildTableSchema();
    engine::Status stat = db_->CreateTable(table_info);

    engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = TABLE_NAME;
    stat = db_->DescribeTable(table_info_get);
    ASSERT_STATS(stat);
    ASSERT_EQ(table_info_get.dimension_, TABLE_DIM);

    std::map<int64_t , std::vector<float>> search_vectors;
//    std::map<int64_t , std::vector<float>> vectors_ids_map;
    {
        engine::IDNumbers vector_ids;
        int64_t nb = 1024000;
        std::vector<float> xb;
        BuildVectors(nb, xb);
        engine::Status status = db_->InsertVectors(TABLE_NAME, nb, xb.data(), vector_ids);
        ASSERT_TRUE(status.ok());

//        std::ofstream myfile("mem_test.txt");
//        for (int64_t i = 0; i < nb; ++i) {
//            int64_t vector_id = vector_ids[i];
//            std::vector<float> vectors;
//            for (int64_t j = 0; j < TABLE_DIM; j++) {
//                vectors.emplace_back(xb[i*TABLE_DIM + j]);
////                std::cout << xb[i*TABLE_DIM + j] << std::endl;
//            }
//            vectors_ids_map[vector_id] = vectors;
//        }

        std::this_thread::sleep_for(std::chrono::seconds(3));

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int64_t> dis(0, nb - 1);

        int64_t numQuery = 1000;
        for (int64_t i = 0; i < numQuery; ++i) {
            int64_t index = dis(gen);
            std::vector<float> search;
            for (int64_t j = 0; j < TABLE_DIM; j++) {
                search.push_back(xb[index * TABLE_DIM + j]);
            }
            search_vectors.insert(std::make_pair(vector_ids[index], search));
//            std::cout << "index: " << index << " vector_ids[index]: " << vector_ids[index] << std::endl;
        }

//        for (int64_t i = 0; i < nb; i += 100000) {
//            std::vector<float> search;
//            for (int64_t j = 0; j < TABLE_DIM; j++) {
//                search.push_back(xb[i * TABLE_DIM + j]);
//            }
//            search_vectors.insert(std::make_pair(vector_ids[i], search));
//        }

    }

    int k = 10;
    for(auto& pair : search_vectors) {
        auto& search = pair.second;
        engine::QueryResults results;
        stat = db_->Query(TABLE_NAME, k, 1, search.data(), results);
        for(int t = 0; t < k; t++) {
//            std::cout << "ID=" << results[0][t].first << " DISTANCE=" << results[0][t].second << std::endl;

//            std::cout << vectors_ids_map[results[0][t].first].size() << std::endl;
//            for (auto& data : vectors_ids_map[results[0][t].first]) {
//                std::cout << data << " ";
//            }
//            std::cout << std::endl;
        }
    //        std::cout << "results[0][0].first: " << results[0][0].first << " pair.first: " << pair.first << " results[0][0].second: " << results[0][0].second << std::endl;
        ASSERT_EQ(results[0][0].first, pair.first);
        ASSERT_LT(results[0][0].second, 0.00001);
    }

    stat = db_->DropAll();
    ASSERT_TRUE(stat.ok());

}

TEST(MEM_TEST, INSERT_TEST) {

    auto options = engine::OptionsFactory::Build();
    options.meta.path = "/tmp/milvus_test";
    options.meta.backend_uri = "sqlite://:@:/";
    auto db_ = engine::DBFactory::Build(options);

    engine::meta::TableSchema table_info = BuildTableSchema();
    engine::Status stat = db_->CreateTable(table_info);

    engine::meta::TableSchema table_info_get;
    table_info_get.table_id_ = TABLE_NAME;
    stat = db_->DescribeTable(table_info_get);
    ASSERT_STATS(stat);
    ASSERT_EQ(table_info_get.dimension_, TABLE_DIM);

    auto start_time = METRICS_NOW_TIME;

    int insert_loop = 1000;
    for (int i = 0; i < insert_loop; ++i) {
        int64_t nb = 204800;
        std::vector<float> xb;
        BuildVectors(nb, xb);
        engine::IDNumbers vector_ids;
        engine::Status status = db_->InsertVectors(TABLE_NAME, nb, xb.data(), vector_ids);
        ASSERT_TRUE(status.ok());
    }
    auto end_time = METRICS_NOW_TIME;
    auto total_time = METRICS_MICROSECONDS(start_time, end_time);
    std::cout << "total_time(ms) : " << total_time << std::endl;

    stat = db_->DropAll();
    ASSERT_TRUE(stat.ok());

}

