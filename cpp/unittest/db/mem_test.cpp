#include "gtest/gtest.h"

#include "db/VectorSource.h"
#include "db/MemTableFile.h"
#include "db/MemTable.h"
#include "utils.h"
#include "db/Factories.h"
#include "db/Constants.h"

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
    status = source.Add(table_file_schema, 50, num_vectors_added);
    ASSERT_TRUE(status.ok());

    ASSERT_EQ(num_vectors_added, 50);

    engine::IDNumbers vector_ids = source.GetVectorIds();
    ASSERT_EQ(vector_ids.size(), 50);

    status = source.Add(table_file_schema, 60, num_vectors_added);
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

    engine::meta::TableSchema table_schema = BuildTableSchema();
    auto status = impl_->CreateTable(table_schema);
    ASSERT_TRUE(status.ok());

    engine::MemTableFile memTableFile(TABLE_NAME, impl_);

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

    ASSERT_TRUE(memTableFile.isFull());

    status = impl_->DropAll();
    ASSERT_TRUE(status.ok());
}

TEST(MEM_TEST, MEM_TABLE_TEST) {

    std::shared_ptr<engine::meta::DBMetaImpl> impl_ = engine::DBMetaImplFactory::Build();

    engine::meta::TableSchema table_schema = BuildTableSchema();
    auto status = impl_->CreateTable(table_schema);
    ASSERT_TRUE(status.ok());

    int64_t n_100 = 100;
    std::vector<float> vectors_100;
    BuildVectors(n_100, vectors_100);

    engine::VectorSource::Ptr source_100 = std::make_shared<engine::VectorSource>(n_100, vectors_100.data());

    engine::MemTable memTable(TABLE_NAME, impl_);

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

    ASSERT_EQ(memTable.GetStackSize(), 2);

    int64_t n_1G = 1024000;
    std::vector<float> vectors_1G;
    BuildVectors(n_1G, vectors_1G);

    engine::VectorSource::Ptr source_1G = std::make_shared<engine::VectorSource>(n_1G, vectors_1G.data());

    status = memTable.Add(source_1G);
    ASSERT_TRUE(status.ok());

    vector_ids = source_1G->GetVectorIds();
    ASSERT_EQ(vector_ids.size(), n_1G);

    int expectedStackSize = 2 + std::ceil((n_1G - n_100) * singleVectorMem / engine::MAX_TABLE_FILE_MEM);
    ASSERT_EQ(memTable.GetStackSize(), expectedStackSize);

    status = impl_->DropAll();
    ASSERT_TRUE(status.ok());
}


