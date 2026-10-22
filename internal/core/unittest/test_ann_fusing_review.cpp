// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.
#include <gtest/gtest.h>
#include <atomic>
#include <fstream>
#include <thread>
#include <filesystem>
#include "filemanager/impl/LocalFileManager.h"
#include "knowhere/thread_pool.h"
#include "knowhere/index/index_factory.h"
#include "knowhere/comp/index_param.h"
#include "test_utils/OriginalCohereFixture.h"

namespace {
using Status = knowhere::CandidateEvalStatus;
struct Factory {
    const std::vector<int64_t>* values;
    mutable std::atomic<int> created{0}, destroyed{0}, wrong_thread{0};
    std::atomic<bool> fail{false};
};
struct Worker { const Factory* factory; std::thread::id owner; };
Status Create(const void* p, void** out) noexcept {
    *out = nullptr;
    try {
        const auto* f = static_cast<const Factory*>(p);
        *out = new Worker{f, std::this_thread::get_id()};
        ++f->created;
        return Status::Success;
    } catch (...) { return Status::Failed; }
}
void Destroy(void* p) noexcept {
    auto* w = static_cast<Worker*>(p);
    ++w->factory->destroyed;
    delete w;
}
Status Eval(void* p, const int32_t* rows, uint32_t n, uint64_t active, uint64_t* result) noexcept {
    auto* w = static_cast<Worker*>(p);
    if (w->owner != std::this_thread::get_id()) {
        ++w->factory->wrong_thread;
        return Status::Failed;
    }
    if (w->factory->fail) return Status::Failed;
    *result = 0;
    for (uint32_t i = 0; i < n; ++i) {
        if (!(active & (uint64_t{1} << i))) continue;
        if (rows[i] < 0 || static_cast<size_t>(rows[i]) >= w->factory->values->size()) return Status::InvalidArgument;
        if ((*w->factory->values)[rows[i]] % 5 < 3) *result |= uint64_t{1} << i;
    }
    return Status::Success;
}
auto View(const Factory& f) {
    return knowhere::CandidateEvaluatorViewV1{1, sizeof(knowhere::CandidateEvaluatorViewV1), &f, Create, Destroy, Eval};
}

// Metadata only: no generated vectors or scalar values. Count and mappings
// describe original Cohere rows; all unrelated operations remain unsupported.
class MetadataIndex : public knowhere::IndexNode {
 public:
    explicit MetadataIndex(int64_t count) : count_(count) {}
    int64_t Count() const override { return count_; }
    int64_t Dim() const override { return 768; }
    int64_t Size() const override { return 0; }
    std::string Type() const override { return "metadata-only"; }
    bool HasRawData(const std::string&) const override { return false; }
    std::unique_ptr<knowhere::BaseConfig> CreateConfig() const override { return nullptr; }
    knowhere::Status Train(knowhere::DataSetPtr, std::shared_ptr<knowhere::Config>, bool) override { return knowhere::Status::not_implemented; }
    knowhere::Status Add(knowhere::DataSetPtr, std::shared_ptr<knowhere::Config>, bool) override { return knowhere::Status::not_implemented; }
    knowhere::Status Serialize(knowhere::BinarySet&) const override { return knowhere::Status::not_implemented; }
    knowhere::Status Deserialize(const knowhere::BinarySet&, std::shared_ptr<knowhere::Config>) override { return knowhere::Status::not_implemented; }
    knowhere::Status DeserializeFromFile(const std::string&, std::shared_ptr<knowhere::Config>) override { return knowhere::Status::not_implemented; }
    knowhere::expected<knowhere::DataSetPtr> Search(knowhere::DataSetPtr, std::unique_ptr<knowhere::Config>, const knowhere::BitsetView&, milvus::OpContext*) const override { return knowhere::expected<knowhere::DataSetPtr>::Err(knowhere::Status::not_implemented, "metadata only"); }
    knowhere::expected<knowhere::DataSetPtr> GetVectorByIds(knowhere::DataSetPtr, milvus::OpContext*) const override { return knowhere::expected<knowhere::DataSetPtr>::Err(knowhere::Status::not_implemented, "metadata only"); }
    knowhere::expected<knowhere::DataSetPtr> GetIndexMeta(std::unique_ptr<knowhere::Config>) const override { return knowhere::expected<knowhere::DataSetPtr>::Err(knowhere::Status::not_implemented, "metadata only"); }
 private:
    int64_t count_;
};

TEST(AnnFusingReview, CallbackOnlyDomainAndMapping) {
    const auto values = LoadOriginalCohereIds();
    Factory f{&values}; const auto descriptor = View(f);
    knowhere::BitsetView filter; filter.set_candidate_evaluator(&descriptor);
    MetadataIndex node(values.size());
    auto prepared = node.PrepareBitset(filter);
    ASSERT_TRUE(prepared.has_value());
    EXPECT_EQ(prepared.value().bitset.size(), values.size());
    EXPECT_EQ(prepared.value().bitset.num_bits(), 0);
    EXPECT_EQ(f.created, 0);
    auto bound = prepared.value().bitset.bind();
    const int32_t offsets[] = {0, 1, 3};
    EXPECT_EQ(bound.test(offsets, 3), uint64_t{4});
    MetadataIndex mapped(4);
    mapped.SetIdMapType(knowhere::IdMap::Type::SEALED);
    const int32_t out_ids[] = {100, 7, 900, 0};
    knowhere::IdMapData mapping;
    mapping.format = knowhere::IdMapData::Format::IDS;
    mapping.out_count = values.size(); mapping.in_count = 4; mapping.out_ids = out_ids;
    mapped.GetIdMap().AddFromData(mapping);
    auto projected = mapped.PrepareBitset(filter);
    ASSERT_TRUE(projected.has_value());
    EXPECT_EQ(projected.value().bitset.size(), 4);
    auto worker = projected.value().bitset.bind();
    const int32_t in_ids[] = {0, 1, 2, 3};
    uint64_t expected = 0;
    for (int i=0; i<4; ++i) if (!(values[out_ids[i]] % 5 < 3)) expected |= uint64_t{1} << i;
    EXPECT_EQ(worker.test(in_ids, 4), expected);
}

std::vector<float> ReadOriginalPrefix(const char* path, size_t count) {
    std::ifstream input(path, std::ios::binary);
    if (!input.good()) throw std::runtime_error("original vector/query source missing");
    std::vector<float> values(count);
    input.read(reinterpret_cast<char*>(values.data()), count * sizeof(float));
    if (!input) throw std::runtime_error("truncated original vector/query prefix");
    return values;
}

TEST(AnnFusingReview, FaissIndependentQueryWorkersAndExactResults) {
    const auto* source = std::getenv("MILVUS_ORIGINAL_COHERE_VECTORS");
    const auto* queries = std::getenv("MILVUS_ORIGINAL_COHERE_QUERIES");
    if (!source || !queries) GTEST_SKIP() << "optional hash-verified original vector/query sources required";
    const auto values = LoadOriginalCohereIds();
    constexpr size_t dim=768, nq=16, k=10;
    auto vectors = ReadOriginalPrefix(source, values.size()*dim);
    auto query = ReadOriginalPrefix(queries, nq*dim);
    auto node = knowhere::IndexFactory::Instance().Create<knowhere::fp32>("FAISS", 0).value();
    knowhere::Json config{{"metric_type","L2"},{"faiss_index_name","Flat"},{"dim",dim},{"k",k}};
    ASSERT_EQ(node.Build(knowhere::GenDataSet(values.size(),dim,vectors.data()),config),knowhere::Status::success);
    Factory f{&values}; const auto descriptor=View(f);
    knowhere::BitsetView dynamic; dynamic.set_candidate_evaluator(&descriptor);
    std::vector<uint8_t> bitmap((values.size()+7)/8,0); size_t count=0;
    for (size_t i=0;i<values.size();++i) if (!(values[i]%5<3)) { bitmap[i/8] |= uint8_t{1}<<(i%8); ++count; }
    knowhere::BitsetView baseline(bitmap.data(),values.size(),count);
    auto dataset=knowhere::GenDataSet(nq,dim,query.data());
    auto expected=node.Search(dataset,config,baseline);
    ASSERT_TRUE(expected.has_value());
    auto actual=node.Search(dataset,config,dynamic);
    ASSERT_TRUE(actual.has_value()) << actual.what();
    for (size_t i=0;i<nq*k;++i) {
        EXPECT_EQ(actual.value()->GetIds()[i],expected.value()->GetIds()[i]);
        EXPECT_FLOAT_EQ(actual.value()->GetDistance()[i],expected.value()->GetDistance()[i]);
    }
    EXPECT_EQ(f.created,nq); EXPECT_EQ(f.destroyed,nq); EXPECT_EQ(f.wrong_thread,0);
    f.fail=true;
    EXPECT_FALSE(node.Search(dataset,config,dynamic).has_value());
    f.fail=false;
    EXPECT_TRUE(node.Search(dataset,config,dynamic).has_value());
    EXPECT_EQ(f.created,f.destroyed);
}

TEST(AnnFusingReview, DiskannFailurePoolRecoveryAndIteratorBinding) {
    const auto* source = std::getenv("MILVUS_ORIGINAL_COHERE_VECTORS");
    const auto* queries = std::getenv("MILVUS_ORIGINAL_COHERE_QUERIES");
    const auto* directory = std::getenv("MILVUS_REVIEW_NATIVE_OUTPUT");
    if (!source || !queries || !directory)
        GTEST_SKIP() << "hash-verified original sources and isolated output required";
    const auto values = LoadOriginalCohereIds();
    constexpr uint32_t dim=768;
    auto vectors = ReadOriginalPrefix(source, values.size()*dim);
    auto query = ReadOriginalPrefix(queries, dim);
    const auto root = std::filesystem::path(directory)/"diskann-functional";
    ASSERT_TRUE(std::filesystem::create_directory(root));
    // New functional-only index on original rows, never the frozen performance
    // graph. DiskANN's file header is the only added data, not replacement rows.
    const auto raw = (root/"original-prefix.bin").string();
    const uint32_t rows = values.size();
    {
        std::ofstream output(raw,std::ios::binary);
        output.write(reinterpret_cast<const char*>(&rows),sizeof(rows));
        output.write(reinterpret_cast<const char*>(&dim),sizeof(dim));
        output.write(reinterpret_cast<const char*>(vectors.data()),vectors.size()*sizeof(float));
        ASSERT_TRUE(output.good());
    }
    knowhere::Json config{{"metric_type","L2"},{"dim",dim},{"k",10},
        {"index_prefix",(root/"index").string()},{"data_path",raw},
        {"max_degree",32},{"search_list_size",64},{"beamwidth",4},
        {"pq_code_budget_gb",0.0001},{"build_dram_budget_gb",1.0},
        {"search_cache_budget_gb",0.0}};
    std::shared_ptr<milvus::FileManager> manager=std::make_shared<milvus::LocalFileManager>();
    auto file_manager = knowhere::Pack(manager);
    // Cardinal builds register DISKANN as a version-routing wrapper. Target
    // the reviewed native implementation explicitly, not the wrapper gate.
    auto node = knowhere::IndexFactory::Instance().Create<knowhere::fp32>("DISKANN_DEPRECATED",0,file_manager).value();
    ASSERT_EQ(node.Build(nullptr,config),knowhere::Status::success);
    knowhere::BinarySet files;
    ASSERT_EQ(node.Serialize(files),knowhere::Status::success);
    ASSERT_EQ(node.Deserialize(files,config),knowhere::Status::success);
    Factory f{&values}; const auto descriptor=View(f);
    knowhere::BitsetView dynamic; dynamic.set_candidate_evaluator(&descriptor);
    auto dataset=knowhere::GenDataSet(1,dim,query.data());
    const auto pool_size=knowhere::ThreadPool::GetGlobalSearchThreadPool()->size();
    // More failing searches than scratch entries: a leak hangs this bounded
    // subprocess instead of silently passing one success/failure pair.
    f.fail=true;
    for (size_t i=0;i<2*pool_size+3;++i)
        ASSERT_FALSE(node.Search(dataset,config,dynamic).has_value());
    f.fail=false;
    auto result=node.Search(dataset,config,dynamic);
    ASSERT_TRUE(result.has_value()) << result.what();
    for (int i=0;i<10;++i) {
        const auto row=result.value()->GetIds()[i];
        ASSERT_GE(row,0); ASSERT_LT(row,values.size());
        EXPECT_LT(values[row]%5,3);
    }
    EXPECT_EQ(f.created,f.destroyed);
    const auto before=f.created.load();
    {
        auto iterators=node.AnnIterator(dataset,config,dynamic,false);
        ASSERT_TRUE(iterators.has_value()) << iterators.what();
        auto& iterator=iterators.value().at(0);
        for (int i=0;i<20;++i) {
            const auto has_next=iterator->HasNext();
            ASSERT_TRUE(has_next.has_value()); ASSERT_TRUE(has_next.value());
            const auto next=iterator->Next();
            ASSERT_TRUE(next.has_value()) << next.what();
            const auto [row,distance]=next.value();
            ASSERT_GE(row,0); ASSERT_LT(row,values.size());
            EXPECT_LT(values[row]%5,3);
        }
        EXPECT_EQ(f.created,before+1);
    }
    EXPECT_EQ(f.created,f.destroyed); EXPECT_EQ(f.wrong_thread,0);
}
}  // namespace
