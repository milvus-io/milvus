#include <cmath>
#include <iostream>
#include <memory>
#include <vector>

#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "knowhere/index/index_factory.h"
#include "knowhere/version.h"

int
main() {
    constexpr int64_t kRows = 2000;
    constexpr int64_t kDim = 8;
    constexpr int64_t kTopK = 1500;

    std::vector<float> base(static_cast<size_t>(kRows * kDim), 0.0f);
    for (int64_t row = 0; row < kRows; ++row) {
        base[static_cast<size_t>(row * kDim)] = static_cast<float>(kRows - row);
    }
    const std::vector<float> query = {
        static_cast<float>(kRows), 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f,
    };

    knowhere::Config build_cfg;
    build_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::IP;
    build_cfg[knowhere::meta::DIM] = kDim;
    build_cfg[knowhere::indexparam::M] = 16;
    build_cfg[knowhere::indexparam::EFCONSTRUCTION] = 64;
    build_cfg[knowhere::indexparam::EF] = 64;

    auto base_ds = knowhere::GenDataSet(kRows, kDim, base.data());
    auto query_ds = knowhere::GenDataSet(1, kDim, query.data());

    auto created = knowhere::IndexFactory::Instance().Create<float>(
        knowhere::IndexEnum::INDEX_HNSW,
        knowhere::Version::GetCurrentVersion());
    if (!created.has_value()) {
        std::cerr << "failed to create hnsw index\n";
        return 1;
    }

    auto index = created.value();
    if (index.Build(*base_ds, build_cfg) != knowhere::Status::success) {
        std::cerr << "failed to build hnsw index\n";
        return 2;
    }
    if (index.Count() != kRows) {
        std::cerr << "unexpected indexed row count: " << index.Count() << "\n";
        return 3;
    }

    knowhere::Config search_cfg;
    search_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::IP;
    search_cfg[knowhere::meta::TOPK] = kTopK;
    search_cfg[knowhere::indexparam::EF] = kRows;

    auto search = index.Search(*query_ds, search_cfg, knowhere::BitsetView{});
    if (!search.has_value() || search.value() == nullptr ||
        search.value()->GetIds() == nullptr ||
        search.value()->GetDistance() == nullptr) {
        std::cerr << "failed to search hnsw index\n";
        return 4;
    }

    int64_t populated = 0;
    for (int64_t i = 0; i < kTopK; ++i) {
        if (search.value()->GetIds()[i] >= 0) {
            ++populated;
        }
    }
    if (populated != kTopK) {
        std::cerr << "search populated " << populated << " results, expected "
                  << kTopK << "\n";
        return 5;
    }

    auto iterators = index.AnnIterator(*query_ds, search_cfg, knowhere::BitsetView{});
    if (!iterators.has_value() || iterators.value().size() != 1 ||
        iterators.value()[0] == nullptr) {
        std::cerr << "failed to initialize hnsw iterator\n";
        return 6;
    }

    int64_t iterator_hits = 0;
    auto& iterator = iterators.value()[0];
    float previous_distance = std::numeric_limits<float>::infinity();
    while (iterator->HasNext() && iterator_hits < kTopK) {
        auto [id, distance] = iterator->Next();
        if (id < 0) {
            break;
        }
        if (distance > previous_distance + 1e-5f) {
            std::cerr << "iterator results are not monotonic\n";
            return 7;
        }
        previous_distance = distance;
        ++iterator_hits;
    }
    if (iterator_hits != kTopK) {
        std::cerr << "iterator produced " << iterator_hits
                  << " results before exhaustion, expected " << kTopK << "\n";
        return 8;
    }

    knowhere::BinarySet binary_set;
    if (index.Serialize(binary_set) != knowhere::Status::success ||
        !binary_set.Contains("index_data")) {
        std::cerr << "failed to serialize hnsw index\n";
        return 9;
    }

    auto restored_created = knowhere::IndexFactory::Instance().Create<float>(
        knowhere::IndexEnum::INDEX_HNSW,
        knowhere::Version::GetCurrentVersion());
    if (!restored_created.has_value()) {
        std::cerr << "failed to create restored hnsw index\n";
        return 10;
    }

    auto restored = restored_created.value();
    if (restored.Deserialize(binary_set, build_cfg) != knowhere::Status::success) {
        std::cerr << "failed to deserialize hnsw index\n";
        return 11;
    }

    auto restored_search =
        restored.Search(*query_ds, search_cfg, knowhere::BitsetView{});
    if (!restored_search.has_value() || restored_search.value() == nullptr ||
        restored_search.value()->GetIds() == nullptr ||
        restored_search.value()->GetDistance() == nullptr) {
        std::cerr << "failed to search restored hnsw index\n";
        return 12;
    }

    populated = 0;
    for (int64_t i = 0; i < kTopK; ++i) {
        if (restored_search.value()->GetIds()[i] >= 0) {
            ++populated;
        }
    }
    if (populated != kTopK) {
        std::cerr << "restored search populated " << populated
                  << " results, expected " << kTopK << "\n";
        return 13;
    }

    auto restored_iterators =
        restored.AnnIterator(*query_ds, search_cfg, knowhere::BitsetView{});
    if (!restored_iterators.has_value() || restored_iterators.value().size() != 1 ||
        restored_iterators.value()[0] == nullptr) {
        std::cerr << "failed to initialize restored hnsw iterator\n";
        return 14;
    }

    iterator_hits = 0;
    auto& restored_iterator = restored_iterators.value()[0];
    previous_distance = std::numeric_limits<float>::infinity();
    while (restored_iterator->HasNext() && iterator_hits < kTopK) {
        auto [id, distance] = restored_iterator->Next();
        if (id < 0) {
            break;
        }
        if (distance > previous_distance + 1e-5f) {
            std::cerr << "restored iterator results are not monotonic\n";
            return 15;
        }
        previous_distance = distance;
        ++iterator_hits;
    }
    if (iterator_hits != kTopK) {
        std::cerr << "restored iterator produced " << iterator_hits
                  << " results before exhaustion, expected " << kTopK << "\n";
        return 16;
    }

    return 0;
}
