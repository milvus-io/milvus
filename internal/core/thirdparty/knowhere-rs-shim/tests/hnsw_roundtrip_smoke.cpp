#include <cmath>
#include <iostream>
#include <string>
#include <vector>

#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "knowhere/expected.h"
#include "knowhere/index/index_factory.h"
#include "knowhere/version.h"

namespace {

template <typename JsonValue>
int
ExpectInvalidSearch(const knowhere::Index<knowhere::IndexNode>& index,
                    const knowhere::DataSetPtr& query_ds,
                    JsonValue&& ef_value,
                    bool nest_under_params,
                    const std::string& expected_message,
                    int exit_code) {
    knowhere::Config invalid_cfg;
    invalid_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::L2;
    invalid_cfg[knowhere::meta::TOPK] = 10;
    if (nest_under_params) {
        invalid_cfg["params"][knowhere::indexparam::EF] =
            std::forward<JsonValue>(ef_value);
    } else {
        invalid_cfg[knowhere::indexparam::EF] =
            std::forward<JsonValue>(ef_value);
    }

    auto search = index.Search(*query_ds, invalid_cfg, knowhere::BitsetView{});
    if (search.has_value() || search.error() != knowhere::Status::invalid_args ||
        search.what().find(expected_message) == std::string::npos) {
        std::cerr << "unexpected invalid hnsw search result: status="
                  << (search.has_value() ? "success"
                                         : knowhere::Status2String(search.error()))
                  << " message=" << (search.has_value() ? "" : search.what())
                  << "\n";
        return exit_code;
    }
    return 0;
}

}  // namespace

int
main() {
    {
        const std::vector<float> base = {
            0.0f, 0.0f, 0.0f, 0.0f,
            1.0f, 0.0f, 0.0f, 0.0f,
            0.0f, 1.0f, 0.0f, 0.0f,
            0.0f, 0.0f, 1.0f, 0.0f,
        };
        const std::vector<float> query = {1.0f, 0.0f, 0.0f, 0.0f};

        knowhere::Config build_cfg;
        build_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::L2;
        build_cfg[knowhere::meta::DIM] = 4;
        build_cfg[knowhere::indexparam::M] = 16;
        build_cfg[knowhere::indexparam::EFCONSTRUCTION] = 64;
        build_cfg[knowhere::indexparam::EF] = 32;

        auto base_ds = knowhere::GenDataSet(4, 4, base.data());
        auto query_ds = knowhere::GenDataSet(1, 4, query.data());

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

        if (const auto rc = ExpectInvalidSearch(index,
                                                query_ds,
                                                -1,
                                                false,
                                                "param 'ef' (-1) should be in range [1, 2147483647]",
                                                23);
            rc != 0) {
            return rc;
        }
        if (const auto rc =
                ExpectInvalidSearch(index,
                                    query_ds,
                                    32.0,
                                    true,
                                    "Type conflict in json: param 'ef' (32.0) should be integer",
                                    24);
            rc != 0) {
            return rc;
        }
        if (const auto rc =
                ExpectInvalidSearch(index,
                                    query_ds,
                                    true,
                                    true,
                                    "Type conflict in json: param 'ef' (true) should be integer",
                                    25);
            rc != 0) {
            return rc;
        }
        if (const auto rc =
                ExpectInvalidSearch(index,
                                    query_ds,
                                    nullptr,
                                    true,
                                    "Type conflict in json: param 'ef' (null) should be integer",
                                    26);
            rc != 0) {
            return rc;
        }
        if (const auto rc = ExpectInvalidSearch(index,
                                                query_ds,
                                                knowhere::Json::array({32}),
                                                true,
                                                "param 'ef' ([32]) should be integer",
                                                27);
            rc != 0) {
            return rc;
        }
        if (const auto rc = ExpectInvalidSearch(index,
                                                query_ds,
                                                1,
                                                false,
                                                "ef(1) should be larger than k(10)",
                                                28);
            rc != 0) {
            return rc;
        }

        knowhere::Config search_cfg;
        search_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::L2;
        search_cfg[knowhere::meta::TOPK] = 2;
        search_cfg[knowhere::indexparam::EF] = 32;

        auto search = index.Search(*query_ds, search_cfg, knowhere::BitsetView{});
        if (!search.has_value() || search.value() == nullptr ||
            search.value()->GetIds() == nullptr ||
            search.value()->GetDistance() == nullptr) {
            std::cerr << "failed to search hnsw index\n";
            return 3;
        }
        auto iterators = index.AnnIterator(*query_ds, search_cfg, knowhere::BitsetView{});
        if (!iterators.has_value() || iterators.value().size() != 1 ||
            iterators.value()[0] == nullptr || !iterators.value()[0]->HasNext()) {
            std::cerr << "failed to initialize hnsw iterator\n";
            return 8;
        }
        auto [iterator_id, iterator_distance] = iterators.value()[0]->Next();
        if (iterator_id != 1 || iterator_distance < 0.0f) {
            std::cerr << "unexpected hnsw iterator top result\n";
            return 9;
        }

        knowhere::BinarySet binary_set;
        if (index.Serialize(binary_set) != knowhere::Status::success ||
            !binary_set.Contains("index_data")) {
            std::cerr << "failed to serialize hnsw index\n";
            return 4;
        }

        auto restored_created = knowhere::IndexFactory::Instance().Create<float>(
            knowhere::IndexEnum::INDEX_HNSW,
            knowhere::Version::GetCurrentVersion());
        if (!restored_created.has_value()) {
            std::cerr << "failed to create restored hnsw index\n";
            return 5;
        }

        auto restored = restored_created.value();
        if (restored.Deserialize(binary_set, build_cfg) != knowhere::Status::success) {
            std::cerr << "failed to deserialize hnsw index\n";
            return 6;
        }

        auto restored_search =
            restored.Search(*query_ds, search_cfg, knowhere::BitsetView{});
        if (!restored_search.has_value() || restored_search.value() == nullptr ||
            restored_search.value()->GetIds() == nullptr ||
            restored_search.value()->GetDistance() == nullptr) {
            std::cerr << "failed to search restored hnsw index\n";
            return 7;
        }
        auto restored_iterators =
            restored.AnnIterator(*query_ds, search_cfg, knowhere::BitsetView{});
        if (!restored_iterators.has_value() || restored_iterators.value().size() != 1 ||
            restored_iterators.value()[0] == nullptr ||
            !restored_iterators.value()[0]->HasNext()) {
            std::cerr << "failed to initialize restored hnsw iterator\n";
            return 10;
        }
    }

    {
        const std::vector<float> base = {
            100.0f, 0.0f, 0.0f, 0.0f,
            0.0f, 1.0f, 0.0f, 0.0f,
            1.0f, 1.0f, 0.0f, 0.0f,
            0.0f, 0.0f, 1.0f, 0.0f,
        };
        const std::vector<float> query = {1.0f, 1.0f, 0.0f, 0.0f};

        knowhere::Config build_cfg;
        build_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::COSINE;
        build_cfg[knowhere::meta::DIM] = 4;
        build_cfg[knowhere::indexparam::M] = 16;
        build_cfg[knowhere::indexparam::EFCONSTRUCTION] = 64;
        build_cfg[knowhere::indexparam::EF] = 32;

        auto base_ds = knowhere::GenDataSet(4, 4, base.data());
        auto query_ds = knowhere::GenDataSet(1, 4, query.data());

        auto created = knowhere::IndexFactory::Instance().Create<float>(
            knowhere::IndexEnum::INDEX_HNSW,
            knowhere::Version::GetCurrentVersion());
        if (!created.has_value()) {
            std::cerr << "failed to create cosine hnsw index\n";
            return 11;
        }

        auto index = created.value();
        if (index.Build(*base_ds, build_cfg) != knowhere::Status::success) {
            std::cerr << "failed to build cosine hnsw index\n";
            return 12;
        }

        knowhere::Config search_cfg;
        search_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::COSINE;
        search_cfg[knowhere::meta::TOPK] = 2;
        search_cfg[knowhere::indexparam::EF] = 32;

        auto direct = index.Search(*query_ds, search_cfg, knowhere::BitsetView{});
        if (!direct.has_value() || direct.value() == nullptr ||
            direct.value()->GetDistance() == nullptr ||
            direct.value()->GetIds() == nullptr) {
            std::cerr << "unexpected cosine hnsw search result before roundtrip\n";
            return 13;
        }
        if (direct.value()->GetIds()[0] != 2 ||
            std::fabs(direct.value()->GetDistance()[0] - 1.0f) > 1e-4f) {
            std::cerr << "cosine hnsw did not prefer the normalized best match before roundtrip"
                      << " id=" << direct.value()->GetIds()[0]
                      << " distance=" << direct.value()->GetDistance()[0] << "\n";
            return 14;
        }
        const auto direct_top_id = direct.value()->GetIds()[0];
        const auto direct_top_distance = direct.value()->GetDistance()[0];

        knowhere::BinarySet binary_set;
        if (index.Serialize(binary_set) != knowhere::Status::success ||
            !binary_set.Contains("index_data")) {
            std::cerr << "failed to serialize cosine hnsw index\n";
            return 15;
        }

        auto restored_created = knowhere::IndexFactory::Instance().Create<float>(
            knowhere::IndexEnum::INDEX_HNSW,
            knowhere::Version::GetCurrentVersion());
        if (!restored_created.has_value()) {
            std::cerr << "failed to create restored cosine hnsw index\n";
            return 16;
        }

        auto restored = restored_created.value();
        knowhere::Config restore_cfg;
        restore_cfg[knowhere::meta::DIM] = 4;
        if (restored.Deserialize(binary_set, restore_cfg) != knowhere::Status::success) {
            std::cerr << "failed to deserialize cosine hnsw index\n";
            return 17;
        }

        auto restored_search =
            restored.Search(*query_ds, search_cfg, knowhere::BitsetView{});
        if (!restored_search.has_value() || restored_search.value() == nullptr ||
            restored_search.value()->GetDistance() == nullptr ||
            restored_search.value()->GetIds() == nullptr ||
            restored_search.value()->GetIds()[0] != direct_top_id ||
            std::fabs(restored_search.value()->GetDistance()[0] - direct_top_distance) >
                1e-4f) {
            std::cerr << "cosine metric was not preserved across hnsw roundtrip\n";
            return 18;
        }

        auto restored_iterators =
            restored.AnnIterator(*query_ds, search_cfg, knowhere::BitsetView{});
        if (!restored_iterators.has_value() || restored_iterators.value().size() != 1 ||
            restored_iterators.value()[0] == nullptr ||
            !restored_iterators.value()[0]->HasNext()) {
            std::cerr << "failed to initialize restored cosine hnsw iterator\n";
            return 19;
        }
        auto direct_iterators =
            index.AnnIterator(*query_ds, search_cfg, knowhere::BitsetView{});
        if (!direct_iterators.has_value() || direct_iterators.value().size() != 1 ||
            direct_iterators.value()[0] == nullptr ||
            !direct_iterators.value()[0]->HasNext()) {
            std::cerr << "failed to initialize direct cosine hnsw iterator\n";
            return 20;
        }
        auto [direct_iterator_id, direct_iterator_distance] =
            direct_iterators.value()[0]->Next();
        if (direct_iterator_id != 2 ||
            std::fabs(direct_iterator_distance - 1.0f) > 1e-4f) {
            std::cerr << "cosine hnsw iterator did not prefer the normalized best match before roundtrip"
                      << " id=" << direct_iterator_id
                      << " distance=" << direct_iterator_distance << "\n";
            return 21;
        }

        auto [id, distance] = restored_iterators.value()[0]->Next();
        if (id != direct_iterator_id ||
            std::fabs(distance - direct_iterator_distance) > 1e-4f) {
            std::cerr << "cosine iterator metric was not preserved across hnsw roundtrip\n";
            return 22;
        }
    }

    return 0;
}
