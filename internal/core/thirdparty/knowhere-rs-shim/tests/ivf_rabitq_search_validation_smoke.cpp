#include <iostream>
#include <string>
#include <vector>

#include "knowhere/bitsetview.h"
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
                    const char* key,
                    JsonValue&& value,
                    bool nest_under_params,
                    const std::string& expected_message,
                    int exit_code) {
    knowhere::Config invalid_cfg;
    invalid_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::COSINE;
    invalid_cfg[knowhere::meta::TOPK] = 10;
    if (nest_under_params) {
        invalid_cfg["params"][key] = std::forward<JsonValue>(value);
    } else {
        invalid_cfg[key] = std::forward<JsonValue>(value);
    }

    auto search = index.Search(*query_ds, invalid_cfg, knowhere::BitsetView{});
    if (search.has_value() || search.error() != knowhere::Status::invalid_args ||
        search.what().find(expected_message) == std::string::npos) {
        std::cerr << "unexpected invalid IVF_RABITQ search result: status="
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
    const std::vector<float> base = {
        1.0f, 0.0f, 0.0f, 0.0f,
        0.0f, 1.0f, 0.0f, 0.0f,
        0.0f, 0.0f, 1.0f, 0.0f,
        0.0f, 0.0f, 0.0f, 1.0f,
    };
    const std::vector<float> query = {1.0f, 0.0f, 0.0f, 0.0f};

    knowhere::Config build_cfg;
    build_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::COSINE;
    build_cfg[knowhere::meta::DIM] = 4;
    build_cfg[knowhere::indexparam::NLIST] = 4;
    build_cfg[knowhere::indexparam::REFINE] = true;
    build_cfg[knowhere::indexparam::REFINE_TYPE] = "SQ8";

    auto base_ds = knowhere::GenDataSet(4, 4, base.data());
    auto query_ds = knowhere::GenDataSet(1, 4, query.data());

    auto created = knowhere::IndexFactory::Instance().Create<float>(
        knowhere::IndexEnum::INDEX_FAISS_IVF_RABITQ,
        knowhere::Version::GetCurrentVersion());
    if (!created.has_value()) {
        std::cerr << "failed to create IVF_RABITQ index\n";
        return 1;
    }

    auto index = created.value();
    if (index.Build(*base_ds, build_cfg) != knowhere::Status::success) {
        std::cerr << "failed to build IVF_RABITQ index\n";
        return 2;
    }

    if (const auto rc = ExpectInvalidSearch(index,
                                            query_ds,
                                            knowhere::indexparam::NPROBE,
                                            -1,
                                            false,
                                            "Out of range in json: param 'nprobe' (-1) should be in range [1, 65536]",
                                            3);
        rc != 0) {
        return rc;
    }
    if (const auto rc = ExpectInvalidSearch(index,
                                            query_ds,
                                            "rbq_bits_query",
                                            6.0,
                                            true,
                                            "Type conflict in json: param 'rbq_bits_query' (6.0) should be integer",
                                            4);
        rc != 0) {
        return rc;
    }
    if (const auto rc = ExpectInvalidSearch(index,
                                            query_ds,
                                            knowhere::indexparam::REFINE_K,
                                            true,
                                            true,
                                            "Type conflict in json: param 'refine_k' (true) should be a number",
                                            5);
        rc != 0) {
        return rc;
    }

    knowhere::Config valid_cfg;
    valid_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::COSINE;
    valid_cfg[knowhere::meta::TOPK] = 2;
    valid_cfg["params"][knowhere::indexparam::NPROBE] = "2";
    valid_cfg["params"]["rbq_bits_query"] = "6";
    valid_cfg["params"][knowhere::indexparam::REFINE_K] = "2.0";

    auto search = index.Search(*query_ds, valid_cfg, knowhere::BitsetView{});
    if (!search.has_value() || search.value() == nullptr ||
        search.value()->GetIds() == nullptr ||
        search.value()->GetDistance() == nullptr) {
        std::cerr << "failed to search IVF_RABITQ index with valid params\n";
        return 6;
    }

    return 0;
}
