#include <cmath>
#include <iostream>
#include <vector>

#include "knowhere/bitsetview.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"
#include "knowhere/index/index_factory.h"
#include "knowhere/version.h"

namespace knowhere {
std::shared_ptr<IndexNode>
MakeHnswRustNode() {
    return nullptr;
}
}  // namespace knowhere

int
main() {
    const std::vector<float> base = {
        1.0f, 0.0f, 0.0f, 0.0f,
        0.0f, 1.0f, 0.0f, 0.0f,
        0.0f, 0.0f, 1.0f, 0.0f,
        0.0f, 0.0f, 0.0f, 1.0f,
    };
    const std::vector<float> queries = {
        1.0f, 0.0f, 0.0f, 0.0f,
        0.0f, 1.0f, 0.0f, 0.0f,
    };

    knowhere::Config build_cfg;
    build_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::COSINE;
    build_cfg[knowhere::meta::DIM] = 4;
    build_cfg[knowhere::indexparam::NLIST] = 64;

    auto base_ds = knowhere::GenDataSet(4, 4, base.data());
    auto query_ds = knowhere::GenDataSet(2, 4, queries.data());

    auto created = knowhere::IndexFactory::Instance().Create<float>(
        knowhere::IndexEnum::INDEX_FAISS_IVFFLAT,
        knowhere::Version::GetCurrentVersion());
    if (!created.has_value()) {
        std::cerr << "failed to create ivf_flat index\n";
        return 1;
    }

    auto index = created.value();
    if (index.Build(*base_ds, build_cfg) != knowhere::Status::success) {
        std::cerr << "failed to build ivf_flat index\n";
        return 2;
    }

    knowhere::Config search_cfg;
    search_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::COSINE;
    search_cfg[knowhere::meta::TOPK] = 2;

    auto iterators = index.AnnIterator(*query_ds, search_cfg, knowhere::BitsetView{});
    if (!iterators.has_value() || iterators.value().size() != 2) {
        std::cerr << "failed to initialize iterators\n";
        return 3;
    }

    for (size_t i = 0; i < iterators.value().size(); ++i) {
        auto& iterator = iterators.value()[i];
        if (iterator == nullptr || !iterator->HasNext()) {
            std::cerr << "iterator " << i << " is empty\n";
            return 4;
        }
        auto [id, distance] = iterator->Next();
        if (id != static_cast<int64_t>(i)) {
            std::cerr << "unexpected top id for iterator " << i << ": " << id
                      << "\n";
            return 5;
        }
        if (std::fabs(distance - 1.0f) > 1e-5f) {
            std::cerr << "unexpected top distance for iterator " << i << ": "
                      << distance << "\n";
            return 6;
        }
    }

    return 0;
}
