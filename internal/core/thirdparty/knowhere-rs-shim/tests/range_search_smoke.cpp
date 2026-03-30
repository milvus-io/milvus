#include <algorithm>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <vector>

#include "knowhere/bitsetview.h"
#include "knowhere/comp/brute_force.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"

int
main() {
    const std::vector<float> base = {
        1.0f, 0.0f,
        0.8f, 0.6f,
        0.3f, 0.9539392f,
        -1.0f, 0.0f,
    };
    const std::vector<float> query = {1.0f, 0.0f};

    auto base_ds = knowhere::GenDataSet(4, 2, base.data());
    auto query_ds = knowhere::GenDataSet(1, 2, query.data());

    knowhere::Config cfg;
    cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::COSINE;
    cfg[knowhere::meta::RADIUS] = 0.5f;
    cfg[knowhere::meta::RANGE_FILTER] = 1.1f;
    cfg[knowhere::meta::RANGE_SEARCH_K] = 10;

    auto result = knowhere::BruteForce::RangeSearch<float>(
        base_ds, query_ds, cfg, knowhere::BitsetView{});
    if (!result.has_value()) {
        std::cerr << "range search returned non-success status: "
                  << static_cast<int>(result.error()) << "\n";
        return 1;
    }

    const auto* lims = result.value()->GetLims();
    const auto* ids = result.value()->GetIds();
    const auto* distances = result.value()->GetDistance();
    if (lims == nullptr || ids == nullptr || distances == nullptr) {
        std::cerr << "range search result missing lims/ids/distances\n";
        return 2;
    }
    if (lims[0] != 0 || lims[1] != 2) {
        std::cerr << "unexpected lims: [" << lims[0] << ", " << lims[1]
                  << "]\n";
        return 3;
    }

    std::vector<int64_t> returned_ids(ids, ids + lims[1]);
    std::sort(returned_ids.begin(), returned_ids.end());
    if (returned_ids != std::vector<int64_t>({0, 1})) {
        std::cerr << "unexpected range search ids: ";
        for (auto id : returned_ids) {
            std::cerr << id << " ";
        }
        std::cerr << "\n";
        return 4;
    }

    for (size_t i = 0; i < lims[1]; ++i) {
        if (!(distances[i] > 0.5f && distances[i] < 1.1f)) {
            std::cerr << "distance outside expected range: " << distances[i]
                      << "\n";
            return 5;
        }
    }

    return 0;
}
