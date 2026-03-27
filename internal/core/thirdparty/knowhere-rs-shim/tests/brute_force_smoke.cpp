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
        1.0f, 0.0f, 0.0f, 0.0f,
        0.0f, 1.0f, 0.0f, 0.0f,
        0.0f, 0.0f, 1.0f, 0.0f,
        0.0f, 0.0f, 0.0f, 1.0f,
    };
    const std::vector<float> query = {1.0f, 0.0f, 0.0f, 0.0f};
    std::vector<int64_t> ids(2, -1);
    std::vector<float> distances(2, 0.0f);

    auto base_ds = knowhere::GenDataSet(4, 4, base.data());
    auto query_ds = knowhere::GenDataSet(1, 4, query.data());

    knowhere::Config cfg;
    cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::COSINE;
    cfg[knowhere::meta::TOPK] = 2;

    auto status = knowhere::BruteForce::SearchWithBuf<float>(
        base_ds,
        query_ds,
        ids.data(),
        distances.data(),
        cfg,
        knowhere::BitsetView{});
    if (status != knowhere::Status::success) {
        std::cerr << "brute-force search returned non-success status\n";
        return 1;
    }
    if (ids[0] != 0 || ids[1] < 0) {
        std::cerr << "unexpected brute-force ids: " << ids[0] << ", " << ids[1]
                  << "\n";
        return 2;
    }
    if (std::fabs(distances[0] - 1.0f) > 1e-5f) {
        std::cerr << "unexpected top distance: " << distances[0] << "\n";
        return 3;
    }

    return 0;
}
