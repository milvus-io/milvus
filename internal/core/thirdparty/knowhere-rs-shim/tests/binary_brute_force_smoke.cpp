#include <cstdint>
#include <iostream>
#include <vector>

#include "knowhere/bitsetview.h"
#include "knowhere/comp/brute_force.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"

int
main() {
    const std::vector<uint8_t> base = {
        0xFF, 0x00,
        0xF0, 0x0F,
        0x00, 0xFF,
        0xAA, 0x55,
    };
    const std::vector<uint8_t> query = {0xFF, 0x00};
    std::vector<int64_t> ids(2, -1);
    std::vector<float> distances(2, 0.0f);

    auto base_ds = knowhere::GenDataSet(4, 16, base.data());
    auto query_ds = knowhere::GenDataSet(1, 16, query.data());

    knowhere::Config cfg;
    cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::JACCARD;
    cfg[knowhere::meta::TOPK] = 2;

    auto status = knowhere::BruteForce::SearchWithBuf<knowhere::bin1>(
        base_ds,
        query_ds,
        ids.data(),
        distances.data(),
        cfg,
        knowhere::BitsetView{});
    if (status != knowhere::Status::success) {
        std::cerr << "binary brute-force search returned non-success status\n";
        return 1;
    }
    if (ids[0] != 0 || ids[1] < 0) {
        std::cerr << "unexpected binary brute-force ids: " << ids[0] << ", "
                  << ids[1] << "\n";
        return 2;
    }
    if (distances[0] != 0.0f) {
        std::cerr << "unexpected binary top distance: " << distances[0] << "\n";
        return 3;
    }

    return 0;
}
