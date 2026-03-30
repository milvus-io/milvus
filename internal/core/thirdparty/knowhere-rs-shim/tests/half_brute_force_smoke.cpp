#include <cmath>
#include <cstdint>
#include <iostream>
#include <vector>

#include "knowhere/bitsetview.h"
#include "knowhere/comp/brute_force.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"

template <typename HalfT>
int
RunHalfBruteForceSmoke(uint16_t one_bits) {
    const std::vector<HalfT> base = {
        HalfT{one_bits}, HalfT{0},        HalfT{0},        HalfT{0},
        HalfT{0},        HalfT{one_bits}, HalfT{0},        HalfT{0},
        HalfT{0},        HalfT{0},        HalfT{one_bits}, HalfT{0},
        HalfT{0},        HalfT{0},        HalfT{0},        HalfT{one_bits},
    };
    const std::vector<HalfT> query = {
        HalfT{one_bits},
        HalfT{0},
        HalfT{0},
        HalfT{0},
    };
    std::vector<int64_t> ids(2, -1);
    std::vector<float> distances(2, 0.0f);

    auto base_ds = knowhere::GenDataSet(4, 4, base.data());
    auto query_ds = knowhere::GenDataSet(1, 4, query.data());

    knowhere::Config cfg;
    cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::L2;
    cfg[knowhere::meta::TOPK] = 2;

    auto status = knowhere::BruteForce::SearchWithBuf<HalfT>(
        base_ds,
        query_ds,
        ids.data(),
        distances.data(),
        cfg,
        knowhere::BitsetView{});
    if (status != knowhere::Status::success) {
        std::cerr << "half brute-force search returned non-success status\n";
        return 1;
    }
    if (ids[0] != 0 || ids[1] < 0) {
        std::cerr << "unexpected half brute-force ids: " << ids[0] << ", "
                  << ids[1] << "\n";
        return 2;
    }
    if (std::fabs(distances[0]) > 1e-5f) {
        std::cerr << "unexpected half top distance: " << distances[0] << "\n";
        return 3;
    }

    return 0;
}

int
main() {
    const auto fp16_status = RunHalfBruteForceSmoke<knowhere::fp16>(0x3C00u);
    if (fp16_status != 0) {
        std::cerr << "fp16 path failed\n";
        return fp16_status;
    }

    const auto bf16_status = RunHalfBruteForceSmoke<knowhere::bf16>(0x3F80u);
    if (bf16_status != 0) {
        std::cerr << "bf16 path failed\n";
        return bf16_status;
    }

    return 0;
}
