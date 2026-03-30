#include <cmath>
#include <cstdint>
#include <iostream>
#include <vector>

#include "knowhere/bitsetview.h"
#include "knowhere/comp/brute_force.h"
#include "knowhere/comp/index_param.h"
#include "knowhere/dataset.h"

namespace {

void
SetBit(std::vector<uint8_t>& bits, int64_t bit_index, bool value) {
    const auto byte_index = static_cast<size_t>(bit_index >> 3);
    const auto bit_mask = static_cast<uint8_t>(1u << (bit_index & 7));
    if (value) {
        bits[byte_index] |= bit_mask;
    } else {
        bits[byte_index] &= static_cast<uint8_t>(~bit_mask);
    }
}

}  // namespace

int
main() {
    const std::vector<float> base = {
        10.0f, 0.0f,
        20.0f, 0.0f,
        30.0f, 0.0f,
        40.0f, 0.0f,
    };
    const std::vector<float> query = {
        30.0f, 0.0f,
    };

    auto base_ds = knowhere::GenDataSet(4, 2, base.data());
    base_ds->SetTensorBeginId(8);
    auto query_ds = knowhere::GenDataSet(1, 2, query.data());

    knowhere::Json search_cfg;
    search_cfg[knowhere::meta::METRIC_TYPE] = knowhere::metric::L2;
    search_cfg[knowhere::meta::TOPK] = int64_t{1};

    std::vector<uint8_t> masked_bits((20 + 7) / 8, 0xFF);
    SetBit(masked_bits, 10, false);
    knowhere::BitsetView bitset(masked_bits.data(), 20);

    auto iterators =
        knowhere::BruteForce::AnnIterator<float>(base_ds, query_ds, search_cfg, bitset);
    if (!iterators.has_value() || iterators.value().size() != 1) {
        std::cerr << "failed to create brute-force iterator\n";
        return 1;
    }

    auto& iterator = iterators.value()[0];
    if (iterator == nullptr || !iterator->HasNext()) {
        std::cerr << "iterator is empty\n";
        return 2;
    }

    auto [id, distance] = iterator->Next();
    if (id != 10) {
        std::cerr << "unexpected id: " << id << "\n";
        return 3;
    }
    if (std::fabs(distance) > 1e-5f) {
        std::cerr << "unexpected distance: " << distance << "\n";
        return 4;
    }

    return 0;
}
