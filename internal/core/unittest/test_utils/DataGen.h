#pragma once
#include "segcore/SegmentDefs.h"
#include <random>
#include <memory>
#include <cstring>
namespace milvus::segcore {

struct GeneratedData {
    std::vector<char> rows_;
    std::vector<std::vector<char>> cols_;
    void
    generate_rows(int N, SchemaPtr schema);
};

void
GeneratedData::generate_rows(int N, SchemaPtr schema) {
    std::vector<int> offset_infos(schema->size() + 1, 0);
    auto sizeof_infos = schema->get_sizeof_infos();
    std::partial_sum(sizeof_infos.begin(), sizeof_infos.end(), offset_infos.begin() + 1);
    auto len_per_row = offset_infos.back();
    assert(len_per_row == schema->get_total_sizeof());

    std::vector<char> result(len_per_row * N);
    for (int index = 0; index < N; ++index) {
        for (int fid = 0; fid < schema->size(); ++fid) {
            auto len = sizeof_infos[fid];
            auto offset = offset_infos[fid];
            auto src = cols_[fid].data() + index * len;
            auto dst = result.data() + offset + index * len_per_row;
            memcpy(dst, src, len);
        }
    }
    rows_ = std::move(result);
}

inline GeneratedData
DataGen(SchemaPtr schema, int64_t N) {
    using std::vector;
    std::vector<vector<char>> cols;
    std::default_random_engine er(42);
    std::normal_distribution<> distr(0, 1);
    int offset = 0;

    auto insert_cols = [&cols](auto& data) {
        using T = std::remove_reference_t<decltype(data)>;
        auto len = sizeof(typename T::value_type) * data.size();
        auto ptr = vector<char>(len);
        memcpy(ptr.data(), data.data(), len);
        cols.emplace_back(std::move(ptr));
    };

    for (auto& field : schema->get_fields()) {
        switch (field.get_data_type()) {
            case engine::DataType::VECTOR_FLOAT: {
                auto dim = field.get_dim();
                vector<float> data(dim * N);
                for (auto& x : data) {
                    x = distr(er) + offset;
                }
                insert_cols(data);
                break;
            }
            case engine::DataType::INT32: {
                vector<int> data(N);
                int count = 0;
                for (auto& x : data) {
                    x = count + offset * N;
                    ++count;
                }
                insert_cols(data);
                break;
            }
            default: {
                throw std::runtime_error("unimplemented");
            }
        }
        ++offset;
    }
    GeneratedData res;
    res.cols_ = std::move(cols);
    res.generate_rows(N, schema);
    return std::move(res);
}

}  // namespace milvus::segcore