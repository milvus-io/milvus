#pragma once
#include "common/Schema.h"
#include <random>
#include <memory>
#include <cstring>
#include "segcore/SegmentBase.h"
namespace milvus::segcore {

struct GeneratedData {
    std::vector<char> rows_;
    std::vector<std::vector<char>> cols_;
    std::vector<idx_t> row_ids_;
    std::vector<Timestamp> timestamps_;
    RowBasedRawData raw_;
    template <typename T>
    auto
    get_col(int index) {
        auto& target = cols_.at(index);
        std::vector<T> ret(target.size() / sizeof(T));
        memcpy(ret.data(), target.data(), target.size());
        return ret;
    }

 private:
    GeneratedData() = default;
    friend GeneratedData
    DataGen(SchemaPtr schema, int64_t N, uint64_t seed);
    void
    generate_rows(int N, SchemaPtr schema);
};
inline void
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
DataGen(SchemaPtr schema, int64_t N, uint64_t seed = 42) {
    using std::vector;
    std::vector<vector<char>> cols;
    std::default_random_engine er(seed);
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
                for (auto& x : data) {
                    x = er() % (2 * N);
                }
                insert_cols(data);
                break;
            }
            case engine::DataType::FLOAT: {
                vector<float> data(N);
                for (auto& x : data) {
                    x = distr(er);
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
    for (int i = 0; i < N; ++i) {
        res.row_ids_.push_back(i);
        res.timestamps_.push_back(i);
    }
    res.raw_.raw_data = res.rows_.data();
    res.raw_.sizeof_per_row = schema->get_total_sizeof();
    res.raw_.count = N;
    return std::move(res);
}

inline auto
CreatePlaceholderGroup(int64_t num_queries, int dim, int64_t seed = 42) {
    namespace ser = milvus::proto::service;
    ser::PlaceholderGroup raw_group;
    auto value = raw_group.add_placeholders();
    value->set_tag("$0");
    value->set_type(ser::PlaceholderType::VECTOR_FLOAT);
    std::normal_distribution<double> dis(0, 1);
    std::default_random_engine e(seed);
    for (int i = 0; i < num_queries; ++i) {
        std::vector<float> vec;
        for (int d = 0; d < dim; ++d) {
            vec.push_back(dis(e));
        }
        // std::string line((char*)vec.data(), (char*)vec.data() + vec.size() * sizeof(float));
        value->add_values(vec.data(), vec.size() * sizeof(float));
    }
    return raw_group;
}

}  // namespace milvus::segcore
