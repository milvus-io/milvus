
#include "knowhere/index/vector_index/definitions.h"
#include "knowhere/adapter/sptag.h"
#include "knowhere/adapter/structure.h"


namespace zilliz {
namespace knowhere {


std::shared_ptr<SPTAG::MetadataSet>
ConvertToMetadataSet(const DatasetPtr &dataset) {
    auto array = dataset->array()[0];
    auto elems = array->length();

    auto p_data = array->data()->GetValues<int64_t>(1, 0);
    auto p_offset = (int64_t *) malloc(sizeof(int64_t) * elems);
    for (auto i = 0; i <= elems; ++i)
        p_offset[i] = i * 8;

    std::shared_ptr<SPTAG::MetadataSet> metaset(new SPTAG::MemMetadataSet(
        SPTAG::ByteArray((std::uint8_t *) p_data, elems * sizeof(int64_t), false),
        SPTAG::ByteArray((std::uint8_t *) p_offset, elems * sizeof(int64_t), true),
        elems));


    return metaset;
}

std::shared_ptr<SPTAG::VectorSet>
ConvertToVectorSet(const DatasetPtr &dataset) {
    auto tensor = dataset->tensor()[0];

    auto p_data = tensor->raw_mutable_data();
    auto dimension = tensor->shape()[1];
    auto rows = tensor->shape()[0];
    auto num_bytes = tensor->size() * sizeof(float);

    SPTAG::ByteArray byte_array(p_data, num_bytes, false);

    auto vectorset = std::make_shared<SPTAG::BasicVectorSet>(byte_array,
                                                             SPTAG::VectorValueType::Float,
                                                             dimension,
                                                             rows);
    return vectorset;
}

std::vector<SPTAG::QueryResult>
ConvertToQueryResult(const DatasetPtr &dataset, const Config &config) {
    auto tensor = dataset->tensor()[0];

    auto p_data = (float *) tensor->raw_mutable_data();
    auto dimension = tensor->shape()[1];
    auto rows = tensor->shape()[0];

    auto k = config[META_K].as<int64_t>();
    std::vector<SPTAG::QueryResult> query_results(rows, SPTAG::QueryResult(nullptr, k, true));
    for (auto i = 0; i < rows; ++i) {
        query_results[i].SetTarget(&p_data[i * dimension]);
    }

    return query_results;
}

DatasetPtr
ConvertToDataset(std::vector<SPTAG::QueryResult> query_results) {
    auto k = query_results[0].GetResultNum();
    auto elems = query_results.size() * k;

    auto p_id = (int64_t *) malloc(sizeof(int64_t) * elems);
    auto p_dist = (float *) malloc(sizeof(float) * elems);
    // TODO: throw if malloc failed.

#pragma omp parallel for
    for (auto i = 0; i < query_results.size(); ++i) {
        auto results = query_results[i].GetResults();
        auto num_result = query_results[i].GetResultNum();
        for (auto j = 0; j < num_result; ++j) {
//            p_id[i * k + j] = results[j].VID;
            p_id[i * k + j] = *(int64_t *) query_results[i].GetMetadata(j).Data();
            p_dist[i * k + j] = results[j].Dist;
        }
    }

    auto id_buf = MakeMutableBufferSmart((uint8_t *) p_id, sizeof(int64_t) * elems);
    auto dist_buf = MakeMutableBufferSmart((uint8_t *) p_dist, sizeof(float) * elems);

    // TODO: magic
    std::vector<BufferPtr> id_bufs{nullptr, id_buf};
    std::vector<BufferPtr> dist_bufs{nullptr, dist_buf};

    auto int64_type = std::make_shared<arrow::Int64Type>();
    auto float_type = std::make_shared<arrow::FloatType>();

    auto id_array_data = arrow::ArrayData::Make(int64_type, elems, id_bufs);
    auto dist_array_data = arrow::ArrayData::Make(float_type, elems, dist_bufs);
//    auto id_array_data = std::make_shared<ArrayData>(int64_type, sizeof(int64_t) * elems, id_bufs);
//    auto dist_array_data = std::make_shared<ArrayData>(float_type, sizeof(float) * elems, dist_bufs);

//    auto ids = ConstructInt64Array((uint8_t*)p_id, sizeof(int64_t) * elems);
//    auto dists = ConstructFloatArray((uint8_t*)p_dist, sizeof(float) * elems);

    auto ids = std::make_shared<NumericArray<arrow::Int64Type>>(id_array_data);
    auto dists = std::make_shared<NumericArray<arrow::FloatType>>(dist_array_data);
    std::vector<ArrayPtr> array{ids, dists};

    auto field_id = std::make_shared<Field>("id", std::make_shared<arrow::Int64Type>());
    auto field_dist = std::make_shared<Field>("dist", std::make_shared<arrow::FloatType>());
    std::vector<FieldPtr> fields{field_id, field_dist};
    auto schema = std::make_shared<Schema>(fields);

    return std::make_shared<Dataset>(array, schema);
}

} // namespace knowhere
} // namespace zilliz
