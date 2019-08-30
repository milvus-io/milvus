////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "utils.h"


void DataGen::Init_with_default() {
    Generate(dim, nb, nq);
}

void DataGen::Generate(const int &dim, const int &nb, const int &nq) {
    this->nb = nb;
    this->nq = nq;
    this->dim = dim;

    GenAll(dim, nb, xb, ids, nq, xq);
    assert(xb.size() == dim * nb);
    assert(xq.size() == dim * nq);

    base_dataset = generate_dataset(nb, dim, xb.data(), ids.data());
    query_dataset = generate_query_dataset(nq, dim, xq.data());

}
zilliz::knowhere::DatasetPtr DataGen::GenQuery(const int &nq) {
    xq.resize(nq * dim);
    for (size_t i = 0; i < nq * dim; ++i) {
        xq[i] = xb[i];
    }
    return generate_query_dataset(nq, dim, xq.data());
}

void GenAll(const int64_t dim,
            const int64_t &nb,
            std::vector<float> &xb,
            std::vector<int64_t> &ids,
            const int64_t &nq,
            std::vector<float> &xq) {
    xb.resize(nb * dim);
    xq.resize(nq * dim);
    ids.resize(nb);
    GenAll(dim, nb, xb.data(), ids.data(), nq, xq.data());
}

void GenAll(const int64_t &dim,
            const int64_t &nb,
            float *xb,
            int64_t *ids,
            const int64_t &nq,
            float *xq) {
    GenBase(dim, nb, xb, ids);
    for (size_t i = 0; i < nq * dim; ++i) {
        xq[i] = xb[i];
    }
}

void GenBase(const int64_t &dim,
             const int64_t &nb,
             float *xb,
             int64_t *ids) {
    for (auto i = 0; i < nb; ++i) {
        for (auto j = 0; j < dim; ++j) {
            //p_data[i * d + j] = float(base + i);
            xb[i * dim + j] = drand48();
        }
        xb[dim * i] += i / 1000.;
        ids[i] = i;
    }
}

FileIOReader::FileIOReader(const std::string &fname) {
    name = fname;
    fs = std::fstream(name, std::ios::in | std::ios::binary);
}

FileIOReader::~FileIOReader() {
    fs.close();
}

size_t FileIOReader::operator()(void *ptr, size_t size) {
    fs.read(reinterpret_cast<char *>(ptr), size);
    return size;
}

FileIOWriter::FileIOWriter(const std::string &fname) {
    name = fname;
    fs = std::fstream(name, std::ios::out | std::ios::binary);
}

FileIOWriter::~FileIOWriter() {
    fs.close();
}

size_t FileIOWriter::operator()(void *ptr, size_t size) {
    fs.write(reinterpret_cast<char *>(ptr), size);
    return size;
}

using namespace zilliz::knowhere;

DatasetPtr
generate_dataset(int64_t nb, int64_t dim, float *xb, long *ids) {
    std::vector<int64_t> shape{nb, dim};
    auto tensor = ConstructFloatTensor((uint8_t *) xb, nb * dim * sizeof(float), shape);
    std::vector<TensorPtr> tensors{tensor};
    std::vector<FieldPtr> tensor_fields{ConstructFloatField("data")};
    auto tensor_schema = std::make_shared<Schema>(tensor_fields);

    auto id_array = ConstructInt64Array((uint8_t *) ids, nb * sizeof(int64_t));
    std::vector<ArrayPtr> arrays{id_array};
    std::vector<FieldPtr> array_fields{ConstructInt64Field("id")};
    auto array_schema = std::make_shared<Schema>(tensor_fields);

    auto dataset = std::make_shared<Dataset>(std::move(arrays), array_schema,
                                             std::move(tensors), tensor_schema);
    return dataset;
}

DatasetPtr
generate_query_dataset(int64_t nb, int64_t dim, float *xb) {
    std::vector<int64_t> shape{nb, dim};
    auto tensor = ConstructFloatTensor((uint8_t *) xb, nb * dim * sizeof(float), shape);
    std::vector<TensorPtr> tensors{tensor};
    std::vector<FieldPtr> tensor_fields{ConstructFloatField("data")};
    auto tensor_schema = std::make_shared<Schema>(tensor_fields);

    auto dataset = std::make_shared<Dataset>(std::move(tensors), tensor_schema);
    return dataset;
}
