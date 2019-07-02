////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////

#include "data_transfer.h"


namespace zilliz {
namespace vecwise {
namespace engine {

using namespace zilliz::knowhere;

DatasetPtr
GenDatasetWithIds(const int64_t &nb, const int64_t &dim, const float *xb, const long *ids) {
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
GenDataset(const int64_t &nb, const int64_t &dim, const float *xb) {
    std::vector<int64_t> shape{nb, dim};
    auto tensor = ConstructFloatTensor((uint8_t *) xb, nb * dim * sizeof(float), shape);
    std::vector<TensorPtr> tensors{tensor};
    std::vector<FieldPtr> tensor_fields{ConstructFloatField("data")};
    auto tensor_schema = std::make_shared<Schema>(tensor_fields);

    auto dataset = std::make_shared<Dataset>(std::move(tensors), tensor_schema);
    return dataset;
}

}
}
}
