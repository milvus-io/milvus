//
//#include "knowhere/index/vector_index/definitions.h"
//#include "knowhere/common/config.h"
//#include "knowhere/index/preprocessor/normalize.h"
//
//
//namespace zilliz {
//namespace knowhere {
//
//DatasetPtr
//NormalizePreprocessor::Preprocess(const DatasetPtr &dataset) {
//    // TODO: wrap dataset->tensor
//    auto tensor = dataset->tensor()[0];
//    auto p_data = (float *)tensor->raw_mutable_data();
//    auto dimension = tensor->shape()[1];
//    auto rows = tensor->shape()[0];
//
//#pragma omp parallel for
//    for (auto i = 0; i < rows; ++i) {
//        Normalize(&(p_data[i * dimension]), dimension);
//    }
//}
//
//void
//NormalizePreprocessor::Normalize(float *arr, int64_t dimension) {
//    double vector_length = 0;
//    for (auto j = 0; j < dimension; j++) {
//        double val = arr[j];
//        vector_length += val * val;
//    }
//    vector_length = std::sqrt(vector_length);
//    if (vector_length < 1e-6) {
//        auto val = (float) (1.0 / std::sqrt((double) dimension));
//        for (int j = 0; j < dimension; j++) arr[j] = val;
//    } else {
//        for (int j = 0; j < dimension; j++) arr[j] = (float) (arr[j] / vector_length);
//    }
//}
//
//} // namespace knowhere
//} // namespace zilliz

