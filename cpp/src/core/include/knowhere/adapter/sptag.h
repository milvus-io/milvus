#pragma once

#include <memory>
#include <knowhere/common/dataset.h>
#include <SPTAG/AnnService/inc/Core/VectorIndex.h>


namespace zilliz {
namespace knowhere {

std::shared_ptr<SPTAG::VectorSet>
ConvertToVectorSet(const DatasetPtr &dataset);

std::shared_ptr<SPTAG::MetadataSet>
ConvertToMetadataSet(const DatasetPtr &dataset);

std::vector<SPTAG::QueryResult>
ConvertToQueryResult(const DatasetPtr &dataset, const Config &config);

DatasetPtr
ConvertToDataset(std::vector<SPTAG::QueryResult> query_results);

} // namespace knowhere
} // namespace zilliz
