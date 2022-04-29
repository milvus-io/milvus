// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "common/Types.h"
#include <knowhere/index/vector_index/helpers/IndexParameter.h>
#include "exceptions/EasyAssert.h"
#include <boost/bimap.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include "common/type_c.h"
#include "pb/schema.pb.h"
#include "CGoHelper.h"
#include "common/Consts.h"

namespace milvus {

using boost::algorithm::to_upper_copy;
namespace Metric = knowhere::Metric;
static const auto metric_bimap = [] {
    boost::bimap<std::string, MetricType> mapping;
    using pos = boost::bimap<std::string, MetricType>::value_type;
    mapping.insert(pos(std::string(Metric::L2), MetricType::METRIC_L2));
    mapping.insert(pos(std::string(Metric::IP), MetricType::METRIC_INNER_PRODUCT));
    mapping.insert(pos(std::string(Metric::JACCARD), MetricType::METRIC_Jaccard));
    mapping.insert(pos(std::string(Metric::TANIMOTO), MetricType::METRIC_Tanimoto));
    mapping.insert(pos(std::string(Metric::HAMMING), MetricType::METRIC_Hamming));
    mapping.insert(pos(std::string(Metric::SUBSTRUCTURE), MetricType::METRIC_Substructure));
    mapping.insert(pos(std::string(Metric::SUPERSTRUCTURE), MetricType::METRIC_Superstructure));
    return mapping;
}();

MetricType
GetMetricType(const std::string& type_name) {
    // Assume Metric is all upper at Knowhere
    auto real_name = to_upper_copy(type_name);
    AssertInfo(metric_bimap.left.count(real_name), "metric type not found: (" + type_name + ")");
    return metric_bimap.left.at(real_name);
}

std::string
MetricTypeToName(MetricType metric_type) {
    AssertInfo(metric_bimap.right.count(metric_type),
               "metric_type enum(" + std::to_string((int)metric_type) + ") not found");
    return metric_bimap.right.at(metric_type);
}

bool
IsPrimaryKeyDataType(DataType data_type) {
    return data_type == engine::DataType::INT64 || data_type == DataType::VARCHAR;
}

}  // namespace milvus
