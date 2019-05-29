/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "utils/Error.h"
#include <memory>
#include <vector>


#pragma once

#include "MetricBase.h"
//#include "PrometheusMetrics.h"

namespace zilliz {
namespace vecwise {
namespace server {

#define METRICS_NOW_TIME std::chrono::system_clock::now()
//#define server::Metrics::GetInstance() server::Metrics::GetInstance()
#define METRICS_MICROSECONDS(a, b) (std::chrono::duration_cast<std::chrono::microseconds> (b-a)).count();

enum class MetricCollectorType {
    INVALID,
    PROMETHEUS,
    ZABBIX
};

class Metrics {
 public:
    static MetricsBase &
    CreateMetricsCollector(MetricCollectorType collector_type);

    static MetricsBase &
    GetInstance();
};



}
}
}



