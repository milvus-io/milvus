/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "MetricBase.h"


namespace zilliz {
namespace milvus {
namespace server {

#define METRICS_NOW_TIME std::chrono::system_clock::now()
#define METRICS_MICROSECONDS(a, b) (std::chrono::duration_cast<std::chrono::microseconds> (b-a)).count();

enum class MetricCollectorType {
    INVALID,
    PROMETHEUS,
    ZABBIX
};

class Metrics {
 public:
    static MetricsBase &GetInstance();

 private:
    static MetricsBase &CreateMetricsCollector();
};


}
}
}



