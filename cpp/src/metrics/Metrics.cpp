/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#include "Metrics.h"
#include "PrometheusMetrics.h"


namespace zilliz {
namespace milvus {
namespace server {

MetricsBase &
Metrics::GetInstance() {
    static MetricsBase &instance = CreateMetricsCollector();
    return instance;
}

MetricsBase &
Metrics::CreateMetricsCollector() {
    ConfigNode &config = ServerConfig::GetInstance().GetConfig(CONFIG_METRIC);
    std::string collector_type_str = config.GetValue(CONFIG_METRIC_COLLECTOR);

    if (collector_type_str == "prometheus") {
        return PrometheusMetrics::GetInstance();
    } else {
        return MetricsBase::GetInstance();
    }
}

}
}
}
