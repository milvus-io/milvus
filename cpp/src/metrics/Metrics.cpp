/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/


#include "Metrics.h"
#include "PrometheusMetrics.h"

namespace zilliz {
namespace vecwise {
namespace server {

MetricsBase &
Metrics::CreateMetricsCollector(MetricCollectorType collector_type) {
    switch (collector_type) {
        case MetricCollectorType::PROMETHEUS:
            static PrometheusMetrics instance = PrometheusMetrics::GetInstance();
            return instance;
        default:return MetricsBase::GetInstance();
    }
}

MetricsBase &
Metrics::GetInstance() {
    ConfigNode &config = ServerConfig::GetInstance().GetConfig(CONFIG_METRIC);
    std::string collector_typr_str = config.GetValue(CONFIG_METRIC_COLLECTOR);

    if (collector_typr_str == "prometheus") {
        return CreateMetricsCollector(MetricCollectorType::PROMETHEUS);
    } else if (collector_typr_str == "zabbix") {
        return CreateMetricsCollector(MetricCollectorType::ZABBIX);
    } else {
        return CreateMetricsCollector(MetricCollectorType::INVALID);
    }
}

}
}
}
