/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "PrometheusMetrics.h"


namespace zilliz {
namespace vecwise {
namespace server {

ServerError
PrometheusMetrics::Init() {
    ConfigNode& configNode = ServerConfig::GetInstance().GetConfig(CONFIG_METRIC);
    startup_ = configNode.GetValue(CONFIG_METRIC_IS_STARTUP) == "true" ? true:false;
    // Following should be read from config file.
    const std::string bind_address = configNode.GetChild(CONFIG_PROMETHEUS).GetValue(CONFIG_METRIC_PROMETHEUS_PORT);
    const std::string uri = std::string("/metrics");
    const std::size_t num_threads = 2;

    // Init Exposer
    exposer_ptr_ = std::make_shared<prometheus::Exposer>(bind_address, uri, num_threads);

    // Exposer Registry
    exposer_ptr_->RegisterCollectable(registry_);

    return SERVER_SUCCESS;
}

}
}
}
