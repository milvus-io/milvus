#pragma once

#include <string>

namespace prometheus {

bool CheckMetricName(const std::string& name);
bool CheckLabelName(const std::string& name);
}  // namespace prometheus
