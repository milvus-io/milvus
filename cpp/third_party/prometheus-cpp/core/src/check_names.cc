
#include "prometheus/check_names.h"

#include <regex>

#if defined(__GLIBCXX__) && __GLIBCXX__ <= 20150623
#define STD_REGEX_IS_BROKEN
#endif
#if defined(_MSC_VER) && _MSC_VER < 1900
#define STD_REGEX_IS_BROKEN
#endif

namespace prometheus {
bool CheckMetricName(const std::string& name) {
  // see https://prometheus.io/docs/concepts/data_model/
  auto reserved_for_internal_purposes = name.compare(0, 2, "__") == 0;
  if (reserved_for_internal_purposes) return false;
#ifdef STD_REGEX_IS_BROKEN
  return !name.empty();
#else
  static const std::regex metric_name_regex("[a-zA-Z_:][a-zA-Z0-9_:]*");
  return std::regex_match(name, metric_name_regex);
#endif
}

bool CheckLabelName(const std::string& name) {
  // see https://prometheus.io/docs/concepts/data_model/
  auto reserved_for_internal_purposes = name.compare(0, 2, "__") == 0;
  if (reserved_for_internal_purposes) return false;
#ifdef STD_REGEX_IS_BROKEN
  return !name.empty();
#else
  static const std::regex label_name_regex("[a-zA-Z_][a-zA-Z0-9_]*");
  return std::regex_match(name, label_name_regex);
#endif
}
}  // namespace prometheus
