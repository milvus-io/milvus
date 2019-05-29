#include "prometheus/detail/counter_builder.h"

#include "prometheus/registry.h"

namespace prometheus {
namespace detail {

CounterBuilder& CounterBuilder::Labels(
    const std::map<std::string, std::string>& labels) {
  labels_ = labels;
  return *this;
}

CounterBuilder& CounterBuilder::Name(const std::string& name) {
  name_ = name;
  return *this;
}

CounterBuilder& CounterBuilder::Help(const std::string& help) {
  help_ = help;
  return *this;
}

Family<Counter>& CounterBuilder::Register(Registry& registry) {
  return registry.Add<Counter>(name_, help_, labels_);
}

}  // namespace detail
}  // namespace prometheus
