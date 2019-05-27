#pragma once

#include <map>
#include <string>

namespace prometheus {

template <typename T>
class Family;
class Counter;
class Registry;

namespace detail {

class CounterBuilder {
 public:
  CounterBuilder& Labels(const std::map<std::string, std::string>& labels);
  CounterBuilder& Name(const std::string&);
  CounterBuilder& Help(const std::string&);
  Family<Counter>& Register(Registry&);

 private:
  std::map<std::string, std::string> labels_;
  std::string name_;
  std::string help_;
};

}  // namespace detail
}  // namespace prometheus
