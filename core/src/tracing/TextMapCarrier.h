#pragma once

//#ifndef LIGHTSTEP_TEXT_MAP_CARRIER
//#define LIGHTSTEP_TEXT_MAP_CARRIER

#include <opentracing/propagation.h>
#include <string>
#include <unordered_map>

using opentracing::TextMapReader;
using opentracing::TextMapWriter;
using opentracing::expected;
using opentracing::string_view;

class TextMapCarrier : public TextMapReader, public TextMapWriter {
 public:
  TextMapCarrier(std::unordered_map<std::string, std::string>& text_map)
      : text_map_(text_map) {}

  expected<void> Set(string_view key, string_view value) const override {
    text_map_[key] = value;
    return {};
  }

  expected<void> ForeachKey(
      std::function<expected<void>(string_view key, string_view value)> f)
      const override {
    for (const auto& key_value : text_map_) {
      auto result = f(key_value.first, key_value.second);
      if (!result) return result;
    }
    return {};
  }

 private:
  std::unordered_map<std::string, std::string>& text_map_;
};

//#endif  // LIGHTSTEP_TEXT_MAP_CARRIER
