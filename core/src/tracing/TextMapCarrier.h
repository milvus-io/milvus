#pragma once

#include <opentracing/propagation.h>
#include <string>
#include <unordered_map>

using opentracing::expected;
using opentracing::string_view;
using opentracing::TextMapReader;
using opentracing::TextMapWriter;

class TextMapCarrier : public TextMapReader, public TextMapWriter {
 public:
    explicit TextMapCarrier(std::unordered_map<std::string, std::string>& text_map);

    expected<void>
    Set(string_view key, string_view value) const override;

    using F = std::function<opentracing::expected<void>(opentracing::string_view, opentracing::string_view)>;

    opentracing::expected<void>
    ForeachKey(F f) const override;

    // Optional, define TextMapReader::LookupKey to allow for faster extraction.
    opentracing::expected<opentracing::string_view>
    LookupKey(opentracing::string_view key) const override;

 private:
    std::unordered_map<std::string, std::string>& text_map_;
};
