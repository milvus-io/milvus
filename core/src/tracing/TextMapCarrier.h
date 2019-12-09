#pragma once

#include <opentracing/propagation.h>

#include <string>
#include <unordered_map>

namespace milvus {
namespace tracing {

class TextMapCarrier : public opentracing::TextMapReader, public opentracing::TextMapWriter {
 public:
    explicit TextMapCarrier(std::unordered_map<std::string, std::string>& text_map);

    opentracing::expected<void>
    Set(opentracing::string_view key, opentracing::string_view value) const override;

    using F = std::function<opentracing::expected<void>(opentracing::string_view, opentracing::string_view)>;

    opentracing::expected<void>
    ForeachKey(F f) const override;

    // Optional, define TextMapReader::LookupKey to allow for faster extraction.
    opentracing::expected<opentracing::string_view>
    LookupKey(opentracing::string_view key) const override;

 private:
    std::unordered_map<std::string, std::string>& text_map_;
};

}  // namespace tracing
}  // namespace milvus