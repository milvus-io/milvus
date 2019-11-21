#pragma once

//#ifndef LIGHTSTEP_TEXT_MAP_CARRIER
//#define LIGHTSTEP_TEXT_MAP_CARRIER

#include <opentracing/propagation.h>
#include <string>
#include <unordered_map>

using opentracing::expected;
using opentracing::string_view;
using opentracing::TextMapReader;
using opentracing::TextMapWriter;

class TextMapCarrier : public TextMapReader, public TextMapWriter {
 public:
    TextMapCarrier(std::unordered_map<std::string, std::string>& text_map) : text_map_(text_map) {
    }

    expected<void>
    Set(string_view key, string_view value) const override {
        //    text_map_[key] = value;
        //    return {};
        opentracing::expected<void> result;

        auto was_successful = text_map_.emplace(key, value);
        if (was_successful.second) {
            // Use a default constructed opentracing::expected<void> to indicate
            // success.
            return result;
        } else {
            // `key` clashes with existing data, so the span context can't be encoded
            // successfully; set opentracing::expected<void> to an std::error_code.
            return opentracing::make_unexpected(std::make_error_code(std::errc::not_supported));
        }
    }

    using F = std::function<opentracing::expected<void>(opentracing::string_view, opentracing::string_view)>;

    opentracing::expected<void>
    ForeachKey(F f) const override {
        // Iterate through all key-value pairs, the tracer will use the relevant keys
        // to extract a span context.
        for (auto& key_value : text_map_) {
            auto was_successful = f(key_value.first, key_value.second);
            if (!was_successful) {
                // If the callback returns and unexpected value, bail out of the loop.
                return was_successful;
            }
        }

        // Indicate successful iteration.
        return {};
    }

    // Optional, define TextMapReader::LookupKey to allow for faster extraction.
    opentracing::expected<opentracing::string_view>
    LookupKey(opentracing::string_view key) const override {
        auto iter = text_map_.find(key);
        if (iter != text_map_.end()) {
            return opentracing::make_unexpected(opentracing::key_not_found_error);
        }
        return opentracing::string_view{iter->second};
    }

 private:
    std::unordered_map<std::string, std::string>& text_map_;
};

//#endif  // LIGHTSTEP_TEXT_MAP_CARRIER
