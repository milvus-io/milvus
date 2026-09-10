// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

// A minimal C++ layer over libbson's C API, covering exactly the BSON subset
// Milvus uses for JSON (de)serialization. It replaces the former dependency on
// the bsoncxx C++ driver, which transitively pulled in libmongoc + utf8proc.
//
// The view types here are NON-OWNING: they reference bytes in a BSON buffer the
// caller must keep alive (same semantics as the bsoncxx ::view types they
// replace). libbson's bson_iter_t reads through offsets into the external
// buffer, so copied iterators / copied bson_value_t views remain valid as long
// as that buffer outlives them.

#include <bson/bson.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "fmt/format.h"

namespace milvus::bson {

// BSON element type tags. Numeric values match the BSON spec exactly (identical
// to libbson's bson_type_t and the former bsoncxx::type), so they can be
// compared directly against the raw type byte in a BSON buffer.
enum class type : uint8_t {
    k_double = 0x01,
    k_string = 0x02,  // BSON_TYPE_UTF8
    k_document = 0x03,
    k_array = 0x04,
    k_binary = 0x05,
    k_bool = 0x08,
    k_null = 0x0A,
    k_int32 = 0x10,
    k_int64 = 0x12,
};

// Scalar accessor results mirror bsoncxx's b_*{ value } shape so call sites can
// keep using `.value`.
struct b_int32 {
    int32_t value;
};
struct b_int64 {
    int64_t value;
};
struct b_double {
    double value;
};
struct b_bool {
    bool value;
};
struct b_string {
    std::string_view value;
};

class array_view;
class document_view;
class value_view;

namespace detail {
// Shared scalar accessors over a copied bson_value_t (used by both value_view
// and element).
struct value_accessors {
    bson_value_t v_{};

    bson::type
    type() const {
        return static_cast<bson::type>(v_.value_type);
    }
    b_int32
    get_int32() const {
        return {v_.value.v_int32};
    }
    b_int64
    get_int64() const {
        return {v_.value.v_int64};
    }
    b_double
    get_double() const {
        return {v_.value.v_double};
    }
    b_bool
    get_bool() const {
        return {v_.value.v_bool};
    }
    b_string
    get_string() const {
        return {std::string_view(v_.value.v_utf8.str, v_.value.v_utf8.len)};
    }
};
}  // namespace detail

// A non-owning view over one key/value pair produced during iteration. Mirrors
// the subset of bsoncxx::document::element / array element Milvus relies on.
// get_value() is defined out-of-line below, once value_view is complete.
class element : public detail::value_accessors {
 public:
    element() = default;
    element(std::string_view key, const bson_value_t& v) : key_(key) {
        v_ = v;
    }

    // bsoncxx key() returns a string_view-like object exposing data()/length().
    std::string_view
    key() const {
        return key_;
    }
    value_view
    get_value() const;

 private:
    std::string_view key_;
};

// A non-owning view over a BSON document or array buffer (length-prefixed BSON
// bytes). array_view and document_view are distinct types (so overloads /
// template specializations can tell them apart) but iterate identically — a
// BSON array is just a document with numeric keys.
class view_base {
 public:
    view_base() = default;
    view_base(const uint8_t* data, size_t len)
        : data_(data), len_(static_cast<uint32_t>(len)) {
    }

    const uint8_t*
    data() const {
        return data_;
    }
    uint32_t
    length() const {
        return len_;
    }

    class iterator {
     public:
        using iterator_category = std::input_iterator_tag;
        using value_type = element;
        using difference_type = std::ptrdiff_t;
        using pointer = const element*;
        using reference = const element&;

        iterator() = default;  // end sentinel

        iterator(const uint8_t* data, uint32_t len) {
            if (data == nullptr || len == 0 ||
                !bson_init_static(&bson_, data, len) ||
                !bson_iter_init(&iter_, &bson_)) {
                valid_ = false;
                return;
            }
            advance();
        }

        const element&
        operator*() const {
            return cur_;
        }
        const element*
        operator->() const {
            return &cur_;
        }
        iterator&
        operator++() {
            advance();
            return *this;
        }
        bool
        operator==(const iterator& o) const {
            if (valid_ != o.valid_) {
                return false;
            }
            if (!valid_) {
                return true;
            }
            return iter_.off == o.iter_.off;
        }
        bool
        operator!=(const iterator& o) const {
            return !(*this == o);
        }

     private:
        void
        advance() {
            if (bson_iter_next(&iter_)) {
                cur_ = element(std::string_view(bson_iter_key(&iter_)),
                               *bson_iter_value(&iter_));
                valid_ = true;
            } else {
                valid_ = false;
            }
        }

        bson_t bson_{};
        bson_iter_t iter_{};
        element cur_{};
        bool valid_{false};
    };

    iterator
    begin() const {
        return iterator(data_, len_);
    }
    iterator
    end() const {
        return iterator();
    }

 protected:
    const uint8_t* data_{nullptr};
    uint32_t len_{0};
};

class document_view : public view_base {
 public:
    using view_base::view_base;
};

class array_view : public view_base {
 public:
    using view_base::view_base;
};

// A non-owning view over a single BSON value. Defined after the view types so
// its document_holder / array_holder can hold complete document_view /
// array_view members.
class value_view : public detail::value_accessors {
 public:
    value_view() {
        v_.value_type = BSON_TYPE_EOD;
    }
    explicit value_view(const bson_value_t& v) {
        v_ = v;
    }

    struct document_holder {
        document_view value;
        document_view
        view() const {
            return value;
        }
    };
    struct array_holder {
        array_view value;
    };

    document_holder
    get_document() const {
        return {document_view(v_.value.v_doc.data, v_.value.v_doc.data_len)};
    }
    array_holder
    get_array() const {
        return {array_view(v_.value.v_doc.data, v_.value.v_doc.data_len)};
    }
};

// element::get_value() needs value_view complete.
inline value_view
element::get_value() const {
    return value_view(v_);
}

// Serialize a BSON document to relaxed extended JSON (debug / logging use).
inline std::string
to_json(const document_view& doc) {
    bson_t b;
    if (doc.data() == nullptr ||
        !bson_init_static(&b, doc.data(), doc.length())) {
        return "{}";
    }
    size_t len = 0;
    char* s = bson_as_relaxed_extended_json(&b, &len);
    if (s == nullptr) {
        return "{}";
    }
    std::string out(s, len);
    bson_free(s);
    return out;
}

}  // namespace milvus::bson

template <>
struct fmt::formatter<milvus::bson::type> : fmt::formatter<std::string> {
    auto
    format(milvus::bson::type t, fmt::format_context& ctx) const {
        std::string name;
        switch (t) {
            case milvus::bson::type::k_int32:
                name = "int32";
                break;
            case milvus::bson::type::k_int64:
                name = "int64";
                break;
            case milvus::bson::type::k_double:
                name = "double";
                break;
            case milvus::bson::type::k_string:
                name = "string";
                break;
            case milvus::bson::type::k_bool:
                name = "bool";
                break;
            case milvus::bson::type::k_null:
                name = "null";
                break;
            case milvus::bson::type::k_document:
                name = "document";
                break;
            case milvus::bson::type::k_array:
                name = "array";
                break;
            default:
                name = "Unknown";
        }
        return fmt::formatter<std::string>::format(name, ctx);
    }
};
