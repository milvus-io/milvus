// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.

#pragma once

#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/Types.h"
#include "common/ValidityView.h"

namespace milvus::index {

// Index offsets, not candidate positions. The caller selects the rows to read;
// no predicate, candidate mask, or search-specific batch limit belongs here.
using ScalarIndexOffsets = std::span<const int64_t>;

// One value/validity lane per input offset, in the same order (including
// duplicates). Strings borrow the pinned index or this batch's owned fallback
// storage. Native index views MUST NOT outlive the caller's index pin.
// Fixed-width values are gathered by value for consumption by existing kernels.
template <typename T>
class ScalarIndexLookupViews {
 public:
    using ValueType =
        std::conditional_t<std::is_same_v<T, std::string>, std::string_view, T>;

    explicit ScalarIndexLookupViews(size_t count)
        : values_(count), valid_(count, false) {
    }

    ScalarIndexLookupViews(const ScalarIndexLookupViews&) = delete;
    ScalarIndexLookupViews&
    operator=(const ScalarIndexLookupViews&) = delete;
    ScalarIndexLookupViews(ScalarIndexLookupViews&&) = default;
    ScalarIndexLookupViews&
    operator=(ScalarIndexLookupViews&&) = default;

    size_t
    size() const {
        return values_.size();
    }

    std::span<const ValueType>
    values() const {
        return {values_.data(), values_.size()};
    }

    ValidityView
    validity() const {
        return ValidityView::FromExpanded(valid_.data());
    }

    bool
    is_valid(size_t lane) const {
        return valid_[lane];
    }

    // Native implementations must supply a view backed by the pinned index.
    // Unset lanes remain NULL; empty strings are valid, distinct from NULL.
    void
    SetView(size_t lane, ValueType value) {
        values_[lane] = value;
        valid_[lane] = true;
    }

    // Compatibility path: do not borrow a temporary optional<string>. Allocate
    // the final owner array once; moving the batch never moves the strings
    // (including SSO strings) or invalidates previously constructed views.
    void
    SetOwned(size_t lane, T value) {
        if constexpr (std::is_same_v<T, std::string>) {
            if (!owned_values_) {
                owned_values_ = std::make_unique<std::vector<T>>(size());
            }
            (*owned_values_)[lane] = std::move(value);
            SetView(lane, (*owned_values_)[lane]);
        } else {
            SetView(lane, std::move(value));
        }
    }

 private:
    FixedVector<ValueType> values_;
    FixedVector<bool> valid_;
    std::unique_ptr<std::vector<T>> owned_values_;
};

}  // namespace milvus::index
