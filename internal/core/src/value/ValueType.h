// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <exception>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "value/Value.h"

namespace milvus {

using valueEnum = const std::unordered_map<std::string, int64_t>;
std::vector<std::string>
OptionValue(const valueEnum& ce);

struct ValueError : public std::exception {
    explicit ValueError(const std::string& name, const std::string& value) : name_(name), value_(value) {
    }

    virtual std::string
    message() = 0;

 protected:
    const std::string name_;
    const std::string value_;
};

struct Immutable : public ValueError {
    explicit Immutable(const std::string& name, const std::string& value) : ValueError(name, value) {
    }

    std::string
    message() override {
        return "Config " + name_ + " is immutable.";
    }
};

struct EnumValueNotFound : public ValueError {
    EnumValueNotFound(const std::string& name, const std::string& value, std::vector<std::string> option_values)
        : ValueError(name, value), option_values_(std::move(option_values)) {
    }

    std::string
    message() override {
        std::stringstream ss;
        ss << "Config " << name_ << "(" << value_ << ") must be one of following: ";
        for (size_t i = 0; i < option_values_.size() - 1; ++i) {
            ss << option_values_[i] << ", ";
        }
        return ss.str();
    }

 private:
    std::vector<std::string> option_values_;
};

struct Invalid : public ValueError {
    Invalid(const std::string& name, const std::string& value, const std::string& reason)
        : ValueError(name, value), reason_(reason) {
    }

    std::string
    message() override {
        return value_ + " is invalid for config " + name_ + ": " + reason_;
    }

 private:
    const std::string reason_;
};

template <typename T>
struct OutOfRange : public ValueError {
    OutOfRange(const std::string& name, const std::string& value, T lower_bound, T upper_bound)
        : ValueError(name, value), lower_bound_(lower_bound), upper_bound_(upper_bound) {
    }

    std::string
    message() override {
        return "Config " + name_ + "(" + value_ + ") must in range [" + std::to_string(lower_bound_) + ", " +
               std::to_string(upper_bound_) + "].";
    }

 private:
    T lower_bound_;
    T upper_bound_;
};

struct Unexpected : public ValueError {
    Unexpected(const std::string& name, const std::string& value) : ValueError(name, value) {
    }

    std::string
    message() override {
        return "An unknown error occurred while setting " + name_ + " as " + value_;
    }
};

class BaseValue {
 public:
    BaseValue(const char* name, const char* alias, bool modifiable);
    virtual ~BaseValue() = default;

 public:
    bool inited_ = false;
    const char* name_;
    const char* alias_;
    const bool modifiable_;

 public:
    virtual void
    Init();

    virtual void
    Set(const std::string& value, bool update) = 0;

    virtual std::string
    Get() = 0;
};
using BaseValuePtr = std::shared_ptr<BaseValue>;

class BoolValue : public BaseValue {
 public:
    BoolValue(const char* name,
              const char* alias,
              bool modifiable,
              Value<bool>& config,
              bool default_value,
              std::function<bool(bool val, std::string& err)> is_valid_fn = nullptr);

 private:
    Value<bool>& config_;
    const bool default_value_;
    std::function<bool(bool val, std::string& err)> is_valid_fn_;

 public:
    void
    Init() override;

    void
    Set(const std::string& value, bool update) override;

    std::string
    Get() override;
};

class StringValue : public BaseValue {
 public:
    StringValue(const char* name,
                const char* alias,
                bool modifiable,
                Value<std::string>& config,
                const char* default_value,
                std::function<bool(const std::string& val, std::string& err)> is_valid_fn = nullptr);

 private:
    Value<std::string>& config_;
    const char* default_value_;
    std::function<bool(const std::string& val, std::string& err)> is_valid_fn_;

 public:
    void
    Init() override;

    void
    Set(const std::string& value, bool update) override;

    std::string
    Get() override;
};

class EnumValue : public BaseValue {
 public:
    EnumValue(const char* name,
              const char* alias,
              bool modifiable,
              valueEnum* enumd,
              Value<int64_t>& config,
              int64_t default_value,
              std::function<bool(int64_t val, std::string& err)> is_valid_fn = nullptr);

 private:
    Value<int64_t>& config_;
    valueEnum* enum_value_;
    const int64_t default_value_;
    std::function<bool(int64_t val, std::string& err)> is_valid_fn_;

 public:
    void
    Init() override;

    void
    Set(const std::string& value, bool update) override;

    std::string
    Get() override;
};

class IntegerValue : public BaseValue {
 public:
    IntegerValue(const char* name,
                 const char* alias,
                 bool modifiable,
                 int64_t lower_bound,
                 int64_t upper_bound,
                 Value<int64_t>& config,
                 int64_t default_value,
                 std::function<bool(int64_t val, std::string& err)> is_valid_fn = nullptr);

 private:
    Value<int64_t>& config_;
    int64_t lower_bound_;
    int64_t upper_bound_;
    const int64_t default_value_;
    std::function<bool(int64_t val, std::string& err)> is_valid_fn_;

 public:
    void
    Init() override;

    void
    Set(const std::string& value, bool update) override;

    std::string
    Get() override;
};

class FloatingValue : public BaseValue {
 public:
    FloatingValue(const char* name,
                  const char* alias,
                  bool modifiable,
                  double lower_bound,
                  double upper_bound,
                  Value<double>& config,
                  double default_value,
                  std::function<bool(double val, std::string& err)> is_valid_fn = nullptr);

 private:
    Value<double>& config_;
    double lower_bound_;
    double upper_bound_;
    const double default_value_;
    std::function<bool(double val, std::string& err)> is_valid_fn_;

 public:
    void
    Init() override;

    void
    Set(const std::string& value, bool update) override;

    std::string
    Get() override;
};

class SizeValue : public BaseValue {
 public:
    SizeValue(const char* name,
              const char* alias,
              bool modifiable,
              int64_t lower_bound,
              int64_t upper_bound,
              Value<int64_t>& config,
              int64_t default_value,
              std::function<bool(int64_t val, std::string& err)> is_valid_fn = nullptr);

 private:
    Value<int64_t>& config_;
    int64_t lower_bound_;
    int64_t upper_bound_;
    const int64_t default_value_;
    std::function<bool(int64_t val, std::string& err)> is_valid_fn_;

 public:
    void
    Init() override;

    void
    Set(const std::string& value, bool update) override;

    std::string
    Get() override;
};

class TimeValue : public BaseValue {
 public:
    TimeValue(const char* name,
              const char* alias,
              bool modifiable,
              int64_t lower_bound,
              int64_t upper_bound,
              Value<int64_t>& config,
              int64_t default_value,
              std::function<bool(int64_t val, std::string& err)> is_valid_fn = nullptr);

 private:
    Value<int64_t>& config_;
    int64_t lower_bound_;
    int64_t upper_bound_;
    const int64_t default_value_;
    std::function<bool(int64_t val, std::string& err)> is_valid_fn_;

 public:
    void
    Init() override;

    void
    Set(const std::string& value, bool update) override;

    std::string
    Get() override;
};

/* create config with {is_valid} function */

#define CreateBoolValue(name, modifiable, config_addr, default, is_valid) \
    std::make_shared<BoolValue>(name, nullptr, modifiable, config_addr, (default), is_valid)

#define CreateStringValue(name, modifiable, config_addr, default, is_valid) \
    std::make_shared<StringValue>(name, nullptr, modifiable, config_addr, (default), is_valid)

#define CreateEnumValue(name, modifiable, enumd, config_addr, default, is_valid) \
    std::make_shared<EnumValue>(name, nullptr, modifiable, enumd, config_addr, (default), is_valid)

#define CreateIntegerValue(name, modifiable, lower_bound, upper_bound, config_addr, default, is_valid)          \
    std::make_shared<IntegerValue>(name, nullptr, modifiable, lower_bound, upper_bound, config_addr, (default), \
                                   is_valid)

#define CreateFloatingValue(name, modifiable, lower_bound, upper_bound, config_addr, default, is_valid)          \
    std::make_shared<FloatingValue>(name, nullptr, modifiable, lower_bound, upper_bound, config_addr, (default), \
                                    is_valid)

#define CreateSizeValue(name, modifiable, lower_bound, upper_bound, config_addr, default, is_valid) \
    std::make_shared<SizeValue>(name, nullptr, modifiable, lower_bound, upper_bound, config_addr, (default), is_valid)

#define CreateTimeValue(name, modifiable, lower_bound, upper_bound, config_addr, default, is_valid) \
    std::make_shared<TimeValue>(name, nullptr, modifiable, lower_bound, upper_bound, config_addr, (default), is_valid)

}  // namespace milvus
