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

#include "Value.h"

namespace milvus {

using configEnum = const std::unordered_map<std::string, int64_t>;
std::vector<std::string>
OptionValue(const configEnum& ce);

struct ConfigError : public std::exception {
    explicit ConfigError(const std::string& name, const std::string& value) : name_(name), value_(value) {
    }

    virtual std::string
    message() = 0;

 protected:
    const std::string name_;
    const std::string value_;
};

struct Immutable : public ConfigError {
    explicit Immutable(const std::string& name, const std::string& value) : ConfigError(name, value) {
    }

    std::string
    message() override {
        return "Config " + name_ + " is immutable.";
    }
};

struct EnumValueNotFound : public ConfigError {
    EnumValueNotFound(const std::string& name, const std::string& value, std::vector<std::string> option_values)
        : ConfigError(name, value), option_values_(std::move(option_values)) {
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

struct Invalid : public ConfigError {
    Invalid(const std::string& name, const std::string& value, const std::string& reason)
        : ConfigError(name, value), reason_(reason) {
    }

    std::string
    message() override {
        return value_ + " is invalid for config " + name_ + ": " + reason_;
    }

 private:
    const std::string reason_;
};

template <typename T>
struct OutOfRange : public ConfigError {
    OutOfRange(const std::string& name, const std::string& value, T lower_bound, T upper_bound)
        : ConfigError(name, value), lower_bound_(lower_bound), upper_bound_(upper_bound) {
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

struct Unexpected : public ConfigError {
    Unexpected(const std::string& name, const std::string& value) : ConfigError(name, value) {
    }

    std::string
    message() override {
        return "An unknown error occurred while setting " + name_ + " as " + value_;
    }
};

class BaseConfig {
 public:
    BaseConfig(const char* name, const char* alias, bool modifiable);
    virtual ~BaseConfig() = default;

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
using BaseConfigPtr = std::shared_ptr<BaseConfig>;

class BoolConfig : public BaseConfig {
 public:
    BoolConfig(const char* name, const char* alias, bool modifiable, Value<bool>& config, bool default_value,
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

class StringConfig : public BaseConfig {
 public:
    StringConfig(const char* name, const char* alias, bool modifiable, Value<std::string>& config,
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

class EnumConfig : public BaseConfig {
 public:
    EnumConfig(const char* name, const char* alias, bool modifiable, configEnum* enumd, Value<int64_t>& config,
               int64_t default_value, std::function<bool(int64_t val, std::string& err)> is_valid_fn = nullptr);

 private:
    Value<int64_t>& config_;
    configEnum* enum_value_;
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

class IntegerConfig : public BaseConfig {
 public:
    IntegerConfig(const char* name, const char* alias, bool modifiable, int64_t lower_bound, int64_t upper_bound,
                  Value<int64_t>& config, int64_t default_value,
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

class FloatingConfig : public BaseConfig {
 public:
    FloatingConfig(const char* name, const char* alias, bool modifiable, double lower_bound, double upper_bound,
                   Value<double>& config, double default_value,
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

class SizeConfig : public BaseConfig {
 public:
    SizeConfig(const char* name, const char* alias, bool modifiable, int64_t lower_bound, int64_t upper_bound,
               Value<int64_t>& config, int64_t default_value,
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

#define CreateBoolConfig(name, modifiable, config_addr, default, is_valid) \
    std::make_shared<BoolConfig>(name, nullptr, modifiable, config_addr, (default), is_valid)

#define CreateStringConfig(name, modifiable, config_addr, default, is_valid) \
    std::make_shared<StringConfig>(name, nullptr, modifiable, config_addr, (default), is_valid)

#define CreateEnumConfig(name, modifiable, enumd, config_addr, default, is_valid) \
    std::make_shared<EnumConfig>(name, nullptr, modifiable, enumd, config_addr, (default), is_valid)

#define CreateIntegerConfig(name, modifiable, lower_bound, upper_bound, config_addr, default, is_valid)          \
    std::make_shared<IntegerConfig>(name, nullptr, modifiable, lower_bound, upper_bound, config_addr, (default), \
                                    is_valid)

#define CreateFloatingConfig(name, modifiable, lower_bound, upper_bound, config_addr, default, is_valid)          \
    std::make_shared<FloatingConfig>(name, nullptr, modifiable, lower_bound, upper_bound, config_addr, (default), \
                                     is_valid)

#define CreateSizeConfig(name, modifiable, lower_bound, upper_bound, config_addr, default, is_valid) \
    std::make_shared<SizeConfig>(name, nullptr, modifiable, lower_bound, upper_bound, config_addr, (default), is_valid)

}  // namespace milvus
