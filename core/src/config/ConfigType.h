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

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace milvus {

using configEnum = const std::unordered_map<std::string, int64_t>;
std::vector<std::string>
OptionValue(const configEnum& ce);

enum SetReturn {
    SUCCESS = 1,
    IMMUTABLE,
    ENUM_VALUE_NOTFOUND,
    INVALID,
    OUT_OF_RANGE,
    UPDATE_FAILURE,
    EXCEPTION,
    UNEXPECTED,
};

struct ConfigStatus {
    ConfigStatus(SetReturn sr, std::string msg) : set_return(sr), message(std::move(msg)) {
    }
    SetReturn set_return;
    std::string message;
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

    virtual ConfigStatus
    Set(const std::string& value, bool update) = 0;

    virtual std::string
    Get() = 0;
};
using BaseConfigPtr = std::shared_ptr<BaseConfig>;

class BoolConfig : public BaseConfig {
 public:
    BoolConfig(const char* name, const char* alias, bool modifiable, bool* config, bool default_value,
               std::function<bool(bool val, std::string& err)> is_valid_fn = nullptr,
               std::function<bool(bool val, bool prev, std::string& err)> update_fn = nullptr);

 private:
    bool* config_;
    const bool default_value_;
    std::function<bool(bool val, std::string& err)> is_valid_fn_;
    std::function<bool(bool val, bool prev, std::string& err)> update_fn_;

 public:
    void
    Init() override;

    ConfigStatus
    Set(const std::string& value, bool update) override;

    std::string
    Get() override;
};

class StringConfig : public BaseConfig {
 public:
    StringConfig(
        const char* name, const char* alias, bool modifiable, std::string* config, const char* default_value,
        std::function<bool(const std::string& val, std::string& err)> is_valid_fn = nullptr,
        std::function<bool(const std::string& val, const std::string& prev, std::string& err)> update_fn = nullptr);

 private:
    std::string* config_;
    const char* default_value_;
    std::function<bool(const std::string& val, std::string& err)> is_valid_fn_;
    std::function<bool(const std::string& val, const std::string& prev, std::string& err)> update_fn_;

 public:
    void
    Init() override;

    ConfigStatus
    Set(const std::string& value, bool update) override;

    std::string
    Get() override;
};

class EnumConfig : public BaseConfig {
 public:
    EnumConfig(const char* name, const char* alias, bool modifiable, configEnum* enumd, int64_t* config,
               int64_t default_value, std::function<bool(int64_t val, std::string& err)> is_valid_fn = nullptr,
               std::function<bool(int64_t val, int64_t prev, std::string& err)> update_fn = nullptr);

 private:
    int64_t* config_;
    configEnum* enum_value_;
    const int64_t default_value_;
    std::function<bool(int64_t val, std::string& err)> is_valid_fn_;
    std::function<bool(int64_t val, int64_t prev, std::string& err)> update_fn_;

 public:
    void
    Init() override;

    ConfigStatus
    Set(const std::string& value, bool update) override;

    std::string
    Get() override;
};

class IntegerConfig : public BaseConfig {
 public:
    IntegerConfig(const char* name, const char* alias, bool modifiable, int64_t lower_bound, int64_t upper_bound,
                  int64_t* config, int64_t default_value,
                  std::function<bool(int64_t val, std::string& err)> is_valid_fn = nullptr,
                  std::function<bool(int64_t val, int64_t prev, std::string& err)> update_fn = nullptr);

 private:
    int64_t* config_;
    int64_t lower_bound_;
    int64_t upper_bound_;
    const int64_t default_value_;
    std::function<bool(int64_t val, std::string& err)> is_valid_fn_;
    std::function<bool(int64_t val, int64_t prev, std::string& err)> update_fn_;

 public:
    void
    Init() override;

    ConfigStatus
    Set(const std::string& value, bool update) override;

    std::string
    Get() override;
};

class FloatingConfig : public BaseConfig {
 public:
    FloatingConfig(const char* name, const char* alias, bool modifiable, double lower_bound, double upper_bound,
                   double* config, double default_value,
                   std::function<bool(double val, std::string& err)> is_valid_fn = nullptr,
                   std::function<bool(double val, double prev, std::string& err)> update_fn = nullptr);

 private:
    double* config_;
    double lower_bound_;
    double upper_bound_;
    const double default_value_;
    std::function<bool(double val, std::string& err)> is_valid_fn_;
    std::function<bool(double val, double prev, std::string& err)> update_fn_;

 public:
    void
    Init() override;

    ConfigStatus
    Set(const std::string& value, bool update) override;

    std::string
    Get() override;
};

class SizeConfig : public BaseConfig {
 public:
    SizeConfig(const char* name, const char* alias, bool modifiable, int64_t lower_bound, int64_t upper_bound,
               int64_t* config, int64_t default_value,
               std::function<bool(int64_t val, std::string& err)> is_valid_fn = nullptr,
               std::function<bool(int64_t val, int64_t prev, std::string& err)> update_fn = nullptr);

 private:
    int64_t* config_;
    int64_t lower_bound_;
    int64_t upper_bound_;
    const int64_t default_value_;
    std::function<bool(int64_t val, std::string& err)> is_valid_fn_;
    std::function<bool(int64_t val, int64_t prev, std::string& err)> update_fn_;

 public:
    void
    Init() override;

    ConfigStatus
    Set(const std::string& value, bool update) override;

    std::string
    Get() override;
};

/* create config without {is_valid, update} function */

#define CreateBoolConfig(name, config_addr, default) \
    CreateBoolConfig_(name, true, config_addr, (default), nullptr, nullptr)

#define CreateStringConfig(name, config_addr, default) \
    CreateStringConfig_(name, true, config_addr, (default), nullptr, nullptr)

#define CreateEnumConfig(name, enumd, config_addr, default) \
    CreateEnumConfig_(name, true, enumd, config_addr, (default), nullptr, nullptr)

#define CreateIntegerConfig(name, lower_bound, upper_bound, config_addr, default) \
    CreateIntegerConfig_(name, true, lower_bound, upper_bound, config_addr, (default), nullptr, nullptr)

#define CreateFloatingConfig(name, lower_bound, upper_bound, config_addr, default) \
    CreateFloatingConfig_(name, true, lower_bound, upper_bound, config_addr, (default), nullptr, nullptr)

#define CreateSizeConfig(name, lower_bound, upper_bound, config_addr, default) \
    CreateSizeConfig_(name, true, lower_bound, upper_bound, config_addr, (default), nullptr, nullptr)

/* create config with {is_valid, update} function */

#define CreateBoolConfig_(name, modifiable, config_addr, default, is_valid, update) \
    std::make_shared<BoolConfig>(name, nullptr, modifiable, config_addr, (default), is_valid, update)

#define CreateStringConfig_(name, modifiable, config_addr, default, is_valid, update) \
    std::make_shared<StringConfig>(name, nullptr, modifiable, config_addr, (default), is_valid, update)

#define CreateEnumConfig_(name, modifiable, enumd, config_addr, default, is_valid, update) \
    std::make_shared<EnumConfig>(name, nullptr, modifiable, enumd, config_addr, (default), is_valid, update)

#define CreateIntegerConfig_(name, modifiable, lower_bound, upper_bound, config_addr, default, is_valid, update) \
    std::make_shared<IntegerConfig>(name, nullptr, modifiable, lower_bound, upper_bound, config_addr, (default), \
                                    is_valid, update)

#define CreateFloatingConfig_(name, modifiable, lower_bound, upper_bound, config_addr, default, is_valid, update) \
    std::make_shared<FloatingConfig>(name, nullptr, modifiable, lower_bound, upper_bound, config_addr, (default), \
                                     is_valid, update)

#define CreateSizeConfig_(name, modifiable, lower_bound, upper_bound, config_addr, default, is_valid, update) \
    std::make_shared<SizeConfig>(name, nullptr, modifiable, lower_bound, upper_bound, config_addr, (default), \
                                 is_valid, update)

}  // namespace milvus
