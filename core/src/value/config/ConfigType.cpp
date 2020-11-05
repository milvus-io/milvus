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

#include "value/config/ConfigType.h"

#include <strings.h>
#include <algorithm>
#include <cassert>
#include <functional>
#include <sstream>
#include <string>

namespace {
std::unordered_map<std::string, int64_t> BYTE_UNITS = {
    {"b", 1},
    {"k", 1024},
    {"m", 1024 * 1024},
    {"g", 1024 * 1024 * 1024},
};

bool
is_integer(const std::string& s) {
    if (not s.empty() && (std::isdigit(s[0]) || s[0] == '-')) {
        auto ss = s.substr(1);
        return std::find_if(ss.begin(), ss.end(), [](unsigned char c) { return !std::isdigit(c); }) == ss.end();
    }
    return false;
}

bool
is_number(const std::string& s) {
    return !s.empty() && std::find_if(s.begin(), s.end(), [](unsigned char c) { return !std::isdigit(c); }) == s.end();
}

bool
is_alpha(const std::string& s) {
    return !s.empty() && std::find_if(s.begin(), s.end(), [](unsigned char c) { return !std::isalpha(c); }) == s.end();
}

template <typename T>
bool
boundary_check(T val, T lower_bound, T upper_bound) {
    return lower_bound <= val && val <= upper_bound;
}

bool
parse_bool(const std::string& str, std::string& err) {
    if (!strcasecmp(str.c_str(), "true")) {
        return true;
    } else if (!strcasecmp(str.c_str(), "false")) {
        return false;
    } else {
        err = "The specified value must be true or false";
        return false;
    }
}

std::string
str_tolower(std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return std::tolower(c); });
    return s;
}

int64_t
parse_bytes(const std::string& str, std::string& err) {
    try {
        if (str.find_first_of('-') != std::string::npos) {
            std::stringstream ss;
            ss << "The specified value for memory (" << str << ") should be a positive integer.";
            err = ss.str();
            return 0;
        }

        std::string s = str;
        if (is_number(s)) {
            return std::stoll(s);
        }
        if (s.length() == 0) {
            return 0;
        }

        auto last_two = s.substr(s.length() - 2, 2);
        auto last_one = s.substr(s.length() - 1);
        if (is_alpha(last_two) && is_alpha(last_one)) {
            if (last_one == "b" or last_one == "B") {
                s = s.substr(0, s.length() - 1);
            }
        }
        auto& units = BYTE_UNITS;
        auto suffix = str_tolower(s.substr(s.length() - 1));

        std::string digits_part;
        if (is_number(suffix)) {
            digits_part = s;
            suffix = 'b';
        } else {
            digits_part = s.substr(0, s.length() - 1);
        }

        if (is_number(digits_part) && (units.find(suffix) != units.end() || is_number(suffix))) {
            auto digits = std::stoll(digits_part);
            return digits * units[suffix];
        } else {
            std::stringstream ss;
            ss << "The specified value for memory (" << str << ") should specify the units."
               << "The postfix should be one of the `b` `k` `m` `g` characters";
            err = ss.str();
        }
    } catch (...) {
        err = "Unknown error happened on parse bytes.";
    }
    return 0;
}

}  // namespace

// Use (void) to silent unused warnings.
#define assertm(exp, msg) assert(((void)msg, exp))

namespace milvus {

std::vector<std::string>
OptionValue(const configEnum& ce) {
    std::vector<std::string> ret;
    for (auto& e : ce) {
        ret.emplace_back(e.first);
    }
    return ret;
}

BaseConfig::BaseConfig(const char* name, const char* alias, bool modifiable)
    : name_(name), alias_(alias), modifiable_(modifiable) {
}

void
BaseConfig::Init() {
    assertm(not inited_, "already initialized");
    inited_ = true;
}

BoolConfig::BoolConfig(const char* name, const char* alias, bool modifiable, Value<bool>& config, bool default_value,
                       std::function<bool(bool val, std::string& err)> is_valid_fn)
    : BaseConfig(name, alias, modifiable),
      config_(config),
      default_value_(default_value),
      is_valid_fn_(std::move(is_valid_fn)) {
}

void
BoolConfig::Init() {
    BaseConfig::Init();
    config_ = default_value_;
}

void
BoolConfig::Set(const std::string& val, bool update) {
    assertm(inited_, "uninitialized");
    try {
        /* Check modifiable */
        if (update and not modifiable_) {
            throw Immutable(name_, val);
        }

        /* Parse from string */
        std::string err;
        bool value = parse_bool(val, err);
        if (not err.empty()) {
            throw Invalid(name_, val, err);
        }

        /* Validate */
        if (is_valid_fn_ && not is_valid_fn_(value, err)) {
            throw Invalid(name_, val, err);
        }

        /* Set value */
        config_ = value;
    } catch (ConfigError& e) {
        throw;
    } catch (...) {
        throw Unexpected(name_, val);
    }
}

std::string
BoolConfig::Get() {
    assertm(inited_, "uninitialized");
    return config_() ? "true" : "false";
}

StringConfig::StringConfig(const char* name, const char* alias, bool modifiable, Value<std::string>& config,
                           const char* default_value,
                           std::function<bool(const std::string& val, std::string& err)> is_valid_fn)
    : BaseConfig(name, alias, modifiable),
      config_(config),
      default_value_(default_value),
      is_valid_fn_(std::move(is_valid_fn)) {
}

void
StringConfig::Init() {
    BaseConfig::Init();
    config_ = default_value_;
}

void
StringConfig::Set(const std::string& val, bool update) {
    assertm(inited_, "uninitialized");
    try {
        /* Check modifiable */
        if (update and not modifiable_) {
            throw Immutable(name_, val);
        }

        /* Validate */
        std::string err;
        if (is_valid_fn_ && not is_valid_fn_(val, err)) {
            throw Invalid(name_, val, err);
        }

        /* Set value */
        config_ = val;
    } catch (ConfigError& e) {
        throw;
    } catch (...) {
        throw Unexpected(name_, val);
    }
}

std::string
StringConfig::Get() {
    assertm(inited_, "uninitialized");
    return config_();
}

EnumConfig::EnumConfig(const char* name, const char* alias, bool modifiable, configEnum* enumd, Value<int64_t>& config,
                       int64_t default_value, std::function<bool(int64_t val, std::string& err)> is_valid_fn)
    : BaseConfig(name, alias, modifiable),
      config_(config),
      enum_value_(enumd),
      default_value_(default_value),
      is_valid_fn_(std::move(is_valid_fn)) {
}

void
EnumConfig::Init() {
    BaseConfig::Init();
    assert(enum_value_ != nullptr);
    assertm(not enum_value_->empty(), "enum value empty");
    config_ = default_value_;
}

void
EnumConfig::Set(const std::string& val, bool update) {
    assertm(inited_, "uninitialized");
    try {
        /* Check modifiable */
        if (update and not modifiable_) {
            throw Immutable(name_, val);
        }

        /* Check if value exist */
        if (enum_value_->find(val) == enum_value_->end()) {
            auto option_values = OptionValue(*enum_value_);
            throw EnumValueNotFound(name_, val, std::move(option_values));
        }

        int64_t value = enum_value_->at(val);

        /* Validate */
        std::string err;
        if (is_valid_fn_ && not is_valid_fn_(value, err)) {
            throw Invalid(name_, val, err);
        }

        /* Set value */
        config_ = value;
    } catch (ConfigError& e) {
        throw;
    } catch (...) {
        throw Unexpected(name_, val);
    }
}

std::string
EnumConfig::Get() {
    assertm(inited_, "uninitialized");
    auto val = config_();
    for (auto& it : *enum_value_) {
        if (val == it.second) {
            return it.first;
        }
    }
    return "unknown";
}

IntegerConfig::IntegerConfig(const char* name, const char* alias, bool modifiable, int64_t lower_bound,
                             int64_t upper_bound, Value<int64_t>& config, int64_t default_value,
                             std::function<bool(int64_t val, std::string& err)> is_valid_fn)
    : BaseConfig(name, alias, modifiable),
      config_(config),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound),
      default_value_(default_value),
      is_valid_fn_(std::move(is_valid_fn)) {
}

void
IntegerConfig::Init() {
    BaseConfig::Init();
    config_ = default_value_;
}

void
IntegerConfig::Set(const std::string& val, bool update) {
    assertm(inited_, "uninitialized");
    try {
        /* Check modifiable */
        if (update and not modifiable_) {
            throw Immutable(name_, val);
        }

        /* Check if it is an integer */
        if (not is_integer(val)) {
            throw Invalid(name_, val, "Not an integer.");
        }

        /* Parse from string */
        int64_t value = std::stoll(val);

        /* Boundary check */
        if (not boundary_check<int64_t>(value, lower_bound_, upper_bound_)) {
            throw OutOfRange<int64_t>(name_, val, lower_bound_, upper_bound_);
        }

        /* Validate */
        std::string err;
        if (is_valid_fn_ && not is_valid_fn_(value, err)) {
            throw Invalid(name_, val, err);
        }

        /* Set value */
        config_ = value;
    } catch (ConfigError& e) {
        throw;
    } catch (...) {
        throw Unexpected(name_, val);
    }
}

std::string
IntegerConfig::Get() {
    assertm(inited_, "uninitialized");
    return std::to_string(config_());
}

FloatingConfig::FloatingConfig(const char* name, const char* alias, bool modifiable, double lower_bound,
                               double upper_bound, Value<double>& config, double default_value,
                               std::function<bool(double val, std::string& err)> is_valid_fn)
    : BaseConfig(name, alias, modifiable),
      config_(config),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound),
      default_value_(default_value),
      is_valid_fn_(std::move(is_valid_fn)) {
}

void
FloatingConfig::Init() {
    BaseConfig::Init();
    config_ = default_value_;
}

void
FloatingConfig::Set(const std::string& val, bool update) {
    assertm(inited_, "uninitialized");
    try {
        /* Check modifiable */
        if (update and not modifiable_) {
            throw Immutable(name_, val);
        }

        /* Parse from string */
        double value = std::stod(val);

        /* Boundary check */
        if (not boundary_check<double>(value, lower_bound_, upper_bound_)) {
            throw OutOfRange<double>(name_, val, lower_bound_, upper_bound_);
        }

        /* Validate */
        std::string err;
        if (is_valid_fn_ && not is_valid_fn_(value, err)) {
            throw Invalid(name_, val, err);
        }

        /* Set value */
        config_ = value;
    } catch (ConfigError& e) {
        throw;
    } catch (...) {
        throw Unexpected(name_, val);
    }
}

std::string
FloatingConfig::Get() {
    assertm(inited_, "uninitialized");
    return std::to_string(config_());
}

SizeConfig::SizeConfig(const char* name, const char* alias, bool modifiable, int64_t lower_bound, int64_t upper_bound,
                       Value<int64_t>& config, int64_t default_value,
                       std::function<bool(int64_t val, std::string& err)> is_valid_fn)
    : BaseConfig(name, alias, modifiable),
      config_(config),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound),
      default_value_(default_value),
      is_valid_fn_(std::move(is_valid_fn)) {
}

void
SizeConfig::Init() {
    BaseConfig::Init();
    config_ = default_value_;
}

void
SizeConfig::Set(const std::string& val, bool update) {
    assertm(inited_, "uninitialized");
    try {
        /* Check modifiable */
        if (update and not modifiable_) {
            throw Immutable(name_, val);
        }

        /* Parse from string */
        std::string err;
        int64_t value = parse_bytes(val, err);
        if (not err.empty()) {
            throw Invalid(name_, val, err);
        }

        /* Boundary check */
        if (not boundary_check<int64_t>(value, lower_bound_, upper_bound_)) {
            throw OutOfRange<int64_t>(name_, val, lower_bound_, upper_bound_);
        }

        /* Validate */
        if (is_valid_fn_ && not is_valid_fn_(value, err)) {
            throw Invalid(name_, val, err);
        }

        /* Set value */
        config_ = value;
    } catch (ConfigError& e) {
        throw;
    } catch (...) {
        throw Unexpected(name_, val);
    }
}

std::string
SizeConfig::Get() {
    assertm(inited_, "uninitialized");
    auto val = config_();
    const int64_t gb = 1024ll * 1024 * 1024;
    const int64_t mb = 1024ll * 1024;
    const int64_t kb = 1024ll;
    if (val % gb == 0) {
        return std::to_string(val / gb) + "GB";
    } else if (val % mb == 0) {
        return std::to_string(val / mb) + "MB";
    } else if (val % kb == 0) {
        return std::to_string(val / kb) + "KB";
    } else {
        return std::to_string(val);
    }
}

}  // namespace milvus
