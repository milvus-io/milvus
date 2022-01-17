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

#include "value/ValueType.h"

#include <strings.h>
#include <algorithm>
#include <cassert>
#include <functional>
#include <map>
#include <regex>
#include <sstream>
#include <string>

namespace {
std::unordered_map<std::string, int64_t> BYTE_UNITS = {
    {"b", 1},
    {"k", 1024},
    {"m", 1024 * 1024},
    {"g", 1024 * 1024 * 1024},
};

std::map<std::string, int64_t> TIME_UNITS = {
    //    {"seconds", 1ll},
    //    {"minutes", 1ll * 60},
    {"hours", 1ll * 60 * 60},
    {"days", 1ll * 60 * 60 * 24},
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
               << " The postfix should be one of the `b` `k` `m` `g` characters.";
            err = ss.str();
        }
    } catch (...) {
        err = "Unknown error happened on parse bytes.";
    }
    return 0;
}

int64_t
parse_time(const std::string& str, std::string& err) {
    try {
        const std::regex regex(R"(\s*([0-9]+)\s*(seconds|minutes|hours|days)\s*)");
        std::smatch base_match;
        auto& units = TIME_UNITS;
        if (std::regex_match(str, base_match, regex) && base_match.size() == 3 &&
            units.find(base_match[2].str()) != units.end()) {
            return stoll(base_match[1].str()) * units[base_match[2].str()];
        } else {
            std::stringstream ss;
            ss << "The specified value for time (" << str << ") should specify the units."
               << " The postfix should be one of the ";
            for (auto& pair : units) {
                ss << "`" << pair.first << "` ";
            }
            ss << "words.";
            err = ss.str();
        }
    } catch (...) {
        err = "Unknown error happened on parse time.";
    }
    return 0;
}

}  // namespace

// Use (void) to silent unused warnings.
#define assertm(exp, msg) assert(((void)msg, exp))

namespace milvus {

std::vector<std::string>
OptionValue(const valueEnum& ce) {
    std::vector<std::string> ret;
    for (auto& e : ce) {
        ret.emplace_back(e.first);
    }
    return ret;
}

BaseValue::BaseValue(const char* name, const char* alias, bool modifiable)
    : name_(name), alias_(alias), modifiable_(modifiable) {
}

void
BaseValue::Init() {
    assertm(not inited_, "already initialized");
    inited_ = true;
}

BoolValue::BoolValue(const char* name,
                     const char* alias,
                     bool modifiable,
                     Value<bool>& config,
                     bool default_value,
                     std::function<bool(bool val, std::string& err)> is_valid_fn)
    : BaseValue(name, alias, modifiable),
      config_(config),
      default_value_(default_value),
      is_valid_fn_(std::move(is_valid_fn)) {
}

void
BoolValue::Init() {
    BaseValue::Init();
    config_ = default_value_;
}

void
BoolValue::Set(const std::string& val, bool update) {
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
    } catch (ValueError& e) {
        throw;
    } catch (...) {
        throw Unexpected(name_, val);
    }
}

std::string
BoolValue::Get() {
    assertm(inited_, "uninitialized");
    return config_() ? "true" : "false";
}

StringValue::StringValue(const char* name,
                         const char* alias,
                         bool modifiable,
                         Value<std::string>& config,
                         const char* default_value,
                         std::function<bool(const std::string& val, std::string& err)> is_valid_fn)
    : BaseValue(name, alias, modifiable),
      config_(config),
      default_value_(default_value),
      is_valid_fn_(std::move(is_valid_fn)) {
}

void
StringValue::Init() {
    BaseValue::Init();
    config_ = default_value_;
}

void
StringValue::Set(const std::string& val, bool update) {
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
    } catch (ValueError& e) {
        throw;
    } catch (...) {
        throw Unexpected(name_, val);
    }
}

std::string
StringValue::Get() {
    assertm(inited_, "uninitialized");
    return config_();
}

EnumValue::EnumValue(const char* name,
                     const char* alias,
                     bool modifiable,
                     valueEnum* enumd,
                     Value<int64_t>& config,
                     int64_t default_value,
                     std::function<bool(int64_t val, std::string& err)> is_valid_fn)
    : BaseValue(name, alias, modifiable),
      config_(config),
      enum_value_(enumd),
      default_value_(default_value),
      is_valid_fn_(std::move(is_valid_fn)) {
}

void
EnumValue::Init() {
    BaseValue::Init();
    assert(enum_value_ != nullptr);
    assertm(not enum_value_->empty(), "enum value empty");
    config_ = default_value_;
}

void
EnumValue::Set(const std::string& val, bool update) {
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
    } catch (ValueError& e) {
        throw;
    } catch (...) {
        throw Unexpected(name_, val);
    }
}

std::string
EnumValue::Get() {
    assertm(inited_, "uninitialized");
    auto val = config_();
    for (auto& it : *enum_value_) {
        if (val == it.second) {
            return it.first;
        }
    }
    return "unknown";
}

IntegerValue::IntegerValue(const char* name,
                           const char* alias,
                           bool modifiable,
                           int64_t lower_bound,
                           int64_t upper_bound,
                           Value<int64_t>& config,
                           int64_t default_value,
                           std::function<bool(int64_t val, std::string& err)> is_valid_fn)
    : BaseValue(name, alias, modifiable),
      config_(config),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound),
      default_value_(default_value),
      is_valid_fn_(std::move(is_valid_fn)) {
}

void
IntegerValue::Init() {
    BaseValue::Init();
    config_ = default_value_;
}

void
IntegerValue::Set(const std::string& val, bool update) {
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
    } catch (ValueError& e) {
        throw;
    } catch (...) {
        throw Unexpected(name_, val);
    }
}

std::string
IntegerValue::Get() {
    assertm(inited_, "uninitialized");
    return std::to_string(config_());
}

FloatingValue::FloatingValue(const char* name,
                             const char* alias,
                             bool modifiable,
                             double lower_bound,
                             double upper_bound,
                             Value<double>& config,
                             double default_value,
                             std::function<bool(double val, std::string& err)> is_valid_fn)
    : BaseValue(name, alias, modifiable),
      config_(config),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound),
      default_value_(default_value),
      is_valid_fn_(std::move(is_valid_fn)) {
}

void
FloatingValue::Init() {
    BaseValue::Init();
    config_ = default_value_;
}

void
FloatingValue::Set(const std::string& val, bool update) {
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
    } catch (ValueError& e) {
        throw;
    } catch (...) {
        throw Unexpected(name_, val);
    }
}

std::string
FloatingValue::Get() {
    assertm(inited_, "uninitialized");
    return std::to_string(config_());
}

SizeValue::SizeValue(const char* name,
                     const char* alias,
                     bool modifiable,
                     int64_t lower_bound,
                     int64_t upper_bound,
                     Value<int64_t>& config,
                     int64_t default_value,
                     std::function<bool(int64_t val, std::string& err)> is_valid_fn)
    : BaseValue(name, alias, modifiable),
      config_(config),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound),
      default_value_(default_value),
      is_valid_fn_(std::move(is_valid_fn)) {
}

void
SizeValue::Init() {
    BaseValue::Init();
    config_ = default_value_;
}

void
SizeValue::Set(const std::string& val, bool update) {
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
    } catch (ValueError& e) {
        throw;
    } catch (...) {
        throw Unexpected(name_, val);
    }
}

std::string
SizeValue::Get() {
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

TimeValue::TimeValue(const char* name,
                     const char* alias,
                     bool modifiable,
                     int64_t lower_bound,
                     int64_t upper_bound,
                     Value<int64_t>& config,
                     int64_t default_value,
                     std::function<bool(int64_t val, std::string& err)> is_valid_fn)
    : BaseValue(name, alias, modifiable),
      config_(config),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound),
      default_value_(default_value),
      is_valid_fn_(std::move(is_valid_fn)) {
}

void
TimeValue::Init() {
    BaseValue::Init();
    config_ = default_value_;
}

void
TimeValue::Set(const std::string& val, bool update) {
    assertm(inited_, "uninitialized");
    try {
        /* Check modifiable */
        if (update and not modifiable_) {
            throw Immutable(name_, val);
        }

        /* Parse from string */
        std::string err;
        int64_t value = parse_time(val, err);
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
    } catch (ValueError& e) {
        throw;
    } catch (...) {
        throw Unexpected(name_, val);
    }
}

std::string
TimeValue::Get() {
    assertm(inited_, "uninitialized");
    auto val = config_();
    const int64_t second = 1ll;
    const int64_t minute = second * 60;
    const int64_t hour = minute * 60;
    const int64_t day = hour * 24;
    if (val % day == 0) {
        return std::to_string(val / day) + "days";
    } else if (val % hour == 0) {
        return std::to_string(val / hour) + "hours";
    } else if (val % minute == 0) {
        return std::to_string(val / minute) + "minutes";
    } else {
        return std::to_string(val) + "seconds";
    }
}

}  // namespace milvus
