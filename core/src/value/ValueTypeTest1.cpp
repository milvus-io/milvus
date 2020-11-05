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

#include <cstring>
#include <functional>

#include "gtest/gtest.h"
#include "value/ValueType.h"

namespace milvus {

#define _MODIFIABLE (true)
#define _IMMUTABLE (false)

template <typename T>
class Utils {
 public:
    bool
    validate_fn(const T& value, std::string& err) {
        validate_value = value;
        return true;
    }

 protected:
    T validate_value;
};

/* ValidBoolValueTest */
class ValidBoolValueTest : public testing::Test, public Utils<bool> {
 protected:
};

TEST_F(ValidBoolValueTest, init_load_update_get_test) {
    auto validate = std::bind(&ValidBoolValueTest::validate_fn, this, std::placeholders::_1, std::placeholders::_2);

    Value<bool> bool_value;
    auto bool_config = CreateBoolValue("b", _MODIFIABLE, bool_value, false, validate);
    ASSERT_EQ(bool_config->modifiable_, true);

    bool_config->Init();
    ASSERT_EQ(bool_value(), false);
    ASSERT_EQ(bool_config->Get(), "false");

    {
        // now `bool_value` is `false`, calling Set(update=false) to set it to `true`
        validate_value = false;

        EXPECT_NO_THROW(bool_config->Set("true", false));

        EXPECT_EQ(bool_value(), true);
        EXPECT_EQ(bool_config->Get(), "true");

        // expect change
        EXPECT_EQ(validate_value, true);
    }

    {
        // now `bool_value` is `true`, calling Set(update=true) to set it to `false`
        validate_value = true;

        EXPECT_NO_THROW(bool_config->Set("false", true));

        EXPECT_EQ(bool_value(), false);
        EXPECT_EQ(bool_config->Get(), "false");

        // expect change
        EXPECT_EQ(validate_value, false);
    }
}

/* ValidStringValueTest */
class ValidStringValueTest : public testing::Test, public Utils<std::string> {
 protected:
};

TEST_F(ValidStringValueTest, init_load_update_get_test) {
    auto validate = std::bind(&ValidStringValueTest::validate_fn, this, std::placeholders::_1, std::placeholders::_2);

    Value<std::string> string_value;
    auto string_config = CreateStringValue("s", _MODIFIABLE, string_value, "Magic", validate);
    ASSERT_EQ(string_config->modifiable_, true);

    string_config->Init();
    ASSERT_EQ(string_value(), "Magic");
    ASSERT_EQ(string_config->Get(), "Magic");

    {
        // now `string_value` is `Magic`, calling Set(update=false) to set it to `cigaM`
        validate_value = "";

        EXPECT_NO_THROW(string_config->Set("cigaM", false));

        EXPECT_EQ(string_value(), "cigaM");
        EXPECT_EQ(string_config->Get(), "cigaM");

        // expect change
        EXPECT_EQ(validate_value, "cigaM");
    }

    {
        // now `string_value` is `cigaM`, calling Set(update=true) to set it to `Check`
        validate_value = "";

        EXPECT_NO_THROW(string_config->Set("Check", true));

        EXPECT_EQ(string_value(), "Check");
        EXPECT_EQ(string_config->Get(), "Check");

        // expect change
        EXPECT_EQ(validate_value, "Check");
    }
}

/* ValidIntegerValueTest */
class ValidIntegerValueTest : public testing::Test, public Utils<int64_t> {
 protected:
};

TEST_F(ValidIntegerValueTest, init_load_update_get_test) {
    auto validate = std::bind(&ValidIntegerValueTest::validate_fn, this, std::placeholders::_1, std::placeholders::_2);

    Value<int64_t> integer_value;
    auto integer_config = CreateIntegerValue("i", _MODIFIABLE, -100, 100, integer_value, 42, validate);
    ASSERT_EQ(integer_config->modifiable_, true);

    integer_config->Init();
    ASSERT_EQ(integer_value(), 42);
    ASSERT_EQ(integer_config->Get(), "42");

    {
        // now `integer_value` is `42`, calling Set(update=false) to set it to `24`
        validate_value = 0;

        EXPECT_NO_THROW(integer_config->Set("24", false));

        EXPECT_EQ(integer_value(), 24);
        EXPECT_EQ(integer_config->Get(), "24");

        // expect change
        EXPECT_EQ(validate_value, 24);
    }

    {
        // now `integer_value` is `24`, calling Set(update=true) to set it to `36`
        validate_value = 0;

        EXPECT_NO_THROW(integer_config->Set("36", true));

        EXPECT_EQ(integer_value(), 36);
        EXPECT_EQ(integer_config->Get(), "36");

        // expect change
        EXPECT_EQ(validate_value, 36);
    }
}

/* ValidFloatingValueTest */
class ValidFloatingValueTest : public testing::Test, public Utils<double> {
 protected:
};

TEST_F(ValidFloatingValueTest, init_load_update_get_test) {
    auto validate = std::bind(&ValidFloatingValueTest::validate_fn, this, std::placeholders::_1, std::placeholders::_2);

    Value<double> floating_value;
    auto floating_config = CreateFloatingValue("f", _MODIFIABLE, -10.0, 10.0, floating_value, 3.14, validate);
    ASSERT_EQ(floating_config->modifiable_, true);

    floating_config->Init();
    ASSERT_FLOAT_EQ(floating_value(), 3.14);
    ASSERT_FLOAT_EQ(std::stof(floating_config->Get()), 3.14);

    {
        // now `floating_value` is `3.14`, calling Set(update=false) to set it to `6.22`
        validate_value = 0.0;

        EXPECT_NO_THROW(floating_config->Set("6.22", false));

        ASSERT_FLOAT_EQ(floating_value(), 6.22);
        ASSERT_FLOAT_EQ(std::stof(floating_config->Get()), 6.22);

        // expect change
        ASSERT_FLOAT_EQ(validate_value, 6.22);
    }

    {
        // now `integer_value` is `6.22`, calling Set(update=true) to set it to `-3.14`
        validate_value = 0.0;

        EXPECT_NO_THROW(floating_config->Set("-3.14", true));

        ASSERT_FLOAT_EQ(floating_value(), -3.14);
        ASSERT_FLOAT_EQ(std::stof(floating_config->Get()), -3.14);

        // expect change
        ASSERT_FLOAT_EQ(validate_value, -3.14);
    }
}

/* ValidEnumValueTest */
class ValidEnumValueTest : public testing::Test, public Utils<int64_t> {
 protected:
};

// template <>
// int64_t Utils<int64_t>::validate_value = 0;
// template <>
// int64_t Utils<int64_t>::new_value = 0;
// template <>
// int64_t Utils<int64_t>::prev_value = 0;

TEST_F(ValidEnumValueTest, init_load_update_get_test) {
    auto validate = std::bind(&ValidEnumValueTest::validate_fn, this, std::placeholders::_1, std::placeholders::_2);

    valueEnum testEnum{
        {"a", 1},
        {"b", 2},
        {"c", 3},
    };
    Value<int64_t> enum_value;
    auto enum_config = CreateEnumValue("e", _MODIFIABLE, &testEnum, enum_value, 1, validate);
    ASSERT_EQ(enum_config->modifiable_, true);

    enum_config->Init();
    ASSERT_EQ(enum_value(), 1);
    ASSERT_EQ(enum_config->Get(), "a");

    {
        // now `enum_value` is `a`, calling Set(update=false) to set it to `b`
        validate_value = 0;

        EXPECT_NO_THROW(enum_config->Set("b", false));

        ASSERT_EQ(enum_value(), 2);
        ASSERT_EQ(enum_config->Get(), "b");

        // expect change
        ASSERT_EQ(validate_value, 2);
    }

    {
        // now `enum_value` is `b`, calling Set(update=true) to set it to `c`
        validate_value = 0;

        EXPECT_NO_THROW(enum_config->Set("c", true));

        ASSERT_EQ(enum_value(), 3);
        ASSERT_EQ(enum_config->Get(), "c");

        // expect change
        ASSERT_EQ(validate_value, 3);
    }
}

/* ValidSizeValueTest */
class ValidSizeValueTest : public testing::Test, public Utils<int64_t> {
 protected:
};

// template <>
// int64_t Utils<int64_t>::validate_value = 0;
// template <>
// int64_t Utils<int64_t>::new_value = 0;
// template <>
// int64_t Utils<int64_t>::prev_value = 0;

TEST_F(ValidSizeValueTest, init_load_update_get_test) {
    auto validate = std::bind(&ValidSizeValueTest::validate_fn, this, std::placeholders::_1, std::placeholders::_2);

    Value<int64_t> size_value;
    auto size_config = CreateSizeValue("i", _MODIFIABLE, 0, 1024 * 1024, size_value, 1024, validate);
    ASSERT_EQ(size_config->modifiable_, true);

    size_config->Init();
    ASSERT_EQ(size_value(), 1024);
    ASSERT_EQ(size_config->Get(), "1KB");

    {
        // now `size_value` is `1024`, calling Set(update=false) to set it to `4096`
        validate_value = 0;

        EXPECT_NO_THROW(size_config->Set("4096", false));

        EXPECT_EQ(size_value(), 4096);
        EXPECT_EQ(size_config->Get(), "4KB");

        // expect change
        EXPECT_EQ(validate_value, 4096);
    }

    {
        // now `size_value` is `4096`, calling Set(update=true) to set it to `256kb`
        validate_value = 0;

        EXPECT_NO_THROW(size_config->Set("256kb", true));

        EXPECT_EQ(size_value(), 256 * 1024);
        EXPECT_EQ(size_config->Get(), "256KB");

        // expect change
        EXPECT_EQ(validate_value, 262144);
    }
}

class ValidTest : public testing::Test {
 protected:
    valueEnum family{
        {"ipv4", 1},
        {"ipv6", 2},
    };

    struct Server {
        Value<bool> running;
        Value<std::string> hostname;
        Value<int64_t> family;
        Value<int64_t> port;
        Value<double> uptime;
    };

    Server server;

 protected:
    void
    SetUp() override {
        config_list = {
            CreateBoolValue("running", true, server.running, true, nullptr),
            CreateStringValue("hostname", true, server.hostname, "Magic", nullptr),
            CreateEnumValue("socket_family", false, &family, server.family, 2, nullptr),
            CreateIntegerValue("port", true, 1024, 65535, server.port, 19530, nullptr),
            CreateFloatingValue("uptime", true, 0, 9999.0, server.uptime, 0, nullptr),
        };
    }

    void
    TearDown() override {
    }

 protected:
    void
    Init() {
        for (auto& config : config_list) {
            config->Init();
        }
    }

    void
    Load() {
        std::unordered_map<std::string, std::string> config_file{
            {"running", "false"},
        };

        for (auto& c : config_file) Set(c.first, c.second, false);
    }

    void
    Set(const std::string& name, const std::string& value, bool update = true) {
        for (auto& config : config_list) {
            if (std::strcmp(name.c_str(), config->name_) == 0) {
                config->Set(value, update);
                return;
            }
        }
        throw "Config " + name + " not found.";
    }

    std::string
    Get(const std::string& name) {
        for (auto& config : config_list) {
            if (std::strcmp(name.c_str(), config->name_) == 0) {
                return config->Get();
            }
        }
        throw "Config " + name + " not found.";
    }

    std::vector<BaseValuePtr> config_list;
};

}  // namespace milvus
