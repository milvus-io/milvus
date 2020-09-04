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

#include "config/ServerConfig.h"
#include "gtest/gtest.h"

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

    bool
    update_fn(const T& value, const T& prev, std::string& err) {
        new_value = value;
        prev_value = prev;
        return true;
    }

 protected:
    T validate_value;
    T new_value;
    T prev_value;
};

/* ValidBoolConfigTest */
class ValidBoolConfigTest : public testing::Test, public Utils<bool> {
 protected:
};

TEST_F(ValidBoolConfigTest, init_load_update_get_test) {
    auto validate = std::bind(&ValidBoolConfigTest::validate_fn, this, std::placeholders::_1, std::placeholders::_2);
    auto update = std::bind(&ValidBoolConfigTest::update_fn, this, std::placeholders::_1, std::placeholders::_2,
                            std::placeholders::_3);

    bool bool_value = true;
    auto bool_config = CreateBoolConfig_("b", _MODIFIABLE, &bool_value, false, validate, update);
    ASSERT_EQ(bool_value, true);
    ASSERT_EQ(bool_config->modifiable_, true);

    bool_config->Init();
    ASSERT_EQ(bool_value, false);
    ASSERT_EQ(bool_config->Get(), "false");

    {
        // now `bool_value` is `false`, calling Set(update=false) to set it to `true`, but not notify update_fn()
        validate_value = false;
        new_value = false;
        prev_value = true;

        ConfigStatus status(SetReturn::SUCCESS, "");
        status = bool_config->Set("true", false);

        EXPECT_EQ(status.set_return, SetReturn::SUCCESS);
        EXPECT_EQ(bool_value, true);
        EXPECT_EQ(bool_config->Get(), "true");

        // expect change
        EXPECT_EQ(validate_value, true);
        // expect not change
        EXPECT_EQ(new_value, false);
        EXPECT_EQ(prev_value, true);
    }

    {
        // now `bool_value` is `true`, calling Set(update=true) to set it to `false`, will notify update_fn()
        validate_value = true;
        new_value = true;
        prev_value = false;

        ConfigStatus status(SetReturn::SUCCESS, "");
        status = bool_config->Set("false", true);

        EXPECT_EQ(status.set_return, SetReturn::SUCCESS);
        EXPECT_EQ(bool_value, false);
        EXPECT_EQ(bool_config->Get(), "false");

        // expect change
        EXPECT_EQ(validate_value, false);
        EXPECT_EQ(new_value, false);
        EXPECT_EQ(prev_value, true);
    }
}

/* ValidStringConfigTest */
class ValidStringConfigTest : public testing::Test, public Utils<std::string> {
 protected:
};

TEST_F(ValidStringConfigTest, init_load_update_get_test) {
    auto validate = std::bind(&ValidStringConfigTest::validate_fn, this, std::placeholders::_1, std::placeholders::_2);
    auto update = std::bind(&ValidStringConfigTest::update_fn, this, std::placeholders::_1, std::placeholders::_2,
                            std::placeholders::_3);

    std::string string_value;
    auto string_config = CreateStringConfig_("s", _MODIFIABLE, &string_value, "Magic", validate, update);
    ASSERT_EQ(string_value, "");
    ASSERT_EQ(string_config->modifiable_, true);

    string_config->Init();
    ASSERT_EQ(string_value, "Magic");
    ASSERT_EQ(string_config->Get(), "Magic");

    {
        // now `string_value` is `Magic`, calling Set(update=false) to set it to `cigaM`, but not notify update_fn()
        validate_value = "";
        new_value = "";
        prev_value = "";

        ConfigStatus status(SetReturn::SUCCESS, "");
        status = string_config->Set("cigaM", false);

        EXPECT_EQ(status.set_return, SetReturn::SUCCESS);
        EXPECT_EQ(string_value, "cigaM");
        EXPECT_EQ(string_config->Get(), "cigaM");

        // expect change
        EXPECT_EQ(validate_value, "cigaM");
        // expect not change
        EXPECT_EQ(new_value, "");
        EXPECT_EQ(prev_value, "");
    }

    {
        // now `string_value` is `cigaM`, calling Set(update=true) to set it to `Check`, will notify update_fn()
        validate_value = "";
        new_value = "";
        prev_value = "";

        ConfigStatus status(SetReturn::SUCCESS, "");
        status = string_config->Set("Check", true);

        EXPECT_EQ(status.set_return, SetReturn::SUCCESS);
        EXPECT_EQ(string_value, "Check");
        EXPECT_EQ(string_config->Get(), "Check");

        // expect change
        EXPECT_EQ(validate_value, "Check");
        EXPECT_EQ(new_value, "Check");
        EXPECT_EQ(prev_value, "cigaM");
    }
}

/* ValidIntegerConfigTest */
class ValidIntegerConfigTest : public testing::Test, public Utils<int64_t> {
 protected:
};

TEST_F(ValidIntegerConfigTest, init_load_update_get_test) {
    auto validate = std::bind(&ValidIntegerConfigTest::validate_fn, this, std::placeholders::_1, std::placeholders::_2);
    auto update = std::bind(&ValidIntegerConfigTest::update_fn, this, std::placeholders::_1, std::placeholders::_2,
                            std::placeholders::_3);

    int64_t integer_value = 0;
    auto integer_config = CreateIntegerConfig_("i", _MODIFIABLE, -100, 100, &integer_value, 42, validate, update);
    ASSERT_EQ(integer_value, 0);
    ASSERT_EQ(integer_config->modifiable_, true);

    integer_config->Init();
    ASSERT_EQ(integer_value, 42);
    ASSERT_EQ(integer_config->Get(), "42");

    {
        // now `integer_value` is `42`, calling Set(update=false) to set it to `24`, but not notify update_fn()
        validate_value = 0;
        new_value = 0;
        prev_value = 0;

        ConfigStatus status(SetReturn::SUCCESS, "");
        status = integer_config->Set("24", false);

        EXPECT_EQ(status.set_return, SetReturn::SUCCESS);
        EXPECT_EQ(integer_value, 24);
        EXPECT_EQ(integer_config->Get(), "24");

        // expect change
        EXPECT_EQ(validate_value, 24);
        // expect not change
        EXPECT_EQ(new_value, 0);
        EXPECT_EQ(prev_value, 0);
    }

    {
        // now `integer_value` is `24`, calling Set(update=true) to set it to `36`, will notify update_fn()
        validate_value = 0;
        new_value = 0;
        prev_value = 0;

        ConfigStatus status(SetReturn::SUCCESS, "");
        status = integer_config->Set("36", true);

        EXPECT_EQ(status.set_return, SetReturn::SUCCESS);
        EXPECT_EQ(integer_value, 36);
        EXPECT_EQ(integer_config->Get(), "36");

        // expect change
        EXPECT_EQ(validate_value, 36);
        EXPECT_EQ(new_value, 36);
        EXPECT_EQ(prev_value, 24);
    }
}

/* ValidFloatingConfigTest */
class ValidFloatingConfigTest : public testing::Test, public Utils<double> {
 protected:
};

TEST_F(ValidFloatingConfigTest, init_load_update_get_test) {
    auto validate =
        std::bind(&ValidFloatingConfigTest::validate_fn, this, std::placeholders::_1, std::placeholders::_2);
    auto update = std::bind(&ValidFloatingConfigTest::update_fn, this, std::placeholders::_1, std::placeholders::_2,
                            std::placeholders::_3);

    double floating_value = 0.0;
    auto floating_config =
        CreateFloatingConfig_("f", _MODIFIABLE, -10.0, 10.0, &floating_value, 3.14, validate, update);
    ASSERT_FLOAT_EQ(floating_value, 0.0);
    ASSERT_EQ(floating_config->modifiable_, true);

    floating_config->Init();
    ASSERT_FLOAT_EQ(floating_value, 3.14);
    ASSERT_FLOAT_EQ(std::stof(floating_config->Get()), 3.14);

    {
        // now `floating_value` is `3.14`, calling Set(update=false) to set it to `6.22`, but not notify update_fn()
        validate_value = 0.0;
        new_value = 0.0;
        prev_value = 0.0;

        ConfigStatus status(SetReturn::SUCCESS, "");
        status = floating_config->Set("6.22", false);

        EXPECT_EQ(status.set_return, SetReturn::SUCCESS);
        ASSERT_FLOAT_EQ(floating_value, 6.22);
        ASSERT_FLOAT_EQ(std::stof(floating_config->Get()), 6.22);

        // expect change
        ASSERT_FLOAT_EQ(validate_value, 6.22);
        // expect not change
        ASSERT_FLOAT_EQ(new_value, 0.0);
        ASSERT_FLOAT_EQ(prev_value, 0.0);
    }

    {
        // now `integer_value` is `6.22`, calling Set(update=true) to set it to `-3.14`, will notify update_fn()
        validate_value = 0.0;
        new_value = 0.0;
        prev_value = 0.0;

        ConfigStatus status(SetReturn::SUCCESS, "");
        status = floating_config->Set("-3.14", true);

        EXPECT_EQ(status.set_return, SetReturn::SUCCESS);
        ASSERT_FLOAT_EQ(floating_value, -3.14);
        ASSERT_FLOAT_EQ(std::stof(floating_config->Get()), -3.14);

        // expect change
        ASSERT_FLOAT_EQ(validate_value, -3.14);
        ASSERT_FLOAT_EQ(new_value, -3.14);
        ASSERT_FLOAT_EQ(prev_value, 6.22);
    }
}

/* ValidEnumConfigTest */
class ValidEnumConfigTest : public testing::Test, public Utils<int64_t> {
 protected:
};

// template <>
// int64_t Utils<int64_t>::validate_value = 0;
// template <>
// int64_t Utils<int64_t>::new_value = 0;
// template <>
// int64_t Utils<int64_t>::prev_value = 0;

TEST_F(ValidEnumConfigTest, init_load_update_get_test) {
    auto validate = std::bind(&ValidEnumConfigTest::validate_fn, this, std::placeholders::_1, std::placeholders::_2);
    auto update = std::bind(&ValidEnumConfigTest::update_fn, this, std::placeholders::_1, std::placeholders::_2,
                            std::placeholders::_3);

    configEnum testEnum{
        {"a", 1},
        {"b", 2},
        {"c", 3},
    };
    int64_t enum_value = 0;
    auto enum_config = CreateEnumConfig_("e", _MODIFIABLE, &testEnum, &enum_value, 1, validate, update);
    ASSERT_EQ(enum_value, 0);
    ASSERT_EQ(enum_config->modifiable_, true);

    enum_config->Init();
    ASSERT_EQ(enum_value, 1);
    ASSERT_EQ(enum_config->Get(), "a");

    {
        // now `enum_value` is `a`, calling Set(update=false) to set it to `b`, but not notify update_fn()
        validate_value = 0;
        new_value = 0;
        prev_value = 0;

        ConfigStatus status(SetReturn::SUCCESS, "");
        status = enum_config->Set("b", false);

        EXPECT_EQ(status.set_return, SetReturn::SUCCESS);
        ASSERT_EQ(enum_value, 2);
        ASSERT_EQ(enum_config->Get(), "b");

        // expect change
        ASSERT_EQ(validate_value, 2);
        // expect not change
        ASSERT_EQ(new_value, 0);
        ASSERT_EQ(prev_value, 0);
    }

    {
        // now `enum_value` is `b`, calling Set(update=true) to set it to `c`, will notify update_fn()
        validate_value = 0;
        new_value = 0;
        prev_value = 0;

        ConfigStatus status(SetReturn::SUCCESS, "");
        status = enum_config->Set("c", true);

        EXPECT_EQ(status.set_return, SetReturn::SUCCESS);
        ASSERT_EQ(enum_value, 3);
        ASSERT_EQ(enum_config->Get(), "c");

        // expect change
        ASSERT_EQ(validate_value, 3);
        ASSERT_EQ(new_value, 3);
        ASSERT_EQ(prev_value, 2);
    }
}

/* ValidSizeConfigTest */
class ValidSizeConfigTest : public testing::Test, public Utils<int64_t> {
 protected:
};

// template <>
// int64_t Utils<int64_t>::validate_value = 0;
// template <>
// int64_t Utils<int64_t>::new_value = 0;
// template <>
// int64_t Utils<int64_t>::prev_value = 0;

TEST_F(ValidSizeConfigTest, init_load_update_get_test) {
    auto validate = std::bind(&ValidSizeConfigTest::validate_fn, this, std::placeholders::_1, std::placeholders::_2);
    auto update = std::bind(&ValidSizeConfigTest::update_fn, this, std::placeholders::_1, std::placeholders::_2,
                            std::placeholders::_3);

    int64_t size_value = 0;
    auto size_config = CreateSizeConfig_("i", _MODIFIABLE, 0, 1024 * 1024, &size_value, 1024, validate, update);
    ASSERT_EQ(size_value, 0);
    ASSERT_EQ(size_config->modifiable_, true);

    size_config->Init();
    ASSERT_EQ(size_value, 1024);
    ASSERT_EQ(size_config->Get(), "1024");

    {
        // now `size_value` is `1024`, calling Set(update=false) to set it to `4096`, but not notify update_fn()
        validate_value = 0;
        new_value = 0;
        prev_value = 0;

        ConfigStatus status(SetReturn::SUCCESS, "");
        status = size_config->Set("4096", false);

        EXPECT_EQ(status.set_return, SetReturn::SUCCESS);
        EXPECT_EQ(size_value, 4096);
        EXPECT_EQ(size_config->Get(), "4096");

        // expect change
        EXPECT_EQ(validate_value, 4096);
        // expect not change
        EXPECT_EQ(new_value, 0);
        EXPECT_EQ(prev_value, 0);
    }

    {
        // now `size_value` is `4096`, calling Set(update=true) to set it to `256kb`, will notify update_fn()
        validate_value = 0;
        new_value = 0;
        prev_value = 0;

        ConfigStatus status(SetReturn::SUCCESS, "");
        status = size_config->Set("256kb", true);

        EXPECT_EQ(status.set_return, SetReturn::SUCCESS);
        EXPECT_EQ(size_value, 256 * 1024);
        EXPECT_EQ(size_config->Get(), "262144");

        // expect change
        EXPECT_EQ(validate_value, 262144);
        EXPECT_EQ(new_value, 262144);
        EXPECT_EQ(prev_value, 4096);
    }
}

class ValidTest : public testing::Test {
 protected:
    configEnum family{
        {"ipv4", 1},
        {"ipv6", 2},
    };

    struct Server {
        bool running = true;
        std::string hostname;
        int64_t family = 0;
        int64_t port = 0;
        double uptime = 0;
    };

    Server server;

 protected:
    void
    SetUp() override {
        config_list = {
            CreateBoolConfig_("running", true, &server.running, true, nullptr, nullptr),
            CreateStringConfig_("hostname", true, &server.hostname, "Magic", nullptr, nullptr),
            CreateEnumConfig_("socket_family", false, &family, &server.family, 2, nullptr, nullptr),
            CreateIntegerConfig_("port", true, 1024, 65535, &server.port, 19530, nullptr, nullptr),
            CreateFloatingConfig_("uptime", true, 0, 9999.0, &server.uptime, 0, nullptr, nullptr),
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

    std::vector<BaseConfigPtr> config_list;
};

}  // namespace milvus
