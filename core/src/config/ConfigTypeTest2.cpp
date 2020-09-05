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

#include "config/ServerConfig.h"
#include "gtest/gtest.h"

namespace milvus {

#define _MODIFIABLE (true)
#define _IMMUTABLE (false)

template <typename T>
class Utils {
 public:
    static bool
    valid_check_failure(const T& value, std::string& err) {
        err = "Value is invalid.";
        return false;
    }

    static bool
    update_failure(const T& value, const T& prev, std::string& err) {
        err = "Update is failure";
        return false;
    }

    static bool
    valid_check_raise_string(const T& value, std::string& err) {
        throw "string exception";
    }

    static bool
    valid_check_raise_exception(const T& value, std::string& err) {
        throw std::bad_alloc();
    }
};

/* BoolConfigTest */
class BoolConfigTest : public testing::Test, public Utils<bool> {};

TEST_F(BoolConfigTest, nullptr_init_test) {
    auto bool_config = CreateBoolConfig_("b", _MODIFIABLE, nullptr, true, nullptr, nullptr);
    ASSERT_DEATH(bool_config->Init(), "nullptr");
}

TEST_F(BoolConfigTest, init_twice_test) {
    bool bool_value;
    auto bool_config = CreateBoolConfig_("b", _MODIFIABLE, &bool_value, true, nullptr, nullptr);
    ASSERT_DEATH(
        {
            bool_config->Init();
            bool_config->Init();
        },
        "initialized");
}

TEST_F(BoolConfigTest, non_init_test) {
    bool bool_value;
    auto bool_config = CreateBoolConfig_("b", _MODIFIABLE, &bool_value, true, nullptr, nullptr);
    ASSERT_DEATH(bool_config->Set("false", true), "uninitialized");
    ASSERT_DEATH(bool_config->Get(), "uninitialized");
}

TEST_F(BoolConfigTest, immutable_update_test) {
    bool bool_value = false;
    auto bool_config = CreateBoolConfig_("b", _IMMUTABLE, &bool_value, true, nullptr, nullptr);
    bool_config->Init();
    ASSERT_EQ(bool_value, true);

    ConfigStatus status(SUCCESS, "");
    status = bool_config->Set("false", true);
    ASSERT_EQ(status.set_return, SetReturn::IMMUTABLE);
    ASSERT_EQ(bool_value, true);
}

TEST_F(BoolConfigTest, set_invalid_value_test) {
    bool bool_value;
    auto bool_config = CreateBoolConfig_("b", _MODIFIABLE, &bool_value, true, nullptr, nullptr);
    bool_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = bool_config->Set(" false", true);
    ASSERT_EQ(status.set_return, SetReturn::INVALID);
    ASSERT_EQ(bool_config->Get(), "true");

    status = bool_config->Set("false ", true);
    ASSERT_EQ(status.set_return, SetReturn::INVALID);
    ASSERT_EQ(bool_config->Get(), "true");

    status = bool_config->Set("afalse", true);
    ASSERT_EQ(status.set_return, SetReturn::INVALID);
    ASSERT_EQ(bool_config->Get(), "true");

    status = bool_config->Set("falsee", true);
    ASSERT_EQ(status.set_return, SetReturn::INVALID);
    ASSERT_EQ(bool_config->Get(), "true");

    status = bool_config->Set("abcdefg", true);
    ASSERT_EQ(status.set_return, SetReturn::INVALID);
    ASSERT_EQ(bool_config->Get(), "true");

    status = bool_config->Set("123456", true);
    ASSERT_EQ(status.set_return, SetReturn::INVALID);
    ASSERT_EQ(bool_config->Get(), "true");

    status = bool_config->Set("", true);
    ASSERT_EQ(status.set_return, SetReturn::INVALID);
    ASSERT_EQ(bool_config->Get(), "true");
}

TEST_F(BoolConfigTest, valid_check_fail_test) {
    bool bool_value;
    auto bool_config = CreateBoolConfig_("b", _MODIFIABLE, &bool_value, true, valid_check_failure, nullptr);
    bool_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = bool_config->Set("123456", true);
    ASSERT_EQ(status.set_return, SetReturn::INVALID);
    ASSERT_EQ(bool_config->Get(), "true");
}

TEST_F(BoolConfigTest, update_fail_test) {
    bool bool_value;
    auto bool_config = CreateBoolConfig_("b", _MODIFIABLE, &bool_value, true, nullptr, update_failure);
    bool_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = bool_config->Set("false", true);
    ASSERT_EQ(status.set_return, SetReturn::UPDATE_FAILURE);
    ASSERT_EQ(bool_config->Get(), "true");
}

TEST_F(BoolConfigTest, string_exception_test) {
    bool bool_value;
    auto bool_config = CreateBoolConfig_("b", _MODIFIABLE, &bool_value, true, valid_check_raise_string, nullptr);
    bool_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = bool_config->Set("false", true);
    ASSERT_EQ(status.set_return, SetReturn::UNEXPECTED);
    ASSERT_EQ(bool_config->Get(), "true");
}

TEST_F(BoolConfigTest, standard_exception_test) {
    bool bool_value;
    auto bool_config = CreateBoolConfig_("b", _MODIFIABLE, &bool_value, true, valid_check_raise_exception, nullptr);
    bool_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = bool_config->Set("false", true);
    ASSERT_EQ(status.set_return, SetReturn::EXCEPTION);
    ASSERT_EQ(bool_config->Get(), "true");
}

/* StringConfigTest */
class StringConfigTest : public testing::Test, public Utils<std::string> {};

TEST_F(StringConfigTest, nullptr_init_test) {
    auto string_config = CreateStringConfig_("s", true, nullptr, "Magic", nullptr, nullptr);
    ASSERT_DEATH(string_config->Init(), "nullptr");
}

TEST_F(StringConfigTest, init_twice_test) {
    std::string string_value;
    auto string_config = CreateStringConfig_("s", _MODIFIABLE, &string_value, "Magic", nullptr, nullptr);
    ASSERT_DEATH(
        {
            string_config->Init();
            string_config->Init();
        },
        "initialized");
}

TEST_F(StringConfigTest, non_init_test) {
    std::string string_value;
    auto string_config = CreateStringConfig_("s", _MODIFIABLE, &string_value, "Magic", nullptr, nullptr);
    ASSERT_DEATH(string_config->Set("value", true), "uninitialized");
    ASSERT_DEATH(string_config->Get(), "uninitialized");
}

TEST_F(StringConfigTest, immutable_update_test) {
    std::string string_value;
    auto string_config = CreateStringConfig_("s", _IMMUTABLE, &string_value, "Magic", nullptr, nullptr);
    string_config->Init();
    ASSERT_EQ(string_value, "Magic");

    ConfigStatus status(SUCCESS, "");
    status = string_config->Set("cigaM", true);
    ASSERT_EQ(status.set_return, SetReturn::IMMUTABLE);
    ASSERT_EQ(string_value, "Magic");
}

TEST_F(StringConfigTest, valid_check_fail_test) {
    std::string string_value;
    auto string_config = CreateStringConfig_("s", _MODIFIABLE, &string_value, "Magic", valid_check_failure, nullptr);
    string_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = string_config->Set("123456", true);
    ASSERT_EQ(status.set_return, SetReturn::INVALID);
    ASSERT_EQ(string_config->Get(), "Magic");
}

TEST_F(StringConfigTest, update_fail_test) {
    std::string string_value;
    auto string_config = CreateStringConfig_("s", _MODIFIABLE, &string_value, "Magic", nullptr, update_failure);
    string_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = string_config->Set("Mi", true);
    ASSERT_EQ(status.set_return, SetReturn::UPDATE_FAILURE);
    ASSERT_EQ(string_config->Get(), "Magic");
}

TEST_F(StringConfigTest, string_exception_test) {
    std::string string_value;
    auto string_config =
        CreateStringConfig_("s", _MODIFIABLE, &string_value, "Magic", valid_check_raise_string, nullptr);
    string_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = string_config->Set("any", true);
    ASSERT_EQ(status.set_return, SetReturn::UNEXPECTED);
    ASSERT_EQ(string_config->Get(), "Magic");
}

TEST_F(StringConfigTest, standard_exception_test) {
    std::string string_value;
    auto string_config =
        CreateStringConfig_("s", _MODIFIABLE, &string_value, "Magic", valid_check_raise_exception, nullptr);
    string_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = string_config->Set("any", true);
    ASSERT_EQ(status.set_return, SetReturn::EXCEPTION);
    ASSERT_EQ(string_config->Get(), "Magic");
}

/* IntegerConfigTest */
class IntegerConfigTest : public testing::Test, public Utils<int64_t> {};

TEST_F(IntegerConfigTest, nullptr_init_test) {
    auto integer_config = CreateIntegerConfig_("i", true, 1024, 65535, nullptr, 19530, nullptr, nullptr);
    ASSERT_DEATH(integer_config->Init(), "nullptr");
}

TEST_F(IntegerConfigTest, init_twice_test) {
    int64_t integer_value;
    auto integer_config = CreateIntegerConfig_("i", true, 1024, 65535, &integer_value, 19530, nullptr, nullptr);
    ASSERT_DEATH(
        {
            integer_config->Init();
            integer_config->Init();
        },
        "initialized");
}

TEST_F(IntegerConfigTest, non_init_test) {
    int64_t integer_value;
    auto integer_config = CreateIntegerConfig_("i", true, 1024, 65535, &integer_value, 19530, nullptr, nullptr);
    ASSERT_DEATH(integer_config->Set("42", true), "uninitialized");
    ASSERT_DEATH(integer_config->Get(), "uninitialized");
}

TEST_F(IntegerConfigTest, immutable_update_test) {
    int64_t integer_value;
    auto integer_config = CreateIntegerConfig_("i", _IMMUTABLE, 1024, 65535, &integer_value, 19530, nullptr, nullptr);
    integer_config->Init();
    ASSERT_EQ(integer_value, 19530);

    ConfigStatus status(SUCCESS, "");
    status = integer_config->Set("2048", true);
    ASSERT_EQ(status.set_return, SetReturn::IMMUTABLE);
    ASSERT_EQ(integer_value, 19530);
}

TEST_F(IntegerConfigTest, set_invalid_value_test) {
}

TEST_F(IntegerConfigTest, valid_check_fail_test) {
    int64_t integer_value;
    auto integer_config =
        CreateIntegerConfig_("i", true, 1024, 65535, &integer_value, 19530, valid_check_failure, nullptr);
    integer_config->Init();
    ConfigStatus status(SUCCESS, "");
    status = integer_config->Set("2048", true);
    ASSERT_EQ(status.set_return, SetReturn::INVALID);
    ASSERT_EQ(integer_config->Get(), "19530");
}

TEST_F(IntegerConfigTest, update_fail_test) {
    int64_t integer_value;
    auto integer_config = CreateIntegerConfig_("i", true, 1024, 65535, &integer_value, 19530, nullptr, update_failure);
    integer_config->Init();
    ConfigStatus status(SUCCESS, "");
    status = integer_config->Set("2048", true);
    ASSERT_EQ(status.set_return, SetReturn::UPDATE_FAILURE);
    ASSERT_EQ(integer_config->Get(), "19530");
}

TEST_F(IntegerConfigTest, string_exception_test) {
    int64_t integer_value;
    auto integer_config =
        CreateIntegerConfig_("i", true, 1024, 65535, &integer_value, 19530, valid_check_raise_string, nullptr);
    integer_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = integer_config->Set("2048", true);
    ASSERT_EQ(status.set_return, SetReturn::UNEXPECTED);
    ASSERT_EQ(integer_config->Get(), "19530");
}

TEST_F(IntegerConfigTest, standard_exception_test) {
    int64_t integer_value;
    auto integer_config =
        CreateIntegerConfig_("i", true, 1024, 65535, &integer_value, 19530, valid_check_raise_exception, nullptr);
    integer_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = integer_config->Set("2048", true);
    ASSERT_EQ(status.set_return, SetReturn::EXCEPTION);
    ASSERT_EQ(integer_config->Get(), "19530");
}

TEST_F(IntegerConfigTest, out_of_range_test) {
    int64_t integer_value;
    auto integer_config = CreateIntegerConfig_("i", true, 1024, 65535, &integer_value, 19530, nullptr, nullptr);
    integer_config->Init();

    {
        ConfigStatus status(SUCCESS, "");
        status = integer_config->Set("1023", true);
        ASSERT_EQ(status.set_return, SetReturn::OUT_OF_RANGE);
        ASSERT_EQ(integer_config->Get(), "19530");
    }

    {
        ConfigStatus status(SUCCESS, "");
        status = integer_config->Set("65536", true);
        ASSERT_EQ(status.set_return, SetReturn::OUT_OF_RANGE);
        ASSERT_EQ(integer_config->Get(), "19530");
    }
}

TEST_F(IntegerConfigTest, invalid_bound_test) {
    int64_t integer_value;
    auto integer_config = CreateIntegerConfig_("i", true, 100, 0, &integer_value, 50, nullptr, nullptr);
    integer_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = integer_config->Set("30", true);
    ASSERT_EQ(status.set_return, SetReturn::OUT_OF_RANGE);
    ASSERT_EQ(integer_config->Get(), "50");
}

TEST_F(IntegerConfigTest, invalid_format_test) {
    int64_t integer_value;
    auto integer_config = CreateIntegerConfig_("i", true, 0, 100, &integer_value, 50, nullptr, nullptr);
    integer_config->Init();

    {
        ConfigStatus status(SUCCESS, "");
        status = integer_config->Set("3-0", true);
        ASSERT_EQ(status.set_return, SetReturn::INVALID);
        ASSERT_EQ(integer_config->Get(), "50");
    }

    {
        ConfigStatus status(SUCCESS, "");
        status = integer_config->Set("30-", true);
        ASSERT_EQ(status.set_return, SetReturn::INVALID);
        ASSERT_EQ(integer_config->Get(), "50");
    }

    {
        ConfigStatus status(SUCCESS, "");
        status = integer_config->Set("+30", true);
        ASSERT_EQ(status.set_return, SetReturn::INVALID);
        ASSERT_EQ(integer_config->Get(), "50");
    }

    {
        ConfigStatus status(SUCCESS, "");
        status = integer_config->Set("a30", true);
        ASSERT_EQ(status.set_return, SetReturn::INVALID);
        ASSERT_EQ(integer_config->Get(), "50");
    }

    {
        ConfigStatus status(SUCCESS, "");
        status = integer_config->Set("30a", true);
        ASSERT_EQ(status.set_return, SetReturn::INVALID);
        ASSERT_EQ(integer_config->Get(), "50");
    }

    {
        ConfigStatus status(SUCCESS, "");
        status = integer_config->Set("3a0", true);
        ASSERT_EQ(status.set_return, SetReturn::INVALID);
        ASSERT_EQ(integer_config->Get(), "50");
    }
}

/* FloatingConfigTest */
class FloatingConfigTest : public testing::Test, public Utils<double> {};

TEST_F(FloatingConfigTest, nullptr_init_test) {
    auto floating_config = CreateFloatingConfig_("f", true, 1.0, 9.9, nullptr, 4.5, nullptr, nullptr);
    ASSERT_DEATH(floating_config->Init(), "nullptr");
}

TEST_F(FloatingConfigTest, init_twice_test) {
    double floating_value;
    auto floating_config = CreateFloatingConfig_("f", true, 1.0, 9.9, &floating_value, 4.5, nullptr, nullptr);
    ASSERT_DEATH(
        {
            floating_config->Init();
            floating_config->Init();
        },
        "initialized");
}

TEST_F(FloatingConfigTest, non_init_test) {
    double floating_value;
    auto floating_config = CreateFloatingConfig_("f", true, 1.0, 9.9, &floating_value, 4.5, nullptr, nullptr);
    ASSERT_DEATH(floating_config->Set("3.14", true), "uninitialized");
    ASSERT_DEATH(floating_config->Get(), "uninitialized");
}

TEST_F(FloatingConfigTest, immutable_update_test) {
    double floating_value;
    auto floating_config = CreateFloatingConfig_("f", _IMMUTABLE, 1.0, 9.9, &floating_value, 4.5, nullptr, nullptr);
    floating_config->Init();
    ASSERT_FLOAT_EQ(floating_value, 4.5);

    ConfigStatus status(SUCCESS, "");
    status = floating_config->Set("1.23", true);
    ASSERT_EQ(status.set_return, SetReturn::IMMUTABLE);
    ASSERT_FLOAT_EQ(std::stof(floating_config->Get()), 4.5);
}

TEST_F(FloatingConfigTest, set_invalid_value_test) {
}

TEST_F(FloatingConfigTest, valid_check_fail_test) {
    double floating_value;
    auto floating_config =
        CreateFloatingConfig_("f", true, 1.0, 9.9, &floating_value, 4.5, valid_check_failure, nullptr);
    floating_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = floating_config->Set("1.23", true);
    ASSERT_EQ(status.set_return, SetReturn::INVALID);
    ASSERT_FLOAT_EQ(std::stof(floating_config->Get()), 4.5);
}

TEST_F(FloatingConfigTest, update_fail_test) {
    double floating_value;
    auto floating_config = CreateFloatingConfig_("f", true, 1.0, 9.9, &floating_value, 4.5, nullptr, update_failure);
    floating_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = floating_config->Set("1.23", true);
    ASSERT_EQ(status.set_return, SetReturn::UPDATE_FAILURE);
    ASSERT_FLOAT_EQ(std::stof(floating_config->Get()), 4.5);
}

TEST_F(FloatingConfigTest, string_exception_test) {
    double floating_value;
    auto floating_config =
        CreateFloatingConfig_("f", true, 1.0, 9.9, &floating_value, 4.5, valid_check_raise_string, nullptr);
    floating_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = floating_config->Set("1.23", true);
    ASSERT_EQ(status.set_return, SetReturn::UNEXPECTED);
    ASSERT_FLOAT_EQ(std::stof(floating_config->Get()), 4.5);
}

TEST_F(FloatingConfigTest, standard_exception_test) {
    double floating_value;
    auto floating_config =
        CreateFloatingConfig_("f", true, 1.0, 9.9, &floating_value, 4.5, valid_check_raise_exception, nullptr);
    floating_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = floating_config->Set("1.23", true);
    ASSERT_EQ(status.set_return, SetReturn::EXCEPTION);
    ASSERT_FLOAT_EQ(std::stof(floating_config->Get()), 4.5);
}

TEST_F(FloatingConfigTest, out_of_range_test) {
    double floating_value;
    auto floating_config =
        CreateFloatingConfig_("f", true, 1.0, 9.9, &floating_value, 4.5, valid_check_raise_exception, nullptr);
    floating_config->Init();

    {
        ConfigStatus status(SUCCESS, "");
        status = floating_config->Set("0.99", true);
        ASSERT_EQ(status.set_return, SetReturn::OUT_OF_RANGE);
        ASSERT_FLOAT_EQ(std::stof(floating_config->Get()), 4.5);
    }

    {
        ConfigStatus status(SUCCESS, "");
        status = floating_config->Set("10.00", true);
        ASSERT_EQ(status.set_return, SetReturn::OUT_OF_RANGE);
        ASSERT_FLOAT_EQ(std::stof(floating_config->Get()), 4.5);
    }
}

TEST_F(FloatingConfigTest, invalid_bound_test) {
    double floating_value;
    auto floating_config =
        CreateFloatingConfig_("f", true, 9.9, 1.0, &floating_value, 4.5, valid_check_raise_exception, nullptr);
    floating_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = floating_config->Set("6.0", true);
    ASSERT_EQ(status.set_return, SetReturn::OUT_OF_RANGE);
    ASSERT_FLOAT_EQ(std::stof(floating_config->Get()), 4.5);
}

TEST_F(FloatingConfigTest, DISABLED_invalid_format_test) {
    double floating_value;
    auto floating_config = CreateFloatingConfig_("f", true, 1.0, 100.0, &floating_value, 4.5, nullptr, nullptr);
    floating_config->Init();

    {
        ConfigStatus status(SUCCESS, "");
        status = floating_config->Set("6.0.1", true);
        ASSERT_EQ(status.set_return, SetReturn::INVALID);
        ASSERT_FLOAT_EQ(std::stof(floating_config->Get()), 4.5);
    }

    {
        ConfigStatus status(SUCCESS, "");
        status = floating_config->Set("6a0", true);
        ASSERT_EQ(status.set_return, SetReturn::INVALID);
        ASSERT_FLOAT_EQ(std::stof(floating_config->Get()), 4.5);
    }
}

/* EnumConfigTest */
class EnumConfigTest : public testing::Test, public Utils<int64_t> {};

TEST_F(EnumConfigTest, nullptr_init_test) {
    configEnum testEnum{
        {"e", 1},
    };
    int64_t testEnumValue;
    auto enum_config_1 = CreateEnumConfig_("e", _MODIFIABLE, &testEnum, nullptr, 2, nullptr, nullptr);
    ASSERT_DEATH(enum_config_1->Init(), "nullptr");

    auto enum_config_2 = CreateEnumConfig_("e", _MODIFIABLE, nullptr, &testEnumValue, 2, nullptr, nullptr);
    ASSERT_DEATH(enum_config_2->Init(), "nullptr");

    auto enum_config_3 = CreateEnumConfig_("e", _MODIFIABLE, nullptr, nullptr, 2, nullptr, nullptr);
    ASSERT_DEATH(enum_config_3->Init(), "nullptr");
}

TEST_F(EnumConfigTest, init_twice_test) {
    configEnum testEnum{
        {"e", 1},
    };
    int64_t enum_value;
    auto enum_config = CreateEnumConfig_("e", _MODIFIABLE, &testEnum, &enum_value, 2, nullptr, nullptr);
    ASSERT_DEATH(
        {
            enum_config->Init();
            enum_config->Init();
        },
        "initialized");
}

TEST_F(EnumConfigTest, non_init_test) {
    configEnum testEnum{
        {"e", 1},
    };
    int64_t enum_value;
    auto enum_config = CreateEnumConfig_("e", _MODIFIABLE, &testEnum, &enum_value, 2, nullptr, nullptr);
    ASSERT_DEATH(enum_config->Set("e", true), "uninitialized");
    ASSERT_DEATH(enum_config->Get(), "uninitialized");
}

TEST_F(EnumConfigTest, immutable_update_test) {
    configEnum testEnum{
        {"a", 1},
        {"b", 2},
        {"c", 3},
    };
    int64_t enum_value = 0;
    auto enum_config = CreateEnumConfig_("e", _IMMUTABLE, &testEnum, &enum_value, 1, nullptr, nullptr);
    enum_config->Init();
    ASSERT_EQ(enum_value, 1);

    ConfigStatus status(SUCCESS, "");
    status = enum_config->Set("b", true);
    ASSERT_EQ(status.set_return, SetReturn::IMMUTABLE);
    ASSERT_EQ(enum_value, 1);
}

TEST_F(EnumConfigTest, set_invalid_value_check) {
    configEnum testEnum{
        {"a", 1},
    };
    int64_t enum_value = 0;
    auto enum_config = CreateEnumConfig_("e", _MODIFIABLE, &testEnum, &enum_value, 1, nullptr, nullptr);
    enum_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = enum_config->Set("b", true);
    ASSERT_EQ(status.set_return, SetReturn::ENUM_VALUE_NOTFOUND);
    ASSERT_EQ(enum_config->Get(), "a");
}

TEST_F(EnumConfigTest, empty_enum_test) {
    configEnum testEnum{};
    int64_t enum_value;
    auto enum_config = CreateEnumConfig_("e", _MODIFIABLE, &testEnum, &enum_value, 2, nullptr, nullptr);
    ASSERT_DEATH(enum_config->Init(), "empty");
}

TEST_F(EnumConfigTest, valid_check_fail_test) {
    configEnum testEnum{
        {"a", 1},
        {"b", 2},
        {"c", 3},
    };
    int64_t enum_value;
    auto enum_config = CreateEnumConfig_("e", _MODIFIABLE, &testEnum, &enum_value, 1, valid_check_failure, nullptr);
    enum_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = enum_config->Set("b", true);
    ASSERT_EQ(status.set_return, SetReturn::INVALID);
    ASSERT_EQ(enum_config->Get(), "a");
}

TEST_F(EnumConfigTest, update_fail_test) {
    configEnum testEnum{
        {"a", 1},
        {"b", 2},
        {"c", 3},
    };
    int64_t enum_value;
    auto enum_config = CreateEnumConfig_("e", _MODIFIABLE, &testEnum, &enum_value, 1, nullptr, update_failure);
    enum_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = enum_config->Set("b", true);
    ASSERT_EQ(status.set_return, SetReturn::UPDATE_FAILURE);
    ASSERT_EQ(enum_config->Get(), "a");
}

TEST_F(EnumConfigTest, string_exception_test) {
    configEnum testEnum{
        {"a", 1},
        {"b", 2},
        {"c", 3},
    };
    int64_t enum_value;
    auto enum_config =
        CreateEnumConfig_("e", _MODIFIABLE, &testEnum, &enum_value, 1, valid_check_raise_string, nullptr);
    enum_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = enum_config->Set("b", true);
    ASSERT_EQ(status.set_return, SetReturn::UNEXPECTED);
    ASSERT_EQ(enum_config->Get(), "a");
}

TEST_F(EnumConfigTest, standard_exception_test) {
    configEnum testEnum{
        {"a", 1},
        {"b", 2},
        {"c", 3},
    };
    int64_t enum_value;
    auto enum_config =
        CreateEnumConfig_("e", _MODIFIABLE, &testEnum, &enum_value, 1, valid_check_raise_exception, nullptr);
    enum_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = enum_config->Set("b", true);
    ASSERT_EQ(status.set_return, SetReturn::EXCEPTION);
    ASSERT_EQ(enum_config->Get(), "a");
}

/* SizeConfigTest */
class SizeConfigTest : public testing::Test, public Utils<int64_t> {};

TEST_F(SizeConfigTest, nullptr_init_test) {
    auto size_config = CreateSizeConfig_("i", true, 1024, 4096, nullptr, 2048, nullptr, nullptr);
    ASSERT_DEATH(size_config->Init(), "nullptr");
}

TEST_F(SizeConfigTest, init_twice_test) {
    int64_t size_value;
    auto size_config = CreateSizeConfig_("i", true, 1024, 4096, &size_value, 2048, nullptr, nullptr);
    ASSERT_DEATH(
        {
            size_config->Init();
            size_config->Init();
        },
        "initialized");
}

TEST_F(SizeConfigTest, non_init_test) {
    int64_t size_value;
    auto size_config = CreateSizeConfig_("i", true, 1024, 4096, &size_value, 2048, nullptr, nullptr);
    ASSERT_DEATH(size_config->Set("3000", true), "uninitialized");
    ASSERT_DEATH(size_config->Get(), "uninitialized");
}

TEST_F(SizeConfigTest, immutable_update_test) {
    int64_t size_value = 0;
    auto size_config = CreateSizeConfig_("i", _IMMUTABLE, 1024, 4096, &size_value, 2048, nullptr, nullptr);
    size_config->Init();
    ASSERT_EQ(size_value, 2048);

    ConfigStatus status(SUCCESS, "");
    status = size_config->Set("3000", true);
    ASSERT_EQ(status.set_return, SetReturn::IMMUTABLE);
    ASSERT_EQ(size_value, 2048);
}

TEST_F(SizeConfigTest, set_invalid_value_test) {
}

TEST_F(SizeConfigTest, valid_check_fail_test) {
    int64_t size_value;
    auto size_config = CreateSizeConfig_("i", true, 1024, 4096, &size_value, 2048, valid_check_failure, nullptr);
    size_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = size_config->Set("3000", true);
    ASSERT_EQ(status.set_return, SetReturn::INVALID);
    ASSERT_EQ(size_config->Get(), "2048");
}

TEST_F(SizeConfigTest, update_fail_test) {
    int64_t size_value;
    auto size_config = CreateSizeConfig_("i", true, 1024, 4096, &size_value, 2048, nullptr, update_failure);
    size_config->Init();
    ConfigStatus status(SUCCESS, "");
    status = size_config->Set("3000", true);
    ASSERT_EQ(status.set_return, SetReturn::UPDATE_FAILURE);
    ASSERT_EQ(size_config->Get(), "2048");
}

TEST_F(SizeConfigTest, string_exception_test) {
    int64_t size_value;
    auto size_config = CreateSizeConfig_("i", true, 1024, 4096, &size_value, 2048, valid_check_raise_string, nullptr);
    size_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = size_config->Set("3000", true);
    ASSERT_EQ(status.set_return, SetReturn::UNEXPECTED);
    ASSERT_EQ(size_config->Get(), "2048");
}

TEST_F(SizeConfigTest, standard_exception_test) {
    int64_t size_value;
    auto size_config =
        CreateSizeConfig_("i", true, 1024, 4096, &size_value, 2048, valid_check_raise_exception, nullptr);
    size_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = size_config->Set("3000", true);
    ASSERT_EQ(status.set_return, SetReturn::EXCEPTION);
    ASSERT_EQ(size_config->Get(), "2048");
}

TEST_F(SizeConfigTest, out_of_range_test) {
    int64_t size_value;
    auto size_config = CreateSizeConfig_("i", true, 1024, 4096, &size_value, 2048, nullptr, nullptr);
    size_config->Init();

    {
        ConfigStatus status(SUCCESS, "");
        status = size_config->Set("1023", true);
        ASSERT_EQ(status.set_return, SetReturn::OUT_OF_RANGE);
        ASSERT_EQ(size_config->Get(), "2048");
    }

    {
        ConfigStatus status(SUCCESS, "");
        status = size_config->Set("4097", true);
        ASSERT_EQ(status.set_return, SetReturn::OUT_OF_RANGE);
        ASSERT_EQ(size_config->Get(), "2048");
    }
}

TEST_F(SizeConfigTest, negative_integer_test) {
    int64_t size_value;
    auto size_config = CreateSizeConfig_("i", true, 1024, 4096, &size_value, 2048, nullptr, nullptr);
    size_config->Init();
    ConfigStatus status(SUCCESS, "");
    status = size_config->Set("-3KB", true);
    ASSERT_EQ(status.set_return, SetReturn::INVALID);
    ASSERT_EQ(size_config->Get(), "2048");
}

TEST_F(SizeConfigTest, invalid_bound_test) {
    int64_t size_value;
    auto size_config = CreateSizeConfig_("i", true, 100, 0, &size_value, 50, nullptr, nullptr);
    size_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = size_config->Set("30", true);
    ASSERT_EQ(status.set_return, SetReturn::OUT_OF_RANGE);
    ASSERT_EQ(size_config->Get(), "50");
}

TEST_F(SizeConfigTest, invalid_unit_test) {
    int64_t size_value;
    auto size_config = CreateSizeConfig_("i", true, 1024, 4096, &size_value, 2048, nullptr, nullptr);
    size_config->Init();

    ConfigStatus status(SUCCESS, "");
    status = size_config->Set("1 TB", true);
    ASSERT_EQ(status.set_return, SetReturn::INVALID);
    ASSERT_EQ(size_config->Get(), "2048");
}

TEST_F(SizeConfigTest, invalid_format_test) {
    int64_t size_value;
    auto size_config = CreateSizeConfig_("i", true, 1024, 4096, &size_value, 2048, nullptr, nullptr);
    size_config->Init();

    {
        ConfigStatus status(SUCCESS, "");
        status = size_config->Set("a10GB", true);
        ASSERT_EQ(status.set_return, SetReturn::INVALID);
        ASSERT_EQ(size_config->Get(), "2048");
    }

    {
        ConfigStatus status(SUCCESS, "");
        status = size_config->Set("200*0", true);
        ASSERT_EQ(status.set_return, SetReturn::INVALID);
        ASSERT_EQ(size_config->Get(), "2048");
    }

    {
        ConfigStatus status(SUCCESS, "");
        status = size_config->Set("10AB", true);
        ASSERT_EQ(status.set_return, SetReturn::INVALID);
        ASSERT_EQ(size_config->Get(), "2048");
    }
}

}  // namespace milvus
