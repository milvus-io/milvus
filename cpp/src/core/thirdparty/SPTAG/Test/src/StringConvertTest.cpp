// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Test.h"
#include "inc/Helper/StringConvert.h"

namespace
{
    namespace Local
    {

        template <typename ValueType>
        void TestConvertSuccCase(ValueType p_val, const char* p_valStr)
        {
            using namespace SPTAG::Helper::Convert;

            std::string str = ConvertToString<ValueType>(p_val);
            if (nullptr != p_valStr)
            {
                BOOST_CHECK(str == p_valStr);
            }

            ValueType val;
            BOOST_CHECK(ConvertStringTo<ValueType>(str.c_str(), val));
            BOOST_CHECK(val == p_val);
        }

    }
}

BOOST_AUTO_TEST_SUITE(StringConvertTest)

BOOST_AUTO_TEST_CASE(ConvertInt8)
{
    Local::TestConvertSuccCase<int8_t>(static_cast<int8_t>(-1), "-1");
    Local::TestConvertSuccCase<int8_t>(static_cast<int8_t>(0), "0");
    Local::TestConvertSuccCase<int8_t>(static_cast<int8_t>(3), "3");
    Local::TestConvertSuccCase<int8_t>(static_cast<int8_t>(100), "100");
}

BOOST_AUTO_TEST_CASE(ConvertInt16)
{
    Local::TestConvertSuccCase<int16_t>(static_cast<int16_t>(-1), "-1");
    Local::TestConvertSuccCase<int16_t>(static_cast<int16_t>(0), "0");
    Local::TestConvertSuccCase<int16_t>(static_cast<int16_t>(3), "3");
    Local::TestConvertSuccCase<int16_t>(static_cast<int16_t>(100), "100");
}

BOOST_AUTO_TEST_CASE(ConvertInt32)
{
    Local::TestConvertSuccCase<int32_t>(static_cast<int32_t>(-1), "-1");
    Local::TestConvertSuccCase<int32_t>(static_cast<int32_t>(0), "0");
    Local::TestConvertSuccCase<int32_t>(static_cast<int32_t>(3), "3");
    Local::TestConvertSuccCase<int32_t>(static_cast<int32_t>(100), "100");
}

BOOST_AUTO_TEST_CASE(ConvertInt64)
{
    Local::TestConvertSuccCase<int64_t>(static_cast<int64_t>(-1), "-1");
    Local::TestConvertSuccCase<int64_t>(static_cast<int64_t>(0), "0");
    Local::TestConvertSuccCase<int64_t>(static_cast<int64_t>(3), "3");
    Local::TestConvertSuccCase<int64_t>(static_cast<int64_t>(100), "100");
}

BOOST_AUTO_TEST_CASE(ConvertUInt8)
{
    Local::TestConvertSuccCase<uint8_t>(static_cast<uint8_t>(0), "0");
    Local::TestConvertSuccCase<uint8_t>(static_cast<uint8_t>(3), "3");
    Local::TestConvertSuccCase<uint8_t>(static_cast<uint8_t>(100), "100");
}

BOOST_AUTO_TEST_CASE(ConvertUInt16)
{
    Local::TestConvertSuccCase<uint16_t>(static_cast<uint16_t>(0), "0");
    Local::TestConvertSuccCase<uint16_t>(static_cast<uint16_t>(3), "3");
    Local::TestConvertSuccCase<uint16_t>(static_cast<uint16_t>(100), "100");
}

BOOST_AUTO_TEST_CASE(ConvertUInt32)
{
    Local::TestConvertSuccCase<uint32_t>(static_cast<uint32_t>(0), "0");
    Local::TestConvertSuccCase<uint32_t>(static_cast<uint32_t>(3), "3");
    Local::TestConvertSuccCase<uint32_t>(static_cast<uint32_t>(100), "100");
}

BOOST_AUTO_TEST_CASE(ConvertUInt64)
{
    Local::TestConvertSuccCase<uint64_t>(static_cast<uint64_t>(0), "0");
    Local::TestConvertSuccCase<uint64_t>(static_cast<uint64_t>(3), "3");
    Local::TestConvertSuccCase<uint64_t>(static_cast<uint64_t>(100), "100");
}

BOOST_AUTO_TEST_CASE(ConvertFloat)
{
    Local::TestConvertSuccCase<float>(static_cast<float>(-1), nullptr);
    Local::TestConvertSuccCase<float>(static_cast<float>(0), nullptr);
    Local::TestConvertSuccCase<float>(static_cast<float>(3), nullptr);
    Local::TestConvertSuccCase<float>(static_cast<float>(100), nullptr);
}

BOOST_AUTO_TEST_CASE(ConvertDouble)
{
    Local::TestConvertSuccCase<double>(static_cast<double>(-1), nullptr);
    Local::TestConvertSuccCase<double>(static_cast<double>(0), nullptr);
    Local::TestConvertSuccCase<double>(static_cast<double>(3), nullptr);
    Local::TestConvertSuccCase<double>(static_cast<double>(100), nullptr);
}

BOOST_AUTO_TEST_CASE(ConvertIndexAlgoType)
{
    Local::TestConvertSuccCase<SPTAG::IndexAlgoType>(SPTAG::IndexAlgoType::BKT, "BKT");
    Local::TestConvertSuccCase<SPTAG::IndexAlgoType>(SPTAG::IndexAlgoType::KDT, "KDT");
}

BOOST_AUTO_TEST_CASE(ConvertVectorValueType)
{
    Local::TestConvertSuccCase<SPTAG::VectorValueType>(SPTAG::VectorValueType::Float, "Float");
    Local::TestConvertSuccCase<SPTAG::VectorValueType>(SPTAG::VectorValueType::Int8, "Int8");
    Local::TestConvertSuccCase<SPTAG::VectorValueType>(SPTAG::VectorValueType::Int16, "Int16");
}

BOOST_AUTO_TEST_CASE(ConvertDistCalcMethod)
{
    Local::TestConvertSuccCase<SPTAG::DistCalcMethod>(SPTAG::DistCalcMethod::Cosine, "Cosine");
    Local::TestConvertSuccCase<SPTAG::DistCalcMethod>(SPTAG::DistCalcMethod::L2, "L2");
}

BOOST_AUTO_TEST_SUITE_END()