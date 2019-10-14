// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Test.h"
#include "inc/Helper/Base64Encode.h"

#include <memory>

BOOST_AUTO_TEST_SUITE(Base64Test)

BOOST_AUTO_TEST_CASE(Base64EncDec)
{
    using namespace SPTAG::Helper::Base64;

    const size_t bufferSize = 1 << 10;
    std::unique_ptr<uint8_t[]> rawBuffer(new uint8_t[bufferSize]);
    std::unique_ptr<char[]> encBuffer(new char[bufferSize]);
    std::unique_ptr<uint8_t[]> rawBuffer2(new uint8_t[bufferSize]);

    for (size_t inputSize = 1; inputSize < 128; ++inputSize)
    {
        for (size_t i = 0; i < inputSize; ++i)
        {
            rawBuffer[i] = static_cast<uint8_t>(i);
        }

        size_t encBufLen = CapacityForEncode(inputSize);
        BOOST_CHECK(encBufLen < bufferSize);

        size_t encOutLen = 0;
        BOOST_CHECK(Encode(rawBuffer.get(), inputSize, encBuffer.get(), encOutLen));
        BOOST_CHECK(encBufLen >= encOutLen);

        size_t decBufLen = CapacityForDecode(encOutLen);
        BOOST_CHECK(decBufLen < bufferSize);

        size_t decOutLen = 0;
        BOOST_CHECK(Decode(encBuffer.get(), encOutLen, rawBuffer.get(), decOutLen));
        BOOST_CHECK(decBufLen >= decOutLen);
    }
}

BOOST_AUTO_TEST_SUITE_END()