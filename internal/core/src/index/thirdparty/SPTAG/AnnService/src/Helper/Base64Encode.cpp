// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Helper/Base64Encode.h"

using namespace SPTAG;
using namespace SPTAG::Helper;

namespace
{
namespace Local
{
const char c_encTable[] =
{
    'A','B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','X','Y','Z',
    'a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z',
    '0','1','2','3','4','5','6','7','8','9','+','/'
};


const std::uint8_t c_decTable[] =
{
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, // 0x00 - 0x0f
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, // 0x10 - 0x1f
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 62, 64, 64, 64, 63, // 0x20 - 0x2f
    52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 64, 64, 64, 64, 64, 64, // 0x30 - 0x3f
    64,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, // 0x40 - 0x4f
    15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 64, 64, 64, 64, 64, // 0x50 - 0x5f
    64, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, // 0x60 - 0x6f
    41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 64, 64, 64, 64, 64, // 0x70 - 0x7f
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, // 0x80 - 0x8f
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, // 0x90 - 0x9f
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, // 0xa0 - 0xaf
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, // 0xb0 - 0xbf
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, // 0xc0 - 0xcf
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, // 0xd0 - 0xdf
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, // 0xe0 - 0xef
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, // 0xf0 - 0xff
};


const char c_paddingChar = '=';
}
}


bool
Base64::Encode(const std::uint8_t* p_in, std::size_t p_inLen, char* p_out, std::size_t& p_outLen)
{
    using namespace Local;

    p_outLen = 0;
    while (p_inLen >= 3)
    {
        p_out[0] = c_encTable[p_in[0] >> 2];
        p_out[1] = c_encTable[((p_in[0] & 0x03) << 4) | ((p_in[1] & 0xf0) >> 4)];
        p_out[2] = c_encTable[((p_in[1] & 0x0f) << 2) | ((p_in[2] & 0xc0) >> 6)];
        p_out[3] = c_encTable[p_in[2] & 0x3f];

        p_in += 3;
        p_inLen -= 3;
        p_out += 4;
        p_outLen += 4;
    }

    switch (p_inLen)
    {
    case 1:
        p_out[0] = c_encTable[p_in[0] >> 2];
        p_out[1] = c_encTable[(p_in[0] & 0x03) << 4];
        p_out[2] = c_paddingChar;
        p_out[3] = c_paddingChar;

        p_outLen += 4;
        break;

    case 2:
        p_out[0] = c_encTable[p_in[0] >> 2];
        p_out[1] = c_encTable[((p_in[0] & 0x03) << 4) | ((p_in[1] & 0xf0) >> 4)];
        p_out[2] = c_encTable[(p_in[1] & 0x0f) << 2];
        p_out[3] = c_paddingChar;

        p_outLen += 4;
        break;
    }

    return true;
}


bool
Base64::Encode(const std::uint8_t* p_in, std::size_t p_inLen, std::ostream& p_out, std::size_t& p_outLen)
{
        using namespace Local;

    p_outLen = 0;
    while (p_inLen >= 3)
    {
        p_out << c_encTable[p_in[0] >> 2];
        p_out << c_encTable[((p_in[0] & 0x03) << 4) | ((p_in[1] & 0xf0) >> 4)];
        p_out << c_encTable[((p_in[1] & 0x0f) << 2) | ((p_in[2] & 0xc0) >> 6)];
        p_out << c_encTable[p_in[2] & 0x3f];

        p_in += 3;
        p_inLen -= 3;
        p_outLen += 4;
    }

    switch (p_inLen)
    {
    case 1:
        p_out << c_encTable[p_in[0] >> 2];
        p_out << c_encTable[(p_in[0] & 0x03) << 4];
        p_out << c_paddingChar;
        p_out << c_paddingChar;

        p_outLen += 4;
        break;

    case 2:
        p_out << c_encTable[p_in[0] >> 2];
        p_out << c_encTable[((p_in[0] & 0x03) << 4) | ((p_in[1] & 0xf0) >> 4)];
        p_out << c_encTable[(p_in[1] & 0x0f) << 2];
        p_out << c_paddingChar;

        p_outLen += 4;
        break;

    default:
        break;
    }

    return true;
}


bool
Base64::Decode(const char* p_in, std::size_t p_inLen, std::uint8_t* p_out, std::size_t& p_outLen)
{
    using namespace Local;

    // Should always be padding.
    if ((p_inLen & 0x03) != 0)
    {
        return false;
    }

    std::uint8_t u0 = 0;
    std::uint8_t u1 = 0;
    std::uint8_t u2 = 0;
    std::uint8_t u3 = 0;

    p_outLen = 0;
    while (p_inLen > 4)
    {
        u0 = c_decTable[static_cast<std::size_t>(p_in[0])];
        u1 = c_decTable[static_cast<std::size_t>(p_in[1])];
        u2 = c_decTable[static_cast<std::size_t>(p_in[2])];
        u3 = c_decTable[static_cast<std::size_t>(p_in[3])];

        if (u0 > 63 || u1 > 63 || u2 > 63 || u3 > 63)
        {
            return false;
        }

        p_out[0] = (u0 << 2) | (u1 >> 4);
        p_out[1] = (u1 << 4) | (u2 >> 2);
        p_out[2] = (u2 << 6) | u3;

        p_inLen -= 4;
        p_in += 4;
        p_out += 3;
        p_outLen += 3;
    }

    u0 = c_decTable[static_cast<std::size_t>(p_in[0])];
    u1 = c_decTable[static_cast<std::size_t>(p_in[1])];
    u2 = c_decTable[static_cast<std::size_t>(p_in[2])];
    u3 = c_decTable[static_cast<std::size_t>(p_in[3])];

    if (u0 > 63 || u1 > 63 || (c_paddingChar == p_in[2] && c_paddingChar != p_in[3]))
    {
        return false;
    }

    if (u2 > 63 && c_paddingChar != p_in[2])
    {
        return false;
    }

    if (u3 > 63 && c_paddingChar != p_in[3])
    {
        return false;
    }


    p_out[0] = (u0 << 2) | (u1 >> 4);
    ++p_outLen;
    if (c_paddingChar == p_in[2])
    {
        if ((u1 & 0x0F) != 0)
        {
            return false;
        }
    }
    else
    {
        p_out[1] = (u1 << 4) | (u2 >> 2);
        ++p_outLen;
        if (c_paddingChar == p_in[3])
        {
            if ((u3 & 0x03) != 0)
            {
                return false;
            }
        }
        else
        {
            p_out[2] = (u2 << 6) | u3;
            ++p_outLen;
        }
    }

    return true;
}


std::size_t
Base64::CapacityForEncode(std::size_t p_inLen)
{
    return ((p_inLen + 2) / 3) * 4;
}


std::size_t
Base64::CapacityForDecode(std::size_t p_inLen)
{
    return (p_inLen / 4) * 3 + ((p_inLen % 4) * 2) / 3;
}

