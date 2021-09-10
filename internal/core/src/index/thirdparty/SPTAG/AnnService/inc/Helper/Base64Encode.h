// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_HELPER_BASE64ENCODE_H_
#define _SPTAG_HELPER_BASE64ENCODE_H_

#include <cstdint>
#include <cstddef> 
#include <ostream>

namespace SPTAG
{
namespace Helper
{
namespace Base64
{

bool Encode(const std::uint8_t* p_in, std::size_t p_inLen, char* p_out, std::size_t& p_outLen);

bool Encode(const std::uint8_t* p_in, std::size_t p_inLen, std::ostream& p_out, std::size_t& p_outLen);

bool Decode(const char* p_in, std::size_t p_inLen, std::uint8_t* p_out, std::size_t& p_outLen);

std::size_t CapacityForEncode(std::size_t p_inLen);

std::size_t CapacityForDecode(std::size_t p_inLen);


} // namespace Base64
} // namespace Helper
} // namespace SPTAG

#endif // _SPTAG_HELPER_BASE64ENCODE_H_
