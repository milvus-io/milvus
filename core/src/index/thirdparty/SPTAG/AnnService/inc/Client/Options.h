// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_CLIENT_OPTIONS_H_
#define _SPTAG_CLIENT_OPTIONS_H_

#include "inc/Helper/ArgumentsParser.h"

#include <string>
#include <vector>
#include <memory>

namespace SPTAG
{
namespace Client
{

class ClientOptions : public Helper::ArgumentsParser
{
public:
    ClientOptions();

    virtual ~ClientOptions();

    std::string m_serverAddr;

    std::string m_serverPort;

    // in milliseconds.
    std::uint32_t m_searchTimeout;

    std::uint32_t m_threadNum;

    std::uint32_t m_socketThreadNum;

};


} // namespace Socket
} // namespace SPTAG

#endif // _SPTAG_CLIENT_OPTIONS_H_
