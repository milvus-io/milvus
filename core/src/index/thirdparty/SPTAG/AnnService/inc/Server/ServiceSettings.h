// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SERVER_SERVICESTTINGS_H_
#define _SPTAG_SERVER_SERVICESTTINGS_H_

#include "../Core/Common.h"

#include <string>

namespace SPTAG
{
namespace Service
{

struct ServiceSettings
{
    ServiceSettings();

    std::string m_vectorSeparator;

    std::string m_listenAddr;

    std::string m_listenPort;

    SizeType m_defaultMaxResultNumber;

    SizeType m_threadNum;

    SizeType m_socketThreadNum;
};




} // namespace Server
} // namespace AnnService


#endif // _SPTAG_SERVER_SERVICESTTINGS_H_

