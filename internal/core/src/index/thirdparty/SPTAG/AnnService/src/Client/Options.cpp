// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Client/Options.h"
#include "inc/Helper/StringConvert.h"

#include <cassert>

using namespace SPTAG;
using namespace SPTAG::Client;

ClientOptions::ClientOptions()
    : m_searchTimeout(9000),
      m_threadNum(1),
      m_socketThreadNum(2)
{
    AddRequiredOption(m_serverAddr, "-s", "--server", "Server address.");
    AddRequiredOption(m_serverPort, "-p", "--port", "Server port.");
    AddOptionalOption(m_searchTimeout, "-t", "", "Search timeout.");
    AddOptionalOption(m_threadNum, "-cth", "", "Client Thread Number.");
    AddOptionalOption(m_socketThreadNum, "-sth", "", "Socket Thread Number.");
}


ClientOptions::~ClientOptions()
{
}
