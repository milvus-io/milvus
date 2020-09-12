// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Aggregator/AggregatorContext.h"
#include "inc/Helper/SimpleIniReader.h"

using namespace SPTAG;
using namespace SPTAG::Aggregator;

RemoteMachine::RemoteMachine()
    : m_connectionID(Socket::c_invalidConnectionID),
      m_status(RemoteMachineStatus::Disconnected)
{
}


AggregatorContext::AggregatorContext(const std::string& p_filePath)
    : m_initialized(false)
{
    Helper::IniReader iniReader;
    if (ErrorCode::Success != iniReader.LoadIniFile(p_filePath))
    {
        return;
    }

    m_settings.reset(new AggregatorSettings);

    m_settings->m_listenAddr = iniReader.GetParameter("Service", "ListenAddr", std::string("0.0.0.0"));
    m_settings->m_listenPort = iniReader.GetParameter("Service", "ListenPort", std::string("8100"));
    m_settings->m_threadNum = iniReader.GetParameter("Service", "ThreadNumber", static_cast<std::uint32_t>(8));
    m_settings->m_socketThreadNum = iniReader.GetParameter("Service", "SocketThreadNumber", static_cast<std::uint32_t>(8));

    const std::string emptyStr;

    std::uint32_t serverNum = iniReader.GetParameter("Servers", "Number", static_cast<std::uint32_t>(0));

    for (std::uint32_t i = 0; i < serverNum; ++i)
    {
        std::string sectionName("Server_");
        sectionName += std::to_string(i);
        if (!iniReader.DoesSectionExist(sectionName))
        {
            continue;
        }

        std::shared_ptr<RemoteMachine> remoteMachine(new RemoteMachine);

        remoteMachine->m_address = iniReader.GetParameter(sectionName, "Address", emptyStr);
        remoteMachine->m_port = iniReader.GetParameter(sectionName, "Port", emptyStr);

        if (remoteMachine->m_address.empty() || remoteMachine->m_port.empty())
        {
            continue;
        }

        m_remoteServers.push_back(std::move(remoteMachine));
    }

    m_initialized = true;
}


AggregatorContext::~AggregatorContext()
{
}


bool
AggregatorContext::IsInitialized() const
{
    return m_initialized;
}


const std::vector<std::shared_ptr<RemoteMachine>>&
AggregatorContext::GetRemoteServers() const
{
    return m_remoteServers;
}


const std::shared_ptr<AggregatorSettings>&
AggregatorContext::GetSettings() const
{
    return m_settings;
}
