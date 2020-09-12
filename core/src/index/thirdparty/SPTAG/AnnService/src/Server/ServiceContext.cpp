// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Server/ServiceContext.h"
#include "inc/Helper/SimpleIniReader.h"
#include "inc/Helper/CommonHelper.h"
#include "inc/Helper/StringConvert.h"

using namespace SPTAG;
using namespace SPTAG::Service;


ServiceContext::ServiceContext(const std::string& p_configFilePath)
    : m_initialized(false)
{
    Helper::IniReader iniReader;
    if (ErrorCode::Success != iniReader.LoadIniFile(p_configFilePath))
    {
        return;
    }

    m_settings.reset(new ServiceSettings);

    m_settings->m_listenAddr = iniReader.GetParameter("Service", "ListenAddr", std::string("0.0.0.0"));
    m_settings->m_listenPort = iniReader.GetParameter("Service", "ListenPort", std::string("8000"));
    m_settings->m_threadNum = iniReader.GetParameter("Service", "ThreadNumber", static_cast<std::uint32_t>(8));
    m_settings->m_socketThreadNum = iniReader.GetParameter("Service", "SocketThreadNumber", static_cast<std::uint32_t>(8));

    m_settings->m_defaultMaxResultNumber = iniReader.GetParameter("QueryConfig", "DefaultMaxResultNumber", static_cast<SizeType>(10));
    m_settings->m_vectorSeparator = iniReader.GetParameter("QueryConfig", "DefaultSeparator", std::string("|"));

    const std::string emptyStr;

    std::string indexListStr = iniReader.GetParameter("Index", "List", emptyStr);
    const auto& indexList = Helper::StrUtils::SplitString(indexListStr, ",");

    for (const auto& indexName : indexList)
    {
        std::string sectionName("Index_");
        sectionName += indexName.c_str();
        if (!iniReader.DoesParameterExist(sectionName, "IndexFolder"))
        {
            continue;
        }

        std::string indexFolder = iniReader.GetParameter(sectionName, "IndexFolder", emptyStr);

        std::shared_ptr<VectorIndex> vectorIndex;
        if (ErrorCode::Success == VectorIndex::LoadIndex(indexFolder, vectorIndex))
        {
            vectorIndex->SetIndexName(indexName);
            m_fullIndexList.emplace(indexName, vectorIndex);
        }
        else
        {
            fprintf(stderr, "Failed loading index: %s\n", indexName.c_str());
        }
    }

    m_initialized = true;
}


ServiceContext::~ServiceContext()
{

}


const std::map<std::string, std::shared_ptr<VectorIndex>>&
ServiceContext::GetIndexMap() const
{
    return m_fullIndexList;
}


const std::shared_ptr<ServiceSettings>&
ServiceContext::GetServiceSettings() const
{
    return m_settings;
}


bool
ServiceContext::IsInitialized() const
{
    return m_initialized;
}
