// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/ClientInterface.h"
#include "inc/Helper/CommonHelper.h"
#include "inc/Helper/Concurrent.h"
#include "inc/Helper/Base64Encode.h"
#include "inc/Helper/StringConvert.h"

#include <boost/asio.hpp>


AnnClient::AnnClient(const char* p_serverAddr, const char* p_serverPort)
    : m_connectionID(SPTAG::Socket::c_invalidConnectionID),
      m_timeoutInMilliseconds(9000)
{
    using namespace SPTAG;

    m_socketClient.reset(new Socket::Client(GetHandlerMap(), 2, 30));

    if (nullptr == p_serverAddr || nullptr == p_serverPort)
    {
        return;
    }

    m_server = p_serverAddr;
    m_port = p_serverPort;

    auto connectCallback = [this](Socket::ConnectionID p_cid, ErrorCode p_ec)
    {
        m_connectionID = p_cid;

        if (ErrorCode::Socket_FailedResolveEndPoint == p_ec)
        {
            return;
        }

        while (Socket::c_invalidConnectionID == m_connectionID)
        {
            ErrorCode errCode;
            std::this_thread::sleep_for(std::chrono::seconds(10));
            m_connectionID = m_socketClient->ConnectToServer(m_server, m_port, errCode);
        }
    };

    m_socketClient->AsyncConnectToServer(m_server, m_port, connectCallback);

    m_socketClient->SetEventOnConnectionClose([this](Socket::ConnectionID p_cid)
    {
        ErrorCode errCode;
        m_connectionID = Socket::c_invalidConnectionID;
        while (Socket::c_invalidConnectionID == m_connectionID)
        {
            std::this_thread::sleep_for(std::chrono::seconds(10));
            m_connectionID = m_socketClient->ConnectToServer(m_server, m_port, errCode);
        }
    });
}


AnnClient::~AnnClient()
{
}


void
AnnClient::SetTimeoutMilliseconds(int p_timeout)
{
    m_timeoutInMilliseconds = p_timeout;
}


void
AnnClient::SetSearchParam(const char* p_name, const char* p_value)
{
    std::lock_guard<std::mutex> guard(m_paramMutex);

    if (nullptr == p_name || '\0' == *p_name)
    {
        return;
    }

    std::string name(p_name);
    SPTAG::Helper::StrUtils::ToLowerInPlace(name);

    if (nullptr == p_value || '\0' == *p_value)
    {
        m_params.erase(name);
        return;
    }

    m_params[name] = p_value;
}


void
AnnClient::ClearSearchParam()
{
    std::lock_guard<std::mutex> guard(m_paramMutex);
    m_params.clear();
}


std::shared_ptr<RemoteSearchResult>
AnnClient::Search(ByteArray p_data, int p_resultNum, const char* p_valueType, bool p_withMetaData)
{
    using namespace SPTAG;

    SPTAG::Socket::RemoteSearchResult ret;
    if (Socket::c_invalidConnectionID != m_connectionID)
    {

        auto signal = std::make_shared<Helper::Concurrent::WaitSignal>(1);

        auto callback = [&ret, signal](RemoteSearchResult p_result)
        {
            if (RemoteSearchResult::ResultStatus::Success == p_result.m_status)
            {
                ret = std::move(p_result);
            }

            signal->FinishOne();
        };

        auto timeoutCallback = [this](std::shared_ptr<Callback> p_callback)
        {
            if (nullptr != p_callback)
            {
                RemoteSearchResult result;
                result.m_status = RemoteSearchResult::ResultStatus::Timeout;

                (*p_callback)(std::move(result));
            }
        };

        auto connectCallback = [callback, this](bool p_connectSucc)
        {
            if (!p_connectSucc)
            {
                RemoteSearchResult result;
                result.m_status = RemoteSearchResult::ResultStatus::FailedNetwork;

                callback(std::move(result));
            }
        };

        Socket::Packet packet;
        packet.Header().m_connectionID = Socket::c_invalidConnectionID;
        packet.Header().m_packetType = Socket::PacketType::SearchRequest;
        packet.Header().m_processStatus = Socket::PacketProcessStatus::Ok;
        packet.Header().m_resourceID = m_callbackManager.Add(std::make_shared<Callback>(std::move(callback)),
            m_timeoutInMilliseconds,
            std::move(timeoutCallback));

        Socket::RemoteQuery query;
        SPTAG::VectorValueType valueType;
        SPTAG::Helper::Convert::ConvertStringTo<SPTAG::VectorValueType>(p_valueType, valueType);
        query.m_queryString = CreateSearchQuery(p_data, p_resultNum, p_withMetaData, valueType);

        packet.Header().m_bodyLength = static_cast<std::uint32_t>(query.EstimateBufferSize());
        packet.AllocateBuffer(packet.Header().m_bodyLength);
        query.Write(packet.Body());
        packet.Header().WriteBuffer(packet.HeaderBuffer());

        m_socketClient->SendPacket(m_connectionID, std::move(packet), connectCallback);

        signal->Wait();
    }
    return std::make_shared<RemoteSearchResult>(ret);
}


bool
AnnClient::IsConnected() const
{
    return m_connectionID != SPTAG::Socket::c_invalidConnectionID;
}


SPTAG::Socket::PacketHandlerMapPtr
AnnClient::GetHandlerMap()
{
    using namespace SPTAG;

    Socket::PacketHandlerMapPtr handlerMap(new Socket::PacketHandlerMap);
    handlerMap->emplace(Socket::PacketType::SearchResponse,
                        std::bind(&AnnClient::SearchResponseHanlder,
                                  this,
                                  std::placeholders::_1,
                                  std::placeholders::_2));

    return handlerMap;
}


void
AnnClient::SearchResponseHanlder(SPTAG::Socket::ConnectionID p_localConnectionID,
                                 SPTAG::Socket::Packet p_packet)
{
    using namespace SPTAG;

    std::shared_ptr<Callback> callback = m_callbackManager.GetAndRemove(p_packet.Header().m_resourceID);
    if (nullptr == callback)
    {
        return;
    }

    if (p_packet.Header().m_processStatus != Socket::PacketProcessStatus::Ok || 0 == p_packet.Header().m_bodyLength)
    {
        Socket::RemoteSearchResult result;
        result.m_status = Socket::RemoteSearchResult::ResultStatus::FailedExecute;

        (*callback)(std::move(result));
    }
    else
    {
        Socket::RemoteSearchResult result;
        result.Read(p_packet.Body());
        (*callback)(std::move(result));
    }
}


std::string
AnnClient::CreateSearchQuery(const ByteArray& p_data,
                             int p_resultNum,
                             bool p_extractMetadata,
                             SPTAG::VectorValueType p_valueType)
{
    std::stringstream out;

    out << "#";
    std::size_t encLen;
    SPTAG::Helper::Base64::Encode(p_data.Data(), p_data.Length(), out, encLen);

    out << " $datatype:" << SPTAG::Helper::Convert::ConvertToString(p_valueType);
    out << " $resultnum:" << std::to_string(p_resultNum);
    out << " $extractmetadata:" << (p_extractMetadata ? "true" : "false");

    {
        std::lock_guard<std::mutex> guard(m_paramMutex);
        for (const auto& param : m_params)
        {
            out << " $" << param.first << ":" << param.second;
        }
    }

    return out.str();
}

