// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_PW_CLIENTINTERFACE_H_
#define _SPTAG_PW_CLIENTINTERFACE_H_

#include "TransferDataType.h"
#include "inc/Socket/Client.h"
#include "inc/Socket/ResourceManager.h"

#include <unordered_map>
#include <atomic>
#include <mutex>

class AnnClient
{
public:
    AnnClient(const char* p_serverAddr, const char* p_serverPort);

    ~AnnClient();

    void SetTimeoutMilliseconds(int p_timeout);

    void SetSearchParam(const char* p_name, const char* p_value);

    void ClearSearchParam();

    std::shared_ptr<RemoteSearchResult> Search(ByteArray p_data, int p_resultNum, const char* p_valueType, bool p_withMetaData);

    bool IsConnected() const;

private:
    std::string CreateSearchQuery(const ByteArray& p_data,
                                  int p_resultNum,
                                  bool p_extractMetadata,
                                  SPTAG::VectorValueType p_valueType);

    SPTAG::Socket::PacketHandlerMapPtr GetHandlerMap();

    void SearchResponseHanlder(SPTAG::Socket::ConnectionID p_localConnectionID,
                               SPTAG::Socket::Packet p_packet);

private:
    typedef std::function<void(SPTAG::Socket::RemoteSearchResult)> Callback;

    std::uint32_t m_timeoutInMilliseconds;

    std::string m_server;

    std::string m_port;

    std::unique_ptr<SPTAG::Socket::Client> m_socketClient;

    std::atomic<SPTAG::Socket::ConnectionID> m_connectionID;

    SPTAG::Socket::ResourceManager<Callback> m_callbackManager;

    std::unordered_map<std::string, std::string> m_params;

    std::mutex m_paramMutex;
};

#endif // _SPTAG_PW_CLIENTINTERFACE_H_
