// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_AGGREGATOR_AGGREGATORCONTEXT_H_
#define _SPTAG_AGGREGATOR_AGGREGATORCONTEXT_H_

#include "inc/Socket/Common.h"
#include "AggregatorSettings.h"

#include <memory>
#include <vector>
#include <atomic>

namespace SPTAG
{
namespace Aggregator
{

enum RemoteMachineStatus : uint8_t
{
    Disconnected = 0,

    Connecting,

    Connected
};


struct RemoteMachine
{
    RemoteMachine();

    std::string m_address;

    std::string m_port;

    Socket::ConnectionID m_connectionID;

    std::atomic<RemoteMachineStatus> m_status;
};

class AggregatorContext
{
public:
    AggregatorContext(const std::string& p_filePath);

    ~AggregatorContext();

    bool IsInitialized() const;

    const std::vector<std::shared_ptr<RemoteMachine>>& GetRemoteServers() const;

    const std::shared_ptr<AggregatorSettings>& GetSettings() const;

private:
    std::vector<std::shared_ptr<RemoteMachine>> m_remoteServers;

    std::shared_ptr<AggregatorSettings> m_settings;

    bool m_initialized;
};

} // namespace Aggregator
} // namespace AnnService


#endif // _SPTAG_AGGREGATOR_AGGREGATORCONTEXT_H_
