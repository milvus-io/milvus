// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SOCKET_PACKET_H_
#define _SPTAG_SOCKET_PACKET_H_

#include "Common.h"

#include <cstdint>
#include <memory>
#include <functional>
#include <array>
#include <unordered_map>

namespace SPTAG
{
namespace Socket
{

enum class PacketType : std::uint8_t
{
    Undefined = 0x00,

    HeartbeatRequest = 0x01,

    RegisterRequest = 0x02,

    SearchRequest = 0x03,

    ResponseMask = 0x80,

    HeartbeatResponse = ResponseMask | HeartbeatRequest,

    RegisterResponse = ResponseMask | RegisterRequest,

    SearchResponse = ResponseMask | SearchRequest
};


enum class PacketProcessStatus : std::uint8_t
{
    Ok = 0x00,

    Timeout = 0x01,

    Dropped = 0x02,

    Failed = 0x03
};


struct PacketHeader
{
    static constexpr std::size_t c_bufferSize = 16;

    PacketHeader();
    PacketHeader(PacketHeader&& p_right);
    PacketHeader(const PacketHeader& p_right);

    std::size_t WriteBuffer(std::uint8_t* p_buffer);

    void ReadBuffer(const std::uint8_t* p_buffer);

    PacketType m_packetType;

    PacketProcessStatus m_processStatus;

    std::uint32_t m_bodyLength;

    // Meaning of this is different with different PacketType.
    // In most request case, it means connection expeced for response.
    // In most response case, it means connection which handled request.
    ConnectionID m_connectionID;

    ResourceID m_resourceID;
};


static_assert(sizeof(PacketHeader) <= PacketHeader::c_bufferSize, "");


class Packet
{
public:
    Packet();
    Packet(Packet&& p_right);
    Packet(const Packet& p_right);

    PacketHeader& Header();

    std::uint8_t* HeaderBuffer() const;

    std::uint8_t* Body() const;

    std::uint8_t* Buffer() const;

    std::uint32_t BufferLength() const;

    std::uint32_t BufferCapacity() const;

    void AllocateBuffer(std::uint32_t p_bodyCapacity);

private:
    PacketHeader m_header;

    std::shared_ptr<std::uint8_t> m_buffer;

    std::uint32_t m_bufferCapacity;
};


struct PacketTypeHash
{
    std::size_t operator()(const PacketType& p_val) const
    {
        return static_cast<std::size_t>(p_val);
    }
};


typedef std::function<void(ConnectionID, Packet)> PacketHandler;

typedef std::unordered_map<PacketType, PacketHandler, PacketTypeHash> PacketHandlerMap;
typedef std::shared_ptr<PacketHandlerMap> PacketHandlerMapPtr;


namespace PacketTypeHelper
{

bool IsRequestPacket(PacketType p_type);

bool IsResponsePacket(PacketType p_type);

PacketType GetCrosspondingResponseType(PacketType p_type);

}


} // namespace SPTAG
} // namespace Socket

#endif // _SPTAG_SOCKET_SOCKETSERVER_H_
