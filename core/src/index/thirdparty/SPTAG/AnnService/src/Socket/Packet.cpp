// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Socket/Packet.h"
#include "inc/Socket/SimpleSerialization.h"

#include <type_traits>

using namespace SPTAG::Socket;

PacketHeader::PacketHeader()
    : m_packetType(PacketType::Undefined),
      m_processStatus(PacketProcessStatus::Ok),
      m_bodyLength(0),
      m_connectionID(c_invalidConnectionID),
      m_resourceID(c_invalidResourceID)
{
}


PacketHeader::PacketHeader(PacketHeader&& p_right)
    : m_packetType(std::move(p_right.m_packetType)),
      m_processStatus(std::move(p_right.m_processStatus)),
      m_bodyLength(std::move(p_right.m_bodyLength)),
      m_connectionID(std::move(p_right.m_connectionID)),
      m_resourceID(std::move(p_right.m_resourceID))
{
}


PacketHeader::PacketHeader(const PacketHeader& p_right)
    : m_packetType(p_right.m_packetType),
      m_processStatus(p_right.m_processStatus),
      m_bodyLength(p_right.m_bodyLength),
      m_connectionID(p_right.m_connectionID),
      m_resourceID(p_right.m_resourceID)
{
}


std::size_t
PacketHeader::WriteBuffer(std::uint8_t* p_buffer)
{
    std::uint8_t* buff = p_buffer;
    buff = SimpleSerialization::SimpleWriteBuffer(m_packetType, buff);
    buff = SimpleSerialization::SimpleWriteBuffer(m_processStatus, buff);
    buff = SimpleSerialization::SimpleWriteBuffer(m_bodyLength, buff);
    buff = SimpleSerialization::SimpleWriteBuffer(m_connectionID, buff);
    buff = SimpleSerialization::SimpleWriteBuffer(m_resourceID, buff);

    return p_buffer - buff;
}


void
PacketHeader::ReadBuffer(const std::uint8_t* p_buffer)
{
    const std::uint8_t* buff = p_buffer;
    buff = SimpleSerialization::SimpleReadBuffer(buff, m_packetType);
    buff = SimpleSerialization::SimpleReadBuffer(buff, m_processStatus);
    buff = SimpleSerialization::SimpleReadBuffer(buff, m_bodyLength);
    buff = SimpleSerialization::SimpleReadBuffer(buff, m_connectionID);
    buff = SimpleSerialization::SimpleReadBuffer(buff, m_resourceID);
}


Packet::Packet()
{
}


Packet::Packet(Packet&& p_right)
    : m_header(std::move(p_right.m_header)),
      m_buffer(std::move(p_right.m_buffer)),
      m_bufferCapacity(std::move(p_right.m_bufferCapacity))
{
}


Packet::Packet(const Packet& p_right)
    : m_header(p_right.m_header),
      m_buffer(p_right.m_buffer),
      m_bufferCapacity(p_right.m_bufferCapacity)
{
}


PacketHeader&
Packet::Header()
{
    return m_header;
}


std::uint8_t*
Packet::HeaderBuffer() const
{
    return m_buffer.get();
}


std::uint8_t*
Packet::Body() const
{
    if (nullptr != m_buffer && PacketHeader::c_bufferSize < m_bufferCapacity)
    {
        return m_buffer.get() + PacketHeader::c_bufferSize;
    }

    return nullptr;
}


std::uint8_t*
Packet::Buffer() const
{
    return m_buffer.get();
}


std::uint32_t
Packet::BufferLength() const
{
    return PacketHeader::c_bufferSize + m_header.m_bodyLength;
}


std::uint32_t
Packet::BufferCapacity() const
{
    return m_bufferCapacity;
}


void
Packet::AllocateBuffer(std::uint32_t p_bodyCapacity)
{
    m_bufferCapacity = PacketHeader::c_bufferSize + p_bodyCapacity;
    m_buffer.reset(new std::uint8_t[m_bufferCapacity], std::default_delete<std::uint8_t[]>());
}


bool
PacketTypeHelper::IsRequestPacket(PacketType p_type)
{
    if (PacketType::Undefined == p_type || PacketType::ResponseMask == p_type)
    {
        return false;
    }

    return (static_cast<std::uint8_t>(p_type) & static_cast<std::uint8_t>(PacketType::ResponseMask)) == 0;
}


bool
PacketTypeHelper::IsResponsePacket(PacketType p_type)
{
    if (PacketType::Undefined == p_type || PacketType::ResponseMask == p_type)
    {
        return false;
    }

    return (static_cast<std::uint8_t>(p_type) & static_cast<std::uint8_t>(PacketType::ResponseMask)) != 0;
}


PacketType
PacketTypeHelper::GetCrosspondingResponseType(PacketType p_type)
{
    if (PacketType::Undefined == p_type || PacketType::ResponseMask == p_type)
    {
        return PacketType::Undefined;
    }

    auto ret = static_cast<std::uint8_t>(p_type) | static_cast<std::uint8_t>(PacketType::ResponseMask);
    return static_cast<PacketType>(ret);
}
