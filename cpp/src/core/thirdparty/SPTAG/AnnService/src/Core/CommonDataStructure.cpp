// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Core/CommonDataStructure.h"

using namespace SPTAG;

const ByteArray ByteArray::c_empty;

ByteArray::ByteArray()
    : m_data(nullptr),
    m_length(0)
{
}


ByteArray::ByteArray(ByteArray&& p_right)
    : m_data(p_right.m_data),
    m_length(p_right.m_length),
    m_dataHolder(std::move(p_right.m_dataHolder))
{
}


ByteArray::ByteArray(std::uint8_t* p_array, std::size_t p_length, bool p_transferOnwership)
    : m_data(p_array),
      m_length(p_length)
{
    if (p_transferOnwership)
    {
        m_dataHolder.reset(m_data, std::default_delete<std::uint8_t[]>());
    }
}


ByteArray::ByteArray(std::uint8_t* p_array, std::size_t p_length, std::shared_ptr<std::uint8_t> p_dataHolder)
    : m_data(p_array),
      m_length(p_length),
      m_dataHolder(std::move(p_dataHolder))
{
}


ByteArray::ByteArray(const ByteArray& p_right)
    : m_data(p_right.m_data),
      m_length(p_right.m_length),
      m_dataHolder(p_right.m_dataHolder)
{
}


ByteArray&
ByteArray::operator= (const ByteArray& p_right)
{
    m_data = p_right.m_data;
    m_length = p_right.m_length;
    m_dataHolder = p_right.m_dataHolder;

    return *this;
}


ByteArray&
ByteArray::operator= (ByteArray&& p_right)
{
    m_data = p_right.m_data;
    m_length = p_right.m_length;
    m_dataHolder = std::move(p_right.m_dataHolder);

    return *this;
}


ByteArray::~ByteArray()
{
}


ByteArray
ByteArray::Alloc(std::size_t p_length)
{
    ByteArray byteArray;
    if (0 == p_length)
    {
        return byteArray;
    }

    byteArray.m_dataHolder.reset(new std::uint8_t[p_length],
                                 std::default_delete<std::uint8_t[]>());

    byteArray.m_length = p_length;
    byteArray.m_data = byteArray.m_dataHolder.get();
    return byteArray;
}


std::uint8_t*
ByteArray::Data() const
{
    return m_data;
}


std::size_t
ByteArray::Length() const
{
	return m_length;
}


void
ByteArray::SetData(std::uint8_t* p_array, std::size_t p_length)
{
    m_data = p_array;
	m_length = p_length;
}


std::shared_ptr<std::uint8_t>
ByteArray::DataHolder() const
{
    return m_dataHolder;
}


void
ByteArray::Clear()
{
    m_data = nullptr;
    m_dataHolder.reset();
    m_length = 0;
}