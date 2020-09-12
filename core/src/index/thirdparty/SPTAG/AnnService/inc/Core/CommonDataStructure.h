// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_COMMONDATASTRUCTURE_H_
#define _SPTAG_COMMONDATASTRUCTURE_H_

#include "inc/Core/Common.h"

namespace SPTAG
{

template<typename T>
class Array
{
public:
    Array();

    Array(T* p_array, std::size_t p_length, bool p_transferOwnership);
    
    Array(T* p_array, std::size_t p_length, std::shared_ptr<T> p_dataHolder);

    Array(Array<T>&& p_right);

    Array(const Array<T>& p_right);

    Array<T>& operator= (Array<T>&& p_right);

    Array<T>& operator= (const Array<T>& p_right);

    T& operator[] (std::size_t p_index);

    const T& operator[] (std::size_t p_index) const;

    ~Array();

    T* Data() const;

    std::size_t Length() const;

    std::shared_ptr<T> DataHolder() const;
    
    void Set(T* p_array, std::size_t p_length, bool p_transferOwnership);

    void Clear();

    static Array<T> Alloc(std::size_t p_length);

    const static Array<T> c_empty;

private:
    T* m_data;

    std::size_t m_length;

    // Notice this is holding an array. Set correct deleter for this.
    std::shared_ptr<T> m_dataHolder;
};

template<typename T>
const Array<T> Array<T>::c_empty;


template<typename T>
Array<T>::Array()
    : m_data(nullptr),
    m_length(0)
{
}

template<typename T>
Array<T>::Array(T* p_array, std::size_t p_length, bool p_transferOnwership)

    : m_data(p_array),
    m_length(p_length)
{
    if (p_transferOnwership)
    {
        m_dataHolder.reset(m_data, std::default_delete<T[]>());
    }
}


template<typename T>
Array<T>::Array(T* p_array, std::size_t p_length, std::shared_ptr<T> p_dataHolder)
    : m_data(p_array),
    m_length(p_length),
    m_dataHolder(std::move(p_dataHolder))
{
}


template<typename T>
Array<T>::Array(Array<T>&& p_right)
    : m_data(p_right.m_data),
    m_length(p_right.m_length),
    m_dataHolder(std::move(p_right.m_dataHolder))
{
}


template<typename T>
Array<T>::Array(const Array<T>& p_right)
    : m_data(p_right.m_data),
    m_length(p_right.m_length),
    m_dataHolder(p_right.m_dataHolder)
{
}


template<typename T>
Array<T>&
Array<T>::operator= (Array<T>&& p_right)
{
    m_data = p_right.m_data;
    m_length = p_right.m_length;
    m_dataHolder = std::move(p_right.m_dataHolder);

    return *this;
}


template<typename T>
Array<T>&
Array<T>::operator= (const Array<T>& p_right)
{
    m_data = p_right.m_data;
    m_length = p_right.m_length;
    m_dataHolder = p_right.m_dataHolder;

    return *this;
}


template<typename T>
T&
Array<T>::operator[] (std::size_t p_index)
{
    return m_data[p_index];
}


template<typename T>
const T&
Array<T>::operator[] (std::size_t p_index) const
{
    return m_data[p_index];
}


template<typename T>
Array<T>::~Array()
{
}


template<typename T>
T*
Array<T>::Data() const
{
    return m_data;
}


template<typename T>
std::size_t
Array<T>::Length() const
{
    return m_length;
}


template<typename T>
std::shared_ptr<T>
Array<T>::DataHolder() const
{
    return m_dataHolder;
}


template<typename T>
void
Array<T>::Set(T* p_array, std::size_t p_length, bool p_transferOwnership)
{
    m_data = p_array;
    m_length = p_length;

    if (p_transferOwnership)
    {
        m_dataHolder.reset(m_data, std::default_delete<T[]>());
    }
}


template<typename T>
void
Array<T>::Clear()
{
    m_data = nullptr;
    m_length = 0;
    m_dataHolder.reset();
}


template<typename T>
Array<T>
Array<T>::Alloc(std::size_t p_length)
{
    Array<T> arr;
    if (0 == p_length)
    {
        return arr;
    }

    arr.m_dataHolder.reset(new T[p_length], std::default_delete<T[]>());

    arr.m_length = p_length;
    arr.m_data = arr.m_dataHolder.get();
    return arr;
}


typedef Array<std::uint8_t> ByteArray;

} // namespace SPTAG

#endif // _SPTAG_COMMONDATASTRUCTURE_H_
