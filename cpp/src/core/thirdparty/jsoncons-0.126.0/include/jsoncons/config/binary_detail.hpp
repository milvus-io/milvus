// Copyright 2017 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_CONFIG_BINARY_DETAIL_HPP
#define JSONCONS_CONFIG_BINARY_DETAIL_HPP

#include <cfloat>
#include <cstddef>
#include <cstdint>
#include <cstring> // std::memcpy
#include <memory>
#include <type_traits> // std::enable_if

#if defined(__apple_build_version__) && ((__clang_major__ < 8) || ((__clang_major__ == 8) && (__clang_minor__ < 1)))
#define APPLE_MISSING_INTRINSICS 1
#endif

// The definitions below follow the definitions in compiler_support_p.h, https://github.com/01org/tinycbor
// MIT license

#ifdef __F16C__
#  include <immintrin.h>
#endif

#ifndef __has_builtin
#  define __has_builtin(x)  0
#endif

#if (defined(__GNUC__) && (__GNUC__ * 100 + __GNUC_MINOR__ >= 403)) || \
    (__has_builtin(__builtin_bswap64) && __has_builtin(__builtin_bswap32))
#  if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#    define JSONCONS_BE64_TO_H     __builtin_bswap64
#    define JSONCONS_H_TO_BE64     __builtin_bswap64
#    define JSONCONS_BE32_TO_H      __builtin_bswap32
#    define JSONCONS_H_TO_BE32      __builtin_bswap32
#    ifdef __INTEL_COMPILER
#      define JSONCONS_BE16_TO_H    _bswap16
#      define JSONCONS_H_TO_BE16    _bswap16
#    elif (__GNUC__ * 100 + __GNUC_MINOR__ >= 608) || __has_builtin(__builtin_bswap16)
#      define JSONCONS_BE16_TO_H    __builtin_bswap16
#      define JSONCONS_H_TO_BE16    __builtin_bswap16
#    else
#      define JSONCONS_BE16_TO_H(x) (((uint16_t)x >> 8) | ((uint16_t)x << 8))
#      define JSONCONS_H_TO_BE16    JSONCONS_BE16_TO_H
#    endif
#    define JSONCONS_LE64_TO_H
#    define JSONCONS_H_TO_LE64
#    define JSONCONS_LE32_TO_H
#    define JSONCONS_H_TO_LE32
#    define JSONCONS_LE16_TO_H
#    define JSONCONS_H_TO_LE16
#  else
#    define JSONCONS_LE64_TO_H     __builtin_bswap64
#    define JSONCONS_H_TO_LE64     __builtin_bswap64
#    define JSONCONS_LE32_TO_H      __builtin_bswap32
#    define JSONCONS_H_TO_LE32      __builtin_bswap32
#    ifdef __INTEL_COMPILER
#      define JSONCONS_LE16_TO_H    _bswap16
#      define JSONCONS_H_TO_LE16    _bswap16
#    elif (__GNUC__ * 100 + __GNUC_MINOR__ >= 608) || __has_builtin(__builtin_bswap16)
#      define JSONCONS_LE16_TO_H    __builtin_bswap16
#      define JSONCONS_H_TO_LE16    __builtin_bswap16
#    else
#      define JSONCONS_LE16_TO_H(x) (((uint16_t)x >> 8) | ((uint16_t)x << 8))
#      define JSONCONS_H_TO_LE16    JSONCONS_LE16_TO_H
#    endif
#    define JSONCONS_BE64_TO_H
#    define JSONCONS_H_TO_BE64
#    define JSONCONS_BE32_TO_H
#    define JSONCONS_H_TO_BE32
#    define JSONCONS_BE16_TO_H
#    define JSONCONS_H_TO_BE16
#  endif
#elif defined(__sun)
#  include <sys/byteorder.h>
#elif defined(_MSC_VER)
/* MSVC, which implies Windows, which implies little-endian and sizeof(long) == 4 */
#  define JSONCONS_BE64_TO_H       _byteswap_uint64
#  define JSONCONS_H_TO_BE64       _byteswap_uint64
#  define JSONCONS_BE32_TO_H        _byteswap_ulong
#  define JSONCONS_H_TO_BE32        _byteswap_ulong
#  define JSONCONS_BE16_TO_H        _byteswap_ushort
#  define JSONCONS_H_TO_BE16        _byteswap_ushort
#  define JSONCONS_LE64_TO_H
#  define JSONCONS_H_TO_LE64
#  define JSONCONS_LE32_TO_H
#  define JSONCONS_H_TO_LE32
#  define JSONCONS_LE16_TO_H
#  define JSONCONS_H_TO_LE16
#endif
#ifndef JSONCONS_BE16_TO_H
#  include <arpa/inet.h>
#  define JSONCONS_BE16_TO_H        ntohs
#  define JSONCONS_H_TO_BE16        htons
#endif
#ifndef JSONCONS_BE32_TO_H
#  include <arpa/inet.h>
#  define JSONCONS_BE32_TO_H        ntohl
#  define JSONCONS_H_TO_BE32        htonl
#endif
#ifndef JSONCONS_BE64_TO_H
#  define JSONCONS_BE64_TO_H       ntohll
#  define JSONCONS_H_TO_BE64       htonll
/* ntohll isn't usually defined */
#  ifndef ntohll
#    if defined(__BYTE_ORDER__) && defined(__ORDER_BIG_ENDIAN__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#      define ntohll
#      define htonll
#    elif defined(__BYTE_ORDER__) && defined(__ORDER_LITTLE_ENDIAN__) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#      define ntohll(x)       ((ntohl((uint32_t)(x)) * UINT64_C(0x100000000)) + (ntohl((x) >> 32)))
#      define htonll          ntohll
#    else
#      error "Unable to determine byte order!"
#    endif
#  endif
#endif

namespace jsoncons { namespace detail { 
  
class read_nbytes_failed : public std::invalid_argument, public virtual json_exception
{
public:
    explicit read_nbytes_failed(size_t count) noexcept
        : std::invalid_argument("")
    {
        buffer_.append("Failed attempting to read ");
        buffer_.append(std::to_string(count));
        buffer_.append(" bytes from vector");
    }
    ~read_nbytes_failed() noexcept
    {
    }
    const char* what() const noexcept override
    {
        return buffer_.c_str();
    }
private:
    std::string buffer_;
};

namespace detail {

static inline bool add_check_overflow(size_t v1, size_t v2, size_t *r)
{
#if ((defined(__GNUC__) && (__GNUC__ >= 5)) && !defined(__INTEL_COMPILER)) || __has_builtin(__builtin_add_overflow)
    return __builtin_add_overflow(v1, v2, r);
#else
    // unsigned additions are well-defined 
    *r = v1 + v2;
    return v1 > v1 + v2;
#endif
}

}

inline 
uint16_t encode_half(double val)
{
#if defined(__F16C__) && !defined(APPLE_MISSING_INTRINSICS)
    return _cvtss_sh((float)val, 3);
#else
    uint64_t v;
    std::memcpy(&v, &val, sizeof(v));
    int sign = v >> 63 << 15;
    int exp = (v >> 52) & 0x7ff;
    int mant = v << 12 >> 12 >> (53-11);    /* keep only the 11 most significant bits of the mantissa */
    exp -= 1023;
    if (exp == 1024) {
        /* infinity or NaN */
        exp = 16;
        mant >>= 1;
    } else if (exp >= 16) {
        /* overflow, as largest number */
        exp = 15;
        mant = 1023;
    } else if (exp >= -14) {
        /* regular normal */
    } else if (exp >= -24) {
        /* subnormal */
        mant |= 1024;
        mant >>= -(exp + 14);
        exp = -15;
    } else {
        /* underflow, make zero */
        return 0;
    }

    /* safe cast here as bit operations above guarantee not to overflow */
    return (uint16_t)(sign | ((exp + 15) << 10) | mant);
#endif
}

/* this function was copied & adapted from RFC 7049 Appendix D */
inline 
double decode_half(uint16_t half)
{
#if defined(__F16C__) && !defined(APPLE_MISSING_INTRINSICS)
    return _cvtsh_ss(half);
#else
    int exp = (half >> 10) & 0x1f;
    int mant = half & 0x3ff;
    double val;
    if (exp == 0) 
    {
        val = ldexp((double)mant, -24);
    }
    else if (exp != 31) 
    {
        val = ldexp(mant + 1024.0, exp - 25);
    } 
    else
    {
        val = mant == 0 ? INFINITY : NAN;
    }
    return half & 0x8000 ? -val : val;
#endif
}

// to_big_endian


template<class T, class OutputIt>
typename std::enable_if<std::is_integral<T>::value && sizeof(T) == sizeof(uint8_t),void>::type
to_big_endian(T val, OutputIt d_first)
{
    *d_first = static_cast<uint8_t>(val);
}


template<typename T, class OutputIt>
typename std::enable_if<std::is_integral<T>::value && 
sizeof(T) == sizeof(uint16_t),void>::type
to_big_endian(T val, OutputIt d_first)
{
    T x = JSONCONS_H_TO_BE16(val);

    uint8_t where[sizeof(T)];
    std::memcpy(where, &x, sizeof(T));

    *d_first++ = where[0];
    *d_first++ = where[1];
}

template<typename T, class OutputIt>
typename std::enable_if<std::is_integral<T>::value && 
sizeof(T) == sizeof(uint32_t),void>::type
to_big_endian(T val, OutputIt d_first)
{
    T x = JSONCONS_H_TO_BE32(val);

    uint8_t where[sizeof(T)];
    std::memcpy(where, &x, sizeof(T));

    *d_first++ = where[0];
    *d_first++ = where[1];
    *d_first++ = where[2];
    *d_first++ = where[3];
}

template<typename T, class OutputIt>
typename std::enable_if<std::is_integral<T>::value && 
sizeof(T) == sizeof(uint64_t),void>::type
to_big_endian(T val, OutputIt d_first)
{
    T x = JSONCONS_H_TO_BE64(val);

    uint8_t where[sizeof(T)];
    std::memcpy(where, &x, sizeof(T));

    *d_first++ = where[0];
    *d_first++ = where[1];
    *d_first++ = where[2];
    *d_first++ = where[3];
    *d_first++ = where[4];
    *d_first++ = where[5];
    *d_first++ = where[6];
    *d_first++ = where[7];
}

template<class OutputIt>
void to_big_endian(float val, OutputIt d_first)
{
    uint32_t where;
    std::memcpy(&where,&val,sizeof(val));
    to_big_endian(where, d_first);
}

template<class OutputIt>
void to_big_endian(double val, OutputIt d_first)
{
    uint64_t where;
    std::memcpy(&where,&val,sizeof(val));
    to_big_endian(where, d_first);
}

// to_little_endian

template<typename T, class OutputIt>
typename std::enable_if<std::is_integral<T>::value && 
sizeof(T) == sizeof(uint32_t),void>::type
to_little_endian(T val, OutputIt d_first)
{
    T x = JSONCONS_H_TO_LE32(val);

    uint8_t where[sizeof(T)];
    std::memcpy(where, &x, sizeof(T));

    *d_first++ = where[0];
    *d_first++ = where[1];
    *d_first++ = where[2];
    *d_first++ = where[3];
}

template<typename T, class OutputIt>
typename std::enable_if<std::is_integral<T>::value && 
sizeof(T) == sizeof(uint64_t),void>::type
to_little_endian(T val, OutputIt d_first)
{
    T x = JSONCONS_H_TO_LE64(val);

    uint8_t where[sizeof(T)];
    std::memcpy(where, &x, sizeof(T));

    *d_first++ = where[0];
    *d_first++ = where[1];
    *d_first++ = where[2];
    *d_first++ = where[3];
    *d_first++ = where[4];
    *d_first++ = where[5];
    *d_first++ = where[6];
    *d_first++ = where[7];
}

template<class OutputIt>
void to_little_endian(float val, OutputIt d_first)
{
    uint32_t where;
    std::memcpy(&where,&val,sizeof(val));
    to_little_endian(where, d_first);
}

template<class OutputIt>
void to_little_endian(double val, OutputIt d_first)
{
    uint64_t where;
    std::memcpy(&where,&val,sizeof(val));
    to_little_endian(where, d_first);
}

// from_big_endian

template<class T>
typename std::enable_if<std::is_integral<T>::value && 
sizeof(T) == sizeof(uint8_t),T>::type
from_big_endian(const uint8_t* first, const uint8_t* last, const uint8_t** endp)
{
    if (first + sizeof(T) > last)
    {
        *endp = first;
        return 0;
    }
    else
    {
        *endp = first + sizeof(T);
        return static_cast<T>(*(first));
    }
}

template<class T>
typename std::enable_if<std::is_integral<T>::value && 
sizeof(T) == sizeof(uint16_t),T>::type
from_big_endian(const uint8_t* first, const uint8_t* last, const uint8_t** endp)
{
    if (first + sizeof(T) > last)
    {
        *endp = first;
        return 0;
    }
    else
    {
        *endp = first + sizeof(T);
        T val;
        std::memcpy(&val,first,sizeof(T));
        return JSONCONS_BE16_TO_H(val);
    }
}
 
template<class T>
typename std::enable_if<std::is_integral<T>::value && sizeof(T) == sizeof(uint32_t),T>::type
from_big_endian(const uint8_t* first, const uint8_t* last, const uint8_t** endp)
{
    if (first + sizeof(T) > last)
    {
        *endp = first;
        return 0;
    }
    else
    {
        *endp = first + sizeof(T);
        T val;
        std::memcpy(&val,first,sizeof(T));
        return JSONCONS_BE32_TO_H(val);
    }
}

template<class T>
typename std::enable_if<std::is_integral<T>::value && sizeof(T) == sizeof(uint64_t),T>::type
from_big_endian(const uint8_t* first, const uint8_t* last, const uint8_t** endp)
{
    if (first + sizeof(T) > last)
    {
        *endp = first;
        return 0;
    }
    else
    {
        *endp = first + sizeof(T);
        T val;
        std::memcpy(&val,first,sizeof(T));
        return JSONCONS_BE64_TO_H(val);
    }
}

template<class T>
typename std::enable_if<std::is_floating_point<T>::value && 
sizeof(T) == sizeof(uint32_t),T>::type
from_big_endian(const uint8_t* first, const uint8_t* last, const uint8_t** endp)
{
    uint32_t data = from_big_endian<uint32_t>(first,last,endp);
    T val;
    std::memcpy(&val,&data,sizeof(T));
    return val;
}

template<class T>
typename std::enable_if<std::is_floating_point<T>::value && 
sizeof(T) == sizeof(uint64_t),T>::type
from_big_endian(const uint8_t* first, const uint8_t* last, const uint8_t** endp)
{
    uint64_t data = from_big_endian<uint64_t>(first,last,endp);
    T val;
    std::memcpy(&val,&data,sizeof(T));
    return val;
}

// from_little_endian

template<class T>
typename std::enable_if<std::is_integral<T>::value && 
sizeof(T) == sizeof(uint8_t),T>::type
from_little_endian(const uint8_t* first, const uint8_t* last, const uint8_t** endp)
{
    if (first + sizeof(T) > last)
    {
        *endp = first;
        return 0;
    }
    else
    {
        *endp = first + sizeof(T);
        return static_cast<T>(*(first));
    }
}

template<class T>
typename std::enable_if<std::is_integral<T>::value && 
sizeof(T) == sizeof(uint16_t),T>::type
from_little_endian(const uint8_t* first, const uint8_t* last, const uint8_t** endp)
{
    if (first + sizeof(T) > last)
    {
        *endp = first;
        return 0;
    }
    else
    {
        *endp = first + sizeof(T);
        T val;
        std::memcpy(&val,first,sizeof(T));
        return JSONCONS_LE16_TO_H(val);
    }
}

template<class T>
typename std::enable_if<std::is_integral<T>::value && sizeof(T) == sizeof(uint32_t),T>::type
from_little_endian(const uint8_t* first, const uint8_t* last, const uint8_t** endp)
{
    if (first + sizeof(T) > last)
    {
        *endp = first;
        return 0;
    }
    else
    {
        *endp = first + sizeof(T);
        T val;
        std::memcpy(&val,first,sizeof(T));
        return JSONCONS_LE32_TO_H(val);
    }
}

template<class T>
typename std::enable_if<std::is_integral<T>::value && sizeof(T) == sizeof(uint64_t),T>::type
from_little_endian(const uint8_t* first, const uint8_t* last, const uint8_t** endp)
{
    if (first + sizeof(T) > last)
    {
        *endp = first;
        return 0;
    }
    else
    {
        *endp = first + sizeof(T);
        T val;
        std::memcpy(&val,first,sizeof(T));
        return JSONCONS_LE64_TO_H(val);
    }
}

template<class T>
typename std::enable_if<std::is_floating_point<T>::value && 
sizeof(T) == sizeof(uint32_t),T>::type
from_little_endian(const uint8_t* first, const uint8_t* last, const uint8_t** endp)
{
    uint32_t data = from_little_endian<uint32_t>(first,last,endp);
    T val;
    std::memcpy(&val,&data,sizeof(T));
    return val;
}

template<class T>
typename std::enable_if<std::is_floating_point<T>::value && 
sizeof(T) == sizeof(uint64_t),T>::type
from_little_endian(const uint8_t* first, const uint8_t* last, const uint8_t** endp)
{
    uint64_t data = from_little_endian<uint64_t>(first,last,endp);
    T val;
    std::memcpy(&val,&data,sizeof(T));
    return val;
}

}}

#endif
