/*
xxHash - Fast Hash algorithm
Copyright (C) 2012-2014, Yann Collet.
BSD 2-Clause License (http://www.opensource.org/licenses/bsd-license.php)

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

* Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
* Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

You can contact the author at :
- xxHash source repository : http://code.google.com/p/xxhash/
*/


//**************************************
// Tuning parameters
//**************************************
/*!XXH_FORCE_MEMORY_ACCESS :
 * By default, access to unaligned memory is controlled by `memcpy()`, which is
 * safe and portable. Unfortunately, on some target/compiler combinations, the
 * generated assembly is sub-optimal. The below switch allow to select different
 * access method for improved performance. Method 0 (default) : use `memcpy()`.
 * Safe and portable. Method 1 : `__packed` statement. It depends on compiler
 * extension (ie, not portable). This method is safe if your compiler supports
 * it, and *generally* as fast or faster than `memcpy`. Method 2 : direct
 * access. This method doesn't depend on compiler but violate C standard. It can
 * generate buggy code on targets which do not support unaligned memory
 * accesses. But in some circumstances, it's the only known way to get the most
 * performance (ie GCC + ARMv6) See http://stackoverflow.com/a/32095106/646947
 * for details. Prefer these methods in priority order (0 > 1 > 2)
 */

#include "util/util.h"

#ifndef XXH_FORCE_MEMORY_ACCESS /* can be defined externally, on command line \
                                   for example */
#if defined(__GNUC__) &&                                     \
    (defined(__ARM_ARCH_6__) || defined(__ARM_ARCH_6J__) ||  \
     defined(__ARM_ARCH_6K__) || defined(__ARM_ARCH_6Z__) || \
     defined(__ARM_ARCH_6ZK__) || defined(__ARM_ARCH_6T2__))
#define XXH_FORCE_MEMORY_ACCESS 2
#elif (defined(__INTEL_COMPILER) && !defined(_WIN32)) ||      \
    (defined(__GNUC__) &&                                     \
     (defined(__ARM_ARCH_7__) || defined(__ARM_ARCH_7A__) ||  \
      defined(__ARM_ARCH_7R__) || defined(__ARM_ARCH_7M__) || \
      defined(__ARM_ARCH_7S__)))
#define XXH_FORCE_MEMORY_ACCESS 1
#endif
#endif

// Unaligned memory access is automatically enabled for "common" CPU, such as x86.
// For others CPU, the compiler will be more cautious, and insert extra code to ensure aligned access is respected.
// If you know your target CPU supports unaligned memory access, you want to force this option manually to improve performance.
// You can also enable this parameter if you know your input data will always be aligned (boundaries of 4, for U32).
#if defined(__ARM_FEATURE_UNALIGNED) || defined(__i386) || defined(_M_IX86) || defined(__x86_64__) || defined(_M_X64)
#  define XXH_USE_UNALIGNED_ACCESS 1
#endif

// XXH_ACCEPT_NULL_INPUT_POINTER :
// If the input pointer is a null pointer, xxHash default behavior is to trigger a memory access error, since it is a bad pointer.
// When this option is enabled, xxHash output for null input pointers will be the same as a null-length input.
// This option has a very small performance cost (only measurable on small inputs).
// By default, this option is disabled. To enable it, uncomment below define :
//#define XXH_ACCEPT_NULL_INPUT_POINTER 1

// XXH_FORCE_NATIVE_FORMAT :
// By default, xxHash library provides endian-independent Hash values, based on little-endian convention.
// Results are therefore identical for little-endian and big-endian CPU.
// This comes at a performance cost for big-endian CPU, since some swapping is required to emulate little-endian format.
// Should endian-independence be of no importance for your application, you may set the #define below to 1.
// It will improve speed for Big-endian CPU.
// This option has no impact on Little_Endian CPU.
#define XXH_FORCE_NATIVE_FORMAT 0

/*!XXH_FORCE_ALIGN_CHECK :
 * This is a minor performance trick, only useful with lots of very small keys.
 * It means : check for aligned/unaligned input.
 * The check costs one initial branch per hash;
 * set it to 0 when the input is guaranteed to be aligned,
 * or when alignment doesn't matter for performance.
 */
#ifndef XXH_FORCE_ALIGN_CHECK /* can be defined externally */
#if defined(__i386) || defined(_M_IX86) || defined(__x86_64__) || \
    defined(_M_X64)
#define XXH_FORCE_ALIGN_CHECK 0
#else
#define XXH_FORCE_ALIGN_CHECK 1
#endif
#endif

//**************************************
// Compiler Specific Options
//**************************************
// Disable some Visual warning messages
#ifdef _MSC_VER  // Visual Studio
#  pragma warning(disable : 4127)      // disable: C4127: conditional expression is constant
#  pragma warning(disable : 4804)      // disable: C4804: 'operation' : unsafe use of type 'bool' in operation (static assert line 313)
#endif

#ifdef _MSC_VER    // Visual Studio
#  define FORCE_INLINE static __forceinline
#else
#  ifdef __GNUC__
#    define FORCE_INLINE static inline __attribute__((always_inline))
#  else
#    define FORCE_INLINE static inline
#  endif
#endif


//**************************************
// Includes & Memory related functions
//**************************************
#include "xxhash.h"
// Modify the local functions below should you wish to use some other memory related routines
// for malloc(), free()
#include <stdlib.h>
FORCE_INLINE void* XXH_malloc(size_t s) { return malloc(s); }
FORCE_INLINE void  XXH_free  (void* p)  { free(p); }
// for memcpy()
#include <string.h>
FORCE_INLINE void* XXH_memcpy(void* dest, const void* src, size_t size) { return memcpy(dest,src,size); }
#include <assert.h> /* assert */

namespace rocksdb {
//**************************************
// Basic Types
//**************************************
#if defined (__STDC_VERSION__) && __STDC_VERSION__ >= 199901L   // C99
# include <stdint.h>
  typedef uint8_t  BYTE;
  typedef uint16_t U16;
  typedef uint32_t U32;
  typedef  int32_t S32;
  typedef uint64_t U64;
#else
  typedef unsigned char      BYTE;
  typedef unsigned short     U16;
  typedef unsigned int       U32;
  typedef   signed int       S32;
  typedef unsigned long long U64;
#endif

#if defined(__GNUC__)  && !defined(XXH_USE_UNALIGNED_ACCESS)
#  define _PACKED __attribute__ ((packed))
#else
#  define _PACKED
#endif

#if !defined(XXH_USE_UNALIGNED_ACCESS) && !defined(__GNUC__)
#  ifdef __IBMC__
#    pragma pack(1)
#  else
#    pragma pack(push, 1)
#  endif
#endif

typedef struct _U32_S { U32 v; } _PACKED U32_S;

#if !defined(XXH_USE_UNALIGNED_ACCESS) && !defined(__GNUC__)
#  pragma pack(pop)
#endif

#define A32(x) (((U32_S *)(x))->v)

#if (defined(XXH_FORCE_MEMORY_ACCESS) && (XXH_FORCE_MEMORY_ACCESS == 2))

/* Force direct memory access. Only works on CPU which support unaligned memory
 * access in hardware */
static U32 XXH_read32(const void* memPtr) { return *(const U32*)memPtr; }

#elif (defined(XXH_FORCE_MEMORY_ACCESS) && (XXH_FORCE_MEMORY_ACCESS == 1))

/* __pack instructions are safer, but compiler specific, hence potentially
 * problematic for some compilers */
/* currently only defined for gcc and icc */
typedef union {
  U32 u32;
} __attribute__((packed)) unalign;
static U32 XXH_read32(const void* ptr) { return ((const unalign*)ptr)->u32; }

#else

/* portable and safe solution. Generally efficient.
 * see : http://stackoverflow.com/a/32095106/646947
 */
static U32 XXH_read32(const void* memPtr) {
  U32 val;
  memcpy(&val, memPtr, sizeof(val));
  return val;
}

#endif /* XXH_FORCE_DIRECT_MEMORY_ACCESS */

//***************************************
// Compiler-specific Functions and Macros
//***************************************
#define GCC_VERSION (__GNUC__ * 100 + __GNUC_MINOR__)

// Note : although _rotl exists for minGW (GCC under windows), performance seems poor
#if defined(_MSC_VER)
#  define XXH_rotl32(x,r) _rotl(x,r)
#define XXH_rotl64(x, r) _rotl64(x, r)
#else
#  define XXH_rotl32(x,r) ((x << r) | (x >> (32 - r)))
#define XXH_rotl64(x, r) ((x << r) | (x >> (64 - r)))
#endif

#if defined(_MSC_VER)     // Visual Studio
#  define XXH_swap32 _byteswap_ulong
#elif GCC_VERSION >= 403
#  define XXH_swap32 __builtin_bswap32
#else
static inline U32 XXH_swap32 (U32 x) {
    return  ((x << 24) & 0xff000000 ) |
        ((x <<  8) & 0x00ff0000 ) |
        ((x >>  8) & 0x0000ff00 ) |
        ((x >> 24) & 0x000000ff );}
#endif


//**************************************
// Constants
//**************************************
#define PRIME32_1   2654435761U
#define PRIME32_2   2246822519U
#define PRIME32_3   3266489917U
#define PRIME32_4    668265263U
#define PRIME32_5    374761393U


//**************************************
// Architecture Macros
//**************************************
typedef enum { XXH_bigEndian=0, XXH_littleEndian=1 } XXH_endianess;
#ifndef XXH_CPU_LITTLE_ENDIAN   // It is possible to define XXH_CPU_LITTLE_ENDIAN externally, for example using a compiler switch
    static const int one = 1;
#   define XXH_CPU_LITTLE_ENDIAN   (*(char*)(&one))
#endif


//**************************************
// Macros
//**************************************
#define XXH_STATIC_ASSERT(c)   { enum { XXH_static_assert = 1/(!!(c)) }; }    // use only *after* variable declarations


//****************************
// Memory reads
//****************************
typedef enum { XXH_aligned, XXH_unaligned } XXH_alignment;

FORCE_INLINE U32 XXH_readLE32_align(const U32* ptr, XXH_endianess endian, XXH_alignment align)
{
    if (align==XXH_unaligned)
        return endian==XXH_littleEndian ? A32(ptr) : XXH_swap32(A32(ptr));
    else
        return endian==XXH_littleEndian ? *ptr : XXH_swap32(*ptr);
}

FORCE_INLINE U32 XXH_readLE32_align(const void* ptr, XXH_endianess endian,
                                    XXH_alignment align) {
  if (align == XXH_unaligned)
    return endian == XXH_littleEndian ? XXH_read32(ptr)
                                      : XXH_swap32(XXH_read32(ptr));
  else
    return endian == XXH_littleEndian ? *(const U32*)ptr
                                      : XXH_swap32(*(const U32*)ptr);
}

FORCE_INLINE U32 XXH_readLE32(const U32* ptr, XXH_endianess endian) {
  return XXH_readLE32_align(ptr, endian, XXH_unaligned);
}

//****************************
// Simple Hash Functions
//****************************
#define XXH_get32bits(p) XXH_readLE32_align(p, endian, align)

FORCE_INLINE U32 XXH32_endian_align(const void* input, int len, U32 seed, XXH_endianess endian, XXH_alignment align)
{
    const BYTE* p = (const BYTE*)input;
    const BYTE* const bEnd = p + len;
    U32 h32;

#ifdef XXH_ACCEPT_NULL_INPUT_POINTER
    if (p==NULL) { len=0; p=(const BYTE*)(size_t)16; }
#endif

    if (len>=16)
    {
        const BYTE* const limit = bEnd - 16;
        U32 v1 = seed + PRIME32_1 + PRIME32_2;
        U32 v2 = seed + PRIME32_2;
        U32 v3 = seed + 0;
        U32 v4 = seed - PRIME32_1;

        do
        {
            v1 += XXH_readLE32_align((const U32*)p, endian, align) * PRIME32_2; v1 = XXH_rotl32(v1, 13); v1 *= PRIME32_1; p+=4;
            v2 += XXH_readLE32_align((const U32*)p, endian, align) * PRIME32_2; v2 = XXH_rotl32(v2, 13); v2 *= PRIME32_1; p+=4;
            v3 += XXH_readLE32_align((const U32*)p, endian, align) * PRIME32_2; v3 = XXH_rotl32(v3, 13); v3 *= PRIME32_1; p+=4;
            v4 += XXH_readLE32_align((const U32*)p, endian, align) * PRIME32_2; v4 = XXH_rotl32(v4, 13); v4 *= PRIME32_1; p+=4;
        } while (p<=limit);

        h32 = XXH_rotl32(v1, 1) + XXH_rotl32(v2, 7) + XXH_rotl32(v3, 12) + XXH_rotl32(v4, 18);
    }
    else
    {
        h32  = seed + PRIME32_5;
    }

    h32 += (U32) len;

    while (p<=bEnd-4)
    {
        h32 += XXH_readLE32_align((const U32*)p, endian, align) * PRIME32_3;
        h32  = XXH_rotl32(h32, 17) * PRIME32_4 ;
        p+=4;
    }

    while (p<bEnd)
    {
        h32 += (*p) * PRIME32_5;
        h32 = XXH_rotl32(h32, 11) * PRIME32_1 ;
        p++;
    }

    h32 ^= h32 >> 15;
    h32 *= PRIME32_2;
    h32 ^= h32 >> 13;
    h32 *= PRIME32_3;
    h32 ^= h32 >> 16;

    return h32;
}


U32 XXH32(const void* input, int len, U32 seed)
{
#if 0
    // Simple version, good for code maintenance, but unfortunately slow for small inputs
    void* state = XXH32_init(seed);
    XXH32_update(state, input, len);
    return XXH32_digest(state);
#else
    XXH_endianess endian_detected = (XXH_endianess)XXH_CPU_LITTLE_ENDIAN;

#  if !defined(XXH_USE_UNALIGNED_ACCESS)
    if ((((size_t)input) & 3))   // Input is aligned, let's leverage the speed advantage
    {
        if ((endian_detected==XXH_littleEndian) || XXH_FORCE_NATIVE_FORMAT)
            return XXH32_endian_align(input, len, seed, XXH_littleEndian, XXH_aligned);
        else
            return XXH32_endian_align(input, len, seed, XXH_bigEndian, XXH_aligned);
    }
#  endif

    if ((endian_detected==XXH_littleEndian) || XXH_FORCE_NATIVE_FORMAT)
        return XXH32_endian_align(input, len, seed, XXH_littleEndian, XXH_unaligned);
    else
        return XXH32_endian_align(input, len, seed, XXH_bigEndian, XXH_unaligned);
#endif
}


//****************************
// Advanced Hash Functions
//****************************

struct XXH_state32_t
{
    U64 total_len;
    U32 seed;
    U32 v1;
    U32 v2;
    U32 v3;
    U32 v4;
    int memsize;
    char memory[16];
};


int XXH32_sizeofState()
{
    XXH_STATIC_ASSERT(XXH32_SIZEOFSTATE >= sizeof(struct XXH_state32_t));   // A compilation error here means XXH32_SIZEOFSTATE is not large enough
    return sizeof(struct XXH_state32_t);
}


XXH_errorcode XXH32_resetState(void* state_in, U32 seed)
{
    struct XXH_state32_t * state = (struct XXH_state32_t *) state_in;
    state->seed = seed;
    state->v1 = seed + PRIME32_1 + PRIME32_2;
    state->v2 = seed + PRIME32_2;
    state->v3 = seed + 0;
    state->v4 = seed - PRIME32_1;
    state->total_len = 0;
    state->memsize = 0;
    return XXH_OK;
}


void* XXH32_init (U32 seed)
{
    void* state = XXH_malloc (sizeof(struct XXH_state32_t));
    XXH32_resetState(state, seed);
    return state;
}


FORCE_INLINE XXH_errorcode XXH32_update_endian (void* state_in, const void* input, int len, XXH_endianess endian)
{
    struct XXH_state32_t * state = (struct XXH_state32_t *) state_in;
    const BYTE* p = (const BYTE*)input;
    const BYTE* const bEnd = p + len;

#ifdef XXH_ACCEPT_NULL_INPUT_POINTER
    if (input==NULL) return XXH_ERROR;
#endif

    state->total_len += len;

    if (state->memsize + len < 16)   // fill in tmp buffer
    {
        XXH_memcpy(state->memory + state->memsize, input, len);
        state->memsize +=  len;
        return XXH_OK;
    }

    if (state->memsize)   // some data left from previous update
    {
        XXH_memcpy(state->memory + state->memsize, input, 16-state->memsize);
        {
            const U32* p32 = (const U32*)state->memory;
            state->v1 += XXH_readLE32(p32, endian) * PRIME32_2; state->v1 = XXH_rotl32(state->v1, 13); state->v1 *= PRIME32_1; p32++;
            state->v2 += XXH_readLE32(p32, endian) * PRIME32_2; state->v2 = XXH_rotl32(state->v2, 13); state->v2 *= PRIME32_1; p32++;
            state->v3 += XXH_readLE32(p32, endian) * PRIME32_2; state->v3 = XXH_rotl32(state->v3, 13); state->v3 *= PRIME32_1; p32++;
            state->v4 += XXH_readLE32(p32, endian) * PRIME32_2; state->v4 = XXH_rotl32(state->v4, 13); state->v4 *= PRIME32_1; p32++;
        }
        p += 16-state->memsize;
        state->memsize = 0;
    }

    if (p <= bEnd-16)
    {
        const BYTE* const limit = bEnd - 16;
        U32 v1 = state->v1;
        U32 v2 = state->v2;
        U32 v3 = state->v3;
        U32 v4 = state->v4;

        do
        {
            v1 += XXH_readLE32((const U32*)p, endian) * PRIME32_2; v1 = XXH_rotl32(v1, 13); v1 *= PRIME32_1; p+=4;
            v2 += XXH_readLE32((const U32*)p, endian) * PRIME32_2; v2 = XXH_rotl32(v2, 13); v2 *= PRIME32_1; p+=4;
            v3 += XXH_readLE32((const U32*)p, endian) * PRIME32_2; v3 = XXH_rotl32(v3, 13); v3 *= PRIME32_1; p+=4;
            v4 += XXH_readLE32((const U32*)p, endian) * PRIME32_2; v4 = XXH_rotl32(v4, 13); v4 *= PRIME32_1; p+=4;
        } while (p<=limit);

        state->v1 = v1;
        state->v2 = v2;
        state->v3 = v3;
        state->v4 = v4;
    }

    if (p < bEnd)
    {
        XXH_memcpy(state->memory, p, bEnd-p);
        state->memsize = (int)(bEnd-p);
    }

    return XXH_OK;
}

XXH_errorcode XXH32_update (void* state_in, const void* input, int len)
{
    XXH_endianess endian_detected = (XXH_endianess)XXH_CPU_LITTLE_ENDIAN;

    if ((endian_detected==XXH_littleEndian) || XXH_FORCE_NATIVE_FORMAT)
        return XXH32_update_endian(state_in, input, len, XXH_littleEndian);
    else
        return XXH32_update_endian(state_in, input, len, XXH_bigEndian);
}



FORCE_INLINE U32 XXH32_intermediateDigest_endian (void* state_in, XXH_endianess endian)
{
    struct XXH_state32_t * state = (struct XXH_state32_t *) state_in;
    const BYTE * p = (const BYTE*)state->memory;
    BYTE* bEnd = (BYTE*)state->memory + state->memsize;
    U32 h32;

    if (state->total_len >= 16)
    {
        h32 = XXH_rotl32(state->v1, 1) + XXH_rotl32(state->v2, 7) + XXH_rotl32(state->v3, 12) + XXH_rotl32(state->v4, 18);
    }
    else
    {
        h32  = state->seed + PRIME32_5;
    }

    h32 += (U32) state->total_len;

    while (p<=bEnd-4)
    {
        h32 += XXH_readLE32((const U32*)p, endian) * PRIME32_3;
        h32  = XXH_rotl32(h32, 17) * PRIME32_4;
        p+=4;
    }

    while (p<bEnd)
    {
        h32 += (*p) * PRIME32_5;
        h32 = XXH_rotl32(h32, 11) * PRIME32_1;
        p++;
    }

    h32 ^= h32 >> 15;
    h32 *= PRIME32_2;
    h32 ^= h32 >> 13;
    h32 *= PRIME32_3;
    h32 ^= h32 >> 16;

    return h32;
}


U32 XXH32_intermediateDigest (void* state_in)
{
    XXH_endianess endian_detected = (XXH_endianess)XXH_CPU_LITTLE_ENDIAN;

    if ((endian_detected==XXH_littleEndian) || XXH_FORCE_NATIVE_FORMAT)
        return XXH32_intermediateDigest_endian(state_in, XXH_littleEndian);
    else
        return XXH32_intermediateDigest_endian(state_in, XXH_bigEndian);
}


U32 XXH32_digest (void* state_in)
{
    U32 h32 = XXH32_intermediateDigest(state_in);

    XXH_free(state_in);

    return h32;
}

/* *******************************************************************
 *  64-bit hash functions
 *********************************************************************/

 #if (defined(XXH_FORCE_MEMORY_ACCESS) && (XXH_FORCE_MEMORY_ACCESS==2))

 /* Force direct memory access. Only works on CPU which support unaligned memory access in hardware */
 static U64 XXH_read64(const void* memPtr) { return *(const U64*) memPtr; }

 #elif (defined(XXH_FORCE_MEMORY_ACCESS) && (XXH_FORCE_MEMORY_ACCESS==1))

 /* __pack instructions are safer, but compiler specific, hence potentially problematic for some compilers */
 /* currently only defined for gcc and icc */
 typedef union { U32 u32; U64 u64; } __attribute__((packed)) unalign64;
 static U64 XXH_read64(const void* ptr) { return ((const unalign64*)ptr)->u64; }

 #else

 /* portable and safe solution. Generally efficient.
  * see : http://stackoverflow.com/a/32095106/646947
  */

 static U64 XXH_read64(const void* memPtr)
 {
     U64 val;
     memcpy(&val, memPtr, sizeof(val));
     return val;
 }
#endif   /* XXH_FORCE_DIRECT_MEMORY_ACCESS */

#if defined(_MSC_VER) /* Visual Studio */
#define XXH_swap64 _byteswap_uint64
#elif XXH_GCC_VERSION >= 403
#define XXH_swap64 __builtin_bswap64
#else
static U64 XXH_swap64(U64 x) {
  return ((x << 56) & 0xff00000000000000ULL) |
         ((x << 40) & 0x00ff000000000000ULL) |
         ((x << 24) & 0x0000ff0000000000ULL) |
         ((x << 8) & 0x000000ff00000000ULL) |
         ((x >> 8) & 0x00000000ff000000ULL) |
         ((x >> 24) & 0x0000000000ff0000ULL) |
         ((x >> 40) & 0x000000000000ff00ULL) |
         ((x >> 56) & 0x00000000000000ffULL);
}
#endif

FORCE_INLINE U64 XXH_readLE64_align(const void* ptr, XXH_endianess endian,
                                    XXH_alignment align) {
  if (align == XXH_unaligned)
    return endian == XXH_littleEndian ? XXH_read64(ptr)
                                      : XXH_swap64(XXH_read64(ptr));
  else
    return endian == XXH_littleEndian ? *(const U64*)ptr
                                      : XXH_swap64(*(const U64*)ptr);
}

FORCE_INLINE U64 XXH_readLE64(const void* ptr, XXH_endianess endian) {
  return XXH_readLE64_align(ptr, endian, XXH_unaligned);
}

static U64 XXH_readBE64(const void* ptr) {
  return XXH_CPU_LITTLE_ENDIAN ? XXH_swap64(XXH_read64(ptr)) : XXH_read64(ptr);
}

/*======   xxh64   ======*/

static const U64 PRIME64_1 =
    11400714785074694791ULL; /* 0b1001111000110111011110011011000110000101111010111100101010000111
                              */
static const U64 PRIME64_2 =
    14029467366897019727ULL; /* 0b1100001010110010101011100011110100100111110101001110101101001111
                              */
static const U64 PRIME64_3 =
    1609587929392839161ULL; /* 0b0001011001010110011001111011000110011110001101110111100111111001
                             */
static const U64 PRIME64_4 =
    9650029242287828579ULL; /* 0b1000010111101011110010100111011111000010101100101010111001100011
                             */
static const U64 PRIME64_5 =
    2870177450012600261ULL; /* 0b0010011111010100111010110010111100010110010101100110011111000101
                             */

static U64 XXH64_round(U64 acc, U64 input) {
  acc += input * PRIME64_2;
  acc = XXH_rotl64(acc, 31);
  acc *= PRIME64_1;
  return acc;
}

static U64 XXH64_mergeRound(U64 acc, U64 val) {
  val = XXH64_round(0, val);
  acc ^= val;
  acc = acc * PRIME64_1 + PRIME64_4;
  return acc;
}

static U64 XXH64_avalanche(U64 h64) {
  h64 ^= h64 >> 33;
  h64 *= PRIME64_2;
  h64 ^= h64 >> 29;
  h64 *= PRIME64_3;
  h64 ^= h64 >> 32;
  return h64;
}

#define XXH_get64bits(p) XXH_readLE64_align(p, endian, align)

static U64 XXH64_finalize(U64 h64, const void* ptr, size_t len,
                          XXH_endianess endian, XXH_alignment align) {
  const BYTE* p = (const BYTE*)ptr;

#define PROCESS1_64          \
  h64 ^= (*p++) * PRIME64_5; \
  h64 = XXH_rotl64(h64, 11) * PRIME64_1;

#define PROCESS4_64                           \
  h64 ^= (U64)(XXH_get32bits(p)) * PRIME64_1; \
  p += 4;                                     \
  h64 = XXH_rotl64(h64, 23) * PRIME64_2 + PRIME64_3;

#define PROCESS8_64                                    \
  {                                                    \
    U64 const k1 = XXH64_round(0, XXH_get64bits(p));   \
    p += 8;                                            \
    h64 ^= k1;                                         \
    h64 = XXH_rotl64(h64, 27) * PRIME64_1 + PRIME64_4; \
  }

  switch (len & 31) {
    case 24:
      PROCESS8_64;
      FALLTHROUGH_INTENDED;
      /* fallthrough */
    case 16:
      PROCESS8_64;
      FALLTHROUGH_INTENDED;
      /* fallthrough */
    case 8:
      PROCESS8_64;
      return XXH64_avalanche(h64);

    case 28:
      PROCESS8_64;
      FALLTHROUGH_INTENDED;
      /* fallthrough */
    case 20:
      PROCESS8_64;
      FALLTHROUGH_INTENDED;
      /* fallthrough */
    case 12:
      PROCESS8_64;
      FALLTHROUGH_INTENDED;
      /* fallthrough */
    case 4:
      PROCESS4_64;
      return XXH64_avalanche(h64);

    case 25:
      PROCESS8_64;
      FALLTHROUGH_INTENDED;
      /* fallthrough */
    case 17:
      PROCESS8_64;
      FALLTHROUGH_INTENDED;
      /* fallthrough */
    case 9:
      PROCESS8_64;
      PROCESS1_64;
      return XXH64_avalanche(h64);

    case 29:
      PROCESS8_64;
      FALLTHROUGH_INTENDED;
      /* fallthrough */
    case 21:
      PROCESS8_64;
      FALLTHROUGH_INTENDED;
      /* fallthrough */
    case 13:
      PROCESS8_64;
      FALLTHROUGH_INTENDED;
      /* fallthrough */
    case 5:
      PROCESS4_64;
      PROCESS1_64;
      return XXH64_avalanche(h64);

    case 26:
      PROCESS8_64;
      FALLTHROUGH_INTENDED;
      /* fallthrough */
    case 18:
      PROCESS8_64;
      FALLTHROUGH_INTENDED;
      /* fallthrough */
    case 10:
      PROCESS8_64;
      PROCESS1_64;
      PROCESS1_64;
      return XXH64_avalanche(h64);

    case 30:
      PROCESS8_64;
      FALLTHROUGH_INTENDED;
      /* fallthrough */
    case 22:
      PROCESS8_64;
      FALLTHROUGH_INTENDED;
      /* fallthrough */
    case 14:
      PROCESS8_64;
      FALLTHROUGH_INTENDED;
      /* fallthrough */
    case 6:
      PROCESS4_64;
      PROCESS1_64;
      PROCESS1_64;
      return XXH64_avalanche(h64);

    case 27:
      PROCESS8_64;
      FALLTHROUGH_INTENDED;
      /* fallthrough */
    case 19:
      PROCESS8_64;
      FALLTHROUGH_INTENDED;
      /* fallthrough */
    case 11:
      PROCESS8_64;
      PROCESS1_64;
      PROCESS1_64;
      PROCESS1_64;
      return XXH64_avalanche(h64);

    case 31:
      PROCESS8_64;
      FALLTHROUGH_INTENDED;
      /* fallthrough */
    case 23:
      PROCESS8_64;
      FALLTHROUGH_INTENDED;
      /* fallthrough */
    case 15:
      PROCESS8_64;
      FALLTHROUGH_INTENDED;
      /* fallthrough */
    case 7:
      PROCESS4_64;
      FALLTHROUGH_INTENDED;
      /* fallthrough */
    case 3:
      PROCESS1_64;
      FALLTHROUGH_INTENDED;
      /* fallthrough */
    case 2:
      PROCESS1_64;
      FALLTHROUGH_INTENDED;
      /* fallthrough */
    case 1:
      PROCESS1_64;
      FALLTHROUGH_INTENDED;
      /* fallthrough */
    case 0:
      return XXH64_avalanche(h64);
  }

  /* impossible to reach */
  assert(0);
  return 0; /* unreachable, but some compilers complain without it */
}

FORCE_INLINE U64 XXH64_endian_align(const void* input, size_t len, U64 seed,
                                    XXH_endianess endian, XXH_alignment align) {
  const BYTE* p = (const BYTE*)input;
  const BYTE* bEnd = p + len;
  U64 h64;

#if defined(XXH_ACCEPT_NULL_INPUT_POINTER) && \
    (XXH_ACCEPT_NULL_INPUT_POINTER >= 1)
  if (p == NULL) {
    len = 0;
    bEnd = p = (const BYTE*)(size_t)32;
  }
#endif

  if (len >= 32) {
    const BYTE* const limit = bEnd - 32;
    U64 v1 = seed + PRIME64_1 + PRIME64_2;
    U64 v2 = seed + PRIME64_2;
    U64 v3 = seed + 0;
    U64 v4 = seed - PRIME64_1;

    do {
      v1 = XXH64_round(v1, XXH_get64bits(p));
      p += 8;
      v2 = XXH64_round(v2, XXH_get64bits(p));
      p += 8;
      v3 = XXH64_round(v3, XXH_get64bits(p));
      p += 8;
      v4 = XXH64_round(v4, XXH_get64bits(p));
      p += 8;
    } while (p <= limit);

    h64 = XXH_rotl64(v1, 1) + XXH_rotl64(v2, 7) + XXH_rotl64(v3, 12) +
          XXH_rotl64(v4, 18);
    h64 = XXH64_mergeRound(h64, v1);
    h64 = XXH64_mergeRound(h64, v2);
    h64 = XXH64_mergeRound(h64, v3);
    h64 = XXH64_mergeRound(h64, v4);

  } else {
    h64 = seed + PRIME64_5;
  }

  h64 += (U64)len;

  return XXH64_finalize(h64, p, len, endian, align);
}

unsigned long long XXH64(const void* input, size_t len,
                         unsigned long long seed) {
#if 0
    /* Simple version, good for code maintenance, but unfortunately slow for small inputs */
    XXH64_state_t state;
    XXH64_reset(&state, seed);
    XXH64_update(&state, input, len);
    return XXH64_digest(&state);
#else
  XXH_endianess endian_detected = (XXH_endianess)XXH_CPU_LITTLE_ENDIAN;

  if (XXH_FORCE_ALIGN_CHECK) {
    if ((((size_t)input) & 7) ==
        0) { /* Input is aligned, let's leverage the speed advantage */
      if ((endian_detected == XXH_littleEndian) || XXH_FORCE_NATIVE_FORMAT)
        return XXH64_endian_align(input, len, seed, XXH_littleEndian,
                                  XXH_aligned);
      else
        return XXH64_endian_align(input, len, seed, XXH_bigEndian, XXH_aligned);
    }
  }

  if ((endian_detected == XXH_littleEndian) || XXH_FORCE_NATIVE_FORMAT)
    return XXH64_endian_align(input, len, seed, XXH_littleEndian,
                              XXH_unaligned);
  else
    return XXH64_endian_align(input, len, seed, XXH_bigEndian, XXH_unaligned);
#endif
}

/*======   Hash Streaming   ======*/

XXH64_state_t* XXH64_createState(void) {
  return (XXH64_state_t*)XXH_malloc(sizeof(XXH64_state_t));
}
XXH_errorcode XXH64_freeState(XXH64_state_t* statePtr) {
  XXH_free(statePtr);
  return XXH_OK;
}

void XXH64_copyState(XXH64_state_t* dstState, const XXH64_state_t* srcState) {
  memcpy(dstState, srcState, sizeof(*dstState));
}

XXH_errorcode XXH64_reset(XXH64_state_t* statePtr, unsigned long long seed) {
  XXH64_state_t state; /* using a local state to memcpy() in order to avoid
                          strict-aliasing warnings */
  memset(&state, 0, sizeof(state));
  state.v1 = seed + PRIME64_1 + PRIME64_2;
  state.v2 = seed + PRIME64_2;
  state.v3 = seed + 0;
  state.v4 = seed - PRIME64_1;
  /* do not write into reserved, planned to be removed in a future version */
  memcpy(statePtr, &state, sizeof(state) - sizeof(state.reserved));
  return XXH_OK;
}

FORCE_INLINE XXH_errorcode XXH64_update_endian(XXH64_state_t* state,
                                               const void* input, size_t len,
                                               XXH_endianess endian) {
  if (input == NULL)
#if defined(XXH_ACCEPT_NULL_INPUT_POINTER) && \
    (XXH_ACCEPT_NULL_INPUT_POINTER >= 1)
    return XXH_OK;
#else
    return XXH_ERROR;
#endif

  {
    const BYTE* p = (const BYTE*)input;
    const BYTE* const bEnd = p + len;

    state->total_len += len;

    if (state->memsize + len < 32) { /* fill in tmp buffer */
      XXH_memcpy(((BYTE*)state->mem64) + state->memsize, input, len);
      state->memsize += (U32)len;
      return XXH_OK;
    }

    if (state->memsize) { /* tmp buffer is full */
      XXH_memcpy(((BYTE*)state->mem64) + state->memsize, input,
                 32 - state->memsize);
      state->v1 =
          XXH64_round(state->v1, XXH_readLE64(state->mem64 + 0, endian));
      state->v2 =
          XXH64_round(state->v2, XXH_readLE64(state->mem64 + 1, endian));
      state->v3 =
          XXH64_round(state->v3, XXH_readLE64(state->mem64 + 2, endian));
      state->v4 =
          XXH64_round(state->v4, XXH_readLE64(state->mem64 + 3, endian));
      p += 32 - state->memsize;
      state->memsize = 0;
    }

    if (p + 32 <= bEnd) {
      const BYTE* const limit = bEnd - 32;
      U64 v1 = state->v1;
      U64 v2 = state->v2;
      U64 v3 = state->v3;
      U64 v4 = state->v4;

      do {
        v1 = XXH64_round(v1, XXH_readLE64(p, endian));
        p += 8;
        v2 = XXH64_round(v2, XXH_readLE64(p, endian));
        p += 8;
        v3 = XXH64_round(v3, XXH_readLE64(p, endian));
        p += 8;
        v4 = XXH64_round(v4, XXH_readLE64(p, endian));
        p += 8;
      } while (p <= limit);

      state->v1 = v1;
      state->v2 = v2;
      state->v3 = v3;
      state->v4 = v4;
    }

    if (p < bEnd) {
      XXH_memcpy(state->mem64, p, (size_t)(bEnd - p));
      state->memsize = (unsigned)(bEnd - p);
    }
  }

  return XXH_OK;
}

XXH_errorcode XXH64_update(XXH64_state_t* state_in, const void* input,
                           size_t len) {
  XXH_endianess endian_detected = (XXH_endianess)XXH_CPU_LITTLE_ENDIAN;

  if ((endian_detected == XXH_littleEndian) || XXH_FORCE_NATIVE_FORMAT)
    return XXH64_update_endian(state_in, input, len, XXH_littleEndian);
  else
    return XXH64_update_endian(state_in, input, len, XXH_bigEndian);
}

FORCE_INLINE U64 XXH64_digest_endian(const XXH64_state_t* state,
                                     XXH_endianess endian) {
  U64 h64;

  if (state->total_len >= 32) {
    U64 const v1 = state->v1;
    U64 const v2 = state->v2;
    U64 const v3 = state->v3;
    U64 const v4 = state->v4;

    h64 = XXH_rotl64(v1, 1) + XXH_rotl64(v2, 7) + XXH_rotl64(v3, 12) +
          XXH_rotl64(v4, 18);
    h64 = XXH64_mergeRound(h64, v1);
    h64 = XXH64_mergeRound(h64, v2);
    h64 = XXH64_mergeRound(h64, v3);
    h64 = XXH64_mergeRound(h64, v4);
  } else {
    h64 = state->v3 /*seed*/ + PRIME64_5;
  }

  h64 += (U64)state->total_len;

  return XXH64_finalize(h64, state->mem64, (size_t)state->total_len, endian,
                        XXH_aligned);
}

unsigned long long XXH64_digest(const XXH64_state_t* state_in) {
  XXH_endianess endian_detected = (XXH_endianess)XXH_CPU_LITTLE_ENDIAN;

  if ((endian_detected == XXH_littleEndian) || XXH_FORCE_NATIVE_FORMAT)
    return XXH64_digest_endian(state_in, XXH_littleEndian);
  else
    return XXH64_digest_endian(state_in, XXH_bigEndian);
}

/*====== Canonical representation   ======*/

void XXH64_canonicalFromHash(XXH64_canonical_t* dst, XXH64_hash_t hash) {
  XXH_STATIC_ASSERT(sizeof(XXH64_canonical_t) == sizeof(XXH64_hash_t));
  if (XXH_CPU_LITTLE_ENDIAN) hash = XXH_swap64(hash);
  memcpy(dst, &hash, sizeof(*dst));
}

XXH64_hash_t XXH64_hashFromCanonical(const XXH64_canonical_t* src) {
  return XXH_readBE64(src);
}
}  // namespace rocksdb
