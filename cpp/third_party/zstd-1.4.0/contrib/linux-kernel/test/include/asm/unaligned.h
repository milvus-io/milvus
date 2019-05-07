#ifndef ASM_UNALIGNED_H
#define ASM_UNALIGNED_H

#include <assert.h>
#include <linux/string.h>
#include <linux/types.h>

#define _LITTLE_ENDIAN 1

static unsigned _isLittleEndian(void)
{
    const union { uint32_t u; uint8_t c[4]; } one = { 1 };
    assert(_LITTLE_ENDIAN == one.c[0]);
    return _LITTLE_ENDIAN;
}

static uint16_t _swap16(uint16_t in)
{
    return ((in & 0xF) << 8) + ((in & 0xF0) >> 8);
}

static uint32_t _swap32(uint32_t in)
{
    return __builtin_bswap32(in);
}

static uint64_t _swap64(uint64_t in)
{
    return __builtin_bswap64(in);
}

/* Little endian */
static uint16_t get_unaligned_le16(const void* memPtr)
{
    uint16_t val;
    memcpy(&val, memPtr, sizeof(val));
    if (!_isLittleEndian()) _swap16(val);
    return val;
}

static uint32_t get_unaligned_le32(const void* memPtr)
{
    uint32_t val;
    memcpy(&val, memPtr, sizeof(val));
    if (!_isLittleEndian()) _swap32(val);
    return val;
}

static uint64_t get_unaligned_le64(const void* memPtr)
{
    uint64_t val;
    memcpy(&val, memPtr, sizeof(val));
    if (!_isLittleEndian()) _swap64(val);
    return val;
}

static void put_unaligned_le16(uint16_t value, void* memPtr)
{
    if (!_isLittleEndian()) value = _swap16(value);
    memcpy(memPtr, &value, sizeof(value));
}

static void put_unaligned_le32(uint32_t value, void* memPtr)
{
    if (!_isLittleEndian()) value = _swap32(value);
    memcpy(memPtr, &value, sizeof(value));
}

static void put_unaligned_le64(uint64_t value, void* memPtr)
{
    if (!_isLittleEndian()) value = _swap64(value);
    memcpy(memPtr, &value, sizeof(value));
}

/* big endian */
static uint32_t get_unaligned_be32(const void* memPtr)
{
    uint32_t val;
    memcpy(&val, memPtr, sizeof(val));
    if (_isLittleEndian()) _swap32(val);
    return val;
}

static uint64_t get_unaligned_be64(const void* memPtr)
{
    uint64_t val;
    memcpy(&val, memPtr, sizeof(val));
    if (_isLittleEndian()) _swap64(val);
    return val;
}

static void put_unaligned_be32(uint32_t value, void* memPtr)
{
    if (_isLittleEndian()) value = _swap32(value);
    memcpy(memPtr, &value, sizeof(value));
}

static void put_unaligned_be64(uint64_t value, void* memPtr)
{
    if (_isLittleEndian()) value = _swap64(value);
    memcpy(memPtr, &value, sizeof(value));
}

/* generic */
extern void __bad_unaligned_access_size(void);

#define __get_unaligned_le(ptr) ((typeof(*(ptr)))({                            \
    __builtin_choose_expr(sizeof(*(ptr)) == 1, *(ptr),                         \
    __builtin_choose_expr(sizeof(*(ptr)) == 2, get_unaligned_le16((ptr)),      \
    __builtin_choose_expr(sizeof(*(ptr)) == 4, get_unaligned_le32((ptr)),      \
    __builtin_choose_expr(sizeof(*(ptr)) == 8, get_unaligned_le64((ptr)),      \
    __bad_unaligned_access_size()))));                                         \
    }))

#define __get_unaligned_be(ptr) ((typeof(*(ptr)))({                            \
    __builtin_choose_expr(sizeof(*(ptr)) == 1, *(ptr),                         \
    __builtin_choose_expr(sizeof(*(ptr)) == 2, get_unaligned_be16((ptr)),      \
    __builtin_choose_expr(sizeof(*(ptr)) == 4, get_unaligned_be32((ptr)),      \
    __builtin_choose_expr(sizeof(*(ptr)) == 8, get_unaligned_be64((ptr)),      \
    __bad_unaligned_access_size()))));                                         \
    }))

#define __put_unaligned_le(val, ptr)                                           \
  ({                                                                           \
    void *__gu_p = (ptr);                                                      \
    switch (sizeof(*(ptr))) {                                                  \
    case 1:                                                                    \
      *(uint8_t *)__gu_p = (uint8_t)(val);                                     \
      break;                                                                   \
    case 2:                                                                    \
      put_unaligned_le16((uint16_t)(val), __gu_p);                             \
      break;                                                                   \
    case 4:                                                                    \
      put_unaligned_le32((uint32_t)(val), __gu_p);                             \
      break;                                                                   \
    case 8:                                                                    \
      put_unaligned_le64((uint64_t)(val), __gu_p);                             \
      break;                                                                   \
    default:                                                                   \
      __bad_unaligned_access_size();                                           \
      break;                                                                   \
    }                                                                          \
    (void)0;                                                                   \
  })

#define __put_unaligned_be(val, ptr)                                           \
  ({                                                                           \
    void *__gu_p = (ptr);                                                      \
    switch (sizeof(*(ptr))) {                                                  \
    case 1:                                                                    \
      *(uint8_t *)__gu_p = (uint8_t)(val);                                     \
      break;                                                                   \
    case 2:                                                                    \
      put_unaligned_be16((uint16_t)(val), __gu_p);                             \
      break;                                                                   \
    case 4:                                                                    \
      put_unaligned_be32((uint32_t)(val), __gu_p);                             \
      break;                                                                   \
    case 8:                                                                    \
      put_unaligned_be64((uint64_t)(val), __gu_p);                             \
      break;                                                                   \
    default:                                                                   \
      __bad_unaligned_access_size();                                           \
      break;                                                                   \
    }                                                                          \
    (void)0;                                                                   \
  })

#if _LITTLE_ENDIAN
#  define get_unaligned __get_unaligned_le
#  define put_unaligned __put_unaligned_le
#else
#  define get_unaligned __get_unaligned_be
#  define put_unaligned __put_unaligned_be
#endif

#endif // ASM_UNALIGNED_H
