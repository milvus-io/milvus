#ifndef LINUX_MATH64_H
#define LINUX_MATH64_H

#include <stdint.h>

static uint64_t div_u64(uint64_t n, uint32_t d)
{
  return n / d;
}

#endif
