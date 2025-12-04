#pragma once

#include <stdint.h>
#include "common/type_c.h"

#ifdef __cplusplus
extern "C" {
#endif

CStatus
compute_phrase_match_slop_c(const char* params, const char* query, const char* data, uint32_t* slop);

#ifdef __cplusplus
}
#endif
