//  Copyright (c) 2017 International Business Machines Corp.
//  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

extern uint32_t crc32c_ppc(uint32_t crc, unsigned char const *buffer,
                           unsigned len);

#ifdef __cplusplus
}
#endif
