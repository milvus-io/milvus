//  Copyright (c) 2017 International Business Machines Corp.
//  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//  This source code is also licensed under the GPLv2 license found in the
//  COPYING file in the root directory of this source tree.

#pragma once

#define __PPC_RA(a) (((a)&0x1f) << 16)
#define __PPC_RB(b) (((b)&0x1f) << 11)
#define __PPC_XA(a) ((((a)&0x1f) << 16) | (((a)&0x20) >> 3))
#define __PPC_XB(b) ((((b)&0x1f) << 11) | (((b)&0x20) >> 4))
#define __PPC_XS(s) ((((s)&0x1f) << 21) | (((s)&0x20) >> 5))
#define __PPC_XT(s) __PPC_XS(s)
#define VSX_XX3(t, a, b) (__PPC_XT(t) | __PPC_XA(a) | __PPC_XB(b))
#define VSX_XX1(s, a, b) (__PPC_XS(s) | __PPC_RA(a) | __PPC_RB(b))

#define PPC_INST_VPMSUMW 0x10000488
#define PPC_INST_VPMSUMD 0x100004c8
#define PPC_INST_MFVSRD 0x7c000066
#define PPC_INST_MTVSRD 0x7c000166

#define VPMSUMW(t, a, b) .long PPC_INST_VPMSUMW | VSX_XX3((t), a, b)
#define VPMSUMD(t, a, b) .long PPC_INST_VPMSUMD | VSX_XX3((t), a, b)
#define MFVRD(a, t) .long PPC_INST_MFVSRD | VSX_XX1((t) + 32, a, 0)
#define MTVRD(t, a) .long PPC_INST_MTVSRD | VSX_XX1((t) + 32, a, 0)
