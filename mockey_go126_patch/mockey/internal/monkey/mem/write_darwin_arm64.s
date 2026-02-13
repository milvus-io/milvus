/*
 * Copyright 2022 ByteDance Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "textflag.h"

#define NOP8 WORD $0x1f2003d5; WORD $0x1f2003d5;
#define NOP64 NOP8; NOP8; NOP8; NOP8; NOP8; NOP8; NOP8; NOP8;
#define NOP512 NOP64; NOP64; NOP64; NOP64; NOP64; NOP64; NOP64; NOP64;
#define NOP4096 NOP512; NOP512; NOP512; NOP512; NOP512; NOP512; NOP512; NOP512;
#define NOP16384 NOP4096; NOP4096; NOP4096; NOP4096;

#define protRW $(0x1|0x2|0x10)
#define mProtect $(0x2000000+74)
#define DSB_SY  WORD $0xd5033f9f
#define IC_IVAU WORD $0xd50b7520
#define ISB_SY  WORD $0xd5033fdf

TEXT ·write(SB),NOSPLIT,$24
    B START
    NOP16384
START:
    MOVD    mProtect, R16
    MOVD    page+24(FP), R0
    MOVD    pageSize+32(FP), R1
    MOVD    protRW, R2
    SVC     $0x80
    CMP     $0, R0
    BEQ     PROTECT_OK
    CALL    mach_task_self(SB)
    MOVD    target+0(FP), R1
    MOVD    len+16(FP), R2
    MOVD    $0, R3
    MOVD    protRW, R4
    CALL    mach_vm_protect(SB)
    CMP     $0, R0
    BNE     RETURN
PROTECT_OK:
    MOVD    target+0(FP), R0
    MOVD    data+8(FP), R1
    MOVD    len+16(FP), R2
    MOVD    R0, to-24(SP)
    MOVD    R1, from-16(SP)
    MOVD    R2, n-8(SP)
    CALL    runtime·memmove(SB)
    DSB_SY
    MOVD    page+24(FP), R0
    MOVD    pageSize+32(FP), R1
FLUSH_LOOP:
    IC_IVAU
    ADD     $64, R0, R0
    SUBS    $64, R1, R1
    BGT     FLUSH_LOOP
    DSB_SY
    ISB_SY
    MOVD    mProtect, R16
    MOVD    page+24(FP), R0
    MOVD    pageSize+32(FP), R1
    MOVD    oriProt+40(FP), R2
    SVC     $0x80
    B       RETURN
    NOP16384
RETURN:
    MOVD R0, ret+48(FP)
    RET
