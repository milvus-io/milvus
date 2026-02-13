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

#define NOP8 BYTE $0x90; BYTE $0x90; BYTE $0x90; BYTE $0x90; BYTE $0x90; BYTE $0x90; BYTE $0x90; BYTE $0x90;
#define NOP64 NOP8; NOP8; NOP8; NOP8; NOP8; NOP8; NOP8; NOP8;
#define NOP512 NOP64; NOP64; NOP64; NOP64; NOP64; NOP64; NOP64; NOP64;
#define NOP4096 NOP512; NOP512; NOP512; NOP512; NOP512; NOP512; NOP512; NOP512;

#define protRW $(0x1|0x2|0x10)
#define mProtect $(0x2000000+74)

TEXT ·write(SB),NOSPLIT,$24
    JMP START
    NOP4096
START:
    MOVQ    mProtect, AX
    MOVQ    page+24(FP), DI
    MOVQ    pageSize+32(FP), SI
    MOVQ    protRW, DX
    SYSCALL
    CMPQ    AX, $0
    JZ      PROTECT_OK
    CALL    mach_task_self(SB)
    MOVQ    AX, DI
    MOVQ    target+0(FP), SI
    MOVQ    len+16(FP), DX
    MOVQ    $0, CX
    MOVQ    protRW, R8
    CALL    mach_vm_protect(SB)
    CMPQ    AX, $0
    JNZ     RETURN
PROTECT_OK:
    MOVQ    target+0(FP), DI
    MOVQ    data+8(FP), SI
    MOVQ    len+16(FP), CX
    MOVQ    DI, to-24(SP)
    MOVQ    SI, from-16(SP)
    MOVQ    CX, n-8(SP)
    CALL    runtime·memmove(SB)
    MOVQ    mProtect, AX
    MOVQ    page+24(FP), DI
    MOVQ    pageSize+32(FP), SI
    MOVQ    oriProt+40(FP), DX
    SYSCALL
    JMP     RETURN
    NOP4096
RETURN:
    MOVQ AX, ret+48(FP)
    RET
