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

#define sysWrite $(0x2000000+4)

TEXT ·callSystemFunc(SB),NOSPLIT,$0
    MOVD    sysWrite, R16
    MOVD    $1, R0
    MOVD    msg_data+0(FP), R1
    MOVD    msg_len+8(FP), R2
    SVC     $0x80
    MOVD    R0, ret+0(FP)
    RET

TEXT ·callTwoAsmFunc(SB),NOSPLIT,$8
    BL      ·asmFunc1(SB)
    MOVD    tmp-8(SP), R0
    MOVD    R0, res+0(FP)
    BL      ·asmFunc2(SB)
    MOVD    tmp-8(SP), R0
    MOVD    R0, res+8(FP)
    RET

TEXT ·asmFunc1(SB),NOSPLIT,$0
    MOVD    $1, R0
    MOVD    R0, res+0(FP)
    RET

TEXT ·asmFunc2(SB),NOSPLIT,$0
    MOVD    $2, R0
    MOVD    R0, res+0(FP)
    RET
