//go:build !mockey_disable_ss && go1.23 && !go1.27
// +build !mockey_disable_ss,go1.23,!go1.27

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

TEXT ·usleepTrampoline(SB),NOSPLIT,$8
    MOVD    usec+0(FP), DI
    MOVQ    pc+8(FP), SI
    MOVD    DI, usec-8(SP)
    CALL    SI
    RET
