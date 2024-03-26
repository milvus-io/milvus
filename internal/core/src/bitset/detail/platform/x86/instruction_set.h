// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <array>
#include <bitset>
#include <string>
#include <vector>

namespace milvus {
namespace bitset {
namespace detail {
namespace x86 {

class InstructionSet {
 public:
    static InstructionSet&
    GetInstance() {
        static InstructionSet inst;
        return inst;
    }

 private:
    InstructionSet();

 public:
    // getters
    std::string
    Vendor() {
        return vendor_;
    }
    std::string
    Brand() {
        return brand_;
    }

    bool
    SSE3() {
        return f_1_ECX_[0];
    }
    bool
    PCLMULQDQ() {
        return f_1_ECX_[1];
    }
    bool
    MONITOR() {
        return f_1_ECX_[3];
    }
    bool
    SSSE3() {
        return f_1_ECX_[9];
    }
    bool
    FMA() {
        return f_1_ECX_[12];
    }
    bool
    CMPXCHG16B() {
        return f_1_ECX_[13];
    }
    bool
    SSE41() {
        return f_1_ECX_[19];
    }
    bool
    SSE42() {
        return f_1_ECX_[20];
    }
    bool
    MOVBE() {
        return f_1_ECX_[22];
    }
    bool
    POPCNT() {
        return f_1_ECX_[23];
    }
    bool
    AES() {
        return f_1_ECX_[25];
    }
    bool
    XSAVE() {
        return f_1_ECX_[26];
    }
    bool
    OSXSAVE() {
        return f_1_ECX_[27];
    }
    bool
    AVX() {
        return f_1_ECX_[28];
    }
    bool
    F16C() {
        return f_1_ECX_[29];
    }
    bool
    RDRAND() {
        return f_1_ECX_[30];
    }

    bool
    MSR() {
        return f_1_EDX_[5];
    }
    bool
    CX8() {
        return f_1_EDX_[8];
    }
    bool
    SEP() {
        return f_1_EDX_[11];
    }
    bool
    CMOV() {
        return f_1_EDX_[15];
    }
    bool
    CLFSH() {
        return f_1_EDX_[19];
    }
    bool
    MMX() {
        return f_1_EDX_[23];
    }
    bool
    FXSR() {
        return f_1_EDX_[24];
    }
    bool
    SSE() {
        return f_1_EDX_[25];
    }
    bool
    SSE2() {
        return f_1_EDX_[26];
    }

    bool
    FSGSBASE() {
        return f_7_EBX_[0];
    }
    bool
    BMI1() {
        return f_7_EBX_[3];
    }
    bool
    HLE() {
        return isIntel_ && f_7_EBX_[4];
    }
    bool
    AVX2() {
        return f_7_EBX_[5];
    }
    bool
    BMI2() {
        return f_7_EBX_[8];
    }
    bool
    ERMS() {
        return f_7_EBX_[9];
    }
    bool
    INVPCID() {
        return f_7_EBX_[10];
    }
    bool
    RTM() {
        return isIntel_ && f_7_EBX_[11];
    }
    bool
    AVX512F() {
        return f_7_EBX_[16];
    }
    bool
    AVX512DQ() {
        return f_7_EBX_[17];
    }
    bool
    RDSEED() {
        return f_7_EBX_[18];
    }
    bool
    ADX() {
        return f_7_EBX_[19];
    }
    bool
    AVX512PF() {
        return f_7_EBX_[26];
    }
    bool
    AVX512ER() {
        return f_7_EBX_[27];
    }
    bool
    AVX512CD() {
        return f_7_EBX_[28];
    }
    bool
    SHA() {
        return f_7_EBX_[29];
    }
    bool
    AVX512BW() {
        return f_7_EBX_[30];
    }
    bool
    AVX512VL() {
        return f_7_EBX_[31];
    }

    bool
    PREFETCHWT1() {
        return f_7_ECX_[0];
    }

    bool
    LAHF() {
        return f_81_ECX_[0];
    }
    bool
    LZCNT() {
        return isIntel_ && f_81_ECX_[5];
    }
    bool
    ABM() {
        return isAMD_ && f_81_ECX_[5];
    }
    bool
    SSE4a() {
        return isAMD_ && f_81_ECX_[6];
    }
    bool
    XOP() {
        return isAMD_ && f_81_ECX_[11];
    }
    bool
    TBM() {
        return isAMD_ && f_81_ECX_[21];
    }

    bool
    SYSCALL() {
        return isIntel_ && f_81_EDX_[11];
    }
    bool
    MMXEXT() {
        return isAMD_ && f_81_EDX_[22];
    }
    bool
    RDTSCP() {
        return isIntel_ && f_81_EDX_[27];
    }
    bool
    _3DNOWEXT() {
        return isAMD_ && f_81_EDX_[30];
    }
    bool
    _3DNOW() {
        return isAMD_ && f_81_EDX_[31];
    }

 private:
    int nIds_ = 0;
    int nExIds_ = 0;
    std::string vendor_;
    std::string brand_;
    bool isIntel_ = false;
    bool isAMD_ = false;
    std::bitset<32> f_1_ECX_ = {0};
    std::bitset<32> f_1_EDX_ = {0};
    std::bitset<32> f_7_EBX_ = {0};
    std::bitset<32> f_7_ECX_ = {0};
    std::bitset<32> f_81_ECX_ = {0};
    std::bitset<32> f_81_EDX_ = {0};
    std::vector<std::array<int, 4>> data_;
    std::vector<std::array<int, 4>> extdata_;
};

bool
cpu_support_avx512();
bool
cpu_support_avx2();
bool
cpu_support_sse4_2();
bool
cpu_support_sse2();

}  // namespace x86
}  // namespace detail
}  // namespace bitset
}  // namespace milvus
