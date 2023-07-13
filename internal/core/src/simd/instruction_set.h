// Copyright (C) 2019-2023 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <cpuid.h>

#include <array>
#include <bitset>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

namespace milvus {
namespace simd {

class InstructionSet {
 public:
    static InstructionSet&
    GetInstance() {
        static InstructionSet inst;
        return inst;
    }

 private:
    InstructionSet()
        : nIds_{0},
          nExIds_{0},
          isIntel_{false},
          isAMD_{false},
          f_1_ECX_{0},
          f_1_EDX_{0},
          f_7_EBX_{0},
          f_7_ECX_{0},
          f_81_ECX_{0},
          f_81_EDX_{0},
          data_{},
          extdata_{} {
        std::array<int, 4> cpui;

        // Calling __cpuid with 0x0 as the function_id argument
        // gets the number of the highest valid function ID.
        __cpuid(0, cpui[0], cpui[1], cpui[2], cpui[3]);
        nIds_ = cpui[0];

        for (int i = 0; i <= nIds_; ++i) {
            __cpuid_count(i, 0, cpui[0], cpui[1], cpui[2], cpui[3]);
            data_.push_back(cpui);
        }

        // Capture vendor string
        char vendor[0x20];
        memset(vendor, 0, sizeof(vendor));
        *reinterpret_cast<int*>(vendor) = data_[0][1];
        *reinterpret_cast<int*>(vendor + 4) = data_[0][3];
        *reinterpret_cast<int*>(vendor + 8) = data_[0][2];
        vendor_ = vendor;
        if (vendor_ == "GenuineIntel") {
            isIntel_ = true;
        } else if (vendor_ == "AuthenticAMD") {
            isAMD_ = true;
        }

        // load bitset with flags for function 0x00000001
        if (nIds_ >= 1) {
            f_1_ECX_ = data_[1][2];
            f_1_EDX_ = data_[1][3];
        }

        // load bitset with flags for function 0x00000007
        if (nIds_ >= 7) {
            f_7_EBX_ = data_[7][1];
            f_7_ECX_ = data_[7][2];
        }

        // Calling __cpuid with 0x80000000 as the function_id argument
        // gets the number of the highest valid extended ID.
        __cpuid(0x80000000, cpui[0], cpui[1], cpui[2], cpui[3]);
        nExIds_ = cpui[0];

        char brand[0x40];
        memset(brand, 0, sizeof(brand));

        for (int i = 0x80000000; i <= nExIds_; ++i) {
            __cpuid_count(i, 0, cpui[0], cpui[1], cpui[2], cpui[3]);
            extdata_.push_back(cpui);
        }

        // load bitset with flags for function 0x80000001
        if (nExIds_ >= (int)0x80000001) {
            f_81_ECX_ = extdata_[1][2];
            f_81_EDX_ = extdata_[1][3];
        }

        // Interpret CPU brand string if reported
        if (nExIds_ >= (int)0x80000004) {
            memcpy(brand, extdata_[2].data(), sizeof(cpui));
            memcpy(brand + 16, extdata_[3].data(), sizeof(cpui));
            memcpy(brand + 32, extdata_[4].data(), sizeof(cpui));
            brand_ = brand;
        }
    };

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
    int nIds_;
    int nExIds_;
    std::string vendor_;
    std::string brand_;
    bool isIntel_;
    bool isAMD_;
    std::bitset<32> f_1_ECX_;
    std::bitset<32> f_1_EDX_;
    std::bitset<32> f_7_EBX_;
    std::bitset<32> f_7_ECX_;
    std::bitset<32> f_81_ECX_;
    std::bitset<32> f_81_EDX_;
    std::vector<std::array<int, 4>> data_;
    std::vector<std::array<int, 4>> extdata_;
};
}  // namespace simd

}  // namespace milvus