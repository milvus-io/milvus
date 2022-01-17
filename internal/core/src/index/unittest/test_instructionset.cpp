// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "faiss/utils/instruction_set.h"

#include <gtest/gtest.h>
#include <iostream>

void
ShowInstructionSet() {
    auto& outstream = std::cout;

    auto support_message = [&outstream](const std::string& isa_feature, bool is_supported) {
        outstream << isa_feature << (is_supported ? " supported" : " not supported") << std::endl;
    };

    faiss::InstructionSet& instruction_set_inst = faiss::InstructionSet::GetInstance();

    std::cout << instruction_set_inst.Vendor() << std::endl;
    std::cout << instruction_set_inst.Brand() << std::endl;

    support_message("3DNOW", instruction_set_inst._3DNOW());
    support_message("3DNOWEXT", instruction_set_inst._3DNOWEXT());
    support_message("ABM", instruction_set_inst.ABM());
    support_message("ADX", instruction_set_inst.ADX());
    support_message("AES", instruction_set_inst.AES());
    support_message("AVX", instruction_set_inst.AVX());
    support_message("AVX2", instruction_set_inst.AVX2());
    support_message("AVX512BW", instruction_set_inst.AVX512BW());
    support_message("AVX512CD", instruction_set_inst.AVX512CD());
    support_message("AVX512DQ", instruction_set_inst.AVX512DQ());
    support_message("AVX512ER", instruction_set_inst.AVX512ER());
    support_message("AVX512F", instruction_set_inst.AVX512F());
    support_message("AVX512PF", instruction_set_inst.AVX512PF());
    support_message("AVX512VL", instruction_set_inst.AVX512VL());
    support_message("BMI1", instruction_set_inst.BMI1());
    support_message("BMI2", instruction_set_inst.BMI2());
    support_message("CLFSH", instruction_set_inst.CLFSH());
    support_message("CMOV", instruction_set_inst.CMOV());
    support_message("CMPXCHG16B", instruction_set_inst.CMPXCHG16B());
    support_message("CX8", instruction_set_inst.CX8());
    support_message("ERMS", instruction_set_inst.ERMS());
    support_message("F16C", instruction_set_inst.F16C());
    support_message("FMA", instruction_set_inst.FMA());
    support_message("FSGSBASE", instruction_set_inst.FSGSBASE());
    support_message("FXSR", instruction_set_inst.FXSR());
    support_message("HLE", instruction_set_inst.HLE());
    support_message("INVPCID", instruction_set_inst.INVPCID());
    support_message("LAHF", instruction_set_inst.LAHF());
    support_message("LZCNT", instruction_set_inst.LZCNT());
    support_message("MMX", instruction_set_inst.MMX());
    support_message("MMXEXT", instruction_set_inst.MMXEXT());
    support_message("MONITOR", instruction_set_inst.MONITOR());
    support_message("MOVBE", instruction_set_inst.MOVBE());
    support_message("MSR", instruction_set_inst.MSR());
    support_message("OSXSAVE", instruction_set_inst.OSXSAVE());
    support_message("PCLMULQDQ", instruction_set_inst.PCLMULQDQ());
    support_message("POPCNT", instruction_set_inst.POPCNT());
    support_message("PREFETCHWT1", instruction_set_inst.PREFETCHWT1());
    support_message("RDRAND", instruction_set_inst.RDRAND());
    support_message("RDSEED", instruction_set_inst.RDSEED());
    support_message("RDTSCP", instruction_set_inst.RDTSCP());
    support_message("RTM", instruction_set_inst.RTM());
    support_message("SEP", instruction_set_inst.SEP());
    support_message("SHA", instruction_set_inst.SHA());
    support_message("SSE", instruction_set_inst.SSE());
    support_message("SSE2", instruction_set_inst.SSE2());
    support_message("SSE3", instruction_set_inst.SSE3());
    support_message("SSE4.1", instruction_set_inst.SSE41());
    support_message("SSE4.2", instruction_set_inst.SSE42());
    support_message("SSE4a", instruction_set_inst.SSE4a());
    support_message("SSSE3", instruction_set_inst.SSSE3());
    support_message("SYSCALL", instruction_set_inst.SYSCALL());
    support_message("TBM", instruction_set_inst.TBM());
    support_message("XOP", instruction_set_inst.XOP());
    support_message("XSAVE", instruction_set_inst.XSAVE());
}

TEST(InstructionSetTest, INSTRUCTION_SET_TEST) {
    ASSERT_NO_FATAL_FAILURE(ShowInstructionSet());
}
