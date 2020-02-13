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

#include "metrics/InstructionSet.h"

#include <gtest/gtest.h>
#include <iostream>

TEST(InstructionSetTest, INSTRUCTION_SET_TEST) {
    auto& outstream = std::cout;

    auto support_message = [&outstream](std::string isa_feature, bool is_supported) {
        outstream << isa_feature << (is_supported ? " supported" : " not supported") << std::endl;
    };

    std::cout << milvus::server::InstructionSet::Vendor() << std::endl;
    std::cout << milvus::server::InstructionSet::Brand() << std::endl;

    support_message("3DNOW",       milvus::server::InstructionSet::_3DNOW());
    support_message("3DNOWEXT",    milvus::server::InstructionSet::_3DNOWEXT());
    support_message("ABM",         milvus::server::InstructionSet::ABM());
    support_message("ADX",         milvus::server::InstructionSet::ADX());
    support_message("AES",         milvus::server::InstructionSet::AES());
    support_message("AVX",         milvus::server::InstructionSet::AVX());
    support_message("AVX2",        milvus::server::InstructionSet::AVX2());
    support_message("AVX512BW",    milvus::server::InstructionSet::AVX512BW());
    support_message("AVX512CD",    milvus::server::InstructionSet::AVX512CD());
    support_message("AVX512DQ",    milvus::server::InstructionSet::AVX512DQ());
    support_message("AVX512ER",    milvus::server::InstructionSet::AVX512ER());
    support_message("AVX512F",     milvus::server::InstructionSet::AVX512F());
    support_message("AVX512PF",    milvus::server::InstructionSet::AVX512PF());
    support_message("AVX512VL",    milvus::server::InstructionSet::AVX512VL());
    support_message("BMI1",        milvus::server::InstructionSet::BMI1());
    support_message("BMI2",        milvus::server::InstructionSet::BMI2());
    support_message("CLFSH",       milvus::server::InstructionSet::CLFSH());
    support_message("CMOV",        milvus::server::InstructionSet::CMOV());
    support_message("CMPXCHG16B",  milvus::server::InstructionSet::CMPXCHG16B());
    support_message("CX8",         milvus::server::InstructionSet::CX8());
    support_message("ERMS",        milvus::server::InstructionSet::ERMS());
    support_message("F16C",        milvus::server::InstructionSet::F16C());
    support_message("FMA",         milvus::server::InstructionSet::FMA());
    support_message("FSGSBASE",    milvus::server::InstructionSet::FSGSBASE());
    support_message("FXSR",        milvus::server::InstructionSet::FXSR());
    support_message("HLE",         milvus::server::InstructionSet::HLE());
    support_message("INVPCID",     milvus::server::InstructionSet::INVPCID());
    support_message("LAHF",        milvus::server::InstructionSet::LAHF());
    support_message("LZCNT",       milvus::server::InstructionSet::LZCNT());
    support_message("MMX",         milvus::server::InstructionSet::MMX());
    support_message("MMXEXT",      milvus::server::InstructionSet::MMXEXT());
    support_message("MONITOR",     milvus::server::InstructionSet::MONITOR());
    support_message("MOVBE",       milvus::server::InstructionSet::MOVBE());
    support_message("MSR",         milvus::server::InstructionSet::MSR());
    support_message("OSXSAVE",     milvus::server::InstructionSet::OSXSAVE());
    support_message("PCLMULQDQ",   milvus::server::InstructionSet::PCLMULQDQ());
    support_message("POPCNT",      milvus::server::InstructionSet::POPCNT());
    support_message("PREFETCHWT1", milvus::server::InstructionSet::PREFETCHWT1());
    support_message("RDRAND",      milvus::server::InstructionSet::RDRAND());
    support_message("RDSEED",      milvus::server::InstructionSet::RDSEED());
    support_message("RDTSCP",      milvus::server::InstructionSet::RDTSCP());
    support_message("RTM",         milvus::server::InstructionSet::RTM());
    support_message("SEP",         milvus::server::InstructionSet::SEP());
    support_message("SHA",         milvus::server::InstructionSet::SHA());
    support_message("SSE",         milvus::server::InstructionSet::SSE());
    support_message("SSE2",        milvus::server::InstructionSet::SSE2());
    support_message("SSE3",        milvus::server::InstructionSet::SSE3());
    support_message("SSE4.1",      milvus::server::InstructionSet::SSE41());
    support_message("SSE4.2",      milvus::server::InstructionSet::SSE42());
    support_message("SSE4a",       milvus::server::InstructionSet::SSE4a());
    support_message("SSSE3",       milvus::server::InstructionSet::SSSE3());
    support_message("SYSCALL",     milvus::server::InstructionSet::SYSCALL());
    support_message("TBM",         milvus::server::InstructionSet::TBM());
    support_message("XOP",         milvus::server::InstructionSet::XOP());
    support_message("XSAVE",       milvus::server::InstructionSet::XSAVE());
}
