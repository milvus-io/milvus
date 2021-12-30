// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#ifndef FAISS_JACCARD_INL_H
#define FAISS_JACCARD_INL_H

#include <faiss/utils/BinaryDistance.h>
#include <faiss/utils/distances_simd_avx.h>
#include <faiss/utils/distances_simd_avx512.h>

namespace faiss {

    struct JaccardComputer8 {
        uint64_t a0;

        JaccardComputer8 () {}

        JaccardComputer8 (const uint8_t *a8, int code_size) {
            set (a8, code_size);
        }

        void set (const uint8_t *a8, int code_size) {
            assert (code_size == 8);
            const uint64_t *a = (uint64_t *)a8;
            a0 = a[0];
        }

        inline float compute (const uint8_t *b8) const {
            const uint64_t *b = (uint64_t *)b8;
            int accu_num = popcount64 (b[0] & a0);
            int accu_den = popcount64 (b[0] | a0);
            return (accu_den == 0) ? 1.0 : ((float)(accu_den - accu_num) / (float)(accu_den));
        }

    };

    struct JaccardComputer16 {
        uint64_t a0, a1;

        JaccardComputer16 () {}

        JaccardComputer16 (const uint8_t *a8, int code_size) {
            set (a8, code_size);
        }

        void set (const uint8_t *a8, int code_size) {
            assert (code_size == 16);
            const uint64_t *a = (uint64_t *)a8;
            a0 = a[0]; a1 = a[1];
        }

        inline float compute (const uint8_t *b8) const {
            const uint64_t *b = (uint64_t *)b8;
            int accu_num = popcount64 (b[0] & a0) + popcount64 (b[1] & a1);
            int accu_den = popcount64 (b[0] | a0) + popcount64 (b[1] | a1);
            return (accu_den == 0) ? 1.0 : ((float)(accu_den - accu_num) / (float)(accu_den));
        }

    };

    struct JaccardComputer32 {
        uint64_t a0, a1, a2, a3;

        JaccardComputer32 () {}

        JaccardComputer32 (const uint8_t *a8, int code_size) {
            set (a8, code_size);
        }

        void set (const uint8_t *a8, int code_size) {
            assert (code_size == 32);
            const uint64_t *a = (uint64_t *)a8;
            a0 = a[0]; a1 = a[1]; a2 = a[2]; a3 = a[3];
        }

        inline float compute (const uint8_t *b8) const {
            const uint64_t *b = (uint64_t *)b8;
            int accu_num = popcount64 (b[0] & a0) + popcount64 (b[1] & a1) +
                           popcount64 (b[2] & a2) + popcount64 (b[3] & a3);
            int accu_den = popcount64 (b[0] | a0) + popcount64 (b[1] | a1) +
                           popcount64 (b[2] | a2) + popcount64 (b[3] | a3);
            return (accu_den == 0) ? 1.0 : ((float)(accu_den - accu_num) / (float)(accu_den));
        }

    };

    struct JaccardComputer64 {
        uint64_t a0, a1, a2, a3, a4, a5, a6, a7;

        JaccardComputer64 () {}

        JaccardComputer64 (const uint8_t *a8, int code_size) {
            set (a8, code_size);
        }

        void set (const uint8_t *a8, int code_size) {
            assert (code_size == 64);
            const uint64_t *a = (uint64_t *)a8;
            a0 = a[0]; a1 = a[1]; a2 = a[2]; a3 = a[3];
            a4 = a[4]; a5 = a[5]; a6 = a[6]; a7 = a[7];
        }

        inline float compute (const uint8_t *b8) const {
            const uint64_t *b = (uint64_t *)b8;
            int accu_num = popcount64 (b[0] & a0) + popcount64 (b[1] & a1) +
                           popcount64 (b[2] & a2) + popcount64 (b[3] & a3) +
                           popcount64 (b[4] & a4) + popcount64 (b[5] & a5) +
                           popcount64 (b[6] & a6) + popcount64 (b[7] & a7);
            int accu_den = popcount64 (b[0] | a0) + popcount64 (b[1] | a1) +
                           popcount64 (b[2] | a2) + popcount64 (b[3] | a3) +
                           popcount64 (b[4] | a4) + popcount64 (b[5] | a5) +
                           popcount64 (b[6] | a6) + popcount64 (b[7] | a7);
            return (accu_den == 0) ? 1.0 : ((float)(accu_den - accu_num) / (float)(accu_den));
        }

    };

    struct JaccardComputer128 {
        uint64_t a0, a1, a2, a3, a4, a5, a6, a7,
                a8, a9, a10, a11, a12, a13, a14, a15;

        JaccardComputer128 () {}

        JaccardComputer128 (const uint8_t *a8, int code_size) {
            set (a8, code_size);
        }

        void set (const uint8_t *au8, int code_size) {
            assert (code_size == 128);
            const uint64_t *a = (uint64_t *)au8;
            a0 = a[0]; a1 = a[1]; a2 = a[2]; a3 = a[3];
            a4 = a[4]; a5 = a[5]; a6 = a[6]; a7 = a[7];
            a8 = a[8]; a9 = a[9]; a10 = a[10]; a11 = a[11];
            a12 = a[12]; a13 = a[13]; a14 = a[14]; a15 = a[15];
        }

        inline float compute (const uint8_t *b16) const {
            const uint64_t *b = (uint64_t *)b16;
            int accu_num = popcount64 (b[0] & a0) + popcount64 (b[1] & a1) +
                           popcount64 (b[2] & a2) + popcount64 (b[3] & a3) +
                           popcount64 (b[4] & a4) + popcount64 (b[5] & a5) +
                           popcount64 (b[6] & a6) + popcount64 (b[7] & a7) +
                           popcount64 (b[8] & a8) + popcount64 (b[9] & a9) +
                           popcount64 (b[10] & a10) + popcount64 (b[11] & a11) +
                           popcount64 (b[12] & a12) + popcount64 (b[13] & a13) +
                           popcount64 (b[14] & a14) + popcount64 (b[15] & a15);
            int accu_den = popcount64 (b[0] | a0) + popcount64 (b[1] | a1) +
                           popcount64 (b[2] | a2) + popcount64 (b[3] | a3) +
                           popcount64 (b[4] | a4) + popcount64 (b[5] | a5) +
                           popcount64 (b[6] | a6) + popcount64 (b[7] | a7) +
                           popcount64 (b[8] | a8) + popcount64 (b[9] | a9) +
                           popcount64 (b[10] | a10) + popcount64 (b[11] | a11) +
                           popcount64 (b[12] | a12) + popcount64 (b[13] | a13) +
                           popcount64 (b[14] | a14) + popcount64 (b[15] | a15);
            return (accu_den == 0) ? 1.0 : ((float)(accu_den - accu_num) / (float)(accu_den));
        }

    };

struct JaccardComputer256 {
        uint64_t a0,a1,a2,a3,a4,a5,a6,a7,
            a8,a9,a10,a11,a12,a13,a14,a15,
            a16,a17,a18,a19,a20,a21,a22,a23,
            a24,a25,a26,a27,a28,a29,a30,a31;

        JaccardComputer256 () {}

        JaccardComputer256 (const uint8_t *a8, int code_size) {
            set (a8, code_size);
        }

        void set (const uint8_t *au8, int code_size) {
            assert (code_size == 256);
            const uint64_t *a = (uint64_t *)au8;
            a0 = a[0]; a1 = a[1]; a2 = a[2]; a3 = a[3];
            a4 = a[4]; a5 = a[5]; a6 = a[6]; a7 = a[7];
            a8 = a[8]; a9 = a[9]; a10 = a[10]; a11 = a[11];
            a12 = a[12]; a13 = a[13]; a14 = a[14]; a15 = a[15];
            a16 = a[16]; a17 = a[17]; a18 = a[18]; a19 = a[19];
            a20 = a[20]; a21 = a[21]; a22 = a[22]; a23 = a[23];
            a24 = a[24]; a25 = a[25]; a26 = a[26]; a27 = a[27];
            a28 = a[28]; a29 = a[29]; a30 = a[30]; a31 = a[31];
        }

        inline float compute (const uint8_t *b16) const {
            const uint64_t *b = (uint64_t *)b16;
            int accu_num = popcount64 (b[0] & a0) + popcount64 (b[1] & a1) +
                           popcount64 (b[2] & a2) + popcount64 (b[3] & a3) +
                           popcount64 (b[4] & a4) + popcount64 (b[5] & a5) +
                           popcount64 (b[6] & a6) + popcount64 (b[7] & a7) +
                           popcount64 (b[8] & a8) + popcount64 (b[9] & a9) +
                           popcount64 (b[10] & a10) + popcount64 (b[11] & a11) +
                           popcount64 (b[12] & a12) + popcount64 (b[13] & a13) +
                           popcount64 (b[14] & a14) + popcount64 (b[15] & a15) +
                           popcount64 (b[16] & a16) + popcount64 (b[17] & a17) +
                           popcount64 (b[18] & a18) + popcount64 (b[19] & a19) +
                           popcount64 (b[20] & a20) + popcount64 (b[21] & a21) +
                           popcount64 (b[22] & a22) + popcount64 (b[23] & a23) +
                           popcount64 (b[24] & a24) + popcount64 (b[25] & a25) +
                           popcount64 (b[26] & a26) + popcount64 (b[27] & a27) +
                           popcount64 (b[28] & a28) + popcount64 (b[29] & a29) +
                           popcount64 (b[30] & a30) + popcount64 (b[31] & a31);
            int accu_den = popcount64 (b[0] | a0) + popcount64 (b[1] | a1) +
                           popcount64 (b[2] | a2) + popcount64 (b[3] | a3) +
                           popcount64 (b[4] | a4) + popcount64 (b[5] | a5) +
                           popcount64 (b[6] | a6) + popcount64 (b[7] | a7) +
                           popcount64 (b[8] | a8) + popcount64 (b[9] | a9) +
                           popcount64 (b[10] | a10) + popcount64 (b[11] | a11) +
                           popcount64 (b[12] | a12) + popcount64 (b[13] | a13) +
                           popcount64 (b[14] | a14) + popcount64 (b[15] | a15) +
                           popcount64 (b[16] | a16) + popcount64 (b[17] | a17) +
                           popcount64 (b[18] | a18) + popcount64 (b[19] | a19) +
                           popcount64 (b[20] | a20) + popcount64 (b[21] | a21) +
                           popcount64 (b[22] | a22) + popcount64 (b[23] | a23) +
                           popcount64 (b[24] | a24) + popcount64 (b[25] | a25) +
                           popcount64 (b[26] | a26) + popcount64 (b[27] | a27) +
                           popcount64 (b[28] | a28) + popcount64 (b[29] | a29) +
                           popcount64 (b[30] | a30) + popcount64 (b[31] | a31);
            return (accu_den == 0) ? 1.0 : ((float)(accu_den - accu_num) / (float)(accu_den));
        }

    };

    struct JaccardComputer512 {
        uint64_t a0,a1,a2,a3,a4,a5,a6,a7,
            a8,a9,a10,a11,a12,a13,a14,a15,
            a16,a17,a18,a19,a20,a21,a22,a23,
            a24,a25,a26,a27,a28,a29,a30,a31,
            a32,a33,a34,a35,a36,a37,a38,a39,
            a40,a41,a42,a43,a44,a45,a46,a47,
            a48,a49,a50,a51,a52,a53,a54,a55,
            a56,a57,a58,a59,a60,a61,a62,a63;

        JaccardComputer512 () {}

        JaccardComputer512 (const uint8_t *a8, int code_size) {
            set (a8, code_size);
        }

        void set (const uint8_t *au8, int code_size) {
            assert (code_size == 512);
            const uint64_t *a = (uint64_t *)au8;
            a0 = a[0]; a1 = a[1]; a2 = a[2]; a3 = a[3];
            a4 = a[4]; a5 = a[5]; a6 = a[6]; a7 = a[7];
            a8 = a[8]; a9 = a[9]; a10 = a[10]; a11 = a[11];
            a12 = a[12]; a13 = a[13]; a14 = a[14]; a15 = a[15];
            a16 = a[16]; a17 = a[17]; a18 = a[18]; a19 = a[19];
            a20 = a[20]; a21 = a[21]; a22 = a[22]; a23 = a[23];
            a24 = a[24]; a25 = a[25]; a26 = a[26]; a27 = a[27];
            a28 = a[28]; a29 = a[29]; a30 = a[30]; a31 = a[31];
            a32 = a[32]; a33 = a[33]; a34 = a[34]; a35 = a[35];
            a36 = a[36]; a37 = a[37]; a38 = a[38]; a39 = a[39];
            a40 = a[40]; a41 = a[41]; a42 = a[42]; a43 = a[43];
            a44 = a[44]; a45 = a[45]; a46 = a[46]; a47 = a[47];
            a48 = a[48]; a49 = a[49]; a50 = a[50]; a51 = a[51];
            a52 = a[52]; a53 = a[53]; a54 = a[54]; a55 = a[55];
            a56 = a[56]; a57 = a[57]; a58 = a[58]; a59 = a[59];
            a60 = a[60]; a61 = a[61]; a62 = a[62]; a63 = a[63];
        }

        inline float compute (const uint8_t *b16) const {
            const uint64_t *b = (uint64_t *)b16;
            int accu_num = popcount64 (b[0] & a0) + popcount64 (b[1] & a1) +
                           popcount64 (b[2] & a2) + popcount64 (b[3] & a3) +
                           popcount64 (b[4] & a4) + popcount64 (b[5] & a5) +
                           popcount64 (b[6] & a6) + popcount64 (b[7] & a7) +
                           popcount64 (b[8] & a8) + popcount64 (b[9] & a9) +
                           popcount64 (b[10] & a10) + popcount64 (b[11] & a11) +
                           popcount64 (b[12] & a12) + popcount64 (b[13] & a13) +
                           popcount64 (b[14] & a14) + popcount64 (b[15] & a15) +
                           popcount64 (b[16] & a16) + popcount64 (b[17] & a17) +
                           popcount64 (b[18] & a18) + popcount64 (b[19] & a19) +
                           popcount64 (b[20] & a20) + popcount64 (b[21] & a21) +
                           popcount64 (b[22] & a22) + popcount64 (b[23] & a23) +
                           popcount64 (b[24] & a24) + popcount64 (b[25] & a25) +
                           popcount64 (b[26] & a26) + popcount64 (b[27] & a27) +
                           popcount64 (b[28] & a28) + popcount64 (b[29] & a29) +
                           popcount64 (b[30] & a30) + popcount64 (b[31] & a31) +
                           popcount64 (b[32] & a32) + popcount64 (b[33] & a33) +
                           popcount64 (b[34] & a34) + popcount64 (b[35] & a35) +
                           popcount64 (b[36] & a36) + popcount64 (b[37] & a37) +
                           popcount64 (b[38] & a38) + popcount64 (b[39] & a39) +
                           popcount64 (b[40] & a40) + popcount64 (b[41] & a41) +
                           popcount64 (b[42] & a42) + popcount64 (b[43] & a43) +
                           popcount64 (b[44] & a44) + popcount64 (b[45] & a45) +
                           popcount64 (b[46] & a46) + popcount64 (b[47] & a47) +
                           popcount64 (b[48] & a48) + popcount64 (b[49] & a49) +
                           popcount64 (b[50] & a50) + popcount64 (b[51] & a51) +
                           popcount64 (b[52] & a52) + popcount64 (b[53] & a53) +
                           popcount64 (b[54] & a54) + popcount64 (b[55] & a55) +
                           popcount64 (b[56] & a56) + popcount64 (b[57] & a57) +
                           popcount64 (b[58] & a58) + popcount64 (b[59] & a59) +
                           popcount64 (b[60] & a60) + popcount64 (b[61] & a61) +
                           popcount64 (b[62] & a62) + popcount64 (b[63] & a63);
            int accu_den = popcount64 (b[0] | a0) + popcount64 (b[1] | a1) +
                           popcount64 (b[2] | a2) + popcount64 (b[3] | a3) +
                           popcount64 (b[4] | a4) + popcount64 (b[5] | a5) +
                           popcount64 (b[6] | a6) + popcount64 (b[7] | a7) +
                           popcount64 (b[8] | a8) + popcount64 (b[9] | a9) +
                           popcount64 (b[10] | a10) + popcount64 (b[11] | a11) +
                           popcount64 (b[12] | a12) + popcount64 (b[13] | a13) +
                           popcount64 (b[14] | a14) + popcount64 (b[15] | a15) +
                           popcount64 (b[16] | a16) + popcount64 (b[17] | a17) +
                           popcount64 (b[18] | a18) + popcount64 (b[19] | a19) +
                           popcount64 (b[20] | a20) + popcount64 (b[21] | a21) +
                           popcount64 (b[22] | a22) + popcount64 (b[23] | a23) +
                           popcount64 (b[24] | a24) + popcount64 (b[25] | a25) +
                           popcount64 (b[26] | a26) + popcount64 (b[27] | a27) +
                           popcount64 (b[28] | a28) + popcount64 (b[29] | a29) +
                           popcount64 (b[30] | a30) + popcount64 (b[31] | a31) +
                           popcount64 (b[32] | a32) + popcount64 (b[33] | a33) +
                           popcount64 (b[34] | a34) + popcount64 (b[35] | a35) +
                           popcount64 (b[36] | a36) + popcount64 (b[37] | a37) +
                           popcount64 (b[38] | a38) + popcount64 (b[39] | a39) +
                           popcount64 (b[40] | a40) + popcount64 (b[41] | a41) +
                           popcount64 (b[42] | a42) + popcount64 (b[43] | a43) +
                           popcount64 (b[44] | a44) + popcount64 (b[45] | a45) +
                           popcount64 (b[46] | a46) + popcount64 (b[47] | a47) +
                           popcount64 (b[48] | a48) + popcount64 (b[49] | a49) +
                           popcount64 (b[50] | a50) + popcount64 (b[51] | a51) +
                           popcount64 (b[52] | a52) + popcount64 (b[53] | a53) +
                           popcount64 (b[54] | a54) + popcount64 (b[55] | a55) +
                           popcount64 (b[56] | a56) + popcount64 (b[57] | a57) +
                           popcount64 (b[58] | a58) + popcount64 (b[59] | a59) +
                           popcount64 (b[60] | a60) + popcount64 (b[61] | a61) +
                           popcount64 (b[62] | a62) + popcount64 (b[63] | a63);
            return (accu_den == 0) ? 1.0 : ((float)(accu_den - accu_num) / (float)(accu_den));
        }

    };

    struct JaccardComputerAVX2 {
        const uint8_t *a;
        int n;

        JaccardComputerAVX2 () {}

        JaccardComputerAVX2 (const uint8_t *a8, int code_size) {
            set (a8, code_size);
        }

        void set (const uint8_t *a8, int code_size) {
            a =  a8;
            n = code_size;
        }

        float compute (const uint8_t *b8) const {
            return jaccard_AVX2(a, b8, n);
        }

    };

    struct JaccardComputerAVX512 {
        const uint8_t *a;
        int n;

        JaccardComputerAVX512 () {}

        JaccardComputerAVX512 (const uint8_t *a8, int code_size) {
            set (a8, code_size);
        }

        void set (const uint8_t *a8, int code_size) {
            a =  a8;
            n = code_size;
        }

        float compute (const uint8_t *b8) const {
            return jaccard__AVX512(a, b8, n);
        }

    };

    struct JaccardComputerDefault {
        const uint8_t *a;
        int n;

        JaccardComputerDefault () {}

        JaccardComputerDefault (const uint8_t *a8, int code_size) {
            set (a8, code_size);
        }

        void set (const uint8_t *a8, int code_size) {
            a =  a8;
            n = code_size;
        }

        float compute (const uint8_t *b8) const {
            return bvec_jaccard(a, b8, n);
        }

    };

// default template
    template<int CODE_SIZE>
    struct JaccardComputer: JaccardComputerDefault {
        JaccardComputer (const uint8_t *a, int code_size):
                JaccardComputerDefault(a, code_size) {}
    };

#define SPECIALIZED_HC(CODE_SIZE)                     \
    template<> struct JaccardComputer<CODE_SIZE>:     \
            JaccardComputer ## CODE_SIZE {            \
        JaccardComputer (const uint8_t *a):           \
        JaccardComputer ## CODE_SIZE(a, CODE_SIZE) {} \
    }

    SPECIALIZED_HC(8);
    SPECIALIZED_HC(16);
    SPECIALIZED_HC(32);
    SPECIALIZED_HC(64);
    SPECIALIZED_HC(128);
    SPECIALIZED_HC(256);
    SPECIALIZED_HC(512);

#undef SPECIALIZED_HC

}

#endif
