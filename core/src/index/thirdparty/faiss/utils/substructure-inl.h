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

#ifndef FAISS_SBUSTRUCTURE_INL_H
#define FAISS_SBUSTRUCTURE_INL_H

#include <faiss/utils/BinaryDistance.h>

namespace faiss {

    struct SubstructureComputer8 {
        uint64_t a0;

        SubstructureComputer8 () {}

        SubstructureComputer8 (const uint8_t *a8, int code_size) {
            set (a8, code_size);
        }

        void set (const uint8_t *a8, int code_size) {
            assert (code_size == 8);
            const uint64_t *a = (uint64_t *)a8;
            a0 = a[0];
        }

        inline bool compute (const uint8_t *b8) const {
            const uint64_t *b = (uint64_t *)b8;
            return (a0 & b[0]) == a0;
        }

    };

    struct SubstructureComputer16 {
        uint64_t a0, a1;

        SubstructureComputer16 () {}

        SubstructureComputer16 (const uint8_t *a8, int code_size) {
            set (a8, code_size);
        }

        void set (const uint8_t *a8, int code_size) {
            assert (code_size == 16);
            const uint64_t *a = (uint64_t *)a8;
            a0 = a[0]; a1 = a[1];
        }

        inline bool compute (const uint8_t *b8) const {
            const uint64_t *b = (uint64_t *)b8;
            return (a0 & b[0]) == a0 && (a1 & b[1]) == a1;
        }

    };

    struct SubstructureComputer32 {
        uint64_t a0, a1, a2, a3;

        SubstructureComputer32 () {}

        SubstructureComputer32 (const uint8_t *a8, int code_size) {
            set (a8, code_size);
        }

        void set (const uint8_t *a8, int code_size) {
            assert (code_size == 32);
            const uint64_t *a = (uint64_t *)a8;
            a0 = a[0]; a1 = a[1]; a2 = a[2]; a3 = a[3];
        }

        inline bool compute (const uint8_t *b8) const {
            const uint64_t *b = (uint64_t *)b8;
            return (a0 & b[0]) == a0 && (a1 & b[1]) == a1 &&
                   (a2 & b[2]) == a2 && (a3 & b[3]) == a3;
        }

    };

    struct SubstructureComputer64 {
        uint64_t a0, a1, a2, a3, a4, a5, a6, a7;

        SubstructureComputer64 () {}

        SubstructureComputer64 (const uint8_t *a8, int code_size) {
            set (a8, code_size);
        }

        void set (const uint8_t *a8, int code_size) {
            assert (code_size == 64);
            const uint64_t *a = (uint64_t *)a8;
            a0 = a[0]; a1 = a[1]; a2 = a[2]; a3 = a[3];
            a4 = a[4]; a5 = a[5]; a6 = a[6]; a7 = a[7];
        }

        inline bool compute (const uint8_t *b8) const {
            const uint64_t *b = (uint64_t *)b8;
            return (a0 & b[0]) == a0 && (a1 & b[1]) == a1 &&
                   (a2 & b[2]) == a2 && (a3 & b[3]) == a3 &&
                   (a4 & b[4]) == a4 && (a5 & b[5]) == a5 &&
                   (a6 & b[6]) == a6 && (a7 & b[7]) == a7;
        }

    };

    struct SubstructureComputer128 {
        uint64_t a0, a1, a2, a3, a4, a5, a6, a7,
                a8, a9, a10, a11, a12, a13, a14, a15;

        SubstructureComputer128 () {}

        SubstructureComputer128 (const uint8_t *a8, int code_size) {
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

        inline bool compute (const uint8_t *b16) const {
            const uint64_t *b = (uint64_t *)b16;
            return (a0 & b[0]) == a0 && (a1 & b[1]) == a1 &&
                   (a2 & b[2]) == a2 && (a3 & b[3]) == a3 &&
                   (a4 & b[4]) == a4 && (a5 & b[5]) == a5 &&
                   (a6 & b[6]) == a6 && (a7 & b[7]) == a7 &&
                   (a8 & b[8]) == a8 && (a9 & b[9]) == a9 &&
                   (a10 & b[10]) == a10 && (a11 & b[11]) == a11 &&
                   (a12 & b[12]) == a12 && (a13 & b[13]) == a13 &&
                   (a14 & b[14]) == a14 && (a15 & b[15]) == a15;
        }

    };

    struct SubstructureComputer256 {
        uint64_t a0,a1,a2,a3,a4,a5,a6,a7,
            a8,a9,a10,a11,a12,a13,a14,a15,
            a16,a17,a18,a19,a20,a21,a22,a23,
            a24,a25,a26,a27,a28,a29,a30,a31;

        SubstructureComputer256 () {}

        SubstructureComputer256 (const uint8_t *a8, int code_size) {
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

        inline bool compute (const uint8_t *b16) const {
            const uint64_t *b = (uint64_t *)b16;
            return (a0 & b[0]) == a0 && (a1 & b[1]) == a1 &&
                   (a2 & b[2]) == a2 && (a3 & b[3]) == a3 &&
                   (a4 & b[4]) == a4 && (a5 & b[5]) == a5 &&
                   (a6 & b[6]) == a6 && (a7 & b[7]) == a7 &&
                   (a8 & b[8]) == a8 && (a9 & b[9]) == a9 &&
                   (a10 & b[10]) == a10 && (a11 & b[11]) == a11 &&
                   (a12 & b[12]) == a12 && (a13 & b[13]) == a13 &&
                   (a14 & b[14]) == a14 && (a15 & b[15]) == a15 &&
                   (a16 & b[16]) == a16 && (a17 & b[17]) == a17 &&
                   (a18 & b[18]) == a18 && (a19 & b[19]) == a19 &&
                   (a20 & b[20]) == a20 && (a21 & b[21]) == a21 &&
                   (a22 & b[22]) == a22 && (a23 & b[23]) == a23 &&
                   (a24 & b[24]) == a24 && (a25 & b[25]) == a25 &&
                   (a26 & b[26]) == a26 && (a27 & b[27]) == a27 &&
                   (a28 & b[28]) == a28 && (a29 & b[29]) == a29 &&
                   (a30 & b[30]) == a30 && (a31 & b[31]) == a31;
        }

    };

    struct SubstructureComputer512 {
        uint64_t a0,a1,a2,a3,a4,a5,a6,a7,
            a8,a9,a10,a11,a12,a13,a14,a15,
            a16,a17,a18,a19,a20,a21,a22,a23,
            a24,a25,a26,a27,a28,a29,a30,a31,
            a32,a33,a34,a35,a36,a37,a38,a39,
            a40,a41,a42,a43,a44,a45,a46,a47,
            a48,a49,a50,a51,a52,a53,a54,a55,
            a56,a57,a58,a59,a60,a61,a62,a63;

        SubstructureComputer512 () {}

        SubstructureComputer512 (const uint8_t *a8, int code_size) {
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

        inline bool compute (const uint8_t *b16) const {
            const uint64_t *b = (uint64_t *)b16;
            return (a0 & b[0]) == a0 && (a1 & b[1]) == a1 &&
                   (a2 & b[2]) == a2 && (a3 & b[3]) == a3 &&
                   (a4 & b[4]) == a4 && (a5 & b[5]) == a5 &&
                   (a6 & b[6]) == a6 && (a7 & b[7]) == a7 &&
                   (a8 & b[8]) == a8 && (a9 & b[9]) == a9 &&
                   (a10 & b[10]) == a10 && (a11 & b[11]) == a11 &&
                   (a12 & b[12]) == a12 && (a13 & b[13]) == a13 &&
                   (a14 & b[14]) == a14 && (a15 & b[15]) == a15 &&
                   (a16 & b[16]) == a16 && (a17 & b[17]) == a17 &&
                   (a18 & b[18]) == a18 && (a19 & b[19]) == a19 &&
                   (a20 & b[20]) == a20 && (a21 & b[21]) == a21 &&
                   (a22 & b[22]) == a22 && (a23 & b[23]) == a23 &&
                   (a24 & b[24]) == a24 && (a25 & b[25]) == a25 &&
                   (a26 & b[26]) == a26 && (a27 & b[27]) == a27 &&
                   (a28 & b[28]) == a28 && (a29 & b[29]) == a29 &&
                   (a30 & b[30]) == a30 && (a31 & b[31]) == a31 &&
                   (a32 & b[32]) == a32 && (a33 & b[33]) == a33 &&
                   (a34 & b[34]) == a34 && (a35 & b[35]) == a35 &&
                   (a36 & b[36]) == a36 && (a37 & b[37]) == a37 &&
                   (a38 & b[38]) == a38 && (a39 & b[39]) == a39 &&
                   (a40 & b[40]) == a40 && (a41 & b[41]) == a41 &&
                   (a42 & b[42]) == a42 && (a43 & b[43]) == a43 &&
                   (a44 & b[44]) == a44 && (a45 & b[45]) == a45 &&
                   (a46 & b[46]) == a46 && (a47 & b[47]) == a47 &&
                   (a48 & b[48]) == a48 && (a49 & b[49]) == a49 &&
                   (a50 & b[50]) == a50 && (a51 & b[51]) == a51 &&
                   (a52 & b[52]) == a52 && (a53 & b[53]) == a53 &&
                   (a54 & b[54]) == a54 && (a55 & b[55]) == a55 &&
                   (a56 & b[56]) == a56 && (a57 & b[57]) == a57 &&
                   (a58 & b[58]) == a58 && (a59 & b[59]) == a59 &&
                   (a60 & b[60]) == a60 && (a61 & b[61]) == a61 &&
                   (a62 & b[62]) == a62 && (a63 & b[63]) == a63;
         }

    };

    struct SubstructureComputerDefault {
        const uint8_t *a;
        int n;

        SubstructureComputerDefault () {}

        SubstructureComputerDefault (const uint8_t *a8, int code_size) {
            set (a8, code_size);
        }

        void set (const uint8_t *a8, int code_size) {
            a =  a8;
            n = code_size;
        }

        bool compute (const uint8_t *b8) const {
            return is_subset(a, b8, n);
        }

    };

// default template
    template<int CODE_SIZE>
    struct SubstructureComputer: SubstructureComputerDefault {
        SubstructureComputer (const uint8_t *a, int code_size):
                SubstructureComputerDefault(a, code_size) {}
    };

#define SPECIALIZED_HC(CODE_SIZE)                     \
    template<> struct SubstructureComputer<CODE_SIZE>:     \
            SubstructureComputer ## CODE_SIZE {            \
        SubstructureComputer (const uint8_t *a):           \
        SubstructureComputer ## CODE_SIZE(a, CODE_SIZE) {} \
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