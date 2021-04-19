/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */



namespace faiss {


inline BitstringWriter::BitstringWriter(uint8_t *code, int code_size):
    code (code), code_size (code_size), i(0)
{
    bzero (code, code_size);
}

inline void BitstringWriter::write(uint64_t x, int nbit) {
    assert (code_size * 8 >= nbit + i);
    // nb of available bits in i / 8
    int na = 8 - (i & 7);

    if (nbit <= na) {
        code[i >> 3] |= x << (i & 7);
        i += nbit;
        return;
    } else {
        int j = i >> 3;
        code[j++] |= x << (i & 7);
        i += nbit;
        x >>= na;
        while (x != 0) {
            code[j++] |= x;
            x >>= 8;
        }
    }
}


inline BitstringReader::BitstringReader(const uint8_t *code, int code_size):
    code (code), code_size (code_size), i(0)
{}

inline uint64_t BitstringReader::read(int nbit) {
    assert (code_size * 8 >= nbit + i);
    // nb of available bits in i / 8
    int na = 8 - (i & 7);
    // get available bits in current byte
    uint64_t res = code[i >> 3] >> (i & 7);
    if (nbit <= na) {
        res &= (1 << nbit) - 1;
        i += nbit;
        return res;
    } else {
        int ofs = na;
        int j = (i >> 3) + 1;
        i += nbit;
        nbit -= na;
        while (nbit > 8) {
            res |= ((uint64_t)code[j++]) << ofs;
            ofs += 8;
            nbit -= 8; // TODO remove nbit
        }
        uint64_t last_byte = code[j];
        last_byte &= (1 << nbit) - 1;
        res |= last_byte << ofs;
        return res;
    }
}


/******************************************************************
 * The HammingComputer series of classes compares a single code of
 * size 4 to 32 to incoming codes. They are intended for use as a
 * template class where it would be inefficient to switch on the code
 * size in the inner loop. Hopefully the compiler will inline the
 * hamming() functions and put the a0, a1, ... in registers.
 ******************************************************************/


struct HammingComputer4 {
    uint32_t a0;

    HammingComputer4 () {}

    HammingComputer4 (const uint8_t *a, int code_size) {
        set (a, code_size);
    }

    void set (const uint8_t *a, int code_size) {
        assert (code_size == 4);
        a0 = *(uint32_t *)a;
    }

    inline int hamming (const uint8_t *b) const {
        return popcount64 (*(uint32_t *)b ^ a0);
    }

};

struct HammingComputer8 {
    uint64_t a0;

    HammingComputer8 () {}

    HammingComputer8 (const uint8_t *a, int code_size) {
        set (a, code_size);
    }

    void set (const uint8_t *a, int code_size) {
        assert (code_size == 8);
        a0 = *(uint64_t *)a;
    }

    inline int hamming (const uint8_t *b) const {
        return popcount64 (*(uint64_t *)b ^ a0);
    }

};


struct HammingComputer16 {
    uint64_t a0, a1;

    HammingComputer16 () {}

    HammingComputer16 (const uint8_t *a8, int code_size) {
        set (a8, code_size);
    }

    void set (const uint8_t *a8, int code_size) {
        assert (code_size == 16);
        const uint64_t *a = (uint64_t *)a8;
        a0 = a[0]; a1 = a[1];
    }

    inline int hamming (const uint8_t *b8) const {
        const uint64_t *b = (uint64_t *)b8;
        return popcount64 (b[0] ^ a0) + popcount64 (b[1] ^ a1);
    }

};

// when applied to an array, 1/2 of the 64-bit accesses are unaligned.
// This incurs a penalty of ~10% wrt. fully aligned accesses.
struct HammingComputer20 {
    uint64_t a0, a1;
    uint32_t a2;

    HammingComputer20 () {}

    HammingComputer20 (const uint8_t *a8, int code_size) {
        set (a8, code_size);
    }

    void set (const uint8_t *a8, int code_size) {
        assert (code_size == 20);
        const uint64_t *a = (uint64_t *)a8;
        a0 = a[0]; a1 = a[1]; a2 = a[2];
    }

    inline int hamming (const uint8_t *b8) const {
        const uint64_t *b = (uint64_t *)b8;
        return popcount64 (b[0] ^ a0) + popcount64 (b[1] ^ a1) +
            popcount64 (*(uint32_t*)(b + 2) ^ a2);
    }
};

struct HammingComputer32 {
    uint64_t a0, a1, a2, a3;

    HammingComputer32 () {}

    HammingComputer32 (const uint8_t *a8, int code_size) {
        set (a8, code_size);
    }

    void set (const uint8_t *a8, int code_size) {
        assert (code_size == 32);
        const uint64_t *a = (uint64_t *)a8;
        a0 = a[0]; a1 = a[1]; a2 = a[2]; a3 = a[3];
    }

    inline int hamming (const uint8_t *b8) const {
        const uint64_t *b = (uint64_t *)b8;
        return popcount64 (b[0] ^ a0) + popcount64 (b[1] ^ a1) +
            popcount64 (b[2] ^ a2) + popcount64 (b[3] ^ a3);
    }

};

struct HammingComputer64 {
    uint64_t a0, a1, a2, a3, a4, a5, a6, a7;

    HammingComputer64 () {}

    HammingComputer64 (const uint8_t *a8, int code_size) {
        set (a8, code_size);
    }

    void set (const uint8_t *a8, int code_size) {
        assert (code_size == 64);
        const uint64_t *a = (uint64_t *)a8;
        a0 = a[0]; a1 = a[1]; a2 = a[2]; a3 = a[3];
        a4 = a[4]; a5 = a[5]; a6 = a[6]; a7 = a[7];
    }

    inline int hamming (const uint8_t *b8) const {
        const uint64_t *b = (uint64_t *)b8;
        return popcount64 (b[0] ^ a0) + popcount64 (b[1] ^ a1) +
            popcount64 (b[2] ^ a2) + popcount64 (b[3] ^ a3) +
            popcount64 (b[4] ^ a4) + popcount64 (b[5] ^ a5) +
            popcount64 (b[6] ^ a6) + popcount64 (b[7] ^ a7);
    }

};

// very inefficient...
struct HammingComputerDefault {
    const uint8_t *a;
    int n;

    HammingComputerDefault () {}

    HammingComputerDefault (const uint8_t *a8, int code_size) {
        set (a8, code_size);
    }

    void set (const uint8_t *a8, int code_size) {
        a =  a8;
        n = code_size;
    }

    int hamming (const uint8_t *b8) const {
        int accu = 0;
        for (int i = 0; i < n; i++)
            accu += popcount64 (a[i] ^ b8[i]);
        return accu;
    }

};

struct HammingComputerM8 {
    const uint64_t *a;
    int n;

    HammingComputerM8 () {}

    HammingComputerM8 (const uint8_t *a8, int code_size) {
        set (a8, code_size);
    }

    void set (const uint8_t *a8, int code_size) {
        assert (code_size % 8 == 0);
        a =  (uint64_t *)a8;
        n = code_size / 8;
    }

    int hamming (const uint8_t *b8) const {
        const uint64_t *b = (uint64_t *)b8;
        int accu = 0;
        for (int i = 0; i < n; i++)
            accu += popcount64 (a[i] ^ b[i]);
        return accu;
    }

};

// even more inefficient!
struct HammingComputerM4 {
    const uint32_t *a;
    int n;

    HammingComputerM4 () {}

    HammingComputerM4 (const uint8_t *a4, int code_size) {
        set (a4, code_size);
    }

    void set (const uint8_t *a4, int code_size) {
        assert (code_size % 4 == 0);
        a =  (uint32_t *)a4;
        n = code_size / 4;
    }

    int hamming (const uint8_t *b8) const {
        const uint32_t *b = (uint32_t *)b8;
        int accu = 0;
        for (int i = 0; i < n; i++)
             accu += popcount64 (a[i] ^ b[i]);
        return accu;
    }

};

/***************************************************************************
 * Equivalence with a template class when code size is known at compile time
 **************************************************************************/

// default template
template<int CODE_SIZE>
struct HammingComputer: HammingComputerM8 {
    HammingComputer (const uint8_t *a, int code_size):
    HammingComputerM8(a, code_size) {}
};

#define SPECIALIZED_HC(CODE_SIZE)                     \
    template<> struct HammingComputer<CODE_SIZE>:     \
            HammingComputer ## CODE_SIZE {            \
        HammingComputer (const uint8_t *a):           \
        HammingComputer ## CODE_SIZE(a, CODE_SIZE) {} \
    }

SPECIALIZED_HC(4);
SPECIALIZED_HC(8);
SPECIALIZED_HC(16);
SPECIALIZED_HC(20);
SPECIALIZED_HC(32);
SPECIALIZED_HC(64);

#undef SPECIALIZED_HC


/***************************************************************************
 * generalized Hamming = number of bytes that are different between
 * two codes.
 ***************************************************************************/


inline int generalized_hamming_64 (uint64_t a) {
    a |= a >> 1;
    a |= a >> 2;
    a |= a >> 4;
    a &= 0x0101010101010101UL;
    return popcount64 (a);
}


struct GenHammingComputer8 {
    uint64_t a0;

    GenHammingComputer8 (const uint8_t *a, int code_size) {
        assert (code_size == 8);
        a0 = *(uint64_t *)a;
    }

    inline int hamming (const uint8_t *b) const {
        return generalized_hamming_64 (*(uint64_t *)b ^ a0);
    }

};


struct GenHammingComputer16 {
    uint64_t a0, a1;
    GenHammingComputer16 (const uint8_t *a8, int code_size) {
        assert (code_size == 16);
        const uint64_t *a = (uint64_t *)a8;
        a0 = a[0]; a1 = a[1];
    }

    inline int hamming (const uint8_t *b8) const {
        const uint64_t *b = (uint64_t *)b8;
        return generalized_hamming_64 (b[0] ^ a0) +
            generalized_hamming_64 (b[1] ^ a1);
    }

};

struct GenHammingComputer32 {
    uint64_t a0, a1, a2, a3;

    GenHammingComputer32 (const uint8_t *a8, int code_size) {
        assert (code_size == 32);
        const uint64_t *a = (uint64_t *)a8;
        a0 = a[0]; a1 = a[1]; a2 = a[2]; a3 = a[3];
    }

    inline int hamming (const uint8_t *b8) const {
        const uint64_t *b = (uint64_t *)b8;
        return generalized_hamming_64 (b[0] ^ a0) +
            generalized_hamming_64 (b[1] ^ a1) +
            generalized_hamming_64 (b[2] ^ a2) +
            generalized_hamming_64 (b[3] ^ a3);
    }

};

struct GenHammingComputerM8 {
    const uint64_t *a;
    int n;

    GenHammingComputerM8 (const uint8_t *a8, int code_size) {
        assert (code_size % 8 == 0);
        a =  (uint64_t *)a8;
        n = code_size / 8;
    }

    int hamming (const uint8_t *b8) const {
        const uint64_t *b = (uint64_t *)b8;
        int accu = 0;
        for (int i = 0; i < n; i++)
            accu += generalized_hamming_64 (a[i] ^ b[i]);
        return accu;
    }

};


/** generalized Hamming distances (= count number of code bytes that
    are the same) */
void generalized_hammings_knn_hc (
        int_maxheap_array_t * ha,
        const uint8_t * a,
        const uint8_t * b,
        size_t nb,
        size_t code_size,
        int ordered = true);



/** This class maintains a list of best distances seen so far.
 *
 * Since the distances are in a limited range (0 to nbit), the
 * object maintains one list per possible distance, and fills
 * in only the n-first lists, such that the sum of sizes of the
 * n lists is below k.
 */
template<class HammingComputer>
struct HCounterState {
  int *counters;
  int64_t *ids_per_dis;

  HammingComputer hc;
  int thres;
  int count_lt;
  int count_eq;
  int k;

 HCounterState(int *counters, int64_t *ids_per_dis,
               const uint8_t *x, int d, int k)
 : counters(counters),
        ids_per_dis(ids_per_dis),
        hc(x, d / 8),
        thres(d + 1),
        count_lt(0),
        count_eq(0),
        k(k) {}

  void update_counter(const uint8_t *y, size_t j) {
    int32_t dis = hc.hamming(y);

    if (dis <= thres) {
      if (dis < thres) {
        ids_per_dis[dis * k + counters[dis]++] = j;
        ++count_lt;
        while (count_lt == k && thres > 0) {
          --thres;
          count_eq = counters[thres];
          count_lt -= count_eq;
        }
      } else if (count_eq < k) {
        ids_per_dis[dis * k + count_eq++] = j;
        counters[dis] = count_eq;
      }
    }
  }
};


} // namespace faiss
