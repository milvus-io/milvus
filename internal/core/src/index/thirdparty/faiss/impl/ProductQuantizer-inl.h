/**
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

namespace faiss {

inline
PQEncoderGeneric::PQEncoderGeneric(uint8_t *code, int nbits,
                                                     uint8_t offset)
    : code(code), offset(offset), nbits(nbits), reg(0)
{
    assert(nbits <= 64);
    if (offset > 0) {
        reg = (*code & ((1 << offset) - 1));
    }
}

inline
void PQEncoderGeneric::encode(uint64_t x)
{
    reg |= (uint8_t)(x << offset);
    x >>= (8 - offset);
    if (offset + nbits >= 8) {
        *code++ = reg;

        for (int i = 0; i < (nbits - (8 - offset)) / 8; ++i) {
            *code++ = (uint8_t)x;
            x >>= 8;
        }

        offset += nbits;
        offset &= 7;
        reg = (uint8_t)x;
    } else {
        offset += nbits;
    }
}

inline
PQEncoderGeneric::~PQEncoderGeneric()
{
    if (offset > 0) {
        *code = reg;
    }
}


inline
PQEncoder8::PQEncoder8(uint8_t *code, int nbits)
    : code(code) {
    assert(8 == nbits);
}

inline
void PQEncoder8::encode(uint64_t x) {
    *code++ = (uint8_t)x;
}

inline
PQEncoder16::PQEncoder16(uint8_t *code, int nbits)
    : code((uint16_t *)code) {
    assert(16 == nbits);
}

inline
void PQEncoder16::encode(uint64_t x) {
    *code++ = (uint16_t)x;
}


inline
PQDecoderGeneric::PQDecoderGeneric(const uint8_t *code,
                                                     int nbits)
    : code(code),
      offset(0),
      nbits(nbits),
      mask((1ull << nbits) - 1),
      reg(0) {
    assert(nbits <= 64);
}

inline
uint64_t PQDecoderGeneric::decode() {
    if (offset == 0) {
        reg = *code;
    }
    uint64_t c = (reg >> offset);

    if (offset + nbits >= 8) {
        uint64_t e = 8 - offset;
        ++code;
        for (int i = 0; i < (nbits - (8 - offset)) / 8; ++i) {
            c |= ((uint64_t)(*code++) << e);
            e += 8;
        }

        offset += nbits;
        offset &= 7;
        if (offset > 0) {
            reg = *code;
            c |= ((uint64_t)reg << e);
        }
    } else {
        offset += nbits;
    }

    return c & mask;
}


inline
PQDecoder8::PQDecoder8(const uint8_t *code, int nbits)
    : code(code) {
    assert(8 == nbits);
}

inline
uint64_t PQDecoder8::decode() {
    return (uint64_t)(*code++);
}


inline
PQDecoder16::PQDecoder16(const uint8_t *code, int nbits)
    : code((uint16_t *)code) {
     assert(16 == nbits);
}

inline
uint64_t PQDecoder16::decode() {
    return (uint64_t)(*code++);
}


} // namespace faiss
