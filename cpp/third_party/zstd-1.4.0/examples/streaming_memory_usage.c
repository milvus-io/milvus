/*
 * Copyright (c) 2017-present, Yann Collet, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under both the BSD-style license (found in the
 * LICENSE file in the root directory of this source tree) and the GPLv2 (found
 * in the COPYING file in the root directory of this source tree).
 * You may select, at your option, one of the above-listed licenses.
 */


/*===   Tuning parameter   ===*/
#ifndef MAX_TESTED_LEVEL
#define MAX_TESTED_LEVEL 12
#endif


/*===   Dependencies   ===*/
#include <stdio.h>     // printf
#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>      // presumes zstd library is installed
#include "common.h"    // Helper functions, CHECK(), and CHECK_ZSTD()


/*===   functions   ===*/

/*! readU32FromChar() :
    @return : unsigned integer value read from input in `char` format
    allows and interprets K, KB, KiB, M, MB and MiB suffix.
    Will also modify `*stringPtr`, advancing it to position where it stopped reading.
    Note : function result can overflow if digit string > MAX_UINT */
static unsigned readU32FromChar(const char** stringPtr)
{
    unsigned result = 0;
    while ((**stringPtr >='0') && (**stringPtr <='9'))
        result *= 10, result += **stringPtr - '0', (*stringPtr)++ ;
    if ((**stringPtr=='K') || (**stringPtr=='M')) {
        result <<= 10;
        if (**stringPtr=='M') result <<= 10;
        (*stringPtr)++ ;
        if (**stringPtr=='i') (*stringPtr)++;
        if (**stringPtr=='B') (*stringPtr)++;
    }
    return result;
}


int main(int argc, char const *argv[]) {

    printf("\n Zstandard (v%s) memory usage for streaming : \n\n", ZSTD_versionString());

    unsigned wLog = 0;
    if (argc > 1) {
        const char* valStr = argv[1];
        wLog = readU32FromChar(&valStr);
    }

    int compressionLevel;
    for (compressionLevel = 1; compressionLevel <= MAX_TESTED_LEVEL; compressionLevel++) {
#define INPUT_SIZE 5
#define COMPRESSED_SIZE 128
        char const dataToCompress[INPUT_SIZE] = "abcde";
        char compressedData[COMPRESSED_SIZE];
        char decompressedData[INPUT_SIZE];
        /* the ZSTD_CCtx_params structure is a way to save parameters and use
         * them across multiple contexts. We use them here so we can call the
         * function ZSTD_estimateCStreamSize_usingCCtxParams().
         */
        ZSTD_CCtx_params* const cctxParams = ZSTD_createCCtxParams();
        CHECK(cctxParams != NULL, "ZSTD_createCCtxParams() failed!");

        /* Set the compression level. */
        CHECK_ZSTD( ZSTD_CCtxParams_setParameter(cctxParams, ZSTD_c_compressionLevel, compressionLevel) );
        /* Set the window log.
         * The value 0 means use the default window log, which is equivalent to
         * not setting it.
         */
        CHECK_ZSTD( ZSTD_CCtxParams_setParameter(cctxParams, ZSTD_c_windowLog, wLog) );

        /* Force the compressor to allocate the maximum memory size for a given
         * level by not providing the pledged source size, or calling
         * ZSTD_compressStream2() with ZSTD_e_end.
         */
        ZSTD_CCtx* const cctx = ZSTD_createCCtx();
        CHECK(cctx != NULL, "ZSTD_createCCtx() failed!");
        CHECK_ZSTD( ZSTD_CCtx_setParametersUsingCCtxParams(cctx, cctxParams) );
        size_t compressedSize;
        {
            ZSTD_inBuffer inBuff = { dataToCompress, sizeof(dataToCompress), 0 };
            ZSTD_outBuffer outBuff = { compressedData, sizeof(compressedData), 0 };
            CHECK_ZSTD( ZSTD_compressStream(cctx, &outBuff, &inBuff) );
            size_t const remaining = ZSTD_endStream(cctx, &outBuff);
            CHECK_ZSTD(remaining);
            CHECK(remaining == 0, "Frame not flushed!");
            compressedSize = outBuff.pos;
        }

        ZSTD_DCtx* const dctx = ZSTD_createDCtx();
        CHECK(dctx != NULL, "ZSTD_createDCtx() failed!");
        /* Set the maximum allowed window log.
         * The value 0 means use the default window log, which is equivalent to
         * not setting it.
         */
        CHECK_ZSTD( ZSTD_DCtx_setParameter(dctx, ZSTD_d_windowLogMax, wLog) );
        /* forces decompressor to use maximum memory size, since the
         * decompressed size is not stored in the frame header.
         */
        {   ZSTD_inBuffer inBuff = { compressedData, compressedSize, 0 };
            ZSTD_outBuffer outBuff = { decompressedData, sizeof(decompressedData), 0 };
            size_t const remaining = ZSTD_decompressStream(dctx, &outBuff, &inBuff);
            CHECK_ZSTD(remaining);
            CHECK(remaining == 0, "Frame not complete!");
            CHECK(outBuff.pos == sizeof(dataToCompress), "Bad decompression!");
        }

        size_t const cstreamSize = ZSTD_sizeof_CStream(cctx);
        size_t const cstreamEstimatedSize = ZSTD_estimateCStreamSize_usingCCtxParams(cctxParams);
        size_t const dstreamSize = ZSTD_sizeof_DStream(dctx);
        size_t const dstreamEstimatedSize = ZSTD_estimateDStreamSize_fromFrame(compressedData, compressedSize);

        CHECK(cstreamSize <= cstreamEstimatedSize, "Compression mem (%u) > estimated (%u)",
                (unsigned)cstreamSize, (unsigned)cstreamEstimatedSize);
        CHECK(dstreamSize <= dstreamEstimatedSize, "Decompression mem (%u) > estimated (%u)",
                (unsigned)dstreamSize, (unsigned)dstreamEstimatedSize);

        printf("Level %2i : Compression Mem = %5u KB (estimated : %5u KB) ; Decompression Mem = %4u KB (estimated : %5u KB)\n",
                compressionLevel,
                (unsigned)(cstreamSize>>10), (unsigned)(cstreamEstimatedSize>>10),
                (unsigned)(dstreamSize>>10), (unsigned)(dstreamEstimatedSize>>10));

        ZSTD_freeDCtx(dctx);
        ZSTD_freeCCtx(cctx);
        ZSTD_freeCCtxParams(cctxParams);
        if (wLog) break;  /* single test */
    }
    return 0;
}
