/**
 * Copyright (c) 2016-present, Yann Collet, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under both the BSD-style license (found in the
 * LICENSE file in the root directory of this source tree) and the GPLv2 (found
 * in the COPYING file in the root directory of this source tree).
 */

/*
  This program takes a file in input,
  performs a zstd round-trip test (compression - decompress)
  compares the result with original
  and generates a crash (double free) on corruption detection.
*/

/*===========================================
*   Dependencies
*==========================================*/
#include <stddef.h>     /* size_t */
#include <stdlib.h>     /* malloc, free, exit */
#include <stdio.h>      /* fprintf */
#include <linux/xxhash.h>
#include <linux/zstd.h>

/*===========================================
*   Macros
*==========================================*/
#define MIN(a,b)  ( (a) < (b) ? (a) : (b) )

static const int kMaxClevel = 22;

static ZSTD_CCtx *cctx = NULL;
void *cws = NULL;
static ZSTD_DCtx *dctx = NULL;
void *dws = NULL;
static void* cBuff = NULL;
static void* rBuff = NULL;
static size_t buffSize = 0;


/** roundTripTest() :
*   Compresses `srcBuff` into `compressedBuff`,
*   then decompresses `compressedBuff` into `resultBuff`.
*   Compression level used is derived from first content byte.
*   @return : result of decompression, which should be == `srcSize`
*          or an error code if either compression or decompression fails.
*   Note : `compressedBuffCapacity` should be `>= ZSTD_compressBound(srcSize)`
*          for compression to be guaranteed to work */
static size_t roundTripTest(void* resultBuff, size_t resultBuffCapacity,
                            void* compressedBuff, size_t compressedBuffCapacity,
                      const void* srcBuff, size_t srcBuffSize)
{
    size_t const hashLength = MIN(128, srcBuffSize);
    unsigned const h32 = xxh32(srcBuff, hashLength, 0);
    int const cLevel = h32 % kMaxClevel;
    ZSTD_parameters const params = ZSTD_getParams(cLevel, srcBuffSize, 0);
    size_t const cSize = ZSTD_compressCCtx(cctx, compressedBuff, compressedBuffCapacity, srcBuff, srcBuffSize, params);
    if (ZSTD_isError(cSize)) {
        fprintf(stderr, "Compression error : %u \n", ZSTD_getErrorCode(cSize));
        return cSize;
    }
    return ZSTD_decompressDCtx(dctx, resultBuff, resultBuffCapacity, compressedBuff, cSize);
}


static size_t checkBuffers(const void* buff1, const void* buff2, size_t buffSize)
{
    const char* ip1 = (const char*)buff1;
    const char* ip2 = (const char*)buff2;
    size_t pos;

    for (pos=0; pos<buffSize; pos++)
        if (ip1[pos]!=ip2[pos])
            break;

    return pos;
}

static void crash(int errorCode){
    /* abort if AFL/libfuzzer, exit otherwise */
    #ifdef FUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION /* could also use __AFL_COMPILER */
        abort();
    #else
        exit(errorCode);
    #endif
}

static void roundTripCheck(const void* srcBuff, size_t srcBuffSize)
{
    size_t const neededBuffSize = ZSTD_compressBound(srcBuffSize);

    /* Allocate all buffers and contexts if not already allocated */
    if (neededBuffSize > buffSize) {
        free(cBuff);
        free(rBuff);
        buffSize = 0;

        cBuff = malloc(neededBuffSize);
        rBuff = malloc(neededBuffSize);
        if (!cBuff || !rBuff) {
            fprintf(stderr, "not enough memory ! \n");
            crash(1);
        }
        buffSize = neededBuffSize;
    }
    if (!cctx) {
        ZSTD_compressionParameters const params = ZSTD_getCParams(kMaxClevel, 0, 0);
        size_t const workspaceSize = ZSTD_CCtxWorkspaceBound(params);
        cws = malloc(workspaceSize);
        if (!cws) {
            fprintf(stderr, "not enough memory ! \n");
            crash(1);
        }
        cctx = ZSTD_initCCtx(cws, workspaceSize);
        if (!cctx) {
            fprintf(stderr, "not enough memory ! \n");
            crash(1);
        }
    }
    if (!dctx) {
        size_t const workspaceSize = ZSTD_DCtxWorkspaceBound();
        dws = malloc(workspaceSize);
        if (!dws) {
            fprintf(stderr, "not enough memory ! \n");
            crash(1);
        }
        dctx = ZSTD_initDCtx(dws, workspaceSize);
        if (!dctx) {
            fprintf(stderr, "not enough memory ! \n");
            crash(1);
        }
    }

    {   size_t const result = roundTripTest(rBuff, buffSize, cBuff, buffSize, srcBuff, srcBuffSize);
        if (ZSTD_isError(result)) {
            fprintf(stderr, "roundTripTest error : %u \n", ZSTD_getErrorCode(result));
            crash(1);
        }
        if (result != srcBuffSize) {
            fprintf(stderr, "Incorrect regenerated size : %u != %u\n", (unsigned)result, (unsigned)srcBuffSize);
            crash(1);
        }
        if (checkBuffers(srcBuff, rBuff, srcBuffSize) != srcBuffSize) {
            fprintf(stderr, "Silent decoding corruption !!!");
            crash(1);
        }
    }

#ifndef SKIP_FREE
    free(cws); cws = NULL; cctx = NULL;
    free(dws); dws = NULL; dctx = NULL;
    free(cBuff); cBuff = NULL;
    free(rBuff); rBuff = NULL;
    buffSize = 0;
#endif
}

int LLVMFuzzerTestOneInput(const unsigned char *srcBuff, size_t srcBuffSize) {
  roundTripCheck(srcBuff, srcBuffSize);
  return 0;
}
