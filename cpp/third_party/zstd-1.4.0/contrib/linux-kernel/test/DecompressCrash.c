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
#include <linux/zstd.h>

/*===========================================
*   Macros
*==========================================*/
#define MIN(a,b)  ( (a) < (b) ? (a) : (b) )

static ZSTD_DCtx *dctx = NULL;
void *dws = NULL;
static void* rBuff = NULL;
static size_t buffSize = 0;

static void crash(int errorCode){
    /* abort if AFL/libfuzzer, exit otherwise */
    #ifdef FUZZING_BUILD_MODE_UNSAFE_FOR_PRODUCTION /* could also use __AFL_COMPILER */
        abort();
    #else
        exit(errorCode);
    #endif
}

static void decompressCheck(const void* srcBuff, size_t srcBuffSize)
{
    size_t const neededBuffSize = 20 * srcBuffSize;

    /* Allocate all buffers and contexts if not already allocated */
    if (neededBuffSize > buffSize) {
        free(rBuff);
        buffSize = 0;

        rBuff = malloc(neededBuffSize);
        if (!rBuff) {
            fprintf(stderr, "not enough memory ! \n");
            crash(1);
        }
        buffSize = neededBuffSize;
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
    ZSTD_decompressDCtx(dctx, rBuff, buffSize, srcBuff, srcBuffSize);

#ifndef SKIP_FREE
    free(dws); dws = NULL; dctx = NULL;
    free(rBuff); rBuff = NULL;
    buffSize = 0;
#endif
}

int LLVMFuzzerTestOneInput(const unsigned char *srcBuff, size_t srcBuffSize) {
  decompressCheck(srcBuff, srcBuffSize);
  return 0;
}
