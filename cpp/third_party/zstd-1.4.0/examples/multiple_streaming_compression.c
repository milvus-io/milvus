/*
 * Copyright (c) 2016-present, Yann Collet, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under both the BSD-style license (found in the
 * LICENSE file in the root directory of this source tree) and the GPLv2 (found
 * in the COPYING file in the root directory of this source tree).
 * You may select, at your option, one of the above-listed licenses.
 */


/* The objective of this example is to show of to compress multiple successive files
*  while preserving memory management.
*  All structures and buffers will be created only once,
*  and shared across all compression operations */

#include <stdio.h>     // printf
#include <stdlib.h>    // free
#include <string.h>    // memset, strcat
#include <zstd.h>      // presumes zstd library is installed
#include "common.h"    // Helper functions, CHECK(), and CHECK_ZSTD()

typedef struct {
    void* buffIn;
    void* buffOut;
    size_t buffInSize;
    size_t buffOutSize;
    ZSTD_CCtx* cctx;
} resources;

static resources createResources_orDie(int cLevel)
{
    resources ress;
    ress.buffInSize = ZSTD_CStreamInSize();   /* can always read one full block */
    ress.buffOutSize= ZSTD_CStreamOutSize();  /* can always flush a full block */
    ress.buffIn = malloc_orDie(ress.buffInSize);
    ress.buffOut= malloc_orDie(ress.buffOutSize);
    ress.cctx = ZSTD_createCCtx();
    CHECK(ress.cctx != NULL, "ZSTD_createCCtx() failed!");

    /* Set any compression parameters you want here.
     * They will persist for every compression operation.
     * Here we set the compression level, and enable the checksum.
     */
    CHECK_ZSTD( ZSTD_CCtx_setParameter(ress.cctx, ZSTD_c_compressionLevel, cLevel) );
    CHECK_ZSTD( ZSTD_CCtx_setParameter(ress.cctx, ZSTD_c_checksumFlag, 1) );
    return ress;
}

static void freeResources(resources ress)
{
    ZSTD_freeCCtx(ress.cctx);
    free(ress.buffIn);
    free(ress.buffOut);
}

static void compressFile_orDie(resources ress, const char* fname, const char* outName)
{
    // Open the input and output files.
    FILE* const fin  = fopen_orDie(fname, "rb");
    FILE* const fout = fopen_orDie(outName, "wb");

    /* Reset the context to a clean state to start a new compression operation.
     * The parameters are sticky, so we keep the compression level and extra
     * parameters that we set in createResources_orDie().
     */
    CHECK_ZSTD( ZSTD_CCtx_reset(ress.cctx, ZSTD_reset_session_only) );

    size_t const toRead = ress.buffInSize;
    size_t read;
    while ( (read = fread_orDie(ress.buffIn, toRead, fin)) ) {
        /* This loop is the same as streaming_compression.c.
         * See that file for detailed comments.
         */
        int const lastChunk = (read < toRead);
        ZSTD_EndDirective const mode = lastChunk ? ZSTD_e_end : ZSTD_e_continue;

        ZSTD_inBuffer input = { ress.buffIn, read, 0 };
        int finished;
        do {
            ZSTD_outBuffer output = { ress.buffOut, ress.buffOutSize, 0 };
            size_t const remaining = ZSTD_compressStream2(ress.cctx, &output, &input, mode);
            CHECK_ZSTD(remaining);
            fwrite_orDie(ress.buffOut, output.pos, fout);
            finished = lastChunk ? (remaining == 0) : (input.pos == input.size);
        } while (!finished);
        CHECK(input.pos == input.size,
              "Impossible: zstd only returns 0 when the input is completely consumed!");
    }

    fclose_orDie(fout);
    fclose_orDie(fin);
}

int main(int argc, const char** argv)
{
    const char* const exeName = argv[0];

    if (argc<2) {
        printf("wrong arguments\n");
        printf("usage:\n");
        printf("%s FILE(s)\n", exeName);
        return 1;
    }

    int const cLevel = 7;
    resources const ress = createResources_orDie(cLevel);
    void* ofnBuffer = NULL;
    size_t ofnbSize = 0;

    int argNb;
    for (argNb = 1; argNb < argc; argNb++) {
        const char* const ifn = argv[argNb];
        size_t const ifnSize = strlen(ifn);
        size_t const ofnSize = ifnSize + 5;
        if (ofnbSize <= ofnSize) {
            ofnbSize = ofnSize + 16;
            free(ofnBuffer);
            ofnBuffer = malloc_orDie(ofnbSize);
        }
        memset(ofnBuffer, 0, ofnSize);
        strcat(ofnBuffer, ifn);
        strcat(ofnBuffer, ".zst");
        compressFile_orDie(ress, ifn, ofnBuffer);
    }

    freeResources(ress);
    free(ofnBuffer);

    printf("compressed %i files \n", argc-1);

    return 0;
}
