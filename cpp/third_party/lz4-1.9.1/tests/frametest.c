/*
    frameTest - test tool for lz4frame
    Copyright (C) Yann Collet 2014-2016

    GPL v2 License

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License along
    with this program; if not, write to the Free Software Foundation, Inc.,
    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

    You can contact the author at :
    - LZ4 homepage : http://www.lz4.org
    - LZ4 source repository : https://github.com/lz4/lz4
*/

/*-************************************
*  Compiler specific
**************************************/
#ifdef _MSC_VER    /* Visual Studio */
#  pragma warning(disable : 4127)     /* disable: C4127: conditional expression is constant */
#  pragma warning(disable : 4146)     /* disable: C4146: minus unsigned expression */
#endif


/*-************************************
*  Includes
**************************************/
#include "util.h"       /* U32 */
#include <stdlib.h>     /* malloc, free */
#include <stdio.h>      /* fprintf */
#include <string.h>     /* strcmp */
#include <time.h>       /* clock_t, clock(), CLOCKS_PER_SEC */
#include <assert.h>
#include "lz4frame.h"   /* include multiple times to test correctness/safety */
#include "lz4frame.h"
#define LZ4F_STATIC_LINKING_ONLY
#include "lz4frame.h"
#include "lz4frame.h"
#include "lz4.h"        /* LZ4_VERSION_STRING */
#define XXH_STATIC_LINKING_ONLY
#include "xxhash.h"     /* XXH64 */


/* unoptimized version; solves endianess & alignment issues */
static void FUZ_writeLE32 (void* dstVoidPtr, U32 value32)
{
    BYTE* dstPtr = (BYTE*)dstVoidPtr;
    dstPtr[0] = (BYTE) value32;
    dstPtr[1] = (BYTE)(value32 >> 8);
    dstPtr[2] = (BYTE)(value32 >> 16);
    dstPtr[3] = (BYTE)(value32 >> 24);
}


/*-************************************
*  Constants
**************************************/
#define LZ4F_MAGIC_SKIPPABLE_START 0x184D2A50U

#define KB *(1U<<10)
#define MB *(1U<<20)
#define GB *(1U<<30)

static const U32 nbTestsDefault = 256 KB;
#define FUZ_COMPRESSIBILITY_DEFAULT 50
static const U32 prime1 = 2654435761U;
static const U32 prime2 = 2246822519U;


/*-************************************
*  Macros
**************************************/
#define DISPLAY(...)          fprintf(stderr, __VA_ARGS__)
#define DISPLAYLEVEL(l, ...)  if (displayLevel>=l) { DISPLAY(__VA_ARGS__); }
#define DISPLAYUPDATE(l, ...) if (displayLevel>=l) { \
            if ((FUZ_GetClockSpan(g_clockTime) > refreshRate) || (displayLevel>=4)) \
            { g_clockTime = clock(); DISPLAY(__VA_ARGS__); \
            if (displayLevel>=4) fflush(stdout); } }
static const clock_t refreshRate = CLOCKS_PER_SEC / 6;
static clock_t g_clockTime = 0;


/*-***************************************
*  Local Parameters
*****************************************/
static U32 no_prompt = 0;
static U32 displayLevel = 2;
static U32 use_pause = 0;


/*-*******************************************************
*  Fuzzer functions
*********************************************************/
#define MIN(a,b)  ( (a) < (b) ? (a) : (b) )
#define MAX(a,b)  ( (a) > (b) ? (a) : (b) )

static clock_t FUZ_GetClockSpan(clock_t clockStart)
{
    return clock() - clockStart;   /* works even if overflow; max span ~ 30 mn */
}


#define FUZ_rotl32(x,r) ((x << r) | (x >> (32 - r)))
unsigned int FUZ_rand(unsigned int* src)
{
    U32 rand32 = *src;
    rand32 *= prime1;
    rand32 += prime2;
    rand32  = FUZ_rotl32(rand32, 13);
    *src = rand32;
    return rand32 >> 5;
}


#define FUZ_RAND15BITS  (FUZ_rand(seed) & 0x7FFF)
#define FUZ_RANDLENGTH  ( (FUZ_rand(seed) & 3) ? (FUZ_rand(seed) % 15) : (FUZ_rand(seed) % 510) + 15)
static void FUZ_fillCompressibleNoiseBuffer(void* buffer, size_t bufferSize, double proba, U32* seed)
{
    BYTE* BBuffer = (BYTE*)buffer;
    size_t pos = 0;
    U32 P32 = (U32)(32768 * proba);

    /* First Byte */
    BBuffer[pos++] = (BYTE)(FUZ_rand(seed));

    while (pos < bufferSize) {
        /* Select : Literal (noise) or copy (within 64K) */
        if (FUZ_RAND15BITS < P32) {
            /* Copy (within 64K) */
            size_t const lengthRand = FUZ_RANDLENGTH + 4;
            size_t const length = MIN(lengthRand, bufferSize - pos);
            size_t const end = pos + length;
            size_t const offsetRand = FUZ_RAND15BITS + 1;
            size_t const offset = MIN(offsetRand, pos);
            size_t match = pos - offset;
            while (pos < end) BBuffer[pos++] = BBuffer[match++];
        } else {
            /* Literal (noise) */
            size_t const lengthRand = FUZ_RANDLENGTH + 4;
            size_t const length = MIN(lengthRand, bufferSize - pos);
            size_t const end = pos + length;
            while (pos < end) BBuffer[pos++] = (BYTE)(FUZ_rand(seed) >> 5);
        }
    }
}


static unsigned FUZ_highbit(U32 v32)
{
    unsigned nbBits = 0;
    if (v32==0) return 0;
    while (v32) {v32 >>= 1; nbBits ++;}
    return nbBits;
}


/*-*******************************************************
*  Tests
*********************************************************/
#define CHECK_V(v,f) v = f; if (LZ4F_isError(v)) { fprintf(stderr, "%s \n", LZ4F_getErrorName(v)); goto _output_error; }
#define CHECK(f)   { LZ4F_errorCode_t const CHECK_V(err_ , f); }

int basicTests(U32 seed, double compressibility)
{
#define COMPRESSIBLE_NOISE_LENGTH (2 MB)
    void* const CNBuffer = malloc(COMPRESSIBLE_NOISE_LENGTH);
    size_t const cBuffSize = LZ4F_compressFrameBound(COMPRESSIBLE_NOISE_LENGTH, NULL);
    void* const compressedBuffer = malloc(cBuffSize);
    void* const decodedBuffer = malloc(COMPRESSIBLE_NOISE_LENGTH);
    U32 randState = seed;
    size_t cSize, testSize;
    LZ4F_decompressionContext_t dCtx = NULL;
    LZ4F_compressionContext_t cctx = NULL;
    U64 crcOrig;
    int basicTests_error = 0;
    LZ4F_preferences_t prefs;
    memset(&prefs, 0, sizeof(prefs));

    if (!CNBuffer || !compressedBuffer || !decodedBuffer) {
        DISPLAY("allocation error, not enough memory to start fuzzer tests \n");
        goto _output_error;
    }
    FUZ_fillCompressibleNoiseBuffer(CNBuffer, COMPRESSIBLE_NOISE_LENGTH, compressibility, &randState);
    crcOrig = XXH64(CNBuffer, COMPRESSIBLE_NOISE_LENGTH, 1);

    /* LZ4F_compressBound() : special case : srcSize == 0 */
    DISPLAYLEVEL(3, "LZ4F_compressBound(0) = ");
    {   size_t const cBound = LZ4F_compressBound(0, NULL);
        if (cBound < 64 KB) goto _output_error;
        DISPLAYLEVEL(3, " %u \n", (U32)cBound);
    }

    /* Special case : null-content frame */
    testSize = 0;
    DISPLAYLEVEL(3, "LZ4F_compressFrame, compress null content : ");
    CHECK_V(cSize, LZ4F_compressFrame(compressedBuffer, LZ4F_compressFrameBound(testSize, NULL), CNBuffer, testSize, NULL));
    DISPLAYLEVEL(3, "null content encoded into a %u bytes frame \n", (unsigned)cSize);

    DISPLAYLEVEL(3, "LZ4F_createDecompressionContext \n");
    CHECK ( LZ4F_createDecompressionContext(&dCtx, LZ4F_VERSION) );

    DISPLAYLEVEL(3, "LZ4F_getFrameInfo on null-content frame (#157) \n");
    assert(cSize >= LZ4F_MIN_SIZE_TO_KNOW_HEADER_LENGTH);
    {   LZ4F_frameInfo_t frame_info;
        size_t const fhs = LZ4F_headerSize(compressedBuffer, LZ4F_MIN_SIZE_TO_KNOW_HEADER_LENGTH);
        size_t avail_in = fhs;
        CHECK( fhs );
        CHECK( LZ4F_getFrameInfo(dCtx, &frame_info, compressedBuffer, &avail_in) );
        if (avail_in != fhs) goto _output_error;  /* must consume all, since header size is supposed to be exact */
    }

    DISPLAYLEVEL(3, "LZ4F_freeDecompressionContext \n");
    CHECK( LZ4F_freeDecompressionContext(dCtx) );
    dCtx = NULL;

    /* test one-pass frame compression */
    testSize = COMPRESSIBLE_NOISE_LENGTH;

    DISPLAYLEVEL(3, "LZ4F_compressFrame, using fast level -3 : ");
    {   LZ4F_preferences_t fastCompressPrefs;
        memset(&fastCompressPrefs, 0, sizeof(fastCompressPrefs));
        fastCompressPrefs.compressionLevel = -3;
        CHECK_V(cSize, LZ4F_compressFrame(compressedBuffer, LZ4F_compressFrameBound(testSize, NULL), CNBuffer, testSize, &fastCompressPrefs));
        DISPLAYLEVEL(3, "Compressed %u bytes into a %u bytes frame \n", (U32)testSize, (U32)cSize);
    }

    DISPLAYLEVEL(3, "LZ4F_compressFrame, using default preferences : ");
    CHECK_V(cSize, LZ4F_compressFrame(compressedBuffer, LZ4F_compressFrameBound(testSize, NULL), CNBuffer, testSize, NULL));
    DISPLAYLEVEL(3, "Compressed %u bytes into a %u bytes frame \n", (U32)testSize, (U32)cSize);

    DISPLAYLEVEL(3, "Decompression test : \n");
    {   size_t decodedBufferSize = COMPRESSIBLE_NOISE_LENGTH;
        size_t compressedBufferSize = cSize;

        CHECK( LZ4F_createDecompressionContext(&dCtx, LZ4F_VERSION) );

        DISPLAYLEVEL(3, "Single Pass decompression : ");
        CHECK( LZ4F_decompress(dCtx, decodedBuffer, &decodedBufferSize, compressedBuffer, &compressedBufferSize, NULL) );
        { U64 const crcDest = XXH64(decodedBuffer, decodedBufferSize, 1);
          if (crcDest != crcOrig) goto _output_error; }
        DISPLAYLEVEL(3, "Regenerated %u bytes \n", (U32)decodedBufferSize);

        DISPLAYLEVEL(3, "Reusing decompression context \n");
        {   size_t const missingBytes = 4;
            size_t iSize = compressedBufferSize - missingBytes;
            const BYTE* cBuff = (const BYTE*) compressedBuffer;
            BYTE* const ostart = (BYTE*)decodedBuffer;
            BYTE* op = ostart;
            BYTE* const oend = (BYTE*)decodedBuffer + COMPRESSIBLE_NOISE_LENGTH;
            size_t decResult, oSize = COMPRESSIBLE_NOISE_LENGTH;
            DISPLAYLEVEL(3, "Missing last %u bytes : ", (U32)missingBytes);
            CHECK_V(decResult, LZ4F_decompress(dCtx, op, &oSize, cBuff, &iSize, NULL));
            if (decResult != missingBytes) {
                DISPLAY("%u bytes missing != %u bytes requested \n", (U32)missingBytes, (U32)decResult);
                goto _output_error;
            }
            DISPLAYLEVEL(3, "indeed, requests %u bytes \n", (unsigned)decResult);
            cBuff += iSize;
            iSize = decResult;
            op += oSize;
            oSize = (size_t)(oend-op);
            decResult = LZ4F_decompress(dCtx, op, &oSize, cBuff, &iSize, NULL);
            if (decResult != 0) goto _output_error;   /* should finish now */
            op += oSize;
            if (op>oend) { DISPLAY("decompression write overflow \n"); goto _output_error; }
            {   U64 const crcDest = XXH64(decodedBuffer, (size_t)(op-ostart), 1);
                if (crcDest != crcOrig) goto _output_error;
        }   }

        {   size_t oSize = 0;
            size_t iSize = 0;
            LZ4F_frameInfo_t fi;
            const BYTE* ip = (BYTE*)compressedBuffer;

            DISPLAYLEVEL(3, "Start by feeding 0 bytes, to get next input size : ");
            CHECK( LZ4F_decompress(dCtx, NULL, &oSize, ip, &iSize, NULL) );
            //DISPLAYLEVEL(3, " %u  \n", (unsigned)errorCode);
            DISPLAYLEVEL(3, " OK  \n");

            DISPLAYLEVEL(3, "LZ4F_getFrameInfo on zero-size input : ");
            {   size_t nullSize = 0;
                size_t const fiError = LZ4F_getFrameInfo(dCtx, &fi, ip, &nullSize);
                if (LZ4F_getErrorCode(fiError) != LZ4F_ERROR_frameHeader_incomplete) {
                    DISPLAYLEVEL(3, "incorrect error : %s != ERROR_frameHeader_incomplete \n",
                                    LZ4F_getErrorName(fiError));
                    goto _output_error;
                }
                DISPLAYLEVEL(3, " correctly failed : %s \n", LZ4F_getErrorName(fiError));
            }

            DISPLAYLEVEL(3, "LZ4F_getFrameInfo on not enough input : ");
            {   size_t inputSize = 6;
                size_t const fiError = LZ4F_getFrameInfo(dCtx, &fi, ip, &inputSize);
                if (LZ4F_getErrorCode(fiError) != LZ4F_ERROR_frameHeader_incomplete) {
                    DISPLAYLEVEL(3, "incorrect error : %s != ERROR_frameHeader_incomplete \n", LZ4F_getErrorName(fiError));
                    goto _output_error;
                }
                DISPLAYLEVEL(3, " correctly failed : %s \n", LZ4F_getErrorName(fiError));
            }

            DISPLAYLEVEL(3, "LZ4F_getFrameInfo on enough input : ");
            iSize = LZ4F_headerSize(ip, LZ4F_MIN_SIZE_TO_KNOW_HEADER_LENGTH);
            CHECK( iSize );
            CHECK( LZ4F_getFrameInfo(dCtx, &fi, ip, &iSize) );
            DISPLAYLEVEL(3, " correctly decoded \n");
        }

        DISPLAYLEVEL(3, "Decode a buggy input : ");
        assert(COMPRESSIBLE_NOISE_LENGTH > 64);
        assert(cSize > 48);
        memcpy(decodedBuffer, (char*)compressedBuffer+16, 32);  /* save correct data */
        memcpy((char*)compressedBuffer+16, (const char*)decodedBuffer+32, 32);  /* insert noise */
        {   size_t dbSize = COMPRESSIBLE_NOISE_LENGTH;
            size_t cbSize = cSize;
            size_t const decompressError = LZ4F_decompress(dCtx, decodedBuffer, &dbSize,
                                                               compressedBuffer, &cbSize,
                                                               NULL);
            if (!LZ4F_isError(decompressError)) goto _output_error;
            DISPLAYLEVEL(3, "error detected : %s \n", LZ4F_getErrorName(decompressError));
        }
        memcpy((char*)compressedBuffer+16, decodedBuffer, 32);  /* restore correct data */

        DISPLAYLEVEL(3, "Reset decompression context, since it's left in error state \n");
        LZ4F_resetDecompressionContext(dCtx);   /* always successful */

        DISPLAYLEVEL(3, "Byte after byte : ");
        {   BYTE* const ostart = (BYTE*)decodedBuffer;
            BYTE* op = ostart;
            BYTE* const oend = (BYTE*)decodedBuffer + COMPRESSIBLE_NOISE_LENGTH;
            const BYTE* ip = (const BYTE*) compressedBuffer;
            const BYTE* const iend = ip + cSize;
            while (ip < iend) {
                size_t oSize = (size_t)(oend-op);
                size_t iSize = 1;
                CHECK( LZ4F_decompress(dCtx, op, &oSize, ip, &iSize, NULL) );
                op += oSize;
                ip += iSize;
            }
            {   U64 const crcDest = XXH64(decodedBuffer, COMPRESSIBLE_NOISE_LENGTH, 1);
                if (crcDest != crcOrig) goto _output_error;
            }
            DISPLAYLEVEL(3, "Regenerated %u/%u bytes \n", (unsigned)(op-ostart), (unsigned)COMPRESSIBLE_NOISE_LENGTH);
        }
    }

    DISPLAYLEVEL(3, "Using 64 KB block : ");
    prefs.frameInfo.blockSizeID = LZ4F_max64KB;
    prefs.frameInfo.contentChecksumFlag = LZ4F_contentChecksumEnabled;
    CHECK_V(cSize, LZ4F_compressFrame(compressedBuffer, LZ4F_compressFrameBound(testSize, &prefs), CNBuffer, testSize, &prefs));
    DISPLAYLEVEL(3, "Compressed %i bytes into a %i bytes frame \n", (int)testSize, (int)cSize);

    DISPLAYLEVEL(3, "without checksum : ");
    prefs.frameInfo.contentChecksumFlag = LZ4F_noContentChecksum;
    CHECK_V(cSize, LZ4F_compressFrame(compressedBuffer, LZ4F_compressFrameBound(testSize, &prefs), CNBuffer, testSize, &prefs));
    DISPLAYLEVEL(3, "Compressed %i bytes into a %i bytes frame \n", (int)testSize, (int)cSize);

    DISPLAYLEVEL(3, "Using 256 KB block : ");
    prefs.frameInfo.blockSizeID = LZ4F_max256KB;
    prefs.frameInfo.contentChecksumFlag = LZ4F_contentChecksumEnabled;
    CHECK_V(cSize, LZ4F_compressFrame(compressedBuffer, LZ4F_compressFrameBound(testSize, &prefs), CNBuffer, testSize, &prefs));
    DISPLAYLEVEL(3, "Compressed %i bytes into a %i bytes frame \n", (int)testSize, (int)cSize);

    DISPLAYLEVEL(3, "Decompression test : \n");
    {   size_t const decodedBufferSize = COMPRESSIBLE_NOISE_LENGTH;
        unsigned const maxBits = FUZ_highbit((U32)decodedBufferSize);
        BYTE* const ostart = (BYTE*)decodedBuffer;
        BYTE* op = ostart;
        BYTE* const oend = ostart + COMPRESSIBLE_NOISE_LENGTH;
        const BYTE* ip = (const BYTE*)compressedBuffer;
        const BYTE* const iend = (const BYTE*)compressedBuffer + cSize;

        DISPLAYLEVEL(3, "random segment sizes : ");
        while (ip < iend) {
            unsigned const nbBits = FUZ_rand(&randState) % maxBits;
            size_t iSize = (FUZ_rand(&randState) & ((1<<nbBits)-1)) + 1;
            size_t oSize = (size_t)(oend-op);
            if (iSize > (size_t)(iend-ip)) iSize = (size_t)(iend-ip);
            CHECK( LZ4F_decompress(dCtx, op, &oSize, ip, &iSize, NULL) );
            op += oSize;
            ip += iSize;
        }
        {   size_t const decodedSize = (size_t)(op - ostart);
            U64 const crcDest = XXH64(decodedBuffer, decodedSize, 1);
            if (crcDest != crcOrig) goto _output_error;
            DISPLAYLEVEL(3, "Regenerated %u bytes \n", (U32)decodedSize);
         }

        CHECK( LZ4F_freeDecompressionContext(dCtx) );
        dCtx = NULL;
    }

    DISPLAYLEVEL(3, "without checksum : ");
    prefs.frameInfo.contentChecksumFlag = LZ4F_noContentChecksum;
    CHECK_V(cSize, LZ4F_compressFrame(compressedBuffer, LZ4F_compressFrameBound(testSize, &prefs), CNBuffer, testSize, &prefs) );
    DISPLAYLEVEL(3, "Compressed %i bytes into a %i bytes frame \n", (int)testSize, (int)cSize);

    DISPLAYLEVEL(3, "Using 1 MB block : ");
    prefs.frameInfo.blockSizeID = LZ4F_max1MB;
    prefs.frameInfo.contentChecksumFlag = LZ4F_contentChecksumEnabled;
    CHECK_V(cSize, LZ4F_compressFrame(compressedBuffer, LZ4F_compressFrameBound(testSize, &prefs), CNBuffer, testSize, &prefs) );
    DISPLAYLEVEL(3, "Compressed %i bytes into a %i bytes frame \n", (int)testSize, (int)cSize);

    DISPLAYLEVEL(3, "without frame checksum : ");
    prefs.frameInfo.contentChecksumFlag = LZ4F_noContentChecksum;
    CHECK_V(cSize, LZ4F_compressFrame(compressedBuffer, LZ4F_compressFrameBound(testSize, &prefs), CNBuffer, testSize, &prefs) );
    DISPLAYLEVEL(3, "Compressed %i bytes into a %i bytes frame \n", (int)testSize, (int)cSize);

    DISPLAYLEVEL(3, "Using 4 MB block : ");
    prefs.frameInfo.blockSizeID = LZ4F_max4MB;
    prefs.frameInfo.contentChecksumFlag = LZ4F_contentChecksumEnabled;
    {   size_t const dstCapacity = LZ4F_compressFrameBound(testSize, &prefs);
        DISPLAYLEVEL(4, "dstCapacity = %u  ; ", (U32)dstCapacity)
        CHECK_V(cSize, LZ4F_compressFrame(compressedBuffer, dstCapacity, CNBuffer, testSize, &prefs) );
        DISPLAYLEVEL(3, "Compressed %u bytes into a %u bytes frame \n", (U32)testSize, (U32)cSize);
    }

    DISPLAYLEVEL(3, "without frame checksum : ");
    prefs.frameInfo.contentChecksumFlag = LZ4F_noContentChecksum;
    {   size_t const dstCapacity = LZ4F_compressFrameBound(testSize, &prefs);
        DISPLAYLEVEL(4, "dstCapacity = %u  ; ", (U32)dstCapacity)
        CHECK_V(cSize, LZ4F_compressFrame(compressedBuffer, dstCapacity, CNBuffer, testSize, &prefs) );
        DISPLAYLEVEL(3, "Compressed %u bytes into a %u bytes frame \n", (U32)testSize, (U32)cSize);
    }

    DISPLAYLEVEL(3, "LZ4F_compressFrame with block checksum : ");
    memset(&prefs, 0, sizeof(prefs));
    prefs.frameInfo.blockChecksumFlag = LZ4F_blockChecksumEnabled;
    CHECK_V(cSize, LZ4F_compressFrame(compressedBuffer, LZ4F_compressFrameBound(testSize, &prefs), CNBuffer, testSize, &prefs) );
    DISPLAYLEVEL(3, "Compressed %i bytes into a %i bytes frame \n", (int)testSize, (int)cSize);

    DISPLAYLEVEL(3, "Decompress with block checksum : ");
    {   size_t iSize = cSize;
        size_t decodedSize = COMPRESSIBLE_NOISE_LENGTH;
        LZ4F_decompressionContext_t dctx;
        CHECK( LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION) );
        CHECK( LZ4F_decompress(dctx, decodedBuffer, &decodedSize, compressedBuffer, &iSize, NULL) );
        if (decodedSize != testSize) goto _output_error;
        if (iSize != cSize) goto _output_error;
        {   U64 const crcDest = XXH64(decodedBuffer, decodedSize, 1);
            U64 const crcSrc = XXH64(CNBuffer, testSize, 1);
            if (crcDest != crcSrc) goto _output_error;
        }
        DISPLAYLEVEL(3, "Regenerated %u bytes \n", (U32)decodedSize);

        CHECK( LZ4F_freeDecompressionContext(dctx) );
    }

    /* frame content size tests */
    {   size_t cErr;
        BYTE* const ostart = (BYTE*)compressedBuffer;
        BYTE* op = ostart;
        CHECK( LZ4F_createCompressionContext(&cctx, LZ4F_VERSION) );

        DISPLAYLEVEL(3, "compress without frameSize : ");
        memset(&(prefs.frameInfo), 0, sizeof(prefs.frameInfo));
        CHECK_V(cErr, LZ4F_compressBegin(cctx, compressedBuffer, testSize, &prefs));
        op += cErr;
        CHECK_V(cErr, LZ4F_compressUpdate(cctx, op, LZ4F_compressBound(testSize, &prefs), CNBuffer, testSize, NULL));
        op += cErr;
        CHECK( LZ4F_compressEnd(cctx, compressedBuffer, testSize, NULL) );
        DISPLAYLEVEL(3, "Compressed %i bytes into a %i bytes frame \n", (int)testSize, (int)(op-ostart));

        DISPLAYLEVEL(3, "compress with frameSize : ");
        prefs.frameInfo.contentSize = testSize;
        op = ostart;
        CHECK_V(cErr, LZ4F_compressBegin(cctx, compressedBuffer, testSize, &prefs));
        op += cErr;
        CHECK_V(cErr, LZ4F_compressUpdate(cctx, op, LZ4F_compressBound(testSize, &prefs), CNBuffer, testSize, NULL));
        op += cErr;
        CHECK( LZ4F_compressEnd(cctx, compressedBuffer, testSize, NULL) );
        DISPLAYLEVEL(3, "Compressed %i bytes into a %i bytes frame \n", (int)testSize, (int)(op-ostart));

        DISPLAYLEVEL(3, "compress with wrong frameSize : ");
        prefs.frameInfo.contentSize = testSize+1;
        op = ostart;
        CHECK_V(cErr, LZ4F_compressBegin(cctx, compressedBuffer, testSize, &prefs));
        op += cErr;
        CHECK_V(cErr, LZ4F_compressUpdate(cctx, op, LZ4F_compressBound(testSize, &prefs), CNBuffer, testSize, NULL));
        op += cErr;
        cErr = LZ4F_compressEnd(cctx, op, testSize, NULL);
        if (!LZ4F_isError(cErr)) goto _output_error;
        DISPLAYLEVEL(3, "Error correctly detected : %s \n", LZ4F_getErrorName(cErr));

        CHECK( LZ4F_freeCompressionContext(cctx) );
        cctx = NULL;
    }

    /* dictID tests */
    {   size_t cErr;
        U32 const dictID = 0x99;
        CHECK( LZ4F_createCompressionContext(&cctx, LZ4F_VERSION) );

        DISPLAYLEVEL(3, "insert a dictID : ");
        memset(&prefs.frameInfo, 0, sizeof(prefs.frameInfo));
        prefs.frameInfo.dictID = dictID;
        CHECK_V(cErr, LZ4F_compressBegin(cctx, compressedBuffer, testSize, &prefs));
        DISPLAYLEVEL(3, "created frame header of size %i bytes  \n", (int)cErr);

        DISPLAYLEVEL(3, "read a dictID : ");
        CHECK( LZ4F_createDecompressionContext(&dCtx, LZ4F_VERSION) );
        memset(&prefs.frameInfo, 0, sizeof(prefs.frameInfo));
        CHECK( LZ4F_getFrameInfo(dCtx, &prefs.frameInfo, compressedBuffer, &cErr) );
        if (prefs.frameInfo.dictID != dictID) goto _output_error;
        DISPLAYLEVEL(3, "%u \n", (U32)prefs.frameInfo.dictID);

        CHECK( LZ4F_freeDecompressionContext(dCtx) ); dCtx = NULL;
        CHECK( LZ4F_freeCompressionContext(cctx) ); cctx = NULL;
    }

    /* Dictionary compression test */
    {   size_t const dictSize = 63 KB;
        size_t const dstCapacity = LZ4F_compressFrameBound(dictSize, NULL);
        size_t cSizeNoDict, cSizeWithDict;
        LZ4F_CDict* const cdict = LZ4F_createCDict(CNBuffer, dictSize);
        if (cdict == NULL) goto _output_error;
        CHECK( LZ4F_createCompressionContext(&cctx, LZ4F_VERSION) );

        DISPLAYLEVEL(3, "LZ4F_compressFrame_usingCDict, with NULL dict : ");
        CHECK_V(cSizeNoDict,
                LZ4F_compressFrame_usingCDict(cctx, compressedBuffer, dstCapacity,
                                              CNBuffer, dictSize,
                                              NULL, NULL) );
        DISPLAYLEVEL(3, "%u bytes \n", (unsigned)cSizeNoDict);

        CHECK( LZ4F_freeCompressionContext(cctx) );
        CHECK( LZ4F_createCompressionContext(&cctx, LZ4F_VERSION) );
        DISPLAYLEVEL(3, "LZ4F_compressFrame_usingCDict, with dict : ");
        CHECK_V(cSizeWithDict,
                LZ4F_compressFrame_usingCDict(cctx, compressedBuffer, dstCapacity,
                                              CNBuffer, dictSize,
                                              cdict, NULL) );
        DISPLAYLEVEL(3, "compressed %u bytes into %u bytes \n",
                        (unsigned)dictSize, (unsigned)cSizeWithDict);
        if (cSizeWithDict >= cSizeNoDict) goto _output_error;  /* must be more efficient */
        crcOrig = XXH64(CNBuffer, dictSize, 0);

        DISPLAYLEVEL(3, "LZ4F_decompress_usingDict : ");
        {   LZ4F_dctx* dctx;
            size_t decodedSize = COMPRESSIBLE_NOISE_LENGTH;
            size_t compressedSize = cSizeWithDict;
            CHECK( LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION) );
            CHECK( LZ4F_decompress_usingDict(dctx,
                                        decodedBuffer, &decodedSize,
                                        compressedBuffer, &compressedSize,
                                        CNBuffer, dictSize,
                                        NULL) );
            if (compressedSize != cSizeWithDict) goto _output_error;
            if (decodedSize != dictSize) goto _output_error;
            { U64 const crcDest = XXH64(decodedBuffer, decodedSize, 0);
              if (crcDest != crcOrig) goto _output_error; }
            DISPLAYLEVEL(3, "Regenerated %u bytes \n", (U32)decodedSize);
            CHECK( LZ4F_freeDecompressionContext(dctx) );
        }

        DISPLAYLEVEL(3, "LZ4F_compressFrame_usingCDict, with dict, negative level : ");
        {   size_t cSizeLevelMax;
            LZ4F_preferences_t cParams;
            memset(&cParams, 0, sizeof(cParams));
            cParams.compressionLevel = -3;
            CHECK_V(cSizeLevelMax,
                LZ4F_compressFrame_usingCDict(cctx, compressedBuffer, dstCapacity,
                                              CNBuffer, dictSize,
                                              cdict, &cParams) );
            DISPLAYLEVEL(3, "%u bytes \n", (unsigned)cSizeLevelMax);
        }

        DISPLAYLEVEL(3, "LZ4F_compressFrame_usingCDict, with dict, level max : ");
        {   size_t cSizeLevelMax;
            LZ4F_preferences_t cParams;
            memset(&cParams, 0, sizeof(cParams));
            cParams.compressionLevel = LZ4F_compressionLevel_max();
            CHECK_V(cSizeLevelMax,
                LZ4F_compressFrame_usingCDict(cctx, compressedBuffer, dstCapacity,
                                              CNBuffer, dictSize,
                                              cdict, &cParams) );
            DISPLAYLEVEL(3, "%u bytes \n", (unsigned)cSizeLevelMax);
        }

        DISPLAYLEVEL(3, "LZ4F_compressFrame_usingCDict, multiple linked blocks : ");
        {   size_t cSizeContiguous;
            size_t const inSize = dictSize * 3;
            size_t const outCapacity = LZ4F_compressFrameBound(inSize, NULL);
            LZ4F_preferences_t cParams;
            memset(&cParams, 0, sizeof(cParams));
            cParams.frameInfo.blockMode = LZ4F_blockLinked;
            cParams.frameInfo.blockSizeID = LZ4F_max64KB;
            CHECK_V(cSizeContiguous,
                LZ4F_compressFrame_usingCDict(cctx, compressedBuffer, outCapacity,
                                              CNBuffer, inSize,
                                              cdict, &cParams) );
            DISPLAYLEVEL(3, "compressed %u bytes into %u bytes \n",
                        (unsigned)inSize, (unsigned)cSizeContiguous);

            DISPLAYLEVEL(3, "LZ4F_decompress_usingDict on multiple linked blocks : ");
            {   LZ4F_dctx* dctx;
                size_t decodedSize = COMPRESSIBLE_NOISE_LENGTH;
                size_t compressedSize = cSizeContiguous;
                CHECK( LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION) );
                CHECK( LZ4F_decompress_usingDict(dctx,
                                            decodedBuffer, &decodedSize,
                                            compressedBuffer, &compressedSize,
                                            CNBuffer, dictSize,
                                            NULL) );
                if (compressedSize != cSizeContiguous) goto _output_error;
                if (decodedSize != inSize) goto _output_error;
                crcOrig = XXH64(CNBuffer, inSize, 0);
                { U64 const crcDest = XXH64(decodedBuffer, decodedSize, 0);
                  if (crcDest != crcOrig) goto _output_error; }
                DISPLAYLEVEL(3, "Regenerated %u bytes \n", (U32)decodedSize);
                CHECK( LZ4F_freeDecompressionContext(dctx) );
            }
        }


        DISPLAYLEVEL(3, "LZ4F_compressFrame_usingCDict, multiple independent blocks : ");
        {   size_t cSizeIndep;
            size_t const inSize = dictSize * 3;
            size_t const outCapacity = LZ4F_compressFrameBound(inSize, NULL);
            LZ4F_preferences_t cParams;
            memset(&cParams, 0, sizeof(cParams));
            cParams.frameInfo.blockMode = LZ4F_blockIndependent;
            cParams.frameInfo.blockSizeID = LZ4F_max64KB;
            CHECK_V(cSizeIndep,
                LZ4F_compressFrame_usingCDict(cctx, compressedBuffer, outCapacity,
                                              CNBuffer, inSize,
                                              cdict, &cParams) );
            DISPLAYLEVEL(3, "compressed %u bytes into %u bytes \n",
                        (unsigned)inSize, (unsigned)cSizeIndep);

            DISPLAYLEVEL(3, "LZ4F_decompress_usingDict on multiple independent blocks : ");
            {   LZ4F_dctx* dctx;
                size_t decodedSize = COMPRESSIBLE_NOISE_LENGTH;
                size_t compressedSize = cSizeIndep;
                CHECK( LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION) );
                CHECK( LZ4F_decompress_usingDict(dctx,
                                            decodedBuffer, &decodedSize,
                                            compressedBuffer, &compressedSize,
                                            CNBuffer, dictSize,
                                            NULL) );
                if (compressedSize != cSizeIndep) goto _output_error;
                if (decodedSize != inSize) goto _output_error;
                crcOrig = XXH64(CNBuffer, inSize, 0);
                { U64 const crcDest = XXH64(decodedBuffer, decodedSize, 0);
                  if (crcDest != crcOrig) goto _output_error; }
                DISPLAYLEVEL(3, "Regenerated %u bytes \n", (U32)decodedSize);
                CHECK( LZ4F_freeDecompressionContext(dctx) );
            }
        }

        LZ4F_freeCDict(cdict);
        CHECK( LZ4F_freeCompressionContext(cctx) ); cctx = NULL;
    }

    DISPLAYLEVEL(3, "getBlockSize test: \n");
    { size_t result;
      unsigned blockSizeID;
      for (blockSizeID = 4; blockSizeID < 8; ++blockSizeID) {
        result = LZ4F_getBlockSize(blockSizeID);
        CHECK(result);
        DISPLAYLEVEL(3, "Returned block size of %u bytes for blockID %u \n",
                         (unsigned)result, blockSizeID);
      }

      /* Test an invalid input that's too large */
      result = LZ4F_getBlockSize(8);
      if(!LZ4F_isError(result) ||
          LZ4F_getErrorCode(result) != LZ4F_ERROR_maxBlockSize_invalid)
        goto _output_error;

      /* Test an invalid input that's too small */
      result = LZ4F_getBlockSize(3);
      if(!LZ4F_isError(result) ||
          LZ4F_getErrorCode(result) != LZ4F_ERROR_maxBlockSize_invalid)
        goto _output_error;
    }


    DISPLAYLEVEL(3, "Skippable frame test : \n");
    {   size_t decodedBufferSize = COMPRESSIBLE_NOISE_LENGTH;
        unsigned maxBits = FUZ_highbit((U32)decodedBufferSize);
        BYTE* op = (BYTE*)decodedBuffer;
        BYTE* const oend = (BYTE*)decodedBuffer + COMPRESSIBLE_NOISE_LENGTH;
        BYTE* ip = (BYTE*)compressedBuffer;
        BYTE* iend = (BYTE*)compressedBuffer + cSize + 8;

        CHECK( LZ4F_createDecompressionContext(&dCtx, LZ4F_VERSION) );

        /* generate skippable frame */
        FUZ_writeLE32(ip, LZ4F_MAGIC_SKIPPABLE_START);
        FUZ_writeLE32(ip+4, (U32)cSize);

        DISPLAYLEVEL(3, "random segment sizes : \n");
        while (ip < iend) {
            unsigned nbBits = FUZ_rand(&randState) % maxBits;
            size_t iSize = (FUZ_rand(&randState) & ((1<<nbBits)-1)) + 1;
            size_t oSize = oend-op;
            if (iSize > (size_t)(iend-ip)) iSize = iend-ip;
            CHECK( LZ4F_decompress(dCtx, op, &oSize, ip, &iSize, NULL) );
            op += oSize;
            ip += iSize;
        }
        DISPLAYLEVEL(3, "Skipped %i bytes \n", (int)decodedBufferSize);

        /* generate zero-size skippable frame */
        DISPLAYLEVEL(3, "zero-size skippable frame\n");
        ip = (BYTE*)compressedBuffer;
        op = (BYTE*)decodedBuffer;
        FUZ_writeLE32(ip, LZ4F_MAGIC_SKIPPABLE_START+1);
        FUZ_writeLE32(ip+4, 0);
        iend = ip+8;

        while (ip < iend) {
            unsigned const nbBits = FUZ_rand(&randState) % maxBits;
            size_t iSize = (FUZ_rand(&randState) & ((1<<nbBits)-1)) + 1;
            size_t oSize = oend-op;
            if (iSize > (size_t)(iend-ip)) iSize = iend-ip;
            CHECK( LZ4F_decompress(dCtx, op, &oSize, ip, &iSize, NULL) );
            op += oSize;
            ip += iSize;
        }
        DISPLAYLEVEL(3, "Skipped %i bytes \n", (int)(ip - (BYTE*)compressedBuffer - 8));

        DISPLAYLEVEL(3, "Skippable frame header complete in first call \n");
        ip = (BYTE*)compressedBuffer;
        op = (BYTE*)decodedBuffer;
        FUZ_writeLE32(ip, LZ4F_MAGIC_SKIPPABLE_START+2);
        FUZ_writeLE32(ip+4, 10);
        iend = ip+18;
        while (ip < iend) {
            size_t iSize = 10;
            size_t oSize = 10;
            if (iSize > (size_t)(iend-ip)) iSize = iend-ip;
            CHECK( LZ4F_decompress(dCtx, op, &oSize, ip, &iSize, NULL) );
            op += oSize;
            ip += iSize;
        }
        DISPLAYLEVEL(3, "Skipped %i bytes \n", (int)(ip - (BYTE*)compressedBuffer - 8));
    }

    DISPLAY("Basic tests completed \n");
_end:
    free(CNBuffer);
    free(compressedBuffer);
    free(decodedBuffer);
    LZ4F_freeDecompressionContext(dCtx); dCtx = NULL;
    LZ4F_freeCompressionContext(cctx); cctx = NULL;
    return basicTests_error;

_output_error:
    basicTests_error = 1;
    DISPLAY("Error detected ! \n");
    goto _end;
}


static void locateBuffDiff(const void* buff1, const void* buff2, size_t size, unsigned nonContiguous)
{
    size_t p=0;
    const BYTE* b1=(const BYTE*)buff1;
    const BYTE* b2=(const BYTE*)buff2;
    DISPLAY("locateBuffDiff: looking for error position \n");
    if (nonContiguous) {
        DISPLAY("mode %u: non-contiguous output (%u bytes), cannot search \n",
                nonContiguous, (unsigned)size);
        return;
    }
    while (p < size && b1[p]==b2[p]) p++;
    if (p != size) {
        DISPLAY("Error at pos %i/%i : %02X != %02X \n", (int)p, (int)size, b1[p], b2[p]);
    }
}


int fuzzerTests(U32 seed, unsigned nbTests, unsigned startTest, double compressibility, U32 duration_s)
{
    int testResult = 0;
    unsigned testNb = 0;
    size_t const srcDataLength = 9 MB;  /* needs to be > 2x4MB to test large blocks */
    void* srcBuffer = NULL;
    size_t const compressedBufferSize = LZ4F_compressFrameBound(srcDataLength, NULL) + 4 MB;  /* needs some margin */
    void* compressedBuffer = NULL;
    void* decodedBuffer = NULL;
    U32 coreRand = seed;
    LZ4F_decompressionContext_t dCtx = NULL;
    LZ4F_compressionContext_t cCtx = NULL;
    clock_t const startClock = clock();
    clock_t const clockDuration = duration_s * CLOCKS_PER_SEC;
#   undef CHECK
#   define EXIT_MSG(...) { DISPLAY("Error => "); DISPLAY(__VA_ARGS__); \
                           DISPLAY(" (seed %u, test nb %u)  \n", seed, testNb); goto _output_error; }
#   define CHECK(cond, ...) { if (cond) { EXIT_MSG(__VA_ARGS__); } }

    /* Create buffers */
    {   size_t const creationStatus = LZ4F_createDecompressionContext(&dCtx, LZ4F_VERSION);
        CHECK(LZ4F_isError(creationStatus), "Allocation failed (error %i)", (int)creationStatus); }
    {   size_t const creationStatus = LZ4F_createCompressionContext(&cCtx, LZ4F_VERSION);
        CHECK(LZ4F_isError(creationStatus), "Allocation failed (error %i)", (int)creationStatus); }
    srcBuffer = malloc(srcDataLength);
    CHECK(srcBuffer==NULL, "srcBuffer Allocation failed");
    compressedBuffer = malloc(compressedBufferSize);
    CHECK(compressedBuffer==NULL, "compressedBuffer Allocation failed");
    decodedBuffer = calloc(1, srcDataLength);   /* calloc avoids decodedBuffer being considered "garbage" by scan-build */
    CHECK(decodedBuffer==NULL, "decodedBuffer Allocation failed");
    FUZ_fillCompressibleNoiseBuffer(srcBuffer, srcDataLength, compressibility, &coreRand);

    /* jump to requested testNb */
    for (testNb =0; (testNb < startTest); testNb++) (void)FUZ_rand(&coreRand);   /* sync randomizer */

    /* main fuzzer test loop */
    for ( ; (testNb < nbTests) || (clockDuration > FUZ_GetClockSpan(startClock)) ; testNb++) {
        U32 randState = coreRand ^ prime1;
        unsigned const srcBits = (FUZ_rand(&randState) % (FUZ_highbit((U32)(srcDataLength-1)) - 1)) + 1;
        size_t const srcSize = (FUZ_rand(&randState) & ((1<<srcBits)-1)) + 1;
        size_t const srcStartId = FUZ_rand(&randState) % (srcDataLength - srcSize);
        const BYTE* const srcStart = (const BYTE*)srcBuffer + srcStartId;
        unsigned const neverFlush = (FUZ_rand(&randState) & 15) == 1;
        U64 const crcOrig = XXH64(srcStart, srcSize, 1);
        LZ4F_preferences_t prefs;
        const LZ4F_preferences_t* prefsPtr = &prefs;
        size_t cSize;

        (void)FUZ_rand(&coreRand);   /* update seed */
        memset(&prefs, 0, sizeof(prefs));
        prefs.frameInfo.blockMode = (LZ4F_blockMode_t)(FUZ_rand(&randState) & 1);
        prefs.frameInfo.blockSizeID = (LZ4F_blockSizeID_t)(4 + (FUZ_rand(&randState) & 3));
        prefs.frameInfo.blockChecksumFlag = (LZ4F_blockChecksum_t)(FUZ_rand(&randState) & 1);
        prefs.frameInfo.contentChecksumFlag = (LZ4F_contentChecksum_t)(FUZ_rand(&randState) & 1);
        prefs.frameInfo.contentSize = ((FUZ_rand(&randState) & 0xF) == 1) ? srcSize : 0;
        prefs.autoFlush = neverFlush ? 0 : (FUZ_rand(&randState) & 7) == 2;
        prefs.compressionLevel = -5 + (int)(FUZ_rand(&randState) % 11);
        if ((FUZ_rand(&randState) & 0xF) == 1) prefsPtr = NULL;

        DISPLAYUPDATE(2, "\r%5u   ", testNb);

        if ((FUZ_rand(&randState) & 0xFFF) == 0) {
            /* create a skippable frame (rare case) */
            BYTE* op = (BYTE*)compressedBuffer;
            FUZ_writeLE32(op, LZ4F_MAGIC_SKIPPABLE_START + (FUZ_rand(&randState) & 15));
            FUZ_writeLE32(op+4, (U32)srcSize);
            cSize = srcSize+8;
        } else if ((FUZ_rand(&randState) & 0xF) == 2) {  /* single pass compression (simple) */
            cSize = LZ4F_compressFrame(compressedBuffer, LZ4F_compressFrameBound(srcSize, prefsPtr), srcStart, srcSize, prefsPtr);
            CHECK(LZ4F_isError(cSize), "LZ4F_compressFrame failed : error %i (%s)", (int)cSize, LZ4F_getErrorName(cSize));
        } else {   /* multi-segments compression */
            const BYTE* ip = srcStart;
            const BYTE* const iend = srcStart + srcSize;
            BYTE* op = (BYTE*)compressedBuffer;
            BYTE* const oend = op + (neverFlush ? LZ4F_compressFrameBound(srcSize, prefsPtr) : compressedBufferSize);  /* when flushes are possible, can't guarantee a max compressed size */
            unsigned const maxBits = FUZ_highbit((U32)srcSize);
            size_t cSegmentSize;
            LZ4F_compressOptions_t cOptions;
            memset(&cOptions, 0, sizeof(cOptions));
            cSegmentSize = LZ4F_compressBegin(cCtx, op, (size_t)(oend-op), prefsPtr);
            CHECK(LZ4F_isError(cSegmentSize), "Compression header failed (error %i)",
                                            (int)cSegmentSize);
            op += cSegmentSize;
            while (ip < iend) {
                unsigned const nbBitsSeg = FUZ_rand(&randState) % maxBits;
                size_t const sampleMax = (FUZ_rand(&randState) & ((1<<nbBitsSeg)-1)) + 1;
                size_t const iSize = MIN(sampleMax, (size_t)(iend-ip));
                size_t const oSize = LZ4F_compressBound(iSize, prefsPtr);
                size_t flushedSize;
                cOptions.stableSrc = ((FUZ_rand(&randState) & 3) == 1);
                DISPLAYLEVEL(6, "Sending %u bytes to compress (stableSrc:%u) \n",
                                (unsigned)iSize, cOptions.stableSrc);

                flushedSize = LZ4F_compressUpdate(cCtx, op, oSize, ip, iSize, &cOptions);
                CHECK(LZ4F_isError(flushedSize), "Compression failed (error %i : %s)",
                            (int)flushedSize, LZ4F_getErrorName(flushedSize));
                op += flushedSize;
                ip += iSize;

                {   unsigned const forceFlush = neverFlush ? 0 : ((FUZ_rand(&randState) & 3) == 1);
                    if (forceFlush) {
                        size_t const flushSize = LZ4F_flush(cCtx, op, (size_t)(oend-op), &cOptions);
                        DISPLAYLEVEL(6,"flushing %u bytes \n", (unsigned)flushSize);
                        CHECK(LZ4F_isError(flushSize), "Compression failed (error %i)", (int)flushSize);
                        op += flushSize;
                }   }
            }
            CHECK(op>=oend, "LZ4F_compressFrameBound overflow");
            {   size_t const dstEndSafeSize = LZ4F_compressBound(0, prefsPtr);
                int const tooSmallDstEnd = ((FUZ_rand(&randState) & 31) == 3);
                size_t const dstEndTooSmallSize = (FUZ_rand(&randState) % dstEndSafeSize) + 1;
                size_t const dstEndSize = tooSmallDstEnd ? dstEndTooSmallSize : dstEndSafeSize;
                BYTE const canaryByte = (BYTE)(FUZ_rand(&randState) & 255);
                size_t flushedSize;
                DISPLAYLEVEL(7,"canaryByte at pos %u / %u \n",
                            (unsigned)((size_t)(op - (BYTE*)compressedBuffer) + dstEndSize),
                            (unsigned)compressedBufferSize);
                assert(op + dstEndSize < (BYTE*)compressedBuffer + compressedBufferSize);
                op[dstEndSize] = canaryByte;
                flushedSize = LZ4F_compressEnd(cCtx, op, dstEndSize, &cOptions);
                CHECK(op[dstEndSize] != canaryByte, "LZ4F_compressEnd writes beyond dstCapacity !");
                if (LZ4F_isError(flushedSize)) {
                    if (tooSmallDstEnd) /* failure is allowed */ continue;
                    CHECK(1, "Compression completion failed (error %i : %s)",
                            (int)flushedSize, LZ4F_getErrorName(flushedSize));
                }
                op += flushedSize;
            }
            cSize = (size_t)(op - (BYTE*)compressedBuffer);
            DISPLAYLEVEL(5, "\nCompressed %u bytes into %u \n", (U32)srcSize, (U32)cSize);
        }

        /* multi-segments decompression */
        {   const BYTE* ip = (const BYTE*)compressedBuffer;
            const BYTE* const iend = ip + cSize;
            BYTE* op = (BYTE*)decodedBuffer;
            BYTE* const oend = op + srcDataLength;
            unsigned const suggestedBits = FUZ_highbit((U32)cSize);
            unsigned const maxBits = MAX(3, suggestedBits);
            unsigned const nonContiguousDst = FUZ_rand(&randState) % 3;   /* 0 : contiguous; 1 : non-contiguous; 2 : dst overwritten */
            size_t totalOut = 0;
            size_t decSize = 0;
            XXH64_state_t xxh64;
            XXH64_reset(&xxh64, 1);
            assert(ip < iend);
            while (ip < iend) {
                unsigned const nbBitsI = (FUZ_rand(&randState) % (maxBits-1)) + 1;
                unsigned const nbBitsO = (FUZ_rand(&randState) % (maxBits)) + 1;
                size_t const iSizeMax = (FUZ_rand(&randState) & ((1<<nbBitsI)-1)) + 1;
                size_t iSize = MIN(iSizeMax, (size_t)(iend-ip));
                size_t const oSizeMax = (FUZ_rand(&randState) & ((1<<nbBitsO)-1)) + 2;
                size_t oSize = MIN(oSizeMax, (size_t)(oend-op));
                LZ4F_decompressOptions_t dOptions;
                memset(&dOptions, 0, sizeof(dOptions));
                dOptions.stableDst = FUZ_rand(&randState) & 1;
                if (nonContiguousDst==2) dOptions.stableDst = 0;   /* overwrite mode */
                decSize = LZ4F_decompress(dCtx, op, &oSize, ip, &iSize, &dOptions);
                if (LZ4F_getErrorCode(decSize) == LZ4F_ERROR_contentChecksum_invalid)
                    locateBuffDiff(srcStart, decodedBuffer, srcSize, nonContiguousDst);
                CHECK(LZ4F_isError(decSize), "Decompression failed (error %i:%s)",
                                    (int)decSize, LZ4F_getErrorName(decSize));
                XXH64_update(&xxh64, op, (U32)oSize);
                totalOut += oSize;
                op += oSize;
                ip += iSize;
                op += nonContiguousDst;
                if (nonContiguousDst==2) op = (BYTE*)decodedBuffer;   /* overwritten destination */
            }
            CHECK(decSize != 0, "Frame decompression failed (error %i)", (int)decSize);
            if (totalOut) {  /* otherwise, it's a skippable frame */
                U64 const crcDecoded = XXH64_digest(&xxh64);
                if (crcDecoded != crcOrig) {
                    locateBuffDiff(srcStart, decodedBuffer, srcSize, nonContiguousDst);
                    EXIT_MSG("Decompression corruption");
            }   }
        }
    }

    DISPLAYLEVEL(2, "\rAll tests completed   \n");

_end:
    LZ4F_freeDecompressionContext(dCtx);
    LZ4F_freeCompressionContext(cCtx);
    free(srcBuffer);
    free(compressedBuffer);
    free(decodedBuffer);

    if (use_pause) {
        DISPLAY("press enter to finish \n");
        (void)getchar();
    }
    return testResult;

_output_error:
    testResult = 1;
    goto _end;
}


int FUZ_usage(const char* programName)
{
    DISPLAY( "Usage :\n");
    DISPLAY( "      %s [args]\n", programName);
    DISPLAY( "\n");
    DISPLAY( "Arguments :\n");
    DISPLAY( " -i#    : Nb of tests (default:%u) \n", nbTestsDefault);
    DISPLAY( " -T#    : Duration of tests, in seconds (default: use Nb of tests) \n");
    DISPLAY( " -s#    : Select seed (default:prompt user)\n");
    DISPLAY( " -t#    : Select starting test number (default:0)\n");
    DISPLAY( " -P#    : Select compressibility in %% (default:%i%%)\n", FUZ_COMPRESSIBILITY_DEFAULT);
    DISPLAY( " -v     : verbose\n");
    DISPLAY( " -h     : display help and exit\n");
    return 0;
}


int main(int argc, const char** argv)
{
    U32 seed=0;
    int seedset=0;
    int argNb;
    int nbTests = nbTestsDefault;
    int testNb = 0;
    int proba = FUZ_COMPRESSIBILITY_DEFAULT;
    int result=0;
    U32 duration=0;
    const char* const programName = argv[0];

    /* Check command line */
    for (argNb=1; argNb<argc; argNb++) {
        const char* argument = argv[argNb];

        if(!argument) continue;   /* Protection if argument empty */

        /* Decode command (note : aggregated short commands are allowed) */
        if (argument[0]=='-') {
            if (!strcmp(argument, "--no-prompt")) {
                no_prompt=1;
                seedset=1;
                displayLevel=1;
                continue;
            }
            argument++;

            while (*argument!=0) {
                switch(*argument)
                {
                case 'h':
                    return FUZ_usage(programName);
                case 'v':
                    argument++;
                    displayLevel++;
                    break;
                case 'q':
                    argument++;
                    displayLevel--;
                    break;
                case 'p': /* pause at the end */
                    argument++;
                    use_pause = 1;
                    break;

                case 'i':
                    argument++;
                    nbTests=0; duration=0;
                    while ((*argument>='0') && (*argument<='9')) {
                        nbTests *= 10;
                        nbTests += *argument - '0';
                        argument++;
                    }
                    break;

                case 'T':
                    argument++;
                    nbTests = 0; duration = 0;
                    for (;;) {
                        switch(*argument)
                        {
                            case 'm': duration *= 60; argument++; continue;
                            case 's':
                            case 'n': argument++; continue;
                            case '0':
                            case '1':
                            case '2':
                            case '3':
                            case '4':
                            case '5':
                            case '6':
                            case '7':
                            case '8':
                            case '9': duration *= 10; duration += *argument++ - '0'; continue;
                        }
                        break;
                    }
                    break;

                case 's':
                    argument++;
                    seed=0;
                    seedset=1;
                    while ((*argument>='0') && (*argument<='9')) {
                        seed *= 10;
                        seed += *argument - '0';
                        argument++;
                    }
                    break;
                case 't':
                    argument++;
                    testNb=0;
                    while ((*argument>='0') && (*argument<='9')) {
                        testNb *= 10;
                        testNb += *argument - '0';
                        argument++;
                    }
                    break;
                case 'P':   /* compressibility % */
                    argument++;
                    proba=0;
                    while ((*argument>='0') && (*argument<='9')) {
                        proba *= 10;
                        proba += *argument - '0';
                        argument++;
                    }
                    if (proba<0) proba=0;
                    if (proba>100) proba=100;
                    break;
                default:
                    ;
                    return FUZ_usage(programName);
                }
            }
        }
    }

    /* Get Seed */
    DISPLAY("Starting lz4frame tester (%i-bits, %s)\n", (int)(sizeof(size_t)*8), LZ4_VERSION_STRING);

    if (!seedset) {
        time_t const t = time(NULL);
        U32 const h = XXH32(&t, sizeof(t), 1);
        seed = h % 10000;
    }
    DISPLAY("Seed = %u\n", seed);
    if (proba!=FUZ_COMPRESSIBILITY_DEFAULT) DISPLAY("Compressibility : %i%%\n", proba);

    if (nbTests<=0) nbTests=1;

    if (testNb==0) result = basicTests(seed, ((double)proba) / 100);
    if (result) return 1;
    return fuzzerTests(seed, nbTests, testNb, ((double)proba) / 100, duration);
}
