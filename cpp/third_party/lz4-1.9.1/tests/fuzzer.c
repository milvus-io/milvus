/*
    fuzzer.c - Fuzzer test tool for LZ4
    Copyright (C) Yann Collet 2012-2017

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
    - LZ4 source repo : https://github.com/lz4/lz4
*/

/*-************************************
*  Compiler options
**************************************/
#ifdef _MSC_VER    /* Visual Studio */
#  pragma warning(disable : 4127)    /* disable: C4127: conditional expression is constant */
#  pragma warning(disable : 4146)    /* disable: C4146: minus unsigned expression */
#  pragma warning(disable : 4310)    /* disable: C4310: constant char value > 127 */
#endif


/*-************************************
*  Dependencies
**************************************/
#if defined(__unix__) && !defined(_AIX)   /* must be included before platform.h for MAP_ANONYMOUS */
#  undef  _GNU_SOURCE     /* in case it's already defined */
#  define _GNU_SOURCE     /* MAP_ANONYMOUS even in -std=c99 mode */
#  include <sys/mman.h>   /* mmap */
#endif
#include "platform.h"   /* _CRT_SECURE_NO_WARNINGS */
#include "util.h"       /* U32 */
#include <stdlib.h>
#include <stdio.h>      /* fgets, sscanf */
#include <string.h>     /* strcmp */
#include <time.h>       /* clock_t, clock, CLOCKS_PER_SEC */
#include <assert.h>
#include <limits.h>     /* INT_MAX */

#if defined(_AIX)
#  include <sys/mman.h>   /* mmap */
#endif

#define LZ4_DISABLE_DEPRECATE_WARNINGS   /* LZ4_decompress_fast */
#define LZ4_STATIC_LINKING_ONLY
#include "lz4.h"
#define LZ4_HC_STATIC_LINKING_ONLY
#include "lz4hc.h"
#define XXH_STATIC_LINKING_ONLY
#include "xxhash.h"


/*-************************************
*  Basic Types
**************************************/
#if !defined(__cplusplus) && !(defined (__STDC_VERSION__) && (__STDC_VERSION__ >= 199901L) /* C99 */)
typedef size_t uintptr_t;   /* true on most systems, except OpenVMS-64 (which doesn't need address overflow test) */
#endif


/*-************************************
*  Constants
**************************************/
#define NB_ATTEMPTS (1<<16)
#define COMPRESSIBLE_NOISE_LENGTH (1 << 21)
#define FUZ_MAX_BLOCK_SIZE (1 << 17)
#define FUZ_MAX_DICT_SIZE  (1 << 15)
#define FUZ_COMPRESSIBILITY_DEFAULT 60
#define PRIME1   2654435761U
#define PRIME2   2246822519U
#define PRIME3   3266489917U

#define KB *(1U<<10)
#define MB *(1U<<20)
#define GB *(1U<<30)


/*-***************************************
*  Macros
*****************************************/
#define DISPLAY(...)         fprintf(stdout, __VA_ARGS__)
#define DISPLAYLEVEL(l, ...) if (g_displayLevel>=l) { DISPLAY(__VA_ARGS__); }
static int g_displayLevel = 2;

#define MIN(a,b)   ( (a) < (b) ? (a) : (b) )


/*-*******************************************************
*  Fuzzer functions
*********************************************************/
static clock_t FUZ_GetClockSpan(clock_t clockStart)
{
    return clock() - clockStart;   /* works even if overflow; max span ~ 30mn */
}

static void FUZ_displayUpdate(unsigned testNb)
{
    static clock_t g_time = 0;
    static const clock_t g_refreshRate = CLOCKS_PER_SEC / 5;
    if ((FUZ_GetClockSpan(g_time) > g_refreshRate) || (g_displayLevel>=4)) {
        g_time = clock();
        DISPLAY("\r%5u   ", testNb);
        fflush(stdout);
    }
}

static U32 FUZ_rotl32(U32 u32, U32 nbBits)
{
    return ((u32 << nbBits) | (u32 >> (32 - nbBits)));
}

static U32 FUZ_rand(U32* src)
{
    U32 rand32 = *src;
    rand32 *= PRIME1;
    rand32 ^= PRIME2;
    rand32  = FUZ_rotl32(rand32, 13);
    *src = rand32;
    return rand32;
}


#define FUZ_RAND15BITS  ((FUZ_rand(seed) >> 3) & 32767)
#define FUZ_RANDLENGTH  ( ((FUZ_rand(seed) >> 7) & 3) ? (FUZ_rand(seed) % 15) : (FUZ_rand(seed) % 510) + 15)
static void FUZ_fillCompressibleNoiseBuffer(void* buffer, size_t bufferSize, double proba, U32* seed)
{
    BYTE* const BBuffer = (BYTE*)buffer;
    size_t pos = 0;
    U32 const P32 = (U32)(32768 * proba);

    /* First Bytes */
    while (pos < 20)
        BBuffer[pos++] = (BYTE)(FUZ_rand(seed));

    while (pos < bufferSize) {
        /* Select : Literal (noise) or copy (within 64K) */
        if (FUZ_RAND15BITS < P32) {
            /* Copy (within 64K) */
            size_t const length = (size_t)FUZ_RANDLENGTH + 4;
            size_t const d = MIN(pos+length, bufferSize);
            size_t match;
            size_t offset = (size_t)FUZ_RAND15BITS + 1;
            while (offset > pos) offset >>= 1;
            match = pos - offset;
            while (pos < d) BBuffer[pos++] = BBuffer[match++];
        } else {
            /* Literal (noise) */
            size_t const length = FUZ_RANDLENGTH;
            size_t const d = MIN(pos+length, bufferSize);
            while (pos < d) BBuffer[pos++] = (BYTE)(FUZ_rand(seed) >> 5);
        }
    }
}


#define MAX_NB_BUFF_I134 150
#define BLOCKSIZE_I134   (32 MB)
/*! FUZ_AddressOverflow() :
*   Aggressively pushes memory allocation limits,
*   and generates patterns which create address space overflow.
*   only possible in 32-bits mode */
static int FUZ_AddressOverflow(void)
{
    char* buffers[MAX_NB_BUFF_I134+1];
    int nbBuff=0;
    int highAddress = 0;

    DISPLAY("Overflow tests : ");

    /* Only possible in 32-bits */
    if (sizeof(void*)==8) {
        DISPLAY("64 bits mode : no overflow \n");
        fflush(stdout);
        return 0;
    }

    buffers[0] = (char*)malloc(BLOCKSIZE_I134);
    buffers[1] = (char*)malloc(BLOCKSIZE_I134);
    if ((!buffers[0]) || (!buffers[1])) {
        free(buffers[0]); free(buffers[1]);
        DISPLAY("not enough memory for tests \n");
        return 0;
    }

    for (nbBuff=2; nbBuff < MAX_NB_BUFF_I134; nbBuff++) {
        DISPLAY("%3i \b\b\b\b", nbBuff); fflush(stdout);
        buffers[nbBuff] = (char*)malloc(BLOCKSIZE_I134);
        if (buffers[nbBuff]==NULL) goto _endOfTests;

        if (((uintptr_t)buffers[nbBuff] > (uintptr_t)0x80000000) && (!highAddress)) {
            DISPLAY("high address detected : ");
            fflush(stdout);
            highAddress=1;
        }

        {   size_t const sizeToGenerateOverflow = (size_t)(- ((uintptr_t)buffers[nbBuff-1]) + 512);
            unsigned const nbOf255 = (unsigned)((sizeToGenerateOverflow / 255) + 1);
            char* const input = buffers[nbBuff-1];
            char* output = buffers[nbBuff];
            int r;
            input[0] = (char)0xF0;   /* Literal length overflow */
            input[1] = (char)0xFF;
            input[2] = (char)0xFF;
            input[3] = (char)0xFF;
            { unsigned u; for(u = 4; u <= nbOf255+4; u++) input[u] = (char)0xff; }
            r = LZ4_decompress_safe(input, output, nbOf255+64, BLOCKSIZE_I134);
            if (r>0) { DISPLAY("LZ4_decompress_safe = %i \n", r); goto _overflowError; }
            input[0] = (char)0x1F;   /* Match length overflow */
            input[1] = (char)0x01;
            input[2] = (char)0x01;
            input[3] = (char)0x00;
            r = LZ4_decompress_safe(input, output, nbOf255+64, BLOCKSIZE_I134);
            if (r>0) { DISPLAY("LZ4_decompress_safe = %i \n", r); goto _overflowError; }

            output = buffers[nbBuff-2];   /* Reverse in/out pointer order */
            input[0] = (char)0xF0;   /* Literal length overflow */
            input[1] = (char)0xFF;
            input[2] = (char)0xFF;
            input[3] = (char)0xFF;
            r = LZ4_decompress_safe(input, output, nbOf255+64, BLOCKSIZE_I134);
            if (r>0) goto _overflowError;
            input[0] = (char)0x1F;   /* Match length overflow */
            input[1] = (char)0x01;
            input[2] = (char)0x01;
            input[3] = (char)0x00;
            r = LZ4_decompress_safe(input, output, nbOf255+64, BLOCKSIZE_I134);
            if (r>0) goto _overflowError;
        }
    }

    nbBuff++;
_endOfTests:
    { int i; for (i=0 ; i<nbBuff; i++) free(buffers[i]); }
    if (!highAddress) DISPLAY("high address not possible \n");
    else DISPLAY("all overflows correctly detected \n");
    return 0;

_overflowError:
    DISPLAY("Address space overflow error !! \n");
    exit(1);
}


#ifdef __unix__   /* is expected to be triggered on linux+gcc */

static void* FUZ_createLowAddr(size_t size)
{
    void* const lowBuff = mmap((void*)(0x1000), size,
                    PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS,
                    -1, 0);
    DISPLAYLEVEL(2, "generating low buffer at address %p \n", lowBuff);
    return lowBuff;
}

static void FUZ_freeLowAddr(void* buffer, size_t size)
{
    if (munmap(buffer, size)) {
        perror("fuzzer: freeing low address buffer");
        abort();
    }
}

#else

static void* FUZ_createLowAddr(size_t size)
{
    return malloc(size);
}

static void FUZ_freeLowAddr(void* buffer, size_t size)
{
    (void)size;
    free(buffer);
}

#endif


/*! FUZ_findDiff() :
*   find the first different byte between buff1 and buff2.
*   presumes buff1 != buff2.
*   presumes a difference exists before end of either buffer.
*   Typically invoked after a checksum mismatch.
*/
static void FUZ_findDiff(const void* buff1, const void* buff2)
{
    const BYTE* const b1 = (const BYTE*)buff1;
    const BYTE* const b2 = (const BYTE*)buff2;
    size_t u = 0;
    while (b1[u]==b2[u]) u++;
    DISPLAY("\nWrong Byte at position %u \n", (unsigned)u);
}


static int FUZ_test(U32 seed, U32 nbCycles, const U32 startCycle, const double compressibility, U32 duration_s)
{
    unsigned long long bytes = 0;
    unsigned long long cbytes = 0;
    unsigned long long hcbytes = 0;
    unsigned long long ccbytes = 0;
    void* const CNBuffer = malloc(COMPRESSIBLE_NOISE_LENGTH);
    size_t const compressedBufferSize = (size_t)LZ4_compressBound(FUZ_MAX_BLOCK_SIZE);
    char* const compressedBuffer = (char*)malloc(compressedBufferSize);
    char* const decodedBuffer = (char*)malloc(FUZ_MAX_DICT_SIZE + FUZ_MAX_BLOCK_SIZE);
    size_t const labSize = 96 KB;
    void* const lowAddrBuffer = FUZ_createLowAddr(labSize);
    void* const stateLZ4   = malloc((size_t)LZ4_sizeofState());
    void* const stateLZ4HC = malloc((size_t)LZ4_sizeofStateHC());
    LZ4_stream_t LZ4dictBody;
    LZ4_streamHC_t* LZ4dictHC = LZ4_createStreamHC();
    U32 coreRandState = seed;
    clock_t const clockStart = clock();
    clock_t const clockDuration = (clock_t)duration_s * CLOCKS_PER_SEC;
    int result = 0;
    unsigned cycleNb;

#   define EXIT_MSG(...) {                             \
    printf("Test %u : ", testNb); printf(__VA_ARGS__); \
    printf(" (seed %u, cycle %u) \n", seed, cycleNb);  \
    exit(1);                                           \
}

#   define FUZ_CHECKTEST(cond, ...)  { if (cond) { EXIT_MSG(__VA_ARGS__) } }

#   define FUZ_DISPLAYTEST(...) {                 \
                testNb++;                         \
                if (g_displayLevel>=4) {          \
                    printf("\r%4u - %2u :", cycleNb, testNb);  \
                    printf(" " __VA_ARGS__);      \
                    printf("   ");                \
                    fflush(stdout);               \
            }   }


    /* init */
    if(!CNBuffer || !compressedBuffer || !decodedBuffer || !LZ4dictHC) {
        DISPLAY("Not enough memory to start fuzzer tests");
        exit(1);
    }
    if ( LZ4_initStream(&LZ4dictBody, sizeof(LZ4dictBody)) == NULL) abort();
    {   U32 randState = coreRandState ^ PRIME3;
        FUZ_fillCompressibleNoiseBuffer(CNBuffer, COMPRESSIBLE_NOISE_LENGTH, compressibility, &randState);
    }

    /* move to startCycle */
    for (cycleNb = 0; cycleNb < startCycle; cycleNb++)
        (void) FUZ_rand(&coreRandState);   /* sync coreRandState */

    /* Main test loop */
    for (cycleNb = startCycle;
        (cycleNb < nbCycles) || (FUZ_GetClockSpan(clockStart) < clockDuration);
        cycleNb++) {
        U32 testNb = 0;
        U32 randState = FUZ_rand(&coreRandState) ^ PRIME3;
        int const blockSize  = (FUZ_rand(&randState) % (FUZ_MAX_BLOCK_SIZE-1)) + 1;
        int const blockStart = (FUZ_rand(&randState) % (COMPRESSIBLE_NOISE_LENGTH - blockSize - 1)) + 1;
        int const dictSizeRand = FUZ_rand(&randState) % FUZ_MAX_DICT_SIZE;
        int const dictSize = MIN(dictSizeRand, blockStart - 1);
        int const compressionLevel = FUZ_rand(&randState) % (LZ4HC_CLEVEL_MAX+1);
        const char* block = ((char*)CNBuffer) + blockStart;
        const char* dict = block - dictSize;
        int compressedSize, HCcompressedSize;
        int blockContinueCompressedSize;
        U32 const crcOrig = XXH32(block, (size_t)blockSize, 0);
        int ret;

        FUZ_displayUpdate(cycleNb);

        /* Compression tests */
        if ( ((FUZ_rand(&randState) & 63) == 2)
          && ((size_t)blockSize < labSize) ) {
            memcpy(lowAddrBuffer, block, blockSize);
            block = (const char*)lowAddrBuffer;
        }

        /* Test compression destSize */
        FUZ_DISPLAYTEST("test LZ4_compress_destSize()");
        {   int srcSize = blockSize;
            int const targetSize = srcSize * ((FUZ_rand(&randState) & 127)+1) >> 7;
            char endCheck = FUZ_rand(&randState) & 255;
            compressedBuffer[targetSize] = endCheck;
            ret = LZ4_compress_destSize(block, compressedBuffer, &srcSize, targetSize);
            FUZ_CHECKTEST(ret > targetSize, "LZ4_compress_destSize() result larger than dst buffer !");
            FUZ_CHECKTEST(compressedBuffer[targetSize] != endCheck, "LZ4_compress_destSize() overwrite dst buffer !");
            FUZ_CHECKTEST(srcSize > blockSize, "LZ4_compress_destSize() fed more than src buffer !");
            DISPLAYLEVEL(5, "destSize : %7i/%7i; content%7i/%7i ", ret, targetSize, srcSize, blockSize);
            if (targetSize>0) {
                /* check correctness */
                U32 const crcBase = XXH32(block, (size_t)srcSize, 0);
                char const canary = FUZ_rand(&randState) & 255;
                FUZ_CHECKTEST((ret==0), "LZ4_compress_destSize() compression failed");
                FUZ_DISPLAYTEST();
                compressedSize = ret;
                decodedBuffer[srcSize] = canary;
                ret = LZ4_decompress_safe(compressedBuffer, decodedBuffer, compressedSize, srcSize);
                FUZ_CHECKTEST(ret<0, "LZ4_decompress_safe() failed on data compressed by LZ4_compress_destSize");
                FUZ_CHECKTEST(ret!=srcSize, "LZ4_decompress_safe() failed : did not fully decompressed data");
                FUZ_CHECKTEST(decodedBuffer[srcSize] != canary, "LZ4_decompress_safe() overwrite dst buffer !");
                { U32 const crcDec = XXH32(decodedBuffer, srcSize, 0);
                  FUZ_CHECKTEST(crcDec!=crcBase, "LZ4_decompress_safe() corrupted decoded data"); }

                DISPLAYLEVEL(5, " OK \n");
            } else {
                DISPLAYLEVEL(5, " \n");
        }   }

        /* Test compression HC destSize */
        FUZ_DISPLAYTEST("test LZ4_compress_HC_destSize()");
        {   int srcSize = blockSize;
            int const targetSize = srcSize * ((FUZ_rand(&randState) & 127)+1) >> 7;
            char const endCheck = FUZ_rand(&randState) & 255;
            void* ctx = LZ4_createHC(block);
            FUZ_CHECKTEST(ctx==NULL, "LZ4_createHC() allocation failed");
            compressedBuffer[targetSize] = endCheck;
            ret = LZ4_compress_HC_destSize(ctx, block, compressedBuffer, &srcSize, targetSize, compressionLevel);
            DISPLAYLEVEL(5, "LZ4_compress_HC_destSize(%i): destSize : %7i/%7i; content%7i/%7i ",
                            compressionLevel, ret, targetSize, srcSize, blockSize);
            LZ4_freeHC(ctx);
            FUZ_CHECKTEST(ret > targetSize, "LZ4_compress_HC_destSize() result larger than dst buffer !");
            FUZ_CHECKTEST(compressedBuffer[targetSize] != endCheck, "LZ4_compress_HC_destSize() overwrite dst buffer !");
            FUZ_CHECKTEST(srcSize > blockSize, "LZ4_compress_HC_destSize() fed more than src buffer !");
            if (targetSize>0) {
                /* check correctness */
                U32 const crcBase = XXH32(block, (size_t)srcSize, 0);
                char const canary = FUZ_rand(&randState) & 255;
                FUZ_CHECKTEST((ret==0), "LZ4_compress_HC_destSize() compression failed");
                FUZ_DISPLAYTEST();
                compressedSize = ret;
                decodedBuffer[srcSize] = canary;
                ret = LZ4_decompress_safe(compressedBuffer, decodedBuffer, compressedSize, srcSize);
                FUZ_CHECKTEST(ret<0, "LZ4_decompress_safe() failed on data compressed by LZ4_compressHC_destSize");
                FUZ_CHECKTEST(ret!=srcSize, "LZ4_decompress_safe() failed : did not fully decompressed data");
                FUZ_CHECKTEST(decodedBuffer[srcSize] != canary, "LZ4_decompress_safe() overwrite dst buffer !");
                {   U32 const crcDec = XXH32(decodedBuffer, srcSize, 0);
                    FUZ_CHECKTEST(crcDec!=crcBase, "LZ4_decompress_safe() corrupted decoded data");
                }
                DISPLAYLEVEL(5, " OK \n");
            } else {
                DISPLAYLEVEL(5, " \n");
        }   }

        /* Test compression HC */
        FUZ_DISPLAYTEST("test LZ4_compress_HC()");
        HCcompressedSize = LZ4_compress_HC(block, compressedBuffer, blockSize, (int)compressedBufferSize, compressionLevel);
        FUZ_CHECKTEST(HCcompressedSize==0, "LZ4_compress_HC() failed");

        /* Test compression HC using external state */
        FUZ_DISPLAYTEST("test LZ4_compress_HC_extStateHC()");
        {   int const r = LZ4_compress_HC_extStateHC(stateLZ4HC, block, compressedBuffer, blockSize, (int)compressedBufferSize, compressionLevel);
            FUZ_CHECKTEST(r==0, "LZ4_compress_HC_extStateHC() failed")
        }

        /* Test compression HC using fast reset external state */
        FUZ_DISPLAYTEST("test LZ4_compress_HC_extStateHC_fastReset()");
        {   int const r = LZ4_compress_HC_extStateHC_fastReset(stateLZ4HC, block, compressedBuffer, blockSize, (int)compressedBufferSize, compressionLevel);
            FUZ_CHECKTEST(r==0, "LZ4_compress_HC_extStateHC_fastReset() failed");
        }

        /* Test compression using external state */
        FUZ_DISPLAYTEST("test LZ4_compress_fast_extState()");
        {   int const r = LZ4_compress_fast_extState(stateLZ4, block, compressedBuffer, blockSize, (int)compressedBufferSize, 8);
            FUZ_CHECKTEST(r==0, "LZ4_compress_fast_extState() failed"); }

        /* Test compression using fast reset external state*/
        FUZ_DISPLAYTEST();
        {   int const r = LZ4_compress_fast_extState_fastReset(stateLZ4, block, compressedBuffer, blockSize, (int)compressedBufferSize, 8);
            FUZ_CHECKTEST(r==0, "LZ4_compress_fast_extState_fastReset() failed"); }

        /* Test compression */
        FUZ_DISPLAYTEST("test LZ4_compress_default()");
        compressedSize = LZ4_compress_default(block, compressedBuffer, blockSize, (int)compressedBufferSize);
        FUZ_CHECKTEST(compressedSize<=0, "LZ4_compress_default() failed");

        /* Decompression tests */

        /* Test decompress_fast() with input buffer size exactly correct => must not read out of bound */
        {   char* const cBuffer_exact = (char*)malloc((size_t)compressedSize);
            assert(cBuffer_exact != NULL);
            assert(compressedSize <= (int)compressedBufferSize);
            memcpy(cBuffer_exact, compressedBuffer, compressedSize);

            /* Test decoding with output size exactly correct => must work */
            FUZ_DISPLAYTEST("LZ4_decompress_fast() with exact output buffer");
            {   int const r = LZ4_decompress_fast(cBuffer_exact, decodedBuffer, blockSize);
                FUZ_CHECKTEST(r<0, "LZ4_decompress_fast failed despite correct space");
                FUZ_CHECKTEST(r!=compressedSize, "LZ4_decompress_fast failed : did not fully read compressed data");
            }
            {   U32 const crcCheck = XXH32(decodedBuffer, (size_t)blockSize, 0);
                FUZ_CHECKTEST(crcCheck!=crcOrig, "LZ4_decompress_fast corrupted decoded data");
            }

            /* Test decoding with one byte missing => must fail */
            FUZ_DISPLAYTEST("LZ4_decompress_fast() with output buffer 1-byte too short");
            decodedBuffer[blockSize-1] = 0;
            {   int const r = LZ4_decompress_fast(cBuffer_exact, decodedBuffer, blockSize-1);
                FUZ_CHECKTEST(r>=0, "LZ4_decompress_fast should have failed, due to Output Size being too small");
            }
            FUZ_CHECKTEST(decodedBuffer[blockSize-1]!=0, "LZ4_decompress_fast overrun specified output buffer");

            /* Test decoding with one byte too much => must fail */
            FUZ_DISPLAYTEST();
            {   int const r = LZ4_decompress_fast(cBuffer_exact, decodedBuffer, blockSize+1);
                FUZ_CHECKTEST(r>=0, "LZ4_decompress_fast should have failed, due to Output Size being too large");
            }

            /* Test decoding with output size exactly what's necessary => must work */
            FUZ_DISPLAYTEST();
            decodedBuffer[blockSize] = 0;
            {   int const r = LZ4_decompress_safe(cBuffer_exact, decodedBuffer, compressedSize, blockSize);
                FUZ_CHECKTEST(r<0, "LZ4_decompress_safe failed despite sufficient space");
                FUZ_CHECKTEST(r!=blockSize, "LZ4_decompress_safe did not regenerate original data");
            }
            FUZ_CHECKTEST(decodedBuffer[blockSize], "LZ4_decompress_safe overrun specified output buffer size");
            {   U32 const crcCheck = XXH32(decodedBuffer, blockSize, 0);
                FUZ_CHECKTEST(crcCheck!=crcOrig, "LZ4_decompress_safe corrupted decoded data");
            }

            /* Test decoding with more than enough output size => must work */
            FUZ_DISPLAYTEST();
            decodedBuffer[blockSize] = 0;
            decodedBuffer[blockSize+1] = 0;
            {   int const r = LZ4_decompress_safe(cBuffer_exact, decodedBuffer, compressedSize, blockSize+1);
                FUZ_CHECKTEST(r<0, "LZ4_decompress_safe failed despite amply sufficient space");
                FUZ_CHECKTEST(r!=blockSize, "LZ4_decompress_safe did not regenerate original data");
            }
            FUZ_CHECKTEST(decodedBuffer[blockSize+1], "LZ4_decompress_safe overrun specified output buffer size");
            {   U32 const crcCheck = XXH32(decodedBuffer, (size_t)blockSize, 0);
                FUZ_CHECKTEST(crcCheck!=crcOrig, "LZ4_decompress_safe corrupted decoded data");
            }

            /* Test decoding with output size being one byte too short => must fail */
            FUZ_DISPLAYTEST();
            decodedBuffer[blockSize-1] = 0;
            {   int const r = LZ4_decompress_safe(cBuffer_exact, decodedBuffer, compressedSize, blockSize-1);
                FUZ_CHECKTEST(r>=0, "LZ4_decompress_safe should have failed, due to Output Size being one byte too short");
            }
            FUZ_CHECKTEST(decodedBuffer[blockSize-1], "LZ4_decompress_safe overrun specified output buffer size");

            /* Test decoding with output size being 10 bytes too short => must fail */
            FUZ_DISPLAYTEST();
            if (blockSize>10) {
                decodedBuffer[blockSize-10] = 0;
                {   int const r = LZ4_decompress_safe(cBuffer_exact, decodedBuffer, compressedSize, blockSize-10);
                    FUZ_CHECKTEST(r>=0, "LZ4_decompress_safe should have failed, due to Output Size being 10 bytes too short");
                }
                FUZ_CHECKTEST(decodedBuffer[blockSize-10], "LZ4_decompress_safe overrun specified output buffer size");
            }

            free(cBuffer_exact);
        }

        /* Test decoding with input size being one byte too short => must fail */
        FUZ_DISPLAYTEST();
        {   int const r = LZ4_decompress_safe(compressedBuffer, decodedBuffer, compressedSize-1, blockSize);
            FUZ_CHECKTEST(r>=0, "LZ4_decompress_safe should have failed, due to input size being one byte too short (blockSize=%i, ret=%i, compressedSize=%i)", blockSize, ret, compressedSize);
        }

        /* Test decoding with input size being one byte too large => must fail */
        FUZ_DISPLAYTEST();
        decodedBuffer[blockSize] = 0;
        {   int const r = LZ4_decompress_safe(compressedBuffer, decodedBuffer, compressedSize+1, blockSize);
            FUZ_CHECKTEST(r>=0, "LZ4_decompress_safe should have failed, due to input size being too large");
        }
        FUZ_CHECKTEST(decodedBuffer[blockSize], "LZ4_decompress_safe overrun specified output buffer size");

        /* Test partial decoding => must work */
        FUZ_DISPLAYTEST("test LZ4_decompress_safe_partial");
        {   size_t const missingBytes = FUZ_rand(&randState) % blockSize;
            int const targetSize = (int)((size_t)blockSize - missingBytes);
            char const sentinel = decodedBuffer[targetSize] = block[targetSize] ^ 0x5A;
            int const decResult = LZ4_decompress_safe_partial(compressedBuffer, decodedBuffer, compressedSize, targetSize, blockSize);
            FUZ_CHECKTEST(decResult<0, "LZ4_decompress_safe_partial failed despite valid input data (error:%i)", decResult);
            FUZ_CHECKTEST(decResult != targetSize, "LZ4_decompress_safe_partial did not regenerated required amount of data (%i < %i <= %i)", decResult, targetSize, blockSize);
            FUZ_CHECKTEST(decodedBuffer[targetSize] != sentinel, "LZ4_decompress_safe_partial overwrite beyond requested size (though %i <= %i <= %i)", decResult, targetSize, blockSize);
        }

        /* Test Compression with limited output size */

        /* Test compression with output size being exactly what's necessary (should work) */
        FUZ_DISPLAYTEST("test LZ4_compress_default() with output buffer just the right size");
        ret = LZ4_compress_default(block, compressedBuffer, blockSize, compressedSize);
        FUZ_CHECKTEST(ret==0, "LZ4_compress_default() failed despite sufficient space");

        /* Test compression with output size being exactly what's necessary and external state (should work) */
        FUZ_DISPLAYTEST("test LZ4_compress_fast_extState() with output buffer just the right size");
        ret = LZ4_compress_fast_extState(stateLZ4, block, compressedBuffer, blockSize, compressedSize, 1);
        FUZ_CHECKTEST(ret==0, "LZ4_compress_fast_extState() failed despite sufficient space");

        /* Test HC compression with output size being exactly what's necessary (should work) */
        FUZ_DISPLAYTEST("test LZ4_compress_HC() with output buffer just the right size");
        ret = LZ4_compress_HC(block, compressedBuffer, blockSize, HCcompressedSize, compressionLevel);
        FUZ_CHECKTEST(ret==0, "LZ4_compress_HC() failed despite sufficient space");

        /* Test HC compression with output size being exactly what's necessary (should work) */
        FUZ_DISPLAYTEST("test LZ4_compress_HC_extStateHC() with output buffer just the right size");
        ret = LZ4_compress_HC_extStateHC(stateLZ4HC, block, compressedBuffer, blockSize, HCcompressedSize, compressionLevel);
        FUZ_CHECKTEST(ret==0, "LZ4_compress_HC_extStateHC() failed despite sufficient space");

        /* Test compression with missing bytes into output buffer => must fail */
        FUZ_DISPLAYTEST("test LZ4_compress_default() with output buffer a bit too short");
        {   int missingBytes = (FUZ_rand(&randState) % 0x3F) + 1;
            if (missingBytes >= compressedSize) missingBytes = compressedSize-1;
            missingBytes += !missingBytes;   /* avoid special case missingBytes==0 */
            compressedBuffer[compressedSize-missingBytes] = 0;
            ret = LZ4_compress_default(block, compressedBuffer, blockSize, compressedSize-missingBytes);
            FUZ_CHECKTEST(ret, "LZ4_compress_default should have failed (output buffer too small by %i byte)", missingBytes);
            FUZ_CHECKTEST(compressedBuffer[compressedSize-missingBytes], "LZ4_compress_default overran output buffer ! (%i missingBytes)", missingBytes)
        }

        /* Test HC compression with missing bytes into output buffer => must fail */
        FUZ_DISPLAYTEST("test LZ4_compress_HC() with output buffer a bit too short");
        {   int missingBytes = (FUZ_rand(&randState) % 0x3F) + 1;
            if (missingBytes >= HCcompressedSize) missingBytes = HCcompressedSize-1;
            missingBytes += !missingBytes;   /* avoid special case missingBytes==0 */
            compressedBuffer[HCcompressedSize-missingBytes] = 0;
            ret = LZ4_compress_HC(block, compressedBuffer, blockSize, HCcompressedSize-missingBytes, compressionLevel);
            FUZ_CHECKTEST(ret, "LZ4_compress_HC should have failed (output buffer too small by %i byte)", missingBytes);
            FUZ_CHECKTEST(compressedBuffer[HCcompressedSize-missingBytes], "LZ4_compress_HC overran output buffer ! (%i missingBytes)", missingBytes)
        }


        /*-******************/
        /* Dictionary tests */
        /*-******************/

        /* Compress using dictionary */
        FUZ_DISPLAYTEST("test LZ4_compress_fast_continue() with dictionary of size %i", dictSize);
        {   LZ4_stream_t LZ4_stream;
            LZ4_initStream(&LZ4_stream, sizeof(LZ4_stream));
            LZ4_compress_fast_continue (&LZ4_stream, dict, compressedBuffer, dictSize, (int)compressedBufferSize, 1);   /* Just to fill hash tables */
            blockContinueCompressedSize = LZ4_compress_fast_continue (&LZ4_stream, block, compressedBuffer, blockSize, (int)compressedBufferSize, 1);
            FUZ_CHECKTEST(blockContinueCompressedSize==0, "LZ4_compress_fast_continue failed");
        }

        /* Decompress with dictionary as prefix */
        FUZ_DISPLAYTEST("test LZ4_decompress_fast_usingDict() with dictionary as prefix");
        memcpy(decodedBuffer, dict, dictSize);
        ret = LZ4_decompress_fast_usingDict(compressedBuffer, decodedBuffer+dictSize, blockSize, decodedBuffer, dictSize);
        FUZ_CHECKTEST(ret!=blockContinueCompressedSize, "LZ4_decompress_fast_usingDict did not read all compressed block input");
        {   U32 const crcCheck = XXH32(decodedBuffer+dictSize, (size_t)blockSize, 0);
            if (crcCheck!=crcOrig) {
                FUZ_findDiff(block, decodedBuffer);
                EXIT_MSG("LZ4_decompress_fast_usingDict corrupted decoded data (dict %i)", dictSize);
            }
        }

        FUZ_DISPLAYTEST("test LZ4_decompress_safe_usingDict()");
        ret = LZ4_decompress_safe_usingDict(compressedBuffer, decodedBuffer+dictSize, blockContinueCompressedSize, blockSize, decodedBuffer, dictSize);
        FUZ_CHECKTEST(ret!=blockSize, "LZ4_decompress_safe_usingDict did not regenerate original data");
        {   U32 const crcCheck = XXH32(decodedBuffer+dictSize, (size_t)blockSize, 0);
            FUZ_CHECKTEST(crcCheck!=crcOrig, "LZ4_decompress_safe_usingDict corrupted decoded data");
        }

        /* Compress using External dictionary */
        FUZ_DISPLAYTEST("test LZ4_compress_fast_continue(), with non-contiguous dictionary");
        dict -= (size_t)(FUZ_rand(&randState) & 0xF) + 1;   /* create space, so now dictionary is an ExtDict */
        if (dict < (char*)CNBuffer) dict = (char*)CNBuffer;
        LZ4_loadDict(&LZ4dictBody, dict, dictSize);
        blockContinueCompressedSize = LZ4_compress_fast_continue(&LZ4dictBody, block, compressedBuffer, blockSize, (int)compressedBufferSize, 1);
        FUZ_CHECKTEST(blockContinueCompressedSize==0, "LZ4_compress_fast_continue failed");

        FUZ_DISPLAYTEST("test LZ4_compress_fast_continue() with dictionary but with an output buffer too short by one byte");
        LZ4_loadDict(&LZ4dictBody, dict, dictSize);
        ret = LZ4_compress_fast_continue(&LZ4dictBody, block, compressedBuffer, blockSize, blockContinueCompressedSize-1, 1);
        FUZ_CHECKTEST(ret>0, "LZ4_compress_fast_continue using ExtDict should fail : one missing byte for output buffer : %i written, %i buffer", ret, blockContinueCompressedSize);

        FUZ_DISPLAYTEST("test LZ4_compress_fast_continue() with dictionary loaded with LZ4_loadDict()");
        DISPLAYLEVEL(5, " compress %i bytes from buffer(%p) into dst(%p) using dict(%p) of size %i \n",
                     blockSize, (const void *)block, (void *)decodedBuffer, (const void *)dict, dictSize);
        LZ4_loadDict(&LZ4dictBody, dict, dictSize);
        ret = LZ4_compress_fast_continue(&LZ4dictBody, block, compressedBuffer, blockSize, blockContinueCompressedSize, 1);
        FUZ_CHECKTEST(ret!=blockContinueCompressedSize, "LZ4_compress_limitedOutput_compressed size is different (%i != %i)", ret, blockContinueCompressedSize);
        FUZ_CHECKTEST(ret<=0, "LZ4_compress_fast_continue should work : enough size available within output buffer");

        /* Decompress with dictionary as external */
        FUZ_DISPLAYTEST("test LZ4_decompress_fast_usingDict() with dictionary as extDict");
        DISPLAYLEVEL(5, " decoding %i bytes from buffer(%p) using dict(%p) of size %i \n",
                     blockSize, (void *)decodedBuffer, (const void *)dict, dictSize);
        decodedBuffer[blockSize] = 0;
        ret = LZ4_decompress_fast_usingDict(compressedBuffer, decodedBuffer, blockSize, dict, dictSize);
        FUZ_CHECKTEST(ret!=blockContinueCompressedSize, "LZ4_decompress_fast_usingDict did not read all compressed block input");
        FUZ_CHECKTEST(decodedBuffer[blockSize], "LZ4_decompress_fast_usingDict overrun specified output buffer size");
        {   U32 const crcCheck = XXH32(decodedBuffer, (size_t)blockSize, 0);
            if (crcCheck!=crcOrig) {
                FUZ_findDiff(block, decodedBuffer);
                EXIT_MSG("LZ4_decompress_fast_usingDict corrupted decoded data (dict %i)", dictSize);
            }
        }

        FUZ_DISPLAYTEST();
        decodedBuffer[blockSize] = 0;
        ret = LZ4_decompress_safe_usingDict(compressedBuffer, decodedBuffer, blockContinueCompressedSize, blockSize, dict, dictSize);
        FUZ_CHECKTEST(ret!=blockSize, "LZ4_decompress_safe_usingDict did not regenerate original data");
        FUZ_CHECKTEST(decodedBuffer[blockSize], "LZ4_decompress_safe_usingDict overrun specified output buffer size");
        {   U32 const crcCheck = XXH32(decodedBuffer, blockSize, 0);
            FUZ_CHECKTEST(crcCheck!=crcOrig, "LZ4_decompress_safe_usingDict corrupted decoded data");
        }

        FUZ_DISPLAYTEST();
        decodedBuffer[blockSize-1] = 0;
        ret = LZ4_decompress_fast_usingDict(compressedBuffer, decodedBuffer, blockSize-1, dict, dictSize);
        FUZ_CHECKTEST(ret>=0, "LZ4_decompress_fast_usingDict should have failed : wrong original size (-1 byte)");
        FUZ_CHECKTEST(decodedBuffer[blockSize-1], "LZ4_decompress_fast_usingDict overrun specified output buffer size");

        FUZ_DISPLAYTEST();
        decodedBuffer[blockSize-1] = 0;
        ret = LZ4_decompress_safe_usingDict(compressedBuffer, decodedBuffer, blockContinueCompressedSize, blockSize-1, dict, dictSize);
        FUZ_CHECKTEST(ret>=0, "LZ4_decompress_safe_usingDict should have failed : not enough output size (-1 byte)");
        FUZ_CHECKTEST(decodedBuffer[blockSize-1], "LZ4_decompress_safe_usingDict overrun specified output buffer size");

        FUZ_DISPLAYTEST();
        {   U32 const missingBytes = (FUZ_rand(&randState) & 0xF) + 2;
            if ((U32)blockSize > missingBytes) {
                decodedBuffer[blockSize-missingBytes] = 0;
                ret = LZ4_decompress_safe_usingDict(compressedBuffer, decodedBuffer, blockContinueCompressedSize, blockSize-missingBytes, dict, dictSize);
                FUZ_CHECKTEST(ret>=0, "LZ4_decompress_safe_usingDict should have failed : output buffer too small (-%u byte)", missingBytes);
                FUZ_CHECKTEST(decodedBuffer[blockSize-missingBytes], "LZ4_decompress_safe_usingDict overrun specified output buffer size (-%u byte) (blockSize=%i)", missingBytes, blockSize);
        }   }

        /* Compress using external dictionary stream */
        {
            LZ4_stream_t LZ4_stream;
            int expectedSize;
            U32 expectedCrc;

            FUZ_DISPLAYTEST("LZ4_compress_fast_continue() after LZ4_loadDict()");
            LZ4_loadDict(&LZ4dictBody, dict, dictSize);
            expectedSize = LZ4_compress_fast_continue(&LZ4dictBody, block, compressedBuffer, blockSize, (int)compressedBufferSize, 1);
            FUZ_CHECKTEST(expectedSize<=0, "LZ4_compress_fast_continue reference compression for extDictCtx should have succeeded");
            expectedCrc = XXH32(compressedBuffer, expectedSize, 0);

            FUZ_DISPLAYTEST("LZ4_compress_fast_continue() after LZ4_attach_dictionary()");
            LZ4_loadDict(&LZ4dictBody, dict, dictSize);
            LZ4_initStream(&LZ4_stream, sizeof(LZ4_stream));
            LZ4_attach_dictionary(&LZ4_stream, &LZ4dictBody);
            blockContinueCompressedSize = LZ4_compress_fast_continue(&LZ4_stream, block, compressedBuffer, blockSize, (int)compressedBufferSize, 1);
            FUZ_CHECKTEST(blockContinueCompressedSize==0, "LZ4_compress_fast_continue using extDictCtx failed");
            FUZ_CHECKTEST(LZ4_stream.internal_donotuse.dirty, "context should be good");

            /* In the future, it might be desirable to let extDictCtx mode's
             * output diverge from the output generated by regular extDict mode.
             * Until that time, this comparison serves as a good regression
             * test.
             */
            FUZ_CHECKTEST(blockContinueCompressedSize != expectedSize, "LZ4_compress_fast_continue using extDictCtx produced different-sized output (%d expected vs %d actual)", expectedSize, blockContinueCompressedSize);
            FUZ_CHECKTEST(XXH32(compressedBuffer, blockContinueCompressedSize, 0) != expectedCrc, "LZ4_compress_fast_continue using extDictCtx produced different output");

            FUZ_DISPLAYTEST("LZ4_compress_fast_continue() after LZ4_attach_dictionary(), but output buffer is 1 byte too short");
            LZ4_resetStream_fast(&LZ4_stream);
            LZ4_attach_dictionary(&LZ4_stream, &LZ4dictBody);
            ret = LZ4_compress_fast_continue(&LZ4_stream, block, compressedBuffer, blockSize, blockContinueCompressedSize-1, 1);
            FUZ_CHECKTEST(ret>0, "LZ4_compress_fast_continue using extDictCtx should fail : one missing byte for output buffer : %i written, %i buffer", ret, blockContinueCompressedSize);
            /* note : context is no longer dirty after a failed compressed block */

            FUZ_DISPLAYTEST();
            LZ4_resetStream_fast(&LZ4_stream);
            LZ4_attach_dictionary(&LZ4_stream, &LZ4dictBody);
            ret = LZ4_compress_fast_continue(&LZ4_stream, block, compressedBuffer, blockSize, blockContinueCompressedSize, 1);
            FUZ_CHECKTEST(ret!=blockContinueCompressedSize, "LZ4_compress_limitedOutput_compressed size is different (%i != %i)", ret, blockContinueCompressedSize);
            FUZ_CHECKTEST(ret<=0, "LZ4_compress_fast_continue using extDictCtx should work : enough size available within output buffer");
            FUZ_CHECKTEST(ret != expectedSize, "LZ4_compress_fast_continue using extDictCtx produced different-sized output");
            FUZ_CHECKTEST(XXH32(compressedBuffer, ret, 0) != expectedCrc, "LZ4_compress_fast_continue using extDictCtx produced different output");
            FUZ_CHECKTEST(LZ4_stream.internal_donotuse.dirty, "context should be good");

            FUZ_DISPLAYTEST();
            LZ4_resetStream_fast(&LZ4_stream);
            LZ4_attach_dictionary(&LZ4_stream, &LZ4dictBody);
            ret = LZ4_compress_fast_continue(&LZ4_stream, block, compressedBuffer, blockSize, blockContinueCompressedSize, 1);
            FUZ_CHECKTEST(ret!=blockContinueCompressedSize, "LZ4_compress_limitedOutput_compressed size is different (%i != %i)", ret, blockContinueCompressedSize);
            FUZ_CHECKTEST(ret<=0, "LZ4_compress_fast_continue using extDictCtx with re-used context should work : enough size available within output buffer");
            FUZ_CHECKTEST(ret != expectedSize, "LZ4_compress_fast_continue using extDictCtx produced different-sized output");
            FUZ_CHECKTEST(XXH32(compressedBuffer, ret, 0) != expectedCrc, "LZ4_compress_fast_continue using extDictCtx produced different output");
            FUZ_CHECKTEST(LZ4_stream.internal_donotuse.dirty, "context should be good");
        }

        /* Decompress with dictionary as external */
        FUZ_DISPLAYTEST();
        decodedBuffer[blockSize] = 0;
        ret = LZ4_decompress_fast_usingDict(compressedBuffer, decodedBuffer, blockSize, dict, dictSize);
        FUZ_CHECKTEST(ret!=blockContinueCompressedSize, "LZ4_decompress_fast_usingDict did not read all compressed block input");
        FUZ_CHECKTEST(decodedBuffer[blockSize], "LZ4_decompress_fast_usingDict overrun specified output buffer size");
        {   U32 const crcCheck = XXH32(decodedBuffer, blockSize, 0);
            if (crcCheck!=crcOrig) {
                FUZ_findDiff(block, decodedBuffer);
                EXIT_MSG("LZ4_decompress_fast_usingDict corrupted decoded data (dict %i)", dictSize);
            }
        }

        FUZ_DISPLAYTEST();
        decodedBuffer[blockSize] = 0;
        ret = LZ4_decompress_safe_usingDict(compressedBuffer, decodedBuffer, blockContinueCompressedSize, blockSize, dict, dictSize);
        FUZ_CHECKTEST(ret!=blockSize, "LZ4_decompress_safe_usingDict did not regenerate original data");
        FUZ_CHECKTEST(decodedBuffer[blockSize], "LZ4_decompress_safe_usingDict overrun specified output buffer size");
        {   U32 const crcCheck = XXH32(decodedBuffer, (size_t)blockSize, 0);
            FUZ_CHECKTEST(crcCheck!=crcOrig, "LZ4_decompress_safe_usingDict corrupted decoded data");
        }

        FUZ_DISPLAYTEST();
        decodedBuffer[blockSize-1] = 0;
        ret = LZ4_decompress_fast_usingDict(compressedBuffer, decodedBuffer, blockSize-1, dict, dictSize);
        FUZ_CHECKTEST(ret>=0, "LZ4_decompress_fast_usingDict should have failed : wrong original size (-1 byte)");
        FUZ_CHECKTEST(decodedBuffer[blockSize-1], "LZ4_decompress_fast_usingDict overrun specified output buffer size");

        FUZ_DISPLAYTEST();
        decodedBuffer[blockSize-1] = 0;
        ret = LZ4_decompress_safe_usingDict(compressedBuffer, decodedBuffer, blockContinueCompressedSize, blockSize-1, dict, dictSize);
        FUZ_CHECKTEST(ret>=0, "LZ4_decompress_safe_usingDict should have failed : not enough output size (-1 byte)");
        FUZ_CHECKTEST(decodedBuffer[blockSize-1], "LZ4_decompress_safe_usingDict overrun specified output buffer size");

        FUZ_DISPLAYTEST("LZ4_decompress_safe_usingDict with a too small output buffer");
        {   U32 const missingBytes = (FUZ_rand(&randState) & 0xF) + 2;
            if ((U32)blockSize > missingBytes) {
                decodedBuffer[blockSize-missingBytes] = 0;
                ret = LZ4_decompress_safe_usingDict(compressedBuffer, decodedBuffer, blockContinueCompressedSize, blockSize-missingBytes, dict, dictSize);
                FUZ_CHECKTEST(ret>=0, "LZ4_decompress_safe_usingDict should have failed : output buffer too small (-%u byte)", missingBytes);
                FUZ_CHECKTEST(decodedBuffer[blockSize-missingBytes], "LZ4_decompress_safe_usingDict overrun specified output buffer size (-%u byte) (blockSize=%i)", missingBytes, blockSize);
        }   }

        /* Compress HC using External dictionary */
        FUZ_DISPLAYTEST("LZ4_compress_HC_continue with an external dictionary");
        dict -= (FUZ_rand(&randState) & 7);    /* even bigger separation */
        if (dict < (char*)CNBuffer) dict = (char*)CNBuffer;
        LZ4_loadDictHC(LZ4dictHC, dict, dictSize);
        LZ4_setCompressionLevel (LZ4dictHC, compressionLevel);
        blockContinueCompressedSize = LZ4_compress_HC_continue(LZ4dictHC, block, compressedBuffer, blockSize, (int)compressedBufferSize);
        FUZ_CHECKTEST(blockContinueCompressedSize==0, "LZ4_compress_HC_continue failed");
        FUZ_CHECKTEST(LZ4dictHC->internal_donotuse.dirty, "Context should be clean");

        FUZ_DISPLAYTEST("LZ4_compress_HC_continue with same external dictionary, but output buffer 1 byte too short");
        LZ4_loadDictHC(LZ4dictHC, dict, dictSize);
        ret = LZ4_compress_HC_continue(LZ4dictHC, block, compressedBuffer, blockSize, blockContinueCompressedSize-1);
        FUZ_CHECKTEST(ret>0, "LZ4_compress_HC_continue using ExtDict should fail : one missing byte for output buffer (expected %i, but result=%i)", blockContinueCompressedSize, ret);
        /* note : context is no longer dirty after a failed compressed block */

        FUZ_DISPLAYTEST("LZ4_compress_HC_continue with same external dictionary, and output buffer exactly the right size");
        LZ4_loadDictHC(LZ4dictHC, dict, dictSize);
        ret = LZ4_compress_HC_continue(LZ4dictHC, block, compressedBuffer, blockSize, blockContinueCompressedSize);
        FUZ_CHECKTEST(ret!=blockContinueCompressedSize, "LZ4_compress_HC_continue size is different : ret(%i) != expected(%i)", ret, blockContinueCompressedSize);
        FUZ_CHECKTEST(ret<=0, "LZ4_compress_HC_continue should work : enough size available within output buffer");
        FUZ_CHECKTEST(LZ4dictHC->internal_donotuse.dirty, "Context should be clean");

        FUZ_DISPLAYTEST();
        decodedBuffer[blockSize] = 0;
        ret = LZ4_decompress_safe_usingDict(compressedBuffer, decodedBuffer, blockContinueCompressedSize, blockSize, dict, dictSize);
        FUZ_CHECKTEST(ret!=blockSize, "LZ4_decompress_safe_usingDict did not regenerate original data");
        FUZ_CHECKTEST(decodedBuffer[blockSize], "LZ4_decompress_safe_usingDict overrun specified output buffer size");
        {   U32 const crcCheck = XXH32(decodedBuffer, (size_t)blockSize, 0);
            if (crcCheck!=crcOrig) {
                FUZ_findDiff(block, decodedBuffer);
                EXIT_MSG("LZ4_decompress_safe_usingDict corrupted decoded data");
        }   }

        /* Compress HC using external dictionary stream */
        FUZ_DISPLAYTEST();
        {   LZ4_streamHC_t* const LZ4_streamHC = LZ4_createStreamHC();

            LZ4_loadDictHC(LZ4dictHC, dict, dictSize);
            LZ4_attach_HC_dictionary(LZ4_streamHC, LZ4dictHC);
            LZ4_setCompressionLevel (LZ4_streamHC, compressionLevel);
            blockContinueCompressedSize = LZ4_compress_HC_continue(LZ4_streamHC, block, compressedBuffer, blockSize, (int)compressedBufferSize);
            FUZ_CHECKTEST(blockContinueCompressedSize==0, "LZ4_compress_HC_continue with ExtDictCtx failed");
            FUZ_CHECKTEST(LZ4_streamHC->internal_donotuse.dirty, "Context should be clean");

            FUZ_DISPLAYTEST();
            LZ4_resetStreamHC_fast (LZ4_streamHC, compressionLevel);
            LZ4_attach_HC_dictionary(LZ4_streamHC, LZ4dictHC);
            ret = LZ4_compress_HC_continue(LZ4_streamHC, block, compressedBuffer, blockSize, blockContinueCompressedSize-1);
            FUZ_CHECKTEST(ret>0, "LZ4_compress_HC_continue using ExtDictCtx should fail : one missing byte for output buffer (%i != %i)", ret, blockContinueCompressedSize);
            /* note : context is no longer dirty after a failed compressed block */

            FUZ_DISPLAYTEST();
            LZ4_resetStreamHC_fast (LZ4_streamHC, compressionLevel);
            LZ4_attach_HC_dictionary(LZ4_streamHC, LZ4dictHC);
            ret = LZ4_compress_HC_continue(LZ4_streamHC, block, compressedBuffer, blockSize, blockContinueCompressedSize);
            FUZ_CHECKTEST(ret!=blockContinueCompressedSize, "LZ4_compress_HC_continue using ExtDictCtx size is different (%i != %i)", ret, blockContinueCompressedSize);
            FUZ_CHECKTEST(ret<=0, "LZ4_compress_HC_continue using ExtDictCtx should work : enough size available within output buffer");
            FUZ_CHECKTEST(LZ4_streamHC->internal_donotuse.dirty, "Context should be clean");

            FUZ_DISPLAYTEST();
            LZ4_resetStreamHC_fast (LZ4_streamHC, compressionLevel);
            LZ4_attach_HC_dictionary(LZ4_streamHC, LZ4dictHC);
            ret = LZ4_compress_HC_continue(LZ4_streamHC, block, compressedBuffer, blockSize, blockContinueCompressedSize);
            FUZ_CHECKTEST(ret!=blockContinueCompressedSize, "LZ4_compress_HC_continue using ExtDictCtx and fast reset size is different (%i != %i)", ret, blockContinueCompressedSize);
            FUZ_CHECKTEST(ret<=0, "LZ4_compress_HC_continue using ExtDictCtx and fast reset should work : enough size available within output buffer");
            FUZ_CHECKTEST(LZ4_streamHC->internal_donotuse.dirty, "Context should be clean");

            LZ4_freeStreamHC(LZ4_streamHC);
        }

        FUZ_DISPLAYTEST();
        decodedBuffer[blockSize] = 0;
        ret = LZ4_decompress_safe_usingDict(compressedBuffer, decodedBuffer, blockContinueCompressedSize, blockSize, dict, dictSize);
        FUZ_CHECKTEST(ret!=blockSize, "LZ4_decompress_safe_usingDict did not regenerate original data");
        FUZ_CHECKTEST(decodedBuffer[blockSize], "LZ4_decompress_safe_usingDict overrun specified output buffer size");
        {   U32 const crcCheck = XXH32(decodedBuffer, (size_t)blockSize, 0);
            if (crcCheck!=crcOrig) {
                FUZ_findDiff(block, decodedBuffer);
                EXIT_MSG("LZ4_decompress_safe_usingDict corrupted decoded data");
        }   }

        /* Compress HC continue destSize */
        FUZ_DISPLAYTEST();
        {   int const availableSpace = (int)(FUZ_rand(&randState) % blockSize) + 5;
            int consumedSize = blockSize;
            FUZ_DISPLAYTEST();
            LZ4_loadDictHC(LZ4dictHC, dict, dictSize);
            LZ4_setCompressionLevel(LZ4dictHC, compressionLevel);
            blockContinueCompressedSize = LZ4_compress_HC_continue_destSize(LZ4dictHC, block, compressedBuffer, &consumedSize, availableSpace);
            DISPLAYLEVEL(5, " LZ4_compress_HC_continue_destSize : compressed %6i/%6i into %6i/%6i at cLevel=%i\n", consumedSize, blockSize, blockContinueCompressedSize, availableSpace, compressionLevel);
            FUZ_CHECKTEST(blockContinueCompressedSize==0, "LZ4_compress_HC_continue_destSize failed");
            FUZ_CHECKTEST(blockContinueCompressedSize > availableSpace, "LZ4_compress_HC_continue_destSize write overflow");
            FUZ_CHECKTEST(consumedSize > blockSize, "LZ4_compress_HC_continue_destSize read overflow");

            FUZ_DISPLAYTEST();
            decodedBuffer[consumedSize] = 0;
            ret = LZ4_decompress_safe_usingDict(compressedBuffer, decodedBuffer, blockContinueCompressedSize, consumedSize, dict, dictSize);
            FUZ_CHECKTEST(ret!=consumedSize, "LZ4_decompress_safe_usingDict did not regenerate original data");
            FUZ_CHECKTEST(decodedBuffer[consumedSize], "LZ4_decompress_safe_usingDict overrun specified output buffer size")
            {   U32 const crcSrc = XXH32(block, (size_t)consumedSize, 0);
                U32 const crcDst = XXH32(decodedBuffer, (size_t)consumedSize, 0);
                if (crcSrc!=crcDst) {
                    FUZ_findDiff(block, decodedBuffer);
                    EXIT_MSG("LZ4_decompress_safe_usingDict corrupted decoded data");
            }   }
        }

        /* ***** End of tests *** */
        /* Fill stats */
        bytes += blockSize;
        cbytes += compressedSize;
        hcbytes += HCcompressedSize;
        ccbytes += blockContinueCompressedSize;
    }

    if (nbCycles<=1) nbCycles = cycleNb;   /* end by time */
    bytes += !bytes;   /* avoid division by 0 */
    printf("\r%7u /%7u   - ", cycleNb, nbCycles);
    printf("all tests completed successfully \n");
    printf("compression ratio: %0.3f%%\n", (double)cbytes/bytes*100);
    printf("HC compression ratio: %0.3f%%\n", (double)hcbytes/bytes*100);
    printf("ratio with dict: %0.3f%%\n", (double)ccbytes/bytes*100);

    /* release memory */
    free(CNBuffer);
    free(compressedBuffer);
    free(decodedBuffer);
    FUZ_freeLowAddr(lowAddrBuffer, labSize);
    LZ4_freeStreamHC(LZ4dictHC);
    free(stateLZ4);
    free(stateLZ4HC);
    return result;
}


#define testInputSize (192 KB)
#define testCompressedSize (128 KB)
#define ringBufferSize (8 KB)

static void FUZ_unitTests(int compressionLevel)
{
    const unsigned testNb = 0;
    const unsigned seed   = 0;
    const unsigned cycleNb= 0;
    char* testInput = (char*)malloc(testInputSize);
    char* testCompressed = (char*)malloc(testCompressedSize);
    char* testVerify = (char*)malloc(testInputSize);
    char ringBuffer[ringBufferSize] = {0};
    U32 randState = 1;

    /* Init */
    if (!testInput || !testCompressed || !testVerify) {
        EXIT_MSG("not enough memory for FUZ_unitTests");
    }
    FUZ_fillCompressibleNoiseBuffer(testInput, testInputSize, 0.50, &randState);

    /* 32-bits address space overflow test */
    FUZ_AddressOverflow();

    /* Test decoding with empty input */
    DISPLAYLEVEL(3, "LZ4_decompress_safe() with empty input");
    LZ4_decompress_safe(testCompressed, testVerify, 0, testInputSize);

    /* Test decoding with a one byte input */
    DISPLAYLEVEL(3, "LZ4_decompress_safe() with one byte input");
    {   char const tmp = (char)0xFF;
        LZ4_decompress_safe(&tmp, testVerify, 1, testInputSize);
    }

    /* Test decoding shortcut edge case */
    DISPLAYLEVEL(3, "LZ4_decompress_safe() with shortcut edge case");
    {   char tmp[17];
        /* 14 bytes of literals, followed by a 14 byte match.
         * Should not read beyond the end of the buffer.
         * See https://github.com/lz4/lz4/issues/508. */
        *tmp = (char)0xEE;
        memset(tmp + 1, 0, 14);
        tmp[15] = 14;
        tmp[16] = 0;
        {   int const r = LZ4_decompress_safe(tmp, testVerify, sizeof(tmp), testInputSize);
            FUZ_CHECKTEST(r >= 0, "LZ4_decompress_safe() should fail");
    }   }

    /* LZ4 streaming tests */
    {   LZ4_stream_t  streamingState;
        U64 crcOrig;
        int result;

        /* Allocation test */
        {   LZ4_stream_t* const statePtr = LZ4_createStream();
            FUZ_CHECKTEST(statePtr==NULL, "LZ4_createStream() allocation failed");
            LZ4_freeStream(statePtr);
        }

        /* simple compression test */
        crcOrig = XXH64(testInput, testCompressedSize, 0);
        LZ4_initStream(&streamingState, sizeof(streamingState));
        result = LZ4_compress_fast_continue(&streamingState, testInput, testCompressed, testCompressedSize, testCompressedSize-1, 1);
        FUZ_CHECKTEST(result==0, "LZ4_compress_fast_continue() compression failed!");
        FUZ_CHECKTEST(streamingState.internal_donotuse.dirty, "context should be clean")

        result = LZ4_decompress_safe(testCompressed, testVerify, result, testCompressedSize);
        FUZ_CHECKTEST(result!=(int)testCompressedSize, "LZ4_decompress_safe() decompression failed");
        { U64 const crcNew = XXH64(testVerify, testCompressedSize, 0);
          FUZ_CHECKTEST(crcOrig!=crcNew, "LZ4_decompress_safe() decompression corruption"); }

        /* ring buffer test */
        {   XXH64_state_t xxhOrig;
            XXH64_state_t xxhNewSafe, xxhNewFast;
            LZ4_streamDecode_t decodeStateSafe, decodeStateFast;
            const U32 maxMessageSizeLog = 10;
            const U32 maxMessageSizeMask = (1<<maxMessageSizeLog) - 1;
            U32 messageSize = (FUZ_rand(&randState) & maxMessageSizeMask) + 1;
            U32 iNext = 0;
            U32 rNext = 0;
            U32 dNext = 0;
            const U32 dBufferSize = ringBufferSize + maxMessageSizeMask;

            XXH64_reset(&xxhOrig, 0);
            XXH64_reset(&xxhNewSafe, 0);
            XXH64_reset(&xxhNewFast, 0);
            LZ4_resetStream_fast(&streamingState);
            LZ4_setStreamDecode(&decodeStateSafe, NULL, 0);
            LZ4_setStreamDecode(&decodeStateFast, NULL, 0);

            while (iNext + messageSize < testCompressedSize) {
                int compressedSize;
                XXH64_update(&xxhOrig, testInput + iNext, messageSize);
                crcOrig = XXH64_digest(&xxhOrig);

                memcpy (ringBuffer + rNext, testInput + iNext, messageSize);
                compressedSize = LZ4_compress_fast_continue(&streamingState, ringBuffer + rNext, testCompressed, messageSize, testCompressedSize-ringBufferSize, 1);
                FUZ_CHECKTEST(compressedSize==0, "LZ4_compress_fast_continue() compression failed");

                result = LZ4_decompress_safe_continue(&decodeStateSafe, testCompressed, testVerify + dNext, compressedSize, messageSize);
                FUZ_CHECKTEST(result!=(int)messageSize, "ringBuffer : LZ4_decompress_safe_continue() test failed");

                XXH64_update(&xxhNewSafe, testVerify + dNext, messageSize);
                { U64 const crcNew = XXH64_digest(&xxhNewSafe);
                  FUZ_CHECKTEST(crcOrig!=crcNew, "LZ4_decompress_safe_continue() decompression corruption"); }

                result = LZ4_decompress_fast_continue(&decodeStateFast, testCompressed, testVerify + dNext, messageSize);
                FUZ_CHECKTEST(result!=compressedSize, "ringBuffer : LZ4_decompress_fast_continue() test failed");

                XXH64_update(&xxhNewFast, testVerify + dNext, messageSize);
                { U64 const crcNew = XXH64_digest(&xxhNewFast);
                  FUZ_CHECKTEST(crcOrig!=crcNew, "LZ4_decompress_fast_continue() decompression corruption"); }

                /* prepare next message */
                iNext += messageSize;
                rNext += messageSize;
                dNext += messageSize;
                messageSize = (FUZ_rand(&randState) & maxMessageSizeMask) + 1;
                if (rNext + messageSize > ringBufferSize) rNext = 0;
                if (dNext + messageSize > dBufferSize) dNext = 0;
            }
        }
    }

    /* LZ4 HC streaming tests */
    {   LZ4_streamHC_t sHC;   /* statically allocated */
        U64 crcOrig;
        int result;
        LZ4_initStreamHC(&sHC, sizeof(sHC));

        /* Allocation test */
        DISPLAYLEVEL(3, " Basic HC allocation : ");
        {   LZ4_streamHC_t* const sp = LZ4_createStreamHC();
            FUZ_CHECKTEST(sp==NULL, "LZ4_createStreamHC() allocation failed");
            LZ4_freeStreamHC(sp);
        }
        DISPLAYLEVEL(3, " OK \n");

        /* simple HC compression test */
        DISPLAYLEVEL(3, " Simple HC round-trip : ");
        {   U64 const crc64 = XXH64(testInput, testCompressedSize, 0);
            LZ4_setCompressionLevel(&sHC, compressionLevel);
            result = LZ4_compress_HC_continue(&sHC, testInput, testCompressed, testCompressedSize, testCompressedSize-1);
            FUZ_CHECKTEST(result==0, "LZ4_compressHC_limitedOutput_continue() compression failed");
            FUZ_CHECKTEST(sHC.internal_donotuse.dirty, "Context should be clean");

            result = LZ4_decompress_safe(testCompressed, testVerify, result, testCompressedSize);
            FUZ_CHECKTEST(result!=(int)testCompressedSize, "LZ4_decompress_safe() decompression failed");
            {   U64 const crcNew = XXH64(testVerify, testCompressedSize, 0);
                FUZ_CHECKTEST(crc64!=crcNew, "LZ4_decompress_safe() decompression corruption");
        }   }
        DISPLAYLEVEL(3, " OK \n");

        /* long sequence test */
        DISPLAYLEVEL(3, " Long sequence HC test : ");
        {   size_t const blockSize = 1 MB;
            size_t const targetSize = 4116;  /* size carefully selected to trigger an overflow */
            void*  const block = malloc(blockSize);
            void*  const dstBlock = malloc(targetSize+1);
            BYTE   const sentinel = 101;
            int srcSize;

            assert(block != NULL); assert(dstBlock != NULL);
            memset(block, 0, blockSize);
            ((char*)dstBlock)[targetSize] = sentinel;

            LZ4_resetStreamHC_fast(&sHC, 3);
            assert(blockSize < INT_MAX);
            srcSize = (int)blockSize;
            assert(targetSize < INT_MAX);
            result = LZ4_compress_HC_destSize(&sHC, (const char*)block, (char*)dstBlock, &srcSize, (int)targetSize, 3);
            DISPLAYLEVEL(4, "cSize=%i; readSize=%i; ", result, srcSize);
            FUZ_CHECKTEST(result!=4116, "LZ4_compress_HC_destSize() : compression must fill dstBuffer completely, but no more !");
            FUZ_CHECKTEST(((char*)dstBlock)[targetSize] != sentinel, "LZ4_compress_HC_destSize()")

            LZ4_resetStreamHC_fast(&sHC, 3);   /* make sure the context is clean after the test */
            free(block);
            free(dstBlock);
        }
        DISPLAYLEVEL(3, " OK \n");

        /* simple dictionary HC compression test */
        DISPLAYLEVEL(3, " HC dictionary compression test : ");
        {   U64 const crc64 = XXH64(testInput + 64 KB, testCompressedSize, 0);
            LZ4_resetStreamHC_fast(&sHC, compressionLevel);
            LZ4_loadDictHC(&sHC, testInput, 64 KB);
            result = LZ4_compress_HC_continue(&sHC, testInput + 64 KB, testCompressed, testCompressedSize, testCompressedSize-1);
            FUZ_CHECKTEST(result==0, "LZ4_compressHC_limitedOutput_continue() dictionary compression failed : result = %i", result);
            FUZ_CHECKTEST(sHC.internal_donotuse.dirty, "Context should be clean");

            result = LZ4_decompress_safe_usingDict(testCompressed, testVerify, result, testCompressedSize, testInput, 64 KB);
            FUZ_CHECKTEST(result!=(int)testCompressedSize, "LZ4_decompress_safe() simple dictionary decompression test failed");
            {   U64 const crcNew = XXH64(testVerify, testCompressedSize, 0);
                FUZ_CHECKTEST(crc64!=crcNew, "LZ4_decompress_safe() simple dictionary decompression test : corruption");
        }   }
        DISPLAYLEVEL(3, " OK \n");

        /* multiple HC compression test with dictionary */
        {   int result1, result2;
            int segSize = testCompressedSize / 2;
            U64 const crc64 = XXH64(testInput + segSize, testCompressedSize, 0);
            LZ4_resetStreamHC_fast(&sHC, compressionLevel);
            LZ4_loadDictHC(&sHC, testInput, segSize);
            result1 = LZ4_compress_HC_continue(&sHC, testInput + segSize, testCompressed, segSize, segSize -1);
            FUZ_CHECKTEST(result1==0, "LZ4_compressHC_limitedOutput_continue() dictionary compression failed : result = %i", result1);
            FUZ_CHECKTEST(sHC.internal_donotuse.dirty, "Context should be clean");
            result2 = LZ4_compress_HC_continue(&sHC, testInput + 2*(size_t)segSize, testCompressed+result1, segSize, segSize-1);
            FUZ_CHECKTEST(result2==0, "LZ4_compressHC_limitedOutput_continue() dictionary compression failed : result = %i", result2);
            FUZ_CHECKTEST(sHC.internal_donotuse.dirty, "Context should be clean");

            result = LZ4_decompress_safe_usingDict(testCompressed, testVerify, result1, segSize, testInput, segSize);
            FUZ_CHECKTEST(result!=segSize, "LZ4_decompress_safe() dictionary decompression part 1 failed");
            result = LZ4_decompress_safe_usingDict(testCompressed+result1, testVerify+segSize, result2, segSize, testInput, 2*segSize);
            FUZ_CHECKTEST(result!=segSize, "LZ4_decompress_safe() dictionary decompression part 2 failed");
            {   U64 const crcNew = XXH64(testVerify, testCompressedSize, 0);
                FUZ_CHECKTEST(crc64!=crcNew, "LZ4_decompress_safe() dictionary decompression corruption");
        }   }

        /* remote dictionary HC compression test */
        {   U64 const crc64 = XXH64(testInput + 64 KB, testCompressedSize, 0);
            LZ4_resetStreamHC_fast(&sHC, compressionLevel);
            LZ4_loadDictHC(&sHC, testInput, 32 KB);
            result = LZ4_compress_HC_continue(&sHC, testInput + 64 KB, testCompressed, testCompressedSize, testCompressedSize-1);
            FUZ_CHECKTEST(result==0, "LZ4_compressHC_limitedOutput_continue() remote dictionary failed : result = %i", result);
            FUZ_CHECKTEST(sHC.internal_donotuse.dirty, "Context should be clean");

            result = LZ4_decompress_safe_usingDict(testCompressed, testVerify, result, testCompressedSize, testInput, 32 KB);
            FUZ_CHECKTEST(result!=(int)testCompressedSize, "LZ4_decompress_safe_usingDict() decompression failed following remote dictionary HC compression test");
            {   U64 const crcNew = XXH64(testVerify, testCompressedSize, 0);
                FUZ_CHECKTEST(crc64!=crcNew, "LZ4_decompress_safe_usingDict() decompression corruption");
        }   }

        /* multiple HC compression with ext. dictionary */
        {   XXH64_state_t crcOrigState;
            XXH64_state_t crcNewState;
            const char* dict = testInput + 3;
            int dictSize = (FUZ_rand(&randState) & 8191);
            char* dst = testVerify;

            size_t segStart = (size_t)dictSize + 7;
            int segSize = (FUZ_rand(&randState) & 8191);
            int segNb = 1;

            LZ4_resetStreamHC_fast(&sHC, compressionLevel);
            LZ4_loadDictHC(&sHC, dict, dictSize);

            XXH64_reset(&crcOrigState, 0);
            XXH64_reset(&crcNewState, 0);

            while (segStart + segSize < testInputSize) {
                XXH64_update(&crcOrigState, testInput + segStart, segSize);
                crcOrig = XXH64_digest(&crcOrigState);
                result = LZ4_compress_HC_continue(&sHC, testInput + segStart, testCompressed, segSize, LZ4_compressBound(segSize));
                FUZ_CHECKTEST(result==0, "LZ4_compressHC_limitedOutput_continue() dictionary compression failed : result = %i", result);
                FUZ_CHECKTEST(sHC.internal_donotuse.dirty, "Context should be clean");

                result = LZ4_decompress_safe_usingDict(testCompressed, dst, result, segSize, dict, dictSize);
                FUZ_CHECKTEST(result!=segSize, "LZ4_decompress_safe_usingDict() dictionary decompression part %i failed", segNb);
                XXH64_update(&crcNewState, dst, segSize);
                {   U64 const crcNew = XXH64_digest(&crcNewState);
                    if (crcOrig != crcNew) FUZ_findDiff(dst, testInput+segStart);
                    FUZ_CHECKTEST(crcOrig!=crcNew, "LZ4_decompress_safe_usingDict() part %i corruption", segNb);
                }

                assert(segSize >= 0);
                dict = dst;
                dictSize = segSize;

                dst += (size_t)segSize + 1;
                segNb ++;

                segStart += (size_t)segSize + (FUZ_rand(&randState) & 0xF) + 1;
                segSize = (FUZ_rand(&randState) & 8191);
            }
        }

        /* ring buffer test */
        {   XXH64_state_t xxhOrig;
            XXH64_state_t xxhNewSafe, xxhNewFast;
            LZ4_streamDecode_t decodeStateSafe, decodeStateFast;
            const U32 maxMessageSizeLog = 10;
            const U32 maxMessageSizeMask = (1<<maxMessageSizeLog) - 1;
            U32 messageSize = (FUZ_rand(&randState) & maxMessageSizeMask) + 1;
            U32 iNext = 0;
            U32 rNext = 0;
            U32 dNext = 0;
            const U32 dBufferSize = ringBufferSize + maxMessageSizeMask;

            XXH64_reset(&xxhOrig, 0);
            XXH64_reset(&xxhNewSafe, 0);
            XXH64_reset(&xxhNewFast, 0);
            LZ4_resetStreamHC_fast(&sHC, compressionLevel);
            LZ4_setStreamDecode(&decodeStateSafe, NULL, 0);
            LZ4_setStreamDecode(&decodeStateFast, NULL, 0);

            while (iNext + messageSize < testCompressedSize) {
                int compressedSize;
                XXH64_update(&xxhOrig, testInput + iNext, messageSize);
                crcOrig = XXH64_digest(&xxhOrig);

                memcpy (ringBuffer + rNext, testInput + iNext, messageSize);
                compressedSize = LZ4_compress_HC_continue(&sHC, ringBuffer + rNext, testCompressed, messageSize, testCompressedSize-ringBufferSize);
                FUZ_CHECKTEST(compressedSize==0, "LZ4_compress_HC_continue() compression failed");
                FUZ_CHECKTEST(sHC.internal_donotuse.dirty, "Context should be clean");

                result = LZ4_decompress_safe_continue(&decodeStateSafe, testCompressed, testVerify + dNext, compressedSize, messageSize);
                FUZ_CHECKTEST(result!=(int)messageSize, "ringBuffer : LZ4_decompress_safe_continue() test failed");

                XXH64_update(&xxhNewSafe, testVerify + dNext, messageSize);
                { U64 const crcNew = XXH64_digest(&xxhNewSafe);
                  FUZ_CHECKTEST(crcOrig!=crcNew, "LZ4_decompress_safe_continue() decompression corruption"); }

                result = LZ4_decompress_fast_continue(&decodeStateFast, testCompressed, testVerify + dNext, messageSize);
                FUZ_CHECKTEST(result!=compressedSize, "ringBuffer : LZ4_decompress_fast_continue() test failed");

                XXH64_update(&xxhNewFast, testVerify + dNext, messageSize);
                { U64 const crcNew = XXH64_digest(&xxhNewFast);
                  FUZ_CHECKTEST(crcOrig!=crcNew, "LZ4_decompress_fast_continue() decompression corruption"); }

                /* prepare next message */
                iNext += messageSize;
                rNext += messageSize;
                dNext += messageSize;
                messageSize = (FUZ_rand(&randState) & maxMessageSizeMask) + 1;
                if (rNext + messageSize > ringBufferSize) rNext = 0;
                if (dNext + messageSize > dBufferSize) dNext = 0;
            }
        }

        /* Ring buffer test : Non synchronized decoder */
        /* This test uses minimum amount of memory required to setup a decoding ring buffer
         * while being unsynchronized with encoder
         * (no assumption done on how the data is encoded, it just follows LZ4 format specification).
         * This size is documented in lz4.h, and is LZ4_decoderRingBufferSize(maxBlockSize).
         */
        {   XXH64_state_t xxhOrig;
            XXH64_state_t xxhNewSafe, xxhNewFast;
            LZ4_streamDecode_t decodeStateSafe, decodeStateFast;
            const int maxMessageSizeLog = 12;
            const int maxMessageSize = 1 << maxMessageSizeLog;
            const int maxMessageSizeMask = maxMessageSize - 1;
            int messageSize;
            U32 totalMessageSize = 0;
            const int dBufferSize = LZ4_decoderRingBufferSize(maxMessageSize);
            char* const ringBufferSafe = testVerify;
            char* const ringBufferFast = testVerify + dBufferSize + 1;   /* used by LZ4_decompress_fast_continue */
            int iNext = 0;
            int dNext = 0;
            int compressedSize;

            assert((size_t)dBufferSize * 2 + 1 < testInputSize);   /* space used by ringBufferSafe and ringBufferFast */
            XXH64_reset(&xxhOrig, 0);
            XXH64_reset(&xxhNewSafe, 0);
            XXH64_reset(&xxhNewFast, 0);
            LZ4_resetStreamHC_fast(&sHC, compressionLevel);
            LZ4_setStreamDecode(&decodeStateSafe, NULL, 0);
            LZ4_setStreamDecode(&decodeStateFast, NULL, 0);

#define BSIZE1 (dBufferSize - (maxMessageSize-1))

            /* first block */
            messageSize = BSIZE1;   /* note : we cheat a bit here, in theory no message should be > maxMessageSize. We just want to fill the decoding ring buffer once. */
            XXH64_update(&xxhOrig, testInput + iNext, (size_t)messageSize);
            crcOrig = XXH64_digest(&xxhOrig);

            compressedSize = LZ4_compress_HC_continue(&sHC, testInput + iNext, testCompressed, messageSize, testCompressedSize-ringBufferSize);
            FUZ_CHECKTEST(compressedSize==0, "LZ4_compress_HC_continue() compression failed");
            FUZ_CHECKTEST(sHC.internal_donotuse.dirty, "Context should be clean");

            result = LZ4_decompress_safe_continue(&decodeStateSafe, testCompressed, ringBufferSafe + dNext, compressedSize, messageSize);
            FUZ_CHECKTEST(result!=messageSize, "64K D.ringBuffer : LZ4_decompress_safe_continue() test failed");

            XXH64_update(&xxhNewSafe, ringBufferSafe + dNext, messageSize);
            { U64 const crcNew = XXH64_digest(&xxhNewSafe);
              FUZ_CHECKTEST(crcOrig!=crcNew, "LZ4_decompress_safe_continue() decompression corruption"); }

            result = LZ4_decompress_fast_continue(&decodeStateFast, testCompressed, ringBufferFast + dNext, messageSize);
            FUZ_CHECKTEST(result!=compressedSize, "64K D.ringBuffer : LZ4_decompress_fast_continue() test failed");

            XXH64_update(&xxhNewFast, ringBufferFast + dNext, messageSize);
            { U64 const crcNew = XXH64_digest(&xxhNewFast);
              FUZ_CHECKTEST(crcOrig!=crcNew, "LZ4_decompress_fast_continue() decompression corruption"); }

            /* prepare second message */
            dNext += messageSize;
            assert(messageSize >= 0);
            totalMessageSize += (unsigned)messageSize;
            messageSize = maxMessageSize;
            iNext = BSIZE1+1;
            assert(BSIZE1 >= 65535);
            memcpy(testInput + iNext, testInput + (BSIZE1-65535), messageSize);  /* will generate a match at max distance == 65535 */
            FUZ_CHECKTEST(dNext+messageSize <= dBufferSize, "Ring buffer test : second message should require restarting from beginning");
            dNext = 0;

            while (totalMessageSize < 9 MB) {
                XXH64_update(&xxhOrig, testInput + iNext, messageSize);
                crcOrig = XXH64_digest(&xxhOrig);

                compressedSize = LZ4_compress_HC_continue(&sHC, testInput + iNext, testCompressed, messageSize, testCompressedSize-ringBufferSize);
                FUZ_CHECKTEST(compressedSize==0, "LZ4_compress_HC_continue() compression failed");
                FUZ_CHECKTEST(sHC.internal_donotuse.dirty, "Context should be clean");
                DISPLAYLEVEL(5, "compressed %i bytes to %i bytes \n", messageSize, compressedSize);

                /* test LZ4_decompress_safe_continue */
                assert(dNext < dBufferSize);
                assert(dBufferSize - dNext >= maxMessageSize);
                result = LZ4_decompress_safe_continue(&decodeStateSafe,
                                                      testCompressed, ringBufferSafe + dNext,
                                                      compressedSize, dBufferSize - dNext);   /* works without knowing messageSize, under assumption that messageSize <= maxMessageSize */
                FUZ_CHECKTEST(result!=messageSize, "D.ringBuffer : LZ4_decompress_safe_continue() test failed");
                XXH64_update(&xxhNewSafe, ringBufferSafe + dNext, messageSize);
                {   U64 const crcNew = XXH64_digest(&xxhNewSafe);
                    if (crcOrig != crcNew) FUZ_findDiff(testInput + iNext, ringBufferSafe + dNext);
                    FUZ_CHECKTEST(crcOrig!=crcNew, "LZ4_decompress_safe_continue() decompression corruption during D.ringBuffer test");
                }

                /* test LZ4_decompress_fast_continue in its own buffer ringBufferFast */
                result = LZ4_decompress_fast_continue(&decodeStateFast, testCompressed, ringBufferFast + dNext, messageSize);
                FUZ_CHECKTEST(result!=compressedSize, "D.ringBuffer : LZ4_decompress_fast_continue() test failed");
                XXH64_update(&xxhNewFast, ringBufferFast + dNext, messageSize);
                {   U64 const crcNew = XXH64_digest(&xxhNewFast);
                    if (crcOrig != crcNew) FUZ_findDiff(testInput + iNext, ringBufferFast + dNext);
                    FUZ_CHECKTEST(crcOrig!=crcNew, "LZ4_decompress_fast_continue() decompression corruption during D.ringBuffer test");
                }

                /* prepare next message */
                dNext += messageSize;
                totalMessageSize += messageSize;
                messageSize = (FUZ_rand(&randState) & maxMessageSizeMask) + 1;
                iNext = (FUZ_rand(&randState) & 65535);
                if (dNext + maxMessageSize > dBufferSize) dNext = 0;
            }
        }
    }

    /* clean up */
    free(testInput);
    free(testCompressed);
    free(testVerify);

    printf("All unit tests completed successfully compressionLevel=%d \n", compressionLevel);
    return;
}


static int FUZ_usage(const char* programName)
{
    DISPLAY( "Usage :\n");
    DISPLAY( "      %s [args]\n", programName);
    DISPLAY( "\n");
    DISPLAY( "Arguments :\n");
    DISPLAY( " -i#    : Nb of tests (default:%i) \n", NB_ATTEMPTS);
    DISPLAY( " -T#    : Duration of tests, in seconds (default: use Nb of tests) \n");
    DISPLAY( " -s#    : Select seed (default:prompt user)\n");
    DISPLAY( " -t#    : Select starting test number (default:0)\n");
    DISPLAY( " -P#    : Select compressibility in %% (default:%i%%)\n", FUZ_COMPRESSIBILITY_DEFAULT);
    DISPLAY( " -v     : verbose\n");
    DISPLAY( " -p     : pause at the end\n");
    DISPLAY( " -h     : display help and exit\n");
    return 0;
}


int main(int argc, const char** argv)
{
    U32 seed = 0;
    int seedset = 0;
    int argNb;
    int nbTests = NB_ATTEMPTS;
    int testNb = 0;
    int proba = FUZ_COMPRESSIBILITY_DEFAULT;
    int use_pause = 0;
    const char* programName = argv[0];
    U32 duration = 0;

    /* Check command line */
    for(argNb=1; argNb<argc; argNb++) {
        const char* argument = argv[argNb];

        if(!argument) continue;   // Protection if argument empty

        // Decode command (note : aggregated commands are allowed)
        if (argument[0]=='-') {
            if (!strcmp(argument, "--no-prompt")) { use_pause=0; seedset=1; g_displayLevel=1; continue; }
            argument++;

            while (*argument!=0) {
                switch(*argument)
                {
                case 'h':   /* display help */
                    return FUZ_usage(programName);

                case 'v':   /* verbose mode */
                    g_displayLevel++;
                    argument++;
                    break;

                case 'p':   /* pause at the end */
                    use_pause=1;
                    argument++;
                    break;

                case 'i':
                    argument++;
                    nbTests = 0; duration = 0;
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
                    seed=0; seedset=1;
                    while ((*argument>='0') && (*argument<='9')) {
                        seed *= 10;
                        seed += *argument - '0';
                        argument++;
                    }
                    break;

                case 't':   /* select starting test nb */
                    argument++;
                    testNb=0;
                    while ((*argument>='0') && (*argument<='9')) {
                        testNb *= 10;
                        testNb += *argument - '0';
                        argument++;
                    }
                    break;

                case 'P':  /* change probability */
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
                default: ;
                }
            }
        }
    }

    printf("Starting LZ4 fuzzer (%i-bits, v%s)\n", (int)(sizeof(size_t)*8), LZ4_versionString());

    if (!seedset) {
        time_t const t = time(NULL);
        U32 const h = XXH32(&t, sizeof(t), 1);
        seed = h % 10000;
    }
    printf("Seed = %u\n", seed);

    if (proba!=FUZ_COMPRESSIBILITY_DEFAULT) printf("Compressibility : %i%%\n", proba);

    if ((seedset==0) && (testNb==0)) { FUZ_unitTests(LZ4HC_CLEVEL_DEFAULT); FUZ_unitTests(LZ4HC_CLEVEL_OPT_MIN); }

    if (nbTests<=0) nbTests=1;

    {   int const result = FUZ_test(seed, nbTests, testNb, ((double)proba) / 100, duration);
        if (use_pause) {
            DISPLAY("press enter ... \n");
            (void)getchar();
        }
        return result;
    }
}
