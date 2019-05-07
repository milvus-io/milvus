/*
    bench.c - Demo program to benchmark open-source compression algorithms
    Copyright (C) Yann Collet 2012-2016

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
*  Compiler options
**************************************/
#ifdef _MSC_VER    /* Visual Studio */
#  pragma warning(disable : 4127)    /* disable: C4127: conditional expression is constant */
#endif


/* *************************************
*  Includes
***************************************/
#include "platform.h"    /* Compiler options */
#include "util.h"        /* UTIL_GetFileSize, UTIL_sleep */
#include <stdlib.h>      /* malloc, free */
#include <string.h>      /* memset */
#include <stdio.h>       /* fprintf, fopen, ftello */
#include <time.h>        /* clock_t, clock, CLOCKS_PER_SEC */
#include <assert.h>      /* assert */

#include "datagen.h"     /* RDG_genBuffer */
#include "xxhash.h"


#include "lz4.h"
#define COMPRESSOR0 LZ4_compress_local
static int LZ4_compress_local(const char* src, char* dst, int srcSize, int dstSize, int clevel) {
  int const acceleration = (clevel < 0) ? -clevel + 1 : 1;
  return LZ4_compress_fast(src, dst, srcSize, dstSize, acceleration);
}
#include "lz4hc.h"
#define COMPRESSOR1 LZ4_compress_HC
#define DEFAULTCOMPRESSOR COMPRESSOR0
#define LZ4_isError(errcode) (errcode==0)


/* *************************************
*  Constants
***************************************/
#ifndef LZ4_GIT_COMMIT_STRING
#  define LZ4_GIT_COMMIT_STRING ""
#else
#  define LZ4_GIT_COMMIT_STRING LZ4_EXPAND_AND_QUOTE(LZ4_GIT_COMMIT)
#endif

#define NBSECONDS             3
#define TIMELOOP_MICROSEC     1*1000000ULL /* 1 second */
#define TIMELOOP_NANOSEC      1*1000000000ULL /* 1 second */
#define ACTIVEPERIOD_MICROSEC 70*1000000ULL /* 70 seconds */
#define COOLPERIOD_SEC        10
#define DECOMP_MULT           1 /* test decompression DECOMP_MULT times longer than compression */

#define KB *(1 <<10)
#define MB *(1 <<20)
#define GB *(1U<<30)

static const size_t maxMemory = (sizeof(size_t)==4)  ?  (2 GB - 64 MB) : (size_t)(1ULL << ((sizeof(size_t)*8)-31));

static U32 g_compressibilityDefault = 50;


/* *************************************
*  console display
***************************************/
#define DISPLAY(...)         fprintf(stderr, __VA_ARGS__)
#define DISPLAYLEVEL(l, ...) if (g_displayLevel>=l) { DISPLAY(__VA_ARGS__); }
static U32 g_displayLevel = 2;   /* 0 : no display;   1: errors;   2 : + result + interaction + warnings;   3 : + progression;   4 : + information */

#define DISPLAYUPDATE(l, ...) if (g_displayLevel>=l) { \
            if ((clock() - g_time > refreshRate) || (g_displayLevel>=4)) \
            { g_time = clock(); DISPLAY(__VA_ARGS__); \
            if (g_displayLevel>=4) fflush(stdout); } }
static const clock_t refreshRate = CLOCKS_PER_SEC * 15 / 100;
static clock_t g_time = 0;


/* *************************************
*  Exceptions
***************************************/
#ifndef DEBUG
#  define DEBUG 0
#endif
#define DEBUGOUTPUT(...) if (DEBUG) DISPLAY(__VA_ARGS__);
#define EXM_THROW(error, ...)                                             \
{                                                                         \
    DEBUGOUTPUT("Error defined at %s, line %i : \n", __FILE__, __LINE__); \
    DISPLAYLEVEL(1, "Error %i : ", error);                                \
    DISPLAYLEVEL(1, __VA_ARGS__);                                         \
    DISPLAYLEVEL(1, "\n");                                                \
    exit(error);                                                          \
}


/* *************************************
*  Benchmark Parameters
***************************************/
static U32 g_nbSeconds = NBSECONDS;
static size_t g_blockSize = 0;
int g_additionalParam = 0;
int g_benchSeparately = 0;

void BMK_setNotificationLevel(unsigned level) { g_displayLevel=level; }

void BMK_setAdditionalParam(int additionalParam) { g_additionalParam=additionalParam; }

void BMK_setNbSeconds(unsigned nbSeconds)
{
    g_nbSeconds = nbSeconds;
    DISPLAYLEVEL(3, "- test >= %u seconds per compression / decompression -\n", g_nbSeconds);
}

void BMK_setBlockSize(size_t blockSize) { g_blockSize = blockSize; }

void BMK_setBenchSeparately(int separate) { g_benchSeparately = (separate!=0); }


/* ********************************************************
*  Bench functions
**********************************************************/
typedef struct {
    const char* srcPtr;
    size_t srcSize;
    char*  cPtr;
    size_t cRoom;
    size_t cSize;
    char*  resPtr;
    size_t resSize;
} blockParam_t;

struct compressionParameters
{
    int (*compressionFunction)(const char* src, char* dst, int srcSize, int dstSize, int cLevel);
};

#define MIN(a,b) ((a)<(b) ? (a) : (b))
#define MAX(a,b) ((a)>(b) ? (a) : (b))

static int BMK_benchMem(const void* srcBuffer, size_t srcSize,
                        const char* displayName, int cLevel,
                        const size_t* fileSizes, U32 nbFiles)
{
    size_t const blockSize = (g_blockSize>=32 ? g_blockSize : srcSize) + (!srcSize) /* avoid div by 0 */ ;
    U32 const maxNbBlocks = (U32) ((srcSize + (blockSize-1)) / blockSize) + nbFiles;
    blockParam_t* const blockTable = (blockParam_t*) malloc(maxNbBlocks * sizeof(blockParam_t));
    size_t const maxCompressedSize = LZ4_compressBound((int)srcSize) + (maxNbBlocks * 1024);   /* add some room for safety */
    void* const compressedBuffer = malloc(maxCompressedSize);
    void* const resultBuffer = malloc(srcSize);
    U32 nbBlocks;
    struct compressionParameters compP;
    int cfunctionId;

    /* checks */
    if (!compressedBuffer || !resultBuffer || !blockTable)
        EXM_THROW(31, "allocation error : not enough memory");

    /* init */
    if (strlen(displayName)>17) displayName += strlen(displayName)-17;   /* can only display 17 characters */

    /* Init */
    if (cLevel < LZ4HC_CLEVEL_MIN) cfunctionId = 0; else cfunctionId = 1;
    switch (cfunctionId)
    {
#ifdef COMPRESSOR0
    case 0 : compP.compressionFunction = COMPRESSOR0; break;
#endif
#ifdef COMPRESSOR1
    case 1 : compP.compressionFunction = COMPRESSOR1; break;
#endif
    default : compP.compressionFunction = DEFAULTCOMPRESSOR;
    }

    /* Init blockTable data */
    {   const char* srcPtr = (const char*)srcBuffer;
        char* cPtr = (char*)compressedBuffer;
        char* resPtr = (char*)resultBuffer;
        U32 fileNb;
        for (nbBlocks=0, fileNb=0; fileNb<nbFiles; fileNb++) {
            size_t remaining = fileSizes[fileNb];
            U32 const nbBlocksforThisFile = (U32)((remaining + (blockSize-1)) / blockSize);
            U32 const blockEnd = nbBlocks + nbBlocksforThisFile;
            for ( ; nbBlocks<blockEnd; nbBlocks++) {
                size_t const thisBlockSize = MIN(remaining, blockSize);
                blockTable[nbBlocks].srcPtr = srcPtr;
                blockTable[nbBlocks].cPtr = cPtr;
                blockTable[nbBlocks].resPtr = resPtr;
                blockTable[nbBlocks].srcSize = thisBlockSize;
                blockTable[nbBlocks].cRoom = (size_t)LZ4_compressBound((int)thisBlockSize);
                srcPtr += thisBlockSize;
                cPtr += blockTable[nbBlocks].cRoom;
                resPtr += thisBlockSize;
                remaining -= thisBlockSize;
    }   }   }

    /* warmimg up memory */
    RDG_genBuffer(compressedBuffer, maxCompressedSize, 0.10, 0.50, 1);

    /* Bench */
    {   U64 fastestC = (U64)(-1LL), fastestD = (U64)(-1LL);
        U64 const crcOrig = XXH64(srcBuffer, srcSize, 0);
        UTIL_time_t coolTime;
        U64 const maxTime = (g_nbSeconds * TIMELOOP_NANOSEC) + 100;
        U32 nbCompressionLoops = (U32)((5 MB) / (srcSize+1)) + 1;  /* conservative initial compression speed estimate */
        U32 nbDecodeLoops = (U32)((200 MB) / (srcSize+1)) + 1;  /* conservative initial decode speed estimate */
        U64 totalCTime=0, totalDTime=0;
        U32 cCompleted=0, dCompleted=0;
#       define NB_MARKS 4
        const char* const marks[NB_MARKS] = { " |", " /", " =",  "\\" };
        U32 markNb = 0;
        size_t cSize = 0;
        double ratio = 0.;

        coolTime = UTIL_getTime();
        DISPLAYLEVEL(2, "\r%79s\r", "");
        while (!cCompleted || !dCompleted) {
            /* overheat protection */
            if (UTIL_clockSpanMicro(coolTime) > ACTIVEPERIOD_MICROSEC) {
                DISPLAYLEVEL(2, "\rcooling down ...    \r");
                UTIL_sleep(COOLPERIOD_SEC);
                coolTime = UTIL_getTime();
            }

            /* Compression */
            DISPLAYLEVEL(2, "%2s-%-17.17s :%10u ->\r", marks[markNb], displayName, (U32)srcSize);
            if (!cCompleted) memset(compressedBuffer, 0xE5, maxCompressedSize);  /* warm up and erase result buffer */

            UTIL_sleepMilli(1);  /* give processor time to other processes */
            UTIL_waitForNextTick();

            if (!cCompleted) {   /* still some time to do compression tests */
                UTIL_time_t const clockStart = UTIL_getTime();
                U32 nbLoops;
                for (nbLoops=0; nbLoops < nbCompressionLoops; nbLoops++) {
                    U32 blockNb;
                    for (blockNb=0; blockNb<nbBlocks; blockNb++) {
                        size_t const rSize = (size_t)compP.compressionFunction(blockTable[blockNb].srcPtr, blockTable[blockNb].cPtr, (int)blockTable[blockNb].srcSize, (int)blockTable[blockNb].cRoom, cLevel);
                        if (LZ4_isError(rSize)) EXM_THROW(1, "LZ4 compression failed");
                        blockTable[blockNb].cSize = rSize;
                }   }
                {   U64 const clockSpan = UTIL_clockSpanNano(clockStart);
                    if (clockSpan > 0) {
                        if (clockSpan < fastestC * nbCompressionLoops)
                            fastestC = clockSpan / nbCompressionLoops;
                        assert(fastestC > 0);
                        nbCompressionLoops = (U32)(TIMELOOP_NANOSEC / fastestC) + 1;  /* aim for ~1sec */
                    } else {
                        assert(nbCompressionLoops < 40000000);   /* avoid overflow */
                        nbCompressionLoops *= 100;
                    }
                    totalCTime += clockSpan;
                    cCompleted = totalCTime>maxTime;
            }   }

            cSize = 0;
            { U32 blockNb; for (blockNb=0; blockNb<nbBlocks; blockNb++) cSize += blockTable[blockNb].cSize; }
            cSize += !cSize;  /* avoid div by 0 */
            ratio = (double)srcSize / (double)cSize;
            markNb = (markNb+1) % NB_MARKS;
            DISPLAYLEVEL(2, "%2s-%-17.17s :%10u ->%10u (%5.3f),%6.1f MB/s\r",
                    marks[markNb], displayName, (U32)srcSize, (U32)cSize, ratio,
                    ((double)srcSize / fastestC) * 1000 );

            (void)fastestD; (void)crcOrig;   /*  unused when decompression disabled */
#if 1
            /* Decompression */
            if (!dCompleted) memset(resultBuffer, 0xD6, srcSize);  /* warm result buffer */

            UTIL_sleepMilli(5); /* give processor time to other processes */
            UTIL_waitForNextTick();

            if (!dCompleted) {
                UTIL_time_t const clockStart = UTIL_getTime();
                U32 nbLoops;
                for (nbLoops=0; nbLoops < nbDecodeLoops; nbLoops++) {
                    U32 blockNb;
                    for (blockNb=0; blockNb<nbBlocks; blockNb++) {
                        int const regenSize = LZ4_decompress_safe(blockTable[blockNb].cPtr, blockTable[blockNb].resPtr, (int)blockTable[blockNb].cSize, (int)blockTable[blockNb].srcSize);
                        if (regenSize < 0) {
                            DISPLAY("LZ4_decompress_safe() failed on block %u \n", blockNb);
                            break;
                        }
                        blockTable[blockNb].resSize = (size_t)regenSize;
                }   }
                {   U64 const clockSpan = UTIL_clockSpanNano(clockStart);
                    if (clockSpan > 0) {
                        if (clockSpan < fastestD * nbDecodeLoops)
                            fastestD = clockSpan / nbDecodeLoops;
                        assert(fastestD > 0);
                        nbDecodeLoops = (U32)(TIMELOOP_NANOSEC / fastestD) + 1;  /* aim for ~1sec */
                    } else {
                        assert(nbDecodeLoops < 40000000);   /* avoid overflow */
                        nbDecodeLoops *= 100;
                    }
                    totalDTime += clockSpan;
                    dCompleted = totalDTime > (DECOMP_MULT*maxTime);
            }   }

            markNb = (markNb+1) % NB_MARKS;
            DISPLAYLEVEL(2, "%2s-%-17.17s :%10u ->%10u (%5.3f),%6.1f MB/s ,%6.1f MB/s\r",
                    marks[markNb], displayName, (U32)srcSize, (U32)cSize, ratio,
                    ((double)srcSize / fastestC) * 1000,
                    ((double)srcSize / fastestD) * 1000);

            /* CRC Checking */
            {   U64 const crcCheck = XXH64(resultBuffer, srcSize, 0);
                if (crcOrig!=crcCheck) {
                    size_t u;
                    DISPLAY("\n!!! WARNING !!! %17s : Invalid Checksum : %x != %x   \n", displayName, (unsigned)crcOrig, (unsigned)crcCheck);
                    for (u=0; u<srcSize; u++) {
                        if (((const BYTE*)srcBuffer)[u] != ((const BYTE*)resultBuffer)[u]) {
                            U32 segNb, bNb, pos;
                            size_t bacc = 0;
                            DISPLAY("Decoding error at pos %u ", (U32)u);
                            for (segNb = 0; segNb < nbBlocks; segNb++) {
                                if (bacc + blockTable[segNb].srcSize > u) break;
                                bacc += blockTable[segNb].srcSize;
                            }
                            pos = (U32)(u - bacc);
                            bNb = pos / (128 KB);
                            DISPLAY("(block %u, sub %u, pos %u) \n", segNb, bNb, pos);
                            break;
                        }
                        if (u==srcSize-1) {  /* should never happen */
                            DISPLAY("no difference detected\n");
                    }   }
                    break;
            }   }   /* CRC Checking */
#endif
        }   /* for (testNb = 1; testNb <= (g_nbSeconds + !g_nbSeconds); testNb++) */

        if (g_displayLevel == 1) {
            double const cSpeed = ((double)srcSize / fastestC) * 1000;
            double const dSpeed = ((double)srcSize / fastestD) * 1000;
            if (g_additionalParam)
                DISPLAY("-%-3i%11i (%5.3f) %6.2f MB/s %6.1f MB/s  %s (param=%d)\n", cLevel, (int)cSize, ratio, cSpeed, dSpeed, displayName, g_additionalParam);
            else
                DISPLAY("-%-3i%11i (%5.3f) %6.2f MB/s %6.1f MB/s  %s\n", cLevel, (int)cSize, ratio, cSpeed, dSpeed, displayName);
        }
        DISPLAYLEVEL(2, "%2i#\n", cLevel);
    }   /* Bench */

    /* clean up */
    free(blockTable);
    free(compressedBuffer);
    free(resultBuffer);
    return 0;
}


static size_t BMK_findMaxMem(U64 requiredMem)
{
    size_t step = 64 MB;
    BYTE* testmem=NULL;

    requiredMem = (((requiredMem >> 26) + 1) << 26);
    requiredMem += 2*step;
    if (requiredMem > maxMemory) requiredMem = maxMemory;

    while (!testmem) {
        if (requiredMem > step) requiredMem -= step;
        else requiredMem >>= 1;
        testmem = (BYTE*) malloc ((size_t)requiredMem);
    }
    free (testmem);

    /* keep some space available */
    if (requiredMem > step) requiredMem -= step;
    else requiredMem >>= 1;

    return (size_t)requiredMem;
}


static void BMK_benchCLevel(void* srcBuffer, size_t benchedSize,
                            const char* displayName, int cLevel, int cLevelLast,
                            const size_t* fileSizes, unsigned nbFiles)
{
    int l;

    const char* pch = strrchr(displayName, '\\'); /* Windows */
    if (!pch) pch = strrchr(displayName, '/'); /* Linux */
    if (pch) displayName = pch+1;

    SET_REALTIME_PRIORITY;

    if (g_displayLevel == 1 && !g_additionalParam)
        DISPLAY("bench %s %s: input %u bytes, %u seconds, %u KB blocks\n", LZ4_VERSION_STRING, LZ4_GIT_COMMIT_STRING, (U32)benchedSize, g_nbSeconds, (U32)(g_blockSize>>10));

    if (cLevelLast < cLevel) cLevelLast = cLevel;

    for (l=cLevel; l <= cLevelLast; l++) {
        BMK_benchMem(srcBuffer, benchedSize,
                     displayName, l,
                     fileSizes, nbFiles);
    }
}


/*! BMK_loadFiles() :
    Loads `buffer` with content of files listed within `fileNamesTable`.
    At most, fills `buffer` entirely */
static void BMK_loadFiles(void* buffer, size_t bufferSize,
                          size_t* fileSizes,
                          const char** fileNamesTable, unsigned nbFiles)
{
    size_t pos = 0, totalSize = 0;
    unsigned n;
    for (n=0; n<nbFiles; n++) {
        FILE* f;
        U64 fileSize = UTIL_getFileSize(fileNamesTable[n]);
        if (UTIL_isDirectory(fileNamesTable[n])) {
            DISPLAYLEVEL(2, "Ignoring %s directory...       \n", fileNamesTable[n]);
            fileSizes[n] = 0;
            continue;
        }
        f = fopen(fileNamesTable[n], "rb");
        if (f==NULL) EXM_THROW(10, "impossible to open file %s", fileNamesTable[n]);
        DISPLAYUPDATE(2, "Loading %s...       \r", fileNamesTable[n]);
        if (fileSize > bufferSize-pos) { /* buffer too small - stop after this file */
            fileSize = bufferSize-pos;
            nbFiles=n;
        }
        { size_t const readSize = fread(((char*)buffer)+pos, 1, (size_t)fileSize, f);
          if (readSize != (size_t)fileSize) EXM_THROW(11, "could not read %s", fileNamesTable[n]);
          pos += readSize; }
        fileSizes[n] = (size_t)fileSize;
        totalSize += (size_t)fileSize;
        fclose(f);
    }

    if (totalSize == 0) EXM_THROW(12, "no data to bench");
}

static void BMK_benchFileTable(const char** fileNamesTable, unsigned nbFiles,
                               int cLevel, int cLevelLast)
{
    void* srcBuffer;
    size_t benchedSize;
    size_t* fileSizes = (size_t*)malloc(nbFiles * sizeof(size_t));
    U64 const totalSizeToLoad = UTIL_getTotalFileSize(fileNamesTable, nbFiles);
    char mfName[20] = {0};

    if (!fileSizes) EXM_THROW(12, "not enough memory for fileSizes");

    /* Memory allocation & restrictions */
    benchedSize = BMK_findMaxMem(totalSizeToLoad * 3) / 3;
    if (benchedSize==0) EXM_THROW(12, "not enough memory");
    if ((U64)benchedSize > totalSizeToLoad) benchedSize = (size_t)totalSizeToLoad;
    if (benchedSize > LZ4_MAX_INPUT_SIZE) {
        benchedSize = LZ4_MAX_INPUT_SIZE;
        DISPLAY("File(s) bigger than LZ4's max input size; testing %u MB only...\n", (U32)(benchedSize >> 20));
    } else {
        if (benchedSize < totalSizeToLoad)
            DISPLAY("Not enough memory; testing %u MB only...\n", (U32)(benchedSize >> 20));
    }
    srcBuffer = malloc(benchedSize + !benchedSize);   /* avoid alloc of zero */
    if (!srcBuffer) EXM_THROW(12, "not enough memory");

    /* Load input buffer */
    BMK_loadFiles(srcBuffer, benchedSize, fileSizes, fileNamesTable, nbFiles);

    /* Bench */
    snprintf (mfName, sizeof(mfName), " %u files", nbFiles);
    {   const char* displayName = (nbFiles > 1) ? mfName : fileNamesTable[0];
        BMK_benchCLevel(srcBuffer, benchedSize,
                        displayName, cLevel, cLevelLast,
                        fileSizes, nbFiles);
    }

    /* clean up */
    free(srcBuffer);
    free(fileSizes);
}


static void BMK_syntheticTest(int cLevel, int cLevelLast, double compressibility)
{
    char name[20] = {0};
    size_t benchedSize = 10000000;
    void* const srcBuffer = malloc(benchedSize);

    /* Memory allocation */
    if (!srcBuffer) EXM_THROW(21, "not enough memory");

    /* Fill input buffer */
    RDG_genBuffer(srcBuffer, benchedSize, compressibility, 0.0, 0);

    /* Bench */
    snprintf (name, sizeof(name), "Synthetic %2u%%", (unsigned)(compressibility*100));
    BMK_benchCLevel(srcBuffer, benchedSize, name, cLevel, cLevelLast, &benchedSize, 1);

    /* clean up */
    free(srcBuffer);
}


int BMK_benchFilesSeparately(const char** fileNamesTable, unsigned nbFiles,
                   int cLevel, int cLevelLast)
{
    unsigned fileNb;
    if (cLevel > LZ4HC_CLEVEL_MAX) cLevel = LZ4HC_CLEVEL_MAX;
    if (cLevelLast > LZ4HC_CLEVEL_MAX) cLevelLast = LZ4HC_CLEVEL_MAX;
    if (cLevelLast < cLevel) cLevelLast = cLevel;
    if (cLevelLast > cLevel) DISPLAYLEVEL(2, "Benchmarking levels from %d to %d\n", cLevel, cLevelLast);

    for (fileNb=0; fileNb<nbFiles; fileNb++)
        BMK_benchFileTable(fileNamesTable+fileNb, 1, cLevel, cLevelLast);

    return 0;
}


int BMK_benchFiles(const char** fileNamesTable, unsigned nbFiles,
                   int cLevel, int cLevelLast)
{
    double const compressibility = (double)g_compressibilityDefault / 100;

    if (cLevel > LZ4HC_CLEVEL_MAX) cLevel = LZ4HC_CLEVEL_MAX;
    if (cLevelLast > LZ4HC_CLEVEL_MAX) cLevelLast = LZ4HC_CLEVEL_MAX;
    if (cLevelLast < cLevel) cLevelLast = cLevel;
    if (cLevelLast > cLevel) DISPLAYLEVEL(2, "Benchmarking levels from %d to %d\n", cLevel, cLevelLast);

    if (nbFiles == 0)
        BMK_syntheticTest(cLevel, cLevelLast, compressibility);
    else {
        if (g_benchSeparately)
            BMK_benchFilesSeparately(fileNamesTable, nbFiles, cLevel, cLevelLast);
        else
            BMK_benchFileTable(fileNamesTable, nbFiles, cLevel, cLevelLast);
    }
    return 0;
}
