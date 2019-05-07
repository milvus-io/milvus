  /*
      checkFrame - verify frame headers
      Copyright (C) Yann Collet 2014-present

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
  #  pragma warning(disable : 4127)        /* disable: C4127: conditional expression is constant */
  #  pragma warning(disable : 4146)        /* disable: C4146: minus unsigned expression */
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


  /*-************************************
  *  Constants
  **************************************/
  #define KB *(1U<<10)
  #define MB *(1U<<20)
  #define GB *(1U<<30)


  /*-************************************
  *  Macros
  **************************************/
  #define DISPLAY(...)          fprintf(stderr, __VA_ARGS__)
  #define DISPLAYLEVEL(l, ...)  if (displayLevel>=l) { DISPLAY(__VA_ARGS__); }

  /**************************************
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
    DISPLAYLEVEL(1, " \n");                                               \
    return(error);                                                          \
}



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

typedef struct {
    void*  srcBuffer;
    size_t srcBufferSize;
    void*  dstBuffer;
    size_t dstBufferSize;
    LZ4F_decompressionContext_t ctx;
} cRess_t;

static int createCResources(cRess_t* ress)
{
    ress->srcBufferSize = 4 MB;
    ress->srcBuffer = malloc(ress->srcBufferSize);
    ress->dstBufferSize = 4 MB;
    ress->dstBuffer = malloc(ress->dstBufferSize);

    if (!ress->srcBuffer || !ress->dstBuffer) {
        free(ress->srcBuffer);
        free(ress->dstBuffer);
        EXM_THROW(20, "Allocation error : not enough memory");
    }

    if (LZ4F_isError( LZ4F_createDecompressionContext(&(ress->ctx), LZ4F_VERSION) )) {
        free(ress->srcBuffer);
        free(ress->dstBuffer);
        EXM_THROW(21, "Unable to create decompression context");
    }
    return 0;
}

static void freeCResources(cRess_t ress)
{
    free(ress.srcBuffer);
    free(ress.dstBuffer);

    (void) LZ4F_freeDecompressionContext(ress.ctx);
}

int frameCheck(cRess_t ress, FILE* const srcFile, unsigned bsid, size_t blockSize)
{
    LZ4F_errorCode_t nextToLoad = 0;
    size_t curblocksize = 0;
    int partialBlock = 0;

    /* Main Loop */
    for (;;) {
        size_t readSize;
        size_t pos = 0;
        size_t decodedBytes = ress.dstBufferSize;
        size_t remaining;
        LZ4F_frameInfo_t frameInfo;

        /* Read input */
        readSize = fread(ress.srcBuffer, 1, ress.srcBufferSize, srcFile);
        if (!readSize) break;   /* reached end of file or stream */

        while (pos < readSize) {  /* still to read */
            /* Decode Input (at least partially) */
            if (!nextToLoad) {
                /* LZ4F_decompress returned 0 : starting new frame */
                curblocksize = 0;
                remaining = readSize - pos;
                nextToLoad = LZ4F_getFrameInfo(ress.ctx, &frameInfo, (char*)(ress.srcBuffer)+pos, &remaining);
                if (LZ4F_isError(nextToLoad))
                    EXM_THROW(22, "Error getting frame info: %s",
                                LZ4F_getErrorName(nextToLoad));
                if (frameInfo.blockSizeID != bsid)
                    EXM_THROW(23, "Block size ID %u != expected %u",
                                frameInfo.blockSizeID, bsid);
                pos += remaining;
                /* nextToLoad should be block header size */
                remaining = nextToLoad;
                decodedBytes = ress.dstBufferSize;
                nextToLoad = LZ4F_decompress(ress.ctx, ress.dstBuffer, &decodedBytes, (char*)(ress.srcBuffer)+pos, &remaining, NULL);
                if (LZ4F_isError(nextToLoad)) EXM_THROW(24, "Decompression error : %s", LZ4F_getErrorName(nextToLoad));
                pos += remaining;
            }
            decodedBytes = ress.dstBufferSize;
            /* nextToLoad should be just enough to cover the next block */
            if (nextToLoad > (readSize - pos)) {
                /* block is not fully contained in current buffer */
                partialBlock = 1;
                remaining = readSize - pos;
            } else {
                if (partialBlock) {
                    partialBlock = 0;
                }
                remaining = nextToLoad;
            }
            nextToLoad = LZ4F_decompress(ress.ctx, ress.dstBuffer, &decodedBytes, (char*)(ress.srcBuffer)+pos, &remaining, NULL);
            if (LZ4F_isError(nextToLoad)) EXM_THROW(24, "Decompression error : %s", LZ4F_getErrorName(nextToLoad));
            curblocksize += decodedBytes;
            pos += remaining;
            if (!partialBlock) {
                /* detect small block due to end of frame; the final 4-byte frame checksum could be left in the buffer */
                if ((curblocksize != 0) && (nextToLoad > 4)) {
                    if (curblocksize != blockSize)
                        EXM_THROW(25, "Block size %u != expected %u, pos %u\n",
                                    (unsigned)curblocksize, (unsigned)blockSize, (unsigned)pos);
                }
                curblocksize = 0;
            }
        }
    }
    /* can be out because readSize == 0, which could be an fread() error */
    if (ferror(srcFile)) EXM_THROW(26, "Read error");

    if (nextToLoad!=0) EXM_THROW(27, "Unfinished stream");

    return 0;
}

int FUZ_usage(const char* programName)
{
    DISPLAY( "Usage :\n");
    DISPLAY( "      %s [args] filename\n", programName);
    DISPLAY( "\n");
    DISPLAY( "Arguments :\n");
    DISPLAY( " -b#    : expected blocksizeID [4-7] (required)\n");
    DISPLAY( " -B#    : expected blocksize [32-4194304] (required)\n");
    DISPLAY( " -v     : verbose\n");
    DISPLAY( " -h     : display help and exit\n");
    return 0;
}


int main(int argc, const char** argv)
{
    int argNb;
    unsigned bsid=0;
    size_t blockSize=0;
    const char* const programName = argv[0];

    /* Check command line */
    for (argNb=1; argNb<argc; argNb++) {
        const char* argument = argv[argNb];

        if(!argument) continue;   /* Protection if argument empty */

        /* Decode command (note : aggregated short commands are allowed) */
        if (argument[0]=='-') {
            if (!strcmp(argument, "--no-prompt")) {
                no_prompt=1;
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

                case 'b':
                    argument++;
                    bsid=0;
                    while ((*argument>='0') && (*argument<='9')) {
                        bsid *= 10;
                        bsid += (unsigned)(*argument - '0');
                        argument++;
                    }
                    break;

                case 'B':
                    argument++;
                    blockSize=0;
                    while ((*argument>='0') && (*argument<='9')) {
                        blockSize *= 10;
                        blockSize += (size_t)(*argument - '0');
                        argument++;
                    }
                    break;

                default:
                    ;
                    return FUZ_usage(programName);
                }
            }
        } else {
            int err;
            FILE *srcFile;
            cRess_t ress;
            if (bsid == 0 || blockSize == 0)
              return FUZ_usage(programName);
            DISPLAY("Starting frame checker (%i-bits, %s)\n", (int)(sizeof(size_t)*8), LZ4_VERSION_STRING);
            err = createCResources(&ress);
            if (err) return (err);
            srcFile = fopen(argument, "rb");
            if ( srcFile==NULL ) {
                freeCResources(ress);
                EXM_THROW(1, "%s: %s \n", argument, strerror(errno));
            }
            err = frameCheck(ress, srcFile, bsid, blockSize);
            freeCResources(ress);
            fclose(srcFile);
            return (err);
        }
    }
    return 0;
}
