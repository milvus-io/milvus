#include <stdio.h>  /* fprintf */
#include <stdlib.h> /* malloc, free, qsort */
#include <string.h>   /* strcmp, strlen */
#include <errno.h>    /* errno */
#include <ctype.h>
#include "zstd_internal.h" /* includes zstd.h */
#include "fileio.h"   /* stdinmark, stdoutmark, ZSTD_EXTENSION */
#include "platform.h"         /* Large Files support */
#include "util.h"
#include "zdict.h"


/*-*************************************
*  Structs
***************************************/
typedef struct {
    U64 totalSizeToLoad;
    unsigned oneSampleTooLarge;
    unsigned nbSamples;
} fileStats;

typedef struct {
  const void* srcBuffer;
  const size_t *samplesSizes;
  size_t nbSamples;
}sampleInfo;



/*! getSampleInfo():
 *  Load from input files and add samples to buffer
 * @return: a sampleInfo struct containing infomation about buffer where samples are stored,
 *          size of each sample, and total number of samples
 */
sampleInfo* getSampleInfo(const char** fileNamesTable, unsigned nbFiles, size_t chunkSize,
                          unsigned maxDictSize, const unsigned displayLevel);



/*! freeSampleInfo():
 *  Free memory allocated for info
 */
void freeSampleInfo(sampleInfo *info);



/*! saveDict():
 *  Save data stored on buff to dictFileName
 */
void saveDict(const char* dictFileName, const void* buff, size_t buffSize);


unsigned readU32FromChar(const char** stringPtr);

/** longCommandWArg() :
 *  check if *stringPtr is the same as longCommand.
 *  If yes, @return 1 and advances *stringPtr to the position which immediately follows longCommand.
 * @return 0 and doesn't modify *stringPtr otherwise.
 */
unsigned longCommandWArg(const char** stringPtr, const char* longCommand);
