#include <stdio.h>  /* fprintf */
#include <stdlib.h> /* malloc, free, qsort */
#include <string.h>   /* strcmp, strlen */
#include <errno.h>    /* errno */
#include <ctype.h>
#include "fastCover.h"
#include "io.h"
#include "util.h"
#include "zdict.h"


/*-*************************************
*  Console display
***************************************/
#define DISPLAY(...)         fprintf(stderr, __VA_ARGS__)
#define DISPLAYLEVEL(l, ...) if (displayLevel>=l) { DISPLAY(__VA_ARGS__); }

static const U64 g_refreshRate = SEC_TO_MICRO / 6;
static UTIL_time_t g_displayClock = UTIL_TIME_INITIALIZER;

#define DISPLAYUPDATE(l, ...) { if (displayLevel>=l) { \
            if ((UTIL_clockSpanMicro(g_displayClock) > g_refreshRate) || (displayLevel>=4)) \
            { g_displayClock = UTIL_getTime(); DISPLAY(__VA_ARGS__); \
            if (displayLevel>=4) fflush(stderr); } } }


/*-*************************************
*  Exceptions
***************************************/
#ifndef DEBUG
#  define DEBUG 0
#endif
#define DEBUGOUTPUT(...) if (DEBUG) DISPLAY(__VA_ARGS__);
#define EXM_THROW(error, ...)                                             \
{                                                                         \
    DEBUGOUTPUT("Error defined at %s, line %i : \n", __FILE__, __LINE__); \
    DISPLAY("Error %i : ", error);                                        \
    DISPLAY(__VA_ARGS__);                                                 \
    DISPLAY("\n");                                                        \
    exit(error);                                                          \
}


/*-*************************************
*  Constants
***************************************/
static const unsigned g_defaultMaxDictSize = 110 KB;
#define DEFAULT_CLEVEL 3


/*-*************************************
*  FASTCOVER
***************************************/
int FASTCOVER_trainFromFiles(const char* dictFileName, sampleInfo *info,
                          unsigned maxDictSize,
                          ZDICT_fastCover_params_t *params) {
    unsigned const displayLevel = params->zParams.notificationLevel;
    void* const dictBuffer = malloc(maxDictSize);

    int result = 0;

    /* Checks */
    if (!dictBuffer)
        EXM_THROW(12, "not enough memory for trainFromFiles");   /* should not happen */

    {   size_t dictSize;
        /* Run the optimize version if either k or d is not provided */
        if (!params->d || !params->k) {
          dictSize = ZDICT_optimizeTrainFromBuffer_fastCover(dictBuffer, maxDictSize, info->srcBuffer,
                                               info->samplesSizes, info->nbSamples, params);
        } else {
          dictSize = ZDICT_trainFromBuffer_fastCover(dictBuffer, maxDictSize, info->srcBuffer,
                                               info->samplesSizes, info->nbSamples, *params);
        }
        DISPLAYLEVEL(2, "k=%u\nd=%u\nf=%u\nsteps=%u\nsplit=%u\n", params->k, params->d, params->f, params->steps, (unsigned)(params->splitPoint*100));
        if (ZDICT_isError(dictSize)) {
            DISPLAYLEVEL(1, "dictionary training failed : %s \n", ZDICT_getErrorName(dictSize));   /* should not happen */
            result = 1;
            goto _done;
        }
        /* save dict */
        DISPLAYLEVEL(2, "Save dictionary of size %u into file %s \n", (U32)dictSize, dictFileName);
        saveDict(dictFileName, dictBuffer, dictSize);
    }

    /* clean up */
_done:
    free(dictBuffer);
    return result;
}



int main(int argCount, const char* argv[])
{
  int displayLevel = 2;
  const char* programName = argv[0];
  int operationResult = 0;

  /* Initialize arguments to default values */
  unsigned k = 0;
  unsigned d = 0;
  unsigned f = 23;
  unsigned steps = 32;
  unsigned nbThreads = 1;
  unsigned split = 100;
  const char* outputFile = "fastCoverDict";
  unsigned dictID = 0;
  unsigned maxDictSize = g_defaultMaxDictSize;

  /* Initialize table to store input files */
  const char** filenameTable = (const char**)malloc(argCount * sizeof(const char*));
  unsigned filenameIdx = 0;

  char* fileNamesBuf = NULL;
  unsigned fileNamesNb = filenameIdx;
  int followLinks = 0; /* follow directory recursively */
  const char** extendedFileList = NULL;

  /* Parse arguments */
  for (int i = 1; i < argCount; i++) {
    const char* argument = argv[i];
    if (longCommandWArg(&argument, "k=")) { k = readU32FromChar(&argument); continue; }
    if (longCommandWArg(&argument, "d=")) { d = readU32FromChar(&argument); continue; }
    if (longCommandWArg(&argument, "f=")) { f = readU32FromChar(&argument); continue; }
    if (longCommandWArg(&argument, "steps=")) { steps = readU32FromChar(&argument); continue; }
    if (longCommandWArg(&argument, "split=")) { split = readU32FromChar(&argument); continue; }
    if (longCommandWArg(&argument, "dictID=")) { dictID = readU32FromChar(&argument); continue; }
    if (longCommandWArg(&argument, "maxdict=")) { maxDictSize = readU32FromChar(&argument); continue; }
    if (longCommandWArg(&argument, "in=")) {
      filenameTable[filenameIdx] = argument;
      filenameIdx++;
      continue;
    }
    if (longCommandWArg(&argument, "out=")) {
      outputFile = argument;
      continue;
    }
    DISPLAYLEVEL(1, "Incorrect parameters\n");
    operationResult = 1;
    return operationResult;
  }

  /* Get the list of all files recursively (because followLinks==0)*/
  extendedFileList = UTIL_createFileList(filenameTable, filenameIdx, &fileNamesBuf,
                                        &fileNamesNb, followLinks);
  if (extendedFileList) {
      unsigned u;
      for (u=0; u<fileNamesNb; u++) DISPLAYLEVEL(4, "%u %s\n", u, extendedFileList[u]);
      free((void*)filenameTable);
      filenameTable = extendedFileList;
      filenameIdx = fileNamesNb;
  }

  size_t blockSize = 0;

  /* Set up zParams */
  ZDICT_params_t zParams;
  zParams.compressionLevel = DEFAULT_CLEVEL;
  zParams.notificationLevel = displayLevel;
  zParams.dictID = dictID;

  /* Set up fastCover params */
  ZDICT_fastCover_params_t params;
  params.zParams = zParams;
  params.k = k;
  params.d = d;
  params.f = f;
  params.steps = steps;
  params.nbThreads = nbThreads;
  params.splitPoint = (double)split/100;

  /* Build dictionary */
  sampleInfo* info = getSampleInfo(filenameTable,
                    filenameIdx, blockSize, maxDictSize, zParams.notificationLevel);
  operationResult = FASTCOVER_trainFromFiles(outputFile, info, maxDictSize, &params);

  /* Free allocated memory */
  UTIL_freeFileList(extendedFileList, fileNamesBuf);
  freeSampleInfo(info);

  return operationResult;
}
