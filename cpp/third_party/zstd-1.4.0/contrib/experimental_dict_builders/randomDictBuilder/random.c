/*-*************************************
*  Dependencies
***************************************/
#include <stdio.h>            /* fprintf */
#include <stdlib.h>           /* malloc, free, qsort */
#include <string.h>           /* memset */
#include <time.h>             /* clock */
#include "random.h"
#include "util.h"             /* UTIL_getFileSize, UTIL_getTotalFileSize */
#ifndef ZDICT_STATIC_LINKING_ONLY
#define ZDICT_STATIC_LINKING_ONLY
#endif
#include "zdict.h"

/*-*************************************
*  Console display
***************************************/
#define DISPLAY(...)         fprintf(stderr, __VA_ARGS__)
#define DISPLAYLEVEL(l, ...) if (displayLevel>=l) { DISPLAY(__VA_ARGS__); }

#define LOCALDISPLAYUPDATE(displayLevel, l, ...)                               \
  if (displayLevel >= l) {                                                     \
    if ((clock() - g_time > refreshRate) || (displayLevel >= 4)) {             \
      g_time = clock();                                                        \
      DISPLAY(__VA_ARGS__);                                                    \
    }                                                                          \
  }
#define DISPLAYUPDATE(l, ...) LOCALDISPLAYUPDATE(displayLevel, l, __VA_ARGS__)
static const clock_t refreshRate = CLOCKS_PER_SEC * 15 / 100;
static clock_t g_time = 0;



/* ********************************************************
*  Random Dictionary Builder
**********************************************************/
/**
 * Returns the sum of the sample sizes.
 */
static size_t RANDOM_sum(const size_t *samplesSizes, unsigned nbSamples) {
  size_t sum = 0;
  unsigned i;
  for (i = 0; i < nbSamples; ++i) {
    sum += samplesSizes[i];
  }
  return sum;
}


/**
 * A segment is an inclusive range in the source.
 */
typedef struct {
  U32 begin;
  U32 end;
} RANDOM_segment_t;


/**
 * Selects a random segment from totalSamplesSize - k + 1 possible segments
 */
static RANDOM_segment_t RANDOM_selectSegment(const size_t totalSamplesSize,
                                            ZDICT_random_params_t parameters) {
    const U32 k = parameters.k;
    RANDOM_segment_t segment;
    unsigned index;

    /* Randomly generate a number from 0 to sampleSizes - k */
    index = rand()%(totalSamplesSize - k + 1);

    /* inclusive */
    segment.begin = index;
    segment.end = index + k - 1;

    return segment;
}


/**
 * Check the validity of the parameters.
 * Returns non-zero if the parameters are valid and 0 otherwise.
 */
static int RANDOM_checkParameters(ZDICT_random_params_t parameters,
                                  size_t maxDictSize) {
    /* k is a required parameter */
    if (parameters.k == 0) {
      return 0;
    }
    /* k <= maxDictSize */
    if (parameters.k > maxDictSize) {
      return 0;
    }
    return 1;
}


/**
 * Given the prepared context build the dictionary.
 */
static size_t RANDOM_buildDictionary(const size_t totalSamplesSize, const BYTE *samples,
                                    void *dictBuffer, size_t dictBufferCapacity,
                                    ZDICT_random_params_t parameters) {
    BYTE *const dict = (BYTE *)dictBuffer;
    size_t tail = dictBufferCapacity;
    const int displayLevel = parameters.zParams.notificationLevel;
    while (tail > 0) {

      /* Select a segment */
      RANDOM_segment_t segment = RANDOM_selectSegment(totalSamplesSize, parameters);

      size_t segmentSize;
      segmentSize = MIN(segment.end - segment.begin + 1, tail);

      tail -= segmentSize;
      memcpy(dict + tail, samples + segment.begin, segmentSize);
      DISPLAYUPDATE(
          2, "\r%u%%       ",
          (U32)(((dictBufferCapacity - tail) * 100) / dictBufferCapacity));
    }

    return tail;
}




ZDICTLIB_API size_t ZDICT_trainFromBuffer_random(
    void *dictBuffer, size_t dictBufferCapacity,
    const void *samplesBuffer, const size_t *samplesSizes, unsigned nbSamples,
    ZDICT_random_params_t parameters) {
      const int displayLevel = parameters.zParams.notificationLevel;
      BYTE* const dict = (BYTE*)dictBuffer;
      /* Checks */
      if (!RANDOM_checkParameters(parameters, dictBufferCapacity)) {
          DISPLAYLEVEL(1, "k is incorrect\n");
          return ERROR(GENERIC);
      }
      if (nbSamples == 0) {
        DISPLAYLEVEL(1, "Random must have at least one input file\n");
        return ERROR(GENERIC);
      }
      if (dictBufferCapacity < ZDICT_DICTSIZE_MIN) {
        DISPLAYLEVEL(1, "dictBufferCapacity must be at least %u\n",
                     ZDICT_DICTSIZE_MIN);
        return ERROR(dstSize_tooSmall);
      }
      const size_t totalSamplesSize = RANDOM_sum(samplesSizes, nbSamples);
      const BYTE *const samples = (const BYTE *)samplesBuffer;

      DISPLAYLEVEL(2, "Building dictionary\n");
      {
        const size_t tail = RANDOM_buildDictionary(totalSamplesSize, samples,
                                  dictBuffer, dictBufferCapacity, parameters);
        const size_t dictSize = ZDICT_finalizeDictionary(
            dict, dictBufferCapacity, dict + tail, dictBufferCapacity - tail,
            samplesBuffer, samplesSizes, nbSamples, parameters.zParams);
        if (!ZSTD_isError(dictSize)) {
            DISPLAYLEVEL(2, "Constructed dictionary of size %u\n",
                          (U32)dictSize);
        }
        return dictSize;
      }
}
