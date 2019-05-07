#include <stdio.h>  /* fprintf */
#include <stdlib.h> /* malloc, free, qsort */
#include <string.h> /* memset */
#include <time.h>   /* clock */
#include "mem.h" /* read */
#include "pool.h"
#include "threading.h"
#include "zstd_internal.h" /* includes zstd.h */
#ifndef ZDICT_STATIC_LINKING_ONLY
#define ZDICT_STATIC_LINKING_ONLY
#endif
#include "zdict.h"


typedef struct {
    unsigned k;                  /* Segment size : constraint: 0 < k : Reasonable range [16, 2048+] */
    unsigned d;                  /* dmer size : constraint: 0 < d <= k : Reasonable range [6, 16] */
    unsigned f;                  /* log of size of frequency array */
    unsigned steps;              /* Number of steps : Only used for optimization : 0 means default (32) : Higher means more parameters checked */
    unsigned nbThreads;          /* Number of threads : constraint: 0 < nbThreads : 1 means single-threaded : Only used for optimization : Ignored if ZSTD_MULTITHREAD is not defined */
    double splitPoint;           /* Percentage of samples used for training: the first nbSamples * splitPoint samples will be used to training, the last nbSamples * (1 - splitPoint) samples will be used for testing, 0 means default (1.0), 1.0 when all samples are used for both training and testing */
    ZDICT_params_t zParams;
} ZDICT_fastCover_params_t;


/*! ZDICT_optimizeTrainFromBuffer_fastCover():
 *  Train a dictionary from an array of samples using a modified version of the COVER algorithm.
 *  Samples must be stored concatenated in a single flat buffer `samplesBuffer`,
 *  supplied with an array of sizes `samplesSizes`, providing the size of each sample, in order.
 *  The resulting dictionary will be saved into `dictBuffer`.
 *  All of the parameters except for f are optional.
 *  If d is non-zero then we don't check multiple values of d, otherwise we check d = {6, 8, 10, 12, 14, 16}.
 *  if steps is zero it defaults to its default value.
 *  If k is non-zero then we don't check multiple values of k, otherwise we check steps values in [16, 2048].
 *
 *  @return: size of dictionary stored into `dictBuffer` (<= `dictBufferCapacity`)
 *           or an error code, which can be tested with ZDICT_isError().
 *           On success `*parameters` contains the parameters selected.
 */
 ZDICTLIB_API size_t ZDICT_optimizeTrainFromBuffer_fastCover(
     void *dictBuffer, size_t dictBufferCapacity, const void *samplesBuffer,
     const size_t *samplesSizes, unsigned nbSamples,
     ZDICT_fastCover_params_t *parameters);


/*! ZDICT_trainFromBuffer_fastCover():
 *  Train a dictionary from an array of samples using a modified version of the COVER algorithm.
 *  Samples must be stored concatenated in a single flat buffer `samplesBuffer`,
 *  supplied with an array of sizes `samplesSizes`, providing the size of each sample, in order.
 *  The resulting dictionary will be saved into `dictBuffer`.
 *  d, k, and f are required.
 *  @return: size of dictionary stored into `dictBuffer` (<= `dictBufferCapacity`)
 *           or an error code, which can be tested with ZDICT_isError().
 */
ZDICTLIB_API size_t ZDICT_trainFromBuffer_fastCover(
    void *dictBuffer, size_t dictBufferCapacity, const void *samplesBuffer,
    const size_t *samplesSizes, unsigned nbSamples, ZDICT_fastCover_params_t parameters);
