#include <stdio.h>  /* fprintf */
#include <stdlib.h> /* malloc, free, qsort */
#include <string.h> /* memset */
#include <time.h>   /* clock */
#include "zstd_internal.h" /* includes zstd.h */
#ifndef ZDICT_STATIC_LINKING_ONLY
#define ZDICT_STATIC_LINKING_ONLY
#endif
#include "zdict.h"



typedef struct {
    unsigned k; /* Segment size : constraint: 0 < k : Reasonable range [16, 2048+]; Default to 200 */
    ZDICT_params_t zParams;
} ZDICT_random_params_t;


/*! ZDICT_trainFromBuffer_random():
 *  Train a dictionary from an array of samples.
 *  Samples must be stored concatenated in a single flat buffer `samplesBuffer`,
 *  supplied with an array of sizes `samplesSizes`, providing the size of each sample, in order.
 *  The resulting dictionary will be saved into `dictBuffer`.
 * @return: size of dictionary stored into `dictBuffer` (<= `dictBufferCapacity`)
 *          or an error code, which can be tested with ZDICT_isError().
 */
ZDICTLIB_API size_t ZDICT_trainFromBuffer_random( void *dictBuffer, size_t dictBufferCapacity,
    const void *samplesBuffer, const size_t *samplesSizes, unsigned nbSamples,
    ZDICT_random_params_t parameters);
