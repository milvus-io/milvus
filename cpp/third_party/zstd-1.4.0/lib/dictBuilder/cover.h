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

/**
 * COVER_best_t is used for two purposes:
 * 1. Synchronizing threads.
 * 2. Saving the best parameters and dictionary.
 *
 * All of the methods except COVER_best_init() are thread safe if zstd is
 * compiled with multithreaded support.
 */
typedef struct COVER_best_s {
  ZSTD_pthread_mutex_t mutex;
  ZSTD_pthread_cond_t cond;
  size_t liveJobs;
  void *dict;
  size_t dictSize;
  ZDICT_cover_params_t parameters;
  size_t compressedSize;
} COVER_best_t;

/**
 * A segment is a range in the source as well as the score of the segment.
 */
typedef struct {
  U32 begin;
  U32 end;
  U32 score;
} COVER_segment_t;

/**
 *Number of epochs and size of each epoch.
 */
typedef struct {
  U32 num;
  U32 size;
} COVER_epoch_info_t;

/**
 * Computes the number of epochs and the size of each epoch.
 * We will make sure that each epoch gets at least 10 * k bytes.
 *
 * The COVER algorithms divide the data up into epochs of equal size and
 * select one segment from each epoch.
 *
 * @param maxDictSize The maximum allowed dictionary size.
 * @param nbDmers     The number of dmers we are training on.
 * @param k           The parameter k (segment size).
 * @param passes      The target number of passes over the dmer corpus.
 *                    More passes means a better dictionary.
 */
COVER_epoch_info_t COVER_computeEpochs(U32 maxDictSize, U32 nbDmers,
                                       U32 k, U32 passes);

/**
 * Warns the user when their corpus is too small.
 */
void COVER_warnOnSmallCorpus(size_t maxDictSize, size_t nbDmers, int displayLevel);

/**
 *  Checks total compressed size of a dictionary
 */
size_t COVER_checkTotalCompressedSize(const ZDICT_cover_params_t parameters,
                                      const size_t *samplesSizes, const BYTE *samples,
                                      size_t *offsets,
                                      size_t nbTrainSamples, size_t nbSamples,
                                      BYTE *const dict, size_t dictBufferCapacity);

/**
 * Returns the sum of the sample sizes.
 */
size_t COVER_sum(const size_t *samplesSizes, unsigned nbSamples) ;

/**
 * Initialize the `COVER_best_t`.
 */
void COVER_best_init(COVER_best_t *best);

/**
 * Wait until liveJobs == 0.
 */
void COVER_best_wait(COVER_best_t *best);

/**
 * Call COVER_best_wait() and then destroy the COVER_best_t.
 */
void COVER_best_destroy(COVER_best_t *best);

/**
 * Called when a thread is about to be launched.
 * Increments liveJobs.
 */
void COVER_best_start(COVER_best_t *best);

/**
 * Called when a thread finishes executing, both on error or success.
 * Decrements liveJobs and signals any waiting threads if liveJobs == 0.
 * If this dictionary is the best so far save it and its parameters.
 */
void COVER_best_finish(COVER_best_t *best, size_t compressedSize,
                       ZDICT_cover_params_t parameters, void *dict,
                       size_t dictSize);
