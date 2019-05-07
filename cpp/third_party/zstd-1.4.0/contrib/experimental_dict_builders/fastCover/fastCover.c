/*-*************************************
*  Dependencies
***************************************/
#include <stdio.h>  /* fprintf */
#include <stdlib.h> /* malloc, free, qsort */
#include <string.h> /* memset */
#include <time.h>   /* clock */
#include "mem.h" /* read */
#include "pool.h"
#include "threading.h"
#include "fastCover.h"
#include "zstd_internal.h" /* includes zstd.h */
#include "zdict.h"


/*-*************************************
*  Constants
***************************************/
#define FASTCOVER_MAX_SAMPLES_SIZE (sizeof(size_t) == 8 ? ((U32)-1) : ((U32)1 GB))
#define FASTCOVER_MAX_F 32
#define DEFAULT_SPLITPOINT 1.0

/*-*************************************
*  Console display
***************************************/
static int g_displayLevel = 2;
#define DISPLAY(...)                                                           \
  {                                                                            \
    fprintf(stderr, __VA_ARGS__);                                              \
    fflush(stderr);                                                            \
  }
#define LOCALDISPLAYLEVEL(displayLevel, l, ...)                                \
  if (displayLevel >= l) {                                                     \
    DISPLAY(__VA_ARGS__);                                                      \
  } /* 0 : no display;   1: errors;   2: default;  3: details;  4: debug */
#define DISPLAYLEVEL(l, ...) LOCALDISPLAYLEVEL(g_displayLevel, l, __VA_ARGS__)

#define LOCALDISPLAYUPDATE(displayLevel, l, ...)                               \
  if (displayLevel >= l) {                                                     \
    if ((clock() - g_time > refreshRate) || (displayLevel >= 4)) {             \
      g_time = clock();                                                        \
      DISPLAY(__VA_ARGS__);                                                    \
    }                                                                          \
  }
#define DISPLAYUPDATE(l, ...) LOCALDISPLAYUPDATE(g_displayLevel, l, __VA_ARGS__)
static const clock_t refreshRate = CLOCKS_PER_SEC * 15 / 100;
static clock_t g_time = 0;


/*-*************************************
* Hash Functions
***************************************/
static const U64 prime6bytes = 227718039650203ULL;
static size_t ZSTD_hash6(U64 u, U32 h) { return (size_t)(((u  << (64-48)) * prime6bytes) >> (64-h)) ; }
static size_t ZSTD_hash6Ptr(const void* p, U32 h) { return ZSTD_hash6(MEM_readLE64(p), h); }

static const U64 prime8bytes = 0xCF1BBCDCB7A56463ULL;
static size_t ZSTD_hash8(U64 u, U32 h) { return (size_t)(((u) * prime8bytes) >> (64-h)) ; }
static size_t ZSTD_hash8Ptr(const void* p, U32 h) { return ZSTD_hash8(MEM_readLE64(p), h); }


/**
 * Hash the d-byte value pointed to by p and mod 2^f
 */
static size_t FASTCOVER_hashPtrToIndex(const void* p, U32 h, unsigned d) {
  if (d == 6) {
    return ZSTD_hash6Ptr(p, h) & ((1 << h) - 1);
  }
  return ZSTD_hash8Ptr(p, h) & ((1 << h) - 1);
}


/*-*************************************
* Context
***************************************/
typedef struct {
  const BYTE *samples;
  size_t *offsets;
  const size_t *samplesSizes;
  size_t nbSamples;
  size_t nbTrainSamples;
  size_t nbTestSamples;
  size_t nbDmers;
  U32 *freqs;
  U16 *segmentFreqs;
  unsigned d;
} FASTCOVER_ctx_t;


/*-*************************************
*  Helper functions
***************************************/
/**
 * Returns the sum of the sample sizes.
 */
static size_t FASTCOVER_sum(const size_t *samplesSizes, unsigned nbSamples) {
  size_t sum = 0;
  unsigned i;
  for (i = 0; i < nbSamples; ++i) {
    sum += samplesSizes[i];
  }
  return sum;
}


/*-*************************************
*  fast functions
***************************************/
/**
 * A segment is a range in the source as well as the score of the segment.
 */
typedef struct {
  U32 begin;
  U32 end;
  U32 score;
} FASTCOVER_segment_t;


/**
 * Selects the best segment in an epoch.
 * Segments of are scored according to the function:
 *
 * Let F(d) be the frequency of all dmers with hash value d.
 * Let S_i be hash value of the dmer at position i of segment S which has length k.
 *
 *     Score(S) = F(S_1) + F(S_2) + ... + F(S_{k-d+1})
 *
 * Once the dmer with hash value d is in the dictionary we set F(d) = F(d)/2.
 */
static FASTCOVER_segment_t FASTCOVER_selectSegment(const FASTCOVER_ctx_t *ctx,
                                                  U32 *freqs, U32 begin,U32 end,
                                                  ZDICT_fastCover_params_t parameters) {
  /* Constants */
  const U32 k = parameters.k;
  const U32 d = parameters.d;
  const U32 dmersInK = k - d + 1;
  /* Try each segment (activeSegment) and save the best (bestSegment) */
  FASTCOVER_segment_t bestSegment = {0, 0, 0};
  FASTCOVER_segment_t activeSegment;
  /* Reset the activeDmers in the segment */
  /* The activeSegment starts at the beginning of the epoch. */
  activeSegment.begin = begin;
  activeSegment.end = begin;
  activeSegment.score = 0;
  {
    /* Slide the activeSegment through the whole epoch.
     * Save the best segment in bestSegment.
     */
    while (activeSegment.end < end) {
      /* Get hash value of current dmer */
      const size_t index = FASTCOVER_hashPtrToIndex(ctx->samples + activeSegment.end, parameters.f, ctx->d);
      /* Add frequency of this index to score if this is the first occurrence of index in active segment */
      if (ctx->segmentFreqs[index] == 0) {
        activeSegment.score += freqs[index];
      }
      ctx->segmentFreqs[index] += 1;
      /* Increment end of segment */
      activeSegment.end += 1;
      /* If the window is now too large, drop the first position */
      if (activeSegment.end - activeSegment.begin == dmersInK + 1) {
        /* Get hash value of the dmer to be eliminated from active segment */
        const size_t delIndex = FASTCOVER_hashPtrToIndex(ctx->samples + activeSegment.begin, parameters.f, ctx->d);
        ctx->segmentFreqs[delIndex] -= 1;
        /* Subtract frequency of this index from score if this is the last occurrence of this index in active segment */
        if (ctx->segmentFreqs[delIndex] == 0) {
          activeSegment.score -= freqs[delIndex];
        }
        /* Increment start of segment */
        activeSegment.begin += 1;
      }
      /* If this segment is the best so far save it */
      if (activeSegment.score > bestSegment.score) {
        bestSegment = activeSegment;
      }
    }
    /* Zero out rest of segmentFreqs array */
    while (activeSegment.begin < end) {
      const size_t delIndex = FASTCOVER_hashPtrToIndex(ctx->samples + activeSegment.begin, parameters.f, ctx->d);
      ctx->segmentFreqs[delIndex] -= 1;
      activeSegment.begin += 1;
    }
  }
  {
    /* Trim off the zero frequency head and tail from the segment. */
    U32 newBegin = bestSegment.end;
    U32 newEnd = bestSegment.begin;
    U32 pos;
    for (pos = bestSegment.begin; pos != bestSegment.end; ++pos) {
      const size_t index = FASTCOVER_hashPtrToIndex(ctx->samples + pos, parameters.f, ctx->d);
      U32 freq = freqs[index];
      if (freq != 0) {
        newBegin = MIN(newBegin, pos);
        newEnd = pos + 1;
      }
    }
    bestSegment.begin = newBegin;
    bestSegment.end = newEnd;
  }
  {
    /*  Zero the frequency of hash value of each dmer covered by the chosen segment. */
    U32 pos;
    for (pos = bestSegment.begin; pos != bestSegment.end; ++pos) {
      const size_t i = FASTCOVER_hashPtrToIndex(ctx->samples + pos, parameters.f, ctx->d);
      freqs[i] = 0;
    }
  }
  return bestSegment;
}

/**
 * Check the validity of the parameters.
 * Returns non-zero if the parameters are valid and 0 otherwise.
 */
static int FASTCOVER_checkParameters(ZDICT_fastCover_params_t parameters,
                                 size_t maxDictSize) {
  /* k, d, and f are required parameters */
  if (parameters.d == 0 || parameters.k == 0 || parameters.f == 0) {
    return 0;
  }
  /* d has to be 6 or 8 */
  if (parameters.d != 6 && parameters.d != 8) {
    return 0;
  }
  /* 0 < f <= FASTCOVER_MAX_F */
  if (parameters.f > FASTCOVER_MAX_F) {
    return 0;
  }
  /* k <= maxDictSize */
  if (parameters.k > maxDictSize) {
    return 0;
  }
  /* d <= k */
  if (parameters.d > parameters.k) {
    return 0;
  }
  /* 0 < splitPoint <= 1 */
  if (parameters.splitPoint <= 0 || parameters.splitPoint > 1) {
    return 0;
  }
  return 1;
}


/**
 * Clean up a context initialized with `FASTCOVER_ctx_init()`.
 */
static void FASTCOVER_ctx_destroy(FASTCOVER_ctx_t *ctx) {
  if (!ctx) {
    return;
  }
  if (ctx->segmentFreqs) {
    free(ctx->segmentFreqs);
    ctx->segmentFreqs = NULL;
  }
  if (ctx->freqs) {
    free(ctx->freqs);
    ctx->freqs = NULL;
  }
  if (ctx->offsets) {
    free(ctx->offsets);
    ctx->offsets = NULL;
  }
}

/**
 * Calculate for frequency of hash value of each dmer in ctx->samples
 */
static void FASTCOVER_computeFrequency(U32 *freqs, unsigned f, FASTCOVER_ctx_t *ctx){
  size_t start; /* start of current dmer */
  for (unsigned i = 0; i < ctx->nbTrainSamples; i++) {
    size_t currSampleStart = ctx->offsets[i];
    size_t currSampleEnd = ctx->offsets[i+1];
    start = currSampleStart;
    while (start + ctx->d <= currSampleEnd) {
      const size_t dmerIndex = FASTCOVER_hashPtrToIndex(ctx->samples + start, f, ctx->d);
      freqs[dmerIndex]++;
      start++;
    }
  }
}

/**
 * Prepare a context for dictionary building.
 * The context is only dependent on the parameter `d` and can used multiple
 * times.
 * Returns 1 on success or zero on error.
 * The context must be destroyed with `FASTCOVER_ctx_destroy()`.
 */
static int FASTCOVER_ctx_init(FASTCOVER_ctx_t *ctx, const void *samplesBuffer,
                          const size_t *samplesSizes, unsigned nbSamples,
                          unsigned d, double splitPoint, unsigned f) {
  const BYTE *const samples = (const BYTE *)samplesBuffer;
  const size_t totalSamplesSize = FASTCOVER_sum(samplesSizes, nbSamples);
  /* Split samples into testing and training sets */
  const unsigned nbTrainSamples = splitPoint < 1.0 ? (unsigned)((double)nbSamples * splitPoint) : nbSamples;
  const unsigned nbTestSamples = splitPoint < 1.0 ? nbSamples - nbTrainSamples : nbSamples;
  const size_t trainingSamplesSize = splitPoint < 1.0 ? FASTCOVER_sum(samplesSizes, nbTrainSamples) : totalSamplesSize;
  const size_t testSamplesSize = splitPoint < 1.0 ? FASTCOVER_sum(samplesSizes + nbTrainSamples, nbTestSamples) : totalSamplesSize;
  /* Checks */
  if (totalSamplesSize < MAX(d, sizeof(U64)) ||
      totalSamplesSize >= (size_t)FASTCOVER_MAX_SAMPLES_SIZE) {
    DISPLAYLEVEL(1, "Total samples size is too large (%u MB), maximum size is %u MB\n",
                 (U32)(totalSamplesSize >> 20), (FASTCOVER_MAX_SAMPLES_SIZE >> 20));
    return 0;
  }
  /* Check if there are at least 5 training samples */
  if (nbTrainSamples < 5) {
    DISPLAYLEVEL(1, "Total number of training samples is %u and is invalid.", nbTrainSamples);
    return 0;
  }
  /* Check if there's testing sample */
  if (nbTestSamples < 1) {
    DISPLAYLEVEL(1, "Total number of testing samples is %u and is invalid.", nbTestSamples);
    return 0;
  }
  /* Zero the context */
  memset(ctx, 0, sizeof(*ctx));
  DISPLAYLEVEL(2, "Training on %u samples of total size %u\n", nbTrainSamples,
               (U32)trainingSamplesSize);
  DISPLAYLEVEL(2, "Testing on %u samples of total size %u\n", nbTestSamples,
               (U32)testSamplesSize);

  ctx->samples = samples;
  ctx->samplesSizes = samplesSizes;
  ctx->nbSamples = nbSamples;
  ctx->nbTrainSamples = nbTrainSamples;
  ctx->nbTestSamples = nbTestSamples;
  ctx->nbDmers = trainingSamplesSize - d + 1;
  ctx->d = d;

  /* The offsets of each file */
  ctx->offsets = (size_t *)malloc((nbSamples + 1) * sizeof(size_t));
  if (!ctx->offsets) {
    DISPLAYLEVEL(1, "Failed to allocate scratch buffers\n");
    FASTCOVER_ctx_destroy(ctx);
    return 0;
  }

  /* Fill offsets from the samplesSizes */
  {
    U32 i;
    ctx->offsets[0] = 0;
    for (i = 1; i <= nbSamples; ++i) {
      ctx->offsets[i] = ctx->offsets[i - 1] + samplesSizes[i - 1];
    }
  }

  /* Initialize frequency array of size 2^f */
  ctx->freqs = (U32 *)calloc((1 << f), sizeof(U32));
  ctx->segmentFreqs = (U16 *)calloc((1 << f), sizeof(U16));
  DISPLAYLEVEL(2, "Computing frequencies\n");
  FASTCOVER_computeFrequency(ctx->freqs, f, ctx);

  return 1;
}


/**
 * Given the prepared context build the dictionary.
 */
static size_t FASTCOVER_buildDictionary(const FASTCOVER_ctx_t *ctx, U32 *freqs,
                                    void *dictBuffer,
                                    size_t dictBufferCapacity,
                                    ZDICT_fastCover_params_t parameters){
  BYTE *const dict = (BYTE *)dictBuffer;
  size_t tail = dictBufferCapacity;
  /* Divide the data up into epochs of equal size.
   * We will select at least one segment from each epoch.
   */
  const U32 epochs = MAX(1, (U32)(dictBufferCapacity / parameters.k));
  const U32 epochSize = (U32)(ctx->nbDmers / epochs);
  size_t epoch;
  DISPLAYLEVEL(2, "Breaking content into %u epochs of size %u\n", epochs,
               epochSize);
  /* Loop through the epochs until there are no more segments or the dictionary
   * is full.
   */
  for (epoch = 0; tail > 0; epoch = (epoch + 1) % epochs) {
    const U32 epochBegin = (U32)(epoch * epochSize);
    const U32 epochEnd = epochBegin + epochSize;
    size_t segmentSize;
    /* Select a segment */
    FASTCOVER_segment_t segment = FASTCOVER_selectSegment(
        ctx, freqs, epochBegin, epochEnd, parameters);

    /* If the segment covers no dmers, then we are out of content */
    if (segment.score == 0) {
      break;
    }

    /* Trim the segment if necessary and if it is too small then we are done */
    segmentSize = MIN(segment.end - segment.begin + parameters.d - 1, tail);
    if (segmentSize < parameters.d) {
      break;
    }

    /* We fill the dictionary from the back to allow the best segments to be
     * referenced with the smallest offsets.
     */
    tail -= segmentSize;
    memcpy(dict + tail, ctx->samples + segment.begin, segmentSize);
    DISPLAYUPDATE(
        2, "\r%u%%       ",
        (U32)(((dictBufferCapacity - tail) * 100) / dictBufferCapacity));
  }
  DISPLAYLEVEL(2, "\r%79s\r", "");
  return tail;
}


/**
 * FASTCOVER_best_t is used for two purposes:
 * 1. Synchronizing threads.
 * 2. Saving the best parameters and dictionary.
 *
 * All of the methods except FASTCOVER_best_init() are thread safe if zstd is
 * compiled with multithreaded support.
 */
typedef struct fast_best_s {
  ZSTD_pthread_mutex_t mutex;
  ZSTD_pthread_cond_t cond;
  size_t liveJobs;
  void *dict;
  size_t dictSize;
  ZDICT_fastCover_params_t parameters;
  size_t compressedSize;
} FASTCOVER_best_t;

/**
 * Initialize the `FASTCOVER_best_t`.
 */
static void FASTCOVER_best_init(FASTCOVER_best_t *best) {
  if (best==NULL) return; /* compatible with init on NULL */
  (void)ZSTD_pthread_mutex_init(&best->mutex, NULL);
  (void)ZSTD_pthread_cond_init(&best->cond, NULL);
  best->liveJobs = 0;
  best->dict = NULL;
  best->dictSize = 0;
  best->compressedSize = (size_t)-1;
  memset(&best->parameters, 0, sizeof(best->parameters));
}

/**
 * Wait until liveJobs == 0.
 */
static void FASTCOVER_best_wait(FASTCOVER_best_t *best) {
  if (!best) {
    return;
  }
  ZSTD_pthread_mutex_lock(&best->mutex);
  while (best->liveJobs != 0) {
    ZSTD_pthread_cond_wait(&best->cond, &best->mutex);
  }
  ZSTD_pthread_mutex_unlock(&best->mutex);
}

/**
 * Call FASTCOVER_best_wait() and then destroy the FASTCOVER_best_t.
 */
static void FASTCOVER_best_destroy(FASTCOVER_best_t *best) {
  if (!best) {
    return;
  }
  FASTCOVER_best_wait(best);
  if (best->dict) {
    free(best->dict);
  }
  ZSTD_pthread_mutex_destroy(&best->mutex);
  ZSTD_pthread_cond_destroy(&best->cond);
}

/**
 * Called when a thread is about to be launched.
 * Increments liveJobs.
 */
static void FASTCOVER_best_start(FASTCOVER_best_t *best) {
  if (!best) {
    return;
  }
  ZSTD_pthread_mutex_lock(&best->mutex);
  ++best->liveJobs;
  ZSTD_pthread_mutex_unlock(&best->mutex);
}

/**
 * Called when a thread finishes executing, both on error or success.
 * Decrements liveJobs and signals any waiting threads if liveJobs == 0.
 * If this dictionary is the best so far save it and its parameters.
 */
static void FASTCOVER_best_finish(FASTCOVER_best_t *best, size_t compressedSize,
                              ZDICT_fastCover_params_t parameters, void *dict,
                              size_t dictSize) {
  if (!best) {
    return;
  }
  {
    size_t liveJobs;
    ZSTD_pthread_mutex_lock(&best->mutex);
    --best->liveJobs;
    liveJobs = best->liveJobs;
    /* If the new dictionary is better */
    if (compressedSize < best->compressedSize) {
      /* Allocate space if necessary */
      if (!best->dict || best->dictSize < dictSize) {
        if (best->dict) {
          free(best->dict);
        }
        best->dict = malloc(dictSize);
        if (!best->dict) {
          best->compressedSize = ERROR(GENERIC);
          best->dictSize = 0;
          return;
        }
      }
      /* Save the dictionary, parameters, and size */
      memcpy(best->dict, dict, dictSize);
      best->dictSize = dictSize;
      best->parameters = parameters;
      best->compressedSize = compressedSize;
    }
    ZSTD_pthread_mutex_unlock(&best->mutex);
    if (liveJobs == 0) {
      ZSTD_pthread_cond_broadcast(&best->cond);
    }
  }
}

/**
 * Parameters for FASTCOVER_tryParameters().
 */
typedef struct FASTCOVER_tryParameters_data_s {
  const FASTCOVER_ctx_t *ctx;
  FASTCOVER_best_t *best;
  size_t dictBufferCapacity;
  ZDICT_fastCover_params_t parameters;
} FASTCOVER_tryParameters_data_t;

/**
 * Tries a set of parameters and updates the FASTCOVER_best_t with the results.
 * This function is thread safe if zstd is compiled with multithreaded support.
 * It takes its parameters as an *OWNING* opaque pointer to support threading.
 */
static void FASTCOVER_tryParameters(void *opaque) {
  /* Save parameters as local variables */
  FASTCOVER_tryParameters_data_t *const data = (FASTCOVER_tryParameters_data_t *)opaque;
  const FASTCOVER_ctx_t *const ctx = data->ctx;
  const ZDICT_fastCover_params_t parameters = data->parameters;
  size_t dictBufferCapacity = data->dictBufferCapacity;
  size_t totalCompressedSize = ERROR(GENERIC);
  /* Allocate space for hash table, dict, and freqs */
  BYTE *const dict = (BYTE * const)malloc(dictBufferCapacity);
  U32 *freqs = (U32*) malloc((1 << parameters.f) * sizeof(U32));
  if (!dict || !freqs) {
    DISPLAYLEVEL(1, "Failed to allocate buffers: out of memory\n");
    goto _cleanup;
  }
  /* Copy the frequencies because we need to modify them */
  memcpy(freqs, ctx->freqs, (1 << parameters.f) * sizeof(U32));
  /* Build the dictionary */
  {
    const size_t tail = FASTCOVER_buildDictionary(ctx, freqs, dict,
                                              dictBufferCapacity, parameters);

    dictBufferCapacity = ZDICT_finalizeDictionary(
        dict, dictBufferCapacity, dict + tail, dictBufferCapacity - tail,
        ctx->samples, ctx->samplesSizes, (unsigned)ctx->nbTrainSamples,
        parameters.zParams);
    if (ZDICT_isError(dictBufferCapacity)) {
      DISPLAYLEVEL(1, "Failed to finalize dictionary\n");
      goto _cleanup;
    }
  }
  /* Check total compressed size */
  {
    /* Pointers */
    ZSTD_CCtx *cctx;
    ZSTD_CDict *cdict;
    void *dst;
    /* Local variables */
    size_t dstCapacity;
    size_t i;
    /* Allocate dst with enough space to compress the maximum sized sample */
    {
      size_t maxSampleSize = 0;
      i = parameters.splitPoint < 1.0 ? ctx->nbTrainSamples : 0;
      for (; i < ctx->nbSamples; ++i) {
        maxSampleSize = MAX(ctx->samplesSizes[i], maxSampleSize);
      }
      dstCapacity = ZSTD_compressBound(maxSampleSize);
      dst = malloc(dstCapacity);
    }
    /* Create the cctx and cdict */
    cctx = ZSTD_createCCtx();
    cdict = ZSTD_createCDict(dict, dictBufferCapacity,
                             parameters.zParams.compressionLevel);
    if (!dst || !cctx || !cdict) {
      goto _compressCleanup;
    }
    /* Compress each sample and sum their sizes (or error) */
    totalCompressedSize = dictBufferCapacity;
    i = parameters.splitPoint < 1.0 ? ctx->nbTrainSamples : 0;
    for (; i < ctx->nbSamples; ++i) {
      const size_t size = ZSTD_compress_usingCDict(
          cctx, dst, dstCapacity, ctx->samples + ctx->offsets[i],
          ctx->samplesSizes[i], cdict);
      if (ZSTD_isError(size)) {
        totalCompressedSize = ERROR(GENERIC);
        goto _compressCleanup;
      }
      totalCompressedSize += size;
    }
  _compressCleanup:
    ZSTD_freeCCtx(cctx);
    ZSTD_freeCDict(cdict);
    if (dst) {
      free(dst);
    }
  }

_cleanup:
  FASTCOVER_best_finish(data->best, totalCompressedSize, parameters, dict,
                    dictBufferCapacity);
  free(data);
  if (dict) {
    free(dict);
  }
  if (freqs) {
    free(freqs);
  }
}

ZDICTLIB_API size_t ZDICT_trainFromBuffer_fastCover(
    void *dictBuffer, size_t dictBufferCapacity, const void *samplesBuffer,
    const size_t *samplesSizes, unsigned nbSamples, ZDICT_fastCover_params_t parameters) {
    BYTE* const dict = (BYTE*)dictBuffer;
    FASTCOVER_ctx_t ctx;
    parameters.splitPoint = 1.0;
    /* Initialize global data */
    g_displayLevel = parameters.zParams.notificationLevel;
    /* Checks */
    if (!FASTCOVER_checkParameters(parameters, dictBufferCapacity)) {
      DISPLAYLEVEL(1, "FASTCOVER parameters incorrect\n");
      return ERROR(GENERIC);
    }
    if (nbSamples == 0) {
      DISPLAYLEVEL(1, "FASTCOVER must have at least one input file\n");
      return ERROR(GENERIC);
    }
    if (dictBufferCapacity < ZDICT_DICTSIZE_MIN) {
      DISPLAYLEVEL(1, "dictBufferCapacity must be at least %u\n",
                   ZDICT_DICTSIZE_MIN);
      return ERROR(dstSize_tooSmall);
    }
    /* Initialize context */
    if (!FASTCOVER_ctx_init(&ctx, samplesBuffer, samplesSizes, nbSamples,
                            parameters.d, parameters.splitPoint, parameters.f)) {
      DISPLAYLEVEL(1, "Failed to initialize context\n");
      return ERROR(GENERIC);
    }
    /* Build the dictionary */
    DISPLAYLEVEL(2, "Building dictionary\n");
    {
      const size_t tail = FASTCOVER_buildDictionary(&ctx, ctx.freqs, dictBuffer,
                                                dictBufferCapacity, parameters);

      const size_t dictionarySize = ZDICT_finalizeDictionary(
          dict, dictBufferCapacity, dict + tail, dictBufferCapacity - tail,
          samplesBuffer, samplesSizes, (unsigned)ctx.nbTrainSamples,
          parameters.zParams);
      if (!ZSTD_isError(dictionarySize)) {
          DISPLAYLEVEL(2, "Constructed dictionary of size %u\n",
                      (U32)dictionarySize);
      }
      FASTCOVER_ctx_destroy(&ctx);
      return dictionarySize;
    }
}



ZDICTLIB_API size_t ZDICT_optimizeTrainFromBuffer_fastCover(
    void *dictBuffer, size_t dictBufferCapacity, const void *samplesBuffer,
    const size_t *samplesSizes, unsigned nbSamples,
    ZDICT_fastCover_params_t *parameters) {
    /* constants */
    const unsigned nbThreads = parameters->nbThreads;
    const double splitPoint =
        parameters->splitPoint <= 0.0 ? DEFAULT_SPLITPOINT : parameters->splitPoint;
    const unsigned kMinD = parameters->d == 0 ? 6 : parameters->d;
    const unsigned kMaxD = parameters->d == 0 ? 8 : parameters->d;
    const unsigned kMinK = parameters->k == 0 ? 50 : parameters->k;
    const unsigned kMaxK = parameters->k == 0 ? 2000 : parameters->k;
    const unsigned kSteps = parameters->steps == 0 ? 40 : parameters->steps;
    const unsigned kStepSize = MAX((kMaxK - kMinK) / kSteps, 1);
    const unsigned kIterations =
        (1 + (kMaxD - kMinD) / 2) * (1 + (kMaxK - kMinK) / kStepSize);
    const unsigned f = parameters->f == 0 ? 23 : parameters->f;

    /* Local variables */
    const int displayLevel = parameters->zParams.notificationLevel;
    unsigned iteration = 1;
    unsigned d;
    unsigned k;
    FASTCOVER_best_t best;
    POOL_ctx *pool = NULL;

    /* Checks */
    if (splitPoint <= 0 || splitPoint > 1) {
      LOCALDISPLAYLEVEL(displayLevel, 1, "Incorrect splitPoint\n");
      return ERROR(GENERIC);
    }
    if (kMinK < kMaxD || kMaxK < kMinK) {
      LOCALDISPLAYLEVEL(displayLevel, 1, "Incorrect k\n");
      return ERROR(GENERIC);
    }
    if (nbSamples == 0) {
      DISPLAYLEVEL(1, "FASTCOVER must have at least one input file\n");
      return ERROR(GENERIC);
    }
    if (dictBufferCapacity < ZDICT_DICTSIZE_MIN) {
      DISPLAYLEVEL(1, "dictBufferCapacity must be at least %u\n",
                   ZDICT_DICTSIZE_MIN);
      return ERROR(dstSize_tooSmall);
    }
    if (nbThreads > 1) {
      pool = POOL_create(nbThreads, 1);
      if (!pool) {
        return ERROR(memory_allocation);
      }
    }
    /* Initialization */
    FASTCOVER_best_init(&best);
    /* Turn down global display level to clean up display at level 2 and below */
    g_displayLevel = displayLevel == 0 ? 0 : displayLevel - 1;
    /* Loop through d first because each new value needs a new context */
    LOCALDISPLAYLEVEL(displayLevel, 2, "Trying %u different sets of parameters\n",
                      kIterations);
    for (d = kMinD; d <= kMaxD; d += 2) {
      /* Initialize the context for this value of d */
      FASTCOVER_ctx_t ctx;
      LOCALDISPLAYLEVEL(displayLevel, 3, "d=%u\n", d);
      if (!FASTCOVER_ctx_init(&ctx, samplesBuffer, samplesSizes, nbSamples, d, splitPoint, f)) {
        LOCALDISPLAYLEVEL(displayLevel, 1, "Failed to initialize context\n");
        FASTCOVER_best_destroy(&best);
        POOL_free(pool);
        return ERROR(GENERIC);
      }
      /* Loop through k reusing the same context */
      for (k = kMinK; k <= kMaxK; k += kStepSize) {
        /* Prepare the arguments */
        FASTCOVER_tryParameters_data_t *data = (FASTCOVER_tryParameters_data_t *)malloc(
            sizeof(FASTCOVER_tryParameters_data_t));
        LOCALDISPLAYLEVEL(displayLevel, 3, "k=%u\n", k);
        if (!data) {
          LOCALDISPLAYLEVEL(displayLevel, 1, "Failed to allocate parameters\n");
          FASTCOVER_best_destroy(&best);
          FASTCOVER_ctx_destroy(&ctx);
          POOL_free(pool);
          return ERROR(GENERIC);
        }
        data->ctx = &ctx;
        data->best = &best;
        data->dictBufferCapacity = dictBufferCapacity;
        data->parameters = *parameters;
        data->parameters.k = k;
        data->parameters.d = d;
        data->parameters.f = f;
        data->parameters.splitPoint = splitPoint;
        data->parameters.steps = kSteps;
        data->parameters.zParams.notificationLevel = g_displayLevel;
        /* Check the parameters */
        if (!FASTCOVER_checkParameters(data->parameters, dictBufferCapacity)) {
          DISPLAYLEVEL(1, "fastCover parameters incorrect\n");
          free(data);
          continue;
        }
        /* Call the function and pass ownership of data to it */
        FASTCOVER_best_start(&best);
        if (pool) {
          POOL_add(pool, &FASTCOVER_tryParameters, data);
        } else {
          FASTCOVER_tryParameters(data);
        }
        /* Print status */
        LOCALDISPLAYUPDATE(displayLevel, 2, "\r%u%%       ",
                           (U32)((iteration * 100) / kIterations));
        ++iteration;
      }
      FASTCOVER_best_wait(&best);
      FASTCOVER_ctx_destroy(&ctx);
    }
    LOCALDISPLAYLEVEL(displayLevel, 2, "\r%79s\r", "");
    /* Fill the output buffer and parameters with output of the best parameters */
    {
      const size_t dictSize = best.dictSize;
      if (ZSTD_isError(best.compressedSize)) {
        const size_t compressedSize = best.compressedSize;
        FASTCOVER_best_destroy(&best);
        POOL_free(pool);
        return compressedSize;
      }
      *parameters = best.parameters;
      memcpy(dictBuffer, best.dict, dictSize);
      FASTCOVER_best_destroy(&best);
      POOL_free(pool);
      return dictSize;
    }

}
