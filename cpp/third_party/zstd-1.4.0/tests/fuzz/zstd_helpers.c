/*
 * Copyright (c) 2016-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under both the BSD-style license (found in the
 * LICENSE file in the root directory of this source tree) and the GPLv2 (found
 * in the COPYING file in the root directory of this source tree).
 */

#define ZSTD_STATIC_LINKING_ONLY
#define ZDICT_STATIC_LINKING_ONLY

#include <string.h>

#include "zstd_helpers.h"
#include "fuzz_helpers.h"
#include "zstd.h"
#include "zdict.h"

static void set(ZSTD_CCtx *cctx, ZSTD_cParameter param, int value)
{
    FUZZ_ZASSERT(ZSTD_CCtx_setParameter(cctx, param, value));
}

static void setRand(ZSTD_CCtx *cctx, ZSTD_cParameter param, unsigned min,
                    unsigned max, uint32_t *state) {
    unsigned const value = FUZZ_rand32(state, min, max);
    set(cctx, param, value);
}

ZSTD_compressionParameters FUZZ_randomCParams(size_t srcSize, uint32_t *state)
{
    /* Select compression parameters */
    ZSTD_compressionParameters cParams;
    cParams.windowLog = FUZZ_rand32(state, ZSTD_WINDOWLOG_MIN, 15);
    cParams.hashLog = FUZZ_rand32(state, ZSTD_HASHLOG_MIN, 15);
    cParams.chainLog = FUZZ_rand32(state, ZSTD_CHAINLOG_MIN, 16);
    cParams.searchLog = FUZZ_rand32(state, ZSTD_SEARCHLOG_MIN, 9);
    cParams.minMatch = FUZZ_rand32(state, ZSTD_MINMATCH_MIN,
                                          ZSTD_MINMATCH_MAX);
    cParams.targetLength = FUZZ_rand32(state, 0, 512);
    cParams.strategy = FUZZ_rand32(state, ZSTD_STRATEGY_MIN, ZSTD_STRATEGY_MAX);
    return ZSTD_adjustCParams(cParams, srcSize, 0);
}

ZSTD_frameParameters FUZZ_randomFParams(uint32_t *state)
{
    /* Select frame parameters */
    ZSTD_frameParameters fParams;
    fParams.contentSizeFlag = FUZZ_rand32(state, 0, 1);
    fParams.checksumFlag = FUZZ_rand32(state, 0, 1);
    fParams.noDictIDFlag = FUZZ_rand32(state, 0, 1);
    return fParams;
}

ZSTD_parameters FUZZ_randomParams(size_t srcSize, uint32_t *state)
{
    ZSTD_parameters params;
    params.cParams = FUZZ_randomCParams(srcSize, state);
    params.fParams = FUZZ_randomFParams(state);
    return params;
}

void FUZZ_setRandomParameters(ZSTD_CCtx *cctx, size_t srcSize, uint32_t *state)
{
    ZSTD_compressionParameters cParams = FUZZ_randomCParams(srcSize, state);
    set(cctx, ZSTD_c_windowLog, cParams.windowLog);
    set(cctx, ZSTD_c_hashLog, cParams.hashLog);
    set(cctx, ZSTD_c_chainLog, cParams.chainLog);
    set(cctx, ZSTD_c_searchLog, cParams.searchLog);
    set(cctx, ZSTD_c_minMatch, cParams.minMatch);
    set(cctx, ZSTD_c_targetLength, cParams.targetLength);
    set(cctx, ZSTD_c_strategy, cParams.strategy);
    /* Select frame parameters */
    setRand(cctx, ZSTD_c_contentSizeFlag, 0, 1, state);
    setRand(cctx, ZSTD_c_checksumFlag, 0, 1, state);
    setRand(cctx, ZSTD_c_dictIDFlag, 0, 1, state);
    /* Select long distance matching parameters */
    setRand(cctx, ZSTD_c_enableLongDistanceMatching, 0, 1, state);
    setRand(cctx, ZSTD_c_ldmHashLog, ZSTD_HASHLOG_MIN, 16, state);
    setRand(cctx, ZSTD_c_ldmMinMatch, ZSTD_LDM_MINMATCH_MIN,
            ZSTD_LDM_MINMATCH_MAX, state);
    setRand(cctx, ZSTD_c_ldmBucketSizeLog, 0, ZSTD_LDM_BUCKETSIZELOG_MAX,
            state);
    setRand(cctx, ZSTD_c_ldmHashRateLog, ZSTD_LDM_HASHRATELOG_MIN,
            ZSTD_LDM_HASHRATELOG_MAX, state);
    /* Set misc parameters */
    setRand(cctx, ZSTD_c_nbWorkers, 0, 2, state);
    setRand(cctx, ZSTD_c_rsyncable, 0, 1, state);
    setRand(cctx, ZSTD_c_forceMaxWindow, 0, 1, state);
    setRand(cctx, ZSTD_c_literalCompressionMode, 0, 2, state);
    setRand(cctx, ZSTD_c_forceAttachDict, 0, 2, state);
}

FUZZ_dict_t FUZZ_train(void const* src, size_t srcSize, uint32_t *state)
{
    size_t const dictSize = MAX(srcSize / 8, 1024);
    size_t const totalSampleSize = dictSize * 11;
    FUZZ_dict_t dict = { malloc(dictSize), dictSize };
    char* const samples = (char*)malloc(totalSampleSize);
    unsigned nbSamples = 100;
    size_t* const samplesSizes = (size_t*)malloc(sizeof(size_t) * nbSamples);
    size_t pos = 0;
    size_t sample = 0;
    ZDICT_fastCover_params_t params;
    FUZZ_ASSERT(dict.buff && samples && samplesSizes);

    for (sample = 0; sample < nbSamples; ++sample) {
      size_t const remaining = totalSampleSize - pos;
      size_t const offset = FUZZ_rand32(state, 0, MAX(srcSize, 1) - 1);
      size_t const limit = MIN(srcSize - offset, remaining);
      size_t const toCopy = MIN(limit, remaining / (nbSamples - sample));
      memcpy(samples + pos, src + offset, toCopy);
      pos += toCopy;
      samplesSizes[sample] = toCopy;

    }
    memset(samples + pos, 0, totalSampleSize - pos);

    memset(&params, 0, sizeof(params));
    params.accel = 5;
    params.k = 40;
    params.d = 8;
    params.f = 14;
    params.zParams.compressionLevel = 1;
    dict.size = ZDICT_trainFromBuffer_fastCover(dict.buff, dictSize,
        samples, samplesSizes, nbSamples, params);
    if (ZSTD_isError(dict.size)) {
        free(dict.buff);
        memset(&dict, 0, sizeof(dict));
    }

    free(samplesSizes);
    free(samples);

    return dict;
}
