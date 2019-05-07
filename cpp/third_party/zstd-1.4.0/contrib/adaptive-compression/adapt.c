/*
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under both the BSD-style license (found in the
 * LICENSE file in the root directory of this source tree) and the GPLv2 (found
 * in the COPYING file in the root directory of this source tree).
 */

#include <stdio.h>      /* fprintf */
#include <stdlib.h>     /* malloc, free */
#include <pthread.h>    /* pthread functions */
#include <string.h>     /* memset */
#include "zstd_internal.h"
#include "util.h"
#include "timefn.h"     /* UTIL_time_t, UTIL_getTime, UTIL_getSpanTimeMicro */

#define DISPLAY(...) fprintf(stderr, __VA_ARGS__)
#define PRINT(...) fprintf(stdout, __VA_ARGS__)
#define DEBUG(l, ...) { if (g_displayLevel>=l) { DISPLAY(__VA_ARGS__); } }
#define FILE_CHUNK_SIZE 4 << 20
#define MAX_NUM_JOBS 2
#define stdinmark  "/*stdin*\\"
#define stdoutmark "/*stdout*\\"
#define MAX_PATH 256
#define DEFAULT_DISPLAY_LEVEL 1
#define DEFAULT_COMPRESSION_LEVEL 6
#define MAX_COMPRESSION_LEVEL_CHANGE 2
#define CONVERGENCE_LOWER_BOUND 5
#define CLEVEL_DECREASE_COOLDOWN 5
#define CHANGE_BY_TWO_THRESHOLD 0.1
#define CHANGE_BY_ONE_THRESHOLD 0.65

#ifndef DEBUG_MODE
static int g_displayLevel = DEFAULT_DISPLAY_LEVEL;
#else
static int g_displayLevel = DEBUG_MODE;
#endif

static unsigned g_compressionLevel = DEFAULT_COMPRESSION_LEVEL;
static UTIL_time_t g_startTime;
static size_t g_streamedSize = 0;
static unsigned g_useProgressBar = 1;
static unsigned g_forceCompressionLevel = 0;
static unsigned g_minCLevel = 1;
static unsigned g_maxCLevel;

typedef struct {
    void* start;
    size_t size;
    size_t capacity;
} buffer_t;

typedef struct {
    size_t filled;
    buffer_t buffer;
} inBuff_t;

typedef struct {
    buffer_t src;
    buffer_t dst;
    unsigned jobID;
    unsigned lastJobPlusOne;
    size_t compressedSize;
    size_t dictSize;
} jobDescription;

typedef struct {
    pthread_mutex_t pMutex;
    int noError;
} mutex_t;

typedef struct {
    pthread_cond_t pCond;
    int noError;
} cond_t;

typedef struct {
    unsigned compressionLevel;
    unsigned numJobs;
    unsigned nextJobID;
    unsigned threadError;

    /*
     * JobIDs for the next jobs to be created, compressed, and written
     */
    unsigned jobReadyID;
    unsigned jobCompressedID;
    unsigned jobWriteID;
    unsigned allJobsCompleted;

    /*
     * counter for how many jobs in a row the compression level has not changed
     * if the counter becomes >= CONVERGENCE_LOWER_BOUND, the next time the
     * compression level tries to change (by non-zero amount) resets the counter
     * to 1 and does not apply the change
     */
    unsigned convergenceCounter;

    /*
     * cooldown counter in order to prevent rapid successive decreases in compression level
     * whenever compression level is decreased, cooldown is set to CLEVEL_DECREASE_COOLDOWN
     * whenever adaptCompressionLevel() is called and cooldown != 0, it is decremented
     * as long as cooldown != 0, the compression level cannot be decreased
     */
    unsigned cooldown;

    /*
     * XWaitYCompletion
     * Range from 0.0 to 1.0
     * if the value is not 1.0, then this implies that thread X waited on thread Y to finish
     * and thread Y was XWaitYCompletion finished at the time of the wait (i.e. compressWaitWriteCompletion=0.5
     * implies that the compression thread waited on the write thread and it was only 50% finished writing a job)
     */
    double createWaitCompressionCompletion;
    double compressWaitCreateCompletion;
    double compressWaitWriteCompletion;
    double writeWaitCompressionCompletion;

    /*
     * Completion values
     * Range from 0.0 to 1.0
     * Jobs are divided into mini-chunks in order to measure completion
     * these values are updated each time a thread finishes its operation on the
     * mini-chunk (i.e. finishes writing out, compressing, etc. this mini-chunk).
     */
    double compressionCompletion;
    double writeCompletion;
    double createCompletion;

    mutex_t jobCompressed_mutex;
    cond_t jobCompressed_cond;
    mutex_t jobReady_mutex;
    cond_t jobReady_cond;
    mutex_t allJobsCompleted_mutex;
    cond_t allJobsCompleted_cond;
    mutex_t jobWrite_mutex;
    cond_t jobWrite_cond;
    mutex_t compressionCompletion_mutex;
    mutex_t createCompletion_mutex;
    mutex_t writeCompletion_mutex;
    mutex_t compressionLevel_mutex;
    size_t lastDictSize;
    inBuff_t input;
    jobDescription* jobs;
    ZSTD_CCtx* cctx;
} adaptCCtx;

typedef struct {
    adaptCCtx* ctx;
    FILE* dstFile;
} outputThreadArg;

typedef struct {
    FILE* srcFile;
    adaptCCtx* ctx;
    outputThreadArg* otArg;
} fcResources;

static void freeCompressionJobs(adaptCCtx* ctx)
{
    unsigned u;
    for (u=0; u<ctx->numJobs; u++) {
        jobDescription job = ctx->jobs[u];
        free(job.dst.start);
        free(job.src.start);
    }
}

static int destroyMutex(mutex_t* mutex)
{
    if (mutex->noError) {
        int const ret = pthread_mutex_destroy(&mutex->pMutex);
        return ret;
    }
    return 0;
}

static int destroyCond(cond_t* cond)
{
    if (cond->noError) {
        int const ret = pthread_cond_destroy(&cond->pCond);
        return ret;
    }
    return 0;
}

static int freeCCtx(adaptCCtx* ctx)
{
    if (!ctx) return 0;
    {
        int error = 0;
        error |= destroyMutex(&ctx->jobCompressed_mutex);
        error |= destroyCond(&ctx->jobCompressed_cond);
        error |= destroyMutex(&ctx->jobReady_mutex);
        error |= destroyCond(&ctx->jobReady_cond);
        error |= destroyMutex(&ctx->allJobsCompleted_mutex);
        error |= destroyCond(&ctx->allJobsCompleted_cond);
        error |= destroyMutex(&ctx->jobWrite_mutex);
        error |= destroyCond(&ctx->jobWrite_cond);
        error |= destroyMutex(&ctx->compressionCompletion_mutex);
        error |= destroyMutex(&ctx->createCompletion_mutex);
        error |= destroyMutex(&ctx->writeCompletion_mutex);
        error |= destroyMutex(&ctx->compressionLevel_mutex);
        error |= ZSTD_isError(ZSTD_freeCCtx(ctx->cctx));
        free(ctx->input.buffer.start);
        if (ctx->jobs){
            freeCompressionJobs(ctx);
            free(ctx->jobs);
        }
        free(ctx);
        return error;
    }
}

static int initMutex(mutex_t* mutex)
{
    int const ret = pthread_mutex_init(&mutex->pMutex, NULL);
    mutex->noError = !ret;
    return ret;
}

static int initCond(cond_t* cond)
{
    int const ret = pthread_cond_init(&cond->pCond, NULL);
    cond->noError = !ret;
    return ret;
}

static int initCCtx(adaptCCtx* ctx, unsigned numJobs)
{
    ctx->compressionLevel = g_compressionLevel;
    {
        int pthreadError = 0;
        pthreadError |= initMutex(&ctx->jobCompressed_mutex);
        pthreadError |= initCond(&ctx->jobCompressed_cond);
        pthreadError |= initMutex(&ctx->jobReady_mutex);
        pthreadError |= initCond(&ctx->jobReady_cond);
        pthreadError |= initMutex(&ctx->allJobsCompleted_mutex);
        pthreadError |= initCond(&ctx->allJobsCompleted_cond);
        pthreadError |= initMutex(&ctx->jobWrite_mutex);
        pthreadError |= initCond(&ctx->jobWrite_cond);
        pthreadError |= initMutex(&ctx->compressionCompletion_mutex);
        pthreadError |= initMutex(&ctx->createCompletion_mutex);
        pthreadError |= initMutex(&ctx->writeCompletion_mutex);
        pthreadError |= initMutex(&ctx->compressionLevel_mutex);
        if (pthreadError) return pthreadError;
    }
    ctx->numJobs = numJobs;
    ctx->jobReadyID = 0;
    ctx->jobCompressedID = 0;
    ctx->jobWriteID = 0;
    ctx->lastDictSize = 0;


    ctx->createWaitCompressionCompletion = 1;
    ctx->compressWaitCreateCompletion = 1;
    ctx->compressWaitWriteCompletion = 1;
    ctx->writeWaitCompressionCompletion = 1;
    ctx->createCompletion = 1;
    ctx->writeCompletion = 1;
    ctx->compressionCompletion = 1;
    ctx->convergenceCounter = 0;
    ctx->cooldown = 0;

    ctx->jobs = calloc(1, numJobs*sizeof(jobDescription));

    if (!ctx->jobs) {
        DISPLAY("Error: could not allocate space for jobs during context creation\n");
        return 1;
    }

    /* initializing jobs */
    {
        unsigned jobNum;
        for (jobNum=0; jobNum<numJobs; jobNum++) {
            jobDescription* job = &ctx->jobs[jobNum];
            job->src.start = malloc(2 * FILE_CHUNK_SIZE);
            job->dst.start = malloc(ZSTD_compressBound(FILE_CHUNK_SIZE));
            job->lastJobPlusOne = 0;
            if (!job->src.start || !job->dst.start) {
                DISPLAY("Could not allocate buffers for jobs\n");
                return 1;
            }
            job->src.capacity = FILE_CHUNK_SIZE;
            job->dst.capacity = ZSTD_compressBound(FILE_CHUNK_SIZE);
        }
    }

    ctx->nextJobID = 0;
    ctx->threadError = 0;
    ctx->allJobsCompleted = 0;

    ctx->cctx = ZSTD_createCCtx();
    if (!ctx->cctx) {
        DISPLAY("Error: could not allocate ZSTD_CCtx\n");
        return 1;
    }

    ctx->input.filled = 0;
    ctx->input.buffer.capacity = 2 * FILE_CHUNK_SIZE;

    ctx->input.buffer.start = malloc(ctx->input.buffer.capacity);
    if (!ctx->input.buffer.start) {
        DISPLAY("Error: could not allocate input buffer\n");
        return 1;
    }
    return 0;
}

static adaptCCtx* createCCtx(unsigned numJobs)
{

    adaptCCtx* const ctx = calloc(1, sizeof(adaptCCtx));
    if (ctx == NULL) {
        DISPLAY("Error: could not allocate space for context\n");
        return NULL;
    }
    {
        int const error = initCCtx(ctx, numJobs);
        if (error) {
            freeCCtx(ctx);
            return NULL;
        }
        return ctx;
    }
}

static void signalErrorToThreads(adaptCCtx* ctx)
{
    ctx->threadError = 1;
    pthread_mutex_lock(&ctx->jobReady_mutex.pMutex);
    pthread_cond_signal(&ctx->jobReady_cond.pCond);
    pthread_mutex_unlock(&ctx->jobReady_mutex.pMutex);

    pthread_mutex_lock(&ctx->jobCompressed_mutex.pMutex);
    pthread_cond_broadcast(&ctx->jobCompressed_cond.pCond);
    pthread_mutex_unlock(&ctx->jobReady_mutex.pMutex);

    pthread_mutex_lock(&ctx->jobWrite_mutex.pMutex);
    pthread_cond_signal(&ctx->jobWrite_cond.pCond);
    pthread_mutex_unlock(&ctx->jobWrite_mutex.pMutex);

    pthread_mutex_lock(&ctx->allJobsCompleted_mutex.pMutex);
    pthread_cond_signal(&ctx->allJobsCompleted_cond.pCond);
    pthread_mutex_unlock(&ctx->allJobsCompleted_mutex.pMutex);
}

static void waitUntilAllJobsCompleted(adaptCCtx* ctx)
{
    if (!ctx) return;
    pthread_mutex_lock(&ctx->allJobsCompleted_mutex.pMutex);
    while (ctx->allJobsCompleted == 0 && !ctx->threadError) {
        pthread_cond_wait(&ctx->allJobsCompleted_cond.pCond, &ctx->allJobsCompleted_mutex.pMutex);
    }
    pthread_mutex_unlock(&ctx->allJobsCompleted_mutex.pMutex);
}

/* map completion percentages to values for changing compression level */
static unsigned convertCompletionToChange(double completion)
{
    if (completion < CHANGE_BY_TWO_THRESHOLD) {
        return 2;
    }
    else if (completion < CHANGE_BY_ONE_THRESHOLD) {
        return 1;
    }
    else {
        return 0;
    }
}

/*
 * Compression level is changed depending on which part of the compression process is lagging
 * Currently, three theads exist for job creation, compression, and file writing respectively.
 * adaptCompressionLevel() increments or decrements compression level based on which of the threads is lagging
 * job creation or file writing lag => increased compression level
 * compression thread lag           => decreased compression level
 * detecting which thread is lagging is done by keeping track of how many calls each thread makes to pthread_cond_wait
 */
static void adaptCompressionLevel(adaptCCtx* ctx)
{
    double createWaitCompressionCompletion;
    double compressWaitCreateCompletion;
    double compressWaitWriteCompletion;
    double writeWaitCompressionCompletion;
    double const threshold = 0.00001;
    unsigned prevCompressionLevel;

    pthread_mutex_lock(&ctx->compressionLevel_mutex.pMutex);
    prevCompressionLevel = ctx->compressionLevel;
    pthread_mutex_unlock(&ctx->compressionLevel_mutex.pMutex);


    if (g_forceCompressionLevel) {
        pthread_mutex_lock(&ctx->compressionLevel_mutex.pMutex);
        ctx->compressionLevel = g_compressionLevel;
        pthread_mutex_unlock(&ctx->compressionLevel_mutex.pMutex);
        return;
    }


    DEBUG(2, "adapting compression level %u\n", prevCompressionLevel);

    /* read and reset completion measurements */
    pthread_mutex_lock(&ctx->compressionCompletion_mutex.pMutex);
    DEBUG(2, "createWaitCompressionCompletion %f\n", ctx->createWaitCompressionCompletion);
    DEBUG(2, "writeWaitCompressionCompletion %f\n", ctx->writeWaitCompressionCompletion);
    createWaitCompressionCompletion = ctx->createWaitCompressionCompletion;
    writeWaitCompressionCompletion = ctx->writeWaitCompressionCompletion;
    pthread_mutex_unlock(&ctx->compressionCompletion_mutex.pMutex);

    pthread_mutex_lock(&ctx->writeCompletion_mutex.pMutex);
    DEBUG(2, "compressWaitWriteCompletion %f\n", ctx->compressWaitWriteCompletion);
    compressWaitWriteCompletion = ctx->compressWaitWriteCompletion;
    pthread_mutex_unlock(&ctx->writeCompletion_mutex.pMutex);

    pthread_mutex_lock(&ctx->createCompletion_mutex.pMutex);
    DEBUG(2, "compressWaitCreateCompletion %f\n", ctx->compressWaitCreateCompletion);
    compressWaitCreateCompletion = ctx->compressWaitCreateCompletion;
    pthread_mutex_unlock(&ctx->createCompletion_mutex.pMutex);
    DEBUG(2, "convergence counter: %u\n", ctx->convergenceCounter);

    assert(g_minCLevel <= prevCompressionLevel && g_maxCLevel >= prevCompressionLevel);

    /* adaptation logic */
    if (ctx->cooldown) ctx->cooldown--;

    if ((1-createWaitCompressionCompletion > threshold || 1-writeWaitCompressionCompletion > threshold) && ctx->cooldown == 0) {
        /* create or write waiting on compression */
        /* use whichever one waited less because it was slower */
        double const completion = MAX(createWaitCompressionCompletion, writeWaitCompressionCompletion);
        unsigned const change = convertCompletionToChange(completion);
        unsigned const boundChange = MIN(change, prevCompressionLevel - g_minCLevel);
        if (ctx->convergenceCounter >= CONVERGENCE_LOWER_BOUND && boundChange != 0) {
            /* reset convergence counter, might have been a spike */
            ctx->convergenceCounter = 0;
            DEBUG(2, "convergence counter reset, no change applied\n");
        }
        else if (boundChange != 0) {
            pthread_mutex_lock(&ctx->compressionLevel_mutex.pMutex);
            ctx->compressionLevel -= boundChange;
            pthread_mutex_unlock(&ctx->compressionLevel_mutex.pMutex);
            ctx->cooldown = CLEVEL_DECREASE_COOLDOWN;
            ctx->convergenceCounter = 1;

            DEBUG(2, "create or write threads waiting on compression, tried to decrease compression level by %u\n\n", boundChange);
        }
    }
    else if (1-compressWaitWriteCompletion > threshold || 1-compressWaitCreateCompletion > threshold) {
        /* compress waiting on write */
        double const completion = MIN(compressWaitWriteCompletion, compressWaitCreateCompletion);
        unsigned const change = convertCompletionToChange(completion);
        unsigned const boundChange = MIN(change, g_maxCLevel - prevCompressionLevel);
        if (ctx->convergenceCounter >= CONVERGENCE_LOWER_BOUND && boundChange != 0) {
            /* reset convergence counter, might have been a spike */
            ctx->convergenceCounter = 0;
            DEBUG(2, "convergence counter reset, no change applied\n");
        }
        else if (boundChange != 0) {
            pthread_mutex_lock(&ctx->compressionLevel_mutex.pMutex);
            ctx->compressionLevel += boundChange;
            pthread_mutex_unlock(&ctx->compressionLevel_mutex.pMutex);
            ctx->cooldown = 0;
            ctx->convergenceCounter = 1;

            DEBUG(2, "compress waiting on write or create, tried to increase compression level by %u\n\n", boundChange);
        }

    }

    pthread_mutex_lock(&ctx->compressionLevel_mutex.pMutex);
    if (ctx->compressionLevel == prevCompressionLevel) {
        ctx->convergenceCounter++;
    }
    pthread_mutex_unlock(&ctx->compressionLevel_mutex.pMutex);
}

static size_t getUseableDictSize(unsigned compressionLevel)
{
    ZSTD_parameters const params = ZSTD_getParams(compressionLevel, 0, 0);
    unsigned const overlapLog = compressionLevel >= (unsigned)ZSTD_maxCLevel() ? 0 : 3;
    size_t const overlapSize = 1 << (params.cParams.windowLog - overlapLog);
    return overlapSize;
}

static void* compressionThread(void* arg)
{
    adaptCCtx* const ctx = (adaptCCtx*)arg;
    unsigned currJob = 0;
    for ( ; ; ) {
        unsigned const currJobIndex = currJob % ctx->numJobs;
        jobDescription* const job = &ctx->jobs[currJobIndex];
        DEBUG(2, "starting compression for job %u\n", currJob);

        {
            /* check if compression thread will have to wait */
            unsigned willWaitForCreate = 0;
            unsigned willWaitForWrite = 0;

            pthread_mutex_lock(&ctx->jobReady_mutex.pMutex);
            if (currJob + 1 > ctx->jobReadyID) willWaitForCreate = 1;
            pthread_mutex_unlock(&ctx->jobReady_mutex.pMutex);

            pthread_mutex_lock(&ctx->jobWrite_mutex.pMutex);
            if (currJob - ctx->jobWriteID >= ctx->numJobs) willWaitForWrite = 1;
            pthread_mutex_unlock(&ctx->jobWrite_mutex.pMutex);


            pthread_mutex_lock(&ctx->createCompletion_mutex.pMutex);
            if (willWaitForCreate) {
                DEBUG(2, "compression will wait for create on job %u\n", currJob);
                ctx->compressWaitCreateCompletion = ctx->createCompletion;
                DEBUG(2, "create completion %f\n", ctx->compressWaitCreateCompletion);

            }
            else {
                ctx->compressWaitCreateCompletion = 1;
            }
            pthread_mutex_unlock(&ctx->createCompletion_mutex.pMutex);

            pthread_mutex_lock(&ctx->writeCompletion_mutex.pMutex);
            if (willWaitForWrite) {
                DEBUG(2, "compression will wait for write on job %u\n", currJob);
                ctx->compressWaitWriteCompletion = ctx->writeCompletion;
                DEBUG(2, "write completion %f\n", ctx->compressWaitWriteCompletion);
            }
            else {
                ctx->compressWaitWriteCompletion = 1;
            }
            pthread_mutex_unlock(&ctx->writeCompletion_mutex.pMutex);

        }

        /* wait until job is ready */
        pthread_mutex_lock(&ctx->jobReady_mutex.pMutex);
        while (currJob + 1 > ctx->jobReadyID && !ctx->threadError) {
            pthread_cond_wait(&ctx->jobReady_cond.pCond, &ctx->jobReady_mutex.pMutex);
        }
        pthread_mutex_unlock(&ctx->jobReady_mutex.pMutex);

        /* wait until job previously in this space is written */
        pthread_mutex_lock(&ctx->jobWrite_mutex.pMutex);
        while (currJob - ctx->jobWriteID >= ctx->numJobs && !ctx->threadError) {
            pthread_cond_wait(&ctx->jobWrite_cond.pCond, &ctx->jobWrite_mutex.pMutex);
        }
        pthread_mutex_unlock(&ctx->jobWrite_mutex.pMutex);
        /* reset compression completion */
        pthread_mutex_lock(&ctx->compressionCompletion_mutex.pMutex);
        ctx->compressionCompletion = 0;
        pthread_mutex_unlock(&ctx->compressionCompletion_mutex.pMutex);

        /* adapt compression level */
        if (currJob) adaptCompressionLevel(ctx);

        pthread_mutex_lock(&ctx->compressionLevel_mutex.pMutex);
        DEBUG(2, "job %u compressed with level %u\n", currJob, ctx->compressionLevel);
        pthread_mutex_unlock(&ctx->compressionLevel_mutex.pMutex);

        /* compress the data */
        {
            size_t const compressionBlockSize = ZSTD_BLOCKSIZE_MAX; /* 128 KB */
            unsigned cLevel;
            unsigned blockNum = 0;
            size_t remaining = job->src.size;
            size_t srcPos = 0;
            size_t dstPos = 0;

            pthread_mutex_lock(&ctx->compressionLevel_mutex.pMutex);
            cLevel = ctx->compressionLevel;
            pthread_mutex_unlock(&ctx->compressionLevel_mutex.pMutex);

            /* reset compressed size */
            job->compressedSize = 0;
            DEBUG(2, "calling ZSTD_compressBegin()\n");
            /* begin compression */
            {
                size_t const useDictSize = MIN(getUseableDictSize(cLevel), job->dictSize);
                ZSTD_parameters params = ZSTD_getParams(cLevel, 0, useDictSize);
                params.cParams.windowLog = 23;
                {
                    size_t const initError = ZSTD_compressBegin_advanced(ctx->cctx, job->src.start + job->dictSize - useDictSize, useDictSize, params, 0);
                    size_t const windowSizeError = ZSTD_CCtx_setParameter(ctx->cctx, ZSTD_c_forceMaxWindow, 1);
                    if (ZSTD_isError(initError) || ZSTD_isError(windowSizeError)) {
                        DISPLAY("Error: something went wrong while starting compression\n");
                        signalErrorToThreads(ctx);
                        return arg;
                    }
                }
            }
            DEBUG(2, "finished with ZSTD_compressBegin()\n");

            do {
                size_t const actualBlockSize = MIN(remaining, compressionBlockSize);

                /* continue compression */
                if (currJob != 0 || blockNum != 0) { /* not first block of first job flush/overwrite the frame header */
                    size_t const hSize = ZSTD_compressContinue(ctx->cctx, job->dst.start + dstPos, job->dst.capacity - dstPos, job->src.start + job->dictSize + srcPos, 0);
                    if (ZSTD_isError(hSize)) {
                        DISPLAY("Error: something went wrong while continuing compression\n");
                        job->compressedSize = hSize;
                        signalErrorToThreads(ctx);
                        return arg;
                    }
                    ZSTD_invalidateRepCodes(ctx->cctx);
                }
                {
                    size_t const ret = (job->lastJobPlusOne == currJob + 1 && remaining == actualBlockSize) ?
                                            ZSTD_compressEnd     (ctx->cctx, job->dst.start + dstPos, job->dst.capacity - dstPos, job->src.start + job->dictSize + srcPos, actualBlockSize) :
                                            ZSTD_compressContinue(ctx->cctx, job->dst.start + dstPos, job->dst.capacity - dstPos, job->src.start + job->dictSize + srcPos, actualBlockSize);
                    if (ZSTD_isError(ret)) {
                        DISPLAY("Error: something went wrong during compression: %s\n", ZSTD_getErrorName(ret));
                        signalErrorToThreads(ctx);
                        return arg;
                    }
                    job->compressedSize += ret;
                    remaining -= actualBlockSize;
                    srcPos += actualBlockSize;
                    dstPos += ret;
                    blockNum++;

                    /* update completion */
                    pthread_mutex_lock(&ctx->compressionCompletion_mutex.pMutex);
                    ctx->compressionCompletion = 1 - (double)remaining/job->src.size;
                    pthread_mutex_unlock(&ctx->compressionCompletion_mutex.pMutex);
                }
            } while (remaining != 0);
            job->dst.size = job->compressedSize;
        }
        pthread_mutex_lock(&ctx->jobCompressed_mutex.pMutex);
        ctx->jobCompressedID++;
        pthread_cond_broadcast(&ctx->jobCompressed_cond.pCond);
        pthread_mutex_unlock(&ctx->jobCompressed_mutex.pMutex);
        if (job->lastJobPlusOne == currJob + 1 || ctx->threadError) {
            /* finished compressing all jobs */
            break;
        }
        DEBUG(2, "finished compressing job %u\n", currJob);
        currJob++;
    }
    return arg;
}

static void displayProgress(unsigned cLevel, unsigned last)
{
    UTIL_time_t currTime = UTIL_getTime();
    if (!g_useProgressBar) return;
    {   double const timeElapsed = (double)(UTIL_getSpanTimeMicro(g_startTime, currTime) / 1000.0);
        double const sizeMB = (double)g_streamedSize / (1 << 20);
        double const avgCompRate = sizeMB * 1000 / timeElapsed;
        fprintf(stderr, "\r| Comp. Level: %2u | Time Elapsed: %7.2f s | Data Size: %7.1f MB | Avg Comp. Rate: %6.2f MB/s |", cLevel, timeElapsed/1000.0, sizeMB, avgCompRate);
        if (last) {
            fprintf(stderr, "\n");
        } else {
            fflush(stderr);
    }   }
}

static void* outputThread(void* arg)
{
    outputThreadArg* const otArg = (outputThreadArg*)arg;
    adaptCCtx* const ctx = otArg->ctx;
    FILE* const dstFile = otArg->dstFile;

    unsigned currJob = 0;
    for ( ; ; ) {
        unsigned const currJobIndex = currJob % ctx->numJobs;
        jobDescription* const job = &ctx->jobs[currJobIndex];
        unsigned willWaitForCompress = 0;
        DEBUG(2, "starting write for job %u\n", currJob);

        pthread_mutex_lock(&ctx->jobCompressed_mutex.pMutex);
        if (currJob + 1 > ctx->jobCompressedID) willWaitForCompress = 1;
        pthread_mutex_unlock(&ctx->jobCompressed_mutex.pMutex);


        pthread_mutex_lock(&ctx->compressionCompletion_mutex.pMutex);
        if (willWaitForCompress) {
            /* write thread is waiting on compression thread */
            ctx->writeWaitCompressionCompletion = ctx->compressionCompletion;
            DEBUG(2, "writer thread waiting for nextJob: %u, writeWaitCompressionCompletion %f\n", currJob, ctx->writeWaitCompressionCompletion);
        }
        else {
            ctx->writeWaitCompressionCompletion = 1;
        }
        pthread_mutex_unlock(&ctx->compressionCompletion_mutex.pMutex);

        pthread_mutex_lock(&ctx->jobCompressed_mutex.pMutex);
        while (currJob + 1 > ctx->jobCompressedID && !ctx->threadError) {
            pthread_cond_wait(&ctx->jobCompressed_cond.pCond, &ctx->jobCompressed_mutex.pMutex);
        }
        pthread_mutex_unlock(&ctx->jobCompressed_mutex.pMutex);

        /* reset write completion */
        pthread_mutex_lock(&ctx->writeCompletion_mutex.pMutex);
        ctx->writeCompletion = 0;
        pthread_mutex_unlock(&ctx->writeCompletion_mutex.pMutex);

        {
            size_t const compressedSize = job->compressedSize;
            size_t remaining = compressedSize;
            if (ZSTD_isError(compressedSize)) {
                DISPLAY("Error: an error occurred during compression\n");
                signalErrorToThreads(ctx);
                return arg;
            }
            {
                size_t const blockSize = MAX(compressedSize >> 7, 1 << 10);
                size_t pos = 0;
                for ( ; ; ) {
                    size_t const writeSize = MIN(remaining, blockSize);
                    size_t const ret = fwrite(job->dst.start + pos, 1, writeSize, dstFile);
                    if (ret != writeSize) break;
                    pos += ret;
                    remaining -= ret;

                    /* update completion variable for writing */
                    pthread_mutex_lock(&ctx->writeCompletion_mutex.pMutex);
                    ctx->writeCompletion = 1 - (double)remaining/compressedSize;
                    pthread_mutex_unlock(&ctx->writeCompletion_mutex.pMutex);

                    if (remaining == 0) break;
                }
                if (pos != compressedSize) {
                    DISPLAY("Error: an error occurred during file write operation\n");
                    signalErrorToThreads(ctx);
                    return arg;
                }
            }
        }
        {
            unsigned cLevel;
            pthread_mutex_lock(&ctx->compressionLevel_mutex.pMutex);
            cLevel = ctx->compressionLevel;
            pthread_mutex_unlock(&ctx->compressionLevel_mutex.pMutex);
            displayProgress(cLevel, job->lastJobPlusOne == currJob + 1);
        }
        pthread_mutex_lock(&ctx->jobWrite_mutex.pMutex);
        ctx->jobWriteID++;
        pthread_cond_signal(&ctx->jobWrite_cond.pCond);
        pthread_mutex_unlock(&ctx->jobWrite_mutex.pMutex);

        if (job->lastJobPlusOne == currJob + 1 || ctx->threadError) {
            /* finished with all jobs */
            pthread_mutex_lock(&ctx->allJobsCompleted_mutex.pMutex);
            ctx->allJobsCompleted = 1;
            pthread_cond_signal(&ctx->allJobsCompleted_cond.pCond);
            pthread_mutex_unlock(&ctx->allJobsCompleted_mutex.pMutex);
            break;
        }
        DEBUG(2, "finished writing job %u\n", currJob);
        currJob++;

    }
    return arg;
}

static int createCompressionJob(adaptCCtx* ctx, size_t srcSize, int last)
{
    unsigned const nextJob = ctx->nextJobID;
    unsigned const nextJobIndex = nextJob % ctx->numJobs;
    jobDescription* const job = &ctx->jobs[nextJobIndex];


    job->src.size = srcSize;
    job->jobID = nextJob;
    if (last) job->lastJobPlusOne = nextJob + 1;
    {
        /* swap buffer */
        void* const copy = job->src.start;
        job->src.start = ctx->input.buffer.start;
        ctx->input.buffer.start = copy;
    }
    job->dictSize = ctx->lastDictSize;

    ctx->nextJobID++;
    /* if not on the last job, reuse data as dictionary in next job */
    if (!last) {
        size_t const oldDictSize = ctx->lastDictSize;
        memcpy(ctx->input.buffer.start, job->src.start + oldDictSize, srcSize);
        ctx->lastDictSize = srcSize;
        ctx->input.filled = srcSize;
    }

    /* signal job ready */
    pthread_mutex_lock(&ctx->jobReady_mutex.pMutex);
    ctx->jobReadyID++;
    pthread_cond_signal(&ctx->jobReady_cond.pCond);
    pthread_mutex_unlock(&ctx->jobReady_mutex.pMutex);

    return 0;
}

static int performCompression(adaptCCtx* ctx, FILE* const srcFile, outputThreadArg* otArg)
{
    /* early error check to exit */
    if (!ctx || !srcFile || !otArg) {
        return 1;
    }

    /* create output thread */
    {
        pthread_t out;
        if (pthread_create(&out, NULL, &outputThread, otArg)) {
            DISPLAY("Error: could not create output thread\n");
            signalErrorToThreads(ctx);
            return 1;
        }
        else if (pthread_detach(out)) {
        	DISPLAY("Error: could not detach output thread\n");
        	signalErrorToThreads(ctx);
        	return 1;
        }
    }

    /* create compression thread */
    {
        pthread_t compression;
        if (pthread_create(&compression, NULL, &compressionThread, ctx)) {
            DISPLAY("Error: could not create compression thread\n");
            signalErrorToThreads(ctx);
            return 1;
        }
        else if (pthread_detach(compression)) {
        	DISPLAY("Error: could not detach compression thread\n");
        	signalErrorToThreads(ctx);
        	return 1;
        }
    }
    {
        unsigned currJob = 0;
        /* creating jobs */
        for ( ; ; ) {
            size_t pos = 0;
            size_t const readBlockSize = 1 << 15;
            size_t remaining = FILE_CHUNK_SIZE;
            unsigned const nextJob = ctx->nextJobID;
            unsigned willWaitForCompress = 0;
            DEBUG(2, "starting creation of job %u\n", currJob);

            pthread_mutex_lock(&ctx->jobCompressed_mutex.pMutex);
            if (nextJob - ctx->jobCompressedID >= ctx->numJobs) willWaitForCompress = 1;
            pthread_mutex_unlock(&ctx->jobCompressed_mutex.pMutex);

            pthread_mutex_lock(&ctx->compressionCompletion_mutex.pMutex);
            if (willWaitForCompress) {
                /* creation thread is waiting, take measurement of completion */
                ctx->createWaitCompressionCompletion = ctx->compressionCompletion;
                DEBUG(2, "create thread waiting for nextJob: %u, createWaitCompressionCompletion %f\n", nextJob, ctx->createWaitCompressionCompletion);
            }
            else {
                ctx->createWaitCompressionCompletion = 1;
            }
            pthread_mutex_unlock(&ctx->compressionCompletion_mutex.pMutex);

            /* wait until the job has been compressed */
            pthread_mutex_lock(&ctx->jobCompressed_mutex.pMutex);
            while (nextJob - ctx->jobCompressedID >= ctx->numJobs && !ctx->threadError) {
                pthread_cond_wait(&ctx->jobCompressed_cond.pCond, &ctx->jobCompressed_mutex.pMutex);
            }
            pthread_mutex_unlock(&ctx->jobCompressed_mutex.pMutex);

            /* reset create completion */
            pthread_mutex_lock(&ctx->createCompletion_mutex.pMutex);
            ctx->createCompletion = 0;
            pthread_mutex_unlock(&ctx->createCompletion_mutex.pMutex);

            while (remaining != 0 && !feof(srcFile)) {
                size_t const ret = fread(ctx->input.buffer.start + ctx->input.filled + pos, 1, readBlockSize, srcFile);
                if (ret != readBlockSize && !feof(srcFile)) {
                    /* error could not read correct number of bytes */
                    DISPLAY("Error: problem occurred during read from src file\n");
                    signalErrorToThreads(ctx);
                    return 1;
                }
                pos += ret;
                remaining -= ret;
                pthread_mutex_lock(&ctx->createCompletion_mutex.pMutex);
                ctx->createCompletion = 1 - (double)remaining/((size_t)FILE_CHUNK_SIZE);
                pthread_mutex_unlock(&ctx->createCompletion_mutex.pMutex);
            }
            if (remaining != 0 && !feof(srcFile)) {
                DISPLAY("Error: problem occurred during read from src file\n");
                signalErrorToThreads(ctx);
                return 1;
            }
            g_streamedSize += pos;
            /* reading was fine, now create the compression job */
            {
                int const last = feof(srcFile);
                int const error = createCompressionJob(ctx, pos, last);
                if (error != 0) {
                    signalErrorToThreads(ctx);
                    return error;
                }
            }
            DEBUG(2, "finished creating job %u\n", currJob);
            currJob++;
            if (feof(srcFile)) {
                break;
            }
        }
    }
    /* success -- created all jobs */
    return 0;
}

static fcResources createFileCompressionResources(const char* const srcFilename, const char* const dstFilenameOrNull)
{
    fcResources fcr;
    unsigned const stdinUsed = !strcmp(srcFilename, stdinmark);
    FILE* const srcFile = stdinUsed ? stdin : fopen(srcFilename, "rb");
    const char* const outFilenameIntermediate = (stdinUsed && !dstFilenameOrNull) ? stdoutmark : dstFilenameOrNull;
    const char* outFilename = outFilenameIntermediate;
    char fileAndSuffix[MAX_PATH];
    size_t const numJobs = MAX_NUM_JOBS;

    memset(&fcr, 0, sizeof(fcr));

    if (!outFilenameIntermediate) {
        if (snprintf(fileAndSuffix, MAX_PATH, "%s.zst", srcFilename) + 1 > MAX_PATH) {
            DISPLAY("Error: output filename is too long\n");
            return fcr;
        }
        outFilename = fileAndSuffix;
    }

    {
        unsigned const stdoutUsed = !strcmp(outFilename, stdoutmark);
        FILE* const dstFile = stdoutUsed ? stdout : fopen(outFilename, "wb");
        fcr.otArg = malloc(sizeof(outputThreadArg));
        if (!fcr.otArg) {
            DISPLAY("Error: could not allocate space for output thread argument\n");
            return fcr;
        }
        fcr.otArg->dstFile = dstFile;
    }
    /* checking for errors */
    if (!fcr.otArg->dstFile || !srcFile) {
        DISPLAY("Error: some file(s) could not be opened\n");
        return fcr;
    }

    /* creating context */
    fcr.ctx = createCCtx(numJobs);
    fcr.otArg->ctx = fcr.ctx;
    fcr.srcFile = srcFile;
    return fcr;
}

static int freeFileCompressionResources(fcResources* fcr)
{
    int ret = 0;
    waitUntilAllJobsCompleted(fcr->ctx);
    ret |= (fcr->srcFile != NULL) ? fclose(fcr->srcFile) : 0;
    ret |= (fcr->ctx != NULL) ? freeCCtx(fcr->ctx) : 0;
    if (fcr->otArg) {
        ret |= (fcr->otArg->dstFile != stdout) ? fclose(fcr->otArg->dstFile) : 0;
        free(fcr->otArg);
        /* no need to freeCCtx() on otArg->ctx because it should be the same context */
    }
    return ret;
}

static int compressFilename(const char* const srcFilename, const char* const dstFilenameOrNull)
{
    int ret = 0;
    fcResources fcr = createFileCompressionResources(srcFilename, dstFilenameOrNull);
    g_streamedSize = 0;
    ret |= performCompression(fcr.ctx, fcr.srcFile, fcr.otArg);
    ret |= freeFileCompressionResources(&fcr);
    return ret;
}

static int compressFilenames(const char** filenameTable, unsigned numFiles, unsigned forceStdout)
{
    int ret = 0;
    unsigned fileNum;
    for (fileNum=0; fileNum<numFiles; fileNum++) {
        const char* filename = filenameTable[fileNum];
        if (!forceStdout) {
            ret |= compressFilename(filename, NULL);
        }
        else {
            ret |= compressFilename(filename, stdoutmark);
        }

    }
    return ret;
}

/*! readU32FromChar() :
    @return : unsigned integer value read from input in `char` format
    allows and interprets K, KB, KiB, M, MB and MiB suffix.
    Will also modify `*stringPtr`, advancing it to position where it stopped reading.
    Note : function result can overflow if digit string > MAX_UINT */
static unsigned readU32FromChar(const char** stringPtr)
{
    unsigned result = 0;
    while ((**stringPtr >='0') && (**stringPtr <='9'))
        result *= 10, result += **stringPtr - '0', (*stringPtr)++ ;
    if ((**stringPtr=='K') || (**stringPtr=='M')) {
        result <<= 10;
        if (**stringPtr=='M') result <<= 10;
        (*stringPtr)++ ;
        if (**stringPtr=='i') (*stringPtr)++;
        if (**stringPtr=='B') (*stringPtr)++;
    }
    return result;
}

static void help(const char* progPath)
{
    PRINT("Usage:\n");
    PRINT("  %s [options] [file(s)]\n", progPath);
    PRINT("\n");
    PRINT("Options:\n");
    PRINT("  -oFILE : specify the output file name\n");
    PRINT("  -i#    : provide initial compression level -- default %d, must be in the range [L, U] where L and U are bound values (see below for defaults)\n", DEFAULT_COMPRESSION_LEVEL);
    PRINT("  -h     : display help/information\n");
    PRINT("  -f     : force the compression level to stay constant\n");
    PRINT("  -c     : force write to stdout\n");
    PRINT("  -p     : hide progress bar\n");
    PRINT("  -q     : quiet mode -- do not show progress bar or other information\n");
    PRINT("  -l#    : provide lower bound for compression level -- default 1\n");
    PRINT("  -u#    : provide upper bound for compression level -- default %u\n", ZSTD_maxCLevel());
}
/* return 0 if successful, else return error */
int main(int argCount, const char* argv[])
{
    const char* outFilename = NULL;
    const char** filenameTable = (const char**)malloc(argCount*sizeof(const char*));
    unsigned filenameIdx = 0;
    unsigned forceStdout = 0;
    unsigned providedInitialCLevel = 0;
    int ret = 0;
    int argNum;
    filenameTable[0] = stdinmark;
    g_maxCLevel = ZSTD_maxCLevel();

    if (filenameTable == NULL) {
        DISPLAY("Error: could not allocate sapce for filename table.\n");
        return 1;
    }

    for (argNum=1; argNum<argCount; argNum++) {
        const char* argument = argv[argNum];

        /* output filename designated with "-o" */
        if (argument[0]=='-' && strlen(argument) > 1) {
            switch (argument[1]) {
                case 'o':
                    argument += 2;
                    outFilename = argument;
                    break;
                case 'i':
                    argument += 2;
                    g_compressionLevel = readU32FromChar(&argument);
                    providedInitialCLevel = 1;
                    break;
                case 'h':
                    help(argv[0]);
                    goto _main_exit;
                case 'p':
                    g_useProgressBar = 0;
                    break;
                case 'c':
                    forceStdout = 1;
                    outFilename = stdoutmark;
                    break;
                case 'f':
                    g_forceCompressionLevel = 1;
                    break;
                case 'q':
                    g_useProgressBar = 0;
                    g_displayLevel = 0;
                    break;
                case 'l':
                    argument += 2;
                    g_minCLevel = readU32FromChar(&argument);
                    break;
                case 'u':
                    argument += 2;
                    g_maxCLevel = readU32FromChar(&argument);
                    break;
                default:
                    DISPLAY("Error: invalid argument provided\n");
                    ret = 1;
                    goto _main_exit;
            }
            continue;
        }

        /* regular files to be compressed */
        filenameTable[filenameIdx++] = argument;
    }

    /* check initial, max, and min compression levels */
    {
        unsigned const minMaxInconsistent = g_minCLevel > g_maxCLevel;
        unsigned const initialNotInRange = g_minCLevel > g_compressionLevel || g_maxCLevel < g_compressionLevel;
        if (minMaxInconsistent || (initialNotInRange && providedInitialCLevel)) {
            DISPLAY("Error: provided compression level parameters are invalid\n");
            ret = 1;
            goto _main_exit;
        }
        else if (initialNotInRange) {
            g_compressionLevel = g_minCLevel;
        }
    }

    /* error checking with number of files */
    if (filenameIdx > 1 && (outFilename != NULL && strcmp(outFilename, stdoutmark))) {
        DISPLAY("Error: multiple input files provided, cannot use specified output file\n");
        ret = 1;
        goto _main_exit;
    }

    /* compress files */
    if (filenameIdx <= 1) {
        ret |= compressFilename(filenameTable[0], outFilename);
    }
    else {
        ret |= compressFilenames(filenameTable, filenameIdx, forceStdout);
    }
_main_exit:
    free(filenameTable);
    return ret;
}
