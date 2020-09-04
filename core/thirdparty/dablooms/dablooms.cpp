/* Copyright @2012 by Justin Hines at Bitly under a very liberal license. See LICENSE in the source distribution. */

#include <sys/stat.h>
#include <stdint.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include <fcntl.h>
#include <math.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>
#include <errno.h>

#include <iostream>

#include "murmur.h"
#include "dablooms.h"

#define DABLOOMS_VERSION "0.9.1"

#define ERROR_TIGHTENING_RATIO 0.5
#define SALT_CONSTANT 0x97c29b3a

const char *dablooms_version(void)
{
    return DABLOOMS_VERSION;
}

void free_bitmap(bitmap_t *bitmap)
{
    if ((munmap(bitmap->array, bitmap->bytes)) < 0) {
        perror("Error, unmapping memory");
    }
    close(bitmap->fd);
    free(bitmap);
}

bitmap_t *bitmap_resize(bitmap_t *bitmap, size_t old_size, size_t new_size)
{
    int fd = bitmap->fd;
    struct stat fileStat;

    fstat(fd, &fileStat);
    size_t size = fileStat.st_size;

    /* grow file if necessary */
    if (size < new_size) {
        if (ftruncate(fd, new_size) < 0) {
            perror("Error increasing file size with ftruncate");
            free_bitmap(bitmap);
            close(fd);
            return NULL;
        }
    }
    lseek(fd, 0, SEEK_SET);

    /* resize if mmap exists and possible on this os, else new mmap */
    if (bitmap->array != NULL) {
#if __linux
        bitmap->array = (char *)mremap(bitmap->array, old_size, new_size, MREMAP_MAYMOVE);
        if (bitmap->array == MAP_FAILED) {
            perror("Error resizing mmap");
            free_bitmap(bitmap);
            close(fd);
            return NULL;
        }
#else
        if (munmap(bitmap->array, bitmap->bytes) < 0) {
            perror("Error unmapping memory");
            free_bitmap(bitmap);
            close(fd);
            return NULL;
        }
        bitmap->array = NULL;
#endif
    }
    if (bitmap->array == NULL) {
        bitmap->array = (char *)mmap(0, new_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (bitmap->array == MAP_FAILED) {
            perror("Error init mmap");
            free_bitmap(bitmap);
            close(fd);
            return NULL;
        }
    }

    bitmap->bytes = new_size;
    return bitmap;
}

/* Create a new bitmap, not full featured, simple to give
 * us a means of interacting with the 4 bit counters */
bitmap_t *new_bitmap(int fd, size_t bytes)
{
    bitmap_t *bitmap;

    if ((bitmap = (bitmap_t *)malloc(sizeof(bitmap_t))) == NULL) {
        return NULL;
    }

    bitmap->bytes = bytes;
    bitmap->fd = fd;
    bitmap->array = NULL;

    if ((bitmap = bitmap_resize(bitmap, 0, bytes)) == NULL) {
        return NULL;
    }

    return bitmap;
}

int bitmap_increment(bitmap_t *bitmap, unsigned int index, long offset)
{
    long access = index / 2 + offset;
    uint8_t temp;
    uint8_t n = bitmap->array[access];
    if (index % 2 != 0) {
        temp = (n & 0x0f);
        n = (n & 0xf0) + ((n & 0x0f) + 0x01);
    } else {
        temp = (n & 0xf0) >> 4;
        n = (n & 0x0f) + ((n & 0xf0) + 0x10);
    }

    if (temp == 0x0f) {
//        fprintf(stderr, "Error, 4 bit int Overflow\n");
        return -1;
    }

    bitmap->array[access] = n;
    return 0;
}

/* increments the four bit counter */
int bitmap_decrement(bitmap_t *bitmap, unsigned int index, long offset)
{
    long access = index / 2 + offset;
    uint8_t temp;
    uint8_t n = bitmap->array[access];

    if (index % 2 != 0) {
        temp = (n & 0x0f);
        n = (n & 0xf0) + ((n & 0x0f) - 0x01);
    } else {
        temp = (n & 0xf0) >> 4;
        n = (n & 0x0f) + ((n & 0xf0) - 0x10);
    }

    if (temp == 0x00) {
//        fprintf(stderr, "Error, Decrementing zero\n");
//        fprintf(stderr, "Bloom filter Error: you have deleted the same id more than 15 times!\n");
        return -1;
    }

    bitmap->array[access] = n;
    return 0;
}

/* decrements the four bit counter */
int bitmap_check(bitmap_t *bitmap, unsigned int index, long offset)
{
    long access = index / 2 + offset;
    if (index % 2 != 0 ) {
        return bitmap->array[access] & 0x0f;
    } else {
        return bitmap->array[access] & 0xf0;
    }
}

int bitmap_flush(bitmap_t *bitmap)
{
    if ((msync(bitmap->array, bitmap->bytes, MS_ASYNC) < 0)) {
        perror("Error, flushing bitmap to disk");
        return -1;
    } else {
        return 0;
    }
}

/*
 * Perform the actual hashing for `key`
 *
 * Only call the hash once to get a pair of initial values (h1 and
 * h2). Use these values to generate all hashes in a quick loop.
 *
 * See paper by Kirsch, Mitzenmacher [2006]
 * http://www.eecs.harvard.edu/~michaelm/postscripts/rsa2008.pdf
 */
void hash_func(counting_bloom_t *bloom, const char *key, size_t key_len, uint32_t *hashes)
{
    int i;
    uint32_t checksum[4];

    MurmurHash3_x64_128(key, key_len, SALT_CONSTANT, checksum);
    uint32_t h1 = checksum[0];
    uint32_t h2 = checksum[1];

    for (i = 0; i < bloom->nfuncs; i++) {
        hashes[i] = (h1 + i * h2) % bloom->counts_per_func;
    }
}

counting_bloom_t *counting_bloom_init(unsigned int capacity, double error_rate, long offset)
{
    counting_bloom_t *bloom;

    if ((bloom = (counting_bloom_t *)malloc(sizeof(counting_bloom_t))) == NULL) {
        fprintf(stderr, "Error, could not realloc a new bloom filter\n");
        return NULL;
    }
    bloom->bitmap = NULL;
    bloom->capacity = capacity;
    bloom->error_rate = error_rate;
    bloom->offset = offset + sizeof(counting_bloom_header_t);
    bloom->nfuncs = (int) ceil(log(1 / error_rate) / log(2));
    bloom->counts_per_func = (int) ceil(capacity * fabs(log(error_rate)) / (bloom->nfuncs * pow(log(2), 2)));
    bloom->size = bloom->nfuncs * bloom->counts_per_func;
    /* rounding-up integer divide by 2 of bloom->size */
    bloom->num_bytes = ((bloom->size + 1) / 2) + sizeof(counting_bloom_header_t);
    bloom->hashes = (uint32_t *)calloc(bloom->nfuncs, sizeof(uint32_t));

    return bloom;
}

int counting_bloom_add(counting_bloom_t *bloom, const char *s, size_t len)
{
    unsigned int index, i, offset;
    unsigned int *hashes = bloom->hashes;

    hash_func(bloom, s, len, hashes);

    bool error = false;
    for (i = 0; i < bloom->nfuncs; i++) {
        offset = i * bloom->counts_per_func;
        index = hashes[i] + offset;
        if (bitmap_increment(bloom->bitmap, index, bloom->offset) == -1) {
            error = true;
        }
    }
    bloom->header->count++;

    //return 0;
    return error ? -1 : 0;
}

int counting_bloom_remove(counting_bloom_t *bloom, const char *s, size_t len)
{
    unsigned int index, i, offset;
    unsigned int *hashes = bloom->hashes;

    hash_func(bloom, s, len, hashes);

    bool error = false;
    for (i = 0; i < bloom->nfuncs; i++) {
        offset = i * bloom->counts_per_func;
        index = hashes[i] + offset;
        if (bitmap_decrement(bloom->bitmap, index, bloom->offset) == -1) {
            error = true;
        }
    }
    bloom->header->count--;

    //return 0;
    return error ? -1 : 0;
}

int counting_bloom_check(counting_bloom_t *bloom, const char *s, size_t len)
{
    unsigned int index, i, offset;
    unsigned int *hashes = bloom->hashes;

    hash_func(bloom, s, len, hashes);

    for (i = 0; i < bloom->nfuncs; i++) {
        offset = i * bloom->counts_per_func;
        index = hashes[i] + offset;
        if (!(bitmap_check(bloom->bitmap, index, bloom->offset))) {
            return 0;
        }
    }
    return 1;
}

int free_scaling_bloom(scaling_bloom_t *bloom)
{
    if(close(bloom->fd) == -1) {
        std::cerr << " Close fd " << bloom->fd << "Failed: " << strerror(errno) << std::endl;
    }

    int i;
    for (i = bloom->num_blooms - 1; i >= 0; i--) {
        free(bloom->blooms[i]->hashes);
        bloom->blooms[i]->hashes = NULL;
        free(bloom->blooms[i]);
        bloom->blooms[i] = NULL;
    }
    free(bloom->blooms);
    free_bitmap(bloom->bitmap);
    free(bloom);
    return 0;
}

/* creates a new counting bloom filter from a given scaling bloom filter, with count and id */
counting_bloom_t *new_counting_bloom_from_scale(scaling_bloom_t *bloom)
{
    int i;
    long offset;
    double error_rate;
    counting_bloom_t *cur_bloom;

    error_rate = bloom->error_rate * (pow(ERROR_TIGHTENING_RATIO, bloom->num_blooms + 1));

    if ((bloom->blooms = (counting_bloom_t **)realloc(bloom->blooms, (bloom->num_blooms + 1) * sizeof(counting_bloom_t *))) == NULL) {
        fprintf(stderr, "Error, could not realloc a new bloom filter\n");
        return NULL;
    }

    cur_bloom = counting_bloom_init(bloom->capacity, error_rate, bloom->num_bytes);
    bloom->blooms[bloom->num_blooms] = cur_bloom;

    bloom->bitmap = bitmap_resize(bloom->bitmap, bloom->num_bytes, bloom->num_bytes + cur_bloom->num_bytes);

    /* reset header pointer, as mmap may have moved */
    bloom->header = (scaling_bloom_header_t *) bloom->bitmap->array;

    /* Set the pointers for these header structs to the right location since mmap may have moved */
    bloom->num_blooms++;
    for (i = 0; i < bloom->num_blooms; i++) {
        offset = bloom->blooms[i]->offset - sizeof(counting_bloom_header_t);
        bloom->blooms[i]->header = (counting_bloom_header_t *) (bloom->bitmap->array + offset);
    }

    bloom->num_bytes += cur_bloom->num_bytes;
    cur_bloom->bitmap = bloom->bitmap;

    return cur_bloom;
}

uint64_t scaling_bloom_clear_seqnums(scaling_bloom_t *bloom)
{
    uint64_t seqnum = bloom->header->mem_seqnum;
    bloom->header->mem_seqnum = 0;
    return seqnum;
}

int scaling_bloom_add(scaling_bloom_t *bloom, const char *s, size_t len, uint64_t id)
{
    int i;
    uint64_t seqnum;

    counting_bloom_t *cur_bloom = NULL;
    for (i = bloom->num_blooms - 1; i >= 0; i--) {
        cur_bloom = bloom->blooms[i];
        if (id >= cur_bloom->header->id) {
            break;
        }
    }

    seqnum = scaling_bloom_clear_seqnums(bloom);

    if ((id > bloom->header->max_id) && (cur_bloom->header->count >= cur_bloom->capacity - 1)) {
        cur_bloom = new_counting_bloom_from_scale(bloom);
        cur_bloom->header->count = 0;
        cur_bloom->header->id = bloom->header->max_id + 1;
    }
    if (bloom->header->max_id < id) {
        bloom->header->max_id = id;
    }
    bool error = false;
    if (counting_bloom_add(cur_bloom, s, len) == -1) {
        error = true;
    }

    bloom->header->mem_seqnum = seqnum + 1;

    //return 1;
    return error ? -1 : 1;
}

int scaling_bloom_remove(scaling_bloom_t *bloom, const char *s, size_t len, uint64_t id)
{
    counting_bloom_t *cur_bloom;
    int i;
    uint64_t seqnum;

    bool error = false;
    for (i = bloom->num_blooms - 1; i >= 0; i--) {
        cur_bloom = bloom->blooms[i];
        if (id >= cur_bloom->header->id) {
            seqnum = scaling_bloom_clear_seqnums(bloom);

            if (counting_bloom_remove(cur_bloom, s, len) == -1) {
                error = true;
            }

            bloom->header->mem_seqnum = seqnum + 1;
            //return 1;
            return error ? -1 : 1;
        }
    }
    return 0;
}

int scaling_bloom_check(scaling_bloom_t *bloom, const char *s, size_t len)
{
    int i;
    counting_bloom_t *cur_bloom;
    for (i = bloom->num_blooms - 1; i >= 0; i--) {
        cur_bloom = bloom->blooms[i];
        if (counting_bloom_check(cur_bloom, s, len)) {
            return 1;
        }
    }
    return 0;
}

int scaling_bloom_flush(scaling_bloom_t *bloom)
{
    if (bitmap_flush(bloom->bitmap) != 0) {
        return -1;
    }

    return bitmap_flush(bloom->bitmap);
}

scaling_bloom_t *scaling_bloom_init(unsigned int capacity, double error_rate, int fd)
{
    scaling_bloom_t *bloom;

    if ((bloom = (scaling_bloom_t *)malloc(sizeof(scaling_bloom_t))) == NULL) {
        return NULL;
    }
    if ((bloom->bitmap = new_bitmap(fd, sizeof(scaling_bloom_header_t))) == NULL) {
        fprintf(stderr, "Error, Could not create bitmap with file\n");
        free_scaling_bloom(bloom);
        return NULL;
    }

    bloom->header = (scaling_bloom_header_t *) bloom->bitmap->array;
    bloom->capacity = capacity;
    bloom->error_rate = error_rate;
    bloom->num_blooms = 0;
    bloom->num_bytes = sizeof(scaling_bloom_header_t);
    bloom->fd = fd;
    bloom->blooms = NULL;

    return bloom;
}

scaling_bloom_t *new_scaling_bloom(unsigned int capacity, double error_rate, const char *filename)
{

    scaling_bloom_t *bloom;
    counting_bloom_t *cur_bloom;
    int fd;

    if ((fd = open(filename, O_RDWR | O_CREAT | O_TRUNC, (mode_t)0600)) < 0) {
        perror("Error, Opening File Failed");
        fprintf(stderr, " %s \n", filename);
        return NULL;
    }

    bloom = scaling_bloom_init(capacity, error_rate, fd);

    if (!(cur_bloom = new_counting_bloom_from_scale(bloom))) {
        fprintf(stderr, "Error, Could not create counting bloom\n");
        free_scaling_bloom(bloom);
        return NULL;
    }
    cur_bloom->header->count = 0;
    cur_bloom->header->id = 0;

    bloom->header->mem_seqnum = 1;
    return bloom;
}

scaling_bloom_t *new_scaling_bloom_from_file(unsigned int capacity, double error_rate, const char *filename)
{
    int fd;
    off_t size;

    scaling_bloom_t *bloom;
    counting_bloom_t *cur_bloom;

    if ((fd = open(filename, O_RDWR, (mode_t)0600)) < 0) {
        fprintf(stderr, "Error, Could not open file %s: %s\n", filename, strerror(errno));
        return NULL;
    }
    if ((size = lseek(fd, 0, SEEK_END)) < 0) {
        perror("Error, calling lseek() to tell file size");
        close(fd);
        return NULL;
    }
    if (size == 0) {
        fprintf(stderr, "Error, File size zero\n");
    }

    bloom = scaling_bloom_init(capacity, error_rate, fd);

    size -= sizeof(scaling_bloom_header_t);
    while (size) {
        cur_bloom = new_counting_bloom_from_scale(bloom);
        // leave count and id as they were set in the file
        size -= cur_bloom->num_bytes;
        if (size < 0) {
            free_scaling_bloom(bloom);
            fprintf(stderr, "Error, Actual filesize and expected filesize are not equal\n");
            return NULL;
        }
    }
    return bloom;
}
