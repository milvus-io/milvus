// LZ4 API example : Dictionary Random Access

#if defined(_MSC_VER) && (_MSC_VER <= 1800)  /* Visual Studio <= 2013 */
#  define _CRT_SECURE_NO_WARNINGS
#  define snprintf sprintf_s
#endif
#include "lz4.h"

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define MIN(x, y)  ((x) < (y) ? (x) : (y))

enum {
    BLOCK_BYTES = 1024,  /* 1 KiB of uncompressed data in a block */
    DICTIONARY_BYTES = 1024, /* Load a 1 KiB dictionary */
    MAX_BLOCKS = 1024 /* For simplicity of implementation */
};

/**
 * Magic bytes for this test case.
 * This is not a great magic number because it is a common word in ASCII.
 * However, it is important to have some versioning system in your format.
 */
const char kTestMagic[] = { 'T', 'E', 'S', 'T' };


void write_int(FILE* fp, int i) {
    size_t written = fwrite(&i, sizeof(i), 1, fp);
    if (written != 1) { exit(10); }
}

void write_bin(FILE* fp, const void* array, size_t arrayBytes) {
    size_t written = fwrite(array, 1, arrayBytes, fp);
    if (written != arrayBytes) { exit(11); }
}

void read_int(FILE* fp, int* i) {
    size_t read = fread(i, sizeof(*i), 1, fp);
    if (read != 1) { exit(12); }
}

size_t read_bin(FILE* fp, void* array, size_t arrayBytes) {
    size_t read = fread(array, 1, arrayBytes, fp);
    if (ferror(fp)) { exit(12); }
    return read;
}

void seek_bin(FILE* fp, long offset, int origin) {
    if (fseek(fp, offset, origin)) { exit(14); }
}


void test_compress(FILE* outFp, FILE* inpFp, void *dict, int dictSize)
{
    LZ4_stream_t lz4Stream_body;
    LZ4_stream_t* lz4Stream = &lz4Stream_body;

    char inpBuf[BLOCK_BYTES];
    int offsets[MAX_BLOCKS];
    int *offsetsEnd = offsets;


    LZ4_initStream(lz4Stream, sizeof(*lz4Stream));

    /* Write header magic */
    write_bin(outFp, kTestMagic, sizeof(kTestMagic));

    *offsetsEnd++ = sizeof(kTestMagic);
    /* Write compressed data blocks.  Each block contains BLOCK_BYTES of plain
       data except possibly the last. */
    for(;;) {
        const int inpBytes = (int) read_bin(inpFp, inpBuf, BLOCK_BYTES);
        if(0 == inpBytes) {
            break;
        }

        /* Forget previously compressed data and load the dictionary */
        LZ4_loadDict(lz4Stream, dict, dictSize);
        {
            char cmpBuf[LZ4_COMPRESSBOUND(BLOCK_BYTES)];
            const int cmpBytes = LZ4_compress_fast_continue(
                lz4Stream, inpBuf, cmpBuf, inpBytes, sizeof(cmpBuf), 1);
            if(cmpBytes <= 0) { exit(1); }
            write_bin(outFp, cmpBuf, (size_t)cmpBytes);
            /* Keep track of the offsets */
            *offsetsEnd = *(offsetsEnd - 1) + cmpBytes;
            ++offsetsEnd;
        }
        if (offsetsEnd - offsets > MAX_BLOCKS) { exit(2); }
    }
    /* Write the tailing jump table */
    {
        int *ptr = offsets;
        while (ptr != offsetsEnd) {
            write_int(outFp, *ptr++);
        }
        write_int(outFp, offsetsEnd - offsets);
    }
}


void test_decompress(FILE* outFp, FILE* inpFp, void *dict, int dictSize, int offset, int length)
{
    LZ4_streamDecode_t lz4StreamDecode_body;
    LZ4_streamDecode_t* lz4StreamDecode = &lz4StreamDecode_body;

    /* The blocks [currentBlock, endBlock) contain the data we want */
    int currentBlock = offset / BLOCK_BYTES;
    int endBlock = ((offset + length - 1) / BLOCK_BYTES) + 1;

    char decBuf[BLOCK_BYTES];
    int offsets[MAX_BLOCKS];

    /* Special cases */
    if (length == 0) { return; }

    /* Read the magic bytes */
    {
        char magic[sizeof(kTestMagic)];
        size_t read = read_bin(inpFp, magic, sizeof(magic));
        if (read != sizeof(magic)) { exit(1); }
        if (memcmp(kTestMagic, magic, sizeof(magic))) { exit(2); }
    }

    /* Read the offsets tail */
    {
        int numOffsets;
        int block;
        int *offsetsPtr = offsets;
        seek_bin(inpFp, -4, SEEK_END);
        read_int(inpFp, &numOffsets);
        if (numOffsets <= endBlock) { exit(3); }
        seek_bin(inpFp, -4 * (numOffsets + 1), SEEK_END);
        for (block = 0; block <= endBlock; ++block) {
            read_int(inpFp, offsetsPtr++);
        }
    }
    /* Seek to the first block to read */
    seek_bin(inpFp, offsets[currentBlock], SEEK_SET);
    offset = offset % BLOCK_BYTES;

    /* Start decoding */
    for(; currentBlock < endBlock; ++currentBlock) {
        char cmpBuf[LZ4_COMPRESSBOUND(BLOCK_BYTES)];
        /* The difference in offsets is the size of the block */
        int  cmpBytes = offsets[currentBlock + 1] - offsets[currentBlock];
        {
            const size_t read = read_bin(inpFp, cmpBuf, (size_t)cmpBytes);
            if(read != (size_t)cmpBytes) { exit(4); }
        }

        /* Load the dictionary */
        LZ4_setStreamDecode(lz4StreamDecode, dict, dictSize);
        {
            const int decBytes = LZ4_decompress_safe_continue(
                lz4StreamDecode, cmpBuf, decBuf, cmpBytes, BLOCK_BYTES);
            if(decBytes <= 0) { exit(5); }
            {
                /* Write out the part of the data we care about */
                int blockLength = MIN(length, (decBytes - offset));
                write_bin(outFp, decBuf + offset, (size_t)blockLength);
                offset = 0;
                length -= blockLength;
            }
        }
    }
}


int compare(FILE* fp0, FILE* fp1, int length)
{
    int result = 0;

    while(0 == result) {
        char b0[4096];
        char b1[4096];
        const size_t r0 = read_bin(fp0, b0, MIN(length, (int)sizeof(b0)));
        const size_t r1 = read_bin(fp1, b1, MIN(length, (int)sizeof(b1)));

        result = (int) r0 - (int) r1;

        if(0 == r0 || 0 == r1) {
            break;
        }
        if(0 == result) {
            result = memcmp(b0, b1, r0);
        }
        length -= r0;
    }

    return result;
}


int main(int argc, char* argv[])
{
    char inpFilename[256] = { 0 };
    char lz4Filename[256] = { 0 };
    char decFilename[256] = { 0 };
    char dictFilename[256] = { 0 };
    int offset;
    int length;
    char dict[DICTIONARY_BYTES];
    int dictSize;

    if(argc < 5) {
        printf("Usage: %s input dictionary offset length", argv[0]);
        return 0;
    }

    snprintf(inpFilename, 256, "%s", argv[1]);
    snprintf(lz4Filename, 256, "%s.lz4s-%d", argv[1], BLOCK_BYTES);
    snprintf(decFilename, 256, "%s.lz4s-%d.dec", argv[1], BLOCK_BYTES);
    snprintf(dictFilename, 256, "%s", argv[2]);
    offset = atoi(argv[3]);
    length = atoi(argv[4]);

    printf("inp    = [%s]\n", inpFilename);
    printf("lz4    = [%s]\n", lz4Filename);
    printf("dec    = [%s]\n", decFilename);
    printf("dict   = [%s]\n", dictFilename);
    printf("offset = [%d]\n", offset);
    printf("length = [%d]\n", length);

    /* Load dictionary */
    {
        FILE* dictFp = fopen(dictFilename, "rb");
        dictSize = (int)read_bin(dictFp, dict, DICTIONARY_BYTES);
        fclose(dictFp);
    }

    /* compress */
    {
        FILE* inpFp = fopen(inpFilename, "rb");
        FILE* outFp = fopen(lz4Filename, "wb");

        printf("compress : %s -> %s\n", inpFilename, lz4Filename);
        test_compress(outFp, inpFp, dict, dictSize);
        printf("compress : done\n");

        fclose(outFp);
        fclose(inpFp);
    }

    /* decompress */
    {
        FILE* inpFp = fopen(lz4Filename, "rb");
        FILE* outFp = fopen(decFilename, "wb");

        printf("decompress : %s -> %s\n", lz4Filename, decFilename);
        test_decompress(outFp, inpFp, dict, DICTIONARY_BYTES, offset, length);
        printf("decompress : done\n");

        fclose(outFp);
        fclose(inpFp);
    }

    /* verify */
    {
        FILE* inpFp = fopen(inpFilename, "rb");
        FILE* decFp = fopen(decFilename, "rb");
        seek_bin(inpFp, offset, SEEK_SET);

        printf("verify : %s <-> %s\n", inpFilename, decFilename);
        const int cmp = compare(inpFp, decFp, length);
        if(0 == cmp) {
            printf("verify : OK\n");
        } else {
            printf("verify : NG\n");
        }

        fclose(decFp);
        fclose(inpFp);
    }

    return 0;
}
