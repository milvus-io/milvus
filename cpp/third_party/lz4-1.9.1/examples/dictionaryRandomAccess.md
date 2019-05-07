# LZ4 API Example : Dictionary Random Access

`dictionaryRandomAccess.c` is LZ4 API example which implements dictionary compression and random access decompression.

Please note that the output file is not compatible with lz4frame and is platform dependent.


## What's the point of this example ?

 - Dictionary based compression for homogenous files.
 - Random access to compressed blocks.


## How the compression works

Reads the dictionary from a file, and uses it as the history for each block.
This allows each block to be independent, but maintains compression ratio.

```
    Dictionary
         +
         |
         v
    +---------+
    | Block#1 |
    +----+----+
         |
         v
      {Out#1}


    Dictionary
         +
         |
         v
    +---------+
    | Block#2 |
    +----+----+
         |
         v
      {Out#2}
```

After writing the magic bytes `TEST` and then the compressed blocks, write out the jump table.
The last 4 bytes is an integer containing the number of blocks in the stream.
If there are `N` blocks, then just before the last 4 bytes is `N + 1` 4 byte integers containing the offsets at the beginning and end of each block.
Let `Offset#K` be the total number of bytes written after writing out `Block#K` *including* the magic bytes for simplicity.

```
+------+---------+     +---------+---+----------+     +----------+-----+
| TEST | Block#1 | ... | Block#N | 4 | Offset#1 | ... | Offset#N | N+1 |
+------+---------+     +---------+---+----------+     +----------+-----+
```

## How the decompression works

Decompression will do reverse order.

 - Seek to the last 4 bytes of the file and read the number of offsets.
 - Read each offset into an array.
 - Seek to the first block containing data we want to read.
   We know where to look because we know each block contains a fixed amount of uncompressed data, except possibly the last.
 - Decompress it and write what data we need from it to the file.
 - Read the next block.
 - Decompress it and write that page to the file.

Continue these procedure until all the required data has been read.
