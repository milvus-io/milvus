Command Line Interface for LZ4 library
============================================

### Build
The Command Line Interface (CLI) can be generated
using the `make` command without any additional parameters.

The `Makefile` script supports all [standard conventions](https://www.gnu.org/prep/standards/html_node/Makefile-Conventions.html),
including standard targets (`all`, `install`, `clean`, etc.)
and standard variables (`CC`, `CFLAGS`, `CPPFLAGS`, etc.).

For advanced use cases, there are targets to different variations of the CLI:
- `lz4` : default CLI, with a command line syntax close to gzip
- `lz4c` : Same as `lz4` with additional support legacy lz4 commands (incompatible with gzip)
- `lz4c32` : Same as `lz4c`, but forced to compile in 32-bits mode

The CLI generates and decodes [LZ4-compressed frames](../doc/lz4_Frame_format.md).


#### Aggregation of parameters
CLI supports aggregation of parameters i.e. `-b1`, `-e18`, and `-i1` can be joined into `-b1e18i1`.


#### Benchmark in Command Line Interface
CLI includes in-memory compression benchmark module for lz4.
The benchmark is conducted using a given filename.
The file is read into memory.
It makes benchmark more precise as it eliminates I/O overhead.

The benchmark measures ratio, compressed size, compression and decompression speed.
One can select compression levels starting from `-b` and ending with `-e`.
The `-i` parameter selects a number of seconds used for each of tested levels.



#### Usage of Command Line Interface
The full list of commands can be obtained with `-h` or `-H` parameter:
```
Usage :
      lz4 [arg] [input] [output]

input   : a filename
          with no FILE, or when FILE is - or stdin, read standard input
Arguments :
 -1     : Fast compression (default)
 -9     : High compression
 -d     : decompression (default for .lz4 extension)
 -z     : force compression
 -D FILE: use FILE as dictionary
 -f     : overwrite output without prompting
 -k     : preserve source files(s)  (default)
--rm    : remove source file(s) after successful de/compression
 -h/-H  : display help/long help and exit

Advanced arguments :
 -V     : display Version number and exit
 -v     : verbose mode
 -q     : suppress warnings; specify twice to suppress errors too
 -c     : force write to standard output, even if it is the console
 -t     : test compressed file integrity
 -m     : multiple input files (implies automatic output filenames)
 -r     : operate recursively on directories (sets also -m)
 -l     : compress using Legacy format (Linux kernel compression)
 -B#    : cut file into blocks of size # bytes [32+]
                     or predefined block size [4-7] (default: 7)
 -BD    : Block dependency (improve compression ratio)
 -BX    : enable block checksum (default:disabled)
--no-frame-crc : disable stream checksum (default:enabled)
--content-size : compressed frame includes original size (default:not present)
--[no-]sparse  : sparse mode (default:enabled on file, disabled on stdout)
--favor-decSpeed: compressed files decompress faster, but are less compressed
--fast[=#]: switch to ultra fast compression level (default: 1)

Benchmark arguments :
 -b#    : benchmark file(s), using # compression level (default : 1)
 -e#    : test all compression levels from -bX to # (default : 1)
 -i#    : minimum evaluation time in seconds (default : 3s)```
```

#### License

All files in this directory are licensed under GPL-v2.
See [COPYING](COPYING) for details.
The text of the license is also included at the top of each source file.
