/*
  LZ4cli - LZ4 Command Line Interface
  Copyright (C) Yann Collet 2011-2016

  GPL v2 License

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; either version 2 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along
  with this program; if not, write to the Free Software Foundation, Inc.,
  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

  You can contact the author at :
  - LZ4 source repository : https://github.com/lz4/lz4
  - LZ4 public forum : https://groups.google.com/forum/#!forum/lz4c
*/
/*
  Note : this is stand-alone program.
  It is not part of LZ4 compression library, it is a user program of the LZ4 library.
  The license of LZ4 library is BSD.
  The license of xxHash library is BSD.
  The license of this compression CLI program is GPLv2.
*/


/****************************
*  Includes
*****************************/
#include "platform.h" /* Compiler options, IS_CONSOLE */
#include "util.h"     /* UTIL_HAS_CREATEFILELIST, UTIL_createFileList */
#include <stdio.h>    /* fprintf, getchar */
#include <stdlib.h>   /* exit, calloc, free */
#include <string.h>   /* strcmp, strlen */
#include "bench.h"    /* BMK_benchFile, BMK_SetNbIterations, BMK_SetBlocksize, BMK_SetPause */
#include "lz4io.h"    /* LZ4IO_compressFilename, LZ4IO_decompressFilename, LZ4IO_compressMultipleFilenames */
#include "lz4hc.h"    /* LZ4HC_CLEVEL_MAX */
#include "lz4.h"      /* LZ4_VERSION_STRING */


/*****************************
*  Constants
******************************/
#define COMPRESSOR_NAME "LZ4 command line interface"
#define AUTHOR "Yann Collet"
#define WELCOME_MESSAGE "*** %s %i-bits v%s, by %s ***\n", COMPRESSOR_NAME, (int)(sizeof(void*)*8), LZ4_versionString(), AUTHOR
#define LZ4_EXTENSION ".lz4"
#define LZ4CAT "lz4cat"
#define UNLZ4 "unlz4"
#define LZ4_LEGACY "lz4c"
static int g_lz4c_legacy_commands = 0;

#define KB *(1U<<10)
#define MB *(1U<<20)
#define GB *(1U<<30)

#define LZ4_BLOCKSIZEID_DEFAULT 7


/*-************************************
*  Macros
***************************************/
#define DISPLAY(...)           fprintf(stderr, __VA_ARGS__)
#define DISPLAYLEVEL(l, ...)   if (displayLevel>=l) { DISPLAY(__VA_ARGS__); }
static unsigned displayLevel = 2;   /* 0 : no display ; 1: errors only ; 2 : downgradable normal ; 3 : non-downgradable normal; 4 : + information */


/*-************************************
*  Exceptions
***************************************/
#define DEBUG 0
#define DEBUGOUTPUT(...) if (DEBUG) DISPLAY(__VA_ARGS__);
#define EXM_THROW(error, ...)                                             \
{                                                                         \
    DEBUGOUTPUT("Error defined at %s, line %i : \n", __FILE__, __LINE__); \
    DISPLAYLEVEL(1, "Error %i : ", error);                                \
    DISPLAYLEVEL(1, __VA_ARGS__);                                         \
    DISPLAYLEVEL(1, "\n");                                                \
    exit(error);                                                          \
}


/*-************************************
*  Version modifiers
***************************************/
#define DEFAULT_COMPRESSOR   LZ4IO_compressFilename
#define DEFAULT_DECOMPRESSOR LZ4IO_decompressFilename
int LZ4IO_compressFilename_Legacy(LZ4IO_prefs_t* const prefs, const char* input_filename, const char* output_filename, int compressionlevel);   /* hidden function */


/*-***************************
*  Functions
*****************************/
static int usage(const char* exeName)
{
    DISPLAY( "Usage : \n");
    DISPLAY( "      %s [arg] [input] [output] \n", exeName);
    DISPLAY( "\n");
    DISPLAY( "input   : a filename \n");
    DISPLAY( "          with no FILE, or when FILE is - or %s, read standard input\n", stdinmark);
    DISPLAY( "Arguments : \n");
    DISPLAY( " -1     : Fast compression (default) \n");
    DISPLAY( " -9     : High compression \n");
    DISPLAY( " -d     : decompression (default for %s extension)\n", LZ4_EXTENSION);
    DISPLAY( " -z     : force compression \n");
    DISPLAY( " -D FILE: use FILE as dictionary \n");
    DISPLAY( " -f     : overwrite output without prompting \n");
    DISPLAY( " -k     : preserve source files(s)  (default) \n");
    DISPLAY( "--rm    : remove source file(s) after successful de/compression \n");
    DISPLAY( " -h/-H  : display help/long help and exit \n");
    return 0;
}

static int usage_advanced(const char* exeName)
{
    DISPLAY(WELCOME_MESSAGE);
    usage(exeName);
    DISPLAY( "\n");
    DISPLAY( "Advanced arguments :\n");
    DISPLAY( " -V     : display Version number and exit \n");
    DISPLAY( " -v     : verbose mode \n");
    DISPLAY( " -q     : suppress warnings; specify twice to suppress errors too\n");
    DISPLAY( " -c     : force write to standard output, even if it is the console\n");
    DISPLAY( " -t     : test compressed file integrity\n");
    DISPLAY( " -m     : multiple input files (implies automatic output filenames)\n");
#ifdef UTIL_HAS_CREATEFILELIST
    DISPLAY( " -r     : operate recursively on directories (sets also -m) \n");
#endif
    DISPLAY( " -l     : compress using Legacy format (Linux kernel compression)\n");
    DISPLAY( " -B#    : cut file into blocks of size # bytes [32+] \n");
    DISPLAY( "                     or predefined block size [4-7] (default: 7) \n");
    DISPLAY( " -BI    : Block Independence (default) \n");
    DISPLAY( " -BD    : Block dependency (improves compression ratio) \n");
    DISPLAY( " -BX    : enable block checksum (default:disabled) \n");
    DISPLAY( "--no-frame-crc : disable stream checksum (default:enabled) \n");
    DISPLAY( "--content-size : compressed frame includes original size (default:not present)\n");
    DISPLAY( "--list  : lists information about .lz4 files. Useful if compressed with --content-size flag.\n");
    DISPLAY( "--[no-]sparse  : sparse mode (default:enabled on file, disabled on stdout)\n");
    DISPLAY( "--favor-decSpeed: compressed files decompress faster, but are less compressed \n");
    DISPLAY( "--fast[=#]: switch to ultra fast compression level (default: %i)\n", 1);
    DISPLAY( "Benchmark arguments : \n");
    DISPLAY( " -b#    : benchmark file(s), using # compression level (default : 1) \n");
    DISPLAY( " -e#    : test all compression levels from -bX to # (default : 1)\n");
    DISPLAY( " -i#    : minimum evaluation time in seconds (default : 3s) \n");
    if (g_lz4c_legacy_commands) {
        DISPLAY( "Legacy arguments : \n");
        DISPLAY( " -c0    : fast compression \n");
        DISPLAY( " -c1    : high compression \n");
        DISPLAY( " -c2,-hc: very high compression \n");
        DISPLAY( " -y     : overwrite output without prompting \n");
    }
    return 0;
}

static int usage_longhelp(const char* exeName)
{
    usage_advanced(exeName);
    DISPLAY( "\n");
    DISPLAY( "****************************\n");
    DISPLAY( "***** Advanced comment *****\n");
    DISPLAY( "****************************\n");
    DISPLAY( "\n");
    DISPLAY( "Which values can [output] have ? \n");
    DISPLAY( "---------------------------------\n");
    DISPLAY( "[output] : a filename \n");
    DISPLAY( "          '%s', or '-' for standard output (pipe mode)\n", stdoutmark);
    DISPLAY( "          '%s' to discard output (test mode) \n", NULL_OUTPUT);
    DISPLAY( "[output] can be left empty. In this case, it receives the following value :\n");
    DISPLAY( "          - if stdout is not the console, then [output] = stdout \n");
    DISPLAY( "          - if stdout is console : \n");
    DISPLAY( "               + for compression, output to filename%s \n", LZ4_EXTENSION);
    DISPLAY( "               + for decompression, output to filename without '%s'\n", LZ4_EXTENSION);
    DISPLAY( "                    > if input filename has no '%s' extension : error \n", LZ4_EXTENSION);
    DISPLAY( "\n");
    DISPLAY( "Compression levels : \n");
    DISPLAY( "---------------------\n");
    DISPLAY( "-0 ... -2  => Fast compression, all identicals\n");
    DISPLAY( "-3 ... -%d => High compression; higher number == more compression but slower\n", LZ4HC_CLEVEL_MAX);
    DISPLAY( "\n");
    DISPLAY( "stdin, stdout and the console : \n");
    DISPLAY( "--------------------------------\n");
    DISPLAY( "To protect the console from binary flooding (bad argument mistake)\n");
    DISPLAY( "%s will refuse to read from console, or write to console \n", exeName);
    DISPLAY( "except if '-c' command is specified, to force output to console \n");
    DISPLAY( "\n");
    DISPLAY( "Simple example :\n");
    DISPLAY( "----------------\n");
    DISPLAY( "1 : compress 'filename' fast, using default output name 'filename.lz4'\n");
    DISPLAY( "          %s filename\n", exeName);
    DISPLAY( "\n");
    DISPLAY( "Short arguments can be aggregated. For example :\n");
    DISPLAY( "----------------------------------\n");
    DISPLAY( "2 : compress 'filename' in high compression mode, overwrite output if exists\n");
    DISPLAY( "          %s -9 -f filename \n", exeName);
    DISPLAY( "    is equivalent to :\n");
    DISPLAY( "          %s -9f filename \n", exeName);
    DISPLAY( "\n");
    DISPLAY( "%s can be used in 'pure pipe mode'. For example :\n", exeName);
    DISPLAY( "-------------------------------------\n");
    DISPLAY( "3 : compress data stream from 'generator', send result to 'consumer'\n");
    DISPLAY( "          generator | %s | consumer \n", exeName);
    if (g_lz4c_legacy_commands) {
        DISPLAY( "\n");
        DISPLAY( "***** Warning  ***** \n");
        DISPLAY( "Legacy arguments take precedence. Therefore : \n");
        DISPLAY( "--------------------------------- \n");
        DISPLAY( "          %s -hc filename \n", exeName);
        DISPLAY( "means 'compress filename in high compression mode' \n");
        DISPLAY( "It is not equivalent to : \n");
        DISPLAY( "          %s -h -c filename \n", exeName);
        DISPLAY( "which displays help text and exits \n");
    }
    return 0;
}

static int badusage(const char* exeName)
{
    DISPLAYLEVEL(1, "Incorrect parameters\n");
    if (displayLevel >= 1) usage(exeName);
    exit(1);
}


static void waitEnter(void)
{
    DISPLAY("Press enter to continue...\n");
    (void)getchar();
}

static const char* lastNameFromPath(const char* path)
{
    const char* name = path;
    if (strrchr(name, '/')) name = strrchr(name, '/') + 1;
    if (strrchr(name, '\\')) name = strrchr(name, '\\') + 1; /* windows */
    return name;
}

/*! exeNameMatch() :
    @return : a non-zero value if exeName matches test, excluding the extension
   */
static int exeNameMatch(const char* exeName, const char* test)
{
    return !strncmp(exeName, test, strlen(test)) &&
        (exeName[strlen(test)] == '\0' || exeName[strlen(test)] == '.');
}

/*! readU32FromChar() :
 * @return : unsigned integer value read from input in `char` format
 *  allows and interprets K, KB, KiB, M, MB and MiB suffix.
 *  Will also modify `*stringPtr`, advancing it to position where it stopped reading.
 *  Note : function result can overflow if digit string > MAX_UINT */
static unsigned readU32FromChar(const char** stringPtr)
{
    unsigned result = 0;
    while ((**stringPtr >='0') && (**stringPtr <='9')) {
        result *= 10;
        result += (unsigned)(**stringPtr - '0');
        (*stringPtr)++ ;
    }
    if ((**stringPtr=='K') || (**stringPtr=='M')) {
        result <<= 10;
        if (**stringPtr=='M') result <<= 10;
        (*stringPtr)++ ;
        if (**stringPtr=='i') (*stringPtr)++;
        if (**stringPtr=='B') (*stringPtr)++;
    }
    return result;
}

/** longCommandWArg() :
 *  check if *stringPtr is the same as longCommand.
 *  If yes, @return 1 and advances *stringPtr to the position which immediately follows longCommand.
 * @return 0 and doesn't modify *stringPtr otherwise.
 */
static int longCommandWArg(const char** stringPtr, const char* longCommand)
{
    size_t const comSize = strlen(longCommand);
    int const result = !strncmp(*stringPtr, longCommand, comSize);
    if (result) *stringPtr += comSize;
    return result;
}

typedef enum { om_auto, om_compress, om_decompress, om_test, om_bench, om_list } operationMode_e;

/** determineOpMode() :
 *  auto-determine operation mode, based on input filename extension
 *  @return `om_decompress` if input filename has .lz4 extension and `om_compress` otherwise.
 */
static operationMode_e determineOpMode(const char* inputFilename)
{
    size_t const inSize  = strlen(inputFilename);
    size_t const extSize = strlen(LZ4_EXTENSION);
    size_t const extStart= (inSize > extSize) ? inSize-extSize : 0;
    if (!strcmp(inputFilename+extStart, LZ4_EXTENSION)) return om_decompress;
    else return om_compress;
}

int main(int argc, const char** argv)
{
    int i,
        cLevel=1,
        cLevelLast=-10000,
        legacy_format=0,
        forceStdout=0,
        main_pause=0,
        multiple_inputs=0,
        all_arguments_are_files=0,
        operationResult=0;
    operationMode_e mode = om_auto;
    const char* input_filename = NULL;
    const char* output_filename= NULL;
    const char* dictionary_filename = NULL;
    char* dynNameSpace = NULL;
    const char** inFileNames = (const char**)calloc((size_t)argc, sizeof(char*));
    unsigned ifnIdx=0;
    LZ4IO_prefs_t* const prefs = LZ4IO_defaultPreferences();
    const char nullOutput[] = NULL_OUTPUT;
    const char extension[] = LZ4_EXTENSION;
    size_t blockSize = LZ4IO_setBlockSizeID(prefs, LZ4_BLOCKSIZEID_DEFAULT);
    const char* const exeName = lastNameFromPath(argv[0]);
#ifdef UTIL_HAS_CREATEFILELIST
    const char** extendedFileList = NULL;
    char* fileNamesBuf = NULL;
    unsigned fileNamesNb, recursive=0;
#endif

    /* Init */
    if (inFileNames==NULL) {
        DISPLAY("Allocation error : not enough memory \n");
        return 1;
    }
    inFileNames[0] = stdinmark;
    LZ4IO_setOverwrite(prefs, 0);

    /* predefined behaviors, based on binary/link name */
    if (exeNameMatch(exeName, LZ4CAT)) {
        mode = om_decompress;
        LZ4IO_setOverwrite(prefs, 1);
        LZ4IO_setPassThrough(prefs, 1);
        LZ4IO_setRemoveSrcFile(prefs, 0);
        forceStdout=1;
        output_filename=stdoutmark;
        displayLevel=1;
        multiple_inputs=1;
    }
    if (exeNameMatch(exeName, UNLZ4)) { mode = om_decompress; }
    if (exeNameMatch(exeName, LZ4_LEGACY)) { g_lz4c_legacy_commands=1; }

    /* command switches */
    for(i=1; i<argc; i++) {
        const char* argument = argv[i];

        if(!argument) continue;   /* Protection if argument empty */

        /* Short commands (note : aggregated short commands are allowed) */
        if (!all_arguments_are_files && argument[0]=='-') {
            /* '-' means stdin/stdout */
            if (argument[1]==0) {
                if (!input_filename) input_filename=stdinmark;
                else output_filename=stdoutmark;
                continue;
            }

            /* long commands (--long-word) */
            if (argument[1]=='-') {
                if (!strcmp(argument,  "--")) { all_arguments_are_files = 1; continue; }
                if (!strcmp(argument,  "--compress")) { mode = om_compress; continue; }
                if ((!strcmp(argument, "--decompress"))
                    || (!strcmp(argument, "--uncompress"))) { mode = om_decompress; continue; }
                if (!strcmp(argument,  "--multiple")) { multiple_inputs = 1; continue; }
                if (!strcmp(argument,  "--test")) { mode = om_test; continue; }
                if (!strcmp(argument,  "--force")) { LZ4IO_setOverwrite(prefs, 1); continue; }
                if (!strcmp(argument,  "--no-force")) { LZ4IO_setOverwrite(prefs, 0); continue; }
                if ((!strcmp(argument, "--stdout"))
                    || (!strcmp(argument, "--to-stdout"))) { forceStdout=1; output_filename=stdoutmark; continue; }
                if (!strcmp(argument,  "--frame-crc")) { LZ4IO_setStreamChecksumMode(prefs, 1); continue; }
                if (!strcmp(argument,  "--no-frame-crc")) { LZ4IO_setStreamChecksumMode(prefs, 0); continue; }
                if (!strcmp(argument,  "--content-size")) { LZ4IO_setContentSize(prefs, 1); continue; }
                if (!strcmp(argument,  "--no-content-size")) { LZ4IO_setContentSize(prefs, 0); continue; }
                if (!strcmp(argument,  "--list")) { mode = om_list; continue; }
                if (!strcmp(argument,  "--sparse")) { LZ4IO_setSparseFile(prefs, 2); continue; }
                if (!strcmp(argument,  "--no-sparse")) { LZ4IO_setSparseFile(prefs, 0); continue; }
                if (!strcmp(argument,  "--favor-decSpeed")) { LZ4IO_favorDecSpeed(prefs, 1); continue; }
                if (!strcmp(argument,  "--verbose")) { displayLevel++; continue; }
                if (!strcmp(argument,  "--quiet")) { if (displayLevel) displayLevel--; continue; }
                if (!strcmp(argument,  "--version")) { DISPLAY(WELCOME_MESSAGE); return 0; }
                if (!strcmp(argument,  "--help")) { usage_advanced(exeName); goto _cleanup; }
                if (!strcmp(argument,  "--keep")) { LZ4IO_setRemoveSrcFile(prefs, 0); continue; }   /* keep source file (default) */
                if (!strcmp(argument,  "--rm")) { LZ4IO_setRemoveSrcFile(prefs, 1); continue; }
                if (longCommandWArg(&argument, "--fast")) {
                        /* Parse optional acceleration factor */
                        if (*argument == '=') {
                            U32 fastLevel;
                            ++argument;
                            fastLevel = readU32FromChar(&argument);
                            if (fastLevel) {
                              cLevel = -(int)fastLevel;
                            } else {
                              badusage(exeName);
                            }
                        } else if (*argument != 0) {
                            /* Invalid character following --fast */
                            badusage(exeName);
                        } else {
                            cLevel = -1;  /* default for --fast */
                        }
                        continue;
                    }
            }

            while (argument[1]!=0) {
                argument ++;

                if (g_lz4c_legacy_commands) {
                    /* Legacy commands (-c0, -c1, -hc, -y) */
                    if (!strcmp(argument,  "c0")) { cLevel=0; argument++; continue; }  /* -c0 (fast compression) */
                    if (!strcmp(argument,  "c1")) { cLevel=9; argument++; continue; }  /* -c1 (high compression) */
                    if (!strcmp(argument,  "c2")) { cLevel=12; argument++; continue; } /* -c2 (very high compression) */
                    if (!strcmp(argument,  "hc")) { cLevel=12; argument++; continue; } /* -hc (very high compression) */
                    if (!strcmp(argument,  "y"))  { LZ4IO_setOverwrite(prefs, 1); continue; } /* -y (answer 'yes' to overwrite permission) */
                }

                if ((*argument>='0') && (*argument<='9')) {
                    cLevel = (int)readU32FromChar(&argument);
                    argument--;
                    continue;
                }


                switch(argument[0])
                {
                    /* Display help */
                case 'V': DISPLAY(WELCOME_MESSAGE); goto _cleanup;   /* Version */
                case 'h': usage_advanced(exeName); goto _cleanup;
                case 'H': usage_longhelp(exeName); goto _cleanup;

                case 'e':
                    argument++;
                    cLevelLast = (int)readU32FromChar(&argument);
                    argument--;
                    break;

                    /* Compression (default) */
                case 'z': mode = om_compress; break;

                case 'D':
                    if (argument[1] == '\0') {
                        /* path is next arg */
                        if (i + 1 == argc) {
                            /* there is no next arg */
                            badusage(exeName);
                        }
                        dictionary_filename = argv[++i];
                    } else {
                        /* path follows immediately */
                        dictionary_filename = argument + 1;
                    }
                    /* skip to end of argument so that we jump to parsing next argument */
                    argument += strlen(argument) - 1;
                    break;

                    /* Use Legacy format (ex : Linux kernel compression) */
                case 'l': legacy_format = 1; blockSize = 8 MB; break;

                    /* Decoding */
                case 'd': mode = om_decompress; break;

                    /* Force stdout, even if stdout==console */
                case 'c':
                  forceStdout=1;
                  output_filename=stdoutmark;
                  LZ4IO_setPassThrough(prefs, 1);
                  break;

                    /* Test integrity */
                case 't': mode = om_test; break;

                    /* Overwrite */
                case 'f': LZ4IO_setOverwrite(prefs, 1); break;

                    /* Verbose mode */
                case 'v': displayLevel++; break;

                    /* Quiet mode */
                case 'q': if (displayLevel) displayLevel--; break;

                    /* keep source file (default anyway, so useless) (for xz/lzma compatibility) */
                case 'k': LZ4IO_setRemoveSrcFile(prefs, 0); break;

                    /* Modify Block Properties */
                case 'B':
                    while (argument[1]!=0) {
                        int exitBlockProperties=0;
                        switch(argument[1])
                        {
                        case 'D': LZ4IO_setBlockMode(prefs, LZ4IO_blockLinked); argument++; break;
                        case 'I': LZ4IO_setBlockMode(prefs, LZ4IO_blockIndependent); argument++; break;
                        case 'X': LZ4IO_setBlockChecksumMode(prefs, 1); argument ++; break;   /* disabled by default */
                        default :
                            if (argument[1] < '0' || argument[1] > '9') {
                                exitBlockProperties=1;
                                break;
                            } else {
                                unsigned B;
                                argument++;
                                B = readU32FromChar(&argument);
                                argument--;
                                if (B < 4) badusage(exeName);
                                if (B <= 7) {
                                    blockSize = LZ4IO_setBlockSizeID(prefs, B);
                                    BMK_setBlockSize(blockSize);
                                    DISPLAYLEVEL(2, "using blocks of size %u KB \n", (U32)(blockSize>>10));
                                } else {
                                    if (B < 32) badusage(exeName);
                                    blockSize = LZ4IO_setBlockSize(prefs, B);
                                    BMK_setBlockSize(blockSize);
                                    if (blockSize >= 1024) {
                                        DISPLAYLEVEL(2, "using blocks of size %u KB \n", (U32)(blockSize>>10));
                                    } else {
                                        DISPLAYLEVEL(2, "using blocks of size %u bytes \n", (U32)(blockSize));
                                    }
                                }
                                break;
                            }
                        }
                        if (exitBlockProperties) break;
                    }
                    break;

                    /* Benchmark */
                case 'b': mode = om_bench; multiple_inputs=1;
                    break;

                    /* hidden command : benchmark files, but do not fuse result */
                case 'S': BMK_setBenchSeparately(1);
                    break;

#ifdef UTIL_HAS_CREATEFILELIST
                    /* recursive */
                case 'r': recursive=1;
#endif
                    /* fall-through */
                    /* Treat non-option args as input files.  See https://code.google.com/p/lz4/issues/detail?id=151 */
                case 'm': multiple_inputs=1;
                    break;

                    /* Modify Nb Seconds (benchmark only) */
                case 'i':
                    {   unsigned iters;
                        argument++;
                        iters = readU32FromChar(&argument);
                        argument--;
                        BMK_setNotificationLevel(displayLevel);
                        BMK_setNbSeconds(iters);   /* notification if displayLevel >= 3 */
                    }
                    break;

                    /* Pause at the end (hidden option) */
                case 'p': main_pause=1; break;

                    /* Unrecognised command */
                default : badusage(exeName);
                }
            }
            continue;
        }

        /* Store in *inFileNames[] if -m is used. */
        if (multiple_inputs) { inFileNames[ifnIdx++]=argument; continue; }

        /* Store first non-option arg in input_filename to preserve original cli logic. */
        if (!input_filename) { input_filename=argument; continue; }

        /* Second non-option arg in output_filename to preserve original cli logic. */
        if (!output_filename) {
            output_filename=argument;
            if (!strcmp (output_filename, nullOutput)) output_filename = nulmark;
            continue;
        }

        /* 3rd non-option arg should not exist */
        DISPLAYLEVEL(1, "Warning : %s won't be used ! Do you want multiple input files (-m) ? \n", argument);
    }

    DISPLAYLEVEL(3, WELCOME_MESSAGE);
#ifdef _POSIX_C_SOURCE
    DISPLAYLEVEL(4, "_POSIX_C_SOURCE defined: %ldL\n", (long) _POSIX_C_SOURCE);
#endif
#ifdef _POSIX_VERSION
    DISPLAYLEVEL(4, "_POSIX_VERSION defined: %ldL\n", (long) _POSIX_VERSION);
#endif
#ifdef PLATFORM_POSIX_VERSION
    DISPLAYLEVEL(4, "PLATFORM_POSIX_VERSION defined: %ldL\n", (long) PLATFORM_POSIX_VERSION);
#endif
#ifdef _FILE_OFFSET_BITS
    DISPLAYLEVEL(4, "_FILE_OFFSET_BITS defined: %ldL\n", (long) _FILE_OFFSET_BITS);
#endif
    if ((mode == om_compress) || (mode == om_bench))
        DISPLAYLEVEL(4, "Blocks size : %u KB\n", (U32)(blockSize>>10));

    if (multiple_inputs) {
        input_filename = inFileNames[0];
#ifdef UTIL_HAS_CREATEFILELIST
        if (recursive) {  /* at this stage, filenameTable is a list of paths, which can contain both files and directories */
            extendedFileList = UTIL_createFileList(inFileNames, ifnIdx, &fileNamesBuf, &fileNamesNb);
            if (extendedFileList) {
                unsigned u;
                for (u=0; u<fileNamesNb; u++) DISPLAYLEVEL(4, "%u %s\n", u, extendedFileList[u]);
                free((void*)inFileNames);
                inFileNames = extendedFileList;
                ifnIdx = fileNamesNb;
        }   }
#endif
    }

    /* benchmark and test modes */
    if (mode == om_bench) {
        BMK_setNotificationLevel(displayLevel);
        operationResult = BMK_benchFiles(inFileNames, ifnIdx, cLevel, cLevelLast);
        goto _cleanup;
    }

    if (mode == om_test) {
        LZ4IO_setTestMode(prefs, 1);
        output_filename = nulmark;
        mode = om_decompress;   /* defer to decompress */
    }

    if (dictionary_filename) {
        if (!strcmp(dictionary_filename, stdinmark) && IS_CONSOLE(stdin)) {
            DISPLAYLEVEL(1, "refusing to read from a console\n");
            exit(1);
        }
        LZ4IO_setDictionaryFilename(prefs, dictionary_filename);
    }

    /* compress or decompress */
    if (!input_filename) input_filename = stdinmark;
    /* Check if input is defined as console; trigger an error in this case */
    if (!strcmp(input_filename, stdinmark) && IS_CONSOLE(stdin) ) {
        DISPLAYLEVEL(1, "refusing to read from a console\n");
        exit(1);
    }
    /* if input==stdin and no output defined, stdout becomes default output */
    if (!strcmp(input_filename, stdinmark) && !output_filename)
        output_filename = stdoutmark;

    /* No output filename ==> try to select one automatically (when possible) */
    while ((!output_filename) && (multiple_inputs==0)) {

        if (!IS_CONSOLE(stdout)) {
            /* Default to stdout whenever stdout is not the console.
             * Note : this policy may change in the future, therefore don't rely on it !
             * To ensure `stdout` is explicitly selected, use `-c` command flag.
             * Conversely, to ensure output will not become `stdout`, use `-m` command flag */
            DISPLAYLEVEL(1, "Warning : using stdout as default output. Do not rely on this behavior: use explicit `-c` instead ! \n");
            output_filename=stdoutmark;
            break;
        }
        if (mode == om_auto) {  /* auto-determine compression or decompression, based on file extension */
            mode = determineOpMode(input_filename);
        }
        if (mode == om_compress) {   /* compression to file */
            size_t const l = strlen(input_filename);
            dynNameSpace = (char*)calloc(1,l+5);
            if (dynNameSpace==NULL) { perror(exeName); exit(1); }
            strcpy(dynNameSpace, input_filename);
            strcat(dynNameSpace, LZ4_EXTENSION);
            output_filename = dynNameSpace;
            DISPLAYLEVEL(2, "Compressed filename will be : %s \n", output_filename);
            break;
        }
        if (mode == om_decompress) {/* decompression to file (automatic name will work only if input filename has correct format extension) */
            size_t outl;
            size_t const inl = strlen(input_filename);
            dynNameSpace = (char*)calloc(1,inl+1);
            if (dynNameSpace==NULL) { perror(exeName); exit(1); }
            strcpy(dynNameSpace, input_filename);
            outl = inl;
            if (inl>4)
                while ((outl >= inl-4) && (input_filename[outl] ==  extension[outl-inl+4])) dynNameSpace[outl--]=0;
            if (outl != inl-5) { DISPLAYLEVEL(1, "Cannot determine an output filename\n"); badusage(exeName); }
            output_filename = dynNameSpace;
            DISPLAYLEVEL(2, "Decoding file %s \n", output_filename);
        }
        break;
    }

    if (multiple_inputs==0 && mode != om_list) assert(output_filename);
    /* when multiple_inputs==1, output_filename may simply be useless,
     * however, output_filename must be !NULL for next strcmp() tests */
    if (!output_filename) output_filename = "*\\dummy^!//";

    /* Check if output is defined as console; trigger an error in this case */
    if (!strcmp(output_filename,stdoutmark) && IS_CONSOLE(stdout) && !forceStdout) {
        DISPLAYLEVEL(1, "refusing to write to console without -c \n");
        exit(1);
    }
    /* Downgrade notification level in stdout and multiple file mode */
    if (!strcmp(output_filename,stdoutmark) && (displayLevel==2)) displayLevel=1;
    if ((multiple_inputs) && (displayLevel==2)) displayLevel=1;

    /* Auto-determine compression or decompression, based on file extension */
    if (mode == om_auto) {
        mode = determineOpMode(input_filename);
    }

    /* IO Stream/File */
    LZ4IO_setNotificationLevel((int)displayLevel);
    if (ifnIdx == 0) multiple_inputs = 0;
    if (mode == om_decompress) {
        if (multiple_inputs) {
            assert(ifnIdx <= INT_MAX);
            operationResult = LZ4IO_decompressMultipleFilenames(prefs, inFileNames, (int)ifnIdx, !strcmp(output_filename,stdoutmark) ? stdoutmark : LZ4_EXTENSION);
        } else {
            operationResult = DEFAULT_DECOMPRESSOR(prefs, input_filename, output_filename);
        }
    } else if (mode == om_list){
        if(!multiple_inputs){
            inFileNames[ifnIdx++] = input_filename;
        }
        operationResult = LZ4IO_displayCompressedFilesInfo(inFileNames, ifnIdx);
        inFileNames=NULL;
    } else {   /* compression is default action */
        if (legacy_format) {
            DISPLAYLEVEL(3, "! Generating LZ4 Legacy format (deprecated) ! \n");
            LZ4IO_compressFilename_Legacy(prefs, input_filename, output_filename, cLevel);
        } else {
            if (multiple_inputs) {
                assert(ifnIdx <= INT_MAX);
                operationResult = LZ4IO_compressMultipleFilenames(prefs, inFileNames, (int)ifnIdx, !strcmp(output_filename,stdoutmark) ? stdoutmark : LZ4_EXTENSION, cLevel);
            } else {
                operationResult = DEFAULT_COMPRESSOR(prefs, input_filename, output_filename, cLevel);
    }   }   }

_cleanup:
    if (main_pause) waitEnter();
    free(dynNameSpace);
#ifdef UTIL_HAS_CREATEFILELIST
    if (extendedFileList) {
        UTIL_freeFileList(extendedFileList, fileNamesBuf);
        inFileNames = NULL;
    }
#endif
    LZ4IO_freePreferences(prefs);
    free((void*)inFileNames);
    return operationResult;
}
