/*
  LZ4io.h - LZ4 File/Stream Interface
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
  It is not part of LZ4 compression library, it is a user code of the LZ4 library.
  - The license of LZ4 library is BSD.
  - The license of xxHash library is BSD.
  - The license of this source file is GPLv2.
*/

#ifndef LZ4IO_H_237902873
#define LZ4IO_H_237902873

/*---   Dependency   ---*/
#include <stddef.h>   /* size_t */


/* ************************************************** */
/* Special input/output values                        */
/* ************************************************** */
#define NULL_OUTPUT "null"
static const char stdinmark[]  = "stdin";
static const char stdoutmark[] = "stdout";
#ifdef _WIN32
static const char nulmark[] = "nul";
#else
static const char nulmark[] = "/dev/null";
#endif

/* ************************************************** */
/* ****************** Type Definitions ************** */
/* ************************************************** */

typedef struct LZ4IO_prefs_s LZ4IO_prefs_t;

LZ4IO_prefs_t* LZ4IO_defaultPreferences(void);
void LZ4IO_freePreferences(LZ4IO_prefs_t* const prefs);


/* ************************************************** */
/* ****************** Functions ********************* */
/* ************************************************** */

/* if output_filename == stdoutmark, writes to stdout */
int LZ4IO_compressFilename(LZ4IO_prefs_t* const prefs, const char* input_filename, const char* output_filename, int compressionlevel);
int LZ4IO_decompressFilename(LZ4IO_prefs_t* const prefs, const char* input_filename, const char* output_filename);

/* if suffix == stdoutmark, writes to stdout */
int LZ4IO_compressMultipleFilenames(LZ4IO_prefs_t* const prefs, const char** inFileNamesTable, int ifntSize, const char* suffix, int compressionlevel);
int LZ4IO_decompressMultipleFilenames(LZ4IO_prefs_t* const prefs, const char** inFileNamesTable, int ifntSize, const char* suffix);


/* ************************************************** */
/* ****************** Parameters ******************** */
/* ************************************************** */

int LZ4IO_setDictionaryFilename(LZ4IO_prefs_t* const prefs, const char* dictionaryFilename);

/* Default setting : passThrough = 0;
   return : passThrough mode (0/1) */
int LZ4IO_setPassThrough(LZ4IO_prefs_t* const prefs, int yes);

/* Default setting : overwrite = 1;
   return : overwrite mode (0/1) */
int LZ4IO_setOverwrite(LZ4IO_prefs_t* const prefs, int yes);

/* Default setting : testMode = 0;
   return : testMode (0/1) */
int LZ4IO_setTestMode(LZ4IO_prefs_t* const prefs, int yes);

/* blockSizeID : valid values : 4-5-6-7
   return : 0 if error, blockSize if OK */
size_t LZ4IO_setBlockSizeID(LZ4IO_prefs_t* const prefs, unsigned blockSizeID);

/* blockSize : valid values : 32 -> 4MB
   return : 0 if error, actual blocksize if OK */
size_t LZ4IO_setBlockSize(LZ4IO_prefs_t* const prefs, size_t blockSize);

/* Default setting : independent blocks */
typedef enum { LZ4IO_blockLinked=0, LZ4IO_blockIndependent} LZ4IO_blockMode_t;
int LZ4IO_setBlockMode(LZ4IO_prefs_t* const prefs, LZ4IO_blockMode_t blockMode);

/* Default setting : no block checksum */
int LZ4IO_setBlockChecksumMode(LZ4IO_prefs_t* const prefs, int xxhash);

/* Default setting : stream checksum enabled */
int LZ4IO_setStreamChecksumMode(LZ4IO_prefs_t* const prefs, int xxhash);

/* Default setting : 0 (no notification) */
int LZ4IO_setNotificationLevel(int level);

/* Default setting : 0 (disabled) */
int LZ4IO_setSparseFile(LZ4IO_prefs_t* const prefs, int enable);

/* Default setting : 0 == no content size present in frame header */
int LZ4IO_setContentSize(LZ4IO_prefs_t* const prefs, int enable);

/* Default setting : 0 == src file preserved */
void LZ4IO_setRemoveSrcFile(LZ4IO_prefs_t* const prefs, unsigned flag);

/* Default setting : 0 == favor compression ratio
 * Note : 1 only works for high compression levels (10+) */
void LZ4IO_favorDecSpeed(LZ4IO_prefs_t* const prefs, int favor);


/* implement --list
 * @return 0 on success, 1 on error */
int LZ4IO_displayCompressedFilesInfo(const char** inFileNames, size_t ifnIdx);


#endif  /* LZ4IO_H_237902873 */
