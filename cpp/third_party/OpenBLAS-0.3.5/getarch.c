/*****************************************************************************
Copyright (c) 2011-2014, The OpenBLAS Project
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

   1. Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.

   2. Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in
      the documentation and/or other materials provided with the
      distribution.
   3. Neither the name of the OpenBLAS project nor the names of 
      its contributors may be used to endorse or promote products 
      derived from this software without specific prior written 
      permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

**********************************************************************************/

/*********************************************************************/
/* Copyright 2009, 2010 The University of Texas at Austin.           */
/* All rights reserved.                                              */
/*                                                                   */
/* Redistribution and use in source and binary forms, with or        */
/* without modification, are permitted provided that the following   */
/* conditions are met:                                               */
/*                                                                   */
/*   1. Redistributions of source code must retain the above         */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer.                                                  */
/*                                                                   */
/*   2. Redistributions in binary form must reproduce the above      */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer in the documentation and/or other materials       */
/*      provided with the distribution.                              */
/*                                                                   */
/*    THIS  SOFTWARE IS PROVIDED  BY THE  UNIVERSITY OF  TEXAS AT    */
/*    AUSTIN  ``AS IS''  AND ANY  EXPRESS OR  IMPLIED WARRANTIES,    */
/*    INCLUDING, BUT  NOT LIMITED  TO, THE IMPLIED  WARRANTIES OF    */
/*    MERCHANTABILITY  AND FITNESS FOR  A PARTICULAR  PURPOSE ARE    */
/*    DISCLAIMED.  IN  NO EVENT SHALL THE UNIVERSITY  OF TEXAS AT    */
/*    AUSTIN OR CONTRIBUTORS BE  LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL,  SPECIAL, EXEMPLARY,  OR  CONSEQUENTIAL DAMAGES    */
/*    (INCLUDING, BUT  NOT LIMITED TO,  PROCUREMENT OF SUBSTITUTE    */
/*    GOODS  OR  SERVICES; LOSS  OF  USE,  DATA,  OR PROFITS;  OR    */
/*    BUSINESS INTERRUPTION) HOWEVER CAUSED  AND ON ANY THEORY OF    */
/*    LIABILITY, WHETHER  IN CONTRACT, STRICT  LIABILITY, OR TORT    */
/*    (INCLUDING NEGLIGENCE OR OTHERWISE)  ARISING IN ANY WAY OUT    */
/*    OF  THE  USE OF  THIS  SOFTWARE,  EVEN  IF ADVISED  OF  THE    */
/*    POSSIBILITY OF SUCH DAMAGE.                                    */
/*                                                                   */
/* The views and conclusions contained in the software and           */
/* documentation are those of the authors and should not be          */
/* interpreted as representing official policies, either expressed   */
/* or implied, of The University of Texas at Austin.                 */
/*********************************************************************/

#if defined(__WIN32__) || defined(__WIN64__) || defined(__CYGWIN32__) || defined(__CYGWIN64__) || defined(_WIN32) || defined(_WIN64)
#define OS_WINDOWS
#endif

#if defined(__i386__) || defined(__x86_64__) || defined(_M_IX86) || defined(_M_X64)
#define INTEL_AMD
#endif

#include <stdio.h>
#include <string.h>
#ifdef OS_WINDOWS
#include <windows.h>
#endif
#if defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__DragonFly__) || defined(__APPLE__)
#include <sys/types.h>
#include <sys/sysctl.h>
#endif
#if defined(linux) || defined(__sun__)
#include <sys/sysinfo.h>
#include <unistd.h>
#endif

/* #define FORCE_P2		*/
/* #define FORCE_KATMAI		*/
/* #define FORCE_COPPERMINE	*/
/* #define FORCE_NORTHWOOD	*/
/* #define FORCE_PRESCOTT	*/
/* #define FORCE_BANIAS		*/
/* #define FORCE_YONAH		*/
/* #define FORCE_CORE2		*/
/* #define FORCE_PENRYN		*/
/* #define FORCE_DUNNINGTON	*/
/* #define FORCE_NEHALEM	*/
/* #define FORCE_SANDYBRIDGE	*/
/* #define FORCE_ATOM		*/
/* #define FORCE_ATHLON		*/
/* #define FORCE_OPTERON	*/
/* #define FORCE_OPTERON_SSE3	*/
/* #define FORCE_BARCELONA	*/
/* #define FORCE_SHANGHAI	*/
/* #define FORCE_ISTANBUL	*/
/* #define FORCE_BOBCAT		*/
/* #define FORCE_BULLDOZER	*/
/* #define FORCE_PILEDRIVER     */
/* #define FORCE_SSE_GENERIC	*/
/* #define FORCE_VIAC3		*/
/* #define FORCE_NANO		*/
/* #define FORCE_POWER3		*/
/* #define FORCE_POWER4		*/
/* #define FORCE_POWER5		*/
/* #define FORCE_POWER6		*/
/* #define FORCE_POWER7		*/
/* #define FORCE_POWER8		*/
/* #define FORCE_PPCG4		*/
/* #define FORCE_PPC970		*/
/* #define FORCE_PPC970MP	*/
/* #define FORCE_PPC440		*/
/* #define FORCE_PPC440FP2	*/
/* #define FORCE_CELL		*/
/* #define FORCE_SICORTEX	*/
/* #define FORCE_LOONGSON3A	*/
/* #define FORCE_LOONGSON3B	*/
/* #define FORCE_I6400		*/
/* #define FORCE_P6600		*/
/* #define FORCE_P5600		*/
/* #define FORCE_I6500		*/
/* #define FORCE_ITANIUM2	*/
/* #define FORCE_SPARC		*/
/* #define FORCE_SPARCV7	*/
/* #define FORCE_ZARCH_GENERIC	*/
/* #define FORCE_Z13		*/
/* #define FORCE_GENERIC	*/

#ifdef FORCE_P2
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "PENTIUM2"
#define ARCHCONFIG   "-DPENTIUM2 " \
		     "-DL1_DATA_SIZE=16384 -DL1_DATA_LINESIZE=32 " \
		     "-DL2_SIZE=512488 -DL2_LINESIZE=32 " \
		     "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 " \
		     "-DHAVE_CMOV -DHAVE_MMX"
#define LIBNAME   "p2"
#define CORENAME  "P5"
#endif

#ifdef FORCE_KATMAI
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "PENTIUM3"
#define ARCHCONFIG   "-DPENTIUM3 " \
		     "-DL1_DATA_SIZE=16384 -DL1_DATA_LINESIZE=32 " \
		     "-DL2_SIZE=524288 -DL2_LINESIZE=32 " \
		     "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 " \
		     "-DHAVE_CMOV -DHAVE_MMX -DHAVE_SSE "
#define LIBNAME   "katmai"
#define CORENAME  "KATMAI"
#endif

#ifdef FORCE_COPPERMINE
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "PENTIUM3"
#define ARCHCONFIG   "-DPENTIUM3 " \
		     "-DL1_DATA_SIZE=16384 -DL1_DATA_LINESIZE=32 " \
		     "-DL2_SIZE=262144 -DL2_LINESIZE=32 " \
		     "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 " \
		     "-DHAVE_CMOV -DHAVE_MMX -DHAVE_SSE "
#define LIBNAME   "coppermine"
#define CORENAME  "COPPERMINE"
#endif

#ifdef FORCE_NORTHWOOD
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "PENTIUM4"
#define ARCHCONFIG   "-DPENTIUM4 " \
		     "-DL1_DATA_SIZE=8192 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=524288 -DL2_LINESIZE=64 " \
		     "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=8 " \
		     "-DHAVE_CMOV -DHAVE_MMX -DHAVE_SSE -DHAVE_SSE2 "
#define LIBNAME   "northwood"
#define CORENAME  "NORTHWOOD"
#endif

#ifdef FORCE_PRESCOTT
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "PENTIUM4"
#define ARCHCONFIG   "-DPENTIUM4 " \
		     "-DL1_DATA_SIZE=16384 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=1048576 -DL2_LINESIZE=64 " \
		     "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=8 " \
		     "-DHAVE_CMOV -DHAVE_MMX -DHAVE_SSE -DHAVE_SSE2 -DHAVE_SSE3"
#define LIBNAME   "prescott"
#define CORENAME  "PRESCOTT"
#endif

#ifdef FORCE_BANIAS
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "BANIAS"
#define ARCHCONFIG   "-DPENTIUMM " \
		     "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=1048576 -DL2_LINESIZE=64 " \
		     "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 " \
		     "-DHAVE_CMOV -DHAVE_MMX -DHAVE_SSE -DHAVE_SSE2 "
#define LIBNAME   "banias"
#define CORENAME  "BANIAS"
#endif

#ifdef FORCE_YONAH
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "YONAH"
#define ARCHCONFIG   "-DPENTIUMM " \
		     "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=1048576 -DL2_LINESIZE=64 " \
		     "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 " \
		     "-DHAVE_CMOV -DHAVE_MMX -DHAVE_SSE -DHAVE_SSE2 "
#define LIBNAME   "yonah"
#define CORENAME  "YONAH"
#endif

#ifdef FORCE_CORE2
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "CONRORE"
#define ARCHCONFIG   "-DCORE2 " \
		     "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=1048576 -DL2_LINESIZE=64 " \
		     "-DDTB_DEFAULT_ENTRIES=256 -DDTB_SIZE=4096 " \
		     "-DHAVE_CMOV -DHAVE_MMX -DHAVE_SSE -DHAVE_SSE2 -DHAVE_SSE3 -DHAVE_SSSE3"
#define LIBNAME   "core2"
#define CORENAME  "CORE2"
#endif

#ifdef FORCE_PENRYN
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "PENRYN"
#define ARCHCONFIG   "-DPENRYN " \
		     "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=1048576 -DL2_LINESIZE=64 " \
		     "-DDTB_DEFAULT_ENTRIES=256 -DDTB_SIZE=4096 " \
		     "-DHAVE_CMOV -DHAVE_MMX -DHAVE_SSE -DHAVE_SSE2 -DHAVE_SSE3 -DHAVE_SSSE3 -DHAVE_SSE4_1"
#define LIBNAME   "penryn"
#define CORENAME  "PENRYN"
#endif

#ifdef FORCE_DUNNINGTON
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "DUNNINGTON"
#define ARCHCONFIG   "-DDUNNINGTON " \
		     "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=1048576 -DL2_LINESIZE=64 " \
		     "-DL3_SIZE=16777216 -DL3_LINESIZE=64 " \
		     "-DDTB_DEFAULT_ENTRIES=256 -DDTB_SIZE=4096 " \
		     "-DHAVE_CMOV -DHAVE_MMX -DHAVE_SSE -DHAVE_SSE2 -DHAVE_SSE3 -DHAVE_SSSE3 -DHAVE_SSE4_1"
#define LIBNAME   "dunnington"
#define CORENAME  "DUNNINGTON"
#endif

#ifdef FORCE_NEHALEM
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "NEHALEM"
#define ARCHCONFIG   "-DNEHALEM " \
		     "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=262144 -DL2_LINESIZE=64 " \
		     "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 " \
		     "-DHAVE_CMOV -DHAVE_MMX -DHAVE_SSE -DHAVE_SSE2 -DHAVE_SSE3 -DHAVE_SSSE3 -DHAVE_SSE4_1 -DHAVE_SSE4_2"
#define LIBNAME   "nehalem"
#define CORENAME  "NEHALEM"
#endif

#ifdef FORCE_SANDYBRIDGE
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "SANDYBRIDGE"
#define ARCHCONFIG   "-DSANDYBRIDGE " \
		     "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=262144 -DL2_LINESIZE=64 " \
		     "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 " \
		     "-DHAVE_CMOV -DHAVE_MMX -DHAVE_SSE -DHAVE_SSE2 -DHAVE_SSE3 -DHAVE_SSSE3 -DHAVE_SSE4_1 -DHAVE_SSE4_2 -DHAVE_AVX"
#define LIBNAME   "sandybridge"
#define CORENAME  "SANDYBRIDGE"
#endif

#ifdef FORCE_HASWELL
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "HASWELL"
#define ARCHCONFIG   "-DHASWELL " \
		     "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=262144 -DL2_LINESIZE=64 " \
		     "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 " \
		     "-DHAVE_CMOV -DHAVE_MMX -DHAVE_SSE -DHAVE_SSE2 -DHAVE_SSE3 -DHAVE_SSSE3 -DHAVE_SSE4_1 -DHAVE_SSE4_2 -DHAVE_AVX " \
                     "-DFMA3"
#define LIBNAME   "haswell"
#define CORENAME  "HASWELL"
#endif

#ifdef FORCE_SKYLAKEX
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "SKYLAKEX"
#define ARCHCONFIG   "-DSKYLAKEX " \
		     "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=262144 -DL2_LINESIZE=64 " \
		     "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 " \
		     "-DHAVE_CMOV -DHAVE_MMX -DHAVE_SSE -DHAVE_SSE2 -DHAVE_SSE3 -DHAVE_SSSE3 -DHAVE_SSE4_1 -DHAVE_SSE4_2 -DHAVE_AVX " \
                     "-DFMA3 -DHAVE_AVX512VL -march=skylake-avx512"
#define LIBNAME   "skylakex"
#define CORENAME  "SKYLAKEX"
#endif

#ifdef FORCE_ATOM
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "ATOM"
#define ARCHCONFIG   "-DATOM " \
		     "-DL1_DATA_SIZE=24576 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=524288 -DL2_LINESIZE=64 " \
		     "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=4 " \
		     "-DHAVE_CMOV -DHAVE_MMX -DHAVE_SSE -DHAVE_SSE2 -DHAVE_SSE3 -DHAVE_SSSE3"
#define LIBNAME   "atom"
#define CORENAME  "ATOM"
#endif

#ifdef FORCE_ATHLON
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "ATHLON"
#define ARCHCONFIG   "-DATHLON " \
		     "-DL1_DATA_SIZE=65536 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=1048576 -DL2_LINESIZE=64 " \
		     "-DDTB_DEFAULT_ENTRIES=32 -DDTB_SIZE=4096 -DHAVE_3DNOW  " \
		     "-DHAVE_3DNOWEX -DHAVE_MMX -DHAVE_SSE "
#define LIBNAME   "athlon"
#define CORENAME  "ATHLON"
#endif

#ifdef FORCE_OPTERON
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "OPTERON"
#define ARCHCONFIG   "-DOPTERON " \
		     "-DL1_DATA_SIZE=65536 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=1048576 -DL2_LINESIZE=64 " \
		     "-DDTB_DEFAULT_ENTRIES=32 -DDTB_SIZE=4096 -DHAVE_3DNOW " \
		     "-DHAVE_3DNOWEX -DHAVE_MMX -DHAVE_SSE -DHAVE_SSE2 "
#define LIBNAME   "opteron"
#define CORENAME  "OPTERON"
#endif

#ifdef FORCE_OPTERON_SSE3
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "OPTERON"
#define ARCHCONFIG   "-DOPTERON " \
		     "-DL1_DATA_SIZE=65536 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=1048576 -DL2_LINESIZE=64 " \
		     "-DDTB_DEFAULT_ENTRIES=32 -DDTB_SIZE=4096 -DHAVE_3DNOW " \
		     "-DHAVE_3DNOWEX -DHAVE_MMX -DHAVE_SSE -DHAVE_SSE2 -DHAVE_SSE3"
#define LIBNAME   "opteron"
#define CORENAME  "OPTERON"
#endif

#if defined(FORCE_BARCELONA) || defined(FORCE_SHANGHAI) || defined(FORCE_ISTANBUL)
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "BARCELONA"
#define ARCHCONFIG   "-DBARCELONA " \
		     "-DL1_DATA_SIZE=65536 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=524288 -DL2_LINESIZE=64  -DL3_SIZE=2097152 " \
		     "-DDTB_DEFAULT_ENTRIES=48 -DDTB_SIZE=4096 " \
		     "-DHAVE_MMX -DHAVE_SSE -DHAVE_SSE2 -DHAVE_SSE3 " \
		     "-DHAVE_SSE4A -DHAVE_MISALIGNSSE -DHAVE_128BITFPU -DHAVE_FASTMOVU"
#define LIBNAME   "barcelona"
#define CORENAME  "BARCELONA"
#endif

#if defined(FORCE_BOBCAT)
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "BOBCAT"
#define ARCHCONFIG   "-DBOBCAT " \
		     "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=524288 -DL2_LINESIZE=64 " \
		     "-DDTB_DEFAULT_ENTRIES=40 -DDTB_SIZE=4096 " \
		     "-DHAVE_MMX -DHAVE_SSE -DHAVE_SSE2 -DHAVE_SSE3 -DHAVE_SSSE3 " \
		     "-DHAVE_SSE4A -DHAVE_MISALIGNSSE -DHAVE_CFLUSH -DHAVE_CMOV"
#define LIBNAME   "bobcat"
#define CORENAME  "BOBCAT"
#endif

#if defined (FORCE_BULLDOZER)
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "BULLDOZER"
#define ARCHCONFIG   "-DBULLDOZER " \
		     "-DL1_DATA_SIZE=49152 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=1024000 -DL2_LINESIZE=64  -DL3_SIZE=16777216 " \
		     "-DDTB_DEFAULT_ENTRIES=32 -DDTB_SIZE=4096 " \
		     "-DHAVE_MMX -DHAVE_SSE -DHAVE_SSE2 -DHAVE_SSE3 " \
		     "-DHAVE_SSE4A -DHAVE_MISALIGNSSE -DHAVE_128BITFPU -DHAVE_FASTMOVU " \
                     "-DHAVE_AVX -DHAVE_FMA4"
#define LIBNAME   "bulldozer"
#define CORENAME  "BULLDOZER"
#endif

#if defined (FORCE_PILEDRIVER)
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "PILEDRIVER"
#define ARCHCONFIG   "-DPILEDRIVER " \
		     "-DL1_DATA_SIZE=16384 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=2097152 -DL2_LINESIZE=64  -DL3_SIZE=12582912 " \
		     "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 " \
		     "-DHAVE_MMX -DHAVE_SSE -DHAVE_SSE2 -DHAVE_SSE3 -DHAVE_SSE4_1 -DHAVE_SSE4_2 " \
		     "-DHAVE_SSE4A -DHAVE_MISALIGNSSE -DHAVE_128BITFPU -DHAVE_FASTMOVU -DHAVE_CFLUSH " \
                     "-DHAVE_AVX -DHAVE_FMA4 -DHAVE_FMA3"
#define LIBNAME   "piledriver"
#define CORENAME  "PILEDRIVER"
#endif

#if defined (FORCE_STEAMROLLER)
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "STEAMROLLER"
#define ARCHCONFIG   "-DSTEAMROLLER " \
		     "-DL1_DATA_SIZE=16384 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=2097152 -DL2_LINESIZE=64  -DL3_SIZE=12582912 " \
		     "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 " \
		     "-DHAVE_MMX -DHAVE_SSE -DHAVE_SSE2 -DHAVE_SSE3 -DHAVE_SSE4_1 -DHAVE_SSE4_2 " \
		     "-DHAVE_SSE4A -DHAVE_MISALIGNSSE -DHAVE_128BITFPU -DHAVE_FASTMOVU -DHAVE_CFLUSH " \
                     "-DHAVE_AVX -DHAVE_FMA4 -DHAVE_FMA3"
#define LIBNAME   "steamroller"
#define CORENAME  "STEAMROLLER"
#endif

#if defined (FORCE_EXCAVATOR)
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "EXCAVATOR"
#define ARCHCONFIG   "-DEXCAVATOR " \
		     "-DL1_DATA_SIZE=16384 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=2097152 -DL2_LINESIZE=64  -DL3_SIZE=12582912 " \
		     "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 " \
		     "-DHAVE_MMX -DHAVE_SSE -DHAVE_SSE2 -DHAVE_SSE3 -DHAVE_SSE4_1 -DHAVE_SSE4_2 " \
		     "-DHAVE_SSE4A -DHAVE_MISALIGNSSE -DHAVE_128BITFPU -DHAVE_FASTMOVU -DHAVE_CFLUSH " \
                     "-DHAVE_AVX -DHAVE_FMA4 -DHAVE_FMA3"
#define LIBNAME   "excavator"
#define CORENAME  "EXCAVATOR"
#endif

#if defined (FORCE_ZEN)
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "ZEN"
#define ARCHCONFIG   "-DZEN " \
		     "-DL1_CODE_SIZE=32768 -DL1_CODE_LINESIZE=64 -DL1_CODE_ASSOCIATIVE=8 " \
		     "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=64 -DL2_CODE_ASSOCIATIVE=8 " \
		     "-DL2_SIZE=524288 -DL2_LINESIZE=64 -DL2_ASSOCIATIVE=8 " \
		     "-DL3_SIZE=16777216 -DL3_LINESIZE=64 -DL3_ASSOCIATIVE=8 " \
		     "-DITB_DEFAULT_ENTRIES=64 -DITB_SIZE=4096 " \
		     "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 " \
		     "-DHAVE_MMX -DHAVE_SSE -DHAVE_SSE2 -DHAVE_SSE3 -DHAVE_SSE4_1 -DHAVE_SSE4_2 " \
		     "-DHAVE_SSE4A -DHAVE_MISALIGNSSE -DHAVE_128BITFPU -DHAVE_FASTMOVU -DHAVE_CFLUSH " \
		     "-DHAVE_AVX -DHAVE_FMA3 -DFMA3"
#define LIBNAME   "zen"
#define CORENAME  "ZEN"
#endif


#ifdef FORCE_SSE_GENERIC
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "GENERIC"
#define ARCHCONFIG   "-DGENERIC " \
		     "-DL1_DATA_SIZE=16384 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=524288 -DL2_LINESIZE=64 " \
		     "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=8 " \
		     "-DHAVE_CMOV -DHAVE_MMX -DHAVE_SSE -DHAVE_SSE2"
#define LIBNAME   "generic"
#define CORENAME  "GENERIC"
#endif

#ifdef FORCE_VIAC3
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "VIAC3"
#define ARCHCONFIG   "-DVIAC3 " \
		     "-DL1_DATA_SIZE=65536 -DL1_DATA_LINESIZE=32 " \
		     "-DL2_SIZE=65536 -DL2_LINESIZE=32 " \
		     "-DDTB_DEFAULT_ENTRIES=128 -DDTB_SIZE=4096 " \
		     "-DHAVE_MMX -DHAVE_SSE "
#define LIBNAME   "viac3"
#define CORENAME  "VIAC3"
#endif

#ifdef FORCE_NANO
#define FORCE
#define FORCE_INTEL
#define ARCHITECTURE    "X86"
#define SUBARCHITECTURE "NANO"
#define ARCHCONFIG   "-DNANO " \
		     "-DL1_DATA_SIZE=65536 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=1048576 -DL2_LINESIZE=64 " \
		     "-DDTB_DEFAULT_ENTRIES=128 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=8 " \
		     "-DHAVE_CMOV -DHAVE_MMX -DHAVE_SSE -DHAVE_SSE2 -DHAVE_SSE3 -DHAVE_SSSE3"
#define LIBNAME   "nano"
#define CORENAME  "NANO"
#endif

#ifdef FORCE_POWER3
#define FORCE
#define ARCHITECTURE    "POWER"
#define SUBARCHITECTURE "POWER3"
#define SUBDIRNAME      "power"
#define ARCHCONFIG   "-DPOWER3 " \
		     "-DL1_DATA_SIZE=65536 -DL1_DATA_LINESIZE=128 " \
		     "-DL2_SIZE=2097152 -DL2_LINESIZE=128 " \
		     "-DDTB_DEFAULT_ENTRIES=256 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=8 "
#define LIBNAME   "power3"
#define CORENAME  "POWER3"
#endif

#ifdef FORCE_POWER4
#define FORCE
#define ARCHITECTURE    "POWER"
#define SUBARCHITECTURE "POWER4"
#define SUBDIRNAME      "power"
#define ARCHCONFIG   "-DPOWER4 " \
		     "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=128 " \
		     "-DL2_SIZE=1509949 -DL2_LINESIZE=128 " \
		     "-DDTB_DEFAULT_ENTRIES=128 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=6 "
#define LIBNAME   "power4"
#define CORENAME  "POWER4"
#endif

#ifdef FORCE_POWER5
#define FORCE
#define ARCHITECTURE    "POWER"
#define SUBARCHITECTURE "POWER5"
#define SUBDIRNAME      "power"
#define ARCHCONFIG   "-DPOWER5 " \
		     "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=128 " \
		     "-DL2_SIZE=1509949 -DL2_LINESIZE=128 " \
		     "-DDTB_DEFAULT_ENTRIES=128 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=6 "
#define LIBNAME   "power5"
#define CORENAME  "POWER5"
#endif

#if defined(FORCE_POWER6) || defined(FORCE_POWER7)
#define FORCE
#define ARCHITECTURE    "POWER"
#define SUBARCHITECTURE "POWER6"
#define SUBDIRNAME      "power"
#define ARCHCONFIG   "-DPOWER6 " \
		     "-DL1_DATA_SIZE=65536 -DL1_DATA_LINESIZE=128 " \
		     "-DL2_SIZE=4194304 -DL2_LINESIZE=128 " \
		     "-DDTB_DEFAULT_ENTRIES=128 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=8 "
#define LIBNAME   "power6"
#define CORENAME  "POWER6"
#endif

#if defined(FORCE_POWER8) 
#define FORCE
#define ARCHITECTURE    "POWER"
#define SUBARCHITECTURE "POWER8"
#define SUBDIRNAME      "power"
#define ARCHCONFIG   "-DPOWER8 " \
		     "-DL1_DATA_SIZE=65536 -DL1_DATA_LINESIZE=128 " \
		     "-DL2_SIZE=4194304 -DL2_LINESIZE=128 " \
		     "-DDTB_DEFAULT_ENTRIES=128 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=8 "
#define LIBNAME   "power8"
#define CORENAME  "POWER8"
#endif


#ifdef FORCE_PPCG4
#define FORCE
#define ARCHITECTURE    "POWER"
#define SUBARCHITECTURE "PPCG4"
#define SUBDIRNAME      "power"
#define ARCHCONFIG   "-DPPCG4 " \
		     "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=32 " \
		     "-DL2_SIZE=262144 -DL2_LINESIZE=32 " \
		     "-DDTB_DEFAULT_ENTRIES=128 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=8 "
#define LIBNAME   "ppcg4"
#define CORENAME  "PPCG4"
#endif

#ifdef FORCE_PPC970
#define FORCE
#define ARCHITECTURE    "POWER"
#define SUBARCHITECTURE "PPC970"
#define SUBDIRNAME      "power"
#define ARCHCONFIG   "-DPPC970 " \
		     "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=128 " \
		     "-DL2_SIZE=512488 -DL2_LINESIZE=128 " \
		     "-DDTB_DEFAULT_ENTRIES=128 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=8 "
#define LIBNAME   "ppc970"
#define CORENAME  "PPC970"
#endif

#ifdef FORCE_PPC970MP
#define FORCE
#define ARCHITECTURE    "POWER"
#define SUBARCHITECTURE "PPC970"
#define SUBDIRNAME      "power"
#define ARCHCONFIG   "-DPPC970 " \
		     "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=128 " \
		     "-DL2_SIZE=1024976 -DL2_LINESIZE=128 " \
		     "-DDTB_DEFAULT_ENTRIES=128 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=8 "
#define LIBNAME   "ppc970mp"
#define CORENAME  "PPC970"
#endif

#ifdef FORCE_PPC440
#define FORCE
#define ARCHITECTURE    "POWER"
#define SUBARCHITECTURE "PPC440"
#define SUBDIRNAME      "power"
#define ARCHCONFIG   "-DPPC440 " \
		     "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=32 " \
		     "-DL2_SIZE=16384 -DL2_LINESIZE=128 " \
		     "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=16 "
#define LIBNAME   "ppc440"
#define CORENAME  "PPC440"
#endif

#ifdef FORCE_PPC440FP2
#define FORCE
#define ARCHITECTURE    "POWER"
#define SUBARCHITECTURE "PPC440FP2"
#define SUBDIRNAME      "power"
#define ARCHCONFIG   "-DPPC440FP2 " \
		     "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=32 " \
		     "-DL2_SIZE=16384 -DL2_LINESIZE=128 " \
		     "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=16 "
#define LIBNAME   "ppc440FP2"
#define CORENAME  "PPC440FP2"
#endif

#ifdef FORCE_CELL
#define FORCE
#define ARCHITECTURE    "POWER"
#define SUBARCHITECTURE "CELL"
#define SUBDIRNAME      "power"
#define ARCHCONFIG   "-DCELL " \
		     "-DL1_DATA_SIZE=262144 -DL1_DATA_LINESIZE=128 " \
		     "-DL2_SIZE=512488 -DL2_LINESIZE=128 " \
		     "-DDTB_DEFAULT_ENTRIES=128 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=8 "
#define LIBNAME   "cell"
#define CORENAME  "CELL"
#endif

#ifdef FORCE_SICORTEX
#define FORCE
#define ARCHITECTURE    "MIPS"
#define SUBARCHITECTURE "SICORTEX"
#define SUBDIRNAME      "mips"
#define ARCHCONFIG   "-DSICORTEX " \
		     "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=32 " \
		     "-DL2_SIZE=512488 -DL2_LINESIZE=32 " \
		     "-DDTB_DEFAULT_ENTRIES=32 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=8 "
#define LIBNAME   "mips"
#define CORENAME  "sicortex"
#endif


#ifdef FORCE_LOONGSON3A
#define FORCE
#define ARCHITECTURE    "MIPS"
#define SUBARCHITECTURE "LOONGSON3A"
#define SUBDIRNAME      "mips64"
#define ARCHCONFIG   "-DLOONGSON3A " \
       "-DL1_DATA_SIZE=65536 -DL1_DATA_LINESIZE=32 " \
       "-DL2_SIZE=512488 -DL2_LINESIZE=32 " \
       "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=4 "
#define LIBNAME   "loongson3a"
#define CORENAME  "LOONGSON3A"
#else
#endif

#ifdef FORCE_LOONGSON3B
#define FORCE
#define ARCHITECTURE    "MIPS"
#define SUBARCHITECTURE "LOONGSON3B"
#define SUBDIRNAME      "mips64"
#define ARCHCONFIG   "-DLOONGSON3B " \
       "-DL1_DATA_SIZE=65536 -DL1_DATA_LINESIZE=32 " \
       "-DL2_SIZE=512488 -DL2_LINESIZE=32 " \
       "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=4 "
#define LIBNAME   "loongson3b"
#define CORENAME  "LOONGSON3B"
#else
#endif

#ifdef FORCE_I6400
#define FORCE
#define ARCHITECTURE    "MIPS"
#define SUBARCHITECTURE "I6400"
#define SUBDIRNAME      "mips64"
#define ARCHCONFIG   "-DI6400 " \
       "-DL1_DATA_SIZE=65536 -DL1_DATA_LINESIZE=32 " \
       "-DL2_SIZE=1048576 -DL2_LINESIZE=32 " \
       "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=8 "
#define LIBNAME   "i6400"
#define CORENAME  "I6400"
#else
#endif

#ifdef FORCE_P6600
#define FORCE
#define ARCHITECTURE    "MIPS"
#define SUBARCHITECTURE "P6600"
#define SUBDIRNAME      "mips64"
#define ARCHCONFIG   "-DP6600 " \
       "-DL1_DATA_SIZE=65536 -DL1_DATA_LINESIZE=32 " \
       "-DL2_SIZE=1048576 -DL2_LINESIZE=32 " \
       "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=8 "
#define LIBNAME   "p6600"
#define CORENAME  "P6600"
#else
#endif

#ifdef FORCE_P5600
#define FORCE
#define ARCHITECTURE    "MIPS"
#define SUBARCHITECTURE "P5600"
#define SUBDIRNAME      "mips"
#define ARCHCONFIG   "-DP5600 " \
       "-DL1_DATA_SIZE=65536 -DL1_DATA_LINESIZE=32 " \
       "-DL2_SIZE=1048576 -DL2_LINESIZE=32 " \
       "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=8 "
#define LIBNAME   "p5600"
#define CORENAME  "P5600"
#else
#endif

#ifdef FORCE_I6500
#define FORCE
#define ARCHITECTURE    "MIPS"
#define SUBARCHITECTURE "I6500"
#define SUBDIRNAME      "mips64"
#define ARCHCONFIG   "-DI6500 " \
       "-DL1_DATA_SIZE=65536 -DL1_DATA_LINESIZE=32 " \
       "-DL2_SIZE=1048576 -DL2_LINESIZE=32 " \
       "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=8 "
#define LIBNAME   "i6500"
#define CORENAME  "I6500"
#else
#endif

#ifdef FORCE_ITANIUM2
#define FORCE
#define ARCHITECTURE    "IA64"
#define SUBARCHITECTURE "ITANIUM2"
#define SUBDIRNAME      "ia64"
#define ARCHCONFIG   "-DITANIUM2 " \
		     "-DL1_DATA_SIZE=262144 -DL1_DATA_LINESIZE=128 " \
		     "-DL2_SIZE=1572864 -DL2_LINESIZE=128 -DDTB_SIZE=16384 -DDTB_DEFAULT_ENTRIES=128 "
#define LIBNAME   "itanium2"
#define CORENAME  "itanium2"
#endif

#ifdef FORCE_SPARC
#define FORCE
#define ARCHITECTURE    "SPARC"
#define SUBARCHITECTURE "SPARC"
#define SUBDIRNAME      "sparc"
#define ARCHCONFIG   "-DSPARC -DV9 " \
		     "-DL1_DATA_SIZE=65536 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=1572864 -DL2_LINESIZE=64 -DDTB_SIZE=8192 -DDTB_DEFAULT_ENTRIES=64 "
#define LIBNAME   "sparc"
#define CORENAME  "sparc"
#endif

#ifdef FORCE_SPARCV7
#define FORCE
#define ARCHITECTURE    "SPARC"
#define SUBARCHITECTURE "SPARC"
#define SUBDIRNAME      "sparc"
#define ARCHCONFIG   "-DSPARC -DV7 " \
		     "-DL1_DATA_SIZE=65536 -DL1_DATA_LINESIZE=64 " \
		     "-DL2_SIZE=1572864 -DL2_LINESIZE=64 -DDTB_SIZE=8192 -DDTB_DEFAULT_ENTRIES=64 "
#define LIBNAME   "sparcv7"
#define CORENAME  "sparcv7"
#endif

#ifdef FORCE_GENERIC
#define FORCE
#define ARCHITECTURE    "GENERIC"
#define SUBARCHITECTURE "GENERIC"
#define SUBDIRNAME      "generic"
#define ARCHCONFIG   "-DGENERIC " \
		     "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=128 " \
		     "-DL2_SIZE=512488 -DL2_LINESIZE=128 " \
		     "-DDTB_DEFAULT_ENTRIES=128 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=8 "
#define LIBNAME   "generic"
#define CORENAME  "generic"
#endif

#ifdef FORCE_ARMV7
#define FORCE
#define ARCHITECTURE    "ARM"
#define SUBARCHITECTURE "ARMV7"
#define SUBDIRNAME      "arm"
#define ARCHCONFIG   "-DARMV7 " \
       "-DL1_DATA_SIZE=65536 -DL1_DATA_LINESIZE=32 " \
       "-DL2_SIZE=512488 -DL2_LINESIZE=32 " \
       "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=4 " \
       "-DHAVE_VFPV3 -DHAVE_VFP"
#define LIBNAME   "armv7"
#define CORENAME  "ARMV7"
#else
#endif

#ifdef FORCE_CORTEXA9
#define FORCE
#define ARCHITECTURE    "ARM"
#define SUBARCHITECTURE "CORTEXA9"
#define SUBDIRNAME      "arm"
#define ARCHCONFIG   "-DCORTEXA9 -DARMV7 " \
       "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=32 " \
       "-DL2_SIZE=1048576 -DL2_LINESIZE=32 " \
       "-DDTB_DEFAULT_ENTRIES=128 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=4 " \
       "-DHAVE_VFPV3 -DHAVE_VFP -DHAVE_NEON"
#define LIBNAME   "cortexa9"
#define CORENAME  "CORTEXA9"
#else
#endif

#ifdef FORCE_CORTEXA15
#define FORCE
#define ARCHITECTURE    "ARM"
#define SUBARCHITECTURE "CORTEXA15"
#define SUBDIRNAME      "arm"
#define ARCHCONFIG   "-DCORTEXA15 -DARMV7 " \
       "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=32 " \
       "-DL2_SIZE=1048576 -DL2_LINESIZE=32 " \
       "-DDTB_DEFAULT_ENTRIES=128 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=4 " \
       "-DHAVE_VFPV3 -DHAVE_VFP -DHAVE_NEON"
#define LIBNAME   "cortexa15"
#define CORENAME  "CORTEXA15"
#else
#endif

#ifdef FORCE_ARMV6
#define FORCE
#define ARCHITECTURE    "ARM"
#define SUBARCHITECTURE "ARMV6"
#define SUBDIRNAME      "arm"
#define ARCHCONFIG   "-DARMV6 " \
       "-DL1_DATA_SIZE=65536 -DL1_DATA_LINESIZE=32 " \
       "-DL2_SIZE=512488 -DL2_LINESIZE=32 " \
       "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=4 " \
       "-DHAVE_VFP"
#define LIBNAME   "armv6"
#define CORENAME  "ARMV6"
#else
#endif

#ifdef FORCE_ARMV5
#define FORCE
#define ARCHITECTURE    "ARM"
#define SUBARCHITECTURE "ARMV5"
#define SUBDIRNAME      "arm"
#define ARCHCONFIG   "-DARMV5 " \
       "-DL1_DATA_SIZE=65536 -DL1_DATA_LINESIZE=32 " \
       "-DL2_SIZE=512488 -DL2_LINESIZE=32 " \
       "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=4 "
#define LIBNAME   "armv5"
#define CORENAME  "ARMV5"
#else
#endif


#ifdef FORCE_ARMV8
#define FORCE
#define ARCHITECTURE    "ARM64"
#define SUBARCHITECTURE "ARMV8"
#define SUBDIRNAME      "arm64"
#define ARCHCONFIG   "-DARMV8 " \
       "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=64 " \
       "-DL2_SIZE=262144 -DL2_LINESIZE=64 " \
       "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 -DL2_ASSOCIATIVE=32 " \
       "-DHAVE_VFPV4 -DHAVE_VFPV3 -DHAVE_VFP -DHAVE_NEON -DARMV8"
#define LIBNAME   "armv8"
#define CORENAME  "ARMV8"
#endif

#ifdef FORCE_CORTEXA53
#define FORCE
#define ARCHITECTURE    "ARM64"
#define SUBARCHITECTURE "CORTEXA53"
#define SUBDIRNAME      "arm64"
#define ARCHCONFIG   "-DCORTEXA53 " \
       "-DL1_CODE_SIZE=32768 -DL1_CODE_LINESIZE=64 -DL1_CODE_ASSOCIATIVE=3 " \
       "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=64 -DL1_DATA_ASSOCIATIVE=2 " \
       "-DL2_SIZE=262144 -DL2_LINESIZE=64 -DL2_ASSOCIATIVE=16 " \
       "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 " \
       "-DHAVE_VFPV4 -DHAVE_VFPV3 -DHAVE_VFP -DHAVE_NEON -DARMV8"
#define LIBNAME   "cortexa53"
#define CORENAME  "CORTEXA53"
#else
#endif

#ifdef FORCE_CORTEXA57
#define FORCE
#define ARCHITECTURE    "ARM64"
#define SUBARCHITECTURE "CORTEXA57"
#define SUBDIRNAME      "arm64"
#define ARCHCONFIG   "-DCORTEXA57 " \
       "-DL1_CODE_SIZE=49152 -DL1_CODE_LINESIZE=64 -DL1_CODE_ASSOCIATIVE=3 " \
       "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=64 -DL1_DATA_ASSOCIATIVE=2 " \
       "-DL2_SIZE=2097152 -DL2_LINESIZE=64 -DL2_ASSOCIATIVE=16 " \
       "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 " \
       "-DHAVE_VFPV4 -DHAVE_VFPV3 -DHAVE_VFP -DHAVE_NEON -DARMV8"
#define LIBNAME   "cortexa57"
#define CORENAME  "CORTEXA57"
#else
#endif

#ifdef FORCE_CORTEXA72
#define FORCE
#define ARCHITECTURE    "ARM64"
#define SUBARCHITECTURE "CORTEXA72"
#define SUBDIRNAME      "arm64"
#define ARCHCONFIG   "-DCORTEXA72 " \
       "-DL1_CODE_SIZE=49152 -DL1_CODE_LINESIZE=64 -DL1_CODE_ASSOCIATIVE=3 " \
       "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=64 -DL1_DATA_ASSOCIATIVE=2 " \
       "-DL2_SIZE=2097152 -DL2_LINESIZE=64 -DL2_ASSOCIATIVE=16 " \
       "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 " \
       "-DHAVE_VFPV4 -DHAVE_VFPV3 -DHAVE_VFP -DHAVE_NEON -DARMV8"
#define LIBNAME   "cortexa72"
#define CORENAME  "CORTEXA72"
#else
#endif

#ifdef FORCE_CORTEXA73
#define FORCE
#define ARCHITECTURE    "ARM64"
#define SUBARCHITECTURE "CORTEXA73"
#define SUBDIRNAME      "arm64"
#define ARCHCONFIG   "-DCORTEXA73 " \
       "-DL1_CODE_SIZE=49152 -DL1_CODE_LINESIZE=64 -DL1_CODE_ASSOCIATIVE=3 " \
       "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=64 -DL1_DATA_ASSOCIATIVE=2 " \
       "-DL2_SIZE=2097152 -DL2_LINESIZE=64 -DL2_ASSOCIATIVE=16 " \
       "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 " \
       "-DHAVE_VFPV4 -DHAVE_VFPV3 -DHAVE_VFP -DHAVE_NEON -DARMV8"
#define LIBNAME   "cortexa73"
#define CORENAME  "CORTEXA73"
#else
#endif

#ifdef FORCE_FALKOR
#define FORCE
#define ARCHITECTURE    "ARM64"
#define SUBARCHITECTURE "FALKOR"
#define SUBDIRNAME      "arm64"
#define ARCHCONFIG   "-DFALKOR " \
       "-DL1_CODE_SIZE=49152 -DL1_CODE_LINESIZE=64 -DL1_CODE_ASSOCIATIVE=3 " \
       "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=64 -DL1_DATA_ASSOCIATIVE=2 " \
       "-DL2_SIZE=2097152 -DL2_LINESIZE=64 -DL2_ASSOCIATIVE=16 " \
       "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 " \
       "-DHAVE_VFPV4 -DHAVE_VFPV3 -DHAVE_VFP -DHAVE_NEON -DARMV8"
#define LIBNAME   "falkor"
#define CORENAME  "FALKOR"
#else
#endif

#ifdef FORCE_THUNDERX
#define FORCE
#define ARCHITECTURE    "ARM64"
#define SUBARCHITECTURE "THUNDERX"
#define SUBDIRNAME      "arm64"
#define ARCHCONFIG   "-DTHUNDERX " \
       "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=128 " \
       "-DL2_SIZE=16777216 -DL2_LINESIZE=128 -DL2_ASSOCIATIVE=16 " \
       "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 " \
       "-DHAVE_VFPV4 -DHAVE_VFPV3 -DHAVE_VFP -DHAVE_NEON -DARMV8"
#define LIBNAME   "thunderx"
#define CORENAME  "THUNDERX"
#else
#endif

#ifdef FORCE_THUNDERX2T99
#define ARMV8
#define FORCE
#define ARCHITECTURE    "ARM64"
#define SUBARCHITECTURE "THUNDERX2T99"
#define SUBDIRNAME      "arm64"
#define ARCHCONFIG   "-DTHUNDERX2T99 " \
       "-DL1_CODE_SIZE=32768 -DL1_CODE_LINESIZE=64 -DL1_CODE_ASSOCIATIVE=8 " \
       "-DL1_DATA_SIZE=32768 -DL1_DATA_LINESIZE=64 -DL1_DATA_ASSOCIATIVE=8 " \
       "-DL2_SIZE=262144 -DL2_LINESIZE=64 -DL2_ASSOCIATIVE=8 " \
       "-DL3_SIZE=33554432 -DL3_LINESIZE=64 -DL3_ASSOCIATIVE=32 " \
       "-DDTB_DEFAULT_ENTRIES=64 -DDTB_SIZE=4096 " \
       "-DHAVE_VFPV4 -DHAVE_VFPV3 -DHAVE_VFP -DHAVE_NEON -DARMV8"
#define LIBNAME   "thunderx2t99"
#define CORENAME  "THUNDERX2T99"
#else
#endif

#ifdef FORCE_ZARCH_GENERIC
#define FORCE
#define ARCHITECTURE    "ZARCH"
#define SUBARCHITECTURE "ZARCH_GENERIC"
#define ARCHCONFIG   "-DZARCH_GENERIC " \
       "-DDTB_DEFAULT_ENTRIES=64"
#define LIBNAME   "zarch_generic"
#define CORENAME  "ZARCH_GENERIC"
#endif

#ifdef FORCE_Z13
#define FORCE
#define ARCHITECTURE    "ZARCH"
#define SUBARCHITECTURE "Z13"
#define ARCHCONFIG   "-DZ13 " \
       "-DDTB_DEFAULT_ENTRIES=64"
#define LIBNAME   "z13"
#define CORENAME  "Z13"
#endif

#ifndef FORCE

#if defined(__powerpc__) || defined(__powerpc) || defined(powerpc) || \
    defined(__PPC__) || defined(PPC) || defined(_POWER) || defined(__POWERPC__)
#ifndef POWER
#define POWER
#endif
#define OPENBLAS_SUPPORTED
#endif

#if defined(__zarch__) || defined(__s390x__)
#define ZARCH
#include "cpuid_zarch.c"
#define OPENBLAS_SUPPORTED
#endif

#ifdef INTEL_AMD
#include "cpuid_x86.c"
#define OPENBLAS_SUPPORTED
#endif

#ifdef __ia64__
#include "cpuid_ia64.c"
#define OPENBLAS_SUPPORTED
#endif

#ifdef __alpha
#include "cpuid_alpha.c"
#define OPENBLAS_SUPPORTED
#endif

#ifdef POWER
#include "cpuid_power.c"
#define OPENBLAS_SUPPORTED
#endif

#ifdef sparc
#include "cpuid_sparc.c"
#define OPENBLAS_SUPPORTED
#endif

#ifdef __mips__
#ifdef __mips64
#include "cpuid_mips64.c"
#else
#include "cpuid_mips.c"
#endif
#define OPENBLAS_SUPPORTED
#endif

#ifdef __arm__
#include "cpuid_arm.c"
#define OPENBLAS_SUPPORTED
#endif

#ifdef __aarch64__
#include "cpuid_arm64.c"
#define OPENBLAS_SUPPORTED
#endif


#ifndef OPENBLAS_SUPPORTED
#error "This arch/CPU is not supported by OpenBLAS."
#endif

#else

#endif

static int get_num_cores(void) {

#ifdef OS_WINDOWS
  SYSTEM_INFO sysinfo;
#elif defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__DragonFly__) || defined(__APPLE__)
  int m[2], count;
  size_t len;
#endif

#if defined(linux) || defined(__sun__)
  //returns the number of processors which are currently online
  return sysconf(_SC_NPROCESSORS_CONF);

#elif defined(OS_WINDOWS)

  GetSystemInfo(&sysinfo);
  return sysinfo.dwNumberOfProcessors;

#elif defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__DragonFly__) || defined(__APPLE__)
  m[0] = CTL_HW;
  m[1] = HW_NCPU;
  len = sizeof(int);
  sysctl(m, 2, &count, &len, NULL, 0);

  return count;
#else
  return 2;
#endif
}

int main(int argc, char *argv[]){

#ifdef FORCE
  char buffer[8192], *p, *q;
  int length;
#endif

  if (argc == 1) return 0;

  switch (argv[1][0]) {

  case '0' : /* for Makefile */

#ifdef FORCE
    printf("CORE=%s\n", CORENAME);
#else
#if defined(INTEL_AMD) || defined(POWER) || defined(__mips__) || defined(__arm__) || defined(__aarch64__) || defined(ZARCH) || defined(sparc)
    printf("CORE=%s\n", get_corename());
#endif
#endif

#ifdef FORCE
    printf("LIBCORE=%s\n", LIBNAME);
#else
    printf("LIBCORE=");
    get_libname();
    printf("\n");
#endif

    printf("NUM_CORES=%d\n", get_num_cores());

#if defined(__arm__) && !defined(FORCE)
        get_features();
#endif


#ifdef INTEL_AMD
#ifndef FORCE
    get_sse();
#else

    sprintf(buffer, "%s", ARCHCONFIG);

    p = &buffer[0];

    while (*p) {
      if ((*p == '-') && (*(p + 1) == 'D')) {
	p += 2;

	while ((*p != ' ') && (*p != '\0')) {

	  if (*p == '=') {
	    printf("=");
	    p ++;
	    while ((*p != ' ') && (*p != '\0')) {
	      printf("%c", *p);
	      p ++;
	    }
	  } else {
	    printf("%c", *p);
	    p ++;
	    if ((*p == ' ') || (*p =='\0')) printf("=1");
	  }
	}

	printf("\n");
      } else p ++;
    }
#endif
#endif

#ifdef MAKE_NB_JOBS
  #if MAKE_NB_JOBS > 0
    printf("MAKE += -j %d\n", MAKE_NB_JOBS);
  #else
    // Let make use parent -j argument or -j1 if there
    // is no make parent
  #endif
#elif NO_PARALLEL_MAKE==1
    printf("MAKE += -j 1\n");
#else
    printf("MAKE += -j %d\n", get_num_cores());
#endif

    break;

  case '1' : /* For config.h */
#ifdef FORCE
    sprintf(buffer, "%s -DCORE_%s\n", ARCHCONFIG, CORENAME);

    p = &buffer[0];
    while (*p) {
      if ((*p == '-') && (*(p + 1) == 'D')) {
	p += 2;
	printf("#define ");

	while ((*p != ' ') && (*p != '\0')) {

	  if (*p == '=') {
	    printf(" ");
	    p ++;
	    while ((*p != ' ') && (*p != '\0')) {
	      printf("%c", *p);
	      p ++;
	    }
	  } else {
	    if (*p != '\n')
	    printf("%c", *p);
	    p ++;
	  }
	}

	printf("\n");
      } else p ++;
    }
#else
    get_cpuconfig();
#endif

#ifdef FORCE
    printf("#define CHAR_CORENAME \"%s\"\n", CORENAME);
#else
#if defined(INTEL_AMD) || defined(POWER) || defined(__mips__) || defined(__arm__) || defined(__aarch64__) || defined(ZARCH) || defined(sparc)
    printf("#define CHAR_CORENAME \"%s\"\n", get_corename());
#endif
#endif

 break;

  case '2' : /* SMP */
    if (get_num_cores() > 1) printf("SMP=1\n");
    break;
  }

  fflush(stdout);

  return 0;
}

