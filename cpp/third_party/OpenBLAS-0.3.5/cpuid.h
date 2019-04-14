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

#ifndef CPUID_H
#define CPUID_H

#if defined(__i386__) || defined(__x86_64__) || defined(_M_IX86) || defined(_M_X64)
#define INTEL_AMD
#endif

#define VENDOR_INTEL      1
#define VENDOR_UMC        2
#define VENDOR_AMD        3
#define VENDOR_CYRIX      4
#define VENDOR_NEXGEN     5
#define VENDOR_CENTAUR    6
#define VENDOR_RISE       7
#define VENDOR_SIS	  8
#define VENDOR_TRANSMETA  9
#define VENDOR_NSC	 10
#define VENDOR_UNKNOWN   99

#define BITMASK(a, b, c) ((((a) >> (b)) & (c)))

#define FAMILY_80486  4
#define FAMILY_P5     5
#define FAMILY_P6     6
#define FAMILY_PM     7
#define FAMILY_IA64   8

#ifdef INTEL_AMD
#define GET_EXFAMILY  1
#define GET_EXMODEL   2
#define GET_TYPE      3
#define GET_FAMILY    4
#define GET_MODEL     5
#define GET_APICID    6
#define GET_LCOUNT    7
#define GET_CHUNKS    8
#define GET_STEPPING  9
#define GET_BLANDID  10
#define GET_FEATURE  11
#define GET_NUMSHARE 12
#define GET_NUMCORES 13
#endif

#ifdef __ia64__
#define GET_ARCHREV   1
#define GET_FAMILY    2
#define GET_MODEL     3
#define GET_REVISION  4
#define GET_NUMBER    5
#endif

#define CORE_UNKNOWN     0
#define CORE_80486       1
#define CORE_P5          2
#define CORE_P6          3
#define CORE_KATMAI      4
#define CORE_COPPERMINE  5
#define CORE_NORTHWOOD   6
#define CORE_PRESCOTT    7
#define CORE_BANIAS      8
#define CORE_ATHLON      9
#define CORE_OPTERON    10
#define CORE_BARCELONA  11
#define CORE_VIAC3      12
#define CORE_YONAH	13
#define CORE_CORE2	14
#define CORE_PENRYN	15
#define CORE_DUNNINGTON	16
#define CORE_NEHALEM	17
#define CORE_ATOM	18
#define CORE_NANO	19
#define CORE_SANDYBRIDGE 20
#define CORE_BOBCAT      21
#define CORE_BULLDOZER   22
#define CORE_PILEDRIVER  23
#define CORE_HASWELL     24
#define CORE_STEAMROLLER 25
#define CORE_EXCAVATOR   26
#define CORE_ZEN         27
#define CORE_SKYLAKEX    28

#define HAVE_SSE      (1 <<  0)
#define HAVE_SSE2     (1 <<  1)
#define HAVE_SSE3     (1 <<  2)
#define HAVE_SSSE3    (1 <<  3)
#define HAVE_SSE4_1   (1 <<  4)
#define HAVE_SSE4_2   (1 <<  5)
#define HAVE_SSE4A    (1 <<  6)
#define HAVE_SSE5     (1 <<  7)
#define HAVE_MMX      (1 <<  8)
#define HAVE_3DNOW    (1 <<  9)
#define HAVE_3DNOWEX  (1 << 10)
#define HAVE_CMOV     (1 << 11)
#define HAVE_PSE      (1 << 12)
#define HAVE_CFLUSH   (1 << 13)
#define HAVE_HIT      (1 << 14)
#define HAVE_MISALIGNSSE (1 << 15)
#define HAVE_128BITFPU   (1 << 16)
#define HAVE_FASTMOVU    (1 << 17)
#define HAVE_AVX      (1 <<  18)
#define HAVE_FMA4     (1 <<  19)
#define HAVE_FMA3     (1 <<  20)
#define HAVE_AVX512VL (1 <<  21)

#define CACHE_INFO_L1_I     1
#define CACHE_INFO_L1_D     2
#define CACHE_INFO_L2       3
#define CACHE_INFO_L3       4
#define CACHE_INFO_L1_ITB   5
#define CACHE_INFO_L1_DTB   6
#define CACHE_INFO_L1_LITB  7
#define CACHE_INFO_L1_LDTB  8
#define CACHE_INFO_L2_ITB   9
#define CACHE_INFO_L2_DTB  10
#define CACHE_INFO_L2_LITB 11
#define CACHE_INFO_L2_LDTB 12

typedef struct {
  int size;
  int associative;
  int linesize;
  int shared;
} cache_info_t;

#define CPUTYPE_UNKNOWN			 0
#define CPUTYPE_INTEL_UNKNOWN		 1
#define CPUTYPE_UMC_UNKNOWN		 2
#define CPUTYPE_AMD_UNKNOWN		 3
#define CPUTYPE_CYRIX_UNKNOWN		 4
#define CPUTYPE_NEXGEN_UNKNOWN		 5
#define CPUTYPE_CENTAUR_UNKNOWN		 6
#define CPUTYPE_RISE_UNKNOWN		 7
#define CPUTYPE_SIS_UNKNOWN		 8
#define CPUTYPE_TRANSMETA_UNKNOWN	 9
#define CPUTYPE_NSC_UNKNOWN		10

#define CPUTYPE_80386			11
#define CPUTYPE_80486			12
#define CPUTYPE_PENTIUM			13
#define CPUTYPE_PENTIUM2		14
#define CPUTYPE_PENTIUM3		15
#define CPUTYPE_PENTIUMM		16
#define CPUTYPE_PENTIUM4		17
#define CPUTYPE_CORE2			18
#define CPUTYPE_PENRYN			19
#define CPUTYPE_DUNNINGTON		20
#define CPUTYPE_NEHALEM			21
#define CPUTYPE_ATOM			22
#define CPUTYPE_ITANIUM			23
#define CPUTYPE_ITANIUM2		24
#define CPUTYPE_AMD5X86			25
#define CPUTYPE_AMDK6			26
#define CPUTYPE_ATHLON			27
#define CPUTYPE_DURON			28
#define CPUTYPE_OPTERON			29
#define CPUTYPE_BARCELONA		30
#define CPUTYPE_SHANGHAI		31
#define CPUTYPE_ISTANBUL		32
#define CPUTYPE_CYRIX5X86		33
#define CPUTYPE_CYRIXM1			34
#define CPUTYPE_CYRIXM2			35
#define CPUTYPE_NEXGENNX586		36
#define CPUTYPE_CENTAURC6		37
#define CPUTYPE_RISEMP6			38
#define CPUTYPE_SYS55X			39
#define CPUTYPE_CRUSOETM3X		40
#define CPUTYPE_NSGEODE			41
#define CPUTYPE_VIAC3			42
#define CPUTYPE_NANO			43
#define CPUTYPE_SANDYBRIDGE             44
#define CPUTYPE_BOBCAT                  45
#define CPUTYPE_BULLDOZER               46
#define CPUTYPE_PILEDRIVER              47
#define CPUTYPE_HASWELL 		48
#define CPUTYPE_STEAMROLLER 		49
#define CPUTYPE_EXCAVATOR 		50
#define CPUTYPE_ZEN 			51
#define CPUTYPE_SKYLAKEX		52

#endif
