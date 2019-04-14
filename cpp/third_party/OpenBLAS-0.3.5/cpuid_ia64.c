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

#include <stdio.h>
#include <string.h>
#include <sys/sysinfo.h>
#include "cpuid.h"

#ifdef __ECC
#include <ia64intrin.h>
#endif

static inline unsigned long cpuid(unsigned long regnum){
  unsigned long value;

#ifdef __ECC
  value = __getIndReg(_IA64_REG_INDR_CPUID, regnum);
#else
  asm ("mov %0=cpuid[%r1]" : "=r"(value) : "rO"(regnum));
#endif

  return value;
}

int have_cpuid(void){ return 1;}

int get_vendor(void){
  unsigned long cpuid0, cpuid1;
  char vendor[18];

  cpuid0 = cpuid(0);
  cpuid1 = cpuid(1);

  *(unsigned long *)(&vendor[0]) = cpuid0;
  *(unsigned long *)(&vendor[8]) = cpuid1;
  vendor[17] = (char)0;

  if (!strcmp(vendor, "GenuineIntel")) return VENDOR_INTEL;

  return VENDOR_UNKNOWN;
}

int get_cputype(int gettype){
  unsigned long cpuid3;

  cpuid3 = cpuid(3);

  switch (gettype) {
  case GET_ARCHREV :
    return BITMASK(cpuid3, 32, 0xff);
  case GET_FAMILY :
    return BITMASK(cpuid3, 24, 0xff);
  case GET_MODEL :
    return BITMASK(cpuid3, 16, 0xff);
  case GET_REVISION :
    return BITMASK(cpuid3,  8, 0xff);
  case GET_NUMBER :
    return BITMASK(cpuid3,  0, 0xff);
  }

  return 0;
}

char *get_cpunamechar(void){
  if (get_cputype(GET_FAMILY) == 0x07) return "ITANIUM";
  if (get_cputype(GET_FAMILY) == 0x1f) return "ITANIUM2";
  if (get_cputype(GET_FAMILY) == 0x20) return "ITANIUM2";

  return "UNKNOWN";
}

char *get_libname(void){
  if (get_cputype(GET_FAMILY) == 0x07) { printf("itanium"); return NULL;}
  if (get_cputype(GET_FAMILY) == 0x1f) { printf("itanium2"); return NULL;}
  if (get_cputype(GET_FAMILY) == 0x20) { printf("itanium2"); return NULL;}

  printf("UNKNOWN");

  return NULL;
}

void get_architecture(void){
  printf("IA64");
}

void get_subarchitecture(void){
    printf("%s", get_cpunamechar());
}

void get_subdirname(void){
    printf("ia64");
}

void get_cpuconfig(void){
  printf("#define %s\n", get_cpunamechar());
  printf("#define L1_DATA_SIZE 262144\n");
  printf("#define L1_DATA_LINESIZE 128\n");
  printf("#define L2_SIZE 1572864\n");
  printf("#define L2_LINESIZE 128\n");
  printf("#define DTB_SIZE 16384\n");
  printf("#define DTB_DEFAULT_ENTRIES 128\n");
}

