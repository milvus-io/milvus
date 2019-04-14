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

#include "common.h"
#include <asm/hwcap.h>
#include <sys/auxv.h>

extern gotoblas_t  gotoblas_ARMV8;
extern gotoblas_t  gotoblas_CORTEXA57;
extern gotoblas_t  gotoblas_THUNDERX;
extern gotoblas_t  gotoblas_THUNDERX2T99;

extern void openblas_warning(int verbose, const char * msg);

#define NUM_CORETYPES    4

/*
 * In case asm/hwcap.h is outdated on the build system, make sure
 * that HWCAP_CPUID is defined 
 */
#ifndef HWCAP_CPUID
#define HWCAP_CPUID (1 << 11)
#endif

#define get_cpu_ftr(id, var) ({					\
		asm("mrs %0, "#id : "=r" (var));		\
	})

static char *corename[] = {
  "armv8",
  "cortexa57",
  "thunderx",
  "thunderx2t99",
  "unknown"
};

char *gotoblas_corename(void) {
  if (gotoblas == &gotoblas_ARMV8)        return corename[ 0];
  if (gotoblas == &gotoblas_CORTEXA57)    return corename[ 1];
  if (gotoblas == &gotoblas_THUNDERX)     return corename[ 2];
  if (gotoblas == &gotoblas_THUNDERX2T99) return corename[ 3];
  return corename[NUM_CORETYPES];
}

static gotoblas_t *force_coretype(char *coretype) {
  int i ;
  int found = -1;
  char message[128];

  for ( i=0 ; i < NUM_CORETYPES; i++)
  {
    if (!strncasecmp(coretype, corename[i], 20))
    {
        found = i;
        break;
    }
  }

  switch (found)
  {
    case  0: return (&gotoblas_ARMV8);
    case  1: return (&gotoblas_CORTEXA57);
    case  2: return (&gotoblas_THUNDERX);
    case  3: return (&gotoblas_THUNDERX2T99);
  }
  snprintf(message, 128, "Core not found: %s\n", coretype);
  openblas_warning(1, message);
  return NULL;
}

static gotoblas_t *get_coretype(void) {
  int implementer, variant, part, arch, revision, midr_el1;
  
  if (!(getauxval(AT_HWCAP) & HWCAP_CPUID)) {
    char coremsg[128];
    snprintf(coremsg, 128, "Kernel lacks cpuid feature support. Auto detection of core type failed !!!\n");
    openblas_warning(1, coremsg);
    return NULL;
  }

  get_cpu_ftr(MIDR_EL1, midr_el1);
  /*
   * MIDR_EL1
   *
   * 31          24 23     20 19          16 15          4 3        0
   * -----------------------------------------------------------------
   * | Implementer | Variant | Architecture | Part Number | Revision |
   * -----------------------------------------------------------------
   */
  implementer = (midr_el1 >> 24) & 0xFF;
  part        = (midr_el1 >> 4)  & 0xFFF;

  switch(implementer)
  {
    case 0x41: // ARM
      switch (part)
      {
        case 0xd07: // Cortex A57
        case 0xd08: // Cortex A72
        case 0xd03: // Cortex A53
          return &gotoblas_CORTEXA57;
      }
      break;
    case 0x42: // Broadcom
      switch (part)
      {
        case 0x516: // Vulcan
          return &gotoblas_THUNDERX2T99;
      }
      break;
    case 0x43: // Cavium
      switch (part)
      {
        case 0x0a1: // ThunderX
          return &gotoblas_THUNDERX;
        case 0x0af: // ThunderX2
          return &gotoblas_THUNDERX2T99;
      }
      break;
  }
  return NULL;
}

void gotoblas_dynamic_init(void) {

  char coremsg[128];
  char coren[22];
  char *p;

  if (gotoblas) return;

  p = getenv("OPENBLAS_CORETYPE");
  if ( p )
  {
    gotoblas = force_coretype(p);
  }
  else
  {
    gotoblas = get_coretype();
  }

  if (gotoblas == NULL)
  {
    snprintf(coremsg, 128, "Falling back to generic ARMV8 core\n");
    openblas_warning(1, coremsg);
    gotoblas = &gotoblas_ARMV8;
  }

  if (gotoblas && gotoblas->init) {
    strncpy(coren, gotoblas_corename(), 20);
    sprintf(coremsg, "Core: %s\n", coren);
    openblas_warning(2, coremsg);
    gotoblas -> init();
  } else {
    openblas_warning(0, "OpenBLAS : Architecture Initialization failed. No initialization function found.\n");
    exit(1);
  }

}

void gotoblas_dynamic_quit(void) {
  gotoblas = NULL;
}
