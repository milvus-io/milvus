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

#if defined(__alpha) && defined(__DECC)
#include <c_asm.h>
#endif

int implver(void){
  int arch;

#ifndef __DECC
  asm __volatile__("implver %0" : "=r"(arch)  : : "memory");
#else
  arch = asm("implver %v0");
#endif
  return arch;
}

void get_architecture(void){
  printf("ALPHA");
}

void get_subarchitecture(void){
  printf("ev%d", implver() + 4);
}

void get_subdirname(void){
  printf("alpha");
}

void get_cpuconfig(void){
  printf("#define EV%d\n", implver() + 4);

  switch (implver()){
  case 0:
    printf("#define L1_DATA_SIZE 16384\n");
    printf("#define L1_DATA_LINESIZE 32\n");
    printf("#define L2_SIZE 2097152\n");
    printf("#define L2_LINESIZE 32\n");
    printf("#define DTB_DEFAULT_ENTRIES 32\n");
    printf("#define DTB_SIZE 8192\n");
    break;

  case 1:
    printf("#define L1_DATA_SIZE 16384\n");
    printf("#define L1_DATA_LINESIZE 32\n");
    printf("#define L2_SIZE 2097152\n");
    printf("#define L2_LINESIZE 64\n");
    printf("#define DTB_DEFAULT_ENTRIES 64\n");
    printf("#define DTB_SIZE 8192\n");
    break;

  case 2:
    printf("#define L1_DATA_SIZE 32768\n");
    printf("#define L1_DATA_LINESIZE 64\n");
    printf("#define L2_SIZE 4194304\n");
    printf("#define L2_LINESIZE 64\n");
    printf("#define DTB_DEFAULT_ENTRIES 64\n");
    printf("#define DTB_SIZE 8192\n");
    break;
  }
}

void get_libname(void){
  printf("ev%d\n", implver() + 4);
}
