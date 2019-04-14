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

#define CPU_UNKNOWN     0
#define CPU_P5600       1
#define CPU_1004K	2

static char *cpuname[] = {
  "UNKNOWN",
  "P5600",
  "1004K"
};

int detect(void){

#ifdef linux
  FILE *infile;
  char buffer[512], *p;

  p = (char *)NULL;
  infile = fopen("/proc/cpuinfo", "r");
  while (fgets(buffer, sizeof(buffer), infile)){
    if (!strncmp("cpu", buffer, 3)){
	p = strchr(buffer, ':') + 2;
#if 0
	fprintf(stderr, "%s \n", p);
#endif
	break;
      }
  }

  fclose(infile);

  if(p != NULL){
  if (strstr(p, "5600")) {
    return CPU_P5600;
  } else if (strstr(p, "1004K")) {
    return CPU_1004K;
  } else  
    return CPU_UNKNOWN;
  }
#endif
    return CPU_UNKNOWN;
}

char *get_corename(void){
  return cpuname[detect()];
}

void get_architecture(void){
  printf("MIPS");
}

void get_subarchitecture(void){
  if(detect()==CPU_P5600|| detect()==CPU_1004K){
    printf("P5600");
  }else{
    printf("UNKNOWN");
  }
}

void get_subdirname(void){
  printf("mips");
}

void get_cpuconfig(void){
  if(detect()==CPU_P5600){
    printf("#define P5600\n");
    printf("#define L1_DATA_SIZE 65536\n");
    printf("#define L1_DATA_LINESIZE 32\n");
    printf("#define L2_SIZE 1048576\n");
    printf("#define L2_LINESIZE 32\n");
    printf("#define DTB_DEFAULT_ENTRIES 64\n");
    printf("#define DTB_SIZE 4096\n");
    printf("#define L2_ASSOCIATIVE 8\n");
  } else if (detect()==CPU_1004K) {
    printf("#define MIPS1004K\n");
    printf("#define L1_DATA_SIZE 32768\n");
    printf("#define L1_DATA_LINESIZE 32\n");
    printf("#define L2_SIZE 26144\n");
    printf("#define DTB_DEFAULT_ENTRIES 8\n");
    printf("#define DTB_SIZE 4096\n");
    printf("#define L2_ASSOCIATIVE 4\n");
  }else{
    printf("#define UNKNOWN\n");
  }
}

void get_libname(void){
  if(detect()==CPU_P5600) {
    printf("p5600\n");
  } else if (detect()==CPU_1004K) {
    printf("1004K\n");
  }else{
    printf("mips\n");
  }
}
