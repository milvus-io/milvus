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

#include  <sys/utsname.h>
#ifdef _AIX
#include <sys/vminfo.h>
#endif
#ifdef __APPLE__
#include <mach/mach.h>
#include <mach/mach_host.h>
#include <mach/host_info.h>
#include <mach/machine.h>
#endif

#define CPUTYPE_UNKNOWN    0
#define CPUTYPE_POWER3     1
#define CPUTYPE_POWER4     2
#define CPUTYPE_PPC970     3
#define CPUTYPE_POWER5     4
#define CPUTYPE_POWER6     5
#define CPUTYPE_CELL       6
#define CPUTYPE_PPCG4	   7
#define CPUTYPE_POWER8     8
#define CPUTYPE_POWER9     9

char *cpuname[] = {
  "UNKNOWN",
  "POWER3",
  "POWER4",
  "PPC970",
  "POWER5",
  "POWER6",
  "CELL",
  "PPCG4",
  "POWER8",
  "POWER9"
};

char *lowercpuname[] = {
  "unknown",
  "power3",
  "power4",
  "ppc970",
  "power5",
  "power6",
  "cell",
  "ppcg4",
  "power8",
  "power9"	
};

char *corename[] = {
  "UNKNOWN",
  "POWER3",
  "POWER4",
  "POWER4",
  "POWER4",
  "POWER6",
  "CELL",
  "PPCG4",
  "POWER8",
  "POWER8"   	
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
	fprintf(stderr, "%s\n", p);
#endif
	break;
      }
  }

  fclose(infile);

  if (!strncasecmp(p, "POWER3", 6)) return CPUTYPE_POWER3;
  if (!strncasecmp(p, "POWER4", 6)) return CPUTYPE_POWER4;
  if (!strncasecmp(p, "PPC970", 6)) return CPUTYPE_PPC970;
  if (!strncasecmp(p, "POWER5", 6)) return CPUTYPE_POWER5;
  if (!strncasecmp(p, "POWER6", 6)) return CPUTYPE_POWER6;
  if (!strncasecmp(p, "POWER7", 6)) return CPUTYPE_POWER6;
  if (!strncasecmp(p, "POWER8", 6)) return CPUTYPE_POWER8;
  if (!strncasecmp(p, "POWER9", 6)) return CPUTYPE_POWER8;
  if (!strncasecmp(p, "Cell",   4)) return CPUTYPE_CELL;
  if (!strncasecmp(p, "7447",   4)) return CPUTYPE_PPCG4;

  return CPUTYPE_UNKNOWN;
#endif

#ifdef _AIX
  FILE *infile;
  char buffer[512], *p;

  p = (char *)NULL;
  infile = popen("prtconf|grep 'Processor Type'", "r");
  while (fgets(buffer, sizeof(buffer), infile)){
    if (!strncmp("Pro", buffer, 3)){
	p = strchr(buffer, ':') + 2;
#if 0
	fprintf(stderr, "%s\n", p);
#endif
	break;
      }
  }

  pclose(infile);

  if (!strncasecmp(p, "POWER3", 6)) return CPUTYPE_POWER3;
  if (!strncasecmp(p, "POWER4", 6)) return CPUTYPE_POWER4;
  if (!strncasecmp(p, "PPC970", 6)) return CPUTYPE_PPC970;
  if (!strncasecmp(p, "POWER5", 6)) return CPUTYPE_POWER5;
  if (!strncasecmp(p, "POWER6", 6)) return CPUTYPE_POWER6;
  if (!strncasecmp(p, "POWER7", 6)) return CPUTYPE_POWER6;
  if (!strncasecmp(p, "POWER8", 6)) return CPUTYPE_POWER8;
  if (!strncasecmp(p, "POWER9", 6)) return CPUTYPE_POWER8;
  if (!strncasecmp(p, "Cell",   4)) return CPUTYPE_CELL;
  if (!strncasecmp(p, "7447",   4)) return CPUTYPE_PPCG4;
  return CPUTYPE_POWER5;
#endif

#ifdef __APPLE__
  host_basic_info_data_t   hostInfo;
  mach_msg_type_number_t  infoCount;

  infoCount = HOST_BASIC_INFO_COUNT;
  host_info(mach_host_self(), HOST_BASIC_INFO, (host_info_t)&hostInfo, &infoCount);

  if (hostInfo.cpu_subtype == CPU_SUBTYPE_POWERPC_7450) return CPUTYPE_PPCG4;
  if (hostInfo.cpu_subtype == CPU_SUBTYPE_POWERPC_970)  return CPUTYPE_PPC970;

  return  CPUTYPE_PPC970;
#endif

#if defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
int id;
__asm __volatile("mfpvr %0" : "=r"(id));
switch ( id >> 16 ) {
  case 0x4e: // POWER9
    return CPUTYPE_POWER8;
    break;
  case 0x4d:
  case 0x4b: // POWER8/8E 
    return CPUTYPE_POWER8;
    break;
  case 0x4a:
  case 0x3f:  // POWER7/7E
    return CPUTYPE_POWER6; 
    break;
  case 0x3e:
    return CPUTYPE_POWER6;
    break;
  case 0x3a:
    return CPUTYPE_POWER5;
    break;
  case 0x35:
  case 0x38: // POWER4 /4+ 
    return CPUTYPE_POWER4;
    break;
  case 0x40:
  case 0x41: // POWER3 /3+ 
    return CPUTYPE_POWER3;
    break;
  case 0x39:
  case 0x3c:
  case 0x44:
  case 0x45:
    return CPUTYPE_PPC970;
    break;
  case 0x70: 
    return CPUTYPE_CELL;
    break;
  case 0x8003: 
    return CPUTYPE_PPCG4;
    break;
  default:  
    return  CPUTYPE_UNKNOWN;
  }
#endif
}

void get_architecture(void){
  printf("POWER");
}

void get_subdirname(void){
    printf("power");
}


void get_subarchitecture(void){
  printf("%s", cpuname[detect()]);
}

void get_cpuconfig(void){
#if 0
#ifdef _AIX
  struct vminfo info;
#endif
#endif

  printf("#define %s\n", cpuname[detect()]);
  printf("#define CORE_%s\n", corename[detect()]);

  printf("#define L1_DATA_SIZE 32768\n");
  printf("#define L1_DATA_LINESIZE 128\n");
  printf("#define L2_SIZE 524288\n");
  printf("#define L2_LINESIZE 128 \n");
  printf("#define DTB_DEFAULT_ENTRIES 128\n");
  printf("#define DTB_SIZE 4096\n");
  printf("#define L2_ASSOCIATIVE 8\n");

#if 0
#ifdef _AIX
  if (vmgetinfo(&info, VMINFO, 0) == 0) {
    if ((info.lgpg_size >> 20) >= 1024) {
      printf("#define ALLOC_HUGETLB\n");
    }
  }
#endif
#endif

}

void get_libname(void){
  printf("%s", lowercpuname[detect()]);
}

char *get_corename(void){
  return cpuname[detect()];
}
