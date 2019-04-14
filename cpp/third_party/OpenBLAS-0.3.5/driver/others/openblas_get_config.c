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

#include "common.h"

#include <string.h>

#if defined(_WIN32) && defined(_MSC_VER)
#if _MSC_VER < 1900
#define snprintf _snprintf
#endif
#endif

static char* openblas_config_str=""
"OpenBLAS "
 VERSION
" "
#ifdef USE64BITINT
  " USE64BITINT "
#endif
#ifdef NO_CBLAS
  "NO_CBLAS "
#endif
#ifdef NO_LAPACK
  "NO_LAPACK "
#endif
#ifdef NO_LAPACKE
  "NO_LAPACKE "
#endif
#ifdef DYNAMIC_ARCH
  "DYNAMIC_ARCH "
#endif
#ifdef NO_AFFINITY
  "NO_AFFINITY "
#endif
#ifdef USE_OPENMP
  "USE_OPENMP "
#endif
#ifndef DYNAMIC_ARCH
  CHAR_CORENAME
#endif
  ;

#ifdef DYNAMIC_ARCH
char *gotoblas_corename();
#endif

static char tmp_config_str[256];
int openblas_get_parallel();

char* CNAME() {
char tmpstr[20];
  strcpy(tmp_config_str, openblas_config_str);
#ifdef DYNAMIC_ARCH
  strcat(tmp_config_str, gotoblas_corename());
#endif
if (openblas_get_parallel() == 0)
  sprintf(tmpstr, " SINGLE_THREADED");
else 
  snprintf(tmpstr,19," MAX_THREADS=%d",MAX_CPU_NUMBER);
  strcat(tmp_config_str, tmpstr);
  return tmp_config_str;
}


char* openblas_get_corename() {
#ifndef DYNAMIC_ARCH 
  return CHAR_CORENAME;
#else
  return gotoblas_corename();
#endif
}

