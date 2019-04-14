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
#include <signal.h>
#include <strings.h>
#define USE_FUNCTABLE
#include "../../interface/functable.h"

func_profile_t function_profile_table[MAX_PROF_TABLE];

int gotoblas_profile = 1;

static struct sigaction sa, ig;

void gotoblas_profile_quit(void) {

  int i;
  unsigned long long calls, fops, cycles, tcycles, area;

  sigaction(SIGPROF, &ig, NULL);

  calls   = 0;
  fops    = 0;
  cycles  = 0;
  tcycles = 0;
  area    = 0;

  for (i = 0; i < MAX_PROF_TABLE; i ++) {
    if (function_profile_table[i].calls) {
      calls   += function_profile_table[i].calls;
      cycles  += function_profile_table[i].cycles;
      tcycles += function_profile_table[i].tcycles;
      area    += function_profile_table[i].area;
      fops    += function_profile_table[i].fops;
    }
  }

  if (cycles > 0) {

    fprintf(stderr, "\n\t====== BLAS Profiling Result =======\n\n");
    fprintf(stderr, "  Function      No. of Calls   Time Consumption   Efficiency  Bytes/cycle  Wall Time(Cycles)\n");

    for (i = 0; i < MAX_PROF_TABLE; i ++) {
      if (function_profile_table[i].calls) {
#ifndef OS_WINDOWS
	fprintf(stderr, "%-12s  : %10Ld        %8.2f%%      %10.3f%%  %8.2f   %Ld\n",
#else
	fprintf(stderr, "%-12s  : %10lld        %8.2f%%      %10.3f%%  %8.2f   %lld\n",
#endif
		func_table[i],
		function_profile_table[i].calls,
		(double)function_profile_table[i].cycles  / (double)cycles * 100.,
		(double)function_profile_table[i].fops    / (double)function_profile_table[i].tcycles * 100.,
			(double)function_profile_table[i].area    / (double)function_profile_table[i].cycles,
			function_profile_table[i].cycles
		);
      }
    }

    fprintf(stderr, " --------------------------------------------------------------------\n");

#ifndef OS_WINDOWS
	fprintf(stderr, "%-12s  : %10Ld                       %10.3f%%  %8.2f\n",
#else
	fprintf(stderr, "%-12s  : %10lld                       %10.3f%%  %8.2f\n",
#endif
		"Total",
		 calls,
		(double)fops    / (double)tcycles * 100.,
		(double)area    / (double)cycles);
  }

  sigaction(SIGPROF, &sa, NULL);
}

void gotoblas_profile_clear(void) {

  int i;

  for (i = 0; i < MAX_PROF_TABLE; i ++) {
    function_profile_table[i].calls  = 0;
    function_profile_table[i].cycles = 0;
    function_profile_table[i].tcycles = 0;
    function_profile_table[i].area = 0;
    function_profile_table[i].fops = 0;
  }

}

void gotoblas_profile_init(void) {

  gotoblas_profile_clear();

  bzero(&sa, sizeof(struct sigaction));
  sa.sa_handler = (void *)gotoblas_profile_quit;
  sa.sa_flags  = SA_NODEFER | SA_RESETHAND;

  bzero(&ig, sizeof(struct sigaction));
  ig.sa_handler = SIG_IGN;
  ig.sa_flags |= SA_NODEFER | SA_RESETHAND;

  sigaction(SIGPROF, &sa, NULL);

}
