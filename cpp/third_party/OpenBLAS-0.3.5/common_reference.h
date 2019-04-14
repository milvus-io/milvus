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
#ifndef ASSEMBLER

#define REF_BU f
#define BLASFUNC_REF_2(x,y) BLASFUNC(x## y)
#define BLASFUNC_REF_1(x,y) BLASFUNC_REF_2(x,y)
#define BLASFUNC_REF(x) BLASFUNC_REF_1(x,REF_BU)

void  BLASFUNC_REF(srot)  (blasint *, float  *, blasint *, float  *, blasint *, float  *, float  *);
void  BLASFUNC_REF(drot)  (blasint *, double *, blasint *, double *, blasint *, double *, double *);
void  BLASFUNC_REF(qrot)  (blasint *, xdouble *, blasint *, xdouble *, blasint *, xdouble *, xdouble *);
void  BLASFUNC_REF(csrot) (blasint *, float  *, blasint *, float  *, blasint *, float  *, float  *);
void  BLASFUNC_REF(zdrot) (blasint *, double *, blasint *, double *, blasint *, double *, double *);
void  BLASFUNC_REF(xqrot) (blasint *, xdouble *, blasint *, xdouble *, blasint *, xdouble *, xdouble *);

void BLASFUNC_REF(sswap) (blasint *, float  *, blasint *, float  *, blasint *);
void BLASFUNC_REF(dswap) (blasint *, double *, blasint *, double *, blasint *);
void BLASFUNC_REF(qswap) (blasint *, xdouble *, blasint *, xdouble *, blasint *);
void BLASFUNC_REF(cswap) (blasint *, float  *, blasint *, float  *, blasint *);
void BLASFUNC_REF(zswap) (blasint *, double *, blasint *, double *, blasint *);
void BLASFUNC_REF(xswap) (blasint *, xdouble *, blasint *, xdouble *, blasint *);

void BLASFUNC_REF(saxpy) (blasint *, float  *, float  *, blasint *, float  *, blasint *);
void BLASFUNC_REF(daxpy) (blasint *, double *, double *, blasint *, double *, blasint *);
void BLASFUNC_REF(caxpy) (blasint *, float  *, float  *, blasint *, float  *, blasint *);
void BLASFUNC_REF(zaxpy) (blasint *, double *, double *, blasint *, double *, blasint *);

float   _Complex BLASFUNC_REF(cdotu)  (blasint *, float  *, blasint *, float  *, blasint *);
float   _Complex BLASFUNC_REF(cdotc)  (blasint *, float  *, blasint *, float  *, blasint *);
double  _Complex BLASFUNC_REF(zdotu)  (blasint *, double  *, blasint *, double  *, blasint *);
double  _Complex BLASFUNC_REF(zdotc)  (blasint *, double  *, blasint *, double  *, blasint *);

void BLASFUNC_REF(drotmg)(double *, double *, double *, double *, double *);

double BLASFUNC_REF(dsdot)(blasint *, float *, blasint *, float *, blasint*);

FLOATRET  BLASFUNC_REF(samax) (blasint *, float  *, blasint *);

#endif
