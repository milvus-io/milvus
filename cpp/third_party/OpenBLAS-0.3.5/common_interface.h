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

#ifndef ASSEMBLER

#ifdef __cplusplus
extern "C" {
	/* Assume C declarations for C++ */
#endif  /* __cplusplus */

int    BLASFUNC(xerbla)(char *, blasint *info, blasint);

void    openblas_set_num_threads_(int *);

FLOATRET  BLASFUNC(sdot)  (blasint *, float  *, blasint *, float  *, blasint *);
FLOATRET  BLASFUNC(sdsdot)(blasint *, float  *,        float  *, blasint *, float  *, blasint *);

double BLASFUNC(dsdot) (blasint *, float  *, blasint *, float  *, blasint *);
double BLASFUNC(ddot)  (blasint *, double *, blasint *, double *, blasint *);
xdouble BLASFUNC(qdot)  (blasint *, xdouble *, blasint *, xdouble *, blasint *);


#ifdef RETURN_BY_STRUCT
typedef struct {
  float r, i;
} myccomplex_t;

typedef struct {
  double r, i;
} myzcomplex_t;

typedef struct {
  xdouble r, i;
} myxcomplex_t;

myccomplex_t    BLASFUNC(cdotu)  (blasint *, float  *, blasint *, float  *, blasint *);
myccomplex_t    BLASFUNC(cdotc)  (blasint *, float  *, blasint *, float  *, blasint *);
myzcomplex_t    BLASFUNC(zdotu)  (blasint *, double  *, blasint *, double  *, blasint *);
myzcomplex_t    BLASFUNC(zdotc)  (blasint *, double  *, blasint *, double  *, blasint *);
myxcomplex_t    BLASFUNC(xdotu)  (blasint *, xdouble  *, blasint *, xdouble  *, blasint *);
myxcomplex_t    BLASFUNC(xdotc)  (blasint *, xdouble  *, blasint *, xdouble  *, blasint *);

#elif defined RETURN_BY_STACK
void  BLASFUNC(cdotu)  (openblas_complex_float   *,  blasint *, float  * , blasint *, float  *,  blasint *);
void  BLASFUNC(cdotc)  (openblas_complex_float   *,  blasint *, float  *,  blasint *, float  *,  blasint *);
void  BLASFUNC(zdotu)  (openblas_complex_double  *, blasint *, double  *, blasint *, double  *, blasint *);
void  BLASFUNC(zdotc)  (openblas_complex_double  *, blasint *, double  *, blasint *, double  *, blasint *);
void  BLASFUNC(xdotu)  (openblas_complex_xdouble *, blasint *, xdouble  *, blasint *, xdouble  *, blasint *);
void  BLASFUNC(xdotc)  (openblas_complex_xdouble *, blasint *, xdouble  *, blasint *, xdouble  *, blasint *);
#else
openblas_complex_float   BLASFUNC(cdotu)  (blasint *, float  *, blasint *, float  *, blasint *);
openblas_complex_float   BLASFUNC(cdotc)  (blasint *, float  *, blasint *, float  *, blasint *);
openblas_complex_double  BLASFUNC(zdotu)  (blasint *, double  *, blasint *, double  *, blasint *);
openblas_complex_double  BLASFUNC(zdotc)  (blasint *, double  *, blasint *, double  *, blasint *);
openblas_complex_xdouble BLASFUNC(xdotu)  (blasint *, xdouble  *, blasint *, xdouble  *, blasint *);
openblas_complex_xdouble BLASFUNC(xdotc)  (blasint *, xdouble  *, blasint *, xdouble  *, blasint *);
#endif

void    BLASFUNC(saxpy) (blasint *, float  *, float  *, blasint *, float  *, blasint *);
void    BLASFUNC(daxpy) (blasint *, double *, double *, blasint *, double *, blasint *);
void    BLASFUNC(qaxpy) (blasint *, xdouble *, xdouble *, blasint *, xdouble *, blasint *);
void    BLASFUNC(caxpy) (blasint *, float  *, float  *, blasint *, float  *, blasint *);
void    BLASFUNC(zaxpy) (blasint *, double *, double *, blasint *, double *, blasint *);
void    BLASFUNC(xaxpy) (blasint *, xdouble *, xdouble *, blasint *, xdouble *, blasint *);
void    BLASFUNC(caxpyc)(blasint *, float  *, float  *, blasint *, float  *, blasint *);
void    BLASFUNC(zaxpyc)(blasint *, double *, double *, blasint *, double *, blasint *);
void    BLASFUNC(xaxpyc)(blasint *, xdouble *, xdouble *, blasint *, xdouble *, blasint *);

void    BLASFUNC(scopy) (blasint *, float  *, blasint *, float  *, blasint *);
void    BLASFUNC(dcopy) (blasint *, double *, blasint *, double *, blasint *);
void    BLASFUNC(qcopy) (blasint *, xdouble *, blasint *, xdouble *, blasint *);
void    BLASFUNC(ccopy) (blasint *, float  *, blasint *, float  *, blasint *);
void    BLASFUNC(zcopy) (blasint *, double *, blasint *, double *, blasint *);
void    BLASFUNC(xcopy) (blasint *, xdouble *, blasint *, xdouble *, blasint *);

void    BLASFUNC(sswap) (blasint *, float  *, blasint *, float  *, blasint *);
void    BLASFUNC(dswap) (blasint *, double *, blasint *, double *, blasint *);
void    BLASFUNC(qswap) (blasint *, xdouble *, blasint *, xdouble *, blasint *);
void    BLASFUNC(cswap) (blasint *, float  *, blasint *, float  *, blasint *);
void    BLASFUNC(zswap) (blasint *, double *, blasint *, double *, blasint *);
void    BLASFUNC(xswap) (blasint *, xdouble *, blasint *, xdouble *, blasint *);

FLOATRET  BLASFUNC(sasum) (blasint *, float  *, blasint *);
FLOATRET  BLASFUNC(scasum)(blasint *, float  *, blasint *);
double BLASFUNC(dasum) (blasint *, double *, blasint *);
xdouble BLASFUNC(qasum) (blasint *, xdouble *, blasint *);
double BLASFUNC(dzasum)(blasint *, double *, blasint *);
xdouble BLASFUNC(qxasum)(blasint *, xdouble *, blasint *);

blasint    BLASFUNC(isamax)(blasint *, float  *, blasint *);
blasint    BLASFUNC(idamax)(blasint *, double *, blasint *);
blasint    BLASFUNC(iqamax)(blasint *, xdouble *, blasint *);
blasint    BLASFUNC(icamax)(blasint *, float  *, blasint *);
blasint    BLASFUNC(izamax)(blasint *, double *, blasint *);
blasint    BLASFUNC(ixamax)(blasint *, xdouble *, blasint *);

blasint    BLASFUNC(ismax) (blasint *, float  *, blasint *);
blasint    BLASFUNC(idmax) (blasint *, double *, blasint *);
blasint    BLASFUNC(iqmax) (blasint *, xdouble *, blasint *);
blasint    BLASFUNC(icmax) (blasint *, float  *, blasint *);
blasint    BLASFUNC(izmax) (blasint *, double *, blasint *);
blasint    BLASFUNC(ixmax) (blasint *, xdouble *, blasint *);

blasint    BLASFUNC(isamin)(blasint *, float  *, blasint *);
blasint    BLASFUNC(idamin)(blasint *, double *, blasint *);
blasint    BLASFUNC(iqamin)(blasint *, xdouble *, blasint *);
blasint    BLASFUNC(icamin)(blasint *, float  *, blasint *);
blasint    BLASFUNC(izamin)(blasint *, double *, blasint *);
blasint    BLASFUNC(ixamin)(blasint *, xdouble *, blasint *);

blasint    BLASFUNC(ismin)(blasint *, float  *, blasint *);
blasint    BLASFUNC(idmin)(blasint *, double *, blasint *);
blasint    BLASFUNC(iqmin)(blasint *, xdouble *, blasint *);
blasint    BLASFUNC(icmin)(blasint *, float  *, blasint *);
blasint    BLASFUNC(izmin)(blasint *, double *, blasint *);
blasint    BLASFUNC(ixmin)(blasint *, xdouble *, blasint *);

FLOATRET  BLASFUNC(samax) (blasint *, float  *, blasint *);
double BLASFUNC(damax) (blasint *, double *, blasint *);
xdouble BLASFUNC(qamax) (blasint *, xdouble *, blasint *);
FLOATRET  BLASFUNC(scamax)(blasint *, float  *, blasint *);
double BLASFUNC(dzamax)(blasint *, double *, blasint *);
xdouble BLASFUNC(qxamax)(blasint *, xdouble *, blasint *);

FLOATRET  BLASFUNC(samin) (blasint *, float  *, blasint *);
double BLASFUNC(damin) (blasint *, double *, blasint *);
xdouble BLASFUNC(qamin) (blasint *, xdouble *, blasint *);
FLOATRET  BLASFUNC(scamin)(blasint *, float  *, blasint *);
double BLASFUNC(dzamin)(blasint *, double *, blasint *);
xdouble BLASFUNC(qxamin)(blasint *, xdouble *, blasint *);

FLOATRET  BLASFUNC(smax)  (blasint *, float  *, blasint *);
double BLASFUNC(dmax)  (blasint *, double *, blasint *);
xdouble BLASFUNC(qmax)  (blasint *, xdouble *, blasint *);
FLOATRET  BLASFUNC(scmax) (blasint *, float  *, blasint *);
double BLASFUNC(dzmax) (blasint *, double *, blasint *);
xdouble BLASFUNC(qxmax) (blasint *, xdouble *, blasint *);

FLOATRET  BLASFUNC(smin)  (blasint *, float  *, blasint *);
double BLASFUNC(dmin)  (blasint *, double *, blasint *);
xdouble BLASFUNC(qmin)  (blasint *, xdouble *, blasint *);
FLOATRET  BLASFUNC(scmin) (blasint *, float  *, blasint *);
double BLASFUNC(dzmin) (blasint *, double *, blasint *);
xdouble BLASFUNC(qxmin) (blasint *, xdouble *, blasint *);

void    BLASFUNC(sscal) (blasint *,  float  *, float  *, blasint *);
void    BLASFUNC(dscal) (blasint *,  double *, double *, blasint *);
void    BLASFUNC(qscal) (blasint *,  xdouble *, xdouble *, blasint *);
void    BLASFUNC(cscal) (blasint *,  float  *, float  *, blasint *);
void    BLASFUNC(zscal) (blasint *,  double *, double *, blasint *);
void    BLASFUNC(xscal) (blasint *,  xdouble *, xdouble *, blasint *);
void    BLASFUNC(csscal)(blasint *,  float  *, float  *, blasint *);
void    BLASFUNC(zdscal)(blasint *,  double *, double *, blasint *);
void    BLASFUNC(xqscal)(blasint *,  xdouble *, xdouble *, blasint *);

FLOATRET  BLASFUNC(snrm2) (blasint *, float  *, blasint *);
FLOATRET  BLASFUNC(scnrm2)(blasint *, float  *, blasint *);

double BLASFUNC(dnrm2) (blasint *, double *, blasint *);
xdouble BLASFUNC(qnrm2) (blasint *, xdouble *, blasint *);
double BLASFUNC(dznrm2)(blasint *, double *, blasint *);
xdouble BLASFUNC(qxnrm2)(blasint *, xdouble *, blasint *);

void  BLASFUNC(srot)  (blasint *, float  *, blasint *, float  *, blasint *, float  *, float  *);
void  BLASFUNC(drot)  (blasint *, double *, blasint *, double *, blasint *, double *, double *);
void  BLASFUNC(qrot)  (blasint *, xdouble *, blasint *, xdouble *, blasint *, xdouble *, xdouble *);
void  BLASFUNC(csrot) (blasint *, float  *, blasint *, float  *, blasint *, float  *, float  *);
void  BLASFUNC(zdrot) (blasint *, double *, blasint *, double *, blasint *, double *, double *);
void  BLASFUNC(xqrot) (blasint *, xdouble *, blasint *, xdouble *, blasint *, xdouble *, xdouble *);

void  BLASFUNC(srotg) (float  *, float  *, float  *, float  *);
void  BLASFUNC(drotg) (double *, double *, double *, double *);
void  BLASFUNC(qrotg) (xdouble *, xdouble *, xdouble *, xdouble *);
void  BLASFUNC(crotg) (float  *, float  *, float  *, float  *);
void  BLASFUNC(zrotg) (double *, double *, double *, double *);
void  BLASFUNC(xrotg) (xdouble *, xdouble *, xdouble *, xdouble *);

void  BLASFUNC(srotmg)(float  *, float  *, float  *, float  *, float  *);
void  BLASFUNC(drotmg)(double *, double *, double *, double *, double *);

void  BLASFUNC(srotm) (blasint *, float  *, blasint *, float  *, blasint *, float  *);
void  BLASFUNC(drotm) (blasint *, double *, blasint *, double *, blasint *, double *);
void  BLASFUNC(qrotm) (blasint *, xdouble *, blasint *, xdouble *, blasint *, xdouble *);

/* Level 2 routines */

void BLASFUNC(sger)(blasint *,    blasint *, float *,  float *, blasint *,
		   float *,  blasint *, float *,  blasint *);
void BLASFUNC(dger)(blasint *,    blasint *, double *, double *, blasint *,
		   double *, blasint *, double *, blasint *);
void BLASFUNC(qger)(blasint *,    blasint *, xdouble *, xdouble *, blasint *,
		   xdouble *, blasint *, xdouble *, blasint *);
void BLASFUNC(cgeru)(blasint *,    blasint *, float *,  float *, blasint *,
		    float *,  blasint *, float *,  blasint *);
void BLASFUNC(cgerc)(blasint *,    blasint *, float *,  float *, blasint *,
		    float *,  blasint *, float *,  blasint *);
void BLASFUNC(zgeru)(blasint *,    blasint *, double *, double *, blasint *,
		    double *, blasint *, double *, blasint *);
void BLASFUNC(zgerc)(blasint *,    blasint *, double *, double *, blasint *,
		    double *, blasint *, double *, blasint *);
void BLASFUNC(xgeru)(blasint *,    blasint *, xdouble *, xdouble *, blasint *,
		    xdouble *, blasint *, xdouble *, blasint *);
void BLASFUNC(xgerc)(blasint *,    blasint *, xdouble *, xdouble *, blasint *,
		    xdouble *, blasint *, xdouble *, blasint *);

void BLASFUNC(sgemv)(char *, blasint *, blasint *, float  *, float  *, blasint *,
		    float  *, blasint *, float  *, float  *, blasint *);
void BLASFUNC(dgemv)(char *, blasint *, blasint *, double *, double *, blasint *,
		    double *, blasint *, double *, double *, blasint *);
void BLASFUNC(qgemv)(char *, blasint *, blasint *, xdouble *, xdouble *, blasint *,
		    xdouble *, blasint *, xdouble *, xdouble *, blasint *);
void BLASFUNC(cgemv)(char *, blasint *, blasint *, float  *, float  *, blasint *,
		    float  *, blasint *, float  *, float  *, blasint *);
void BLASFUNC(zgemv)(char *, blasint *, blasint *, double *, double *, blasint *,
		    double *, blasint *, double *, double *, blasint *);
void BLASFUNC(xgemv)(char *, blasint *, blasint *, xdouble *, xdouble *, blasint *,
		    xdouble *, blasint *, xdouble *, xdouble *, blasint *);

void BLASFUNC(strsv) (char *, char *, char *, blasint *, float  *, blasint *,
		     float  *, blasint *);
void BLASFUNC(dtrsv) (char *, char *, char *, blasint *, double *, blasint *,
		     double *, blasint *);
void BLASFUNC(qtrsv) (char *, char *, char *, blasint *, xdouble *, blasint *,
		     xdouble *, blasint *);
void BLASFUNC(ctrsv) (char *, char *, char *, blasint *, float  *, blasint *,
		     float  *, blasint *);
void BLASFUNC(ztrsv) (char *, char *, char *, blasint *, double *, blasint *,
		     double *, blasint *);
void BLASFUNC(xtrsv) (char *, char *, char *, blasint *, xdouble *, blasint *,
		     xdouble *, blasint *);

void BLASFUNC(strmv) (char *, char *, char *, blasint *, float  *, blasint *,
		     float  *, blasint *);
void BLASFUNC(dtrmv) (char *, char *, char *, blasint *, double *, blasint *,
		     double *, blasint *);
void BLASFUNC(qtrmv) (char *, char *, char *, blasint *, xdouble *, blasint *,
		     xdouble *, blasint *);
void BLASFUNC(ctrmv) (char *, char *, char *, blasint *, float  *, blasint *,
		     float  *, blasint *);
void BLASFUNC(ztrmv) (char *, char *, char *, blasint *, double *, blasint *,
		     double *, blasint *);
void BLASFUNC(xtrmv) (char *, char *, char *, blasint *, xdouble *, blasint *,
		     xdouble *, blasint *);

void BLASFUNC(stpsv) (char *, char *, char *, blasint *, float  *, float  *, blasint *);
void BLASFUNC(dtpsv) (char *, char *, char *, blasint *, double *, double *, blasint *);
void BLASFUNC(qtpsv) (char *, char *, char *, blasint *, xdouble *, xdouble *, blasint *);
void BLASFUNC(ctpsv) (char *, char *, char *, blasint *, float  *, float  *, blasint *);
void BLASFUNC(ztpsv) (char *, char *, char *, blasint *, double *, double *, blasint *);
void BLASFUNC(xtpsv) (char *, char *, char *, blasint *, xdouble *, xdouble *, blasint *);

void BLASFUNC(stpmv) (char *, char *, char *, blasint *, float  *, float  *, blasint *);
void BLASFUNC(dtpmv) (char *, char *, char *, blasint *, double *, double *, blasint *);
void BLASFUNC(qtpmv) (char *, char *, char *, blasint *, xdouble *, xdouble *, blasint *);
void BLASFUNC(ctpmv) (char *, char *, char *, blasint *, float  *, float  *, blasint *);
void BLASFUNC(ztpmv) (char *, char *, char *, blasint *, double *, double *, blasint *);
void BLASFUNC(xtpmv) (char *, char *, char *, blasint *, xdouble *, xdouble *, blasint *);

void BLASFUNC(stbmv) (char *, char *, char *, blasint *, blasint *, float  *, blasint *, float  *, blasint *);
void BLASFUNC(dtbmv) (char *, char *, char *, blasint *, blasint *, double *, blasint *, double *, blasint *);
void BLASFUNC(qtbmv) (char *, char *, char *, blasint *, blasint *, xdouble *, blasint *, xdouble *, blasint *);
void BLASFUNC(ctbmv) (char *, char *, char *, blasint *, blasint *, float  *, blasint *, float  *, blasint *);
void BLASFUNC(ztbmv) (char *, char *, char *, blasint *, blasint *, double *, blasint *, double *, blasint *);
void BLASFUNC(xtbmv) (char *, char *, char *, blasint *, blasint *, xdouble *, blasint *, xdouble *, blasint *);

void BLASFUNC(stbsv) (char *, char *, char *, blasint *, blasint *, float  *, blasint *, float  *, blasint *);
void BLASFUNC(dtbsv) (char *, char *, char *, blasint *, blasint *, double *, blasint *, double *, blasint *);
void BLASFUNC(qtbsv) (char *, char *, char *, blasint *, blasint *, xdouble *, blasint *, xdouble *, blasint *);
void BLASFUNC(ctbsv) (char *, char *, char *, blasint *, blasint *, float  *, blasint *, float  *, blasint *);
void BLASFUNC(ztbsv) (char *, char *, char *, blasint *, blasint *, double *, blasint *, double *, blasint *);
void BLASFUNC(xtbsv) (char *, char *, char *, blasint *, blasint *, xdouble *, blasint *, xdouble *, blasint *);

void BLASFUNC(ssymv) (char *, blasint *, float  *, float *, blasint *,
		     float  *, blasint *, float *, float *, blasint *);
void BLASFUNC(dsymv) (char *, blasint *, double  *, double *, blasint *,
		     double  *, blasint *, double *, double *, blasint *);
void BLASFUNC(qsymv) (char *, blasint *, xdouble  *, xdouble *, blasint *,
		     xdouble  *, blasint *, xdouble *, xdouble *, blasint *);
void BLASFUNC(csymv) (char *, blasint *, float  *, float *, blasint *,
		     float  *, blasint *, float *, float *, blasint *);
void BLASFUNC(zsymv) (char *, blasint *, double  *, double *, blasint *,
		     double  *, blasint *, double *, double *, blasint *);
void BLASFUNC(xsymv) (char *, blasint *, xdouble  *, xdouble *, blasint *,
		     xdouble  *, blasint *, xdouble *, xdouble *, blasint *);

void BLASFUNC(sspmv) (char *, blasint *, float  *, float *,
		     float  *, blasint *, float *, float *, blasint *);
void BLASFUNC(dspmv) (char *, blasint *, double  *, double *,
		     double  *, blasint *, double *, double *, blasint *);
void BLASFUNC(qspmv) (char *, blasint *, xdouble  *, xdouble *,
		     xdouble  *, blasint *, xdouble *, xdouble *, blasint *);
void BLASFUNC(cspmv) (char *, blasint *, float  *, float *,
		     float  *, blasint *, float *, float *, blasint *);
void BLASFUNC(zspmv) (char *, blasint *, double  *, double *,
		     double  *, blasint *, double *, double *, blasint *);
void BLASFUNC(xspmv) (char *, blasint *, xdouble  *, xdouble *,
		     xdouble  *, blasint *, xdouble *, xdouble *, blasint *);

void BLASFUNC(ssyr) (char *, blasint *, float   *, float  *, blasint *,
		    float  *, blasint *);
void BLASFUNC(dsyr) (char *, blasint *, double  *, double *, blasint *,
		    double *, blasint *);
void BLASFUNC(qsyr) (char *, blasint *, xdouble  *, xdouble *, blasint *,
		    xdouble *, blasint *);
void BLASFUNC(csyr) (char *, blasint *, float   *, float  *, blasint *,
		    float  *, blasint *);
void BLASFUNC(zsyr) (char *, blasint *, double  *, double *, blasint *,
		    double *, blasint *);
void BLASFUNC(xsyr) (char *, blasint *, xdouble  *, xdouble *, blasint *,
		    xdouble *, blasint *);

void BLASFUNC(ssyr2) (char *, blasint *, float   *,
		     float  *, blasint *, float  *, blasint *, float  *, blasint *);
void BLASFUNC(dsyr2) (char *, blasint *, double  *,
		     double *, blasint *, double *, blasint *, double *, blasint *);
void BLASFUNC(qsyr2) (char *, blasint *, xdouble  *,
		     xdouble *, blasint *, xdouble *, blasint *, xdouble *, blasint *);
void BLASFUNC(csyr2) (char *, blasint *, float   *,
		     float  *, blasint *, float  *, blasint *, float  *, blasint *);
void BLASFUNC(zsyr2) (char *, blasint *, double  *,
		     double *, blasint *, double *, blasint *, double *, blasint *);
void BLASFUNC(xsyr2) (char *, blasint *, xdouble  *,
		     xdouble *, blasint *, xdouble *, blasint *, xdouble *, blasint *);

void BLASFUNC(sspr) (char *, blasint *, float   *, float  *, blasint *,
		    float  *);
void BLASFUNC(dspr) (char *, blasint *, double  *, double *, blasint *,
		    double *);
void BLASFUNC(qspr) (char *, blasint *, xdouble  *, xdouble *, blasint *,
		    xdouble *);
void BLASFUNC(cspr) (char *, blasint *, float   *, float  *, blasint *,
		    float  *);
void BLASFUNC(zspr) (char *, blasint *, double  *, double *, blasint *,
		    double *);
void BLASFUNC(xspr) (char *, blasint *, xdouble  *, xdouble *, blasint *,
		    xdouble *);

void BLASFUNC(sspr2) (char *, blasint *, float   *,
		     float  *, blasint *, float  *, blasint *, float  *);
void BLASFUNC(dspr2) (char *, blasint *, double  *,
		     double *, blasint *, double *, blasint *, double *);
void BLASFUNC(qspr2) (char *, blasint *, xdouble  *,
		     xdouble *, blasint *, xdouble *, blasint *, xdouble *);
void BLASFUNC(cspr2) (char *, blasint *, float   *,
		     float  *, blasint *, float  *, blasint *, float  *);
void BLASFUNC(zspr2) (char *, blasint *, double  *,
		     double *, blasint *, double *, blasint *, double *);
void BLASFUNC(xspr2) (char *, blasint *, xdouble  *,
		     xdouble *, blasint *, xdouble *, blasint *, xdouble *);

void BLASFUNC(cher) (char *, blasint *, float   *, float  *, blasint *,
		    float  *, blasint *);
void BLASFUNC(zher) (char *, blasint *, double  *, double *, blasint *,
		    double *, blasint *);
void BLASFUNC(xher) (char *, blasint *, xdouble  *, xdouble *, blasint *,
		    xdouble *, blasint *);

void BLASFUNC(chpr) (char *, blasint *, float   *, float  *, blasint *, float  *);
void BLASFUNC(zhpr) (char *, blasint *, double  *, double *, blasint *, double *);
void BLASFUNC(xhpr) (char *, blasint *, xdouble  *, xdouble *, blasint *, xdouble *);

void BLASFUNC(cher2) (char *, blasint *, float   *,
		     float  *, blasint *, float  *, blasint *, float  *, blasint *);
void BLASFUNC(zher2) (char *, blasint *, double  *,
		     double *, blasint *, double *, blasint *, double *, blasint *);
void BLASFUNC(xher2) (char *, blasint *, xdouble  *,
		     xdouble *, blasint *, xdouble *, blasint *, xdouble *, blasint *);

void BLASFUNC(chpr2) (char *, blasint *, float   *,
		     float  *, blasint *, float  *, blasint *, float  *);
void BLASFUNC(zhpr2) (char *, blasint *, double  *,
		     double *, blasint *, double *, blasint *, double *);
void BLASFUNC(xhpr2) (char *, blasint *, xdouble  *,
		     xdouble *, blasint *, xdouble *, blasint *, xdouble *);

void BLASFUNC(chemv) (char *, blasint *, float  *, float *, blasint *,
		     float  *, blasint *, float *, float *, blasint *);
void BLASFUNC(zhemv) (char *, blasint *, double  *, double *, blasint *,
		     double  *, blasint *, double *, double *, blasint *);
void BLASFUNC(xhemv) (char *, blasint *, xdouble  *, xdouble *, blasint *,
		     xdouble  *, blasint *, xdouble *, xdouble *, blasint *);

void BLASFUNC(chpmv) (char *, blasint *, float  *, float *,
		     float  *, blasint *, float *, float *, blasint *);
void BLASFUNC(zhpmv) (char *, blasint *, double  *, double *,
		     double  *, blasint *, double *, double *, blasint *);
void BLASFUNC(xhpmv) (char *, blasint *, xdouble  *, xdouble *,
		     xdouble  *, blasint *, xdouble *, xdouble *, blasint *);

int BLASFUNC(snorm)(char *, blasint *, blasint *, float  *, blasint *);
int BLASFUNC(dnorm)(char *, blasint *, blasint *, double *, blasint *);
int BLASFUNC(cnorm)(char *, blasint *, blasint *, float  *, blasint *);
int BLASFUNC(znorm)(char *, blasint *, blasint *, double *, blasint *);

void BLASFUNC(sgbmv)(char *, blasint *, blasint *, blasint *, blasint *, float  *, float  *, blasint *,
		    float  *, blasint *, float  *, float  *, blasint *);
void BLASFUNC(dgbmv)(char *, blasint *, blasint *, blasint *, blasint *, double *, double *, blasint *,
		    double *, blasint *, double *, double *, blasint *);
void BLASFUNC(qgbmv)(char *, blasint *, blasint *, blasint *, blasint *, xdouble *, xdouble *, blasint *,
		    xdouble *, blasint *, xdouble *, xdouble *, blasint *);
void BLASFUNC(cgbmv)(char *, blasint *, blasint *, blasint *, blasint *, float  *, float  *, blasint *,
		    float  *, blasint *, float  *, float  *, blasint *);
void BLASFUNC(zgbmv)(char *, blasint *, blasint *, blasint *, blasint *, double *, double *, blasint *,
		    double *, blasint *, double *, double *, blasint *);
void BLASFUNC(xgbmv)(char *, blasint *, blasint *, blasint *, blasint *, xdouble *, xdouble *, blasint *,
		    xdouble *, blasint *, xdouble *, xdouble *, blasint *);

void BLASFUNC(ssbmv)(char *, blasint *, blasint *, float  *, float  *, blasint *,
		    float  *, blasint *, float  *, float  *, blasint *);
void BLASFUNC(dsbmv)(char *, blasint *, blasint *, double *, double *, blasint *,
		    double *, blasint *, double *, double *, blasint *);
void BLASFUNC(qsbmv)(char *, blasint *, blasint *, xdouble *, xdouble *, blasint *,
		    xdouble *, blasint *, xdouble *, xdouble *, blasint *);
void BLASFUNC(csbmv)(char *, blasint *, blasint *, float  *, float  *, blasint *,
		    float  *, blasint *, float  *, float  *, blasint *);
void BLASFUNC(zsbmv)(char *, blasint *, blasint *, double *, double *, blasint *,
		    double *, blasint *, double *, double *, blasint *);
void BLASFUNC(xsbmv)(char *, blasint *, blasint *, xdouble *, xdouble *, blasint *,
		    xdouble *, blasint *, xdouble *, xdouble *, blasint *);

void BLASFUNC(chbmv)(char *, blasint *, blasint *, float  *, float  *, blasint *,
		    float  *, blasint *, float  *, float  *, blasint *);
void BLASFUNC(zhbmv)(char *, blasint *, blasint *, double *, double *, blasint *,
		    double *, blasint *, double *, double *, blasint *);
void BLASFUNC(xhbmv)(char *, blasint *, blasint *, xdouble *, xdouble *, blasint *,
		    xdouble *, blasint *, xdouble *, xdouble *, blasint *);

/* Level 3 routines */

void BLASFUNC(sgemm)(char *, char *, blasint *, blasint *, blasint *, float *,
	   float  *, blasint *, float  *, blasint *, float  *, float  *, blasint *);
void BLASFUNC(dgemm)(char *, char *, blasint *, blasint *, blasint *, double *,
	   double *, blasint *, double *, blasint *, double *, double *, blasint *);
void BLASFUNC(qgemm)(char *, char *, blasint *, blasint *, blasint *, xdouble *,
	   xdouble *, blasint *, xdouble *, blasint *, xdouble *, xdouble *, blasint *);
void BLASFUNC(cgemm)(char *, char *, blasint *, blasint *, blasint *, float *,
	   float  *, blasint *, float  *, blasint *, float  *, float  *, blasint *);
void BLASFUNC(zgemm)(char *, char *, blasint *, blasint *, blasint *, double *,
	   double *, blasint *, double *, blasint *, double *, double *, blasint *);
void BLASFUNC(xgemm)(char *, char *, blasint *, blasint *, blasint *, xdouble *,
	   xdouble *, blasint *, xdouble *, blasint *, xdouble *, xdouble *, blasint *);

void BLASFUNC(cgemm3m)(char *, char *, blasint *, blasint *, blasint *, float *,
	   float  *, blasint *, float  *, blasint *, float  *, float  *, blasint *);
void BLASFUNC(zgemm3m)(char *, char *, blasint *, blasint *, blasint *, double *,
	   double *, blasint *, double *, blasint *, double *, double *, blasint *);
void BLASFUNC(xgemm3m)(char *, char *, blasint *, blasint *, blasint *, xdouble *,
	   xdouble *, blasint *, xdouble *, blasint *, xdouble *, xdouble *, blasint *);

int BLASFUNC(sge2mm)(char *, char *, char *, blasint *, blasint *,
		     float *, float  *, blasint *, float  *, blasint *,
		     float *, float  *, blasint *);
int BLASFUNC(dge2mm)(char *, char *, char *, blasint *, blasint *,
		     double *, double  *, blasint *, double  *, blasint *,
		     double *, double  *, blasint *);
int BLASFUNC(cge2mm)(char *, char *, char *, blasint *, blasint *,
		     float *, float  *, blasint *, float  *, blasint *,
		     float *, float  *, blasint *);
int BLASFUNC(zge2mm)(char *, char *, char *, blasint *, blasint *,
		     double *, double  *, blasint *, double  *, blasint *,
		     double *, double  *, blasint *);

void BLASFUNC(strsm)(char *, char *, char *, char *, blasint *, blasint *,
	   float *,  float *, blasint *, float *, blasint *);
void BLASFUNC(dtrsm)(char *, char *, char *, char *, blasint *, blasint *,
	   double *,  double *, blasint *, double *, blasint *);
void BLASFUNC(qtrsm)(char *, char *, char *, char *, blasint *, blasint *,
	   xdouble *,  xdouble *, blasint *, xdouble *, blasint *);
void BLASFUNC(ctrsm)(char *, char *, char *, char *, blasint *, blasint *,
	   float *,  float *, blasint *, float *, blasint *);
void BLASFUNC(ztrsm)(char *, char *, char *, char *, blasint *, blasint *,
	   double *,  double *, blasint *, double *, blasint *);
void BLASFUNC(xtrsm)(char *, char *, char *, char *, blasint *, blasint *,
	   xdouble *,  xdouble *, blasint *, xdouble *, blasint *);

void BLASFUNC(strmm)(char *, char *, char *, char *, blasint *, blasint *,
	   float *,  float *, blasint *, float *, blasint *);
void BLASFUNC(dtrmm)(char *, char *, char *, char *, blasint *, blasint *,
	   double *,  double *, blasint *, double *, blasint *);
void BLASFUNC(qtrmm)(char *, char *, char *, char *, blasint *, blasint *,
	   xdouble *,  xdouble *, blasint *, xdouble *, blasint *);
void BLASFUNC(ctrmm)(char *, char *, char *, char *, blasint *, blasint *,
	   float *,  float *, blasint *, float *, blasint *);
void BLASFUNC(ztrmm)(char *, char *, char *, char *, blasint *, blasint *,
	   double *,  double *, blasint *, double *, blasint *);
void BLASFUNC(xtrmm)(char *, char *, char *, char *, blasint *, blasint *,
	   xdouble *,  xdouble *, blasint *, xdouble *, blasint *);

void BLASFUNC(ssymm)(char *, char *, blasint *, blasint *, float  *, float  *, blasint *,
	   float  *, blasint *, float  *, float  *, blasint *);
void BLASFUNC(dsymm)(char *, char *, blasint *, blasint *, double *, double *, blasint *,
	   double *, blasint *, double *, double *, blasint *);
void BLASFUNC(qsymm)(char *, char *, blasint *, blasint *, xdouble *, xdouble *, blasint *,
	   xdouble *, blasint *, xdouble *, xdouble *, blasint *);
void BLASFUNC(csymm)(char *, char *, blasint *, blasint *, float  *, float  *, blasint *,
	   float  *, blasint *, float  *, float  *, blasint *);
void BLASFUNC(zsymm)(char *, char *, blasint *, blasint *, double *, double *, blasint *,
	   double *, blasint *, double *, double *, blasint *);
void BLASFUNC(xsymm)(char *, char *, blasint *, blasint *, xdouble *, xdouble *, blasint *,
	   xdouble *, blasint *, xdouble *, xdouble *, blasint *);

void BLASFUNC(csymm3m)(char *, char *, blasint *, blasint *, float  *, float  *, blasint *,
	   float  *, blasint *, float  *, float  *, blasint *);
void BLASFUNC(zsymm3m)(char *, char *, blasint *, blasint *, double *, double *, blasint *,
	   double *, blasint *, double *, double *, blasint *);
void BLASFUNC(xsymm3m)(char *, char *, blasint *, blasint *, xdouble *, xdouble *, blasint *,
	   xdouble *, blasint *, xdouble *, xdouble *, blasint *);

void BLASFUNC(ssyrk)(char *, char *, blasint *, blasint *, float  *, float  *, blasint *,
	   float  *, float  *, blasint *);
void BLASFUNC(dsyrk)(char *, char *, blasint *, blasint *, double *, double *, blasint *,
	   double *, double *, blasint *);
void BLASFUNC(qsyrk)(char *, char *, blasint *, blasint *, xdouble *, xdouble *, blasint *,
	   xdouble *, xdouble *, blasint *);
void BLASFUNC(csyrk)(char *, char *, blasint *, blasint *, float  *, float  *, blasint *,
	   float  *, float  *, blasint *);
void BLASFUNC(zsyrk)(char *, char *, blasint *, blasint *, double *, double *, blasint *,
	   double *, double *, blasint *);
void BLASFUNC(xsyrk)(char *, char *, blasint *, blasint *, xdouble *, xdouble *, blasint *,
	   xdouble *, xdouble *, blasint *);

void BLASFUNC(ssyr2k)(char *, char *, blasint *, blasint *, float  *, float  *, blasint *,
	   float *, blasint *, float  *, float  *, blasint *);
void BLASFUNC(dsyr2k)(char *, char *, blasint *, blasint *, double *, double *, blasint *,
	   double*, blasint *, double *, double *, blasint *);
void BLASFUNC(qsyr2k)(char *, char *, blasint *, blasint *, xdouble *, xdouble *, blasint *,
	   xdouble*, blasint *, xdouble *, xdouble *, blasint *);
void BLASFUNC(csyr2k)(char *, char *, blasint *, blasint *, float  *, float  *, blasint *,
	   float *, blasint *, float  *, float  *, blasint *);
void BLASFUNC(zsyr2k)(char *, char *, blasint *, blasint *, double *, double *, blasint *,
	   double*, blasint *, double *, double *, blasint *);
void BLASFUNC(xsyr2k)(char *, char *, blasint *, blasint *, xdouble *, xdouble *, blasint *,
	   xdouble*, blasint *, xdouble *, xdouble *, blasint *);

void BLASFUNC(chemm)(char *, char *, blasint *, blasint *, float  *, float  *, blasint *,
	   float  *, blasint *, float  *, float  *, blasint *);
void BLASFUNC(zhemm)(char *, char *, blasint *, blasint *, double *, double *, blasint *,
	   double *, blasint *, double *, double *, blasint *);
void BLASFUNC(xhemm)(char *, char *, blasint *, blasint *, xdouble *, xdouble *, blasint *,
	   xdouble *, blasint *, xdouble *, xdouble *, blasint *);

void BLASFUNC(chemm3m)(char *, char *, blasint *, blasint *, float  *, float  *, blasint *,
	   float  *, blasint *, float  *, float  *, blasint *);
void BLASFUNC(zhemm3m)(char *, char *, blasint *, blasint *, double *, double *, blasint *,
	   double *, blasint *, double *, double *, blasint *);
void BLASFUNC(xhemm3m)(char *, char *, blasint *, blasint *, xdouble *, xdouble *, blasint *,
	   xdouble *, blasint *, xdouble *, xdouble *, blasint *);

void BLASFUNC(cherk)(char *, char *, blasint *, blasint *, float  *, float  *, blasint *,
	   float  *, float  *, blasint *);
void BLASFUNC(zherk)(char *, char *, blasint *, blasint *, double *, double *, blasint *,
	   double *, double *, blasint *);
void BLASFUNC(xherk)(char *, char *, blasint *, blasint *, xdouble *, xdouble *, blasint *,
	   xdouble *, xdouble *, blasint *);

void BLASFUNC(cher2k)(char *, char *, blasint *, blasint *, float  *, float  *, blasint *,
	   float *, blasint *, float  *, float  *, blasint *);
void BLASFUNC(zher2k)(char *, char *, blasint *, blasint *, double *, double *, blasint *,
	   double*, blasint *, double *, double *, blasint *);
void BLASFUNC(xher2k)(char *, char *, blasint *, blasint *, xdouble *, xdouble *, blasint *,
	   xdouble*, blasint *, xdouble *, xdouble *, blasint *);

int BLASFUNC(cher2m)(char *, char *, char *, blasint *, blasint *, float  *, float  *, blasint *,
	   float *, blasint *, float  *, float  *, blasint *);
int BLASFUNC(zher2m)(char *, char *, char *, blasint *, blasint *, double *, double *, blasint *,
	   double*, blasint *, double *, double *, blasint *);
int BLASFUNC(xher2m)(char *, char *, char *, blasint *, blasint *, xdouble *, xdouble *, blasint *,
	   xdouble*, blasint *, xdouble *, xdouble *, blasint *);

int BLASFUNC(sgemt)(char *, blasint *, blasint *, float  *, float  *, blasint *,
		    float  *, blasint *);
int BLASFUNC(dgemt)(char *, blasint *, blasint *, double *, double *, blasint *,
		    double *, blasint *);
int BLASFUNC(cgemt)(char *, blasint *, blasint *, float  *, float  *, blasint *,
		    float  *, blasint *);
int BLASFUNC(zgemt)(char *, blasint *, blasint *, double *, double *, blasint *,
		    double *, blasint *);

int BLASFUNC(sgema)(char *, char *, blasint *, blasint *, float  *,
		    float  *, blasint *, float *, float  *, blasint *, float *, blasint *);
int BLASFUNC(dgema)(char *, char *, blasint *, blasint *, double *,
		    double *, blasint *, double*, double *, blasint *, double*, blasint *);
int BLASFUNC(cgema)(char *, char *, blasint *, blasint *, float  *,
		    float  *, blasint *, float *, float  *, blasint *, float *, blasint *);
int BLASFUNC(zgema)(char *, char *, blasint *, blasint *, double *,
		    double *, blasint *, double*, double *, blasint *, double*, blasint *);

int BLASFUNC(sgems)(char *, char *, blasint *, blasint *, float  *,
		    float  *, blasint *, float *, float  *, blasint *, float *, blasint *);
int BLASFUNC(dgems)(char *, char *, blasint *, blasint *, double *,
		    double *, blasint *, double*, double *, blasint *, double*, blasint *);
int BLASFUNC(cgems)(char *, char *, blasint *, blasint *, float  *,
		    float  *, blasint *, float *, float  *, blasint *, float *, blasint *);
int BLASFUNC(zgems)(char *, char *, blasint *, blasint *, double *,
		    double *, blasint *, double*, double *, blasint *, double*, blasint *);

int BLASFUNC(sgemc)(char *, char *, blasint *, blasint *, blasint *, float *,
	   float  *, blasint *, float  *, blasint *, float  *, blasint *, float  *, float  *, blasint *);
int BLASFUNC(dgemc)(char *, char *, blasint *, blasint *, blasint *, double *,
	   double *, blasint *, double *, blasint *, double *, blasint *, double *, double *, blasint *);
int BLASFUNC(qgemc)(char *, char *, blasint *, blasint *, blasint *, xdouble *,
	   xdouble *, blasint *, xdouble *, blasint *, xdouble *, blasint *,  xdouble *, xdouble *, blasint *);
int BLASFUNC(cgemc)(char *, char *, blasint *, blasint *, blasint *, float *,
	   float  *, blasint *, float  *, blasint *, float  *, blasint *, float  *, float  *, blasint *);
int BLASFUNC(zgemc)(char *, char *, blasint *, blasint *, blasint *, double *,
	   double *, blasint *, double *, blasint *, double *, blasint *, double *, double *, blasint *);
int BLASFUNC(xgemc)(char *, char *, blasint *, blasint *, blasint *, xdouble *,
	   xdouble *, blasint *, xdouble *, blasint *, xdouble *, blasint *, xdouble *, xdouble *, blasint *);

/* Lapack routines */

int BLASFUNC(sgetf2)(blasint *, blasint *, float  *, blasint *, blasint *, blasint *);
int BLASFUNC(dgetf2)(blasint *, blasint *, double *, blasint *, blasint *, blasint *);
int BLASFUNC(qgetf2)(blasint *, blasint *, xdouble *, blasint *, blasint *, blasint *);
int BLASFUNC(cgetf2)(blasint *, blasint *, float  *, blasint *, blasint *, blasint *);
int BLASFUNC(zgetf2)(blasint *, blasint *, double *, blasint *, blasint *, blasint *);
int BLASFUNC(xgetf2)(blasint *, blasint *, xdouble *, blasint *, blasint *, blasint *);

int BLASFUNC(sgetrf)(blasint *, blasint *, float  *, blasint *, blasint *, blasint *);
int BLASFUNC(dgetrf)(blasint *, blasint *, double *, blasint *, blasint *, blasint *);
int BLASFUNC(qgetrf)(blasint *, blasint *, xdouble *, blasint *, blasint *, blasint *);
int BLASFUNC(cgetrf)(blasint *, blasint *, float  *, blasint *, blasint *, blasint *);
int BLASFUNC(zgetrf)(blasint *, blasint *, double *, blasint *, blasint *, blasint *);
int BLASFUNC(xgetrf)(blasint *, blasint *, xdouble *, blasint *, blasint *, blasint *);

int BLASFUNC(slaswp)(blasint *, float  *, blasint *, blasint *, blasint *, blasint *, blasint *);
int BLASFUNC(dlaswp)(blasint *, double *, blasint *, blasint *, blasint *, blasint *, blasint *);
int BLASFUNC(qlaswp)(blasint *, xdouble *, blasint *, blasint *, blasint *, blasint *, blasint *);
int BLASFUNC(claswp)(blasint *, float  *, blasint *, blasint *, blasint *, blasint *, blasint *);
int BLASFUNC(zlaswp)(blasint *, double *, blasint *, blasint *, blasint *, blasint *, blasint *);
int BLASFUNC(xlaswp)(blasint *, xdouble *, blasint *, blasint *, blasint *, blasint *, blasint *);

int BLASFUNC(sgetrs)(char *, blasint *, blasint *, float  *, blasint *, blasint *, float  *, blasint *, blasint *);
int BLASFUNC(dgetrs)(char *, blasint *, blasint *, double *, blasint *, blasint *, double *, blasint *, blasint *);
int BLASFUNC(qgetrs)(char *, blasint *, blasint *, xdouble *, blasint *, blasint *, xdouble *, blasint *, blasint *);
int BLASFUNC(cgetrs)(char *, blasint *, blasint *, float  *, blasint *, blasint *, float  *, blasint *, blasint *);
int BLASFUNC(zgetrs)(char *, blasint *, blasint *, double *, blasint *, blasint *, double *, blasint *, blasint *);
int BLASFUNC(xgetrs)(char *, blasint *, blasint *, xdouble *, blasint *, blasint *, xdouble *, blasint *, blasint *);

int BLASFUNC(sgesv)(blasint *, blasint *, float  *, blasint *, blasint *, float *, blasint *, blasint *);
int BLASFUNC(dgesv)(blasint *, blasint *, double *, blasint *, blasint *, double*, blasint *, blasint *);
int BLASFUNC(qgesv)(blasint *, blasint *, xdouble *, blasint *, blasint *, xdouble*, blasint *, blasint *);
int BLASFUNC(cgesv)(blasint *, blasint *, float  *, blasint *, blasint *, float *, blasint *, blasint *);
int BLASFUNC(zgesv)(blasint *, blasint *, double *, blasint *, blasint *, double*, blasint *, blasint *);
int BLASFUNC(xgesv)(blasint *, blasint *, xdouble *, blasint *, blasint *, xdouble*, blasint *, blasint *);

int BLASFUNC(spotf2)(char *, blasint *, float  *, blasint *, blasint *);
int BLASFUNC(dpotf2)(char *, blasint *, double *, blasint *, blasint *);
int BLASFUNC(qpotf2)(char *, blasint *, xdouble *, blasint *, blasint *);
int BLASFUNC(cpotf2)(char *, blasint *, float  *, blasint *, blasint *);
int BLASFUNC(zpotf2)(char *, blasint *, double *, blasint *, blasint *);
int BLASFUNC(xpotf2)(char *, blasint *, xdouble *, blasint *, blasint *);

int BLASFUNC(spotrf)(char *, blasint *, float  *, blasint *, blasint *);
int BLASFUNC(dpotrf)(char *, blasint *, double *, blasint *, blasint *);
int BLASFUNC(qpotrf)(char *, blasint *, xdouble *, blasint *, blasint *);
int BLASFUNC(cpotrf)(char *, blasint *, float  *, blasint *, blasint *);
int BLASFUNC(zpotrf)(char *, blasint *, double *, blasint *, blasint *);
int BLASFUNC(xpotrf)(char *, blasint *, xdouble *, blasint *, blasint *);

int BLASFUNC(spotrs)(char *, blasint *, blasint *, float   *, blasint *, float   *, blasint *, blasint *);
int BLASFUNC(dpotrs)(char *, blasint *, blasint *, double  *, blasint *, double  *, blasint *, blasint *);
int BLASFUNC(qpotrs)(char *, blasint *, blasint *, xdouble *, blasint *, xdouble *, blasint *, blasint *);
int BLASFUNC(cpotrs)(char *, blasint *, blasint *, float   *, blasint *, float   *, blasint *, blasint *);
int BLASFUNC(zpotrs)(char *, blasint *, blasint *, double  *, blasint *, double  *, blasint *, blasint *);
int BLASFUNC(xpotrs)(char *, blasint *, blasint *, xdouble *, blasint *, xdouble *, blasint *, blasint *);

int BLASFUNC(slauu2)(char *, blasint *, float  *, blasint *, blasint *);
int BLASFUNC(dlauu2)(char *, blasint *, double *, blasint *, blasint *);
int BLASFUNC(qlauu2)(char *, blasint *, xdouble *, blasint *, blasint *);
int BLASFUNC(clauu2)(char *, blasint *, float  *, blasint *, blasint *);
int BLASFUNC(zlauu2)(char *, blasint *, double *, blasint *, blasint *);
int BLASFUNC(xlauu2)(char *, blasint *, xdouble *, blasint *, blasint *);

int BLASFUNC(slauum)(char *, blasint *, float  *, blasint *, blasint *);
int BLASFUNC(dlauum)(char *, blasint *, double *, blasint *, blasint *);
int BLASFUNC(qlauum)(char *, blasint *, xdouble *, blasint *, blasint *);
int BLASFUNC(clauum)(char *, blasint *, float  *, blasint *, blasint *);
int BLASFUNC(zlauum)(char *, blasint *, double *, blasint *, blasint *);
int BLASFUNC(xlauum)(char *, blasint *, xdouble *, blasint *, blasint *);

int BLASFUNC(strti2)(char *, char *, blasint *, float  *, blasint *, blasint *);
int BLASFUNC(dtrti2)(char *, char *, blasint *, double *, blasint *, blasint *);
int BLASFUNC(qtrti2)(char *, char *, blasint *, xdouble *, blasint *, blasint *);
int BLASFUNC(ctrti2)(char *, char *, blasint *, float  *, blasint *, blasint *);
int BLASFUNC(ztrti2)(char *, char *, blasint *, double *, blasint *, blasint *);
int BLASFUNC(xtrti2)(char *, char *, blasint *, xdouble *, blasint *, blasint *);

int BLASFUNC(strtri)(char *, char *, blasint *, float  *, blasint *, blasint *);
int BLASFUNC(dtrtri)(char *, char *, blasint *, double *, blasint *, blasint *);
int BLASFUNC(qtrtri)(char *, char *, blasint *, xdouble *, blasint *, blasint *);
int BLASFUNC(ctrtri)(char *, char *, blasint *, float  *, blasint *, blasint *);
int BLASFUNC(ztrtri)(char *, char *, blasint *, double *, blasint *, blasint *);
int BLASFUNC(xtrtri)(char *, char *, blasint *, xdouble *, blasint *, blasint *);


FLOATRET  BLASFUNC(slamch)(char *);
double    BLASFUNC(dlamch)(char *);
xdouble   BLASFUNC(qlamch)(char *);

FLOATRET  BLASFUNC(slamc3)(float *, float *);
double    BLASFUNC(dlamc3)(double *, double *);
xdouble   BLASFUNC(qlamc3)(xdouble *, xdouble *);

/* BLAS extensions */

void    BLASFUNC(saxpby) (blasint *, float  *, float  *, blasint *, float *, float  *, blasint *);
void    BLASFUNC(daxpby) (blasint *, double  *, double  *, blasint *, double *, double  *, blasint *);
void    BLASFUNC(caxpby) (blasint *, float  *, float  *, blasint *, float *, float  *, blasint *);
void    BLASFUNC(zaxpby) (blasint *, double  *, double  *, blasint *, double *, double  *, blasint *);

void    BLASFUNC(somatcopy) (char *, char *, blasint *, blasint *, float  *, float  *, blasint *, float  *, blasint *);
void    BLASFUNC(domatcopy) (char *, char *, blasint *, blasint *, double  *, double  *, blasint *, double  *, blasint *);
void    BLASFUNC(comatcopy) (char *, char *, blasint *, blasint *, float  *, float  *, blasint *, float  *, blasint *);
void    BLASFUNC(zomatcopy) (char *, char *, blasint *, blasint *, double  *, double  *, blasint *, double  *, blasint *);

void    BLASFUNC(simatcopy) (char *, char *, blasint *, blasint *, float  *, float  *, blasint *, blasint *);
void    BLASFUNC(dimatcopy) (char *, char *, blasint *, blasint *, double  *, double  *, blasint *, blasint *);
void    BLASFUNC(cimatcopy) (char *, char *, blasint *, blasint *, float  *, float  *, blasint *, blasint *);
void    BLASFUNC(zimatcopy) (char *, char *, blasint *, blasint *, double  *, double  *, blasint *, blasint *);

void    BLASFUNC(sgeadd) (blasint *, blasint *, float *, float *, blasint *, float *, float *, blasint*); 
void    BLASFUNC(dgeadd) (blasint *, blasint *, double *, double *, blasint *, double *, double *, blasint*); 
void    BLASFUNC(cgeadd) (blasint *, blasint *, float *, float *, blasint *, float *, float *, blasint*); 
void    BLASFUNC(zgeadd) (blasint *, blasint *, double *, double *, blasint *, double *, double *, blasint*); 


#ifdef __cplusplus
}

#endif  /* __cplusplus */

#endif
