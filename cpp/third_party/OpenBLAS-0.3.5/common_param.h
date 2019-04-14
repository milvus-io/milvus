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

#ifndef COMMON_PARAM_H
#define COMMON_PARAM_H

#ifndef ASSEMBLER

#ifdef DYNAMIC_ARCH

typedef struct {
  int dtb_entries;
  int offsetA, offsetB, align;

  int sgemm_p, sgemm_q, sgemm_r;
  int sgemm_unroll_m, sgemm_unroll_n, sgemm_unroll_mn;

  int exclusive_cache;

  float  (*samax_k) (BLASLONG, float *, BLASLONG);
  float  (*samin_k) (BLASLONG, float *, BLASLONG);
  float  (*smax_k)  (BLASLONG, float *, BLASLONG);
  float  (*smin_k)  (BLASLONG, float *, BLASLONG);
BLASLONG (*isamax_k)(BLASLONG, float *, BLASLONG);
BLASLONG (*isamin_k)(BLASLONG, float *, BLASLONG);
BLASLONG (*ismax_k) (BLASLONG, float *, BLASLONG);
BLASLONG (*ismin_k) (BLASLONG, float *, BLASLONG);

  float  (*snrm2_k) (BLASLONG, float *, BLASLONG);
  float  (*sasum_k) (BLASLONG, float *, BLASLONG);
  int    (*scopy_k) (BLASLONG, float *, BLASLONG, float *, BLASLONG);
  float  (*sdot_k)  (BLASLONG, float *, BLASLONG, float *, BLASLONG);
  double (*dsdot_k) (BLASLONG, float *, BLASLONG, float *, BLASLONG);

  int    (*srot_k)  (BLASLONG, float *, BLASLONG, float *, BLASLONG, float, float);

  int    (*saxpy_k) (BLASLONG, BLASLONG, BLASLONG, float, float *, BLASLONG, float *, BLASLONG, float *, BLASLONG);
  int    (*sscal_k) (BLASLONG, BLASLONG, BLASLONG, float, float *, BLASLONG, float *, BLASLONG, float *, BLASLONG);
  int    (*sswap_k) (BLASLONG, BLASLONG, BLASLONG, float, float *, BLASLONG, float *, BLASLONG, float *, BLASLONG);

  int    (*sgemv_n) (BLASLONG, BLASLONG, BLASLONG, float, float *, BLASLONG, float *, BLASLONG, float *, BLASLONG, float *);
  int    (*sgemv_t) (BLASLONG, BLASLONG, BLASLONG, float, float *, BLASLONG, float *, BLASLONG, float *, BLASLONG, float *);
  int    (*sger_k)  (BLASLONG, BLASLONG, BLASLONG, float, float *, BLASLONG, float *, BLASLONG, float *, BLASLONG, float *);

  int    (*ssymv_L) (BLASLONG, BLASLONG, float,  float  *, BLASLONG, float  *, BLASLONG, float  *, BLASLONG, float *);
  int    (*ssymv_U) (BLASLONG, BLASLONG, float,  float  *, BLASLONG, float  *, BLASLONG, float  *, BLASLONG, float *);

  int    (*sgemm_kernel   )(BLASLONG, BLASLONG, BLASLONG, float, float *, float *, float *, BLASLONG);
  int    (*sgemm_beta     )(BLASLONG, BLASLONG, BLASLONG, float, float *, BLASLONG, float *, BLASLONG, float  *, BLASLONG);

  int    (*sgemm_incopy   )(BLASLONG, BLASLONG, float *, BLASLONG, float *);
  int    (*sgemm_itcopy   )(BLASLONG, BLASLONG, float *, BLASLONG, float *);
  int    (*sgemm_oncopy   )(BLASLONG, BLASLONG, float *, BLASLONG, float *);
  int    (*sgemm_otcopy   )(BLASLONG, BLASLONG, float *, BLASLONG, float *);

  int    (*strsm_kernel_LN)(BLASLONG, BLASLONG, BLASLONG, float, float *, float *, float *, BLASLONG, BLASLONG);
  int    (*strsm_kernel_LT)(BLASLONG, BLASLONG, BLASLONG, float, float *, float *, float *, BLASLONG, BLASLONG);
  int    (*strsm_kernel_RN)(BLASLONG, BLASLONG, BLASLONG, float, float *, float *, float *, BLASLONG, BLASLONG);
  int    (*strsm_kernel_RT)(BLASLONG, BLASLONG, BLASLONG, float, float *, float *, float *, BLASLONG, BLASLONG);

  int    (*strsm_iunucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*strsm_iunncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*strsm_iutucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*strsm_iutncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*strsm_ilnucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*strsm_ilnncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*strsm_iltucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*strsm_iltncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*strsm_ounucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*strsm_ounncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*strsm_outucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*strsm_outncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*strsm_olnucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*strsm_olnncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*strsm_oltucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*strsm_oltncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);

  int    (*strmm_kernel_RN)(BLASLONG, BLASLONG, BLASLONG, float, float *, float *, float *, BLASLONG, BLASLONG);
  int    (*strmm_kernel_RT)(BLASLONG, BLASLONG, BLASLONG, float, float *, float *, float *, BLASLONG, BLASLONG);
  int    (*strmm_kernel_LN)(BLASLONG, BLASLONG, BLASLONG, float, float *, float *, float *, BLASLONG, BLASLONG);
  int    (*strmm_kernel_LT)(BLASLONG, BLASLONG, BLASLONG, float, float *, float *, float *, BLASLONG, BLASLONG);

  int    (*strmm_iunucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*strmm_iunncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*strmm_iutucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*strmm_iutncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*strmm_ilnucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*strmm_ilnncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*strmm_iltucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*strmm_iltncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*strmm_ounucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*strmm_ounncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*strmm_outucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*strmm_outncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*strmm_olnucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*strmm_olnncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*strmm_oltucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*strmm_oltncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);

  int    (*ssymm_iutcopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*ssymm_iltcopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*ssymm_outcopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*ssymm_oltcopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);

  int	 (*sneg_tcopy)   (BLASLONG, BLASLONG, float *, BLASLONG, float *);
  int    (*slaswp_ncopy) (BLASLONG, BLASLONG, BLASLONG, float *, BLASLONG, blasint *, float *);

  int dgemm_p, dgemm_q, dgemm_r;
  int dgemm_unroll_m, dgemm_unroll_n, dgemm_unroll_mn;

  double (*damax_k) (BLASLONG, double *, BLASLONG);
  double (*damin_k) (BLASLONG, double *, BLASLONG);
  double (*dmax_k)  (BLASLONG, double *, BLASLONG);
  double (*dmin_k)  (BLASLONG, double *, BLASLONG);
BLASLONG (*idamax_k)(BLASLONG, double *, BLASLONG);
BLASLONG (*idamin_k)(BLASLONG, double *, BLASLONG);
BLASLONG (*idmax_k) (BLASLONG, double *, BLASLONG);
BLASLONG (*idmin_k) (BLASLONG, double *, BLASLONG);

  double (*dnrm2_k) (BLASLONG, double *, BLASLONG);
  double (*dasum_k) (BLASLONG, double *, BLASLONG);
  int    (*dcopy_k) (BLASLONG, double *, BLASLONG, double *, BLASLONG);
  double (*ddot_k)  (BLASLONG, double *, BLASLONG, double *, BLASLONG);
  int    (*drot_k)  (BLASLONG, double *, BLASLONG, double *, BLASLONG, double, double);

  int    (*daxpy_k) (BLASLONG, BLASLONG, BLASLONG, double, double *, BLASLONG, double *, BLASLONG, double *, BLASLONG);
  int    (*dscal_k) (BLASLONG, BLASLONG, BLASLONG, double, double *, BLASLONG, double *, BLASLONG, double *, BLASLONG);
  int    (*dswap_k) (BLASLONG, BLASLONG, BLASLONG, double, double *, BLASLONG, double *, BLASLONG, double *, BLASLONG);

  int    (*dgemv_n) (BLASLONG, BLASLONG, BLASLONG, double, double *, BLASLONG, double *, BLASLONG, double *, BLASLONG, double *);
  int    (*dgemv_t) (BLASLONG, BLASLONG, BLASLONG, double, double *, BLASLONG, double *, BLASLONG, double *, BLASLONG, double *);
  int    (*dger_k)  (BLASLONG, BLASLONG, BLASLONG, double, double *, BLASLONG, double *, BLASLONG, double *, BLASLONG, double *);

  int    (*dsymv_L) (BLASLONG, BLASLONG, double,  double  *, BLASLONG, double  *, BLASLONG, double  *, BLASLONG, double *);
  int    (*dsymv_U) (BLASLONG, BLASLONG, double,  double  *, BLASLONG, double  *, BLASLONG, double  *, BLASLONG, double *);

  int    (*dgemm_kernel   )(BLASLONG, BLASLONG, BLASLONG, double, double *, double *, double *, BLASLONG);
  int    (*dgemm_beta     )(BLASLONG, BLASLONG, BLASLONG, double, double *, BLASLONG, double *, BLASLONG, double  *, BLASLONG);

  int    (*dgemm_incopy   )(BLASLONG, BLASLONG, double *, BLASLONG, double *);
  int    (*dgemm_itcopy   )(BLASLONG, BLASLONG, double *, BLASLONG, double *);
  int    (*dgemm_oncopy   )(BLASLONG, BLASLONG, double *, BLASLONG, double *);
  int    (*dgemm_otcopy   )(BLASLONG, BLASLONG, double *, BLASLONG, double *);

  int    (*dtrsm_kernel_LN)(BLASLONG, BLASLONG, BLASLONG, double, double *, double *, double *, BLASLONG, BLASLONG);
  int    (*dtrsm_kernel_LT)(BLASLONG, BLASLONG, BLASLONG, double, double *, double *, double *, BLASLONG, BLASLONG);
  int    (*dtrsm_kernel_RN)(BLASLONG, BLASLONG, BLASLONG, double, double *, double *, double *, BLASLONG, BLASLONG);
  int    (*dtrsm_kernel_RT)(BLASLONG, BLASLONG, BLASLONG, double, double *, double *, double *, BLASLONG, BLASLONG);

  int    (*dtrsm_iunucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*dtrsm_iunncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*dtrsm_iutucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*dtrsm_iutncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*dtrsm_ilnucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*dtrsm_ilnncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*dtrsm_iltucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*dtrsm_iltncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*dtrsm_ounucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*dtrsm_ounncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*dtrsm_outucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*dtrsm_outncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*dtrsm_olnucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*dtrsm_olnncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*dtrsm_oltucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*dtrsm_oltncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);

  int    (*dtrmm_kernel_RN)(BLASLONG, BLASLONG, BLASLONG, double, double *, double *, double *, BLASLONG, BLASLONG);
  int    (*dtrmm_kernel_RT)(BLASLONG, BLASLONG, BLASLONG, double, double *, double *, double *, BLASLONG, BLASLONG);
  int    (*dtrmm_kernel_LN)(BLASLONG, BLASLONG, BLASLONG, double, double *, double *, double *, BLASLONG, BLASLONG);
  int    (*dtrmm_kernel_LT)(BLASLONG, BLASLONG, BLASLONG, double, double *, double *, double *, BLASLONG, BLASLONG);

  int    (*dtrmm_iunucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*dtrmm_iunncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*dtrmm_iutucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*dtrmm_iutncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*dtrmm_ilnucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*dtrmm_ilnncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*dtrmm_iltucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*dtrmm_iltncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*dtrmm_ounucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*dtrmm_ounncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*dtrmm_outucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*dtrmm_outncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*dtrmm_olnucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*dtrmm_olnncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*dtrmm_oltucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*dtrmm_oltncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);

  int    (*dsymm_iutcopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*dsymm_iltcopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*dsymm_outcopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*dsymm_oltcopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);

  int	 (*dneg_tcopy)   (BLASLONG, BLASLONG, double *, BLASLONG, double *);
  int    (*dlaswp_ncopy) (BLASLONG, BLASLONG, BLASLONG, double *, BLASLONG, blasint *, double *);

#ifdef EXPRECISION

  int qgemm_p, qgemm_q, qgemm_r;
  int qgemm_unroll_m, qgemm_unroll_n, qgemm_unroll_mn;

 xdouble (*qamax_k) (BLASLONG, xdouble *, BLASLONG);
 xdouble (*qamin_k) (BLASLONG, xdouble *, BLASLONG);
 xdouble (*qmax_k)  (BLASLONG, xdouble *, BLASLONG);
 xdouble (*qmin_k)  (BLASLONG, xdouble *, BLASLONG);
BLASLONG (*iqamax_k)(BLASLONG, xdouble *, BLASLONG);
BLASLONG (*iqamin_k)(BLASLONG, xdouble *, BLASLONG);
BLASLONG (*iqmax_k) (BLASLONG, xdouble *, BLASLONG);
BLASLONG (*iqmin_k) (BLASLONG, xdouble *, BLASLONG);

 xdouble (*qnrm2_k) (BLASLONG, xdouble *, BLASLONG);
 xdouble (*qasum_k) (BLASLONG, xdouble *, BLASLONG);
  int    (*qcopy_k) (BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG);
 xdouble (*qdot_k)  (BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG);
  int    (*qrot_k)  (BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble, xdouble);

  int    (*qaxpy_k) (BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG);
  int    (*qscal_k) (BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG);
  int    (*qswap_k) (BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG);

  int    (*qgemv_n) (BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*qgemv_t) (BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*qger_k)  (BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *);

  int    (*qsymv_L) (BLASLONG, BLASLONG, xdouble,  xdouble  *, BLASLONG, xdouble  *, BLASLONG, xdouble  *, BLASLONG, xdouble *);
  int    (*qsymv_U) (BLASLONG, BLASLONG, xdouble,  xdouble  *, BLASLONG, xdouble  *, BLASLONG, xdouble  *, BLASLONG, xdouble *);

  int    (*qgemm_kernel   )(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG);
  int    (*qgemm_beta     )(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble  *, BLASLONG);

  int    (*qgemm_incopy   )(BLASLONG, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*qgemm_itcopy   )(BLASLONG, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*qgemm_oncopy   )(BLASLONG, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*qgemm_otcopy   )(BLASLONG, BLASLONG, xdouble *, BLASLONG, xdouble *);

  int    (*qtrsm_kernel_LN)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG, BLASLONG);
  int    (*qtrsm_kernel_LT)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG, BLASLONG);
  int    (*qtrsm_kernel_RN)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG, BLASLONG);
  int    (*qtrsm_kernel_RT)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG, BLASLONG);

  int    (*qtrsm_iunucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrsm_iunncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrsm_iutucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrsm_iutncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrsm_ilnucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrsm_ilnncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrsm_iltucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrsm_iltncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrsm_ounucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrsm_ounncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrsm_outucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrsm_outncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrsm_olnucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrsm_olnncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrsm_oltucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrsm_oltncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);

  int    (*qtrmm_kernel_RN)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG, BLASLONG);
  int    (*qtrmm_kernel_RT)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG, BLASLONG);
  int    (*qtrmm_kernel_LN)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG, BLASLONG);
  int    (*qtrmm_kernel_LT)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG, BLASLONG);

  int    (*qtrmm_iunucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrmm_iunncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrmm_iutucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrmm_iutncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrmm_ilnucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrmm_ilnncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrmm_iltucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrmm_iltncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrmm_ounucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrmm_ounncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrmm_outucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrmm_outncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrmm_olnucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrmm_olnncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrmm_oltucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*qtrmm_oltncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);

  int    (*qsymm_iutcopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*qsymm_iltcopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*qsymm_outcopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*qsymm_oltcopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);

  int	 (*qneg_tcopy)   (BLASLONG, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*qlaswp_ncopy) (BLASLONG, BLASLONG, BLASLONG, xdouble *, BLASLONG, blasint *, xdouble *);

#endif

  int cgemm_p, cgemm_q, cgemm_r;
  int cgemm_unroll_m, cgemm_unroll_n, cgemm_unroll_mn;

  float (*camax_k) (BLASLONG, float *, BLASLONG);
  float (*camin_k) (BLASLONG, float *, BLASLONG);
BLASLONG (*icamax_k)(BLASLONG, float *, BLASLONG);
BLASLONG (*icamin_k)(BLASLONG, float *, BLASLONG);

  float (*cnrm2_k) (BLASLONG, float *, BLASLONG);
  float (*casum_k) (BLASLONG, float *, BLASLONG);
  int    (*ccopy_k) (BLASLONG, float *, BLASLONG, float *, BLASLONG);
  openblas_complex_float (*cdotu_k) (BLASLONG, float *, BLASLONG, float *, BLASLONG);
  openblas_complex_float (*cdotc_k) (BLASLONG, float *, BLASLONG, float *, BLASLONG);
  int    (*csrot_k) (BLASLONG, float *, BLASLONG, float *, BLASLONG, float, float);

  int    (*caxpy_k) (BLASLONG, BLASLONG, BLASLONG, float, float, float *, BLASLONG, float *, BLASLONG, float *, BLASLONG);
  int    (*caxpyc_k)(BLASLONG, BLASLONG, BLASLONG, float, float, float *, BLASLONG, float *, BLASLONG, float *, BLASLONG);
  int    (*cscal_k) (BLASLONG, BLASLONG, BLASLONG, float, float, float *, BLASLONG, float *, BLASLONG, float *, BLASLONG);
  int    (*cswap_k) (BLASLONG, BLASLONG, BLASLONG, float, float, float *, BLASLONG, float *, BLASLONG, float *, BLASLONG);

  int    (*cgemv_n) (BLASLONG, BLASLONG, BLASLONG, float, float, float *, BLASLONG, float *, BLASLONG, float *, BLASLONG, float *);
  int    (*cgemv_t) (BLASLONG, BLASLONG, BLASLONG, float, float, float *, BLASLONG, float *, BLASLONG, float *, BLASLONG, float *);
  int    (*cgemv_r) (BLASLONG, BLASLONG, BLASLONG, float, float, float *, BLASLONG, float *, BLASLONG, float *, BLASLONG, float *);
  int    (*cgemv_c) (BLASLONG, BLASLONG, BLASLONG, float, float, float *, BLASLONG, float *, BLASLONG, float *, BLASLONG, float *);
  int    (*cgemv_o) (BLASLONG, BLASLONG, BLASLONG, float, float, float *, BLASLONG, float *, BLASLONG, float *, BLASLONG, float *);
  int    (*cgemv_u) (BLASLONG, BLASLONG, BLASLONG, float, float, float *, BLASLONG, float *, BLASLONG, float *, BLASLONG, float *);
  int    (*cgemv_s) (BLASLONG, BLASLONG, BLASLONG, float, float, float *, BLASLONG, float *, BLASLONG, float *, BLASLONG, float *);
  int    (*cgemv_d) (BLASLONG, BLASLONG, BLASLONG, float, float, float *, BLASLONG, float *, BLASLONG, float *, BLASLONG, float *);
  int    (*cgeru_k) (BLASLONG, BLASLONG, BLASLONG, float, float, float *, BLASLONG, float *, BLASLONG, float *, BLASLONG, float *);
  int    (*cgerc_k) (BLASLONG, BLASLONG, BLASLONG, float, float, float *, BLASLONG, float *, BLASLONG, float *, BLASLONG, float *);
  int    (*cgerv_k) (BLASLONG, BLASLONG, BLASLONG, float, float, float *, BLASLONG, float *, BLASLONG, float *, BLASLONG, float *);
  int    (*cgerd_k) (BLASLONG, BLASLONG, BLASLONG, float, float, float *, BLASLONG, float *, BLASLONG, float *, BLASLONG, float *);

  int    (*csymv_L) (BLASLONG, BLASLONG, float,  float, float  *, BLASLONG, float  *, BLASLONG, float  *, BLASLONG, float *);
  int    (*csymv_U) (BLASLONG, BLASLONG, float,  float, float  *, BLASLONG, float  *, BLASLONG, float  *, BLASLONG, float *);
  int    (*chemv_L) (BLASLONG, BLASLONG, float,  float, float  *, BLASLONG, float  *, BLASLONG, float  *, BLASLONG, float *);
  int    (*chemv_U) (BLASLONG, BLASLONG, float,  float, float  *, BLASLONG, float  *, BLASLONG, float  *, BLASLONG, float *);
  int    (*chemv_M) (BLASLONG, BLASLONG, float,  float, float  *, BLASLONG, float  *, BLASLONG, float  *, BLASLONG, float *);
  int    (*chemv_V) (BLASLONG, BLASLONG, float,  float, float  *, BLASLONG, float  *, BLASLONG, float  *, BLASLONG, float *);

  int    (*cgemm_kernel_n )(BLASLONG, BLASLONG, BLASLONG, float, float, float *, float *, float *, BLASLONG);
  int    (*cgemm_kernel_l )(BLASLONG, BLASLONG, BLASLONG, float, float, float *, float *, float *, BLASLONG);
  int    (*cgemm_kernel_r )(BLASLONG, BLASLONG, BLASLONG, float, float, float *, float *, float *, BLASLONG);
  int    (*cgemm_kernel_b )(BLASLONG, BLASLONG, BLASLONG, float, float, float *, float *, float *, BLASLONG);
  int    (*cgemm_beta     )(BLASLONG, BLASLONG, BLASLONG, float, float, float *, BLASLONG, float *, BLASLONG, float  *, BLASLONG);

  int    (*cgemm_incopy   )(BLASLONG, BLASLONG, float *, BLASLONG, float *);
  int    (*cgemm_itcopy   )(BLASLONG, BLASLONG, float *, BLASLONG, float *);
  int    (*cgemm_oncopy   )(BLASLONG, BLASLONG, float *, BLASLONG, float *);
  int    (*cgemm_otcopy   )(BLASLONG, BLASLONG, float *, BLASLONG, float *);

  int    (*ctrsm_kernel_LN)(BLASLONG, BLASLONG, BLASLONG, float, float, float *, float *, float *, BLASLONG, BLASLONG);
  int    (*ctrsm_kernel_LT)(BLASLONG, BLASLONG, BLASLONG, float, float, float *, float *, float *, BLASLONG, BLASLONG);
  int    (*ctrsm_kernel_LR)(BLASLONG, BLASLONG, BLASLONG, float, float, float *, float *, float *, BLASLONG, BLASLONG);
  int    (*ctrsm_kernel_LC)(BLASLONG, BLASLONG, BLASLONG, float, float, float *, float *, float *, BLASLONG, BLASLONG);
  int    (*ctrsm_kernel_RN)(BLASLONG, BLASLONG, BLASLONG, float, float, float *, float *, float *, BLASLONG, BLASLONG);
  int    (*ctrsm_kernel_RT)(BLASLONG, BLASLONG, BLASLONG, float, float, float *, float *, float *, BLASLONG, BLASLONG);
  int    (*ctrsm_kernel_RR)(BLASLONG, BLASLONG, BLASLONG, float, float, float *, float *, float *, BLASLONG, BLASLONG);
  int    (*ctrsm_kernel_RC)(BLASLONG, BLASLONG, BLASLONG, float, float, float *, float *, float *, BLASLONG, BLASLONG);

  int    (*ctrsm_iunucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*ctrsm_iunncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*ctrsm_iutucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*ctrsm_iutncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*ctrsm_ilnucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*ctrsm_ilnncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*ctrsm_iltucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*ctrsm_iltncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*ctrsm_ounucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*ctrsm_ounncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*ctrsm_outucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*ctrsm_outncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*ctrsm_olnucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*ctrsm_olnncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*ctrsm_oltucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);
  int    (*ctrsm_oltncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, float *);

  int    (*ctrmm_kernel_RN)(BLASLONG, BLASLONG, BLASLONG, float, float, float *, float *, float *, BLASLONG, BLASLONG);
  int    (*ctrmm_kernel_RT)(BLASLONG, BLASLONG, BLASLONG, float, float, float *, float *, float *, BLASLONG, BLASLONG);
  int    (*ctrmm_kernel_RR)(BLASLONG, BLASLONG, BLASLONG, float, float, float *, float *, float *, BLASLONG, BLASLONG);
  int    (*ctrmm_kernel_RC)(BLASLONG, BLASLONG, BLASLONG, float, float, float *, float *, float *, BLASLONG, BLASLONG);
  int    (*ctrmm_kernel_LN)(BLASLONG, BLASLONG, BLASLONG, float, float, float *, float *, float *, BLASLONG, BLASLONG);
  int    (*ctrmm_kernel_LT)(BLASLONG, BLASLONG, BLASLONG, float, float, float *, float *, float *, BLASLONG, BLASLONG);
  int    (*ctrmm_kernel_LR)(BLASLONG, BLASLONG, BLASLONG, float, float, float *, float *, float *, BLASLONG, BLASLONG);
  int    (*ctrmm_kernel_LC)(BLASLONG, BLASLONG, BLASLONG, float, float, float *, float *, float *, BLASLONG, BLASLONG);

  int    (*ctrmm_iunucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*ctrmm_iunncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*ctrmm_iutucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*ctrmm_iutncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*ctrmm_ilnucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*ctrmm_ilnncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*ctrmm_iltucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*ctrmm_iltncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*ctrmm_ounucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*ctrmm_ounncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*ctrmm_outucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*ctrmm_outncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*ctrmm_olnucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*ctrmm_olnncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*ctrmm_oltucopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*ctrmm_oltncopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);

  int    (*csymm_iutcopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*csymm_iltcopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*csymm_outcopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*csymm_oltcopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);

  int    (*chemm_iutcopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*chemm_iltcopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*chemm_outcopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*chemm_oltcopy)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);

  int cgemm3m_p, cgemm3m_q, cgemm3m_r;
  int cgemm3m_unroll_m, cgemm3m_unroll_n, cgemm3m_unroll_mn;

  int    (*cgemm3m_kernel)(BLASLONG, BLASLONG, BLASLONG, float, float, float *, float *, float *, BLASLONG);

  int    (*cgemm3m_incopyb)(BLASLONG, BLASLONG, float *, BLASLONG, float *);
  int    (*cgemm3m_incopyr)(BLASLONG, BLASLONG, float *, BLASLONG, float *);
  int    (*cgemm3m_incopyi)(BLASLONG, BLASLONG, float *, BLASLONG, float *);
  int    (*cgemm3m_itcopyb)(BLASLONG, BLASLONG, float *, BLASLONG, float *);
  int    (*cgemm3m_itcopyr)(BLASLONG, BLASLONG, float *, BLASLONG, float *);
  int    (*cgemm3m_itcopyi)(BLASLONG, BLASLONG, float *, BLASLONG, float *);

  int    (*cgemm3m_oncopyb)(BLASLONG, BLASLONG, float *, BLASLONG, float, float, float *);
  int    (*cgemm3m_oncopyr)(BLASLONG, BLASLONG, float *, BLASLONG, float, float, float *);
  int    (*cgemm3m_oncopyi)(BLASLONG, BLASLONG, float *, BLASLONG, float, float, float *);
  int    (*cgemm3m_otcopyb)(BLASLONG, BLASLONG, float *, BLASLONG, float, float, float *);
  int    (*cgemm3m_otcopyr)(BLASLONG, BLASLONG, float *, BLASLONG, float, float, float *);
  int    (*cgemm3m_otcopyi)(BLASLONG, BLASLONG, float *, BLASLONG, float, float, float *);

  int    (*csymm3m_iucopyb)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*csymm3m_ilcopyb)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*csymm3m_iucopyr)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*csymm3m_ilcopyr)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*csymm3m_iucopyi)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*csymm3m_ilcopyi)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);

  int    (*csymm3m_oucopyb)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float, float, float *);
  int    (*csymm3m_olcopyb)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float, float, float *);
  int    (*csymm3m_oucopyr)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float, float, float *);
  int    (*csymm3m_olcopyr)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float, float, float *);
  int    (*csymm3m_oucopyi)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float, float, float *);
  int    (*csymm3m_olcopyi)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float, float, float *);

  int    (*chemm3m_iucopyb)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*chemm3m_ilcopyb)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*chemm3m_iucopyr)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*chemm3m_ilcopyr)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*chemm3m_iucopyi)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);
  int    (*chemm3m_ilcopyi)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float *);

  int    (*chemm3m_oucopyb)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float, float, float *);
  int    (*chemm3m_olcopyb)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float, float, float *);
  int    (*chemm3m_oucopyr)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float, float, float *);
  int    (*chemm3m_olcopyr)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float, float, float *);
  int    (*chemm3m_oucopyi)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float, float, float *);
  int    (*chemm3m_olcopyi)(BLASLONG, BLASLONG, float *, BLASLONG, BLASLONG, BLASLONG, float, float, float *);

  int	 (*cneg_tcopy)   (BLASLONG, BLASLONG, float *, BLASLONG, float *);
  int    (*claswp_ncopy) (BLASLONG, BLASLONG, BLASLONG, float *, BLASLONG, blasint *, float *);

  int zgemm_p, zgemm_q, zgemm_r;
  int zgemm_unroll_m, zgemm_unroll_n, zgemm_unroll_mn;

  double (*zamax_k) (BLASLONG, double *, BLASLONG);
  double (*zamin_k) (BLASLONG, double *, BLASLONG);
BLASLONG (*izamax_k)(BLASLONG, double *, BLASLONG);
BLASLONG (*izamin_k)(BLASLONG, double *, BLASLONG);

  double (*znrm2_k) (BLASLONG, double *, BLASLONG);
  double (*zasum_k) (BLASLONG, double *, BLASLONG);
  int    (*zcopy_k) (BLASLONG, double *, BLASLONG, double *, BLASLONG);
  openblas_complex_double (*zdotu_k) (BLASLONG, double *, BLASLONG, double *, BLASLONG);
  openblas_complex_double (*zdotc_k) (BLASLONG, double *, BLASLONG, double *, BLASLONG);
  int    (*zdrot_k) (BLASLONG, double *, BLASLONG, double *, BLASLONG, double, double);

  int    (*zaxpy_k) (BLASLONG, BLASLONG, BLASLONG, double, double, double *, BLASLONG, double *, BLASLONG, double *, BLASLONG);
  int    (*zaxpyc_k)(BLASLONG, BLASLONG, BLASLONG, double, double, double *, BLASLONG, double *, BLASLONG, double *, BLASLONG);
  int    (*zscal_k) (BLASLONG, BLASLONG, BLASLONG, double, double, double *, BLASLONG, double *, BLASLONG, double *, BLASLONG);
  int    (*zswap_k) (BLASLONG, BLASLONG, BLASLONG, double, double, double *, BLASLONG, double *, BLASLONG, double *, BLASLONG);

  int    (*zgemv_n) (BLASLONG, BLASLONG, BLASLONG, double, double, double *, BLASLONG, double *, BLASLONG, double *, BLASLONG, double *);
  int    (*zgemv_t) (BLASLONG, BLASLONG, BLASLONG, double, double, double *, BLASLONG, double *, BLASLONG, double *, BLASLONG, double *);
  int    (*zgemv_r) (BLASLONG, BLASLONG, BLASLONG, double, double, double *, BLASLONG, double *, BLASLONG, double *, BLASLONG, double *);
  int    (*zgemv_c) (BLASLONG, BLASLONG, BLASLONG, double, double, double *, BLASLONG, double *, BLASLONG, double *, BLASLONG, double *);
  int    (*zgemv_o) (BLASLONG, BLASLONG, BLASLONG, double, double, double *, BLASLONG, double *, BLASLONG, double *, BLASLONG, double *);
  int    (*zgemv_u) (BLASLONG, BLASLONG, BLASLONG, double, double, double *, BLASLONG, double *, BLASLONG, double *, BLASLONG, double *);
  int    (*zgemv_s) (BLASLONG, BLASLONG, BLASLONG, double, double, double *, BLASLONG, double *, BLASLONG, double *, BLASLONG, double *);
  int    (*zgemv_d) (BLASLONG, BLASLONG, BLASLONG, double, double, double *, BLASLONG, double *, BLASLONG, double *, BLASLONG, double *);
  int    (*zgeru_k) (BLASLONG, BLASLONG, BLASLONG, double, double, double *, BLASLONG, double *, BLASLONG, double *, BLASLONG, double *);
  int    (*zgerc_k) (BLASLONG, BLASLONG, BLASLONG, double, double, double *, BLASLONG, double *, BLASLONG, double *, BLASLONG, double *);
  int    (*zgerv_k) (BLASLONG, BLASLONG, BLASLONG, double, double, double *, BLASLONG, double *, BLASLONG, double *, BLASLONG, double *);
  int    (*zgerd_k) (BLASLONG, BLASLONG, BLASLONG, double, double, double *, BLASLONG, double *, BLASLONG, double *, BLASLONG, double *);

  int    (*zsymv_L) (BLASLONG, BLASLONG, double,  double, double  *, BLASLONG, double  *, BLASLONG, double  *, BLASLONG, double *);
  int    (*zsymv_U) (BLASLONG, BLASLONG, double,  double, double  *, BLASLONG, double  *, BLASLONG, double  *, BLASLONG, double *);
  int    (*zhemv_L) (BLASLONG, BLASLONG, double,  double, double  *, BLASLONG, double  *, BLASLONG, double  *, BLASLONG, double *);
  int    (*zhemv_U) (BLASLONG, BLASLONG, double,  double, double  *, BLASLONG, double  *, BLASLONG, double  *, BLASLONG, double *);
  int    (*zhemv_M) (BLASLONG, BLASLONG, double,  double, double  *, BLASLONG, double  *, BLASLONG, double  *, BLASLONG, double *);
  int    (*zhemv_V) (BLASLONG, BLASLONG, double,  double, double  *, BLASLONG, double  *, BLASLONG, double  *, BLASLONG, double *);

  int    (*zgemm_kernel_n )(BLASLONG, BLASLONG, BLASLONG, double, double, double *, double *, double *, BLASLONG);
  int    (*zgemm_kernel_l )(BLASLONG, BLASLONG, BLASLONG, double, double, double *, double *, double *, BLASLONG);
  int    (*zgemm_kernel_r )(BLASLONG, BLASLONG, BLASLONG, double, double, double *, double *, double *, BLASLONG);
  int    (*zgemm_kernel_b )(BLASLONG, BLASLONG, BLASLONG, double, double, double *, double *, double *, BLASLONG);
  int    (*zgemm_beta     )(BLASLONG, BLASLONG, BLASLONG, double, double, double *, BLASLONG, double *, BLASLONG, double  *, BLASLONG);

  int    (*zgemm_incopy   )(BLASLONG, BLASLONG, double *, BLASLONG, double *);
  int    (*zgemm_itcopy   )(BLASLONG, BLASLONG, double *, BLASLONG, double *);
  int    (*zgemm_oncopy   )(BLASLONG, BLASLONG, double *, BLASLONG, double *);
  int    (*zgemm_otcopy   )(BLASLONG, BLASLONG, double *, BLASLONG, double *);

  int    (*ztrsm_kernel_LN)(BLASLONG, BLASLONG, BLASLONG, double, double, double *, double *, double *, BLASLONG, BLASLONG);
  int    (*ztrsm_kernel_LT)(BLASLONG, BLASLONG, BLASLONG, double, double, double *, double *, double *, BLASLONG, BLASLONG);
  int    (*ztrsm_kernel_LR)(BLASLONG, BLASLONG, BLASLONG, double, double, double *, double *, double *, BLASLONG, BLASLONG);
  int    (*ztrsm_kernel_LC)(BLASLONG, BLASLONG, BLASLONG, double, double, double *, double *, double *, BLASLONG, BLASLONG);
  int    (*ztrsm_kernel_RN)(BLASLONG, BLASLONG, BLASLONG, double, double, double *, double *, double *, BLASLONG, BLASLONG);
  int    (*ztrsm_kernel_RT)(BLASLONG, BLASLONG, BLASLONG, double, double, double *, double *, double *, BLASLONG, BLASLONG);
  int    (*ztrsm_kernel_RR)(BLASLONG, BLASLONG, BLASLONG, double, double, double *, double *, double *, BLASLONG, BLASLONG);
  int    (*ztrsm_kernel_RC)(BLASLONG, BLASLONG, BLASLONG, double, double, double *, double *, double *, BLASLONG, BLASLONG);

  int    (*ztrsm_iunucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*ztrsm_iunncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*ztrsm_iutucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*ztrsm_iutncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*ztrsm_ilnucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*ztrsm_ilnncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*ztrsm_iltucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*ztrsm_iltncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*ztrsm_ounucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*ztrsm_ounncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*ztrsm_outucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*ztrsm_outncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*ztrsm_olnucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*ztrsm_olnncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*ztrsm_oltucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);
  int    (*ztrsm_oltncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, double *);

  int    (*ztrmm_kernel_RN)(BLASLONG, BLASLONG, BLASLONG, double, double, double *, double *, double *, BLASLONG, BLASLONG);
  int    (*ztrmm_kernel_RT)(BLASLONG, BLASLONG, BLASLONG, double, double, double *, double *, double *, BLASLONG, BLASLONG);
  int    (*ztrmm_kernel_RR)(BLASLONG, BLASLONG, BLASLONG, double, double, double *, double *, double *, BLASLONG, BLASLONG);
  int    (*ztrmm_kernel_RC)(BLASLONG, BLASLONG, BLASLONG, double, double, double *, double *, double *, BLASLONG, BLASLONG);
  int    (*ztrmm_kernel_LN)(BLASLONG, BLASLONG, BLASLONG, double, double, double *, double *, double *, BLASLONG, BLASLONG);
  int    (*ztrmm_kernel_LT)(BLASLONG, BLASLONG, BLASLONG, double, double, double *, double *, double *, BLASLONG, BLASLONG);
  int    (*ztrmm_kernel_LR)(BLASLONG, BLASLONG, BLASLONG, double, double, double *, double *, double *, BLASLONG, BLASLONG);
  int    (*ztrmm_kernel_LC)(BLASLONG, BLASLONG, BLASLONG, double, double, double *, double *, double *, BLASLONG, BLASLONG);

  int    (*ztrmm_iunucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*ztrmm_iunncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*ztrmm_iutucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*ztrmm_iutncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*ztrmm_ilnucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*ztrmm_ilnncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*ztrmm_iltucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*ztrmm_iltncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*ztrmm_ounucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*ztrmm_ounncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*ztrmm_outucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*ztrmm_outncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*ztrmm_olnucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*ztrmm_olnncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*ztrmm_oltucopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*ztrmm_oltncopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);

  int    (*zsymm_iutcopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*zsymm_iltcopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*zsymm_outcopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*zsymm_oltcopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);

  int    (*zhemm_iutcopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*zhemm_iltcopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*zhemm_outcopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*zhemm_oltcopy)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);

  int zgemm3m_p, zgemm3m_q, zgemm3m_r;
  int zgemm3m_unroll_m, zgemm3m_unroll_n, zgemm3m_unroll_mn;

  int    (*zgemm3m_kernel)(BLASLONG, BLASLONG, BLASLONG, double, double, double *, double *, double *, BLASLONG);

  int    (*zgemm3m_incopyb)(BLASLONG, BLASLONG, double *, BLASLONG, double *);
  int    (*zgemm3m_incopyr)(BLASLONG, BLASLONG, double *, BLASLONG, double *);
  int    (*zgemm3m_incopyi)(BLASLONG, BLASLONG, double *, BLASLONG, double *);
  int    (*zgemm3m_itcopyb)(BLASLONG, BLASLONG, double *, BLASLONG, double *);
  int    (*zgemm3m_itcopyr)(BLASLONG, BLASLONG, double *, BLASLONG, double *);
  int    (*zgemm3m_itcopyi)(BLASLONG, BLASLONG, double *, BLASLONG, double *);

  int    (*zgemm3m_oncopyb)(BLASLONG, BLASLONG, double *, BLASLONG, double, double, double *);
  int    (*zgemm3m_oncopyr)(BLASLONG, BLASLONG, double *, BLASLONG, double, double, double *);
  int    (*zgemm3m_oncopyi)(BLASLONG, BLASLONG, double *, BLASLONG, double, double, double *);
  int    (*zgemm3m_otcopyb)(BLASLONG, BLASLONG, double *, BLASLONG, double, double, double *);
  int    (*zgemm3m_otcopyr)(BLASLONG, BLASLONG, double *, BLASLONG, double, double, double *);
  int    (*zgemm3m_otcopyi)(BLASLONG, BLASLONG, double *, BLASLONG, double, double, double *);

  int    (*zsymm3m_iucopyb)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*zsymm3m_ilcopyb)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*zsymm3m_iucopyr)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*zsymm3m_ilcopyr)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*zsymm3m_iucopyi)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*zsymm3m_ilcopyi)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);

  int    (*zsymm3m_oucopyb)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double, double, double *);
  int    (*zsymm3m_olcopyb)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double, double, double *);
  int    (*zsymm3m_oucopyr)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double, double, double *);
  int    (*zsymm3m_olcopyr)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double, double, double *);
  int    (*zsymm3m_oucopyi)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double, double, double *);
  int    (*zsymm3m_olcopyi)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double, double, double *);

  int    (*zhemm3m_iucopyb)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*zhemm3m_ilcopyb)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*zhemm3m_iucopyr)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*zhemm3m_ilcopyr)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*zhemm3m_iucopyi)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);
  int    (*zhemm3m_ilcopyi)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double *);

  int    (*zhemm3m_oucopyb)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double, double, double *);
  int    (*zhemm3m_olcopyb)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double, double, double *);
  int    (*zhemm3m_oucopyr)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double, double, double *);
  int    (*zhemm3m_olcopyr)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double, double, double *);
  int    (*zhemm3m_oucopyi)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double, double, double *);
  int    (*zhemm3m_olcopyi)(BLASLONG, BLASLONG, double *, BLASLONG, BLASLONG, BLASLONG, double, double, double *);

  int	 (*zneg_tcopy)   (BLASLONG, BLASLONG, double *, BLASLONG, double *);
  int    (*zlaswp_ncopy) (BLASLONG, BLASLONG, BLASLONG, double *, BLASLONG, blasint *, double *);

#ifdef EXPRECISION

  int xgemm_p, xgemm_q, xgemm_r;
  int xgemm_unroll_m, xgemm_unroll_n, xgemm_unroll_mn;

  xdouble (*xamax_k) (BLASLONG, xdouble *, BLASLONG);
  xdouble (*xamin_k) (BLASLONG, xdouble *, BLASLONG);
BLASLONG (*ixamax_k)(BLASLONG, xdouble *, BLASLONG);
BLASLONG (*ixamin_k)(BLASLONG, xdouble *, BLASLONG);

  xdouble (*xnrm2_k) (BLASLONG, xdouble *, BLASLONG);
  xdouble (*xasum_k) (BLASLONG, xdouble *, BLASLONG);
  int    (*xcopy_k) (BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG);
  openblas_complex_xdouble (*xdotu_k) (BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG);
  openblas_complex_xdouble (*xdotc_k) (BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG);
  int    (*xqrot_k) (BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble, xdouble);

  int    (*xaxpy_k) (BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG);
  int    (*xaxpyc_k)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG);
  int    (*xscal_k) (BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG);
  int    (*xswap_k) (BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG);

  int    (*xgemv_n) (BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*xgemv_t) (BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*xgemv_r) (BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*xgemv_c) (BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*xgemv_o) (BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*xgemv_u) (BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*xgemv_s) (BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*xgemv_d) (BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*xgeru_k) (BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*xgerc_k) (BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*xgerv_k) (BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*xgerd_k) (BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble *);

  int    (*xsymv_L) (BLASLONG, BLASLONG, xdouble,  xdouble, xdouble  *, BLASLONG, xdouble  *, BLASLONG, xdouble  *, BLASLONG, xdouble *);
  int    (*xsymv_U) (BLASLONG, BLASLONG, xdouble,  xdouble, xdouble  *, BLASLONG, xdouble  *, BLASLONG, xdouble  *, BLASLONG, xdouble *);
  int    (*xhemv_L) (BLASLONG, BLASLONG, xdouble,  xdouble, xdouble  *, BLASLONG, xdouble  *, BLASLONG, xdouble  *, BLASLONG, xdouble *);
  int    (*xhemv_U) (BLASLONG, BLASLONG, xdouble,  xdouble, xdouble  *, BLASLONG, xdouble  *, BLASLONG, xdouble  *, BLASLONG, xdouble *);
  int    (*xhemv_M) (BLASLONG, BLASLONG, xdouble,  xdouble, xdouble  *, BLASLONG, xdouble  *, BLASLONG, xdouble  *, BLASLONG, xdouble *);
  int    (*xhemv_V) (BLASLONG, BLASLONG, xdouble,  xdouble, xdouble  *, BLASLONG, xdouble  *, BLASLONG, xdouble  *, BLASLONG, xdouble *);

  int    (*xgemm_kernel_n )(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG);
  int    (*xgemm_kernel_l )(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG);
  int    (*xgemm_kernel_r )(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG);
  int    (*xgemm_kernel_b )(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG);
  int    (*xgemm_beta     )(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, BLASLONG, xdouble *, BLASLONG, xdouble  *, BLASLONG);

  int    (*xgemm_incopy   )(BLASLONG, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*xgemm_itcopy   )(BLASLONG, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*xgemm_oncopy   )(BLASLONG, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*xgemm_otcopy   )(BLASLONG, BLASLONG, xdouble *, BLASLONG, xdouble *);

  int    (*xtrsm_kernel_LN)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG, BLASLONG);
  int    (*xtrsm_kernel_LT)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG, BLASLONG);
  int    (*xtrsm_kernel_LR)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG, BLASLONG);
  int    (*xtrsm_kernel_LC)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG, BLASLONG);
  int    (*xtrsm_kernel_RN)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG, BLASLONG);
  int    (*xtrsm_kernel_RT)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG, BLASLONG);
  int    (*xtrsm_kernel_RR)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG, BLASLONG);
  int    (*xtrsm_kernel_RC)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG, BLASLONG);

  int    (*xtrsm_iunucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrsm_iunncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrsm_iutucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrsm_iutncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrsm_ilnucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrsm_ilnncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrsm_iltucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrsm_iltncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrsm_ounucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrsm_ounncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrsm_outucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrsm_outncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrsm_olnucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrsm_olnncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrsm_oltucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrsm_oltncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, xdouble *);

  int    (*xtrmm_kernel_RN)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG, BLASLONG);
  int    (*xtrmm_kernel_RT)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG, BLASLONG);
  int    (*xtrmm_kernel_RR)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG, BLASLONG);
  int    (*xtrmm_kernel_RC)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG, BLASLONG);
  int    (*xtrmm_kernel_LN)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG, BLASLONG);
  int    (*xtrmm_kernel_LT)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG, BLASLONG);
  int    (*xtrmm_kernel_LR)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG, BLASLONG);
  int    (*xtrmm_kernel_LC)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG, BLASLONG);

  int    (*xtrmm_iunucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrmm_iunncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrmm_iutucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrmm_iutncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrmm_ilnucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrmm_ilnncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrmm_iltucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrmm_iltncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrmm_ounucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrmm_ounncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrmm_outucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrmm_outncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrmm_olnucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrmm_olnncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrmm_oltucopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xtrmm_oltncopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);

  int    (*xsymm_iutcopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xsymm_iltcopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xsymm_outcopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xsymm_oltcopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);

  int    (*xhemm_iutcopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xhemm_iltcopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xhemm_outcopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xhemm_oltcopy)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);

  int xgemm3m_p, xgemm3m_q, xgemm3m_r;
  int xgemm3m_unroll_m, xgemm3m_unroll_n, xgemm3m_unroll_mn;

  int    (*xgemm3m_kernel)(BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *, xdouble *, xdouble *, BLASLONG);

  int    (*xgemm3m_incopyb)(BLASLONG, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*xgemm3m_incopyr)(BLASLONG, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*xgemm3m_incopyi)(BLASLONG, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*xgemm3m_itcopyb)(BLASLONG, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*xgemm3m_itcopyr)(BLASLONG, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*xgemm3m_itcopyi)(BLASLONG, BLASLONG, xdouble *, BLASLONG, xdouble *);

  int    (*xgemm3m_oncopyb)(BLASLONG, BLASLONG, xdouble *, BLASLONG, xdouble, xdouble, xdouble *);
  int    (*xgemm3m_oncopyr)(BLASLONG, BLASLONG, xdouble *, BLASLONG, xdouble, xdouble, xdouble *);
  int    (*xgemm3m_oncopyi)(BLASLONG, BLASLONG, xdouble *, BLASLONG, xdouble, xdouble, xdouble *);
  int    (*xgemm3m_otcopyb)(BLASLONG, BLASLONG, xdouble *, BLASLONG, xdouble, xdouble, xdouble *);
  int    (*xgemm3m_otcopyr)(BLASLONG, BLASLONG, xdouble *, BLASLONG, xdouble, xdouble, xdouble *);
  int    (*xgemm3m_otcopyi)(BLASLONG, BLASLONG, xdouble *, BLASLONG, xdouble, xdouble, xdouble *);

  int    (*xsymm3m_iucopyb)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xsymm3m_ilcopyb)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xsymm3m_iucopyr)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xsymm3m_ilcopyr)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xsymm3m_iucopyi)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xsymm3m_ilcopyi)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);

  int    (*xsymm3m_oucopyb)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *);
  int    (*xsymm3m_olcopyb)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *);
  int    (*xsymm3m_oucopyr)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *);
  int    (*xsymm3m_olcopyr)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *);
  int    (*xsymm3m_oucopyi)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *);
  int    (*xsymm3m_olcopyi)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *);

  int    (*xhemm3m_iucopyb)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xhemm3m_ilcopyb)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xhemm3m_iucopyr)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xhemm3m_ilcopyr)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xhemm3m_iucopyi)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);
  int    (*xhemm3m_ilcopyi)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble *);

  int    (*xhemm3m_oucopyb)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *);
  int    (*xhemm3m_olcopyb)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *);
  int    (*xhemm3m_oucopyr)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *);
  int    (*xhemm3m_olcopyr)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *);
  int    (*xhemm3m_oucopyi)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *);
  int    (*xhemm3m_olcopyi)(BLASLONG, BLASLONG, xdouble *, BLASLONG, BLASLONG, BLASLONG, xdouble, xdouble, xdouble *);

  int	 (*xneg_tcopy)   (BLASLONG, BLASLONG, xdouble *, BLASLONG, xdouble *);
  int    (*xlaswp_ncopy) (BLASLONG, BLASLONG, BLASLONG, xdouble *, BLASLONG, blasint *, xdouble *);

#endif


  void (*init)(void);

  int snum_opt, dnum_opt, qnum_opt;

  int    (*saxpby_k)     (BLASLONG, float, float*, BLASLONG,float, float*, BLASLONG);
  int    (*daxpby_k)     (BLASLONG, double, double*, BLASLONG,double, double*, BLASLONG);
  int    (*caxpby_k)     (BLASLONG, float, float,  float*, BLASLONG,float,float, float*, BLASLONG);
  int    (*zaxpby_k)     (BLASLONG, double, double,  double*, BLASLONG,double,double, double*, BLASLONG);

  int    (*somatcopy_k_cn)	(BLASLONG, BLASLONG, float, float*, BLASLONG, float*, BLASLONG);
  int    (*somatcopy_k_ct)	(BLASLONG, BLASLONG, float, float*, BLASLONG, float*, BLASLONG);
  int    (*somatcopy_k_rn)	(BLASLONG, BLASLONG, float, float*, BLASLONG, float*, BLASLONG);
  int    (*somatcopy_k_rt)	(BLASLONG, BLASLONG, float, float*, BLASLONG, float*, BLASLONG);

  int    (*domatcopy_k_cn)	(BLASLONG, BLASLONG, double, double*, BLASLONG, double*, BLASLONG);
  int    (*domatcopy_k_ct)	(BLASLONG, BLASLONG, double, double*, BLASLONG, double*, BLASLONG);
  int    (*domatcopy_k_rn)	(BLASLONG, BLASLONG, double, double*, BLASLONG, double*, BLASLONG);
  int    (*domatcopy_k_rt)	(BLASLONG, BLASLONG, double, double*, BLASLONG, double*, BLASLONG);

  int    (*comatcopy_k_cn)	(BLASLONG, BLASLONG, float, float, float*, BLASLONG, float*, BLASLONG);
  int    (*comatcopy_k_ct)	(BLASLONG, BLASLONG, float, float, float*, BLASLONG, float*, BLASLONG);
  int    (*comatcopy_k_rn)	(BLASLONG, BLASLONG, float, float, float*, BLASLONG, float*, BLASLONG);
  int    (*comatcopy_k_rt)	(BLASLONG, BLASLONG, float, float, float*, BLASLONG, float*, BLASLONG);

  int    (*comatcopy_k_cnc)	(BLASLONG, BLASLONG, float, float, float*, BLASLONG, float*, BLASLONG);
  int    (*comatcopy_k_ctc)	(BLASLONG, BLASLONG, float, float, float*, BLASLONG, float*, BLASLONG);
  int    (*comatcopy_k_rnc)	(BLASLONG, BLASLONG, float, float, float*, BLASLONG, float*, BLASLONG);
  int    (*comatcopy_k_rtc)	(BLASLONG, BLASLONG, float, float, float*, BLASLONG, float*, BLASLONG);

  int    (*zomatcopy_k_cn)	(BLASLONG, BLASLONG, double, double, double*, BLASLONG, double*, BLASLONG);
  int    (*zomatcopy_k_ct)	(BLASLONG, BLASLONG, double, double, double*, BLASLONG, double*, BLASLONG);
  int    (*zomatcopy_k_rn)	(BLASLONG, BLASLONG, double, double, double*, BLASLONG, double*, BLASLONG);
  int    (*zomatcopy_k_rt)	(BLASLONG, BLASLONG, double, double, double*, BLASLONG, double*, BLASLONG);

  int    (*zomatcopy_k_cnc)	(BLASLONG, BLASLONG, double, double, double*, BLASLONG, double*, BLASLONG);
  int    (*zomatcopy_k_ctc)	(BLASLONG, BLASLONG, double, double, double*, BLASLONG, double*, BLASLONG);
  int    (*zomatcopy_k_rnc)	(BLASLONG, BLASLONG, double, double, double*, BLASLONG, double*, BLASLONG);
  int    (*zomatcopy_k_rtc)	(BLASLONG, BLASLONG, double, double, double*, BLASLONG, double*, BLASLONG);

  int    (*simatcopy_k_cn)	(BLASLONG, BLASLONG, float, float*, BLASLONG);
  int    (*simatcopy_k_ct)	(BLASLONG, BLASLONG, float, float*, BLASLONG);
  int    (*simatcopy_k_rn)	(BLASLONG, BLASLONG, float, float*, BLASLONG);
  int    (*simatcopy_k_rt)	(BLASLONG, BLASLONG, float, float*, BLASLONG);

  int    (*dimatcopy_k_cn)	(BLASLONG, BLASLONG, double, double*, BLASLONG);
  int    (*dimatcopy_k_ct)	(BLASLONG, BLASLONG, double, double*, BLASLONG);
  int    (*dimatcopy_k_rn)	(BLASLONG, BLASLONG, double, double*, BLASLONG);
  int    (*dimatcopy_k_rt)	(BLASLONG, BLASLONG, double, double*, BLASLONG);

  int    (*cimatcopy_k_cn)	(BLASLONG, BLASLONG, float, float, float*, BLASLONG);
  int    (*cimatcopy_k_ct)	(BLASLONG, BLASLONG, float, float, float*, BLASLONG);
  int    (*cimatcopy_k_rn)	(BLASLONG, BLASLONG, float, float, float*, BLASLONG);
  int    (*cimatcopy_k_rt)	(BLASLONG, BLASLONG, float, float, float*, BLASLONG);

  int    (*cimatcopy_k_cnc)	(BLASLONG, BLASLONG, float, float, float*, BLASLONG);
  int    (*cimatcopy_k_ctc)	(BLASLONG, BLASLONG, float, float, float*, BLASLONG);
  int    (*cimatcopy_k_rnc)	(BLASLONG, BLASLONG, float, float, float*, BLASLONG);
  int    (*cimatcopy_k_rtc)	(BLASLONG, BLASLONG, float, float, float*, BLASLONG);

  int    (*zimatcopy_k_cn)	(BLASLONG, BLASLONG, double, double, double*, BLASLONG);
  int    (*zimatcopy_k_ct)	(BLASLONG, BLASLONG, double, double, double*, BLASLONG);
  int    (*zimatcopy_k_rn)	(BLASLONG, BLASLONG, double, double, double*, BLASLONG);
  int    (*zimatcopy_k_rt)	(BLASLONG, BLASLONG, double, double, double*, BLASLONG);

  int    (*zimatcopy_k_cnc)	(BLASLONG, BLASLONG, double, double, double*, BLASLONG);
  int    (*zimatcopy_k_ctc)	(BLASLONG, BLASLONG, double, double, double*, BLASLONG);
  int    (*zimatcopy_k_rnc)	(BLASLONG, BLASLONG, double, double, double*, BLASLONG);
  int    (*zimatcopy_k_rtc)	(BLASLONG, BLASLONG, double, double, double*, BLASLONG);

  int    (*sgeadd_k) (BLASLONG, BLASLONG, float, float *, BLASLONG, float, float *, BLASLONG); 
  int    (*dgeadd_k) (BLASLONG, BLASLONG, double, double *, BLASLONG, double, double *, BLASLONG); 
  int    (*cgeadd_k) (BLASLONG, BLASLONG, float, float,  float *,  BLASLONG, float, float, float *, BLASLONG); 
  int    (*zgeadd_k) (BLASLONG, BLASLONG, double, double, double *, BLASLONG, double, double, double *, BLASLONG); 

} gotoblas_t;

extern gotoblas_t *gotoblas;

#define DTB_ENTRIES  gotoblas -> dtb_entries
#define GEMM_OFFSET_A	gotoblas -> offsetA
#define GEMM_OFFSET_B	gotoblas -> offsetB
#define GEMM_ALIGN	gotoblas -> align

#define HAVE_EX_L2	gotoblas -> exclusive_cache

#define	SGEMM_P		gotoblas -> sgemm_p
#define	SGEMM_Q		gotoblas -> sgemm_q
#define	SGEMM_R		gotoblas -> sgemm_r
#define	SGEMM_UNROLL_M	gotoblas -> sgemm_unroll_m
#define	SGEMM_UNROLL_N	gotoblas -> sgemm_unroll_n
#define SGEMM_UNROLL_MN	gotoblas -> sgemm_unroll_mn

#define	DGEMM_P		gotoblas -> dgemm_p
#define	DGEMM_Q		gotoblas -> dgemm_q
#define	DGEMM_R		gotoblas -> dgemm_r
#define	DGEMM_UNROLL_M	gotoblas -> dgemm_unroll_m
#define	DGEMM_UNROLL_N	gotoblas -> dgemm_unroll_n
#define DGEMM_UNROLL_MN	gotoblas -> dgemm_unroll_mn

#define	QGEMM_P		gotoblas -> qgemm_p
#define	QGEMM_Q		gotoblas -> qgemm_q
#define	QGEMM_R		gotoblas -> qgemm_r
#define	QGEMM_UNROLL_M	gotoblas -> qgemm_unroll_m
#define	QGEMM_UNROLL_N	gotoblas -> qgemm_unroll_n
#define QGEMM_UNROLL_MN	gotoblas -> qgemm_unroll_mn

#define	CGEMM_P		gotoblas -> cgemm_p
#define	CGEMM_Q		gotoblas -> cgemm_q
#define	CGEMM_R		gotoblas -> cgemm_r
#define	CGEMM_UNROLL_M	gotoblas -> cgemm_unroll_m
#define	CGEMM_UNROLL_N	gotoblas -> cgemm_unroll_n
#define CGEMM_UNROLL_MN	gotoblas -> cgemm_unroll_mn

#define	ZGEMM_P		gotoblas -> zgemm_p
#define	ZGEMM_Q		gotoblas -> zgemm_q
#define	ZGEMM_R		gotoblas -> zgemm_r
#define	ZGEMM_UNROLL_M	gotoblas -> zgemm_unroll_m
#define	ZGEMM_UNROLL_N	gotoblas -> zgemm_unroll_n
#define ZGEMM_UNROLL_MN	gotoblas -> zgemm_unroll_mn

#define	XGEMM_P		gotoblas -> xgemm_p
#define	XGEMM_Q		gotoblas -> xgemm_q
#define	XGEMM_R		gotoblas -> xgemm_r
#define	XGEMM_UNROLL_M	gotoblas -> xgemm_unroll_m
#define	XGEMM_UNROLL_N	gotoblas -> xgemm_unroll_n
#define XGEMM_UNROLL_MN	gotoblas -> xgemm_unroll_mn

#define	CGEMM3M_P		gotoblas -> cgemm3m_p
#define	CGEMM3M_Q		gotoblas -> cgemm3m_q
#define	CGEMM3M_R		gotoblas -> cgemm3m_r
#define	CGEMM3M_UNROLL_M	gotoblas -> cgemm3m_unroll_m
#define	CGEMM3M_UNROLL_N	gotoblas -> cgemm3m_unroll_n
#define CGEMM3M_UNROLL_MN	gotoblas -> cgemm3m_unroll_mn

#define	ZGEMM3M_P		gotoblas -> zgemm3m_p
#define	ZGEMM3M_Q		gotoblas -> zgemm3m_q
#define	ZGEMM3M_R		gotoblas -> zgemm3m_r
#define	ZGEMM3M_UNROLL_M	gotoblas -> zgemm3m_unroll_m
#define	ZGEMM3M_UNROLL_N	gotoblas -> zgemm3m_unroll_n
#define ZGEMM3M_UNROLL_MN	gotoblas -> zgemm3m_unroll_mn

#define	XGEMM3M_P		gotoblas -> xgemm3m_p
#define	XGEMM3M_Q		gotoblas -> xgemm3m_q
#define	XGEMM3M_R		gotoblas -> xgemm3m_r
#define	XGEMM3M_UNROLL_M	gotoblas -> xgemm3m_unroll_m
#define	XGEMM3M_UNROLL_N	gotoblas -> xgemm3m_unroll_n
#define XGEMM3M_UNROLL_MN	gotoblas -> xgemm3m_unroll_mn

#else

#define DTB_ENTRIES  DTB_DEFAULT_ENTRIES

#define GEMM_OFFSET_A	GEMM_DEFAULT_OFFSET_A
#define GEMM_OFFSET_B	GEMM_DEFAULT_OFFSET_B
#define GEMM_ALIGN	GEMM_DEFAULT_ALIGN

#ifdef HAVE_EXCLUSIVE_CACHE
#define HAVE_EX_L2	1
#else
#define HAVE_EX_L2	0
#endif

#define	SGEMM_P		SGEMM_DEFAULT_P
#define	SGEMM_Q		SGEMM_DEFAULT_Q
#define	SGEMM_R		SGEMM_DEFAULT_R
#define SGEMM_UNROLL_M	SGEMM_DEFAULT_UNROLL_M
#define SGEMM_UNROLL_N	SGEMM_DEFAULT_UNROLL_N
#ifdef  SGEMM_DEFAULT_UNROLL_MN
#define SGEMM_UNROLL_MN	SGEMM_DEFAULT_UNROLL_MN
#else
#define SGEMM_UNROLL_MN	MAX((SGEMM_UNROLL_M), (SGEMM_UNROLL_N))
#endif

#define	DGEMM_P		DGEMM_DEFAULT_P
#define	DGEMM_Q		DGEMM_DEFAULT_Q
#define	DGEMM_R		DGEMM_DEFAULT_R
#define DGEMM_UNROLL_M	DGEMM_DEFAULT_UNROLL_M
#define DGEMM_UNROLL_N	DGEMM_DEFAULT_UNROLL_N
#ifdef  DGEMM_DEFAULT_UNROLL_MN
#define DGEMM_UNROLL_MN	DGEMM_DEFAULT_UNROLL_MN
#else
#define DGEMM_UNROLL_MN	MAX((DGEMM_UNROLL_M), (DGEMM_UNROLL_N))
#endif

#define	QGEMM_P		QGEMM_DEFAULT_P
#define	QGEMM_Q		QGEMM_DEFAULT_Q
#define	QGEMM_R		QGEMM_DEFAULT_R
#define QGEMM_UNROLL_M	QGEMM_DEFAULT_UNROLL_M
#define QGEMM_UNROLL_N	QGEMM_DEFAULT_UNROLL_N
#define QGEMM_UNROLL_MN	MAX((QGEMM_UNROLL_M), (QGEMM_UNROLL_N))

#define	CGEMM_P		CGEMM_DEFAULT_P
#define	CGEMM_Q		CGEMM_DEFAULT_Q
#define	CGEMM_R		CGEMM_DEFAULT_R
#define CGEMM_UNROLL_M	CGEMM_DEFAULT_UNROLL_M
#define CGEMM_UNROLL_N	CGEMM_DEFAULT_UNROLL_N
#ifdef  CGEMM_DEFAULT_UNROLL_MN
#define CGEMM_UNROLL_MN	CGEMM_DEFAULT_UNROLL_MN
#else
#define CGEMM_UNROLL_MN	MAX((CGEMM_UNROLL_M), (CGEMM_UNROLL_N))
#endif

#define	ZGEMM_P		ZGEMM_DEFAULT_P
#define	ZGEMM_Q		ZGEMM_DEFAULT_Q
#define	ZGEMM_R		ZGEMM_DEFAULT_R
#define ZGEMM_UNROLL_M	ZGEMM_DEFAULT_UNROLL_M
#define ZGEMM_UNROLL_N	ZGEMM_DEFAULT_UNROLL_N
#ifdef  ZGEMM_DEFAULT_UNROLL_MN
#define ZGEMM_UNROLL_MN	ZGEMM_DEFAULT_UNROLL_MN
#else
#define ZGEMM_UNROLL_MN	MAX((ZGEMM_UNROLL_M), (ZGEMM_UNROLL_N))
#endif

#define	XGEMM_P		XGEMM_DEFAULT_P
#define	XGEMM_Q		XGEMM_DEFAULT_Q
#define	XGEMM_R		XGEMM_DEFAULT_R
#define XGEMM_UNROLL_M	XGEMM_DEFAULT_UNROLL_M
#define XGEMM_UNROLL_N	XGEMM_DEFAULT_UNROLL_N
#define XGEMM_UNROLL_MN	MAX((XGEMM_UNROLL_M), (XGEMM_UNROLL_N))

#ifdef CGEMM3M_DEFAULT_UNROLL_N

#define	CGEMM3M_P		CGEMM3M_DEFAULT_P
#define	CGEMM3M_Q		CGEMM3M_DEFAULT_Q
#define	CGEMM3M_R		CGEMM3M_DEFAULT_R
#define CGEMM3M_UNROLL_M	CGEMM3M_DEFAULT_UNROLL_M
#define CGEMM3M_UNROLL_N	CGEMM3M_DEFAULT_UNROLL_N
#define CGEMM3M_UNROLL_MN	MAX((CGEMM3M_UNROLL_M), (CGEMM3M_UNROLL_N))

#else

#define	CGEMM3M_P		SGEMM_DEFAULT_P
#define	CGEMM3M_Q		SGEMM_DEFAULT_Q
#define	CGEMM3M_R		SGEMM_DEFAULT_R
#define CGEMM3M_UNROLL_M	SGEMM_DEFAULT_UNROLL_M
#define CGEMM3M_UNROLL_N	SGEMM_DEFAULT_UNROLL_N
#define CGEMM3M_UNROLL_MN	MAX((CGEMM_UNROLL_M), (CGEMM_UNROLL_N))

#endif


#ifdef ZGEMM3M_DEFAULT_UNROLL_N

#define	ZGEMM3M_P		ZGEMM3M_DEFAULT_P
#define	ZGEMM3M_Q		ZGEMM3M_DEFAULT_Q
#define	ZGEMM3M_R		ZGEMM3M_DEFAULT_R
#define ZGEMM3M_UNROLL_M	ZGEMM3M_DEFAULT_UNROLL_M
#define ZGEMM3M_UNROLL_N	ZGEMM3M_DEFAULT_UNROLL_N
#define ZGEMM3M_UNROLL_MN	MAX((ZGEMM_UNROLL_M), (ZGEMM_UNROLL_N))

#else

#define	ZGEMM3M_P		DGEMM_DEFAULT_P
#define	ZGEMM3M_Q		DGEMM_DEFAULT_Q
#define	ZGEMM3M_R		DGEMM_DEFAULT_R
#define ZGEMM3M_UNROLL_M	DGEMM_DEFAULT_UNROLL_M
#define ZGEMM3M_UNROLL_N	DGEMM_DEFAULT_UNROLL_N
#define ZGEMM3M_UNROLL_MN	MAX((ZGEMM_UNROLL_M), (ZGEMM_UNROLL_N))

#endif

#define	XGEMM3M_P		QGEMM_DEFAULT_P
#define	XGEMM3M_Q		QGEMM_DEFAULT_Q
#define	XGEMM3M_R		QGEMM_DEFAULT_R
#define XGEMM3M_UNROLL_M	QGEMM_DEFAULT_UNROLL_M
#define XGEMM3M_UNROLL_N	QGEMM_DEFAULT_UNROLL_N
#define XGEMM3M_UNROLL_MN	MAX((QGEMM_UNROLL_M), (QGEMM_UNROLL_N))


#endif
#endif

#ifndef COMPLEX
#if   defined(XDOUBLE)
#define GEMM_P			QGEMM_P
#define GEMM_Q			QGEMM_Q
#define GEMM_R			QGEMM_R
#define GEMM_UNROLL_M		QGEMM_UNROLL_M
#define GEMM_UNROLL_N		QGEMM_UNROLL_N
#define GEMM_UNROLL_MN		QGEMM_UNROLL_MN
#define GEMM_DEFAULT_P		QGEMM_DEFAULT_P
#define GEMM_DEFAULT_Q		QGEMM_DEFAULT_Q
#define GEMM_DEFAULT_R		QGEMM_DEFAULT_R
#define GEMM_DEFAULT_UNROLL_M	QGEMM_DEFAULT_UNROLL_M
#define GEMM_DEFAULT_UNROLL_N	QGEMM_DEFAULT_UNROLL_N
#elif defined(DOUBLE)
#define GEMM_P			DGEMM_P
#define GEMM_Q			DGEMM_Q
#define GEMM_R			DGEMM_R
#define GEMM_UNROLL_M		DGEMM_UNROLL_M
#define GEMM_UNROLL_N		DGEMM_UNROLL_N
#define GEMM_UNROLL_MN		DGEMM_UNROLL_MN
#define GEMM_DEFAULT_P		DGEMM_DEFAULT_P
#define GEMM_DEFAULT_Q		DGEMM_DEFAULT_Q
#define GEMM_DEFAULT_R		DGEMM_DEFAULT_R
#define GEMM_DEFAULT_UNROLL_M	DGEMM_DEFAULT_UNROLL_M
#define GEMM_DEFAULT_UNROLL_N	DGEMM_DEFAULT_UNROLL_N
#else
#define GEMM_P			SGEMM_P
#define GEMM_Q			SGEMM_Q
#define GEMM_R			SGEMM_R
#define GEMM_UNROLL_M		SGEMM_UNROLL_M
#define GEMM_UNROLL_N		SGEMM_UNROLL_N
#define GEMM_UNROLL_MN		SGEMM_UNROLL_MN
#define GEMM_DEFAULT_P		SGEMM_DEFAULT_P
#define GEMM_DEFAULT_Q		SGEMM_DEFAULT_Q
#define GEMM_DEFAULT_R		SGEMM_DEFAULT_R
#define GEMM_DEFAULT_UNROLL_M	SGEMM_DEFAULT_UNROLL_M
#define GEMM_DEFAULT_UNROLL_N	SGEMM_DEFAULT_UNROLL_N
#endif
#else
#if   defined(XDOUBLE)
#define GEMM_P			XGEMM_P
#define GEMM_Q			XGEMM_Q
#define GEMM_R			XGEMM_R
#define GEMM_UNROLL_M		XGEMM_UNROLL_M
#define GEMM_UNROLL_N		XGEMM_UNROLL_N
#define GEMM_UNROLL_MN		XGEMM_UNROLL_MN
#define GEMM_DEFAULT_P		XGEMM_DEFAULT_P
#define GEMM_DEFAULT_Q		XGEMM_DEFAULT_Q
#define GEMM_DEFAULT_R		XGEMM_DEFAULT_R
#define GEMM_DEFAULT_UNROLL_M	XGEMM_DEFAULT_UNROLL_M
#define GEMM_DEFAULT_UNROLL_N	XGEMM_DEFAULT_UNROLL_N
#elif defined(DOUBLE)
#define GEMM_P			ZGEMM_P
#define GEMM_Q			ZGEMM_Q
#define GEMM_R			ZGEMM_R
#define GEMM_UNROLL_M		ZGEMM_UNROLL_M
#define GEMM_UNROLL_N		ZGEMM_UNROLL_N
#define GEMM_UNROLL_MN		ZGEMM_UNROLL_MN
#define GEMM_DEFAULT_P		ZGEMM_DEFAULT_P
#define GEMM_DEFAULT_Q		ZGEMM_DEFAULT_Q
#define GEMM_DEFAULT_R		ZGEMM_DEFAULT_R
#define GEMM_DEFAULT_UNROLL_M	ZGEMM_DEFAULT_UNROLL_M
#define GEMM_DEFAULT_UNROLL_N	ZGEMM_DEFAULT_UNROLL_N
#else
#define GEMM_P			CGEMM_P
#define GEMM_Q			CGEMM_Q
#define GEMM_R			CGEMM_R
#define GEMM_UNROLL_M		CGEMM_UNROLL_M
#define GEMM_UNROLL_N		CGEMM_UNROLL_N
#define GEMM_UNROLL_MN		CGEMM_UNROLL_MN
#define GEMM_DEFAULT_P		CGEMM_DEFAULT_P
#define GEMM_DEFAULT_Q		CGEMM_DEFAULT_Q
#define GEMM_DEFAULT_R		CGEMM_DEFAULT_R
#define GEMM_DEFAULT_UNROLL_M	CGEMM_DEFAULT_UNROLL_M
#define GEMM_DEFAULT_UNROLL_N	CGEMM_DEFAULT_UNROLL_N
#endif
#endif

#ifdef XDOUBLE
#define GEMM3M_UNROLL_M	XGEMM3M_UNROLL_M
#define GEMM3M_UNROLL_N	XGEMM3M_UNROLL_N
#elif defined(DOUBLE)
#define GEMM3M_UNROLL_M	ZGEMM3M_UNROLL_M
#define GEMM3M_UNROLL_N	ZGEMM3M_UNROLL_N
#else
#define GEMM3M_UNROLL_M	CGEMM3M_UNROLL_M
#define GEMM3M_UNROLL_N	CGEMM3M_UNROLL_N
#endif


#ifndef QGEMM_DEFAULT_UNROLL_M
#define QGEMM_DEFAULT_UNROLL_M 2
#endif

#ifndef QGEMM_DEFAULT_UNROLL_N
#define QGEMM_DEFAULT_UNROLL_N 2
#endif

#ifndef XGEMM_DEFAULT_UNROLL_M
#define XGEMM_DEFAULT_UNROLL_M 2
#endif

#ifndef XGEMM_DEFAULT_UNROLL_N
#define XGEMM_DEFAULT_UNROLL_N 2
#endif

#ifndef GEMM_THREAD
#define GEMM_THREAD gemm_thread_n
#endif

#ifndef SGEMM_DEFAULT_R
#define SGEMM_DEFAULT_R (((BUFFER_SIZE - ((SGEMM_DEFAULT_P * SGEMM_DEFAULT_Q *  4 + GEMM_DEFAULT_OFFSET_A + GEMM_DEFAULT_ALIGN) & ~GEMM_DEFAULT_ALIGN)) / (SGEMM_DEFAULT_Q *  4) - 15) & ~15)
#endif

#ifndef DGEMM_DEFAULT_R
#define DGEMM_DEFAULT_R (((BUFFER_SIZE - ((DGEMM_DEFAULT_P * DGEMM_DEFAULT_Q *  8 + GEMM_DEFAULT_OFFSET_A + GEMM_DEFAULT_ALIGN) & ~GEMM_DEFAULT_ALIGN)) / (DGEMM_DEFAULT_Q *  8) - 15) & ~15)
#endif

#ifndef QGEMM_DEFAULT_R
#define QGEMM_DEFAULT_R (((BUFFER_SIZE - ((QGEMM_DEFAULT_P * QGEMM_DEFAULT_Q * 16 + GEMM_DEFAULT_OFFSET_A + GEMM_DEFAULT_ALIGN) & ~GEMM_DEFAULT_ALIGN)) / (QGEMM_DEFAULT_Q * 16) - 15) & ~15)
#endif

#ifndef CGEMM_DEFAULT_R
#define CGEMM_DEFAULT_R (((BUFFER_SIZE - ((CGEMM_DEFAULT_P * CGEMM_DEFAULT_Q *  8 + GEMM_DEFAULT_OFFSET_A + GEMM_DEFAULT_ALIGN) & ~GEMM_DEFAULT_ALIGN)) / (CGEMM_DEFAULT_Q *  8) - 15) & ~15)
#endif

#ifndef ZGEMM_DEFAULT_R
#define ZGEMM_DEFAULT_R (((BUFFER_SIZE - ((ZGEMM_DEFAULT_P * ZGEMM_DEFAULT_Q * 16 + GEMM_DEFAULT_OFFSET_A + GEMM_DEFAULT_ALIGN) & ~GEMM_DEFAULT_ALIGN)) / (ZGEMM_DEFAULT_Q * 16) - 15) & ~15)
#endif

#ifndef XGEMM_DEFAULT_R
#define XGEMM_DEFAULT_R (((BUFFER_SIZE - ((XGEMM_DEFAULT_P * XGEMM_DEFAULT_Q * 32 + GEMM_DEFAULT_OFFSET_A + GEMM_DEFAULT_ALIGN) & ~GEMM_DEFAULT_ALIGN)) / (XGEMM_DEFAULT_Q * 32) - 15) & ~15)
#endif

#ifndef SNUMOPT
#define SNUMOPT		2
#endif

#ifndef DNUMOPT
#define DNUMOPT		2
#endif

#ifndef QNUMOPT
#define QNUMOPT		1
#endif

#ifndef GEMM3M_P
#ifdef XDOUBLE
#define GEMM3M_P	XGEMM3M_P
#elif defined(DOUBLE)
#define GEMM3M_P	ZGEMM3M_P
#else
#define GEMM3M_P	CGEMM3M_P
#endif
#endif

#ifndef GEMM3M_Q
#ifdef XDOUBLE
#define GEMM3M_Q	XGEMM3M_Q
#elif defined(DOUBLE)
#define GEMM3M_Q	ZGEMM3M_Q
#else
#define GEMM3M_Q	CGEMM3M_Q
#endif
#endif

#ifndef GEMM3M_R
#ifdef XDOUBLE
#define GEMM3M_R	XGEMM3M_R
#elif defined(DOUBLE)
#define GEMM3M_R	ZGEMM3M_R
#else
#define GEMM3M_R	CGEMM3M_R
#endif
#endif


#endif
