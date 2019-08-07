/*****************************************************************************
  Copyright (c) 2014, Intel Corp.
  All rights reserved.

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are met:

    * Redistributions of source code must retain the above copyright notice,
      this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in the
      documentation and/or other materials provided with the distribution.
    * Neither the name of Intel Corporation nor the names of its contributors
      may be used to endorse or promote products derived from this software
      without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
  AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
  IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
  ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
  LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
  SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
  INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
  CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
  ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
  THE POSSIBILITY OF SUCH DAMAGE.
******************************************************************************
* Contents: Native C interface to LAPACK utility functions
* Author: Intel Corporation
* Created in January, 2010
*****************************************************************************/

#ifndef _LAPACKE_UTILS_H_
#define _LAPACKE_UTILS_H_

#include "lapacke.h"

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

#ifndef ABS
#define ABS(x) (((x) < 0) ? -(x) : (x))
#endif
#ifndef MAX
#define MAX(x,y) (((x) > (y)) ? (x) : (y))
#endif
#ifndef MIN
#define MIN(x,y) (((x) < (y)) ? (x) : (y))
#endif
#ifndef MAX3
#define MAX3(x,y,z) (((x) > MAX(y,z)) ? (x) : MAX(y,z))
#endif
#ifndef MIN3
#define MIN3(x,y,z) (((x) < MIN(y,z)) ? (x) : MIN(y,z))
#endif

#define IS_S_NONZERO(x) ( (x) < 0 || (x) > 0 )
#define IS_D_NONZERO(x) ( (x) < 0 || (x) > 0 )
#define IS_C_NONZERO(x) ( IS_S_NONZERO(*((float*)&x)) ||  \
                          IS_S_NONZERO(*(((float*)&x)+1)) )
#define IS_Z_NONZERO(x) ( IS_D_NONZERO(*((double*)&x)) || \
                          IS_D_NONZERO(*(((double*)&x)+1)) )

/* Error handler */
void LAPACKE_xerbla( const char *name, lapack_int info );

/* Compare two chars (case-insensitive) */
lapack_logical LAPACKE_lsame( char ca,  char cb );

/* Functions to convert column-major to row-major 2d arrays and vice versa. */
void LAPACKE_cgb_trans( int matrix_layout, lapack_int m, lapack_int n,
                        lapack_int kl, lapack_int ku,
                        const lapack_complex_float *in, lapack_int ldin,
                        lapack_complex_float *out, lapack_int ldout );
void LAPACKE_cge_trans( int matrix_layout, lapack_int m, lapack_int n,
                        const lapack_complex_float* in, lapack_int ldin,
                        lapack_complex_float* out, lapack_int ldout );
void LAPACKE_cgg_trans( int matrix_layout, lapack_int m, lapack_int n,
                        const lapack_complex_float* in, lapack_int ldin,
                        lapack_complex_float* out, lapack_int ldout );
void LAPACKE_chb_trans( int matrix_layout, char uplo, lapack_int n,
                        lapack_int kd,
                        const lapack_complex_float *in, lapack_int ldin,
                        lapack_complex_float *out, lapack_int ldout );
void LAPACKE_che_trans( int matrix_layout, char uplo, lapack_int n,
                        const lapack_complex_float *in, lapack_int ldin,
                        lapack_complex_float *out, lapack_int ldout );
void LAPACKE_chp_trans( int matrix_layout, char uplo, lapack_int n,
                        const lapack_complex_float *in,
                        lapack_complex_float *out );
void LAPACKE_chs_trans( int matrix_layout, lapack_int n,
                        const lapack_complex_float *in, lapack_int ldin,
                        lapack_complex_float *out, lapack_int ldout );
void LAPACKE_cpb_trans( int matrix_layout, char uplo, lapack_int n,
                        lapack_int kd,
                        const lapack_complex_float *in, lapack_int ldin,
                        lapack_complex_float *out, lapack_int ldout );
void LAPACKE_cpf_trans( int matrix_layout, char transr, char uplo,
                        lapack_int n, const lapack_complex_float *in,
                        lapack_complex_float *out );
void LAPACKE_cpo_trans( int matrix_layout, char uplo, lapack_int n,
                        const lapack_complex_float *in, lapack_int ldin,
                        lapack_complex_float *out, lapack_int ldout );
void LAPACKE_cpp_trans( int matrix_layout, char uplo, lapack_int n,
                        const lapack_complex_float *in,
                        lapack_complex_float *out );
void LAPACKE_csp_trans( int matrix_layout, char uplo, lapack_int n,
                        const lapack_complex_float *in,
                        lapack_complex_float *out );
void LAPACKE_csy_trans( int matrix_layout, char uplo, lapack_int n,
                        const lapack_complex_float *in, lapack_int ldin,
                        lapack_complex_float *out, lapack_int ldout );
void LAPACKE_ctb_trans( int matrix_layout, char uplo, char diag,
                        lapack_int n, lapack_int kd,
                        const lapack_complex_float *in, lapack_int ldin,
                        lapack_complex_float *out, lapack_int ldout );
void LAPACKE_ctf_trans( int matrix_layout, char transr, char uplo, char diag,
                        lapack_int n, const lapack_complex_float *in,
                        lapack_complex_float *out );
void LAPACKE_ctp_trans( int matrix_layout, char uplo, char diag,
                        lapack_int n, const lapack_complex_float *in,
                        lapack_complex_float *out );
void LAPACKE_ctr_trans( int matrix_layout, char uplo, char diag, lapack_int n,
                        const lapack_complex_float *in, lapack_int ldin,
                        lapack_complex_float *out, lapack_int ldout );

void LAPACKE_dgb_trans( int matrix_layout, lapack_int m, lapack_int n,
                        lapack_int kl, lapack_int ku,
                        const double *in, lapack_int ldin,
                        double *out, lapack_int ldout );
void LAPACKE_dge_trans( int matrix_layout, lapack_int m, lapack_int n,
                        const double* in, lapack_int ldin,
                        double* out, lapack_int ldout );
void LAPACKE_dgg_trans( int matrix_layout, lapack_int m, lapack_int n,
                        const double* in, lapack_int ldin,
                        double* out, lapack_int ldout );
void LAPACKE_dhs_trans( int matrix_layout, lapack_int n,
                        const double *in, lapack_int ldin,
                        double *out, lapack_int ldout );
void LAPACKE_dpb_trans( int matrix_layout, char uplo, lapack_int n,
                        lapack_int kd,
                        const double *in, lapack_int ldin,
                        double *out, lapack_int ldout );
void LAPACKE_dpf_trans( int matrix_layout, char transr, char uplo,
                        lapack_int n, const double *in,
                        double *out );
void LAPACKE_dpo_trans( int matrix_layout, char uplo, lapack_int n,
                        const double *in, lapack_int ldin,
                        double *out, lapack_int ldout );
void LAPACKE_dpp_trans( int matrix_layout, char uplo, lapack_int n,
                        const double *in,
                        double *out );
void LAPACKE_dsb_trans( int matrix_layout, char uplo, lapack_int n,
                        lapack_int kd,
                        const double *in, lapack_int ldin,
                        double *out, lapack_int ldout );
void LAPACKE_dsp_trans( int matrix_layout, char uplo, lapack_int n,
                        const double *in,
                        double *out );
void LAPACKE_dsy_trans( int matrix_layout, char uplo, lapack_int n,
                        const double *in, lapack_int ldin,
                        double *out, lapack_int ldout );
void LAPACKE_dtb_trans( int matrix_layout, char uplo, char diag,
                        lapack_int n, lapack_int kd,
                        const double *in, lapack_int ldin,
                        double *out, lapack_int ldout );
void LAPACKE_dtf_trans( int matrix_layout, char transr, char uplo, char diag,
                        lapack_int n, const double *in,
                        double *out );
void LAPACKE_dtp_trans( int matrix_layout, char uplo, char diag,
                        lapack_int n, const double *in,
                        double *out );
void LAPACKE_dtr_trans( int matrix_layout, char uplo, char diag, lapack_int n,
                        const double *in, lapack_int ldin,
                        double *out, lapack_int ldout );

void LAPACKE_sgb_trans( int matrix_layout, lapack_int m, lapack_int n,
                        lapack_int kl, lapack_int ku,
                        const float *in, lapack_int ldin,
                        float *out, lapack_int ldout );
void LAPACKE_sge_trans( int matrix_layout, lapack_int m, lapack_int n,
                        const float* in, lapack_int ldin,
                        float* out, lapack_int ldout );
void LAPACKE_sgg_trans( int matrix_layout, lapack_int m, lapack_int n,
                        const float* in, lapack_int ldin,
                        float* out, lapack_int ldout );
void LAPACKE_shs_trans( int matrix_layout, lapack_int n,
                        const float *in, lapack_int ldin,
                        float *out, lapack_int ldout );
void LAPACKE_spb_trans( int matrix_layout, char uplo, lapack_int n,
                        lapack_int kd,
                        const float *in, lapack_int ldin,
                        float *out, lapack_int ldout );
void LAPACKE_spf_trans( int matrix_layout, char transr, char uplo,
                        lapack_int n, const float *in,
                        float *out );
void LAPACKE_spo_trans( int matrix_layout, char uplo, lapack_int n,
                        const float *in, lapack_int ldin,
                        float *out, lapack_int ldout );
void LAPACKE_spp_trans( int matrix_layout, char uplo, lapack_int n,
                        const float *in,
                        float *out );
void LAPACKE_ssb_trans( int matrix_layout, char uplo, lapack_int n,
                        lapack_int kd,
                        const float *in, lapack_int ldin,
                        float *out, lapack_int ldout );
void LAPACKE_ssp_trans( int matrix_layout, char uplo, lapack_int n,
                        const float *in,
                        float *out );
void LAPACKE_ssy_trans( int matrix_layout, char uplo, lapack_int n,
                        const float *in, lapack_int ldin,
                        float *out, lapack_int ldout );
void LAPACKE_stb_trans( int matrix_layout, char uplo, char diag,
                        lapack_int n, lapack_int kd,
                        const float *in, lapack_int ldin,
                        float *out, lapack_int ldout );
void LAPACKE_stf_trans( int matrix_layout, char transr, char uplo, char diag,
                        lapack_int n, const float *in,
                        float *out );
void LAPACKE_stp_trans( int matrix_layout, char uplo, char diag,
                        lapack_int n, const float *in,
                        float *out );
void LAPACKE_str_trans( int matrix_layout, char uplo, char diag, lapack_int n,
                        const float *in, lapack_int ldin,
                        float *out, lapack_int ldout );

void LAPACKE_zgb_trans( int matrix_layout, lapack_int m, lapack_int n,
                        lapack_int kl, lapack_int ku,
                        const lapack_complex_double *in, lapack_int ldin,
                        lapack_complex_double *out, lapack_int ldout );
void LAPACKE_zge_trans( int matrix_layout, lapack_int m, lapack_int n,
                        const lapack_complex_double* in, lapack_int ldin,
                        lapack_complex_double* out, lapack_int ldout );
void LAPACKE_zgg_trans( int matrix_layout, lapack_int m, lapack_int n,
                        const lapack_complex_double* in, lapack_int ldin,
                        lapack_complex_double* out, lapack_int ldout );
void LAPACKE_zhb_trans( int matrix_layout, char uplo, lapack_int n,
                        lapack_int kd,
                        const lapack_complex_double *in, lapack_int ldin,
                        lapack_complex_double *out, lapack_int ldout );
void LAPACKE_zhe_trans( int matrix_layout, char uplo, lapack_int n,
                        const lapack_complex_double *in, lapack_int ldin,
                        lapack_complex_double *out, lapack_int ldout );
void LAPACKE_zhp_trans( int matrix_layout, char uplo, lapack_int n,
                        const lapack_complex_double *in,
                        lapack_complex_double *out );
void LAPACKE_zhs_trans( int matrix_layout, lapack_int n,
                        const lapack_complex_double *in, lapack_int ldin,
                        lapack_complex_double *out, lapack_int ldout );
void LAPACKE_zpb_trans( int matrix_layout, char uplo, lapack_int n,
                        lapack_int kd,
                        const lapack_complex_double *in, lapack_int ldin,
                        lapack_complex_double *out, lapack_int ldout );
void LAPACKE_zpf_trans( int matrix_layout, char transr, char uplo,
                        lapack_int n, const lapack_complex_double *in,
                        lapack_complex_double *out );
void LAPACKE_zpo_trans( int matrix_layout, char uplo, lapack_int n,
                        const lapack_complex_double *in, lapack_int ldin,
                        lapack_complex_double *out, lapack_int ldout );
void LAPACKE_zpp_trans( int matrix_layout, char uplo, lapack_int n,
                        const lapack_complex_double *in,
                        lapack_complex_double *out );
void LAPACKE_zsp_trans( int matrix_layout, char uplo, lapack_int n,
                        const lapack_complex_double *in,
                        lapack_complex_double *out );
void LAPACKE_zsy_trans( int matrix_layout, char uplo, lapack_int n,
                        const lapack_complex_double *in, lapack_int ldin,
                        lapack_complex_double *out, lapack_int ldout );
void LAPACKE_ztb_trans( int matrix_layout, char uplo, char diag,
                        lapack_int n, lapack_int kd,
                        const lapack_complex_double *in, lapack_int ldin,
                        lapack_complex_double *out, lapack_int ldout );
void LAPACKE_ztf_trans( int matrix_layout, char transr, char uplo, char diag,
                        lapack_int n, const lapack_complex_double *in,
                        lapack_complex_double *out );
void LAPACKE_ztp_trans( int matrix_layout, char uplo, char diag,
                        lapack_int n, const lapack_complex_double *in,
                        lapack_complex_double *out );
void LAPACKE_ztr_trans( int matrix_layout, char uplo, char diag, lapack_int n,
                        const lapack_complex_double *in, lapack_int ldin,
                        lapack_complex_double *out, lapack_int ldout );

/* NaN checkers */
#define LAPACK_SISNAN( x ) ( x != x )
#define LAPACK_DISNAN( x ) ( x != x )
#define LAPACK_CISNAN( x ) ( LAPACK_SISNAN(*((float*) &x)) || \
                              LAPACK_SISNAN(*(((float*) &x)+1)) )
#define LAPACK_ZISNAN( x ) ( LAPACK_DISNAN(*((double*)&x)) || \
                              LAPACK_DISNAN(*(((double*)&x)+1)) )

/* NaN checkers for vectors */
lapack_logical LAPACKE_c_nancheck( lapack_int n,
                                    const lapack_complex_float *x,
                                    lapack_int incx );
lapack_logical LAPACKE_d_nancheck( lapack_int n,
                                    const double *x,
                                    lapack_int incx );
lapack_logical LAPACKE_s_nancheck( lapack_int n,
                                    const float *x,
                                    lapack_int incx );
lapack_logical LAPACKE_z_nancheck( lapack_int n,
                                    const lapack_complex_double *x,
                                    lapack_int incx );
/* NaN checkers for matrices */
lapack_logical LAPACKE_cgb_nancheck( int matrix_layout, lapack_int m,
                                      lapack_int n, lapack_int kl,
                                      lapack_int ku,
                                      const lapack_complex_float *ab,
                                      lapack_int ldab );
lapack_logical LAPACKE_cge_nancheck( int matrix_layout, lapack_int m,
                                      lapack_int n,
                                      const lapack_complex_float *a,
                                      lapack_int lda );
lapack_logical LAPACKE_cgg_nancheck( int matrix_layout, lapack_int m,
                                      lapack_int n,
                                      const lapack_complex_float *a,
                                      lapack_int lda );
lapack_logical LAPACKE_cgt_nancheck( lapack_int n,
                                      const lapack_complex_float *dl,
                                      const lapack_complex_float *d,
                                      const lapack_complex_float *du );
lapack_logical LAPACKE_chb_nancheck( int matrix_layout, char uplo,
                                      lapack_int n, lapack_int kd,
                                      const lapack_complex_float* ab,
                                      lapack_int ldab );
lapack_logical LAPACKE_che_nancheck( int matrix_layout, char uplo,
                                      lapack_int n,
                                      const lapack_complex_float *a,
                                      lapack_int lda );
lapack_logical LAPACKE_chp_nancheck( lapack_int n,
                                      const lapack_complex_float *ap );
lapack_logical LAPACKE_chs_nancheck( int matrix_layout, lapack_int n,
                                      const lapack_complex_float *a,
                                      lapack_int lda );
lapack_logical LAPACKE_cpb_nancheck( int matrix_layout, char uplo,
                                      lapack_int n, lapack_int kd,
                                      const lapack_complex_float* ab,
                                      lapack_int ldab );
lapack_logical LAPACKE_cpf_nancheck( lapack_int n,
                                      const lapack_complex_float *a );
lapack_logical LAPACKE_cpo_nancheck( int matrix_layout, char uplo,
                                      lapack_int n,
                                      const lapack_complex_float *a,
                                      lapack_int lda );
lapack_logical LAPACKE_cpp_nancheck( lapack_int n,
                                      const lapack_complex_float *ap );
lapack_logical LAPACKE_cpt_nancheck( lapack_int n,
                                      const float *d,
                                      const lapack_complex_float *e );
lapack_logical LAPACKE_csp_nancheck( lapack_int n,
                                      const lapack_complex_float *ap );
lapack_logical LAPACKE_cst_nancheck( lapack_int n,
                                      const lapack_complex_float *d,
                                      const lapack_complex_float *e );
lapack_logical LAPACKE_csy_nancheck( int matrix_layout, char uplo,
                                      lapack_int n,
                                      const lapack_complex_float *a,
                                      lapack_int lda );
lapack_logical LAPACKE_ctb_nancheck( int matrix_layout, char uplo, char diag,
                                      lapack_int n, lapack_int kd,
                                      const lapack_complex_float* ab,
                                      lapack_int ldab );
lapack_logical LAPACKE_ctf_nancheck( int matrix_layout, char transr,
                                      char uplo, char diag,
                                      lapack_int n,
                                      const lapack_complex_float *a );
lapack_logical LAPACKE_ctp_nancheck( int matrix_layout, char uplo, char diag,
                                      lapack_int n,
                                      const lapack_complex_float *ap );
lapack_logical LAPACKE_ctr_nancheck( int matrix_layout, char uplo, char diag,
                                      lapack_int n,
                                      const lapack_complex_float *a,
                                      lapack_int lda );

lapack_logical LAPACKE_dgb_nancheck( int matrix_layout, lapack_int m,
                                      lapack_int n, lapack_int kl,
                                      lapack_int ku,
                                      const double *ab,
                                      lapack_int ldab );
lapack_logical LAPACKE_dge_nancheck( int matrix_layout, lapack_int m,
                                      lapack_int n,
                                      const double *a,
                                      lapack_int lda );
lapack_logical LAPACKE_dgg_nancheck( int matrix_layout, lapack_int m,
                                      lapack_int n,
                                      const double *a,
                                      lapack_int lda );
lapack_logical LAPACKE_dgt_nancheck( lapack_int n,
                                      const double *dl,
                                      const double *d,
                                      const double *du );
lapack_logical LAPACKE_dhs_nancheck( int matrix_layout, lapack_int n,
                                      const double *a,
                                      lapack_int lda );
lapack_logical LAPACKE_dpb_nancheck( int matrix_layout, char uplo,
                                      lapack_int n, lapack_int kd,
                                      const double* ab,
                                      lapack_int ldab );
lapack_logical LAPACKE_dpf_nancheck( lapack_int n,
                                      const double *a );
lapack_logical LAPACKE_dpo_nancheck( int matrix_layout, char uplo,
                                      lapack_int n,
                                      const double *a,
                                      lapack_int lda );
lapack_logical LAPACKE_dpp_nancheck( lapack_int n,
                                      const double *ap );
lapack_logical LAPACKE_dpt_nancheck( lapack_int n,
                                      const double *d,
                                      const double *e );
lapack_logical LAPACKE_dsb_nancheck( int matrix_layout, char uplo,
                                      lapack_int n, lapack_int kd,
                                      const double* ab,
                                      lapack_int ldab );
lapack_logical LAPACKE_dsp_nancheck( lapack_int n,
                                      const double *ap );
lapack_logical LAPACKE_dst_nancheck( lapack_int n,
                                      const double *d,
                                      const double *e );
lapack_logical LAPACKE_dsy_nancheck( int matrix_layout, char uplo,
                                      lapack_int n,
                                      const double *a,
                                      lapack_int lda );
lapack_logical LAPACKE_dtb_nancheck( int matrix_layout, char uplo, char diag,
                                      lapack_int n, lapack_int kd,
                                      const double* ab,
                                      lapack_int ldab );
lapack_logical LAPACKE_dtf_nancheck( int matrix_layout, char transr,
                                      char uplo, char diag,
                                      lapack_int n,
                                      const double *a );
lapack_logical LAPACKE_dtp_nancheck( int matrix_layout, char uplo, char diag,
                                      lapack_int n,
                                      const double *ap );
lapack_logical LAPACKE_dtr_nancheck( int matrix_layout, char uplo, char diag,
                                      lapack_int n,
                                      const double *a,
                                      lapack_int lda );

lapack_logical LAPACKE_sgb_nancheck( int matrix_layout, lapack_int m,
                                      lapack_int n, lapack_int kl,
                                      lapack_int ku,
                                      const float *ab,
                                      lapack_int ldab );
lapack_logical LAPACKE_sge_nancheck( int matrix_layout, lapack_int m,
                                      lapack_int n,
                                      const float *a,
                                      lapack_int lda );
lapack_logical LAPACKE_sgg_nancheck( int matrix_layout, lapack_int m,
                                      lapack_int n,
                                      const float *a,
                                      lapack_int lda );
lapack_logical LAPACKE_sgt_nancheck( lapack_int n,
                                      const float *dl,
                                      const float *d,
                                      const float *du );
lapack_logical LAPACKE_shs_nancheck( int matrix_layout, lapack_int n,
                                      const float *a,
                                      lapack_int lda );
lapack_logical LAPACKE_spb_nancheck( int matrix_layout, char uplo,
                                      lapack_int n, lapack_int kd,
                                      const float* ab,
                                      lapack_int ldab );
lapack_logical LAPACKE_spf_nancheck( lapack_int n,
                                      const float *a );
lapack_logical LAPACKE_spo_nancheck( int matrix_layout, char uplo,
                                      lapack_int n,
                                      const float *a,
                                      lapack_int lda );
lapack_logical LAPACKE_spp_nancheck( lapack_int n,
                                      const float *ap );
lapack_logical LAPACKE_spt_nancheck( lapack_int n,
                                      const float *d,
                                      const float *e );
lapack_logical LAPACKE_ssb_nancheck( int matrix_layout, char uplo,
                                      lapack_int n, lapack_int kd,
                                      const float* ab,
                                      lapack_int ldab );
lapack_logical LAPACKE_ssp_nancheck( lapack_int n,
                                      const float *ap );
lapack_logical LAPACKE_sst_nancheck( lapack_int n,
                                      const float *d,
                                      const float *e );
lapack_logical LAPACKE_ssy_nancheck( int matrix_layout, char uplo,
                                      lapack_int n,
                                      const float *a,
                                      lapack_int lda );
lapack_logical LAPACKE_stb_nancheck( int matrix_layout, char uplo, char diag,
                                      lapack_int n, lapack_int kd,
                                      const float* ab,
                                      lapack_int ldab );
lapack_logical LAPACKE_stf_nancheck( int matrix_layout, char transr,
                                      char uplo, char diag,
                                      lapack_int n,
                                      const float *a );
lapack_logical LAPACKE_stp_nancheck( int matrix_layout, char uplo, char diag,
                                      lapack_int n,
                                      const float *ap );
lapack_logical LAPACKE_str_nancheck( int matrix_layout, char uplo, char diag,
                                      lapack_int n,
                                      const float *a,
                                      lapack_int lda );

lapack_logical LAPACKE_zgb_nancheck( int matrix_layout, lapack_int m,
                                      lapack_int n, lapack_int kl,
                                      lapack_int ku,
                                      const lapack_complex_double *ab,
                                      lapack_int ldab );
lapack_logical LAPACKE_zge_nancheck( int matrix_layout, lapack_int m,
                                      lapack_int n,
                                      const lapack_complex_double *a,
                                      lapack_int lda );
lapack_logical LAPACKE_zgg_nancheck( int matrix_layout, lapack_int m,
                                      lapack_int n,
                                      const lapack_complex_double *a,
                                      lapack_int lda );
lapack_logical LAPACKE_zgt_nancheck( lapack_int n,
                                      const lapack_complex_double *dl,
                                      const lapack_complex_double *d,
                                      const lapack_complex_double *du );
lapack_logical LAPACKE_zhb_nancheck( int matrix_layout, char uplo,
                                      lapack_int n, lapack_int kd,
                                      const lapack_complex_double* ab,
                                      lapack_int ldab );
lapack_logical LAPACKE_zhe_nancheck( int matrix_layout, char uplo,
                                      lapack_int n,
                                      const lapack_complex_double *a,
                                      lapack_int lda );
lapack_logical LAPACKE_zhp_nancheck( lapack_int n,
                                      const lapack_complex_double *ap );
lapack_logical LAPACKE_zhs_nancheck( int matrix_layout, lapack_int n,
                                      const lapack_complex_double *a,
                                      lapack_int lda );
lapack_logical LAPACKE_zpb_nancheck( int matrix_layout, char uplo,
                                      lapack_int n, lapack_int kd,
                                      const lapack_complex_double* ab,
                                      lapack_int ldab );
lapack_logical LAPACKE_zpf_nancheck( lapack_int n,
                                      const lapack_complex_double *a );
lapack_logical LAPACKE_zpo_nancheck( int matrix_layout, char uplo,
                                      lapack_int n,
                                      const lapack_complex_double *a,
                                      lapack_int lda );
lapack_logical LAPACKE_zpp_nancheck( lapack_int n,
                                      const lapack_complex_double *ap );
lapack_logical LAPACKE_zpt_nancheck( lapack_int n,
                                      const double *d,
                                      const lapack_complex_double *e );
lapack_logical LAPACKE_zsp_nancheck( lapack_int n,
                                      const lapack_complex_double *ap );
lapack_logical LAPACKE_zst_nancheck( lapack_int n,
                                      const lapack_complex_double *d,
                                      const lapack_complex_double *e );
lapack_logical LAPACKE_zsy_nancheck( int matrix_layout, char uplo,
                                      lapack_int n,
                                      const lapack_complex_double *a,
                                      lapack_int lda );
lapack_logical LAPACKE_ztb_nancheck( int matrix_layout, char uplo, char diag,
                                      lapack_int n, lapack_int kd,
                                      const lapack_complex_double* ab,
                                      lapack_int ldab );
lapack_logical LAPACKE_ztf_nancheck( int matrix_layout, char transr,
                                      char uplo, char diag,
                                      lapack_int n,
                                      const lapack_complex_double *a );
lapack_logical LAPACKE_ztp_nancheck( int matrix_layout, char uplo, char diag,
                                      lapack_int n,
                                      const lapack_complex_double *ap );
lapack_logical LAPACKE_ztr_nancheck( int matrix_layout, char uplo, char diag,
                                      lapack_int n,
                                      const lapack_complex_double *a,
                                      lapack_int lda );

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif  /* _LAPACKE_UTILS_H_ */
