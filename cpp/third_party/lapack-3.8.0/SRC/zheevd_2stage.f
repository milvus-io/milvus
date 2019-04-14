*> \brief <b> ZHEEVD_2STAGE computes the eigenvalues and, optionally, the left and/or right eigenvectors for HE matrices</b>
*
*  @precisions fortran z -> s d c
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZHEEVD_2STAGE + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zheevd_2stage.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zheevd_2stage.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zheevd_2stage.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZHEEVD_2STAGE( JOBZ, UPLO, N, A, LDA, W, WORK, LWORK,
*                          RWORK, LRWORK, IWORK, LIWORK, INFO )
*
*       IMPLICIT NONE
*
*       .. Scalar Arguments ..
*       CHARACTER          JOBZ, UPLO
*       INTEGER            INFO, LDA, LIWORK, LRWORK, LWORK, N
*       ..
*       .. Array Arguments ..
*       INTEGER            IWORK( * )
*       DOUBLE PRECISION   RWORK( * ), W( * )
*       COMPLEX*16         A( LDA, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZHEEVD_2STAGE computes all eigenvalues and, optionally, eigenvectors of a
*> complex Hermitian matrix A using the 2stage technique for
*> the reduction to tridiagonal.  If eigenvectors are desired, it uses a
*> divide and conquer algorithm.
*>
*> The divide and conquer algorithm makes very mild assumptions about
*> floating point arithmetic. It will work on machines with a guard
*> digit in add/subtract, or on those binary machines without guard
*> digits which subtract like the Cray X-MP, Cray Y-MP, Cray C-90, or
*> Cray-2. It could conceivably fail on hexadecimal or decimal machines
*> without guard digits, but we know of none.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] JOBZ
*> \verbatim
*>          JOBZ is CHARACTER*1
*>          = 'N':  Compute eigenvalues only;
*>          = 'V':  Compute eigenvalues and eigenvectors.
*>                  Not available in this release.
*> \endverbatim
*>
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          = 'U':  Upper triangle of A is stored;
*>          = 'L':  Lower triangle of A is stored.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA, N)
*>          On entry, the Hermitian matrix A.  If UPLO = 'U', the
*>          leading N-by-N upper triangular part of A contains the
*>          upper triangular part of the matrix A.  If UPLO = 'L',
*>          the leading N-by-N lower triangular part of A contains
*>          the lower triangular part of the matrix A.
*>          On exit, if JOBZ = 'V', then if INFO = 0, A contains the
*>          orthonormal eigenvectors of the matrix A.
*>          If JOBZ = 'N', then on exit the lower triangle (if UPLO='L')
*>          or the upper triangle (if UPLO='U') of A, including the
*>          diagonal, is destroyed.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[out] W
*> \verbatim
*>          W is DOUBLE PRECISION array, dimension (N)
*>          If INFO = 0, the eigenvalues in ascending order.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (MAX(1,LWORK))
*>          On exit, if INFO = 0, WORK(1) returns the optimal LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK.
*>          If N <= 1,               LWORK must be at least 1.
*>          If JOBZ = 'N' and N > 1, LWORK must be queried.
*>                                   LWORK = MAX(1, dimension) where
*>                                   dimension = max(stage1,stage2) + (KD+1)*N + N+1
*>                                             = N*KD + N*max(KD+1,FACTOPTNB) 
*>                                               + max(2*KD*KD, KD*NTHREADS) 
*>                                               + (KD+1)*N + N+1
*>                                   where KD is the blocking size of the reduction,
*>                                   FACTOPTNB is the blocking used by the QR or LQ
*>                                   algorithm, usually FACTOPTNB=128 is a good choice
*>                                   NTHREADS is the number of threads used when
*>                                   openMP compilation is enabled, otherwise =1.
*>          If JOBZ = 'V' and N > 1, LWORK must be at least 2*N + N**2
*>
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal sizes of the WORK, RWORK and
*>          IWORK arrays, returns these values as the first entries of
*>          the WORK, RWORK and IWORK arrays, and no error message
*>          related to LWORK or LRWORK or LIWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array,
*>                                         dimension (LRWORK)
*>          On exit, if INFO = 0, RWORK(1) returns the optimal LRWORK.
*> \endverbatim
*>
*> \param[in] LRWORK
*> \verbatim
*>          LRWORK is INTEGER
*>          The dimension of the array RWORK.
*>          If N <= 1,                LRWORK must be at least 1.
*>          If JOBZ  = 'N' and N > 1, LRWORK must be at least N.
*>          If JOBZ  = 'V' and N > 1, LRWORK must be at least
*>                         1 + 5*N + 2*N**2.
*>
*>          If LRWORK = -1, then a workspace query is assumed; the
*>          routine only calculates the optimal sizes of the WORK, RWORK
*>          and IWORK arrays, returns these values as the first entries
*>          of the WORK, RWORK and IWORK arrays, and no error message
*>          related to LWORK or LRWORK or LIWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (MAX(1,LIWORK))
*>          On exit, if INFO = 0, IWORK(1) returns the optimal LIWORK.
*> \endverbatim
*>
*> \param[in] LIWORK
*> \verbatim
*>          LIWORK is INTEGER
*>          The dimension of the array IWORK.
*>          If N <= 1,                LIWORK must be at least 1.
*>          If JOBZ  = 'N' and N > 1, LIWORK must be at least 1.
*>          If JOBZ  = 'V' and N > 1, LIWORK must be at least 3 + 5*N.
*>
*>          If LIWORK = -1, then a workspace query is assumed; the
*>          routine only calculates the optimal sizes of the WORK, RWORK
*>          and IWORK arrays, returns these values as the first entries
*>          of the WORK, RWORK and IWORK arrays, and no error message
*>          related to LWORK or LRWORK or LIWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
*>          > 0:  if INFO = i and JOBZ = 'N', then the algorithm failed
*>                to converge; i off-diagonal elements of an intermediate
*>                tridiagonal form did not converge to zero;
*>                if INFO = i and JOBZ = 'V', then the algorithm failed
*>                to compute an eigenvalue while working on the submatrix
*>                lying in rows and columns INFO/(N+1) through
*>                mod(INFO,N+1).
*> \endverbatim
*
*  Authors:
*  ========
*
*> \author Univ. of Tennessee
*> \author Univ. of California Berkeley
*> \author Univ. of Colorado Denver
*> \author NAG Ltd.
*
*> \date November 2017
*
*> \ingroup complex16HEeigen
*
*> \par Further Details:
*  =====================
*>
*>  Modified description of INFO. Sven, 16 Feb 05.
*
*> \par Contributors:
*  ==================
*>
*> Jeff Rutter, Computer Science Division, University of California
*> at Berkeley, USA
*>
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  All details about the 2stage techniques are available in:
*>
*>  Azzam Haidar, Hatem Ltaief, and Jack Dongarra.
*>  Parallel reduction to condensed forms for symmetric eigenvalue problems
*>  using aggregated fine-grained and memory-aware kernels. In Proceedings
*>  of 2011 International Conference for High Performance Computing,
*>  Networking, Storage and Analysis (SC '11), New York, NY, USA,
*>  Article 8 , 11 pages.
*>  http://doi.acm.org/10.1145/2063384.2063394
*>
*>  A. Haidar, J. Kurzak, P. Luszczek, 2013.
*>  An improved parallel singular value algorithm and its implementation 
*>  for multicore hardware, In Proceedings of 2013 International Conference
*>  for High Performance Computing, Networking, Storage and Analysis (SC '13).
*>  Denver, Colorado, USA, 2013.
*>  Article 90, 12 pages.
*>  http://doi.acm.org/10.1145/2503210.2503292
*>
*>  A. Haidar, R. Solca, S. Tomov, T. Schulthess and J. Dongarra.
*>  A novel hybrid CPU-GPU generalized eigensolver for electronic structure 
*>  calculations based on fine-grained memory aware tasks.
*>  International Journal of High Performance Computing Applications.
*>  Volume 28 Issue 2, Pages 196-209, May 2014.
*>  http://hpc.sagepub.com/content/28/2/196 
*>
*> \endverbatim
*
*  =====================================================================
      SUBROUTINE ZHEEVD_2STAGE( JOBZ, UPLO, N, A, LDA, W, WORK, LWORK,
     $                   RWORK, LRWORK, IWORK, LIWORK, INFO )
*
      IMPLICIT NONE
*
*  -- LAPACK driver routine (version 3.8.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2017
*
*     .. Scalar Arguments ..
      CHARACTER          JOBZ, UPLO
      INTEGER            INFO, LDA, LIWORK, LRWORK, LWORK, N
*     ..
*     .. Array Arguments ..
      INTEGER            IWORK( * )
      DOUBLE PRECISION   RWORK( * ), W( * )
      COMPLEX*16         A( LDA, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D0, ONE = 1.0D0 )
      COMPLEX*16         CONE
      PARAMETER          ( CONE = ( 1.0D0, 0.0D0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            LOWER, LQUERY, WANTZ
      INTEGER            IINFO, IMAX, INDE, INDRWK, INDTAU, INDWK2,
     $                   INDWRK, ISCALE, LIWMIN, LLRWK, LLWORK,
     $                   LLWRK2, LRWMIN, LWMIN,
     $                   LHTRD, LWTRD, KD, IB, INDHOUS


      DOUBLE PRECISION   ANRM, BIGNUM, EPS, RMAX, RMIN, SAFMIN, SIGMA,
     $                   SMLNUM
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV2STAGE
      DOUBLE PRECISION   DLAMCH, ZLANHE
      EXTERNAL           LSAME, DLAMCH, ZLANHE, ILAENV2STAGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           DSCAL, DSTERF, XERBLA, ZLACPY, ZLASCL,
     $                   ZSTEDC, ZUNMTR, ZHETRD_2STAGE
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE, MAX, SQRT 
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      WANTZ = LSAME( JOBZ, 'V' )
      LOWER = LSAME( UPLO, 'L' )
      LQUERY = ( LWORK.EQ.-1 .OR. LRWORK.EQ.-1 .OR. LIWORK.EQ.-1 )
*
      INFO = 0
      IF( .NOT.( LSAME( JOBZ, 'N' ) ) ) THEN
         INFO = -1
      ELSE IF( .NOT.( LOWER .OR. LSAME( UPLO, 'U' ) ) ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -5
      END IF
*
      IF( INFO.EQ.0 ) THEN
         IF( N.LE.1 ) THEN
            LWMIN = 1
            LRWMIN = 1
            LIWMIN = 1
         ELSE
            KD    = ILAENV2STAGE( 1, 'ZHETRD_2STAGE', JOBZ,
     $                            N, -1, -1, -1 )
            IB    = ILAENV2STAGE( 2, 'ZHETRD_2STAGE', JOBZ,
     $                            N, KD, -1, -1 )
            LHTRD = ILAENV2STAGE( 3, 'ZHETRD_2STAGE', JOBZ,
     $                            N, KD, IB, -1 )
            LWTRD = ILAENV2STAGE( 4, 'ZHETRD_2STAGE', JOBZ,
     $                            N, KD, IB, -1 )
            IF( WANTZ ) THEN
               LWMIN = 2*N + N*N
               LRWMIN = 1 + 5*N + 2*N**2
               LIWMIN = 3 + 5*N
            ELSE
               LWMIN = N + 1 + LHTRD + LWTRD
               LRWMIN = N
               LIWMIN = 1
            END IF
         END IF
         WORK( 1 )  = LWMIN
         RWORK( 1 ) = LRWMIN
         IWORK( 1 ) = LIWMIN
*
         IF( LWORK.LT.LWMIN .AND. .NOT.LQUERY ) THEN
            INFO = -8
         ELSE IF( LRWORK.LT.LRWMIN .AND. .NOT.LQUERY ) THEN
            INFO = -10
         ELSE IF( LIWORK.LT.LIWMIN .AND. .NOT.LQUERY ) THEN
            INFO = -12
         END IF
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZHEEVD_2STAGE', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 )
     $   RETURN
*
      IF( N.EQ.1 ) THEN
         W( 1 ) = DBLE( A( 1, 1 ) )
         IF( WANTZ )
     $      A( 1, 1 ) = CONE
         RETURN
      END IF
*
*     Get machine constants.
*
      SAFMIN = DLAMCH( 'Safe minimum' )
      EPS    = DLAMCH( 'Precision' )
      SMLNUM = SAFMIN / EPS
      BIGNUM = ONE / SMLNUM
      RMIN   = SQRT( SMLNUM )
      RMAX   = SQRT( BIGNUM )
*
*     Scale matrix to allowable range, if necessary.
*
      ANRM = ZLANHE( 'M', UPLO, N, A, LDA, RWORK )
      ISCALE = 0
      IF( ANRM.GT.ZERO .AND. ANRM.LT.RMIN ) THEN
         ISCALE = 1
         SIGMA = RMIN / ANRM
      ELSE IF( ANRM.GT.RMAX ) THEN
         ISCALE = 1
         SIGMA = RMAX / ANRM
      END IF
      IF( ISCALE.EQ.1 )
     $   CALL ZLASCL( UPLO, 0, 0, ONE, SIGMA, N, N, A, LDA, INFO )
*
*     Call ZHETRD_2STAGE to reduce Hermitian matrix to tridiagonal form.
*
      INDE    = 1
      INDRWK  = INDE + N
      LLRWK   = LRWORK - INDRWK + 1
      INDTAU  = 1
      INDHOUS = INDTAU + N
      INDWRK  = INDHOUS + LHTRD
      LLWORK  = LWORK - INDWRK + 1
      INDWK2  = INDWRK + N*N
      LLWRK2  = LWORK - INDWK2 + 1
*
      CALL ZHETRD_2STAGE( JOBZ, UPLO, N, A, LDA, W, RWORK( INDE ),
     $                    WORK( INDTAU ), WORK( INDHOUS ), LHTRD, 
     $                    WORK( INDWRK ), LLWORK, IINFO )
*
*     For eigenvalues only, call DSTERF.  For eigenvectors, first call
*     ZSTEDC to generate the eigenvector matrix, WORK(INDWRK), of the
*     tridiagonal matrix, then call ZUNMTR to multiply it to the
*     Householder transformations represented as Householder vectors in
*     A.
*
      IF( .NOT.WANTZ ) THEN
         CALL DSTERF( N, W, RWORK( INDE ), INFO )
      ELSE
         CALL ZSTEDC( 'I', N, W, RWORK( INDE ), WORK( INDWRK ), N,
     $                WORK( INDWK2 ), LLWRK2, RWORK( INDRWK ), LLRWK,
     $                IWORK, LIWORK, INFO )
         CALL ZUNMTR( 'L', UPLO, 'N', N, N, A, LDA, WORK( INDTAU ),
     $                WORK( INDWRK ), N, WORK( INDWK2 ), LLWRK2, IINFO )
         CALL ZLACPY( 'A', N, N, WORK( INDWRK ), N, A, LDA )
      END IF
*
*     If matrix was scaled, then rescale eigenvalues appropriately.
*
      IF( ISCALE.EQ.1 ) THEN
         IF( INFO.EQ.0 ) THEN
            IMAX = N
         ELSE
            IMAX = INFO - 1
         END IF
         CALL DSCAL( IMAX, ONE / SIGMA, W, 1 )
      END IF
*
      WORK( 1 )  = LWMIN
      RWORK( 1 ) = LRWMIN
      IWORK( 1 ) = LIWMIN
*
      RETURN
*
*     End of ZHEEVD_2STAGE
*
      END
