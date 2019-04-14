*> \brief \b DGESVJ
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DGESVJ + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dgesvj.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dgesvj.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dgesvj.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DGESVJ( JOBA, JOBU, JOBV, M, N, A, LDA, SVA, MV, V,
*                          LDV, WORK, LWORK, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, LDV, LWORK, M, MV, N
*       CHARACTER*1        JOBA, JOBU, JOBV
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   A( LDA, * ), SVA( N ), V( LDV, * ),
*      $                   WORK( LWORK )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DGESVJ computes the singular value decomposition (SVD) of a real
*> M-by-N matrix A, where M >= N. The SVD of A is written as
*>                                    [++]   [xx]   [x0]   [xx]
*>              A = U * SIGMA * V^t,  [++] = [xx] * [ox] * [xx]
*>                                    [++]   [xx]
*> where SIGMA is an N-by-N diagonal matrix, U is an M-by-N orthonormal
*> matrix, and V is an N-by-N orthogonal matrix. The diagonal elements
*> of SIGMA are the singular values of A. The columns of U and V are the
*> left and the right singular vectors of A, respectively.
*> DGESVJ can sometimes compute tiny singular values and their singular vectors much
*> more accurately than other SVD routines, see below under Further Details.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] JOBA
*> \verbatim
*>          JOBA is CHARACTER*1
*>          Specifies the structure of A.
*>          = 'L': The input matrix A is lower triangular;
*>          = 'U': The input matrix A is upper triangular;
*>          = 'G': The input matrix A is general M-by-N matrix, M >= N.
*> \endverbatim
*>
*> \param[in] JOBU
*> \verbatim
*>          JOBU is CHARACTER*1
*>          Specifies whether to compute the left singular vectors
*>          (columns of U):
*>          = 'U': The left singular vectors corresponding to the nonzero
*>                 singular values are computed and returned in the leading
*>                 columns of A. See more details in the description of A.
*>                 The default numerical orthogonality threshold is set to
*>                 approximately TOL=CTOL*EPS, CTOL=DSQRT(M), EPS=DLAMCH('E').
*>          = 'C': Analogous to JOBU='U', except that user can control the
*>                 level of numerical orthogonality of the computed left
*>                 singular vectors. TOL can be set to TOL = CTOL*EPS, where
*>                 CTOL is given on input in the array WORK.
*>                 No CTOL smaller than ONE is allowed. CTOL greater
*>                 than 1 / EPS is meaningless. The option 'C'
*>                 can be used if M*EPS is satisfactory orthogonality
*>                 of the computed left singular vectors, so CTOL=M could
*>                 save few sweeps of Jacobi rotations.
*>                 See the descriptions of A and WORK(1).
*>          = 'N': The matrix U is not computed. However, see the
*>                 description of A.
*> \endverbatim
*>
*> \param[in] JOBV
*> \verbatim
*>          JOBV is CHARACTER*1
*>          Specifies whether to compute the right singular vectors, that
*>          is, the matrix V:
*>          = 'V' : the matrix V is computed and returned in the array V
*>          = 'A' : the Jacobi rotations are applied to the MV-by-N
*>                  array V. In other words, the right singular vector
*>                  matrix V is not computed explicitly, instead it is
*>                  applied to an MV-by-N matrix initially stored in the
*>                  first MV rows of V.
*>          = 'N' : the matrix V is not computed and the array V is not
*>                  referenced
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the input matrix A. 1/DLAMCH('E') > M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the input matrix A.
*>          M >= N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension (LDA,N)
*>          On entry, the M-by-N matrix A.
*>          On exit :
*>          If JOBU .EQ. 'U' .OR. JOBU .EQ. 'C' :
*>                 If INFO .EQ. 0 :
*>                 RANKA orthonormal columns of U are returned in the
*>                 leading RANKA columns of the array A. Here RANKA <= N
*>                 is the number of computed singular values of A that are
*>                 above the underflow threshold DLAMCH('S'). The singular
*>                 vectors corresponding to underflowed or zero singular
*>                 values are not computed. The value of RANKA is returned
*>                 in the array WORK as RANKA=NINT(WORK(2)). Also see the
*>                 descriptions of SVA and WORK. The computed columns of U
*>                 are mutually numerically orthogonal up to approximately
*>                 TOL=DSQRT(M)*EPS (default); or TOL=CTOL*EPS (JOBU.EQ.'C'),
*>                 see the description of JOBU.
*>                 If INFO .GT. 0 :
*>                 the procedure DGESVJ did not converge in the given number
*>                 of iterations (sweeps). In that case, the computed
*>                 columns of U may not be orthogonal up to TOL. The output
*>                 U (stored in A), SIGMA (given by the computed singular
*>                 values in SVA(1:N)) and V is still a decomposition of the
*>                 input matrix A in the sense that the residual
*>                 ||A-SCALE*U*SIGMA*V^T||_2 / ||A||_2 is small.
*>
*>          If JOBU .EQ. 'N' :
*>                 If INFO .EQ. 0 :
*>                 Note that the left singular vectors are 'for free' in the
*>                 one-sided Jacobi SVD algorithm. However, if only the
*>                 singular values are needed, the level of numerical
*>                 orthogonality of U is not an issue and iterations are
*>                 stopped when the columns of the iterated matrix are
*>                 numerically orthogonal up to approximately M*EPS. Thus,
*>                 on exit, A contains the columns of U scaled with the
*>                 corresponding singular values.
*>                 If INFO .GT. 0 :
*>                 the procedure DGESVJ did not converge in the given number
*>                 of iterations (sweeps).
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,M).
*> \endverbatim
*>
*> \param[out] SVA
*> \verbatim
*>          SVA is DOUBLE PRECISION array, dimension (N)
*>          On exit :
*>          If INFO .EQ. 0 :
*>          depending on the value SCALE = WORK(1), we have:
*>                 If SCALE .EQ. ONE :
*>                 SVA(1:N) contains the computed singular values of A.
*>                 During the computation SVA contains the Euclidean column
*>                 norms of the iterated matrices in the array A.
*>                 If SCALE .NE. ONE :
*>                 The singular values of A are SCALE*SVA(1:N), and this
*>                 factored representation is due to the fact that some of the
*>                 singular values of A might underflow or overflow.
*>          If INFO .GT. 0 :
*>          the procedure DGESVJ did not converge in the given number of
*>          iterations (sweeps) and SCALE*SVA(1:N) may not be accurate.
*> \endverbatim
*>
*> \param[in] MV
*> \verbatim
*>          MV is INTEGER
*>          If JOBV .EQ. 'A', then the product of Jacobi rotations in DGESVJ
*>          is applied to the first MV rows of V. See the description of JOBV.
*> \endverbatim
*>
*> \param[in,out] V
*> \verbatim
*>          V is DOUBLE PRECISION array, dimension (LDV,N)
*>          If JOBV = 'V', then V contains on exit the N-by-N matrix of
*>                         the right singular vectors;
*>          If JOBV = 'A', then V contains the product of the computed right
*>                         singular vector matrix and the initial matrix in
*>                         the array V.
*>          If JOBV = 'N', then V is not referenced.
*> \endverbatim
*>
*> \param[in] LDV
*> \verbatim
*>          LDV is INTEGER
*>          The leading dimension of the array V, LDV .GE. 1.
*>          If JOBV .EQ. 'V', then LDV .GE. max(1,N).
*>          If JOBV .EQ. 'A', then LDV .GE. max(1,MV) .
*> \endverbatim
*>
*> \param[in,out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (LWORK)
*>          On entry :
*>          If JOBU .EQ. 'C' :
*>          WORK(1) = CTOL, where CTOL defines the threshold for convergence.
*>                    The process stops if all columns of A are mutually
*>                    orthogonal up to CTOL*EPS, EPS=DLAMCH('E').
*>                    It is required that CTOL >= ONE, i.e. it is not
*>                    allowed to force the routine to obtain orthogonality
*>                    below EPS.
*>          On exit :
*>          WORK(1) = SCALE is the scaling factor such that SCALE*SVA(1:N)
*>                    are the computed singular values of A.
*>                    (See description of SVA().)
*>          WORK(2) = NINT(WORK(2)) is the number of the computed nonzero
*>                    singular values.
*>          WORK(3) = NINT(WORK(3)) is the number of the computed singular
*>                    values that are larger than the underflow threshold.
*>          WORK(4) = NINT(WORK(4)) is the number of sweeps of Jacobi
*>                    rotations needed for numerical convergence.
*>          WORK(5) = max_{i.NE.j} |COS(A(:,i),A(:,j))| in the last sweep.
*>                    This is useful information in cases when DGESVJ did
*>                    not converge, as it can be used to estimate whether
*>                    the output is stil useful and for post festum analysis.
*>          WORK(6) = the largest absolute value over all sines of the
*>                    Jacobi rotation angles in the last sweep. It can be
*>                    useful for a post festum analysis.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          length of WORK, WORK >= MAX(6,M+N)
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0 : successful exit.
*>          < 0 : if INFO = -i, then the i-th argument had an illegal value
*>          > 0 : DGESVJ did not converge in the maximal allowed number (30)
*>                of sweeps. The output may still be useful. See the
*>                description of WORK.
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
*> \date June 2017
*
*> \ingroup doubleGEcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  The orthogonal N-by-N matrix V is obtained as a product of Jacobi plane
*>  rotations. The rotations are implemented as fast scaled rotations of
*>  Anda and Park [1]. In the case of underflow of the Jacobi angle, a
*>  modified Jacobi transformation of Drmac [4] is used. Pivot strategy uses
*>  column interchanges of de Rijk [2]. The relative accuracy of the computed
*>  singular values and the accuracy of the computed singular vectors (in
*>  angle metric) is as guaranteed by the theory of Demmel and Veselic [3].
*>  The condition number that determines the accuracy in the full rank case
*>  is essentially min_{D=diag} kappa(A*D), where kappa(.) is the
*>  spectral condition number. The best performance of this Jacobi SVD
*>  procedure is achieved if used in an  accelerated version of Drmac and
*>  Veselic [5,6], and it is the kernel routine in the SIGMA library [7].
*>  Some tunning parameters (marked with [TP]) are available for the
*>  implementer.
*>  The computational range for the nonzero singular values is the  machine
*>  number interval ( UNDERFLOW , OVERFLOW ). In extreme cases, even
*>  denormalized singular values can be computed with the corresponding
*>  gradual loss of accurate digits.
*> \endverbatim
*
*> \par Contributors:
*  ==================
*>
*> \verbatim
*>
*>  ============
*>
*>  Zlatko Drmac (Zagreb, Croatia) and Kresimir Veselic (Hagen, Germany)
*> \endverbatim
*
*> \par References:
*  ================
*>
*> \verbatim
*>
*> [1] A. A. Anda and H. Park: Fast plane rotations with dynamic scaling.
*>     SIAM J. matrix Anal. Appl., Vol. 15 (1994), pp. 162-174.
*> [2] P. P. M. De Rijk: A one-sided Jacobi algorithm for computing the
*>     singular value decomposition on a vector computer.
*>     SIAM J. Sci. Stat. Comp., Vol. 10 (1998), pp. 359-371.
*> [3] J. Demmel and K. Veselic: Jacobi method is more accurate than QR.
*> [4] Z. Drmac: Implementation of Jacobi rotations for accurate singular
*>     value computation in floating point arithmetic.
*>     SIAM J. Sci. Comp., Vol. 18 (1997), pp. 1200-1222.
*> [5] Z. Drmac and K. Veselic: New fast and accurate Jacobi SVD algorithm I.
*>     SIAM J. Matrix Anal. Appl. Vol. 35, No. 2 (2008), pp. 1322-1342.
*>     LAPACK Working note 169.
*> [6] Z. Drmac and K. Veselic: New fast and accurate Jacobi SVD algorithm II.
*>     SIAM J. Matrix Anal. Appl. Vol. 35, No. 2 (2008), pp. 1343-1362.
*>     LAPACK Working note 170.
*> [7] Z. Drmac: SIGMA - mathematical software library for accurate SVD, PSV,
*>     QSVD, (H,K)-SVD computations.
*>     Department of Mathematics, University of Zagreb, 2008.
*> \endverbatim
*
*>  \par Bugs, examples and comments:
*   =================================
*>
*> \verbatim
*>  ===========================
*>  Please report all bugs and send interesting test examples and comments to
*>  drmac@math.hr. Thank you.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE DGESVJ( JOBA, JOBU, JOBV, M, N, A, LDA, SVA, MV, V,
     $                   LDV, WORK, LWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, LDV, LWORK, M, MV, N
      CHARACTER*1        JOBA, JOBU, JOBV
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   A( LDA, * ), SVA( N ), V( LDV, * ),
     $                   WORK( LWORK )
*     ..
*
*  =====================================================================
*
*     .. Local Parameters ..
      DOUBLE PRECISION   ZERO, HALF, ONE
      PARAMETER          ( ZERO = 0.0D0, HALF = 0.5D0, ONE = 1.0D0)
      INTEGER            NSWEEP
      PARAMETER          ( NSWEEP = 30 )
*     ..
*     .. Local Scalars ..
      DOUBLE PRECISION   AAPP, AAPP0, AAPQ, AAQQ, APOAQ, AQOAP, BIG,
     $                   BIGTHETA, CS, CTOL, EPSLN, LARGE, MXAAPQ,
     $                   MXSINJ, ROOTBIG, ROOTEPS, ROOTSFMIN, ROOTTOL,
     $                   SKL, SFMIN, SMALL, SN, T, TEMP1, THETA,
     $                   THSIGN, TOL
      INTEGER            BLSKIP, EMPTSW, i, ibr, IERR, igl, IJBLSK, ir1,
     $                   ISWROT, jbc, jgl, KBL, LKAHEAD, MVL, N2, N34,
     $                   N4, NBL, NOTROT, p, PSKIPPED, q, ROWSKIP,
     $                   SWBAND
      LOGICAL            APPLV, GOSCALE, LOWER, LSVEC, NOSCALE, ROTOK,
     $                   RSVEC, UCTOL, UPPER
*     ..
*     .. Local Arrays ..
      DOUBLE PRECISION   FASTR( 5 )
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DABS, MAX, MIN, DBLE, DSIGN, DSQRT
*     ..
*     .. External Functions ..
*     ..
*     from BLAS
      DOUBLE PRECISION   DDOT, DNRM2
      EXTERNAL           DDOT, DNRM2
      INTEGER            IDAMAX
      EXTERNAL           IDAMAX
*     from LAPACK
      DOUBLE PRECISION   DLAMCH
      EXTERNAL           DLAMCH
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
*     ..
*     from BLAS
      EXTERNAL           DAXPY, DCOPY, DROTM, DSCAL, DSWAP
*     from LAPACK
      EXTERNAL           DLASCL, DLASET, DLASSQ, XERBLA
*
      EXTERNAL           DGSVJ0, DGSVJ1
*     ..
*     .. Executable Statements ..
*
*     Test the input arguments
*
      LSVEC = LSAME( JOBU, 'U' )
      UCTOL = LSAME( JOBU, 'C' )
      RSVEC = LSAME( JOBV, 'V' )
      APPLV = LSAME( JOBV, 'A' )
      UPPER = LSAME( JOBA, 'U' )
      LOWER = LSAME( JOBA, 'L' )
*
      IF( .NOT.( UPPER .OR. LOWER .OR. LSAME( JOBA, 'G' ) ) ) THEN
         INFO = -1
      ELSE IF( .NOT.( LSVEC .OR. UCTOL .OR. LSAME( JOBU, 'N' ) ) ) THEN
         INFO = -2
      ELSE IF( .NOT.( RSVEC .OR. APPLV .OR. LSAME( JOBV, 'N' ) ) ) THEN
         INFO = -3
      ELSE IF( M.LT.0 ) THEN
         INFO = -4
      ELSE IF( ( N.LT.0 ) .OR. ( N.GT.M ) ) THEN
         INFO = -5
      ELSE IF( LDA.LT.M ) THEN
         INFO = -7
      ELSE IF( MV.LT.0 ) THEN
         INFO = -9
      ELSE IF( ( RSVEC .AND. ( LDV.LT.N ) ) .OR.
     $         ( APPLV .AND. ( LDV.LT.MV ) ) ) THEN
         INFO = -11
      ELSE IF( UCTOL .AND. ( WORK( 1 ).LE.ONE ) ) THEN
         INFO = -12
      ELSE IF( LWORK.LT.MAX( M+N, 6 ) ) THEN
         INFO = -13
      ELSE
         INFO = 0
      END IF
*
*     #:(
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DGESVJ', -INFO )
         RETURN
      END IF
*
* #:) Quick return for void matrix
*
      IF( ( M.EQ.0 ) .OR. ( N.EQ.0 ) )RETURN
*
*     Set numerical parameters
*     The stopping criterion for Jacobi rotations is
*
*     max_{i<>j}|A(:,i)^T * A(:,j)|/(||A(:,i)||*||A(:,j)||) < CTOL*EPS
*
*     where EPS is the round-off and CTOL is defined as follows:
*
      IF( UCTOL ) THEN
*        ... user controlled
         CTOL = WORK( 1 )
      ELSE
*        ... default
         IF( LSVEC .OR. RSVEC .OR. APPLV ) THEN
            CTOL = DSQRT( DBLE( M ) )
         ELSE
            CTOL = DBLE( M )
         END IF
      END IF
*     ... and the machine dependent parameters are
*[!]  (Make sure that DLAMCH() works properly on the target machine.)
*
      EPSLN = DLAMCH( 'Epsilon' )
      ROOTEPS = DSQRT( EPSLN )
      SFMIN = DLAMCH( 'SafeMinimum' )
      ROOTSFMIN = DSQRT( SFMIN )
      SMALL = SFMIN / EPSLN
      BIG = DLAMCH( 'Overflow' )
*     BIG         = ONE    / SFMIN
      ROOTBIG = ONE / ROOTSFMIN
      LARGE = BIG / DSQRT( DBLE( M*N ) )
      BIGTHETA = ONE / ROOTEPS
*
      TOL = CTOL*EPSLN
      ROOTTOL = DSQRT( TOL )
*
      IF( DBLE( M )*EPSLN.GE.ONE ) THEN
         INFO = -4
         CALL XERBLA( 'DGESVJ', -INFO )
         RETURN
      END IF
*
*     Initialize the right singular vector matrix.
*
      IF( RSVEC ) THEN
         MVL = N
         CALL DLASET( 'A', MVL, N, ZERO, ONE, V, LDV )
      ELSE IF( APPLV ) THEN
         MVL = MV
      END IF
      RSVEC = RSVEC .OR. APPLV
*
*     Initialize SVA( 1:N ) = ( ||A e_i||_2, i = 1:N )
*(!)  If necessary, scale A to protect the largest singular value
*     from overflow. It is possible that saving the largest singular
*     value destroys the information about the small ones.
*     This initial scaling is almost minimal in the sense that the
*     goal is to make sure that no column norm overflows, and that
*     DSQRT(N)*max_i SVA(i) does not overflow. If INFinite entries
*     in A are detected, the procedure returns with INFO=-6.
*
      SKL= ONE / DSQRT( DBLE( M )*DBLE( N ) )
      NOSCALE = .TRUE.
      GOSCALE = .TRUE.
*
      IF( LOWER ) THEN
*        the input matrix is M-by-N lower triangular (trapezoidal)
         DO 1874 p = 1, N
            AAPP = ZERO
            AAQQ = ONE
            CALL DLASSQ( M-p+1, A( p, p ), 1, AAPP, AAQQ )
            IF( AAPP.GT.BIG ) THEN
               INFO = -6
               CALL XERBLA( 'DGESVJ', -INFO )
               RETURN
            END IF
            AAQQ = DSQRT( AAQQ )
            IF( ( AAPP.LT.( BIG / AAQQ ) ) .AND. NOSCALE ) THEN
               SVA( p ) = AAPP*AAQQ
            ELSE
               NOSCALE = .FALSE.
               SVA( p ) = AAPP*( AAQQ*SKL)
               IF( GOSCALE ) THEN
                  GOSCALE = .FALSE.
                  DO 1873 q = 1, p - 1
                     SVA( q ) = SVA( q )*SKL
 1873             CONTINUE
               END IF
            END IF
 1874    CONTINUE
      ELSE IF( UPPER ) THEN
*        the input matrix is M-by-N upper triangular (trapezoidal)
         DO 2874 p = 1, N
            AAPP = ZERO
            AAQQ = ONE
            CALL DLASSQ( p, A( 1, p ), 1, AAPP, AAQQ )
            IF( AAPP.GT.BIG ) THEN
               INFO = -6
               CALL XERBLA( 'DGESVJ', -INFO )
               RETURN
            END IF
            AAQQ = DSQRT( AAQQ )
            IF( ( AAPP.LT.( BIG / AAQQ ) ) .AND. NOSCALE ) THEN
               SVA( p ) = AAPP*AAQQ
            ELSE
               NOSCALE = .FALSE.
               SVA( p ) = AAPP*( AAQQ*SKL)
               IF( GOSCALE ) THEN
                  GOSCALE = .FALSE.
                  DO 2873 q = 1, p - 1
                     SVA( q ) = SVA( q )*SKL
 2873             CONTINUE
               END IF
            END IF
 2874    CONTINUE
      ELSE
*        the input matrix is M-by-N general dense
         DO 3874 p = 1, N
            AAPP = ZERO
            AAQQ = ONE
            CALL DLASSQ( M, A( 1, p ), 1, AAPP, AAQQ )
            IF( AAPP.GT.BIG ) THEN
               INFO = -6
               CALL XERBLA( 'DGESVJ', -INFO )
               RETURN
            END IF
            AAQQ = DSQRT( AAQQ )
            IF( ( AAPP.LT.( BIG / AAQQ ) ) .AND. NOSCALE ) THEN
               SVA( p ) = AAPP*AAQQ
            ELSE
               NOSCALE = .FALSE.
               SVA( p ) = AAPP*( AAQQ*SKL)
               IF( GOSCALE ) THEN
                  GOSCALE = .FALSE.
                  DO 3873 q = 1, p - 1
                     SVA( q ) = SVA( q )*SKL
 3873             CONTINUE
               END IF
            END IF
 3874    CONTINUE
      END IF
*
      IF( NOSCALE )SKL= ONE
*
*     Move the smaller part of the spectrum from the underflow threshold
*(!)  Start by determining the position of the nonzero entries of the
*     array SVA() relative to ( SFMIN, BIG ).
*
      AAPP = ZERO
      AAQQ = BIG
      DO 4781 p = 1, N
         IF( SVA( p ).NE.ZERO )AAQQ = MIN( AAQQ, SVA( p ) )
         AAPP = MAX( AAPP, SVA( p ) )
 4781 CONTINUE
*
* #:) Quick return for zero matrix
*
      IF( AAPP.EQ.ZERO ) THEN
         IF( LSVEC )CALL DLASET( 'G', M, N, ZERO, ONE, A, LDA )
         WORK( 1 ) = ONE
         WORK( 2 ) = ZERO
         WORK( 3 ) = ZERO
         WORK( 4 ) = ZERO
         WORK( 5 ) = ZERO
         WORK( 6 ) = ZERO
         RETURN
      END IF
*
* #:) Quick return for one-column matrix
*
      IF( N.EQ.1 ) THEN
         IF( LSVEC )CALL DLASCL( 'G', 0, 0, SVA( 1 ), SKL, M, 1,
     $                           A( 1, 1 ), LDA, IERR )
         WORK( 1 ) = ONE / SKL
         IF( SVA( 1 ).GE.SFMIN ) THEN
            WORK( 2 ) = ONE
         ELSE
            WORK( 2 ) = ZERO
         END IF
         WORK( 3 ) = ZERO
         WORK( 4 ) = ZERO
         WORK( 5 ) = ZERO
         WORK( 6 ) = ZERO
         RETURN
      END IF
*
*     Protect small singular values from underflow, and try to
*     avoid underflows/overflows in computing Jacobi rotations.
*
      SN = DSQRT( SFMIN / EPSLN )
      TEMP1 = DSQRT( BIG / DBLE( N ) )
      IF( ( AAPP.LE.SN ) .OR. ( AAQQ.GE.TEMP1 ) .OR.
     $    ( ( SN.LE.AAQQ ) .AND. ( AAPP.LE.TEMP1 ) ) ) THEN
         TEMP1 = MIN( BIG, TEMP1 / AAPP )
*         AAQQ  = AAQQ*TEMP1
*         AAPP  = AAPP*TEMP1
      ELSE IF( ( AAQQ.LE.SN ) .AND. ( AAPP.LE.TEMP1 ) ) THEN
         TEMP1 = MIN( SN / AAQQ, BIG / ( AAPP*DSQRT( DBLE( N ) ) ) )
*         AAQQ  = AAQQ*TEMP1
*         AAPP  = AAPP*TEMP1
      ELSE IF( ( AAQQ.GE.SN ) .AND. ( AAPP.GE.TEMP1 ) ) THEN
         TEMP1 = MAX( SN / AAQQ, TEMP1 / AAPP )
*         AAQQ  = AAQQ*TEMP1
*         AAPP  = AAPP*TEMP1
      ELSE IF( ( AAQQ.LE.SN ) .AND. ( AAPP.GE.TEMP1 ) ) THEN
         TEMP1 = MIN( SN / AAQQ, BIG / ( DSQRT( DBLE( N ) )*AAPP ) )
*         AAQQ  = AAQQ*TEMP1
*         AAPP  = AAPP*TEMP1
      ELSE
         TEMP1 = ONE
      END IF
*
*     Scale, if necessary
*
      IF( TEMP1.NE.ONE ) THEN
         CALL DLASCL( 'G', 0, 0, ONE, TEMP1, N, 1, SVA, N, IERR )
      END IF
      SKL= TEMP1*SKL
      IF( SKL.NE.ONE ) THEN
         CALL DLASCL( JOBA, 0, 0, ONE, SKL, M, N, A, LDA, IERR )
         SKL= ONE / SKL
      END IF
*
*     Row-cyclic Jacobi SVD algorithm with column pivoting
*
      EMPTSW = ( N*( N-1 ) ) / 2
      NOTROT = 0
      FASTR( 1 ) = ZERO
*
*     A is represented in factored form A = A * diag(WORK), where diag(WORK)
*     is initialized to identity. WORK is updated during fast scaled
*     rotations.
*
      DO 1868 q = 1, N
         WORK( q ) = ONE
 1868 CONTINUE
*
*
      SWBAND = 3
*[TP] SWBAND is a tuning parameter [TP]. It is meaningful and effective
*     if DGESVJ is used as a computational routine in the preconditioned
*     Jacobi SVD algorithm DGESVJ. For sweeps i=1:SWBAND the procedure
*     works on pivots inside a band-like region around the diagonal.
*     The boundaries are determined dynamically, based on the number of
*     pivots above a threshold.
*
      KBL = MIN( 8, N )
*[TP] KBL is a tuning parameter that defines the tile size in the
*     tiling of the p-q loops of pivot pairs. In general, an optimal
*     value of KBL depends on the matrix dimensions and on the
*     parameters of the computer's memory.
*
      NBL = N / KBL
      IF( ( NBL*KBL ).NE.N )NBL = NBL + 1
*
      BLSKIP = KBL**2
*[TP] BLKSKIP is a tuning parameter that depends on SWBAND and KBL.
*
      ROWSKIP = MIN( 5, KBL )
*[TP] ROWSKIP is a tuning parameter.
*
      LKAHEAD = 1
*[TP] LKAHEAD is a tuning parameter.
*
*     Quasi block transformations, using the lower (upper) triangular
*     structure of the input matrix. The quasi-block-cycling usually
*     invokes cubic convergence. Big part of this cycle is done inside
*     canonical subspaces of dimensions less than M.
*
      IF( ( LOWER .OR. UPPER ) .AND. ( N.GT.MAX( 64, 4*KBL ) ) ) THEN
*[TP] The number of partition levels and the actual partition are
*     tuning parameters.
         N4 = N / 4
         N2 = N / 2
         N34 = 3*N4
         IF( APPLV ) THEN
            q = 0
         ELSE
            q = 1
         END IF
*
         IF( LOWER ) THEN
*
*     This works very well on lower triangular matrices, in particular
*     in the framework of the preconditioned Jacobi SVD (xGEJSV).
*     The idea is simple:
*     [+ 0 0 0]   Note that Jacobi transformations of [0 0]
*     [+ + 0 0]                                       [0 0]
*     [+ + x 0]   actually work on [x 0]              [x 0]
*     [+ + x x]                    [x x].             [x x]
*
            CALL DGSVJ0( JOBV, M-N34, N-N34, A( N34+1, N34+1 ), LDA,
     $                   WORK( N34+1 ), SVA( N34+1 ), MVL,
     $                   V( N34*q+1, N34+1 ), LDV, EPSLN, SFMIN, TOL,
     $                   2, WORK( N+1 ), LWORK-N, IERR )
*
            CALL DGSVJ0( JOBV, M-N2, N34-N2, A( N2+1, N2+1 ), LDA,
     $                   WORK( N2+1 ), SVA( N2+1 ), MVL,
     $                   V( N2*q+1, N2+1 ), LDV, EPSLN, SFMIN, TOL, 2,
     $                   WORK( N+1 ), LWORK-N, IERR )
*
            CALL DGSVJ1( JOBV, M-N2, N-N2, N4, A( N2+1, N2+1 ), LDA,
     $                   WORK( N2+1 ), SVA( N2+1 ), MVL,
     $                   V( N2*q+1, N2+1 ), LDV, EPSLN, SFMIN, TOL, 1,
     $                   WORK( N+1 ), LWORK-N, IERR )
*
            CALL DGSVJ0( JOBV, M-N4, N2-N4, A( N4+1, N4+1 ), LDA,
     $                   WORK( N4+1 ), SVA( N4+1 ), MVL,
     $                   V( N4*q+1, N4+1 ), LDV, EPSLN, SFMIN, TOL, 1,
     $                   WORK( N+1 ), LWORK-N, IERR )
*
            CALL DGSVJ0( JOBV, M, N4, A, LDA, WORK, SVA, MVL, V, LDV,
     $                   EPSLN, SFMIN, TOL, 1, WORK( N+1 ), LWORK-N,
     $                   IERR )
*
            CALL DGSVJ1( JOBV, M, N2, N4, A, LDA, WORK, SVA, MVL, V,
     $                   LDV, EPSLN, SFMIN, TOL, 1, WORK( N+1 ),
     $                   LWORK-N, IERR )
*
*
         ELSE IF( UPPER ) THEN
*
*
            CALL DGSVJ0( JOBV, N4, N4, A, LDA, WORK, SVA, MVL, V, LDV,
     $                   EPSLN, SFMIN, TOL, 2, WORK( N+1 ), LWORK-N,
     $                   IERR )
*
            CALL DGSVJ0( JOBV, N2, N4, A( 1, N4+1 ), LDA, WORK( N4+1 ),
     $                   SVA( N4+1 ), MVL, V( N4*q+1, N4+1 ), LDV,
     $                   EPSLN, SFMIN, TOL, 1, WORK( N+1 ), LWORK-N,
     $                   IERR )
*
            CALL DGSVJ1( JOBV, N2, N2, N4, A, LDA, WORK, SVA, MVL, V,
     $                   LDV, EPSLN, SFMIN, TOL, 1, WORK( N+1 ),
     $                   LWORK-N, IERR )
*
            CALL DGSVJ0( JOBV, N2+N4, N4, A( 1, N2+1 ), LDA,
     $                   WORK( N2+1 ), SVA( N2+1 ), MVL,
     $                   V( N2*q+1, N2+1 ), LDV, EPSLN, SFMIN, TOL, 1,
     $                   WORK( N+1 ), LWORK-N, IERR )

         END IF
*
      END IF
*
*     .. Row-cyclic pivot strategy with de Rijk's pivoting ..
*
      DO 1993 i = 1, NSWEEP
*
*     .. go go go ...
*
         MXAAPQ = ZERO
         MXSINJ = ZERO
         ISWROT = 0
*
         NOTROT = 0
         PSKIPPED = 0
*
*     Each sweep is unrolled using KBL-by-KBL tiles over the pivot pairs
*     1 <= p < q <= N. This is the first step toward a blocked implementation
*     of the rotations. New implementation, based on block transformations,
*     is under development.
*
         DO 2000 ibr = 1, NBL
*
            igl = ( ibr-1 )*KBL + 1
*
            DO 1002 ir1 = 0, MIN( LKAHEAD, NBL-ibr )
*
               igl = igl + ir1*KBL
*
               DO 2001 p = igl, MIN( igl+KBL-1, N-1 )
*
*     .. de Rijk's pivoting
*
                  q = IDAMAX( N-p+1, SVA( p ), 1 ) + p - 1
                  IF( p.NE.q ) THEN
                     CALL DSWAP( M, A( 1, p ), 1, A( 1, q ), 1 )
                     IF( RSVEC )CALL DSWAP( MVL, V( 1, p ), 1,
     $                                      V( 1, q ), 1 )
                     TEMP1 = SVA( p )
                     SVA( p ) = SVA( q )
                     SVA( q ) = TEMP1
                     TEMP1 = WORK( p )
                     WORK( p ) = WORK( q )
                     WORK( q ) = TEMP1
                  END IF
*
                  IF( ir1.EQ.0 ) THEN
*
*        Column norms are periodically updated by explicit
*        norm computation.
*        Caveat:
*        Unfortunately, some BLAS implementations compute DNRM2(M,A(1,p),1)
*        as DSQRT(DDOT(M,A(1,p),1,A(1,p),1)), which may cause the result to
*        overflow for ||A(:,p)||_2 > DSQRT(overflow_threshold), and to
*        underflow for ||A(:,p)||_2 < DSQRT(underflow_threshold).
*        Hence, DNRM2 cannot be trusted, not even in the case when
*        the true norm is far from the under(over)flow boundaries.
*        If properly implemented DNRM2 is available, the IF-THEN-ELSE
*        below should read "AAPP = DNRM2( M, A(1,p), 1 ) * WORK(p)".
*
                     IF( ( SVA( p ).LT.ROOTBIG ) .AND.
     $                   ( SVA( p ).GT.ROOTSFMIN ) ) THEN
                        SVA( p ) = DNRM2( M, A( 1, p ), 1 )*WORK( p )
                     ELSE
                        TEMP1 = ZERO
                        AAPP = ONE
                        CALL DLASSQ( M, A( 1, p ), 1, TEMP1, AAPP )
                        SVA( p ) = TEMP1*DSQRT( AAPP )*WORK( p )
                     END IF
                     AAPP = SVA( p )
                  ELSE
                     AAPP = SVA( p )
                  END IF
*
                  IF( AAPP.GT.ZERO ) THEN
*
                     PSKIPPED = 0
*
                     DO 2002 q = p + 1, MIN( igl+KBL-1, N )
*
                        AAQQ = SVA( q )
*
                        IF( AAQQ.GT.ZERO ) THEN
*
                           AAPP0 = AAPP
                           IF( AAQQ.GE.ONE ) THEN
                              ROTOK = ( SMALL*AAPP ).LE.AAQQ
                              IF( AAPP.LT.( BIG / AAQQ ) ) THEN
                                 AAPQ = ( DDOT( M, A( 1, p ), 1, A( 1,
     $                                  q ), 1 )*WORK( p )*WORK( q ) /
     $                                  AAQQ ) / AAPP
                              ELSE
                                 CALL DCOPY( M, A( 1, p ), 1,
     $                                       WORK( N+1 ), 1 )
                                 CALL DLASCL( 'G', 0, 0, AAPP,
     $                                        WORK( p ), M, 1,
     $                                        WORK( N+1 ), LDA, IERR )
                                 AAPQ = DDOT( M, WORK( N+1 ), 1,
     $                                  A( 1, q ), 1 )*WORK( q ) / AAQQ
                              END IF
                           ELSE
                              ROTOK = AAPP.LE.( AAQQ / SMALL )
                              IF( AAPP.GT.( SMALL / AAQQ ) ) THEN
                                 AAPQ = ( DDOT( M, A( 1, p ), 1, A( 1,
     $                                  q ), 1 )*WORK( p )*WORK( q ) /
     $                                  AAQQ ) / AAPP
                              ELSE
                                 CALL DCOPY( M, A( 1, q ), 1,
     $                                       WORK( N+1 ), 1 )
                                 CALL DLASCL( 'G', 0, 0, AAQQ,
     $                                        WORK( q ), M, 1,
     $                                        WORK( N+1 ), LDA, IERR )
                                 AAPQ = DDOT( M, WORK( N+1 ), 1,
     $                                  A( 1, p ), 1 )*WORK( p ) / AAPP
                              END IF
                           END IF
*
                           MXAAPQ = MAX( MXAAPQ, DABS( AAPQ ) )
*
*        TO rotate or NOT to rotate, THAT is the question ...
*
                           IF( DABS( AAPQ ).GT.TOL ) THEN
*
*           .. rotate
*[RTD]      ROTATED = ROTATED + ONE
*
                              IF( ir1.EQ.0 ) THEN
                                 NOTROT = 0
                                 PSKIPPED = 0
                                 ISWROT = ISWROT + 1
                              END IF
*
                              IF( ROTOK ) THEN
*
                                 AQOAP = AAQQ / AAPP
                                 APOAQ = AAPP / AAQQ
                                 THETA = -HALF*DABS(AQOAP-APOAQ)/AAPQ
*
                                 IF( DABS( THETA ).GT.BIGTHETA ) THEN
*
                                    T = HALF / THETA
                                    FASTR( 3 ) = T*WORK( p ) / WORK( q )
                                    FASTR( 4 ) = -T*WORK( q ) /
     $                                           WORK( p )
                                    CALL DROTM( M, A( 1, p ), 1,
     $                                          A( 1, q ), 1, FASTR )
                                    IF( RSVEC )CALL DROTM( MVL,
     $                                              V( 1, p ), 1,
     $                                              V( 1, q ), 1,
     $                                              FASTR )
                                    SVA( q ) = AAQQ*DSQRT( MAX( ZERO,
     $                                         ONE+T*APOAQ*AAPQ ) )
                                    AAPP = AAPP*DSQRT( MAX( ZERO,
     $                                     ONE-T*AQOAP*AAPQ ) )
                                    MXSINJ = MAX( MXSINJ, DABS( T ) )
*
                                 ELSE
*
*                 .. choose correct signum for THETA and rotate
*
                                    THSIGN = -DSIGN( ONE, AAPQ )
                                    T = ONE / ( THETA+THSIGN*
     $                                  DSQRT( ONE+THETA*THETA ) )
                                    CS = DSQRT( ONE / ( ONE+T*T ) )
                                    SN = T*CS
*
                                    MXSINJ = MAX( MXSINJ, DABS( SN ) )
                                    SVA( q ) = AAQQ*DSQRT( MAX( ZERO,
     $                                         ONE+T*APOAQ*AAPQ ) )
                                    AAPP = AAPP*DSQRT( MAX( ZERO,
     $                                     ONE-T*AQOAP*AAPQ ) )
*
                                    APOAQ = WORK( p ) / WORK( q )
                                    AQOAP = WORK( q ) / WORK( p )
                                    IF( WORK( p ).GE.ONE ) THEN
                                       IF( WORK( q ).GE.ONE ) THEN
                                          FASTR( 3 ) = T*APOAQ
                                          FASTR( 4 ) = -T*AQOAP
                                          WORK( p ) = WORK( p )*CS
                                          WORK( q ) = WORK( q )*CS
                                          CALL DROTM( M, A( 1, p ), 1,
     $                                                A( 1, q ), 1,
     $                                                FASTR )
                                          IF( RSVEC )CALL DROTM( MVL,
     $                                        V( 1, p ), 1, V( 1, q ),
     $                                        1, FASTR )
                                       ELSE
                                          CALL DAXPY( M, -T*AQOAP,
     $                                                A( 1, q ), 1,
     $                                                A( 1, p ), 1 )
                                          CALL DAXPY( M, CS*SN*APOAQ,
     $                                                A( 1, p ), 1,
     $                                                A( 1, q ), 1 )
                                          WORK( p ) = WORK( p )*CS
                                          WORK( q ) = WORK( q ) / CS
                                          IF( RSVEC ) THEN
                                             CALL DAXPY( MVL, -T*AQOAP,
     $                                                   V( 1, q ), 1,
     $                                                   V( 1, p ), 1 )
                                             CALL DAXPY( MVL,
     $                                                   CS*SN*APOAQ,
     $                                                   V( 1, p ), 1,
     $                                                   V( 1, q ), 1 )
                                          END IF
                                       END IF
                                    ELSE
                                       IF( WORK( q ).GE.ONE ) THEN
                                          CALL DAXPY( M, T*APOAQ,
     $                                                A( 1, p ), 1,
     $                                                A( 1, q ), 1 )
                                          CALL DAXPY( M, -CS*SN*AQOAP,
     $                                                A( 1, q ), 1,
     $                                                A( 1, p ), 1 )
                                          WORK( p ) = WORK( p ) / CS
                                          WORK( q ) = WORK( q )*CS
                                          IF( RSVEC ) THEN
                                             CALL DAXPY( MVL, T*APOAQ,
     $                                                   V( 1, p ), 1,
     $                                                   V( 1, q ), 1 )
                                             CALL DAXPY( MVL,
     $                                                   -CS*SN*AQOAP,
     $                                                   V( 1, q ), 1,
     $                                                   V( 1, p ), 1 )
                                          END IF
                                       ELSE
                                          IF( WORK( p ).GE.WORK( q ) )
     $                                        THEN
                                             CALL DAXPY( M, -T*AQOAP,
     $                                                   A( 1, q ), 1,
     $                                                   A( 1, p ), 1 )
                                             CALL DAXPY( M, CS*SN*APOAQ,
     $                                                   A( 1, p ), 1,
     $                                                   A( 1, q ), 1 )
                                             WORK( p ) = WORK( p )*CS
                                             WORK( q ) = WORK( q ) / CS
                                             IF( RSVEC ) THEN
                                                CALL DAXPY( MVL,
     $                                               -T*AQOAP,
     $                                               V( 1, q ), 1,
     $                                               V( 1, p ), 1 )
                                                CALL DAXPY( MVL,
     $                                               CS*SN*APOAQ,
     $                                               V( 1, p ), 1,
     $                                               V( 1, q ), 1 )
                                             END IF
                                          ELSE
                                             CALL DAXPY( M, T*APOAQ,
     $                                                   A( 1, p ), 1,
     $                                                   A( 1, q ), 1 )
                                             CALL DAXPY( M,
     $                                                   -CS*SN*AQOAP,
     $                                                   A( 1, q ), 1,
     $                                                   A( 1, p ), 1 )
                                             WORK( p ) = WORK( p ) / CS
                                             WORK( q ) = WORK( q )*CS
                                             IF( RSVEC ) THEN
                                                CALL DAXPY( MVL,
     $                                               T*APOAQ, V( 1, p ),
     $                                               1, V( 1, q ), 1 )
                                                CALL DAXPY( MVL,
     $                                               -CS*SN*AQOAP,
     $                                               V( 1, q ), 1,
     $                                               V( 1, p ), 1 )
                                             END IF
                                          END IF
                                       END IF
                                    END IF
                                 END IF
*
                              ELSE
*              .. have to use modified Gram-Schmidt like transformation
                                 CALL DCOPY( M, A( 1, p ), 1,
     $                                       WORK( N+1 ), 1 )
                                 CALL DLASCL( 'G', 0, 0, AAPP, ONE, M,
     $                                        1, WORK( N+1 ), LDA,
     $                                        IERR )
                                 CALL DLASCL( 'G', 0, 0, AAQQ, ONE, M,
     $                                        1, A( 1, q ), LDA, IERR )
                                 TEMP1 = -AAPQ*WORK( p ) / WORK( q )
                                 CALL DAXPY( M, TEMP1, WORK( N+1 ), 1,
     $                                       A( 1, q ), 1 )
                                 CALL DLASCL( 'G', 0, 0, ONE, AAQQ, M,
     $                                        1, A( 1, q ), LDA, IERR )
                                 SVA( q ) = AAQQ*DSQRT( MAX( ZERO,
     $                                      ONE-AAPQ*AAPQ ) )
                                 MXSINJ = MAX( MXSINJ, SFMIN )
                              END IF
*           END IF ROTOK THEN ... ELSE
*
*           In the case of cancellation in updating SVA(q), SVA(p)
*           recompute SVA(q), SVA(p).
*
                              IF( ( SVA( q ) / AAQQ )**2.LE.ROOTEPS )
     $                            THEN
                                 IF( ( AAQQ.LT.ROOTBIG ) .AND.
     $                               ( AAQQ.GT.ROOTSFMIN ) ) THEN
                                    SVA( q ) = DNRM2( M, A( 1, q ), 1 )*
     $                                         WORK( q )
                                 ELSE
                                    T = ZERO
                                    AAQQ = ONE
                                    CALL DLASSQ( M, A( 1, q ), 1, T,
     $                                           AAQQ )
                                    SVA( q ) = T*DSQRT( AAQQ )*WORK( q )
                                 END IF
                              END IF
                              IF( ( AAPP / AAPP0 ).LE.ROOTEPS ) THEN
                                 IF( ( AAPP.LT.ROOTBIG ) .AND.
     $                               ( AAPP.GT.ROOTSFMIN ) ) THEN
                                    AAPP = DNRM2( M, A( 1, p ), 1 )*
     $                                     WORK( p )
                                 ELSE
                                    T = ZERO
                                    AAPP = ONE
                                    CALL DLASSQ( M, A( 1, p ), 1, T,
     $                                           AAPP )
                                    AAPP = T*DSQRT( AAPP )*WORK( p )
                                 END IF
                                 SVA( p ) = AAPP
                              END IF
*
                           ELSE
*        A(:,p) and A(:,q) already numerically orthogonal
                              IF( ir1.EQ.0 )NOTROT = NOTROT + 1
*[RTD]      SKIPPED  = SKIPPED  + 1
                              PSKIPPED = PSKIPPED + 1
                           END IF
                        ELSE
*        A(:,q) is zero column
                           IF( ir1.EQ.0 )NOTROT = NOTROT + 1
                           PSKIPPED = PSKIPPED + 1
                        END IF
*
                        IF( ( i.LE.SWBAND ) .AND.
     $                      ( PSKIPPED.GT.ROWSKIP ) ) THEN
                           IF( ir1.EQ.0 )AAPP = -AAPP
                           NOTROT = 0
                           GO TO 2103
                        END IF
*
 2002                CONTINUE
*     END q-LOOP
*
 2103                CONTINUE
*     bailed out of q-loop
*
                     SVA( p ) = AAPP
*
                  ELSE
                     SVA( p ) = AAPP
                     IF( ( ir1.EQ.0 ) .AND. ( AAPP.EQ.ZERO ) )
     $                   NOTROT = NOTROT + MIN( igl+KBL-1, N ) - p
                  END IF
*
 2001          CONTINUE
*     end of the p-loop
*     end of doing the block ( ibr, ibr )
 1002       CONTINUE
*     end of ir1-loop
*
* ... go to the off diagonal blocks
*
            igl = ( ibr-1 )*KBL + 1
*
            DO 2010 jbc = ibr + 1, NBL
*
               jgl = ( jbc-1 )*KBL + 1
*
*        doing the block at ( ibr, jbc )
*
               IJBLSK = 0
               DO 2100 p = igl, MIN( igl+KBL-1, N )
*
                  AAPP = SVA( p )
                  IF( AAPP.GT.ZERO ) THEN
*
                     PSKIPPED = 0
*
                     DO 2200 q = jgl, MIN( jgl+KBL-1, N )
*
                        AAQQ = SVA( q )
                        IF( AAQQ.GT.ZERO ) THEN
                           AAPP0 = AAPP
*
*     .. M x 2 Jacobi SVD ..
*
*        Safe Gram matrix computation
*
                           IF( AAQQ.GE.ONE ) THEN
                              IF( AAPP.GE.AAQQ ) THEN
                                 ROTOK = ( SMALL*AAPP ).LE.AAQQ
                              ELSE
                                 ROTOK = ( SMALL*AAQQ ).LE.AAPP
                              END IF
                              IF( AAPP.LT.( BIG / AAQQ ) ) THEN
                                 AAPQ = ( DDOT( M, A( 1, p ), 1, A( 1,
     $                                  q ), 1 )*WORK( p )*WORK( q ) /
     $                                  AAQQ ) / AAPP
                              ELSE
                                 CALL DCOPY( M, A( 1, p ), 1,
     $                                       WORK( N+1 ), 1 )
                                 CALL DLASCL( 'G', 0, 0, AAPP,
     $                                        WORK( p ), M, 1,
     $                                        WORK( N+1 ), LDA, IERR )
                                 AAPQ = DDOT( M, WORK( N+1 ), 1,
     $                                  A( 1, q ), 1 )*WORK( q ) / AAQQ
                              END IF
                           ELSE
                              IF( AAPP.GE.AAQQ ) THEN
                                 ROTOK = AAPP.LE.( AAQQ / SMALL )
                              ELSE
                                 ROTOK = AAQQ.LE.( AAPP / SMALL )
                              END IF
                              IF( AAPP.GT.( SMALL / AAQQ ) ) THEN
                                 AAPQ = ( DDOT( M, A( 1, p ), 1, A( 1,
     $                                  q ), 1 )*WORK( p )*WORK( q ) /
     $                                  AAQQ ) / AAPP
                              ELSE
                                 CALL DCOPY( M, A( 1, q ), 1,
     $                                       WORK( N+1 ), 1 )
                                 CALL DLASCL( 'G', 0, 0, AAQQ,
     $                                        WORK( q ), M, 1,
     $                                        WORK( N+1 ), LDA, IERR )
                                 AAPQ = DDOT( M, WORK( N+1 ), 1,
     $                                  A( 1, p ), 1 )*WORK( p ) / AAPP
                              END IF
                           END IF
*
                           MXAAPQ = MAX( MXAAPQ, DABS( AAPQ ) )
*
*        TO rotate or NOT to rotate, THAT is the question ...
*
                           IF( DABS( AAPQ ).GT.TOL ) THEN
                              NOTROT = 0
*[RTD]      ROTATED  = ROTATED + 1
                              PSKIPPED = 0
                              ISWROT = ISWROT + 1
*
                              IF( ROTOK ) THEN
*
                                 AQOAP = AAQQ / AAPP
                                 APOAQ = AAPP / AAQQ
                                 THETA = -HALF*DABS(AQOAP-APOAQ)/AAPQ
                                 IF( AAQQ.GT.AAPP0 )THETA = -THETA
*
                                 IF( DABS( THETA ).GT.BIGTHETA ) THEN
                                    T = HALF / THETA
                                    FASTR( 3 ) = T*WORK( p ) / WORK( q )
                                    FASTR( 4 ) = -T*WORK( q ) /
     $                                           WORK( p )
                                    CALL DROTM( M, A( 1, p ), 1,
     $                                          A( 1, q ), 1, FASTR )
                                    IF( RSVEC )CALL DROTM( MVL,
     $                                              V( 1, p ), 1,
     $                                              V( 1, q ), 1,
     $                                              FASTR )
                                    SVA( q ) = AAQQ*DSQRT( MAX( ZERO,
     $                                         ONE+T*APOAQ*AAPQ ) )
                                    AAPP = AAPP*DSQRT( MAX( ZERO,
     $                                     ONE-T*AQOAP*AAPQ ) )
                                    MXSINJ = MAX( MXSINJ, DABS( T ) )
                                 ELSE
*
*                 .. choose correct signum for THETA and rotate
*
                                    THSIGN = -DSIGN( ONE, AAPQ )
                                    IF( AAQQ.GT.AAPP0 )THSIGN = -THSIGN
                                    T = ONE / ( THETA+THSIGN*
     $                                  DSQRT( ONE+THETA*THETA ) )
                                    CS = DSQRT( ONE / ( ONE+T*T ) )
                                    SN = T*CS
                                    MXSINJ = MAX( MXSINJ, DABS( SN ) )
                                    SVA( q ) = AAQQ*DSQRT( MAX( ZERO,
     $                                         ONE+T*APOAQ*AAPQ ) )
                                    AAPP = AAPP*DSQRT( MAX( ZERO,
     $                                     ONE-T*AQOAP*AAPQ ) )
*
                                    APOAQ = WORK( p ) / WORK( q )
                                    AQOAP = WORK( q ) / WORK( p )
                                    IF( WORK( p ).GE.ONE ) THEN
*
                                       IF( WORK( q ).GE.ONE ) THEN
                                          FASTR( 3 ) = T*APOAQ
                                          FASTR( 4 ) = -T*AQOAP
                                          WORK( p ) = WORK( p )*CS
                                          WORK( q ) = WORK( q )*CS
                                          CALL DROTM( M, A( 1, p ), 1,
     $                                                A( 1, q ), 1,
     $                                                FASTR )
                                          IF( RSVEC )CALL DROTM( MVL,
     $                                        V( 1, p ), 1, V( 1, q ),
     $                                        1, FASTR )
                                       ELSE
                                          CALL DAXPY( M, -T*AQOAP,
     $                                                A( 1, q ), 1,
     $                                                A( 1, p ), 1 )
                                          CALL DAXPY( M, CS*SN*APOAQ,
     $                                                A( 1, p ), 1,
     $                                                A( 1, q ), 1 )
                                          IF( RSVEC ) THEN
                                             CALL DAXPY( MVL, -T*AQOAP,
     $                                                   V( 1, q ), 1,
     $                                                   V( 1, p ), 1 )
                                             CALL DAXPY( MVL,
     $                                                   CS*SN*APOAQ,
     $                                                   V( 1, p ), 1,
     $                                                   V( 1, q ), 1 )
                                          END IF
                                          WORK( p ) = WORK( p )*CS
                                          WORK( q ) = WORK( q ) / CS
                                       END IF
                                    ELSE
                                       IF( WORK( q ).GE.ONE ) THEN
                                          CALL DAXPY( M, T*APOAQ,
     $                                                A( 1, p ), 1,
     $                                                A( 1, q ), 1 )
                                          CALL DAXPY( M, -CS*SN*AQOAP,
     $                                                A( 1, q ), 1,
     $                                                A( 1, p ), 1 )
                                          IF( RSVEC ) THEN
                                             CALL DAXPY( MVL, T*APOAQ,
     $                                                   V( 1, p ), 1,
     $                                                   V( 1, q ), 1 )
                                             CALL DAXPY( MVL,
     $                                                   -CS*SN*AQOAP,
     $                                                   V( 1, q ), 1,
     $                                                   V( 1, p ), 1 )
                                          END IF
                                          WORK( p ) = WORK( p ) / CS
                                          WORK( q ) = WORK( q )*CS
                                       ELSE
                                          IF( WORK( p ).GE.WORK( q ) )
     $                                        THEN
                                             CALL DAXPY( M, -T*AQOAP,
     $                                                   A( 1, q ), 1,
     $                                                   A( 1, p ), 1 )
                                             CALL DAXPY( M, CS*SN*APOAQ,
     $                                                   A( 1, p ), 1,
     $                                                   A( 1, q ), 1 )
                                             WORK( p ) = WORK( p )*CS
                                             WORK( q ) = WORK( q ) / CS
                                             IF( RSVEC ) THEN
                                                CALL DAXPY( MVL,
     $                                               -T*AQOAP,
     $                                               V( 1, q ), 1,
     $                                               V( 1, p ), 1 )
                                                CALL DAXPY( MVL,
     $                                               CS*SN*APOAQ,
     $                                               V( 1, p ), 1,
     $                                               V( 1, q ), 1 )
                                             END IF
                                          ELSE
                                             CALL DAXPY( M, T*APOAQ,
     $                                                   A( 1, p ), 1,
     $                                                   A( 1, q ), 1 )
                                             CALL DAXPY( M,
     $                                                   -CS*SN*AQOAP,
     $                                                   A( 1, q ), 1,
     $                                                   A( 1, p ), 1 )
                                             WORK( p ) = WORK( p ) / CS
                                             WORK( q ) = WORK( q )*CS
                                             IF( RSVEC ) THEN
                                                CALL DAXPY( MVL,
     $                                               T*APOAQ, V( 1, p ),
     $                                               1, V( 1, q ), 1 )
                                                CALL DAXPY( MVL,
     $                                               -CS*SN*AQOAP,
     $                                               V( 1, q ), 1,
     $                                               V( 1, p ), 1 )
                                             END IF
                                          END IF
                                       END IF
                                    END IF
                                 END IF
*
                              ELSE
                                 IF( AAPP.GT.AAQQ ) THEN
                                    CALL DCOPY( M, A( 1, p ), 1,
     $                                          WORK( N+1 ), 1 )
                                    CALL DLASCL( 'G', 0, 0, AAPP, ONE,
     $                                           M, 1, WORK( N+1 ), LDA,
     $                                           IERR )
                                    CALL DLASCL( 'G', 0, 0, AAQQ, ONE,
     $                                           M, 1, A( 1, q ), LDA,
     $                                           IERR )
                                    TEMP1 = -AAPQ*WORK( p ) / WORK( q )
                                    CALL DAXPY( M, TEMP1, WORK( N+1 ),
     $                                          1, A( 1, q ), 1 )
                                    CALL DLASCL( 'G', 0, 0, ONE, AAQQ,
     $                                           M, 1, A( 1, q ), LDA,
     $                                           IERR )
                                    SVA( q ) = AAQQ*DSQRT( MAX( ZERO,
     $                                         ONE-AAPQ*AAPQ ) )
                                    MXSINJ = MAX( MXSINJ, SFMIN )
                                 ELSE
                                    CALL DCOPY( M, A( 1, q ), 1,
     $                                          WORK( N+1 ), 1 )
                                    CALL DLASCL( 'G', 0, 0, AAQQ, ONE,
     $                                           M, 1, WORK( N+1 ), LDA,
     $                                           IERR )
                                    CALL DLASCL( 'G', 0, 0, AAPP, ONE,
     $                                           M, 1, A( 1, p ), LDA,
     $                                           IERR )
                                    TEMP1 = -AAPQ*WORK( q ) / WORK( p )
                                    CALL DAXPY( M, TEMP1, WORK( N+1 ),
     $                                          1, A( 1, p ), 1 )
                                    CALL DLASCL( 'G', 0, 0, ONE, AAPP,
     $                                           M, 1, A( 1, p ), LDA,
     $                                           IERR )
                                    SVA( p ) = AAPP*DSQRT( MAX( ZERO,
     $                                         ONE-AAPQ*AAPQ ) )
                                    MXSINJ = MAX( MXSINJ, SFMIN )
                                 END IF
                              END IF
*           END IF ROTOK THEN ... ELSE
*
*           In the case of cancellation in updating SVA(q)
*           .. recompute SVA(q)
                              IF( ( SVA( q ) / AAQQ )**2.LE.ROOTEPS )
     $                            THEN
                                 IF( ( AAQQ.LT.ROOTBIG ) .AND.
     $                               ( AAQQ.GT.ROOTSFMIN ) ) THEN
                                    SVA( q ) = DNRM2( M, A( 1, q ), 1 )*
     $                                         WORK( q )
                                 ELSE
                                    T = ZERO
                                    AAQQ = ONE
                                    CALL DLASSQ( M, A( 1, q ), 1, T,
     $                                           AAQQ )
                                    SVA( q ) = T*DSQRT( AAQQ )*WORK( q )
                                 END IF
                              END IF
                              IF( ( AAPP / AAPP0 )**2.LE.ROOTEPS ) THEN
                                 IF( ( AAPP.LT.ROOTBIG ) .AND.
     $                               ( AAPP.GT.ROOTSFMIN ) ) THEN
                                    AAPP = DNRM2( M, A( 1, p ), 1 )*
     $                                     WORK( p )
                                 ELSE
                                    T = ZERO
                                    AAPP = ONE
                                    CALL DLASSQ( M, A( 1, p ), 1, T,
     $                                           AAPP )
                                    AAPP = T*DSQRT( AAPP )*WORK( p )
                                 END IF
                                 SVA( p ) = AAPP
                              END IF
*              end of OK rotation
                           ELSE
                              NOTROT = NOTROT + 1
*[RTD]      SKIPPED  = SKIPPED  + 1
                              PSKIPPED = PSKIPPED + 1
                              IJBLSK = IJBLSK + 1
                           END IF
                        ELSE
                           NOTROT = NOTROT + 1
                           PSKIPPED = PSKIPPED + 1
                           IJBLSK = IJBLSK + 1
                        END IF
*
                        IF( ( i.LE.SWBAND ) .AND. ( IJBLSK.GE.BLSKIP ) )
     $                      THEN
                           SVA( p ) = AAPP
                           NOTROT = 0
                           GO TO 2011
                        END IF
                        IF( ( i.LE.SWBAND ) .AND.
     $                      ( PSKIPPED.GT.ROWSKIP ) ) THEN
                           AAPP = -AAPP
                           NOTROT = 0
                           GO TO 2203
                        END IF
*
 2200                CONTINUE
*        end of the q-loop
 2203                CONTINUE
*
                     SVA( p ) = AAPP
*
                  ELSE
*
                     IF( AAPP.EQ.ZERO )NOTROT = NOTROT +
     $                   MIN( jgl+KBL-1, N ) - jgl + 1
                     IF( AAPP.LT.ZERO )NOTROT = 0
*
                  END IF
*
 2100          CONTINUE
*     end of the p-loop
 2010       CONTINUE
*     end of the jbc-loop
 2011       CONTINUE
*2011 bailed out of the jbc-loop
            DO 2012 p = igl, MIN( igl+KBL-1, N )
               SVA( p ) = DABS( SVA( p ) )
 2012       CONTINUE
***
 2000    CONTINUE
*2000 :: end of the ibr-loop
*
*     .. update SVA(N)
         IF( ( SVA( N ).LT.ROOTBIG ) .AND. ( SVA( N ).GT.ROOTSFMIN ) )
     $       THEN
            SVA( N ) = DNRM2( M, A( 1, N ), 1 )*WORK( N )
         ELSE
            T = ZERO
            AAPP = ONE
            CALL DLASSQ( M, A( 1, N ), 1, T, AAPP )
            SVA( N ) = T*DSQRT( AAPP )*WORK( N )
         END IF
*
*     Additional steering devices
*
         IF( ( i.LT.SWBAND ) .AND. ( ( MXAAPQ.LE.ROOTTOL ) .OR.
     $       ( ISWROT.LE.N ) ) )SWBAND = i
*
         IF( ( i.GT.SWBAND+1 ) .AND. ( MXAAPQ.LT.DSQRT( DBLE( N ) )*
     $       TOL ) .AND. ( DBLE( N )*MXAAPQ*MXSINJ.LT.TOL ) ) THEN
            GO TO 1994
         END IF
*
         IF( NOTROT.GE.EMPTSW )GO TO 1994
*
 1993 CONTINUE
*     end i=1:NSWEEP loop
*
* #:( Reaching this point means that the procedure has not converged.
      INFO = NSWEEP - 1
      GO TO 1995
*
 1994 CONTINUE
* #:) Reaching this point means numerical convergence after the i-th
*     sweep.
*
      INFO = 0
* #:) INFO = 0 confirms successful iterations.
 1995 CONTINUE
*
*     Sort the singular values and find how many are above
*     the underflow threshold.
*
      N2 = 0
      N4 = 0
      DO 5991 p = 1, N - 1
         q = IDAMAX( N-p+1, SVA( p ), 1 ) + p - 1
         IF( p.NE.q ) THEN
            TEMP1 = SVA( p )
            SVA( p ) = SVA( q )
            SVA( q ) = TEMP1
            TEMP1 = WORK( p )
            WORK( p ) = WORK( q )
            WORK( q ) = TEMP1
            CALL DSWAP( M, A( 1, p ), 1, A( 1, q ), 1 )
            IF( RSVEC )CALL DSWAP( MVL, V( 1, p ), 1, V( 1, q ), 1 )
         END IF
         IF( SVA( p ).NE.ZERO ) THEN
            N4 = N4 + 1
            IF( SVA( p )*SKL.GT.SFMIN )N2 = N2 + 1
         END IF
 5991 CONTINUE
      IF( SVA( N ).NE.ZERO ) THEN
         N4 = N4 + 1
         IF( SVA( N )*SKL.GT.SFMIN )N2 = N2 + 1
      END IF
*
*     Normalize the left singular vectors.
*
      IF( LSVEC .OR. UCTOL ) THEN
         DO 1998 p = 1, N2
            CALL DSCAL( M, WORK( p ) / SVA( p ), A( 1, p ), 1 )
 1998    CONTINUE
      END IF
*
*     Scale the product of Jacobi rotations (assemble the fast rotations).
*
      IF( RSVEC ) THEN
         IF( APPLV ) THEN
            DO 2398 p = 1, N
               CALL DSCAL( MVL, WORK( p ), V( 1, p ), 1 )
 2398       CONTINUE
         ELSE
            DO 2399 p = 1, N
               TEMP1 = ONE / DNRM2( MVL, V( 1, p ), 1 )
               CALL DSCAL( MVL, TEMP1, V( 1, p ), 1 )
 2399       CONTINUE
         END IF
      END IF
*
*     Undo scaling, if necessary (and possible).
      IF( ( ( SKL.GT.ONE ) .AND. ( SVA( 1 ).LT.( BIG / SKL) ) )
     $    .OR. ( ( SKL.LT.ONE ) .AND. ( SVA( MAX( N2, 1 ) ) .GT.
     $    ( SFMIN / SKL) ) ) ) THEN
         DO 2400 p = 1, N
            SVA( P ) = SKL*SVA( P )
 2400    CONTINUE
         SKL= ONE
      END IF
*
      WORK( 1 ) = SKL
*     The singular values of A are SKL*SVA(1:N). If SKL.NE.ONE
*     then some of the singular values may overflow or underflow and
*     the spectrum is given in this factored representation.
*
      WORK( 2 ) = DBLE( N4 )
*     N4 is the number of computed nonzero singular values of A.
*
      WORK( 3 ) = DBLE( N2 )
*     N2 is the number of singular values of A greater than SFMIN.
*     If N2<N, SVA(N2:N) contains ZEROS and/or denormalized numbers
*     that may carry some information.
*
      WORK( 4 ) = DBLE( i )
*     i is the index of the last sweep before declaring convergence.
*
      WORK( 5 ) = MXAAPQ
*     MXAAPQ is the largest absolute value of scaled pivots in the
*     last sweep
*
      WORK( 6 ) = MXSINJ
*     MXSINJ is the largest absolute value of the sines of Jacobi angles
*     in the last sweep
*
      RETURN
*     ..
*     .. END OF DGESVJ
*     ..
      END
