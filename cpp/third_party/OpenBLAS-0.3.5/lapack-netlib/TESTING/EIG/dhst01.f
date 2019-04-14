*> \brief \b DHST01
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DHST01( N, ILO, IHI, A, LDA, H, LDH, Q, LDQ, WORK,
*                          LWORK, RESULT )
*
*       .. Scalar Arguments ..
*       INTEGER            IHI, ILO, LDA, LDH, LDQ, LWORK, N
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   A( LDA, * ), H( LDH, * ), Q( LDQ, * ),
*      $                   RESULT( 2 ), WORK( LWORK )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DHST01 tests the reduction of a general matrix A to upper Hessenberg
*> form:  A = Q*H*Q'.  Two test ratios are computed;
*>
*> RESULT(1) = norm( A - Q*H*Q' ) / ( norm(A) * N * EPS )
*> RESULT(2) = norm( I - Q'*Q ) / ( N * EPS )
*>
*> The matrix Q is assumed to be given explicitly as it would be
*> following DGEHRD + DORGHR.
*>
*> In this version, ILO and IHI are not used and are assumed to be 1 and
*> N, respectively.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] ILO
*> \verbatim
*>          ILO is INTEGER
*> \endverbatim
*>
*> \param[in] IHI
*> \verbatim
*>          IHI is INTEGER
*>
*>          A is assumed to be upper triangular in rows and columns
*>          1:ILO-1 and IHI+1:N, so Q differs from the identity only in
*>          rows and columns ILO+1:IHI.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension (LDA,N)
*>          The original n by n matrix A.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[in] H
*> \verbatim
*>          H is DOUBLE PRECISION array, dimension (LDH,N)
*>          The upper Hessenberg matrix H from the reduction A = Q*H*Q'
*>          as computed by DGEHRD.  H is assumed to be zero below the
*>          first subdiagonal.
*> \endverbatim
*>
*> \param[in] LDH
*> \verbatim
*>          LDH is INTEGER
*>          The leading dimension of the array H.  LDH >= max(1,N).
*> \endverbatim
*>
*> \param[in] Q
*> \verbatim
*>          Q is DOUBLE PRECISION array, dimension (LDQ,N)
*>          The orthogonal matrix Q from the reduction A = Q*H*Q' as
*>          computed by DGEHRD + DORGHR.
*> \endverbatim
*>
*> \param[in] LDQ
*> \verbatim
*>          LDQ is INTEGER
*>          The leading dimension of the array Q.  LDQ >= max(1,N).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (LWORK)
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The length of the array WORK.  LWORK >= 2*N*N.
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is DOUBLE PRECISION array, dimension (2)
*>          RESULT(1) = norm( A - Q*H*Q' ) / ( norm(A) * N * EPS )
*>          RESULT(2) = norm( I - Q'*Q ) / ( N * EPS )
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
*> \date December 2016
*
*> \ingroup double_eig
*
*  =====================================================================
      SUBROUTINE DHST01( N, ILO, IHI, A, LDA, H, LDH, Q, LDQ, WORK,
     $                   LWORK, RESULT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            IHI, ILO, LDA, LDH, LDQ, LWORK, N
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   A( LDA, * ), H( LDH, * ), Q( LDQ, * ),
     $                   RESULT( 2 ), WORK( LWORK )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ONE, ZERO
      PARAMETER          ( ONE = 1.0D+0, ZERO = 0.0D+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            LDWORK
      DOUBLE PRECISION   ANORM, EPS, OVFL, SMLNUM, UNFL, WNORM
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH, DLANGE
      EXTERNAL           DLAMCH, DLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           DGEMM, DLABAD, DLACPY, DORT01
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Quick return if possible
*
      IF( N.LE.0 ) THEN
         RESULT( 1 ) = ZERO
         RESULT( 2 ) = ZERO
         RETURN
      END IF
*
      UNFL = DLAMCH( 'Safe minimum' )
      EPS = DLAMCH( 'Precision' )
      OVFL = ONE / UNFL
      CALL DLABAD( UNFL, OVFL )
      SMLNUM = UNFL*N / EPS
*
*     Test 1:  Compute norm( A - Q*H*Q' ) / ( norm(A) * N * EPS )
*
*     Copy A to WORK
*
      LDWORK = MAX( 1, N )
      CALL DLACPY( ' ', N, N, A, LDA, WORK, LDWORK )
*
*     Compute Q*H
*
      CALL DGEMM( 'No transpose', 'No transpose', N, N, N, ONE, Q, LDQ,
     $            H, LDH, ZERO, WORK( LDWORK*N+1 ), LDWORK )
*
*     Compute A - Q*H*Q'
*
      CALL DGEMM( 'No transpose', 'Transpose', N, N, N, -ONE,
     $            WORK( LDWORK*N+1 ), LDWORK, Q, LDQ, ONE, WORK,
     $            LDWORK )
*
      ANORM = MAX( DLANGE( '1', N, N, A, LDA, WORK( LDWORK*N+1 ) ),
     $        UNFL )
      WNORM = DLANGE( '1', N, N, WORK, LDWORK, WORK( LDWORK*N+1 ) )
*
*     Note that RESULT(1) cannot overflow and is bounded by 1/(N*EPS)
*
      RESULT( 1 ) = MIN( WNORM, ANORM ) / MAX( SMLNUM, ANORM*EPS ) / N
*
*     Test 2:  Compute norm( I - Q'*Q ) / ( N * EPS )
*
      CALL DORT01( 'Columns', N, N, Q, LDQ, WORK, LWORK, RESULT( 2 ) )
*
      RETURN
*
*     End of DHST01
*
      END
