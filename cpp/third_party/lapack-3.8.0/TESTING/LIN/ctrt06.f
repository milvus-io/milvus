*> \brief \b CTRT06
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CTRT06( RCOND, RCONDC, UPLO, DIAG, N, A, LDA, RWORK,
*                          RAT )
*
*       .. Scalar Arguments ..
*       CHARACTER          DIAG, UPLO
*       INTEGER            LDA, N
*       REAL               RAT, RCOND, RCONDC
*       ..
*       .. Array Arguments ..
*       REAL               RWORK( * )
*       COMPLEX            A( LDA, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CTRT06 computes a test ratio comparing RCOND (the reciprocal
*> condition number of a triangular matrix A) and RCONDC, the estimate
*> computed by CTRCON.  Information about the triangular matrix A is
*> used if one estimate is zero and the other is non-zero to decide if
*> underflow in the estimate is justified.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] RCOND
*> \verbatim
*>          RCOND is REAL
*>          The estimate of the reciprocal condition number obtained by
*>          forming the explicit inverse of the matrix A and computing
*>          RCOND = 1/( norm(A) * norm(inv(A)) ).
*> \endverbatim
*>
*> \param[in] RCONDC
*> \verbatim
*>          RCONDC is REAL
*>          The estimate of the reciprocal condition number computed by
*>          CTRCON.
*> \endverbatim
*>
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER
*>          Specifies whether the matrix A is upper or lower triangular.
*>          = 'U':  Upper triangular
*>          = 'L':  Lower triangular
*> \endverbatim
*>
*> \param[in] DIAG
*> \verbatim
*>          DIAG is CHARACTER
*>          Specifies whether or not the matrix A is unit triangular.
*>          = 'N':  Non-unit triangular
*>          = 'U':  Unit triangular
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N)
*>          The triangular matrix A.  If UPLO = 'U', the leading n by n
*>          upper triangular part of the array A contains the upper
*>          triangular matrix, and the strictly lower triangular part of
*>          A is not referenced.  If UPLO = 'L', the leading n by n lower
*>          triangular part of the array A contains the lower triangular
*>          matrix, and the strictly upper triangular part of A is not
*>          referenced.  If DIAG = 'U', the diagonal elements of A are
*>          also not referenced and are assumed to be 1.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is REAL array, dimension (N)
*> \endverbatim
*>
*> \param[out] RAT
*> \verbatim
*>          RAT is REAL
*>          The test ratio.  If both RCOND and RCONDC are nonzero,
*>             RAT = MAX( RCOND, RCONDC )/MIN( RCOND, RCONDC ) - 1.
*>          If RAT = 0, the two estimates are exactly the same.
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
*> \ingroup complex_lin
*
*  =====================================================================
      SUBROUTINE CTRT06( RCOND, RCONDC, UPLO, DIAG, N, A, LDA, RWORK,
     $                   RAT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          DIAG, UPLO
      INTEGER            LDA, N
      REAL               RAT, RCOND, RCONDC
*     ..
*     .. Array Arguments ..
      REAL               RWORK( * )
      COMPLEX            A( LDA, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
*     ..
*     .. Local Scalars ..
      REAL               ANORM, BIGNUM, EPS, RMAX, RMIN
*     ..
*     .. External Functions ..
      REAL               CLANTR, SLAMCH
      EXTERNAL           CLANTR, SLAMCH
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN
*     ..
*     .. Executable Statements ..
*
      EPS = SLAMCH( 'Epsilon' )
      RMAX = MAX( RCOND, RCONDC )
      RMIN = MIN( RCOND, RCONDC )
*
*     Do the easy cases first.
*
      IF( RMIN.LT.ZERO ) THEN
*
*        Invalid value for RCOND or RCONDC, return 1/EPS.
*
         RAT = ONE / EPS
*
      ELSE IF( RMIN.GT.ZERO ) THEN
*
*        Both estimates are positive, return RMAX/RMIN - 1.
*
         RAT = RMAX / RMIN - ONE
*
      ELSE IF( RMAX.EQ.ZERO ) THEN
*
*        Both estimates zero.
*
         RAT = ZERO
*
      ELSE
*
*        One estimate is zero, the other is non-zero.  If the matrix is
*        ill-conditioned, return the nonzero estimate multiplied by
*        1/EPS; if the matrix is badly scaled, return the nonzero
*        estimate multiplied by BIGNUM/TMAX, where TMAX is the maximum
*        element in absolute value in A.
*
         BIGNUM = ONE / SLAMCH( 'Safe minimum' )
         ANORM = CLANTR( 'M', UPLO, DIAG, N, N, A, LDA, RWORK )
*
         RAT = RMAX*( MIN( BIGNUM / MAX( ONE, ANORM ), ONE / EPS ) )
      END IF
*
      RETURN
*
*     End of CTRT06
*
      END
