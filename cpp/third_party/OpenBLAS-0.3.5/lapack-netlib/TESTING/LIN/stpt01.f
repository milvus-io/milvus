*> \brief \b STPT01
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE STPT01( UPLO, DIAG, N, AP, AINVP, RCOND, WORK, RESID )
*
*       .. Scalar Arguments ..
*       CHARACTER          DIAG, UPLO
*       INTEGER            N
*       REAL               RCOND, RESID
*       ..
*       .. Array Arguments ..
*       REAL               AINVP( * ), AP( * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> STPT01 computes the residual for a triangular matrix A times its
*> inverse when A is stored in packed format:
*>    RESID = norm(A*AINV - I) / ( N * norm(A) * norm(AINV) * EPS ),
*> where EPS is the machine epsilon.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          Specifies whether the matrix A is upper or lower triangular.
*>          = 'U':  Upper triangular
*>          = 'L':  Lower triangular
*> \endverbatim
*>
*> \param[in] DIAG
*> \verbatim
*>          DIAG is CHARACTER*1
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
*> \param[in] AP
*> \verbatim
*>          AP is REAL array, dimension (N*(N+1)/2)
*>          The original upper or lower triangular matrix A, packed
*>          columnwise in a linear array.  The j-th column of A is stored
*>          in the array AP as follows:
*>          if UPLO = 'U', AP((j-1)*j/2 + i) = A(i,j) for 1<=i<=j;
*>          if UPLO = 'L',
*>             AP((j-1)*(n-j) + j*(j+1)/2 + i-j) = A(i,j) for j<=i<=n.
*> \endverbatim
*>
*> \param[in,out] AINVP
*> \verbatim
*>          AINVP is REAL array, dimension (N*(N+1)/2)
*>          On entry, the (triangular) inverse of the matrix A, packed
*>          columnwise in a linear array as in AP.
*>          On exit, the contents of AINVP are destroyed.
*> \endverbatim
*>
*> \param[out] RCOND
*> \verbatim
*>          RCOND is REAL
*>          The reciprocal condition number of A, computed as
*>          1/(norm(A) * norm(AINV)).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (N)
*> \endverbatim
*>
*> \param[out] RESID
*> \verbatim
*>          RESID is REAL
*>          norm(A*AINV - I) / ( N * norm(A) * norm(AINV) * EPS )
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
*> \ingroup single_lin
*
*  =====================================================================
      SUBROUTINE STPT01( UPLO, DIAG, N, AP, AINVP, RCOND, WORK, RESID )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          DIAG, UPLO
      INTEGER            N
      REAL               RCOND, RESID
*     ..
*     .. Array Arguments ..
      REAL               AINVP( * ), AP( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            UNITD
      INTEGER            J, JC
      REAL               AINVNM, ANORM, EPS
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      REAL               SLAMCH, SLANTP
      EXTERNAL           LSAME, SLAMCH, SLANTP
*     ..
*     .. External Subroutines ..
      EXTERNAL           STPMV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          REAL
*     ..
*     .. Executable Statements ..
*
*     Quick exit if N = 0.
*
      IF( N.LE.0 ) THEN
         RCOND = ONE
         RESID = ZERO
         RETURN
      END IF
*
*     Exit with RESID = 1/EPS if ANORM = 0 or AINVNM = 0.
*
      EPS = SLAMCH( 'Epsilon' )
      ANORM = SLANTP( '1', UPLO, DIAG, N, AP, WORK )
      AINVNM = SLANTP( '1', UPLO, DIAG, N, AINVP, WORK )
      IF( ANORM.LE.ZERO .OR. AINVNM.LE.ZERO ) THEN
         RCOND = ZERO
         RESID = ONE / EPS
         RETURN
      END IF
      RCOND = ( ONE / ANORM ) / AINVNM
*
*     Compute A * AINV, overwriting AINV.
*
      UNITD = LSAME( DIAG, 'U' )
      IF( LSAME( UPLO, 'U' ) ) THEN
         JC = 1
         DO 10 J = 1, N
            IF( UNITD )
     $         AINVP( JC+J-1 ) = ONE
*
*           Form the j-th column of A*AINV
*
            CALL STPMV( 'Upper', 'No transpose', DIAG, J, AP,
     $                  AINVP( JC ), 1 )
*
*           Subtract 1 from the diagonal
*
            AINVP( JC+J-1 ) = AINVP( JC+J-1 ) - ONE
            JC = JC + J
   10    CONTINUE
      ELSE
         JC = 1
         DO 20 J = 1, N
            IF( UNITD )
     $         AINVP( JC ) = ONE
*
*           Form the j-th column of A*AINV
*
            CALL STPMV( 'Lower', 'No transpose', DIAG, N-J+1, AP( JC ),
     $                  AINVP( JC ), 1 )
*
*           Subtract 1 from the diagonal
*
            AINVP( JC ) = AINVP( JC ) - ONE
            JC = JC + N - J + 1
   20    CONTINUE
      END IF
*
*     Compute norm(A*AINV - I) / (N * norm(A) * norm(AINV) * EPS)
*
      RESID = SLANTP( '1', UPLO, 'Non-unit', N, AINVP, WORK )
*
      RESID = ( ( RESID*RCOND ) / REAL( N ) ) / EPS
*
      RETURN
*
*     End of STPT01
*
      END
