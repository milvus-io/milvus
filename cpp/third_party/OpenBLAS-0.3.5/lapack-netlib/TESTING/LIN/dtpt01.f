*> \brief \b DTPT01
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DTPT01( UPLO, DIAG, N, AP, AINVP, RCOND, WORK, RESID )
*
*       .. Scalar Arguments ..
*       CHARACTER          DIAG, UPLO
*       INTEGER            N
*       DOUBLE PRECISION   RCOND, RESID
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   AINVP( * ), AP( * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DTPT01 computes the residual for a triangular matrix A times its
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
*>          AP is DOUBLE PRECISION array, dimension (N*(N+1)/2)
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
*>          AINVP is DOUBLE PRECISION array, dimension (N*(N+1)/2)
*>          On entry, the (triangular) inverse of the matrix A, packed
*>          columnwise in a linear array as in AP.
*>          On exit, the contents of AINVP are destroyed.
*> \endverbatim
*>
*> \param[out] RCOND
*> \verbatim
*>          RCOND is DOUBLE PRECISION
*>          The reciprocal condition number of A, computed as
*>          1/(norm(A) * norm(AINV)).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (N)
*> \endverbatim
*>
*> \param[out] RESID
*> \verbatim
*>          RESID is DOUBLE PRECISION
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
*> \ingroup double_lin
*
*  =====================================================================
      SUBROUTINE DTPT01( UPLO, DIAG, N, AP, AINVP, RCOND, WORK, RESID )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          DIAG, UPLO
      INTEGER            N
      DOUBLE PRECISION   RCOND, RESID
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   AINVP( * ), AP( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            UNITD
      INTEGER            J, JC
      DOUBLE PRECISION   AINVNM, ANORM, EPS
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      DOUBLE PRECISION   DLAMCH, DLANTP
      EXTERNAL           LSAME, DLAMCH, DLANTP
*     ..
*     .. External Subroutines ..
      EXTERNAL           DTPMV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE
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
      EPS = DLAMCH( 'Epsilon' )
      ANORM = DLANTP( '1', UPLO, DIAG, N, AP, WORK )
      AINVNM = DLANTP( '1', UPLO, DIAG, N, AINVP, WORK )
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
            CALL DTPMV( 'Upper', 'No transpose', DIAG, J, AP,
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
            CALL DTPMV( 'Lower', 'No transpose', DIAG, N-J+1, AP( JC ),
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
      RESID = DLANTP( '1', UPLO, 'Non-unit', N, AINVP, WORK )
*
      RESID = ( ( RESID*RCOND ) / DBLE( N ) ) / EPS
*
      RETURN
*
*     End of DTPT01
*
      END
