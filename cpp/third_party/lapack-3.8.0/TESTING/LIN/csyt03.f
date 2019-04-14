*> \brief \b CSYT03
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CSYT03( UPLO, N, A, LDA, AINV, LDAINV, WORK, LDWORK,
*                          RWORK, RCOND, RESID )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            LDA, LDAINV, LDWORK, N
*       REAL               RCOND, RESID
*       ..
*       .. Array Arguments ..
*       REAL               RWORK( * )
*       COMPLEX            A( LDA, * ), AINV( LDAINV, * ),
*      $                   WORK( LDWORK, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CSYT03 computes the residual for a complex symmetric matrix times
*> its inverse:
*>    norm( I - A*AINV ) / ( N * norm(A) * norm(AINV) * EPS )
*> where EPS is the machine epsilon.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          Specifies whether the upper or lower triangular part of the
*>          complex symmetric matrix A is stored:
*>          = 'U':  Upper triangular
*>          = 'L':  Lower triangular
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of rows and columns of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N)
*>          The original complex symmetric matrix A.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N)
*> \endverbatim
*>
*> \param[in,out] AINV
*> \verbatim
*>          AINV is COMPLEX array, dimension (LDAINV,N)
*>          On entry, the inverse of the matrix A, stored as a symmetric
*>          matrix in the same format as A.
*>          In this version, AINV is expanded into a full matrix and
*>          multiplied by A, so the opposing triangle of AINV will be
*>          changed; i.e., if the upper triangular part of AINV is
*>          stored, the lower triangular part will be used as work space.
*> \endverbatim
*>
*> \param[in] LDAINV
*> \verbatim
*>          LDAINV is INTEGER
*>          The leading dimension of the array AINV.  LDAINV >= max(1,N).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX array, dimension (LDWORK,N)
*> \endverbatim
*>
*> \param[in] LDWORK
*> \verbatim
*>          LDWORK is INTEGER
*>          The leading dimension of the array WORK.  LDWORK >= max(1,N).
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is REAL array, dimension (N)
*> \endverbatim
*>
*> \param[out] RCOND
*> \verbatim
*>          RCOND is REAL
*>          The reciprocal of the condition number of A, computed as
*>          RCOND = 1/ (norm(A) * norm(AINV)).
*> \endverbatim
*>
*> \param[out] RESID
*> \verbatim
*>          RESID is REAL
*>          norm(I - A*AINV) / ( N * norm(A) * norm(AINV) * EPS )
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
      SUBROUTINE CSYT03( UPLO, N, A, LDA, AINV, LDAINV, WORK, LDWORK,
     $                   RWORK, RCOND, RESID )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            LDA, LDAINV, LDWORK, N
      REAL               RCOND, RESID
*     ..
*     .. Array Arguments ..
      REAL               RWORK( * )
      COMPLEX            A( LDA, * ), AINV( LDAINV, * ),
     $                   WORK( LDWORK, * )
*     ..
*
*  =====================================================================
*
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
      COMPLEX            CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0E+0, 0.0E+0 ),
     $                   CONE = ( 1.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      INTEGER            I, J
      REAL               AINVNM, ANORM, EPS
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      REAL               CLANGE, CLANSY, SLAMCH
      EXTERNAL           LSAME, CLANGE, CLANSY, SLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           CSYMM
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          REAL
*     ..
*     .. Executable Statements ..
*
*     Quick exit if N = 0
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
      ANORM = CLANSY( '1', UPLO, N, A, LDA, RWORK )
      AINVNM = CLANSY( '1', UPLO, N, AINV, LDAINV, RWORK )
      IF( ANORM.LE.ZERO .OR. AINVNM.LE.ZERO ) THEN
         RCOND = ZERO
         RESID = ONE / EPS
         RETURN
      END IF
      RCOND = ( ONE/ANORM ) / AINVNM
*
*     Expand AINV into a full matrix and call CSYMM to multiply
*     AINV on the left by A (store the result in WORK).
*
      IF( LSAME( UPLO, 'U' ) ) THEN
         DO 20 J = 1, N
            DO 10 I = 1, J - 1
               AINV( J, I ) = AINV( I, J )
   10       CONTINUE
   20    CONTINUE
      ELSE
         DO 40 J = 1, N
            DO 30 I = J + 1, N
               AINV( J, I ) = AINV( I, J )
   30       CONTINUE
   40    CONTINUE
      END IF
      CALL CSYMM( 'Left', UPLO, N, N, -CONE, A, LDA, AINV, LDAINV,
     $            CZERO, WORK, LDWORK )
*
*     Add the identity matrix to WORK .
*
      DO 50 I = 1, N
         WORK( I, I ) = WORK( I, I ) + CONE
   50 CONTINUE
*
*     Compute norm(I - A*AINV) / (N * norm(A) * norm(AINV) * EPS)
*
      RESID = CLANGE( '1', N, N, WORK, LDWORK, RWORK )
*
      RESID = ( ( RESID*RCOND )/EPS ) / REAL( N )
*
      RETURN
*
*     End of CSYT03
*
      END
