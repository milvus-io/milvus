*> \brief \b DPPT03
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DPPT03( UPLO, N, A, AINV, WORK, LDWORK, RWORK, RCOND,
*                          RESID )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            LDWORK, N
*       DOUBLE PRECISION   RCOND, RESID
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   A( * ), AINV( * ), RWORK( * ),
*      $                   WORK( LDWORK, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DPPT03 computes the residual for a symmetric packed matrix times its
*> inverse:
*>    norm( I - A*AINV ) / ( N * norm(A) * norm(AINV) * EPS ),
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
*>          symmetric matrix A is stored:
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
*>          A is DOUBLE PRECISION array, dimension (N*(N+1)/2)
*>          The original symmetric matrix A, stored as a packed
*>          triangular matrix.
*> \endverbatim
*>
*> \param[in] AINV
*> \verbatim
*>          AINV is DOUBLE PRECISION array, dimension (N*(N+1)/2)
*>          The (symmetric) inverse of the matrix A, stored as a packed
*>          triangular matrix.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (LDWORK,N)
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
*>          RWORK is DOUBLE PRECISION array, dimension (N)
*> \endverbatim
*>
*> \param[out] RCOND
*> \verbatim
*>          RCOND is DOUBLE PRECISION
*>          The reciprocal of the condition number of A, computed as
*>          ( 1/norm(A) ) / norm(AINV).
*> \endverbatim
*>
*> \param[out] RESID
*> \verbatim
*>          RESID is DOUBLE PRECISION
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
*> \ingroup double_lin
*
*  =====================================================================
      SUBROUTINE DPPT03( UPLO, N, A, AINV, WORK, LDWORK, RWORK, RCOND,
     $                   RESID )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            LDWORK, N
      DOUBLE PRECISION   RCOND, RESID
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   A( * ), AINV( * ), RWORK( * ),
     $                   WORK( LDWORK, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, J, JJ
      DOUBLE PRECISION   AINVNM, ANORM, EPS
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      DOUBLE PRECISION   DLAMCH, DLANGE, DLANSP
      EXTERNAL           LSAME, DLAMCH, DLANGE, DLANSP
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE
*     ..
*     .. External Subroutines ..
      EXTERNAL           DCOPY, DSPMV
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
      ANORM = DLANSP( '1', UPLO, N, A, RWORK )
      AINVNM = DLANSP( '1', UPLO, N, AINV, RWORK )
      IF( ANORM.LE.ZERO .OR. AINVNM.EQ.ZERO ) THEN
         RCOND = ZERO
         RESID = ONE / EPS
         RETURN
      END IF
      RCOND = ( ONE / ANORM ) / AINVNM
*
*     UPLO = 'U':
*     Copy the leading N-1 x N-1 submatrix of AINV to WORK(1:N,2:N) and
*     expand it to a full matrix, then multiply by A one column at a
*     time, moving the result one column to the left.
*
      IF( LSAME( UPLO, 'U' ) ) THEN
*
*        Copy AINV
*
         JJ = 1
         DO 10 J = 1, N - 1
            CALL DCOPY( J, AINV( JJ ), 1, WORK( 1, J+1 ), 1 )
            CALL DCOPY( J-1, AINV( JJ ), 1, WORK( J, 2 ), LDWORK )
            JJ = JJ + J
   10    CONTINUE
         JJ = ( ( N-1 )*N ) / 2 + 1
         CALL DCOPY( N-1, AINV( JJ ), 1, WORK( N, 2 ), LDWORK )
*
*        Multiply by A
*
         DO 20 J = 1, N - 1
            CALL DSPMV( 'Upper', N, -ONE, A, WORK( 1, J+1 ), 1, ZERO,
     $                  WORK( 1, J ), 1 )
   20    CONTINUE
         CALL DSPMV( 'Upper', N, -ONE, A, AINV( JJ ), 1, ZERO,
     $               WORK( 1, N ), 1 )
*
*     UPLO = 'L':
*     Copy the trailing N-1 x N-1 submatrix of AINV to WORK(1:N,1:N-1)
*     and multiply by A, moving each column to the right.
*
      ELSE
*
*        Copy AINV
*
         CALL DCOPY( N-1, AINV( 2 ), 1, WORK( 1, 1 ), LDWORK )
         JJ = N + 1
         DO 30 J = 2, N
            CALL DCOPY( N-J+1, AINV( JJ ), 1, WORK( J, J-1 ), 1 )
            CALL DCOPY( N-J, AINV( JJ+1 ), 1, WORK( J, J ), LDWORK )
            JJ = JJ + N - J + 1
   30    CONTINUE
*
*        Multiply by A
*
         DO 40 J = N, 2, -1
            CALL DSPMV( 'Lower', N, -ONE, A, WORK( 1, J-1 ), 1, ZERO,
     $                  WORK( 1, J ), 1 )
   40    CONTINUE
         CALL DSPMV( 'Lower', N, -ONE, A, AINV( 1 ), 1, ZERO,
     $               WORK( 1, 1 ), 1 )
*
      END IF
*
*     Add the identity matrix to WORK .
*
      DO 50 I = 1, N
         WORK( I, I ) = WORK( I, I ) + ONE
   50 CONTINUE
*
*     Compute norm(I - A*AINV) / (N * norm(A) * norm(AINV) * EPS)
*
      RESID = DLANGE( '1', N, N, WORK, LDWORK, RWORK )
*
      RESID = ( ( RESID*RCOND ) / EPS ) / DBLE( N )
*
      RETURN
*
*     End of DPPT03
*
      END
