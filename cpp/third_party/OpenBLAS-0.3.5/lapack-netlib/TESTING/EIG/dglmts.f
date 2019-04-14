*> \brief \b DGLMTS
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DGLMTS( N, M, P, A, AF, LDA, B, BF, LDB, D, DF, X, U,
*                          WORK, LWORK, RWORK, RESULT )
*
*       .. Scalar Arguments ..
*       INTEGER            LDA, LDB, LWORK, M, N, P
*       DOUBLE PRECISION   RESULT
*       ..
*       .. Array Arguments ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DGLMTS tests DGGGLM - a subroutine for solving the generalized
*> linear model problem.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of rows of the matrices A and B.  N >= 0.
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of columns of the matrix A.  M >= 0.
*> \endverbatim
*>
*> \param[in] P
*> \verbatim
*>          P is INTEGER
*>          The number of columns of the matrix B.  P >= 0.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension (LDA,M)
*>          The N-by-M matrix A.
*> \endverbatim
*>
*> \param[out] AF
*> \verbatim
*>          AF is DOUBLE PRECISION array, dimension (LDA,M)
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the arrays A, AF. LDA >= max(M,N).
*> \endverbatim
*>
*> \param[in] B
*> \verbatim
*>          B is DOUBLE PRECISION array, dimension (LDB,P)
*>          The N-by-P matrix A.
*> \endverbatim
*>
*> \param[out] BF
*> \verbatim
*>          BF is DOUBLE PRECISION array, dimension (LDB,P)
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the arrays B, BF. LDB >= max(P,N).
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension( N )
*>          On input, the left hand side of the GLM.
*> \endverbatim
*>
*> \param[out] DF
*> \verbatim
*>          DF is DOUBLE PRECISION array, dimension( N )
*> \endverbatim
*>
*> \param[out] X
*> \verbatim
*>          X is DOUBLE PRECISION array, dimension( M )
*>          solution vector X in the GLM problem.
*> \endverbatim
*>
*> \param[out] U
*> \verbatim
*>          U is DOUBLE PRECISION array, dimension( P )
*>          solution vector U in the GLM problem.
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
*>          The dimension of the array WORK.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (M)
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is DOUBLE PRECISION
*>          The test ratio:
*>                           norm( d - A*x - B*u )
*>            RESULT = -----------------------------------------
*>                     (norm(A)+norm(B))*(norm(x)+norm(u))*EPS
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
      SUBROUTINE DGLMTS( N, M, P, A, AF, LDA, B, BF, LDB, D, DF, X, U,
     $                   WORK, LWORK, RWORK, RESULT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            LDA, LDB, LWORK, M, N, P
      DOUBLE PRECISION   RESULT
*     ..
*     .. Array Arguments ..
*
*  ====================================================================
*
      DOUBLE PRECISION   A( LDA, * ), AF( LDA, * ), B( LDB, * ),
     $                   BF( LDB, * ), D( * ), DF( * ), RWORK( * ),
     $                   U( * ), WORK( LWORK ), X( * )
*     ..
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            INFO
      DOUBLE PRECISION   ANORM, BNORM, DNORM, EPS, UNFL, XNORM, YNORM
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DASUM, DLAMCH, DLANGE
      EXTERNAL           DASUM, DLAMCH, DLANGE
*     ..
*     .. External Subroutines ..
*
      EXTERNAL           DCOPY, DGEMV, DGGGLM, DLACPY
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX
*     ..
*     .. Executable Statements ..
*
      EPS = DLAMCH( 'Epsilon' )
      UNFL = DLAMCH( 'Safe minimum' )
      ANORM = MAX( DLANGE( '1', N, M, A, LDA, RWORK ), UNFL )
      BNORM = MAX( DLANGE( '1', N, P, B, LDB, RWORK ), UNFL )
*
*     Copy the matrices A and B to the arrays AF and BF,
*     and the vector D the array DF.
*
      CALL DLACPY( 'Full', N, M, A, LDA, AF, LDA )
      CALL DLACPY( 'Full', N, P, B, LDB, BF, LDB )
      CALL DCOPY( N, D, 1, DF, 1 )
*
*     Solve GLM problem
*
      CALL DGGGLM( N, M, P, AF, LDA, BF, LDB, DF, X, U, WORK, LWORK,
     $             INFO )
*
*     Test the residual for the solution of LSE
*
*                       norm( d - A*x - B*u )
*       RESULT = -----------------------------------------
*                (norm(A)+norm(B))*(norm(x)+norm(u))*EPS
*
      CALL DCOPY( N, D, 1, DF, 1 )
      CALL DGEMV( 'No transpose', N, M, -ONE, A, LDA, X, 1, ONE, DF, 1 )
*
      CALL DGEMV( 'No transpose', N, P, -ONE, B, LDB, U, 1, ONE, DF, 1 )
*
      DNORM = DASUM( N, DF, 1 )
      XNORM = DASUM( M, X, 1 ) + DASUM( P, U, 1 )
      YNORM = ANORM + BNORM
*
      IF( XNORM.LE.ZERO ) THEN
         RESULT = ZERO
      ELSE
         RESULT = ( ( DNORM / YNORM ) / XNORM ) / EPS
      END IF
*
      RETURN
*
*     End of DGLMTS
*
      END
