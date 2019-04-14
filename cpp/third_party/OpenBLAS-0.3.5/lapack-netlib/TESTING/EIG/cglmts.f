*> \brief \b CGLMTS
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CGLMTS( N, M, P, A, AF, LDA, B, BF, LDB, D, DF,
*                          X, U, WORK, LWORK, RWORK, RESULT )
*
*       .. Scalar Arguments ..
*       INTEGER            LDA, LDB, LWORK, M, P, N
*       REAL               RESULT
*       ..
*       .. Array Arguments ..
*       REAL               RWORK( * )
*       COMPLEX            A( LDA, * ), AF( LDA, * ), B( LDB, * ),
*      $                   BF( LDB, * ), D( * ), DF( * ), U( * ),
*      $                   WORK( LWORK ), X( * )
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CGLMTS tests CGGGLM - a subroutine for solving the generalized
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
*>          A is COMPLEX array, dimension (LDA,M)
*>          The N-by-M matrix A.
*> \endverbatim
*>
*> \param[out] AF
*> \verbatim
*>          AF is COMPLEX array, dimension (LDA,M)
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
*>          B is COMPLEX array, dimension (LDB,P)
*>          The N-by-P matrix A.
*> \endverbatim
*>
*> \param[out] BF
*> \verbatim
*>          BF is COMPLEX array, dimension (LDB,P)
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
*>          D is COMPLEX array, dimension( N )
*>          On input, the left hand side of the GLM.
*> \endverbatim
*>
*> \param[out] DF
*> \verbatim
*>          DF is COMPLEX array, dimension( N )
*> \endverbatim
*>
*> \param[out] X
*> \verbatim
*>          X is COMPLEX array, dimension( M )
*>          solution vector X in the GLM problem.
*> \endverbatim
*>
*> \param[out] U
*> \verbatim
*>          U is COMPLEX array, dimension( P )
*>          solution vector U in the GLM problem.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX array, dimension (LWORK)
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
*>          RWORK is REAL array, dimension (M)
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is REAL
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
*> \ingroup complex_eig
*
*  =====================================================================
      SUBROUTINE CGLMTS( N, M, P, A, AF, LDA, B, BF, LDB, D, DF,
     $                   X, U, WORK, LWORK, RWORK, RESULT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            LDA, LDB, LWORK, M, P, N
      REAL               RESULT
*     ..
*     .. Array Arguments ..
      REAL               RWORK( * )
      COMPLEX            A( LDA, * ), AF( LDA, * ), B( LDB, * ),
     $                   BF( LDB, * ), D( * ), DF( * ), U( * ),
     $                   WORK( LWORK ), X( * )
*
*  ====================================================================
*
*     .. Parameters ..
      REAL               ZERO
      PARAMETER          ( ZERO = 0.0E+0 )
      COMPLEX            CONE
      PARAMETER          ( CONE = 1.0E+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            INFO
      REAL               ANORM, BNORM, EPS, XNORM, YNORM, DNORM, UNFL
*     ..
*     .. External Functions ..
      REAL               SCASUM, SLAMCH, CLANGE
      EXTERNAL           SCASUM, SLAMCH, CLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           CLACPY
*
*     .. Intrinsic Functions ..
      INTRINSIC          MAX
*     ..
*     .. Executable Statements ..
*
      EPS = SLAMCH( 'Epsilon' )
      UNFL = SLAMCH( 'Safe minimum' )
      ANORM = MAX( CLANGE( '1', N, M, A, LDA, RWORK ), UNFL )
      BNORM = MAX( CLANGE( '1', N, P, B, LDB, RWORK ), UNFL )
*
*     Copy the matrices A and B to the arrays AF and BF,
*     and the vector D the array DF.
*
      CALL CLACPY( 'Full', N, M, A, LDA, AF, LDA )
      CALL CLACPY( 'Full', N, P, B, LDB, BF, LDB )
      CALL CCOPY( N, D, 1, DF, 1 )
*
*     Solve GLM problem
*
      CALL CGGGLM( N, M, P, AF, LDA, BF, LDB, DF, X, U, WORK, LWORK,
     $             INFO )
*
*     Test the residual for the solution of LSE
*
*                       norm( d - A*x - B*u )
*       RESULT = -----------------------------------------
*                (norm(A)+norm(B))*(norm(x)+norm(u))*EPS
*
      CALL CCOPY( N, D, 1, DF, 1 )
      CALL CGEMV( 'No transpose', N, M, -CONE, A, LDA, X, 1, CONE,
     $            DF, 1 )
*
      CALL CGEMV( 'No transpose', N, P, -CONE, B, LDB, U, 1, CONE,
     $            DF, 1 )
*
      DNORM = SCASUM( N, DF, 1 )
      XNORM = SCASUM( M, X, 1 ) + SCASUM( P, U, 1 )
      YNORM = ANORM + BNORM
*
      IF( XNORM.LE.ZERO ) THEN
         RESULT = ZERO
      ELSE
         RESULT =  ( ( DNORM / YNORM ) / XNORM ) /EPS
      END IF
*
      RETURN
*
*     End of CGLMTS
*
      END
