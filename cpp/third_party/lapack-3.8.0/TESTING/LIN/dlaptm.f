*> \brief \b DLAPTM
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DLAPTM( N, NRHS, ALPHA, D, E, X, LDX, BETA, B, LDB )
*
*       .. Scalar Arguments ..
*       INTEGER            LDB, LDX, N, NRHS
*       DOUBLE PRECISION   ALPHA, BETA
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   B( LDB, * ), D( * ), E( * ), X( LDX, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DLAPTM multiplies an N by NRHS matrix X by a symmetric tridiagonal
*> matrix A and stores the result in a matrix B.  The operation has the
*> form
*>
*>    B := alpha * A * X + beta * B
*>
*> where alpha may be either 1. or -1. and beta may be 0., 1., or -1.
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
*> \param[in] NRHS
*> \verbatim
*>          NRHS is INTEGER
*>          The number of right hand sides, i.e., the number of columns
*>          of the matrices X and B.
*> \endverbatim
*>
*> \param[in] ALPHA
*> \verbatim
*>          ALPHA is DOUBLE PRECISION
*>          The scalar alpha.  ALPHA must be 1. or -1.; otherwise,
*>          it is assumed to be 0.
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension (N)
*>          The n diagonal elements of the tridiagonal matrix A.
*> \endverbatim
*>
*> \param[in] E
*> \verbatim
*>          E is DOUBLE PRECISION array, dimension (N-1)
*>          The (n-1) subdiagonal or superdiagonal elements of A.
*> \endverbatim
*>
*> \param[in] X
*> \verbatim
*>          X is DOUBLE PRECISION array, dimension (LDX,NRHS)
*>          The N by NRHS matrix X.
*> \endverbatim
*>
*> \param[in] LDX
*> \verbatim
*>          LDX is INTEGER
*>          The leading dimension of the array X.  LDX >= max(N,1).
*> \endverbatim
*>
*> \param[in] BETA
*> \verbatim
*>          BETA is DOUBLE PRECISION
*>          The scalar beta.  BETA must be 0., 1., or -1.; otherwise,
*>          it is assumed to be 1.
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is DOUBLE PRECISION array, dimension (LDB,NRHS)
*>          On entry, the N by NRHS matrix B.
*>          On exit, B is overwritten by the matrix expression
*>          B := alpha * A * X + beta * B.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B.  LDB >= max(N,1).
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
      SUBROUTINE DLAPTM( N, NRHS, ALPHA, D, E, X, LDX, BETA, B, LDB )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            LDB, LDX, N, NRHS
      DOUBLE PRECISION   ALPHA, BETA
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   B( LDB, * ), D( * ), E( * ), X( LDX, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ONE, ZERO
      PARAMETER          ( ONE = 1.0D+0, ZERO = 0.0D+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, J
*     ..
*     .. Executable Statements ..
*
      IF( N.EQ.0 )
     $   RETURN
*
*     Multiply B by BETA if BETA.NE.1.
*
      IF( BETA.EQ.ZERO ) THEN
         DO 20 J = 1, NRHS
            DO 10 I = 1, N
               B( I, J ) = ZERO
   10       CONTINUE
   20    CONTINUE
      ELSE IF( BETA.EQ.-ONE ) THEN
         DO 40 J = 1, NRHS
            DO 30 I = 1, N
               B( I, J ) = -B( I, J )
   30       CONTINUE
   40    CONTINUE
      END IF
*
      IF( ALPHA.EQ.ONE ) THEN
*
*        Compute B := B + A*X
*
         DO 60 J = 1, NRHS
            IF( N.EQ.1 ) THEN
               B( 1, J ) = B( 1, J ) + D( 1 )*X( 1, J )
            ELSE
               B( 1, J ) = B( 1, J ) + D( 1 )*X( 1, J ) +
     $                     E( 1 )*X( 2, J )
               B( N, J ) = B( N, J ) + E( N-1 )*X( N-1, J ) +
     $                     D( N )*X( N, J )
               DO 50 I = 2, N - 1
                  B( I, J ) = B( I, J ) + E( I-1 )*X( I-1, J ) +
     $                        D( I )*X( I, J ) + E( I )*X( I+1, J )
   50          CONTINUE
            END IF
   60    CONTINUE
      ELSE IF( ALPHA.EQ.-ONE ) THEN
*
*        Compute B := B - A*X
*
         DO 80 J = 1, NRHS
            IF( N.EQ.1 ) THEN
               B( 1, J ) = B( 1, J ) - D( 1 )*X( 1, J )
            ELSE
               B( 1, J ) = B( 1, J ) - D( 1 )*X( 1, J ) -
     $                     E( 1 )*X( 2, J )
               B( N, J ) = B( N, J ) - E( N-1 )*X( N-1, J ) -
     $                     D( N )*X( N, J )
               DO 70 I = 2, N - 1
                  B( I, J ) = B( I, J ) - E( I-1 )*X( I-1, J ) -
     $                        D( I )*X( I, J ) - E( I )*X( I+1, J )
   70          CONTINUE
            END IF
   80    CONTINUE
      END IF
      RETURN
*
*     End of DLAPTM
*
      END
