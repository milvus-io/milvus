*> \brief \b CLAGTM performs a matrix-matrix product of the form C = αAB+βC, where A is a tridiagonal matrix, B and C are rectangular matrices, and α and β are scalars, which may be 0, 1, or -1.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CLAGTM + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/clagtm.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/clagtm.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/clagtm.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CLAGTM( TRANS, N, NRHS, ALPHA, DL, D, DU, X, LDX, BETA,
*                          B, LDB )
*
*       .. Scalar Arguments ..
*       CHARACTER          TRANS
*       INTEGER            LDB, LDX, N, NRHS
*       REAL               ALPHA, BETA
*       ..
*       .. Array Arguments ..
*       COMPLEX            B( LDB, * ), D( * ), DL( * ), DU( * ),
*      $                   X( LDX, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CLAGTM performs a matrix-vector product of the form
*>
*>    B := alpha * A * X + beta * B
*>
*> where A is a tridiagonal matrix of order N, B and X are N by NRHS
*> matrices, and alpha and beta are real scalars, each of which may be
*> 0., 1., or -1.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] TRANS
*> \verbatim
*>          TRANS is CHARACTER*1
*>          Specifies the operation applied to A.
*>          = 'N':  No transpose, B := alpha * A * X + beta * B
*>          = 'T':  Transpose,    B := alpha * A**T * X + beta * B
*>          = 'C':  Conjugate transpose, B := alpha * A**H * X + beta * B
*> \endverbatim
*>
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
*>          ALPHA is REAL
*>          The scalar alpha.  ALPHA must be 0., 1., or -1.; otherwise,
*>          it is assumed to be 0.
*> \endverbatim
*>
*> \param[in] DL
*> \verbatim
*>          DL is COMPLEX array, dimension (N-1)
*>          The (n-1) sub-diagonal elements of T.
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is COMPLEX array, dimension (N)
*>          The diagonal elements of T.
*> \endverbatim
*>
*> \param[in] DU
*> \verbatim
*>          DU is COMPLEX array, dimension (N-1)
*>          The (n-1) super-diagonal elements of T.
*> \endverbatim
*>
*> \param[in] X
*> \verbatim
*>          X is COMPLEX array, dimension (LDX,NRHS)
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
*>          BETA is REAL
*>          The scalar beta.  BETA must be 0., 1., or -1.; otherwise,
*>          it is assumed to be 1.
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX array, dimension (LDB,NRHS)
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
*> \ingroup complexOTHERauxiliary
*
*  =====================================================================
      SUBROUTINE CLAGTM( TRANS, N, NRHS, ALPHA, DL, D, DU, X, LDX, BETA,
     $                   B, LDB )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          TRANS
      INTEGER            LDB, LDX, N, NRHS
      REAL               ALPHA, BETA
*     ..
*     .. Array Arguments ..
      COMPLEX            B( LDB, * ), D( * ), DL( * ), DU( * ),
     $                   X( LDX, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ONE, ZERO
      PARAMETER          ( ONE = 1.0E+0, ZERO = 0.0E+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, J
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          CONJG
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
         IF( LSAME( TRANS, 'N' ) ) THEN
*
*           Compute B := B + A*X
*
            DO 60 J = 1, NRHS
               IF( N.EQ.1 ) THEN
                  B( 1, J ) = B( 1, J ) + D( 1 )*X( 1, J )
               ELSE
                  B( 1, J ) = B( 1, J ) + D( 1 )*X( 1, J ) +
     $                        DU( 1 )*X( 2, J )
                  B( N, J ) = B( N, J ) + DL( N-1 )*X( N-1, J ) +
     $                        D( N )*X( N, J )
                  DO 50 I = 2, N - 1
                     B( I, J ) = B( I, J ) + DL( I-1 )*X( I-1, J ) +
     $                           D( I )*X( I, J ) + DU( I )*X( I+1, J )
   50             CONTINUE
               END IF
   60       CONTINUE
         ELSE IF( LSAME( TRANS, 'T' ) ) THEN
*
*           Compute B := B + A**T * X
*
            DO 80 J = 1, NRHS
               IF( N.EQ.1 ) THEN
                  B( 1, J ) = B( 1, J ) + D( 1 )*X( 1, J )
               ELSE
                  B( 1, J ) = B( 1, J ) + D( 1 )*X( 1, J ) +
     $                        DL( 1 )*X( 2, J )
                  B( N, J ) = B( N, J ) + DU( N-1 )*X( N-1, J ) +
     $                        D( N )*X( N, J )
                  DO 70 I = 2, N - 1
                     B( I, J ) = B( I, J ) + DU( I-1 )*X( I-1, J ) +
     $                           D( I )*X( I, J ) + DL( I )*X( I+1, J )
   70             CONTINUE
               END IF
   80       CONTINUE
         ELSE IF( LSAME( TRANS, 'C' ) ) THEN
*
*           Compute B := B + A**H * X
*
            DO 100 J = 1, NRHS
               IF( N.EQ.1 ) THEN
                  B( 1, J ) = B( 1, J ) + CONJG( D( 1 ) )*X( 1, J )
               ELSE
                  B( 1, J ) = B( 1, J ) + CONJG( D( 1 ) )*X( 1, J ) +
     $                        CONJG( DL( 1 ) )*X( 2, J )
                  B( N, J ) = B( N, J ) + CONJG( DU( N-1 ) )*
     $                        X( N-1, J ) + CONJG( D( N ) )*X( N, J )
                  DO 90 I = 2, N - 1
                     B( I, J ) = B( I, J ) + CONJG( DU( I-1 ) )*
     $                           X( I-1, J ) + CONJG( D( I ) )*
     $                           X( I, J ) + CONJG( DL( I ) )*
     $                           X( I+1, J )
   90             CONTINUE
               END IF
  100       CONTINUE
         END IF
      ELSE IF( ALPHA.EQ.-ONE ) THEN
         IF( LSAME( TRANS, 'N' ) ) THEN
*
*           Compute B := B - A*X
*
            DO 120 J = 1, NRHS
               IF( N.EQ.1 ) THEN
                  B( 1, J ) = B( 1, J ) - D( 1 )*X( 1, J )
               ELSE
                  B( 1, J ) = B( 1, J ) - D( 1 )*X( 1, J ) -
     $                        DU( 1 )*X( 2, J )
                  B( N, J ) = B( N, J ) - DL( N-1 )*X( N-1, J ) -
     $                        D( N )*X( N, J )
                  DO 110 I = 2, N - 1
                     B( I, J ) = B( I, J ) - DL( I-1 )*X( I-1, J ) -
     $                           D( I )*X( I, J ) - DU( I )*X( I+1, J )
  110             CONTINUE
               END IF
  120       CONTINUE
         ELSE IF( LSAME( TRANS, 'T' ) ) THEN
*
*           Compute B := B - A**T*X
*
            DO 140 J = 1, NRHS
               IF( N.EQ.1 ) THEN
                  B( 1, J ) = B( 1, J ) - D( 1 )*X( 1, J )
               ELSE
                  B( 1, J ) = B( 1, J ) - D( 1 )*X( 1, J ) -
     $                        DL( 1 )*X( 2, J )
                  B( N, J ) = B( N, J ) - DU( N-1 )*X( N-1, J ) -
     $                        D( N )*X( N, J )
                  DO 130 I = 2, N - 1
                     B( I, J ) = B( I, J ) - DU( I-1 )*X( I-1, J ) -
     $                           D( I )*X( I, J ) - DL( I )*X( I+1, J )
  130             CONTINUE
               END IF
  140       CONTINUE
         ELSE IF( LSAME( TRANS, 'C' ) ) THEN
*
*           Compute B := B - A**H*X
*
            DO 160 J = 1, NRHS
               IF( N.EQ.1 ) THEN
                  B( 1, J ) = B( 1, J ) - CONJG( D( 1 ) )*X( 1, J )
               ELSE
                  B( 1, J ) = B( 1, J ) - CONJG( D( 1 ) )*X( 1, J ) -
     $                        CONJG( DL( 1 ) )*X( 2, J )
                  B( N, J ) = B( N, J ) - CONJG( DU( N-1 ) )*
     $                        X( N-1, J ) - CONJG( D( N ) )*X( N, J )
                  DO 150 I = 2, N - 1
                     B( I, J ) = B( I, J ) - CONJG( DU( I-1 ) )*
     $                           X( I-1, J ) - CONJG( D( I ) )*
     $                           X( I, J ) - CONJG( DL( I ) )*
     $                           X( I+1, J )
  150             CONTINUE
               END IF
  160       CONTINUE
         END IF
      END IF
      RETURN
*
*     End of CLAGTM
*
      END
