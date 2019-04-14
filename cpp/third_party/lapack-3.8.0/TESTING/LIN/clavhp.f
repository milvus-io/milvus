*> \brief \b CLAVHP
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CLAVHP( UPLO, TRANS, DIAG, N, NRHS, A, IPIV, B, LDB,
*                          INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          DIAG, TRANS, UPLO
*       INTEGER            INFO, LDB, N, NRHS
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * )
*       COMPLEX            A( * ), B( LDB, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    CLAVHP  performs one of the matrix-vector operations
*>       x := A*x  or  x := A^H*x,
*>    where x is an N element vector and  A is one of the factors
*>    from the symmetric factorization computed by CHPTRF.
*>    CHPTRF produces a factorization of the form
*>         U * D * U^H     or     L * D * L^H,
*>    where U (or L) is a product of permutation and unit upper (lower)
*>    triangular matrices, U^H (or L^H) is the conjugate transpose of
*>    U (or L), and D is Hermitian and block diagonal with 1 x 1 and
*>    2 x 2 diagonal blocks.  The multipliers for the transformations
*>    and the upper or lower triangular parts of the diagonal blocks
*>    are stored columnwise in packed format in the linear array A.
*>
*>    If TRANS = 'N' or 'n', CLAVHP multiplies either by U or U * D
*>    (or L or L * D).
*>    If TRANS = 'C' or 'c', CLAVHP multiplies either by U^H or D * U^H
*>    (or L^H or D * L^H ).
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \verbatim
*>  UPLO   - CHARACTER*1
*>           On entry, UPLO specifies whether the triangular matrix
*>           stored in A is upper or lower triangular.
*>              UPLO = 'U' or 'u'   The matrix is upper triangular.
*>              UPLO = 'L' or 'l'   The matrix is lower triangular.
*>           Unchanged on exit.
*>
*>  TRANS  - CHARACTER*1
*>           On entry, TRANS specifies the operation to be performed as
*>           follows:
*>              TRANS = 'N' or 'n'   x := A*x.
*>              TRANS = 'C' or 'c'   x := A^H*x.
*>           Unchanged on exit.
*>
*>  DIAG   - CHARACTER*1
*>           On entry, DIAG specifies whether the diagonal blocks are
*>           assumed to be unit matrices, as follows:
*>              DIAG = 'U' or 'u'   Diagonal blocks are unit matrices.
*>              DIAG = 'N' or 'n'   Diagonal blocks are non-unit.
*>           Unchanged on exit.
*>
*>  N      - INTEGER
*>           On entry, N specifies the order of the matrix A.
*>           N must be at least zero.
*>           Unchanged on exit.
*>
*>  NRHS   - INTEGER
*>           On entry, NRHS specifies the number of right hand sides,
*>           i.e., the number of vectors x to be multiplied by A.
*>           NRHS must be at least zero.
*>           Unchanged on exit.
*>
*>  A      - COMPLEX array, dimension( N*(N+1)/2 )
*>           On entry, A contains a block diagonal matrix and the
*>           multipliers of the transformations used to obtain it,
*>           stored as a packed triangular matrix.
*>           Unchanged on exit.
*>
*>  IPIV   - INTEGER array, dimension( N )
*>           On entry, IPIV contains the vector of pivot indices as
*>           determined by CSPTRF or CHPTRF.
*>           If IPIV( K ) = K, no interchange was done.
*>           If IPIV( K ) <> K but IPIV( K ) > 0, then row K was inter-
*>           changed with row IPIV( K ) and a 1 x 1 pivot block was used.
*>           If IPIV( K ) < 0 and UPLO = 'U', then row K-1 was exchanged
*>           with row | IPIV( K ) | and a 2 x 2 pivot block was used.
*>           If IPIV( K ) < 0 and UPLO = 'L', then row K+1 was exchanged
*>           with row | IPIV( K ) | and a 2 x 2 pivot block was used.
*>
*>  B      - COMPLEX array, dimension( LDB, NRHS )
*>           On entry, B contains NRHS vectors of length N.
*>           On exit, B is overwritten with the product A * B.
*>
*>  LDB    - INTEGER
*>           On entry, LDB contains the leading dimension of B as
*>           declared in the calling program.  LDB must be at least
*>           max( 1, N ).
*>           Unchanged on exit.
*>
*>  INFO   - INTEGER
*>           INFO is the error flag.
*>           On exit, a value of 0 indicates a successful exit.
*>           A negative value, say -K, indicates that the K-th argument
*>           has an illegal value.
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
      SUBROUTINE CLAVHP( UPLO, TRANS, DIAG, N, NRHS, A, IPIV, B, LDB,
     $                   INFO )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          DIAG, TRANS, UPLO
      INTEGER            INFO, LDB, N, NRHS
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * )
      COMPLEX            A( * ), B( LDB, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX            ONE
      PARAMETER          ( ONE = ( 1.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            NOUNIT
      INTEGER            J, K, KC, KCNEXT, KP
      COMPLEX            D11, D12, D21, D22, T1, T2
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           CGEMV, CGERU, CLACGV, CSCAL, CSWAP, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, CONJG, MAX
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
      IF( .NOT.LSAME( UPLO, 'U' ) .AND. .NOT.LSAME( UPLO, 'L' ) ) THEN
         INFO = -1
      ELSE IF( .NOT.LSAME( TRANS, 'N' ) .AND. .NOT.LSAME( TRANS, 'C' ) )
     $          THEN
         INFO = -2
      ELSE IF( .NOT.LSAME( DIAG, 'U' ) .AND. .NOT.LSAME( DIAG, 'N' ) )
     $          THEN
         INFO = -3
      ELSE IF( N.LT.0 ) THEN
         INFO = -4
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -8
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CLAVHP ', -INFO )
         RETURN
      END IF
*
*     Quick return if possible.
*
      IF( N.EQ.0 )
     $   RETURN
*
      NOUNIT = LSAME( DIAG, 'N' )
*------------------------------------------
*
*     Compute  B := A * B  (No transpose)
*
*------------------------------------------
      IF( LSAME( TRANS, 'N' ) ) THEN
*
*        Compute  B := U*B
*        where U = P(m)*inv(U(m))* ... *P(1)*inv(U(1))
*
         IF( LSAME( UPLO, 'U' ) ) THEN
*
*        Loop forward applying the transformations.
*
            K = 1
            KC = 1
   10       CONTINUE
            IF( K.GT.N )
     $         GO TO 30
*
*           1 x 1 pivot block
*
            IF( IPIV( K ).GT.0 ) THEN
*
*              Multiply by the diagonal element if forming U * D.
*
               IF( NOUNIT )
     $            CALL CSCAL( NRHS, A( KC+K-1 ), B( K, 1 ), LDB )
*
*              Multiply by P(K) * inv(U(K))  if K > 1.
*
               IF( K.GT.1 ) THEN
*
*                 Apply the transformation.
*
                  CALL CGERU( K-1, NRHS, ONE, A( KC ), 1, B( K, 1 ),
     $                        LDB, B( 1, 1 ), LDB )
*
*                 Interchange if P(K) != I.
*
                  KP = IPIV( K )
                  IF( KP.NE.K )
     $               CALL CSWAP( NRHS, B( K, 1 ), LDB, B( KP, 1 ), LDB )
               END IF
               KC = KC + K
               K = K + 1
            ELSE
*
*              2 x 2 pivot block
*
               KCNEXT = KC + K
*
*              Multiply by the diagonal block if forming U * D.
*
               IF( NOUNIT ) THEN
                  D11 = A( KCNEXT-1 )
                  D22 = A( KCNEXT+K )
                  D12 = A( KCNEXT+K-1 )
                  D21 = CONJG( D12 )
                  DO 20 J = 1, NRHS
                     T1 = B( K, J )
                     T2 = B( K+1, J )
                     B( K, J ) = D11*T1 + D12*T2
                     B( K+1, J ) = D21*T1 + D22*T2
   20             CONTINUE
               END IF
*
*              Multiply by  P(K) * inv(U(K))  if K > 1.
*
               IF( K.GT.1 ) THEN
*
*                 Apply the transformations.
*
                  CALL CGERU( K-1, NRHS, ONE, A( KC ), 1, B( K, 1 ),
     $                        LDB, B( 1, 1 ), LDB )
                  CALL CGERU( K-1, NRHS, ONE, A( KCNEXT ), 1,
     $                        B( K+1, 1 ), LDB, B( 1, 1 ), LDB )
*
*                 Interchange if P(K) != I.
*
                  KP = ABS( IPIV( K ) )
                  IF( KP.NE.K )
     $               CALL CSWAP( NRHS, B( K, 1 ), LDB, B( KP, 1 ), LDB )
               END IF
               KC = KCNEXT + K + 1
               K = K + 2
            END IF
            GO TO 10
   30       CONTINUE
*
*        Compute  B := L*B
*        where L = P(1)*inv(L(1))* ... *P(m)*inv(L(m)) .
*
         ELSE
*
*           Loop backward applying the transformations to B.
*
            K = N
            KC = N*( N+1 ) / 2 + 1
   40       CONTINUE
            IF( K.LT.1 )
     $         GO TO 60
            KC = KC - ( N-K+1 )
*
*           Test the pivot index.  If greater than zero, a 1 x 1
*           pivot was used, otherwise a 2 x 2 pivot was used.
*
            IF( IPIV( K ).GT.0 ) THEN
*
*              1 x 1 pivot block:
*
*              Multiply by the diagonal element if forming L * D.
*
               IF( NOUNIT )
     $            CALL CSCAL( NRHS, A( KC ), B( K, 1 ), LDB )
*
*              Multiply by  P(K) * inv(L(K))  if K < N.
*
               IF( K.NE.N ) THEN
                  KP = IPIV( K )
*
*                 Apply the transformation.
*
                  CALL CGERU( N-K, NRHS, ONE, A( KC+1 ), 1, B( K, 1 ),
     $                        LDB, B( K+1, 1 ), LDB )
*
*                 Interchange if a permutation was applied at the
*                 K-th step of the factorization.
*
                  IF( KP.NE.K )
     $               CALL CSWAP( NRHS, B( K, 1 ), LDB, B( KP, 1 ), LDB )
               END IF
               K = K - 1
*
            ELSE
*
*              2 x 2 pivot block:
*
               KCNEXT = KC - ( N-K+2 )
*
*              Multiply by the diagonal block if forming L * D.
*
               IF( NOUNIT ) THEN
                  D11 = A( KCNEXT )
                  D22 = A( KC )
                  D21 = A( KCNEXT+1 )
                  D12 = CONJG( D21 )
                  DO 50 J = 1, NRHS
                     T1 = B( K-1, J )
                     T2 = B( K, J )
                     B( K-1, J ) = D11*T1 + D12*T2
                     B( K, J ) = D21*T1 + D22*T2
   50             CONTINUE
               END IF
*
*              Multiply by  P(K) * inv(L(K))  if K < N.
*
               IF( K.NE.N ) THEN
*
*                 Apply the transformation.
*
                  CALL CGERU( N-K, NRHS, ONE, A( KC+1 ), 1, B( K, 1 ),
     $                        LDB, B( K+1, 1 ), LDB )
                  CALL CGERU( N-K, NRHS, ONE, A( KCNEXT+2 ), 1,
     $                        B( K-1, 1 ), LDB, B( K+1, 1 ), LDB )
*
*                 Interchange if a permutation was applied at the
*                 K-th step of the factorization.
*
                  KP = ABS( IPIV( K ) )
                  IF( KP.NE.K )
     $               CALL CSWAP( NRHS, B( K, 1 ), LDB, B( KP, 1 ), LDB )
               END IF
               KC = KCNEXT
               K = K - 2
            END IF
            GO TO 40
   60       CONTINUE
         END IF
*-------------------------------------------------
*
*     Compute  B := A^H * B  (conjugate transpose)
*
*-------------------------------------------------
      ELSE
*
*        Form  B := U^H*B
*        where U  = P(m)*inv(U(m))* ... *P(1)*inv(U(1))
*        and   U^H = inv(U^H(1))*P(1)* ... *inv(U^H(m))*P(m)
*
         IF( LSAME( UPLO, 'U' ) ) THEN
*
*           Loop backward applying the transformations.
*
            K = N
            KC = N*( N+1 ) / 2 + 1
   70       IF( K.LT.1 )
     $         GO TO 90
            KC = KC - K
*
*           1 x 1 pivot block.
*
            IF( IPIV( K ).GT.0 ) THEN
               IF( K.GT.1 ) THEN
*
*                 Interchange if P(K) != I.
*
                  KP = IPIV( K )
                  IF( KP.NE.K )
     $               CALL CSWAP( NRHS, B( K, 1 ), LDB, B( KP, 1 ), LDB )
*
*                 Apply the transformation:
*                    y := y - B' * conjg(x)
*                 where x is a column of A and y is a row of B.
*
                  CALL CLACGV( NRHS, B( K, 1 ), LDB )
                  CALL CGEMV( 'Conjugate', K-1, NRHS, ONE, B, LDB,
     $                        A( KC ), 1, ONE, B( K, 1 ), LDB )
                  CALL CLACGV( NRHS, B( K, 1 ), LDB )
               END IF
               IF( NOUNIT )
     $            CALL CSCAL( NRHS, A( KC+K-1 ), B( K, 1 ), LDB )
               K = K - 1
*
*           2 x 2 pivot block.
*
            ELSE
               KCNEXT = KC - ( K-1 )
               IF( K.GT.2 ) THEN
*
*                 Interchange if P(K) != I.
*
                  KP = ABS( IPIV( K ) )
                  IF( KP.NE.K-1 )
     $               CALL CSWAP( NRHS, B( K-1, 1 ), LDB, B( KP, 1 ),
     $                           LDB )
*
*                 Apply the transformations.
*
                  CALL CLACGV( NRHS, B( K, 1 ), LDB )
                  CALL CGEMV( 'Conjugate', K-2, NRHS, ONE, B, LDB,
     $                        A( KC ), 1, ONE, B( K, 1 ), LDB )
                  CALL CLACGV( NRHS, B( K, 1 ), LDB )
*
                  CALL CLACGV( NRHS, B( K-1, 1 ), LDB )
                  CALL CGEMV( 'Conjugate', K-2, NRHS, ONE, B, LDB,
     $                        A( KCNEXT ), 1, ONE, B( K-1, 1 ), LDB )
                  CALL CLACGV( NRHS, B( K-1, 1 ), LDB )
               END IF
*
*              Multiply by the diagonal block if non-unit.
*
               IF( NOUNIT ) THEN
                  D11 = A( KC-1 )
                  D22 = A( KC+K-1 )
                  D12 = A( KC+K-2 )
                  D21 = CONJG( D12 )
                  DO 80 J = 1, NRHS
                     T1 = B( K-1, J )
                     T2 = B( K, J )
                     B( K-1, J ) = D11*T1 + D12*T2
                     B( K, J ) = D21*T1 + D22*T2
   80             CONTINUE
               END IF
               KC = KCNEXT
               K = K - 2
            END IF
            GO TO 70
   90       CONTINUE
*
*        Form  B := L^H*B
*        where L  = P(1)*inv(L(1))* ... *P(m)*inv(L(m))
*        and   L^H = inv(L(m))*P(m)* ... *inv(L(1))*P(1)
*
         ELSE
*
*           Loop forward applying the L-transformations.
*
            K = 1
            KC = 1
  100       CONTINUE
            IF( K.GT.N )
     $         GO TO 120
*
*           1 x 1 pivot block
*
            IF( IPIV( K ).GT.0 ) THEN
               IF( K.LT.N ) THEN
*
*                 Interchange if P(K) != I.
*
                  KP = IPIV( K )
                  IF( KP.NE.K )
     $               CALL CSWAP( NRHS, B( K, 1 ), LDB, B( KP, 1 ), LDB )
*
*                 Apply the transformation
*
                  CALL CLACGV( NRHS, B( K, 1 ), LDB )
                  CALL CGEMV( 'Conjugate', N-K, NRHS, ONE, B( K+1, 1 ),
     $                        LDB, A( KC+1 ), 1, ONE, B( K, 1 ), LDB )
                  CALL CLACGV( NRHS, B( K, 1 ), LDB )
               END IF
               IF( NOUNIT )
     $            CALL CSCAL( NRHS, A( KC ), B( K, 1 ), LDB )
               KC = KC + N - K + 1
               K = K + 1
*
*           2 x 2 pivot block.
*
            ELSE
               KCNEXT = KC + N - K + 1
               IF( K.LT.N-1 ) THEN
*
*              Interchange if P(K) != I.
*
                  KP = ABS( IPIV( K ) )
                  IF( KP.NE.K+1 )
     $               CALL CSWAP( NRHS, B( K+1, 1 ), LDB, B( KP, 1 ),
     $                           LDB )
*
*                 Apply the transformation
*
                  CALL CLACGV( NRHS, B( K+1, 1 ), LDB )
                  CALL CGEMV( 'Conjugate', N-K-1, NRHS, ONE,
     $                        B( K+2, 1 ), LDB, A( KCNEXT+1 ), 1, ONE,
     $                        B( K+1, 1 ), LDB )
                  CALL CLACGV( NRHS, B( K+1, 1 ), LDB )
*
                  CALL CLACGV( NRHS, B( K, 1 ), LDB )
                  CALL CGEMV( 'Conjugate', N-K-1, NRHS, ONE,
     $                        B( K+2, 1 ), LDB, A( KC+2 ), 1, ONE,
     $                        B( K, 1 ), LDB )
                  CALL CLACGV( NRHS, B( K, 1 ), LDB )
               END IF
*
*              Multiply by the diagonal block if non-unit.
*
               IF( NOUNIT ) THEN
                  D11 = A( KC )
                  D22 = A( KCNEXT )
                  D21 = A( KC+1 )
                  D12 = CONJG( D21 )
                  DO 110 J = 1, NRHS
                     T1 = B( K, J )
                     T2 = B( K+1, J )
                     B( K, J ) = D11*T1 + D12*T2
                     B( K+1, J ) = D21*T1 + D22*T2
  110             CONTINUE
               END IF
               KC = KCNEXT + ( N-K )
               K = K + 2
            END IF
            GO TO 100
  120       CONTINUE
         END IF
*
      END IF
      RETURN
*
*     End of CLAVHP
*
      END
