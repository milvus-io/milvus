*> \brief \b DTPLQT2 computes a LQ factorization of a real or complex "triangular-pentagonal" matrix, which is composed of a triangular block and a pentagonal block, using the compact WY representation for Q.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DTPLQT2 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dtplqt2.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dtplqt2.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dtplqt2.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DTPLQT2( M, N, L, A, LDA, B, LDB, T, LDT, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER   INFO, LDA, LDB, LDT, N, M, L
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   A( LDA, * ), B( LDB, * ), T( LDT, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DTPLQT2 computes a LQ a factorization of a real "triangular-pentagonal"
*> matrix C, which is composed of a triangular block A and pentagonal block B,
*> using the compact WY representation for Q.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The total number of rows of the matrix B.
*>          M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix B, and the order of
*>          the triangular matrix A.
*>          N >= 0.
*> \endverbatim
*>
*> \param[in] L
*> \verbatim
*>          L is INTEGER
*>          The number of rows of the lower trapezoidal part of B.
*>          MIN(M,N) >= L >= 0.  See Further Details.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension (LDA,M)
*>          On entry, the lower triangular M-by-M matrix A.
*>          On exit, the elements on and below the diagonal of the array
*>          contain the lower triangular matrix L.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,M).
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is DOUBLE PRECISION array, dimension (LDB,N)
*>          On entry, the pentagonal M-by-N matrix B.  The first N-L columns
*>          are rectangular, and the last L columns are lower trapezoidal.
*>          On exit, B contains the pentagonal matrix V.  See Further Details.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B.  LDB >= max(1,M).
*> \endverbatim
*>
*> \param[out] T
*> \verbatim
*>          T is DOUBLE PRECISION array, dimension (LDT,M)
*>          The N-by-N upper triangular factor T of the block reflector.
*>          See Further Details.
*> \endverbatim
*>
*> \param[in] LDT
*> \verbatim
*>          LDT is INTEGER
*>          The leading dimension of the array T.  LDT >= max(1,M)
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0: successful exit
*>          < 0: if INFO = -i, the i-th argument had an illegal value
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
*> \date June 2017
*
*> \ingroup doubleOTHERcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  The input matrix C is a M-by-(M+N) matrix
*>
*>               C = [ A ][ B ]
*>
*>
*>  where A is an lower triangular M-by-M matrix, and B is M-by-N pentagonal
*>  matrix consisting of a M-by-(N-L) rectangular matrix B1 left of a M-by-L
*>  upper trapezoidal matrix B2:
*>
*>               B = [ B1 ][ B2 ]
*>                   [ B1 ]  <-     M-by-(N-L) rectangular
*>                   [ B2 ]  <-     M-by-L lower trapezoidal.
*>
*>  The lower trapezoidal matrix B2 consists of the first L columns of a
*>  N-by-N lower triangular matrix, where 0 <= L <= MIN(M,N).  If L=0,
*>  B is rectangular M-by-N; if M=L=N, B is lower triangular.
*>
*>  The matrix W stores the elementary reflectors H(i) in the i-th row
*>  above the diagonal (of A) in the M-by-(M+N) input matrix C
*>
*>               C = [ A ][ B ]
*>                   [ A ]  <- lower triangular M-by-M
*>                   [ B ]  <- M-by-N pentagonal
*>
*>  so that W can be represented as
*>
*>               W = [ I ][ V ]
*>                   [ I ]  <- identity, M-by-M
*>                   [ V ]  <- M-by-N, same form as B.
*>
*>  Thus, all of information needed for W is contained on exit in B, which
*>  we call V above.  Note that V has the same form as B; that is,
*>
*>               W = [ V1 ][ V2 ]
*>                   [ V1 ] <-     M-by-(N-L) rectangular
*>                   [ V2 ] <-     M-by-L lower trapezoidal.
*>
*>  The rows of V represent the vectors which define the H(i)'s.
*>  The (M+N)-by-(M+N) block reflector H is then given by
*>
*>               H = I - W**T * T * W
*>
*>  where W^H is the conjugate transpose of W and T is the upper triangular
*>  factor of the block reflector.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE DTPLQT2( M, N, L, A, LDA, B, LDB, T, LDT, INFO )
*
*  -- LAPACK computational routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      INTEGER   INFO, LDA, LDB, LDT, N, M, L
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   A( LDA, * ), B( LDB, * ), T( LDT, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION  ONE, ZERO
      PARAMETER( ONE = 1.0, ZERO = 0.0 )
*     ..
*     .. Local Scalars ..
      INTEGER   I, J, P, MP, NP
      DOUBLE PRECISION   ALPHA
*     ..
*     .. External Subroutines ..
      EXTERNAL  DLARFG, DGEMV, DGER, DTRMV, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Test the input arguments
*
      INFO = 0
      IF( M.LT.0 ) THEN
         INFO = -1
      ELSE IF( N.LT.0 ) THEN
         INFO = -2
      ELSE IF( L.LT.0 .OR. L.GT.MIN(M,N) ) THEN
         INFO = -3
      ELSE IF( LDA.LT.MAX( 1, M ) ) THEN
         INFO = -5
      ELSE IF( LDB.LT.MAX( 1, M ) ) THEN
         INFO = -7
      ELSE IF( LDT.LT.MAX( 1, M ) ) THEN
         INFO = -9
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DTPLQT2', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 .OR. M.EQ.0 ) RETURN
*
      DO I = 1, M
*
*        Generate elementary reflector H(I) to annihilate B(I,:)
*
         P = N-L+MIN( L, I )
         CALL DLARFG( P+1, A( I, I ), B( I, 1 ), LDB, T( 1, I ) )
         IF( I.LT.M ) THEN
*
*           W(M-I:1) := C(I+1:M,I:N) * C(I,I:N) [use W = T(M,:)]
*
            DO J = 1, M-I
               T( M, J ) = (A( I+J, I ))
            END DO
            CALL DGEMV( 'N', M-I, P, ONE, B( I+1, 1 ), LDB,
     $                  B( I, 1 ), LDB, ONE, T( M, 1 ), LDT )
*
*           C(I+1:M,I:N) = C(I+1:M,I:N) + alpha * C(I,I:N)*W(M-1:1)^H
*
            ALPHA = -(T( 1, I ))
            DO J = 1, M-I
               A( I+J, I ) = A( I+J, I ) + ALPHA*(T( M, J ))
            END DO
            CALL DGER( M-I, P, ALPHA,  T( M, 1 ), LDT,
     $          B( I, 1 ), LDB, B( I+1, 1 ), LDB )
         END IF
      END DO
*
      DO I = 2, M
*
*        T(I,1:I-1) := C(I:I-1,1:N) * (alpha * C(I,I:N)^H)
*
         ALPHA = -T( 1, I )

         DO J = 1, I-1
            T( I, J ) = ZERO
         END DO
         P = MIN( I-1, L )
         NP = MIN( N-L+1, N )
         MP = MIN( P+1, M )
*
*        Triangular part of B2
*
         DO J = 1, P
            T( I, J ) = ALPHA*B( I, N-L+J )
         END DO
         CALL DTRMV( 'L', 'N', 'N', P, B( 1, NP ), LDB,
     $               T( I, 1 ), LDT )
*
*        Rectangular part of B2
*
         CALL DGEMV( 'N', I-1-P, L,  ALPHA, B( MP, NP ), LDB,
     $               B( I, NP ), LDB, ZERO, T( I,MP ), LDT )
*
*        B1
*
         CALL DGEMV( 'N', I-1, N-L, ALPHA, B, LDB, B( I, 1 ), LDB,
     $               ONE, T( I, 1 ), LDT )
*
*        T(1:I-1,I) := T(1:I-1,1:I-1) * T(I,1:I-1)
*
        CALL DTRMV( 'L', 'T', 'N', I-1, T, LDT, T( I, 1 ), LDT )
*
*        T(I,I) = tau(I)
*
         T( I, I ) = T( 1, I )
         T( 1, I ) = ZERO
      END DO
      DO I=1,M
         DO J= I+1,M
            T(I,J)=T(J,I)
            T(J,I)= ZERO
         END DO
      END DO

*
*     End of DTPLQT2
*
      END
