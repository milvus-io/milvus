*> \brief \b ZTPQRT2 computes a QR factorization of a real or complex "triangular-pentagonal" matrix, which is composed of a triangular block and a pentagonal block, using the compact WY representation for Q.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZTPQRT2 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/ztpqrt2.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/ztpqrt2.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/ztpqrt2.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZTPQRT2( M, N, L, A, LDA, B, LDB, T, LDT, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER   INFO, LDA, LDB, LDT, N, M, L
*       ..
*       .. Array Arguments ..
*       COMPLEX*16   A( LDA, * ), B( LDB, * ), T( LDT, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZTPQRT2 computes a QR factorization of a complex "triangular-pentagonal"
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
*>          The number of rows of the upper trapezoidal part of B.
*>          MIN(M,N) >= L >= 0.  See Further Details.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>          On entry, the upper triangular N-by-N matrix A.
*>          On exit, the elements on and above the diagonal of the array
*>          contain the upper triangular matrix R.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX*16 array, dimension (LDB,N)
*>          On entry, the pentagonal M-by-N matrix B.  The first M-L rows
*>          are rectangular, and the last L rows are upper trapezoidal.
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
*>          T is COMPLEX*16 array, dimension (LDT,N)
*>          The N-by-N upper triangular factor T of the block reflector.
*>          See Further Details.
*> \endverbatim
*>
*> \param[in] LDT
*> \verbatim
*>          LDT is INTEGER
*>          The leading dimension of the array T.  LDT >= max(1,N)
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
*> \date December 2016
*
*> \ingroup complex16OTHERcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  The input matrix C is a (N+M)-by-N matrix
*>
*>               C = [ A ]
*>                   [ B ]
*>
*>  where A is an upper triangular N-by-N matrix, and B is M-by-N pentagonal
*>  matrix consisting of a (M-L)-by-N rectangular matrix B1 on top of a L-by-N
*>  upper trapezoidal matrix B2:
*>
*>               B = [ B1 ]  <- (M-L)-by-N rectangular
*>                   [ B2 ]  <-     L-by-N upper trapezoidal.
*>
*>  The upper trapezoidal matrix B2 consists of the first L rows of a
*>  N-by-N upper triangular matrix, where 0 <= L <= MIN(M,N).  If L=0,
*>  B is rectangular M-by-N; if M=L=N, B is upper triangular.
*>
*>  The matrix W stores the elementary reflectors H(i) in the i-th column
*>  below the diagonal (of A) in the (N+M)-by-N input matrix C
*>
*>               C = [ A ]  <- upper triangular N-by-N
*>                   [ B ]  <- M-by-N pentagonal
*>
*>  so that W can be represented as
*>
*>               W = [ I ]  <- identity, N-by-N
*>                   [ V ]  <- M-by-N, same form as B.
*>
*>  Thus, all of information needed for W is contained on exit in B, which
*>  we call V above.  Note that V has the same form as B; that is,
*>
*>               V = [ V1 ] <- (M-L)-by-N rectangular
*>                   [ V2 ] <-     L-by-N upper trapezoidal.
*>
*>  The columns of V represent the vectors which define the H(i)'s.
*>  The (M+N)-by-(M+N) block reflector H is then given by
*>
*>               H = I - W * T * W**H
*>
*>  where W**H is the conjugate transpose of W and T is the upper triangular
*>  factor of the block reflector.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE ZTPQRT2( M, N, L, A, LDA, B, LDB, T, LDT, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER   INFO, LDA, LDB, LDT, N, M, L
*     ..
*     .. Array Arguments ..
      COMPLEX*16   A( LDA, * ), B( LDB, * ), T( LDT, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX*16  ONE, ZERO
      PARAMETER( ONE = (1.0,0.0), ZERO = (0.0,0.0) )
*     ..
*     .. Local Scalars ..
      INTEGER   I, J, P, MP, NP
      COMPLEX*16   ALPHA
*     ..
*     .. External Subroutines ..
      EXTERNAL  ZLARFG, ZGEMV, ZGERC, ZTRMV, XERBLA
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
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -5
      ELSE IF( LDB.LT.MAX( 1, M ) ) THEN
         INFO = -7
      ELSE IF( LDT.LT.MAX( 1, N ) ) THEN
         INFO = -9
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZTPQRT2', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 .OR. M.EQ.0 ) RETURN
*
      DO I = 1, N
*
*        Generate elementary reflector H(I) to annihilate B(:,I)
*
         P = M-L+MIN( L, I )
         CALL ZLARFG( P+1, A( I, I ), B( 1, I ), 1, T( I, 1 ) )
         IF( I.LT.N ) THEN
*
*           W(1:N-I) := C(I:M,I+1:N)**H * C(I:M,I) [use W = T(:,N)]
*
            DO J = 1, N-I
               T( J, N ) = CONJG(A( I, I+J ))
            END DO
            CALL ZGEMV( 'C', P, N-I, ONE, B( 1, I+1 ), LDB,
     $                  B( 1, I ), 1, ONE, T( 1, N ), 1 )
*
*           C(I:M,I+1:N) = C(I:m,I+1:N) + alpha*C(I:M,I)*W(1:N-1)**H
*
            ALPHA = -CONJG(T( I, 1 ))
            DO J = 1, N-I
               A( I, I+J ) = A( I, I+J ) + ALPHA*CONJG(T( J, N ))
            END DO
            CALL ZGERC( P, N-I, ALPHA, B( 1, I ), 1,
     $           T( 1, N ), 1, B( 1, I+1 ), LDB )
         END IF
      END DO
*
      DO I = 2, N
*
*        T(1:I-1,I) := C(I:M,1:I-1)**H * (alpha * C(I:M,I))
*
         ALPHA = -T( I, 1 )

         DO J = 1, I-1
            T( J, I ) = ZERO
         END DO
         P = MIN( I-1, L )
         MP = MIN( M-L+1, M )
         NP = MIN( P+1, N )
*
*        Triangular part of B2
*
         DO J = 1, P
            T( J, I ) = ALPHA*B( M-L+J, I )
         END DO
         CALL ZTRMV( 'U', 'C', 'N', P, B( MP, 1 ), LDB,
     $               T( 1, I ), 1 )
*
*        Rectangular part of B2
*
         CALL ZGEMV( 'C', L, I-1-P, ALPHA, B( MP, NP ), LDB,
     $               B( MP, I ), 1, ZERO, T( NP, I ), 1 )
*
*        B1
*
         CALL ZGEMV( 'C', M-L, I-1, ALPHA, B, LDB, B( 1, I ), 1,
     $               ONE, T( 1, I ), 1 )
*
*        T(1:I-1,I) := T(1:I-1,1:I-1) * T(1:I-1,I)
*
         CALL ZTRMV( 'U', 'N', 'N', I-1, T, LDT, T( 1, I ), 1 )
*
*        T(I,I) = tau(I)
*
         T( I, I ) = T( I, 1 )
         T( I, 1 ) = ZERO
      END DO

*
*     End of ZTPQRT2
*
      END
