*> \brief \b DTPLQT
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DTPQRT + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dtplqt.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dtplqt.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dtplqt.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DTPLQT( M, N, L, MB, A, LDA, B, LDB, T, LDT, WORK,
*                          INFO )
*
*       .. Scalar Arguments ..
*       INTEGER           INFO, LDA, LDB, LDT, N, M, L, MB
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION  A( LDA, * ), B( LDB, * ), T( LDT, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DTPLQT computes a blocked LQ factorization of a real
*> "triangular-pentagonal" matrix C, which is composed of a
*> triangular block A and pentagonal block B, using the compact
*> WY representation for Q.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix B, and the order of the
*>          triangular matrix A.
*>          M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix B.
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
*> \param[in] MB
*> \verbatim
*>          MB is INTEGER
*>          The block size to be used in the blocked QR.  M >= MB >= 1.
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
*>          T is DOUBLE PRECISION array, dimension (LDT,N)
*>          The lower triangular block reflectors stored in compact form
*>          as a sequence of upper triangular blocks.  See Further Details.
*> \endverbatim
*>
*> \param[in] LDT
*> \verbatim
*>          LDT is INTEGER
*>          The leading dimension of the array T.  LDT >= MB.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (MB*M)
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
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
*>               C = [ A ] [ B ]
*>
*>
*>  where A is an lower triangular M-by-M matrix, and B is M-by-N pentagonal
*>  matrix consisting of a M-by-(N-L) rectangular matrix B1 on left of a M-by-L
*>  upper trapezoidal matrix B2:
*>          [ B ] = [ B1 ] [ B2 ]
*>                   [ B1 ]  <- M-by-(N-L) rectangular
*>                   [ B2 ]  <-     M-by-L lower trapezoidal.
*>
*>  The lower trapezoidal matrix B2 consists of the first L columns of a
*>  M-by-M lower triangular matrix, where 0 <= L <= MIN(M,N).  If L=0,
*>  B is rectangular M-by-N; if M=L=N, B is lower triangular.
*>
*>  The matrix W stores the elementary reflectors H(i) in the i-th row
*>  above the diagonal (of A) in the M-by-(M+N) input matrix C
*>            [ C ] = [ A ] [ B ]
*>                   [ A ]  <- lower triangular M-by-M
*>                   [ B ]  <- M-by-N pentagonal
*>
*>  so that W can be represented as
*>            [ W ] = [ I ] [ V ]
*>                   [ I ]  <- identity, M-by-M
*>                   [ V ]  <- M-by-N, same form as B.
*>
*>  Thus, all of information needed for W is contained on exit in B, which
*>  we call V above.  Note that V has the same form as B; that is,
*>            [ V ] = [ V1 ] [ V2 ]
*>                   [ V1 ] <- M-by-(N-L) rectangular
*>                   [ V2 ] <-     M-by-L lower trapezoidal.
*>
*>  The rows of V represent the vectors which define the H(i)'s.
*>
*>  The number of blocks is B = ceiling(M/MB), where each
*>  block is of order MB except for the last block, which is of order
*>  IB = M - (M-1)*MB.  For each of the B blocks, a upper triangular block
*>  reflector factor is computed: T1, T2, ..., TB.  The MB-by-MB (and IB-by-IB
*>  for the last block) T's are stored in the MB-by-N matrix T as
*>
*>               T = [T1 T2 ... TB].
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE DTPLQT( M, N, L, MB, A, LDA, B, LDB, T, LDT, WORK,
     $                   INFO )
*
*  -- LAPACK computational routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      INTEGER INFO, LDA, LDB, LDT, N, M, L, MB
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION A( LDA, * ), B( LDB, * ), T( LDT, * ), WORK( * )
*     ..
*
* =====================================================================
*
*     ..
*     .. Local Scalars ..
      INTEGER    I, IB, LB, NB, IINFO
*     ..
*     .. External Subroutines ..
      EXTERNAL   DTPLQT2, DTPRFB, XERBLA
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
      ELSE IF( L.LT.0 .OR. (L.GT.MIN(M,N) .AND. MIN(M,N).GE.0)) THEN
         INFO = -3
      ELSE IF( MB.LT.1 .OR. (MB.GT.M .AND. M.GT.0)) THEN
         INFO = -4
      ELSE IF( LDA.LT.MAX( 1, M ) ) THEN
         INFO = -6
      ELSE IF( LDB.LT.MAX( 1, M ) ) THEN
         INFO = -8
      ELSE IF( LDT.LT.MB ) THEN
         INFO = -10
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DTPLQT', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( M.EQ.0 .OR. N.EQ.0 ) RETURN
*
      DO I = 1, M, MB
*
*     Compute the QR factorization of the current block
*
         IB = MIN( M-I+1, MB )
         NB = MIN( N-L+I+IB-1, N )
         IF( I.GE.L ) THEN
            LB = 0
         ELSE
            LB = NB-N+L-I+1
         END IF
*
         CALL DTPLQT2( IB, NB, LB, A(I,I), LDA, B( I, 1 ), LDB,
     $                 T(1, I ), LDT, IINFO )
*
*     Update by applying H**T to B(I+IB:M,:) from the right
*
         IF( I+IB.LE.M ) THEN
            CALL DTPRFB( 'R', 'N', 'F', 'R', M-I-IB+1, NB, IB, LB,
     $                    B( I, 1 ), LDB, T( 1, I ), LDT,
     $                    A( I+IB, I ), LDA, B( I+IB, 1 ), LDB,
     $                    WORK, M-I-IB+1)
         END IF
      END DO
      RETURN
*
*     End of DTPLQT
*
      END
