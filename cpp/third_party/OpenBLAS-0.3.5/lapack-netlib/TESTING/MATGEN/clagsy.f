*> \brief \b CLAGSY
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CLAGSY( N, K, D, A, LDA, ISEED, WORK, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, K, LDA, N
*       ..
*       .. Array Arguments ..
*       INTEGER            ISEED( 4 )
*       REAL               D( * )
*       COMPLEX            A( LDA, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CLAGSY generates a complex symmetric matrix A, by pre- and post-
*> multiplying a real diagonal matrix D with a random unitary matrix:
*> A = U*D*U**T. The semi-bandwidth may then be reduced to k by
*> additional unitary transformations.
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
*> \param[in] K
*> \verbatim
*>          K is INTEGER
*>          The number of nonzero subdiagonals within the band of A.
*>          0 <= K <= N-1.
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is REAL array, dimension (N)
*>          The diagonal elements of the diagonal matrix D.
*> \endverbatim
*>
*> \param[out] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N)
*>          The generated n by n symmetric matrix A (the full matrix is
*>          stored).
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= N.
*> \endverbatim
*>
*> \param[in,out] ISEED
*> \verbatim
*>          ISEED is INTEGER array, dimension (4)
*>          On entry, the seed of the random number generator; the array
*>          elements must be between 0 and 4095, and ISEED(4) must be
*>          odd.
*>          On exit, the seed is updated.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX array, dimension (2*N)
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
*> \ingroup complex_matgen
*
*  =====================================================================
      SUBROUTINE CLAGSY( N, K, D, A, LDA, ISEED, WORK, INFO )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, K, LDA, N
*     ..
*     .. Array Arguments ..
      INTEGER            ISEED( 4 )
      REAL               D( * )
      COMPLEX            A( LDA, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX            ZERO, ONE, HALF
      PARAMETER          ( ZERO = ( 0.0E+0, 0.0E+0 ),
     $                   ONE = ( 1.0E+0, 0.0E+0 ),
     $                   HALF = ( 0.5E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      INTEGER            I, II, J, JJ
      REAL               WN
      COMPLEX            ALPHA, TAU, WA, WB
*     ..
*     .. External Subroutines ..
      EXTERNAL           CAXPY, CGEMV, CGERC, CLACGV, CLARNV, CSCAL,
     $                   CSYMV, XERBLA
*     ..
*     .. External Functions ..
      REAL               SCNRM2
      COMPLEX            CDOTC
      EXTERNAL           SCNRM2, CDOTC
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, REAL
*     ..
*     .. Executable Statements ..
*
*     Test the input arguments
*
      INFO = 0
      IF( N.LT.0 ) THEN
         INFO = -1
      ELSE IF( K.LT.0 .OR. K.GT.N-1 ) THEN
         INFO = -2
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -5
      END IF
      IF( INFO.LT.0 ) THEN
         CALL XERBLA( 'CLAGSY', -INFO )
         RETURN
      END IF
*
*     initialize lower triangle of A to diagonal matrix
*
      DO 20 J = 1, N
         DO 10 I = J + 1, N
            A( I, J ) = ZERO
   10    CONTINUE
   20 CONTINUE
      DO 30 I = 1, N
         A( I, I ) = D( I )
   30 CONTINUE
*
*     Generate lower triangle of symmetric matrix
*
      DO 60 I = N - 1, 1, -1
*
*        generate random reflection
*
         CALL CLARNV( 3, ISEED, N-I+1, WORK )
         WN = SCNRM2( N-I+1, WORK, 1 )
         WA = ( WN / ABS( WORK( 1 ) ) )*WORK( 1 )
         IF( WN.EQ.ZERO ) THEN
            TAU = ZERO
         ELSE
            WB = WORK( 1 ) + WA
            CALL CSCAL( N-I, ONE / WB, WORK( 2 ), 1 )
            WORK( 1 ) = ONE
            TAU = REAL( WB / WA )
         END IF
*
*        apply random reflection to A(i:n,i:n) from the left
*        and the right
*
*        compute  y := tau * A * conjg(u)
*
         CALL CLACGV( N-I+1, WORK, 1 )
         CALL CSYMV( 'Lower', N-I+1, TAU, A( I, I ), LDA, WORK, 1, ZERO,
     $               WORK( N+1 ), 1 )
         CALL CLACGV( N-I+1, WORK, 1 )
*
*        compute  v := y - 1/2 * tau * ( u, y ) * u
*
         ALPHA = -HALF*TAU*CDOTC( N-I+1, WORK, 1, WORK( N+1 ), 1 )
         CALL CAXPY( N-I+1, ALPHA, WORK, 1, WORK( N+1 ), 1 )
*
*        apply the transformation as a rank-2 update to A(i:n,i:n)
*
*        CALL CSYR2( 'Lower', N-I+1, -ONE, WORK, 1, WORK( N+1 ), 1,
*        $               A( I, I ), LDA )
*
         DO 50 JJ = I, N
            DO 40 II = JJ, N
               A( II, JJ ) = A( II, JJ ) -
     $                       WORK( II-I+1 )*WORK( N+JJ-I+1 ) -
     $                       WORK( N+II-I+1 )*WORK( JJ-I+1 )
   40       CONTINUE
   50    CONTINUE
   60 CONTINUE
*
*     Reduce number of subdiagonals to K
*
      DO 100 I = 1, N - 1 - K
*
*        generate reflection to annihilate A(k+i+1:n,i)
*
         WN = SCNRM2( N-K-I+1, A( K+I, I ), 1 )
         WA = ( WN / ABS( A( K+I, I ) ) )*A( K+I, I )
         IF( WN.EQ.ZERO ) THEN
            TAU = ZERO
         ELSE
            WB = A( K+I, I ) + WA
            CALL CSCAL( N-K-I, ONE / WB, A( K+I+1, I ), 1 )
            A( K+I, I ) = ONE
            TAU = REAL( WB / WA )
         END IF
*
*        apply reflection to A(k+i:n,i+1:k+i-1) from the left
*
         CALL CGEMV( 'Conjugate transpose', N-K-I+1, K-1, ONE,
     $               A( K+I, I+1 ), LDA, A( K+I, I ), 1, ZERO, WORK, 1 )
         CALL CGERC( N-K-I+1, K-1, -TAU, A( K+I, I ), 1, WORK, 1,
     $               A( K+I, I+1 ), LDA )
*
*        apply reflection to A(k+i:n,k+i:n) from the left and the right
*
*        compute  y := tau * A * conjg(u)
*
         CALL CLACGV( N-K-I+1, A( K+I, I ), 1 )
         CALL CSYMV( 'Lower', N-K-I+1, TAU, A( K+I, K+I ), LDA,
     $               A( K+I, I ), 1, ZERO, WORK, 1 )
         CALL CLACGV( N-K-I+1, A( K+I, I ), 1 )
*
*        compute  v := y - 1/2 * tau * ( u, y ) * u
*
         ALPHA = -HALF*TAU*CDOTC( N-K-I+1, A( K+I, I ), 1, WORK, 1 )
         CALL CAXPY( N-K-I+1, ALPHA, A( K+I, I ), 1, WORK, 1 )
*
*        apply symmetric rank-2 update to A(k+i:n,k+i:n)
*
*        CALL CSYR2( 'Lower', N-K-I+1, -ONE, A( K+I, I ), 1, WORK, 1,
*        $               A( K+I, K+I ), LDA )
*
         DO 80 JJ = K + I, N
            DO 70 II = JJ, N
               A( II, JJ ) = A( II, JJ ) - A( II, I )*WORK( JJ-K-I+1 ) -
     $                       WORK( II-K-I+1 )*A( JJ, I )
   70       CONTINUE
   80    CONTINUE
*
         A( K+I, I ) = -WA
         DO 90 J = K + I + 1, N
            A( J, I ) = ZERO
   90    CONTINUE
  100 CONTINUE
*
*     Store full symmetric matrix
*
      DO 120 J = 1, N
         DO 110 I = J + 1, N
            A( J, I ) = A( I, J )
  110    CONTINUE
  120 CONTINUE
      RETURN
*
*     End of CLAGSY
*
      END
