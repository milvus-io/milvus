*> \brief \b CSYEQUB
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CSYEQUB + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/csyequb.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/csyequb.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/csyequb.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CSYEQUB( UPLO, N, A, LDA, S, SCOND, AMAX, WORK, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, N
*       REAL               AMAX, SCOND
*       CHARACTER          UPLO
*       ..
*       .. Array Arguments ..
*       COMPLEX            A( LDA, * ), WORK( * )
*       REAL               S( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CSYEQUB computes row and column scalings intended to equilibrate a
*> symmetric matrix A (with respect to the Euclidean norm) and reduce
*> its condition number. The scale factors S are computed by the BIN
*> algorithm (see references) so that the scaled matrix B with elements
*> B(i,j) = S(i)*A(i,j)*S(j) has a condition number within a factor N of
*> the smallest possible condition number over all possible diagonal
*> scalings.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          = 'U':  Upper triangle of A is stored;
*>          = 'L':  Lower triangle of A is stored.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix A. N >= 0.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N)
*>          The N-by-N symmetric matrix whose scaling factors are to be
*>          computed.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A. LDA >= max(1,N).
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is REAL array, dimension (N)
*>          If INFO = 0, S contains the scale factors for A.
*> \endverbatim
*>
*> \param[out] SCOND
*> \verbatim
*>          SCOND is REAL
*>          If INFO = 0, S contains the ratio of the smallest S(i) to
*>          the largest S(i). If SCOND >= 0.1 and AMAX is neither too
*>          large nor too small, it is not worth scaling by S.
*> \endverbatim
*>
*> \param[out] AMAX
*> \verbatim
*>          AMAX is REAL
*>          Largest absolute value of any matrix element. If AMAX is
*>          very close to overflow or very close to underflow, the
*>          matrix should be scaled.
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
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
*>          > 0:  if INFO = i, the i-th diagonal element is nonpositive.
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
*> \date November 2017
*
*> \ingroup complexSYcomputational
*
*> \par References:
*  ================
*>
*>  Livne, O.E. and Golub, G.H., "Scaling by Binormalization", \n
*>  Numerical Algorithms, vol. 35, no. 1, pp. 97-120, January 2004. \n
*>  DOI 10.1023/B:NUMA.0000016606.32820.69 \n
*>  Tech report version: http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.3.1679
*>
*  =====================================================================
      SUBROUTINE CSYEQUB( UPLO, N, A, LDA, S, SCOND, AMAX, WORK, INFO )
*
*  -- LAPACK computational routine (version 3.8.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2017
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, N
      REAL               AMAX, SCOND
      CHARACTER          UPLO
*     ..
*     .. Array Arguments ..
      COMPLEX            A( LDA, * ), WORK( * )
      REAL               S( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ONE, ZERO
      PARAMETER          ( ONE = 1.0E0, ZERO = 0.0E0 )
      INTEGER            MAX_ITER
      PARAMETER          ( MAX_ITER = 100 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, J, ITER
      REAL               AVG, STD, TOL, C0, C1, C2, T, U, SI, D, BASE,
     $                   SMIN, SMAX, SMLNUM, BIGNUM, SCALE, SUMSQ
      LOGICAL            UP
      COMPLEX            ZDUM
*     ..
*     .. External Functions ..
      REAL               SLAMCH
      LOGICAL            LSAME
      EXTERNAL           LSAME, SLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           CLASSQ, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, AIMAG, INT, LOG, MAX, MIN, REAL, SQRT
*     ..
*     .. Statement Functions ..
      REAL               CABS1
*     ..
*     .. Statement Function Definitions ..
      CABS1( ZDUM ) = ABS( REAL( ZDUM ) ) + ABS( AIMAG( ZDUM ) )
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
      IF ( .NOT. ( LSAME( UPLO, 'U' ) .OR. LSAME( UPLO, 'L' ) ) ) THEN
         INFO = -1
      ELSE IF ( N .LT. 0 ) THEN
         INFO = -2
      ELSE IF ( LDA .LT. MAX( 1, N ) ) THEN
         INFO = -4
      END IF
      IF ( INFO .NE. 0 ) THEN
         CALL XERBLA( 'CSYEQUB', -INFO )
         RETURN
      END IF

      UP = LSAME( UPLO, 'U' )
      AMAX = ZERO
*
*     Quick return if possible.
*
      IF ( N .EQ. 0 ) THEN
         SCOND = ONE
         RETURN
      END IF

      DO I = 1, N
         S( I ) = ZERO
      END DO

      AMAX = ZERO
      IF ( UP ) THEN
         DO J = 1, N
            DO I = 1, J-1
               S( I ) = MAX( S( I ), CABS1( A( I, J ) ) )
               S( J ) = MAX( S( J ), CABS1( A( I, J ) ) )
               AMAX = MAX( AMAX, CABS1( A( I, J ) ) )
            END DO
            S( J ) = MAX( S( J ), CABS1( A( J, J ) ) )
            AMAX = MAX( AMAX, CABS1( A( J, J ) ) )
         END DO
      ELSE
         DO J = 1, N
            S( J ) = MAX( S( J ), CABS1( A( J, J ) ) )
            AMAX = MAX( AMAX, CABS1( A( J, J ) ) )
            DO I = J+1, N
               S( I ) = MAX( S( I ), CABS1( A( I, J ) ) )
               S( J ) = MAX( S( J ), CABS1( A( I, J ) ) )
               AMAX = MAX( AMAX, CABS1( A( I, J ) ) )
            END DO
         END DO
      END IF
      DO J = 1, N
         S( J ) = 1.0 / S( J )
      END DO

      TOL = ONE / SQRT( 2.0E0 * N )

      DO ITER = 1, MAX_ITER
         SCALE = 0.0E0
         SUMSQ = 0.0E0
*        beta = |A|s
         DO I = 1, N
             WORK( I ) = ZERO
         END DO
         IF ( UP ) THEN
            DO J = 1, N
               DO I = 1, J-1
                  WORK( I ) = WORK( I ) + CABS1( A( I, J ) ) * S( J )
                  WORK( J ) = WORK( J ) + CABS1( A( I, J ) ) * S( I )
               END DO
               WORK( J ) = WORK( J ) + CABS1( A( J, J ) ) * S( J )
            END DO
         ELSE
            DO J = 1, N
               WORK( J ) = WORK( J ) + CABS1( A( J, J ) ) * S( J )
               DO I = J+1, N
                  WORK( I ) = WORK( I ) + CABS1( A( I, J ) ) * S( J )
                  WORK( J ) = WORK( J ) + CABS1( A( I, J ) ) * S( I )
               END DO
            END DO
         END IF

*        avg = s^T beta / n
         AVG = 0.0E0
         DO I = 1, N
            AVG = AVG + S( I )*WORK( I )
         END DO
         AVG = AVG / N

         STD = 0.0E0
         DO I = N+1, 2*N
            WORK( I ) = S( I-N ) * WORK( I-N ) - AVG
         END DO
         CALL CLASSQ( N, WORK( N+1 ), 1, SCALE, SUMSQ )
         STD = SCALE * SQRT( SUMSQ / N )

         IF ( STD .LT. TOL * AVG ) GOTO 999

         DO I = 1, N
            T = CABS1( A( I, I ) )
            SI = S( I )
            C2 = ( N-1 ) * T
            C1 = ( N-2 ) * ( WORK( I ) - T*SI )
            C0 = -(T*SI)*SI + 2*WORK( I )*SI - N*AVG
            D = C1*C1 - 4*C0*C2

            IF ( D .LE. 0 ) THEN
               INFO = -1
               RETURN
            END IF
            SI = -2*C0 / ( C1 + SQRT( D ) )

            D = SI - S( I )
            U = ZERO
            IF ( UP ) THEN
               DO J = 1, I
                  T = CABS1( A( J, I ) )
                  U = U + S( J )*T
                  WORK( J ) = WORK( J ) + D*T
               END DO
               DO J = I+1,N
                  T = CABS1( A( I, J ) )
                  U = U + S( J )*T
                  WORK( J ) = WORK( J ) + D*T
               END DO
            ELSE
               DO J = 1, I
                  T = CABS1( A( I, J ) )
                  U = U + S( J )*T
                  WORK( J ) = WORK( J ) + D*T
               END DO
               DO J = I+1,N
                  T = CABS1( A( J, I ) )
                  U = U + S( J )*T
                  WORK( J ) = WORK( J ) + D*T
               END DO
            END IF

            AVG = AVG + ( U + WORK( I ) ) * D / N
            S( I ) = SI
         END DO
      END DO

 999  CONTINUE

      SMLNUM = SLAMCH( 'SAFEMIN' )
      BIGNUM = ONE / SMLNUM
      SMIN = BIGNUM
      SMAX = ZERO
      T = ONE / SQRT( AVG )
      BASE = SLAMCH( 'B' )
      U = ONE / LOG( BASE )
      DO I = 1, N
         S( I ) = BASE ** INT( U * LOG( S( I ) * T ) )
         SMIN = MIN( SMIN, S( I ) )
         SMAX = MAX( SMAX, S( I ) )
      END DO
      SCOND = MAX( SMIN, SMLNUM ) / MIN( SMAX, BIGNUM )
*
      END
