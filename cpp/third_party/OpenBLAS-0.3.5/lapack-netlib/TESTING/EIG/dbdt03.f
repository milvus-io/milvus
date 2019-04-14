*> \brief \b DBDT03
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DBDT03( UPLO, N, KD, D, E, U, LDU, S, VT, LDVT, WORK,
*                          RESID )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            KD, LDU, LDVT, N
*       DOUBLE PRECISION   RESID
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   D( * ), E( * ), S( * ), U( LDU, * ),
*      $                   VT( LDVT, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DBDT03 reconstructs a bidiagonal matrix B from its SVD:
*>    S = U' * B * V
*> where U and V are orthogonal matrices and S is diagonal.
*>
*> The test ratio to test the singular value decomposition is
*>    RESID = norm( B - U * S * VT ) / ( n * norm(B) * EPS )
*> where VT = V' and EPS is the machine precision.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          Specifies whether the matrix B is upper or lower bidiagonal.
*>          = 'U':  Upper bidiagonal
*>          = 'L':  Lower bidiagonal
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix B.
*> \endverbatim
*>
*> \param[in] KD
*> \verbatim
*>          KD is INTEGER
*>          The bandwidth of the bidiagonal matrix B.  If KD = 1, the
*>          matrix B is bidiagonal, and if KD = 0, B is diagonal and E is
*>          not referenced.  If KD is greater than 1, it is assumed to be
*>          1, and if KD is less than 0, it is assumed to be 0.
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension (N)
*>          The n diagonal elements of the bidiagonal matrix B.
*> \endverbatim
*>
*> \param[in] E
*> \verbatim
*>          E is DOUBLE PRECISION array, dimension (N-1)
*>          The (n-1) superdiagonal elements of the bidiagonal matrix B
*>          if UPLO = 'U', or the (n-1) subdiagonal elements of B if
*>          UPLO = 'L'.
*> \endverbatim
*>
*> \param[in] U
*> \verbatim
*>          U is DOUBLE PRECISION array, dimension (LDU,N)
*>          The n by n orthogonal matrix U in the reduction B = U'*A*P.
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER
*>          The leading dimension of the array U.  LDU >= max(1,N)
*> \endverbatim
*>
*> \param[in] S
*> \verbatim
*>          S is DOUBLE PRECISION array, dimension (N)
*>          The singular values from the SVD of B, sorted in decreasing
*>          order.
*> \endverbatim
*>
*> \param[in] VT
*> \verbatim
*>          VT is DOUBLE PRECISION array, dimension (LDVT,N)
*>          The n by n orthogonal matrix V' in the reduction
*>          B = U * S * V'.
*> \endverbatim
*>
*> \param[in] LDVT
*> \verbatim
*>          LDVT is INTEGER
*>          The leading dimension of the array VT.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (2*N)
*> \endverbatim
*>
*> \param[out] RESID
*> \verbatim
*>          RESID is DOUBLE PRECISION
*>          The test ratio:  norm(B - U * S * V') / ( n * norm(A) * EPS )
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
      SUBROUTINE DBDT03( UPLO, N, KD, D, E, U, LDU, S, VT, LDVT, WORK,
     $                   RESID )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            KD, LDU, LDVT, N
      DOUBLE PRECISION   RESID
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   D( * ), E( * ), S( * ), U( LDU, * ),
     $                   VT( LDVT, * ), WORK( * )
*     ..
*
* ======================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, J
      DOUBLE PRECISION   BNORM, EPS
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            IDAMAX
      DOUBLE PRECISION   DASUM, DLAMCH
      EXTERNAL           LSAME, IDAMAX, DASUM, DLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           DGEMV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Quick return if possible
*
      RESID = ZERO
      IF( N.LE.0 )
     $   RETURN
*
*     Compute B - U * S * V' one column at a time.
*
      BNORM = ZERO
      IF( KD.GE.1 ) THEN
*
*        B is bidiagonal.
*
         IF( LSAME( UPLO, 'U' ) ) THEN
*
*           B is upper bidiagonal.
*
            DO 20 J = 1, N
               DO 10 I = 1, N
                  WORK( N+I ) = S( I )*VT( I, J )
   10          CONTINUE
               CALL DGEMV( 'No transpose', N, N, -ONE, U, LDU,
     $                     WORK( N+1 ), 1, ZERO, WORK, 1 )
               WORK( J ) = WORK( J ) + D( J )
               IF( J.GT.1 ) THEN
                  WORK( J-1 ) = WORK( J-1 ) + E( J-1 )
                  BNORM = MAX( BNORM, ABS( D( J ) )+ABS( E( J-1 ) ) )
               ELSE
                  BNORM = MAX( BNORM, ABS( D( J ) ) )
               END IF
               RESID = MAX( RESID, DASUM( N, WORK, 1 ) )
   20       CONTINUE
         ELSE
*
*           B is lower bidiagonal.
*
            DO 40 J = 1, N
               DO 30 I = 1, N
                  WORK( N+I ) = S( I )*VT( I, J )
   30          CONTINUE
               CALL DGEMV( 'No transpose', N, N, -ONE, U, LDU,
     $                     WORK( N+1 ), 1, ZERO, WORK, 1 )
               WORK( J ) = WORK( J ) + D( J )
               IF( J.LT.N ) THEN
                  WORK( J+1 ) = WORK( J+1 ) + E( J )
                  BNORM = MAX( BNORM, ABS( D( J ) )+ABS( E( J ) ) )
               ELSE
                  BNORM = MAX( BNORM, ABS( D( J ) ) )
               END IF
               RESID = MAX( RESID, DASUM( N, WORK, 1 ) )
   40       CONTINUE
         END IF
      ELSE
*
*        B is diagonal.
*
         DO 60 J = 1, N
            DO 50 I = 1, N
               WORK( N+I ) = S( I )*VT( I, J )
   50       CONTINUE
            CALL DGEMV( 'No transpose', N, N, -ONE, U, LDU, WORK( N+1 ),
     $                  1, ZERO, WORK, 1 )
            WORK( J ) = WORK( J ) + D( J )
            RESID = MAX( RESID, DASUM( N, WORK, 1 ) )
   60    CONTINUE
         J = IDAMAX( N, D, 1 )
         BNORM = ABS( D( J ) )
      END IF
*
*     Compute norm(B - U * S * V') / ( n * norm(B) * EPS )
*
      EPS = DLAMCH( 'Precision' )
*
      IF( BNORM.LE.ZERO ) THEN
         IF( RESID.NE.ZERO )
     $      RESID = ONE / EPS
      ELSE
         IF( BNORM.GE.RESID ) THEN
            RESID = ( RESID / BNORM ) / ( DBLE( N )*EPS )
         ELSE
            IF( BNORM.LT.ONE ) THEN
               RESID = ( MIN( RESID, DBLE( N )*BNORM ) / BNORM ) /
     $                 ( DBLE( N )*EPS )
            ELSE
               RESID = MIN( RESID / BNORM, DBLE( N ) ) /
     $                 ( DBLE( N )*EPS )
            END IF
         END IF
      END IF
*
      RETURN
*
*     End of DBDT03
*
      END
