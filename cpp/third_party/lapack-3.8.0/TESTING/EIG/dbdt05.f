*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DBDT05( M, N, A, LDA, S, NS, U, LDU,
*                          VT, LDVT, WORK, RESID )
*
*       .. Scalar Arguments ..
*       INTEGER            LDA, LDU, LDVT, N, NS
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
*> DBDT05 reconstructs a bidiagonal matrix B from its (partial) SVD:
*>    S = U' * B * V
*> where U and V are orthogonal matrices and S is diagonal.
*>
*> The test ratio to test the singular value decomposition is
*>    RESID = norm( S - U' * B * V ) / ( n * norm(B) * EPS )
*> where VT = V' and EPS is the machine precision.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrices A and U.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrices A and VT.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension (LDA,N)
*>          The m by n matrix A.
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,M).
*> \endverbatim
*>
*> \param[in] S
*> \verbatim
*>          S is DOUBLE PRECISION array, dimension (NS)
*>          The singular values from the (partial) SVD of B, sorted in
*>          decreasing order.
*> \endverbatim
*>
*> \param[in] NS
*> \verbatim
*>          NS is INTEGER
*>          The number of singular values/vectors from the (partial)
*>          SVD of B.
*> \endverbatim
*>
*> \param[in] U
*> \verbatim
*>          U is DOUBLE PRECISION array, dimension (LDU,NS)
*>          The n by ns orthogonal matrix U in S = U' * B * V.
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER
*>          The leading dimension of the array U.  LDU >= max(1,N)
*> \endverbatim
*>
*> \param[in] VT
*> \verbatim
*>          VT is DOUBLE PRECISION array, dimension (LDVT,N)
*>          The n by ns orthogonal matrix V in S = U' * B * V.
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
*>          WORK is DOUBLE PRECISION array, dimension (M,N)
*> \endverbatim
*>
*> \param[out] RESID
*> \verbatim
*>          RESID is DOUBLE PRECISION
*>          The test ratio:  norm(S - U' * A * V) / ( n * norm(A) * EPS )
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
      SUBROUTINE DBDT05( M, N, A, LDA, S, NS, U, LDU,
     $                    VT, LDVT, WORK, RESID )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            LDA, LDU, LDVT, M, N, NS
      DOUBLE PRECISION   RESID
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   A( LDA, * ), S( * ), U( LDU, * ),
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
      DOUBLE PRECISION   ANORM, EPS
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            IDAMAX
      DOUBLE PRECISION   DASUM, DLAMCH, DLANGE
      EXTERNAL           LSAME, IDAMAX, DASUM, DLAMCH, DLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           DGEMM
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Quick return if possible.
*
      RESID = ZERO
      IF( MIN( M, N ).LE.0 .OR. NS.LE.0 )
     $   RETURN
*
      EPS = DLAMCH( 'Precision' )
      ANORM = DLANGE( 'M', M, N, A, LDA, WORK )
*
*     Compute U' * A * V.
*
      CALL DGEMM( 'N', 'T', M, NS, N, ONE, A, LDA, VT,
     $            LDVT, ZERO, WORK( 1+NS*NS ), M )
      CALL DGEMM( 'T', 'N', NS, NS, M, -ONE, U, LDU, WORK( 1+NS*NS ),
     $            M, ZERO, WORK, NS )
*
*     norm(S - U' * B * V)
*
      J = 0
      DO 10 I = 1, NS
         WORK( J+I ) =  WORK( J+I ) + S( I )
         RESID = MAX( RESID, DASUM( NS, WORK( J+1 ), 1 ) )
         J = J + NS
   10 CONTINUE
*
      IF( ANORM.LE.ZERO ) THEN
         IF( RESID.NE.ZERO )
     $      RESID = ONE / EPS
      ELSE
         IF( ANORM.GE.RESID ) THEN
            RESID = ( RESID / ANORM ) / ( DBLE( N )*EPS )
         ELSE
            IF( ANORM.LT.ONE ) THEN
               RESID = ( MIN( RESID, DBLE( N )*ANORM ) / ANORM ) /
     $                 ( DBLE( N )*EPS )
            ELSE
               RESID = MIN( RESID / ANORM, DBLE( N ) ) /
     $                 ( DBLE( N )*EPS )
            END IF
         END IF
      END IF
*
      RETURN
*
*     End of DBDT05
*
      END
