*> \brief \b CBDT05
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CBDT05( M, N, A, LDA, S, NS, U, LDU,
*                          VT, LDVT, WORK, RESID )
*
*       .. Scalar Arguments ..
*       INTEGER            LDA, LDU, LDVT, N, NS
*       REAL               RESID
*       ..
*       .. Array Arguments ..
*      REAL               S( * )
*      COMPLEX            A( LDA, * ), U( * ), VT( LDVT, * ), WORK( * )
*       ..
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CBDT05 reconstructs a bidiagonal matrix B from its (partial) SVD:
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
*>          A is COMPLEX array, dimension (LDA,N)
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
*>          S is REAL array, dimension (NS)
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
*>          U is COMPLEX array, dimension (LDU,NS)
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
*>          VT is COMPLEX array, dimension (LDVT,N)
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
*>          WORK is COMPLEX array, dimension (M,N)
*> \endverbatim
*>
*> \param[out] RESID
*> \verbatim
*>          RESID is REAL
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
      SUBROUTINE CBDT05( M, N, A, LDA, S, NS, U, LDU,
     $                    VT, LDVT, WORK, RESID )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            LDA, LDU, LDVT, M, N, NS
      REAL               RESID
*     ..
*     .. Array Arguments ..
      REAL               S( * )
      COMPLEX            A( LDA, * ), U( * ), VT( LDVT, * ), WORK( * )
*     ..
*
* ======================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
      COMPLEX            CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0E+0, 0.0E+0 ),
     $                   CONE = ( 1.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      INTEGER            I, J
      REAL               ANORM, EPS
*     ..
*     .. Local Arrays ..
      REAL               DUM( 1 )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ISAMAX
      REAL               SASUM, SLAMCH, CLANGE
      EXTERNAL           LSAME, ISAMAX, SASUM, SLAMCH, CLANGE
      REAL               SCASUM
*     ..
*     .. External Subroutines ..
      EXTERNAL           CGEMM
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, REAL, MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Quick return if possible.
*
      RESID = ZERO
      IF( MIN( M, N ).LE.0 .OR. NS.LE.0 )
     $   RETURN
*
      EPS = SLAMCH( 'Precision' )
      ANORM = CLANGE( 'M', M, N, A, LDA, DUM )
*
*     Compute U' * A * V.
*
      CALL CGEMM( 'N', 'C', M, NS, N, CONE, A, LDA, VT,
     $            LDVT, CZERO, WORK( 1+NS*NS ), M )
      CALL CGEMM( 'C', 'N', NS, NS, M, -CONE, U, LDU, WORK( 1+NS*NS ),
     $            M, CZERO, WORK, NS )
*
*     norm(S - U' * B * V)
*
      J = 0
      DO 10 I = 1, NS
         WORK( J+I ) =  WORK( J+I ) + CMPLX( S( I ), ZERO )
         RESID = MAX( RESID, SCASUM( NS, WORK( J+1 ), 1 ) )
         J = J + NS
   10 CONTINUE
*
      IF( ANORM.LE.ZERO ) THEN
         IF( RESID.NE.ZERO )
     $      RESID = ONE / EPS
      ELSE
         IF( ANORM.GE.RESID ) THEN
            RESID = ( RESID / ANORM ) / ( REAL( N )*EPS )
         ELSE
            IF( ANORM.LT.ONE ) THEN
               RESID = ( MIN( RESID, REAL( N )*ANORM ) / ANORM ) /
     $                 ( REAL( N )*EPS )
            ELSE
               RESID = MIN( RESID / ANORM, REAL( N ) ) /
     $                 ( REAL( N )*EPS )
            END IF
         END IF
      END IF
*
      RETURN
*
*     End of CBDT05
*
      END
