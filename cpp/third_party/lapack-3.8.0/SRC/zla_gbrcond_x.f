*> \brief \b ZLA_GBRCOND_X computes the infinity norm condition number of op(A)*diag(x) for general banded matrices.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZLA_GBRCOND_X + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zla_gbrcond_x.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zla_gbrcond_x.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zla_gbrcond_x.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       DOUBLE PRECISION FUNCTION ZLA_GBRCOND_X( TRANS, N, KL, KU, AB,
*                                                LDAB, AFB, LDAFB, IPIV,
*                                                X, INFO, WORK, RWORK )
*
*       .. Scalar Arguments ..
*       CHARACTER          TRANS
*       INTEGER            N, KL, KU, KD, KE, LDAB, LDAFB, INFO
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * )
*       COMPLEX*16         AB( LDAB, * ), AFB( LDAFB, * ), WORK( * ),
*      $                   X( * )
*       DOUBLE PRECISION   RWORK( * )
*
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    ZLA_GBRCOND_X Computes the infinity norm condition number of
*>    op(A) * diag(X) where X is a COMPLEX*16 vector.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] TRANS
*> \verbatim
*>          TRANS is CHARACTER*1
*>     Specifies the form of the system of equations:
*>       = 'N':  A * X = B     (No transpose)
*>       = 'T':  A**T * X = B  (Transpose)
*>       = 'C':  A**H * X = B  (Conjugate Transpose = Transpose)
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>     The number of linear equations, i.e., the order of the
*>     matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] KL
*> \verbatim
*>          KL is INTEGER
*>     The number of subdiagonals within the band of A.  KL >= 0.
*> \endverbatim
*>
*> \param[in] KU
*> \verbatim
*>          KU is INTEGER
*>     The number of superdiagonals within the band of A.  KU >= 0.
*> \endverbatim
*>
*> \param[in] AB
*> \verbatim
*>          AB is COMPLEX*16 array, dimension (LDAB,N)
*>     On entry, the matrix A in band storage, in rows 1 to KL+KU+1.
*>     The j-th column of A is stored in the j-th column of the
*>     array AB as follows:
*>     AB(KU+1+i-j,j) = A(i,j) for max(1,j-KU)<=i<=min(N,j+kl)
*> \endverbatim
*>
*> \param[in] LDAB
*> \verbatim
*>          LDAB is INTEGER
*>     The leading dimension of the array AB.  LDAB >= KL+KU+1.
*> \endverbatim
*>
*> \param[in] AFB
*> \verbatim
*>          AFB is COMPLEX*16 array, dimension (LDAFB,N)
*>     Details of the LU factorization of the band matrix A, as
*>     computed by ZGBTRF.  U is stored as an upper triangular
*>     band matrix with KL+KU superdiagonals in rows 1 to KL+KU+1,
*>     and the multipliers used during the factorization are stored
*>     in rows KL+KU+2 to 2*KL+KU+1.
*> \endverbatim
*>
*> \param[in] LDAFB
*> \verbatim
*>          LDAFB is INTEGER
*>     The leading dimension of the array AFB.  LDAFB >= 2*KL+KU+1.
*> \endverbatim
*>
*> \param[in] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (N)
*>     The pivot indices from the factorization A = P*L*U
*>     as computed by ZGBTRF; row i of the matrix was interchanged
*>     with row IPIV(i).
*> \endverbatim
*>
*> \param[in] X
*> \verbatim
*>          X is COMPLEX*16 array, dimension (N)
*>     The vector X in the formula op(A) * diag(X).
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>       = 0:  Successful exit.
*>     i > 0:  The ith argument is invalid.
*> \endverbatim
*>
*> \param[in] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (2*N).
*>     Workspace.
*> \endverbatim
*>
*> \param[in] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (N).
*>     Workspace.
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
*> \ingroup complex16GBcomputational
*
*  =====================================================================
      DOUBLE PRECISION FUNCTION ZLA_GBRCOND_X( TRANS, N, KL, KU, AB,
     $                                         LDAB, AFB, LDAFB, IPIV,
     $                                         X, INFO, WORK, RWORK )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          TRANS
      INTEGER            N, KL, KU, KD, KE, LDAB, LDAFB, INFO
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * )
      COMPLEX*16         AB( LDAB, * ), AFB( LDAFB, * ), WORK( * ),
     $                   X( * )
      DOUBLE PRECISION   RWORK( * )
*
*
*  =====================================================================
*
*     .. Local Scalars ..
      LOGICAL            NOTRANS
      INTEGER            KASE, I, J
      DOUBLE PRECISION   AINVNM, ANORM, TMP
      COMPLEX*16         ZDUM
*     ..
*     .. Local Arrays ..
      INTEGER            ISAVE( 3 )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZLACN2, ZGBTRS, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX
*     ..
*     .. Statement Functions ..
      DOUBLE PRECISION   CABS1
*     ..
*     .. Statement Function Definitions ..
      CABS1( ZDUM ) = ABS( DBLE( ZDUM ) ) + ABS( DIMAG( ZDUM ) )
*     ..
*     .. Executable Statements ..
*
      ZLA_GBRCOND_X = 0.0D+0
*
      INFO = 0
      NOTRANS = LSAME( TRANS, 'N' )
      IF ( .NOT. NOTRANS .AND. .NOT. LSAME(TRANS, 'T') .AND. .NOT.
     $     LSAME( TRANS, 'C' ) ) THEN
         INFO = -1
      ELSE IF( N.LT.0 ) THEN
         INFO = -2
      ELSE IF( KL.LT.0 .OR. KL.GT.N-1 ) THEN
         INFO = -3
      ELSE IF( KU.LT.0 .OR. KU.GT.N-1 ) THEN
         INFO = -4
      ELSE IF( LDAB.LT.KL+KU+1 ) THEN
         INFO = -6
      ELSE IF( LDAFB.LT.2*KL+KU+1 ) THEN
         INFO = -8
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZLA_GBRCOND_X', -INFO )
         RETURN
      END IF
*
*     Compute norm of op(A)*op2(C).
*
      KD = KU + 1
      KE = KL + 1
      ANORM = 0.0D+0
      IF ( NOTRANS ) THEN
         DO I = 1, N
            TMP = 0.0D+0
            DO J = MAX( I-KL, 1 ), MIN( I+KU, N )
               TMP = TMP + CABS1( AB( KD+I-J, J) * X( J ) )
            END DO
            RWORK( I ) = TMP
            ANORM = MAX( ANORM, TMP )
         END DO
      ELSE
         DO I = 1, N
            TMP = 0.0D+0
            DO J = MAX( I-KL, 1 ), MIN( I+KU, N )
               TMP = TMP + CABS1( AB( KE-I+J, I ) * X( J ) )
            END DO
            RWORK( I ) = TMP
            ANORM = MAX( ANORM, TMP )
         END DO
      END IF
*
*     Quick return if possible.
*
      IF( N.EQ.0 ) THEN
         ZLA_GBRCOND_X = 1.0D+0
         RETURN
      ELSE IF( ANORM .EQ. 0.0D+0 ) THEN
         RETURN
      END IF
*
*     Estimate the norm of inv(op(A)).
*
      AINVNM = 0.0D+0
*
      KASE = 0
   10 CONTINUE
      CALL ZLACN2( N, WORK( N+1 ), WORK, AINVNM, KASE, ISAVE )
      IF( KASE.NE.0 ) THEN
         IF( KASE.EQ.2 ) THEN
*
*           Multiply by R.
*
            DO I = 1, N
               WORK( I ) = WORK( I ) * RWORK( I )
            END DO
*
            IF ( NOTRANS ) THEN
               CALL ZGBTRS( 'No transpose', N, KL, KU, 1, AFB, LDAFB,
     $              IPIV, WORK, N, INFO )
            ELSE
               CALL ZGBTRS( 'Conjugate transpose', N, KL, KU, 1, AFB,
     $              LDAFB, IPIV, WORK, N, INFO )
            ENDIF
*
*           Multiply by inv(X).
*
            DO I = 1, N
               WORK( I ) = WORK( I ) / X( I )
            END DO
         ELSE
*
*           Multiply by inv(X**H).
*
            DO I = 1, N
               WORK( I ) = WORK( I ) / X( I )
            END DO
*
            IF ( NOTRANS ) THEN
               CALL ZGBTRS( 'Conjugate transpose', N, KL, KU, 1, AFB,
     $              LDAFB, IPIV, WORK, N, INFO )
            ELSE
               CALL ZGBTRS( 'No transpose', N, KL, KU, 1, AFB, LDAFB,
     $              IPIV, WORK, N, INFO )
            END IF
*
*           Multiply by R.
*
            DO I = 1, N
               WORK( I ) = WORK( I ) * RWORK( I )
            END DO
         END IF
         GO TO 10
      END IF
*
*     Compute the estimate of the reciprocal condition number.
*
      IF( AINVNM .NE. 0.0D+0 )
     $   ZLA_GBRCOND_X = 1.0D+0 / AINVNM
*
      RETURN
*
      END
