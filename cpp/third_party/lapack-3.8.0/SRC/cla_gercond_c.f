*> \brief \b CLA_GERCOND_C computes the infinity norm condition number of op(A)*inv(diag(c)) for general matrices.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CLA_GERCOND_C + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cla_gercond_c.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cla_gercond_c.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cla_gercond_c.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       REAL FUNCTION CLA_GERCOND_C( TRANS, N, A, LDA, AF, LDAF, IPIV, C,
*                                    CAPPLY, INFO, WORK, RWORK )
*
*       .. Scalar Aguments ..
*       CHARACTER          TRANS
*       LOGICAL            CAPPLY
*       INTEGER            N, LDA, LDAF, INFO
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * )
*       COMPLEX            A( LDA, * ), AF( LDAF, * ), WORK( * )
*       REAL               C( * ), RWORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>
*>    CLA_GERCOND_C computes the infinity norm condition number of
*>    op(A) * inv(diag(C)) where C is a REAL vector.
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
*> \param[in] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N)
*>     On entry, the N-by-N matrix A
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>     The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[in] AF
*> \verbatim
*>          AF is COMPLEX array, dimension (LDAF,N)
*>     The factors L and U from the factorization
*>     A = P*L*U as computed by CGETRF.
*> \endverbatim
*>
*> \param[in] LDAF
*> \verbatim
*>          LDAF is INTEGER
*>     The leading dimension of the array AF.  LDAF >= max(1,N).
*> \endverbatim
*>
*> \param[in] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (N)
*>     The pivot indices from the factorization A = P*L*U
*>     as computed by CGETRF; row i of the matrix was interchanged
*>     with row IPIV(i).
*> \endverbatim
*>
*> \param[in] C
*> \verbatim
*>          C is REAL array, dimension (N)
*>     The vector C in the formula op(A) * inv(diag(C)).
*> \endverbatim
*>
*> \param[in] CAPPLY
*> \verbatim
*>          CAPPLY is LOGICAL
*>     If .TRUE. then access the vector C in the formula above.
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
*>          WORK is COMPLEX array, dimension (2*N).
*>     Workspace.
*> \endverbatim
*>
*> \param[in] RWORK
*> \verbatim
*>          RWORK is REAL array, dimension (N).
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
*> \ingroup complexGEcomputational
*
*  =====================================================================
      REAL FUNCTION CLA_GERCOND_C( TRANS, N, A, LDA, AF, LDAF, IPIV, C,
     $                             CAPPLY, INFO, WORK, RWORK )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Aguments ..
      CHARACTER          TRANS
      LOGICAL            CAPPLY
      INTEGER            N, LDA, LDAF, INFO
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * )
      COMPLEX            A( LDA, * ), AF( LDAF, * ), WORK( * )
      REAL               C( * ), RWORK( * )
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      LOGICAL            NOTRANS
      INTEGER            KASE, I, J
      REAL               AINVNM, ANORM, TMP
      COMPLEX            ZDUM
*     ..
*     .. Local Arrays ..
      INTEGER            ISAVE( 3 )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           CLACN2, CGETRS, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, REAL, AIMAG
*     ..
*     .. Statement Functions ..
      REAL               CABS1
*     ..
*     .. Statement Function Definitions ..
      CABS1( ZDUM ) = ABS( REAL( ZDUM ) ) + ABS( AIMAG( ZDUM ) )
*     ..
*     .. Executable Statements ..
      CLA_GERCOND_C = 0.0E+0
*
      INFO = 0
      NOTRANS = LSAME( TRANS, 'N' )
      IF ( .NOT. NOTRANS .AND. .NOT. LSAME( TRANS, 'T' ) .AND. .NOT.
     $     LSAME( TRANS, 'C' ) ) THEN
         INFO = -1
      ELSE IF( N.LT.0 ) THEN
         INFO = -2
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -4
      ELSE IF( LDAF.LT.MAX( 1, N ) ) THEN
         INFO = -6
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CLA_GERCOND_C', -INFO )
         RETURN
      END IF
*
*     Compute norm of op(A)*op2(C).
*
      ANORM = 0.0E+0
      IF ( NOTRANS ) THEN
         DO I = 1, N
            TMP = 0.0E+0
            IF ( CAPPLY ) THEN
               DO J = 1, N
                  TMP = TMP + CABS1( A( I, J ) ) / C( J )
               END DO
            ELSE
               DO J = 1, N
                  TMP = TMP + CABS1( A( I, J ) )
               END DO
            END IF
            RWORK( I ) = TMP
            ANORM = MAX( ANORM, TMP )
         END DO
      ELSE
         DO I = 1, N
            TMP = 0.0E+0
            IF ( CAPPLY ) THEN
               DO J = 1, N
                  TMP = TMP + CABS1( A( J, I ) ) / C( J )
               END DO
            ELSE
               DO J = 1, N
                  TMP = TMP + CABS1( A( J, I ) )
               END DO
            END IF
            RWORK( I ) = TMP
            ANORM = MAX( ANORM, TMP )
         END DO
      END IF
*
*     Quick return if possible.
*
      IF( N.EQ.0 ) THEN
         CLA_GERCOND_C = 1.0E+0
         RETURN
      ELSE IF( ANORM .EQ. 0.0E+0 ) THEN
         RETURN
      END IF
*
*     Estimate the norm of inv(op(A)).
*
      AINVNM = 0.0E+0
*
      KASE = 0
   10 CONTINUE
      CALL CLACN2( N, WORK( N+1 ), WORK, AINVNM, KASE, ISAVE )
      IF( KASE.NE.0 ) THEN
         IF( KASE.EQ.2 ) THEN
*
*           Multiply by R.
*
            DO I = 1, N
               WORK( I ) = WORK( I ) * RWORK( I )
            END DO
*
            IF (NOTRANS) THEN
               CALL CGETRS( 'No transpose', N, 1, AF, LDAF, IPIV,
     $            WORK, N, INFO )
            ELSE
               CALL CGETRS( 'Conjugate transpose', N, 1, AF, LDAF, IPIV,
     $            WORK, N, INFO )
            ENDIF
*
*           Multiply by inv(C).
*
            IF ( CAPPLY ) THEN
               DO I = 1, N
                  WORK( I ) = WORK( I ) * C( I )
               END DO
            END IF
         ELSE
*
*           Multiply by inv(C**H).
*
            IF ( CAPPLY ) THEN
               DO I = 1, N
                  WORK( I ) = WORK( I ) * C( I )
               END DO
            END IF
*
            IF ( NOTRANS ) THEN
               CALL CGETRS( 'Conjugate transpose', N, 1, AF, LDAF, IPIV,
     $            WORK, N, INFO )
            ELSE
               CALL CGETRS( 'No transpose', N, 1, AF, LDAF, IPIV,
     $            WORK, N, INFO )
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
      IF( AINVNM .NE. 0.0E+0 )
     $   CLA_GERCOND_C = 1.0E+0 / AINVNM
*
      RETURN
*
      END
