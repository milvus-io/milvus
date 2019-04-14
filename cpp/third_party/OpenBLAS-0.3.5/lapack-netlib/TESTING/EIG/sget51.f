*> \brief \b SGET51
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SGET51( ITYPE, N, A, LDA, B, LDB, U, LDU, V, LDV, WORK,
*                          RESULT )
*
*       .. Scalar Arguments ..
*       INTEGER            ITYPE, LDA, LDB, LDU, LDV, N
*       REAL               RESULT
*       ..
*       .. Array Arguments ..
*       REAL               A( LDA, * ), B( LDB, * ), U( LDU, * ),
*      $                   V( LDV, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>      SGET51  generally checks a decomposition of the form
*>
*>              A = U B V'
*>
*>      where ' means transpose and U and V are orthogonal.
*>
*>      Specifically, if ITYPE=1
*>
*>              RESULT = | A - U B V' | / ( |A| n ulp )
*>
*>      If ITYPE=2, then:
*>
*>              RESULT = | A - B | / ( |A| n ulp )
*>
*>      If ITYPE=3, then:
*>
*>              RESULT = | I - UU' | / ( n ulp )
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] ITYPE
*> \verbatim
*>          ITYPE is INTEGER
*>          Specifies the type of tests to be performed.
*>          =1: RESULT = | A - U B V' | / ( |A| n ulp )
*>          =2: RESULT = | A - B | / ( |A| n ulp )
*>          =3: RESULT = | I - UU' | / ( n ulp )
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The size of the matrix.  If it is zero, SGET51 does nothing.
*>          It must be at least zero.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is REAL array, dimension (LDA, N)
*>          The original (unfactored) matrix.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of A.  It must be at least 1
*>          and at least N.
*> \endverbatim
*>
*> \param[in] B
*> \verbatim
*>          B is REAL array, dimension (LDB, N)
*>          The factored matrix.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of B.  It must be at least 1
*>          and at least N.
*> \endverbatim
*>
*> \param[in] U
*> \verbatim
*>          U is REAL array, dimension (LDU, N)
*>          The orthogonal matrix on the left-hand side in the
*>          decomposition.
*>          Not referenced if ITYPE=2
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER
*>          The leading dimension of U.  LDU must be at least N and
*>          at least 1.
*> \endverbatim
*>
*> \param[in] V
*> \verbatim
*>          V is REAL array, dimension (LDV, N)
*>          The orthogonal matrix on the left-hand side in the
*>          decomposition.
*>          Not referenced if ITYPE=2
*> \endverbatim
*>
*> \param[in] LDV
*> \verbatim
*>          LDV is INTEGER
*>          The leading dimension of V.  LDV must be at least N and
*>          at least 1.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (2*N**2)
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is REAL
*>          The values computed by the test specified by ITYPE.  The
*>          value is currently limited to 1/ulp, to avoid overflow.
*>          Errors are flagged by RESULT=10/ulp.
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
*> \ingroup single_eig
*
*  =====================================================================
      SUBROUTINE SGET51( ITYPE, N, A, LDA, B, LDB, U, LDU, V, LDV, WORK,
     $                   RESULT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            ITYPE, LDA, LDB, LDU, LDV, N
      REAL               RESULT
*     ..
*     .. Array Arguments ..
      REAL               A( LDA, * ), B( LDB, * ), U( LDU, * ),
     $                   V( LDV, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE, TEN
      PARAMETER          ( ZERO = 0.0, ONE = 1.0E0, TEN = 10.0E0 )
*     ..
*     .. Local Scalars ..
      INTEGER            JCOL, JDIAG, JROW
      REAL               ANORM, ULP, UNFL, WNORM
*     ..
*     .. External Functions ..
      REAL               SLAMCH, SLANGE
      EXTERNAL           SLAMCH, SLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           SGEMM, SLACPY
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN, REAL
*     ..
*     .. Executable Statements ..
*
      RESULT = ZERO
      IF( N.LE.0 )
     $   RETURN
*
*     Constants
*
      UNFL = SLAMCH( 'Safe minimum' )
      ULP = SLAMCH( 'Epsilon' )*SLAMCH( 'Base' )
*
*     Some Error Checks
*
      IF( ITYPE.LT.1 .OR. ITYPE.GT.3 ) THEN
         RESULT = TEN / ULP
         RETURN
      END IF
*
      IF( ITYPE.LE.2 ) THEN
*
*        Tests scaled by the norm(A)
*
         ANORM = MAX( SLANGE( '1', N, N, A, LDA, WORK ), UNFL )
*
         IF( ITYPE.EQ.1 ) THEN
*
*           ITYPE=1: Compute W = A - UBV'
*
            CALL SLACPY( ' ', N, N, A, LDA, WORK, N )
            CALL SGEMM( 'N', 'N', N, N, N, ONE, U, LDU, B, LDB, ZERO,
     $                  WORK( N**2+1 ), N )
*
            CALL SGEMM( 'N', 'C', N, N, N, -ONE, WORK( N**2+1 ), N, V,
     $                  LDV, ONE, WORK, N )
*
         ELSE
*
*           ITYPE=2: Compute W = A - B
*
            CALL SLACPY( ' ', N, N, B, LDB, WORK, N )
*
            DO 20 JCOL = 1, N
               DO 10 JROW = 1, N
                  WORK( JROW+N*( JCOL-1 ) ) = WORK( JROW+N*( JCOL-1 ) )
     $                - A( JROW, JCOL )
   10          CONTINUE
   20       CONTINUE
         END IF
*
*        Compute norm(W)/ ( ulp*norm(A) )
*
         WNORM = SLANGE( '1', N, N, WORK, N, WORK( N**2+1 ) )
*
         IF( ANORM.GT.WNORM ) THEN
            RESULT = ( WNORM / ANORM ) / ( N*ULP )
         ELSE
            IF( ANORM.LT.ONE ) THEN
               RESULT = ( MIN( WNORM, N*ANORM ) / ANORM ) / ( N*ULP )
            ELSE
               RESULT = MIN( WNORM / ANORM, REAL( N ) ) / ( N*ULP )
            END IF
         END IF
*
      ELSE
*
*        Tests not scaled by norm(A)
*
*        ITYPE=3: Compute  UU' - I
*
         CALL SGEMM( 'N', 'C', N, N, N, ONE, U, LDU, U, LDU, ZERO, WORK,
     $               N )
*
         DO 30 JDIAG = 1, N
            WORK( ( N+1 )*( JDIAG-1 )+1 ) = WORK( ( N+1 )*( JDIAG-1 )+
     $         1 ) - ONE
   30    CONTINUE
*
         RESULT = MIN( SLANGE( '1', N, N, WORK, N, WORK( N**2+1 ) ),
     $            REAL( N ) ) / ( N*ULP )
      END IF
*
      RETURN
*
*     End of SGET51
*
      END
