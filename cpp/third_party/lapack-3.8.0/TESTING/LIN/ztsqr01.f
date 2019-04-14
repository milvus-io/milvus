*> \brief \b ZTSQR01
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZTSQR01(TSSW, M,N, MB, NB, RESULT)
*
*       .. Scalar Arguments ..
*       INTEGER M, N, MB
*       .. Return values ..
*       DOUBLE PRECISION RESULT(6)
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZTSQR01 tests ZGEQR , ZGELQ, ZGEMLQ and ZGEMQR.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] TSSW
*> \verbatim
*>          TSSW is CHARACTER
*>          'TS' for testing tall skinny QR
*>               and anything else for testing short wide LQ
*> \endverbatim
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          Number of rows in test matrix.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          Number of columns in test matrix.
*> \endverbatim
*> \param[in] MB
*> \verbatim
*>          MB is INTEGER
*>          Number of row in row block in test matrix.
*> \endverbatim
*>
*> \param[in] NB
*> \verbatim
*>          NB is INTEGER
*>          Number of columns in column block test matrix.
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is DOUBLE PRECISION array, dimension (6)
*>          Results of each of the six tests below.
*>
*>          RESULT(1) = | A - Q R | or | A - L Q |
*>          RESULT(2) = | I - Q^H Q | or | I - Q Q^H |
*>          RESULT(3) = | Q C - Q C |
*>          RESULT(4) = | Q^H C - Q^H C |
*>          RESULT(5) = | C Q - C Q |
*>          RESULT(6) = | C Q^H - C Q^H |
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
*> \date April 2012
*
*  =====================================================================
      SUBROUTINE ZTSQR01(TSSW, M, N, MB, NB, RESULT)
      IMPLICIT NONE
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     April 2012
*
*     .. Scalar Arguments ..
      CHARACTER         TSSW
      INTEGER           M, N, MB, NB
*     .. Return values ..
      DOUBLE PRECISION  RESULT(6)
*
*  =====================================================================
*
*     ..
*     .. Local allocatable arrays
      COMPLEX*16, ALLOCATABLE :: AF(:,:), Q(:,:),
     $  R(:,:), RWORK(:), WORK( : ), T(:),
     $  CF(:,:), DF(:,:), A(:,:), C(:,:), D(:,:), LQ(:,:)
*
*     .. Parameters ..
      DOUBLE PRECISION ZERO
      COMPLEX*16 ONE, CZERO
      PARAMETER( ZERO = 0.0, ONE = (1.0,0.0), CZERO=(0.0,0.0) )
*     ..
*     .. Local Scalars ..
      LOGICAL TESTZEROS, TS
      INTEGER INFO, J, K, L, LWORK, TSIZE, MNB
      DOUBLE PRECISION   ANORM, EPS, RESID, CNORM, DNORM
*     ..
*     .. Local Arrays ..
      INTEGER            ISEED( 4 )
      COMPLEX*16         TQUERY( 5 ), WORKQUERY
*     ..
*     .. External Functions ..
      DOUBLE PRECISION DLAMCH, ZLANGE, ZLANSY
      LOGICAL  LSAME
      INTEGER ILAENV
      EXTERNAL DLAMCH, ZLANGE, ZLANSY, LSAME, ILAENV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC  MAX, MIN
*     .. Scalars in Common ..
      CHARACTER*32       srnamt
*     ..
*     .. Common blocks ..
      COMMON             / srnamc / srnamt
*     ..
*     .. Data statements ..
      DATA ISEED / 1988, 1989, 1990, 1991 /
*
*     TEST TALL SKINNY OR SHORT WIDE
*
      TS = LSAME(TSSW, 'TS')
*
*     TEST MATRICES WITH HALF OF MATRIX BEING ZEROS
*
      TESTZEROS = .FALSE.
*
      EPS = DLAMCH( 'Epsilon' )
      K = MIN(M,N)
      L = MAX(M,N,1)
      MNB = MAX ( MB, NB)
      LWORK = MAX(3,L)*MNB
*
*     Dynamically allocate local arrays
*
      ALLOCATE ( A(M,N), AF(M,N), Q(L,L), R(M,L), RWORK(L),
     $           C(M,N), CF(M,N),
     $           D(N,M), DF(N,M), LQ(L,N) )
*
*     Put random numbers into A and copy to AF
*
      DO J=1,N
         CALL ZLARNV( 2, ISEED, M, A( 1, J ) )
      END DO
      IF (TESTZEROS) THEN
         IF (M.GE.4) THEN
            DO J=1,N
               CALL ZLARNV( 2, ISEED, M/2, A( M/4, J ) )
            END DO
         END IF
      END IF
      CALL ZLACPY( 'Full', M, N, A, M, AF, M )
*
      IF (TS) THEN
*
*     Factor the matrix A in the array AF.
*
      CALL ZGEQR( M, N, AF, M, TQUERY, -1, WORKQUERY, -1, INFO )
      TSIZE = INT( TQUERY( 1 ) )
      LWORK = INT( WORKQUERY )
      CALL ZGEMQR( 'L', 'N', M, M, K, AF, M, TQUERY, TSIZE, CF, M,
     $             WORKQUERY, -1, INFO)
      LWORK = MAX( LWORK, INT( WORKQUERY ) )
      CALL ZGEMQR( 'L', 'N', M, N, K, AF, M, TQUERY, TSIZE, CF, M,
     $             WORKQUERY, -1, INFO)
      LWORK = MAX( LWORK, INT( WORKQUERY ) )
      CALL ZGEMQR( 'L', 'C', M, N, K, AF, M, TQUERY, TSIZE, CF, M,
     $             WORKQUERY, -1, INFO)
      LWORK = MAX( LWORK, INT( WORKQUERY ) )
      CALL ZGEMQR( 'R', 'N', N, M, K, AF, M, TQUERY, TSIZE, DF, N,
     $             WORKQUERY, -1, INFO)
      LWORK = MAX( LWORK, INT( WORKQUERY ) )
      CALL ZGEMQR( 'R', 'C', N, M, K, AF, M, TQUERY, TSIZE, DF, N,
     $             WORKQUERY, -1, INFO)
      LWORK = MAX( LWORK, INT( WORKQUERY ) )
      ALLOCATE ( T( TSIZE ) )
      ALLOCATE ( WORK( LWORK ) )
      srnamt = 'ZGEQR'
      CALL ZGEQR( M, N, AF, M, T, TSIZE, WORK, LWORK, INFO )
*
*     Generate the m-by-m matrix Q
*
      CALL ZLASET( 'Full', M, M, CZERO, ONE, Q, M )
      srnamt = 'ZGEMQR'
      CALL ZGEMQR( 'L', 'N', M, M, K, AF, M, T, TSIZE, Q, M,
     $              WORK, LWORK, INFO )
*
*     Copy R
*
      CALL ZLASET( 'Full', M, N, CZERO, CZERO, R, M )
      CALL ZLACPY( 'Upper', M, N, AF, M, R, M )
*
*     Compute |R - Q'*A| / |A| and store in RESULT(1)
*
      CALL ZGEMM( 'C', 'N', M, N, M, -ONE, Q, M, A, M, ONE, R, M )
      ANORM = ZLANGE( '1', M, N, A, M, RWORK )
      RESID = ZLANGE( '1', M, N, R, M, RWORK )
      IF( ANORM.GT.ZERO ) THEN
         RESULT( 1 ) = RESID / (EPS*MAX(1,M)*ANORM)
      ELSE
         RESULT( 1 ) = ZERO
      END IF
*
*     Compute |I - Q'*Q| and store in RESULT(2)
*
      CALL ZLASET( 'Full', M, M, CZERO, ONE, R, M )
      CALL ZHERK( 'U', 'C', M, M, DREAL(-ONE), Q, M, DREAL(ONE), R, M )
      RESID = ZLANSY( '1', 'Upper', M, R, M, RWORK )
      RESULT( 2 ) = RESID / (EPS*MAX(1,M))
*
*     Generate random m-by-n matrix C and a copy CF
*
      DO J=1,N
         CALL ZLARNV( 2, ISEED, M, C( 1, J ) )
      END DO
      CNORM = ZLANGE( '1', M, N, C, M, RWORK)
      CALL ZLACPY( 'Full', M, N, C, M, CF, M )
*
*     Apply Q to C as Q*C
*
      srnamt = 'ZGEMQR'
      CALL ZGEMQR( 'L', 'N', M, N, K, AF, M, T, TSIZE, CF, M,
     $             WORK, LWORK, INFO)
*
*     Compute |Q*C - Q*C| / |C|
*
      CALL ZGEMM( 'N', 'N', M, N, M, -ONE, Q, M, C, M, ONE, CF, M )
      RESID = ZLANGE( '1', M, N, CF, M, RWORK )
      IF( CNORM.GT.ZERO ) THEN
         RESULT( 3 ) = RESID / (EPS*MAX(1,M)*CNORM)
      ELSE
         RESULT( 3 ) = ZERO
      END IF
*
*     Copy C into CF again
*
      CALL ZLACPY( 'Full', M, N, C, M, CF, M )
*
*     Apply Q to C as QT*C
*
      srnamt = 'ZGEMQR'
      CALL ZGEMQR( 'L', 'C', M, N, K, AF, M, T, TSIZE, CF, M,
     $             WORK, LWORK, INFO)
*
*     Compute |QT*C - QT*C| / |C|
*
      CALL ZGEMM( 'C', 'N', M, N, M, -ONE, Q, M, C, M, ONE, CF, M )
      RESID = ZLANGE( '1', M, N, CF, M, RWORK )
      IF( CNORM.GT.ZERO ) THEN
         RESULT( 4 ) = RESID / (EPS*MAX(1,M)*CNORM)
      ELSE
         RESULT( 4 ) = ZERO
      END IF
*
*     Generate random n-by-m matrix D and a copy DF
*
      DO J=1,M
         CALL ZLARNV( 2, ISEED, N, D( 1, J ) )
      END DO
      DNORM = ZLANGE( '1', N, M, D, N, RWORK)
      CALL ZLACPY( 'Full', N, M, D, N, DF, N )
*
*     Apply Q to D as D*Q
*
      srnamt = 'ZGEMQR'
      CALL ZGEMQR( 'R', 'N', N, M, K, AF, M, T, TSIZE, DF, N,
     $             WORK, LWORK, INFO)
*
*     Compute |D*Q - D*Q| / |D|
*
      CALL ZGEMM( 'N', 'N', N, M, M, -ONE, D, N, Q, M, ONE, DF, N )
      RESID = ZLANGE( '1', N, M, DF, N, RWORK )
      IF( DNORM.GT.ZERO ) THEN
         RESULT( 5 ) = RESID / (EPS*MAX(1,M)*DNORM)
      ELSE
         RESULT( 5 ) = ZERO
      END IF
*
*     Copy D into DF again
*
      CALL ZLACPY( 'Full', N, M, D, N, DF, N )
*
*     Apply Q to D as D*QT
*
      CALL ZGEMQR( 'R', 'C', N, M, K, AF, M, T, TSIZE, DF, N,
     $             WORK, LWORK, INFO)
*
*     Compute |D*QT - D*QT| / |D|
*
      CALL ZGEMM( 'N', 'C', N, M, M, -ONE, D, N, Q, M, ONE, DF, N )
      RESID = ZLANGE( '1', N, M, DF, N, RWORK )
      IF( CNORM.GT.ZERO ) THEN
         RESULT( 6 ) = RESID / (EPS*MAX(1,M)*DNORM)
      ELSE
         RESULT( 6 ) = ZERO
      END IF
*
*     Short and wide
*
      ELSE
      CALL ZGELQ( M, N, AF, M, TQUERY, -1, WORKQUERY, -1, INFO )
      TSIZE = INT( TQUERY( 1 ) )
      LWORK = INT( WORKQUERY )
      CALL ZGEMLQ( 'R', 'N', N, N, K, AF, M, TQUERY, TSIZE, Q, N,
     $              WORKQUERY, -1, INFO )
      LWORK = MAX( LWORK, INT( WORKQUERY ) )
      CALL ZGEMLQ( 'L', 'N', N, M, K, AF, M, TQUERY, TSIZE, DF, N,
     $             WORKQUERY, -1, INFO)
      LWORK = MAX( LWORK, INT( WORKQUERY ) )
      CALL ZGEMLQ( 'L', 'C', N, M, K, AF, M, TQUERY, TSIZE, DF, N,
     $             WORKQUERY, -1, INFO)
      LWORK = MAX( LWORK, INT( WORKQUERY ) )
      CALL ZGEMLQ( 'R', 'N', M, N, K, AF, M, TQUERY, TSIZE, CF, M,
     $             WORKQUERY, -1, INFO)
      LWORK = MAX( LWORK, INT( WORKQUERY ) )
      CALL ZGEMLQ( 'R', 'C', M, N, K, AF, M, TQUERY, TSIZE, CF, M,
     $             WORKQUERY, -1, INFO)
      LWORK = MAX( LWORK, INT( WORKQUERY ) )
      ALLOCATE ( T( TSIZE ) )
      ALLOCATE ( WORK( LWORK ) )
      srnamt = 'ZGELQ'
      CALL ZGELQ( M, N, AF, M, T, TSIZE, WORK, LWORK, INFO )
*
*
*     Generate the n-by-n matrix Q
*
      CALL ZLASET( 'Full', N, N, CZERO, ONE, Q, N )
      srnamt = 'ZGEMLQ'
      CALL ZGEMLQ( 'R', 'N', N, N, K, AF, M, T, TSIZE, Q, N,
     $              WORK, LWORK, INFO )
*
*     Copy R
*
      CALL ZLASET( 'Full', M, N, CZERO, CZERO, LQ, L )
      CALL ZLACPY( 'Lower', M, N, AF, M, LQ, L )
*
*     Compute |L - A*Q'| / |A| and store in RESULT(1)
*
      CALL ZGEMM( 'N', 'C', M, N, N, -ONE, A, M, Q, N, ONE, LQ, L )
      ANORM = ZLANGE( '1', M, N, A, M, RWORK )
      RESID = ZLANGE( '1', M, N, LQ, L, RWORK )
      IF( ANORM.GT.ZERO ) THEN
         RESULT( 1 ) = RESID / (EPS*MAX(1,N)*ANORM)
      ELSE
         RESULT( 1 ) = ZERO
      END IF
*
*     Compute |I - Q'*Q| and store in RESULT(2)
*
      CALL ZLASET( 'Full', N, N, CZERO, ONE, LQ, L )
      CALL ZHERK( 'U', 'C', N, N, DREAL(-ONE), Q, N, DREAL(ONE), LQ, L)
      RESID = ZLANSY( '1', 'Upper', N, LQ, L, RWORK )
      RESULT( 2 ) = RESID / (EPS*MAX(1,N))
*
*     Generate random m-by-n matrix C and a copy CF
*
      DO J=1,M
         CALL ZLARNV( 2, ISEED, N, D( 1, J ) )
      END DO
      DNORM = ZLANGE( '1', N, M, D, N, RWORK)
      CALL ZLACPY( 'Full', N, M, D, N, DF, N )
*
*     Apply Q to C as Q*C
*
      CALL ZGEMLQ( 'L', 'N', N, M, K, AF, M, T, TSIZE, DF, N,
     $             WORK, LWORK, INFO)
*
*     Compute |Q*D - Q*D| / |D|
*
      CALL ZGEMM( 'N', 'N', N, M, N, -ONE, Q, N, D, N, ONE, DF, N )
      RESID = ZLANGE( '1', N, M, DF, N, RWORK )
      IF( DNORM.GT.ZERO ) THEN
         RESULT( 3 ) = RESID / (EPS*MAX(1,N)*DNORM)
      ELSE
         RESULT( 3 ) = ZERO
      END IF
*
*     Copy D into DF again
*
      CALL ZLACPY( 'Full', N, M, D, N, DF, N )
*
*     Apply Q to D as QT*D
*
      CALL ZGEMLQ( 'L', 'C', N, M, K, AF, M, T, TSIZE, DF, N,
     $             WORK, LWORK, INFO)
*
*     Compute |QT*D - QT*D| / |D|
*
      CALL ZGEMM( 'C', 'N', N, M, N, -ONE, Q, N, D, N, ONE, DF, N )
      RESID = ZLANGE( '1', N, M, DF, N, RWORK )
      IF( DNORM.GT.ZERO ) THEN
         RESULT( 4 ) = RESID / (EPS*MAX(1,N)*DNORM)
      ELSE
         RESULT( 4 ) = ZERO
      END IF
*
*     Generate random n-by-m matrix D and a copy DF
*
      DO J=1,N
         CALL ZLARNV( 2, ISEED, M, C( 1, J ) )
      END DO
      CNORM = ZLANGE( '1', M, N, C, M, RWORK)
      CALL ZLACPY( 'Full', M, N, C, M, CF, M )
*
*     Apply Q to C as C*Q
*
      CALL ZGEMLQ( 'R', 'N', M, N, K, AF, M, T, TSIZE, CF, M,
     $             WORK, LWORK, INFO)
*
*     Compute |C*Q - C*Q| / |C|
*
      CALL ZGEMM( 'N', 'N', M, N, N, -ONE, C, M, Q, N, ONE, CF, M )
      RESID = ZLANGE( '1', N, M, DF, N, RWORK )
      IF( CNORM.GT.ZERO ) THEN
         RESULT( 5 ) = RESID / (EPS*MAX(1,N)*CNORM)
      ELSE
         RESULT( 5 ) = ZERO
      END IF
*
*     Copy C into CF again
*
      CALL ZLACPY( 'Full', M, N, C, M, CF, M )
*
*     Apply Q to D as D*QT
*
      CALL ZGEMLQ( 'R', 'C', M, N, K, AF, M, T, TSIZE, CF, M,
     $             WORK, LWORK, INFO)
*
*     Compute |C*QT - C*QT| / |C|
*
      CALL ZGEMM( 'N', 'C', M, N, N, -ONE, C, M, Q, N, ONE, CF, M )
      RESID = ZLANGE( '1', M, N, CF, M, RWORK )
      IF( CNORM.GT.ZERO ) THEN
         RESULT( 6 ) = RESID / (EPS*MAX(1,N)*CNORM)
      ELSE
         RESULT( 6 ) = ZERO
      END IF
*
      END IF
*
*     Deallocate all arrays
*
      DEALLOCATE ( A, AF, Q, R, RWORK, WORK, T, C, D, CF, DF)
*
      RETURN
      END
