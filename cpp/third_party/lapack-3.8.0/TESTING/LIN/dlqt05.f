*> \brief \b DLQT05
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DLQT05(M,N,L,NB,RESULT)
*
*       .. Scalar Arguments ..
*       INTEGER LWORK, M, N, L, NB, LDT
*       .. Return values ..
*       DOUBLE PRECISION RESULT(6)
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DQRT05 tests DTPLQT and DTPMLQT.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          Number of rows in lower part of the test matrix.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          Number of columns in test matrix.
*> \endverbatim
*>
*> \param[in] L
*> \verbatim
*>          L is INTEGER
*>          The number of rows of the upper trapezoidal part the
*>          lower test matrix.  0 <= L <= M.
*> \endverbatim
*>
*> \param[in] NB
*> \verbatim
*>          NB is INTEGER
*>          Block size of test matrix.  NB <= N.
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is DOUBLE PRECISION array, dimension (6)
*>          Results of each of the six tests below.
*>
*>          RESULT(1) = | A - Q R |
*>          RESULT(2) = | I - Q^H Q |
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
*> \ingroup double_lin
*
*  =====================================================================
      SUBROUTINE DLQT05(M,N,L,NB,RESULT)
      IMPLICIT NONE
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     April 2012
*
*     .. Scalar Arguments ..
      INTEGER LWORK, M, N, L, NB, LDT
*     .. Return values ..
      DOUBLE PRECISION RESULT(6)
*
*  =====================================================================
*
*     ..
*     .. Local allocatable arrays
      DOUBLE PRECISION, ALLOCATABLE :: AF(:,:), Q(:,:),
     $  R(:,:), RWORK(:), WORK( : ), T(:,:),
     $  CF(:,:), DF(:,:), A(:,:), C(:,:), D(:,:)
*
*     .. Parameters ..
      DOUBLE PRECISION ONE, ZERO
      PARAMETER( ZERO = 0.0, ONE = 1.0 )
*     ..
*     .. Local Scalars ..
      INTEGER INFO, J, K, N2, NP1,i
      DOUBLE PRECISION   ANORM, EPS, RESID, CNORM, DNORM
*     ..
*     .. Local Arrays ..
      INTEGER            ISEED( 4 )
*     ..
*     .. External Functions ..
      DOUBLE PRECISION DLAMCH, DLANGE, DLANSY
      LOGICAL  LSAME
      EXTERNAL DLAMCH, DLANGE, DLANSY, LSAME
*     ..
*     .. Data statements ..
      DATA ISEED / 1988, 1989, 1990, 1991 /
*
      EPS = DLAMCH( 'Epsilon' )
      K = M
      N2 = M+N
      IF( N.GT.0 ) THEN
         NP1 = M+1
      ELSE
         NP1 = 1
      END IF
      LWORK = N2*N2*NB
*
*     Dynamically allocate all arrays
*
      ALLOCATE(A(M,N2),AF(M,N2),Q(N2,N2),R(N2,N2),RWORK(N2),
     $           WORK(LWORK),T(NB,M),C(N2,M),CF(N2,M),
     $           D(M,N2),DF(M,N2) )
*
*     Put random stuff into A
*
      LDT=NB
      CALL DLASET( 'Full', M, N2, ZERO, ZERO, A, M )
      CALL DLASET( 'Full', NB, M, ZERO, ZERO, T, NB )
      DO J=1,M
         CALL DLARNV( 2, ISEED, M-J+1, A( J, J ) )
      END DO
      IF( N.GT.0 ) THEN
         DO J=1,N-L
            CALL DLARNV( 2, ISEED, M, A( 1, MIN(N+M,M+1) + J - 1 ) )
         END DO
      END IF
      IF( L.GT.0 ) THEN
         DO J=1,L
            CALL DLARNV( 2, ISEED, M-J+1, A( J, MIN(N+M,N+M-L+1)
     $          + J - 1 ) )
         END DO
      END IF
*
*     Copy the matrix A to the array AF.
*
      CALL DLACPY( 'Full', M, N2, A, M, AF, M )
*
*     Factor the matrix A in the array AF.
*
      CALL DTPLQT( M,N,L,NB,AF,M,AF(1,NP1),M,T,LDT,WORK,INFO)
*
*     Generate the (M+N)-by-(M+N) matrix Q by applying H to I
*
      CALL DLASET( 'Full', N2, N2, ZERO, ONE, Q, N2 )
      CALL DGEMLQT( 'L', 'N', N2, N2, K, NB, AF, M, T, LDT, Q, N2,
     $              WORK, INFO )
*
*     Copy L
*
      CALL DLASET( 'Full', N2, N2, ZERO, ZERO, R, N2 )
      CALL DLACPY( 'Lower', M, N2, AF, M, R, N2 )
*
*     Compute |L - A*Q*T| / |A| and store in RESULT(1)
*
      CALL DGEMM( 'N', 'T', M, N2, N2, -ONE,  A, M, Q, N2, ONE, R, N2)
      ANORM = DLANGE( '1', M, N2, A, M, RWORK )
      RESID = DLANGE( '1', M, N2, R, N2, RWORK )
      IF( ANORM.GT.ZERO ) THEN
         RESULT( 1 ) = RESID / (EPS*ANORM*MAX(1,N2))
      ELSE
         RESULT( 1 ) = ZERO
      END IF
*
*     Compute |I - Q*Q'| and store in RESULT(2)
*
      CALL DLASET( 'Full', N2, N2, ZERO, ONE, R, N2 )
      CALL DSYRK( 'U', 'N', N2, N2, -ONE, Q, N2, ONE, R, N2 )
      RESID = DLANSY( '1', 'Upper', N2, R, N2, RWORK )
      RESULT( 2 ) = RESID / (EPS*MAX(1,N2))
*
*     Generate random m-by-n matrix C and a copy CF
*
      CALL DLASET( 'Full', N2, M, ZERO, ONE, C, N2 )
      DO J=1,M
         CALL DLARNV( 2, ISEED, N2, C( 1, J ) )
      END DO
      CNORM = DLANGE( '1', N2, M, C, N2, RWORK)
      CALL DLACPY( 'Full', N2, M, C, N2, CF, N2 )
*
*     Apply Q to C as Q*C
*
      CALL DTPMLQT( 'L','N', N,M,K,L,NB,AF(1, NP1),M,T,LDT,CF,N2,
     $               CF(NP1,1),N2,WORK,INFO)
*
*     Compute |Q*C - Q*C| / |C|
*
      CALL DGEMM( 'N', 'N', N2, M, N2, -ONE, Q, N2, C, N2, ONE, CF, N2 )
      RESID = DLANGE( '1', N2, M, CF, N2, RWORK )
      IF( CNORM.GT.ZERO ) THEN
         RESULT( 3 ) = RESID / (EPS*MAX(1,N2)*CNORM)
      ELSE
         RESULT( 3 ) = ZERO
      END IF

*
*     Copy C into CF again
*
      CALL DLACPY( 'Full', N2, M, C, N2, CF, N2 )
*
*     Apply Q to C as QT*C
*
      CALL DTPMLQT( 'L','T',N,M,K,L,NB,AF(1,NP1),M,T,LDT,CF,N2,
     $              CF(NP1,1),N2,WORK,INFO)
*
*     Compute |QT*C - QT*C| / |C|
*
      CALL DGEMM('T','N',N2,M,N2,-ONE,Q,N2,C,N2,ONE,CF,N2)
      RESID = DLANGE( '1', N2, M, CF, N2, RWORK )

      IF( CNORM.GT.ZERO ) THEN
         RESULT( 4 ) = RESID / (EPS*MAX(1,N2)*CNORM)
      ELSE
         RESULT( 4 ) = ZERO
      END IF
*
*     Generate random m-by-n matrix D and a copy DF
*
      DO J=1,N2
         CALL DLARNV( 2, ISEED, M, D( 1, J ) )
      END DO
      DNORM = DLANGE( '1', M, N2, D, M, RWORK)
      CALL DLACPY( 'Full', M, N2, D, M, DF, M )
*
*     Apply Q to D as D*Q
*
      CALL DTPMLQT('R','N',M,N,K,L,NB,AF(1,NP1),M,T,LDT,DF,M,
     $             DF(1,NP1),M,WORK,INFO)
*
*     Compute |D*Q - D*Q| / |D|
*
      CALL DGEMM('N','N',M,N2,N2,-ONE,D,M,Q,N2,ONE,DF,M)
      RESID = DLANGE('1',M, N2,DF,M,RWORK )
      IF( CNORM.GT.ZERO ) THEN
         RESULT( 5 ) = RESID / (EPS*MAX(1,N2)*DNORM)
      ELSE
         RESULT( 5 ) = ZERO
      END IF
*
*     Copy D into DF again
*
      CALL DLACPY('Full',M,N2,D,M,DF,M )
*
*     Apply Q to D as D*QT
*
      CALL DTPMLQT('R','T',M,N,K,L,NB,AF(1,NP1),M,T,LDT,DF,M,
     $             DF(1,NP1),M,WORK,INFO)

*
*     Compute |D*QT - D*QT| / |D|
*
      CALL DGEMM( 'N', 'T', M, N2, N2, -ONE, D, M, Q, N2, ONE, DF, M )
      RESID = DLANGE( '1', M, N2, DF, M, RWORK )
      IF( CNORM.GT.ZERO ) THEN
         RESULT( 6 ) = RESID / (EPS*MAX(1,N2)*DNORM)
      ELSE
         RESULT( 6 ) = ZERO
      END IF
*
*     Deallocate all arrays
*
      DEALLOCATE ( A, AF, Q, R, RWORK, WORK, T, C, D, CF, DF)
      RETURN
      END
