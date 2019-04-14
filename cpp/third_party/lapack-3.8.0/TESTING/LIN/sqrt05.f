*> \brief \b SQRT05
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SQRT05(M,N,L,NB,RESULT)
*
*       .. Scalar Arguments ..
*       INTEGER LWORK, M, N, L, NB, LDT
*       .. Return values ..
*       REAL RESULT(6)
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SQRT05 tests STPQRT and STPMQRT.
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
*>          RESULT is REAL array, dimension (6)
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
*> \ingroup single_lin
*
*  =====================================================================
      SUBROUTINE SQRT05(M,N,L,NB,RESULT)
      IMPLICIT NONE
*
*  -- LAPACK test routine (version 3.8.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     April 2012
*
*     .. Scalar Arguments ..
      INTEGER LWORK, M, N, L, NB, LDT
*     .. Return values ..
      REAL RESULT(6)
*
*  =====================================================================
*
*     ..
*     .. Local allocatable arrays
      REAL, ALLOCATABLE :: AF(:,:), Q(:,:),
     $  R(:,:), RWORK(:), WORK( : ), T(:,:),
     $  CF(:,:), DF(:,:), A(:,:), C(:,:), D(:,:)
*
*     .. Parameters ..
      REAL ZERO, ONE
      PARAMETER( ZERO = 0.0, ONE = 1.0 )
*     ..
*     .. Local Scalars ..
      INTEGER INFO, J, K, M2, NP1
      REAL   ANORM, EPS, RESID, CNORM, DNORM
*     ..
*     .. Local Arrays ..
      INTEGER            ISEED( 4 )
*     ..
*     .. External Subroutine ..
      EXTERNAL SGEMM, SLARNV, STPMQRT, STPQRT, SGEMQRT, SSYRK, SLACPY,
     $         SLASET
*     ..
*     .. External Functions ..
      REAL SLAMCH
      REAL SLANGE, SLANSY
      LOGICAL  LSAME
      EXTERNAL SLAMCH, SLANGE, SLANSY, LSAME
*     ..
*     .. Data statements ..
      DATA ISEED / 1988, 1989, 1990, 1991 /
*
      EPS = SLAMCH( 'Epsilon' )
      K = N
      M2 = M+N
      IF( M.GT.0 ) THEN
         NP1 = N+1
      ELSE
         NP1 = 1
      END IF
      LWORK = M2*M2*NB
*
*     Dynamically allocate all arrays
*
      ALLOCATE(A(M2,N),AF(M2,N),Q(M2,M2),R(M2,M2),RWORK(M2),
     $           WORK(LWORK),T(NB,N),C(M2,N),CF(M2,N),
     $           D(N,M2),DF(N,M2) )
*
*     Put random stuff into A
*
      LDT=NB
      CALL SLASET( 'Full', M2, N, ZERO, ZERO, A, M2 )
      CALL SLASET( 'Full', NB, N, ZERO, ZERO, T, NB )
      DO J=1,N
         CALL SLARNV( 2, ISEED, J, A( 1, J ) )
      END DO
      IF( M.GT.0 ) THEN
         DO J=1,N
            CALL SLARNV( 2, ISEED, M-L, A( N+1, J ) )
         END DO
      END IF
      IF( L.GT.0 ) THEN
         DO J=1,N
            CALL SLARNV( 2, ISEED, MIN(J,L), A( N+M-L+1, J ) )
         END DO
      END IF
*
*     Copy the matrix A to the array AF.
*
      CALL SLACPY( 'Full', M2, N, A, M2, AF, M2 )
*
*     Factor the matrix A in the array AF.
*
      CALL STPQRT( M,N,L,NB,AF,M2,AF(NP1,1),M2,T,LDT,WORK,INFO)
*
*     Generate the (M+N)-by-(M+N) matrix Q by applying H to I
*
      CALL SLASET( 'Full', M2, M2, ZERO, ONE, Q, M2 )
      CALL SGEMQRT( 'R', 'N', M2, M2, K, NB, AF, M2, T, LDT, Q, M2,
     $              WORK, INFO )
*
*     Copy R
*
      CALL SLASET( 'Full', M2, N, ZERO, ZERO, R, M2 )
      CALL SLACPY( 'Upper', M2, N, AF, M2, R, M2 )
*
*     Compute |R - Q'*A| / |A| and store in RESULT(1)
*
      CALL SGEMM( 'T', 'N', M2, N, M2, -ONE, Q, M2, A, M2, ONE, R, M2 )
      ANORM = SLANGE( '1', M2, N, A, M2, RWORK )
      RESID = SLANGE( '1', M2, N, R, M2, RWORK )
      IF( ANORM.GT.ZERO ) THEN
         RESULT( 1 ) = RESID / (EPS*ANORM*MAX(1,M2))
      ELSE
         RESULT( 1 ) = ZERO
      END IF
*
*     Compute |I - Q'*Q| and store in RESULT(2)
*
      CALL SLASET( 'Full', M2, M2, ZERO, ONE, R, M2 )
      CALL SSYRK( 'U', 'C', M2, M2, -ONE, Q, M2, ONE,
     $            R, M2 )
      RESID = SLANSY( '1', 'Upper', M2, R, M2, RWORK )
      RESULT( 2 ) = RESID / (EPS*MAX(1,M2))
*
*     Generate random m-by-n matrix C and a copy CF
*
      DO J=1,N
         CALL SLARNV( 2, ISEED, M2, C( 1, J ) )
      END DO
      CNORM = SLANGE( '1', M2, N, C, M2, RWORK)
      CALL SLACPY( 'Full', M2, N, C, M2, CF, M2 )
*
*     Apply Q to C as Q*C
*
      CALL STPMQRT( 'L','N', M,N,K,L,NB,AF(NP1,1),M2,T,LDT,CF,
     $ M2,CF(NP1,1),M2,WORK,INFO)
*
*     Compute |Q*C - Q*C| / |C|
*
      CALL SGEMM( 'N', 'N', M2, N, M2, -ONE, Q,M2,C,M2,ONE,CF,M2)
      RESID = SLANGE( '1', M2, N, CF, M2, RWORK )
      IF( CNORM.GT.ZERO ) THEN
         RESULT( 3 ) = RESID / (EPS*MAX(1,M2)*CNORM)
      ELSE
         RESULT( 3 ) = ZERO
      END IF
*
*     Copy C into CF again
*
      CALL SLACPY( 'Full', M2, N, C, M2, CF, M2 )
*
*     Apply Q to C as QT*C
*
      CALL STPMQRT('L','T',M,N,K,L,NB,AF(NP1,1),M2,T,LDT,CF,M2,
     $              CF(NP1,1),M2,WORK,INFO)
*
*     Compute |QT*C - QT*C| / |C|
*
      CALL SGEMM('T','N',M2,N,M2,-ONE,Q,M2,C,M2,ONE,CF,M2)
      RESID = SLANGE( '1', M2, N, CF, M2, RWORK )
      IF( CNORM.GT.ZERO ) THEN
         RESULT( 4 ) = RESID / (EPS*MAX(1,M2)*CNORM)
      ELSE
         RESULT( 4 ) = ZERO
      END IF
*
*     Generate random n-by-m matrix D and a copy DF
*
      DO J=1,M2
         CALL SLARNV( 2, ISEED, N, D( 1, J ) )
      END DO
      DNORM = SLANGE( '1', N, M2, D, N, RWORK)
      CALL SLACPY( 'Full', N, M2, D, N, DF, N )
*
*     Apply Q to D as D*Q
*
      CALL STPMQRT('R','N',N,M,N,L,NB,AF(NP1,1),M2,T,LDT,DF,N,
     $             DF(1,NP1),N,WORK,INFO)
*
*     Compute |D*Q - D*Q| / |D|
*
      CALL SGEMM('N','N',N,M2,M2,-ONE,D,N,Q,M2,ONE,DF,N)
      RESID = SLANGE('1',N, M2,DF,N,RWORK )
      IF( CNORM.GT.ZERO ) THEN
         RESULT( 5 ) = RESID / (EPS*MAX(1,M2)*DNORM)
      ELSE
         RESULT( 5 ) = ZERO
      END IF
*
*     Copy D into DF again
*
      CALL SLACPY('Full',N,M2,D,N,DF,N )
*
*     Apply Q to D as D*QT
*
      CALL STPMQRT('R','T',N,M,N,L,NB,AF(NP1,1),M2,T,LDT,DF,N,
     $             DF(1,NP1),N,WORK,INFO)

*
*     Compute |D*QT - D*QT| / |D|
*
      CALL SGEMM( 'N', 'T', N, M2, M2, -ONE, D, N, Q, M2, ONE, DF, N )
      RESID = SLANGE( '1', N, M2, DF, N, RWORK )
      IF( CNORM.GT.ZERO ) THEN
         RESULT( 6 ) = RESID / (EPS*MAX(1,M2)*DNORM)
      ELSE
         RESULT( 6 ) = ZERO
      END IF
*
*     Deallocate all arrays
*
      DEALLOCATE ( A, AF, Q, R, RWORK, WORK, T, C, D, CF, DF)
      RETURN
      END

