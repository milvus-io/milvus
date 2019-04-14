*> \brief \b CQRT05
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CQRT05(M,N,L,NB,RESULT)
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
*> CQRT05 tests CTPQRT and CTPMQRT.
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
*> \ingroup complex_lin
*
*  =====================================================================
      SUBROUTINE CQRT05(M,N,L,NB,RESULT)
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
      REAL RESULT(6)
*
*  =====================================================================
*
*     ..
*     .. Local allocatable arrays
      COMPLEX, ALLOCATABLE :: AF(:,:), Q(:,:),
     $  R(:,:), WORK( : ), T(:,:),
     $  CF(:,:), DF(:,:), A(:,:), C(:,:), D(:,:)
      REAL, ALLOCATABLE :: RWORK(:)
*
*     .. Parameters ..
      REAL ZERO
      COMPLEX ONE, CZERO
      PARAMETER( ZERO = 0.0, ONE = (1.0,0.0), CZERO=(0.0,0.0) )
*     ..
*     .. Local Scalars ..
      INTEGER INFO, J, K, M2, NP1
      REAL   ANORM, EPS, RESID, CNORM, DNORM
*     ..
*     .. Local Arrays ..
      INTEGER            ISEED( 4 )
*     ..
*     .. External Functions ..
      REAL SLAMCH
      REAL CLANGE, CLANSY
      LOGICAL  LSAME
      EXTERNAL SLAMCH, CLANGE, CLANSY, LSAME
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
      CALL CLASET( 'Full', M2, N, CZERO, CZERO, A, M2 )
      CALL CLASET( 'Full', NB, N, CZERO, CZERO, T, NB )
      DO J=1,N
         CALL CLARNV( 2, ISEED, J, A( 1, J ) )
      END DO
      IF( M.GT.0 ) THEN
         DO J=1,N
            CALL CLARNV( 2, ISEED, M-L, A( MIN(N+M,N+1), J ) )
         END DO
      END IF
      IF( L.GT.0 ) THEN
         DO J=1,N
            CALL CLARNV( 2, ISEED, MIN(J,L), A( MIN(N+M,N+M-L+1), J ) )
         END DO
      END IF
*
*     Copy the matrix A to the array AF.
*
      CALL CLACPY( 'Full', M2, N, A, M2, AF, M2 )
*
*     Factor the matrix A in the array AF.
*
      CALL CTPQRT( M,N,L,NB,AF,M2,AF(NP1,1),M2,T,LDT,WORK,INFO)
*
*     Generate the (M+N)-by-(M+N) matrix Q by applying H to I
*
      CALL CLASET( 'Full', M2, M2, CZERO, ONE, Q, M2 )
      CALL CGEMQRT( 'R', 'N', M2, M2, K, NB, AF, M2, T, LDT, Q, M2,
     $              WORK, INFO )
*
*     Copy R
*
      CALL CLASET( 'Full', M2, N, CZERO, CZERO, R, M2 )
      CALL CLACPY( 'Upper', M2, N, AF, M2, R, M2 )
*
*     Compute |R - Q'*A| / |A| and store in RESULT(1)
*
      CALL CGEMM( 'C', 'N', M2, N, M2, -ONE, Q, M2, A, M2, ONE, R, M2 )
      ANORM = CLANGE( '1', M2, N, A, M2, RWORK )
      RESID = CLANGE( '1', M2, N, R, M2, RWORK )
      IF( ANORM.GT.ZERO ) THEN
         RESULT( 1 ) = RESID / (EPS*ANORM*MAX(1,M2))
      ELSE
         RESULT( 1 ) = ZERO
      END IF
*
*     Compute |I - Q'*Q| and store in RESULT(2)
*
      CALL CLASET( 'Full', M2, M2, CZERO, ONE, R, M2 )
      CALL CHERK( 'U', 'C', M2, M2, REAL(-ONE), Q, M2, REAL(ONE),
     $            R, M2 )
      RESID = CLANSY( '1', 'Upper', M2, R, M2, RWORK )
      RESULT( 2 ) = RESID / (EPS*MAX(1,M2))
*
*     Generate random m-by-n matrix C and a copy CF
*
      DO J=1,N
         CALL CLARNV( 2, ISEED, M2, C( 1, J ) )
      END DO
      CNORM = CLANGE( '1', M2, N, C, M2, RWORK)
      CALL CLACPY( 'Full', M2, N, C, M2, CF, M2 )
*
*     Apply Q to C as Q*C
*
      CALL CTPMQRT( 'L','N', M,N,K,L,NB,AF(NP1,1),M2,T,LDT,CF,M2,
     $               CF(NP1,1),M2,WORK,INFO)
*
*     Compute |Q*C - Q*C| / |C|
*
      CALL CGEMM( 'N', 'N', M2, N, M2, -ONE, Q, M2, C, M2, ONE, CF, M2 )
      RESID = CLANGE( '1', M2, N, CF, M2, RWORK )
      IF( CNORM.GT.ZERO ) THEN
         RESULT( 3 ) = RESID / (EPS*MAX(1,M2)*CNORM)
      ELSE
         RESULT( 3 ) = ZERO
      END IF
*
*     Copy C into CF again
*
      CALL CLACPY( 'Full', M2, N, C, M2, CF, M2 )
*
*     Apply Q to C as QT*C
*
      CALL CTPMQRT( 'L','C',M,N,K,L,NB,AF(NP1,1),M2,T,LDT,CF,M2,
     $              CF(NP1,1),M2,WORK,INFO)
*
*     Compute |QT*C - QT*C| / |C|
*
      CALL CGEMM('C','N',M2,N,M2,-ONE,Q,M2,C,M2,ONE,CF,M2)
      RESID = CLANGE( '1', M2, N, CF, M2, RWORK )
      IF( CNORM.GT.ZERO ) THEN
         RESULT( 4 ) = RESID / (EPS*MAX(1,M2)*CNORM)
      ELSE
         RESULT( 4 ) = ZERO
      END IF
*
*     Generate random n-by-m matrix D and a copy DF
*
      DO J=1,M2
         CALL CLARNV( 2, ISEED, N, D( 1, J ) )
      END DO
      DNORM = CLANGE( '1', N, M2, D, N, RWORK)
      CALL CLACPY( 'Full', N, M2, D, N, DF, N )
*
*     Apply Q to D as D*Q
*
      CALL CTPMQRT('R','N',N,M,N,L,NB,AF(NP1,1),M2,T,LDT,DF,N,
     $             DF(1,NP1),N,WORK,INFO)
*
*     Compute |D*Q - D*Q| / |D|
*
      CALL CGEMM('N','N',N,M2,M2,-ONE,D,N,Q,M2,ONE,DF,N)
      RESID = CLANGE('1',N, M2,DF,N,RWORK )
      IF( CNORM.GT.ZERO ) THEN
         RESULT( 5 ) = RESID / (EPS*MAX(1,M2)*DNORM)
      ELSE
         RESULT( 5 ) = ZERO
      END IF
*
*     Copy D into DF again
*
      CALL CLACPY('Full',N,M2,D,N,DF,N )
*
*     Apply Q to D as D*QT
*
      CALL CTPMQRT('R','C',N,M,N,L,NB,AF(NP1,1),M2,T,LDT,DF,N,
     $             DF(1,NP1),N,WORK,INFO)

*
*     Compute |D*QT - D*QT| / |D|
*
      CALL CGEMM( 'N', 'C', N, M2, M2, -ONE, D, N, Q, M2, ONE, DF, N )
      RESID = CLANGE( '1', N, M2, DF, N, RWORK )
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

