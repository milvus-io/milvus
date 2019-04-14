*> \brief \b SGET34
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SGET34( RMAX, LMAX, NINFO, KNT )
*
*       .. Scalar Arguments ..
*       INTEGER            KNT, LMAX
*       REAL               RMAX
*       ..
*       .. Array Arguments ..
*       INTEGER            NINFO( 2 )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SGET34 tests SLAEXC, a routine for swapping adjacent blocks (either
*> 1 by 1 or 2 by 2) on the diagonal of a matrix in real Schur form.
*> Thus, SLAEXC computes an orthogonal matrix Q such that
*>
*>     Q' * [ A B ] * Q  = [ C1 B1 ]
*>          [ 0 C ]        [ 0  A1 ]
*>
*> where C1 is similar to C and A1 is similar to A.  Both A and C are
*> assumed to be in standard form (equal diagonal entries and
*> offdiagonal with differing signs) and A1 and C1 are returned with the
*> same properties.
*>
*> The test code verifies these last last assertions, as well as that
*> the residual in the above equation is small.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[out] RMAX
*> \verbatim
*>          RMAX is REAL
*>          Value of the largest test ratio.
*> \endverbatim
*>
*> \param[out] LMAX
*> \verbatim
*>          LMAX is INTEGER
*>          Example number where largest test ratio achieved.
*> \endverbatim
*>
*> \param[out] NINFO
*> \verbatim
*>          NINFO is INTEGER array, dimension (2)
*>          NINFO(J) is the number of examples where INFO=J occurred.
*> \endverbatim
*>
*> \param[out] KNT
*> \verbatim
*>          KNT is INTEGER
*>          Total number of examples tested.
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
      SUBROUTINE SGET34( RMAX, LMAX, NINFO, KNT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            KNT, LMAX
      REAL               RMAX
*     ..
*     .. Array Arguments ..
      INTEGER            NINFO( 2 )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, HALF, ONE
      PARAMETER          ( ZERO = 0.0E0, HALF = 0.5E0, ONE = 1.0E0 )
      REAL               TWO, THREE
      PARAMETER          ( TWO = 2.0E0, THREE = 3.0E0 )
      INTEGER            LWORK
      PARAMETER          ( LWORK = 32 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, IA, IA11, IA12, IA21, IA22, IAM, IB, IC,
     $                   IC11, IC12, IC21, IC22, ICM, INFO, J
      REAL               BIGNUM, EPS, RES, SMLNUM, TNRM
*     ..
*     .. Local Arrays ..
      REAL               Q( 4, 4 ), RESULT( 2 ), T( 4, 4 ), T1( 4, 4 ),
     $                   VAL( 9 ), VM( 2 ), WORK( LWORK )
*     ..
*     .. External Functions ..
      REAL               SLAMCH
      EXTERNAL           SLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           SCOPY, SLAEXC
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, REAL, SIGN, SQRT
*     ..
*     .. Executable Statements ..
*
*     Get machine parameters
*
      EPS = SLAMCH( 'P' )
      SMLNUM = SLAMCH( 'S' ) / EPS
      BIGNUM = ONE / SMLNUM
      CALL SLABAD( SMLNUM, BIGNUM )
*
*     Set up test case parameters
*
      VAL( 1 ) = ZERO
      VAL( 2 ) = SQRT( SMLNUM )
      VAL( 3 ) = ONE
      VAL( 4 ) = TWO
      VAL( 5 ) = SQRT( BIGNUM )
      VAL( 6 ) = -SQRT( SMLNUM )
      VAL( 7 ) = -ONE
      VAL( 8 ) = -TWO
      VAL( 9 ) = -SQRT( BIGNUM )
      VM( 1 ) = ONE
      VM( 2 ) = ONE + TWO*EPS
      CALL SCOPY( 16, VAL( 4 ), 0, T( 1, 1 ), 1 )
*
      NINFO( 1 ) = 0
      NINFO( 2 ) = 0
      KNT = 0
      LMAX = 0
      RMAX = ZERO
*
*     Begin test loop
*
      DO 40 IA = 1, 9
         DO 30 IAM = 1, 2
            DO 20 IB = 1, 9
               DO 10 IC = 1, 9
                  T( 1, 1 ) = VAL( IA )*VM( IAM )
                  T( 2, 2 ) = VAL( IC )
                  T( 1, 2 ) = VAL( IB )
                  T( 2, 1 ) = ZERO
                  TNRM = MAX( ABS( T( 1, 1 ) ), ABS( T( 2, 2 ) ),
     $                   ABS( T( 1, 2 ) ) )
                  CALL SCOPY( 16, T, 1, T1, 1 )
                  CALL SCOPY( 16, VAL( 1 ), 0, Q, 1 )
                  CALL SCOPY( 4, VAL( 3 ), 0, Q, 5 )
                  CALL SLAEXC( .TRUE., 2, T, 4, Q, 4, 1, 1, 1, WORK,
     $                         INFO )
                  IF( INFO.NE.0 )
     $               NINFO( INFO ) = NINFO( INFO ) + 1
                  CALL SHST01( 2, 1, 2, T1, 4, T, 4, Q, 4, WORK, LWORK,
     $                         RESULT )
                  RES = RESULT( 1 ) + RESULT( 2 )
                  IF( INFO.NE.0 )
     $               RES = RES + ONE / EPS
                  IF( T( 1, 1 ).NE.T1( 2, 2 ) )
     $               RES = RES + ONE / EPS
                  IF( T( 2, 2 ).NE.T1( 1, 1 ) )
     $               RES = RES + ONE / EPS
                  IF( T( 2, 1 ).NE.ZERO )
     $               RES = RES + ONE / EPS
                  KNT = KNT + 1
                  IF( RES.GT.RMAX ) THEN
                     LMAX = KNT
                     RMAX = RES
                  END IF
   10          CONTINUE
   20       CONTINUE
   30    CONTINUE
   40 CONTINUE
*
      DO 110 IA = 1, 5
         DO 100 IAM = 1, 2
            DO 90 IB = 1, 5
               DO 80 IC11 = 1, 5
                  DO 70 IC12 = 2, 5
                     DO 60 IC21 = 2, 4
                        DO 50 IC22 = -1, 1, 2
                           T( 1, 1 ) = VAL( IA )*VM( IAM )
                           T( 1, 2 ) = VAL( IB )
                           T( 1, 3 ) = -TWO*VAL( IB )
                           T( 2, 1 ) = ZERO
                           T( 2, 2 ) = VAL( IC11 )
                           T( 2, 3 ) = VAL( IC12 )
                           T( 3, 1 ) = ZERO
                           T( 3, 2 ) = -VAL( IC21 )
                           T( 3, 3 ) = VAL( IC11 )*REAL( IC22 )
                           TNRM = MAX( ABS( T( 1, 1 ) ),
     $                            ABS( T( 1, 2 ) ), ABS( T( 1, 3 ) ),
     $                            ABS( T( 2, 2 ) ), ABS( T( 2, 3 ) ),
     $                            ABS( T( 3, 2 ) ), ABS( T( 3, 3 ) ) )
                           CALL SCOPY( 16, T, 1, T1, 1 )
                           CALL SCOPY( 16, VAL( 1 ), 0, Q, 1 )
                           CALL SCOPY( 4, VAL( 3 ), 0, Q, 5 )
                           CALL SLAEXC( .TRUE., 3, T, 4, Q, 4, 1, 1, 2,
     $                                  WORK, INFO )
                           IF( INFO.NE.0 )
     $                        NINFO( INFO ) = NINFO( INFO ) + 1
                           CALL SHST01( 3, 1, 3, T1, 4, T, 4, Q, 4,
     $                                  WORK, LWORK, RESULT )
                           RES = RESULT( 1 ) + RESULT( 2 )
                           IF( INFO.EQ.0 ) THEN
                              IF( T1( 1, 1 ).NE.T( 3, 3 ) )
     $                           RES = RES + ONE / EPS
                              IF( T( 3, 1 ).NE.ZERO )
     $                           RES = RES + ONE / EPS
                              IF( T( 3, 2 ).NE.ZERO )
     $                           RES = RES + ONE / EPS
                              IF( T( 2, 1 ).NE.0 .AND.
     $                            ( T( 1, 1 ).NE.T( 2,
     $                            2 ) .OR. SIGN( ONE, T( 1,
     $                            2 ) ).EQ.SIGN( ONE, T( 2, 1 ) ) ) )
     $                            RES = RES + ONE / EPS
                           END IF
                           KNT = KNT + 1
                           IF( RES.GT.RMAX ) THEN
                              LMAX = KNT
                              RMAX = RES
                           END IF
   50                   CONTINUE
   60                CONTINUE
   70             CONTINUE
   80          CONTINUE
   90       CONTINUE
  100    CONTINUE
  110 CONTINUE
*
      DO 180 IA11 = 1, 5
         DO 170 IA12 = 2, 5
            DO 160 IA21 = 2, 4
               DO 150 IA22 = -1, 1, 2
                  DO 140 ICM = 1, 2
                     DO 130 IB = 1, 5
                        DO 120 IC = 1, 5
                           T( 1, 1 ) = VAL( IA11 )
                           T( 1, 2 ) = VAL( IA12 )
                           T( 1, 3 ) = -TWO*VAL( IB )
                           T( 2, 1 ) = -VAL( IA21 )
                           T( 2, 2 ) = VAL( IA11 )*REAL( IA22 )
                           T( 2, 3 ) = VAL( IB )
                           T( 3, 1 ) = ZERO
                           T( 3, 2 ) = ZERO
                           T( 3, 3 ) = VAL( IC )*VM( ICM )
                           TNRM = MAX( ABS( T( 1, 1 ) ),
     $                            ABS( T( 1, 2 ) ), ABS( T( 1, 3 ) ),
     $                            ABS( T( 2, 2 ) ), ABS( T( 2, 3 ) ),
     $                            ABS( T( 3, 2 ) ), ABS( T( 3, 3 ) ) )
                           CALL SCOPY( 16, T, 1, T1, 1 )
                           CALL SCOPY( 16, VAL( 1 ), 0, Q, 1 )
                           CALL SCOPY( 4, VAL( 3 ), 0, Q, 5 )
                           CALL SLAEXC( .TRUE., 3, T, 4, Q, 4, 1, 2, 1,
     $                                  WORK, INFO )
                           IF( INFO.NE.0 )
     $                        NINFO( INFO ) = NINFO( INFO ) + 1
                           CALL SHST01( 3, 1, 3, T1, 4, T, 4, Q, 4,
     $                                  WORK, LWORK, RESULT )
                           RES = RESULT( 1 ) + RESULT( 2 )
                           IF( INFO.EQ.0 ) THEN
                              IF( T1( 3, 3 ).NE.T( 1, 1 ) )
     $                           RES = RES + ONE / EPS
                              IF( T( 2, 1 ).NE.ZERO )
     $                           RES = RES + ONE / EPS
                              IF( T( 3, 1 ).NE.ZERO )
     $                           RES = RES + ONE / EPS
                              IF( T( 3, 2 ).NE.0 .AND.
     $                            ( T( 2, 2 ).NE.T( 3,
     $                            3 ) .OR. SIGN( ONE, T( 2,
     $                            3 ) ).EQ.SIGN( ONE, T( 3, 2 ) ) ) )
     $                            RES = RES + ONE / EPS
                           END IF
                           KNT = KNT + 1
                           IF( RES.GT.RMAX ) THEN
                              LMAX = KNT
                              RMAX = RES
                           END IF
  120                   CONTINUE
  130                CONTINUE
  140             CONTINUE
  150          CONTINUE
  160       CONTINUE
  170    CONTINUE
  180 CONTINUE
*
      DO 300 IA11 = 1, 5
         DO 290 IA12 = 2, 5
            DO 280 IA21 = 2, 4
               DO 270 IA22 = -1, 1, 2
                  DO 260 IB = 1, 5
                     DO 250 IC11 = 3, 4
                        DO 240 IC12 = 3, 4
                           DO 230 IC21 = 3, 4
                              DO 220 IC22 = -1, 1, 2
                                 DO 210 ICM = 5, 7
                                    IAM = 1
                                    T( 1, 1 ) = VAL( IA11 )*VM( IAM )
                                    T( 1, 2 ) = VAL( IA12 )*VM( IAM )
                                    T( 1, 3 ) = -TWO*VAL( IB )
                                    T( 1, 4 ) = HALF*VAL( IB )
                                    T( 2, 1 ) = -T( 1, 2 )*VAL( IA21 )
                                    T( 2, 2 ) = VAL( IA11 )*
     $                                          REAL( IA22 )*VM( IAM )
                                    T( 2, 3 ) = VAL( IB )
                                    T( 2, 4 ) = THREE*VAL( IB )
                                    T( 3, 1 ) = ZERO
                                    T( 3, 2 ) = ZERO
                                    T( 3, 3 ) = VAL( IC11 )*
     $                                          ABS( VAL( ICM ) )
                                    T( 3, 4 ) = VAL( IC12 )*
     $                                          ABS( VAL( ICM ) )
                                    T( 4, 1 ) = ZERO
                                    T( 4, 2 ) = ZERO
                                    T( 4, 3 ) = -T( 3, 4 )*VAL( IC21 )*
     $                                          ABS( VAL( ICM ) )
                                    T( 4, 4 ) = VAL( IC11 )*
     $                                          REAL( IC22 )*
     $                                          ABS( VAL( ICM ) )
                                    TNRM = ZERO
                                    DO 200 I = 1, 4
                                       DO 190 J = 1, 4
                                          TNRM = MAX( TNRM,
     $                                           ABS( T( I, J ) ) )
  190                                  CONTINUE
  200                               CONTINUE
                                    CALL SCOPY( 16, T, 1, T1, 1 )
                                    CALL SCOPY( 16, VAL( 1 ), 0, Q, 1 )
                                    CALL SCOPY( 4, VAL( 3 ), 0, Q, 5 )
                                    CALL SLAEXC( .TRUE., 4, T, 4, Q, 4,
     $                                           1, 2, 2, WORK, INFO )
                                    IF( INFO.NE.0 )
     $                                 NINFO( INFO ) = NINFO( INFO ) + 1
                                    CALL SHST01( 4, 1, 4, T1, 4, T, 4,
     $                                           Q, 4, WORK, LWORK,
     $                                           RESULT )
                                    RES = RESULT( 1 ) + RESULT( 2 )
                                    IF( INFO.EQ.0 ) THEN
                                       IF( T( 3, 1 ).NE.ZERO )
     $                                    RES = RES + ONE / EPS
                                       IF( T( 4, 1 ).NE.ZERO )
     $                                    RES = RES + ONE / EPS
                                       IF( T( 3, 2 ).NE.ZERO )
     $                                    RES = RES + ONE / EPS
                                       IF( T( 4, 2 ).NE.ZERO )
     $                                    RES = RES + ONE / EPS
                                       IF( T( 2, 1 ).NE.0 .AND.
     $                                     ( T( 1, 1 ).NE.T( 2,
     $                                     2 ) .OR. SIGN( ONE, T( 1,
     $                                     2 ) ).EQ.SIGN( ONE, T( 2,
     $                                     1 ) ) ) )RES = RES +
     $                                     ONE / EPS
                                       IF( T( 4, 3 ).NE.0 .AND.
     $                                     ( T( 3, 3 ).NE.T( 4,
     $                                     4 ) .OR. SIGN( ONE, T( 3,
     $                                     4 ) ).EQ.SIGN( ONE, T( 4,
     $                                     3 ) ) ) )RES = RES +
     $                                     ONE / EPS
                                    END IF
                                    KNT = KNT + 1
                                    IF( RES.GT.RMAX ) THEN
                                       LMAX = KNT
                                       RMAX = RES
                                    END IF
  210                            CONTINUE
  220                         CONTINUE
  230                      CONTINUE
  240                   CONTINUE
  250                CONTINUE
  260             CONTINUE
  270          CONTINUE
  280       CONTINUE
  290    CONTINUE
  300 CONTINUE
*
      RETURN
*
*     End of SGET34
*
      END
