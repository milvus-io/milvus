*> \brief \b CCHKEQ
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CCHKEQ( THRESH, NOUT )
*
*       .. Scalar Arguments ..
*       INTEGER            NOUT
*       REAL               THRESH
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CCHKEQ tests CGEEQU, CGBEQU, CPOEQU, CPPEQU and CPBEQU
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] THRESH
*> \verbatim
*>          THRESH is REAL
*>          Threshold for testing routines. Should be between 2 and 10.
*> \endverbatim
*>
*> \param[in] NOUT
*> \verbatim
*>          NOUT is INTEGER
*>          The unit number for output.
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
*> \ingroup complex_lin
*
*  =====================================================================
      SUBROUTINE CCHKEQ( THRESH, NOUT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            NOUT
      REAL               THRESH
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE, TEN
      PARAMETER          ( ZERO = 0.0E0, ONE = 1.0E+0, TEN = 1.0E1 )
      COMPLEX            CZERO
      PARAMETER          ( CZERO = ( 0.0E0, 0.0E0 ) )
      COMPLEX            CONE
      PARAMETER          ( CONE = ( 1.0E0, 0.0E0 ) )
      INTEGER            NSZ, NSZB
      PARAMETER          ( NSZ = 5, NSZB = 3*NSZ-2 )
      INTEGER            NSZP, NPOW
      PARAMETER          ( NSZP = ( NSZ*( NSZ+1 ) ) / 2,
     $                   NPOW = 2*NSZ+1 )
*     ..
*     .. Local Scalars ..
      LOGICAL            OK
      CHARACTER*3        PATH
      INTEGER            I, INFO, J, KL, KU, M, N
      REAL               CCOND, EPS, NORM, RATIO, RCMAX, RCMIN, RCOND
*     ..
*     .. Local Arrays ..
      REAL               C( NSZ ), POW( NPOW ), R( NSZ ), RESLTS( 5 ),
     $                   RPOW( NPOW )
      COMPLEX            A( NSZ, NSZ ), AB( NSZB, NSZ ), AP( NSZP )
*     ..
*     .. External Functions ..
      REAL               SLAMCH
      EXTERNAL           SLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           CGBEQU, CGEEQU, CPBEQU, CPOEQU, CPPEQU
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, MIN
*     ..
*     .. Executable Statements ..
*
      PATH( 1:1 ) = 'Complex precision'
      PATH( 2:3 ) = 'EQ'
*
      EPS = SLAMCH( 'P' )
      DO 10 I = 1, 5
         RESLTS( I ) = ZERO
   10 CONTINUE
      DO 20 I = 1, NPOW
         POW( I ) = TEN**( I-1 )
         RPOW( I ) = ONE / POW( I )
   20 CONTINUE
*
*     Test CGEEQU
*
      DO 80 N = 0, NSZ
         DO 70 M = 0, NSZ
*
            DO 40 J = 1, NSZ
               DO 30 I = 1, NSZ
                  IF( I.LE.M .AND. J.LE.N ) THEN
                     A( I, J ) = POW( I+J+1 )*( -1 )**( I+J )
                  ELSE
                     A( I, J ) = CZERO
                  END IF
   30          CONTINUE
   40       CONTINUE
*
            CALL CGEEQU( M, N, A, NSZ, R, C, RCOND, CCOND, NORM, INFO )
*
            IF( INFO.NE.0 ) THEN
               RESLTS( 1 ) = ONE
            ELSE
               IF( N.NE.0 .AND. M.NE.0 ) THEN
                  RESLTS( 1 ) = MAX( RESLTS( 1 ),
     $                          ABS( ( RCOND-RPOW( M ) ) / RPOW( M ) ) )
                  RESLTS( 1 ) = MAX( RESLTS( 1 ),
     $                          ABS( ( CCOND-RPOW( N ) ) / RPOW( N ) ) )
                  RESLTS( 1 ) = MAX( RESLTS( 1 ),
     $                          ABS( ( NORM-POW( N+M+1 ) ) / POW( N+M+
     $                          1 ) ) )
                  DO 50 I = 1, M
                     RESLTS( 1 ) = MAX( RESLTS( 1 ),
     $                             ABS( ( R( I )-RPOW( I+N+1 ) ) /
     $                             RPOW( I+N+1 ) ) )
   50             CONTINUE
                  DO 60 J = 1, N
                     RESLTS( 1 ) = MAX( RESLTS( 1 ),
     $                             ABS( ( C( J )-POW( N-J+1 ) ) /
     $                             POW( N-J+1 ) ) )
   60             CONTINUE
               END IF
            END IF
*
   70    CONTINUE
   80 CONTINUE
*
*     Test with zero rows and columns
*
      DO 90 J = 1, NSZ
         A( MAX( NSZ-1, 1 ), J ) = CZERO
   90 CONTINUE
      CALL CGEEQU( NSZ, NSZ, A, NSZ, R, C, RCOND, CCOND, NORM, INFO )
      IF( INFO.NE.MAX( NSZ-1, 1 ) )
     $   RESLTS( 1 ) = ONE
*
      DO 100 J = 1, NSZ
         A( MAX( NSZ-1, 1 ), J ) = CONE
  100 CONTINUE
      DO 110 I = 1, NSZ
         A( I, MAX( NSZ-1, 1 ) ) = CZERO
  110 CONTINUE
      CALL CGEEQU( NSZ, NSZ, A, NSZ, R, C, RCOND, CCOND, NORM, INFO )
      IF( INFO.NE.NSZ+MAX( NSZ-1, 1 ) )
     $   RESLTS( 1 ) = ONE
      RESLTS( 1 ) = RESLTS( 1 ) / EPS
*
*     Test CGBEQU
*
      DO 250 N = 0, NSZ
         DO 240 M = 0, NSZ
            DO 230 KL = 0, MAX( M-1, 0 )
               DO 220 KU = 0, MAX( N-1, 0 )
*
                  DO 130 J = 1, NSZ
                     DO 120 I = 1, NSZB
                        AB( I, J ) = CZERO
  120                CONTINUE
  130             CONTINUE
                  DO 150 J = 1, N
                     DO 140 I = 1, M
                        IF( I.LE.MIN( M, J+KL ) .AND. I.GE.
     $                      MAX( 1, J-KU ) .AND. J.LE.N ) THEN
                           AB( KU+1+I-J, J ) = POW( I+J+1 )*
     $                                         ( -1 )**( I+J )
                        END IF
  140                CONTINUE
  150             CONTINUE
*
                  CALL CGBEQU( M, N, KL, KU, AB, NSZB, R, C, RCOND,
     $                         CCOND, NORM, INFO )
*
                  IF( INFO.NE.0 ) THEN
                     IF( .NOT.( ( N+KL.LT.M .AND. INFO.EQ.N+KL+1 ) .OR.
     $                   ( M+KU.LT.N .AND. INFO.EQ.2*M+KU+1 ) ) ) THEN
                        RESLTS( 2 ) = ONE
                     END IF
                  ELSE
                     IF( N.NE.0 .AND. M.NE.0 ) THEN
*
                        RCMIN = R( 1 )
                        RCMAX = R( 1 )
                        DO 160 I = 1, M
                           RCMIN = MIN( RCMIN, R( I ) )
                           RCMAX = MAX( RCMAX, R( I ) )
  160                   CONTINUE
                        RATIO = RCMIN / RCMAX
                        RESLTS( 2 ) = MAX( RESLTS( 2 ),
     $                                ABS( ( RCOND-RATIO ) / RATIO ) )
*
                        RCMIN = C( 1 )
                        RCMAX = C( 1 )
                        DO 170 J = 1, N
                           RCMIN = MIN( RCMIN, C( J ) )
                           RCMAX = MAX( RCMAX, C( J ) )
  170                   CONTINUE
                        RATIO = RCMIN / RCMAX
                        RESLTS( 2 ) = MAX( RESLTS( 2 ),
     $                                ABS( ( CCOND-RATIO ) / RATIO ) )
*
                        RESLTS( 2 ) = MAX( RESLTS( 2 ),
     $                                ABS( ( NORM-POW( N+M+1 ) ) /
     $                                POW( N+M+1 ) ) )
                        DO 190 I = 1, M
                           RCMAX = ZERO
                           DO 180 J = 1, N
                              IF( I.LE.J+KL .AND. I.GE.J-KU ) THEN
                                 RATIO = ABS( R( I )*POW( I+J+1 )*
     $                                   C( J ) )
                                 RCMAX = MAX( RCMAX, RATIO )
                              END IF
  180                      CONTINUE
                           RESLTS( 2 ) = MAX( RESLTS( 2 ),
     $                                   ABS( ONE-RCMAX ) )
  190                   CONTINUE
*
                        DO 210 J = 1, N
                           RCMAX = ZERO
                           DO 200 I = 1, M
                              IF( I.LE.J+KL .AND. I.GE.J-KU ) THEN
                                 RATIO = ABS( R( I )*POW( I+J+1 )*
     $                                   C( J ) )
                                 RCMAX = MAX( RCMAX, RATIO )
                              END IF
  200                      CONTINUE
                           RESLTS( 2 ) = MAX( RESLTS( 2 ),
     $                                   ABS( ONE-RCMAX ) )
  210                   CONTINUE
                     END IF
                  END IF
*
  220          CONTINUE
  230       CONTINUE
  240    CONTINUE
  250 CONTINUE
      RESLTS( 2 ) = RESLTS( 2 ) / EPS
*
*     Test CPOEQU
*
      DO 290 N = 0, NSZ
*
         DO 270 I = 1, NSZ
            DO 260 J = 1, NSZ
               IF( I.LE.N .AND. J.EQ.I ) THEN
                  A( I, J ) = POW( I+J+1 )*( -1 )**( I+J )
               ELSE
                  A( I, J ) = CZERO
               END IF
  260       CONTINUE
  270    CONTINUE
*
         CALL CPOEQU( N, A, NSZ, R, RCOND, NORM, INFO )
*
         IF( INFO.NE.0 ) THEN
            RESLTS( 3 ) = ONE
         ELSE
            IF( N.NE.0 ) THEN
               RESLTS( 3 ) = MAX( RESLTS( 3 ),
     $                       ABS( ( RCOND-RPOW( N ) ) / RPOW( N ) ) )
               RESLTS( 3 ) = MAX( RESLTS( 3 ),
     $                       ABS( ( NORM-POW( 2*N+1 ) ) / POW( 2*N+
     $                       1 ) ) )
               DO 280 I = 1, N
                  RESLTS( 3 ) = MAX( RESLTS( 3 ),
     $                          ABS( ( R( I )-RPOW( I+1 ) ) / RPOW( I+
     $                          1 ) ) )
  280          CONTINUE
            END IF
         END IF
  290 CONTINUE
      A( MAX( NSZ-1, 1 ), MAX( NSZ-1, 1 ) ) = -CONE
      CALL CPOEQU( NSZ, A, NSZ, R, RCOND, NORM, INFO )
      IF( INFO.NE.MAX( NSZ-1, 1 ) )
     $   RESLTS( 3 ) = ONE
      RESLTS( 3 ) = RESLTS( 3 ) / EPS
*
*     Test CPPEQU
*
      DO 360 N = 0, NSZ
*
*        Upper triangular packed storage
*
         DO 300 I = 1, ( N*( N+1 ) ) / 2
            AP( I ) = CZERO
  300    CONTINUE
         DO 310 I = 1, N
            AP( ( I*( I+1 ) ) / 2 ) = POW( 2*I+1 )
  310    CONTINUE
*
         CALL CPPEQU( 'U', N, AP, R, RCOND, NORM, INFO )
*
         IF( INFO.NE.0 ) THEN
            RESLTS( 4 ) = ONE
         ELSE
            IF( N.NE.0 ) THEN
               RESLTS( 4 ) = MAX( RESLTS( 4 ),
     $                       ABS( ( RCOND-RPOW( N ) ) / RPOW( N ) ) )
               RESLTS( 4 ) = MAX( RESLTS( 4 ),
     $                       ABS( ( NORM-POW( 2*N+1 ) ) / POW( 2*N+
     $                       1 ) ) )
               DO 320 I = 1, N
                  RESLTS( 4 ) = MAX( RESLTS( 4 ),
     $                          ABS( ( R( I )-RPOW( I+1 ) ) / RPOW( I+
     $                          1 ) ) )
  320          CONTINUE
            END IF
         END IF
*
*        Lower triangular packed storage
*
         DO 330 I = 1, ( N*( N+1 ) ) / 2
            AP( I ) = CZERO
  330    CONTINUE
         J = 1
         DO 340 I = 1, N
            AP( J ) = POW( 2*I+1 )
            J = J + ( N-I+1 )
  340    CONTINUE
*
         CALL CPPEQU( 'L', N, AP, R, RCOND, NORM, INFO )
*
         IF( INFO.NE.0 ) THEN
            RESLTS( 4 ) = ONE
         ELSE
            IF( N.NE.0 ) THEN
               RESLTS( 4 ) = MAX( RESLTS( 4 ),
     $                       ABS( ( RCOND-RPOW( N ) ) / RPOW( N ) ) )
               RESLTS( 4 ) = MAX( RESLTS( 4 ),
     $                       ABS( ( NORM-POW( 2*N+1 ) ) / POW( 2*N+
     $                       1 ) ) )
               DO 350 I = 1, N
                  RESLTS( 4 ) = MAX( RESLTS( 4 ),
     $                          ABS( ( R( I )-RPOW( I+1 ) ) / RPOW( I+
     $                          1 ) ) )
  350          CONTINUE
            END IF
         END IF
*
  360 CONTINUE
      I = ( NSZ*( NSZ+1 ) ) / 2 - 2
      AP( I ) = -CONE
      CALL CPPEQU( 'L', NSZ, AP, R, RCOND, NORM, INFO )
      IF( INFO.NE.MAX( NSZ-1, 1 ) )
     $   RESLTS( 4 ) = ONE
      RESLTS( 4 ) = RESLTS( 4 ) / EPS
*
*     Test CPBEQU
*
      DO 460 N = 0, NSZ
         DO 450 KL = 0, MAX( N-1, 0 )
*
*           Test upper triangular storage
*
            DO 380 J = 1, NSZ
               DO 370 I = 1, NSZB
                  AB( I, J ) = CZERO
  370          CONTINUE
  380       CONTINUE
            DO 390 J = 1, N
               AB( KL+1, J ) = POW( 2*J+1 )
  390       CONTINUE
*
            CALL CPBEQU( 'U', N, KL, AB, NSZB, R, RCOND, NORM, INFO )
*
            IF( INFO.NE.0 ) THEN
               RESLTS( 5 ) = ONE
            ELSE
               IF( N.NE.0 ) THEN
                  RESLTS( 5 ) = MAX( RESLTS( 5 ),
     $                          ABS( ( RCOND-RPOW( N ) ) / RPOW( N ) ) )
                  RESLTS( 5 ) = MAX( RESLTS( 5 ),
     $                          ABS( ( NORM-POW( 2*N+1 ) ) / POW( 2*N+
     $                          1 ) ) )
                  DO 400 I = 1, N
                     RESLTS( 5 ) = MAX( RESLTS( 5 ),
     $                             ABS( ( R( I )-RPOW( I+1 ) ) /
     $                             RPOW( I+1 ) ) )
  400             CONTINUE
               END IF
            END IF
            IF( N.NE.0 ) THEN
               AB( KL+1, MAX( N-1, 1 ) ) = -CONE
               CALL CPBEQU( 'U', N, KL, AB, NSZB, R, RCOND, NORM, INFO )
               IF( INFO.NE.MAX( N-1, 1 ) )
     $            RESLTS( 5 ) = ONE
            END IF
*
*           Test lower triangular storage
*
            DO 420 J = 1, NSZ
               DO 410 I = 1, NSZB
                  AB( I, J ) = CZERO
  410          CONTINUE
  420       CONTINUE
            DO 430 J = 1, N
               AB( 1, J ) = POW( 2*J+1 )
  430       CONTINUE
*
            CALL CPBEQU( 'L', N, KL, AB, NSZB, R, RCOND, NORM, INFO )
*
            IF( INFO.NE.0 ) THEN
               RESLTS( 5 ) = ONE
            ELSE
               IF( N.NE.0 ) THEN
                  RESLTS( 5 ) = MAX( RESLTS( 5 ),
     $                          ABS( ( RCOND-RPOW( N ) ) / RPOW( N ) ) )
                  RESLTS( 5 ) = MAX( RESLTS( 5 ),
     $                          ABS( ( NORM-POW( 2*N+1 ) ) / POW( 2*N+
     $                          1 ) ) )
                  DO 440 I = 1, N
                     RESLTS( 5 ) = MAX( RESLTS( 5 ),
     $                             ABS( ( R( I )-RPOW( I+1 ) ) /
     $                             RPOW( I+1 ) ) )
  440             CONTINUE
               END IF
            END IF
            IF( N.NE.0 ) THEN
               AB( 1, MAX( N-1, 1 ) ) = -CONE
               CALL CPBEQU( 'L', N, KL, AB, NSZB, R, RCOND, NORM, INFO )
               IF( INFO.NE.MAX( N-1, 1 ) )
     $            RESLTS( 5 ) = ONE
            END IF
  450    CONTINUE
  460 CONTINUE
      RESLTS( 5 ) = RESLTS( 5 ) / EPS
      OK = ( RESLTS( 1 ).LE.THRESH ) .AND.
     $     ( RESLTS( 2 ).LE.THRESH ) .AND.
     $     ( RESLTS( 3 ).LE.THRESH ) .AND.
     $     ( RESLTS( 4 ).LE.THRESH ) .AND. ( RESLTS( 5 ).LE.THRESH )
      WRITE( NOUT, FMT = * )
      IF( OK ) THEN
         WRITE( NOUT, FMT = 9999 )PATH
      ELSE
         IF( RESLTS( 1 ).GT.THRESH )
     $      WRITE( NOUT, FMT = 9998 )RESLTS( 1 ), THRESH
         IF( RESLTS( 2 ).GT.THRESH )
     $      WRITE( NOUT, FMT = 9997 )RESLTS( 2 ), THRESH
         IF( RESLTS( 3 ).GT.THRESH )
     $      WRITE( NOUT, FMT = 9996 )RESLTS( 3 ), THRESH
         IF( RESLTS( 4 ).GT.THRESH )
     $      WRITE( NOUT, FMT = 9995 )RESLTS( 4 ), THRESH
         IF( RESLTS( 5 ).GT.THRESH )
     $      WRITE( NOUT, FMT = 9994 )RESLTS( 5 ), THRESH
      END IF
 9999 FORMAT( 1X, 'All tests for ', A3,
     $      ' routines passed the threshold' )
 9998 FORMAT( ' CGEEQU failed test with value ', E10.3, ' exceeding',
     $      ' threshold ', E10.3 )
 9997 FORMAT( ' CGBEQU failed test with value ', E10.3, ' exceeding',
     $      ' threshold ', E10.3 )
 9996 FORMAT( ' CPOEQU failed test with value ', E10.3, ' exceeding',
     $      ' threshold ', E10.3 )
 9995 FORMAT( ' CPPEQU failed test with value ', E10.3, ' exceeding',
     $      ' threshold ', E10.3 )
 9994 FORMAT( ' CPBEQU failed test with value ', E10.3, ' exceeding',
     $      ' threshold ', E10.3 )
      RETURN
*
*     End of CCHKEQ
*
      END
