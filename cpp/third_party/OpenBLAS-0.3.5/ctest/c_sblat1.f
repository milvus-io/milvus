      PROGRAM SCBLAT1
*     Test program for the REAL             Level 1 CBLAS.
*     Based upon the original CBLAS test routine together with:
*     F06EAF Example Program Text
*     .. Parameters ..
      INTEGER          NOUT
      PARAMETER        (NOUT=6)
*     .. Scalars in Common ..
      INTEGER          ICASE, INCX, INCY, MODE, N
      LOGICAL          PASS
*     .. Local Scalars ..
      REAL             SFAC
      INTEGER          IC
*     .. External Subroutines ..
      EXTERNAL         CHECK0, CHECK1, CHECK2, CHECK3, HEADER
*     .. Common blocks ..
      COMMON           /COMBLA/ICASE, N, INCX, INCY, MODE, PASS
*     .. Data statements ..
      DATA             SFAC/9.765625E-4/
*     .. Executable Statements ..
      WRITE (NOUT,99999)
      DO 20 IC = 1, 10
         ICASE = IC
         CALL HEADER
*
*        .. Initialize  PASS,  INCX,  INCY, and MODE for a new case. ..
*        .. the value 9999 for INCX, INCY or MODE will appear in the ..
*        .. detailed  output, if any, for cases  that do not involve ..
*        .. these parameters ..
*
         PASS = .TRUE.
         INCX = 9999
         INCY = 9999
         MODE = 9999
         IF (ICASE.EQ.3) THEN
            CALL CHECK0(SFAC)
         ELSE IF (ICASE.EQ.7 .OR. ICASE.EQ.8 .OR. ICASE.EQ.9 .OR.
     +            ICASE.EQ.10) THEN
            CALL CHECK1(SFAC)
         ELSE IF (ICASE.EQ.1 .OR. ICASE.EQ.2 .OR. ICASE.EQ.5 .OR.
     +            ICASE.EQ.6) THEN
            CALL CHECK2(SFAC)
         ELSE IF (ICASE.EQ.4) THEN
            CALL CHECK3(SFAC)
         END IF
*        -- Print
         IF (PASS) WRITE (NOUT,99998)
   20 CONTINUE
      STOP
*
99999 FORMAT (' Real CBLAS Test Program Results',/1X)
99998 FORMAT ('                                    ----- PASS -----')
      END
      SUBROUTINE HEADER
*     .. Parameters ..
      INTEGER          NOUT
      PARAMETER        (NOUT=6)
*     .. Scalars in Common ..
      INTEGER          ICASE, INCX, INCY, MODE, N
      LOGICAL          PASS
*     .. Local Arrays ..
      CHARACTER*15      L(10)
*     .. Common blocks ..
      COMMON           /COMBLA/ICASE, N, INCX, INCY, MODE, PASS
*     .. Data statements ..
      DATA             L(1)/'CBLAS_SDOT '/
      DATA             L(2)/'CBLAS_SAXPY '/
      DATA             L(3)/'CBLAS_SROTG '/
      DATA             L(4)/'CBLAS_SROT '/
      DATA             L(5)/'CBLAS_SCOPY '/
      DATA             L(6)/'CBLAS_SSWAP '/
      DATA             L(7)/'CBLAS_SNRM2 '/
      DATA             L(8)/'CBLAS_SASUM '/
      DATA             L(9)/'CBLAS_SSCAL '/
      DATA             L(10)/'CBLAS_ISAMAX'/
*     .. Executable Statements ..
      WRITE (NOUT,99999) ICASE, L(ICASE)
      RETURN
*
99999 FORMAT (/' Test of subprogram number',I3,9X,A15)
      END
      SUBROUTINE CHECK0(SFAC)
*     .. Parameters ..
      INTEGER           NOUT
      PARAMETER         (NOUT=6)
*     .. Scalar Arguments ..
      REAL              SFAC
*     .. Scalars in Common ..
      INTEGER           ICASE, INCX, INCY, MODE, N
      LOGICAL           PASS
*     .. Local Scalars ..
      REAL              SA, SB, SC, SS
      INTEGER           K
*     .. Local Arrays ..
      REAL              DA1(8), DATRUE(8), DB1(8), DBTRUE(8), DC1(8),
     +                  DS1(8)
*     .. External Subroutines ..
      EXTERNAL          SROTGTEST, STEST1
*     .. Common blocks ..
      COMMON            /COMBLA/ICASE, N, INCX, INCY, MODE, PASS
*     .. Data statements ..
      DATA              DA1/0.3E0, 0.4E0, -0.3E0, -0.4E0, -0.3E0, 0.0E0,
     +                  0.0E0, 1.0E0/
      DATA              DB1/0.4E0, 0.3E0, 0.4E0, 0.3E0, -0.4E0, 0.0E0,
     +                  1.0E0, 0.0E0/
      DATA              DC1/0.6E0, 0.8E0, -0.6E0, 0.8E0, 0.6E0, 1.0E0,
     +                  0.0E0, 1.0E0/
      DATA              DS1/0.8E0, 0.6E0, 0.8E0, -0.6E0, 0.8E0, 0.0E0,
     +                  1.0E0, 0.0E0/
      DATA              DATRUE/0.5E0, 0.5E0, 0.5E0, -0.5E0, -0.5E0,
     +                  0.0E0, 1.0E0, 1.0E0/
      DATA              DBTRUE/0.0E0, 0.6E0, 0.0E0, -0.6E0, 0.0E0,
     +                  0.0E0, 1.0E0, 0.0E0/
*     .. Executable Statements ..
*
*     Compute true values which cannot be prestored
*     in decimal notation
*
      DBTRUE(1) = 1.0E0/0.6E0
      DBTRUE(3) = -1.0E0/0.6E0
      DBTRUE(5) = 1.0E0/0.6E0
*
      DO 20 K = 1, 8
*        .. Set N=K for identification in output if any ..
         N = K
         IF (ICASE.EQ.3) THEN
*           .. SROTGTEST ..
            IF (K.GT.8) GO TO 40
            SA = DA1(K)
            SB = DB1(K)
            CALL SROTGTEST(SA,SB,SC,SS)
            CALL STEST1(SA,DATRUE(K),DATRUE(K),SFAC)
            CALL STEST1(SB,DBTRUE(K),DBTRUE(K),SFAC)
            CALL STEST1(SC,DC1(K),DC1(K),SFAC)
            CALL STEST1(SS,DS1(K),DS1(K),SFAC)
         ELSE
            WRITE (NOUT,*) ' Shouldn''t be here in CHECK0'
            STOP
         END IF
   20 CONTINUE
   40 RETURN
      END
      SUBROUTINE CHECK1(SFAC)
*     .. Parameters ..
      INTEGER           NOUT
      PARAMETER         (NOUT=6)
*     .. Scalar Arguments ..
      REAL              SFAC
*     .. Scalars in Common ..
      INTEGER           ICASE, INCX, INCY, MODE, N
      LOGICAL           PASS
*     .. Local Scalars ..
      INTEGER           I, LEN, NP1
*     .. Local Arrays ..
      REAL              DTRUE1(5), DTRUE3(5), DTRUE5(8,5,2), DV(8,5,2),
     +                  SA(10), STEMP(1), STRUE(8), SX(8)
      INTEGER           ITRUE2(5)
*     .. External Functions ..
      REAL              SASUMTEST, SNRM2TEST
      INTEGER           ISAMAXTEST
      EXTERNAL          SASUMTEST, SNRM2TEST, ISAMAXTEST
*     .. External Subroutines ..
      EXTERNAL          ITEST1, SSCALTEST, STEST, STEST1
*     .. Intrinsic Functions ..
      INTRINSIC         MAX
*     .. Common blocks ..
      COMMON            /COMBLA/ICASE, N, INCX, INCY, MODE, PASS
*     .. Data statements ..
      DATA              SA/0.3E0, -1.0E0, 0.0E0, 1.0E0, 0.3E0, 0.3E0,
     +                  0.3E0, 0.3E0, 0.3E0, 0.3E0/
      DATA              DV/0.1E0, 2.0E0, 2.0E0, 2.0E0, 2.0E0, 2.0E0,
     +                  2.0E0, 2.0E0, 0.3E0, 3.0E0, 3.0E0, 3.0E0, 3.0E0,
     +                  3.0E0, 3.0E0, 3.0E0, 0.3E0, -0.4E0, 4.0E0,
     +                  4.0E0, 4.0E0, 4.0E0, 4.0E0, 4.0E0, 0.2E0,
     +                  -0.6E0, 0.3E0, 5.0E0, 5.0E0, 5.0E0, 5.0E0,
     +                  5.0E0, 0.1E0, -0.3E0, 0.5E0, -0.1E0, 6.0E0,
     +                  6.0E0, 6.0E0, 6.0E0, 0.1E0, 8.0E0, 8.0E0, 8.0E0,
     +                  8.0E0, 8.0E0, 8.0E0, 8.0E0, 0.3E0, 9.0E0, 9.0E0,
     +                  9.0E0, 9.0E0, 9.0E0, 9.0E0, 9.0E0, 0.3E0, 2.0E0,
     +                  -0.4E0, 2.0E0, 2.0E0, 2.0E0, 2.0E0, 2.0E0,
     +                  0.2E0, 3.0E0, -0.6E0, 5.0E0, 0.3E0, 2.0E0,
     +                  2.0E0, 2.0E0, 0.1E0, 4.0E0, -0.3E0, 6.0E0,
     +                  -0.5E0, 7.0E0, -0.1E0, 3.0E0/
      DATA              DTRUE1/0.0E0, 0.3E0, 0.5E0, 0.7E0, 0.6E0/
      DATA              DTRUE3/0.0E0, 0.3E0, 0.7E0, 1.1E0, 1.0E0/
      DATA              DTRUE5/0.10E0, 2.0E0, 2.0E0, 2.0E0, 2.0E0,
     +                  2.0E0, 2.0E0, 2.0E0, -0.3E0, 3.0E0, 3.0E0,
     +                  3.0E0, 3.0E0, 3.0E0, 3.0E0, 3.0E0, 0.0E0, 0.0E0,
     +                  4.0E0, 4.0E0, 4.0E0, 4.0E0, 4.0E0, 4.0E0,
     +                  0.20E0, -0.60E0, 0.30E0, 5.0E0, 5.0E0, 5.0E0,
     +                  5.0E0, 5.0E0, 0.03E0, -0.09E0, 0.15E0, -0.03E0,
     +                  6.0E0, 6.0E0, 6.0E0, 6.0E0, 0.10E0, 8.0E0,
     +                  8.0E0, 8.0E0, 8.0E0, 8.0E0, 8.0E0, 8.0E0,
     +                  0.09E0, 9.0E0, 9.0E0, 9.0E0, 9.0E0, 9.0E0,
     +                  9.0E0, 9.0E0, 0.09E0, 2.0E0, -0.12E0, 2.0E0,
     +                  2.0E0, 2.0E0, 2.0E0, 2.0E0, 0.06E0, 3.0E0,
     +                  -0.18E0, 5.0E0, 0.09E0, 2.0E0, 2.0E0, 2.0E0,
     +                  0.03E0, 4.0E0, -0.09E0, 6.0E0, -0.15E0, 7.0E0,
     +                  -0.03E0, 3.0E0/
      DATA              ITRUE2/0, 1, 2, 2, 3/
*     .. Executable Statements ..
      DO 80 INCX = 1, 2
         DO 60 NP1 = 1, 5
            N = NP1 - 1
            LEN = 2*MAX(N,1)
*           .. Set vector arguments ..
            DO 20 I = 1, LEN
               SX(I) = DV(I,NP1,INCX)
   20       CONTINUE
*
            IF (ICASE.EQ.7) THEN
*              .. SNRM2TEST ..
               STEMP(1) = DTRUE1(NP1)
               CALL STEST1(SNRM2TEST(N,SX,INCX),STEMP(1),STEMP,SFAC)
            ELSE IF (ICASE.EQ.8) THEN
*              .. SASUMTEST ..
               STEMP(1) = DTRUE3(NP1)
               CALL STEST1(SASUMTEST(N,SX,INCX),STEMP(1),STEMP,SFAC)
            ELSE IF (ICASE.EQ.9) THEN
*              .. SSCALTEST ..
               CALL SSCALTEST(N,SA((INCX-1)*5+NP1),SX,INCX)
               DO 40 I = 1, LEN
                  STRUE(I) = DTRUE5(I,NP1,INCX)
   40          CONTINUE
               CALL STEST(LEN,SX,STRUE,STRUE,SFAC)
            ELSE IF (ICASE.EQ.10) THEN
*              .. ISAMAXTEST ..
               CALL ITEST1(ISAMAXTEST(N,SX,INCX),ITRUE2(NP1))
            ELSE
               WRITE (NOUT,*) ' Shouldn''t be here in CHECK1'
               STOP
            END IF
   60    CONTINUE
   80 CONTINUE
      RETURN
      END
      SUBROUTINE CHECK2(SFAC)
*     .. Parameters ..
      INTEGER           NOUT
      PARAMETER         (NOUT=6)
*     .. Scalar Arguments ..
      REAL              SFAC
*     .. Scalars in Common ..
      INTEGER           ICASE, INCX, INCY, MODE, N
      LOGICAL           PASS
*     .. Local Scalars ..
      REAL              SA
      INTEGER           I, J, KI, KN, KSIZE, LENX, LENY, MX, MY
*     .. Local Arrays ..
      REAL              DT10X(7,4,4), DT10Y(7,4,4), DT7(4,4),
     +                  DT8(7,4,4), DX1(7),
     +                  DY1(7), SSIZE1(4), SSIZE2(14,2), STX(7), STY(7),
     +                  SX(7), SY(7)
      INTEGER           INCXS(4), INCYS(4), LENS(4,2), NS(4)
*     .. External Functions ..
      REAL              SDOTTEST
      EXTERNAL          SDOTTEST
*     .. External Subroutines ..
      EXTERNAL          SAXPYTEST, SCOPYTEST, SSWAPTEST, STEST, STEST1
*     .. Intrinsic Functions ..
      INTRINSIC         ABS, MIN
*     .. Common blocks ..
      COMMON            /COMBLA/ICASE, N, INCX, INCY, MODE, PASS
*     .. Data statements ..
      DATA              SA/0.3E0/
      DATA              INCXS/1, 2, -2, -1/
      DATA              INCYS/1, -2, 1, -2/
      DATA              LENS/1, 1, 2, 4, 1, 1, 3, 7/
      DATA              NS/0, 1, 2, 4/
      DATA              DX1/0.6E0, 0.1E0, -0.5E0, 0.8E0, 0.9E0, -0.3E0,
     +                  -0.4E0/
      DATA              DY1/0.5E0, -0.9E0, 0.3E0, 0.7E0, -0.6E0, 0.2E0,
     +                  0.8E0/
      DATA              DT7/0.0E0, 0.30E0, 0.21E0, 0.62E0, 0.0E0,
     +                  0.30E0, -0.07E0, 0.85E0, 0.0E0, 0.30E0, -0.79E0,
     +                  -0.74E0, 0.0E0, 0.30E0, 0.33E0, 1.27E0/
      DATA              DT8/0.5E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.68E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.68E0, -0.87E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.68E0, -0.87E0, 0.15E0,
     +                  0.94E0, 0.0E0, 0.0E0, 0.0E0, 0.5E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.68E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.35E0, -0.9E0, 0.48E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.38E0, -0.9E0, 0.57E0, 0.7E0, -0.75E0,
     +                  0.2E0, 0.98E0, 0.5E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.68E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.35E0, -0.72E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.38E0,
     +                  -0.63E0, 0.15E0, 0.88E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.5E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.68E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.68E0, -0.9E0, 0.33E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.68E0, -0.9E0, 0.33E0, 0.7E0,
     +                  -0.75E0, 0.2E0, 1.04E0/
      DATA              DT10X/0.6E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.5E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.5E0, -0.9E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.5E0, -0.9E0, 0.3E0, 0.7E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.6E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.5E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.3E0, 0.1E0, 0.5E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.8E0, 0.1E0, -0.6E0,
     +                  0.8E0, 0.3E0, -0.3E0, 0.5E0, 0.6E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.5E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, -0.9E0,
     +                  0.1E0, 0.5E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.7E0,
     +                  0.1E0, 0.3E0, 0.8E0, -0.9E0, -0.3E0, 0.5E0,
     +                  0.6E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.5E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.5E0, 0.3E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.5E0, 0.3E0, -0.6E0, 0.8E0, 0.0E0, 0.0E0,
     +                  0.0E0/
      DATA              DT10Y/0.5E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.6E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.6E0, 0.1E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.6E0, 0.1E0, -0.5E0, 0.8E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.5E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.6E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, -0.5E0, -0.9E0, 0.6E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, -0.4E0, -0.9E0, 0.9E0,
     +                  0.7E0, -0.5E0, 0.2E0, 0.6E0, 0.5E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.6E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, -0.5E0,
     +                  0.6E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  -0.4E0, 0.9E0, -0.5E0, 0.6E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.5E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.6E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.6E0, -0.9E0, 0.1E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.6E0, -0.9E0, 0.1E0, 0.7E0,
     +                  -0.5E0, 0.2E0, 0.8E0/
      DATA              SSIZE1/0.0E0, 0.3E0, 1.6E0, 3.2E0/
      DATA              SSIZE2/0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 1.17E0, 1.17E0, 1.17E0, 1.17E0, 1.17E0,
     +                  1.17E0, 1.17E0, 1.17E0, 1.17E0, 1.17E0, 1.17E0,
     +                  1.17E0, 1.17E0, 1.17E0/
*     .. Executable Statements ..
*
      DO 120 KI = 1, 4
         INCX = INCXS(KI)
         INCY = INCYS(KI)
         MX = ABS(INCX)
         MY = ABS(INCY)
*
         DO 100 KN = 1, 4
            N = NS(KN)
            KSIZE = MIN(2,KN)
            LENX = LENS(KN,MX)
            LENY = LENS(KN,MY)
*           .. Initialize all argument arrays ..
            DO 20 I = 1, 7
               SX(I) = DX1(I)
               SY(I) = DY1(I)
   20       CONTINUE
*
            IF (ICASE.EQ.1) THEN
*              .. SDOTTEST ..
               CALL STEST1(SDOTTEST(N,SX,INCX,SY,INCY),DT7(KN,KI),
     +                     SSIZE1(KN),SFAC)
            ELSE IF (ICASE.EQ.2) THEN
*              .. SAXPYTEST ..
               CALL SAXPYTEST(N,SA,SX,INCX,SY,INCY)
               DO 40 J = 1, LENY
                  STY(J) = DT8(J,KN,KI)
   40          CONTINUE
               CALL STEST(LENY,SY,STY,SSIZE2(1,KSIZE),SFAC)
            ELSE IF (ICASE.EQ.5) THEN
*              .. SCOPYTEST ..
               DO 60 I = 1, 7
                  STY(I) = DT10Y(I,KN,KI)
   60          CONTINUE
               CALL SCOPYTEST(N,SX,INCX,SY,INCY)
               CALL STEST(LENY,SY,STY,SSIZE2(1,1),1.0E0)
            ELSE IF (ICASE.EQ.6) THEN
*              .. SSWAPTEST ..
               CALL SSWAPTEST(N,SX,INCX,SY,INCY)
               DO 80 I = 1, 7
                  STX(I) = DT10X(I,KN,KI)
                  STY(I) = DT10Y(I,KN,KI)
   80          CONTINUE
               CALL STEST(LENX,SX,STX,SSIZE2(1,1),1.0E0)
               CALL STEST(LENY,SY,STY,SSIZE2(1,1),1.0E0)
            ELSE
               WRITE (NOUT,*) ' Shouldn''t be here in CHECK2'
               STOP
            END IF
  100    CONTINUE
  120 CONTINUE
      RETURN
      END
      SUBROUTINE CHECK3(SFAC)
*     .. Parameters ..
      INTEGER           NOUT
      PARAMETER         (NOUT=6)
*     .. Scalar Arguments ..
      REAL              SFAC
*     .. Scalars in Common ..
      INTEGER           ICASE, INCX, INCY, MODE, N
      LOGICAL           PASS
*     .. Local Scalars ..
      REAL              SC, SS
      INTEGER           I, K, KI, KN, KSIZE, LENX, LENY, MX, MY
*     .. Local Arrays ..
      REAL              COPYX(5), COPYY(5), DT9X(7,4,4), DT9Y(7,4,4),
     +                  DX1(7), DY1(7), MWPC(11), MWPS(11), MWPSTX(5),
     +                  MWPSTY(5), MWPTX(11,5), MWPTY(11,5), MWPX(5),
     +                  MWPY(5), SSIZE2(14,2), STX(7), STY(7), SX(7),
     +                  SY(7)
      INTEGER           INCXS(4), INCYS(4), LENS(4,2), MWPINX(11),
     +                  MWPINY(11), MWPN(11), NS(4)
*     .. External Subroutines ..
      EXTERNAL          SROTTEST, STEST
*     .. Intrinsic Functions ..
      INTRINSIC         ABS, MIN
*     .. Common blocks ..
      COMMON            /COMBLA/ICASE, N, INCX, INCY, MODE, PASS
*     .. Data statements ..
      DATA              INCXS/1, 2, -2, -1/
      DATA              INCYS/1, -2, 1, -2/
      DATA              LENS/1, 1, 2, 4, 1, 1, 3, 7/
      DATA              NS/0, 1, 2, 4/
      DATA              DX1/0.6E0, 0.1E0, -0.5E0, 0.8E0, 0.9E0, -0.3E0,
     +                  -0.4E0/
      DATA              DY1/0.5E0, -0.9E0, 0.3E0, 0.7E0, -0.6E0, 0.2E0,
     +                  0.8E0/
      DATA              SC, SS/0.8E0, 0.6E0/
      DATA              DT9X/0.6E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.78E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.78E0, -0.46E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.78E0, -0.46E0, -0.22E0,
     +                  1.06E0, 0.0E0, 0.0E0, 0.0E0, 0.6E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.78E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.66E0, 0.1E0, -0.1E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.96E0, 0.1E0, -0.76E0, 0.8E0, 0.90E0,
     +                  -0.3E0, -0.02E0, 0.6E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.78E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, -0.06E0, 0.1E0,
     +                  -0.1E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.90E0,
     +                  0.1E0, -0.22E0, 0.8E0, 0.18E0, -0.3E0, -0.02E0,
     +                  0.6E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.78E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.78E0, 0.26E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.78E0, 0.26E0, -0.76E0, 1.12E0,
     +                  0.0E0, 0.0E0, 0.0E0/
      DATA              DT9Y/0.5E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.04E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.04E0, -0.78E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.04E0, -0.78E0, 0.54E0,
     +                  0.08E0, 0.0E0, 0.0E0, 0.0E0, 0.5E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.04E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.7E0,
     +                  -0.9E0, -0.12E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.64E0, -0.9E0, -0.30E0, 0.7E0, -0.18E0, 0.2E0,
     +                  0.28E0, 0.5E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.04E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.7E0, -1.08E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.64E0, -1.26E0,
     +                  0.54E0, 0.20E0, 0.0E0, 0.0E0, 0.0E0, 0.5E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.04E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.04E0, -0.9E0, 0.18E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.04E0, -0.9E0, 0.18E0, 0.7E0,
     +                  -0.18E0, 0.2E0, 0.16E0/
      DATA              SSIZE2/0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0, 0.0E0,
     +                  0.0E0, 1.17E0, 1.17E0, 1.17E0, 1.17E0, 1.17E0,
     +                  1.17E0, 1.17E0, 1.17E0, 1.17E0, 1.17E0, 1.17E0,
     +                  1.17E0, 1.17E0, 1.17E0/
*     .. Executable Statements ..
*
      DO 60 KI = 1, 4
         INCX = INCXS(KI)
         INCY = INCYS(KI)
         MX = ABS(INCX)
         MY = ABS(INCY)
*
         DO 40 KN = 1, 4
            N = NS(KN)
            KSIZE = MIN(2,KN)
            LENX = LENS(KN,MX)
            LENY = LENS(KN,MY)
*
            IF (ICASE.EQ.4) THEN
*              .. SROTTEST ..
               DO 20 I = 1, 7
                  SX(I) = DX1(I)
                  SY(I) = DY1(I)
                  STX(I) = DT9X(I,KN,KI)
                  STY(I) = DT9Y(I,KN,KI)
   20          CONTINUE
               CALL SROTTEST(N,SX,INCX,SY,INCY,SC,SS)
               CALL STEST(LENX,SX,STX,SSIZE2(1,KSIZE),SFAC)
               CALL STEST(LENY,SY,STY,SSIZE2(1,KSIZE),SFAC)
            ELSE
               WRITE (NOUT,*) ' Shouldn''t be here in CHECK3'
               STOP
            END IF
   40    CONTINUE
   60 CONTINUE
*
      MWPC(1) = 1
      DO 80 I = 2, 11
         MWPC(I) = 0
   80 CONTINUE
      MWPS(1) = 0
      DO 100 I = 2, 6
         MWPS(I) = 1
  100 CONTINUE
      DO 120 I = 7, 11
         MWPS(I) = -1
  120 CONTINUE
      MWPINX(1) = 1
      MWPINX(2) = 1
      MWPINX(3) = 1
      MWPINX(4) = -1
      MWPINX(5) = 1
      MWPINX(6) = -1
      MWPINX(7) = 1
      MWPINX(8) = 1
      MWPINX(9) = -1
      MWPINX(10) = 1
      MWPINX(11) = -1
      MWPINY(1) = 1
      MWPINY(2) = 1
      MWPINY(3) = -1
      MWPINY(4) = -1
      MWPINY(5) = 2
      MWPINY(6) = 1
      MWPINY(7) = 1
      MWPINY(8) = -1
      MWPINY(9) = -1
      MWPINY(10) = 2
      MWPINY(11) = 1
      DO 140 I = 1, 11
         MWPN(I) = 5
  140 CONTINUE
      MWPN(5) = 3
      MWPN(10) = 3
      DO 160 I = 1, 5
         MWPX(I) = I
         MWPY(I) = I
         MWPTX(1,I) = I
         MWPTY(1,I) = I
         MWPTX(2,I) = I
         MWPTY(2,I) = -I
         MWPTX(3,I) = 6 - I
         MWPTY(3,I) = I - 6
         MWPTX(4,I) = I
         MWPTY(4,I) = -I
         MWPTX(6,I) = 6 - I
         MWPTY(6,I) = I - 6
         MWPTX(7,I) = -I
         MWPTY(7,I) = I
         MWPTX(8,I) = I - 6
         MWPTY(8,I) = 6 - I
         MWPTX(9,I) = -I
         MWPTY(9,I) = I
         MWPTX(11,I) = I - 6
         MWPTY(11,I) = 6 - I
  160 CONTINUE
      MWPTX(5,1) = 1
      MWPTX(5,2) = 3
      MWPTX(5,3) = 5
      MWPTX(5,4) = 4
      MWPTX(5,5) = 5
      MWPTY(5,1) = -1
      MWPTY(5,2) = 2
      MWPTY(5,3) = -2
      MWPTY(5,4) = 4
      MWPTY(5,5) = -3
      MWPTX(10,1) = -1
      MWPTX(10,2) = -3
      MWPTX(10,3) = -5
      MWPTX(10,4) = 4
      MWPTX(10,5) = 5
      MWPTY(10,1) = 1
      MWPTY(10,2) = 2
      MWPTY(10,3) = 2
      MWPTY(10,4) = 4
      MWPTY(10,5) = 3
      DO 200 I = 1, 11
         INCX = MWPINX(I)
         INCY = MWPINY(I)
         DO 180 K = 1, 5
            COPYX(K) = MWPX(K)
            COPYY(K) = MWPY(K)
            MWPSTX(K) = MWPTX(I,K)
            MWPSTY(K) = MWPTY(I,K)
  180    CONTINUE
         CALL SROTTEST(MWPN(I),COPYX,INCX,COPYY,INCY,MWPC(I),MWPS(I))
         CALL STEST(5,COPYX,MWPSTX,MWPSTX,SFAC)
         CALL STEST(5,COPYY,MWPSTY,MWPSTY,SFAC)
  200 CONTINUE
      RETURN
      END
      SUBROUTINE STEST(LEN,SCOMP,STRUE,SSIZE,SFAC)
*     ********************************* STEST **************************
*
*     THIS SUBR COMPARES ARRAYS  SCOMP() AND STRUE() OF LENGTH LEN TO
*     SEE IF THE TERM BY TERM DIFFERENCES, MULTIPLIED BY SFAC, ARE
*     NEGLIGIBLE.
*
*     C. L. LAWSON, JPL, 1974 DEC 10
*
*     .. Parameters ..
      INTEGER          NOUT
      PARAMETER        (NOUT=6)
*     .. Scalar Arguments ..
      REAL             SFAC
      INTEGER          LEN
*     .. Array Arguments ..
      REAL             SCOMP(LEN), SSIZE(LEN), STRUE(LEN)
*     .. Scalars in Common ..
      INTEGER          ICASE, INCX, INCY, MODE, N
      LOGICAL          PASS
*     .. Local Scalars ..
      REAL             SD
      INTEGER          I
*     .. External Functions ..
      REAL             SDIFF
      EXTERNAL         SDIFF
*     .. Intrinsic Functions ..
      INTRINSIC        ABS
*     .. Common blocks ..
      COMMON           /COMBLA/ICASE, N, INCX, INCY, MODE, PASS
*     .. Executable Statements ..
*
      DO 40 I = 1, LEN
         SD = SCOMP(I) - STRUE(I)
         IF (SDIFF(ABS(SSIZE(I))+ABS(SFAC*SD),ABS(SSIZE(I))).EQ.0.0E0)
     +       GO TO 40
*
*                             HERE    SCOMP(I) IS NOT CLOSE TO STRUE(I).
*
         IF ( .NOT. PASS) GO TO 20
*                             PRINT FAIL MESSAGE AND HEADER.
         PASS = .FALSE.
         WRITE (NOUT,99999)
         WRITE (NOUT,99998)
   20    WRITE (NOUT,99997) ICASE, N, INCX, INCY, MODE, I, SCOMP(I),
     +     STRUE(I), SD, SSIZE(I)
   40 CONTINUE
      RETURN
*
99999 FORMAT ('                                       FAIL')
99998 FORMAT (/' CASE  N INCX INCY MODE  I                            ',
     +       ' COMP(I)                             TRUE(I)  DIFFERENCE',
     +       '     SIZE(I)',/1X)
99997 FORMAT (1X,I4,I3,3I5,I3,2E36.8,2E12.4)
      END
      SUBROUTINE STEST1(SCOMP1,STRUE1,SSIZE,SFAC)
*     ************************* STEST1 *****************************
*
*     THIS IS AN INTERFACE SUBROUTINE TO ACCOMODATE THE FORTRAN
*     REQUIREMENT THAT WHEN A DUMMY ARGUMENT IS AN ARRAY, THE
*     ACTUAL ARGUMENT MUST ALSO BE AN ARRAY OR AN ARRAY ELEMENT.
*
*     C.L. LAWSON, JPL, 1978 DEC 6
*
*     .. Scalar Arguments ..
      REAL              SCOMP1, SFAC, STRUE1
*     .. Array Arguments ..
      REAL              SSIZE(*)
*     .. Local Arrays ..
      REAL              SCOMP(1), STRUE(1)
*     .. External Subroutines ..
      EXTERNAL          STEST
*     .. Executable Statements ..
*
      SCOMP(1) = SCOMP1
      STRUE(1) = STRUE1
      CALL STEST(1,SCOMP,STRUE,SSIZE,SFAC)
*
      RETURN
      END
      REAL             FUNCTION SDIFF(SA,SB)
*     ********************************* SDIFF **************************
*     COMPUTES DIFFERENCE OF TWO NUMBERS.  C. L. LAWSON, JPL 1974 FEB 15
*
*     .. Scalar Arguments ..
      REAL                            SA, SB
*     .. Executable Statements ..
      SDIFF = SA - SB
      RETURN
      END
      SUBROUTINE ITEST1(ICOMP,ITRUE)
*     ********************************* ITEST1 *************************
*
*     THIS SUBROUTINE COMPARES THE VARIABLES ICOMP AND ITRUE FOR
*     EQUALITY.
*     C. L. LAWSON, JPL, 1974 DEC 10
*
*     .. Parameters ..
      INTEGER           NOUT
      PARAMETER         (NOUT=6)
*     .. Scalar Arguments ..
      INTEGER           ICOMP, ITRUE
*     .. Scalars in Common ..
      INTEGER           ICASE, INCX, INCY, MODE, N
      LOGICAL           PASS
*     .. Local Scalars ..
      INTEGER           ID
*     .. Common blocks ..
      COMMON            /COMBLA/ICASE, N, INCX, INCY, MODE, PASS
*     .. Executable Statements ..
*
      IF (ICOMP.EQ.ITRUE) GO TO 40
*
*                            HERE ICOMP IS NOT EQUAL TO ITRUE.
*
      IF ( .NOT. PASS) GO TO 20
*                             PRINT FAIL MESSAGE AND HEADER.
      PASS = .FALSE.
      WRITE (NOUT,99999)
      WRITE (NOUT,99998)
   20 ID = ICOMP - ITRUE
      WRITE (NOUT,99997) ICASE, N, INCX, INCY, MODE, ICOMP, ITRUE, ID
   40 CONTINUE
      RETURN
*
99999 FORMAT ('                                       FAIL')
99998 FORMAT (/' CASE  N INCX INCY MODE                               ',
     +       ' COMP                                TRUE     DIFFERENCE',
     +       /1X)
99997 FORMAT (1X,I4,I3,3I5,2I36,I12)
      END
