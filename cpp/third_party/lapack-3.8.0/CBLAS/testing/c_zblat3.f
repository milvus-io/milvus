      PROGRAM ZBLAT3
*
*  Test program for the COMPLEX*16          Level 3 Blas.
*
*  The program must be driven by a short data file. The first 13 records
*  of the file are read using list-directed input, the last 9 records
*  are read using the format ( A12,L2 ). An annotated example of a data
*  file can be obtained by deleting the first 3 characters from the
*  following 22 lines:
*  'CBLAT3.SNAP'     NAME OF SNAPSHOT OUTPUT FILE
*  -1                UNIT NUMBER OF SNAPSHOT FILE (NOT USED IF .LT. 0)
*  F        LOGICAL FLAG, T TO REWIND SNAPSHOT FILE AFTER EACH RECORD.
*  F        LOGICAL FLAG, T TO STOP ON FAILURES.
*  T        LOGICAL FLAG, T TO TEST ERROR EXITS.
*  2        0 TO TEST COLUMN-MAJOR, 1 TO TEST ROW-MAJOR, 2 TO TEST BOTH
*  16.0     THRESHOLD VALUE OF TEST RATIO
*  6                 NUMBER OF VALUES OF N
*  0 1 2 3 5 9       VALUES OF N
*  3                 NUMBER OF VALUES OF ALPHA
*  (0.0,0.0) (1.0,0.0) (0.7,-0.9)       VALUES OF ALPHA
*  3                 NUMBER OF VALUES OF BETA
*  (0.0,0.0) (1.0,0.0) (1.3,-1.1)       VALUES OF BETA
*  ZGEMM  T PUT F FOR NO TEST. SAME COLUMNS.
*  ZHEMM  T PUT F FOR NO TEST. SAME COLUMNS.
*  ZSYMM  T PUT F FOR NO TEST. SAME COLUMNS.
*  ZTRMM  T PUT F FOR NO TEST. SAME COLUMNS.
*  ZTRSM  T PUT F FOR NO TEST. SAME COLUMNS.
*  ZHERK  T PUT F FOR NO TEST. SAME COLUMNS.
*  ZSYRK  T PUT F FOR NO TEST. SAME COLUMNS.
*  ZHER2K T PUT F FOR NO TEST. SAME COLUMNS.
*  ZSYR2K T PUT F FOR NO TEST. SAME COLUMNS.
*
*  See:
*
*     Dongarra J. J., Du Croz J. J., Duff I. S. and Hammarling S.
*     A Set of Level 3 Basic Linear Algebra Subprograms.
*
*     Technical Memorandum No.88 (Revision 1), Mathematics and
*     Computer Science Division, Argonne National Laboratory, 9700
*     South Cass Avenue, Argonne, Illinois 60439, US.
*
*  -- Written on 8-February-1989.
*     Jack Dongarra, Argonne National Laboratory.
*     Iain Duff, AERE Harwell.
*     Jeremy Du Croz, Numerical Algorithms Group Ltd.
*     Sven Hammarling, Numerical Algorithms Group Ltd.
*
*     .. Parameters ..
      INTEGER            NIN, NOUT
      PARAMETER          ( NIN = 5, NOUT = 6 )
      INTEGER            NSUBS
      PARAMETER          ( NSUBS = 9 )
      COMPLEX*16         ZERO, ONE
      PARAMETER          ( ZERO = ( 0.0D0, 0.0D0 ),
     $                   ONE = ( 1.0D0, 0.0D0 ) )
      DOUBLE PRECISION   RZERO, RHALF, RONE
      PARAMETER          ( RZERO = 0.0D0, RHALF = 0.5D0, RONE = 1.0D0 )
      INTEGER            NMAX
      PARAMETER          ( NMAX = 65 )
      INTEGER            NIDMAX, NALMAX, NBEMAX
      PARAMETER          ( NIDMAX = 9, NALMAX = 7, NBEMAX = 7 )
*     .. Local Scalars ..
      DOUBLE PRECISION   EPS, ERR, THRESH
      INTEGER            I, ISNUM, J, N, NALF, NBET, NIDIM, NTRA,
     $                   LAYOUT
      LOGICAL            FATAL, LTESTT, REWI, SAME, SFATAL, TRACE,
     $                   TSTERR, CORDER, RORDER
      CHARACTER*1        TRANSA, TRANSB
      CHARACTER*12       SNAMET
      CHARACTER*32       SNAPS
*     .. Local Arrays ..
      COMPLEX*16         AA( NMAX*NMAX ), AB( NMAX, 2*NMAX ),
     $                   ALF( NALMAX ), AS( NMAX*NMAX ),
     $                   BB( NMAX*NMAX ), BET( NBEMAX ),
     $                   BS( NMAX*NMAX ), C( NMAX, NMAX ),
     $                   CC( NMAX*NMAX ), CS( NMAX*NMAX ), CT( NMAX ),
     $                   W( 2*NMAX )
      DOUBLE PRECISION   G( NMAX )
      INTEGER            IDIM( NIDMAX )
      LOGICAL            LTEST( NSUBS )
      CHARACTER*12       SNAMES( NSUBS )
*     .. External Functions ..
      DOUBLE PRECISION   DDIFF
      LOGICAL            LZE
      EXTERNAL           DDIFF, LZE
*     .. External Subroutines ..
      EXTERNAL           ZCHK1, ZCHK2, ZCHK3, ZCHK4, ZCHK5,ZMMCH
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN
*     .. Scalars in Common ..
      INTEGER            INFOT, NOUTC
      LOGICAL            LERR, OK
      CHARACTER*12       SRNAMT
*     .. Common blocks ..
      COMMON             /INFOC/INFOT, NOUTC, OK, LERR
      COMMON             /SRNAMC/SRNAMT
*     .. Data statements ..
      DATA               SNAMES/'cblas_zgemm ', 'cblas_zhemm ',
     $                   'cblas_zsymm ', 'cblas_ztrmm ', 'cblas_ztrsm ',
     $                   'cblas_zherk ', 'cblas_zsyrk ', 'cblas_zher2k',
     $                   'cblas_zsyr2k'/
*     .. Executable Statements ..
*
      NOUTC = NOUT
*
*     Read name and unit number for snapshot output file and open file.
*
      READ( NIN, FMT = * )SNAPS
      READ( NIN, FMT = * )NTRA
      TRACE = NTRA.GE.0
      IF( TRACE )THEN
         OPEN( NTRA, FILE = SNAPS, STATUS = 'NEW' )
      END IF
*     Read the flag that directs rewinding of the snapshot file.
      READ( NIN, FMT = * )REWI
      REWI = REWI.AND.TRACE
*     Read the flag that directs stopping on any failure.
      READ( NIN, FMT = * )SFATAL
*     Read the flag that indicates whether error exits are to be tested.
      READ( NIN, FMT = * )TSTERR
*     Read the flag that indicates whether row-major data layout to be tested.
      READ( NIN, FMT = * )LAYOUT
*     Read the threshold value of the test ratio
      READ( NIN, FMT = * )THRESH
*
*     Read and check the parameter values for the tests.
*
*     Values of N
      READ( NIN, FMT = * )NIDIM
      IF( NIDIM.LT.1.OR.NIDIM.GT.NIDMAX )THEN
         WRITE( NOUT, FMT = 9997 )'N', NIDMAX
         GO TO 220
      END IF
      READ( NIN, FMT = * )( IDIM( I ), I = 1, NIDIM )
      DO 10 I = 1, NIDIM
         IF( IDIM( I ).LT.0.OR.IDIM( I ).GT.NMAX )THEN
            WRITE( NOUT, FMT = 9996 )NMAX
            GO TO 220
         END IF
   10 CONTINUE
*     Values of ALPHA
      READ( NIN, FMT = * )NALF
      IF( NALF.LT.1.OR.NALF.GT.NALMAX )THEN
         WRITE( NOUT, FMT = 9997 )'ALPHA', NALMAX
         GO TO 220
      END IF
      READ( NIN, FMT = * )( ALF( I ), I = 1, NALF )
*     Values of BETA
      READ( NIN, FMT = * )NBET
      IF( NBET.LT.1.OR.NBET.GT.NBEMAX )THEN
         WRITE( NOUT, FMT = 9997 )'BETA', NBEMAX
         GO TO 220
      END IF
      READ( NIN, FMT = * )( BET( I ), I = 1, NBET )
*
*     Report values of parameters.
*
      WRITE( NOUT, FMT = 9995 )
      WRITE( NOUT, FMT = 9994 )( IDIM( I ), I = 1, NIDIM )
      WRITE( NOUT, FMT = 9993 )( ALF( I ), I = 1, NALF )
      WRITE( NOUT, FMT = 9992 )( BET( I ), I = 1, NBET )
      IF( .NOT.TSTERR )THEN
         WRITE( NOUT, FMT = * )
         WRITE( NOUT, FMT = 9984 )
      END IF
      WRITE( NOUT, FMT = * )
      WRITE( NOUT, FMT = 9999 )THRESH
      WRITE( NOUT, FMT = * )

      RORDER = .FALSE.
      CORDER = .FALSE.
      IF (LAYOUT.EQ.2) THEN
         RORDER = .TRUE.
         CORDER = .TRUE.
         WRITE( *, FMT = 10002 )
      ELSE IF (LAYOUT.EQ.1) THEN
         RORDER = .TRUE.
         WRITE( *, FMT = 10001 )
      ELSE IF (LAYOUT.EQ.0) THEN
         CORDER = .TRUE.
         WRITE( *, FMT = 10000 )
      END IF
      WRITE( *, FMT = * )

*
*     Read names of subroutines and flags which indicate
*     whether they are to be tested.
*
      DO 20 I = 1, NSUBS
         LTEST( I ) = .FALSE.
   20 CONTINUE
   30 READ( NIN, FMT = 9988, END = 60 )SNAMET, LTESTT
      DO 40 I = 1, NSUBS
         IF( SNAMET.EQ.SNAMES( I ) )
     $      GO TO 50
   40 CONTINUE
      WRITE( NOUT, FMT = 9990 )SNAMET
      STOP
   50 LTEST( I ) = LTESTT
      GO TO 30
*
   60 CONTINUE
      CLOSE ( NIN )
*
*     Compute EPS (the machine precision).
*
      EPS = RONE
   70 CONTINUE
      IF( DDIFF( RONE + EPS, RONE ).EQ.RZERO )
     $   GO TO 80
      EPS = RHALF*EPS
      GO TO 70
   80 CONTINUE
      EPS = EPS + EPS
      WRITE( NOUT, FMT = 9998 )EPS
*
*     Check the reliability of ZMMCH using exact data.
*
      N = MIN( 32, NMAX )
      DO 100 J = 1, N
         DO 90 I = 1, N
            AB( I, J ) = MAX( I - J + 1, 0 )
   90    CONTINUE
         AB( J, NMAX + 1 ) = J
         AB( 1, NMAX + J ) = J
         C( J, 1 ) = ZERO
  100 CONTINUE
      DO 110 J = 1, N
         CC( J ) = J*( ( J + 1 )*J )/2 - ( ( J + 1 )*J*( J - 1 ) )/3
  110 CONTINUE
*     CC holds the exact result. On exit from ZMMCH CT holds
*     the result computed by ZMMCH.
      TRANSA = 'N'
      TRANSB = 'N'
      CALL ZMMCH( TRANSA, TRANSB, N, 1, N, ONE, AB, NMAX,
     $            AB( 1, NMAX + 1 ), NMAX, ZERO, C, NMAX, CT, G, CC,
     $            NMAX, EPS, ERR, FATAL, NOUT, .TRUE. )
      SAME = LZE( CC, CT, N )
      IF( .NOT.SAME.OR.ERR.NE.RZERO )THEN
         WRITE( NOUT, FMT = 9989 )TRANSA, TRANSB, SAME, ERR
         STOP
      END IF
      TRANSB = 'C'
      CALL ZMMCH( TRANSA, TRANSB, N, 1, N, ONE, AB, NMAX,
     $            AB( 1, NMAX + 1 ), NMAX, ZERO, C, NMAX, CT, G, CC,
     $            NMAX, EPS, ERR, FATAL, NOUT, .TRUE. )
      SAME = LZE( CC, CT, N )
      IF( .NOT.SAME.OR.ERR.NE.RZERO )THEN
         WRITE( NOUT, FMT = 9989 )TRANSA, TRANSB, SAME, ERR
         STOP
      END IF
      DO 120 J = 1, N
         AB( J, NMAX + 1 ) = N - J + 1
         AB( 1, NMAX + J ) = N - J + 1
  120 CONTINUE
      DO 130 J = 1, N
         CC( N - J + 1 ) = J*( ( J + 1 )*J )/2 -
     $                     ( ( J + 1 )*J*( J - 1 ) )/3
  130 CONTINUE
      TRANSA = 'C'
      TRANSB = 'N'
      CALL ZMMCH( TRANSA, TRANSB, N, 1, N, ONE, AB, NMAX,
     $            AB( 1, NMAX + 1 ), NMAX, ZERO, C, NMAX, CT, G, CC,
     $            NMAX, EPS, ERR, FATAL, NOUT, .TRUE. )
      SAME = LZE( CC, CT, N )
      IF( .NOT.SAME.OR.ERR.NE.RZERO )THEN
         WRITE( NOUT, FMT = 9989 )TRANSA, TRANSB, SAME, ERR
         STOP
      END IF
      TRANSB = 'C'
      CALL ZMMCH( TRANSA, TRANSB, N, 1, N, ONE, AB, NMAX,
     $            AB( 1, NMAX + 1 ), NMAX, ZERO, C, NMAX, CT, G, CC,
     $            NMAX, EPS, ERR, FATAL, NOUT, .TRUE. )
      SAME = LZE( CC, CT, N )
      IF( .NOT.SAME.OR.ERR.NE.RZERO )THEN
         WRITE( NOUT, FMT = 9989 )TRANSA, TRANSB, SAME, ERR
         STOP
      END IF
*
*     Test each subroutine in turn.
*
      DO 200 ISNUM = 1, NSUBS
         WRITE( NOUT, FMT = * )
         IF( .NOT.LTEST( ISNUM ) )THEN
*           Subprogram is not to be tested.
            WRITE( NOUT, FMT = 9987 )SNAMES( ISNUM )
         ELSE
            SRNAMT = SNAMES( ISNUM )
*           Test error exits.
            IF( TSTERR )THEN
               CALL CZ3CHKE( SNAMES( ISNUM ) )
               WRITE( NOUT, FMT = * )
            END IF
*           Test computations.
            INFOT = 0
            OK = .TRUE.
            FATAL = .FALSE.
            GO TO ( 140, 150, 150, 160, 160, 170, 170,
     $              180, 180 )ISNUM
*           Test ZGEMM, 01.
  140       IF (CORDER) THEN
            CALL ZCHK1(SNAMES( ISNUM ), EPS, THRESH, NOUT, NTRA, TRACE,
     $                 REWI, FATAL, NIDIM, IDIM, NALF, ALF, NBET, BET,
     $                 NMAX, AB, AA, AS, AB( 1, NMAX + 1 ), BB, BS, C,
     $                 CC, CS, CT, G, 0 )
            END IF
            IF (RORDER) THEN
            CALL ZCHK1(SNAMES( ISNUM ), EPS, THRESH, NOUT, NTRA, TRACE,
     $                 REWI, FATAL, NIDIM, IDIM, NALF, ALF, NBET, BET,
     $                 NMAX, AB, AA, AS, AB( 1, NMAX + 1 ), BB, BS, C,
     $                 CC, CS, CT, G, 1 )
            END IF
            GO TO 190
*           Test ZHEMM, 02, ZSYMM, 03.
  150       IF (CORDER) THEN
            CALL ZCHK2(SNAMES( ISNUM ), EPS, THRESH, NOUT, NTRA, TRACE,
     $                 REWI, FATAL, NIDIM, IDIM, NALF, ALF, NBET, BET,
     $                 NMAX, AB, AA, AS, AB( 1, NMAX + 1 ), BB, BS, C,
     $                 CC, CS, CT, G, 0 )
            END IF
            IF (RORDER) THEN
            CALL ZCHK2(SNAMES( ISNUM ), EPS, THRESH, NOUT, NTRA, TRACE,
     $                 REWI, FATAL, NIDIM, IDIM, NALF, ALF, NBET, BET,
     $                 NMAX, AB, AA, AS, AB( 1, NMAX + 1 ), BB, BS, C,
     $                 CC, CS, CT, G, 1 )
            END IF
            GO TO 190
*           Test ZTRMM, 04, ZTRSM, 05.
  160       IF (CORDER) THEN
            CALL ZCHK3(SNAMES( ISNUM ), EPS, THRESH, NOUT, NTRA, TRACE,
     $                 REWI, FATAL, NIDIM, IDIM, NALF, ALF, NMAX, AB,
     $                 AA, AS, AB( 1, NMAX + 1 ), BB, BS, CT, G, C,
     $		       0 )
            END IF
            IF (RORDER) THEN
            CALL ZCHK3(SNAMES( ISNUM ), EPS, THRESH, NOUT, NTRA, TRACE,
     $                 REWI, FATAL, NIDIM, IDIM, NALF, ALF, NMAX, AB,
     $                 AA, AS, AB( 1, NMAX + 1 ), BB, BS, CT, G, C,
     $		       1 )
            END IF
            GO TO 190
*           Test ZHERK, 06, ZSYRK, 07.
  170       IF (CORDER) THEN
            CALL ZCHK4(SNAMES( ISNUM ), EPS, THRESH, NOUT, NTRA, TRACE,
     $                 REWI, FATAL, NIDIM, IDIM, NALF, ALF, NBET, BET,
     $                 NMAX, AB, AA, AS, AB( 1, NMAX + 1 ), BB, BS, C,
     $                 CC, CS, CT, G, 0 )
            END IF
            IF (RORDER) THEN
            CALL ZCHK4(SNAMES( ISNUM ), EPS, THRESH, NOUT, NTRA, TRACE,
     $                 REWI, FATAL, NIDIM, IDIM, NALF, ALF, NBET, BET,
     $                 NMAX, AB, AA, AS, AB( 1, NMAX + 1 ), BB, BS, C,
     $                 CC, CS, CT, G, 1 )
            END IF
            GO TO 190
*           Test ZHER2K, 08, ZSYR2K, 09.
  180       IF (CORDER) THEN
            CALL ZCHK5(SNAMES( ISNUM ), EPS, THRESH, NOUT, NTRA, TRACE,
     $                 REWI, FATAL, NIDIM, IDIM, NALF, ALF, NBET, BET,
     $                 NMAX, AB, AA, AS, BB, BS, C, CC, CS, CT, G, W,
     $		       0 )
            END IF
            IF (RORDER) THEN
            CALL ZCHK5(SNAMES( ISNUM ), EPS, THRESH, NOUT, NTRA, TRACE,
     $                 REWI, FATAL, NIDIM, IDIM, NALF, ALF, NBET, BET,
     $                 NMAX, AB, AA, AS, BB, BS, C, CC, CS, CT, G, W,
     $		       1 )
            END IF
            GO TO 190
*
  190       IF( FATAL.AND.SFATAL )
     $         GO TO 210
         END IF
  200 CONTINUE
      WRITE( NOUT, FMT = 9986 )
      GO TO 230
*
  210 CONTINUE
      WRITE( NOUT, FMT = 9985 )
      GO TO 230
*
  220 CONTINUE
      WRITE( NOUT, FMT = 9991 )
*
  230 CONTINUE
      IF( TRACE )
     $   CLOSE ( NTRA )
      CLOSE ( NOUT )
      STOP
*
10002 FORMAT( ' COLUMN-MAJOR AND ROW-MAJOR DATA LAYOUTS ARE TESTED' )
10001 FORMAT(' ROW-MAJOR DATA LAYOUT IS TESTED' )
10000 FORMAT(' COLUMN-MAJOR DATA LAYOUT IS TESTED' )
 9999 FORMAT(' ROUTINES PASS COMPUTATIONAL TESTS IF TEST RATIO IS LES',
     $      'S THAN', F8.2 )
 9998 FORMAT(' RELATIVE MACHINE PRECISION IS TAKEN TO BE', 1P, E9.1 )
 9997 FORMAT(' NUMBER OF VALUES OF ', A, ' IS LESS THAN 1 OR GREATER ',
     $      'THAN ', I2 )
 9996 FORMAT( ' VALUE OF N IS LESS THAN 0 OR GREATER THAN ', I2 )
 9995 FORMAT('TESTS OF THE COMPLEX*16        LEVEL 3 BLAS', //' THE F',
     $      'OLLOWING PARAMETER VALUES WILL BE USED:' )
 9994 FORMAT( '   FOR N              ', 9I6 )
 9993 FORMAT( '   FOR ALPHA          ',
     $      7( '(', F4.1, ',', F4.1, ')  ', : ) )
 9992 FORMAT( '   FOR BETA           ',
     $      7( '(', F4.1, ',', F4.1, ')  ', : ) )
 9991 FORMAT( ' AMEND DATA FILE OR INCREASE ARRAY SIZES IN PROGRAM',
     $      /' ******* TESTS ABANDONED *******' )
 9990 FORMAT(' SUBPROGRAM NAME ', A12,' NOT RECOGNIZED', /' ******* T',
     $      'ESTS ABANDONED *******' )
 9989 FORMAT(' ERROR IN ZMMCH -  IN-LINE DOT PRODUCTS ARE BEING EVALU',
     $      'ATED WRONGLY.', /' ZMMCH WAS CALLED WITH TRANSA = ', A1,
     $      'AND TRANSB = ', A1, /' AND RETURNED SAME = ', L1, ' AND ',
     $    ' ERR = ', F12.3, '.', /' THIS MAY BE DUE TO FAULTS IN THE ',
     $     'ARITHMETIC OR THE COMPILER.', /' ******* TESTS ABANDONED ',
     $      '*******' )
 9988 FORMAT( A12,L2 )
 9987 FORMAT( 1X, A12,' WAS NOT TESTED' )
 9986 FORMAT( /' END OF TESTS' )
 9985 FORMAT( /' ******* FATAL ERROR - TESTS ABANDONED *******' )
 9984 FORMAT( ' ERROR-EXITS WILL NOT BE TESTED' )
*
*     End of ZBLAT3.
*
      END
      SUBROUTINE ZCHK1( SNAME, EPS, THRESH, NOUT, NTRA, TRACE, REWI,
     $                  FATAL, NIDIM, IDIM, NALF, ALF, NBET, BET, NMAX,
     $                  A, AA, AS, B, BB, BS, C, CC, CS, CT, G,
     $                  IORDER )
*
*  Tests ZGEMM.
*
*  Auxiliary routine for test program for Level 3 Blas.
*
*  -- Written on 8-February-1989.
*     Jack Dongarra, Argonne National Laboratory.
*     Iain Duff, AERE Harwell.
*     Jeremy Du Croz, Numerical Algorithms Group Ltd.
*     Sven Hammarling, Numerical Algorithms Group Ltd.
*
*     .. Parameters ..
      COMPLEX*16         ZERO
      PARAMETER          ( ZERO = ( 0.0, 0.0 ) )
      DOUBLE PRECISION   RZERO
      PARAMETER          ( RZERO = 0.0 )
*     .. Scalar Arguments ..
      DOUBLE PRECISION   EPS, THRESH
      INTEGER            NALF, NBET, NIDIM, NMAX, NOUT, NTRA, IORDER
      LOGICAL            FATAL, REWI, TRACE
      CHARACTER*12       SNAME
*     .. Array Arguments ..
      COMPLEX*16         A( NMAX, NMAX ), AA( NMAX*NMAX ), ALF( NALF ),
     $                   AS( NMAX*NMAX ), B( NMAX, NMAX ),
     $                   BB( NMAX*NMAX ), BET( NBET ), BS( NMAX*NMAX ),
     $                   C( NMAX, NMAX ), CC( NMAX*NMAX ),
     $                   CS( NMAX*NMAX ), CT( NMAX )
      DOUBLE PRECISION   G( NMAX )
      INTEGER            IDIM( NIDIM )
*     .. Local Scalars ..
      COMPLEX*16         ALPHA, ALS, BETA, BLS
      DOUBLE PRECISION   ERR, ERRMAX
      INTEGER            I, IA, IB, ICA, ICB, IK, IM, IN, K, KS, LAA,
     $                   LBB, LCC, LDA, LDAS, LDB, LDBS, LDC, LDCS, M,
     $                   MA, MB, MS, N, NA, NARGS, NB, NC, NS
      LOGICAL            NULL, RESET, SAME, TRANA, TRANB
      CHARACTER*1        TRANAS, TRANBS, TRANSA, TRANSB
      CHARACTER*3        ICH
*     .. Local Arrays ..
      LOGICAL            ISAME( 13 )
*     .. External Functions ..
      LOGICAL            LZE, LZERES
      EXTERNAL           LZE, LZERES
*     .. External Subroutines ..
      EXTERNAL           CZGEMM, ZMAKE, ZMMCH
*     .. Intrinsic Functions ..
      INTRINSIC          MAX
*     .. Scalars in Common ..
      INTEGER            INFOT, NOUTC
      LOGICAL            LERR, OK
*     .. Common blocks ..
      COMMON             /INFOC/INFOT, NOUTC, OK, LERR
*     .. Data statements ..
      DATA               ICH/'NTC'/
*     .. Executable Statements ..
*
      NARGS = 13
      NC = 0
      RESET = .TRUE.
      ERRMAX = RZERO
*
      DO 110 IM = 1, NIDIM
         M = IDIM( IM )
*
         DO 100 IN = 1, NIDIM
            N = IDIM( IN )
*           Set LDC to 1 more than minimum value if room.
            LDC = M
            IF( LDC.LT.NMAX )
     $         LDC = LDC + 1
*           Skip tests if not enough room.
            IF( LDC.GT.NMAX )
     $         GO TO 100
            LCC = LDC*N
            NULL = N.LE.0.OR.M.LE.0
*
            DO 90 IK = 1, NIDIM
               K = IDIM( IK )
*
               DO 80 ICA = 1, 3
                  TRANSA = ICH( ICA: ICA )
                  TRANA = TRANSA.EQ.'T'.OR.TRANSA.EQ.'C'
*
                  IF( TRANA )THEN
                     MA = K
                     NA = M
                  ELSE
                     MA = M
                     NA = K
                  END IF
*                 Set LDA to 1 more than minimum value if room.
                  LDA = MA
                  IF( LDA.LT.NMAX )
     $               LDA = LDA + 1
*                 Skip tests if not enough room.
                  IF( LDA.GT.NMAX )
     $               GO TO 80
                  LAA = LDA*NA
*
*                 Generate the matrix A.
*
                  CALL ZMAKE( 'ge', ' ', ' ', MA, NA, A, NMAX, AA, LDA,
     $                        RESET, ZERO )
*
                  DO 70 ICB = 1, 3
                     TRANSB = ICH( ICB: ICB )
                     TRANB = TRANSB.EQ.'T'.OR.TRANSB.EQ.'C'
*
                     IF( TRANB )THEN
                        MB = N
                        NB = K
                     ELSE
                        MB = K
                        NB = N
                     END IF
*                    Set LDB to 1 more than minimum value if room.
                     LDB = MB
                     IF( LDB.LT.NMAX )
     $                  LDB = LDB + 1
*                    Skip tests if not enough room.
                     IF( LDB.GT.NMAX )
     $                  GO TO 70
                     LBB = LDB*NB
*
*                    Generate the matrix B.
*
                     CALL ZMAKE( 'ge', ' ', ' ', MB, NB, B, NMAX, BB,
     $                           LDB, RESET, ZERO )
*
                     DO 60 IA = 1, NALF
                        ALPHA = ALF( IA )
*
                        DO 50 IB = 1, NBET
                           BETA = BET( IB )
*
*                          Generate the matrix C.
*
                           CALL ZMAKE( 'ge', ' ', ' ', M, N, C, NMAX,
     $                                 CC, LDC, RESET, ZERO )
*
                           NC = NC + 1
*
*                          Save every datum before calling the
*                          subroutine.
*
                           TRANAS = TRANSA
                           TRANBS = TRANSB
                           MS = M
                           NS = N
                           KS = K
                           ALS = ALPHA
                           DO 10 I = 1, LAA
                              AS( I ) = AA( I )
   10                      CONTINUE
                           LDAS = LDA
                           DO 20 I = 1, LBB
                              BS( I ) = BB( I )
   20                      CONTINUE
                           LDBS = LDB
                           BLS = BETA
                           DO 30 I = 1, LCC
                              CS( I ) = CC( I )
   30                      CONTINUE
                           LDCS = LDC
*
*                          Call the subroutine.
*
                           IF( TRACE )
     $                        CALL ZPRCN1(NTRA, NC, SNAME, IORDER,
     $                        TRANSA, TRANSB, M, N, K, ALPHA, LDA,
     $                        LDB, BETA, LDC)
                           IF( REWI )
     $                        REWIND NTRA
                           CALL CZGEMM( IORDER, TRANSA, TRANSB, M, N,
     $                                 K, ALPHA, AA, LDA, BB, LDB,
     $                                 BETA, CC, LDC )
*
*                          Check if error-exit was taken incorrectly.
*
                           IF( .NOT.OK )THEN
                              WRITE( NOUT, FMT = 9994 )
                              FATAL = .TRUE.
                              GO TO 120
                           END IF
*
*                          See what data changed inside subroutines.
*
                           ISAME( 1 ) = TRANSA.EQ.TRANAS
                           ISAME( 2 ) = TRANSB.EQ.TRANBS
                           ISAME( 3 ) = MS.EQ.M
                           ISAME( 4 ) = NS.EQ.N
                           ISAME( 5 ) = KS.EQ.K
                           ISAME( 6 ) = ALS.EQ.ALPHA
                           ISAME( 7 ) = LZE( AS, AA, LAA )
                           ISAME( 8 ) = LDAS.EQ.LDA
                           ISAME( 9 ) = LZE( BS, BB, LBB )
                           ISAME( 10 ) = LDBS.EQ.LDB
                           ISAME( 11 ) = BLS.EQ.BETA
                           IF( NULL )THEN
                              ISAME( 12 ) = LZE( CS, CC, LCC )
                           ELSE
                             ISAME( 12 ) = LZERES( 'ge', ' ', M, N, CS,
     $                                      CC, LDC )
                           END IF
                           ISAME( 13 ) = LDCS.EQ.LDC
*
*                          If data was incorrectly changed, report
*                          and return.
*
                           SAME = .TRUE.
                           DO 40 I = 1, NARGS
                              SAME = SAME.AND.ISAME( I )
                              IF( .NOT.ISAME( I ) )
     $                           WRITE( NOUT, FMT = 9998 )I
   40                      CONTINUE
                           IF( .NOT.SAME )THEN
                              FATAL = .TRUE.
                              GO TO 120
                           END IF
*
                           IF( .NOT.NULL )THEN
*
*                             Check the result.
*
                             CALL ZMMCH( TRANSA, TRANSB, M, N, K,
     $                                   ALPHA, A, NMAX, B, NMAX, BETA,
     $                                   C, NMAX, CT, G, CC, LDC, EPS,
     $                                   ERR, FATAL, NOUT, .TRUE. )
                              ERRMAX = MAX( ERRMAX, ERR )
*                             If got really bad answer, report and
*                             return.
                              IF( FATAL )
     $                           GO TO 120
                           END IF
*
   50                   CONTINUE
*
   60                CONTINUE
*
   70             CONTINUE
*
   80          CONTINUE
*
   90       CONTINUE
*
  100    CONTINUE
*
  110 CONTINUE
*
*     Report result.
*
      IF( ERRMAX.LT.THRESH )THEN
         IF ( IORDER.EQ.0) WRITE( NOUT, FMT = 10000 )SNAME, NC
         IF ( IORDER.EQ.1) WRITE( NOUT, FMT = 10001 )SNAME, NC
      ELSE
         IF ( IORDER.EQ.0) WRITE( NOUT, FMT = 10002 )SNAME, NC, ERRMAX
         IF ( IORDER.EQ.1) WRITE( NOUT, FMT = 10003 )SNAME, NC, ERRMAX
      END IF
      GO TO 130
*
  120 CONTINUE
      WRITE( NOUT, FMT = 9996 )SNAME
      CALL ZPRCN1(NOUT, NC, SNAME, IORDER, TRANSA, TRANSB,
     $           M, N, K, ALPHA, LDA, LDB, BETA, LDC)
*
  130 CONTINUE
      RETURN
*
10003 FORMAT( ' ', A12,' COMPLETED THE ROW-MAJOR    COMPUTATIONAL ',
     $ 'TESTS (', I6, ' CALLS)', /' ******* BUT WITH MAXIMUM TEST ',
     $ 'RATIO ', F8.2, ' - SUSPECT *******' )
10002 FORMAT( ' ', A12,' COMPLETED THE COLUMN-MAJOR COMPUTATIONAL ',
     $ 'TESTS (', I6, ' CALLS)', /' ******* BUT WITH MAXIMUM TEST ',
     $ 'RATIO ', F8.2, ' - SUSPECT *******' )
10001 FORMAT( ' ', A12,' PASSED THE ROW-MAJOR    COMPUTATIONAL TESTS',
     $ ' (', I6, ' CALL', 'S)' )
10000 FORMAT( ' ', A12,' PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS',
     $ ' (', I6, ' CALL', 'S)' )
 9998 FORMAT(' ******* FATAL ERROR - PARAMETER NUMBER ', I2, ' WAS CH',
     $      'ANGED INCORRECTLY *******' )
 9996 FORMAT( ' ******* ', A12,' FAILED ON CALL NUMBER:' )
 9995 FORMAT( 1X, I6, ': ', A12,'(''', A1, ''',''', A1, ''',',
     $     3( I3, ',' ), '(', F4.1, ',', F4.1, '), A,', I3, ', B,', I3,
     $     ',(', F4.1, ',', F4.1, '), C,', I3, ').' )
 9994 FORMAT(' ******* FATAL ERROR - ERROR-EXIT TAKEN ON VALID CALL *',
     $      '******' )
*
*     End of ZCHK1.
*
      END
*
      SUBROUTINE ZPRCN1(NOUT, NC, SNAME, IORDER, TRANSA, TRANSB, M, N,
     $                 K, ALPHA, LDA, LDB, BETA, LDC)
      INTEGER          NOUT, NC, IORDER, M, N, K, LDA, LDB, LDC
      DOUBLE COMPLEX   ALPHA, BETA
      CHARACTER*1      TRANSA, TRANSB
      CHARACTER*12     SNAME
      CHARACTER*14     CRC, CTA,CTB

      IF (TRANSA.EQ.'N')THEN
         CTA = '  CblasNoTrans'
      ELSE IF (TRANSA.EQ.'T')THEN
         CTA = '    CblasTrans'
      ELSE
         CTA = 'CblasConjTrans'
      END IF
      IF (TRANSB.EQ.'N')THEN
         CTB = '  CblasNoTrans'
      ELSE IF (TRANSB.EQ.'T')THEN
         CTB = '    CblasTrans'
      ELSE
         CTB = 'CblasConjTrans'
      END IF
      IF (IORDER.EQ.1)THEN
         CRC = ' CblasRowMajor'
      ELSE
         CRC = ' CblasColMajor'
      END IF
      WRITE(NOUT, FMT = 9995)NC,SNAME,CRC, CTA,CTB
      WRITE(NOUT, FMT = 9994)M, N, K, ALPHA, LDA, LDB, BETA, LDC

 9995 FORMAT( 1X, I6, ': ', A12,'(', A14, ',', A14, ',', A14, ',')
 9994 FORMAT( 10X, 3( I3, ',' ) ,' (', F4.1,',',F4.1,') , A,',
     $ I3, ', B,', I3, ', (', F4.1,',',F4.1,') , C,', I3, ').' )
      END
*
      SUBROUTINE ZCHK2( SNAME, EPS, THRESH, NOUT, NTRA, TRACE, REWI,
     $                  FATAL, NIDIM, IDIM, NALF, ALF, NBET, BET, NMAX,
     $                  A, AA, AS, B, BB, BS, C, CC, CS, CT, G,
     $                  IORDER )
*
*  Tests ZHEMM and ZSYMM.
*
*  Auxiliary routine for test program for Level 3 Blas.
*
*  -- Written on 8-February-1989.
*     Jack Dongarra, Argonne National Laboratory.
*     Iain Duff, AERE Harwell.
*     Jeremy Du Croz, Numerical Algorithms Group Ltd.
*     Sven Hammarling, Numerical Algorithms Group Ltd.
*
*     .. Parameters ..
      COMPLEX*16         ZERO
      PARAMETER          ( ZERO = ( 0.0D0, 0.0D0 ) )
      DOUBLE PRECISION   RZERO
      PARAMETER          ( RZERO = 0.0D0 )
*     .. Scalar Arguments ..
      DOUBLE PRECISION   EPS, THRESH
      INTEGER            NALF, NBET, NIDIM, NMAX, NOUT, NTRA, IORDER
      LOGICAL            FATAL, REWI, TRACE
      CHARACTER*12       SNAME
*     .. Array Arguments ..
      COMPLEX*16         A( NMAX, NMAX ), AA( NMAX*NMAX ), ALF( NALF ),
     $                   AS( NMAX*NMAX ), B( NMAX, NMAX ),
     $                   BB( NMAX*NMAX ), BET( NBET ), BS( NMAX*NMAX ),
     $                   C( NMAX, NMAX ), CC( NMAX*NMAX ),
     $                   CS( NMAX*NMAX ), CT( NMAX )
      DOUBLE PRECISION   G( NMAX )
      INTEGER            IDIM( NIDIM )
*     .. Local Scalars ..
      COMPLEX*16         ALPHA, ALS, BETA, BLS
      DOUBLE PRECISION   ERR, ERRMAX
      INTEGER            I, IA, IB, ICS, ICU, IM, IN, LAA, LBB, LCC,
     $                   LDA, LDAS, LDB, LDBS, LDC, LDCS, M, MS, N, NA,
     $                   NARGS, NC, NS
      LOGICAL            CONJ, LEFT, NULL, RESET, SAME
      CHARACTER*1        SIDE, SIDES, UPLO, UPLOS
      CHARACTER*2        ICHS, ICHU
*     .. Local Arrays ..
      LOGICAL            ISAME( 13 )
*     .. External Functions ..
      LOGICAL            LZE, LZERES
      EXTERNAL           LZE, LZERES
*     .. External Subroutines ..
      EXTERNAL           CZHEMM, ZMAKE, ZMMCH, CZSYMM
*     .. Intrinsic Functions ..
      INTRINSIC          MAX
*     .. Scalars in Common ..
      INTEGER            INFOT, NOUTC
      LOGICAL            LERR, OK
*     .. Common blocks ..
      COMMON             /INFOC/INFOT, NOUTC, OK, LERR
*     .. Data statements ..
      DATA               ICHS/'LR'/, ICHU/'UL'/
*     .. Executable Statements ..
      CONJ = SNAME( 8: 9 ).EQ.'he'
*
      NARGS = 12
      NC = 0
      RESET = .TRUE.
      ERRMAX = RZERO
*
      DO 100 IM = 1, NIDIM
         M = IDIM( IM )
*
         DO 90 IN = 1, NIDIM
            N = IDIM( IN )
*           Set LDC to 1 more than minimum value if room.
            LDC = M
            IF( LDC.LT.NMAX )
     $         LDC = LDC + 1
*           Skip tests if not enough room.
            IF( LDC.GT.NMAX )
     $         GO TO 90
            LCC = LDC*N
            NULL = N.LE.0.OR.M.LE.0
*           Set LDB to 1 more than minimum value if room.
            LDB = M
            IF( LDB.LT.NMAX )
     $         LDB = LDB + 1
*           Skip tests if not enough room.
            IF( LDB.GT.NMAX )
     $         GO TO 90
            LBB = LDB*N
*
*           Generate the matrix B.
*
            CALL ZMAKE( 'ge', ' ', ' ', M, N, B, NMAX, BB, LDB, RESET,
     $                  ZERO )
*
            DO 80 ICS = 1, 2
               SIDE = ICHS( ICS: ICS )
               LEFT = SIDE.EQ.'L'
*
               IF( LEFT )THEN
                  NA = M
               ELSE
                  NA = N
               END IF
*              Set LDA to 1 more than minimum value if room.
               LDA = NA
               IF( LDA.LT.NMAX )
     $            LDA = LDA + 1
*              Skip tests if not enough room.
               IF( LDA.GT.NMAX )
     $            GO TO 80
               LAA = LDA*NA
*
               DO 70 ICU = 1, 2
                  UPLO = ICHU( ICU: ICU )
*
*                 Generate the hermitian or symmetric matrix A.
*
                  CALL ZMAKE(SNAME( 8: 9 ), UPLO, ' ', NA, NA, A, NMAX,
     $                        AA, LDA, RESET, ZERO )
*
                  DO 60 IA = 1, NALF
                     ALPHA = ALF( IA )
*
                     DO 50 IB = 1, NBET
                        BETA = BET( IB )
*
*                       Generate the matrix C.
*
                        CALL ZMAKE( 'ge', ' ', ' ', M, N, C, NMAX, CC,
     $                              LDC, RESET, ZERO )
*
                        NC = NC + 1
*
*                       Save every datum before calling the
*                       subroutine.
*
                        SIDES = SIDE
                        UPLOS = UPLO
                        MS = M
                        NS = N
                        ALS = ALPHA
                        DO 10 I = 1, LAA
                           AS( I ) = AA( I )
   10                   CONTINUE
                        LDAS = LDA
                        DO 20 I = 1, LBB
                           BS( I ) = BB( I )
   20                   CONTINUE
                        LDBS = LDB
                        BLS = BETA
                        DO 30 I = 1, LCC
                           CS( I ) = CC( I )
   30                   CONTINUE
                        LDCS = LDC
*
*                       Call the subroutine.
*
                        IF( TRACE )
     $                      CALL ZPRCN2(NTRA, NC, SNAME, IORDER,
     $                      SIDE, UPLO, M, N, ALPHA, LDA, LDB,
     $                      BETA, LDC)
                        IF( REWI )
     $                     REWIND NTRA
                        IF( CONJ )THEN
                           CALL CZHEMM( IORDER, SIDE, UPLO, M, N,
     $                                 ALPHA, AA, LDA, BB, LDB, BETA,
     $                                 CC, LDC )
                        ELSE
                           CALL CZSYMM( IORDER, SIDE, UPLO, M, N,
     $                                 ALPHA, AA, LDA, BB, LDB, BETA,
     $                                 CC, LDC )
                        END IF
*
*                       Check if error-exit was taken incorrectly.
*
                        IF( .NOT.OK )THEN
                           WRITE( NOUT, FMT = 9994 )
                           FATAL = .TRUE.
                           GO TO 110
                        END IF
*
*                       See what data changed inside subroutines.
*
                        ISAME( 1 ) = SIDES.EQ.SIDE
                        ISAME( 2 ) = UPLOS.EQ.UPLO
                        ISAME( 3 ) = MS.EQ.M
                        ISAME( 4 ) = NS.EQ.N
                        ISAME( 5 ) = ALS.EQ.ALPHA
                        ISAME( 6 ) = LZE( AS, AA, LAA )
                        ISAME( 7 ) = LDAS.EQ.LDA
                        ISAME( 8 ) = LZE( BS, BB, LBB )
                        ISAME( 9 ) = LDBS.EQ.LDB
                        ISAME( 10 ) = BLS.EQ.BETA
                        IF( NULL )THEN
                           ISAME( 11 ) = LZE( CS, CC, LCC )
                        ELSE
                           ISAME( 11 ) = LZERES( 'ge', ' ', M, N, CS,
     $                                   CC, LDC )
                        END IF
                        ISAME( 12 ) = LDCS.EQ.LDC
*
*                       If data was incorrectly changed, report and
*                       return.
*
                        SAME = .TRUE.
                        DO 40 I = 1, NARGS
                           SAME = SAME.AND.ISAME( I )
                           IF( .NOT.ISAME( I ) )
     $                        WRITE( NOUT, FMT = 9998 )I
   40                   CONTINUE
                        IF( .NOT.SAME )THEN
                           FATAL = .TRUE.
                           GO TO 110
                        END IF
*
                        IF( .NOT.NULL )THEN
*
*                          Check the result.
*
                           IF( LEFT )THEN
                              CALL ZMMCH( 'N', 'N', M, N, M, ALPHA, A,
     $                                    NMAX, B, NMAX, BETA, C, NMAX,
     $                                    CT, G, CC, LDC, EPS, ERR,
     $                                    FATAL, NOUT, .TRUE. )
                           ELSE
                              CALL ZMMCH( 'N', 'N', M, N, N, ALPHA, B,
     $                                    NMAX, A, NMAX, BETA, C, NMAX,
     $                                    CT, G, CC, LDC, EPS, ERR,
     $                                    FATAL, NOUT, .TRUE. )
                           END IF
                           ERRMAX = MAX( ERRMAX, ERR )
*                          If got really bad answer, report and
*                          return.
                           IF( FATAL )
     $                        GO TO 110
                        END IF
*
   50                CONTINUE
*
   60             CONTINUE
*
   70          CONTINUE
*
   80       CONTINUE
*
   90    CONTINUE
*
  100 CONTINUE
*
*     Report result.
*
      IF( ERRMAX.LT.THRESH )THEN
         IF ( IORDER.EQ.0) WRITE( NOUT, FMT = 10000 )SNAME, NC
         IF ( IORDER.EQ.1) WRITE( NOUT, FMT = 10001 )SNAME, NC
      ELSE
         IF ( IORDER.EQ.0) WRITE( NOUT, FMT = 10002 )SNAME, NC, ERRMAX
         IF ( IORDER.EQ.1) WRITE( NOUT, FMT = 10003 )SNAME, NC, ERRMAX
      END IF
      GO TO 120
*
  110 CONTINUE
      WRITE( NOUT, FMT = 9996 )SNAME
      CALL ZPRCN2(NOUT, NC, SNAME, IORDER, SIDE, UPLO, M, N, ALPHA, LDA,
     $           LDB, BETA, LDC)
*
  120 CONTINUE
      RETURN
*
10003 FORMAT( ' ', A12,' COMPLETED THE ROW-MAJOR    COMPUTATIONAL ',
     $ 'TESTS (', I6, ' CALLS)', /' ******* BUT WITH MAXIMUM TEST ',
     $ 'RATIO ', F8.2, ' - SUSPECT *******' )
10002 FORMAT( ' ', A12,' COMPLETED THE COLUMN-MAJOR COMPUTATIONAL ',
     $ 'TESTS (', I6, ' CALLS)', /' ******* BUT WITH MAXIMUM TEST ',
     $ 'RATIO ', F8.2, ' - SUSPECT *******' )
10001 FORMAT( ' ', A12,' PASSED THE ROW-MAJOR    COMPUTATIONAL TESTS',
     $ ' (', I6, ' CALL', 'S)' )
10000 FORMAT( ' ', A12,' PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS',
     $ ' (', I6, ' CALL', 'S)' )
 9998 FORMAT(' ******* FATAL ERROR - PARAMETER NUMBER ', I2, ' WAS CH',
     $      'ANGED INCORRECTLY *******' )
 9996 FORMAT( ' ******* ', A12,' FAILED ON CALL NUMBER:' )
 9995 FORMAT(1X, I6, ': ', A12,'(', 2( '''', A1, ''',' ), 2( I3, ',' ),
     $      '(', F4.1, ',', F4.1, '), A,', I3, ', B,', I3, ',(', F4.1,
     $      ',', F4.1, '), C,', I3, ')    .' )
 9994 FORMAT(' ******* FATAL ERROR - ERROR-EXIT TAKEN ON VALID CALL *',
     $      '******' )
*
*     End of ZCHK2.
*
      END
*
      SUBROUTINE ZPRCN2(NOUT, NC, SNAME, IORDER, SIDE, UPLO, M, N,
     $                 ALPHA, LDA, LDB, BETA, LDC)
      INTEGER          NOUT, NC, IORDER, M, N, LDA, LDB, LDC
      DOUBLE COMPLEX   ALPHA, BETA
      CHARACTER*1      SIDE, UPLO
      CHARACTER*12     SNAME
      CHARACTER*14     CRC, CS,CU

      IF (SIDE.EQ.'L')THEN
         CS =  '     CblasLeft'
      ELSE
         CS =  '    CblasRight'
      END IF
      IF (UPLO.EQ.'U')THEN
         CU =  '    CblasUpper'
      ELSE
         CU =  '    CblasLower'
      END IF
      IF (IORDER.EQ.1)THEN
         CRC = ' CblasRowMajor'
      ELSE
         CRC = ' CblasColMajor'
      END IF
      WRITE(NOUT, FMT = 9995)NC,SNAME,CRC, CS,CU
      WRITE(NOUT, FMT = 9994)M, N, ALPHA, LDA, LDB, BETA, LDC

 9995 FORMAT( 1X, I6, ': ', A12,'(', A14, ',', A14, ',', A14, ',')
 9994 FORMAT( 10X, 2( I3, ',' ),' (',F4.1,',',F4.1, '), A,', I3,
     $ ', B,', I3, ', (',F4.1,',',F4.1, '), ', 'C,', I3, ').' )
      END
*
      SUBROUTINE ZCHK3( SNAME, EPS, THRESH, NOUT, NTRA, TRACE, REWI,
     $                  FATAL, NIDIM, IDIM, NALF, ALF, NMAX, A, AA, AS,
     $                  B, BB, BS, CT, G, C, IORDER )
*
*  Tests ZTRMM and ZTRSM.
*
*  Auxiliary routine for test program for Level 3 Blas.
*
*  -- Written on 8-February-1989.
*     Jack Dongarra, Argonne National Laboratory.
*     Iain Duff, AERE Harwell.
*     Jeremy Du Croz, Numerical Algorithms Group Ltd.
*     Sven Hammarling, Numerical Algorithms Group Ltd.
*
*     .. Parameters ..
      COMPLEX*16    ZERO, ONE
      PARAMETER     ( ZERO = ( 0.0D0, 0.0D0 ), ONE = ( 1.0D0, 0.0D0 ) )
      DOUBLE PRECISION  RZERO
      PARAMETER     ( RZERO = 0.0D0 )
*     .. Scalar Arguments ..
      DOUBLE PRECISION   EPS, THRESH
      INTEGER            NALF, NIDIM, NMAX, NOUT, NTRA, IORDER
      LOGICAL            FATAL, REWI, TRACE
      CHARACTER*12       SNAME
*     .. Array Arguments ..
      COMPLEX*16         A( NMAX, NMAX ), AA( NMAX*NMAX ), ALF( NALF ),
     $                   AS( NMAX*NMAX ), B( NMAX, NMAX ),
     $                   BB( NMAX*NMAX ), BS( NMAX*NMAX ),
     $                   C( NMAX, NMAX ), CT( NMAX )
      DOUBLE PRECISION   G( NMAX )
      INTEGER            IDIM( NIDIM )
*     .. Local Scalars ..
      COMPLEX*16         ALPHA, ALS
      DOUBLE PRECISION   ERR, ERRMAX
      INTEGER           I, IA, ICD, ICS, ICT, ICU, IM, IN, J, LAA, LBB,
     $                   LDA, LDAS, LDB, LDBS, M, MS, N, NA, NARGS, NC,
     $                   NS
      LOGICAL            LEFT, NULL, RESET, SAME
      CHARACTER*1       DIAG, DIAGS, SIDE, SIDES, TRANAS, TRANSA, UPLO,
     $                   UPLOS
      CHARACTER*2        ICHD, ICHS, ICHU
      CHARACTER*3        ICHT
*     .. Local Arrays ..
      LOGICAL            ISAME( 13 )
*     .. External Functions ..
      LOGICAL            LZE, LZERES
      EXTERNAL           LZE, LZERES
*     .. External Subroutines ..
      EXTERNAL           ZMAKE, ZMMCH, CZTRMM, CZTRSM
*     .. Intrinsic Functions ..
      INTRINSIC          MAX
*     .. Scalars in Common ..
      INTEGER            INFOT, NOUTC
      LOGICAL            LERR, OK
*     .. Common blocks ..
      COMMON             /INFOC/INFOT, NOUTC, OK, LERR
*     .. Data statements ..
      DATA              ICHU/'UL'/, ICHT/'NTC'/, ICHD/'UN'/, ICHS/'LR'/
*     .. Executable Statements ..
*
      NARGS = 11
      NC = 0
      RESET = .TRUE.
      ERRMAX = RZERO
*     Set up zero matrix for ZMMCH.
      DO 20 J = 1, NMAX
         DO 10 I = 1, NMAX
            C( I, J ) = ZERO
   10    CONTINUE
   20 CONTINUE
*
      DO 140 IM = 1, NIDIM
         M = IDIM( IM )
*
         DO 130 IN = 1, NIDIM
            N = IDIM( IN )
*           Set LDB to 1 more than minimum value if room.
            LDB = M
            IF( LDB.LT.NMAX )
     $         LDB = LDB + 1
*           Skip tests if not enough room.
            IF( LDB.GT.NMAX )
     $         GO TO 130
            LBB = LDB*N
            NULL = M.LE.0.OR.N.LE.0
*
            DO 120 ICS = 1, 2
               SIDE = ICHS( ICS: ICS )
               LEFT = SIDE.EQ.'L'
               IF( LEFT )THEN
                  NA = M
               ELSE
                  NA = N
               END IF
*              Set LDA to 1 more than minimum value if room.
               LDA = NA
               IF( LDA.LT.NMAX )
     $            LDA = LDA + 1
*              Skip tests if not enough room.
               IF( LDA.GT.NMAX )
     $            GO TO 130
               LAA = LDA*NA
*
               DO 110 ICU = 1, 2
                  UPLO = ICHU( ICU: ICU )
*
                  DO 100 ICT = 1, 3
                     TRANSA = ICHT( ICT: ICT )
*
                     DO 90 ICD = 1, 2
                        DIAG = ICHD( ICD: ICD )
*
                        DO 80 IA = 1, NALF
                           ALPHA = ALF( IA )
*
*                          Generate the matrix A.
*
                           CALL ZMAKE( 'tr', UPLO, DIAG, NA, NA, A,
     $                                 NMAX, AA, LDA, RESET, ZERO )
*
*                          Generate the matrix B.
*
                           CALL ZMAKE( 'ge', ' ', ' ', M, N, B, NMAX,
     $                                 BB, LDB, RESET, ZERO )
*
                           NC = NC + 1
*
*                          Save every datum before calling the
*                          subroutine.
*
                           SIDES = SIDE
                           UPLOS = UPLO
                           TRANAS = TRANSA
                           DIAGS = DIAG
                           MS = M
                           NS = N
                           ALS = ALPHA
                           DO 30 I = 1, LAA
                              AS( I ) = AA( I )
   30                      CONTINUE
                           LDAS = LDA
                           DO 40 I = 1, LBB
                              BS( I ) = BB( I )
   40                      CONTINUE
                           LDBS = LDB
*
*                          Call the subroutine.
*
                           IF( SNAME( 10: 11 ).EQ.'mm' )THEN
                              IF( TRACE )
     $                           CALL ZPRCN3( NTRA, NC, SNAME, IORDER,
     $                           SIDE, UPLO, TRANSA, DIAG, M, N, ALPHA,
     $                           LDA, LDB)
                              IF( REWI )
     $                           REWIND NTRA
                              CALL CZTRMM(IORDER, SIDE, UPLO, TRANSA,
     $                                    DIAG, M, N, ALPHA, AA, LDA,
     $                                    BB, LDB )
                           ELSE IF( SNAME( 10: 11 ).EQ.'sm' )THEN
                              IF( TRACE )
     $                           CALL ZPRCN3( NTRA, NC, SNAME, IORDER,
     $                           SIDE, UPLO, TRANSA, DIAG, M, N, ALPHA,
     $                           LDA, LDB)
                              IF( REWI )
     $                           REWIND NTRA
                              CALL CZTRSM(IORDER, SIDE, UPLO, TRANSA,
     $                                   DIAG, M, N, ALPHA, AA, LDA,
     $                                   BB, LDB )
                           END IF
*
*                          Check if error-exit was taken incorrectly.
*
                           IF( .NOT.OK )THEN
                              WRITE( NOUT, FMT = 9994 )
                              FATAL = .TRUE.
                              GO TO 150
                           END IF
*
*                          See what data changed inside subroutines.
*
                           ISAME( 1 ) = SIDES.EQ.SIDE
                           ISAME( 2 ) = UPLOS.EQ.UPLO
                           ISAME( 3 ) = TRANAS.EQ.TRANSA
                           ISAME( 4 ) = DIAGS.EQ.DIAG
                           ISAME( 5 ) = MS.EQ.M
                           ISAME( 6 ) = NS.EQ.N
                           ISAME( 7 ) = ALS.EQ.ALPHA
                           ISAME( 8 ) = LZE( AS, AA, LAA )
                           ISAME( 9 ) = LDAS.EQ.LDA
                           IF( NULL )THEN
                              ISAME( 10 ) = LZE( BS, BB, LBB )
                           ELSE
                             ISAME( 10 ) = LZERES( 'ge', ' ', M, N, BS,
     $                                      BB, LDB )
                           END IF
                           ISAME( 11 ) = LDBS.EQ.LDB
*
*                          If data was incorrectly changed, report and
*                          return.
*
                           SAME = .TRUE.
                           DO 50 I = 1, NARGS
                              SAME = SAME.AND.ISAME( I )
                              IF( .NOT.ISAME( I ) )
     $                           WRITE( NOUT, FMT = 9998 )I
   50                      CONTINUE
                           IF( .NOT.SAME )THEN
                              FATAL = .TRUE.
                              GO TO 150
                           END IF
*
                           IF( .NOT.NULL )THEN
                              IF( SNAME( 10: 11 ).EQ.'mm' )THEN
*
*                                Check the result.
*
                                 IF( LEFT )THEN
                                   CALL ZMMCH( TRANSA, 'N', M, N, M,
     $                                         ALPHA, A, NMAX, B, NMAX,
     $                                         ZERO, C, NMAX, CT, G,
     $                                         BB, LDB, EPS, ERR,
     $                                         FATAL, NOUT, .TRUE. )
                                 ELSE
                                   CALL ZMMCH( 'N', TRANSA, M, N, N,
     $                                         ALPHA, B, NMAX, A, NMAX,
     $                                         ZERO, C, NMAX, CT, G,
     $                                         BB, LDB, EPS, ERR,
     $                                         FATAL, NOUT, .TRUE. )
                                 END IF
                              ELSE IF( SNAME( 10: 11 ).EQ.'sm' )THEN
*
*                                Compute approximation to original
*                                matrix.
*
                                 DO 70 J = 1, N
                                    DO 60 I = 1, M
                                       C( I, J ) = BB( I + ( J - 1 )*
     $                                             LDB )
                                       BB( I + ( J - 1 )*LDB ) = ALPHA*
     $                                    B( I, J )
   60                               CONTINUE
   70                            CONTINUE
*
                                 IF( LEFT )THEN
                                    CALL ZMMCH( TRANSA, 'N', M, N, M,
     $                                          ONE, A, NMAX, C, NMAX,
     $                                          ZERO, B, NMAX, CT, G,
     $                                          BB, LDB, EPS, ERR,
     $                                          FATAL, NOUT, .FALSE. )
                                 ELSE
                                    CALL ZMMCH( 'N', TRANSA, M, N, N,
     $                                          ONE, C, NMAX, A, NMAX,
     $                                          ZERO, B, NMAX, CT, G,
     $                                          BB, LDB, EPS, ERR,
     $                                          FATAL, NOUT, .FALSE. )
                                 END IF
                              END IF
                              ERRMAX = MAX( ERRMAX, ERR )
*                             If got really bad answer, report and
*                             return.
                              IF( FATAL )
     $                           GO TO 150
                           END IF
*
   80                   CONTINUE
*
   90                CONTINUE
*
  100             CONTINUE
*
  110          CONTINUE
*
  120       CONTINUE
*
  130    CONTINUE
*
  140 CONTINUE
*
*     Report result.
*
      IF( ERRMAX.LT.THRESH )THEN
         IF ( IORDER.EQ.0) WRITE( NOUT, FMT = 10000 )SNAME, NC
         IF ( IORDER.EQ.1) WRITE( NOUT, FMT = 10001 )SNAME, NC
      ELSE
         IF ( IORDER.EQ.0) WRITE( NOUT, FMT = 10002 )SNAME, NC, ERRMAX
         IF ( IORDER.EQ.1) WRITE( NOUT, FMT = 10003 )SNAME, NC, ERRMAX
      END IF
      GO TO 160
*
  150 CONTINUE
      WRITE( NOUT, FMT = 9996 )SNAME
      IF( TRACE )
     $   CALL ZPRCN3( NTRA, NC, SNAME, IORDER, SIDE, UPLO, TRANSA, DIAG,
     $         M, N, ALPHA, LDA, LDB)
*
  160 CONTINUE
      RETURN
*
10003 FORMAT( ' ', A12,' COMPLETED THE ROW-MAJOR    COMPUTATIONAL ',
     $ 'TESTS (', I6, ' CALLS)', /' ******* BUT WITH MAXIMUM TEST ',
     $ 'RATIO ', F8.2, ' - SUSPECT *******' )
10002 FORMAT( ' ', A12,' COMPLETED THE COLUMN-MAJOR COMPUTATIONAL ',
     $ 'TESTS (', I6, ' CALLS)', /' ******* BUT WITH MAXIMUM TEST ',
     $ 'RATIO ', F8.2, ' - SUSPECT *******' )
10001 FORMAT( ' ', A12,' PASSED THE ROW-MAJOR    COMPUTATIONAL TESTS',
     $ ' (', I6, ' CALL', 'S)' )
10000 FORMAT( ' ', A12,' PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS',
     $ ' (', I6, ' CALL', 'S)' )
 9998 FORMAT(' ******* FATAL ERROR - PARAMETER NUMBER ', I2, ' WAS CH',
     $      'ANGED INCORRECTLY *******' )
 9996 FORMAT(' ******* ', A12,' FAILED ON CALL NUMBER:' )
 9995 FORMAT(1X, I6, ': ', A12,'(', 4( '''', A1, ''',' ), 2( I3, ',' ),
     $     '(', F4.1, ',', F4.1, '), A,', I3, ', B,', I3, ')         ',
     $      '      .' )
 9994 FORMAT(' ******* FATAL ERROR - ERROR-EXIT TAKEN ON VALID CALL *',
     $      '******' )
*
*     End of ZCHK3.
*
      END
*
      SUBROUTINE ZPRCN3(NOUT, NC, SNAME, IORDER, SIDE, UPLO, TRANSA,
     $                 DIAG, M, N, ALPHA, LDA, LDB)
      INTEGER          NOUT, NC, IORDER, M, N, LDA, LDB
      DOUBLE COMPLEX   ALPHA
      CHARACTER*1      SIDE, UPLO, TRANSA, DIAG
      CHARACTER*12     SNAME
      CHARACTER*14     CRC, CS, CU, CA, CD

      IF (SIDE.EQ.'L')THEN
         CS =  '     CblasLeft'
      ELSE
         CS =  '    CblasRight'
      END IF
      IF (UPLO.EQ.'U')THEN
         CU =  '    CblasUpper'
      ELSE
         CU =  '    CblasLower'
      END IF
      IF (TRANSA.EQ.'N')THEN
         CA =  '  CblasNoTrans'
      ELSE IF (TRANSA.EQ.'T')THEN
         CA =  '    CblasTrans'
      ELSE
         CA =  'CblasConjTrans'
      END IF
      IF (DIAG.EQ.'N')THEN
         CD =  '  CblasNonUnit'
      ELSE
         CD =  '     CblasUnit'
      END IF
      IF (IORDER.EQ.1)THEN
         CRC = ' CblasRowMajor'
      ELSE
         CRC = ' CblasColMajor'
      END IF
      WRITE(NOUT, FMT = 9995)NC,SNAME,CRC, CS,CU
      WRITE(NOUT, FMT = 9994)CA, CD, M, N, ALPHA, LDA, LDB

 9995 FORMAT( 1X, I6, ': ', A12,'(', A14, ',', A14, ',', A14, ',')
 9994 FORMAT( 10X, 2( A14, ',') , 2( I3, ',' ), ' (', F4.1, ',',
     $    F4.1, '), A,', I3, ', B,', I3, ').' )
      END
*
      SUBROUTINE ZCHK4( SNAME, EPS, THRESH, NOUT, NTRA, TRACE, REWI,
     $                  FATAL, NIDIM, IDIM, NALF, ALF, NBET, BET, NMAX,
     $                  A, AA, AS, B, BB, BS, C, CC, CS, CT, G,
     $                  IORDER )
*
*  Tests ZHERK and ZSYRK.
*
*  Auxiliary routine for test program for Level 3 Blas.
*
*  -- Written on 8-February-1989.
*     Jack Dongarra, Argonne National Laboratory.
*     Iain Duff, AERE Harwell.
*     Jeremy Du Croz, Numerical Algorithms Group Ltd.
*     Sven Hammarling, Numerical Algorithms Group Ltd.
*
*     .. Parameters ..
      COMPLEX*16         ZERO
      PARAMETER          ( ZERO = ( 0.0D0, 0.0D0 ) )
      DOUBLE PRECISION   RONE, RZERO
      PARAMETER          ( RONE = 1.0D0, RZERO = 0.0D0 )
*     .. Scalar Arguments ..
      DOUBLE PRECISION   EPS, THRESH
      INTEGER            NALF, NBET, NIDIM, NMAX, NOUT, NTRA, IORDER
      LOGICAL            FATAL, REWI, TRACE
      CHARACTER*12       SNAME
*     .. Array Arguments ..
      COMPLEX*16         A( NMAX, NMAX ), AA( NMAX*NMAX ), ALF( NALF ),
     $                   AS( NMAX*NMAX ), B( NMAX, NMAX ),
     $                   BB( NMAX*NMAX ), BET( NBET ), BS( NMAX*NMAX ),
     $                   C( NMAX, NMAX ), CC( NMAX*NMAX ),
     $                   CS( NMAX*NMAX ), CT( NMAX )
      DOUBLE PRECISION   G( NMAX )
      INTEGER            IDIM( NIDIM )
*     .. Local Scalars ..
      COMPLEX*16         ALPHA, ALS, BETA, BETS
      DOUBLE PRECISION   ERR, ERRMAX, RALPHA, RALS, RBETA, RBETS
      INTEGER            I, IA, IB, ICT, ICU, IK, IN, J, JC, JJ, K, KS,
     $                   LAA, LCC, LDA, LDAS, LDC, LDCS, LJ, MA, N, NA,
     $                   NARGS, NC, NS
      LOGICAL            CONJ, NULL, RESET, SAME, TRAN, UPPER
      CHARACTER*1        TRANS, TRANSS, TRANST, UPLO, UPLOS
      CHARACTER*2        ICHT, ICHU
*     .. Local Arrays ..
      LOGICAL            ISAME( 13 )
*     .. External Functions ..
      LOGICAL            LZE, LZERES
      EXTERNAL           LZE, LZERES
*     .. External Subroutines ..
      EXTERNAL           CZHERK, ZMAKE, ZMMCH, CZSYRK
*     .. Intrinsic Functions ..
      INTRINSIC          DCMPLX, MAX, DBLE
*     .. Scalars in Common ..
      INTEGER            INFOT, NOUTC
      LOGICAL            LERR, OK
*     .. Common blocks ..
      COMMON             /INFOC/INFOT, NOUTC, OK, LERR
*     .. Data statements ..
      DATA               ICHT/'NC'/, ICHU/'UL'/
*     .. Executable Statements ..
      CONJ = SNAME( 8: 9 ).EQ.'he'
*
      NARGS = 10
      NC = 0
      RESET = .TRUE.
      ERRMAX = RZERO
*
      DO 100 IN = 1, NIDIM
         N = IDIM( IN )
*        Set LDC to 1 more than minimum value if room.
         LDC = N
         IF( LDC.LT.NMAX )
     $      LDC = LDC + 1
*        Skip tests if not enough room.
         IF( LDC.GT.NMAX )
     $      GO TO 100
         LCC = LDC*N
*
         DO 90 IK = 1, NIDIM
            K = IDIM( IK )
*
            DO 80 ICT = 1, 2
               TRANS = ICHT( ICT: ICT )
               TRAN = TRANS.EQ.'C'
               IF( TRAN.AND..NOT.CONJ )
     $            TRANS = 'T'
               IF( TRAN )THEN
                  MA = K
                  NA = N
               ELSE
                  MA = N
                  NA = K
               END IF
*              Set LDA to 1 more than minimum value if room.
               LDA = MA
               IF( LDA.LT.NMAX )
     $            LDA = LDA + 1
*              Skip tests if not enough room.
               IF( LDA.GT.NMAX )
     $            GO TO 80
               LAA = LDA*NA
*
*              Generate the matrix A.
*
               CALL ZMAKE( 'ge', ' ', ' ', MA, NA, A, NMAX, AA, LDA,
     $                     RESET, ZERO )
*
               DO 70 ICU = 1, 2
                  UPLO = ICHU( ICU: ICU )
                  UPPER = UPLO.EQ.'U'
*
                  DO 60 IA = 1, NALF
                     ALPHA = ALF( IA )
                     IF( CONJ )THEN
                        RALPHA = DBLE( ALPHA )
                        ALPHA = DCMPLX( RALPHA, RZERO )
                     END IF
*
                     DO 50 IB = 1, NBET
                        BETA = BET( IB )
                        IF( CONJ )THEN
                           RBETA = DBLE( BETA )
                           BETA = DCMPLX( RBETA, RZERO )
                        END IF
                        NULL = N.LE.0
                        IF( CONJ )
     $                     NULL = NULL.OR.( ( K.LE.0.OR.RALPHA.EQ.
     $                            RZERO ).AND.RBETA.EQ.RONE )
*
*                       Generate the matrix C.
*
                        CALL ZMAKE( SNAME( 8: 9 ), UPLO, ' ', N, N, C,
     $                              NMAX, CC, LDC, RESET, ZERO )
*
                        NC = NC + 1
*
*                       Save every datum before calling the subroutine.
*
                        UPLOS = UPLO
                        TRANSS = TRANS
                        NS = N
                        KS = K
                        IF( CONJ )THEN
                           RALS = RALPHA
                        ELSE
                           ALS = ALPHA
                        END IF
                        DO 10 I = 1, LAA
                           AS( I ) = AA( I )
   10                   CONTINUE
                        LDAS = LDA
                        IF( CONJ )THEN
                           RBETS = RBETA
                        ELSE
                           BETS = BETA
                        END IF
                        DO 20 I = 1, LCC
                           CS( I ) = CC( I )
   20                   CONTINUE
                        LDCS = LDC
*
*                       Call the subroutine.
*
                        IF( CONJ )THEN
                           IF( TRACE )
     $                        CALL ZPRCN6( NTRA, NC, SNAME, IORDER,
     $                        UPLO, TRANS, N, K, RALPHA, LDA, RBETA,
     $                        LDC)
                           IF( REWI )
     $                        REWIND NTRA
                           CALL CZHERK( IORDER, UPLO, TRANS, N, K,
     $                                 RALPHA, AA, LDA, RBETA, CC,
     $                                 LDC )
                        ELSE
                           IF( TRACE )
     $                        CALL ZPRCN4( NTRA, NC, SNAME, IORDER,
     $                        UPLO, TRANS, N, K, ALPHA, LDA, BETA, LDC)
                           IF( REWI )
     $                        REWIND NTRA
                           CALL CZSYRK( IORDER, UPLO, TRANS, N, K,
     $                                 ALPHA, AA, LDA, BETA, CC, LDC )
                        END IF
*
*                       Check if error-exit was taken incorrectly.
*
                        IF( .NOT.OK )THEN
                           WRITE( NOUT, FMT = 9992 )
                           FATAL = .TRUE.
                           GO TO 120
                        END IF
*
*                       See what data changed inside subroutines.
*
                        ISAME( 1 ) = UPLOS.EQ.UPLO
                        ISAME( 2 ) = TRANSS.EQ.TRANS
                        ISAME( 3 ) = NS.EQ.N
                        ISAME( 4 ) = KS.EQ.K
                        IF( CONJ )THEN
                           ISAME( 5 ) = RALS.EQ.RALPHA
                        ELSE
                           ISAME( 5 ) = ALS.EQ.ALPHA
                        END IF
                        ISAME( 6 ) = LZE( AS, AA, LAA )
                        ISAME( 7 ) = LDAS.EQ.LDA
                        IF( CONJ )THEN
                           ISAME( 8 ) = RBETS.EQ.RBETA
                        ELSE
                           ISAME( 8 ) = BETS.EQ.BETA
                        END IF
                        IF( NULL )THEN
                           ISAME( 9 ) = LZE( CS, CC, LCC )
                        ELSE
                           ISAME( 9 ) = LZERES( SNAME( 8: 9 ), UPLO, N,
     $                                  N, CS, CC, LDC )
                        END IF
                        ISAME( 10 ) = LDCS.EQ.LDC
*
*                       If data was incorrectly changed, report and
*                       return.
*
                        SAME = .TRUE.
                        DO 30 I = 1, NARGS
                           SAME = SAME.AND.ISAME( I )
                           IF( .NOT.ISAME( I ) )
     $                        WRITE( NOUT, FMT = 9998 )I
   30                   CONTINUE
                        IF( .NOT.SAME )THEN
                           FATAL = .TRUE.
                           GO TO 120
                        END IF
*
                        IF( .NOT.NULL )THEN
*
*                          Check the result column by column.
*
                           IF( CONJ )THEN
                              TRANST = 'C'
                           ELSE
                              TRANST = 'T'
                           END IF
                           JC = 1
                           DO 40 J = 1, N
                              IF( UPPER )THEN
                                 JJ = 1
                                 LJ = J
                              ELSE
                                 JJ = J
                                 LJ = N - J + 1
                              END IF
                              IF( TRAN )THEN
                                 CALL ZMMCH( TRANST, 'N', LJ, 1, K,
     $                                       ALPHA, A( 1, JJ ), NMAX,
     $                                       A( 1, J ), NMAX, BETA,
     $                                       C( JJ, J ), NMAX, CT, G,
     $                                       CC( JC ), LDC, EPS, ERR,
     $                                       FATAL, NOUT, .TRUE. )
                              ELSE
                                 CALL ZMMCH( 'N', TRANST, LJ, 1, K,
     $                                       ALPHA, A( JJ, 1 ), NMAX,
     $                                       A( J, 1 ), NMAX, BETA,
     $                                       C( JJ, J ), NMAX, CT, G,
     $                                       CC( JC ), LDC, EPS, ERR,
     $                                       FATAL, NOUT, .TRUE. )
                              END IF
                              IF( UPPER )THEN
                                 JC = JC + LDC
                              ELSE
                                 JC = JC + LDC + 1
                              END IF
                              ERRMAX = MAX( ERRMAX, ERR )
*                             If got really bad answer, report and
*                             return.
                              IF( FATAL )
     $                           GO TO 110
   40                      CONTINUE
                        END IF
*
   50                CONTINUE
*
   60             CONTINUE
*
   70          CONTINUE
*
   80       CONTINUE
*
   90    CONTINUE
*
  100 CONTINUE
*
*     Report result.
*
      IF( ERRMAX.LT.THRESH )THEN
         IF ( IORDER.EQ.0) WRITE( NOUT, FMT = 10000 )SNAME, NC
         IF ( IORDER.EQ.1) WRITE( NOUT, FMT = 10001 )SNAME, NC
      ELSE
         IF ( IORDER.EQ.0) WRITE( NOUT, FMT = 10002 )SNAME, NC, ERRMAX
         IF ( IORDER.EQ.1) WRITE( NOUT, FMT = 10003 )SNAME, NC, ERRMAX
      END IF
      GO TO 130
*
  110 CONTINUE
      IF( N.GT.1 )
     $   WRITE( NOUT, FMT = 9995 )J
*
  120 CONTINUE
      WRITE( NOUT, FMT = 9996 )SNAME
      IF( CONJ )THEN
      CALL ZPRCN6( NOUT, NC, SNAME, IORDER, UPLO, TRANS, N, K, RALPHA,
     $   LDA, rBETA, LDC)
      ELSE
      CALL ZPRCN4( NOUT, NC, SNAME, IORDER, UPLO, TRANS, N, K, ALPHA,
     $   LDA, BETA, LDC)
      END IF
*
  130 CONTINUE
      RETURN
*
10003 FORMAT( ' ', A12,' COMPLETED THE ROW-MAJOR    COMPUTATIONAL ',
     $ 'TESTS (', I6, ' CALLS)', /' ******* BUT WITH MAXIMUM TEST ',
     $ 'RATIO ', F8.2, ' - SUSPECT *******' )
10002 FORMAT( ' ', A12,' COMPLETED THE COLUMN-MAJOR COMPUTATIONAL ',
     $ 'TESTS (', I6, ' CALLS)', /' ******* BUT WITH MAXIMUM TEST ',
     $ 'RATIO ', F8.2, ' - SUSPECT *******' )
10001 FORMAT( ' ', A12,' PASSED THE ROW-MAJOR    COMPUTATIONAL TESTS',
     $ ' (', I6, ' CALL', 'S)' )
10000 FORMAT( ' ', A12,' PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS',
     $ ' (', I6, ' CALL', 'S)' )
 9998 FORMAT(' ******* FATAL ERROR - PARAMETER NUMBER ', I2, ' WAS CH',
     $      'ANGED INCORRECTLY *******' )
 9996 FORMAT( ' ******* ', A12,' FAILED ON CALL NUMBER:' )
 9995 FORMAT( '      THESE ARE THE RESULTS FOR COLUMN ', I3 )
 9994 FORMAT(1X, I6, ': ', A12,'(', 2( '''', A1, ''',' ), 2( I3, ',' ),
     $     F4.1, ', A,', I3, ',', F4.1, ', C,', I3, ')               ',
     $      '          .' )
 9993 FORMAT(1X, I6, ': ', A12,'(', 2( '''', A1, ''',' ), 2( I3, ',' ),
     $      '(', F4.1, ',', F4.1, ') , A,', I3, ',(', F4.1, ',', F4.1,
     $      '), C,', I3, ')          .' )
 9992 FORMAT(' ******* FATAL ERROR - ERROR-EXIT TAKEN ON VALID CALL *',
     $      '******' )
*
*     End of CCHK4.
*
      END
*
      SUBROUTINE ZPRCN4(NOUT, NC, SNAME, IORDER, UPLO, TRANSA,
     $                 N, K, ALPHA, LDA, BETA, LDC)
      INTEGER          NOUT, NC, IORDER, N, K, LDA, LDC
      DOUBLE COMPLEX   ALPHA, BETA
      CHARACTER*1      UPLO, TRANSA
      CHARACTER*12     SNAME
      CHARACTER*14     CRC, CU, CA

      IF (UPLO.EQ.'U')THEN
         CU =  '    CblasUpper'
      ELSE
         CU =  '    CblasLower'
      END IF
      IF (TRANSA.EQ.'N')THEN
         CA =  '  CblasNoTrans'
      ELSE IF (TRANSA.EQ.'T')THEN
         CA =  '    CblasTrans'
      ELSE
         CA =  'CblasConjTrans'
      END IF
      IF (IORDER.EQ.1)THEN
         CRC = ' CblasRowMajor'
      ELSE
         CRC = ' CblasColMajor'
      END IF
      WRITE(NOUT, FMT = 9995)NC, SNAME, CRC, CU, CA
      WRITE(NOUT, FMT = 9994)N, K, ALPHA, LDA, BETA, LDC

 9995 FORMAT( 1X, I6, ': ', A12,'(', 3( A14, ',') )
 9994 FORMAT( 10X, 2( I3, ',' ), ' (', F4.1, ',', F4.1 ,'), A,',
     $        I3, ', (', F4.1,',', F4.1, '), C,', I3, ').' )
      END
*
*
      SUBROUTINE ZPRCN6(NOUT, NC, SNAME, IORDER, UPLO, TRANSA,
     $                 N, K, ALPHA, LDA, BETA, LDC)
      INTEGER          NOUT, NC, IORDER, N, K, LDA, LDC
      DOUBLE PRECISION ALPHA, BETA
      CHARACTER*1      UPLO, TRANSA
      CHARACTER*12     SNAME
      CHARACTER*14     CRC, CU, CA

      IF (UPLO.EQ.'U')THEN
         CU =  '    CblasUpper'
      ELSE
         CU =  '    CblasLower'
      END IF
      IF (TRANSA.EQ.'N')THEN
         CA =  '  CblasNoTrans'
      ELSE IF (TRANSA.EQ.'T')THEN
         CA =  '    CblasTrans'
      ELSE
         CA =  'CblasConjTrans'
      END IF
      IF (IORDER.EQ.1)THEN
         CRC = ' CblasRowMajor'
      ELSE
         CRC = ' CblasColMajor'
      END IF
      WRITE(NOUT, FMT = 9995)NC, SNAME, CRC, CU, CA
      WRITE(NOUT, FMT = 9994)N, K, ALPHA, LDA, BETA, LDC

 9995 FORMAT( 1X, I6, ': ', A12,'(', 3( A14, ',') )
 9994 FORMAT( 10X, 2( I3, ',' ),
     $      F4.1, ', A,', I3, ',', F4.1, ', C,', I3, ').' )
      END
*
      SUBROUTINE ZCHK5( SNAME, EPS, THRESH, NOUT, NTRA, TRACE, REWI,
     $                  FATAL, NIDIM, IDIM, NALF, ALF, NBET, BET, NMAX,
     $                  AB, AA, AS, BB, BS, C, CC, CS, CT, G, W,
     $                  IORDER )
*
*  Tests ZHER2K and ZSYR2K.
*
*  Auxiliary routine for test program for Level 3 Blas.
*
*  -- Written on 8-February-1989.
*     Jack Dongarra, Argonne National Laboratory.
*     Iain Duff, AERE Harwell.
*     Jeremy Du Croz, Numerical Algorithms Group Ltd.
*     Sven Hammarling, Numerical Algorithms Group Ltd.
*
*     .. Parameters ..
      COMPLEX*16    ZERO, ONE
      PARAMETER     ( ZERO = ( 0.0D0, 0.0D0 ), ONE = ( 1.0D0, 0.0D0 ) )
      DOUBLE PRECISION RONE, RZERO
      PARAMETER     ( RONE = 1.0D0, RZERO = 0.0D0 )
*     .. Scalar Arguments ..
      DOUBLE PRECISION  EPS, THRESH
      INTEGER           NALF, NBET, NIDIM, NMAX, NOUT, NTRA, IORDER
      LOGICAL           FATAL, REWI, TRACE
      CHARACTER*12      SNAME
*     .. Array Arguments ..
      COMPLEX*16         AA( NMAX*NMAX ), AB( 2*NMAX*NMAX ),
     $                   ALF( NALF ), AS( NMAX*NMAX ), BB( NMAX*NMAX ),
     $                   BET( NBET ), BS( NMAX*NMAX ), C( NMAX, NMAX ),
     $                   CC( NMAX*NMAX ), CS( NMAX*NMAX ), CT( NMAX ),
     $                   W( 2*NMAX )
      DOUBLE PRECISION   G( NMAX )
      INTEGER            IDIM( NIDIM )
*     .. Local Scalars ..
      COMPLEX*16         ALPHA, ALS, BETA, BETS
      DOUBLE PRECISION   ERR, ERRMAX, RBETA, RBETS
      INTEGER            I, IA, IB, ICT, ICU, IK, IN, J, JC, JJ, JJAB,
     $                   K, KS, LAA, LBB, LCC, LDA, LDAS, LDB, LDBS,
     $                   LDC, LDCS, LJ, MA, N, NA, NARGS, NC, NS
      LOGICAL            CONJ, NULL, RESET, SAME, TRAN, UPPER
      CHARACTER*1        TRANS, TRANSS, TRANST, UPLO, UPLOS
      CHARACTER*2        ICHT, ICHU
*     .. Local Arrays ..
      LOGICAL            ISAME( 13 )
*     .. External Functions ..
      LOGICAL            LZE, LZERES
      EXTERNAL           LZE, LZERES
*     .. External Subroutines ..
      EXTERNAL           CZHER2K, ZMAKE, ZMMCH, CZSYR2K
*     .. Intrinsic Functions ..
      INTRINSIC          DCMPLX, DCONJG, MAX, DBLE
*     .. Scalars in Common ..
      INTEGER            INFOT, NOUTC
      LOGICAL            LERR, OK
*     .. Common blocks ..
      COMMON             /INFOC/INFOT, NOUTC, OK, LERR
*     .. Data statements ..
      DATA               ICHT/'NC'/, ICHU/'UL'/
*     .. Executable Statements ..
      CONJ = SNAME( 8: 9 ).EQ.'he'
*
      NARGS = 12
      NC = 0
      RESET = .TRUE.
      ERRMAX = RZERO
*
      DO 130 IN = 1, NIDIM
         N = IDIM( IN )
*        Set LDC to 1 more than minimum value if room.
         LDC = N
         IF( LDC.LT.NMAX )
     $      LDC = LDC + 1
*        Skip tests if not enough room.
         IF( LDC.GT.NMAX )
     $      GO TO 130
         LCC = LDC*N
*
         DO 120 IK = 1, NIDIM
            K = IDIM( IK )
*
            DO 110 ICT = 1, 2
               TRANS = ICHT( ICT: ICT )
               TRAN = TRANS.EQ.'C'
               IF( TRAN.AND..NOT.CONJ )
     $            TRANS = 'T'
               IF( TRAN )THEN
                  MA = K
                  NA = N
               ELSE
                  MA = N
                  NA = K
               END IF
*              Set LDA to 1 more than minimum value if room.
               LDA = MA
               IF( LDA.LT.NMAX )
     $            LDA = LDA + 1
*              Skip tests if not enough room.
               IF( LDA.GT.NMAX )
     $            GO TO 110
               LAA = LDA*NA
*
*              Generate the matrix A.
*
               IF( TRAN )THEN
                  CALL ZMAKE( 'ge', ' ', ' ', MA, NA, AB, 2*NMAX, AA,
     $                        LDA, RESET, ZERO )
               ELSE
                 CALL ZMAKE( 'ge', ' ', ' ', MA, NA, AB, NMAX, AA, LDA,
     $                        RESET, ZERO )
               END IF
*
*              Generate the matrix B.
*
               LDB = LDA
               LBB = LAA
               IF( TRAN )THEN
                  CALL ZMAKE( 'ge', ' ', ' ', MA, NA, AB( K + 1 ),
     $                        2*NMAX, BB, LDB, RESET, ZERO )
               ELSE
                  CALL ZMAKE( 'ge', ' ', ' ', MA, NA, AB( K*NMAX + 1 ),
     $                        NMAX, BB, LDB, RESET, ZERO )
               END IF
*
               DO 100 ICU = 1, 2
                  UPLO = ICHU( ICU: ICU )
                  UPPER = UPLO.EQ.'U'
*
                  DO 90 IA = 1, NALF
                     ALPHA = ALF( IA )
*
                     DO 80 IB = 1, NBET
                        BETA = BET( IB )
                        IF( CONJ )THEN
                           RBETA = DBLE( BETA )
                           BETA = DCMPLX( RBETA, RZERO )
                        END IF
                        NULL = N.LE.0
                        IF( CONJ )
     $                     NULL = NULL.OR.( ( K.LE.0.OR.ALPHA.EQ.
     $                            ZERO ).AND.RBETA.EQ.RONE )
*
*                       Generate the matrix C.
*
                        CALL ZMAKE( SNAME( 8: 9 ), UPLO, ' ', N, N, C,
     $                              NMAX, CC, LDC, RESET, ZERO )
*
                        NC = NC + 1
*
*                       Save every datum before calling the subroutine.
*
                        UPLOS = UPLO
                        TRANSS = TRANS
                        NS = N
                        KS = K
                        ALS = ALPHA
                        DO 10 I = 1, LAA
                           AS( I ) = AA( I )
   10                   CONTINUE
                        LDAS = LDA
                        DO 20 I = 1, LBB
                           BS( I ) = BB( I )
   20                   CONTINUE
                        LDBS = LDB
                        IF( CONJ )THEN
                           RBETS = RBETA
                        ELSE
                           BETS = BETA
                        END IF
                        DO 30 I = 1, LCC
                           CS( I ) = CC( I )
   30                   CONTINUE
                        LDCS = LDC
*
*                       Call the subroutine.
*
                        IF( CONJ )THEN
                           IF( TRACE )
     $                        CALL ZPRCN7( NTRA, NC, SNAME, IORDER,
     $                        UPLO, TRANS, N, K, ALPHA, LDA, LDB,
     $                        RBETA, LDC)
                           IF( REWI )
     $                        REWIND NTRA
                           CALL CZHER2K( IORDER, UPLO, TRANS, N, K,
     $                                  ALPHA, AA, LDA, BB, LDB, RBETA,
     $                                  CC, LDC )
                        ELSE
                           IF( TRACE )
     $                        CALL ZPRCN5( NTRA, NC, SNAME, IORDER,
     $                        UPLO, TRANS, N, K, ALPHA, LDA, LDB,
     $                        BETA, LDC)
                           IF( REWI )
     $                        REWIND NTRA
                           CALL CZSYR2K( IORDER, UPLO, TRANS, N, K,
     $                                  ALPHA, AA, LDA, BB, LDB, BETA,
     $                                  CC, LDC )
                        END IF
*
*                       Check if error-exit was taken incorrectly.
*
                        IF( .NOT.OK )THEN
                           WRITE( NOUT, FMT = 9992 )
                           FATAL = .TRUE.
                           GO TO 150
                        END IF
*
*                       See what data changed inside subroutines.
*
                        ISAME( 1 ) = UPLOS.EQ.UPLO
                        ISAME( 2 ) = TRANSS.EQ.TRANS
                        ISAME( 3 ) = NS.EQ.N
                        ISAME( 4 ) = KS.EQ.K
                        ISAME( 5 ) = ALS.EQ.ALPHA
                        ISAME( 6 ) = LZE( AS, AA, LAA )
                        ISAME( 7 ) = LDAS.EQ.LDA
                        ISAME( 8 ) = LZE( BS, BB, LBB )
                        ISAME( 9 ) = LDBS.EQ.LDB
                        IF( CONJ )THEN
                           ISAME( 10 ) = RBETS.EQ.RBETA
                        ELSE
                           ISAME( 10 ) = BETS.EQ.BETA
                        END IF
                        IF( NULL )THEN
                           ISAME( 11 ) = LZE( CS, CC, LCC )
                        ELSE
                           ISAME( 11 ) = LZERES( 'he', UPLO, N, N, CS,
     $                                   CC, LDC )
                        END IF
                        ISAME( 12 ) = LDCS.EQ.LDC
*
*                       If data was incorrectly changed, report and
*                       return.
*
                        SAME = .TRUE.
                        DO 40 I = 1, NARGS
                           SAME = SAME.AND.ISAME( I )
                           IF( .NOT.ISAME( I ) )
     $                        WRITE( NOUT, FMT = 9998 )I
   40                   CONTINUE
                        IF( .NOT.SAME )THEN
                           FATAL = .TRUE.
                           GO TO 150
                        END IF
*
                        IF( .NOT.NULL )THEN
*
*                          Check the result column by column.
*
                           IF( CONJ )THEN
                              TRANST = 'C'
                           ELSE
                              TRANST = 'T'
                           END IF
                           JJAB = 1
                           JC = 1
                           DO 70 J = 1, N
                              IF( UPPER )THEN
                                 JJ = 1
                                 LJ = J
                              ELSE
                                 JJ = J
                                 LJ = N - J + 1
                              END IF
                              IF( TRAN )THEN
                                 DO 50 I = 1, K
                                    W( I ) = ALPHA*AB( ( J - 1 )*2*
     $                                       NMAX + K + I )
                                    IF( CONJ )THEN
                                       W( K + I ) = DCONJG( ALPHA )*
     $                                              AB( ( J - 1 )*2*
     $                                              NMAX + I )
                                    ELSE
                                       W( K + I ) = ALPHA*
     $                                              AB( ( J - 1 )*2*
     $                                              NMAX + I )
                                    END IF
   50                            CONTINUE
                                 CALL ZMMCH( TRANST, 'N', LJ, 1, 2*K,
     $                                      ONE, AB( JJAB ), 2*NMAX, W,
     $                                       2*NMAX, BETA, C( JJ, J ),
     $                                      NMAX, CT, G, CC( JC ), LDC,
     $                                       EPS, ERR, FATAL, NOUT,
     $                                       .TRUE. )
                              ELSE
                                 DO 60 I = 1, K
                                    IF( CONJ )THEN
                                       W( I ) = ALPHA*DCONJG( AB( ( K +
     $                                          I - 1 )*NMAX + J ) )
                                       W( K + I ) = DCONJG( ALPHA*
     $                                             AB( ( I - 1 )*NMAX +
     $                                              J ) )
                                    ELSE
                                      W( I ) = ALPHA*AB( ( K + I - 1 )*
     $                                          NMAX + J )
                                      W( K + I ) = ALPHA*
     $                                             AB( ( I - 1 )*NMAX +
     $                                              J )
                                    END IF
   60                            CONTINUE
                                 CALL ZMMCH( 'N', 'N', LJ, 1, 2*K, ONE,
     $                                       AB( JJ ), NMAX, W, 2*NMAX,
     $                                      BETA, C( JJ, J ), NMAX, CT,
     $                                      G, CC( JC ), LDC, EPS, ERR,
     $                                       FATAL, NOUT, .TRUE. )
                              END IF
                              IF( UPPER )THEN
                                 JC = JC + LDC
                              ELSE
                                 JC = JC + LDC + 1
                                 IF( TRAN )
     $                              JJAB = JJAB + 2*NMAX
                              END IF
                              ERRMAX = MAX( ERRMAX, ERR )
*                             If got really bad answer, report and
*                             return.
                              IF( FATAL )
     $                           GO TO 140
   70                      CONTINUE
                        END IF
*
   80                CONTINUE
*
   90             CONTINUE
*
  100          CONTINUE
*
  110       CONTINUE
*
  120    CONTINUE
*
  130 CONTINUE
*
*     Report result.
*
      IF( ERRMAX.LT.THRESH )THEN
         IF ( IORDER.EQ.0) WRITE( NOUT, FMT = 10000 )SNAME, NC
         IF ( IORDER.EQ.1) WRITE( NOUT, FMT = 10001 )SNAME, NC
      ELSE
         IF ( IORDER.EQ.0) WRITE( NOUT, FMT = 10002 )SNAME, NC, ERRMAX
         IF ( IORDER.EQ.1) WRITE( NOUT, FMT = 10003 )SNAME, NC, ERRMAX
      END IF
      GO TO 160
*
  140 CONTINUE
      IF( N.GT.1 )
     $   WRITE( NOUT, FMT = 9995 )J
*
  150 CONTINUE
      WRITE( NOUT, FMT = 9996 )SNAME
      IF( CONJ )THEN
         CALL ZPRCN7( NOUT, NC, SNAME, IORDER, UPLO, TRANS, N, K,
     $      ALPHA, LDA, LDB, RBETA, LDC)
      ELSE
         CALL ZPRCN5( NOUT, NC, SNAME, IORDER, UPLO, TRANS, N, K,
     $      ALPHA, LDA, LDB, BETA, LDC)
      END IF
*
  160 CONTINUE
      RETURN
*
10003 FORMAT( ' ', A12,' COMPLETED THE ROW-MAJOR    COMPUTATIONAL ',
     $ 'TESTS (', I6, ' CALLS)', /' ******* BUT WITH MAXIMUM TEST ',
     $ 'RATIO ', F8.2, ' - SUSPECT *******' )
10002 FORMAT( ' ', A12,' COMPLETED THE COLUMN-MAJOR COMPUTATIONAL ',
     $ 'TESTS (', I6, ' CALLS)', /' ******* BUT WITH MAXIMUM TEST ',
     $ 'RATIO ', F8.2, ' - SUSPECT *******' )
10001 FORMAT( ' ', A12,' PASSED THE ROW-MAJOR    COMPUTATIONAL TESTS',
     $ ' (', I6, ' CALL', 'S)' )
10000 FORMAT( ' ', A12,' PASSED THE COLUMN-MAJOR COMPUTATIONAL TESTS',
     $ ' (', I6, ' CALL', 'S)' )
 9998 FORMAT(' ******* FATAL ERROR - PARAMETER NUMBER ', I2, ' WAS CH',
     $      'ANGED INCORRECTLY *******' )
 9996 FORMAT( ' ******* ', A12,' FAILED ON CALL NUMBER:' )
 9995 FORMAT( '      THESE ARE THE RESULTS FOR COLUMN ', I3 )
 9994 FORMAT(1X, I6, ': ', A12,'(', 2( '''', A1, ''',' ), 2( I3, ',' ),
     $      '(', F4.1, ',', F4.1, '), A,', I3, ', B,', I3, ',', F4.1,
     $      ', C,', I3, ')           .' )
 9993 FORMAT(1X, I6, ': ', A12,'(', 2( '''', A1, ''',' ), 2( I3, ',' ),
     $      '(', F4.1, ',', F4.1, '), A,', I3, ', B,', I3, ',(', F4.1,
     $      ',', F4.1, '), C,', I3, ')    .' )
 9992 FORMAT(' ******* FATAL ERROR - ERROR-EXIT TAKEN ON VALID CALL *',
     $      '******' )
*
*     End of ZCHK5.
*
      END
*
      SUBROUTINE ZPRCN5(NOUT, NC, SNAME, IORDER, UPLO, TRANSA,
     $                 N, K, ALPHA, LDA, LDB, BETA, LDC)
      INTEGER          NOUT, NC, IORDER, N, K, LDA, LDB, LDC
      DOUBLE COMPLEX   ALPHA, BETA
      CHARACTER*1      UPLO, TRANSA
      CHARACTER*12     SNAME
      CHARACTER*14     CRC, CU, CA

      IF (UPLO.EQ.'U')THEN
         CU =  '    CblasUpper'
      ELSE
         CU =  '    CblasLower'
      END IF
      IF (TRANSA.EQ.'N')THEN
         CA =  '  CblasNoTrans'
      ELSE IF (TRANSA.EQ.'T')THEN
         CA =  '    CblasTrans'
      ELSE
         CA =  'CblasConjTrans'
      END IF
      IF (IORDER.EQ.1)THEN
         CRC = ' CblasRowMajor'
      ELSE
         CRC = ' CblasColMajor'
      END IF
      WRITE(NOUT, FMT = 9995)NC, SNAME, CRC, CU, CA
      WRITE(NOUT, FMT = 9994)N, K, ALPHA, LDA, LDB, BETA, LDC

 9995 FORMAT( 1X, I6, ': ', A12,'(', 3( A14, ',') )
 9994 FORMAT( 10X, 2( I3, ',' ), ' (', F4.1, ',', F4.1, '), A,',
     $  I3, ', B', I3, ', (', F4.1, ',', F4.1, '), C,', I3, ').' )
      END
*
*
      SUBROUTINE ZPRCN7(NOUT, NC, SNAME, IORDER, UPLO, TRANSA,
     $                 N, K, ALPHA, LDA, LDB, BETA, LDC)
      INTEGER          NOUT, NC, IORDER, N, K, LDA, LDB, LDC
      DOUBLE COMPLEX   ALPHA
      DOUBLE PRECISION BETA
      CHARACTER*1      UPLO, TRANSA
      CHARACTER*12     SNAME
      CHARACTER*14     CRC, CU, CA

      IF (UPLO.EQ.'U')THEN
         CU =  '    CblasUpper'
      ELSE
         CU =  '    CblasLower'
      END IF
      IF (TRANSA.EQ.'N')THEN
         CA =  '  CblasNoTrans'
      ELSE IF (TRANSA.EQ.'T')THEN
         CA =  '    CblasTrans'
      ELSE
         CA =  'CblasConjTrans'
      END IF
      IF (IORDER.EQ.1)THEN
         CRC = ' CblasRowMajor'
      ELSE
         CRC = ' CblasColMajor'
      END IF
      WRITE(NOUT, FMT = 9995)NC, SNAME, CRC, CU, CA
      WRITE(NOUT, FMT = 9994)N, K, ALPHA, LDA, LDB, BETA, LDC

 9995 FORMAT( 1X, I6, ': ', A12,'(', 3( A14, ',') )
 9994 FORMAT( 10X, 2( I3, ',' ), ' (', F4.1, ',', F4.1, '), A,',
     $      I3, ', B', I3, ',', F4.1, ', C,', I3, ').' )
      END
*
      SUBROUTINE ZMAKE( TYPE, UPLO, DIAG, M, N, A, NMAX, AA, LDA, RESET,
     $                  TRANSL )
*
*  Generates values for an M by N matrix A.
*  Stores the values in the array AA in the data structure required
*  by the routine, with unwanted elements set to rogue value.
*
*  TYPE is 'ge', 'he', 'sy' or 'tr'.
*
*  Auxiliary routine for test program for Level 3 Blas.
*
*  -- Written on 8-February-1989.
*     Jack Dongarra, Argonne National Laboratory.
*     Iain Duff, AERE Harwell.
*     Jeremy Du Croz, Numerical Algorithms Group Ltd.
*     Sven Hammarling, Numerical Algorithms Group Ltd.
*
*     .. Parameters ..
      COMPLEX*16         ZERO, ONE
      PARAMETER          ( ZERO = ( 0.0D0, 0.0D0 ),
     $                   ONE = ( 1.0D0, 0.0D0 ) )
      COMPLEX*16         ROGUE
      PARAMETER          ( ROGUE = ( -1.0D10, 1.0D10 ) )
      DOUBLE PRECISION   RZERO
      PARAMETER          ( RZERO = 0.0D0 )
      DOUBLE PRECISION   RROGUE
      PARAMETER          ( RROGUE = -1.0D10 )
*     .. Scalar Arguments ..
      COMPLEX*16         TRANSL
      INTEGER            LDA, M, N, NMAX
      LOGICAL            RESET
      CHARACTER*1        DIAG, UPLO
      CHARACTER*2        TYPE
*     .. Array Arguments ..
      COMPLEX*16         A( NMAX, * ), AA( * )
*     .. Local Scalars ..
      INTEGER            I, IBEG, IEND, J, JJ
      LOGICAL            GEN, HER, LOWER, SYM, TRI, UNIT, UPPER
*     .. External Functions ..
      COMPLEX*16         ZBEG
      EXTERNAL           ZBEG
*     .. Intrinsic Functions ..
      INTRINSIC          DCMPLX, DCONJG, DBLE
*     .. Executable Statements ..
      GEN = TYPE.EQ.'ge'
      HER = TYPE.EQ.'he'
      SYM = TYPE.EQ.'sy'
      TRI = TYPE.EQ.'tr'
      UPPER = ( HER.OR.SYM.OR.TRI ).AND.UPLO.EQ.'U'
      LOWER = ( HER.OR.SYM.OR.TRI ).AND.UPLO.EQ.'L'
      UNIT = TRI.AND.DIAG.EQ.'U'
*
*     Generate data in array A.
*
      DO 20 J = 1, N
         DO 10 I = 1, M
            IF( GEN.OR.( UPPER.AND.I.LE.J ).OR.( LOWER.AND.I.GE.J ) )
     $          THEN
               A( I, J ) = ZBEG( RESET ) + TRANSL
               IF( I.NE.J )THEN
*                 Set some elements to zero
                  IF( N.GT.3.AND.J.EQ.N/2 )
     $               A( I, J ) = ZERO
                  IF( HER )THEN
                     A( J, I ) = DCONJG( A( I, J ) )
                  ELSE IF( SYM )THEN
                     A( J, I ) = A( I, J )
                  ELSE IF( TRI )THEN
                     A( J, I ) = ZERO
                  END IF
               END IF
            END IF
   10    CONTINUE
         IF( HER )
     $      A( J, J ) = DCMPLX( DBLE( A( J, J ) ), RZERO )
         IF( TRI )
     $      A( J, J ) = A( J, J ) + ONE
         IF( UNIT )
     $      A( J, J ) = ONE
   20 CONTINUE
*
*     Store elements in array AS in data structure required by routine.
*
      IF( TYPE.EQ.'ge' )THEN
         DO 50 J = 1, N
            DO 30 I = 1, M
               AA( I + ( J - 1 )*LDA ) = A( I, J )
   30       CONTINUE
            DO 40 I = M + 1, LDA
               AA( I + ( J - 1 )*LDA ) = ROGUE
   40       CONTINUE
   50    CONTINUE
      ELSE IF( TYPE.EQ.'he'.OR.TYPE.EQ.'sy'.OR.TYPE.EQ.'tr' )THEN
         DO 90 J = 1, N
            IF( UPPER )THEN
               IBEG = 1
               IF( UNIT )THEN
                  IEND = J - 1
               ELSE
                  IEND = J
               END IF
            ELSE
               IF( UNIT )THEN
                  IBEG = J + 1
               ELSE
                  IBEG = J
               END IF
               IEND = N
            END IF
            DO 60 I = 1, IBEG - 1
               AA( I + ( J - 1 )*LDA ) = ROGUE
   60       CONTINUE
            DO 70 I = IBEG, IEND
               AA( I + ( J - 1 )*LDA ) = A( I, J )
   70       CONTINUE
            DO 80 I = IEND + 1, LDA
               AA( I + ( J - 1 )*LDA ) = ROGUE
   80       CONTINUE
            IF( HER )THEN
               JJ = J + ( J - 1 )*LDA
               AA( JJ ) = DCMPLX( DBLE( AA( JJ ) ), RROGUE )
            END IF
   90    CONTINUE
      END IF
      RETURN
*
*     End of ZMAKE.
*
      END
      SUBROUTINE ZMMCH( TRANSA, TRANSB, M, N, KK, ALPHA, A, LDA, B, LDB,
     $                  BETA, C, LDC, CT, G, CC, LDCC, EPS, ERR, FATAL,
     $                  NOUT, MV )
*
*  Checks the results of the computational tests.
*
*  Auxiliary routine for test program for Level 3 Blas.
*
*  -- Written on 8-February-1989.
*     Jack Dongarra, Argonne National Laboratory.
*     Iain Duff, AERE Harwell.
*     Jeremy Du Croz, Numerical Algorithms Group Ltd.
*     Sven Hammarling, Numerical Algorithms Group Ltd.
*
*     .. Parameters ..
      COMPLEX*16         ZERO
      PARAMETER          ( ZERO = ( 0.0D0, 0.0D0 ) )
      DOUBLE PRECISION   RZERO, RONE
      PARAMETER          ( RZERO = 0.0D0, RONE = 1.0D0 )
*     .. Scalar Arguments ..
      COMPLEX*16         ALPHA, BETA
      DOUBLE PRECISION   EPS, ERR
      INTEGER            KK, LDA, LDB, LDC, LDCC, M, N, NOUT
      LOGICAL            FATAL, MV
      CHARACTER*1        TRANSA, TRANSB
*     .. Array Arguments ..
      COMPLEX*16         A( LDA, * ), B( LDB, * ), C( LDC, * ),
     $                   CC( LDCC, * ), CT( * )
      DOUBLE PRECISION   G( * )
*     .. Local Scalars ..
      COMPLEX*16         CL
      DOUBLE PRECISION   ERRI
      INTEGER            I, J, K
      LOGICAL            CTRANA, CTRANB, TRANA, TRANB
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DIMAG, DCONJG, MAX, DBLE, SQRT
*     .. Statement Functions ..
      DOUBLE PRECISION   ABS1
*     .. Statement Function definitions ..
      ABS1( CL ) = ABS( DBLE( CL ) ) + ABS( DIMAG( CL ) )
*     .. Executable Statements ..
      TRANA = TRANSA.EQ.'T'.OR.TRANSA.EQ.'C'
      TRANB = TRANSB.EQ.'T'.OR.TRANSB.EQ.'C'
      CTRANA = TRANSA.EQ.'C'
      CTRANB = TRANSB.EQ.'C'
*
*     Compute expected result, one column at a time, in CT using data
*     in A, B and C.
*     Compute gauges in G.
*
      DO 220 J = 1, N
*
         DO 10 I = 1, M
            CT( I ) = ZERO
            G( I ) = RZERO
   10    CONTINUE
         IF( .NOT.TRANA.AND..NOT.TRANB )THEN
            DO 30 K = 1, KK
               DO 20 I = 1, M
                  CT( I ) = CT( I ) + A( I, K )*B( K, J )
                  G( I ) = G( I ) + ABS1( A( I, K ) )*ABS1( B( K, J ) )
   20          CONTINUE
   30       CONTINUE
         ELSE IF( TRANA.AND..NOT.TRANB )THEN
            IF( CTRANA )THEN
               DO 50 K = 1, KK
                  DO 40 I = 1, M
                     CT( I ) = CT( I ) + DCONJG( A( K, I ) )*B( K, J )
                     G( I ) = G( I ) + ABS1( A( K, I ) )*
     $                        ABS1( B( K, J ) )
   40             CONTINUE
   50          CONTINUE
            ELSE
               DO 70 K = 1, KK
                  DO 60 I = 1, M
                     CT( I ) = CT( I ) + A( K, I )*B( K, J )
                     G( I ) = G( I ) + ABS1( A( K, I ) )*
     $                        ABS1( B( K, J ) )
   60             CONTINUE
   70          CONTINUE
            END IF
         ELSE IF( .NOT.TRANA.AND.TRANB )THEN
            IF( CTRANB )THEN
               DO 90 K = 1, KK
                  DO 80 I = 1, M
                     CT( I ) = CT( I ) + A( I, K )*DCONJG( B( J, K ) )
                     G( I ) = G( I ) + ABS1( A( I, K ) )*
     $                        ABS1( B( J, K ) )
   80             CONTINUE
   90          CONTINUE
            ELSE
               DO 110 K = 1, KK
                  DO 100 I = 1, M
                     CT( I ) = CT( I ) + A( I, K )*B( J, K )
                     G( I ) = G( I ) + ABS1( A( I, K ) )*
     $                        ABS1( B( J, K ) )
  100             CONTINUE
  110          CONTINUE
            END IF
         ELSE IF( TRANA.AND.TRANB )THEN
            IF( CTRANA )THEN
               IF( CTRANB )THEN
                  DO 130 K = 1, KK
                     DO 120 I = 1, M
                        CT( I ) = CT( I ) + DCONJG( A( K, I ) )*
     $                            DCONJG( B( J, K ) )
                        G( I ) = G( I ) + ABS1( A( K, I ) )*
     $                           ABS1( B( J, K ) )
  120                CONTINUE
  130             CONTINUE
               ELSE
                  DO 150 K = 1, KK
                     DO 140 I = 1, M
                        CT( I ) = CT( I ) + DCONJG( A( K, I ) )*
     $                            B( J, K )
                        G( I ) = G( I ) + ABS1( A( K, I ) )*
     $                           ABS1( B( J, K ) )
  140                CONTINUE
  150             CONTINUE
               END IF
            ELSE
               IF( CTRANB )THEN
                  DO 170 K = 1, KK
                     DO 160 I = 1, M
                        CT( I ) = CT( I ) + A( K, I )*
     $                            DCONJG( B( J, K ) )
                        G( I ) = G( I ) + ABS1( A( K, I ) )*
     $                           ABS1( B( J, K ) )
  160                CONTINUE
  170             CONTINUE
               ELSE
                  DO 190 K = 1, KK
                     DO 180 I = 1, M
                        CT( I ) = CT( I ) + A( K, I )*B( J, K )
                        G( I ) = G( I ) + ABS1( A( K, I ) )*
     $                           ABS1( B( J, K ) )
  180                CONTINUE
  190             CONTINUE
               END IF
            END IF
         END IF
         DO 200 I = 1, M
            CT( I ) = ALPHA*CT( I ) + BETA*C( I, J )
            G( I ) = ABS1( ALPHA )*G( I ) +
     $               ABS1( BETA )*ABS1( C( I, J ) )
  200    CONTINUE
*
*        Compute the error ratio for this result.
*
         ERR = ZERO
         DO 210 I = 1, M
            ERRI = ABS1( CT( I ) - CC( I, J ) )/EPS
            IF( G( I ).NE.RZERO )
     $         ERRI = ERRI/G( I )
            ERR = MAX( ERR, ERRI )
            IF( ERR*SQRT( EPS ).GE.RONE )
     $         GO TO 230
  210    CONTINUE
*
  220 CONTINUE
*
*     If the loop completes, all results are at least half accurate.
      GO TO 250
*
*     Report fatal error.
*
  230 FATAL = .TRUE.
      WRITE( NOUT, FMT = 9999 )
      DO 240 I = 1, M
         IF( MV )THEN
            WRITE( NOUT, FMT = 9998 )I, CT( I ), CC( I, J )
         ELSE
            WRITE( NOUT, FMT = 9998 )I, CC( I, J ), CT( I )
         END IF
  240 CONTINUE
      IF( N.GT.1 )
     $   WRITE( NOUT, FMT = 9997 )J
*
  250 CONTINUE
      RETURN
*
 9999 FORMAT( ' ******* FATAL ERROR - COMPUTED RESULT IS LESS THAN HAL',
     $      'F ACCURATE *******', /'                       EXPECTED RE',
     $      'SULT                    COMPUTED RESULT' )
 9998 FORMAT( 1X, I7, 2( '  (', G15.6, ',', G15.6, ')' ) )
 9997 FORMAT( '      THESE ARE THE RESULTS FOR COLUMN ', I3 )
*
*     End of ZMMCH.
*
      END
      LOGICAL FUNCTION LZE( RI, RJ, LR )
*
*  Tests if two arrays are identical.
*
*  Auxiliary routine for test program for Level 3 Blas.
*
*  -- Written on 8-February-1989.
*     Jack Dongarra, Argonne National Laboratory.
*     Iain Duff, AERE Harwell.
*     Jeremy Du Croz, Numerical Algorithms Group Ltd.
*     Sven Hammarling, Numerical Algorithms Group Ltd.
*
*     .. Scalar Arguments ..
      INTEGER            LR
*     .. Array Arguments ..
      COMPLEX*16         RI( * ), RJ( * )
*     .. Local Scalars ..
      INTEGER            I
*     .. Executable Statements ..
      DO 10 I = 1, LR
         IF( RI( I ).NE.RJ( I ) )
     $      GO TO 20
   10 CONTINUE
      LZE = .TRUE.
      GO TO 30
   20 CONTINUE
      LZE = .FALSE.
   30 RETURN
*
*     End of LZE.
*
      END
      LOGICAL FUNCTION LZERES( TYPE, UPLO, M, N, AA, AS, LDA )
*
*  Tests if selected elements in two arrays are equal.
*
*  TYPE is 'ge' or 'he' or 'sy'.
*
*  Auxiliary routine for test program for Level 3 Blas.
*
*  -- Written on 8-February-1989.
*     Jack Dongarra, Argonne National Laboratory.
*     Iain Duff, AERE Harwell.
*     Jeremy Du Croz, Numerical Algorithms Group Ltd.
*     Sven Hammarling, Numerical Algorithms Group Ltd.
*
*     .. Scalar Arguments ..
      INTEGER            LDA, M, N
      CHARACTER*1        UPLO
      CHARACTER*2        TYPE
*     .. Array Arguments ..
      COMPLEX*16         AA( LDA, * ), AS( LDA, * )
*     .. Local Scalars ..
      INTEGER            I, IBEG, IEND, J
      LOGICAL            UPPER
*     .. Executable Statements ..
      UPPER = UPLO.EQ.'U'
      IF( TYPE.EQ.'ge' )THEN
         DO 20 J = 1, N
            DO 10 I = M + 1, LDA
               IF( AA( I, J ).NE.AS( I, J ) )
     $            GO TO 70
   10       CONTINUE
   20    CONTINUE
      ELSE IF( TYPE.EQ.'he'.OR.TYPE.EQ.'sy' )THEN
         DO 50 J = 1, N
            IF( UPPER )THEN
               IBEG = 1
               IEND = J
            ELSE
               IBEG = J
               IEND = N
            END IF
            DO 30 I = 1, IBEG - 1
               IF( AA( I, J ).NE.AS( I, J ) )
     $            GO TO 70
   30       CONTINUE
            DO 40 I = IEND + 1, LDA
               IF( AA( I, J ).NE.AS( I, J ) )
     $            GO TO 70
   40       CONTINUE
   50    CONTINUE
      END IF
*
   60 CONTINUE
      LZERES = .TRUE.
      GO TO 80
   70 CONTINUE
      LZERES = .FALSE.
   80 RETURN
*
*     End of LZERES.
*
      END
      COMPLEX*16     FUNCTION ZBEG( RESET )
*
*  Generates complex numbers as pairs of random numbers uniformly
*  distributed between -0.5 and 0.5.
*
*  Auxiliary routine for test program for Level 3 Blas.
*
*  -- Written on 8-February-1989.
*     Jack Dongarra, Argonne National Laboratory.
*     Iain Duff, AERE Harwell.
*     Jeremy Du Croz, Numerical Algorithms Group Ltd.
*     Sven Hammarling, Numerical Algorithms Group Ltd.
*
*     .. Scalar Arguments ..
      LOGICAL            RESET
*     .. Local Scalars ..
      INTEGER            I, IC, J, MI, MJ
*     .. Save statement ..
      SAVE               I, IC, J, MI, MJ
*     .. Intrinsic Functions ..
      INTRINSIC          DCMPLX
*     .. Executable Statements ..
      IF( RESET )THEN
*        Initialize local variables.
         MI = 891
         MJ = 457
         I = 7
         J = 7
         IC = 0
         RESET = .FALSE.
      END IF
*
*     The sequence of values of I or J is bounded between 1 and 999.
*     If initial I or J = 1,2,3,6,7 or 9, the period will be 50.
*     If initial I or J = 4 or 8, the period will be 25.
*     If initial I or J = 5, the period will be 10.
*     IC is used to break up the period by skipping 1 value of I or J
*     in 6.
*
      IC = IC + 1
   10 I = I*MI
      J = J*MJ
      I = I - 1000*( I/1000 )
      J = J - 1000*( J/1000 )
      IF( IC.GE.5 )THEN
         IC = 0
         GO TO 10
      END IF
      ZBEG = DCMPLX( ( I - 500 )/1001.0D0, ( J - 500 )/1001.0D0 )
      RETURN
*
*     End of ZBEG.
*
      END
      DOUBLE PRECISION FUNCTION DDIFF( X, Y )
*
*  Auxiliary routine for test program for Level 3 Blas.
*
*  -- Written on 8-February-1989.
*     Jack Dongarra, Argonne National Laboratory.
*     Iain Duff, AERE Harwell.
*     Jeremy Du Croz, Numerical Algorithms Group Ltd.
*     Sven Hammarling, Numerical Algorithms Group Ltd.
*
*     .. Scalar Arguments ..
      DOUBLE PRECISION   X, Y
*     .. Executable Statements ..
      DDIFF = X - Y
      RETURN
*
*     End of DDIFF.
*
      END

