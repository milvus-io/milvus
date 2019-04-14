      PROGRAM SBLAT2
*
*  Test program for the REAL             Level 2 Blas.
*
*  The program must be driven by a short data file. The first 17 records
*  of the file are read using list-directed input, the last 16 records
*  are read using the format ( A12, L2 ). An annotated example of a data
*  file can be obtained by deleting the first 3 characters from the
*  following 33 lines:
*  'SBLAT2.SNAP'     NAME OF SNAPSHOT OUTPUT FILE
*  -1                UNIT NUMBER OF SNAPSHOT FILE (NOT USED IF .LT. 0)
*  F        LOGICAL FLAG, T TO REWIND SNAPSHOT FILE AFTER EACH RECORD.
*  F        LOGICAL FLAG, T TO STOP ON FAILURES.
*  T        LOGICAL FLAG, T TO TEST ERROR EXITS.
*  2        0 TO TEST COLUMN-MAJOR, 1 TO TEST ROW-MAJOR, 2 TO TEST BOTH
*  16.0     THRESHOLD VALUE OF TEST RATIO
*  6                 NUMBER OF VALUES OF N
*  0 1 2 3 5 9       VALUES OF N
*  4                 NUMBER OF VALUES OF K
*  0 1 2 4           VALUES OF K
*  4                 NUMBER OF VALUES OF INCX AND INCY
*  1 2 -1 -2         VALUES OF INCX AND INCY
*  3                 NUMBER OF VALUES OF ALPHA
*  0.0 1.0 0.7       VALUES OF ALPHA
*  3                 NUMBER OF VALUES OF BETA
*  0.0 1.0 0.9       VALUES OF BETA
*  cblas_sgemv  T PUT F FOR NO TEST. SAME COLUMNS.
*  cblas_sgbmv  T PUT F FOR NO TEST. SAME COLUMNS.
*  cblas_ssymv  T PUT F FOR NO TEST. SAME COLUMNS.
*  cblas_ssbmv  T PUT F FOR NO TEST. SAME COLUMNS.
*  cblas_sspmv  T PUT F FOR NO TEST. SAME COLUMNS.
*  cblas_strmv  T PUT F FOR NO TEST. SAME COLUMNS.
*  cblas_stbmv  T PUT F FOR NO TEST. SAME COLUMNS.
*  cblas_stpmv  T PUT F FOR NO TEST. SAME COLUMNS.
*  cblas_strsv  T PUT F FOR NO TEST. SAME COLUMNS.
*  cblas_stbsv  T PUT F FOR NO TEST. SAME COLUMNS.
*  cblas_stpsv  T PUT F FOR NO TEST. SAME COLUMNS.
*  cblas_sger   T PUT F FOR NO TEST. SAME COLUMNS.
*  cblas_ssyr   T PUT F FOR NO TEST. SAME COLUMNS.
*  cblas_sspr   T PUT F FOR NO TEST. SAME COLUMNS.
*  cblas_ssyr2  T PUT F FOR NO TEST. SAME COLUMNS.
*  cblas_sspr2  T PUT F FOR NO TEST. SAME COLUMNS.
*
*     See:
*
*        Dongarra J. J., Du Croz J. J., Hammarling S.  and Hanson R. J..
*        An  extended  set of Fortran  Basic Linear Algebra Subprograms.
*
*        Technical  Memoranda  Nos. 41 (revision 3) and 81,  Mathematics
*        and  Computer Science  Division,  Argonne  National Laboratory,
*        9700 South Cass Avenue, Argonne, Illinois 60439, US.
*
*        Or
*
*        NAG  Technical Reports TR3/87 and TR4/87,  Numerical Algorithms
*        Group  Ltd.,  NAG  Central  Office,  256  Banbury  Road, Oxford
*        OX2 7DE, UK,  and  Numerical Algorithms Group Inc.,  1101  31st
*        Street,  Suite 100,  Downers Grove,  Illinois 60515-1263,  USA.
*
*
*  -- Written on 10-August-1987.
*     Richard Hanson, Sandia National Labs.
*     Jeremy Du Croz, NAG Central Office.
*
*     .. Parameters ..
      INTEGER            NIN, NOUT
      PARAMETER          ( NIN = 5, NOUT = 6 )
      INTEGER            NSUBS
      PARAMETER          ( NSUBS = 16 )
      REAL               ZERO, HALF, ONE
      PARAMETER          ( ZERO = 0.0, HALF = 0.5, ONE = 1.0 )
      INTEGER            NMAX, INCMAX
      PARAMETER          ( NMAX = 65, INCMAX = 2 )
      INTEGER            NINMAX, NIDMAX, NKBMAX, NALMAX, NBEMAX
      PARAMETER          ( NINMAX = 7, NIDMAX = 9, NKBMAX = 7,
     $                   NALMAX = 7, NBEMAX = 7 )
*     .. Local Scalars ..
      REAL               EPS, ERR, THRESH
      INTEGER            I, ISNUM, J, N, NALF, NBET, NIDIM, NINC, NKB,
     $                   NTRA, LAYOUT
      LOGICAL            FATAL, LTESTT, REWI, SAME, SFATAL, TRACE,
     $                   TSTERR, CORDER, RORDER
      CHARACTER*1        TRANS
      CHARACTER*12       SNAMET
      CHARACTER*32       SNAPS
*     .. Local Arrays ..
      REAL               A( NMAX, NMAX ), AA( NMAX*NMAX ),
     $                   ALF( NALMAX ), AS( NMAX*NMAX ), BET( NBEMAX ),
     $                   G( NMAX ), X( NMAX ), XS( NMAX*INCMAX ),
     $                   XX( NMAX*INCMAX ), Y( NMAX ),
     $                   YS( NMAX*INCMAX ), YT( NMAX ),
     $                   YY( NMAX*INCMAX ), Z( 2*NMAX )
      INTEGER            IDIM( NIDMAX ), INC( NINMAX ), KB( NKBMAX )
      LOGICAL            LTEST( NSUBS )
      CHARACTER*12       SNAMES( NSUBS )
*     .. External Functions ..
      REAL               SDIFF
      LOGICAL            LSE
      EXTERNAL           SDIFF, LSE
*     .. External Subroutines ..
      EXTERNAL           SCHK1, SCHK2, SCHK3, SCHK4, SCHK5, SCHK6,
     $                   CS2CHKE, SMVCH
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, MIN
*     .. Scalars in Common ..
      INTEGER            INFOT, NOUTC
      LOGICAL            OK
      CHARACTER*12       SRNAMT
*     .. Common blocks ..
      COMMON             /INFOC/INFOT, NOUTC, OK
      COMMON             /SRNAMC/SRNAMT
*     .. Data statements ..
      DATA               SNAMES/'cblas_sgemv ', 'cblas_sgbmv ',
     $                   'cblas_ssymv ','cblas_ssbmv ','cblas_sspmv ',
     $                   'cblas_strmv ','cblas_stbmv ','cblas_stpmv ',
     $                   'cblas_strsv ','cblas_stbsv ','cblas_stpsv ',
     $                   'cblas_sger  ','cblas_ssyr  ','cblas_sspr  ',
     $                   'cblas_ssyr2 ','cblas_sspr2 '/
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
         OPEN( NTRA, FILE = SNAPS )
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
         GO TO 230
      END IF
      READ( NIN, FMT = * )( IDIM( I ), I = 1, NIDIM )
      DO 10 I = 1, NIDIM
         IF( IDIM( I ).LT.0.OR.IDIM( I ).GT.NMAX )THEN
            WRITE( NOUT, FMT = 9996 )NMAX
            GO TO 230
         END IF
   10 CONTINUE
*     Values of K
      READ( NIN, FMT = * )NKB
      IF( NKB.LT.1.OR.NKB.GT.NKBMAX )THEN
         WRITE( NOUT, FMT = 9997 )'K', NKBMAX
         GO TO 230
      END IF
      READ( NIN, FMT = * )( KB( I ), I = 1, NKB )
      DO 20 I = 1, NKB
         IF( KB( I ).LT.0 )THEN
            WRITE( NOUT, FMT = 9995 )
            GO TO 230
         END IF
   20 CONTINUE
*     Values of INCX and INCY
      READ( NIN, FMT = * )NINC
      IF( NINC.LT.1.OR.NINC.GT.NINMAX )THEN
         WRITE( NOUT, FMT = 9997 )'INCX AND INCY', NINMAX
         GO TO 230
      END IF
      READ( NIN, FMT = * )( INC( I ), I = 1, NINC )
      DO 30 I = 1, NINC
         IF( INC( I ).EQ.0.OR.ABS( INC( I ) ).GT.INCMAX )THEN
            WRITE( NOUT, FMT = 9994 )INCMAX
            GO TO 230
         END IF
   30 CONTINUE
*     Values of ALPHA
      READ( NIN, FMT = * )NALF
      IF( NALF.LT.1.OR.NALF.GT.NALMAX )THEN
         WRITE( NOUT, FMT = 9997 )'ALPHA', NALMAX
         GO TO 230
      END IF
      READ( NIN, FMT = * )( ALF( I ), I = 1, NALF )
*     Values of BETA
      READ( NIN, FMT = * )NBET
      IF( NBET.LT.1.OR.NBET.GT.NBEMAX )THEN
         WRITE( NOUT, FMT = 9997 )'BETA', NBEMAX
         GO TO 230
      END IF
      READ( NIN, FMT = * )( BET( I ), I = 1, NBET )
*
*     Report values of parameters.
*
      WRITE( NOUT, FMT = 9993 )
      WRITE( NOUT, FMT = 9992 )( IDIM( I ), I = 1, NIDIM )
      WRITE( NOUT, FMT = 9991 )( KB( I ), I = 1, NKB )
      WRITE( NOUT, FMT = 9990 )( INC( I ), I = 1, NINC )
      WRITE( NOUT, FMT = 9989 )( ALF( I ), I = 1, NALF )
      WRITE( NOUT, FMT = 9988 )( BET( I ), I = 1, NBET )
      IF( .NOT.TSTERR )THEN
         WRITE( NOUT, FMT = * )
         WRITE( NOUT, FMT = 9980 )
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
      DO 40 I = 1, NSUBS
         LTEST( I ) = .FALSE.
   40 CONTINUE
   50 READ( NIN, FMT = 9984, END = 80 )SNAMET, LTESTT
      DO 60 I = 1, NSUBS
         IF( SNAMET.EQ.SNAMES( I ) )
     $      GO TO 70
   60 CONTINUE
      WRITE( NOUT, FMT = 9986 )SNAMET
      STOP
   70 LTEST( I ) = LTESTT
      GO TO 50
*
   80 CONTINUE
      CLOSE ( NIN )
*
*     Compute EPS (the machine precision).
*
      EPS = ONE
   90 CONTINUE
      IF( SDIFF( ONE + EPS, ONE ).EQ.ZERO )
     $   GO TO 100
      EPS = HALF*EPS
      GO TO 90
  100 CONTINUE
      EPS = EPS + EPS
      WRITE( NOUT, FMT = 9998 )EPS
*
*     Check the reliability of SMVCH using exact data.
*
      N = MIN( 32, NMAX )
      DO 120 J = 1, N
         DO 110 I = 1, N
            A( I, J ) = MAX( I - J + 1, 0 )
  110    CONTINUE
         X( J ) = J
         Y( J ) = ZERO
  120 CONTINUE
      DO 130 J = 1, N
         YY( J ) = J*( ( J + 1 )*J )/2 - ( ( J + 1 )*J*( J - 1 ) )/3
  130 CONTINUE
*     YY holds the exact result. On exit from SMVCH YT holds
*     the result computed by SMVCH.
      TRANS = 'N'
      CALL SMVCH( TRANS, N, N, ONE, A, NMAX, X, 1, ZERO, Y, 1, YT, G,
     $            YY, EPS, ERR, FATAL, NOUT, .TRUE. )
      SAME = LSE( YY, YT, N )
      IF( .NOT.SAME.OR.ERR.NE.ZERO )THEN
         WRITE( NOUT, FMT = 9985 )TRANS, SAME, ERR
         STOP
      END IF
      TRANS = 'T'
      CALL SMVCH( TRANS, N, N, ONE, A, NMAX, X, -1, ZERO, Y, -1, YT, G,
     $            YY, EPS, ERR, FATAL, NOUT, .TRUE. )
      SAME = LSE( YY, YT, N )
      IF( .NOT.SAME.OR.ERR.NE.ZERO )THEN
         WRITE( NOUT, FMT = 9985 )TRANS, SAME, ERR
         STOP
      END IF
*
*     Test each subroutine in turn.
*
      DO 210 ISNUM = 1, NSUBS
         WRITE( NOUT, FMT = * )
         IF( .NOT.LTEST( ISNUM ) )THEN
*           Subprogram is not to be tested.
            WRITE( NOUT, FMT = 9983 )SNAMES( ISNUM )
         ELSE
            SRNAMT = SNAMES( ISNUM )
*           Test error exits.
            IF( TSTERR )THEN
               CALL CS2CHKE( SNAMES( ISNUM ) )
               WRITE( NOUT, FMT = * )
            END IF
*           Test computations.
            INFOT = 0
            OK = .TRUE.
            FATAL = .FALSE.
            GO TO ( 140, 140, 150, 150, 150, 160, 160,
     $              160, 160, 160, 160, 170, 180, 180,
     $              190, 190 )ISNUM
*           Test SGEMV, 01, and SGBMV, 02.
  140       IF (CORDER) THEN
            CALL SCHK1( SNAMES( ISNUM ), EPS, THRESH, NOUT, NTRA, TRACE,
     $                  REWI, FATAL, NIDIM, IDIM, NKB, KB, NALF, ALF,
     $                  NBET, BET, NINC, INC, NMAX, INCMAX, A, AA, AS,
     $                  X, XX, XS, Y, YY, YS, YT, G, 0 )
            END IF
            IF (RORDER) THEN
            CALL SCHK1( SNAMES( ISNUM ), EPS, THRESH, NOUT, NTRA, TRACE,
     $                  REWI, FATAL, NIDIM, IDIM, NKB, KB, NALF, ALF,
     $                  NBET, BET, NINC, INC, NMAX, INCMAX, A, AA, AS,
     $                  X, XX, XS, Y, YY, YS, YT, G, 1 )
            END IF
            GO TO 200
*           Test SSYMV, 03, SSBMV, 04, and SSPMV, 05.
  150       IF (CORDER) THEN
            CALL SCHK2( SNAMES( ISNUM ), EPS, THRESH, NOUT, NTRA, TRACE,
     $                  REWI, FATAL, NIDIM, IDIM, NKB, KB, NALF, ALF,
     $                  NBET, BET, NINC, INC, NMAX, INCMAX, A, AA, AS,
     $                  X, XX, XS, Y, YY, YS, YT, G, 0 )
            END IF
            IF (RORDER) THEN
            CALL SCHK2( SNAMES( ISNUM ), EPS, THRESH, NOUT, NTRA, TRACE,
     $                  REWI, FATAL, NIDIM, IDIM, NKB, KB, NALF, ALF,
     $                  NBET, BET, NINC, INC, NMAX, INCMAX, A, AA, AS,
     $                  X, XX, XS, Y, YY, YS, YT, G, 1 )
            END IF
            GO TO 200
*           Test STRMV, 06, STBMV, 07, STPMV, 08,
*           STRSV, 09, STBSV, 10, and STPSV, 11.
  160       IF (CORDER) THEN
            CALL SCHK3( SNAMES( ISNUM ), EPS, THRESH, NOUT, NTRA, TRACE,
     $                  REWI, FATAL, NIDIM, IDIM, NKB, KB, NINC, INC,
     $                  NMAX, INCMAX, A, AA, AS, Y, YY, YS, YT, G, Z,
     $			0 )
            END IF
            IF (RORDER) THEN
            CALL SCHK3( SNAMES( ISNUM ), EPS, THRESH, NOUT, NTRA, TRACE,
     $                  REWI, FATAL, NIDIM, IDIM, NKB, KB, NINC, INC,
     $                  NMAX, INCMAX, A, AA, AS, Y, YY, YS, YT, G, Z,
     $			1 )
            END IF
            GO TO 200
*           Test SGER, 12.
  170       IF (CORDER) THEN
            CALL SCHK4( SNAMES( ISNUM ), EPS, THRESH, NOUT, NTRA, TRACE,
     $                  REWI, FATAL, NIDIM, IDIM, NALF, ALF, NINC, INC,
     $                  NMAX, INCMAX, A, AA, AS, X, XX, XS, Y, YY, YS,
     $                  YT, G, Z, 0 )
            END IF
            IF (RORDER) THEN
            CALL SCHK4( SNAMES( ISNUM ), EPS, THRESH, NOUT, NTRA, TRACE,
     $                  REWI, FATAL, NIDIM, IDIM, NALF, ALF, NINC, INC,
     $                  NMAX, INCMAX, A, AA, AS, X, XX, XS, Y, YY, YS,
     $                  YT, G, Z, 1 )
            END IF
            GO TO 200
*           Test SSYR, 13, and SSPR, 14.
  180       IF (CORDER) THEN
            CALL SCHK5( SNAMES( ISNUM ), EPS, THRESH, NOUT, NTRA, TRACE,
     $                  REWI, FATAL, NIDIM, IDIM, NALF, ALF, NINC, INC,
     $                  NMAX, INCMAX, A, AA, AS, X, XX, XS, Y, YY, YS,
     $                  YT, G, Z, 0 )
            END IF
            IF (RORDER) THEN
            CALL SCHK5( SNAMES( ISNUM ), EPS, THRESH, NOUT, NTRA, TRACE,
     $                  REWI, FATAL, NIDIM, IDIM, NALF, ALF, NINC, INC,
     $                  NMAX, INCMAX, A, AA, AS, X, XX, XS, Y, YY, YS,
     $                  YT, G, Z, 1 )
            END IF
            GO TO 200
*           Test SSYR2, 15, and SSPR2, 16.
  190       IF (CORDER) THEN
            CALL SCHK6( SNAMES( ISNUM ), EPS, THRESH, NOUT, NTRA, TRACE,
     $                  REWI, FATAL, NIDIM, IDIM, NALF, ALF, NINC, INC,
     $                  NMAX, INCMAX, A, AA, AS, X, XX, XS, Y, YY, YS,
     $                  YT, G, Z, 0 )
            END IF
            IF (RORDER) THEN
            CALL SCHK6( SNAMES( ISNUM ), EPS, THRESH, NOUT, NTRA, TRACE,
     $                  REWI, FATAL, NIDIM, IDIM, NALF, ALF, NINC, INC,
     $                  NMAX, INCMAX, A, AA, AS, X, XX, XS, Y, YY, YS,
     $                  YT, G, Z, 1 )
            END IF
*
  200       IF( FATAL.AND.SFATAL )
     $         GO TO 220
         END IF
  210 CONTINUE
      WRITE( NOUT, FMT = 9982 )
      GO TO 240
*
  220 CONTINUE
      WRITE( NOUT, FMT = 9981 )
      GO TO 240
*
  230 CONTINUE
      WRITE( NOUT, FMT = 9987 )
*
  240 CONTINUE
      IF( TRACE )
     $   CLOSE ( NTRA )
      CLOSE ( NOUT )
      STOP
*
10002 FORMAT( ' COLUMN-MAJOR AND ROW-MAJOR DATA LAYOUTS ARE TESTED' )
10001 FORMAT( ' ROW-MAJOR DATA LAYOUT IS TESTED' )
10000 FORMAT( ' COLUMN-MAJOR DATA LAYOUT IS TESTED' )
 9999 FORMAT( ' ROUTINES PASS COMPUTATIONAL TESTS IF TEST RATIO IS LES',
     $      'S THAN', F8.2 )
 9998 FORMAT( ' RELATIVE MACHINE PRECISION IS TAKEN TO BE', 1P, E9.1 )
 9997 FORMAT( ' NUMBER OF VALUES OF ', A, ' IS LESS THAN 1 OR GREATER ',
     $      'THAN ', I2 )
 9996 FORMAT( ' VALUE OF N IS LESS THAN 0 OR GREATER THAN ', I2 )
 9995 FORMAT( ' VALUE OF K IS LESS THAN 0' )
 9994 FORMAT( ' ABSOLUTE VALUE OF INCX OR INCY IS 0 OR GREATER THAN ',
     $      I2 )
 9993 FORMAT( ' TESTS OF THE REAL             LEVEL 2 BLAS', //' THE F',
     $      'OLLOWING PARAMETER VALUES WILL BE USED:' )
 9992 FORMAT( '   FOR N              ', 9I6 )
 9991 FORMAT( '   FOR K              ', 7I6 )
 9990 FORMAT( '   FOR INCX AND INCY  ', 7I6 )
 9989 FORMAT( '   FOR ALPHA          ', 7F6.1 )
 9988 FORMAT( '   FOR BETA           ', 7F6.1 )
 9987 FORMAT( ' AMEND DATA FILE OR INCREASE ARRAY SIZES IN PROGRAM',
     $      /' ******* TESTS ABANDONED *******' )
 9986 FORMAT( ' SUBPROGRAM NAME ',A12, ' NOT RECOGNIZED', /' ******* T',
     $      'ESTS ABANDONED *******' )
 9985 FORMAT( ' ERROR IN SMVCH -  IN-LINE DOT PRODUCTS ARE BEING EVALU',
     $      'ATED WRONGLY.', /' SMVCH WAS CALLED WITH TRANS = ', A1,
     $      ' AND RETURNED SAME = ', L1, ' AND ERR = ', F12.3, '.', /
     $   ' THIS MAY BE DUE TO FAULTS IN THE ARITHMETIC OR THE COMPILER.'
     $      , /' ******* TESTS ABANDONED *******' )
 9984 FORMAT(A12, L2 )
 9983 FORMAT( 1X,A12, ' WAS NOT TESTED' )
 9982 FORMAT( /' END OF TESTS' )
 9981 FORMAT( /' ******* FATAL ERROR - TESTS ABANDONED *******' )
 9980 FORMAT( ' ERROR-EXITS WILL NOT BE TESTED' )
*
*     End of SBLAT2.
*
      END
      SUBROUTINE SCHK1( SNAME, EPS, THRESH, NOUT, NTRA, TRACE, REWI,
     $                  FATAL, NIDIM, IDIM, NKB, KB, NALF, ALF, NBET,
     $                  BET, NINC, INC, NMAX, INCMAX, A, AA, AS, X, XX,
     $                  XS, Y, YY, YS, YT, G, IORDER )
*
*  Tests SGEMV and SGBMV.
*
*  Auxiliary routine for test program for Level 2 Blas.
*
*  -- Written on 10-August-1987.
*     Richard Hanson, Sandia National Labs.
*     Jeremy Du Croz, NAG Central Office.
*
*     .. Parameters ..
      REAL               ZERO, HALF
      PARAMETER          ( ZERO = 0.0, HALF = 0.5 )
*     .. Scalar Arguments ..
      REAL               EPS, THRESH
      INTEGER            INCMAX, NALF, NBET, NIDIM, NINC, NKB, NMAX,
     $                   NOUT, NTRA, IORDER
      LOGICAL            FATAL, REWI, TRACE
      CHARACTER*12       SNAME
*     .. Array Arguments ..
      REAL               A( NMAX, NMAX ), AA( NMAX*NMAX ), ALF( NALF ),
     $                   AS( NMAX*NMAX ), BET( NBET ), G( NMAX ),
     $                   X( NMAX ), XS( NMAX*INCMAX ),
     $                   XX( NMAX*INCMAX ), Y( NMAX ),
     $                   YS( NMAX*INCMAX ), YT( NMAX ),
     $                   YY( NMAX*INCMAX )
      INTEGER            IDIM( NIDIM ), INC( NINC ), KB( NKB )
*     .. Local Scalars ..
      REAL               ALPHA, ALS, BETA, BLS, ERR, ERRMAX, TRANSL
      INTEGER            I, IA, IB, IC, IKU, IM, IN, INCX, INCXS, INCY,
     $                   INCYS, IX, IY, KL, KLS, KU, KUS, LAA, LDA,
     $                   LDAS, LX, LY, M, ML, MS, N, NARGS, NC, ND, NK,
     $                   NL, NS
      LOGICAL            BANDED, FULL, NULL, RESET, SAME, TRAN
      CHARACTER*1        TRANS, TRANSS
      CHARACTER*14       CTRANS
      CHARACTER*3        ICH
*     .. Local Arrays ..
      LOGICAL            ISAME( 13 )
*     .. External Functions ..
      LOGICAL            LSE, LSERES
      EXTERNAL           LSE, LSERES
*     .. External Subroutines ..
      EXTERNAL           CSGBMV, CSGEMV, SMAKE, SMVCH
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, MIN
*     .. Scalars in Common ..
      INTEGER            INFOT, NOUTC
      LOGICAL            OK
*     .. Common blocks ..
      COMMON             /INFOC/INFOT, NOUTC, OK
*     .. Data statements ..
      DATA               ICH/'NTC'/
*     .. Executable Statements ..
      FULL = SNAME( 9: 9 ).EQ.'e'
      BANDED = SNAME( 9: 9 ).EQ.'b'
*     Define the number of arguments.
      IF( FULL )THEN
         NARGS = 11
      ELSE IF( BANDED )THEN
         NARGS = 13
      END IF
*
      NC = 0
      RESET = .TRUE.
      ERRMAX = ZERO
*
      DO 120 IN = 1, NIDIM
         N = IDIM( IN )
         ND = N/2 + 1
*
         DO 110 IM = 1, 2
            IF( IM.EQ.1 )
     $         M = MAX( N - ND, 0 )
            IF( IM.EQ.2 )
     $         M = MIN( N + ND, NMAX )
*
            IF( BANDED )THEN
               NK = NKB
            ELSE
               NK = 1
            END IF
            DO 100 IKU = 1, NK
               IF( BANDED )THEN
                  KU = KB( IKU )
                  KL = MAX( KU - 1, 0 )
               ELSE
                  KU = N - 1
                  KL = M - 1
               END IF
*              Set LDA to 1 more than minimum value if room.
               IF( BANDED )THEN
                  LDA = KL + KU + 1
               ELSE
                  LDA = M
               END IF
               IF( LDA.LT.NMAX )
     $            LDA = LDA + 1
*              Skip tests if not enough room.
               IF( LDA.GT.NMAX )
     $            GO TO 100
               LAA = LDA*N
               NULL = N.LE.0.OR.M.LE.0
*
*              Generate the matrix A.
*
               TRANSL = ZERO
               CALL SMAKE( SNAME( 8: 9 ), ' ', ' ', M, N, A, NMAX, AA,
     $                     LDA, KL, KU, RESET, TRANSL )
*
               DO 90 IC = 1, 3
                  TRANS = ICH( IC: IC )
                  IF (TRANS.EQ.'N')THEN
                     CTRANS = '  CblasNoTrans'
                  ELSE IF (TRANS.EQ.'T')THEN
                     CTRANS = '    CblasTrans'
                  ELSE
                     CTRANS = 'CblasConjTrans'
                  END IF
                  TRAN = TRANS.EQ.'T'.OR.TRANS.EQ.'C'
*
                  IF( TRAN )THEN
                     ML = N
                     NL = M
                  ELSE
                     ML = M
                     NL = N
                  END IF
*
                  DO 80 IX = 1, NINC
                     INCX = INC( IX )
                     LX = ABS( INCX )*NL
*
*                    Generate the vector X.
*
                     TRANSL = HALF
                     CALL SMAKE( 'ge', ' ', ' ', 1, NL, X, 1, XX,
     $                           ABS( INCX ), 0, NL - 1, RESET, TRANSL )
                     IF( NL.GT.1 )THEN
                        X( NL/2 ) = ZERO
                        XX( 1 + ABS( INCX )*( NL/2 - 1 ) ) = ZERO
                     END IF
*
                     DO 70 IY = 1, NINC
                        INCY = INC( IY )
                        LY = ABS( INCY )*ML
*
                        DO 60 IA = 1, NALF
                           ALPHA = ALF( IA )
*
                           DO 50 IB = 1, NBET
                              BETA = BET( IB )
*
*                             Generate the vector Y.
*
                              TRANSL = ZERO
                              CALL SMAKE( 'ge', ' ', ' ', 1, ML, Y, 1,
     $                                    YY, ABS( INCY ), 0, ML - 1,
     $                                    RESET, TRANSL )
*
                              NC = NC + 1
*
*                             Save every datum before calling the
*                             subroutine.
*
                              TRANSS = TRANS
                              MS = M
                              NS = N
                              KLS = KL
                              KUS = KU
                              ALS = ALPHA
                              DO 10 I = 1, LAA
                                 AS( I ) = AA( I )
   10                         CONTINUE
                              LDAS = LDA
                              DO 20 I = 1, LX
                                 XS( I ) = XX( I )
   20                         CONTINUE
                              INCXS = INCX
                              BLS = BETA
                              DO 30 I = 1, LY
                                 YS( I ) = YY( I )
   30                         CONTINUE
                              INCYS = INCY
*
*                             Call the subroutine.
*
                              IF( FULL )THEN
                                 IF( TRACE )
     $                              WRITE( NTRA, FMT = 9994 )NC, SNAME,
     $                              CTRANS, M, N, ALPHA, LDA, INCX,
     $                              BETA, INCY
                                 IF( REWI )
     $                              REWIND NTRA
                                 CALL CSGEMV( IORDER, TRANS, M, N,
     $                                       ALPHA, AA, LDA, XX, INCX,
     $                                       BETA, YY, INCY )
                              ELSE IF( BANDED )THEN
                                 IF( TRACE )
     $                              WRITE( NTRA, FMT = 9995 )NC, SNAME,
     $                              CTRANS, M, N, KL, KU, ALPHA, LDA,
     $                              INCX, BETA, INCY
                                 IF( REWI )
     $                              REWIND NTRA
                                 CALL CSGBMV( IORDER, TRANS, M, N, KL,
     $                                       KU, ALPHA, AA, LDA, XX,
     $                                       INCX, BETA, YY, INCY )
                              END IF
*
*                             Check if error-exit was taken incorrectly.
*
                              IF( .NOT.OK )THEN
                                 WRITE( NOUT, FMT = 9993 )
                                 FATAL = .TRUE.
                                 GO TO 130
                              END IF
*
*                             See what data changed inside subroutines.
*
                              ISAME( 1 ) = TRANS.EQ.TRANSS
                              ISAME( 2 ) = MS.EQ.M
                              ISAME( 3 ) = NS.EQ.N
                              IF( FULL )THEN
                                 ISAME( 4 ) = ALS.EQ.ALPHA
                                 ISAME( 5 ) = LSE( AS, AA, LAA )
                                 ISAME( 6 ) = LDAS.EQ.LDA
                                 ISAME( 7 ) = LSE( XS, XX, LX )
                                 ISAME( 8 ) = INCXS.EQ.INCX
                                 ISAME( 9 ) = BLS.EQ.BETA
                                 IF( NULL )THEN
                                    ISAME( 10 ) = LSE( YS, YY, LY )
                                 ELSE
                                    ISAME( 10 ) = LSERES( 'ge', ' ', 1,
     $                                            ML, YS, YY,
     $                                            ABS( INCY ) )
                                 END IF
                                 ISAME( 11 ) = INCYS.EQ.INCY
                              ELSE IF( BANDED )THEN
                                 ISAME( 4 ) = KLS.EQ.KL
                                 ISAME( 5 ) = KUS.EQ.KU
                                 ISAME( 6 ) = ALS.EQ.ALPHA
                                 ISAME( 7 ) = LSE( AS, AA, LAA )
                                 ISAME( 8 ) = LDAS.EQ.LDA
                                 ISAME( 9 ) = LSE( XS, XX, LX )
                                 ISAME( 10 ) = INCXS.EQ.INCX
                                 ISAME( 11 ) = BLS.EQ.BETA
                                 IF( NULL )THEN
                                    ISAME( 12 ) = LSE( YS, YY, LY )
                                 ELSE
                                    ISAME( 12 ) = LSERES( 'ge', ' ', 1,
     $                                            ML, YS, YY,
     $                                            ABS( INCY ) )
                                 END IF
                                 ISAME( 13 ) = INCYS.EQ.INCY
                              END IF
*
*                             If data was incorrectly changed, report
*                             and return.
*
                              SAME = .TRUE.
                              DO 40 I = 1, NARGS
                                 SAME = SAME.AND.ISAME( I )
                                 IF( .NOT.ISAME( I ) )
     $                              WRITE( NOUT, FMT = 9998 )I
   40                         CONTINUE
                              IF( .NOT.SAME )THEN
                                 FATAL = .TRUE.
                                 GO TO 130
                              END IF
*
                              IF( .NOT.NULL )THEN
*
*                                Check the result.
*
                                 CALL SMVCH( TRANS, M, N, ALPHA, A,
     $                                       NMAX, X, INCX, BETA, Y,
     $                                       INCY, YT, G, YY, EPS, ERR,
     $                                       FATAL, NOUT, .TRUE. )
                                 ERRMAX = MAX( ERRMAX, ERR )
*                                If got really bad answer, report and
*                                return.
                                 IF( FATAL )
     $                              GO TO 130
                              ELSE
*                                Avoid repeating tests with M.le.0 or
*                                N.le.0.
                                 GO TO 110
                              END IF
*
   50                      CONTINUE
*
   60                   CONTINUE
*
   70                CONTINUE
*
   80             CONTINUE
*
   90          CONTINUE
*
  100       CONTINUE
*
  110    CONTINUE
*
  120 CONTINUE
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
      GO TO 140
*
  130 CONTINUE
      WRITE( NOUT, FMT = 9996 )SNAME
      IF( FULL )THEN
         WRITE( NOUT, FMT = 9994 )NC, SNAME, CTRANS, M, N, ALPHA, LDA,
     $      INCX, BETA, INCY
      ELSE IF( BANDED )THEN
         WRITE( NOUT, FMT = 9995 )NC, SNAME, CTRANS, M, N, KL, KU,
     $      ALPHA, LDA, INCX, BETA, INCY
      END IF
*
  140 CONTINUE
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
 9998 FORMAT( ' ******* FATAL ERROR - PARAMETER NUMBER ', I2, ' WAS CH',
     $      'ANGED INCORRECTLY *******' )
 9997 FORMAT( ' ',A12, ' COMPLETED THE COMPUTATIONAL TESTS (', I6, ' C',
     $      'ALLS)', /' ******* BUT WITH MAXIMUM TEST RATIO', F8.2,
     $      ' - SUSPECT *******' )
 9996 FORMAT( ' ******* ',A12, ' FAILED ON CALL NUMBER:' )
 9995 FORMAT( 1X, I6, ': ',A12, '(', A14, ',', 4( I3, ',' ), F4.1,
     $      ', A,', I3, ',',/ 10x, 'X,', I2, ',', F4.1, ', Y,',
     $      I2, ') .' )
 9994 FORMAT( 1X, I6, ': ',A12, '(', A14, ',', 2( I3, ',' ), F4.1,
     $      ', A,', I3, ', X,', I2, ',', F4.1, ', Y,', I2,
     $      ') .' )
 9993 FORMAT( ' ******* FATAL ERROR - ERROR-EXIT TAKEN ON VALID CALL *',
     $      '******' )
*
*     End of SCHK1.
*
      END
      SUBROUTINE SCHK2( SNAME, EPS, THRESH, NOUT, NTRA, TRACE, REWI,
     $                  FATAL, NIDIM, IDIM, NKB, KB, NALF, ALF, NBET,
     $                  BET, NINC, INC, NMAX, INCMAX, A, AA, AS, X, XX,
     $                  XS, Y, YY, YS, YT, G, IORDER )
*
*  Tests SSYMV, SSBMV and SSPMV.
*
*  Auxiliary routine for test program for Level 2 Blas.
*
*  -- Written on 10-August-1987.
*     Richard Hanson, Sandia National Labs.
*     Jeremy Du Croz, NAG Central Office.
*
*     .. Parameters ..
      REAL               ZERO, HALF
      PARAMETER          ( ZERO = 0.0, HALF = 0.5 )
*     .. Scalar Arguments ..
      REAL               EPS, THRESH
      INTEGER            INCMAX, NALF, NBET, NIDIM, NINC, NKB, NMAX,
     $                   NOUT, NTRA, IORDER
      LOGICAL            FATAL, REWI, TRACE
      CHARACTER*12       SNAME
*     .. Array Arguments ..
      REAL               A( NMAX, NMAX ), AA( NMAX*NMAX ), ALF( NALF ),
     $                   AS( NMAX*NMAX ), BET( NBET ), G( NMAX ),
     $                   X( NMAX ), XS( NMAX*INCMAX ),
     $                   XX( NMAX*INCMAX ), Y( NMAX ),
     $                   YS( NMAX*INCMAX ), YT( NMAX ),
     $                   YY( NMAX*INCMAX )
      INTEGER            IDIM( NIDIM ), INC( NINC ), KB( NKB )
*     .. Local Scalars ..
      REAL               ALPHA, ALS, BETA, BLS, ERR, ERRMAX, TRANSL
      INTEGER            I, IA, IB, IC, IK, IN, INCX, INCXS, INCY,
     $                   INCYS, IX, IY, K, KS, LAA, LDA, LDAS, LX, LY,
     $                   N, NARGS, NC, NK, NS
      LOGICAL            BANDED, FULL, NULL, PACKED, RESET, SAME
      CHARACTER*1        UPLO, UPLOS
      CHARACTER*14       CUPLO
      CHARACTER*2        ICH
*     .. Local Arrays ..
      LOGICAL            ISAME( 13 )
*     .. External Functions ..
      LOGICAL            LSE, LSERES
      EXTERNAL           LSE, LSERES
*     .. External Subroutines ..
      EXTERNAL           SMAKE, SMVCH, CSSBMV, CSSPMV, CSSYMV
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX
*     .. Scalars in Common ..
      INTEGER            INFOT, NOUTC
      LOGICAL            OK
*     .. Common blocks ..
      COMMON             /INFOC/INFOT, NOUTC, OK
*     .. Data statements ..
      DATA               ICH/'UL'/
*     .. Executable Statements ..
      FULL = SNAME( 9: 9 ).EQ.'y'
      BANDED = SNAME( 9: 9 ).EQ.'b'
      PACKED = SNAME( 9: 9 ).EQ.'p'
*     Define the number of arguments.
      IF( FULL )THEN
         NARGS = 10
      ELSE IF( BANDED )THEN
         NARGS = 11
      ELSE IF( PACKED )THEN
         NARGS = 9
      END IF
*
      NC = 0
      RESET = .TRUE.
      ERRMAX = ZERO
*
      DO 110 IN = 1, NIDIM
         N = IDIM( IN )
*
         IF( BANDED )THEN
            NK = NKB
         ELSE
            NK = 1
         END IF
         DO 100 IK = 1, NK
            IF( BANDED )THEN
               K = KB( IK )
            ELSE
               K = N - 1
            END IF
*           Set LDA to 1 more than minimum value if room.
            IF( BANDED )THEN
               LDA = K + 1
            ELSE
               LDA = N
            END IF
            IF( LDA.LT.NMAX )
     $         LDA = LDA + 1
*           Skip tests if not enough room.
            IF( LDA.GT.NMAX )
     $         GO TO 100
            IF( PACKED )THEN
               LAA = ( N*( N + 1 ) )/2
            ELSE
               LAA = LDA*N
            END IF
            NULL = N.LE.0
*
            DO 90 IC = 1, 2
               UPLO = ICH( IC: IC )
               IF (UPLO.EQ.'U')THEN
                  CUPLO = '    CblasUpper'
               ELSE
                  CUPLO = '    CblasLower'
               END IF
*
*              Generate the matrix A.
*
               TRANSL = ZERO
               CALL SMAKE( SNAME( 8: 9 ), UPLO, ' ', N, N, A, NMAX, AA,
     $                     LDA, K, K, RESET, TRANSL )
*
               DO 80 IX = 1, NINC
                  INCX = INC( IX )
                  LX = ABS( INCX )*N
*
*                 Generate the vector X.
*
                  TRANSL = HALF
                  CALL SMAKE( 'ge', ' ', ' ', 1, N, X, 1, XX,
     $                        ABS( INCX ), 0, N - 1, RESET, TRANSL )
                  IF( N.GT.1 )THEN
                     X( N/2 ) = ZERO
                     XX( 1 + ABS( INCX )*( N/2 - 1 ) ) = ZERO
                  END IF
*
                  DO 70 IY = 1, NINC
                     INCY = INC( IY )
                     LY = ABS( INCY )*N
*
                     DO 60 IA = 1, NALF
                        ALPHA = ALF( IA )
*
                        DO 50 IB = 1, NBET
                           BETA = BET( IB )
*
*                          Generate the vector Y.
*
                           TRANSL = ZERO
                           CALL SMAKE( 'ge', ' ', ' ', 1, N, Y, 1, YY,
     $                                 ABS( INCY ), 0, N - 1, RESET,
     $                                 TRANSL )
*
                           NC = NC + 1
*
*                          Save every datum before calling the
*                          subroutine.
*
                           UPLOS = UPLO
                           NS = N
                           KS = K
                           ALS = ALPHA
                           DO 10 I = 1, LAA
                              AS( I ) = AA( I )
   10                      CONTINUE
                           LDAS = LDA
                           DO 20 I = 1, LX
                              XS( I ) = XX( I )
   20                      CONTINUE
                           INCXS = INCX
                           BLS = BETA
                           DO 30 I = 1, LY
                              YS( I ) = YY( I )
   30                      CONTINUE
                           INCYS = INCY
*
*                          Call the subroutine.
*
                           IF( FULL )THEN
                              IF( TRACE )
     $                           WRITE( NTRA, FMT = 9993 )NC, SNAME,
     $                           CUPLO, N, ALPHA, LDA, INCX, BETA, INCY
                              IF( REWI )
     $                           REWIND NTRA
                              CALL CSSYMV( IORDER, UPLO, N, ALPHA, AA,
     $                                   LDA, XX, INCX, BETA, YY, INCY )
                           ELSE IF( BANDED )THEN
                              IF( TRACE )
     $                           WRITE( NTRA, FMT = 9994 )NC, SNAME,
     $                           CUPLO, N, K, ALPHA, LDA, INCX, BETA,
     $                           INCY
                              IF( REWI )
     $                           REWIND NTRA
                              CALL CSSBMV( IORDER, UPLO, N, K, ALPHA,
     $                                    AA, LDA, XX, INCX, BETA, YY,
     $					  INCY )
                           ELSE IF( PACKED )THEN
                              IF( TRACE )
     $                           WRITE( NTRA, FMT = 9995 )NC, SNAME,
     $                           CUPLO, N, ALPHA, INCX, BETA, INCY
                              IF( REWI )
     $                           REWIND NTRA
                              CALL CSSPMV( IORDER, UPLO, N, ALPHA, AA,
     $                                    XX, INCX, BETA, YY, INCY )
                           END IF
*
*                          Check if error-exit was taken incorrectly.
*
                           IF( .NOT.OK )THEN
                              WRITE( NOUT, FMT = 9992 )
                              FATAL = .TRUE.
                              GO TO 120
                           END IF
*
*                          See what data changed inside subroutines.
*
                           ISAME( 1 ) = UPLO.EQ.UPLOS
                           ISAME( 2 ) = NS.EQ.N
                           IF( FULL )THEN
                              ISAME( 3 ) = ALS.EQ.ALPHA
                              ISAME( 4 ) = LSE( AS, AA, LAA )
                              ISAME( 5 ) = LDAS.EQ.LDA
                              ISAME( 6 ) = LSE( XS, XX, LX )
                              ISAME( 7 ) = INCXS.EQ.INCX
                              ISAME( 8 ) = BLS.EQ.BETA
                              IF( NULL )THEN
                                 ISAME( 9 ) = LSE( YS, YY, LY )
                              ELSE
                                 ISAME( 9 ) = LSERES( 'ge', ' ', 1, N,
     $                                        YS, YY, ABS( INCY ) )
                              END IF
                              ISAME( 10 ) = INCYS.EQ.INCY
                           ELSE IF( BANDED )THEN
                              ISAME( 3 ) = KS.EQ.K
                              ISAME( 4 ) = ALS.EQ.ALPHA
                              ISAME( 5 ) = LSE( AS, AA, LAA )
                              ISAME( 6 ) = LDAS.EQ.LDA
                              ISAME( 7 ) = LSE( XS, XX, LX )
                              ISAME( 8 ) = INCXS.EQ.INCX
                              ISAME( 9 ) = BLS.EQ.BETA
                              IF( NULL )THEN
                                 ISAME( 10 ) = LSE( YS, YY, LY )
                              ELSE
                                 ISAME( 10 ) = LSERES( 'ge', ' ', 1, N,
     $                                         YS, YY, ABS( INCY ) )
                              END IF
                              ISAME( 11 ) = INCYS.EQ.INCY
                           ELSE IF( PACKED )THEN
                              ISAME( 3 ) = ALS.EQ.ALPHA
                              ISAME( 4 ) = LSE( AS, AA, LAA )
                              ISAME( 5 ) = LSE( XS, XX, LX )
                              ISAME( 6 ) = INCXS.EQ.INCX
                              ISAME( 7 ) = BLS.EQ.BETA
                              IF( NULL )THEN
                                 ISAME( 8 ) = LSE( YS, YY, LY )
                              ELSE
                                 ISAME( 8 ) = LSERES( 'ge', ' ', 1, N,
     $                                        YS, YY, ABS( INCY ) )
                              END IF
                              ISAME( 9 ) = INCYS.EQ.INCY
                           END IF
*
*                          If data was incorrectly changed, report and
*                          return.
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
                              CALL SMVCH( 'N', N, N, ALPHA, A, NMAX, X,
     $                                    INCX, BETA, Y, INCY, YT, G,
     $                                    YY, EPS, ERR, FATAL, NOUT,
     $                                    .TRUE. )
                              ERRMAX = MAX( ERRMAX, ERR )
*                             If got really bad answer, report and
*                             return.
                              IF( FATAL )
     $                           GO TO 120
                           ELSE
*                             Avoid repeating tests with N.le.0
                              GO TO 110
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
      IF( FULL )THEN
         WRITE( NOUT, FMT = 9993 )NC, SNAME, CUPLO, N, ALPHA, LDA,
     $         INCX, BETA, INCY
      ELSE IF( BANDED )THEN
         WRITE( NOUT, FMT = 9994 )NC, SNAME, CUPLO, N, K, ALPHA, LDA,
     $      INCX, BETA, INCY
      ELSE IF( PACKED )THEN
         WRITE( NOUT, FMT = 9995 )NC, SNAME, CUPLO, N, ALPHA, INCX,
     $      BETA, INCY
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
 9998 FORMAT( ' ******* FATAL ERROR - PARAMETER NUMBER ', I2, ' WAS CH',
     $      'ANGED INCORRECTLY *******' )
 9997 FORMAT( ' ',A12, ' COMPLETED THE COMPUTATIONAL TESTS (', I6, ' C',
     $      'ALLS)', /' ******* BUT WITH MAXIMUM TEST RATIO', F8.2,
     $      ' - SUSPECT *******' )
 9996 FORMAT( ' ******* ',A12, ' FAILED ON CALL NUMBER:' )
 9995 FORMAT( 1X, I6, ': ',A12, '(', A14, ',', I3, ',', F4.1, ', AP',
     $      ', X,', I2, ',', F4.1, ', Y,', I2, ') .' )
 9994 FORMAT( 1X, I6, ': ',A12, '(', A14, ',', 2( I3, ',' ), F4.1,
     $      ', A,', I3, ', X,', I2, ',', F4.1, ', Y,', I2,
     $      ') .' )
 9993 FORMAT( 1X, I6, ': ',A12, '(', A14, ',', I3, ',', F4.1, ', A,',
     $      I3, ', X,', I2, ',', F4.1, ', Y,', I2, ') .' )
 9992 FORMAT( ' ******* FATAL ERROR - ERROR-EXIT TAKEN ON VALID CALL *',
     $      '******' )
*
*     End of SCHK2.
*
      END
      SUBROUTINE SCHK3( SNAME, EPS, THRESH, NOUT, NTRA, TRACE, REWI,
     $                  FATAL, NIDIM, IDIM, NKB, KB, NINC, INC, NMAX,
     $                  INCMAX, A, AA, AS, X, XX, XS, XT, G, Z, IORDER )
*
*  Tests STRMV, STBMV, STPMV, STRSV, STBSV and STPSV.
*
*  Auxiliary routine for test program for Level 2 Blas.
*
*  -- Written on 10-August-1987.
*     Richard Hanson, Sandia National Labs.
*     Jeremy Du Croz, NAG Central Office.
*
*     .. Parameters ..
      REAL               ZERO, HALF, ONE
      PARAMETER          ( ZERO = 0.0, HALF = 0.5, ONE = 1.0 )
*     .. Scalar Arguments ..
      REAL               EPS, THRESH
      INTEGER            INCMAX, NIDIM, NINC, NKB, NMAX, NOUT, NTRA,
     $			 IORDER
      LOGICAL            FATAL, REWI, TRACE
      CHARACTER*12       SNAME
*     .. Array Arguments ..
      REAL               A( NMAX, NMAX ), AA( NMAX*NMAX ),
     $                   AS( NMAX*NMAX ), G( NMAX ), X( NMAX ),
     $                   XS( NMAX*INCMAX ), XT( NMAX ),
     $                   XX( NMAX*INCMAX ), Z( NMAX )
      INTEGER            IDIM( NIDIM ), INC( NINC ), KB( NKB )
*     .. Local Scalars ..
      REAL               ERR, ERRMAX, TRANSL
      INTEGER            I, ICD, ICT, ICU, IK, IN, INCX, INCXS, IX, K,
     $                   KS, LAA, LDA, LDAS, LX, N, NARGS, NC, NK, NS
      LOGICAL            BANDED, FULL, NULL, PACKED, RESET, SAME
      CHARACTER*1        DIAG, DIAGS, TRANS, TRANSS, UPLO, UPLOS
      CHARACTER*14       CUPLO,CTRANS,CDIAG
      CHARACTER*2        ICHD, ICHU
      CHARACTER*3        ICHT
*     .. Local Arrays ..
      LOGICAL            ISAME( 13 )
*     .. External Functions ..
      LOGICAL            LSE, LSERES
      EXTERNAL           LSE, LSERES
*     .. External Subroutines ..
      EXTERNAL           SMAKE, SMVCH, CSTBMV, CSTBSV, CSTPMV,
     $			 CSTPSV, CSTRMV,  CSTRSV
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX
*     .. Scalars in Common ..
      INTEGER            INFOT, NOUTC
      LOGICAL            OK
*     .. Common blocks ..
      COMMON             /INFOC/INFOT, NOUTC, OK
*     .. Data statements ..
      DATA               ICHU/'UL'/, ICHT/'NTC'/, ICHD/'UN'/
*     .. Executable Statements ..
      FULL = SNAME( 9: 9 ).EQ.'r'
      BANDED = SNAME( 9: 9 ).EQ.'b'
      PACKED = SNAME( 9: 9 ).EQ.'p'
*     Define the number of arguments.
      IF( FULL )THEN
         NARGS = 8
      ELSE IF( BANDED )THEN
         NARGS = 9
      ELSE IF( PACKED )THEN
         NARGS = 7
      END IF
*
      NC = 0
      RESET = .TRUE.
      ERRMAX = ZERO
*     Set up zero vector for SMVCH.
      DO 10 I = 1, NMAX
         Z( I ) = ZERO
   10 CONTINUE
*
      DO 110 IN = 1, NIDIM
         N = IDIM( IN )
*
         IF( BANDED )THEN
            NK = NKB
         ELSE
            NK = 1
         END IF
         DO 100 IK = 1, NK
            IF( BANDED )THEN
               K = KB( IK )
            ELSE
               K = N - 1
            END IF
*           Set LDA to 1 more than minimum value if room.
            IF( BANDED )THEN
               LDA = K + 1
            ELSE
               LDA = N
            END IF
            IF( LDA.LT.NMAX )
     $         LDA = LDA + 1
*           Skip tests if not enough room.
            IF( LDA.GT.NMAX )
     $         GO TO 100
            IF( PACKED )THEN
               LAA = ( N*( N + 1 ) )/2
            ELSE
               LAA = LDA*N
            END IF
            NULL = N.LE.0
*
            DO 90 ICU = 1, 2
               UPLO = ICHU( ICU: ICU )
               IF (UPLO.EQ.'U')THEN
                  CUPLO = '    CblasUpper'
               ELSE
                  CUPLO = '    CblasLower'
               END IF
*
               DO 80 ICT = 1, 3
                  TRANS = ICHT( ICT: ICT )
                  IF (TRANS.EQ.'N')THEN
                     CTRANS = '  CblasNoTrans'
                  ELSE IF (TRANS.EQ.'T')THEN
                     CTRANS = '    CblasTrans'
                  ELSE
                     CTRANS = 'CblasConjTrans'
                  END IF
*
                  DO 70 ICD = 1, 2
                     DIAG = ICHD( ICD: ICD )
                     IF (DIAG.EQ.'N')THEN
                        CDIAG = '  CblasNonUnit'
                     ELSE
                        CDIAG = '     CblasUnit'
                     END IF
*
*                    Generate the matrix A.
*
                     TRANSL = ZERO
                     CALL SMAKE( SNAME( 8: 9 ), UPLO, DIAG, N, N, A,
     $                           NMAX, AA, LDA, K, K, RESET, TRANSL )
*
                     DO 60 IX = 1, NINC
                        INCX = INC( IX )
                        LX = ABS( INCX )*N
*
*                       Generate the vector X.
*
                        TRANSL = HALF
                        CALL SMAKE( 'ge', ' ', ' ', 1, N, X, 1, XX,
     $                              ABS( INCX ), 0, N - 1, RESET,
     $                              TRANSL )
                        IF( N.GT.1 )THEN
                           X( N/2 ) = ZERO
                           XX( 1 + ABS( INCX )*( N/2 - 1 ) ) = ZERO
                        END IF
*
                        NC = NC + 1
*
*                       Save every datum before calling the subroutine.
*
                        UPLOS = UPLO
                        TRANSS = TRANS
                        DIAGS = DIAG
                        NS = N
                        KS = K
                        DO 20 I = 1, LAA
                           AS( I ) = AA( I )
   20                   CONTINUE
                        LDAS = LDA
                        DO 30 I = 1, LX
                           XS( I ) = XX( I )
   30                   CONTINUE
                        INCXS = INCX
*
*                       Call the subroutine.
*
                        IF( SNAME( 10: 11 ).EQ.'mv' )THEN
                           IF( FULL )THEN
                              IF( TRACE )
     $                           WRITE( NTRA, FMT = 9993 )NC, SNAME,
     $                           CUPLO, CTRANS, CDIAG, N, LDA, INCX
                              IF( REWI )
     $                           REWIND NTRA
                              CALL CSTRMV( IORDER, UPLO, TRANS, DIAG,
     $                                    N, AA, LDA, XX, INCX )
                           ELSE IF( BANDED )THEN
                              IF( TRACE )
     $                           WRITE( NTRA, FMT = 9994 )NC, SNAME,
     $                           CUPLO, CTRANS, CDIAG, N, K, LDA, INCX
                              IF( REWI )
     $                           REWIND NTRA
                              CALL CSTBMV( IORDER, UPLO, TRANS, DIAG,
     $                                    N, K, AA, LDA, XX, INCX )
                           ELSE IF( PACKED )THEN
                              IF( TRACE )
     $                           WRITE( NTRA, FMT = 9995 )NC, SNAME,
     $                           CUPLO, CTRANS, CDIAG, N, INCX
                              IF( REWI )
     $                           REWIND NTRA
                              CALL CSTPMV( IORDER, UPLO, TRANS, DIAG,
     $                                    N, AA, XX, INCX )
                           END IF
                        ELSE IF( SNAME( 10: 11 ).EQ.'sv' )THEN
                           IF( FULL )THEN
                              IF( TRACE )
     $                           WRITE( NTRA, FMT = 9993 )NC, SNAME,
     $                           CUPLO, CTRANS, CDIAG, N, LDA, INCX
                              IF( REWI )
     $                           REWIND NTRA
                              CALL CSTRSV( IORDER, UPLO, TRANS, DIAG,
     $                                    N, AA, LDA, XX, INCX )
                           ELSE IF( BANDED )THEN
                              IF( TRACE )
     $                           WRITE( NTRA, FMT = 9994 )NC, SNAME,
     $                           CUPLO, CTRANS, CDIAG, N, K, LDA, INCX
                              IF( REWI )
     $                           REWIND NTRA
                              CALL CSTBSV( IORDER, UPLO, TRANS, DIAG,
     $                                    N, K, AA, LDA, XX, INCX )
                           ELSE IF( PACKED )THEN
                              IF( TRACE )
     $                           WRITE( NTRA, FMT = 9995 )NC, SNAME,
     $                           CUPLO, CTRANS, CDIAG, N, INCX
                              IF( REWI )
     $                           REWIND NTRA
                              CALL CSTPSV( IORDER, UPLO, TRANS, DIAG,
     $                                    N, AA, XX, INCX )
                           END IF
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
                        ISAME( 1 ) = UPLO.EQ.UPLOS
                        ISAME( 2 ) = TRANS.EQ.TRANSS
                        ISAME( 3 ) = DIAG.EQ.DIAGS
                        ISAME( 4 ) = NS.EQ.N
                        IF( FULL )THEN
                           ISAME( 5 ) = LSE( AS, AA, LAA )
                           ISAME( 6 ) = LDAS.EQ.LDA
                           IF( NULL )THEN
                              ISAME( 7 ) = LSE( XS, XX, LX )
                           ELSE
                              ISAME( 7 ) = LSERES( 'ge', ' ', 1, N, XS,
     $                                     XX, ABS( INCX ) )
                           END IF
                           ISAME( 8 ) = INCXS.EQ.INCX
                        ELSE IF( BANDED )THEN
                           ISAME( 5 ) = KS.EQ.K
                           ISAME( 6 ) = LSE( AS, AA, LAA )
                           ISAME( 7 ) = LDAS.EQ.LDA
                           IF( NULL )THEN
                              ISAME( 8 ) = LSE( XS, XX, LX )
                           ELSE
                              ISAME( 8 ) = LSERES( 'ge', ' ', 1, N, XS,
     $                                     XX, ABS( INCX ) )
                           END IF
                           ISAME( 9 ) = INCXS.EQ.INCX
                        ELSE IF( PACKED )THEN
                           ISAME( 5 ) = LSE( AS, AA, LAA )
                           IF( NULL )THEN
                              ISAME( 6 ) = LSE( XS, XX, LX )
                           ELSE
                              ISAME( 6 ) = LSERES( 'ge', ' ', 1, N, XS,
     $                                     XX, ABS( INCX ) )
                           END IF
                           ISAME( 7 ) = INCXS.EQ.INCX
                        END IF
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
                           GO TO 120
                        END IF
*
                        IF( .NOT.NULL )THEN
                           IF( SNAME( 10: 11 ).EQ.'mv' )THEN
*
*                             Check the result.
*
                              CALL SMVCH( TRANS, N, N, ONE, A, NMAX, X,
     $                                    INCX, ZERO, Z, INCX, XT, G,
     $                                    XX, EPS, ERR, FATAL, NOUT,
     $                                    .TRUE. )
                           ELSE IF( SNAME( 10: 11 ).EQ.'sv' )THEN
*
*                             Compute approximation to original vector.
*
                              DO 50 I = 1, N
                                 Z( I ) = XX( 1 + ( I - 1 )*
     $                                    ABS( INCX ) )
                                 XX( 1 + ( I - 1 )*ABS( INCX ) )
     $                              = X( I )
   50                         CONTINUE
                              CALL SMVCH( TRANS, N, N, ONE, A, NMAX, Z,
     $                                    INCX, ZERO, X, INCX, XT, G,
     $                                    XX, EPS, ERR, FATAL, NOUT,
     $                                    .FALSE. )
                           END IF
                           ERRMAX = MAX( ERRMAX, ERR )
*                          If got really bad answer, report and return.
                           IF( FATAL )
     $                        GO TO 120
                        ELSE
*                          Avoid repeating tests with N.le.0.
                           GO TO 110
                        END IF
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
      IF( FULL )THEN
         WRITE( NOUT, FMT = 9993 )NC, SNAME, CUPLO, CTRANS, CDIAG, N,
     $          LDA, INCX
      ELSE IF( BANDED )THEN
         WRITE( NOUT, FMT = 9994 )NC, SNAME, CUPLO, CTRANS, CDIAG, N,
     $          K, LDA, INCX
      ELSE IF( PACKED )THEN
         WRITE( NOUT, FMT = 9995 )NC, SNAME, CUPLO, CTRANS, CDIAG, N,
     $          INCX
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
 9998 FORMAT( ' ******* FATAL ERROR - PARAMETER NUMBER ', I2, ' WAS CH',
     $      'ANGED INCORRECTLY *******' )
 9997 FORMAT( ' ',A12, ' COMPLETED THE COMPUTATIONAL TESTS (', I6, ' C',
     $      'ALLS)', /' ******* BUT WITH MAXIMUM TEST RATIO', F8.2,
     $      ' - SUSPECT *******' )
 9996 FORMAT( ' ******* ',A12, ' FAILED ON CALL NUMBER:' )
 9995 FORMAT( 1X, I6, ': ',A12, '(', 3( A14,',' ),/ 10x, I3, ', AP, ',
     $      'X,', I2, ') .' )
 9994 FORMAT( 1X, I6, ': ',A12, '(', 3( A14,',' ),/ 10x, 2( I3, ',' ),
     $      ' A,', I3, ', X,', I2, ') .' )
 9993 FORMAT( 1X, I6, ': ',A12, '(', 3( A14,',' ),/ 10x, I3, ', A,',
     $      I3, ', X,', I2, ') .' )
 9992 FORMAT( ' ******* FATAL ERROR - ERROR-EXIT TAKEN ON VALID CALL *',
     $      '******' )
*
*     End of SCHK3.
*
      END
      SUBROUTINE SCHK4( SNAME, EPS, THRESH, NOUT, NTRA, TRACE, REWI,
     $                  FATAL, NIDIM, IDIM, NALF, ALF, NINC, INC, NMAX,
     $                  INCMAX, A, AA, AS, X, XX, XS, Y, YY, YS, YT, G,
     $                  Z, IORDER )
*
*  Tests SGER.
*
*  Auxiliary routine for test program for Level 2 Blas.
*
*  -- Written on 10-August-1987.
*     Richard Hanson, Sandia National Labs.
*     Jeremy Du Croz, NAG Central Office.
*
*     .. Parameters ..
      REAL               ZERO, HALF, ONE
      PARAMETER          ( ZERO = 0.0, HALF = 0.5, ONE = 1.0 )
*     .. Scalar Arguments ..
      REAL               EPS, THRESH
      INTEGER            INCMAX, NALF, NIDIM, NINC, NMAX, NOUT, NTRA,
     $			 IORDER
      LOGICAL            FATAL, REWI, TRACE
      CHARACTER*12       SNAME
*     .. Array Arguments ..
      REAL               A( NMAX, NMAX ), AA( NMAX*NMAX ), ALF( NALF ),
     $                   AS( NMAX*NMAX ), G( NMAX ), X( NMAX ),
     $                   XS( NMAX*INCMAX ), XX( NMAX*INCMAX ),
     $                   Y( NMAX ), YS( NMAX*INCMAX ), YT( NMAX ),
     $                   YY( NMAX*INCMAX ), Z( NMAX )
      INTEGER            IDIM( NIDIM ), INC( NINC )
*     .. Local Scalars ..
      REAL               ALPHA, ALS, ERR, ERRMAX, TRANSL
      INTEGER            I, IA, IM, IN, INCX, INCXS, INCY, INCYS, IX,
     $                   IY, J, LAA, LDA, LDAS, LX, LY, M, MS, N, NARGS,
     $                   NC, ND, NS
      LOGICAL            NULL, RESET, SAME
*     .. Local Arrays ..
      REAL               W( 1 )
      LOGICAL            ISAME( 13 )
*     .. External Functions ..
      LOGICAL            LSE, LSERES
      EXTERNAL           LSE, LSERES
*     .. External Subroutines ..
      EXTERNAL           CSGER, SMAKE, SMVCH
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, MIN
*     .. Scalars in Common ..
      INTEGER            INFOT, NOUTC
      LOGICAL            OK
*     .. Common blocks ..
      COMMON             /INFOC/INFOT, NOUTC, OK
*     .. Executable Statements ..
*     Define the number of arguments.
      NARGS = 9
*
      NC = 0
      RESET = .TRUE.
      ERRMAX = ZERO
*
      DO 120 IN = 1, NIDIM
         N = IDIM( IN )
         ND = N/2 + 1
*
         DO 110 IM = 1, 2
            IF( IM.EQ.1 )
     $         M = MAX( N - ND, 0 )
            IF( IM.EQ.2 )
     $         M = MIN( N + ND, NMAX )
*
*           Set LDA to 1 more than minimum value if room.
            LDA = M
            IF( LDA.LT.NMAX )
     $         LDA = LDA + 1
*           Skip tests if not enough room.
            IF( LDA.GT.NMAX )
     $         GO TO 110
            LAA = LDA*N
            NULL = N.LE.0.OR.M.LE.0
*
            DO 100 IX = 1, NINC
               INCX = INC( IX )
               LX = ABS( INCX )*M
*
*              Generate the vector X.
*
               TRANSL = HALF
               CALL SMAKE( 'ge', ' ', ' ', 1, M, X, 1, XX, ABS( INCX ),
     $                     0, M - 1, RESET, TRANSL )
               IF( M.GT.1 )THEN
                  X( M/2 ) = ZERO
                  XX( 1 + ABS( INCX )*( M/2 - 1 ) ) = ZERO
               END IF
*
               DO 90 IY = 1, NINC
                  INCY = INC( IY )
                  LY = ABS( INCY )*N
*
*                 Generate the vector Y.
*
                  TRANSL = ZERO
                  CALL SMAKE( 'ge', ' ', ' ', 1, N, Y, 1, YY,
     $                        ABS( INCY ), 0, N - 1, RESET, TRANSL )
                  IF( N.GT.1 )THEN
                     Y( N/2 ) = ZERO
                     YY( 1 + ABS( INCY )*( N/2 - 1 ) ) = ZERO
                  END IF
*
                  DO 80 IA = 1, NALF
                     ALPHA = ALF( IA )
*
*                    Generate the matrix A.
*
                     TRANSL = ZERO
                     CALL SMAKE( SNAME( 8: 9 ), ' ', ' ', M, N, A, NMAX,
     $                           AA, LDA, M - 1, N - 1, RESET, TRANSL )
*
                     NC = NC + 1
*
*                    Save every datum before calling the subroutine.
*
                     MS = M
                     NS = N
                     ALS = ALPHA
                     DO 10 I = 1, LAA
                        AS( I ) = AA( I )
   10                CONTINUE
                     LDAS = LDA
                     DO 20 I = 1, LX
                        XS( I ) = XX( I )
   20                CONTINUE
                     INCXS = INCX
                     DO 30 I = 1, LY
                        YS( I ) = YY( I )
   30                CONTINUE
                     INCYS = INCY
*
*                    Call the subroutine.
*
                     IF( TRACE )
     $                  WRITE( NTRA, FMT = 9994 )NC, SNAME, M, N,
     $                  ALPHA, INCX, INCY, LDA
                     IF( REWI )
     $                  REWIND NTRA
                     CALL CSGER( IORDER, M, N, ALPHA, XX, INCX, YY,
     $                          INCY, AA, LDA )
*
*                    Check if error-exit was taken incorrectly.
*
                     IF( .NOT.OK )THEN
                        WRITE( NOUT, FMT = 9993 )
                        FATAL = .TRUE.
                        GO TO 140
                     END IF
*
*                    See what data changed inside subroutine.
*
                     ISAME( 1 ) = MS.EQ.M
                     ISAME( 2 ) = NS.EQ.N
                     ISAME( 3 ) = ALS.EQ.ALPHA
                     ISAME( 4 ) = LSE( XS, XX, LX )
                     ISAME( 5 ) = INCXS.EQ.INCX
                     ISAME( 6 ) = LSE( YS, YY, LY )
                     ISAME( 7 ) = INCYS.EQ.INCY
                     IF( NULL )THEN
                        ISAME( 8 ) = LSE( AS, AA, LAA )
                     ELSE
                        ISAME( 8 ) = LSERES( 'ge', ' ', M, N, AS, AA,
     $                               LDA )
                     END IF
                     ISAME( 9 ) = LDAS.EQ.LDA
*
*                    If data was incorrectly changed, report and return.
*
                     SAME = .TRUE.
                     DO 40 I = 1, NARGS
                        SAME = SAME.AND.ISAME( I )
                        IF( .NOT.ISAME( I ) )
     $                     WRITE( NOUT, FMT = 9998 )I
   40                CONTINUE
                     IF( .NOT.SAME )THEN
                        FATAL = .TRUE.
                        GO TO 140
                     END IF
*
                     IF( .NOT.NULL )THEN
*
*                       Check the result column by column.
*
                        IF( INCX.GT.0 )THEN
                           DO 50 I = 1, M
                              Z( I ) = X( I )
   50                      CONTINUE
                        ELSE
                           DO 60 I = 1, M
                              Z( I ) = X( M - I + 1 )
   60                      CONTINUE
                        END IF
                        DO 70 J = 1, N
                           IF( INCY.GT.0 )THEN
                              W( 1 ) = Y( J )
                           ELSE
                              W( 1 ) = Y( N - J + 1 )
                           END IF
                           CALL SMVCH( 'N', M, 1, ALPHA, Z, NMAX, W, 1,
     $                                 ONE, A( 1, J ), 1, YT, G,
     $                                 AA( 1 + ( J - 1 )*LDA ), EPS,
     $                                 ERR, FATAL, NOUT, .TRUE. )
                           ERRMAX = MAX( ERRMAX, ERR )
*                          If got really bad answer, report and return.
                           IF( FATAL )
     $                        GO TO 130
   70                   CONTINUE
                     ELSE
*                       Avoid repeating tests with M.le.0 or N.le.0.
                        GO TO 110
                     END IF
*
   80             CONTINUE
*
   90          CONTINUE
*
  100       CONTINUE
*
  110    CONTINUE
*
  120 CONTINUE
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
      GO TO 150
*
  130 CONTINUE
      WRITE( NOUT, FMT = 9995 )J
*
  140 CONTINUE
      WRITE( NOUT, FMT = 9996 )SNAME
      WRITE( NOUT, FMT = 9994 )NC, SNAME, M, N, ALPHA, INCX, INCY, LDA
*
  150 CONTINUE
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
 9998 FORMAT( ' ******* FATAL ERROR - PARAMETER NUMBER ', I2, ' WAS CH',
     $      'ANGED INCORRECTLY *******' )
 9997 FORMAT( ' ',A12, ' COMPLETED THE COMPUTATIONAL TESTS (', I6, ' C',
     $      'ALLS)', /' ******* BUT WITH MAXIMUM TEST RATIO', F8.2,
     $      ' - SUSPECT *******' )
 9996 FORMAT( ' ******* ',A12, ' FAILED ON CALL NUMBER:' )
 9995 FORMAT( '      THESE ARE THE RESULTS FOR COLUMN ', I3 )
 9994 FORMAT( 1X, I6, ': ',A12, '(', 2( I3, ',' ), F4.1, ', X,', I2,
     $      ', Y,', I2, ', A,', I3, ')                  .' )
 9993 FORMAT( ' ******* FATAL ERROR - ERROR-EXIT TAKEN ON VALID CALL *',
     $      '******' )
*
*     End of SCHK4.
*
      END
      SUBROUTINE SCHK5( SNAME, EPS, THRESH, NOUT, NTRA, TRACE, REWI,
     $                  FATAL, NIDIM, IDIM, NALF, ALF, NINC, INC, NMAX,
     $                  INCMAX, A, AA, AS, X, XX, XS, Y, YY, YS, YT, G,
     $                  Z, IORDER )
*
*  Tests SSYR and SSPR.
*
*  Auxiliary routine for test program for Level 2 Blas.
*
*  -- Written on 10-August-1987.
*     Richard Hanson, Sandia National Labs.
*     Jeremy Du Croz, NAG Central Office.
*
*     .. Parameters ..
      REAL               ZERO, HALF, ONE
      PARAMETER          ( ZERO = 0.0, HALF = 0.5, ONE = 1.0 )
*     .. Scalar Arguments ..
      REAL               EPS, THRESH
      INTEGER            INCMAX, NALF, NIDIM, NINC, NMAX, NOUT, NTRA,
     $			 IORDER
      LOGICAL            FATAL, REWI, TRACE
      CHARACTER*12       SNAME
*     .. Array Arguments ..
      REAL               A( NMAX, NMAX ), AA( NMAX*NMAX ), ALF( NALF ),
     $                   AS( NMAX*NMAX ), G( NMAX ), X( NMAX ),
     $                   XS( NMAX*INCMAX ), XX( NMAX*INCMAX ),
     $                   Y( NMAX ), YS( NMAX*INCMAX ), YT( NMAX ),
     $                   YY( NMAX*INCMAX ), Z( NMAX )
      INTEGER            IDIM( NIDIM ), INC( NINC )
*     .. Local Scalars ..
      REAL               ALPHA, ALS, ERR, ERRMAX, TRANSL
      INTEGER            I, IA, IC, IN, INCX, INCXS, IX, J, JA, JJ, LAA,
     $                   LDA, LDAS, LJ, LX, N, NARGS, NC, NS
      LOGICAL            FULL, NULL, PACKED, RESET, SAME, UPPER
      CHARACTER*1        UPLO, UPLOS
      CHARACTER*14       CUPLO
      CHARACTER*2        ICH
*     .. Local Arrays ..
      REAL               W( 1 )
      LOGICAL            ISAME( 13 )
*     .. External Functions ..
      LOGICAL            LSE, LSERES
      EXTERNAL           LSE, LSERES
*     .. External Subroutines ..
      EXTERNAL           SMAKE, SMVCH, CSSPR, CSSYR
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX
*     .. Scalars in Common ..
      INTEGER            INFOT, NOUTC
      LOGICAL            OK
*     .. Common blocks ..
      COMMON             /INFOC/INFOT, NOUTC, OK
*     .. Data statements ..
      DATA               ICH/'UL'/
*     .. Executable Statements ..
      FULL = SNAME( 9: 9 ).EQ.'y'
      PACKED = SNAME( 9: 9 ).EQ.'p'
*     Define the number of arguments.
      IF( FULL )THEN
         NARGS = 7
      ELSE IF( PACKED )THEN
         NARGS = 6
      END IF
*
      NC = 0
      RESET = .TRUE.
      ERRMAX = ZERO
*
      DO 100 IN = 1, NIDIM
         N = IDIM( IN )
*        Set LDA to 1 more than minimum value if room.
         LDA = N
         IF( LDA.LT.NMAX )
     $      LDA = LDA + 1
*        Skip tests if not enough room.
         IF( LDA.GT.NMAX )
     $      GO TO 100
         IF( PACKED )THEN
            LAA = ( N*( N + 1 ) )/2
         ELSE
            LAA = LDA*N
         END IF
*
         DO 90 IC = 1, 2
            UPLO = ICH( IC: IC )
            IF (UPLO.EQ.'U')THEN
               CUPLO = '    CblasUpper'
            ELSE
               CUPLO = '    CblasLower'
            END IF
            UPPER = UPLO.EQ.'U'
*
            DO 80 IX = 1, NINC
               INCX = INC( IX )
               LX = ABS( INCX )*N
*
*              Generate the vector X.
*
               TRANSL = HALF
               CALL SMAKE( 'ge', ' ', ' ', 1, N, X, 1, XX, ABS( INCX ),
     $                     0, N - 1, RESET, TRANSL )
               IF( N.GT.1 )THEN
                  X( N/2 ) = ZERO
                  XX( 1 + ABS( INCX )*( N/2 - 1 ) ) = ZERO
               END IF
*
               DO 70 IA = 1, NALF
                  ALPHA = ALF( IA )
                  NULL = N.LE.0.OR.ALPHA.EQ.ZERO
*
*                 Generate the matrix A.
*
                  TRANSL = ZERO
                  CALL SMAKE( SNAME( 8: 9 ), UPLO, ' ', N, N, A, NMAX,
     $                        AA, LDA, N - 1, N - 1, RESET, TRANSL )
*
                  NC = NC + 1
*
*                 Save every datum before calling the subroutine.
*
                  UPLOS = UPLO
                  NS = N
                  ALS = ALPHA
                  DO 10 I = 1, LAA
                     AS( I ) = AA( I )
   10             CONTINUE
                  LDAS = LDA
                  DO 20 I = 1, LX
                     XS( I ) = XX( I )
   20             CONTINUE
                  INCXS = INCX
*
*                 Call the subroutine.
*
                  IF( FULL )THEN
                     IF( TRACE )
     $                  WRITE( NTRA, FMT = 9993 )NC, SNAME, CUPLO, N,
     $                  ALPHA, INCX, LDA
                     IF( REWI )
     $                  REWIND NTRA
                     CALL CSSYR( IORDER, UPLO, N, ALPHA, XX, INCX,
     $				  AA, LDA )
                  ELSE IF( PACKED )THEN
                     IF( TRACE )
     $                  WRITE( NTRA, FMT = 9994 )NC, SNAME, CUPLO, N,
     $                  ALPHA, INCX
                     IF( REWI )
     $                  REWIND NTRA
                     CALL CSSPR( IORDER, UPLO, N, ALPHA, XX, INCX, AA )
                  END IF
*
*                 Check if error-exit was taken incorrectly.
*
                  IF( .NOT.OK )THEN
                     WRITE( NOUT, FMT = 9992 )
                     FATAL = .TRUE.
                     GO TO 120
                  END IF
*
*                 See what data changed inside subroutines.
*
                  ISAME( 1 ) = UPLO.EQ.UPLOS
                  ISAME( 2 ) = NS.EQ.N
                  ISAME( 3 ) = ALS.EQ.ALPHA
                  ISAME( 4 ) = LSE( XS, XX, LX )
                  ISAME( 5 ) = INCXS.EQ.INCX
                  IF( NULL )THEN
                     ISAME( 6 ) = LSE( AS, AA, LAA )
                  ELSE
                     ISAME( 6 ) = LSERES( SNAME( 8: 9 ), UPLO, N, N, AS,
     $                            AA, LDA )
                  END IF
                  IF( .NOT.PACKED )THEN
                     ISAME( 7 ) = LDAS.EQ.LDA
                  END IF
*
*                 If data was incorrectly changed, report and return.
*
                  SAME = .TRUE.
                  DO 30 I = 1, NARGS
                     SAME = SAME.AND.ISAME( I )
                     IF( .NOT.ISAME( I ) )
     $                  WRITE( NOUT, FMT = 9998 )I
   30             CONTINUE
                  IF( .NOT.SAME )THEN
                     FATAL = .TRUE.
                     GO TO 120
                  END IF
*
                  IF( .NOT.NULL )THEN
*
*                    Check the result column by column.
*
                     IF( INCX.GT.0 )THEN
                        DO 40 I = 1, N
                           Z( I ) = X( I )
   40                   CONTINUE
                     ELSE
                        DO 50 I = 1, N
                           Z( I ) = X( N - I + 1 )
   50                   CONTINUE
                     END IF
                     JA = 1
                     DO 60 J = 1, N
                        W( 1 ) = Z( J )
                        IF( UPPER )THEN
                           JJ = 1
                           LJ = J
                        ELSE
                           JJ = J
                           LJ = N - J + 1
                        END IF
                        CALL SMVCH( 'N', LJ, 1, ALPHA, Z( JJ ), LJ, W,
     $                              1, ONE, A( JJ, J ), 1, YT, G,
     $                              AA( JA ), EPS, ERR, FATAL, NOUT,
     $                              .TRUE. )
                        IF( FULL )THEN
                           IF( UPPER )THEN
                              JA = JA + LDA
                           ELSE
                              JA = JA + LDA + 1
                           END IF
                        ELSE
                           JA = JA + LJ
                        END IF
                        ERRMAX = MAX( ERRMAX, ERR )
*                       If got really bad answer, report and return.
                        IF( FATAL )
     $                     GO TO 110
   60                CONTINUE
                  ELSE
*                    Avoid repeating tests if N.le.0.
                     IF( N.LE.0 )
     $                  GO TO 100
                  END IF
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
      WRITE( NOUT, FMT = 9995 )J
*
  120 CONTINUE
      WRITE( NOUT, FMT = 9996 )SNAME
      IF( FULL )THEN
         WRITE( NOUT, FMT = 9993 )NC, SNAME, CUPLO, N, ALPHA, INCX, LDA
      ELSE IF( PACKED )THEN
         WRITE( NOUT, FMT = 9994 )NC, SNAME, CUPLO, N, ALPHA, INCX
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
 9998 FORMAT( ' ******* FATAL ERROR - PARAMETER NUMBER ', I2, ' WAS CH',
     $      'ANGED INCORRECTLY *******' )
 9997 FORMAT( ' ',A12, ' COMPLETED THE COMPUTATIONAL TESTS (', I6, ' C',
     $      'ALLS)', /' ******* BUT WITH MAXIMUM TEST RATIO', F8.2,
     $      ' - SUSPECT *******' )
 9996 FORMAT( ' ******* ',A12, ' FAILED ON CALL NUMBER:' )
 9995 FORMAT( '      THESE ARE THE RESULTS FOR COLUMN ', I3 )
 9994 FORMAT( 1X, I6, ': ',A12, '(', A14, ',', I3, ',', F4.1, ', X,',
     $      I2, ', AP) .' )
 9993 FORMAT( 1X, I6, ': ',A12, '(', A14, ',', I3, ',', F4.1, ', X,',
     $      I2, ', A,', I3, ') .' )
 9992 FORMAT( ' ******* FATAL ERROR - ERROR-EXIT TAKEN ON VALID CALL *',
     $      '******' )
*
*     End of SCHK5.
*
      END
      SUBROUTINE SCHK6( SNAME, EPS, THRESH, NOUT, NTRA, TRACE, REWI,
     $                  FATAL, NIDIM, IDIM, NALF, ALF, NINC, INC, NMAX,
     $                  INCMAX, A, AA, AS, X, XX, XS, Y, YY, YS, YT, G,
     $                  Z, IORDER )
*
*  Tests SSYR2 and SSPR2.
*
*  Auxiliary routine for test program for Level 2 Blas.
*
*  -- Written on 10-August-1987.
*     Richard Hanson, Sandia National Labs.
*     Jeremy Du Croz, NAG Central Office.
*
*     .. Parameters ..
      REAL               ZERO, HALF, ONE
      PARAMETER          ( ZERO = 0.0, HALF = 0.5, ONE = 1.0 )
*     .. Scalar Arguments ..
      REAL               EPS, THRESH
      INTEGER            INCMAX, NALF, NIDIM, NINC, NMAX, NOUT, NTRA,
     $			 IORDER
      LOGICAL            FATAL, REWI, TRACE
      CHARACTER*12       SNAME
*     .. Array Arguments ..
      REAL               A( NMAX, NMAX ), AA( NMAX*NMAX ), ALF( NALF ),
     $                   AS( NMAX*NMAX ), G( NMAX ), X( NMAX ),
     $                   XS( NMAX*INCMAX ), XX( NMAX*INCMAX ),
     $                   Y( NMAX ), YS( NMAX*INCMAX ), YT( NMAX ),
     $                   YY( NMAX*INCMAX ), Z( NMAX, 2 )
      INTEGER            IDIM( NIDIM ), INC( NINC )
*     .. Local Scalars ..
      REAL               ALPHA, ALS, ERR, ERRMAX, TRANSL
      INTEGER            I, IA, IC, IN, INCX, INCXS, INCY, INCYS, IX,
     $                   IY, J, JA, JJ, LAA, LDA, LDAS, LJ, LX, LY, N,
     $                   NARGS, NC, NS
      LOGICAL            FULL, NULL, PACKED, RESET, SAME, UPPER
      CHARACTER*1        UPLO, UPLOS
      CHARACTER*14       CUPLO
      CHARACTER*2        ICH
*     .. Local Arrays ..
      REAL               W( 2 )
      LOGICAL            ISAME( 13 )
*     .. External Functions ..
      LOGICAL            LSE, LSERES
      EXTERNAL           LSE, LSERES
*     .. External Subroutines ..
      EXTERNAL           SMAKE, SMVCH, CSSPR2, CSSYR2
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX
*     .. Scalars in Common ..
      INTEGER            INFOT, NOUTC
      LOGICAL            OK
*     .. Common blocks ..
      COMMON             /INFOC/INFOT, NOUTC, OK
*     .. Data statements ..
      DATA               ICH/'UL'/
*     .. Executable Statements ..
      FULL = SNAME( 9: 9 ).EQ.'y'
      PACKED = SNAME( 9: 9 ).EQ.'p'
*     Define the number of arguments.
      IF( FULL )THEN
         NARGS = 9
      ELSE IF( PACKED )THEN
         NARGS = 8
      END IF
*
      NC = 0
      RESET = .TRUE.
      ERRMAX = ZERO
*
      DO 140 IN = 1, NIDIM
         N = IDIM( IN )
*        Set LDA to 1 more than minimum value if room.
         LDA = N
         IF( LDA.LT.NMAX )
     $      LDA = LDA + 1
*        Skip tests if not enough room.
         IF( LDA.GT.NMAX )
     $      GO TO 140
         IF( PACKED )THEN
            LAA = ( N*( N + 1 ) )/2
         ELSE
            LAA = LDA*N
         END IF
*
         DO 130 IC = 1, 2
            UPLO = ICH( IC: IC )
            IF (UPLO.EQ.'U')THEN
               CUPLO = '    CblasUpper'
            ELSE
               CUPLO = '    CblasLower'
            END IF
            UPPER = UPLO.EQ.'U'
*
            DO 120 IX = 1, NINC
               INCX = INC( IX )
               LX = ABS( INCX )*N
*
*              Generate the vector X.
*
               TRANSL = HALF
               CALL SMAKE( 'ge', ' ', ' ', 1, N, X, 1, XX, ABS( INCX ),
     $                     0, N - 1, RESET, TRANSL )
               IF( N.GT.1 )THEN
                  X( N/2 ) = ZERO
                  XX( 1 + ABS( INCX )*( N/2 - 1 ) ) = ZERO
               END IF
*
               DO 110 IY = 1, NINC
                  INCY = INC( IY )
                  LY = ABS( INCY )*N
*
*                 Generate the vector Y.
*
                  TRANSL = ZERO
                  CALL SMAKE( 'ge', ' ', ' ', 1, N, Y, 1, YY,
     $                        ABS( INCY ), 0, N - 1, RESET, TRANSL )
                  IF( N.GT.1 )THEN
                     Y( N/2 ) = ZERO
                     YY( 1 + ABS( INCY )*( N/2 - 1 ) ) = ZERO
                  END IF
*
                  DO 100 IA = 1, NALF
                     ALPHA = ALF( IA )
                     NULL = N.LE.0.OR.ALPHA.EQ.ZERO
*
*                    Generate the matrix A.
*
                     TRANSL = ZERO
                     CALL SMAKE( SNAME( 8: 9 ), UPLO, ' ', N, N, A,
     $                           NMAX, AA, LDA, N - 1, N - 1, RESET,
     $                           TRANSL )
*
                     NC = NC + 1
*
*                    Save every datum before calling the subroutine.
*
                     UPLOS = UPLO
                     NS = N
                     ALS = ALPHA
                     DO 10 I = 1, LAA
                        AS( I ) = AA( I )
   10                CONTINUE
                     LDAS = LDA
                     DO 20 I = 1, LX
                        XS( I ) = XX( I )
   20                CONTINUE
                     INCXS = INCX
                     DO 30 I = 1, LY
                        YS( I ) = YY( I )
   30                CONTINUE
                     INCYS = INCY
*
*                    Call the subroutine.
*
                     IF( FULL )THEN
                        IF( TRACE )
     $                     WRITE( NTRA, FMT = 9993 )NC, SNAME, CUPLO, N,
     $                     ALPHA, INCX, INCY, LDA
                        IF( REWI )
     $                     REWIND NTRA
                        CALL CSSYR2( IORDER, UPLO, N, ALPHA, XX, INCX,
     $                              YY, INCY, AA, LDA )
                     ELSE IF( PACKED )THEN
                        IF( TRACE )
     $                     WRITE( NTRA, FMT = 9994 )NC, SNAME, CUPLO, N,
     $                     ALPHA, INCX, INCY
                        IF( REWI )
     $                     REWIND NTRA
                        CALL CSSPR2( IORDER, UPLO, N, ALPHA, XX, INCX,
     $                              YY, INCY, AA )
                     END IF
*
*                    Check if error-exit was taken incorrectly.
*
                     IF( .NOT.OK )THEN
                        WRITE( NOUT, FMT = 9992 )
                        FATAL = .TRUE.
                        GO TO 160
                     END IF
*
*                    See what data changed inside subroutines.
*
                     ISAME( 1 ) = UPLO.EQ.UPLOS
                     ISAME( 2 ) = NS.EQ.N
                     ISAME( 3 ) = ALS.EQ.ALPHA
                     ISAME( 4 ) = LSE( XS, XX, LX )
                     ISAME( 5 ) = INCXS.EQ.INCX
                     ISAME( 6 ) = LSE( YS, YY, LY )
                     ISAME( 7 ) = INCYS.EQ.INCY
                     IF( NULL )THEN
                        ISAME( 8 ) = LSE( AS, AA, LAA )
                     ELSE
                        ISAME( 8 ) = LSERES( SNAME( 8: 9 ), UPLO, N, N,
     $                               AS, AA, LDA )
                     END IF
                     IF( .NOT.PACKED )THEN
                        ISAME( 9 ) = LDAS.EQ.LDA
                     END IF
*
*                    If data was incorrectly changed, report and return.
*
                     SAME = .TRUE.
                     DO 40 I = 1, NARGS
                        SAME = SAME.AND.ISAME( I )
                        IF( .NOT.ISAME( I ) )
     $                     WRITE( NOUT, FMT = 9998 )I
   40                CONTINUE
                     IF( .NOT.SAME )THEN
                        FATAL = .TRUE.
                        GO TO 160
                     END IF
*
                     IF( .NOT.NULL )THEN
*
*                       Check the result column by column.
*
                        IF( INCX.GT.0 )THEN
                           DO 50 I = 1, N
                              Z( I, 1 ) = X( I )
   50                      CONTINUE
                        ELSE
                           DO 60 I = 1, N
                              Z( I, 1 ) = X( N - I + 1 )
   60                      CONTINUE
                        END IF
                        IF( INCY.GT.0 )THEN
                           DO 70 I = 1, N
                              Z( I, 2 ) = Y( I )
   70                      CONTINUE
                        ELSE
                           DO 80 I = 1, N
                              Z( I, 2 ) = Y( N - I + 1 )
   80                      CONTINUE
                        END IF
                        JA = 1
                        DO 90 J = 1, N
                           W( 1 ) = Z( J, 2 )
                           W( 2 ) = Z( J, 1 )
                           IF( UPPER )THEN
                              JJ = 1
                              LJ = J
                           ELSE
                              JJ = J
                              LJ = N - J + 1
                           END IF
                           CALL SMVCH( 'N', LJ, 2, ALPHA, Z( JJ, 1 ),
     $                                 NMAX, W, 1, ONE, A( JJ, J ), 1,
     $                                 YT, G, AA( JA ), EPS, ERR, FATAL,
     $                                 NOUT, .TRUE. )
                           IF( FULL )THEN
                              IF( UPPER )THEN
                                 JA = JA + LDA
                              ELSE
                                 JA = JA + LDA + 1
                              END IF
                           ELSE
                              JA = JA + LJ
                           END IF
                           ERRMAX = MAX( ERRMAX, ERR )
*                          If got really bad answer, report and return.
                           IF( FATAL )
     $                        GO TO 150
   90                   CONTINUE
                     ELSE
*                       Avoid repeating tests with N.le.0.
                        IF( N.LE.0 )
     $                     GO TO 140
                     END IF
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
      GO TO 170
*
  150 CONTINUE
      WRITE( NOUT, FMT = 9995 )J
*
  160 CONTINUE
      WRITE( NOUT, FMT = 9996 )SNAME
      IF( FULL )THEN
         WRITE( NOUT, FMT = 9993 )NC, SNAME, CUPLO, N, ALPHA, INCX,
     $      INCY, LDA
      ELSE IF( PACKED )THEN
         WRITE( NOUT, FMT = 9994 )NC, SNAME, CUPLO, N, ALPHA, INCX, INCY
      END IF
*
  170 CONTINUE
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
 9998 FORMAT( ' ******* FATAL ERROR - PARAMETER NUMBER ', I2, ' WAS CH',
     $      'ANGED INCORRECTLY *******' )
 9997 FORMAT( ' ',A12, ' COMPLETED THE COMPUTATIONAL TESTS (', I6, ' C',
     $      'ALLS)', /' ******* BUT WITH MAXIMUM TEST RATIO', F8.2,
     $      ' - SUSPECT *******' )
 9996 FORMAT( ' ******* ',A12, ' FAILED ON CALL NUMBER:' )
 9995 FORMAT( '      THESE ARE THE RESULTS FOR COLUMN ', I3 )
 9994 FORMAT( 1X, I6, ': ',A12, '(', A14, ',', I3, ',', F4.1, ', X,',
     $      I2, ', Y,', I2, ', AP) .' )
 9993 FORMAT( 1X, I6, ': ',A12, '(', A14, ',', I3, ',', F4.1, ', X,',
     $      I2, ', Y,', I2, ', A,', I3, ') .' )
 9992 FORMAT( ' ******* FATAL ERROR - ERROR-EXIT TAKEN ON VALID CALL *',
     $      '******' )
*
*     End of SCHK6.
*
      END
      SUBROUTINE SMAKE( TYPE, UPLO, DIAG, M, N, A, NMAX, AA, LDA, KL,
     $                  KU, RESET, TRANSL )
*
*  Generates values for an M by N matrix A within the bandwidth
*  defined by KL and KU.
*  Stores the values in the array AA in the data structure required
*  by the routine, with unwanted elements set to rogue value.
*
*  TYPE is 'ge', 'gb', 'sy', 'sb', 'sp', 'tr', 'tb' OR 'tp'.
*
*  Auxiliary routine for test program for Level 2 Blas.
*
*  -- Written on 10-August-1987.
*     Richard Hanson, Sandia National Labs.
*     Jeremy Du Croz, NAG Central Office.
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0, ONE = 1.0 )
      REAL               ROGUE
      PARAMETER          ( ROGUE = -1.0E10 )
*     .. Scalar Arguments ..
      REAL               TRANSL
      INTEGER            KL, KU, LDA, M, N, NMAX
      LOGICAL            RESET
      CHARACTER*1        DIAG, UPLO
      CHARACTER*2        TYPE
*     .. Array Arguments ..
      REAL               A( NMAX, * ), AA( * )
*     .. Local Scalars ..
      INTEGER            I, I1, I2, I3, IBEG, IEND, IOFF, J, KK
      LOGICAL            GEN, LOWER, SYM, TRI, UNIT, UPPER
*     .. External Functions ..
      REAL               SBEG
      EXTERNAL           SBEG
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN
*     .. Executable Statements ..
      GEN = TYPE( 1: 1 ).EQ.'g'
      SYM = TYPE( 1: 1 ).EQ.'s'
      TRI = TYPE( 1: 1 ).EQ.'t'
      UPPER = ( SYM.OR.TRI ).AND.UPLO.EQ.'U'
      LOWER = ( SYM.OR.TRI ).AND.UPLO.EQ.'L'
      UNIT = TRI.AND.DIAG.EQ.'U'
*
*     Generate data in array A.
*
      DO 20 J = 1, N
         DO 10 I = 1, M
            IF( GEN.OR.( UPPER.AND.I.LE.J ).OR.( LOWER.AND.I.GE.J ) )
     $          THEN
               IF( ( I.LE.J.AND.J - I.LE.KU ).OR.
     $             ( I.GE.J.AND.I - J.LE.KL ) )THEN
                  A( I, J ) = SBEG( RESET ) + TRANSL
               ELSE
                  A( I, J ) = ZERO
               END IF
               IF( I.NE.J )THEN
                  IF( SYM )THEN
                     A( J, I ) = A( I, J )
                  ELSE IF( TRI )THEN
                     A( J, I ) = ZERO
                  END IF
               END IF
            END IF
   10    CONTINUE
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
      ELSE IF( TYPE.EQ.'gb' )THEN
         DO 90 J = 1, N
            DO 60 I1 = 1, KU + 1 - J
               AA( I1 + ( J - 1 )*LDA ) = ROGUE
   60       CONTINUE
            DO 70 I2 = I1, MIN( KL + KU + 1, KU + 1 + M - J )
               AA( I2 + ( J - 1 )*LDA ) = A( I2 + J - KU - 1, J )
   70       CONTINUE
            DO 80 I3 = I2, LDA
               AA( I3 + ( J - 1 )*LDA ) = ROGUE
   80       CONTINUE
   90    CONTINUE
      ELSE IF( TYPE.EQ.'sy'.OR.TYPE.EQ.'tr' )THEN
         DO 130 J = 1, N
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
            DO 100 I = 1, IBEG - 1
               AA( I + ( J - 1 )*LDA ) = ROGUE
  100       CONTINUE
            DO 110 I = IBEG, IEND
               AA( I + ( J - 1 )*LDA ) = A( I, J )
  110       CONTINUE
            DO 120 I = IEND + 1, LDA
               AA( I + ( J - 1 )*LDA ) = ROGUE
  120       CONTINUE
  130    CONTINUE
      ELSE IF( TYPE.EQ.'sb'.OR.TYPE.EQ.'tb' )THEN
         DO 170 J = 1, N
            IF( UPPER )THEN
               KK = KL + 1
               IBEG = MAX( 1, KL + 2 - J )
               IF( UNIT )THEN
                  IEND = KL
               ELSE
                  IEND = KL + 1
               END IF
            ELSE
               KK = 1
               IF( UNIT )THEN
                  IBEG = 2
               ELSE
                  IBEG = 1
               END IF
               IEND = MIN( KL + 1, 1 + M - J )
            END IF
            DO 140 I = 1, IBEG - 1
               AA( I + ( J - 1 )*LDA ) = ROGUE
  140       CONTINUE
            DO 150 I = IBEG, IEND
               AA( I + ( J - 1 )*LDA ) = A( I + J - KK, J )
  150       CONTINUE
            DO 160 I = IEND + 1, LDA
               AA( I + ( J - 1 )*LDA ) = ROGUE
  160       CONTINUE
  170    CONTINUE
      ELSE IF( TYPE.EQ.'sp'.OR.TYPE.EQ.'tp' )THEN
         IOFF = 0
         DO 190 J = 1, N
            IF( UPPER )THEN
               IBEG = 1
               IEND = J
            ELSE
               IBEG = J
               IEND = N
            END IF
            DO 180 I = IBEG, IEND
               IOFF = IOFF + 1
               AA( IOFF ) = A( I, J )
               IF( I.EQ.J )THEN
                  IF( UNIT )
     $               AA( IOFF ) = ROGUE
               END IF
  180       CONTINUE
  190    CONTINUE
      END IF
      RETURN
*
*     End of SMAKE.
*
      END
      SUBROUTINE SMVCH( TRANS, M, N, ALPHA, A, NMAX, X, INCX, BETA, Y,
     $                  INCY, YT, G, YY, EPS, ERR, FATAL, NOUT, MV )
*
*  Checks the results of the computational tests.
*
*  Auxiliary routine for test program for Level 2 Blas.
*
*  -- Written on 10-August-1987.
*     Richard Hanson, Sandia National Labs.
*     Jeremy Du Croz, NAG Central Office.
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0, ONE = 1.0 )
*     .. Scalar Arguments ..
      REAL               ALPHA, BETA, EPS, ERR
      INTEGER            INCX, INCY, M, N, NMAX, NOUT
      LOGICAL            FATAL, MV
      CHARACTER*1        TRANS
*     .. Array Arguments ..
      REAL               A( NMAX, * ), G( * ), X( * ), Y( * ), YT( * ),
     $                   YY( * )
*     .. Local Scalars ..
      REAL               ERRI
      INTEGER            I, INCXL, INCYL, IY, J, JX, KX, KY, ML, NL
      LOGICAL            TRAN
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, SQRT
*     .. Executable Statements ..
      TRAN = TRANS.EQ.'T'.OR.TRANS.EQ.'C'
      IF( TRAN )THEN
         ML = N
         NL = M
      ELSE
         ML = M
         NL = N
      END IF
      IF( INCX.LT.0 )THEN
         KX = NL
         INCXL = -1
      ELSE
         KX = 1
         INCXL = 1
      END IF
      IF( INCY.LT.0 )THEN
         KY = ML
         INCYL = -1
      ELSE
         KY = 1
         INCYL = 1
      END IF
*
*     Compute expected result in YT using data in A, X and Y.
*     Compute gauges in G.
*
      IY = KY
      DO 30 I = 1, ML
         YT( IY ) = ZERO
         G( IY ) = ZERO
         JX = KX
         IF( TRAN )THEN
            DO 10 J = 1, NL
               YT( IY ) = YT( IY ) + A( J, I )*X( JX )
               G( IY ) = G( IY ) + ABS( A( J, I )*X( JX ) )
               JX = JX + INCXL
   10       CONTINUE
         ELSE
            DO 20 J = 1, NL
               YT( IY ) = YT( IY ) + A( I, J )*X( JX )
               G( IY ) = G( IY ) + ABS( A( I, J )*X( JX ) )
               JX = JX + INCXL
   20       CONTINUE
         END IF
         YT( IY ) = ALPHA*YT( IY ) + BETA*Y( IY )
         G( IY ) = ABS( ALPHA )*G( IY ) + ABS( BETA*Y( IY ) )
         IY = IY + INCYL
   30 CONTINUE
*
*     Compute the error ratio for this result.
*
      ERR = ZERO
      DO 40 I = 1, ML
         ERRI = ABS( YT( I ) - YY( 1 + ( I - 1 )*ABS( INCY ) ) )/EPS
         IF( G( I ).NE.ZERO )
     $      ERRI = ERRI/G( I )
         ERR = MAX( ERR, ERRI )
         IF( ERR*SQRT( EPS ).GE.ONE )
     $      GO TO 50
   40 CONTINUE
*     If the loop completes, all results are at least half accurate.
      GO TO 70
*
*     Report fatal error.
*
   50 FATAL = .TRUE.
      WRITE( NOUT, FMT = 9999 )
      DO 60 I = 1, ML
         IF( MV )THEN
            WRITE( NOUT, FMT = 9998 )I, YT( I ),
     $         YY( 1 + ( I - 1 )*ABS( INCY ) )
         ELSE
            WRITE( NOUT, FMT = 9998 )I,
     $         YY( 1 + ( I - 1 )*ABS( INCY ) ), YT(I)
         END IF
   60 CONTINUE
*
   70 CONTINUE
      RETURN
*
 9999 FORMAT( ' ******* FATAL ERROR - COMPUTED RESULT IS LESS THAN HAL',
     $      'F ACCURATE *******', /'           EXPECTED RESULT   COMPU',
     $      'TED RESULT' )
 9998 FORMAT( 1X, I7, 2G18.6 )
*
*     End of SMVCH.
*
      END
      LOGICAL FUNCTION LSE( RI, RJ, LR )
*
*  Tests if two arrays are identical.
*
*  Auxiliary routine for test program for Level 2 Blas.
*
*  -- Written on 10-August-1987.
*     Richard Hanson, Sandia National Labs.
*     Jeremy Du Croz, NAG Central Office.
*
*     .. Scalar Arguments ..
      INTEGER            LR
*     .. Array Arguments ..
      REAL               RI( * ), RJ( * )
*     .. Local Scalars ..
      INTEGER            I
*     .. Executable Statements ..
      DO 10 I = 1, LR
         IF( RI( I ).NE.RJ( I ) )
     $      GO TO 20
   10 CONTINUE
      LSE = .TRUE.
      GO TO 30
   20 CONTINUE
      LSE = .FALSE.
   30 RETURN
*
*     End of LSE.
*
      END
      LOGICAL FUNCTION LSERES( TYPE, UPLO, M, N, AA, AS, LDA )
*
*  Tests if selected elements in two arrays are equal.
*
*  TYPE is 'ge', 'sy' or 'sp'.
*
*  Auxiliary routine for test program for Level 2 Blas.
*
*  -- Written on 10-August-1987.
*     Richard Hanson, Sandia National Labs.
*     Jeremy Du Croz, NAG Central Office.
*
*     .. Scalar Arguments ..
      INTEGER            LDA, M, N
      CHARACTER*1        UPLO
      CHARACTER*2        TYPE
*     .. Array Arguments ..
      REAL               AA( LDA, * ), AS( LDA, * )
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
      ELSE IF( TYPE.EQ.'sy' )THEN
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
      LSERES = .TRUE.
      GO TO 80
   70 CONTINUE
      LSERES = .FALSE.
   80 RETURN
*
*     End of LSERES.
*
      END
      REAL FUNCTION SBEG( RESET )
*
*  Generates random numbers uniformly distributed between -0.5 and 0.5.
*
*  Auxiliary routine for test program for Level 2 Blas.
*
*  -- Written on 10-August-1987.
*     Richard Hanson, Sandia National Labs.
*     Jeremy Du Croz, NAG Central Office.
*
*     .. Scalar Arguments ..
      LOGICAL            RESET
*     .. Local Scalars ..
      INTEGER            I, IC, MI
*     .. Save statement ..
      SAVE               I, IC, MI
*     .. Intrinsic Functions ..
      INTRINSIC          REAL
*     .. Executable Statements ..
      IF( RESET )THEN
*        Initialize local variables.
         MI = 891
         I = 7
         IC = 0
         RESET = .FALSE.
      END IF
*
*     The sequence of values of I is bounded between 1 and 999.
*     If initial I = 1,2,3,6,7 or 9, the period will be 50.
*     If initial I = 4 or 8, the period will be 25.
*     If initial I = 5, the period will be 10.
*     IC is used to break up the period by skipping 1 value of I in 6.
*
      IC = IC + 1
   10 I = I*MI
      I = I - 1000*( I/1000 )
      IF( IC.GE.5 )THEN
         IC = 0
         GO TO 10
      END IF
      SBEG = REAL( I - 500 )/1001.0
      RETURN
*
*     End of SBEG.
*
      END
      REAL FUNCTION SDIFF( X, Y )
*
*  Auxiliary routine for test program for Level 2 Blas.
*
*  -- Written on 10-August-1987.
*     Richard Hanson, Sandia National Labs.
*
*     .. Scalar Arguments ..
      REAL               X, Y
*     .. Executable Statements ..
      SDIFF = X - Y
      RETURN
*
*     End of SDIFF.
*
      END
