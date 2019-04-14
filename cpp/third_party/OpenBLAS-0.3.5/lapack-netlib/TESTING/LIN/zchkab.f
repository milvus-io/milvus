*> \brief \b ZCHKAB
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       PROGRAM ZCHKAB
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZCHKAB is the test program for the COMPLEX*16 LAPACK
*> ZCGESV/ZCPOSV routine
*>
*> The program must be driven by a short data file. The first 5 records
*> specify problem dimensions and program options using list-directed
*> input. The remaining lines specify the LAPACK test paths and the
*> number of matrix types to use in testing.  An annotated example of a
*> data file can be obtained by deleting the first 3 characters from the
*> following 9 lines:
*> Data file for testing COMPLEX*16 LAPACK ZCGESV
*> 7                      Number of values of M
*> 0 1 2 3 5 10 16        Values of M (row dimension)
*> 1                      Number of values of NRHS
*> 2                      Values of NRHS (number of right hand sides)
*> 20.0                   Threshold value of test ratio
*> T                      Put T to test the LAPACK routine
*> T                      Put T to test the error exits
*> DGE    11              List types on next line if 0 < NTYPES < 11
*> DPO    9               List types on next line if 0 < NTYPES <  9
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \verbatim
*>  NMAX    INTEGER
*>          The maximum allowable value for N
*>
*>  MAXIN   INTEGER
*>          The number of different values that can be used for each of
*>          M, N, NRHS, NB, and NX
*>
*>  MAXRHS  INTEGER
*>          The maximum number of right hand sides
*>
*>  NIN     INTEGER
*>          The unit number for input
*>
*>  NOUT    INTEGER
*>          The unit number for output
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
*> \ingroup complex16_lin
*
*  =====================================================================
      PROGRAM ZCHKAB
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     April 2012
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            NMAX
      PARAMETER          ( NMAX = 132 )
      INTEGER            MAXIN
      PARAMETER          ( MAXIN = 12 )
      INTEGER            MAXRHS
      PARAMETER          ( MAXRHS = 16 )
      INTEGER            MATMAX
      PARAMETER          ( MATMAX = 30 )
      INTEGER            NIN, NOUT
      PARAMETER          ( NIN = 5, NOUT = 6 )
      INTEGER            LDAMAX
      PARAMETER          ( LDAMAX = NMAX )
*     ..
*     .. Local Scalars ..
      LOGICAL            FATAL, TSTDRV, TSTERR
      CHARACTER          C1
      CHARACTER*2        C2
      CHARACTER*3        PATH
      CHARACTER*10       INTSTR
      CHARACTER*72       ALINE
      INTEGER            I, IC, K, LDA, NM, NMATS,
     $                   NNS, NRHS, NTYPES,
     $                   VERS_MAJOR, VERS_MINOR, VERS_PATCH
      DOUBLE PRECISION   EPS, S1, S2, THRESH
      REAL               SEPS
*     ..
*     .. Local Arrays ..
      LOGICAL            DOTYPE( MATMAX )
      INTEGER            IWORK( NMAX ), MVAL( MAXIN ), NSVAL( MAXIN )
      DOUBLE PRECISION   RWORK(NMAX)
      COMPLEX*16         A( LDAMAX*NMAX, 2 ), B( NMAX*MAXRHS, 2 ),
     $                   WORK( NMAX*MAXRHS*2 )
      COMPLEX            SWORK(NMAX*(NMAX+MAXRHS))
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH, DSECND
      LOGICAL            LSAME, LSAMEN
      REAL               SLAMCH
      EXTERNAL           DLAMCH, DSECND, LSAME, LSAMEN, SLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAREQ, ZDRVAB, ZDRVAC, ZERRAB, ZERRAC,
     $                   ILAVER
*     ..
*     .. Scalars in Common ..
      LOGICAL            LERR, OK
      CHARACTER*32       SRNAMT
      INTEGER            INFOT, NUNIT
*     ..
*     .. Common blocks ..
      COMMON             / INFOC / INFOT, NUNIT, OK, LERR
      COMMON             / SRNAMC / SRNAMT
*
*     .. Data statements ..
      DATA               INTSTR / '0123456789' /
*     ..
*     .. Executable Statements ..
*
      S1 = DSECND( )
      LDA = NMAX
      FATAL = .FALSE.
*
*     Read a dummy line.
*
      READ( NIN, FMT = * )
*
*     Report values of parameters.
*
      CALL ILAVER( VERS_MAJOR, VERS_MINOR, VERS_PATCH )
      WRITE( NOUT, FMT = 9994 ) VERS_MAJOR, VERS_MINOR, VERS_PATCH
*
*     Read the values of M
*
      READ( NIN, FMT = * )NM
      IF( NM.LT.1 ) THEN
         WRITE( NOUT, FMT = 9996 )' NM ', NM, 1
         NM = 0
         FATAL = .TRUE.
      ELSE IF( NM.GT.MAXIN ) THEN
         WRITE( NOUT, FMT = 9995 )' NM ', NM, MAXIN
         NM = 0
         FATAL = .TRUE.
      END IF
      READ( NIN, FMT = * )( MVAL( I ), I = 1, NM )
      DO 10 I = 1, NM
         IF( MVAL( I ).LT.0 ) THEN
            WRITE( NOUT, FMT = 9996 )' M  ', MVAL( I ), 0
            FATAL = .TRUE.
         ELSE IF( MVAL( I ).GT.NMAX ) THEN
            WRITE( NOUT, FMT = 9995 )' M  ', MVAL( I ), NMAX
            FATAL = .TRUE.
         END IF
   10 CONTINUE
      IF( NM.GT.0 )
     $   WRITE( NOUT, FMT = 9993 )'M   ', ( MVAL( I ), I = 1, NM )
*
*     Read the values of NRHS
*
      READ( NIN, FMT = * )NNS
      IF( NNS.LT.1 ) THEN
         WRITE( NOUT, FMT = 9996 )' NNS', NNS, 1
         NNS = 0
         FATAL = .TRUE.
      ELSE IF( NNS.GT.MAXIN ) THEN
         WRITE( NOUT, FMT = 9995 )' NNS', NNS, MAXIN
         NNS = 0
         FATAL = .TRUE.
      END IF
      READ( NIN, FMT = * )( NSVAL( I ), I = 1, NNS )
      DO 30 I = 1, NNS
         IF( NSVAL( I ).LT.0 ) THEN
            WRITE( NOUT, FMT = 9996 )'NRHS', NSVAL( I ), 0
            FATAL = .TRUE.
         ELSE IF( NSVAL( I ).GT.MAXRHS ) THEN
            WRITE( NOUT, FMT = 9995 )'NRHS', NSVAL( I ), MAXRHS
            FATAL = .TRUE.
         END IF
   30 CONTINUE
      IF( NNS.GT.0 )
     $   WRITE( NOUT, FMT = 9993 )'NRHS', ( NSVAL( I ), I = 1, NNS )
*
*     Read the threshold value for the test ratios.
*
      READ( NIN, FMT = * )THRESH
      WRITE( NOUT, FMT = 9992 )THRESH
*
*     Read the flag that indicates whether to test the driver routine.
*
      READ( NIN, FMT = * )TSTDRV
*
*     Read the flag that indicates whether to test the error exits.
*
      READ( NIN, FMT = * )TSTERR
*
      IF( FATAL ) THEN
         WRITE( NOUT, FMT = 9999 )
         STOP
      END IF
*
*     Calculate and print the machine dependent constants.
*
      SEPS = SLAMCH( 'Underflow threshold' )
      WRITE( NOUT, FMT = 9991 )'(single precision) underflow', SEPS
      SEPS = SLAMCH( 'Overflow threshold' )
      WRITE( NOUT, FMT = 9991 )'(single precision) overflow ', SEPS
      SEPS = SLAMCH( 'Epsilon' )
      WRITE( NOUT, FMT = 9991 )'(single precision) precision', SEPS
      WRITE( NOUT, FMT = * )
*
      EPS = DLAMCH( 'Underflow threshold' )
      WRITE( NOUT, FMT = 9991 )'(double precision) underflow', EPS
      EPS = DLAMCH( 'Overflow threshold' )
      WRITE( NOUT, FMT = 9991 )'(double precision) overflow ', EPS
      EPS = DLAMCH( 'Epsilon' )
      WRITE( NOUT, FMT = 9991 )'(double precision) precision', EPS
      WRITE( NOUT, FMT = * )
*
   80 CONTINUE
*
*     Read a test path and the number of matrix types to use.
*
      READ( NIN, FMT = '(A72)', END = 140 )ALINE
      PATH = ALINE( 1: 3 )
      NMATS = MATMAX
      I = 3
   90 CONTINUE
      I = I + 1
      IF( I.GT.72 ) THEN
         NMATS = MATMAX
         GO TO 130
      END IF
      IF( ALINE( I: I ).EQ.' ' )
     $   GO TO 90
      NMATS = 0
  100 CONTINUE
      C1 = ALINE( I: I )
      DO 110 K = 1, 10
         IF( C1.EQ.INTSTR( K: K ) ) THEN
            IC = K - 1
            GO TO 120
         END IF
  110 CONTINUE
      GO TO 130
  120 CONTINUE
      NMATS = NMATS*10 + IC
      I = I + 1
      IF( I.GT.72 )
     $   GO TO 130
      GO TO 100
  130 CONTINUE
      C1 = PATH( 1: 1 )
      C2 = PATH( 2: 3 )
      NRHS = NSVAL( 1 )
      NRHS = NSVAL( 1 )
*
*     Check first character for correct precision.
*
      IF( .NOT.LSAME( C1, 'Zomplex precision' ) ) THEN
            WRITE( NOUT, FMT = 9990 )PATH
*
      ELSE IF( NMATS.LE.0 ) THEN
*
*        Check for a positive number of tests requested.
*
         WRITE( NOUT, FMT = 9990 )'ZCGESV'
         GO TO 140
*
      ELSE IF( LSAMEN( 2, C2, 'GE' ) ) THEN
*
*        GE:  general matrices
*
      NTYPES = 11
      CALL ALAREQ( 'ZGE', NMATS, DOTYPE, NTYPES, NIN, NOUT )
*
*        Test the error exits
*
         IF( TSTERR )
     $     CALL ZERRAB( NOUT )
*
         IF( TSTDRV ) THEN
            CALL ZDRVAB( DOTYPE, NM, MVAL, NNS,
     $                   NSVAL, THRESH, LDA, A( 1, 1 ),
     $                   A( 1, 2 ), B( 1, 1 ), B( 1, 2 ),
     $                   WORK, RWORK, SWORK, IWORK, NOUT )
         ELSE
            WRITE( NOUT, FMT = 9989 )'ZCGESV'
         END IF
*
      ELSE IF( LSAMEN( 2, C2, 'PO' ) ) THEN
*
*        PO:  positive definite matrices
*
         NTYPES = 9
         CALL ALAREQ( 'DPO', NMATS, DOTYPE, NTYPES, NIN, NOUT )
*
         IF( TSTERR )
     $      CALL ZERRAC( NOUT )
*
*
         IF( TSTDRV ) THEN
            CALL ZDRVAC( DOTYPE, NM, MVAL, NNS, NSVAL,
     $                   THRESH, LDA, A( 1, 1 ), A( 1, 2 ),
     $                   B( 1, 1 ), B( 1, 2 ),
     $                   WORK, RWORK, SWORK, NOUT )
         ELSE
            WRITE( NOUT, FMT = 9989 )'ZCPOSV'
         END IF
*
      ELSE
*
      END IF
*
*     Go back to get another input line.
*
      GO TO 80
*
*     Branch to this line when the last record is read.
*
  140 CONTINUE
      CLOSE ( NIN )
      S2 = DSECND( )
      WRITE( NOUT, FMT = 9998 )
      WRITE( NOUT, FMT = 9997 )S2 - S1
*
 9999 FORMAT( / ' Execution not attempted due to input errors' )
 9998 FORMAT( / ' End of tests' )
 9997 FORMAT( ' Total time used = ', F12.2, ' seconds', / )
 9996 FORMAT( ' Invalid input value: ', A4, '=', I6, '; must be >=',
     $      I6 )
 9995 FORMAT( ' Invalid input value: ', A4, '=', I6, '; must be <=',
     $      I6 )
 9994 FORMAT( ' Tests of the COMPLEX*16 LAPACK ZCGESV/ZCPOSV routines ',
     $      / ' LAPACK VERSION ', I1, '.', I1, '.', I1,
     $      / / ' The following parameter values will be used:' )
 9993 FORMAT( 4X, A4, ':  ', 10I6, / 11X, 10I6 )
 9992 FORMAT( / ' Routines pass computational tests if test ratio is ',
     $      'less than', F8.2, / )
 9991 FORMAT( ' Relative machine ', A, ' is taken to be', D16.6 )
 9990 FORMAT( / 1X, A6, ' routines were not tested' )
 9989 FORMAT( / 1X, A6, ' driver routines were not tested' )
*
*     End of ZCHKAB
*
      END
