*> \brief \b SCHKRFP
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       PROGRAM SCHKRFP
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SCHKRFP is the main test program for the REAL linear
*> equation routines with RFP storage format
*>
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \verbatim
*>  MAXIN   INTEGER
*>          The number of different values that can be used for each of
*>          M, N, or NB
*>
*>  MAXRHS  INTEGER
*>          The maximum number of right hand sides
*>
*>  NTYPES  INTEGER
*>
*>  NMAX    INTEGER
*>          The maximum allowable value for N.
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
*> \ingroup single_lin
*
*  =====================================================================
      PROGRAM SCHKRFP
*
*  -- LAPACK test routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     April 2012
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            MAXIN
      PARAMETER          ( MAXIN = 12 )
      INTEGER            NMAX
      PARAMETER          ( NMAX =  50 )
      INTEGER            MAXRHS
      PARAMETER          ( MAXRHS = 16 )
      INTEGER            NTYPES
      PARAMETER          ( NTYPES = 9 )
      INTEGER            NIN, NOUT
      PARAMETER          ( NIN = 5, NOUT = 6 )
*     ..
*     .. Local Scalars ..
      LOGICAL            FATAL, TSTERR
      INTEGER            VERS_MAJOR, VERS_MINOR, VERS_PATCH
      INTEGER            I, NN, NNS, NNT
      REAL               EPS, S1, S2, THRESH
*     ..
*     .. Local Arrays ..
      INTEGER            NVAL( MAXIN ), NSVAL( MAXIN ), NTVAL( NTYPES )
      REAL               WORKA( NMAX, NMAX )
      REAL               WORKASAV( NMAX, NMAX )
      REAL               WORKB( NMAX, MAXRHS )
      REAL               WORKXACT( NMAX, MAXRHS )
      REAL               WORKBSAV( NMAX, MAXRHS )
      REAL               WORKX( NMAX, MAXRHS )
      REAL               WORKAFAC( NMAX, NMAX )
      REAL               WORKAINV( NMAX, NMAX )
      REAL               WORKARF( (NMAX*(NMAX+1))/2 )
      REAL               WORKAP( (NMAX*(NMAX+1))/2 )
      REAL               WORKARFINV( (NMAX*(NMAX+1))/2 )
      REAL               S_WORK_SLATMS( 3 * NMAX )
      REAL               S_WORK_SPOT01( NMAX )
      REAL               S_TEMP_SPOT02( NMAX, MAXRHS )
      REAL               S_TEMP_SPOT03( NMAX, NMAX )
      REAL               S_WORK_SLANSY( NMAX )
      REAL               S_WORK_SPOT02( NMAX )
      REAL               S_WORK_SPOT03( NMAX )
*     ..
*     .. External Functions ..
      REAL               SLAMCH, SECOND
      EXTERNAL           SLAMCH, SECOND
*     ..
*     .. External Subroutines ..
      EXTERNAL           ILAVER, SDRVRFP, SDRVRF1, SDRVRF2, SDRVRF3,
     +                   SDRVRF4
*     ..
*     .. Executable Statements ..
*
      S1 = SECOND( )
      FATAL = .FALSE.
*
*     Read a dummy line.
*
      READ( NIN, FMT = * )
*
*     Report LAPACK version tag (e.g. LAPACK-3.2.0)
*
      CALL ILAVER( VERS_MAJOR, VERS_MINOR, VERS_PATCH )
      WRITE( NOUT, FMT = 9994 ) VERS_MAJOR, VERS_MINOR, VERS_PATCH
*
*     Read the values of N
*
      READ( NIN, FMT = * )NN
      IF( NN.LT.1 ) THEN
         WRITE( NOUT, FMT = 9996 )' NN ', NN, 1
         NN = 0
         FATAL = .TRUE.
      ELSE IF( NN.GT.MAXIN ) THEN
         WRITE( NOUT, FMT = 9995 )' NN ', NN, MAXIN
         NN = 0
         FATAL = .TRUE.
      END IF
      READ( NIN, FMT = * )( NVAL( I ), I = 1, NN )
      DO 10 I = 1, NN
         IF( NVAL( I ).LT.0 ) THEN
            WRITE( NOUT, FMT = 9996 )' M  ', NVAL( I ), 0
            FATAL = .TRUE.
         ELSE IF( NVAL( I ).GT.NMAX ) THEN
            WRITE( NOUT, FMT = 9995 )' M  ', NVAL( I ), NMAX
            FATAL = .TRUE.
         END IF
   10 CONTINUE
      IF( NN.GT.0 )
     $   WRITE( NOUT, FMT = 9993 )'N   ', ( NVAL( I ), I = 1, NN )
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
*     Read the matrix types
*
      READ( NIN, FMT = * )NNT
      IF( NNT.LT.1 ) THEN
         WRITE( NOUT, FMT = 9996 )' NMA', NNT, 1
         NNT = 0
         FATAL = .TRUE.
      ELSE IF( NNT.GT.NTYPES ) THEN
         WRITE( NOUT, FMT = 9995 )' NMA', NNT, NTYPES
         NNT = 0
         FATAL = .TRUE.
      END IF
      READ( NIN, FMT = * )( NTVAL( I ), I = 1, NNT )
      DO 320 I = 1, NNT
         IF( NTVAL( I ).LT.0 ) THEN
            WRITE( NOUT, FMT = 9996 )'TYPE', NTVAL( I ), 0
            FATAL = .TRUE.
         ELSE IF( NTVAL( I ).GT.NTYPES ) THEN
            WRITE( NOUT, FMT = 9995 )'TYPE', NTVAL( I ), NTYPES
            FATAL = .TRUE.
         END IF
  320 CONTINUE
      IF( NNT.GT.0 )
     $   WRITE( NOUT, FMT = 9993 )'TYPE', ( NTVAL( I ), I = 1, NNT )
*
*     Read the threshold value for the test ratios.
*
      READ( NIN, FMT = * )THRESH
      WRITE( NOUT, FMT = 9992 )THRESH
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
      EPS = SLAMCH( 'Underflow threshold' )
      WRITE( NOUT, FMT = 9991 )'underflow', EPS
      EPS = SLAMCH( 'Overflow threshold' )
      WRITE( NOUT, FMT = 9991 )'overflow ', EPS
      EPS = SLAMCH( 'Epsilon' )
      WRITE( NOUT, FMT = 9991 )'precision', EPS
      WRITE( NOUT, FMT = * )
*
*     Test the error exit of:
*
      IF( TSTERR )
     $   CALL SERRRFP( NOUT )
*
*     Test the routines: spftrf, spftri, spftrs (as in SDRVPO).
*     This also tests the routines: stfsm, stftri, stfttr, strttf.
*
      CALL SDRVRFP( NOUT, NN, NVAL, NNS, NSVAL, NNT, NTVAL, THRESH,
     $              WORKA, WORKASAV, WORKAFAC, WORKAINV, WORKB,
     $              WORKBSAV, WORKXACT, WORKX, WORKARF, WORKARFINV,
     $              S_WORK_SLATMS, S_WORK_SPOT01, S_TEMP_SPOT02,
     $              S_TEMP_SPOT03, S_WORK_SLANSY, S_WORK_SPOT02,
     $              S_WORK_SPOT03 )
*
*     Test the routine: slansf
*
      CALL SDRVRF1( NOUT, NN, NVAL, THRESH, WORKA, NMAX, WORKARF,
     +              S_WORK_SLANSY )
*
*     Test the conversion routines:
*       stfttp, stpttf, stfttr, strttf, strttp and stpttr.
*
      CALL SDRVRF2( NOUT, NN, NVAL, WORKA, NMAX, WORKARF,
     +              WORKAP, WORKASAV )
*
*     Test the routine: stfsm
*
      CALL SDRVRF3( NOUT, NN, NVAL, THRESH, WORKA, NMAX, WORKARF,
     +              WORKAINV, WORKAFAC, S_WORK_SLANSY,
     +              S_WORK_SPOT03, S_WORK_SPOT01 )
*
*
*     Test the routine: ssfrk
*
      CALL SDRVRF4( NOUT, NN, NVAL, THRESH, WORKA, WORKAFAC, NMAX,
     +              WORKARF, WORKAINV, NMAX, S_WORK_SLANSY)
*
      CLOSE ( NIN )
      S2 = SECOND( )
      WRITE( NOUT, FMT = 9998 )
      WRITE( NOUT, FMT = 9997 )S2 - S1
*
 9999 FORMAT( / ' Execution not attempted due to input errors' )
 9998 FORMAT( / ' End of tests' )
 9997 FORMAT( ' Total time used = ', F12.2, ' seconds', / )
 9996 FORMAT( ' !! Invalid input value: ', A4, '=', I6, '; must be >=',
     $      I6 )
 9995 FORMAT( ' !! Invalid input value: ', A4, '=', I6, '; must be <=',
     $      I6 )
 9994 FORMAT( /  ' Tests of the REAL LAPACK RFP routines ',
     $      / ' LAPACK VERSION ', I1, '.', I1, '.', I1,
     $      / / ' The following parameter values will be used:' )
 9993 FORMAT( 4X, A4, ':  ', 10I6, / 11X, 10I6 )
 9992 FORMAT( / ' Routines pass computational tests if test ratio is ',
     $      'less than', F8.2, / )
 9991 FORMAT( ' Relative machine ', A, ' is taken to be', D16.6 )
*
*     End of SCHKRFP
*
      END
