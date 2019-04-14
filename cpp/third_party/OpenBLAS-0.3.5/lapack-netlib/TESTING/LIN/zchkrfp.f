*> \brief \b ZCHKRFP
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       PROGRAM ZCHKRFP
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZCHKRFP is the main test program for the COMPLEX*16 linear equation
*> routines with RFP storage format
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
*> \ingroup complex16_lin
*
*  =====================================================================
      PROGRAM ZCHKRFP
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
      DOUBLE PRECISION   EPS, S1, S2, THRESH

*     ..
*     .. Local Arrays ..
      INTEGER            NVAL( MAXIN ), NSVAL( MAXIN ), NTVAL( NTYPES )
      COMPLEX*16         WORKA( NMAX, NMAX )
      COMPLEX*16         WORKASAV( NMAX, NMAX )
      COMPLEX*16         WORKB( NMAX, MAXRHS )
      COMPLEX*16         WORKXACT( NMAX, MAXRHS )
      COMPLEX*16         WORKBSAV( NMAX, MAXRHS )
      COMPLEX*16         WORKX( NMAX, MAXRHS )
      COMPLEX*16         WORKAFAC( NMAX, NMAX )
      COMPLEX*16         WORKAINV( NMAX, NMAX )
      COMPLEX*16         WORKARF( (NMAX*(NMAX+1))/2 )
      COMPLEX*16         WORKAP( (NMAX*(NMAX+1))/2 )
      COMPLEX*16         WORKARFINV( (NMAX*(NMAX+1))/2 )
      COMPLEX*16         Z_WORK_ZLATMS( 3 * NMAX )
      COMPLEX*16         Z_WORK_ZPOT02( NMAX, MAXRHS )
      COMPLEX*16         Z_WORK_ZPOT03( NMAX, NMAX )
      DOUBLE PRECISION   D_WORK_ZLATMS( NMAX )
      DOUBLE PRECISION   D_WORK_ZLANHE( NMAX )
      DOUBLE PRECISION   D_WORK_ZPOT01( NMAX )
      DOUBLE PRECISION   D_WORK_ZPOT02( NMAX )
      DOUBLE PRECISION   D_WORK_ZPOT03( NMAX )
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH, DSECND
      EXTERNAL           DLAMCH, DSECND
*     ..
*     .. External Subroutines ..
      EXTERNAL           ILAVER, ZDRVRFP, ZDRVRF1, ZDRVRF2, ZDRVRF3,
     +                   ZDRVRF4
*     ..
*     .. Executable Statements ..
*
      S1 = DSECND( )
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
      EPS = DLAMCH( 'Underflow threshold' )
      WRITE( NOUT, FMT = 9991 )'underflow', EPS
      EPS = DLAMCH( 'Overflow threshold' )
      WRITE( NOUT, FMT = 9991 )'overflow ', EPS
      EPS = DLAMCH( 'Epsilon' )
      WRITE( NOUT, FMT = 9991 )'precision', EPS
      WRITE( NOUT, FMT = * )
*
*     Test the error exit of:
*
      IF( TSTERR )
     $   CALL ZERRRFP( NOUT )
*
*    Test the routines: zpftrf, zpftri, zpftrs (as in ZDRVPO).
*    This also tests the routines: ztfsm, ztftri, ztfttr, ztrttf.
*
      CALL ZDRVRFP( NOUT, NN, NVAL, NNS, NSVAL, NNT, NTVAL, THRESH,
     $              WORKA, WORKASAV, WORKAFAC, WORKAINV, WORKB,
     $              WORKBSAV, WORKXACT, WORKX, WORKARF, WORKARFINV,
     $              Z_WORK_ZLATMS, Z_WORK_ZPOT02,
     $              Z_WORK_ZPOT03, D_WORK_ZLATMS, D_WORK_ZLANHE,
     $              D_WORK_ZPOT01, D_WORK_ZPOT02, D_WORK_ZPOT03 )
*
*    Test the routine: zlanhf
*
      CALL ZDRVRF1( NOUT, NN, NVAL, THRESH, WORKA, NMAX, WORKARF,
     +              D_WORK_ZLANHE )
*
*    Test the conversion routines:
*       zhfttp, ztpthf, ztfttr, ztrttf, ztrttp and ztpttr.
*
      CALL ZDRVRF2( NOUT, NN, NVAL, WORKA, NMAX, WORKARF,
     +              WORKAP, WORKASAV )
*
*    Test the routine: ztfsm
*
      CALL ZDRVRF3( NOUT, NN, NVAL, THRESH, WORKA, NMAX, WORKARF,
     +              WORKAINV, WORKAFAC, D_WORK_ZLANHE,
     +              Z_WORK_ZPOT03, Z_WORK_ZPOT02 )

*
*    Test the routine: zhfrk
*
      CALL ZDRVRF4( NOUT, NN, NVAL, THRESH, WORKA, WORKAFAC, NMAX,
     +              WORKARF, WORKAINV, NMAX,D_WORK_ZLANHE)
*
      CLOSE ( NIN )
      S2 = DSECND( )
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
 9994 FORMAT( /  ' Tests of the COMPLEX*16 LAPACK RFP routines ',
     $      / ' LAPACK VERSION ', I1, '.', I1, '.', I1,
     $      / / ' The following parameter values will be used:' )
 9993 FORMAT( 4X, A4, ':  ', 10I6, / 11X, 10I6 )
 9992 FORMAT( / ' Routines pass computational tests if test ratio is ',
     $      'less than', F8.2, / )
 9991 FORMAT( ' Relative machine ', A, ' is taken to be', D16.6 )
*
*     End of ZCHKRFP
*
      END
