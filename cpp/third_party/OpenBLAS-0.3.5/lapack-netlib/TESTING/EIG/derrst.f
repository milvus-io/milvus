*> \brief \b DERRST
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DERRST( PATH, NUNIT )
*
*       .. Scalar Arguments ..
*       CHARACTER*3        PATH
*       INTEGER            NUNIT
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DERRST tests the error exits for DSYTRD, DORGTR, DORMTR, DSPTRD,
*> DOPGTR, DOPMTR, DSTEQR, SSTERF, SSTEBZ, SSTEIN, DPTEQR, DSBTRD,
*> DSYEV, SSYEVX, SSYEVD, DSBEV, SSBEVX, SSBEVD,
*> DSPEV, SSPEVX, SSPEVD, DSTEV, SSTEVX, SSTEVD, and SSTEDC.
*> DSYEVD_2STAGE, DSYEVR_2STAGE, DSYEVX_2STAGE,
*> DSYEV_2STAGE, DSBEV_2STAGE, DSBEVD_2STAGE,
*> DSBEVX_2STAGE, DSYTRD_2STAGE, DSYTRD_SY2SB,
*> DSYTRD_SB2ST
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] PATH
*> \verbatim
*>          PATH is CHARACTER*3
*>          The LAPACK path name for the routines to be tested.
*> \endverbatim
*>
*> \param[in] NUNIT
*> \verbatim
*>          NUNIT is INTEGER
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
*> \ingroup double_eig
*
*  =====================================================================
      SUBROUTINE DERRST( PATH, NUNIT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER*3        PATH
      INTEGER            NUNIT
*     ..
*
*  =====================================================================
*
*     NMAX has to be at least 3 or LIW may be too small
*     .. Parameters ..
      INTEGER            NMAX, LIW, LW
      PARAMETER          ( NMAX = 3, LIW = 12*NMAX, LW = 20*NMAX )
*     ..
*     .. Local Scalars ..
      CHARACTER*2        C2
      INTEGER            I, INFO, J, M, N, NSPLIT, NT
*     ..
*     .. Local Arrays ..
      INTEGER            I1( NMAX ), I2( NMAX ), I3( NMAX ), IW( LIW )
      DOUBLE PRECISION   A( NMAX, NMAX ), C( NMAX, NMAX ), D( NMAX ),
     $                   E( NMAX ), Q( NMAX, NMAX ), R( NMAX ),
     $                   TAU( NMAX ), W( LW ), X( NMAX ),
     $                   Z( NMAX, NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           CHKXER, DOPGTR, DOPMTR, DORGTR, DORMTR, DPTEQR,
     $                   DSBEV, DSBEVD, DSBEVX, DSBTRD, DSPEV, DSPEVD,
     $                   DSPEVX, DSPTRD, DSTEBZ, DSTEDC, DSTEIN, DSTEQR,
     $                   DSTERF, DSTEV, DSTEVD, DSTEVR, DSTEVX, DSYEV,
     $                   DSYEVD, DSYEVR, DSYEVX, DSYTRD,
     $                   DSYEVD_2STAGE, DSYEVR_2STAGE, DSYEVX_2STAGE,
     $                   DSYEV_2STAGE, DSBEV_2STAGE, DSBEVD_2STAGE,
     $                   DSBEVX_2STAGE, DSYTRD_2STAGE, DSYTRD_SY2SB,
     $                   DSYTRD_SB2ST
*     ..
*     .. Scalars in Common ..
      LOGICAL            LERR, OK
      CHARACTER*32       SRNAMT
      INTEGER            INFOT, NOUT
*     ..
*     .. Common blocks ..
      COMMON             / INFOC / INFOT, NOUT, OK, LERR
      COMMON             / SRNAMC / SRNAMT
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE
*     ..
*     .. Executable Statements ..
*
      NOUT = NUNIT
      WRITE( NOUT, FMT = * )
      C2 = PATH( 2: 3 )
*
*     Set the variables to innocuous values.
*
      DO 20 J = 1, NMAX
         DO 10 I = 1, NMAX
            A( I, J ) = 1.D0 / DBLE( I+J )
   10    CONTINUE
   20 CONTINUE
      DO 30 J = 1, NMAX
         D( J ) = DBLE( J )
         E( J ) = 0.0D0
         I1( J ) = J
         I2( J ) = J
         TAU( J ) = 1.D0
   30 CONTINUE
      OK = .TRUE.
      NT = 0
*
*     Test error exits for the ST path.
*
      IF( LSAMEN( 2, C2, 'ST' ) ) THEN
*
*        DSYTRD
*
         SRNAMT = 'DSYTRD'
         INFOT = 1
         CALL DSYTRD( '/', 0, A, 1, D, E, TAU, W, 1, INFO )
         CALL CHKXER( 'DSYTRD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSYTRD( 'U', -1, A, 1, D, E, TAU, W, 1, INFO )
         CALL CHKXER( 'DSYTRD', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DSYTRD( 'U', 2, A, 1, D, E, TAU, W, 1, INFO )
         CALL CHKXER( 'DSYTRD', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DSYTRD( 'U', 0, A, 1, D, E, TAU, W, 0, INFO )
         CALL CHKXER( 'DSYTRD', INFOT, NOUT, LERR, OK )
         NT = NT + 4
*
*        DSYTRD_2STAGE
*
         SRNAMT = 'DSYTRD_2STAGE'
         INFOT = 1
         CALL DSYTRD_2STAGE( '/', 'U', 0, A, 1, D, E, TAU, 
     $                                  C, 1, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 1
         CALL DSYTRD_2STAGE( 'H', 'U', 0, A, 1, D, E, TAU, 
     $                                  C, 1, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSYTRD_2STAGE( 'N', '/', 0, A, 1, D, E, TAU, 
     $                                  C, 1, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSYTRD_2STAGE( 'N', 'U', -1, A, 1, D, E, TAU, 
     $                                  C, 1, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DSYTRD_2STAGE( 'N', 'U', 2, A, 1, D, E, TAU, 
     $                                  C, 1, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DSYTRD_2STAGE( 'N', 'U', 0, A, 1, D, E, TAU, 
     $                                  C, 0, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DSYTRD_2STAGE( 'N', 'U', 0, A, 1, D, E, TAU, 
     $                                  C, 1, W, 0, INFO )
         CALL CHKXER( 'DSYTRD_2STAGE', INFOT, NOUT, LERR, OK )
         NT = NT + 7
*
*        DSYTRD_SY2SB
*
         SRNAMT = 'DSYTRD_SY2SB'
         INFOT = 1
         CALL DSYTRD_SY2SB( '/', 0, 0, A, 1, C, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_SY2SB', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSYTRD_SY2SB( 'U', -1, 0, A, 1, C, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_SY2SB', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSYTRD_SY2SB( 'U', 0, -1, A, 1, C, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_SY2SB', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DSYTRD_SY2SB( 'U', 2, 0, A, 1, C, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_SY2SB', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DSYTRD_SY2SB( 'U', 0, 2, A, 1, C, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_SY2SB', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DSYTRD_SY2SB( 'U', 0, 0, A, 1, C, 1, TAU, W, 0, INFO )
         CALL CHKXER( 'DSYTRD_SY2SB', INFOT, NOUT, LERR, OK )
         NT = NT + 6
*
*        DSYTRD_SB2ST
*
         SRNAMT = 'DSYTRD_SB2ST'
         INFOT = 1
         CALL DSYTRD_SB2ST( '/', 'N', 'U', 0, 0, A, 1, D, E, 
     $                                    C, 1, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_SB2ST', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSYTRD_SB2ST( 'Y', '/', 'U', 0, 0, A, 1, D, E, 
     $                                    C, 1, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_SB2ST', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSYTRD_SB2ST( 'Y', 'H', 'U', 0, 0, A, 1, D, E, 
     $                                    C, 1, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_SB2ST', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSYTRD_SB2ST( 'Y', 'N', '/', 0, 0, A, 1, D, E, 
     $                                    C, 1, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_SB2ST', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DSYTRD_SB2ST( 'Y', 'N', 'U', -1, 0, A, 1, D, E, 
     $                                    C, 1, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_SB2ST', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DSYTRD_SB2ST( 'Y', 'N', 'U', 0, -1, A, 1, D, E, 
     $                                    C, 1, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_SB2ST', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DSYTRD_SB2ST( 'Y', 'N', 'U', 0, 1, A, 1, D, E, 
     $                                    C, 1, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_SB2ST', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DSYTRD_SB2ST( 'Y', 'N', 'U', 0, 0, A, 1, D, E, 
     $                                    C, 0, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_SB2ST', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL DSYTRD_SB2ST( 'Y', 'N', 'U', 0, 0, A, 1, D, E, 
     $                                    C, 1, W, 0, INFO )
         CALL CHKXER( 'DSYTRD_SB2ST', INFOT, NOUT, LERR, OK )
         NT = NT + 9
*
*        DORGTR
*
         SRNAMT = 'DORGTR'
         INFOT = 1
         CALL DORGTR( '/', 0, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'DORGTR', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DORGTR( 'U', -1, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'DORGTR', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DORGTR( 'U', 2, A, 1, TAU, W, 1, INFO )
         CALL CHKXER( 'DORGTR', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DORGTR( 'U', 3, A, 3, TAU, W, 1, INFO )
         CALL CHKXER( 'DORGTR', INFOT, NOUT, LERR, OK )
         NT = NT + 4
*
*        DORMTR
*
         SRNAMT = 'DORMTR'
         INFOT = 1
         CALL DORMTR( '/', 'U', 'N', 0, 0, A, 1, TAU, C, 1, W, 1, INFO )
         CALL CHKXER( 'DORMTR', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DORMTR( 'L', '/', 'N', 0, 0, A, 1, TAU, C, 1, W, 1, INFO )
         CALL CHKXER( 'DORMTR', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DORMTR( 'L', 'U', '/', 0, 0, A, 1, TAU, C, 1, W, 1, INFO )
         CALL CHKXER( 'DORMTR', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DORMTR( 'L', 'U', 'N', -1, 0, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'DORMTR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DORMTR( 'L', 'U', 'N', 0, -1, A, 1, TAU, C, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'DORMTR', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DORMTR( 'L', 'U', 'N', 2, 0, A, 1, TAU, C, 2, W, 1, INFO )
         CALL CHKXER( 'DORMTR', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DORMTR( 'R', 'U', 'N', 0, 2, A, 1, TAU, C, 1, W, 1, INFO )
         CALL CHKXER( 'DORMTR', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DORMTR( 'L', 'U', 'N', 2, 0, A, 2, TAU, C, 1, W, 1, INFO )
         CALL CHKXER( 'DORMTR', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DORMTR( 'L', 'U', 'N', 0, 2, A, 1, TAU, C, 1, W, 1, INFO )
         CALL CHKXER( 'DORMTR', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DORMTR( 'R', 'U', 'N', 2, 0, A, 1, TAU, C, 2, W, 1, INFO )
         CALL CHKXER( 'DORMTR', INFOT, NOUT, LERR, OK )
         NT = NT + 10
*
*        DSPTRD
*
         SRNAMT = 'DSPTRD'
         INFOT = 1
         CALL DSPTRD( '/', 0, A, D, E, TAU, INFO )
         CALL CHKXER( 'DSPTRD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSPTRD( 'U', -1, A, D, E, TAU, INFO )
         CALL CHKXER( 'DSPTRD', INFOT, NOUT, LERR, OK )
         NT = NT + 2
*
*        DOPGTR
*
         SRNAMT = 'DOPGTR'
         INFOT = 1
         CALL DOPGTR( '/', 0, A, TAU, Z, 1, W, INFO )
         CALL CHKXER( 'DOPGTR', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DOPGTR( 'U', -1, A, TAU, Z, 1, W, INFO )
         CALL CHKXER( 'DOPGTR', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DOPGTR( 'U', 2, A, TAU, Z, 1, W, INFO )
         CALL CHKXER( 'DOPGTR', INFOT, NOUT, LERR, OK )
         NT = NT + 3
*
*        DOPMTR
*
         SRNAMT = 'DOPMTR'
         INFOT = 1
         CALL DOPMTR( '/', 'U', 'N', 0, 0, A, TAU, C, 1, W, INFO )
         CALL CHKXER( 'DOPMTR', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DOPMTR( 'L', '/', 'N', 0, 0, A, TAU, C, 1, W, INFO )
         CALL CHKXER( 'DOPMTR', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DOPMTR( 'L', 'U', '/', 0, 0, A, TAU, C, 1, W, INFO )
         CALL CHKXER( 'DOPMTR', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DOPMTR( 'L', 'U', 'N', -1, 0, A, TAU, C, 1, W, INFO )
         CALL CHKXER( 'DOPMTR', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DOPMTR( 'L', 'U', 'N', 0, -1, A, TAU, C, 1, W, INFO )
         CALL CHKXER( 'DOPMTR', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DOPMTR( 'L', 'U', 'N', 2, 0, A, TAU, C, 1, W, INFO )
         CALL CHKXER( 'DOPMTR', INFOT, NOUT, LERR, OK )
         NT = NT + 6
*
*        DPTEQR
*
         SRNAMT = 'DPTEQR'
         INFOT = 1
         CALL DPTEQR( '/', 0, D, E, Z, 1, W, INFO )
         CALL CHKXER( 'DPTEQR', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DPTEQR( 'N', -1, D, E, Z, 1, W, INFO )
         CALL CHKXER( 'DPTEQR', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DPTEQR( 'V', 2, D, E, Z, 1, W, INFO )
         CALL CHKXER( 'DPTEQR', INFOT, NOUT, LERR, OK )
         NT = NT + 3
*
*        DSTEBZ
*
         SRNAMT = 'DSTEBZ'
         INFOT = 1
         CALL DSTEBZ( '/', 'E', 0, 0.0D0, 1.0D0, 1, 0, 0.0D0, D, E, M,
     $                NSPLIT, X, I1, I2, W, IW, INFO )
         CALL CHKXER( 'DSTEBZ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSTEBZ( 'A', '/', 0, 0.0D0, 0.0D0, 0, 0, 0.0D0, D, E, M,
     $                NSPLIT, X, I1, I2, W, IW, INFO )
         CALL CHKXER( 'DSTEBZ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSTEBZ( 'A', 'E', -1, 0.0D0, 0.0D0, 0, 0, 0.0D0, D, E, M,
     $                NSPLIT, X, I1, I2, W, IW, INFO )
         CALL CHKXER( 'DSTEBZ', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DSTEBZ( 'V', 'E', 0, 0.0D0, 0.0D0, 0, 0, 0.0D0, D, E, M,
     $                NSPLIT, X, I1, I2, W, IW, INFO )
         CALL CHKXER( 'DSTEBZ', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DSTEBZ( 'I', 'E', 0, 0.0D0, 0.0D0, 0, 0, 0.0D0, D, E, M,
     $                NSPLIT, X, I1, I2, W, IW, INFO )
         CALL CHKXER( 'DSTEBZ', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DSTEBZ( 'I', 'E', 1, 0.0D0, 0.0D0, 2, 1, 0.0D0, D, E, M,
     $                NSPLIT, X, I1, I2, W, IW, INFO )
         CALL CHKXER( 'DSTEBZ', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DSTEBZ( 'I', 'E', 1, 0.0D0, 0.0D0, 1, 0, 0.0D0, D, E, M,
     $                NSPLIT, X, I1, I2, W, IW, INFO )
         CALL CHKXER( 'DSTEBZ', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DSTEBZ( 'I', 'E', 1, 0.0D0, 0.0D0, 1, 2, 0.0D0, D, E, M,
     $                NSPLIT, X, I1, I2, W, IW, INFO )
         CALL CHKXER( 'DSTEBZ', INFOT, NOUT, LERR, OK )
         NT = NT + 8
*
*        DSTEIN
*
         SRNAMT = 'DSTEIN'
         INFOT = 1
         CALL DSTEIN( -1, D, E, 0, X, I1, I2, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSTEIN', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DSTEIN( 0, D, E, -1, X, I1, I2, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSTEIN', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DSTEIN( 0, D, E, 1, X, I1, I2, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSTEIN', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DSTEIN( 2, D, E, 0, X, I1, I2, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSTEIN', INFOT, NOUT, LERR, OK )
         NT = NT + 4
*
*        DSTEQR
*
         SRNAMT = 'DSTEQR'
         INFOT = 1
         CALL DSTEQR( '/', 0, D, E, Z, 1, W, INFO )
         CALL CHKXER( 'DSTEQR', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSTEQR( 'N', -1, D, E, Z, 1, W, INFO )
         CALL CHKXER( 'DSTEQR', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DSTEQR( 'V', 2, D, E, Z, 1, W, INFO )
         CALL CHKXER( 'DSTEQR', INFOT, NOUT, LERR, OK )
         NT = NT + 3
*
*        DSTERF
*
         SRNAMT = 'DSTERF'
         INFOT = 1
         CALL DSTERF( -1, D, E, INFO )
         CALL CHKXER( 'DSTERF', INFOT, NOUT, LERR, OK )
         NT = NT + 1
*
*        DSTEDC
*
         SRNAMT = 'DSTEDC'
         INFOT = 1
         CALL DSTEDC( '/', 0, D, E, Z, 1, W, 1, IW, 1, INFO )
         CALL CHKXER( 'DSTEDC', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSTEDC( 'N', -1, D, E, Z, 1, W, 1, IW, 1, INFO )
         CALL CHKXER( 'DSTEDC', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DSTEDC( 'V', 2, D, E, Z, 1, W, 23, IW, 28, INFO )
         CALL CHKXER( 'DSTEDC', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSTEDC( 'N', 1, D, E, Z, 1, W, 0, IW, 1, INFO )
         CALL CHKXER( 'DSTEDC', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSTEDC( 'I', 2, D, E, Z, 2, W, 0, IW, 12, INFO )
         CALL CHKXER( 'DSTEDC', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSTEDC( 'V', 2, D, E, Z, 2, W, 0, IW, 28, INFO )
         CALL CHKXER( 'DSTEDC', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DSTEDC( 'N', 1, D, E, Z, 1, W, 1, IW, 0, INFO )
         CALL CHKXER( 'DSTEDC', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DSTEDC( 'I', 2, D, E, Z, 2, W, 19, IW, 0, INFO )
         CALL CHKXER( 'DSTEDC', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DSTEDC( 'V', 2, D, E, Z, 2, W, 23, IW, 0, INFO )
         CALL CHKXER( 'DSTEDC', INFOT, NOUT, LERR, OK )
         NT = NT + 9
*
*        DSTEVD
*
         SRNAMT = 'DSTEVD'
         INFOT = 1
         CALL DSTEVD( '/', 0, D, E, Z, 1, W, 1, IW, 1, INFO )
         CALL CHKXER( 'DSTEVD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSTEVD( 'N', -1, D, E, Z, 1, W, 1, IW, 1, INFO )
         CALL CHKXER( 'DSTEVD', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DSTEVD( 'V', 2, D, E, Z, 1, W, 19, IW, 12, INFO )
         CALL CHKXER( 'DSTEVD', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSTEVD( 'N', 1, D, E, Z, 1, W, 0, IW, 1, INFO )
         CALL CHKXER( 'DSTEVD', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSTEVD( 'V', 2, D, E, Z, 2, W, 12, IW, 12, INFO )
         CALL CHKXER( 'DSTEVD', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DSTEVD( 'N', 0, D, E, Z, 1, W, 1, IW, 0, INFO )
         CALL CHKXER( 'DSTEVD', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DSTEVD( 'V', 2, D, E, Z, 2, W, 19, IW, 11, INFO )
         CALL CHKXER( 'DSTEVD', INFOT, NOUT, LERR, OK )
         NT = NT + 7
*
*        DSTEV
*
         SRNAMT = 'DSTEV '
         INFOT = 1
         CALL DSTEV( '/', 0, D, E, Z, 1, W, INFO )
         CALL CHKXER( 'DSTEV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSTEV( 'N', -1, D, E, Z, 1, W, INFO )
         CALL CHKXER( 'DSTEV ', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DSTEV( 'V', 2, D, E, Z, 1, W, INFO )
         CALL CHKXER( 'DSTEV ', INFOT, NOUT, LERR, OK )
         NT = NT + 3
*
*        DSTEVX
*
         SRNAMT = 'DSTEVX'
         INFOT = 1
         CALL DSTEVX( '/', 'A', 0, D, E, 0.0D0, 0.0D0, 0, 0, 0.0D0, M,
     $                X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSTEVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSTEVX( 'N', '/', 0, D, E, 0.0D0, 1.0D0, 1, 0, 0.0D0, M,
     $                X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSTEVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSTEVX( 'N', 'A', -1, D, E, 0.0D0, 0.0D0, 0, 0, 0.0D0, M,
     $                X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSTEVX', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DSTEVX( 'N', 'V', 1, D, E, 0.0D0, 0.0D0, 0, 0, 0.0D0, M,
     $                X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSTEVX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSTEVX( 'N', 'I', 1, D, E, 0.0D0, 0.0D0, 0, 0, 0.0D0, M,
     $                X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSTEVX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSTEVX( 'N', 'I', 1, D, E, 0.0D0, 0.0D0, 2, 1, 0.0D0, M,
     $                X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSTEVX', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DSTEVX( 'N', 'I', 2, D, E, 0.0D0, 0.0D0, 2, 1, 0.0D0, M,
     $                X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSTEVX', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DSTEVX( 'N', 'I', 1, D, E, 0.0D0, 0.0D0, 1, 2, 0.0D0, M,
     $                X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSTEVX', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL DSTEVX( 'V', 'A', 2, D, E, 0.0D0, 0.0D0, 0, 0, 0.0D0, M,
     $                X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSTEVX', INFOT, NOUT, LERR, OK )
         NT = NT + 9
*
*        DSTEVR
*
         N = 1
         SRNAMT = 'DSTEVR'
         INFOT = 1
         CALL DSTEVR( '/', 'A', 0, D, E, 0.0D0, 0.0D0, 1, 1, 0.0D0, M,
     $                R, Z, 1, IW, X, 20*N, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSTEVR', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSTEVR( 'V', '/', 0, D, E, 0.0D0, 0.0D0, 1, 1, 0.0D0, M,
     $                R, Z, 1, IW, X, 20*N, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSTEVR', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSTEVR( 'V', 'A', -1, D, E, 0.0D0, 0.0D0, 1, 1, 0.0D0, M,
     $                R, Z, 1, IW, X, 20*N, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSTEVR', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DSTEVR( 'V', 'V', 1, D, E, 0.0D0, 0.0D0, 1, 1, 0.0D0, M,
     $                R, Z, 1, IW, X, 20*N, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSTEVR', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSTEVR( 'V', 'I', 1, D, E, 0.0D0, 0.0D0, 0, 1, 0.0D0, M,
     $                W, Z, 1, IW, X, 20*N, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSTEVR', INFOT, NOUT, LERR, OK )
         INFOT = 9
         N = 2
         CALL DSTEVR( 'V', 'I', 2, D, E, 0.0D0, 0.0D0, 2, 1, 0.0D0, M,
     $                W, Z, 1, IW, X, 20*N, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSTEVR', INFOT, NOUT, LERR, OK )
         INFOT = 14
         N = 1
         CALL DSTEVR( 'V', 'I', 1, D, E, 0.0D0, 0.0D0, 1, 1, 0.0D0, M,
     $                W, Z, 0, IW, X, 20*N, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSTEVR', INFOT, NOUT, LERR, OK )
         INFOT = 17
         CALL DSTEVR( 'V', 'I', 1, D, E, 0.0D0, 0.0D0, 1, 1, 0.0D0, M,
     $                W, Z, 1, IW, X, 20*N-1, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSTEVR', INFOT, NOUT, LERR, OK )
         INFOT = 19
         CALL DSTEVR( 'V', 'I', 1, D, E, 0.0D0, 0.0D0, 1, 1, 0.0D0, M,
     $                W, Z, 1, IW, X, 20*N, IW( 2*N+1 ), 10*N-1, INFO )
         CALL CHKXER( 'DSTEVR', INFOT, NOUT, LERR, OK )
         NT = NT + 9
*
*        DSYEVD
*
         SRNAMT = 'DSYEVD'
         INFOT = 1
         CALL DSYEVD( '/', 'U', 0, A, 1, X, W, 1, IW, 1, INFO )
         CALL CHKXER( 'DSYEVD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSYEVD( 'N', '/', 0, A, 1, X, W, 1, IW, 1, INFO )
         CALL CHKXER( 'DSYEVD', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSYEVD( 'N', 'U', -1, A, 1, X, W, 1, IW, 1, INFO )
         CALL CHKXER( 'DSYEVD', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DSYEVD( 'N', 'U', 2, A, 1, X, W, 3, IW, 1, INFO )
         CALL CHKXER( 'DSYEVD', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSYEVD( 'N', 'U', 1, A, 1, X, W, 0, IW, 1, INFO )
         CALL CHKXER( 'DSYEVD', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSYEVD( 'N', 'U', 2, A, 2, X, W, 4, IW, 1, INFO )
         CALL CHKXER( 'DSYEVD', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSYEVD( 'V', 'U', 2, A, 2, X, W, 20, IW, 12, INFO )
         CALL CHKXER( 'DSYEVD', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DSYEVD( 'N', 'U', 1, A, 1, X, W, 1, IW, 0, INFO )
         CALL CHKXER( 'DSYEVD', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DSYEVD( 'N', 'U', 2, A, 2, X, W, 5, IW, 0, INFO )
         CALL CHKXER( 'DSYEVD', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DSYEVD( 'V', 'U', 2, A, 2, X, W, 27, IW, 11, INFO )
         CALL CHKXER( 'DSYEVD', INFOT, NOUT, LERR, OK )
         NT = NT + 10
*
*        DSYEVD_2STAGE
*
         SRNAMT = 'DSYEVD_2STAGE'
         INFOT = 1
         CALL DSYEVD_2STAGE( '/', 'U', 0, A, 1, X, W, 1, IW, 1, INFO )
         CALL CHKXER( 'DSYEVD_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 1
         CALL DSYEVD_2STAGE( 'V', 'U', 0, A, 1, X, W, 1, IW, 1, INFO )
         CALL CHKXER( 'DSYEVD_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSYEVD_2STAGE( 'N', '/', 0, A, 1, X, W, 1, IW, 1, INFO )
         CALL CHKXER( 'DSYEVD_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSYEVD_2STAGE( 'N', 'U', -1, A, 1, X, W, 1, IW, 1, INFO )
         CALL CHKXER( 'DSYEVD_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DSYEVD_2STAGE( 'N', 'U', 2, A, 1, X, W, 3, IW, 1, INFO )
         CALL CHKXER( 'DSYEVD_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSYEVD_2STAGE( 'N', 'U', 1, A, 1, X, W, 0, IW, 1, INFO )
         CALL CHKXER( 'DSYEVD_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSYEVD_2STAGE( 'N', 'U', 2, A, 2, X, W, 4, IW, 1, INFO )
         CALL CHKXER( 'DSYEVD_2STAGE', INFOT, NOUT, LERR, OK )
*         INFOT = 8
*         CALL DSYEVD_2STAGE( 'V', 'U', 2, A, 2, X, W, 20, IW, 12, INFO )
*         CALL CHKXER( 'DSYEVD_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DSYEVD_2STAGE( 'N', 'U', 1, A, 1, X, W, 1, IW, 0, INFO )
         CALL CHKXER( 'DSYEVD_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DSYEVD_2STAGE( 'N', 'U', 2, A, 2, X, W, 25, IW, 0, INFO )
         CALL CHKXER( 'DSYEVD_2STAGE', INFOT, NOUT, LERR, OK )
*         INFOT = 10
*         CALL DSYEVD_2STAGE( 'V', 'U', 2, A, 2, X, W, 27, IW, 11, INFO )
*         CALL CHKXER( 'DSYEVD_2STAGE', INFOT, NOUT, LERR, OK )
         NT = NT + 9
*
*        DSYEVR
*
         SRNAMT = 'DSYEVR'
         N = 1
         INFOT = 1
         CALL DSYEVR( '/', 'A', 'U', 0, A, 1, 0.0D0, 0.0D0, 1, 1, 0.0D0,
     $                M, R, Z, 1, IW, Q, 26*N, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSYEVR', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSYEVR( 'V', '/', 'U', 0, A, 1, 0.0D0, 0.0D0, 1, 1, 0.0D0,
     $                M, R, Z, 1, IW, Q, 26*N, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSYEVR', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSYEVR( 'V', 'A', '/', -1, A, 1, 0.0D0, 0.0D0, 1, 1,
     $                0.0D0, M, R, Z, 1, IW, Q, 26*N, IW( 2*N+1 ), 10*N,
     $                INFO )
         CALL CHKXER( 'DSYEVR', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DSYEVR( 'V', 'A', 'U', -1, A, 1, 0.0D0, 0.0D0, 1, 1,
     $                0.0D0, M, R, Z, 1, IW, Q, 26*N, IW( 2*N+1 ), 10*N,
     $                INFO )
         CALL CHKXER( 'DSYEVR', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DSYEVR( 'V', 'A', 'U', 2, A, 1, 0.0D0, 0.0D0, 1, 1, 0.0D0,
     $                M, R, Z, 1, IW, Q, 26*N, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSYEVR', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSYEVR( 'V', 'V', 'U', 1, A, 1, 0.0D0, 0.0D0, 1, 1, 0.0D0,
     $                M, R, Z, 1, IW, Q, 26*N, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSYEVR', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DSYEVR( 'V', 'I', 'U', 1, A, 1, 0.0D0, 0.0D0, 0, 1, 0.0D0,
     $                M, R, Z, 1, IW, Q, 26*N, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSYEVR', INFOT, NOUT, LERR, OK )
         INFOT = 10
*
         CALL DSYEVR( 'V', 'I', 'U', 2, A, 2, 0.0D0, 0.0D0, 2, 1, 0.0D0,
     $                M, R, Z, 1, IW, Q, 26*N, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSYEVR', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL DSYEVR( 'V', 'I', 'U', 1, A, 1, 0.0D0, 0.0D0, 1, 1, 0.0D0,
     $                M, R, Z, 0, IW, Q, 26*N, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSYEVR', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL DSYEVR( 'V', 'I', 'U', 1, A, 1, 0.0D0, 0.0D0, 1, 1, 0.0D0,
     $                M, R, Z, 1, IW, Q, 26*N-1, IW( 2*N+1 ), 10*N,
     $                INFO )
         CALL CHKXER( 'DSYEVR', INFOT, NOUT, LERR, OK )
         INFOT = 20
         CALL DSYEVR( 'V', 'I', 'U', 1, A, 1, 0.0D0, 0.0D0, 1, 1, 0.0D0,
     $                M, R, Z, 1, IW, Q, 26*N, IW( 2*N+1 ), 10*N-1,
     $                INFO )
         CALL CHKXER( 'DSYEVR', INFOT, NOUT, LERR, OK )
         NT = NT + 11
*
*        DSYEVR_2STAGE
*
         SRNAMT = 'DSYEVR_2STAGE'
         N = 1
         INFOT = 1
         CALL DSYEVR_2STAGE( '/', 'A', 'U', 0, A, 1,
     $                0.0D0, 0.0D0, 1, 1, 0.0D0,
     $                M, R, Z, 1, IW, Q, 26*N, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSYEVR_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 1
         CALL DSYEVR_2STAGE( 'V', 'A', 'U', 0, A, 1,
     $                0.0D0, 0.0D0, 1, 1, 0.0D0,
     $                M, R, Z, 1, IW, Q, 26*N, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSYEVR_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSYEVR_2STAGE( 'N', '/', 'U', 0, A, 1,
     $                0.0D0, 0.0D0, 1, 1, 0.0D0,
     $                M, R, Z, 1, IW, Q, 26*N, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSYEVR_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSYEVR_2STAGE( 'N', 'A', '/', -1, A, 1,
     $                0.0D0, 0.0D0, 1, 1, 0.0D0,
     $                M, R, Z, 1, IW, Q, 26*N, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSYEVR_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DSYEVR_2STAGE( 'N', 'A', 'U', -1, A, 1,
     $                0.0D0, 0.0D0, 1, 1, 0.0D0,
     $                M, R, Z, 1, IW, Q, 26*N, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSYEVR_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DSYEVR_2STAGE( 'N', 'A', 'U', 2, A, 1,
     $                0.0D0, 0.0D0, 1, 1, 0.0D0,
     $                M, R, Z, 1, IW, Q, 26*N, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSYEVR_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSYEVR_2STAGE( 'N', 'V', 'U', 1, A, 1,
     $                0.0D0, 0.0D0, 1, 1, 0.0D0,
     $                M, R, Z, 1, IW, Q, 26*N, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSYEVR_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DSYEVR_2STAGE( 'N', 'I', 'U', 1, A, 1,
     $                0.0D0, 0.0D0, 0, 1, 0.0D0,
     $                M, R, Z, 1, IW, Q, 26*N, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSYEVR_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DSYEVR_2STAGE( 'N', 'I', 'U', 2, A, 2,
     $                0.0D0, 0.0D0, 2, 1, 0.0D0,
     $                M, R, Z, 1, IW, Q, 26*N, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSYEVR_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL DSYEVR_2STAGE( 'N', 'I', 'U', 1, A, 1,
     $                0.0D0, 0.0D0, 1, 1, 0.0D0,
     $                M, R, Z, 0, IW, Q, 26*N, IW( 2*N+1 ), 10*N, INFO )
         CALL CHKXER( 'DSYEVR_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL DSYEVR_2STAGE( 'N', 'I', 'U', 1, A, 1,
     $                0.0D0, 0.0D0, 1, 1, 0.0D0,
     $                M, R, Z, 1, IW, Q, 0, IW( 2*N+1 ), 10*N,
     $                INFO )
         CALL CHKXER( 'DSYEVR_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 20
         CALL DSYEVR_2STAGE( 'N', 'I', 'U', 1, A, 1,
     $                0.0D0, 0.0D0, 1, 1, 0.0D0,
     $                M, R, Z, 1, IW, Q, 26*N, IW( 2*N+1 ), 0,
     $                INFO )
         CALL CHKXER( 'DSYEVR_2STAGE', INFOT, NOUT, LERR, OK )
         NT = NT + 12
*
*        DSYEV
*
         SRNAMT = 'DSYEV '
         INFOT = 1
         CALL DSYEV( '/', 'U', 0, A, 1, X, W, 1, INFO )
         CALL CHKXER( 'DSYEV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSYEV( 'N', '/', 0, A, 1, X, W, 1, INFO )
         CALL CHKXER( 'DSYEV ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSYEV( 'N', 'U', -1, A, 1, X, W, 1, INFO )
         CALL CHKXER( 'DSYEV ', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DSYEV( 'N', 'U', 2, A, 1, X, W, 3, INFO )
         CALL CHKXER( 'DSYEV ', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSYEV( 'N', 'U', 1, A, 1, X, W, 1, INFO )
         CALL CHKXER( 'DSYEV ', INFOT, NOUT, LERR, OK )
         NT = NT + 5
*
*        DSYEV_2STAGE
*
         SRNAMT = 'DSYEV_2STAGE '
         INFOT = 1
         CALL DSYEV_2STAGE( '/', 'U', 0, A, 1, X, W, 1, INFO )
         CALL CHKXER( 'DSYEV_2STAGE ', INFOT, NOUT, LERR, OK )
         INFOT = 1
         CALL DSYEV_2STAGE( 'V', 'U', 0, A, 1, X, W, 1, INFO )
         CALL CHKXER( 'DSYEV_2STAGE ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSYEV_2STAGE( 'N', '/', 0, A, 1, X, W, 1, INFO )
         CALL CHKXER( 'DSYEV_2STAGE ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSYEV_2STAGE( 'N', 'U', -1, A, 1, X, W, 1, INFO )
         CALL CHKXER( 'DSYEV_2STAGE ', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DSYEV_2STAGE( 'N', 'U', 2, A, 1, X, W, 3, INFO )
         CALL CHKXER( 'DSYEV_2STAGE ', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSYEV_2STAGE( 'N', 'U', 1, A, 1, X, W, 1, INFO )
         CALL CHKXER( 'DSYEV_2STAGE ', INFOT, NOUT, LERR, OK )
         NT = NT + 6
*
*        DSYEVX
*
         SRNAMT = 'DSYEVX'
         INFOT = 1
         CALL DSYEVX( '/', 'A', 'U', 0, A, 1, 0.0D0, 0.0D0, 0, 0, 0.0D0,
     $                M, X, Z, 1, W, 1, IW, I3, INFO )
         CALL CHKXER( 'DSYEVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSYEVX( 'N', '/', 'U', 0, A, 1, 0.0D0, 1.0D0, 1, 0, 0.0D0,
     $                M, X, Z, 1, W, 1, IW, I3, INFO )
         CALL CHKXER( 'DSYEVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSYEVX( 'N', 'A', '/', 0, A, 1, 0.0D0, 0.0D0, 0, 0, 0.0D0,
     $                M, X, Z, 1, W, 1, IW, I3, INFO )
         INFOT = 4
         CALL DSYEVX( 'N', 'A', 'U', -1, A, 1, 0.0D0, 0.0D0, 0, 0,
     $                0.0D0, M, X, Z, 1, W, 1, IW, I3, INFO )
         CALL CHKXER( 'DSYEVX', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DSYEVX( 'N', 'A', 'U', 2, A, 1, 0.0D0, 0.0D0, 0, 0, 0.0D0,
     $                M, X, Z, 1, W, 16, IW, I3, INFO )
         CALL CHKXER( 'DSYEVX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSYEVX( 'N', 'V', 'U', 1, A, 1, 0.0D0, 0.0D0, 0, 0, 0.0D0,
     $                M, X, Z, 1, W, 8, IW, I3, INFO )
         CALL CHKXER( 'DSYEVX', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DSYEVX( 'N', 'I', 'U', 1, A, 1, 0.0D0, 0.0D0, 0, 0, 0.0D0,
     $                M, X, Z, 1, W, 8, IW, I3, INFO )
         CALL CHKXER( 'DSYEVX', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DSYEVX( 'N', 'I', 'U', 1, A, 1, 0.0D0, 0.0D0, 2, 1, 0.0D0,
     $                M, X, Z, 1, W, 8, IW, I3, INFO )
         CALL CHKXER( 'DSYEVX', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DSYEVX( 'N', 'I', 'U', 2, A, 2, 0.0D0, 0.0D0, 2, 1, 0.0D0,
     $                M, X, Z, 1, W, 16, IW, I3, INFO )
         CALL CHKXER( 'DSYEVX', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DSYEVX( 'N', 'I', 'U', 1, A, 1, 0.0D0, 0.0D0, 1, 2, 0.0D0,
     $                M, X, Z, 1, W, 8, IW, I3, INFO )
         CALL CHKXER( 'DSYEVX', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL DSYEVX( 'V', 'A', 'U', 2, A, 2, 0.0D0, 0.0D0, 0, 0, 0.0D0,
     $                M, X, Z, 1, W, 16, IW, I3, INFO )
         CALL CHKXER( 'DSYEVX', INFOT, NOUT, LERR, OK )
         INFOT = 17
         CALL DSYEVX( 'V', 'A', 'U', 1, A, 1, 0.0D0, 0.0D0, 0, 0, 0.0D0,
     $                M, X, Z, 1, W, 0, IW, I3, INFO )
         CALL CHKXER( 'DSYEVX', INFOT, NOUT, LERR, OK )
         NT = NT + 12
*
*        DSYEVX_2STAGE
*
         SRNAMT = 'DSYEVX_2STAGE'
         INFOT = 1
         CALL DSYEVX_2STAGE( '/', 'A', 'U', 0, A, 1,
     $                 0.0D0, 0.0D0, 0, 0, 0.0D0,
     $                M, X, Z, 1, W, 1, IW, I3, INFO )
         CALL CHKXER( 'DSYEVX_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 1
         CALL DSYEVX_2STAGE( 'V', 'A', 'U', 0, A, 1,
     $                 0.0D0, 0.0D0, 0, 0, 0.0D0,
     $                M, X, Z, 1, W, 1, IW, I3, INFO )
         CALL CHKXER( 'DSYEVX_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSYEVX_2STAGE( 'N', '/', 'U', 0, A, 1,
     $                0.0D0, 1.0D0, 1, 0, 0.0D0,
     $                M, X, Z, 1, W, 1, IW, I3, INFO )
         CALL CHKXER( 'DSYEVX_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSYEVX_2STAGE( 'N', 'A', '/', 0, A, 1,
     $                0.0D0, 0.0D0, 0, 0, 0.0D0,
     $                M, X, Z, 1, W, 1, IW, I3, INFO )
         INFOT = 4
         CALL DSYEVX_2STAGE( 'N', 'A', 'U', -1, A, 1,
     $                0.0D0, 0.0D0, 0, 0, 0.0D0,
     $                M, X, Z, 1, W, 1, IW, I3, INFO )
         CALL CHKXER( 'DSYEVX_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DSYEVX_2STAGE( 'N', 'A', 'U', 2, A, 1,
     $                0.0D0, 0.0D0, 0, 0, 0.0D0,
     $                M, X, Z, 1, W, 16, IW, I3, INFO )
         CALL CHKXER( 'DSYEVX_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSYEVX_2STAGE( 'N', 'V', 'U', 1, A, 1,
     $                0.0D0, 0.0D0, 0, 0, 0.0D0,
     $                M, X, Z, 1, W, 8, IW, I3, INFO )
         CALL CHKXER( 'DSYEVX_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DSYEVX_2STAGE( 'N', 'I', 'U', 1, A, 1,
     $                0.0D0, 0.0D0, 0, 0, 0.0D0,
     $                M, X, Z, 1, W, 8, IW, I3, INFO )
         CALL CHKXER( 'DSYEVX_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DSYEVX_2STAGE( 'N', 'I', 'U', 1, A, 1,
     $                0.0D0, 0.0D0, 2, 1, 0.0D0,
     $                M, X, Z, 1, W, 8, IW, I3, INFO )
         CALL CHKXER( 'DSYEVX_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DSYEVX_2STAGE( 'N', 'I', 'U', 2, A, 2,
     $                0.0D0, 0.0D0, 2, 1, 0.0D0,
     $                M, X, Z, 1, W, 16, IW, I3, INFO )
         CALL CHKXER( 'DSYEVX_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DSYEVX_2STAGE( 'N', 'I', 'U', 1, A, 1,
     $                0.0D0, 0.0D0, 1, 2, 0.0D0,
     $                M, X, Z, 1, W, 8, IW, I3, INFO )
         CALL CHKXER( 'DSYEVX_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL DSYEVX_2STAGE( 'N', 'A', 'U', 2, A, 2,
     $                0.0D0, 0.0D0, 0, 0, 0.0D0,
     $                M, X, Z, 0, W, 16, IW, I3, INFO )
         CALL CHKXER( 'DSYEVX_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 17
         CALL DSYEVX_2STAGE( 'N', 'A', 'U', 1, A, 1,
     $                0.0D0, 0.0D0, 0, 0, 0.0D0,
     $                M, X, Z, 1, W, 0, IW, I3, INFO )
         CALL CHKXER( 'DSYEVX_2STAGE', INFOT, NOUT, LERR, OK )
         NT = NT + 13
*
*        DSPEVD
*
         SRNAMT = 'DSPEVD'
         INFOT = 1
         CALL DSPEVD( '/', 'U', 0, A, X, Z, 1, W, 1, IW, 1, INFO )
         CALL CHKXER( 'DSPEVD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSPEVD( 'N', '/', 0, A, X, Z, 1, W, 1, IW, 1, INFO )
         CALL CHKXER( 'DSPEVD', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSPEVD( 'N', 'U', -1, A, X, Z, 1, W, 1, IW, 1, INFO )
         CALL CHKXER( 'DSPEVD', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DSPEVD( 'V', 'U', 2, A, X, Z, 1, W, 23, IW, 12, INFO )
         CALL CHKXER( 'DSPEVD', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DSPEVD( 'N', 'U', 1, A, X, Z, 1, W, 0, IW, 1, INFO )
         CALL CHKXER( 'DSPEVD', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DSPEVD( 'N', 'U', 2, A, X, Z, 1, W, 3, IW, 1, INFO )
         CALL CHKXER( 'DSPEVD', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DSPEVD( 'V', 'U', 2, A, X, Z, 2, W, 16, IW, 12, INFO )
         CALL CHKXER( 'DSPEVD', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DSPEVD( 'N', 'U', 1, A, X, Z, 1, W, 1, IW, 0, INFO )
         CALL CHKXER( 'DSPEVD', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DSPEVD( 'N', 'U', 2, A, X, Z, 1, W, 4, IW, 0, INFO )
         CALL CHKXER( 'DSPEVD', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DSPEVD( 'V', 'U', 2, A, X, Z, 2, W, 23, IW, 11, INFO )
         CALL CHKXER( 'DSPEVD', INFOT, NOUT, LERR, OK )
         NT = NT + 10
*
*        DSPEV
*
         SRNAMT = 'DSPEV '
         INFOT = 1
         CALL DSPEV( '/', 'U', 0, A, W, Z, 1, X, INFO )
         CALL CHKXER( 'DSPEV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSPEV( 'N', '/', 0, A, W, Z, 1, X, INFO )
         CALL CHKXER( 'DSPEV ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSPEV( 'N', 'U', -1, A, W, Z, 1, X, INFO )
         CALL CHKXER( 'DSPEV ', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DSPEV( 'V', 'U', 2, A, W, Z, 1, X, INFO )
         CALL CHKXER( 'DSPEV ', INFOT, NOUT, LERR, OK )
         NT = NT + 4
*
*        DSPEVX
*
         SRNAMT = 'DSPEVX'
         INFOT = 1
         CALL DSPEVX( '/', 'A', 'U', 0, A, 0.0D0, 0.0D0, 0, 0, 0.0D0, M,
     $                X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSPEVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSPEVX( 'N', '/', 'U', 0, A, 0.0D0, 0.0D0, 0, 0, 0.0D0, M,
     $                X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSPEVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSPEVX( 'N', 'A', '/', 0, A, 0.0D0, 0.0D0, 0, 0, 0.0D0, M,
     $                X, Z, 1, W, IW, I3, INFO )
         INFOT = 4
         CALL DSPEVX( 'N', 'A', 'U', -1, A, 0.0D0, 0.0D0, 0, 0, 0.0D0,
     $                M, X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSPEVX', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DSPEVX( 'N', 'V', 'U', 1, A, 0.0D0, 0.0D0, 0, 0, 0.0D0, M,
     $                X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSPEVX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSPEVX( 'N', 'I', 'U', 1, A, 0.0D0, 0.0D0, 0, 0, 0.0D0, M,
     $                X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSPEVX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DSPEVX( 'N', 'I', 'U', 1, A, 0.0D0, 0.0D0, 2, 1, 0.0D0, M,
     $                X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSPEVX', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DSPEVX( 'N', 'I', 'U', 2, A, 0.0D0, 0.0D0, 2, 1, 0.0D0, M,
     $                X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSPEVX', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DSPEVX( 'N', 'I', 'U', 1, A, 0.0D0, 0.0D0, 1, 2, 0.0D0, M,
     $                X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSPEVX', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL DSPEVX( 'V', 'A', 'U', 2, A, 0.0D0, 0.0D0, 0, 0, 0.0D0, M,
     $                X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSPEVX', INFOT, NOUT, LERR, OK )
         NT = NT + 10
*
*     Test error exits for the SB path.
*
      ELSE IF( LSAMEN( 2, C2, 'SB' ) ) THEN
*
*        DSBTRD
*
         SRNAMT = 'DSBTRD'
         INFOT = 1
         CALL DSBTRD( '/', 'U', 0, 0, A, 1, D, E, Z, 1, W, INFO )
         CALL CHKXER( 'DSBTRD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSBTRD( 'N', '/', 0, 0, A, 1, D, E, Z, 1, W, INFO )
         CALL CHKXER( 'DSBTRD', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSBTRD( 'N', 'U', -1, 0, A, 1, D, E, Z, 1, W, INFO )
         CALL CHKXER( 'DSBTRD', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DSBTRD( 'N', 'U', 0, -1, A, 1, D, E, Z, 1, W, INFO )
         CALL CHKXER( 'DSBTRD', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DSBTRD( 'N', 'U', 1, 1, A, 1, D, E, Z, 1, W, INFO )
         CALL CHKXER( 'DSBTRD', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DSBTRD( 'V', 'U', 2, 0, A, 1, D, E, Z, 1, W, INFO )
         CALL CHKXER( 'DSBTRD', INFOT, NOUT, LERR, OK )
         NT = NT + 6
*
*        DSYTRD_SB2ST
*
         SRNAMT = 'DSYTRD_SB2ST'
         INFOT = 1
         CALL DSYTRD_SB2ST( '/', 'N', 'U', 0, 0, A, 1, D, E, 
     $                                    C, 1, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_SB2ST', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSYTRD_SB2ST( 'N', '/', 'U', 0, 0, A, 1, D, E, 
     $                                    C, 1, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_SB2ST', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSYTRD_SB2ST( 'N', 'H', 'U', 0, 0, A, 1, D, E, 
     $                                    C, 1, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_SB2ST', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSYTRD_SB2ST( 'N', 'N', '/', 0, 0, A, 1, D, E, 
     $                                    C, 1, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_SB2ST', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DSYTRD_SB2ST( 'N', 'N', 'U', -1, 0, A, 1, D, E, 
     $                                    C, 1, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_SB2ST', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DSYTRD_SB2ST( 'N', 'N', 'U', 0, -1, A, 1, D, E, 
     $                                    C, 1, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_SB2ST', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DSYTRD_SB2ST( 'N', 'N', 'U', 0, 1, A, 1, D, E, 
     $                                    C, 1, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_SB2ST', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DSYTRD_SB2ST( 'N', 'N', 'U', 0, 0, A, 1, D, E, 
     $                                    C, 0, W, 1, INFO )
         CALL CHKXER( 'DSYTRD_SB2ST', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL DSYTRD_SB2ST( 'N', 'N', 'U', 0, 0, A, 1, D, E, 
     $                                    C, 1, W, 0, INFO )
         CALL CHKXER( 'DSYTRD_SB2ST', INFOT, NOUT, LERR, OK )
         NT = NT + 9
*
*        DSBEVD
*
         SRNAMT = 'DSBEVD'
         INFOT = 1
         CALL DSBEVD( '/', 'U', 0, 0, A, 1, X, Z, 1, W, 1, IW, 1, INFO )
         CALL CHKXER( 'DSBEVD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSBEVD( 'N', '/', 0, 0, A, 1, X, Z, 1, W, 1, IW, 1, INFO )
         CALL CHKXER( 'DSBEVD', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSBEVD( 'N', 'U', -1, 0, A, 1, X, Z, 1, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'DSBEVD', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DSBEVD( 'N', 'U', 0, -1, A, 1, X, Z, 1, W, 1, IW, 1,
     $                INFO )
         CALL CHKXER( 'DSBEVD', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DSBEVD( 'N', 'U', 2, 1, A, 1, X, Z, 1, W, 4, IW, 1, INFO )
         CALL CHKXER( 'DSBEVD', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DSBEVD( 'V', 'U', 2, 1, A, 2, X, Z, 1, W, 25, IW, 12,
     $                INFO )
         CALL CHKXER( 'DSBEVD', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DSBEVD( 'N', 'U', 1, 0, A, 1, X, Z, 1, W, 0, IW, 1, INFO )
         CALL CHKXER( 'DSBEVD', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DSBEVD( 'N', 'U', 2, 0, A, 1, X, Z, 1, W, 3, IW, 1, INFO )
         CALL CHKXER( 'DSBEVD', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DSBEVD( 'V', 'U', 2, 0, A, 1, X, Z, 2, W, 18, IW, 12,
     $                INFO )
         CALL CHKXER( 'DSBEVD', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL DSBEVD( 'N', 'U', 1, 0, A, 1, X, Z, 1, W, 1, IW, 0, INFO )
         CALL CHKXER( 'DSBEVD', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL DSBEVD( 'V', 'U', 2, 0, A, 1, X, Z, 2, W, 25, IW, 11,
     $                INFO )
         CALL CHKXER( 'DSBEVD', INFOT, NOUT, LERR, OK )
         NT = NT + 11
*
*        DSBEVD_2STAGE
*
         SRNAMT = 'DSBEVD_2STAGE'
         INFOT = 1
         CALL DSBEVD_2STAGE( '/', 'U', 0, 0, A, 1, X, Z, 1, W,
     $                                        1, IW, 1, INFO )
         CALL CHKXER( 'DSBEVD_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 1
         CALL DSBEVD_2STAGE( 'V', 'U', 0, 0, A, 1, X, Z, 1, W,
     $                                        1, IW, 1, INFO )
         CALL CHKXER( 'DSBEVD_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSBEVD_2STAGE( 'N', '/', 0, 0, A, 1, X, Z, 1, W,
     $                                        1, IW, 1, INFO )
         CALL CHKXER( 'DSBEVD_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSBEVD_2STAGE( 'N', 'U', -1, 0, A, 1, X, Z, 1, W,
     $                                         1, IW, 1, INFO )
         CALL CHKXER( 'DSBEVD_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DSBEVD_2STAGE( 'N', 'U', 0, -1, A, 1, X, Z, 1, W,
     $                                         1, IW, 1, INFO )
         CALL CHKXER( 'DSBEVD_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DSBEVD_2STAGE( 'N', 'U', 2, 1, A, 1, X, Z, 1, W,
     $                                        4, IW, 1, INFO )
         CALL CHKXER( 'DSBEVD_2STAGE', INFOT, NOUT, LERR, OK )
*         INFOT = 9
*         CALL DSBEVD_2STAGE( 'V', 'U', 2, 1, A, 2, X, Z, 1, W,
*     $                                      25, IW, 12, INFO )
*         CALL CHKXER( 'DSBEVD_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DSBEVD_2STAGE( 'N', 'U', 1, 0, A, 1, X, Z, 1, W,
     $                                        0, IW, 1, INFO )
         CALL CHKXER( 'DSBEVD_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DSBEVD_2STAGE( 'N', 'U', 2, 0, A, 1, X, Z, 1, W,
     $                                        3, IW, 1, INFO )
         CALL CHKXER( 'DSBEVD_2STAGE', INFOT, NOUT, LERR, OK )
*         INFOT = 11
*         CALL DSBEVD_2STAGE( 'V', 'U', 2, 0, A, 1, X, Z, 2, W,
*     $                                      18, IW, 12, INFO )
*         CALL CHKXER( 'DSBEVD_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL DSBEVD_2STAGE( 'N', 'U', 1, 0, A, 1, X, Z, 1, W,
     $                                        1, IW, 0, INFO )
         CALL CHKXER( 'DSBEVD_2STAGE', INFOT, NOUT, LERR, OK )
*         INFOT = 13
*         CALL DSBEVD_2STAGE( 'V', 'U', 2, 0, A, 1, X, Z, 2, W,
*     $                                      25, IW, 11, INFO )
*         CALL CHKXER( 'DSBEVD_2STAGE', INFOT, NOUT, LERR, OK )
*         NT = NT + 12
         NT = NT + 9
*
*        DSBEV
*
         SRNAMT = 'DSBEV '
         INFOT = 1
         CALL DSBEV( '/', 'U', 0, 0, A, 1, X, Z, 1, W, INFO )
         CALL CHKXER( 'DSBEV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSBEV( 'N', '/', 0, 0, A, 1, X, Z, 1, W, INFO )
         CALL CHKXER( 'DSBEV ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSBEV( 'N', 'U', -1, 0, A, 1, X, Z, 1, W, INFO )
         CALL CHKXER( 'DSBEV ', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DSBEV( 'N', 'U', 0, -1, A, 1, X, Z, 1, W, INFO )
         CALL CHKXER( 'DSBEV ', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DSBEV( 'N', 'U', 2, 1, A, 1, X, Z, 1, W, INFO )
         CALL CHKXER( 'DSBEV ', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DSBEV( 'V', 'U', 2, 0, A, 1, X, Z, 1, W, INFO )
         CALL CHKXER( 'DSBEV ', INFOT, NOUT, LERR, OK )
         NT = NT + 6
*
*        DSBEV_2STAGE
*
         SRNAMT = 'DSBEV_2STAGE '
         INFOT = 1
         CALL DSBEV_2STAGE( '/', 'U', 0, 0, A, 1, X, Z, 1, W, 0, INFO )
         CALL CHKXER( 'DSBEV_2STAGE ', INFOT, NOUT, LERR, OK )
         INFOT = 1
         CALL DSBEV_2STAGE( 'V', 'U', 0, 0, A, 1, X, Z, 1, W, 0, INFO )
         CALL CHKXER( 'DSBEV_2STAGE ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSBEV_2STAGE( 'N', '/', 0, 0, A, 1, X, Z, 1, W, 0, INFO )
         CALL CHKXER( 'DSBEV_2STAGE ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSBEV_2STAGE( 'N', 'U', -1, 0, A, 1, X, Z, 1, W, 0, INFO )
         CALL CHKXER( 'DSBEV_2STAGE ', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DSBEV_2STAGE( 'N', 'U', 0, -1, A, 1, X, Z, 1, W, 0, INFO )
         CALL CHKXER( 'DSBEV_2STAGE ', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DSBEV_2STAGE( 'N', 'U', 2, 1, A, 1, X, Z, 1, W, 0, INFO )
         CALL CHKXER( 'DSBEV_2STAGE ', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DSBEV_2STAGE( 'N', 'U', 2, 0, A, 1, X, Z, 0, W, 0, INFO )
         CALL CHKXER( 'DSBEV_2STAGE ', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DSBEV_2STAGE( 'N', 'U', 0, 0, A, 1, X, Z, 1, W, 0, INFO )
         CALL CHKXER( 'DSBEV_2STAGE ', INFOT, NOUT, LERR, OK )
         NT = NT + 8
*
*        DSBEVX
*
         SRNAMT = 'DSBEVX'
         INFOT = 1
         CALL DSBEVX( '/', 'A', 'U', 0, 0, A, 1, Q, 1, 0.0D0, 0.0D0, 0,
     $                0, 0.0D0, M, X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSBEVX( 'N', '/', 'U', 0, 0, A, 1, Q, 1, 0.0D0, 0.0D0, 0,
     $                0, 0.0D0, M, X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSBEVX( 'N', 'A', '/', 0, 0, A, 1, Q, 1, 0.0D0, 0.0D0, 0,
     $                0, 0.0D0, M, X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DSBEVX( 'N', 'A', 'U', -1, 0, A, 1, Q, 1, 0.0D0, 0.0D0, 0,
     $                0, 0.0D0, M, X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DSBEVX( 'N', 'A', 'U', 0, -1, A, 1, Q, 1, 0.0D0, 0.0D0, 0,
     $                0, 0.0D0, M, X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DSBEVX( 'N', 'A', 'U', 2, 1, A, 1, Q, 1, 0.0D0, 0.0D0, 0,
     $                0, 0.0D0, M, X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DSBEVX( 'V', 'A', 'U', 2, 0, A, 1, Q, 1, 0.0D0, 0.0D0, 0,
     $                0, 0.0D0, M, X, Z, 2, W, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DSBEVX( 'N', 'V', 'U', 1, 0, A, 1, Q, 1, 0.0D0, 0.0D0, 0,
     $                0, 0.0D0, M, X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DSBEVX( 'N', 'I', 'U', 1, 0, A, 1, Q, 1, 0.0D0, 0.0D0, 0,
     $                0, 0.0D0, M, X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DSBEVX( 'N', 'I', 'U', 1, 0, A, 1, Q, 1, 0.0D0, 0.0D0, 2,
     $                1, 0.0D0, M, X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL DSBEVX( 'N', 'I', 'U', 2, 0, A, 1, Q, 1, 0.0D0, 0.0D0, 2,
     $                1, 0.0D0, M, X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL DSBEVX( 'N', 'I', 'U', 1, 0, A, 1, Q, 1, 0.0D0, 0.0D0, 1,
     $                2, 0.0D0, M, X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX', INFOT, NOUT, LERR, OK )
         INFOT = 18
         CALL DSBEVX( 'V', 'A', 'U', 2, 0, A, 1, Q, 2, 0.0D0, 0.0D0, 0,
     $                0, 0.0D0, M, X, Z, 1, W, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX', INFOT, NOUT, LERR, OK )
         NT = NT + 13
*
*        DSBEVX_2STAGE
*
         SRNAMT = 'DSBEVX_2STAGE'
         INFOT = 1
         CALL DSBEVX_2STAGE( '/', 'A', 'U', 0, 0, A, 1, Q, 1, 0.0D0,
     $          0.0D0, 0, 0, 0.0D0, M, X, Z, 1, W, 0, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 1
         CALL DSBEVX_2STAGE( 'V', 'A', 'U', 0, 0, A, 1, Q, 1, 0.0D0,
     $          0.0D0, 0, 0, 0.0D0, M, X, Z, 1, W, 0, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DSBEVX_2STAGE( 'N', '/', 'U', 0, 0, A, 1, Q, 1, 0.0D0,
     $          0.0D0, 0, 0, 0.0D0, M, X, Z, 1, W, 0, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DSBEVX_2STAGE( 'N', 'A', '/', 0, 0, A, 1, Q, 1, 0.0D0,
     $          0.0D0, 0, 0, 0.0D0, M, X, Z, 1, W, 0, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DSBEVX_2STAGE( 'N', 'A', 'U', -1, 0, A, 1, Q, 1, 0.0D0,
     $           0.0D0, 0, 0, 0.0D0, M, X, Z, 1, W, 0, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DSBEVX_2STAGE( 'N', 'A', 'U', 0, -1, A, 1, Q, 1, 0.0D0,
     $           0.0D0, 0, 0, 0.0D0, M, X, Z, 1, W, 0, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DSBEVX_2STAGE( 'N', 'A', 'U', 2, 1, A, 1, Q, 1, 0.0D0,
     $          0.0D0, 0, 0, 0.0D0, M, X, Z, 1, W, 0, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX_2STAGE', INFOT, NOUT, LERR, OK )
*         INFOT = 9
*         CALL DSBEVX_2STAGE( 'V', 'A', 'U', 2, 0, A, 1, Q, 1, 0.0D0,
*     $          0.0D0, 0, 0, 0.0D0, M, X, Z, 2, W, 0, IW, I3, INFO )
*         CALL CHKXER( 'DSBEVX_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DSBEVX_2STAGE( 'N', 'V', 'U', 1, 0, A, 1, Q, 1, 0.0D0,
     $          0.0D0, 0, 0, 0.0D0, M, X, Z, 1, W, 0, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DSBEVX_2STAGE( 'N', 'I', 'U', 1, 0, A, 1, Q, 1, 0.0D0,
     $          0.0D0, 0, 0, 0.0D0, M, X, Z, 1, W, 0, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DSBEVX_2STAGE( 'N', 'I', 'U', 1, 0, A, 1, Q, 1, 0.0D0,
     $          0.0D0, 2, 1, 0.0D0, M, X, Z, 1, W, 0, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL DSBEVX_2STAGE( 'N', 'I', 'U', 2, 0, A, 1, Q, 1, 0.0D0,
     $          0.0D0, 2, 1, 0.0D0, M, X, Z, 1, W, 0, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL DSBEVX_2STAGE( 'N', 'I', 'U', 1, 0, A, 1, Q, 1, 0.0D0,
     $          0.0D0, 1, 2, 0.0D0, M, X, Z, 1, W, 0, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX_2STAGE', INFOT, NOUT, LERR, OK )
*         INFOT = 18
*         CALL DSBEVX_2STAGE( 'V', 'A', 'U', 2, 0, A, 1, Q, 2, 0.0D0,
*     $          0.0D0, 0, 0, 0.0D0, M, X, Z, 1, W, 0, IW, I3, INFO )
*         CALL CHKXER( 'DSBEVX_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 20
         CALL DSBEVX_2STAGE( 'N', 'A', 'U', 0, 0, A, 1, Q, 1, 0.0D0,
     $           0.0D0, 0, 0, 0.0D0, M, X, Z, 1, W, 0, IW, I3, INFO )
         CALL CHKXER( 'DSBEVX_2STAGE', INFOT, NOUT, LERR, OK )
*         NT = NT + 15
         NT = NT + 13
      END IF
*
*     Print a summary line.
*
      IF( OK ) THEN
         WRITE( NOUT, FMT = 9999 )PATH, NT
      ELSE
         WRITE( NOUT, FMT = 9998 )PATH
      END IF
*
 9999 FORMAT( 1X, A3, ' routines passed the tests of the error exits',
     $      ' (', I3, ' tests done)' )
 9998 FORMAT( ' *** ', A3, ' routines failed the tests of the error ',
     $      'exits ***' )
*
      RETURN
*
*     End of DERRST
*
      END
