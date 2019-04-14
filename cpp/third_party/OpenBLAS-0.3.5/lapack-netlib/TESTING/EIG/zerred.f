*> \brief \b ZERRED
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZERRED( PATH, NUNIT )
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
*> ZERRED tests the error exits for the eigenvalue driver routines for
*> DOUBLE COMPLEX PRECISION matrices:
*>
*> PATH  driver   description
*> ----  ------   -----------
*> ZEV   ZGEEV    find eigenvalues/eigenvectors for nonsymmetric A
*> ZES   ZGEES    find eigenvalues/Schur form for nonsymmetric A
*> ZVX   ZGEEVX   ZGEEV + balancing and condition estimation
*> ZSX   ZGEESX   ZGEES + balancing and condition estimation
*> ZBD   ZGESVD   compute SVD of an M-by-N matrix A
*>       ZGESDD   compute SVD of an M-by-N matrix A(by divide and
*>                conquer)
*>       ZGEJSV   compute SVD of an M-by-N matrix A where M >= N
*>       ZGESVDX  compute SVD of an M-by-N matrix A(by bisection
*>                and inverse iteration)
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
*> \date June 2016
*
*> \ingroup complex16_eig
*
*  =====================================================================
      SUBROUTINE ZERRED( PATH, NUNIT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      CHARACTER*3        PATH
      INTEGER            NUNIT
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            NMAX, LW
      PARAMETER          ( NMAX = 4, LW = 5*NMAX )
      DOUBLE PRECISION   ONE, ZERO
      PARAMETER          ( ONE = 1.0D0, ZERO = 0.0D0 )
*     ..
*     .. Local Scalars ..
      CHARACTER*2        C2
      INTEGER            I, IHI, ILO, INFO, J, NS, NT, SDIM
      DOUBLE PRECISION   ABNRM
*     ..
*     .. Local Arrays ..
      LOGICAL            B( NMAX )
      INTEGER            IW( 4*NMAX )
      DOUBLE PRECISION   R1( NMAX ), R2( NMAX ), RW( LW ), S( NMAX )
      COMPLEX*16         A( NMAX, NMAX ), U( NMAX, NMAX ),
     $                   VL( NMAX, NMAX ), VR( NMAX, NMAX ),
     $                   VT( NMAX, NMAX ), W( 10*NMAX ), X( NMAX )
*     ..
*     .. External Subroutines ..
      EXTERNAL           CHKXER, ZGEES, ZGEESX, ZGEEV, ZGEEVX, ZGESVJ,
     $                   ZGESDD, ZGESVD
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN, ZSLECT
      EXTERNAL           LSAMEN, ZSLECT
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          LEN_TRIM
*     ..
*     .. Arrays in Common ..
      LOGICAL            SELVAL( 20 )
      DOUBLE PRECISION   SELWI( 20 ), SELWR( 20 )
*     ..
*     .. Scalars in Common ..
      LOGICAL            LERR, OK
      CHARACTER*32       SRNAMT
      INTEGER            INFOT, NOUT, SELDIM, SELOPT
*     ..
*     .. Common blocks ..
      COMMON             / INFOC / INFOT, NOUT, OK, LERR
      COMMON             / SRNAMC / SRNAMT
      COMMON             / SSLCT / SELOPT, SELDIM, SELVAL, SELWR, SELWI
*     ..
*     .. Executable Statements ..
*
      NOUT = NUNIT
      WRITE( NOUT, FMT = * )
      C2 = PATH( 2: 3 )
*
*     Initialize A
*
      DO 20 J = 1, NMAX
         DO 10 I = 1, NMAX
            A( I, J ) = ZERO
   10    CONTINUE
   20 CONTINUE
      DO 30 I = 1, NMAX
         A( I, I ) = ONE
   30 CONTINUE
      OK = .TRUE.
      NT = 0
*
      IF( LSAMEN( 2, C2, 'EV' ) ) THEN
*
*        Test ZGEEV
*
         SRNAMT = 'ZGEEV '
         INFOT = 1
         CALL ZGEEV( 'X', 'N', 0, A, 1, X, VL, 1, VR, 1, W, 1, RW,
     $               INFO )
         CALL CHKXER( 'ZGEEV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGEEV( 'N', 'X', 0, A, 1, X, VL, 1, VR, 1, W, 1, RW,
     $               INFO )
         CALL CHKXER( 'ZGEEV ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGEEV( 'N', 'N', -1, A, 1, X, VL, 1, VR, 1, W, 1, RW,
     $               INFO )
         CALL CHKXER( 'ZGEEV ', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGEEV( 'N', 'N', 2, A, 1, X, VL, 1, VR, 1, W, 4, RW,
     $               INFO )
         CALL CHKXER( 'ZGEEV ', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZGEEV( 'V', 'N', 2, A, 2, X, VL, 1, VR, 1, W, 4, RW,
     $               INFO )
         CALL CHKXER( 'ZGEEV ', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL ZGEEV( 'N', 'V', 2, A, 2, X, VL, 1, VR, 1, W, 4, RW,
     $               INFO )
         CALL CHKXER( 'ZGEEV ', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL ZGEEV( 'V', 'V', 1, A, 1, X, VL, 1, VR, 1, W, 1, RW,
     $               INFO )
         CALL CHKXER( 'ZGEEV ', INFOT, NOUT, LERR, OK )
         NT = NT + 7
*
      ELSE IF( LSAMEN( 2, C2, 'ES' ) ) THEN
*
*        Test ZGEES
*
         SRNAMT = 'ZGEES '
         INFOT = 1
         CALL ZGEES( 'X', 'N', ZSLECT, 0, A, 1, SDIM, X, VL, 1, W, 1,
     $               RW, B, INFO )
         CALL CHKXER( 'ZGEES ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGEES( 'N', 'X', ZSLECT, 0, A, 1, SDIM, X, VL, 1, W, 1,
     $               RW, B, INFO )
         CALL CHKXER( 'ZGEES ', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZGEES( 'N', 'S', ZSLECT, -1, A, 1, SDIM, X, VL, 1, W, 1,
     $               RW, B, INFO )
         CALL CHKXER( 'ZGEES ', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZGEES( 'N', 'S', ZSLECT, 2, A, 1, SDIM, X, VL, 1, W, 4,
     $               RW, B, INFO )
         CALL CHKXER( 'ZGEES ', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL ZGEES( 'V', 'S', ZSLECT, 2, A, 2, SDIM, X, VL, 1, W, 4,
     $               RW, B, INFO )
         CALL CHKXER( 'ZGEES ', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL ZGEES( 'N', 'S', ZSLECT, 1, A, 1, SDIM, X, VL, 1, W, 1,
     $               RW, B, INFO )
         CALL CHKXER( 'ZGEES ', INFOT, NOUT, LERR, OK )
         NT = NT + 6
*
      ELSE IF( LSAMEN( 2, C2, 'VX' ) ) THEN
*
*        Test ZGEEVX
*
         SRNAMT = 'ZGEEVX'
         INFOT = 1
         CALL ZGEEVX( 'X', 'N', 'N', 'N', 0, A, 1, X, VL, 1, VR, 1, ILO,
     $                IHI, S, ABNRM, R1, R2, W, 1, RW, INFO )
         CALL CHKXER( 'ZGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGEEVX( 'N', 'X', 'N', 'N', 0, A, 1, X, VL, 1, VR, 1, ILO,
     $                IHI, S, ABNRM, R1, R2, W, 1, RW, INFO )
         CALL CHKXER( 'ZGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGEEVX( 'N', 'N', 'X', 'N', 0, A, 1, X, VL, 1, VR, 1, ILO,
     $                IHI, S, ABNRM, R1, R2, W, 1, RW, INFO )
         CALL CHKXER( 'ZGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZGEEVX( 'N', 'N', 'N', 'X', 0, A, 1, X, VL, 1, VR, 1, ILO,
     $                IHI, S, ABNRM, R1, R2, W, 1, RW, INFO )
         CALL CHKXER( 'ZGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGEEVX( 'N', 'N', 'N', 'N', -1, A, 1, X, VL, 1, VR, 1,
     $                ILO, IHI, S, ABNRM, R1, R2, W, 1, RW, INFO )
         CALL CHKXER( 'ZGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZGEEVX( 'N', 'N', 'N', 'N', 2, A, 1, X, VL, 1, VR, 1, ILO,
     $                IHI, S, ABNRM, R1, R2, W, 4, RW, INFO )
         CALL CHKXER( 'ZGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL ZGEEVX( 'N', 'V', 'N', 'N', 2, A, 2, X, VL, 1, VR, 1, ILO,
     $                IHI, S, ABNRM, R1, R2, W, 4, RW, INFO )
         CALL CHKXER( 'ZGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL ZGEEVX( 'N', 'N', 'V', 'N', 2, A, 2, X, VL, 1, VR, 1, ILO,
     $                IHI, S, ABNRM, R1, R2, W, 4, RW, INFO )
         CALL CHKXER( 'ZGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 20
         CALL ZGEEVX( 'N', 'N', 'N', 'N', 1, A, 1, X, VL, 1, VR, 1, ILO,
     $                IHI, S, ABNRM, R1, R2, W, 1, RW, INFO )
         CALL CHKXER( 'ZGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 20
         CALL ZGEEVX( 'N', 'N', 'V', 'V', 1, A, 1, X, VL, 1, VR, 1, ILO,
     $                IHI, S, ABNRM, R1, R2, W, 2, RW, INFO )
         CALL CHKXER( 'ZGEEVX', INFOT, NOUT, LERR, OK )
         NT = NT + 10
*
      ELSE IF( LSAMEN( 2, C2, 'SX' ) ) THEN
*
*        Test ZGEESX
*
         SRNAMT = 'ZGEESX'
         INFOT = 1
         CALL ZGEESX( 'X', 'N', ZSLECT, 'N', 0, A, 1, SDIM, X, VL, 1,
     $                R1( 1 ), R2( 1 ), W, 1, RW, B, INFO )
         CALL CHKXER( 'ZGEESX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGEESX( 'N', 'X', ZSLECT, 'N', 0, A, 1, SDIM, X, VL, 1,
     $                R1( 1 ), R2( 1 ), W, 1, RW, B, INFO )
         CALL CHKXER( 'ZGEESX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZGEESX( 'N', 'N', ZSLECT, 'X', 0, A, 1, SDIM, X, VL, 1,
     $                R1( 1 ), R2( 1 ), W, 1, RW, B, INFO )
         CALL CHKXER( 'ZGEESX', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGEESX( 'N', 'N', ZSLECT, 'N', -1, A, 1, SDIM, X, VL, 1,
     $                R1( 1 ), R2( 1 ), W, 1, RW, B, INFO )
         CALL CHKXER( 'ZGEESX', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZGEESX( 'N', 'N', ZSLECT, 'N', 2, A, 1, SDIM, X, VL, 1,
     $                R1( 1 ), R2( 1 ), W, 4, RW, B, INFO )
         CALL CHKXER( 'ZGEESX', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL ZGEESX( 'V', 'N', ZSLECT, 'N', 2, A, 2, SDIM, X, VL, 1,
     $                R1( 1 ), R2( 1 ), W, 4, RW, B, INFO )
         CALL CHKXER( 'ZGEESX', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL ZGEESX( 'N', 'N', ZSLECT, 'N', 1, A, 1, SDIM, X, VL, 1,
     $                R1( 1 ), R2( 1 ), W, 1, RW, B, INFO )
         CALL CHKXER( 'ZGEESX', INFOT, NOUT, LERR, OK )
         NT = NT + 7
*
      ELSE IF( LSAMEN( 2, C2, 'BD' ) ) THEN
*
*        Test ZGESVD
*
         SRNAMT = 'ZGESVD'
         INFOT = 1
         CALL ZGESVD( 'X', 'N', 0, 0, A, 1, S, U, 1, VT, 1, W, 1, RW,
     $                INFO )
         CALL CHKXER( 'ZGESVD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGESVD( 'N', 'X', 0, 0, A, 1, S, U, 1, VT, 1, W, 1, RW,
     $                INFO )
         CALL CHKXER( 'ZGESVD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGESVD( 'O', 'O', 0, 0, A, 1, S, U, 1, VT, 1, W, 1, RW,
     $                INFO )
         CALL CHKXER( 'ZGESVD', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGESVD( 'N', 'N', -1, 0, A, 1, S, U, 1, VT, 1, W, 1, RW,
     $                INFO )
         CALL CHKXER( 'ZGESVD', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZGESVD( 'N', 'N', 0, -1, A, 1, S, U, 1, VT, 1, W, 1, RW,
     $                INFO )
         CALL CHKXER( 'ZGESVD', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZGESVD( 'N', 'N', 2, 1, A, 1, S, U, 1, VT, 1, W, 5, RW,
     $                INFO )
         CALL CHKXER( 'ZGESVD', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL ZGESVD( 'A', 'N', 2, 1, A, 2, S, U, 1, VT, 1, W, 5, RW,
     $                INFO )
         CALL CHKXER( 'ZGESVD', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL ZGESVD( 'N', 'A', 1, 2, A, 1, S, U, 1, VT, 1, W, 5, RW,
     $                INFO )
         CALL CHKXER( 'ZGESVD', INFOT, NOUT, LERR, OK )
         NT = NT + 8
         IF( OK ) THEN
            WRITE( NOUT, FMT = 9999 )SRNAMT( 1:LEN_TRIM( SRNAMT ) ),
     $           NT
         ELSE
            WRITE( NOUT, FMT = 9998 )
         END IF
*
*        Test ZGESDD
*
         SRNAMT = 'ZGESDD'
         INFOT = 1
         CALL ZGESDD( 'X', 0, 0, A, 1, S, U, 1, VT, 1, W, 1, RW, IW,
     $                INFO )
         CALL CHKXER( 'ZGESDD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGESDD( 'N', -1, 0, A, 1, S, U, 1, VT, 1, W, 1, RW, IW,
     $                INFO )
         CALL CHKXER( 'ZGESDD', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGESDD( 'N', 0, -1, A, 1, S, U, 1, VT, 1, W, 1, RW, IW,
     $                INFO )
         CALL CHKXER( 'ZGESDD', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGESDD( 'N', 2, 1, A, 1, S, U, 1, VT, 1, W, 5, RW, IW,
     $                INFO )
         CALL CHKXER( 'ZGESDD', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZGESDD( 'A', 2, 1, A, 2, S, U, 1, VT, 1, W, 5, RW, IW,
     $                INFO )
         CALL CHKXER( 'ZGESDD', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL ZGESDD( 'A', 1, 2, A, 1, S, U, 1, VT, 1, W, 5, RW, IW,
     $                INFO )
         CALL CHKXER( 'ZGESDD', INFOT, NOUT, LERR, OK )
         NT = NT - 2
         IF( OK ) THEN
            WRITE( NOUT, FMT = 9999 )SRNAMT( 1:LEN_TRIM( SRNAMT ) ),
     $           NT
         ELSE
            WRITE( NOUT, FMT = 9998 )
         END IF
*
*        Test ZGEJSV
*
         SRNAMT = 'ZGEJSV'
         INFOT = 1
         CALL ZGEJSV( 'X', 'U', 'V', 'R', 'N', 'N',
     $                 0, 0, A, 1, S, U, 1, VT, 1,
     $                 W, 1, RW, 1, IW, INFO)
         CALL CHKXER( 'ZGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGEJSV( 'G', 'X', 'V', 'R', 'N', 'N',
     $                 0, 0, A, 1, S, U, 1, VT, 1,
     $                 W, 1, RW, 1, IW, INFO)
         CALL CHKXER( 'ZGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGEJSV( 'G', 'U', 'X', 'R', 'N', 'N',
     $                 0, 0, A, 1, S, U, 1, VT, 1,
     $                 W, 1, RW, 1, IW, INFO)
         CALL CHKXER( 'ZGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZGEJSV( 'G', 'U', 'V', 'X', 'N', 'N',
     $                 0, 0, A, 1, S, U, 1, VT, 1,
     $                 W, 1, RW, 1, IW, INFO)
         CALL CHKXER( 'ZGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGEJSV( 'G', 'U', 'V', 'R', 'X', 'N',
     $                 0, 0, A, 1, S, U, 1, VT, 1,
     $                 W, 1, RW, 1, IW, INFO)
         CALL CHKXER( 'ZGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZGEJSV( 'G', 'U', 'V', 'R', 'N', 'X',
     $                 0, 0, A, 1, S, U, 1, VT, 1,
     $                 W, 1, RW, 1, IW, INFO)
         CALL CHKXER( 'ZGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZGEJSV( 'G', 'U', 'V', 'R', 'N', 'N',
     $                 -1, 0, A, 1, S, U, 1, VT, 1,
     $                 W, 1, RW, 1, IW, INFO)
         CALL CHKXER( 'ZGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZGEJSV( 'G', 'U', 'V', 'R', 'N', 'N',
     $                 0, -1, A, 1, S, U, 1, VT, 1,
     $                 W, 1, RW, 1, IW, INFO)
         CALL CHKXER( 'ZGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL ZGEJSV( 'G', 'U', 'V', 'R', 'N', 'N',
     $                 2, 1, A, 1, S, U, 1, VT, 1,
     $                 W, 1, RW, 1, IW, INFO)
         CALL CHKXER( 'ZGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL ZGEJSV( 'G', 'U', 'V', 'R', 'N', 'N',
     $                 2, 2, A, 2, S, U, 1, VT, 2,
     $                 W, 1, RW, 1, IW, INFO)
         CALL CHKXER( 'ZGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL ZGEJSV( 'G', 'U', 'V', 'R', 'N', 'N',
     $                 2, 2, A, 2, S, U, 2, VT, 1,
     $                 W, 1, RW, 1, IW, INFO)
         CALL CHKXER( 'ZGEJSV', INFOT, NOUT, LERR, OK )
         NT = 11
         IF( OK ) THEN
            WRITE( NOUT, FMT = 9999 )SRNAMT( 1:LEN_TRIM( SRNAMT ) ),
     $           NT
         ELSE
            WRITE( NOUT, FMT = 9998 )
         END IF
*
*        Test ZGESVDX
*
         SRNAMT = 'ZGESVDX'
         INFOT = 1
         CALL ZGESVDX( 'X', 'N', 'A', 0, 0, A, 1, ZERO, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, RW, IW, INFO )
         CALL CHKXER( 'ZGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGESVDX( 'N', 'X', 'A', 0, 0, A, 1, ZERO, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, RW, IW, INFO )
         CALL CHKXER( 'ZGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGESVDX( 'N', 'N', 'X', 0, 0, A, 1, ZERO, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, RW, IW, INFO )
         CALL CHKXER( 'ZGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZGESVDX( 'N', 'N', 'A', -1, 0, A, 1, ZERO, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, RW, IW, INFO )
         CALL CHKXER( 'ZGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGESVDX( 'N', 'N', 'A', 0, -1, A, 1, ZERO, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, RW, IW, INFO )
         CALL CHKXER( 'ZGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZGESVDX( 'N', 'N', 'A', 2, 1, A, 1, ZERO, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, RW, IW, INFO )
         CALL CHKXER( 'ZGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZGESVDX( 'N', 'N', 'V', 2, 1, A, 2, -ONE, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, RW, IW, INFO )
         CALL CHKXER( 'ZGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL ZGESVDX( 'N', 'N', 'V', 2, 1, A, 2, ONE, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, RW, IW, INFO )
         CALL CHKXER( 'ZGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL ZGESVDX( 'N', 'N', 'I', 2, 2, A, 2, ZERO, ZERO,
     $                 0, 1, NS, S, U, 1, VT, 1, W, 1, RW, IW, INFO )
         CALL CHKXER( 'ZGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL ZGESVDX( 'V', 'N', 'I', 2, 2, A, 2, ZERO, ZERO,
     $                 1, 0, NS, S, U, 1, VT, 1, W, 1, RW, IW, INFO )
         CALL CHKXER( 'ZGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL ZGESVDX( 'V', 'N', 'A', 2, 2, A, 2, ZERO, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, RW, IW, INFO )
         CALL CHKXER( 'ZGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 17
         CALL ZGESVDX( 'N', 'V', 'A', 2, 2, A, 2, ZERO, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, RW, IW, INFO )
         CALL CHKXER( 'ZGESVDX', INFOT, NOUT, LERR, OK )
         NT = 12
         IF( OK ) THEN
            WRITE( NOUT, FMT = 9999 )SRNAMT( 1:LEN_TRIM( SRNAMT ) ),
     $           NT
         ELSE
            WRITE( NOUT, FMT = 9998 )
         END IF
      END IF
*
*     Print a summary line.
*
      IF( .NOT.LSAMEN( 2, C2, 'BD' ) ) THEN
         IF( OK ) THEN
            WRITE( NOUT, FMT = 9999 )SRNAMT( 1:LEN_TRIM( SRNAMT ) ),
     $           NT
         ELSE
            WRITE( NOUT, FMT = 9998 )
         END IF
      END IF
*
 9999 FORMAT( 1X, A, ' passed the tests of the error exits (', I3,
     $      ' tests done)' )
 9998 FORMAT( ' *** ', A, ' failed the tests of the error exits ***' )
      RETURN
*
*     End of ZERRED
*
      END
