*> \brief \b DERRED
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DERRED( PATH, NUNIT )
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
*> DERRED tests the error exits for the eigenvalue driver routines for
*> DOUBLE PRECISION matrices:
*>
*> PATH  driver   description
*> ----  ------   -----------
*> SEV   DGEEV    find eigenvalues/eigenvectors for nonsymmetric A
*> SES   DGEES    find eigenvalues/Schur form for nonsymmetric A
*> SVX   DGEEVX   SGEEV + balancing and condition estimation
*> SSX   DGEESX   SGEES + balancing and condition estimation
*> DBD   DGESVD   compute SVD of an M-by-N matrix A
*>       DGESDD   compute SVD of an M-by-N matrix A (by divide and
*>                conquer)
*>       DGEJSV   compute SVD of an M-by-N matrix A where M >= N
*>       DGESVDX  compute SVD of an M-by-N matrix A(by bisection
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
*> \ingroup double_eig
*
*  =====================================================================
      SUBROUTINE DERRED( PATH, NUNIT )
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
      INTEGER            NMAX
      DOUBLE PRECISION   ONE, ZERO
      PARAMETER          ( NMAX = 4, ONE = 1.0D0, ZERO = 0.0D0 )
*     ..
*     .. Local Scalars ..
      CHARACTER*2        C2
      INTEGER            I, IHI, ILO, INFO, J, NS, NT, SDIM
      DOUBLE PRECISION   ABNRM
*     ..
*     .. Local Arrays ..
      LOGICAL            B( NMAX )
      INTEGER            IW( 2*NMAX )
      DOUBLE PRECISION   A( NMAX, NMAX ), R1( NMAX ), R2( NMAX ),
     $                   S( NMAX ), U( NMAX, NMAX ), VL( NMAX, NMAX ),
     $                   VR( NMAX, NMAX ), VT( NMAX, NMAX ),
     $                   W( 10*NMAX ), WI( NMAX ), WR( NMAX )
*     ..
*     .. External Subroutines ..
      EXTERNAL           CHKXER, DGEES, DGEESX, DGEEV, DGEEVX, DGEJSV,
     $                   DGESDD, DGESVD
*     ..
*     .. External Functions ..
      LOGICAL            DSLECT, LSAMEN
      EXTERNAL           DSLECT, LSAMEN
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
*        Test DGEEV
*
         SRNAMT = 'DGEEV '
         INFOT = 1
         CALL DGEEV( 'X', 'N', 0, A, 1, WR, WI, VL, 1, VR, 1, W, 1,
     $               INFO )
         CALL CHKXER( 'DGEEV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGEEV( 'N', 'X', 0, A, 1, WR, WI, VL, 1, VR, 1, W, 1,
     $               INFO )
         CALL CHKXER( 'DGEEV ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGEEV( 'N', 'N', -1, A, 1, WR, WI, VL, 1, VR, 1, W, 1,
     $               INFO )
         CALL CHKXER( 'DGEEV ', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGEEV( 'N', 'N', 2, A, 1, WR, WI, VL, 1, VR, 1, W, 6,
     $               INFO )
         CALL CHKXER( 'DGEEV ', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DGEEV( 'V', 'N', 2, A, 2, WR, WI, VL, 1, VR, 1, W, 8,
     $               INFO )
         CALL CHKXER( 'DGEEV ', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DGEEV( 'N', 'V', 2, A, 2, WR, WI, VL, 1, VR, 1, W, 8,
     $               INFO )
         CALL CHKXER( 'DGEEV ', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL DGEEV( 'V', 'V', 1, A, 1, WR, WI, VL, 1, VR, 1, W, 3,
     $               INFO )
         CALL CHKXER( 'DGEEV ', INFOT, NOUT, LERR, OK )
         NT = NT + 7
*
      ELSE IF( LSAMEN( 2, C2, 'ES' ) ) THEN
*
*        Test DGEES
*
         SRNAMT = 'DGEES '
         INFOT = 1
         CALL DGEES( 'X', 'N', DSLECT, 0, A, 1, SDIM, WR, WI, VL, 1, W,
     $               1, B, INFO )
         CALL CHKXER( 'DGEES ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGEES( 'N', 'X', DSLECT, 0, A, 1, SDIM, WR, WI, VL, 1, W,
     $               1, B, INFO )
         CALL CHKXER( 'DGEES ', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGEES( 'N', 'S', DSLECT, -1, A, 1, SDIM, WR, WI, VL, 1, W,
     $               1, B, INFO )
         CALL CHKXER( 'DGEES ', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DGEES( 'N', 'S', DSLECT, 2, A, 1, SDIM, WR, WI, VL, 1, W,
     $               6, B, INFO )
         CALL CHKXER( 'DGEES ', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DGEES( 'V', 'S', DSLECT, 2, A, 2, SDIM, WR, WI, VL, 1, W,
     $               6, B, INFO )
         CALL CHKXER( 'DGEES ', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL DGEES( 'N', 'S', DSLECT, 1, A, 1, SDIM, WR, WI, VL, 1, W,
     $               2, B, INFO )
         CALL CHKXER( 'DGEES ', INFOT, NOUT, LERR, OK )
         NT = NT + 6
*
      ELSE IF( LSAMEN( 2, C2, 'VX' ) ) THEN
*
*        Test DGEEVX
*
         SRNAMT = 'DGEEVX'
         INFOT = 1
         CALL DGEEVX( 'X', 'N', 'N', 'N', 0, A, 1, WR, WI, VL, 1, VR, 1,
     $                ILO, IHI, S, ABNRM, R1, R2, W, 1, IW, INFO )
         CALL CHKXER( 'DGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGEEVX( 'N', 'X', 'N', 'N', 0, A, 1, WR, WI, VL, 1, VR, 1,
     $                ILO, IHI, S, ABNRM, R1, R2, W, 1, IW, INFO )
         CALL CHKXER( 'DGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGEEVX( 'N', 'N', 'X', 'N', 0, A, 1, WR, WI, VL, 1, VR, 1,
     $                ILO, IHI, S, ABNRM, R1, R2, W, 1, IW, INFO )
         CALL CHKXER( 'DGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGEEVX( 'N', 'N', 'N', 'X', 0, A, 1, WR, WI, VL, 1, VR, 1,
     $                ILO, IHI, S, ABNRM, R1, R2, W, 1, IW, INFO )
         CALL CHKXER( 'DGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGEEVX( 'N', 'N', 'N', 'N', -1, A, 1, WR, WI, VL, 1, VR,
     $                1, ILO, IHI, S, ABNRM, R1, R2, W, 1, IW, INFO )
         CALL CHKXER( 'DGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DGEEVX( 'N', 'N', 'N', 'N', 2, A, 1, WR, WI, VL, 1, VR, 1,
     $                ILO, IHI, S, ABNRM, R1, R2, W, 1, IW, INFO )
         CALL CHKXER( 'DGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DGEEVX( 'N', 'V', 'N', 'N', 2, A, 2, WR, WI, VL, 1, VR, 1,
     $                ILO, IHI, S, ABNRM, R1, R2, W, 6, IW, INFO )
         CALL CHKXER( 'DGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL DGEEVX( 'N', 'N', 'V', 'N', 2, A, 2, WR, WI, VL, 1, VR, 1,
     $                ILO, IHI, S, ABNRM, R1, R2, W, 6, IW, INFO )
         CALL CHKXER( 'DGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 21
         CALL DGEEVX( 'N', 'N', 'N', 'N', 1, A, 1, WR, WI, VL, 1, VR, 1,
     $                ILO, IHI, S, ABNRM, R1, R2, W, 1, IW, INFO )
         CALL CHKXER( 'DGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 21
         CALL DGEEVX( 'N', 'V', 'N', 'N', 1, A, 1, WR, WI, VL, 1, VR, 1,
     $                ILO, IHI, S, ABNRM, R1, R2, W, 2, IW, INFO )
         CALL CHKXER( 'DGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 21
         CALL DGEEVX( 'N', 'N', 'V', 'V', 1, A, 1, WR, WI, VL, 1, VR, 1,
     $                ILO, IHI, S, ABNRM, R1, R2, W, 3, IW, INFO )
         CALL CHKXER( 'DGEEVX', INFOT, NOUT, LERR, OK )
         NT = NT + 11
*
      ELSE IF( LSAMEN( 2, C2, 'SX' ) ) THEN
*
*        Test DGEESX
*
         SRNAMT = 'DGEESX'
         INFOT = 1
         CALL DGEESX( 'X', 'N', DSLECT, 'N', 0, A, 1, SDIM, WR, WI, VL,
     $                1, R1( 1 ), R2( 1 ), W, 1, IW, 1, B, INFO )
         CALL CHKXER( 'DGEESX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGEESX( 'N', 'X', DSLECT, 'N', 0, A, 1, SDIM, WR, WI, VL,
     $                1, R1( 1 ), R2( 1 ), W, 1, IW, 1, B, INFO )
         CALL CHKXER( 'DGEESX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGEESX( 'N', 'N', DSLECT, 'X', 0, A, 1, SDIM, WR, WI, VL,
     $                1, R1( 1 ), R2( 1 ), W, 1, IW, 1, B, INFO )
         CALL CHKXER( 'DGEESX', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGEESX( 'N', 'N', DSLECT, 'N', -1, A, 1, SDIM, WR, WI, VL,
     $                1, R1( 1 ), R2( 1 ), W, 1, IW, 1, B, INFO )
         CALL CHKXER( 'DGEESX', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DGEESX( 'N', 'N', DSLECT, 'N', 2, A, 1, SDIM, WR, WI, VL,
     $                1, R1( 1 ), R2( 1 ), W, 6, IW, 1, B, INFO )
         CALL CHKXER( 'DGEESX', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL DGEESX( 'V', 'N', DSLECT, 'N', 2, A, 2, SDIM, WR, WI, VL,
     $                1, R1( 1 ), R2( 1 ), W, 6, IW, 1, B, INFO )
         CALL CHKXER( 'DGEESX', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL DGEESX( 'N', 'N', DSLECT, 'N', 1, A, 1, SDIM, WR, WI, VL,
     $                1, R1( 1 ), R2( 1 ), W, 2, IW, 1, B, INFO )
         CALL CHKXER( 'DGEESX', INFOT, NOUT, LERR, OK )
         NT = NT + 7
*
      ELSE IF( LSAMEN( 2, C2, 'BD' ) ) THEN
*
*        Test DGESVD
*
         SRNAMT = 'DGESVD'
         INFOT = 1
         CALL DGESVD( 'X', 'N', 0, 0, A, 1, S, U, 1, VT, 1, W, 1, INFO )
         CALL CHKXER( 'DGESVD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGESVD( 'N', 'X', 0, 0, A, 1, S, U, 1, VT, 1, W, 1, INFO )
         CALL CHKXER( 'DGESVD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGESVD( 'O', 'O', 0, 0, A, 1, S, U, 1, VT, 1, W, 1, INFO )
         CALL CHKXER( 'DGESVD', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGESVD( 'N', 'N', -1, 0, A, 1, S, U, 1, VT, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'DGESVD', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGESVD( 'N', 'N', 0, -1, A, 1, S, U, 1, VT, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'DGESVD', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DGESVD( 'N', 'N', 2, 1, A, 1, S, U, 1, VT, 1, W, 5, INFO )
         CALL CHKXER( 'DGESVD', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DGESVD( 'A', 'N', 2, 1, A, 2, S, U, 1, VT, 1, W, 5, INFO )
         CALL CHKXER( 'DGESVD', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DGESVD( 'N', 'A', 1, 2, A, 1, S, U, 1, VT, 1, W, 5, INFO )
         CALL CHKXER( 'DGESVD', INFOT, NOUT, LERR, OK )
         NT = 8
         IF( OK ) THEN
            WRITE( NOUT, FMT = 9999 )SRNAMT( 1:LEN_TRIM( SRNAMT ) ),
     $           NT
         ELSE
            WRITE( NOUT, FMT = 9998 )
         END IF
*
*        Test DGESDD
*
         SRNAMT = 'DGESDD'
         INFOT = 1
         CALL DGESDD( 'X', 0, 0, A, 1, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'DGESDD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGESDD( 'N', -1, 0, A, 1, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'DGESDD', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGESDD( 'N', 0, -1, A, 1, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'DGESDD', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGESDD( 'N', 2, 1, A, 1, S, U, 1, VT, 1, W, 5, IW, INFO )
         CALL CHKXER( 'DGESDD', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DGESDD( 'A', 2, 1, A, 2, S, U, 1, VT, 1, W, 5, IW, INFO )
         CALL CHKXER( 'DGESDD', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DGESDD( 'A', 1, 2, A, 1, S, U, 1, VT, 1, W, 5, IW, INFO )
         CALL CHKXER( 'DGESDD', INFOT, NOUT, LERR, OK )
         NT = 6
         IF( OK ) THEN
            WRITE( NOUT, FMT = 9999 )SRNAMT( 1:LEN_TRIM( SRNAMT ) ),
     $           NT
         ELSE
            WRITE( NOUT, FMT = 9998 )
         END IF
*
*        Test DGEJSV
*
         SRNAMT = 'DGEJSV'
         INFOT = 1
         CALL DGEJSV( 'X', 'U', 'V', 'R', 'N', 'N',
     $                 0, 0, A, 1, S, U, 1, VT, 1,
     $                 W, 1, IW, INFO)
         CALL CHKXER( 'DGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGEJSV( 'G', 'X', 'V', 'R', 'N', 'N',
     $                 0, 0, A, 1, S, U, 1, VT, 1,
     $                 W, 1, IW, INFO)
         CALL CHKXER( 'DGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGEJSV( 'G', 'U', 'X', 'R', 'N', 'N',
     $                 0, 0, A, 1, S, U, 1, VT, 1,
     $                 W, 1, IW, INFO)
         CALL CHKXER( 'DGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGEJSV( 'G', 'U', 'V', 'X', 'N', 'N',
     $                 0, 0, A, 1, S, U, 1, VT, 1,
     $                 W, 1, IW, INFO)
         CALL CHKXER( 'DGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGEJSV( 'G', 'U', 'V', 'R', 'X', 'N',
     $                 0, 0, A, 1, S, U, 1, VT, 1,
     $                 W, 1, IW, INFO)
         CALL CHKXER( 'DGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL DGEJSV( 'G', 'U', 'V', 'R', 'N', 'X',
     $                 0, 0, A, 1, S, U, 1, VT, 1,
     $                 W, 1, IW, INFO)
         CALL CHKXER( 'DGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DGEJSV( 'G', 'U', 'V', 'R', 'N', 'N',
     $                 -1, 0, A, 1, S, U, 1, VT, 1,
     $                 W, 1, IW, INFO)
         CALL CHKXER( 'DGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DGEJSV( 'G', 'U', 'V', 'R', 'N', 'N',
     $                 0, -1, A, 1, S, U, 1, VT, 1,
     $                 W, 1, IW, INFO)
         CALL CHKXER( 'DGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DGEJSV( 'G', 'U', 'V', 'R', 'N', 'N',
     $                 2, 1, A, 1, S, U, 1, VT, 1,
     $                 W, 1, IW, INFO)
         CALL CHKXER( 'DGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL DGEJSV( 'G', 'U', 'V', 'R', 'N', 'N',
     $                 2, 2, A, 2, S, U, 1, VT, 2,
     $                 W, 1, IW, INFO)
         CALL CHKXER( 'DGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL DGEJSV( 'G', 'U', 'V', 'R', 'N', 'N',
     $                 2, 2, A, 2, S, U, 2, VT, 1,
     $                 W, 1, IW, INFO)
         CALL CHKXER( 'DGEJSV', INFOT, NOUT, LERR, OK )
         NT = 11
         IF( OK ) THEN
            WRITE( NOUT, FMT = 9999 )SRNAMT( 1:LEN_TRIM( SRNAMT ) ),
     $           NT
         ELSE
            WRITE( NOUT, FMT = 9998 )
         END IF
*
*        Test DGESVDX
*
         SRNAMT = 'DGESVDX'
         INFOT = 1
         CALL DGESVDX( 'X', 'N', 'A', 0, 0, A, 1, ZERO, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'DGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL DGESVDX( 'N', 'X', 'A', 0, 0, A, 1, ZERO, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'DGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL DGESVDX( 'N', 'N', 'X', 0, 0, A, 1, ZERO, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'DGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL DGESVDX( 'N', 'N', 'A', -1, 0, A, 1, ZERO, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'DGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL DGESVDX( 'N', 'N', 'A', 0, -1, A, 1, ZERO, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'DGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL DGESVDX( 'N', 'N', 'A', 2, 1, A, 1, ZERO, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'DGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL DGESVDX( 'N', 'N', 'V', 2, 1, A, 2, -ONE, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'DGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL DGESVDX( 'N', 'N', 'V', 2, 1, A, 2, ONE, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'DGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL DGESVDX( 'N', 'N', 'I', 2, 2, A, 2, ZERO, ZERO,
     $                 0, 1, NS, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'DGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL DGESVDX( 'V', 'N', 'I', 2, 2, A, 2, ZERO, ZERO,
     $                 1, 0, NS, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'DGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL DGESVDX( 'V', 'N', 'A', 2, 2, A, 2, ZERO, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'DGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 17
         CALL DGESVDX( 'N', 'V', 'A', 2, 2, A, 2, ZERO, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'DGESVDX', INFOT, NOUT, LERR, OK )
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
*     End of DERRED
      END
