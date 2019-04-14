*> \brief \b SERRED
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SERRED( PATH, NUNIT )
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
*> SERRED tests the error exits for the eigenvalue driver routines for
*> REAL matrices:
*>
*> PATH  driver   description
*> ----  ------   -----------
*> SEV   SGEEV    find eigenvalues/eigenvectors for nonsymmetric A
*> SES   SGEES    find eigenvalues/Schur form for nonsymmetric A
*> SVX   SGEEVX   SGEEV + balancing and condition estimation
*> SSX   SGEESX   SGEES + balancing and condition estimation
*> SBD   SGESVD   compute SVD of an M-by-N matrix A
*>       SGESDD   compute SVD of an M-by-N matrix A (by divide and
*>                conquer)
*>       SGEJSV   compute SVD of an M-by-N matrix A where M >= N
*>       SGESVDX  compute SVD of an M-by-N matrix A(by bisection
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
*> \ingroup single_eig
*
*  =====================================================================
      SUBROUTINE SERRED( PATH, NUNIT )
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
      REAL               ONE, ZERO
      PARAMETER          ( NMAX = 4, ONE = 1.0E0, ZERO = 0.0E0 )
*     ..
*     .. Local Scalars ..
      CHARACTER*2        C2
      INTEGER            I, IHI, ILO, INFO, J, NS, NT, SDIM
      REAL               ABNRM
*     ..
*     .. Local Arrays ..
      LOGICAL            B( NMAX )
      INTEGER            IW( 2*NMAX )
      REAL               A( NMAX, NMAX ), R1( NMAX ), R2( NMAX ),
     $                   S( NMAX ), U( NMAX, NMAX ), VL( NMAX, NMAX ),
     $                   VR( NMAX, NMAX ), VT( NMAX, NMAX ),
     $                   W( 10*NMAX ), WI( NMAX ), WR( NMAX )
*     ..
*     .. External Subroutines ..
      EXTERNAL           CHKXER, SGEES, SGEESX, SGEEV, SGEEVX, SGEJSV,
     $                   SGESDD, SGESVD
*     ..
*     .. External Functions ..
      LOGICAL            SSLECT, LSAMEN
      EXTERNAL           SSLECT, LSAMEN
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          LEN_TRIM
*     ..
*     .. Arrays in Common ..
      LOGICAL            SELVAL( 20 )
      REAL               SELWI( 20 ), SELWR( 20 )
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
*        Test SGEEV
*
         SRNAMT = 'SGEEV '
         INFOT = 1
         CALL SGEEV( 'X', 'N', 0, A, 1, WR, WI, VL, 1, VR, 1, W, 1,
     $               INFO )
         CALL CHKXER( 'SGEEV ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGEEV( 'N', 'X', 0, A, 1, WR, WI, VL, 1, VR, 1, W, 1,
     $               INFO )
         CALL CHKXER( 'SGEEV ', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGEEV( 'N', 'N', -1, A, 1, WR, WI, VL, 1, VR, 1, W, 1,
     $               INFO )
         CALL CHKXER( 'SGEEV ', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGEEV( 'N', 'N', 2, A, 1, WR, WI, VL, 1, VR, 1, W, 6,
     $               INFO )
         CALL CHKXER( 'SGEEV ', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL SGEEV( 'V', 'N', 2, A, 2, WR, WI, VL, 1, VR, 1, W, 8,
     $               INFO )
         CALL CHKXER( 'SGEEV ', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL SGEEV( 'N', 'V', 2, A, 2, WR, WI, VL, 1, VR, 1, W, 8,
     $               INFO )
         CALL CHKXER( 'SGEEV ', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL SGEEV( 'V', 'V', 1, A, 1, WR, WI, VL, 1, VR, 1, W, 3,
     $               INFO )
         CALL CHKXER( 'SGEEV ', INFOT, NOUT, LERR, OK )
         NT = NT + 7
*
      ELSE IF( LSAMEN( 2, C2, 'ES' ) ) THEN
*
*        Test SGEES
*
         SRNAMT = 'SGEES '
         INFOT = 1
         CALL SGEES( 'X', 'N', SSLECT, 0, A, 1, SDIM, WR, WI, VL, 1, W,
     $               1, B, INFO )
         CALL CHKXER( 'SGEES ', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGEES( 'N', 'X', SSLECT, 0, A, 1, SDIM, WR, WI, VL, 1, W,
     $               1, B, INFO )
         CALL CHKXER( 'SGEES ', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGEES( 'N', 'S', SSLECT, -1, A, 1, SDIM, WR, WI, VL, 1, W,
     $               1, B, INFO )
         CALL CHKXER( 'SGEES ', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SGEES( 'N', 'S', SSLECT, 2, A, 1, SDIM, WR, WI, VL, 1, W,
     $               6, B, INFO )
         CALL CHKXER( 'SGEES ', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL SGEES( 'V', 'S', SSLECT, 2, A, 2, SDIM, WR, WI, VL, 1, W,
     $               6, B, INFO )
         CALL CHKXER( 'SGEES ', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL SGEES( 'N', 'S', SSLECT, 1, A, 1, SDIM, WR, WI, VL, 1, W,
     $               2, B, INFO )
         CALL CHKXER( 'SGEES ', INFOT, NOUT, LERR, OK )
         NT = NT + 6
*
      ELSE IF( LSAMEN( 2, C2, 'VX' ) ) THEN
*
*        Test SGEEVX
*
         SRNAMT = 'SGEEVX'
         INFOT = 1
         CALL SGEEVX( 'X', 'N', 'N', 'N', 0, A, 1, WR, WI, VL, 1, VR, 1,
     $                ILO, IHI, S, ABNRM, R1, R2, W, 1, IW, INFO )
         CALL CHKXER( 'SGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGEEVX( 'N', 'X', 'N', 'N', 0, A, 1, WR, WI, VL, 1, VR, 1,
     $                ILO, IHI, S, ABNRM, R1, R2, W, 1, IW, INFO )
         CALL CHKXER( 'SGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGEEVX( 'N', 'N', 'X', 'N', 0, A, 1, WR, WI, VL, 1, VR, 1,
     $                ILO, IHI, S, ABNRM, R1, R2, W, 1, IW, INFO )
         CALL CHKXER( 'SGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGEEVX( 'N', 'N', 'N', 'X', 0, A, 1, WR, WI, VL, 1, VR, 1,
     $                ILO, IHI, S, ABNRM, R1, R2, W, 1, IW, INFO )
         CALL CHKXER( 'SGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGEEVX( 'N', 'N', 'N', 'N', -1, A, 1, WR, WI, VL, 1, VR,
     $                1, ILO, IHI, S, ABNRM, R1, R2, W, 1, IW, INFO )
         CALL CHKXER( 'SGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SGEEVX( 'N', 'N', 'N', 'N', 2, A, 1, WR, WI, VL, 1, VR, 1,
     $                ILO, IHI, S, ABNRM, R1, R2, W, 1, IW, INFO )
         CALL CHKXER( 'SGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL SGEEVX( 'N', 'V', 'N', 'N', 2, A, 2, WR, WI, VL, 1, VR, 1,
     $                ILO, IHI, S, ABNRM, R1, R2, W, 6, IW, INFO )
         CALL CHKXER( 'SGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL SGEEVX( 'N', 'N', 'V', 'N', 2, A, 2, WR, WI, VL, 1, VR, 1,
     $                ILO, IHI, S, ABNRM, R1, R2, W, 6, IW, INFO )
         CALL CHKXER( 'SGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 21
         CALL SGEEVX( 'N', 'N', 'N', 'N', 1, A, 1, WR, WI, VL, 1, VR, 1,
     $                ILO, IHI, S, ABNRM, R1, R2, W, 1, IW, INFO )
         CALL CHKXER( 'SGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 21
         CALL SGEEVX( 'N', 'V', 'N', 'N', 1, A, 1, WR, WI, VL, 1, VR, 1,
     $                ILO, IHI, S, ABNRM, R1, R2, W, 2, IW, INFO )
         CALL CHKXER( 'SGEEVX', INFOT, NOUT, LERR, OK )
         INFOT = 21
         CALL SGEEVX( 'N', 'N', 'V', 'V', 1, A, 1, WR, WI, VL, 1, VR, 1,
     $                ILO, IHI, S, ABNRM, R1, R2, W, 3, IW, INFO )
         CALL CHKXER( 'SGEEVX', INFOT, NOUT, LERR, OK )
         NT = NT + 11
*
      ELSE IF( LSAMEN( 2, C2, 'SX' ) ) THEN
*
*        Test SGEESX
*
         SRNAMT = 'SGEESX'
         INFOT = 1
         CALL SGEESX( 'X', 'N', SSLECT, 'N', 0, A, 1, SDIM, WR, WI, VL,
     $                1, R1( 1 ), R2( 1 ), W, 1, IW, 1, B, INFO )
         CALL CHKXER( 'SGEESX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGEESX( 'N', 'X', SSLECT, 'N', 0, A, 1, SDIM, WR, WI, VL,
     $                1, R1( 1 ), R2( 1 ), W, 1, IW, 1, B, INFO )
         CALL CHKXER( 'SGEESX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGEESX( 'N', 'N', SSLECT, 'X', 0, A, 1, SDIM, WR, WI, VL,
     $                1, R1( 1 ), R2( 1 ), W, 1, IW, 1, B, INFO )
         CALL CHKXER( 'SGEESX', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGEESX( 'N', 'N', SSLECT, 'N', -1, A, 1, SDIM, WR, WI, VL,
     $                1, R1( 1 ), R2( 1 ), W, 1, IW, 1, B, INFO )
         CALL CHKXER( 'SGEESX', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SGEESX( 'N', 'N', SSLECT, 'N', 2, A, 1, SDIM, WR, WI, VL,
     $                1, R1( 1 ), R2( 1 ), W, 6, IW, 1, B, INFO )
         CALL CHKXER( 'SGEESX', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL SGEESX( 'V', 'N', SSLECT, 'N', 2, A, 2, SDIM, WR, WI, VL,
     $                1, R1( 1 ), R2( 1 ), W, 6, IW, 1, B, INFO )
         CALL CHKXER( 'SGEESX', INFOT, NOUT, LERR, OK )
         INFOT = 16
         CALL SGEESX( 'N', 'N', SSLECT, 'N', 1, A, 1, SDIM, WR, WI, VL,
     $                1, R1( 1 ), R2( 1 ), W, 2, IW, 1, B, INFO )
         CALL CHKXER( 'SGEESX', INFOT, NOUT, LERR, OK )
         NT = NT + 7
*
      ELSE IF( LSAMEN( 2, C2, 'BD' ) ) THEN
*
*        Test SGESVD
*
         SRNAMT = 'SGESVD'
         INFOT = 1
         CALL SGESVD( 'X', 'N', 0, 0, A, 1, S, U, 1, VT, 1, W, 1, INFO )
         CALL CHKXER( 'SGESVD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGESVD( 'N', 'X', 0, 0, A, 1, S, U, 1, VT, 1, W, 1, INFO )
         CALL CHKXER( 'SGESVD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGESVD( 'O', 'O', 0, 0, A, 1, S, U, 1, VT, 1, W, 1, INFO )
         CALL CHKXER( 'SGESVD', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGESVD( 'N', 'N', -1, 0, A, 1, S, U, 1, VT, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'SGESVD', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGESVD( 'N', 'N', 0, -1, A, 1, S, U, 1, VT, 1, W, 1,
     $                INFO )
         CALL CHKXER( 'SGESVD', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SGESVD( 'N', 'N', 2, 1, A, 1, S, U, 1, VT, 1, W, 5, INFO )
         CALL CHKXER( 'SGESVD', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL SGESVD( 'A', 'N', 2, 1, A, 2, S, U, 1, VT, 1, W, 5, INFO )
         CALL CHKXER( 'SGESVD', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL SGESVD( 'N', 'A', 1, 2, A, 1, S, U, 1, VT, 1, W, 5, INFO )
         CALL CHKXER( 'SGESVD', INFOT, NOUT, LERR, OK )
         NT = 8
         IF( OK ) THEN
            WRITE( NOUT, FMT = 9999 )SRNAMT( 1:LEN_TRIM( SRNAMT ) ),
     $           NT
         ELSE
            WRITE( NOUT, FMT = 9998 )
         END IF
*
*        Test SGESDD
*
         SRNAMT = 'SGESDD'
         INFOT = 1
         CALL SGESDD( 'X', 0, 0, A, 1, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'SGESDD', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGESDD( 'N', -1, 0, A, 1, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'SGESDD', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGESDD( 'N', 0, -1, A, 1, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'SGESDD', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGESDD( 'N', 2, 1, A, 1, S, U, 1, VT, 1, W, 5, IW, INFO )
         CALL CHKXER( 'SGESDD', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SGESDD( 'A', 2, 1, A, 2, S, U, 1, VT, 1, W, 5, IW, INFO )
         CALL CHKXER( 'SGESDD', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL SGESDD( 'A', 1, 2, A, 1, S, U, 1, VT, 1, W, 5, IW, INFO )
         CALL CHKXER( 'SGESDD', INFOT, NOUT, LERR, OK )
         NT = 6
         IF( OK ) THEN
            WRITE( NOUT, FMT = 9999 )SRNAMT( 1:LEN_TRIM( SRNAMT ) ),
     $           NT
         ELSE
            WRITE( NOUT, FMT = 9998 )
         END IF
*
*        Test SGEJSV
*
         SRNAMT = 'SGEJSV'
         INFOT = 1
         CALL SGEJSV( 'X', 'U', 'V', 'R', 'N', 'N',
     $                 0, 0, A, 1, S, U, 1, VT, 1,
     $                 W, 1, IW, INFO)
         CALL CHKXER( 'SGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGEJSV( 'G', 'X', 'V', 'R', 'N', 'N',
     $                 0, 0, A, 1, S, U, 1, VT, 1,
     $                 W, 1, IW, INFO)
         CALL CHKXER( 'SGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGEJSV( 'G', 'U', 'X', 'R', 'N', 'N',
     $                 0, 0, A, 1, S, U, 1, VT, 1,
     $                 W, 1, IW, INFO)
         CALL CHKXER( 'SGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGEJSV( 'G', 'U', 'V', 'X', 'N', 'N',
     $                 0, 0, A, 1, S, U, 1, VT, 1,
     $                 W, 1, IW, INFO)
         CALL CHKXER( 'SGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGEJSV( 'G', 'U', 'V', 'R', 'X', 'N',
     $                 0, 0, A, 1, S, U, 1, VT, 1,
     $                 W, 1, IW, INFO)
         CALL CHKXER( 'SGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SGEJSV( 'G', 'U', 'V', 'R', 'N', 'X',
     $                 0, 0, A, 1, S, U, 1, VT, 1,
     $                 W, 1, IW, INFO)
         CALL CHKXER( 'SGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SGEJSV( 'G', 'U', 'V', 'R', 'N', 'N',
     $                 -1, 0, A, 1, S, U, 1, VT, 1,
     $                 W, 1, IW, INFO)
         CALL CHKXER( 'SGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SGEJSV( 'G', 'U', 'V', 'R', 'N', 'N',
     $                 0, -1, A, 1, S, U, 1, VT, 1,
     $                 W, 1, IW, INFO)
         CALL CHKXER( 'SGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL SGEJSV( 'G', 'U', 'V', 'R', 'N', 'N',
     $                 2, 1, A, 1, S, U, 1, VT, 1,
     $                 W, 1, IW, INFO)
         CALL CHKXER( 'SGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 13
         CALL SGEJSV( 'G', 'U', 'V', 'R', 'N', 'N',
     $                 2, 2, A, 2, S, U, 1, VT, 2,
     $                 W, 1, IW, INFO)
         CALL CHKXER( 'SGEJSV', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL SGEJSV( 'G', 'U', 'V', 'R', 'N', 'N',
     $                 2, 2, A, 2, S, U, 2, VT, 1,
     $                 W, 1, IW, INFO)
         CALL CHKXER( 'SGEJSV', INFOT, NOUT, LERR, OK )
         NT = 11
         IF( OK ) THEN
            WRITE( NOUT, FMT = 9999 )SRNAMT( 1:LEN_TRIM( SRNAMT ) ),
     $           NT
         ELSE
            WRITE( NOUT, FMT = 9998 )
         END IF
*
*        Test SGESVDX
*
         SRNAMT = 'SGESVDX'
         INFOT = 1
         CALL SGESVDX( 'X', 'N', 'A', 0, 0, A, 1, ZERO, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'SGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SGESVDX( 'N', 'X', 'A', 0, 0, A, 1, ZERO, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'SGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SGESVDX( 'N', 'N', 'X', 0, 0, A, 1, ZERO, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'SGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SGESVDX( 'N', 'N', 'A', -1, 0, A, 1, ZERO, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'SGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SGESVDX( 'N', 'N', 'A', 0, -1, A, 1, ZERO, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'SGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SGESVDX( 'N', 'N', 'A', 2, 1, A, 1, ZERO, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'SGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SGESVDX( 'N', 'N', 'V', 2, 1, A, 2, -ONE, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'SGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL SGESVDX( 'N', 'N', 'V', 2, 1, A, 2, ONE, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'SGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL SGESVDX( 'N', 'N', 'I', 2, 2, A, 2, ZERO, ZERO,
     $                 0, 1, NS, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'SGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL SGESVDX( 'V', 'N', 'I', 2, 2, A, 2, ZERO, ZERO,
     $                 1, 0, NS, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'SGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 15
         CALL SGESVDX( 'V', 'N', 'A', 2, 2, A, 2, ZERO, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'SGESVDX', INFOT, NOUT, LERR, OK )
         INFOT = 17
         CALL SGESVDX( 'N', 'V', 'A', 2, 2, A, 2, ZERO, ZERO,
     $                 0, 0, NS, S, U, 1, VT, 1, W, 1, IW, INFO )
         CALL CHKXER( 'SGESVDX', INFOT, NOUT, LERR, OK )
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
*     End of SERRED
*
      END
