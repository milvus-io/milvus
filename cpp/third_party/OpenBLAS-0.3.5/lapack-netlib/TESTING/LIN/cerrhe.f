*> \brief \b CERRHE
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CERRHE( PATH, NUNIT )
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
*> CERRHE tests the error exits for the COMPLEX routines
*> for Hermitian indefinite matrices.
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
*> \date November 2017
*
*> \ingroup complex_lin
*
*  =====================================================================
      SUBROUTINE CERRHE( PATH, NUNIT )
*
*  -- LAPACK test routine (version 3.8.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2017
*
*     .. Scalar Arguments ..
      CHARACTER*3        PATH
      INTEGER            NUNIT
*     ..
*
*  =====================================================================
*
*
*     .. Parameters ..
      INTEGER            NMAX
      PARAMETER          ( NMAX = 4 )
*     ..
*     .. Local Scalars ..
      CHARACTER*2        C2
      INTEGER            I, INFO, J
      REAL               ANRM, RCOND
*     ..
*     .. Local Arrays ..
      INTEGER            IP( NMAX )
      REAL               R( NMAX ), R1( NMAX ), R2( NMAX )
      COMPLEX            A( NMAX, NMAX ), AF( NMAX, NMAX ), B( NMAX ),
     $                   E( NMAX ), W( 2*NMAX ), X( NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHECON, CSYCON_3, CHECON_ROOK, CHERFS,
     $                   CHETF2, CHETF2_RK, CHETF2_ROOK, CHETRF_AA, 
     $                   CHETRF, CHETRF_RK, CHETRF_ROOK, CHETRI,
     $                   CHETRI_3, CHETRI_3X, CHETRI_ROOK, CHETRI2,
     $                   CHETRI2X, CHETRS, CHETRS_3, CHETRS_ROOK,
     $                   CHETRS_AA, CHKXER, CHPCON, CHPRFS, CHPTRF,
     $                   CHETRF_AA_2STAGE, CHETRS_AA_2STAGE,
     $                   CHPTRI, CHPTRS
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
      INTRINSIC          CMPLX, REAL
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
            A( I, J ) = CMPLX( 1. / REAL( I+J ), -1. / REAL( I+J ) )
            AF( I, J ) = CMPLX( 1. / REAL( I+J ), -1. / REAL( I+J ) )
   10    CONTINUE
         B( J ) = 0.E+0
         E( J ) = 0.E+0
         R1( J ) = 0.E+0
         R2( J ) = 0.E+0
         W( J ) = 0.E+0
         X( J ) = 0.E+0
         IP( J ) = J
   20 CONTINUE
      ANRM = 1.0
      OK = .TRUE.
*
      IF( LSAMEN( 2, C2, 'HE' ) ) THEN
*
*        Test error exits of the routines that use factorization
*        of a Hermitian indefinite matrix with patrial
*        (Bunch-Kaufman) diagonal pivoting method.
*
*        CHETRF
*
         SRNAMT = 'CHETRF'
         INFOT = 1
         CALL CHETRF( '/', 0, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'CHETRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHETRF( 'U', -1, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'CHETRF', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CHETRF( 'U', 2, A, 1, IP, W, 4, INFO )
         CALL CHKXER( 'CHETRF', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CHETRF( 'U', 0, A, 1, IP, W, 0, INFO )
         CALL CHKXER( 'CHETRF', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CHETRF( 'U', 0, A, 1, IP, W, -2, INFO )
         CALL CHKXER( 'CHETRF', INFOT, NOUT, LERR, OK )
*
*        CHETF2
*
         SRNAMT = 'CHETF2'
         INFOT = 1
         CALL CHETF2( '/', 0, A, 1, IP, INFO )
         CALL CHKXER( 'CHETF2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHETF2( 'U', -1, A, 1, IP, INFO )
         CALL CHKXER( 'CHETF2', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CHETF2( 'U', 2, A, 1, IP, INFO )
         CALL CHKXER( 'CHETF2', INFOT, NOUT, LERR, OK )
*
*        CHETRI
*
         SRNAMT = 'CHETRI'
         INFOT = 1
         CALL CHETRI( '/', 0, A, 1, IP, W, INFO )
         CALL CHKXER( 'CHETRI', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHETRI( 'U', -1, A, 1, IP, W, INFO )
         CALL CHKXER( 'CHETRI', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CHETRI( 'U', 2, A, 1, IP, W, INFO )
         CALL CHKXER( 'CHETRI', INFOT, NOUT, LERR, OK )
*
*        CHETRI2
*
         SRNAMT = 'CHETRI2'
         INFOT = 1
         CALL CHETRI2( '/', 0, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'CHETRI2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHETRI2( 'U', -1, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'CHETRI2', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CHETRI2( 'U', 2, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'CHETRI2', INFOT, NOUT, LERR, OK )
*
*        CHETRI2X
*
         SRNAMT = 'CHETRI2X'
         INFOT = 1
         CALL CHETRI2X( '/', 0, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'CHETRI2X', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHETRI2X( 'U', -1, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'CHETRI2X', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CHETRI2X( 'U', 2, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'CHETRI2X', INFOT, NOUT, LERR, OK )
*
*        CHETRS
*
         SRNAMT = 'CHETRS'
         INFOT = 1
         CALL CHETRS( '/', 0, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'CHETRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHETRS( 'U', -1, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'CHETRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CHETRS( 'U', 0, -1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'CHETRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CHETRS( 'U', 2, 1, A, 1, IP, B, 2, INFO )
         CALL CHKXER( 'CHETRS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CHETRS( 'U', 2, 1, A, 2, IP, B, 1, INFO )
         CALL CHKXER( 'CHETRS', INFOT, NOUT, LERR, OK )
*
*        CHERFS
*
         SRNAMT = 'CHERFS'
         INFOT = 1
         CALL CHERFS( '/', 0, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'CHERFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHERFS( 'U', -1, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1, R2,
     $                W, R, INFO )
         CALL CHKXER( 'CHERFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CHERFS( 'U', 0, -1, A, 1, AF, 1, IP, B, 1, X, 1, R1, R2,
     $                W, R, INFO )
         CALL CHKXER( 'CHERFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CHERFS( 'U', 2, 1, A, 1, AF, 2, IP, B, 2, X, 2, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'CHERFS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CHERFS( 'U', 2, 1, A, 2, AF, 1, IP, B, 2, X, 2, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'CHERFS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL CHERFS( 'U', 2, 1, A, 2, AF, 2, IP, B, 1, X, 2, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'CHERFS', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL CHERFS( 'U', 2, 1, A, 2, AF, 2, IP, B, 2, X, 1, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'CHERFS', INFOT, NOUT, LERR, OK )
*
*        CHECON
*
         SRNAMT = 'CHECON'
         INFOT = 1
         CALL CHECON( '/', 0, A, 1, IP, ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CHECON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHECON( 'U', -1, A, 1, IP, ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CHECON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CHECON( 'U', 2, A, 1, IP, ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CHECON', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CHECON( 'U', 1, A, 1, IP, -ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CHECON', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'HR' ) ) THEN
*
*        Test error exits of the routines that use factorization
*        of a Hermitian indefinite matrix with rook
*        (bounded Bunch-Kaufman) diagonal pivoting method.
*
*        CHETRF_ROOK
*
         SRNAMT = 'CHETRF_ROOK'
         INFOT = 1
         CALL CHETRF_ROOK( '/', 0, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'CHETRF_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHETRF_ROOK( 'U', -1, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'CHETRF_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CHETRF_ROOK( 'U', 2, A, 1, IP, W, 4, INFO )
         CALL CHKXER( 'CHETRF_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CHETRF_ROOK( 'U', 0, A, 1, IP, W, 0, INFO )
         CALL CHKXER( 'CHETRF_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CHETRF_ROOK( 'U', 0, A, 1, IP, W, -2, INFO )
         CALL CHKXER( 'CHETRF_ROOK', INFOT, NOUT, LERR, OK )
*
*        CHETF2_ROOK
*
         SRNAMT = 'CHETF2_ROOK'
         INFOT = 1
         CALL CHETF2_ROOK( '/', 0, A, 1, IP, INFO )
         CALL CHKXER( 'CHETF2_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHETF2_ROOK( 'U', -1, A, 1, IP, INFO )
         CALL CHKXER( 'CHETF2_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CHETF2_ROOK( 'U', 2, A, 1, IP, INFO )
         CALL CHKXER( 'CHETF2_ROOK', INFOT, NOUT, LERR, OK )
*
*        CHETRI_ROOK
*
         SRNAMT = 'CHETRI_ROOK'
         INFOT = 1
         CALL CHETRI_ROOK( '/', 0, A, 1, IP, W, INFO )
         CALL CHKXER( 'CHETRI_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHETRI_ROOK( 'U', -1, A, 1, IP, W, INFO )
         CALL CHKXER( 'CHETRI_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CHETRI_ROOK( 'U', 2, A, 1, IP, W, INFO )
         CALL CHKXER( 'CHETRI_ROOK', INFOT, NOUT, LERR, OK )
*
*        CHETRS_ROOK
*
         SRNAMT = 'CHETRS_ROOK'
         INFOT = 1
         CALL CHETRS_ROOK( '/', 0, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'CHETRS_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHETRS_ROOK( 'U', -1, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'CHETRS_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CHETRS_ROOK( 'U', 0, -1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'CHETRS_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CHETRS_ROOK( 'U', 2, 1, A, 1, IP, B, 2, INFO )
         CALL CHKXER( 'CHETRS_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CHETRS_ROOK( 'U', 2, 1, A, 2, IP, B, 1, INFO )
         CALL CHKXER( 'CHETRS_ROOK', INFOT, NOUT, LERR, OK )
*
*        CHECON_ROOK
*
         SRNAMT = 'CHECON_ROOK'
         INFOT = 1
         CALL CHECON_ROOK( '/', 0, A, 1, IP, ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CHECON_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHECON_ROOK( 'U', -1, A, 1, IP, ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CHECON_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CHECON_ROOK( 'U', 2, A, 1, IP, ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CHECON_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CHECON_ROOK( 'U', 1, A, 1, IP, -ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CHECON_ROOK', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'HK' ) ) THEN
*
*        Test error exits of the routines that use factorization
*        of a Hermitian indefinite matrix with rook
*        (bounded Bunch-Kaufman) pivoting with the new storage
*        format for factors L ( or U) and D.
*
*        L (or U) is stored in A, diagonal of D is stored on the
*        diagonal of A, subdiagonal of D is stored in a separate array E.
*
*        CHETRF_RK
*
         SRNAMT = 'CHETRF_RK'
         INFOT = 1
         CALL CHETRF_RK( '/', 0, A, 1, E, IP, W, 1, INFO )
         CALL CHKXER( 'CHETRF_RK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHETRF_RK( 'U', -1, A, 1, E, IP, W, 1, INFO )
         CALL CHKXER( 'CHETRF_RK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CHETRF_RK( 'U', 2, A, 1, E, IP, W, 4, INFO )
         CALL CHKXER( 'CHETRF_RK', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CHETRF_RK( 'U', 0, A, 1, E, IP, W, 0, INFO )
         CALL CHKXER( 'CHETRF_RK', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CHETRF_RK( 'U', 0, A, 1, E, IP, W, -2, INFO )
         CALL CHKXER( 'CHETRF_RK', INFOT, NOUT, LERR, OK )
*
*        CHETF2_RK
*
         SRNAMT = 'CHETF2_RK'
         INFOT = 1
         CALL CHETF2_RK( '/', 0, A, 1, E, IP, INFO )
         CALL CHKXER( 'CHETF2_RK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHETF2_RK( 'U', -1, A, 1, E, IP, INFO )
         CALL CHKXER( 'CHETF2_RK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CHETF2_RK( 'U', 2, A, 1, E, IP, INFO )
         CALL CHKXER( 'CHETF2_RK', INFOT, NOUT, LERR, OK )
*
*        CHETRI_3
*
         SRNAMT = 'CHETRI_3'
         INFOT = 1
         CALL CHETRI_3( '/', 0, A, 1, E, IP, W, 1, INFO )
         CALL CHKXER( 'CHETRI_3', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHETRI_3( 'U', -1, A, 1, E, IP, W, 1, INFO )
         CALL CHKXER( 'CHETRI_3', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CHETRI_3( 'U', 2, A, 1, E, IP, W, 1, INFO )
         CALL CHKXER( 'CHETRI_3', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CHETRI_3( 'U', 0, A, 1, E, IP, W, 0, INFO )
         CALL CHKXER( 'CHETRI_3', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CHETRI_3( 'U', 0, A, 1, E, IP, W, -2, INFO )
         CALL CHKXER( 'CHETRI_3', INFOT, NOUT, LERR, OK )
*
*        CHETRI_3X
*
         SRNAMT = 'CHETRI_3X'
         INFOT = 1
         CALL CHETRI_3X( '/', 0, A, 1, E, IP, W, 1, INFO )
         CALL CHKXER( 'CHETRI_3X', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHETRI_3X( 'U', -1, A, 1, E, IP, W, 1, INFO )
         CALL CHKXER( 'CHETRI_3X', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CHETRI_3X( 'U', 2, A, 1, E, IP, W, 1, INFO )
         CALL CHKXER( 'CHETRI_3X', INFOT, NOUT, LERR, OK )
*
*        CHETRS_3
*
         SRNAMT = 'CHETRS_3'
         INFOT = 1
         CALL CHETRS_3( '/', 0, 0, A, 1, E, IP, B, 1, INFO )
         CALL CHKXER( 'CHETRS_3', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHETRS_3( 'U', -1, 0, A, 1, E, IP, B, 1, INFO )
         CALL CHKXER( 'CHETRS_3', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CHETRS_3( 'U', 0, -1, A, 1, E, IP, B, 1, INFO )
         CALL CHKXER( 'CHETRS_3', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CHETRS_3( 'U', 2, 1, A, 1, E, IP, B, 2, INFO )
         CALL CHKXER( 'CHETRS_3', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL CHETRS_3( 'U', 2, 1, A, 2, E, IP, B, 1, INFO )
         CALL CHKXER( 'CHETRS_3', INFOT, NOUT, LERR, OK )
*
*        CHECON_3
*
         SRNAMT = 'CHECON_3'
         INFOT = 1
         CALL CHECON_3( '/', 0, A, 1,  E, IP, ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CHECON_3', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHECON_3( 'U', -1, A, 1, E, IP, ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CHECON_3', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CHECON_3( 'U', 2, A, 1, E, IP, ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CHECON_3', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CHECON_3( 'U', 1, A, 1, E, IP, -1.0E0, RCOND, W, INFO)
         CALL CHKXER( 'CHECON_3', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'HA' ) ) THEN
*
*        Test error exits of the routines that use factorization
*        of a Hermitian indefinite matrix with Aasen's algorithm.
*
*        CHETRF_AA
*
         SRNAMT = 'CHETRF_AA'
         INFOT = 1
         CALL CHETRF_AA( '/', 0, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'CHETRF_AA', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHETRF_AA( 'U', -1, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'CHETRF_AA', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CHETRF_AA( 'U', 2, A, 1, IP, W, 4, INFO )
         CALL CHKXER( 'CHETRF_AA', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CHETRF_AA( 'U', 2, A, 2, IP, W, 0, INFO )
         CALL CHKXER( 'CHETRF_AA', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CHETRF_AA( 'U', 2, A, 2, IP, W, -2, INFO )
         CALL CHKXER( 'CHETRF_AA', INFOT, NOUT, LERR, OK )
*
*        CHETRS_AA
*
         SRNAMT = 'CHETRS_AA'
         INFOT = 1
         CALL CHETRS_AA( '/', 0, 0, A, 1, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'CHETRS_AA', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHETRS_AA( 'U', -1, 0, A, 1, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'CHETRS_AA', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CHETRS_AA( 'U', 0, -1, A, 1, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'CHETRS_AA', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CHETRS_AA( 'U', 2, 1, A, 1, IP, B, 2, W, 1, INFO )
         CALL CHKXER( 'CHETRS_AA', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CHETRS_AA( 'U', 2, 1, A, 2, IP, B, 1, W, 1, INFO )
         CALL CHKXER( 'CHETRS_AA', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL CHETRS_AA( 'U', 2, 1, A, 2, IP, B, 2, W, 0, INFO )
         CALL CHKXER( 'CHETRS_AA', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL CHETRS_AA( 'U', 2, 1, A, 2, IP, B, 2, W, -2, INFO )
         CALL CHKXER( 'CHETRS_AA', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'H2' ) ) THEN
*
*        Test error exits of the routines that use factorization
*        of a symmetric indefinite matrix with Aasen's algorithm.
*
*        CHETRF_AA_2STAGE
*
         SRNAMT = 'CHETRF_AA_2STAGE'
         INFOT = 1
         CALL CHETRF_AA_2STAGE( '/', 0, A, 1, A, 1, IP, IP, W, 1,
     $                          INFO )
         CALL CHKXER( 'CHETRF_AA_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHETRF_AA_2STAGE( 'U', -1, A, 1, A, 1, IP, IP, W, 1,
     $                           INFO )
         CALL CHKXER( 'CHETRF_AA_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CHETRF_AA_2STAGE( 'U', 2, A, 1, A, 2, IP, IP, W, 1,
     $                           INFO )
         CALL CHKXER( 'CHETRF_AA_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CHETRF_AA_2STAGE( 'U', 2, A, 2, A, 1, IP, IP, W, 1,
     $                           INFO )
         CALL CHKXER( 'CHETRF_AA_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL CHETRF_AA_2STAGE( 'U', 2, A, 2, A, 8, IP, IP, W, 0,
     $                           INFO )
         CALL CHKXER( 'CHETRF_AA_2STAGE', INFOT, NOUT, LERR, OK )
*
*        CHETRS_AA_2STAGE
*
         SRNAMT = 'CHETRS_AA_2STAGE'
         INFOT = 1
         CALL CHETRS_AA_2STAGE( '/', 0, 0, A, 1, A, 1, IP, IP,
     $                          B, 1, INFO )
         CALL CHKXER( 'CHETRS_AA_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHETRS_AA_2STAGE( 'U', -1, 0, A, 1, A, 1, IP, IP,
     $                          B, 1, INFO )
         CALL CHKXER( 'CHETRS_AA_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CHETRS_AA_2STAGE( 'U', 0, -1, A, 1, A, 1, IP, IP,
     $                          B, 1, INFO )
         CALL CHKXER( 'CHETRS_AA_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CHETRS_AA_2STAGE( 'U', 2, 1, A, 1, A, 1, IP, IP,
     $                          B, 1, INFO )
         CALL CHKXER( 'CHETRS_AA_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CHETRS_AA_2STAGE( 'U', 2, 1, A, 2, A, 1, IP, IP,
     $                          B, 1, INFO )
         CALL CHKXER( 'CHETRS_AA_2STAGE', INFOT, NOUT, LERR, OK )
         INFOT = 11
         CALL CHETRS_AA_2STAGE( 'U', 2, 1, A, 2, A, 8, IP, IP,
     $                          B, 1, INFO )
         CALL CHKXER( 'CHETRS_AA_STAGE', INFOT, NOUT, LERR, OK )
*
*        Test error exits of the routines that use factorization
*        of a Hermitian indefinite packed matrix with patrial
*        (Bunch-Kaufman) diagonal pivoting method.
*
      ELSE IF( LSAMEN( 2, C2, 'HP' ) ) THEN
*
*        CHPTRF
*
         SRNAMT = 'CHPTRF'
         INFOT = 1
         CALL CHPTRF( '/', 0, A, IP, INFO )
         CALL CHKXER( 'CHPTRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHPTRF( 'U', -1, A, IP, INFO )
         CALL CHKXER( 'CHPTRF', INFOT, NOUT, LERR, OK )
*
*        CHPTRI
*
         SRNAMT = 'CHPTRI'
         INFOT = 1
         CALL CHPTRI( '/', 0, A, IP, W, INFO )
         CALL CHKXER( 'CHPTRI', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHPTRI( 'U', -1, A, IP, W, INFO )
         CALL CHKXER( 'CHPTRI', INFOT, NOUT, LERR, OK )
*
*        CHPTRS
*
         SRNAMT = 'CHPTRS'
         INFOT = 1
         CALL CHPTRS( '/', 0, 0, A, IP, B, 1, INFO )
         CALL CHKXER( 'CHPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHPTRS( 'U', -1, 0, A, IP, B, 1, INFO )
         CALL CHKXER( 'CHPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CHPTRS( 'U', 0, -1, A, IP, B, 1, INFO )
         CALL CHKXER( 'CHPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CHPTRS( 'U', 2, 1, A, IP, B, 1, INFO )
         CALL CHKXER( 'CHPTRS', INFOT, NOUT, LERR, OK )
*
*        CHPRFS
*
         SRNAMT = 'CHPRFS'
         INFOT = 1
         CALL CHPRFS( '/', 0, 0, A, AF, IP, B, 1, X, 1, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'CHPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHPRFS( 'U', -1, 0, A, AF, IP, B, 1, X, 1, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'CHPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CHPRFS( 'U', 0, -1, A, AF, IP, B, 1, X, 1, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'CHPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CHPRFS( 'U', 2, 1, A, AF, IP, B, 1, X, 2, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'CHPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL CHPRFS( 'U', 2, 1, A, AF, IP, B, 2, X, 1, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'CHPRFS', INFOT, NOUT, LERR, OK )
*
*        CHPCON
*
         SRNAMT = 'CHPCON'
         INFOT = 1
         CALL CHPCON( '/', 0, A, IP, ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CHPCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CHPCON( 'U', -1, A, IP, ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CHPCON', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CHPCON( 'U', 1, A, IP, -ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CHPCON', INFOT, NOUT, LERR, OK )
      END IF
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of CERRHE
*
      END
