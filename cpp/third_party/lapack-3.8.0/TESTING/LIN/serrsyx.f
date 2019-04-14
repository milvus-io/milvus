*> \brief \b SERRSYX
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SERRSY( PATH, NUNIT )
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
*> SERRSY tests the error exits for the REAL routines
*> for symmetric indefinite matrices.
*>
*> Note that this file is used only when the XBLAS are available,
*> otherwise serrsy.f defines this subroutine.
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
*> \ingroup single_lin
*
*  =====================================================================
      SUBROUTINE SERRSY( PATH, NUNIT )
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
*     .. Parameters ..
      INTEGER            NMAX
      PARAMETER          ( NMAX = 4 )
*     ..
*     .. Local Scalars ..
      CHARACTER          EQ
      CHARACTER*2        C2
      INTEGER            I, INFO, J, N_ERR_BNDS, NPARAMS
      REAL               ANRM, RCOND, BERR
*     ..
*     .. Local Arrays ..
      INTEGER            IP( NMAX ), IW( NMAX )
      REAL               A( NMAX, NMAX ), AF( NMAX, NMAX ), B( NMAX ),
     $                   E( NMAX ), R1( NMAX ), R2( NMAX ), W( 3*NMAX ),
     $                   X( NMAX ), S( NMAX ), ERR_BNDS_N( NMAX, 3 ),
     $                   ERR_BNDS_C( NMAX, 3 ), PARAMS( 1 )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, SSPCON, SSPRFS, SSPTRF, SSPTRI,
     $                   SSPTRS, SSYCON, SSYCON_3, SSYCON_ROOK, SSYRFS,
     $                   SSYTF2, SSYTF2_RK, SSYTF2_ROOK, SSYTRF,
     $                   SSYTRF_RK, SSYTRF_ROOK, SSYTRI, SSYTRI_3,
     $                   SSYTRI_3X, SSYTRI_ROOK, SSYTRI2, SSYTRI2X,
     $                   SSYTRS, SSYTRS_3, SSYTRS_ROOK, SSYRFSX
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
      INTRINSIC          REAL
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
            A( I, J ) = 1. / REAL( I+J )
            AF( I, J ) = 1. / REAL( I+J )
   10    CONTINUE
         B( J ) = 0.E+0
         E( J ) = 0.E+0
         R1( J ) = 0.E+0
         R2( J ) = 0.E+0
         W( J ) = 0.E+0
         X( J ) = 0.E+0
         IP( J ) = J
         IW( J ) = J
   20 CONTINUE
      ANRM = 1.0
      RCOND = 1.0
      OK = .TRUE.
*
      IF( LSAMEN( 2, C2, 'SY' ) ) THEN
*
*        Test error exits of the routines that use factorization
*        of a symmetric indefinite matrix with patrial
*        (Bunch-Kaufman) pivoting.
*
*        SSYTRF
*
         SRNAMT = 'SSYTRF'
         INFOT = 1
         CALL SSYTRF( '/', 0, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'SSYTRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYTRF( 'U', -1, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'SSYTRF', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SSYTRF( 'U', 2, A, 1, IP, W, 4, INFO )
         CALL CHKXER( 'SSYTRF', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SSYTRF( 'U', 0, A, 1, IP, W, 0, INFO )
         CALL CHKXER( 'SSYTRF', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SSYTRF( 'U', 0, A, 1, IP, W, -2, INFO )
         CALL CHKXER( 'SSYTRF', INFOT, NOUT, LERR, OK )
*
*        SSYTF2
*
         SRNAMT = 'SSYTF2'
         INFOT = 1
         CALL SSYTF2( '/', 0, A, 1, IP, INFO )
         CALL CHKXER( 'SSYTF2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYTF2( 'U', -1, A, 1, IP, INFO )
         CALL CHKXER( 'SSYTF2', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SSYTF2( 'U', 2, A, 1, IP, INFO )
         CALL CHKXER( 'SSYTF2', INFOT, NOUT, LERR, OK )
*
*        SSYTRI
*
         SRNAMT = 'SSYTRI'
         INFOT = 1
         CALL SSYTRI( '/', 0, A, 1, IP, W, INFO )
         CALL CHKXER( 'SSYTRI', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYTRI( 'U', -1, A, 1, IP, W, INFO )
         CALL CHKXER( 'SSYTRI', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SSYTRI( 'U', 2, A, 1, IP, W, INFO )
         CALL CHKXER( 'SSYTRI', INFOT, NOUT, LERR, OK )
*
*        SSYTRI2
*
         SRNAMT = 'SSYTRI2'
         INFOT = 1
         CALL SSYTRI2( '/', 0, A, 1, IP, W, IW, INFO )
         CALL CHKXER( 'SSYTRI', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYTRI2( 'U', -1, A, 1, IP, W, IW, INFO )
         CALL CHKXER( 'SSYTRI', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SSYTRI2( 'U', 2, A, 1, IP, W, IW, INFO )
         CALL CHKXER( 'SSYTRI', INFOT, NOUT, LERR, OK )
*
*        SSYTRI2X
*
         SRNAMT = 'SSYTRI2X'
         INFOT = 1
         CALL SSYTRI2X( '/', 0, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'SSYTRI2X', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYTRI2X( 'U', -1, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'SSYTRI2X', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SSYTRI2X( 'U', 2, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'SSYTRI2X', INFOT, NOUT, LERR, OK )
*
*        SSYTRS
*
         SRNAMT = 'SSYTRS'
         INFOT = 1
         CALL SSYTRS( '/', 0, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'SSYTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYTRS( 'U', -1, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'SSYTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SSYTRS( 'U', 0, -1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'SSYTRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SSYTRS( 'U', 2, 1, A, 1, IP, B, 2, INFO )
         CALL CHKXER( 'SSYTRS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SSYTRS( 'U', 2, 1, A, 2, IP, B, 1, INFO )
         CALL CHKXER( 'SSYTRS', INFOT, NOUT, LERR, OK )
*
*        SSYRFS
*
         SRNAMT = 'SSYRFS'
         INFOT = 1
         CALL SSYRFS( '/', 0, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'SSYRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYRFS( 'U', -1, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1, R2,
     $                W, IW, INFO )
         CALL CHKXER( 'SSYRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SSYRFS( 'U', 0, -1, A, 1, AF, 1, IP, B, 1, X, 1, R1, R2,
     $                W, IW, INFO )
         CALL CHKXER( 'SSYRFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SSYRFS( 'U', 2, 1, A, 1, AF, 2, IP, B, 2, X, 2, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'SSYRFS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SSYRFS( 'U', 2, 1, A, 2, AF, 1, IP, B, 2, X, 2, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'SSYRFS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL SSYRFS( 'U', 2, 1, A, 2, AF, 2, IP, B, 1, X, 2, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'SSYRFS', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL SSYRFS( 'U', 2, 1, A, 2, AF, 2, IP, B, 2, X, 1, R1, R2, W,
     $                IW, INFO )
         CALL CHKXER( 'SSYRFS', INFOT, NOUT, LERR, OK )
*
*        SSYRFSX
*
         N_ERR_BNDS = 3
         NPARAMS = 0
         SRNAMT = 'SSYRFSX'
         INFOT = 1
         CALL SSYRFSX( '/', EQ, 0, 0, A, 1, AF, 1, IP, S, B, 1, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, IW, INFO )
         CALL CHKXER( 'SSYRFSX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYRFSX( 'U', EQ, -1, 0, A, 1, AF, 1, IP, S, B, 1, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, IW, INFO )
         CALL CHKXER( 'SSYRFSX', INFOT, NOUT, LERR, OK )
         EQ = 'N'
         INFOT = 3
         CALL SSYRFSX( 'U', EQ, -1, 0, A, 1, AF, 1, IP, S, B, 1, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, IW, INFO )
         CALL CHKXER( 'SSYRFSX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SSYRFSX( 'U', EQ, 0, -1, A, 1, AF, 1, IP, S, B, 1, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, IW, INFO )
         CALL CHKXER( 'SSYRFSX', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SSYRFSX( 'U', EQ, 2, 1, A, 1, AF, 2, IP, S, B, 2, X, 2,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, IW, INFO )
         CALL CHKXER( 'SSYRFSX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SSYRFSX( 'U', EQ, 2, 1, A, 2, AF, 1, IP, S, B, 2, X, 2,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, IW, INFO )
         CALL CHKXER( 'SSYRFSX', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL SSYRFSX( 'U', EQ, 2, 1, A, 2, AF, 2, IP, S, B, 1, X, 2,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, IW, INFO )
         CALL CHKXER( 'SSYRFSX', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL SSYRFSX( 'U', EQ, 2, 1, A, 2, AF, 2, IP, S, B, 2, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, IW, INFO )
         CALL CHKXER( 'SSYRFSX', INFOT, NOUT, LERR, OK )
*
*        SSYCON
*
         SRNAMT = 'SSYCON'
         INFOT = 1
         CALL SSYCON( '/', 0, A, 1, IP, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'SSYCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYCON( 'U', -1, A, 1, IP, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'SSYCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SSYCON( 'U', 2, A, 1, IP, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'SSYCON', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SSYCON( 'U', 1, A, 1, IP, -1.0, RCOND, W, IW, INFO )
         CALL CHKXER( 'SSYCON', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'SR' ) ) THEN
*
*        Test error exits of the routines that use factorization
*        of a symmetric indefinite matrix with rook
*        (bounded Bunch-Kaufman) pivoting.
*
*        SSYTRF_ROOK
*
         SRNAMT = 'SSYTRF_ROOK'
         INFOT = 1
         CALL SSYTRF_ROOK( '/', 0, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'SSYTRF_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYTRF_ROOK( 'U', -1, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'SSYTRF_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SSYTRF_ROOK( 'U', 2, A, 1, IP, W, 4, INFO )
         CALL CHKXER( 'SSYTRF_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SSYTRF_ROOK( 'U', 0, A, 1, IP, W, 0, INFO )
         CALL CHKXER( 'SSYTRF_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SSYTRF_ROOK( 'U', 0, A, 1, IP, W, -2, INFO )
         CALL CHKXER( 'SSYTRF_ROOK', INFOT, NOUT, LERR, OK )
*
*        SSYTF2_ROOK
*
         SRNAMT = 'SSYTF2_ROOK'
         INFOT = 1
         CALL SSYTF2_ROOK( '/', 0, A, 1, IP, INFO )
         CALL CHKXER( 'SSYTF2_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYTF2_ROOK( 'U', -1, A, 1, IP, INFO )
         CALL CHKXER( 'SSYTF2_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SSYTF2_ROOK( 'U', 2, A, 1, IP, INFO )
         CALL CHKXER( 'SSYTF2_ROOK', INFOT, NOUT, LERR, OK )
*
*        SSYTRI_ROOK
*
         SRNAMT = 'SSYTRI_ROOK'
         INFOT = 1
         CALL SSYTRI_ROOK( '/', 0, A, 1, IP, W, INFO )
         CALL CHKXER( 'SSYTRI_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYTRI_ROOK( 'U', -1, A, 1, IP, W, INFO )
         CALL CHKXER( 'SSYTRI_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SSYTRI_ROOK( 'U', 2, A, 1, IP, W, INFO )
         CALL CHKXER( 'SSYTRI_ROOK', INFOT, NOUT, LERR, OK )
*
*        SSYTRS_ROOK
*
         SRNAMT = 'SSYTRS_ROOK'
         INFOT = 1
         CALL SSYTRS_ROOK( '/', 0, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'SSYTRS_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYTRS_ROOK( 'U', -1, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'SSYTRS_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SSYTRS_ROOK( 'U', 0, -1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'SSYTRS_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SSYTRS_ROOK( 'U', 2, 1, A, 1, IP, B, 2, INFO )
         CALL CHKXER( 'SSYTRS_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SSYTRS_ROOK( 'U', 2, 1, A, 2, IP, B, 1, INFO )
         CALL CHKXER( 'SSYTRS_ROOK', INFOT, NOUT, LERR, OK )
*
*        SSYCON_ROOK
*
         SRNAMT = 'SSYCON_ROOK'
         INFOT = 1
         CALL SSYCON_ROOK( '/', 0, A, 1, IP, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'SSYCON_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYCON_ROOK( 'U', -1, A, 1, IP, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'SSYCON_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SSYCON_ROOK( 'U', 2, A, 1, IP, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'SSYCON_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL SSYCON_ROOK( 'U', 1, A, 1, IP, -1.0, RCOND, W, IW, INFO )
         CALL CHKXER( 'SSYCON_ROOK', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'SK' ) ) THEN
*
*        Test error exits of the routines that use factorization
*        of a symmetric indefinite matrix with rook
*        (bounded Bunch-Kaufman) pivoting with the new storage
*        format for factors L ( or U) and D.
*
*        L (or U) is stored in A, diagonal of D is stored on the
*        diagonal of A, subdiagonal of D is stored in a separate array E.
*
*        SSYTRF_RK
*
         SRNAMT = 'SSYTRF_RK'
         INFOT = 1
         CALL SSYTRF_RK( '/', 0, A, 1, E, IP, W, 1, INFO )
         CALL CHKXER( 'SSYTRF_RK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYTRF_RK( 'U', -1, A, 1, E, IP, W, 1, INFO )
         CALL CHKXER( 'SSYTRF_RK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SSYTRF_RK( 'U', 2, A, 1, E, IP, W, 4, INFO )
         CALL CHKXER( 'SSYTRF_RK', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SSYTRF_RK( 'U', 0, A, 1, E, IP, W, 0, INFO )
         CALL CHKXER( 'SSYTRF_RK', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SSYTRF_RK( 'U', 0, A, 1, E, IP, W, -2, INFO )
         CALL CHKXER( 'SSYTRF_RK', INFOT, NOUT, LERR, OK )
*
*        SSYTF2_RK
*
         SRNAMT = 'SSYTF2_RK'
         INFOT = 1
         CALL SSYTF2_RK( '/', 0, A, 1, E, IP, INFO )
         CALL CHKXER( 'SSYTF2_RK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYTF2_RK( 'U', -1, A, 1, E, IP, INFO )
         CALL CHKXER( 'SSYTF2_RK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SSYTF2_RK( 'U', 2, A, 1, E, IP, INFO )
         CALL CHKXER( 'SSYTF2_RK', INFOT, NOUT, LERR, OK )
*
*        SSYTRI_3
*
         SRNAMT = 'SSYTRI_3'
         INFOT = 1
         CALL SSYTRI_3( '/', 0, A, 1, E, IP, W, 1, INFO )
         CALL CHKXER( 'SSYTRI_3', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYTRI_3( 'U', -1, A, 1, E, IP, W, 1, INFO )
         CALL CHKXER( 'SSYTRI_3', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SSYTRI_3( 'U', 2, A, 1, E, IP, W, 1, INFO )
         CALL CHKXER( 'SSYTRI_3', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SSYTRI_3( 'U', 0, A, 1, E, IP, W, 0, INFO )
         CALL CHKXER( 'SSYTRI_3', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SSYTRI_3( 'U', 0, A, 1, E, IP, W, -2, INFO )
         CALL CHKXER( 'SSYTRI_3', INFOT, NOUT, LERR, OK )
*
*        SSYTRI_3X
*
         SRNAMT = 'SSYTRI_3X'
         INFOT = 1
         CALL SSYTRI_3X( '/', 0, A, 1, E, IP, W, 1, INFO )
         CALL CHKXER( 'SSYTRI_3X', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYTRI_3X( 'U', -1, A, 1, E, IP, W, 1, INFO )
         CALL CHKXER( 'SSYTRI_3X', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SSYTRI_3X( 'U', 2, A, 1, E, IP, W, 1, INFO )
         CALL CHKXER( 'SSYTRI_3X', INFOT, NOUT, LERR, OK )
*
*        SSYTRS_3
*
         SRNAMT = 'SSYTRS_3'
         INFOT = 1
         CALL SSYTRS_3( '/', 0, 0, A, 1, E, IP, B, 1, INFO )
         CALL CHKXER( 'SSYTRS_3', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYTRS_3( 'U', -1, 0, A, 1, E, IP, B, 1, INFO )
         CALL CHKXER( 'SSYTRS_3', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SSYTRS_3( 'U', 0, -1, A, 1, E, IP, B, 1, INFO )
         CALL CHKXER( 'SSYTRS_3', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SSYTRS_3( 'U', 2, 1, A, 1, E, IP, B, 2, INFO )
         CALL CHKXER( 'SSYTRS_3', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL SSYTRS_3( 'U', 2, 1, A, 2, E, IP, B, 1, INFO )
         CALL CHKXER( 'SSYTRS_3', INFOT, NOUT, LERR, OK )
*
*        SSYCON_3
*
         SRNAMT = 'SSYCON_3'
         INFOT = 1
         CALL SSYCON_3( '/', 0, A, 1,  E, IP, ANRM, RCOND, W, IW,
     $                   INFO )
         CALL CHKXER( 'SSYCON_3', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSYCON_3( 'U', -1, A, 1, E, IP, ANRM, RCOND, W, IW,
     $                   INFO )
         CALL CHKXER( 'SSYCON_3', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL SSYCON_3( 'U', 2, A, 1, E, IP, ANRM, RCOND, W, IW,
     $                   INFO )
         CALL CHKXER( 'SSYCON_3', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SSYCON_3( 'U', 1, A, 1, E, IP, -1.0E0, RCOND, W, IW,
     $                   INFO)
         CALL CHKXER( 'SSYCON_3', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'SP' ) ) THEN
*
*        Test error exits of the routines that use factorization
*        of a symmetric indefinite packed matrix with patrial
*        (Bunch-Kaufman) pivoting.
*
*        SSPTRF
*
         SRNAMT = 'SSPTRF'
         INFOT = 1
         CALL SSPTRF( '/', 0, A, IP, INFO )
         CALL CHKXER( 'SSPTRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSPTRF( 'U', -1, A, IP, INFO )
         CALL CHKXER( 'SSPTRF', INFOT, NOUT, LERR, OK )
*
*        SSPTRI
*
         SRNAMT = 'SSPTRI'
         INFOT = 1
         CALL SSPTRI( '/', 0, A, IP, W, INFO )
         CALL CHKXER( 'SSPTRI', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSPTRI( 'U', -1, A, IP, W, INFO )
         CALL CHKXER( 'SSPTRI', INFOT, NOUT, LERR, OK )
*
*        SSPTRS
*
         SRNAMT = 'SSPTRS'
         INFOT = 1
         CALL SSPTRS( '/', 0, 0, A, IP, B, 1, INFO )
         CALL CHKXER( 'SSPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSPTRS( 'U', -1, 0, A, IP, B, 1, INFO )
         CALL CHKXER( 'SSPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SSPTRS( 'U', 0, -1, A, IP, B, 1, INFO )
         CALL CHKXER( 'SSPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL SSPTRS( 'U', 2, 1, A, IP, B, 1, INFO )
         CALL CHKXER( 'SSPTRS', INFOT, NOUT, LERR, OK )
*
*        SSPRFS
*
         SRNAMT = 'SSPRFS'
         INFOT = 1
         CALL SSPRFS( '/', 0, 0, A, AF, IP, B, 1, X, 1, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'SSPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSPRFS( 'U', -1, 0, A, AF, IP, B, 1, X, 1, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'SSPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL SSPRFS( 'U', 0, -1, A, AF, IP, B, 1, X, 1, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'SSPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL SSPRFS( 'U', 2, 1, A, AF, IP, B, 1, X, 2, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'SSPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL SSPRFS( 'U', 2, 1, A, AF, IP, B, 2, X, 1, R1, R2, W, IW,
     $                INFO )
         CALL CHKXER( 'SSPRFS', INFOT, NOUT, LERR, OK )
*
*        SSPCON
*
         SRNAMT = 'SSPCON'
         INFOT = 1
         CALL SSPCON( '/', 0, A, IP, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'SSPCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL SSPCON( 'U', -1, A, IP, ANRM, RCOND, W, IW, INFO )
         CALL CHKXER( 'SSPCON', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL SSPCON( 'U', 1, A, IP, -1.0, RCOND, W, IW, INFO )
         CALL CHKXER( 'SSPCON', INFOT, NOUT, LERR, OK )
      END IF
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of SERRSY
*
      END
