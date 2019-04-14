*> \brief \b CERRSYX
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CERRSY( PATH, NUNIT )
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
*> CERRSY tests the error exits for the COMPLEX routines
*> for symmetric indefinite matrices.
*>
*> Note that this file is used only when the XBLAS are available,
*> otherwise cerrsy.f defines this subroutine.
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
*> \ingroup complex_lin
*
*  =====================================================================
      SUBROUTINE CERRSY( PATH, NUNIT )
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
      INTEGER            IP( NMAX )
      REAL               R( NMAX ), R1( NMAX ), R2( NMAX ),
     $                   S( NMAX ), ERR_BNDS_N( NMAX, 3 ),
     $                   ERR_BNDS_C( NMAX, 3 ), PARAMS( 1 )
      COMPLEX            A( NMAX, NMAX ), AF( NMAX, NMAX ), B( NMAX ),
     $                   E( NMAX), W( 2*NMAX ), X( NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, CSPCON, CSPRFS, CSPTRF, CSPTRI,
     $                   CSPTRS, CSYCON, CSYRFS, CSYTF2, CSYTRF, CSYTRI,
     $                   CSYTRI2, CSYTRS, CSYRFSX, CSYCON_ROOK,
     $                   CSYTF2_ROOK, CSYTRF_ROOK, CSYTRI_ROOK,
     $                   CSYTRS_ROOK
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
         B( J ) = 0.E0
         E( J ) = 0.E0
         R1( J ) = 0.E0
         R2( J ) = 0.E0
         W( J ) = 0.E0
         X( J ) = 0.E0
         IP( J ) = J
   20 CONTINUE
      ANRM = 1.0
      OK = .TRUE.

      IF( LSAMEN( 2, C2, 'SY' ) ) THEN
*
*        Test error exits of the routines that use factorization
*        of a symmetric indefinite matrix with patrial
*        (Bunch-Kaufman) diagonal pivoting method.
*
*        CSYTRF
*
         SRNAMT = 'CSYTRF'
         INFOT = 1
         CALL CSYTRF( '/', 0, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'CSYTRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSYTRF( 'U', -1, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'CSYTRF', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CSYTRF( 'U', 2, A, 1, IP, W, 4, INFO )
         CALL CHKXER( 'CSYTRF', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CSYTRF( 'U', 0, A, 1, IP, W, 0, INFO )
         CALL CHKXER( 'CSYTRF', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CSYTRF( 'U', 0, A, 1, IP, W, -2, INFO )
         CALL CHKXER( 'CSYTRF', INFOT, NOUT, LERR, OK )
*
*        CSYTF2
*
         SRNAMT = 'CSYTF2'
         INFOT = 1
         CALL CSYTF2( '/', 0, A, 1, IP, INFO )
         CALL CHKXER( 'CSYTF2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSYTF2( 'U', -1, A, 1, IP, INFO )
         CALL CHKXER( 'CSYTF2', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CSYTF2( 'U', 2, A, 1, IP, INFO )
         CALL CHKXER( 'CSYTF2', INFOT, NOUT, LERR, OK )
*
*        CSYTRI
*
         SRNAMT = 'CSYTRI'
         INFOT = 1
         CALL CSYTRI( '/', 0, A, 1, IP, W, INFO )
         CALL CHKXER( 'CSYTRI', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSYTRI( 'U', -1, A, 1, IP, W, INFO )
         CALL CHKXER( 'CSYTRI', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CSYTRI( 'U', 2, A, 1, IP, W, INFO )
         CALL CHKXER( 'CSYTRI', INFOT, NOUT, LERR, OK )
*
*        CSYTRI2
*
         SRNAMT = 'CSYTRI2'
         INFOT = 1
         CALL CSYTRI2( '/', 0, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'CSYTRI2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSYTRI2( 'U', -1, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'CSYTRI2', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CSYTRI2( 'U', 2, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'CSYTRI2', INFOT, NOUT, LERR, OK )
*
*        CSYTRI2X
*
         SRNAMT = 'CSYTRI2X'
         INFOT = 1
         CALL CSYTRI2X( '/', 0, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'CSYTRI2X', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSYTRI2X( 'U', -1, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'CSYTRI2X', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CSYTRI2X( 'U', 2, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'CSYTRI2X', INFOT, NOUT, LERR, OK )
*
*        CSYTRS
*
         SRNAMT = 'CSYTRS'
         INFOT = 1
         CALL CSYTRS( '/', 0, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'CSYTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSYTRS( 'U', -1, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'CSYTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CSYTRS( 'U', 0, -1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'CSYTRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CSYTRS( 'U', 2, 1, A, 1, IP, B, 2, INFO )
         CALL CHKXER( 'CSYTRS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CSYTRS( 'U', 2, 1, A, 2, IP, B, 1, INFO )
         CALL CHKXER( 'CSYTRS', INFOT, NOUT, LERR, OK )
*
*        CSYRFS
*
         SRNAMT = 'CSYRFS'
         INFOT = 1
         CALL CSYRFS( '/', 0, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'CSYRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSYRFS( 'U', -1, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1, R2,
     $                W, R, INFO )
         CALL CHKXER( 'CSYRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CSYRFS( 'U', 0, -1, A, 1, AF, 1, IP, B, 1, X, 1, R1, R2,
     $                W, R, INFO )
         CALL CHKXER( 'CSYRFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CSYRFS( 'U', 2, 1, A, 1, AF, 2, IP, B, 2, X, 2, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'CSYRFS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CSYRFS( 'U', 2, 1, A, 2, AF, 1, IP, B, 2, X, 2, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'CSYRFS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL CSYRFS( 'U', 2, 1, A, 2, AF, 2, IP, B, 1, X, 2, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'CSYRFS', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL CSYRFS( 'U', 2, 1, A, 2, AF, 2, IP, B, 2, X, 1, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'CSYRFS', INFOT, NOUT, LERR, OK )
*
*        CSYRFSX
*
         N_ERR_BNDS = 3
         NPARAMS = 0
         SRNAMT = 'CSYRFSX'
         INFOT = 1
         CALL CSYRFSX( '/', EQ, 0, 0, A, 1, AF, 1, IP, S, B, 1, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, R, INFO )
         CALL CHKXER( 'CSYRFSX', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSYRFSX( 'U', EQ, -1, 0, A, 1, AF, 1, IP, S, B, 1, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, R, INFO )
         CALL CHKXER( 'CSYRFSX', INFOT, NOUT, LERR, OK )
         EQ = 'N'
         INFOT = 3
         CALL CSYRFSX( 'U', EQ, -1, 0, A, 1, AF, 1, IP, S, B, 1, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, R, INFO )
         CALL CHKXER( 'CSYRFSX', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CSYRFSX( 'U', EQ, 0, -1, A, 1, AF, 1, IP, S, B, 1, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, R, INFO )
         CALL CHKXER( 'CSYRFSX', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CSYRFSX( 'U', EQ, 2, 1, A, 1, AF, 2, IP, S, B, 2, X, 2,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, R, INFO )
         CALL CHKXER( 'CSYRFSX', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CSYRFSX( 'U', EQ, 2, 1, A, 2, AF, 1, IP, S, B, 2, X, 2,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, R, INFO )
         CALL CHKXER( 'CSYRFSX', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL CSYRFSX( 'U', EQ, 2, 1, A, 2, AF, 2, IP, S, B, 1, X, 2,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, R, INFO )
         CALL CHKXER( 'CSYRFSX', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL CSYRFSX( 'U', EQ, 2, 1, A, 2, AF, 2, IP, S, B, 2, X, 1,
     $        RCOND, BERR, N_ERR_BNDS, ERR_BNDS_N, ERR_BNDS_C, NPARAMS,
     $        PARAMS, W, R, INFO )
         CALL CHKXER( 'CSYRFSX', INFOT, NOUT, LERR, OK )
*
*        CSYCON
*
         SRNAMT = 'CSYCON'
         INFOT = 1
         CALL CSYCON( '/', 0, A, 1, IP, ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CSYCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSYCON( 'U', -1, A, 1, IP, ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CSYCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CSYCON( 'U', 2, A, 1, IP, ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CSYCON', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CSYCON( 'U', 1, A, 1, IP, -ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CSYCON', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'SR' ) ) THEN
*
*        Test error exits of the routines that use factorization
*        of a symmetric indefinite matrix with rook
*        (bounded Bunch-Kaufman) diagonal pivoting method.
*
*        CSYTRF_ROOK
*
         SRNAMT = 'CSYTRF_ROOK'
         INFOT = 1
         CALL CSYTRF_ROOK( '/', 0, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'CSYTRF_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSYTRF_ROOK( 'U', -1, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'CSYTRF_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CSYTRF_ROOK( 'U', 2, A, 1, IP, W, 4, INFO )
         CALL CHKXER( 'CSYTRF_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CSYTRF_ROOK( 'U', 0, A, 1, IP, W, 0, INFO )
         CALL CHKXER( 'CSYTRF_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CSYTRF_ROOK( 'U', 0, A, 1, IP, W, -2, INFO )
         CALL CHKXER( 'CSYTRF_ROOK', INFOT, NOUT, LERR, OK )
*
*        CSYTF2_ROOK
*
         SRNAMT = 'CSYTF2_ROOK'
         INFOT = 1
         CALL CSYTF2_ROOK( '/', 0, A, 1, IP, INFO )
         CALL CHKXER( 'CSYTF2_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSYTF2_ROOK( 'U', -1, A, 1, IP, INFO )
         CALL CHKXER( 'CSYTF2_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CSYTF2_ROOK( 'U', 2, A, 1, IP, INFO )
         CALL CHKXER( 'CSYTF2_ROOK', INFOT, NOUT, LERR, OK )
*
*        CSYTRI_ROOK
*
         SRNAMT = 'CSYTRI_ROOK'
         INFOT = 1
         CALL CSYTRI_ROOK( '/', 0, A, 1, IP, W, INFO )
         CALL CHKXER( 'CSYTRI_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSYTRI_ROOK( 'U', -1, A, 1, IP, W, INFO )
         CALL CHKXER( 'CSYTRI_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CSYTRI_ROOK( 'U', 2, A, 1, IP, W, INFO )
         CALL CHKXER( 'CSYTRI_ROOK', INFOT, NOUT, LERR, OK )
*
*        CSYTRS_ROOK
*
         SRNAMT = 'CSYTRS_ROOK'
         INFOT = 1
         CALL CSYTRS_ROOK( '/', 0, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'CSYTRS_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSYTRS_ROOK( 'U', -1, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'CSYTRS_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CSYTRS_ROOK( 'U', 0, -1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'CSYTRS_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CSYTRS_ROOK( 'U', 2, 1, A, 1, IP, B, 2, INFO )
         CALL CHKXER( 'CSYTRS_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CSYTRS_ROOK( 'U', 2, 1, A, 2, IP, B, 1, INFO )
         CALL CHKXER( 'CSYTRS_ROOK', INFOT, NOUT, LERR, OK )
*
*        CSYCON_ROOK
*
         SRNAMT = 'CSYCON_ROOK'
         INFOT = 1
         CALL CSYCON_ROOK( '/', 0, A, 1, IP, ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CSYCON_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSYCON_ROOK( 'U', -1, A, 1, IP, ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CSYCON_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CSYCON_ROOK( 'U', 2, A, 1, IP, ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CSYCON_ROOK', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL CSYCON_ROOK( 'U', 1, A, 1, IP, -ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CSYCON_ROOK', INFOT, NOUT, LERR, OK )
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
*        CSYTRF_RK
*
         SRNAMT = 'CSYTRF_RK'
         INFOT = 1
         CALL CSYTRF_RK( '/', 0, A, 1, E, IP, W, 1, INFO )
         CALL CHKXER( 'CSYTRF_RK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSYTRF_RK( 'U', -1, A, 1, E, IP, W, 1, INFO )
         CALL CHKXER( 'CSYTRF_RK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CSYTRF_RK( 'U', 2, A, 1, E, IP, W, 4, INFO )
         CALL CHKXER( 'CSYTRF_RK', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CSYTRF_RK( 'U', 0, A, 1, E, IP, W, 0, INFO )
         CALL CHKXER( 'CSYTRF_RK', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CSYTRF_RK( 'U', 0, A, 1, E, IP, W, -2, INFO )
         CALL CHKXER( 'CSYTRF_RK', INFOT, NOUT, LERR, OK )
*
*        CSYTF2_RK
*
         SRNAMT = 'CSYTF2_RK'
         INFOT = 1
         CALL CSYTF2_RK( '/', 0, A, 1, E, IP, INFO )
         CALL CHKXER( 'CSYTF2_RK', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSYTF2_RK( 'U', -1, A, 1, E, IP, INFO )
         CALL CHKXER( 'CSYTF2_RK', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CSYTF2_RK( 'U', 2, A, 1, E, IP, INFO )
         CALL CHKXER( 'CSYTF2_RK', INFOT, NOUT, LERR, OK )
*
*        CSYTRI_3
*
         SRNAMT = 'CSYTRI_3'
         INFOT = 1
         CALL CSYTRI_3( '/', 0, A, 1, E, IP, W, 1, INFO )
         CALL CHKXER( 'CSYTRI_3', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSYTRI_3( 'U', -1, A, 1, E, IP, W, 1, INFO )
         CALL CHKXER( 'CSYTRI_3', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CSYTRI_3( 'U', 2, A, 1, E, IP, W, 1, INFO )
         CALL CHKXER( 'CSYTRI_3', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CSYTRI_3( 'U', 0, A, 1, E, IP, W, 0, INFO )
         CALL CHKXER( 'CSYTRI_3', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CSYTRI_3( 'U', 0, A, 1, E, IP, W, -2, INFO )
         CALL CHKXER( 'CSYTRI_3', INFOT, NOUT, LERR, OK )
*
*        CSYTRI_3X
*
         SRNAMT = 'CSYTRI_3X'
         INFOT = 1
         CALL CSYTRI_3X( '/', 0, A, 1, E, IP, W, 1, INFO )
         CALL CHKXER( 'CSYTRI_3X', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSYTRI_3X( 'U', -1, A, 1, E, IP, W, 1, INFO )
         CALL CHKXER( 'CSYTRI_3X', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CSYTRI_3X( 'U', 2, A, 1, E, IP, W, 1, INFO )
         CALL CHKXER( 'CSYTRI_3X', INFOT, NOUT, LERR, OK )
*
*        CSYTRS_3
*
         SRNAMT = 'CSYTRS_3'
         INFOT = 1
         CALL CSYTRS_3( '/', 0, 0, A, 1, E, IP, B, 1, INFO )
         CALL CHKXER( 'CSYTRS_3', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSYTRS_3( 'U', -1, 0, A, 1, E, IP, B, 1, INFO )
         CALL CHKXER( 'CSYTRS_3', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CSYTRS_3( 'U', 0, -1, A, 1, E, IP, B, 1, INFO )
         CALL CHKXER( 'CSYTRS_3', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CSYTRS_3( 'U', 2, 1, A, 1, E, IP, B, 2, INFO )
         CALL CHKXER( 'CSYTRS_3', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL CSYTRS_3( 'U', 2, 1, A, 2, E, IP, B, 1, INFO )
         CALL CHKXER( 'CSYTRS_3', INFOT, NOUT, LERR, OK )
*
*        CSYCON_3
*
         SRNAMT = 'CSYCON_3'
         INFOT = 1
         CALL CSYCON_3( '/', 0, A, 1,  E, IP, ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CSYCON_3', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSYCON_3( 'U', -1, A, 1, E, IP, ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CSYCON_3', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL CSYCON_3( 'U', 2, A, 1, E, IP, ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CSYCON_3', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CSYCON_3( 'U', 1, A, 1, E, IP, -1.0E0, RCOND, W, INFO)
         CALL CHKXER( 'CSYCON_3', INFOT, NOUT, LERR, OK )
*
      ELSE IF( LSAMEN( 2, C2, 'SP' ) ) THEN
*
*        Test error exits of the routines that use factorization
*        of a symmetric indefinite packed matrix with patrial
*        (Bunch-Kaufman) diagonal pivoting method.
*
*        CSPTRF
*
         SRNAMT = 'CSPTRF'
         INFOT = 1
         CALL CSPTRF( '/', 0, A, IP, INFO )
         CALL CHKXER( 'CSPTRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSPTRF( 'U', -1, A, IP, INFO )
         CALL CHKXER( 'CSPTRF', INFOT, NOUT, LERR, OK )
*
*        CSPTRI
*
         SRNAMT = 'CSPTRI'
         INFOT = 1
         CALL CSPTRI( '/', 0, A, IP, W, INFO )
         CALL CHKXER( 'CSPTRI', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSPTRI( 'U', -1, A, IP, W, INFO )
         CALL CHKXER( 'CSPTRI', INFOT, NOUT, LERR, OK )
*
*        CSPTRS
*
         SRNAMT = 'CSPTRS'
         INFOT = 1
         CALL CSPTRS( '/', 0, 0, A, IP, B, 1, INFO )
         CALL CHKXER( 'CSPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSPTRS( 'U', -1, 0, A, IP, B, 1, INFO )
         CALL CHKXER( 'CSPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CSPTRS( 'U', 0, -1, A, IP, B, 1, INFO )
         CALL CHKXER( 'CSPTRS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL CSPTRS( 'U', 2, 1, A, IP, B, 1, INFO )
         CALL CHKXER( 'CSPTRS', INFOT, NOUT, LERR, OK )
*
*        CSPRFS
*
         SRNAMT = 'CSPRFS'
         INFOT = 1
         CALL CSPRFS( '/', 0, 0, A, AF, IP, B, 1, X, 1, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'CSPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSPRFS( 'U', -1, 0, A, AF, IP, B, 1, X, 1, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'CSPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL CSPRFS( 'U', 0, -1, A, AF, IP, B, 1, X, 1, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'CSPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL CSPRFS( 'U', 2, 1, A, AF, IP, B, 1, X, 2, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'CSPRFS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL CSPRFS( 'U', 2, 1, A, AF, IP, B, 2, X, 1, R1, R2, W, R,
     $                INFO )
         CALL CHKXER( 'CSPRFS', INFOT, NOUT, LERR, OK )
*
*        CSPCON
*
         SRNAMT = 'CSPCON'
         INFOT = 1
         CALL CSPCON( '/', 0, A, IP, ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CSPCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL CSPCON( 'U', -1, A, IP, ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CSPCON', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL CSPCON( 'U', 1, A, IP, -ANRM, RCOND, W, INFO )
         CALL CHKXER( 'CSPCON', INFOT, NOUT, LERR, OK )
      END IF
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of CERRSY
*
      END
