*> \brief \b DERRQR
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DERRQR( PATH, NUNIT )
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
*> DERRQR tests the error exits for the DOUBLE PRECISION routines
*> that use the QR decomposition of a general matrix.
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
*> \ingroup double_lin
*
*  =====================================================================
      SUBROUTINE DERRQR( PATH, NUNIT )
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
      PARAMETER          ( NMAX = 2 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, INFO, J
*     ..
*     .. Local Arrays ..
      DOUBLE PRECISION   A( NMAX, NMAX ), AF( NMAX, NMAX ), B( NMAX ),
     $                   W( NMAX ), X( NMAX )
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, DGEQR2, DGEQR2P, DGEQRF,
     $                   DGEQRFP, DGEQRS, DORG2R, DORGQR, DORM2R,
     $                   DORMQR
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
*
*     Set the variables to innocuous values.
*
      DO 20 J = 1, NMAX
         DO 10 I = 1, NMAX
            A( I, J ) = 1.D0 / DBLE( I+J )
            AF( I, J ) = 1.D0 / DBLE( I+J )
   10    CONTINUE
         B( J ) = 0.D0
         W( J ) = 0.D0
         X( J ) = 0.D0
   20 CONTINUE
      OK = .TRUE.
*
*     Error exits for QR factorization
*
*     DGEQRF
*
      SRNAMT = 'DGEQRF'
      INFOT = 1
      CALL DGEQRF( -1, 0, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'DGEQRF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DGEQRF( 0, -1, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'DGEQRF', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DGEQRF( 2, 1, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'DGEQRF', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL DGEQRF( 1, 2, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'DGEQRF', INFOT, NOUT, LERR, OK )
*
*     DGEQRFP
*
      SRNAMT = 'DGEQRFP'
      INFOT = 1
      CALL DGEQRFP( -1, 0, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'DGEQRFP', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DGEQRFP( 0, -1, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'DGEQRFP', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DGEQRFP( 2, 1, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'DGEQRFP', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL DGEQRFP( 1, 2, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'DGEQRFP', INFOT, NOUT, LERR, OK )
*
*     DGEQR2
*
      SRNAMT = 'DGEQR2'
      INFOT = 1
      CALL DGEQR2( -1, 0, A, 1, B, W, INFO )
      CALL CHKXER( 'DGEQR2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DGEQR2( 0, -1, A, 1, B, W, INFO )
      CALL CHKXER( 'DGEQR2', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DGEQR2( 2, 1, A, 1, B, W, INFO )
      CALL CHKXER( 'DGEQR2', INFOT, NOUT, LERR, OK )
*
*     DGEQR2P
*
      SRNAMT = 'DGEQR2P'
      INFOT = 1
      CALL DGEQR2P( -1, 0, A, 1, B, W, INFO )
      CALL CHKXER( 'DGEQR2P', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DGEQR2P( 0, -1, A, 1, B, W, INFO )
      CALL CHKXER( 'DGEQR2P', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DGEQR2P( 2, 1, A, 1, B, W, INFO )
      CALL CHKXER( 'DGEQR2P', INFOT, NOUT, LERR, OK )
*
*     DGEQRS
*
      SRNAMT = 'DGEQRS'
      INFOT = 1
      CALL DGEQRS( -1, 0, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'DGEQRS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DGEQRS( 0, -1, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'DGEQRS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DGEQRS( 1, 2, 0, A, 2, X, B, 2, W, 1, INFO )
      CALL CHKXER( 'DGEQRS', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DGEQRS( 0, 0, -1, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'DGEQRS', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DGEQRS( 2, 1, 0, A, 1, X, B, 2, W, 1, INFO )
      CALL CHKXER( 'DGEQRS', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL DGEQRS( 2, 1, 0, A, 2, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'DGEQRS', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL DGEQRS( 1, 1, 2, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'DGEQRS', INFOT, NOUT, LERR, OK )
*
*     DORGQR
*
      SRNAMT = 'DORGQR'
      INFOT = 1
      CALL DORGQR( -1, 0, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'DORGQR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DORGQR( 0, -1, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'DORGQR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DORGQR( 1, 2, 0, A, 1, X, W, 2, INFO )
      CALL CHKXER( 'DORGQR', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DORGQR( 0, 0, -1, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'DORGQR', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DORGQR( 1, 1, 2, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'DORGQR', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DORGQR( 2, 2, 0, A, 1, X, W, 2, INFO )
      CALL CHKXER( 'DORGQR', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL DORGQR( 2, 2, 0, A, 2, X, W, 1, INFO )
      CALL CHKXER( 'DORGQR', INFOT, NOUT, LERR, OK )
*
*     DORG2R
*
      SRNAMT = 'DORG2R'
      INFOT = 1
      CALL DORG2R( -1, 0, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'DORG2R', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DORG2R( 0, -1, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'DORG2R', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DORG2R( 1, 2, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'DORG2R', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DORG2R( 0, 0, -1, A, 1, X, W, INFO )
      CALL CHKXER( 'DORG2R', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DORG2R( 2, 1, 2, A, 2, X, W, INFO )
      CALL CHKXER( 'DORG2R', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DORG2R( 2, 1, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'DORG2R', INFOT, NOUT, LERR, OK )
*
*     DORMQR
*
      SRNAMT = 'DORMQR'
      INFOT = 1
      CALL DORMQR( '/', 'N', 0, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'DORMQR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DORMQR( 'L', '/', 0, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'DORMQR', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DORMQR( 'L', 'N', -1, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'DORMQR', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DORMQR( 'L', 'N', 0, -1, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'DORMQR', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DORMQR( 'L', 'N', 0, 0, -1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'DORMQR', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DORMQR( 'L', 'N', 0, 1, 1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'DORMQR', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DORMQR( 'R', 'N', 1, 0, 1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'DORMQR', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL DORMQR( 'L', 'N', 2, 1, 0, A, 1, X, AF, 2, W, 1, INFO )
      CALL CHKXER( 'DORMQR', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL DORMQR( 'R', 'N', 1, 2, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'DORMQR', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL DORMQR( 'L', 'N', 2, 1, 0, A, 2, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'DORMQR', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL DORMQR( 'L', 'N', 1, 2, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'DORMQR', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL DORMQR( 'R', 'N', 2, 1, 0, A, 1, X, AF, 2, W, 1, INFO )
      CALL CHKXER( 'DORMQR', INFOT, NOUT, LERR, OK )
*
*     DORM2R
*
      SRNAMT = 'DORM2R'
      INFOT = 1
      CALL DORM2R( '/', 'N', 0, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'DORM2R', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DORM2R( 'L', '/', 0, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'DORM2R', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DORM2R( 'L', 'N', -1, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'DORM2R', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DORM2R( 'L', 'N', 0, -1, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'DORM2R', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DORM2R( 'L', 'N', 0, 0, -1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'DORM2R', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DORM2R( 'L', 'N', 0, 1, 1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'DORM2R', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DORM2R( 'R', 'N', 1, 0, 1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'DORM2R', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL DORM2R( 'L', 'N', 2, 1, 0, A, 1, X, AF, 2, W, INFO )
      CALL CHKXER( 'DORM2R', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL DORM2R( 'R', 'N', 1, 2, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'DORM2R', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL DORM2R( 'L', 'N', 2, 1, 0, A, 2, X, AF, 1, W, INFO )
      CALL CHKXER( 'DORM2R', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of DERRQR
*
      END
