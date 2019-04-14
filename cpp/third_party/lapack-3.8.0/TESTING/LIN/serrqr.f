*> \brief \b SERRQR
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SERRQR( PATH, NUNIT )
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
*> SERRQR tests the error exits for the REAL routines
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
*> \ingroup single_lin
*
*  =====================================================================
      SUBROUTINE SERRQR( PATH, NUNIT )
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
      REAL               A( NMAX, NMAX ), AF( NMAX, NMAX ), B( NMAX ),
     $                   W( NMAX ), X( NMAX )
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, SGEQR2, SGEQR2P, SGEQRF,
     $                   SGEQRFP, SGEQRS, SORG2R, SORGQR, SORM2R,
     $                   SORMQR
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
*
*     Set the variables to innocuous values.
*
      DO 20 J = 1, NMAX
         DO 10 I = 1, NMAX
            A( I, J ) = 1. / REAL( I+J )
            AF( I, J ) = 1. / REAL( I+J )
   10    CONTINUE
         B( J ) = 0.
         W( J ) = 0.
         X( J ) = 0.
   20 CONTINUE
      OK = .TRUE.
*
*     Error exits for QR factorization
*
*     SGEQRF
*
      SRNAMT = 'SGEQRF'
      INFOT = 1
      CALL SGEQRF( -1, 0, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'SGEQRF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGEQRF( 0, -1, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'SGEQRF', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SGEQRF( 2, 1, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'SGEQRF', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SGEQRF( 1, 2, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'SGEQRF', INFOT, NOUT, LERR, OK )
*
*     SGEQRFP
*
      SRNAMT = 'SGEQRFP'
      INFOT = 1
      CALL SGEQRFP( -1, 0, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'SGEQRFP', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGEQRFP( 0, -1, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'SGEQRFP', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SGEQRFP( 2, 1, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'SGEQRFP', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SGEQRFP( 1, 2, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'SGEQRFP', INFOT, NOUT, LERR, OK )
*
*     SGEQR2
*
      SRNAMT = 'SGEQR2'
      INFOT = 1
      CALL SGEQR2( -1, 0, A, 1, B, W, INFO )
      CALL CHKXER( 'SGEQR2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGEQR2( 0, -1, A, 1, B, W, INFO )
      CALL CHKXER( 'SGEQR2', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SGEQR2( 2, 1, A, 1, B, W, INFO )
      CALL CHKXER( 'SGEQR2', INFOT, NOUT, LERR, OK )
*
*     SGEQR2P
*
      SRNAMT = 'SGEQR2P'
      INFOT = 1
      CALL SGEQR2P( -1, 0, A, 1, B, W, INFO )
      CALL CHKXER( 'SGEQR2P', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGEQR2P( 0, -1, A, 1, B, W, INFO )
      CALL CHKXER( 'SGEQR2P', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SGEQR2P( 2, 1, A, 1, B, W, INFO )
      CALL CHKXER( 'SGEQR2P', INFOT, NOUT, LERR, OK )
*
*     SGEQRS
*
      SRNAMT = 'SGEQRS'
      INFOT = 1
      CALL SGEQRS( -1, 0, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'SGEQRS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGEQRS( 0, -1, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'SGEQRS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGEQRS( 1, 2, 0, A, 2, X, B, 2, W, 1, INFO )
      CALL CHKXER( 'SGEQRS', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SGEQRS( 0, 0, -1, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'SGEQRS', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SGEQRS( 2, 1, 0, A, 1, X, B, 2, W, 1, INFO )
      CALL CHKXER( 'SGEQRS', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL SGEQRS( 2, 1, 0, A, 2, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'SGEQRS', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL SGEQRS( 1, 1, 2, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'SGEQRS', INFOT, NOUT, LERR, OK )
*
*     SORGQR
*
      SRNAMT = 'SORGQR'
      INFOT = 1
      CALL SORGQR( -1, 0, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'SORGQR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SORGQR( 0, -1, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'SORGQR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SORGQR( 1, 2, 0, A, 1, X, W, 2, INFO )
      CALL CHKXER( 'SORGQR', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SORGQR( 0, 0, -1, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'SORGQR', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SORGQR( 1, 1, 2, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'SORGQR', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORGQR( 2, 2, 0, A, 1, X, W, 2, INFO )
      CALL CHKXER( 'SORGQR', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL SORGQR( 2, 2, 0, A, 2, X, W, 1, INFO )
      CALL CHKXER( 'SORGQR', INFOT, NOUT, LERR, OK )
*
*     SORG2R
*
      SRNAMT = 'SORG2R'
      INFOT = 1
      CALL SORG2R( -1, 0, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'SORG2R', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SORG2R( 0, -1, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'SORG2R', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SORG2R( 1, 2, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'SORG2R', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SORG2R( 0, 0, -1, A, 1, X, W, INFO )
      CALL CHKXER( 'SORG2R', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SORG2R( 2, 1, 2, A, 2, X, W, INFO )
      CALL CHKXER( 'SORG2R', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORG2R( 2, 1, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'SORG2R', INFOT, NOUT, LERR, OK )
*
*     SORMQR
*
      SRNAMT = 'SORMQR'
      INFOT = 1
      CALL SORMQR( '/', 'N', 0, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMQR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SORMQR( 'L', '/', 0, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMQR', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SORMQR( 'L', 'N', -1, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMQR', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SORMQR( 'L', 'N', 0, -1, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMQR', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORMQR( 'L', 'N', 0, 0, -1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMQR', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORMQR( 'L', 'N', 0, 1, 1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMQR', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORMQR( 'R', 'N', 1, 0, 1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMQR', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SORMQR( 'L', 'N', 2, 1, 0, A, 1, X, AF, 2, W, 1, INFO )
      CALL CHKXER( 'SORMQR', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SORMQR( 'R', 'N', 1, 2, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMQR', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL SORMQR( 'L', 'N', 2, 1, 0, A, 2, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMQR', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL SORMQR( 'L', 'N', 1, 2, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'SORMQR', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL SORMQR( 'R', 'N', 2, 1, 0, A, 1, X, AF, 2, W, 1, INFO )
      CALL CHKXER( 'SORMQR', INFOT, NOUT, LERR, OK )
*
*     SORM2R
*
      SRNAMT = 'SORM2R'
      INFOT = 1
      CALL SORM2R( '/', 'N', 0, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORM2R', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SORM2R( 'L', '/', 0, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORM2R', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SORM2R( 'L', 'N', -1, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORM2R', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SORM2R( 'L', 'N', 0, -1, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORM2R', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORM2R( 'L', 'N', 0, 0, -1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORM2R', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORM2R( 'L', 'N', 0, 1, 1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORM2R', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SORM2R( 'R', 'N', 1, 0, 1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORM2R', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SORM2R( 'L', 'N', 2, 1, 0, A, 1, X, AF, 2, W, INFO )
      CALL CHKXER( 'SORM2R', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SORM2R( 'R', 'N', 1, 2, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORM2R', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL SORM2R( 'L', 'N', 2, 1, 0, A, 2, X, AF, 1, W, INFO )
      CALL CHKXER( 'SORM2R', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of SERRQR
*
      END
