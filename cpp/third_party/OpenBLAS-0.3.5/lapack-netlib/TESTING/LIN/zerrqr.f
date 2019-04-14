*> \brief \b ZERRQR
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZERRQR( PATH, NUNIT )
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
*> ZERRQR tests the error exits for the COMPLEX*16 routines
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
*> \ingroup complex16_lin
*
*  =====================================================================
      SUBROUTINE ZERRQR( PATH, NUNIT )
*
*  -- LAPACK test routine ((version 3.7.0) --
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
      COMPLEX*16         A( NMAX, NMAX ), AF( NMAX, NMAX ), B( NMAX ),
     $                   W( NMAX ), X( NMAX )
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, ZGEQR2, ZGEQR2P, ZGEQRF,
     $                   ZGEQRFP, ZGEQRS, ZUNG2R, ZUNGQR, ZUNM2R,
     $                   ZUNMQR
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
      INTRINSIC          DBLE, DCMPLX
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
            A( I, J ) = DCMPLX( 1.D0 / DBLE( I+J ),
     $                  -1.D0 / DBLE( I+J ) )
            AF( I, J ) = DCMPLX( 1.D0 / DBLE( I+J ),
     $                   -1.D0 / DBLE( I+J ) )
   10    CONTINUE
         B( J ) = 0.D0
         W( J ) = 0.D0
         X( J ) = 0.D0
   20 CONTINUE
      OK = .TRUE.
*
*     Error exits for QR factorization
*
*     ZGEQRF
*
      SRNAMT = 'ZGEQRF'
      INFOT = 1
      CALL ZGEQRF( -1, 0, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'ZGEQRF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZGEQRF( 0, -1, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'ZGEQRF', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZGEQRF( 2, 1, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'ZGEQRF', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL ZGEQRF( 1, 2, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'ZGEQRF', INFOT, NOUT, LERR, OK )
*
*     ZGEQRFP
*
      SRNAMT = 'ZGEQRFP'
      INFOT = 1
      CALL ZGEQRFP( -1, 0, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'ZGEQRFP', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZGEQRFP( 0, -1, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'ZGEQRFP', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZGEQRFP( 2, 1, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'ZGEQRFP', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL ZGEQRFP( 1, 2, A, 1, B, W, 1, INFO )
      CALL CHKXER( 'ZGEQRFP', INFOT, NOUT, LERR, OK )
*
*     ZGEQR2
*
      SRNAMT = 'ZGEQR2'
      INFOT = 1
      CALL ZGEQR2( -1, 0, A, 1, B, W, INFO )
      CALL CHKXER( 'ZGEQR2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZGEQR2( 0, -1, A, 1, B, W, INFO )
      CALL CHKXER( 'ZGEQR2', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZGEQR2( 2, 1, A, 1, B, W, INFO )
      CALL CHKXER( 'ZGEQR2', INFOT, NOUT, LERR, OK )
*
*     ZGEQR2P
*
      SRNAMT = 'ZGEQR2P'
      INFOT = 1
      CALL ZGEQR2P( -1, 0, A, 1, B, W, INFO )
      CALL CHKXER( 'ZGEQR2P', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZGEQR2P( 0, -1, A, 1, B, W, INFO )
      CALL CHKXER( 'ZGEQR2P', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZGEQR2P( 2, 1, A, 1, B, W, INFO )
      CALL CHKXER( 'ZGEQR2P', INFOT, NOUT, LERR, OK )
*
*     ZGEQRS
*
      SRNAMT = 'ZGEQRS'
      INFOT = 1
      CALL ZGEQRS( -1, 0, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'ZGEQRS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZGEQRS( 0, -1, 0, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'ZGEQRS', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZGEQRS( 1, 2, 0, A, 2, X, B, 2, W, 1, INFO )
      CALL CHKXER( 'ZGEQRS', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZGEQRS( 0, 0, -1, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'ZGEQRS', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZGEQRS( 2, 1, 0, A, 1, X, B, 2, W, 1, INFO )
      CALL CHKXER( 'ZGEQRS', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL ZGEQRS( 2, 1, 0, A, 2, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'ZGEQRS', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL ZGEQRS( 1, 1, 2, A, 1, X, B, 1, W, 1, INFO )
      CALL CHKXER( 'ZGEQRS', INFOT, NOUT, LERR, OK )
*
*     ZUNGQR
*
      SRNAMT = 'ZUNGQR'
      INFOT = 1
      CALL ZUNGQR( -1, 0, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'ZUNGQR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZUNGQR( 0, -1, 0, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'ZUNGQR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZUNGQR( 1, 2, 0, A, 1, X, W, 2, INFO )
      CALL CHKXER( 'ZUNGQR', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZUNGQR( 0, 0, -1, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'ZUNGQR', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZUNGQR( 1, 1, 2, A, 1, X, W, 1, INFO )
      CALL CHKXER( 'ZUNGQR', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZUNGQR( 2, 2, 0, A, 1, X, W, 2, INFO )
      CALL CHKXER( 'ZUNGQR', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL ZUNGQR( 2, 2, 0, A, 2, X, W, 1, INFO )
      CALL CHKXER( 'ZUNGQR', INFOT, NOUT, LERR, OK )
*
*     ZUNG2R
*
      SRNAMT = 'ZUNG2R'
      INFOT = 1
      CALL ZUNG2R( -1, 0, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'ZUNG2R', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZUNG2R( 0, -1, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'ZUNG2R', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZUNG2R( 1, 2, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'ZUNG2R', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZUNG2R( 0, 0, -1, A, 1, X, W, INFO )
      CALL CHKXER( 'ZUNG2R', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZUNG2R( 2, 1, 2, A, 2, X, W, INFO )
      CALL CHKXER( 'ZUNG2R', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZUNG2R( 2, 1, 0, A, 1, X, W, INFO )
      CALL CHKXER( 'ZUNG2R', INFOT, NOUT, LERR, OK )
*
*     ZUNMQR
*
      SRNAMT = 'ZUNMQR'
      INFOT = 1
      CALL ZUNMQR( '/', 'N', 0, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMQR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZUNMQR( 'L', '/', 0, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMQR', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZUNMQR( 'L', 'N', -1, 0, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMQR', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZUNMQR( 'L', 'N', 0, -1, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMQR', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZUNMQR( 'L', 'N', 0, 0, -1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMQR', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZUNMQR( 'L', 'N', 0, 1, 1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMQR', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZUNMQR( 'R', 'N', 1, 0, 1, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMQR', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL ZUNMQR( 'L', 'N', 2, 1, 0, A, 1, X, AF, 2, W, 1, INFO )
      CALL CHKXER( 'ZUNMQR', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL ZUNMQR( 'R', 'N', 1, 2, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMQR', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL ZUNMQR( 'L', 'N', 2, 1, 0, A, 2, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMQR', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL ZUNMQR( 'L', 'N', 1, 2, 0, A, 1, X, AF, 1, W, 1, INFO )
      CALL CHKXER( 'ZUNMQR', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL ZUNMQR( 'R', 'N', 2, 1, 0, A, 1, X, AF, 2, W, 1, INFO )
      CALL CHKXER( 'ZUNMQR', INFOT, NOUT, LERR, OK )
*
*     ZUNM2R
*
      SRNAMT = 'ZUNM2R'
      INFOT = 1
      CALL ZUNM2R( '/', 'N', 0, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNM2R', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZUNM2R( 'L', '/', 0, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNM2R', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZUNM2R( 'L', 'N', -1, 0, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNM2R', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZUNM2R( 'L', 'N', 0, -1, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNM2R', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZUNM2R( 'L', 'N', 0, 0, -1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNM2R', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZUNM2R( 'L', 'N', 0, 1, 1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNM2R', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZUNM2R( 'R', 'N', 1, 0, 1, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNM2R', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL ZUNM2R( 'L', 'N', 2, 1, 0, A, 1, X, AF, 2, W, INFO )
      CALL CHKXER( 'ZUNM2R', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL ZUNM2R( 'R', 'N', 1, 2, 0, A, 1, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNM2R', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL ZUNM2R( 'L', 'N', 2, 1, 0, A, 2, X, AF, 1, W, INFO )
      CALL CHKXER( 'ZUNM2R', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of ZERRQR
*
      END
