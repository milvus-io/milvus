*> \brief \b ZERRTSQR
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZERRTSQR( PATH, NUNIT )
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
*> ZERRTSQR tests the error exits for the ZOUBLE PRECISION routines
*> that use the TSQR decomposition of a general matrix.
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
*> \author Univ. of Colorado Zenver
*> \author NAG Ltd.
*
*> \date December 2016
*
*> \ingroup double_lin
*
*  =====================================================================
      SUBROUTINE ZERRTSQR( PATH, NUNIT )
      IMPLICIT NONE
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
      INTEGER            I, INFO, J, NB
*     ..
*     .. Local Arrays ..
      COMPLEX*16         A( NMAX, NMAX ), T( NMAX, NMAX ), W( NMAX ),
     $                   C( NMAX, NMAX ), TAU(NMAX)
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, ZGEQR,
     $                   ZGEMQR, ZGELQ, ZGEMLQ
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
      DO J = 1, NMAX
         DO I = 1, NMAX
            A( I, J ) = 1.D0 / DBLE( I+J )
            C( I, J ) = 1.D0 / DBLE( I+J )
            T( I, J ) = 1.D0 / DBLE( I+J )
         END DO
         W( J ) = 0.D0
      END DO
      OK = .TRUE.
*
*     Error exits for TS factorization
*
*     ZGEQR
*
      SRNAMT = 'ZGEQR'
      INFOT = 1
      CALL ZGEQR( -1, 0, A, 1, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'ZGEQR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZGEQR( 0, -1, A, 1, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'ZGEQR', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZGEQR( 1, 1, A, 0, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'ZGEQR', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL ZGEQR( 3, 2, A, 3, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'ZGEQR', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL ZGEQR( 3, 2, A, 3, TAU, 8, W, 0, INFO )
      CALL CHKXER( 'ZGEQR', INFOT, NOUT, LERR, OK )
*
*     ZGEMQR
*
      TAU(1)=1
      TAU(2)=1
      SRNAMT = 'ZGEMQR'
      NB=1
      INFOT = 1
      CALL ZGEMQR( '/', 'N', 0, 0, 0, A, 1, TAU, 1, C, 1, W, 1,INFO)
      CALL CHKXER( 'ZGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZGEMQR( 'L', '/', 0, 0, 0, A, 1, TAU, 1, C, 1, W, 1,INFO)
      CALL CHKXER( 'ZGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZGEMQR( 'L', 'N', -1, 0, 0, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'ZGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZGEMQR( 'L', 'N', 0, -1, 0, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'ZGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZGEMQR( 'L', 'N', 0, 0, -1, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'ZGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZGEMQR( 'R', 'N', 0, 0, -1, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'ZGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL ZGEMQR( 'L', 'N', 2, 1, 0, A, 0, TAU, 1, C, 1, W, 1,INFO)
      CALL CHKXER( 'ZGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 9
      CALL ZGEMQR( 'R', 'N', 2, 2, 1, A, 2, TAU, 0, C, 1, W, 1,INFO)
      CALL CHKXER( 'ZGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 9
      CALL ZGEMQR( 'L', 'N', 2, 2, 1, A, 2, TAU, 0, C, 1, W, 1,INFO)
      CALL CHKXER( 'ZGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 11
      CALL ZGEMQR( 'L', 'N', 2, 1, 1, A, 2, TAU, 6, C, 0, W, 1,INFO)
      CALL CHKXER( 'ZGEMQR', INFOT, NOUT, LERR, OK )
      INFOT = 13
      CALL ZGEMQR( 'L', 'N', 2, 2, 1, A, 2, TAU, 6, C, 2, W, 0,INFO)
      CALL CHKXER( 'ZGEMQR', INFOT, NOUT, LERR, OK )
*
*     ZGELQ
*
      SRNAMT = 'ZGELQ'
      INFOT = 1
      CALL ZGELQ( -1, 0, A, 1, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'ZGELQ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZGELQ( 0, -1, A, 1, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'ZGELQ', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZGELQ( 1, 1, A, 0, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'ZGELQ', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL ZGELQ( 2, 3, A, 3, TAU, 1, W, 1, INFO )
      CALL CHKXER( 'ZGELQ', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL ZGELQ( 2, 3, A, 3, TAU, 8, W, 0, INFO )
      CALL CHKXER( 'ZGELQ', INFOT, NOUT, LERR, OK )
*
*     ZGEMLQ
*
      TAU(1)=1
      TAU(2)=1
      SRNAMT = 'ZGEMLQ'
      NB=1
      INFOT = 1
      CALL ZGEMLQ( '/', 'N', 0, 0, 0, A, 1, TAU, 1, C, 1, W, 1,INFO)
      CALL CHKXER( 'ZGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZGEMLQ( 'L', '/', 0, 0, 0, A, 1, TAU, 1, C, 1, W, 1,INFO)
      CALL CHKXER( 'ZGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZGEMLQ( 'L', 'N', -1, 0, 0, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'ZGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZGEMLQ( 'L', 'N', 0, -1, 0, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'ZGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZGEMLQ( 'L', 'N', 0, 0, -1, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'ZGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZGEMLQ( 'R', 'N', 0, 0, -1, A, 1, TAU, 1, C, 1, W,1,INFO)
      CALL CHKXER( 'ZGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL ZGEMLQ( 'L', 'N', 1, 2, 0, A, 0, TAU, 1, C, 1, W, 1,INFO)
      CALL CHKXER( 'ZGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 9
      CALL ZGEMLQ( 'R', 'N', 2, 2, 1, A, 1, TAU, 0, C, 1, W, 1,INFO)
      CALL CHKXER( 'ZGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 9
      CALL ZGEMLQ( 'L', 'N', 2, 2, 1, A, 1, TAU, 0, C, 1, W, 1,INFO)
      CALL CHKXER( 'ZGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 11
      CALL ZGEMLQ( 'L', 'N', 1, 2, 1, A, 1, TAU, 6, C, 0, W, 1,INFO)
      CALL CHKXER( 'ZGEMLQ', INFOT, NOUT, LERR, OK )
      INFOT = 13
      CALL ZGEMLQ( 'L', 'N', 2, 2, 1, A, 2, TAU, 6, C, 2, W, 0,INFO)
      CALL CHKXER( 'ZGEMLQ', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of DERRTSQR
*
      END
