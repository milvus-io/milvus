*> \brief \b DERRQRTP
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DERRQRTP( PATH, NUNIT )
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
*> DERRQRTP tests the error exits for the REAL routines
*> that use the QRT decomposition of a triangular-pentagonal matrix.
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
      SUBROUTINE DERRQRTP( PATH, NUNIT )
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
      INTEGER            I, INFO, J
*     ..
*     .. Local Arrays ..
      DOUBLE PRECISION   A( NMAX, NMAX ), T( NMAX, NMAX ), W( NMAX ),
     $                   B( NMAX, NMAX ), C( NMAX, NMAX )
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, DTPQRT2, DTPQRT,
     $                   DTPMQRT
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
         W( J ) = 0.0
      END DO
      OK = .TRUE.
*
*     Error exits for TPQRT factorization
*
*     DTPQRT
*
      SRNAMT = 'DTPQRT'
      INFOT = 1
      CALL DTPQRT( -1, 1, 0, 1, A, 1, B, 1, T, 1, W, INFO )
      CALL CHKXER( 'DTPQRT', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DTPQRT( 1, -1, 0, 1, A, 1, B, 1, T, 1, W, INFO )
      CALL CHKXER( 'DTPQRT', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DTPQRT( 0, 1, -1, 1, A, 1, B, 1, T, 1, W, INFO )
      CALL CHKXER( 'DTPQRT', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DTPQRT( 0, 1, 1, 1, A, 1, B, 1, T, 1, W, INFO )
      CALL CHKXER( 'DTPQRT', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DTPQRT( 0, 1, 0, 0, A, 1, B, 1, T, 1, W, INFO )
      CALL CHKXER( 'DTPQRT', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DTPQRT( 0, 1, 0, 2, A, 1, B, 1, T, 1, W, INFO )
      CALL CHKXER( 'DTPQRT', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL DTPQRT( 1, 2, 0, 2, A, 1, B, 1, T, 1, W, INFO )
      CALL CHKXER( 'DTPQRT', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL DTPQRT( 2, 1, 0, 1, A, 1, B, 1, T, 1, W, INFO )
      CALL CHKXER( 'DTPQRT', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL DTPQRT( 2, 2, 1, 2, A, 2, B, 2, T, 1, W, INFO )
      CALL CHKXER( 'DTPQRT', INFOT, NOUT, LERR, OK )
*
*     DTPQRT2
*
      SRNAMT = 'DTPQRT2'
      INFOT = 1
      CALL DTPQRT2( -1, 0, 0, A, 1, B, 1, T, 1, INFO )
      CALL CHKXER( 'DTPQRT2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DTPQRT2( 0, -1, 0, A, 1, B, 1, T, 1, INFO )
      CALL CHKXER( 'DTPQRT2', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DTPQRT2( 0, 0, -1, A, 1, B, 1, T, 1, INFO )
      CALL CHKXER( 'DTPQRT2', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DTPQRT2( 2, 2, 0, A, 1, B, 2, T, 2, INFO )
      CALL CHKXER( 'DTPQRT2', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL DTPQRT2( 2, 2, 0, A, 2, B, 1, T, 2, INFO )
      CALL CHKXER( 'DTPQRT2', INFOT, NOUT, LERR, OK )
      INFOT = 9
      CALL DTPQRT2( 2, 2, 0, A, 2, B, 2, T, 1, INFO )
      CALL CHKXER( 'DTPQRT2', INFOT, NOUT, LERR, OK )
*
*     DTPMQRT
*
      SRNAMT = 'DTPMQRT'
      INFOT = 1
      CALL DTPMQRT( '/', 'N', 0, 0, 0, 0, 1, A, 1, T, 1, B, 1, C, 1,
     $              W, INFO )
      CALL CHKXER( 'DTPMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DTPMQRT( 'L', '/', 0, 0, 0, 0, 1, A, 1, T, 1, B, 1, C, 1,
     $              W, INFO )
      CALL CHKXER( 'DTPMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DTPMQRT( 'L', 'N', -1, 0, 0, 0, 1, A, 1, T, 1, B, 1, C, 1,
     $              W, INFO )
      CALL CHKXER( 'DTPMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DTPMQRT( 'L', 'N', 0, -1, 0, 0, 1, A, 1, T, 1, B, 1, C, 1,
     $              W, INFO )
      CALL CHKXER( 'DTPMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DTPMQRT( 'L', 'N', 0, 0, -1, 0, 1, A, 1, T, 1, B, 1, C, 1,
     $              W, INFO )
      INFOT = 6
      CALL DTPMQRT( 'L', 'N', 0, 0, 0, -1, 1, A, 1, T, 1, B, 1, C, 1,
     $              W, INFO )
      CALL CHKXER( 'DTPMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL DTPMQRT( 'L', 'N', 0, 0, 0, 0, 0, A, 1, T, 1, B, 1, C, 1,
     $              W, INFO )
      CALL CHKXER( 'DTPMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 9
      CALL DTPMQRT( 'R', 'N', 1, 2, 1, 1, 1, A, 1, T, 1, B, 1, C, 1,
     $              W, INFO )
      CALL CHKXER( 'DTPMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 9
      CALL DTPMQRT( 'L', 'N', 2, 1, 1, 1, 1, A, 1, T, 1, B, 1, C, 1,
     $              W, INFO )
      CALL CHKXER( 'DTPMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 11
      CALL DTPMQRT( 'R', 'N', 1, 1, 1, 1, 1, A, 1, T, 0, B, 1, C, 1,
     $              W, INFO )
      CALL CHKXER( 'DTPMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 13
      CALL DTPMQRT( 'L', 'N', 1, 1, 1, 1, 1, A, 1, T, 1, B, 0, C, 1,
     $              W, INFO )
      CALL CHKXER( 'DTPMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 15
      CALL DTPMQRT( 'L', 'N', 1, 1, 1, 1, 1, A, 1, T, 1, B, 1, C, 0,
     $              W, INFO )
      CALL CHKXER( 'DTPMQRT', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of DERRQRT
*
      END
