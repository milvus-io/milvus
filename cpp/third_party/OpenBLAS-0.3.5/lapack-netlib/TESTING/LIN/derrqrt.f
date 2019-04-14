*> \brief \b DERRQRT
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DERRQRT( PATH, NUNIT )
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
*> DERRQRT tests the error exits for the DOUBLE PRECISION routines
*> that use the QRT decomposition of a general matrix.
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
      SUBROUTINE DERRQRT( PATH, NUNIT )
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
     $                   C( NMAX, NMAX )
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, DGEQRT2, DGEQRT3, DGEQRT,
     $                   DGEMQRT
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
*     Error exits for QRT factorization
*
*     DGEQRT
*
      SRNAMT = 'DGEQRT'
      INFOT = 1
      CALL DGEQRT( -1, 0, 1, A, 1, T, 1, W, INFO )
      CALL CHKXER( 'DGEQRT', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DGEQRT( 0, -1, 1, A, 1, T, 1, W, INFO )
      CALL CHKXER( 'DGEQRT', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DGEQRT( 0, 0, 0, A, 1, T, 1, W, INFO )
      CALL CHKXER( 'DGEQRT', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DGEQRT( 2, 1, 1, A, 1, T, 1, W, INFO )
      CALL CHKXER( 'DGEQRT', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL DGEQRT( 2, 2, 2, A, 2, T, 1, W, INFO )
      CALL CHKXER( 'DGEQRT', INFOT, NOUT, LERR, OK )
*
*     DGEQRT2
*
      SRNAMT = 'DGEQRT2'
      INFOT = 1
      CALL DGEQRT2( -1, 0, A, 1, T, 1, INFO )
      CALL CHKXER( 'DGEQRT2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DGEQRT2( 0, -1, A, 1, T, 1, INFO )
      CALL CHKXER( 'DGEQRT2', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DGEQRT2( 2, 1, A, 1, T, 1, INFO )
      CALL CHKXER( 'DGEQRT2', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL DGEQRT2( 2, 2, A, 2, T, 1, INFO )
      CALL CHKXER( 'DGEQRT2', INFOT, NOUT, LERR, OK )
*
*     DGEQRT3
*
      SRNAMT = 'DGEQRT3'
      INFOT = 1
      CALL DGEQRT3( -1, 0, A, 1, T, 1, INFO )
      CALL CHKXER( 'DGEQRT3', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DGEQRT3( 0, -1, A, 1, T, 1, INFO )
      CALL CHKXER( 'DGEQRT3', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DGEQRT3( 2, 1, A, 1, T, 1, INFO )
      CALL CHKXER( 'DGEQRT3', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL DGEQRT3( 2, 2, A, 2, T, 1, INFO )
      CALL CHKXER( 'DGEQRT3', INFOT, NOUT, LERR, OK )
*
*     DGEMQRT
*
      SRNAMT = 'DGEMQRT'
      INFOT = 1
      CALL DGEMQRT( '/', 'N', 0, 0, 0, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'DGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DGEMQRT( 'L', '/', 0, 0, 0, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'DGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DGEMQRT( 'L', 'N', -1, 0, 0, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'DGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DGEMQRT( 'L', 'N', 0, -1, 0, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'DGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DGEMQRT( 'L', 'N', 0, 0, -1, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'DGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DGEMQRT( 'R', 'N', 0, 0, -1, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'DGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL DGEMQRT( 'L', 'N', 0, 0, 0, 0, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'DGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL DGEMQRT( 'R', 'N', 1, 2, 1, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'DGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL DGEMQRT( 'L', 'N', 2, 1, 1, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'DGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL DGEMQRT( 'R', 'N', 1, 1, 1, 1, A, 1, T, 0, C, 1, W, INFO )
      CALL CHKXER( 'DGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL DGEMQRT( 'L', 'N', 1, 1, 1, 1, A, 1, T, 1, C, 0, W, INFO )
      CALL CHKXER( 'DGEMQRT', INFOT, NOUT, LERR, OK )
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
