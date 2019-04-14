*> \brief \b SERRQRT
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SERRQRT( PATH, NUNIT )
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
*> SERRQRT tests the error exits for the REAL routines
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
*> \ingroup single_lin
*
*  =====================================================================
      SUBROUTINE SERRQRT( PATH, NUNIT )
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
      REAL               A( NMAX, NMAX ), T( NMAX, NMAX ), W( NMAX ),
     $                   C( NMAX, NMAX )
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, SGEQRT2, SGEQRT3, SGEQRT,
     $                   SGEMQRT
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
      INTRINSIC          FLOAT
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
            A( I, J ) = 1.0 / FLOAT( I+J )
            C( I, J ) = 1.0 / FLOAT( I+J )
            T( I, J ) = 1.0 / FLOAT( I+J )
         END DO
         W( J ) = 0.0
      END DO
      OK = .TRUE.
*
*     Error exits for QRT factorization
*
*     SGEQRT
*
      SRNAMT = 'SGEQRT'
      INFOT = 1
      CALL SGEQRT( -1, 0, 1, A, 1, T, 1, W, INFO )
      CALL CHKXER( 'SGEQRT', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGEQRT( 0, -1, 1, A, 1, T, 1, W, INFO )
      CALL CHKXER( 'SGEQRT', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SGEQRT( 0, 0, 0, A, 1, T, 1, W, INFO )
      CALL CHKXER( 'SGEQRT', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SGEQRT( 2, 1, 1, A, 1, T, 1, W, INFO )
      CALL CHKXER( 'SGEQRT', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL SGEQRT( 2, 2, 2, A, 2, T, 1, W, INFO )
      CALL CHKXER( 'SGEQRT', INFOT, NOUT, LERR, OK )
*
*     SGEQRT2
*
      SRNAMT = 'SGEQRT2'
      INFOT = 1
      CALL SGEQRT2( -1, 0, A, 1, T, 1, INFO )
      CALL CHKXER( 'SGEQRT2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGEQRT2( 0, -1, A, 1, T, 1, INFO )
      CALL CHKXER( 'SGEQRT2', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SGEQRT2( 2, 1, A, 1, T, 1, INFO )
      CALL CHKXER( 'SGEQRT2', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL SGEQRT2( 2, 2, A, 2, T, 1, INFO )
      CALL CHKXER( 'SGEQRT2', INFOT, NOUT, LERR, OK )
*
*     SGEQRT3
*
      SRNAMT = 'SGEQRT3'
      INFOT = 1
      CALL SGEQRT3( -1, 0, A, 1, T, 1, INFO )
      CALL CHKXER( 'SGEQRT3', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGEQRT3( 0, -1, A, 1, T, 1, INFO )
      CALL CHKXER( 'SGEQRT3', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SGEQRT3( 2, 1, A, 1, T, 1, INFO )
      CALL CHKXER( 'SGEQRT3', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL SGEQRT3( 2, 2, A, 2, T, 1, INFO )
      CALL CHKXER( 'SGEQRT3', INFOT, NOUT, LERR, OK )
*
*     SGEMQRT
*
      SRNAMT = 'SGEMQRT'
      INFOT = 1
      CALL SGEMQRT( '/', 'N', 0, 0, 0, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'SGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL SGEMQRT( 'L', '/', 0, 0, 0, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'SGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL SGEMQRT( 'L', 'N', -1, 0, 0, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'SGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL SGEMQRT( 'L', 'N', 0, -1, 0, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'SGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SGEMQRT( 'L', 'N', 0, 0, -1, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'SGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL SGEMQRT( 'R', 'N', 0, 0, -1, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'SGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL SGEMQRT( 'L', 'N', 0, 0, 0, 0, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'SGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL SGEMQRT( 'R', 'N', 1, 2, 1, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'SGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL SGEMQRT( 'L', 'N', 2, 1, 1, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'SGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL SGEMQRT( 'R', 'N', 1, 1, 1, 1, A, 1, T, 0, C, 1, W, INFO )
      CALL CHKXER( 'SGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL SGEMQRT( 'L', 'N', 1, 1, 1, 1, A, 1, T, 1, C, 0, W, INFO )
      CALL CHKXER( 'SGEMQRT', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of SERRQRT
*
      END
