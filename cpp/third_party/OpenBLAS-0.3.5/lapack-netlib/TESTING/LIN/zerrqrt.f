*> \brief \b ZERRQRT
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZERRQRT( PATH, NUNIT )
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
*> ZERRQRT tests the error exits for the COMPLEX*16 routines
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
*> \ingroup complex16_lin
*
*  =====================================================================
      SUBROUTINE ZERRQRT( PATH, NUNIT )
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
      COMPLEX*16         A( NMAX, NMAX ), T( NMAX, NMAX ), W( NMAX ),
     $                   C( NMAX, NMAX )
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, ZGEQRT2, ZGEQRT3, ZGEQRT,
     $                   ZGEMQRT
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
      DO J = 1, NMAX
         DO I = 1, NMAX
            A( I, J ) = 1.D0 / DCMPLX( DBLE( I+J ), 0.D0 )
            C( I, J ) = 1.D0 / DCMPLX( DBLE( I+J ), 0.D0 )
            T( I, J ) = 1.D0 / DCMPLX( DBLE( I+J ), 0.D0 )
         END DO
         W( J ) = 0.D0
      END DO
      OK = .TRUE.
*
*     Error exits for QRT factorization
*
*     ZGEQRT
*
      SRNAMT = 'ZGEQRT'
      INFOT = 1
      CALL ZGEQRT( -1, 0, 1, A, 1, T, 1, W, INFO )
      CALL CHKXER( 'ZGEQRT', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZGEQRT( 0, -1, 1, A, 1, T, 1, W, INFO )
      CALL CHKXER( 'ZGEQRT', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZGEQRT( 0, 0, 0, A, 1, T, 1, W, INFO )
      CALL CHKXER( 'ZGEQRT', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZGEQRT( 2, 1, 1, A, 1, T, 1, W, INFO )
      CALL CHKXER( 'ZGEQRT', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL ZGEQRT( 2, 2, 2, A, 2, T, 1, W, INFO )
      CALL CHKXER( 'ZGEQRT', INFOT, NOUT, LERR, OK )
*
*     ZGEQRT2
*
      SRNAMT = 'ZGEQRT2'
      INFOT = 1
      CALL ZGEQRT2( -1, 0, A, 1, T, 1, INFO )
      CALL CHKXER( 'ZGEQRT2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZGEQRT2( 0, -1, A, 1, T, 1, INFO )
      CALL CHKXER( 'ZGEQRT2', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZGEQRT2( 2, 1, A, 1, T, 1, INFO )
      CALL CHKXER( 'ZGEQRT2', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL ZGEQRT2( 2, 2, A, 2, T, 1, INFO )
      CALL CHKXER( 'ZGEQRT2', INFOT, NOUT, LERR, OK )
*
*     ZGEQRT3
*
      SRNAMT = 'ZGEQRT3'
      INFOT = 1
      CALL ZGEQRT3( -1, 0, A, 1, T, 1, INFO )
      CALL CHKXER( 'ZGEQRT3', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZGEQRT3( 0, -1, A, 1, T, 1, INFO )
      CALL CHKXER( 'ZGEQRT3', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZGEQRT3( 2, 1, A, 1, T, 1, INFO )
      CALL CHKXER( 'ZGEQRT3', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL ZGEQRT3( 2, 2, A, 2, T, 1, INFO )
      CALL CHKXER( 'ZGEQRT3', INFOT, NOUT, LERR, OK )
*
*     ZGEMQRT
*
      SRNAMT = 'ZGEMQRT'
      INFOT = 1
      CALL ZGEMQRT( '/', 'N', 0, 0, 0, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'ZGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZGEMQRT( 'L', '/', 0, 0, 0, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'ZGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL ZGEMQRT( 'L', 'N', -1, 0, 0, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'ZGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZGEMQRT( 'L', 'N', 0, -1, 0, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'ZGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZGEMQRT( 'L', 'N', 0, 0, -1, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'ZGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL ZGEMQRT( 'R', 'N', 0, 0, -1, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'ZGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL ZGEMQRT( 'L', 'N', 0, 0, 0, 0, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'ZGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL ZGEMQRT( 'R', 'N', 1, 2, 1, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'ZGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL ZGEMQRT( 'L', 'N', 2, 1, 1, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'ZGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL ZGEMQRT( 'R', 'N', 1, 1, 1, 1, A, 1, T, 0, C, 1, W, INFO )
      CALL CHKXER( 'ZGEMQRT', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL ZGEMQRT( 'L', 'N', 1, 1, 1, 1, A, 1, T, 1, C, 0, W, INFO )
      CALL CHKXER( 'ZGEMQRT', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of ZERRQRT
*
      END
