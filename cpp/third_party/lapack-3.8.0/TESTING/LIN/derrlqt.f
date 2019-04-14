*> \brief \b DERLQT
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DERRLQT( PATH, NUNIT )
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
*> DERRLQT tests the error exits for the DOUBLE PRECISION routines
*> that use the LQT decomposition of a general matrix.
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
      SUBROUTINE DERRLQT( PATH, NUNIT )
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
      EXTERNAL           ALAESM, CHKXER, DGELQT3, DGELQT,
     $                   DGEMLQT
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
*     Error exits for LQT factorization
*
*     DGELQT
*
      SRNAMT = 'DGELQT'
      INFOT = 1
      CALL DGELQT( -1, 0, 1, A, 1, T, 1, W, INFO )
      CALL CHKXER( 'DGELQT', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DGELQT( 0, -1, 1, A, 1, T, 1, W, INFO )
      CALL CHKXER( 'DGELQT', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DGELQT( 0, 0, 0, A, 1, T, 1, W, INFO )
      CALL CHKXER( 'DGELQT', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DGELQT( 2, 1, 1, A, 1, T, 1, W, INFO )
      CALL CHKXER( 'DGELQT', INFOT, NOUT, LERR, OK )
      INFOT = 7
      CALL DGELQT( 2, 2, 2, A, 2, T, 1, W, INFO )
      CALL CHKXER( 'DGELQT', INFOT, NOUT, LERR, OK )
*
*     DGELQT3
*
      SRNAMT = 'DGELQT3'
      INFOT = 1
      CALL DGELQT3( -1, 0, A, 1, T, 1, INFO )
      CALL CHKXER( 'DGELQT3', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DGELQT3( 0, -1, A, 1, T, 1, INFO )
      CALL CHKXER( 'DGELQT3', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DGELQT3( 2, 2, A, 1, T, 1, INFO )
      CALL CHKXER( 'DGELQT3', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL DGELQT3( 2, 2, A, 2, T, 1, INFO )
      CALL CHKXER( 'DGELQT3', INFOT, NOUT, LERR, OK )
*
*     DGEMLQT
*
      SRNAMT = 'DGEMLQT'
      INFOT = 1
      CALL DGEMLQT( '/', 'N', 0, 0, 0, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'DGEMLQT', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DGEMLQT( 'L', '/', 0, 0, 0, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'DGEMLQT', INFOT, NOUT, LERR, OK )
      INFOT = 3
      CALL DGEMLQT( 'L', 'N', -1, 0, 0, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'DGEMLQT', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DGEMLQT( 'L', 'N', 0, -1, 0, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'DGEMLQT', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DGEMLQT( 'L', 'N', 0, 0, -1, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'DGEMLQT', INFOT, NOUT, LERR, OK )
      INFOT = 5
      CALL DGEMLQT( 'R', 'N', 0, 0, -1, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'DGEMLQT', INFOT, NOUT, LERR, OK )
      INFOT = 6
      CALL DGEMLQT( 'L', 'N', 0, 0, 0, 0, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'DGEMLQT', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL DGEMLQT( 'R', 'N', 2, 2, 2, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'DGEMLQT', INFOT, NOUT, LERR, OK )
      INFOT = 8
      CALL DGEMLQT( 'L', 'N', 2, 2, 2, 1, A, 1, T, 1, C, 1, W, INFO )
      CALL CHKXER( 'DGEMLQT', INFOT, NOUT, LERR, OK )
      INFOT = 10
      CALL DGEMLQT( 'R', 'N', 1, 1, 1, 1, A, 1, T, 0, C, 1, W, INFO )
      CALL CHKXER( 'DGEMLQT', INFOT, NOUT, LERR, OK )
      INFOT = 12
      CALL DGEMLQT( 'L', 'N', 1, 1, 1, 1, A, 1, T, 1, C, 0, W, INFO )
      CALL CHKXER( 'DGEMLQT', INFOT, NOUT, LERR, OK )
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of DERRLQT
*
      END
