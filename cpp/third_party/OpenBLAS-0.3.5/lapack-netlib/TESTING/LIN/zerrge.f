*> \brief \b ZERRGE
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZERRGE( PATH, NUNIT )
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
*> ZERRGE tests the error exits for the COMPLEX*16 routines
*> for general matrices.
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
      SUBROUTINE ZERRGE( PATH, NUNIT )
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
      CHARACTER*2        C2
      INTEGER            I, INFO, J
      DOUBLE PRECISION   ANRM, CCOND, RCOND
*     ..
*     .. Local Arrays ..
      INTEGER            IP( NMAX )
      DOUBLE PRECISION   R( NMAX ), R1( NMAX ), R2( NMAX )
      COMPLEX*16         A( NMAX, NMAX ), AF( NMAX, NMAX ), B( NMAX ),
     $                   W( 2*NMAX ), X( NMAX )
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      EXTERNAL           LSAMEN
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, ZGBCON, ZGBEQU, ZGBRFS, ZGBTF2,
     $                   ZGBTRF, ZGBTRS, ZGECON, ZGEEQU, ZGERFS, ZGETF2,
     $                   ZGETRF, ZGETRI, ZGETRS
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
      C2 = PATH( 2: 3 )
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
         R1( J ) = 0.D0
         R2( J ) = 0.D0
         W( J ) = 0.D0
         X( J ) = 0.D0
         IP( J ) = J
   20 CONTINUE
      OK = .TRUE.
*
*     Test error exits of the routines that use the LU decomposition
*     of a general matrix.
*
      IF( LSAMEN( 2, C2, 'GE' ) ) THEN
*
*        ZGETRF
*
         SRNAMT = 'ZGETRF'
         INFOT = 1
         CALL ZGETRF( -1, 0, A, 1, IP, INFO )
         CALL CHKXER( 'ZGETRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGETRF( 0, -1, A, 1, IP, INFO )
         CALL CHKXER( 'ZGETRF', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZGETRF( 2, 1, A, 1, IP, INFO )
         CALL CHKXER( 'ZGETRF', INFOT, NOUT, LERR, OK )
*
*        ZGETF2
*
         SRNAMT = 'ZGETF2'
         INFOT = 1
         CALL ZGETF2( -1, 0, A, 1, IP, INFO )
         CALL CHKXER( 'ZGETF2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGETF2( 0, -1, A, 1, IP, INFO )
         CALL CHKXER( 'ZGETF2', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZGETF2( 2, 1, A, 1, IP, INFO )
         CALL CHKXER( 'ZGETF2', INFOT, NOUT, LERR, OK )
*
*        ZGETRI
*
         SRNAMT = 'ZGETRI'
         INFOT = 1
         CALL ZGETRI( -1, A, 1, IP, W, 1, INFO )
         CALL CHKXER( 'ZGETRI', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGETRI( 2, A, 1, IP, W, 2, INFO )
         CALL CHKXER( 'ZGETRI', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZGETRI( 2, A, 2, IP, W, 1, INFO )
         CALL CHKXER( 'ZGETRI', INFOT, NOUT, LERR, OK )
*
*        ZGETRS
*
         SRNAMT = 'ZGETRS'
         INFOT = 1
         CALL ZGETRS( '/', 0, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'ZGETRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGETRS( 'N', -1, 0, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'ZGETRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGETRS( 'N', 0, -1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'ZGETRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGETRS( 'N', 2, 1, A, 1, IP, B, 2, INFO )
         CALL CHKXER( 'ZGETRS', INFOT, NOUT, LERR, OK )
         INFOT = 8
         CALL ZGETRS( 'N', 2, 1, A, 2, IP, B, 1, INFO )
         CALL CHKXER( 'ZGETRS', INFOT, NOUT, LERR, OK )
*
*        ZGERFS
*
         SRNAMT = 'ZGERFS'
         INFOT = 1
         CALL ZGERFS( '/', 0, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'ZGERFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGERFS( 'N', -1, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1, R2,
     $                W, R, INFO )
         CALL CHKXER( 'ZGERFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGERFS( 'N', 0, -1, A, 1, AF, 1, IP, B, 1, X, 1, R1, R2,
     $                W, R, INFO )
         CALL CHKXER( 'ZGERFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGERFS( 'N', 2, 1, A, 1, AF, 2, IP, B, 2, X, 2, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'ZGERFS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZGERFS( 'N', 2, 1, A, 2, AF, 1, IP, B, 2, X, 2, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'ZGERFS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL ZGERFS( 'N', 2, 1, A, 2, AF, 2, IP, B, 1, X, 2, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'ZGERFS', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL ZGERFS( 'N', 2, 1, A, 2, AF, 2, IP, B, 2, X, 1, R1, R2, W,
     $                R, INFO )
         CALL CHKXER( 'ZGERFS', INFOT, NOUT, LERR, OK )
*
*        ZGECON
*
         SRNAMT = 'ZGECON'
         INFOT = 1
         CALL ZGECON( '/', 0, A, 1, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'ZGECON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGECON( '1', -1, A, 1, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'ZGECON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZGECON( '1', 2, A, 1, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'ZGECON', INFOT, NOUT, LERR, OK )
*
*        ZGEEQU
*
         SRNAMT = 'ZGEEQU'
         INFOT = 1
         CALL ZGEEQU( -1, 0, A, 1, R1, R2, RCOND, CCOND, ANRM, INFO )
         CALL CHKXER( 'ZGEEQU', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGEEQU( 0, -1, A, 1, R1, R2, RCOND, CCOND, ANRM, INFO )
         CALL CHKXER( 'ZGEEQU', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZGEEQU( 2, 2, A, 1, R1, R2, RCOND, CCOND, ANRM, INFO )
         CALL CHKXER( 'ZGEEQU', INFOT, NOUT, LERR, OK )
*
*     Test error exits of the routines that use the LU decomposition
*     of a general band matrix.
*
      ELSE IF( LSAMEN( 2, C2, 'GB' ) ) THEN
*
*        ZGBTRF
*
         SRNAMT = 'ZGBTRF'
         INFOT = 1
         CALL ZGBTRF( -1, 0, 0, 0, A, 1, IP, INFO )
         CALL CHKXER( 'ZGBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGBTRF( 0, -1, 0, 0, A, 1, IP, INFO )
         CALL CHKXER( 'ZGBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGBTRF( 1, 1, -1, 0, A, 1, IP, INFO )
         CALL CHKXER( 'ZGBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZGBTRF( 1, 1, 0, -1, A, 1, IP, INFO )
         CALL CHKXER( 'ZGBTRF', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZGBTRF( 2, 2, 1, 1, A, 3, IP, INFO )
         CALL CHKXER( 'ZGBTRF', INFOT, NOUT, LERR, OK )
*
*        ZGBTF2
*
         SRNAMT = 'ZGBTF2'
         INFOT = 1
         CALL ZGBTF2( -1, 0, 0, 0, A, 1, IP, INFO )
         CALL CHKXER( 'ZGBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGBTF2( 0, -1, 0, 0, A, 1, IP, INFO )
         CALL CHKXER( 'ZGBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGBTF2( 1, 1, -1, 0, A, 1, IP, INFO )
         CALL CHKXER( 'ZGBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZGBTF2( 1, 1, 0, -1, A, 1, IP, INFO )
         CALL CHKXER( 'ZGBTF2', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZGBTF2( 2, 2, 1, 1, A, 3, IP, INFO )
         CALL CHKXER( 'ZGBTF2', INFOT, NOUT, LERR, OK )
*
*        ZGBTRS
*
         SRNAMT = 'ZGBTRS'
         INFOT = 1
         CALL ZGBTRS( '/', 0, 0, 0, 1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'ZGBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGBTRS( 'N', -1, 0, 0, 1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'ZGBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGBTRS( 'N', 1, -1, 0, 1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'ZGBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZGBTRS( 'N', 1, 0, -1, 1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'ZGBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGBTRS( 'N', 1, 0, 0, -1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'ZGBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZGBTRS( 'N', 2, 1, 1, 1, A, 3, IP, B, 2, INFO )
         CALL CHKXER( 'ZGBTRS', INFOT, NOUT, LERR, OK )
         INFOT = 10
         CALL ZGBTRS( 'N', 2, 0, 0, 1, A, 1, IP, B, 1, INFO )
         CALL CHKXER( 'ZGBTRS', INFOT, NOUT, LERR, OK )
*
*        ZGBRFS
*
         SRNAMT = 'ZGBRFS'
         INFOT = 1
         CALL ZGBRFS( '/', 0, 0, 0, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1,
     $                R2, W, R, INFO )
         CALL CHKXER( 'ZGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGBRFS( 'N', -1, 0, 0, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1,
     $                R2, W, R, INFO )
         CALL CHKXER( 'ZGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGBRFS( 'N', 1, -1, 0, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1,
     $                R2, W, R, INFO )
         CALL CHKXER( 'ZGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZGBRFS( 'N', 1, 0, -1, 0, A, 1, AF, 1, IP, B, 1, X, 1, R1,
     $                R2, W, R, INFO )
         CALL CHKXER( 'ZGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 5
         CALL ZGBRFS( 'N', 1, 0, 0, -1, A, 1, AF, 1, IP, B, 1, X, 1, R1,
     $                R2, W, R, INFO )
         CALL CHKXER( 'ZGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 7
         CALL ZGBRFS( 'N', 2, 1, 1, 1, A, 2, AF, 4, IP, B, 2, X, 2, R1,
     $                R2, W, R, INFO )
         CALL CHKXER( 'ZGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 9
         CALL ZGBRFS( 'N', 2, 1, 1, 1, A, 3, AF, 3, IP, B, 2, X, 2, R1,
     $                R2, W, R, INFO )
         CALL CHKXER( 'ZGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 12
         CALL ZGBRFS( 'N', 2, 0, 0, 1, A, 1, AF, 1, IP, B, 1, X, 2, R1,
     $                R2, W, R, INFO )
         CALL CHKXER( 'ZGBRFS', INFOT, NOUT, LERR, OK )
         INFOT = 14
         CALL ZGBRFS( 'N', 2, 0, 0, 1, A, 1, AF, 1, IP, B, 2, X, 1, R1,
     $                R2, W, R, INFO )
         CALL CHKXER( 'ZGBRFS', INFOT, NOUT, LERR, OK )
*
*        ZGBCON
*
         SRNAMT = 'ZGBCON'
         INFOT = 1
         CALL ZGBCON( '/', 0, 0, 0, A, 1, IP, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'ZGBCON', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGBCON( '1', -1, 0, 0, A, 1, IP, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'ZGBCON', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGBCON( '1', 1, -1, 0, A, 1, IP, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'ZGBCON', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZGBCON( '1', 1, 0, -1, A, 1, IP, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'ZGBCON', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZGBCON( '1', 2, 1, 1, A, 3, IP, ANRM, RCOND, W, R, INFO )
         CALL CHKXER( 'ZGBCON', INFOT, NOUT, LERR, OK )
*
*        ZGBEQU
*
         SRNAMT = 'ZGBEQU'
         INFOT = 1
         CALL ZGBEQU( -1, 0, 0, 0, A, 1, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'ZGBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 2
         CALL ZGBEQU( 0, -1, 0, 0, A, 1, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'ZGBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 3
         CALL ZGBEQU( 1, 1, -1, 0, A, 1, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'ZGBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 4
         CALL ZGBEQU( 1, 1, 0, -1, A, 1, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'ZGBEQU', INFOT, NOUT, LERR, OK )
         INFOT = 6
         CALL ZGBEQU( 2, 2, 1, 1, A, 2, R1, R2, RCOND, CCOND, ANRM,
     $                INFO )
         CALL CHKXER( 'ZGBEQU', INFOT, NOUT, LERR, OK )
      END IF
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of ZERRGE
*
      END
