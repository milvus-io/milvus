*> \brief \b ZERRPS
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZERRPS( PATH, NUNIT )
*
*       .. Scalar Arguments ..
*       INTEGER            NUNIT
*       CHARACTER*3        PATH
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZERRPS tests the error exits for the COMPLEX routines
*> for ZPSTRF.
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
      SUBROUTINE ZERRPS( PATH, NUNIT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            NUNIT
      CHARACTER*3        PATH
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            NMAX
      PARAMETER          ( NMAX = 4 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, INFO, J, RANK
*     ..
*     .. Local Arrays ..
      COMPLEX*16         A( NMAX, NMAX )
      DOUBLE PRECISION   RWORK( 2*NMAX )
      INTEGER            PIV( NMAX )
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, ZPSTF2, ZPSTRF
*     ..
*     .. Scalars in Common ..
      INTEGER            INFOT, NOUT
      LOGICAL            LERR, OK
      CHARACTER*32       SRNAMT
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
      DO 110 J = 1, NMAX
         DO 100 I = 1, NMAX
            A( I, J ) = 1.D0 / DBLE( I+J )
*
  100    CONTINUE
         PIV( J ) = J
         RWORK( J ) = 0.D0
         RWORK( NMAX+J ) = 0.D0
*
  110 CONTINUE
      OK = .TRUE.
*
*
*        Test error exits of the routines that use the Cholesky
*        decomposition of an Hermitian positive semidefinite matrix.
*
*        ZPSTRF
*
      SRNAMT = 'ZPSTRF'
      INFOT = 1
      CALL ZPSTRF( '/', 0, A, 1, PIV, RANK, -1.D0, RWORK, INFO )
      CALL CHKXER( 'ZPSTRF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZPSTRF( 'U', -1, A, 1, PIV, RANK, -1.D0, RWORK, INFO )
      CALL CHKXER( 'ZPSTRF', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZPSTRF( 'U', 2, A, 1, PIV, RANK, -1.D0, RWORK, INFO )
      CALL CHKXER( 'ZPSTRF', INFOT, NOUT, LERR, OK )
*
*        ZPSTF2
*
      SRNAMT = 'ZPSTF2'
      INFOT = 1
      CALL ZPSTF2( '/', 0, A, 1, PIV, RANK, -1.D0, RWORK, INFO )
      CALL CHKXER( 'ZPSTF2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL ZPSTF2( 'U', -1, A, 1, PIV, RANK, -1.D0, RWORK, INFO )
      CALL CHKXER( 'ZPSTF2', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL ZPSTF2( 'U', 2, A, 1, PIV, RANK, -1.D0, RWORK, INFO )
      CALL CHKXER( 'ZPSTF2', INFOT, NOUT, LERR, OK )
*
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of ZERRPS
*
      END
