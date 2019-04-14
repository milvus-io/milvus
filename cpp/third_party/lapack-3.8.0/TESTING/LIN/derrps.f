*> \brief \b DERRPS
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DERRPS( PATH, NUNIT )
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
*> DERRPS tests the error exits for the DOUBLE PRECISION routines
*> for DPSTRF.
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
      SUBROUTINE DERRPS( PATH, NUNIT )
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
      DOUBLE PRECISION   A( NMAX, NMAX ), WORK( 2*NMAX )
      INTEGER            PIV( NMAX )
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAESM, CHKXER, DPSTF2, DPSTRF
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
         WORK( J ) = 0.D0
         WORK( NMAX+J ) = 0.D0
*
  110 CONTINUE
      OK = .TRUE.
*
*
*        Test error exits of the routines that use the Cholesky
*        decomposition of a symmetric positive semidefinite matrix.
*
*        DPSTRF
*
      SRNAMT = 'DPSTRF'
      INFOT = 1
      CALL DPSTRF( '/', 0, A, 1, PIV, RANK, -1.D0, WORK, INFO )
      CALL CHKXER( 'DPSTRF', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DPSTRF( 'U', -1, A, 1, PIV, RANK, -1.D0, WORK, INFO )
      CALL CHKXER( 'DPSTRF', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DPSTRF( 'U', 2, A, 1, PIV, RANK, -1.D0, WORK, INFO )
      CALL CHKXER( 'DPSTRF', INFOT, NOUT, LERR, OK )
*
*        DPSTF2
*
      SRNAMT = 'DPSTF2'
      INFOT = 1
      CALL DPSTF2( '/', 0, A, 1, PIV, RANK, -1.D0, WORK, INFO )
      CALL CHKXER( 'DPSTF2', INFOT, NOUT, LERR, OK )
      INFOT = 2
      CALL DPSTF2( 'U', -1, A, 1, PIV, RANK, -1.D0, WORK, INFO )
      CALL CHKXER( 'DPSTF2', INFOT, NOUT, LERR, OK )
      INFOT = 4
      CALL DPSTF2( 'U', 2, A, 1, PIV, RANK, -1.D0, WORK, INFO )
      CALL CHKXER( 'DPSTF2', INFOT, NOUT, LERR, OK )
*
*
*     Print a summary line.
*
      CALL ALAESM( PATH, OK, NOUT )
*
      RETURN
*
*     End of DERRPS
*
      END
