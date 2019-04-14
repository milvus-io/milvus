*> \brief \b ILADIAG
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ILADIAG + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/iladiag.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/iladiag.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/iladiag.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       INTEGER FUNCTION ILADIAG( DIAG )
*
*       .. Scalar Arguments ..
*       CHARACTER          DIAG
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> This subroutine translated from a character string specifying if a
*> matrix has unit diagonal or not to the relevant BLAST-specified
*> integer constant.
*>
*> ILADIAG returns an INTEGER.  If ILADIAG < 0, then the input is not a
*> character indicating a unit or non-unit diagonal.  Otherwise ILADIAG
*> returns the constant value corresponding to DIAG.
*> \endverbatim
*
*  Arguments:
*  ==========
*
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
*> \ingroup auxOTHERcomputational
*
*  =====================================================================
      INTEGER FUNCTION ILADIAG( DIAG )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          DIAG
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER BLAS_NON_UNIT_DIAG, BLAS_UNIT_DIAG
      PARAMETER ( BLAS_NON_UNIT_DIAG = 131, BLAS_UNIT_DIAG = 132 )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. Executable Statements ..
      IF( LSAME( DIAG, 'N' ) ) THEN
         ILADIAG = BLAS_NON_UNIT_DIAG
      ELSE IF( LSAME( DIAG, 'U' ) ) THEN
         ILADIAG = BLAS_UNIT_DIAG
      ELSE
         ILADIAG = -1
      END IF
      RETURN
*
*     End of ILADIAG
*
      END
