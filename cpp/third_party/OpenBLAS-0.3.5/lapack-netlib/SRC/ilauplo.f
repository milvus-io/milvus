*> \brief \b ILAUPLO
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ILAUPLO + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/ilauplo.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/ilauplo.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/ilauplo.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       INTEGER FUNCTION ILAUPLO( UPLO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> This subroutine translated from a character string specifying a
*> upper- or lower-triangular matrix to the relevant BLAST-specified
*> integer constant.
*>
*> ILAUPLO returns an INTEGER.  If ILAUPLO < 0, then the input is not
*> a character indicating an upper- or lower-triangular matrix.
*> Otherwise ILAUPLO returns the constant value corresponding to UPLO.
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
      INTEGER FUNCTION ILAUPLO( UPLO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER BLAS_UPPER, BLAS_LOWER
      PARAMETER ( BLAS_UPPER = 121, BLAS_LOWER = 122 )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. Executable Statements ..
      IF( LSAME( UPLO, 'U' ) ) THEN
         ILAUPLO = BLAS_UPPER
      ELSE IF( LSAME( UPLO, 'L' ) ) THEN
         ILAUPLO = BLAS_LOWER
      ELSE
         ILAUPLO = -1
      END IF
      RETURN
*
*     End of ILAUPLO
*
      END
