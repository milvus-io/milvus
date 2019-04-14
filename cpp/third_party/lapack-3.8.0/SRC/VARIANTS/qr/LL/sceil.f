C> \brief \b SCEIL
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       REAL FUNCTION SCEIL( A )
*
*       .. Scalar Arguments ..
*       REAL A
*       ..
*
*    =====================================================================
*
*       .. Intrinsic Functions ..
* 	      INTRINSIC          INT
*       ..
*       .. Executable Statements ..*
*
*       IF (A-INT(A).EQ.0) THEN
*           SCEIL = A
*       ELSE IF (A.GT.0) THEN
*           SCEIL = INT(A)+1;
*       ELSE
*           SCEIL = INT(A)
*       END IF
*
*       RETURN
*
*       END
*  Purpose
*  =======
*
C>\details \b Purpose:
C>\verbatim
C>\endverbatim
*
*  Arguments:
*  ==========
*
*
*  Authors:
*  ========
*
C> \author Univ. of Tennessee
C> \author Univ. of California Berkeley
C> \author Univ. of Colorado Denver
C> \author NAG Ltd.
*
C> \date December 2016
*
C> \ingroup variantsOTHERcomputational
*
*  =====================================================================
      REAL FUNCTION SCEIL( A )
*
*  -- LAPACK computational routine (version 3.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..*
      REAL A
*     ..
*
*  =====================================================================
*
*     .. Intrinsic Functions ..
	      INTRINSIC          INT
*     ..
*     .. Executable Statements ..*
*
      IF (A-INT(A).EQ.0) THEN
          SCEIL = A
      ELSE IF (A.GT.0) THEN
          SCEIL = INT(A)+1;
      ELSE
          SCEIL = INT(A)
      END IF

      RETURN
*
      END
