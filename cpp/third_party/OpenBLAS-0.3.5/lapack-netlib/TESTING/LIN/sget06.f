*> \brief \b SGET06
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       REAL             FUNCTION SGET06( RCOND, RCONDC )
*
*       .. Scalar Arguments ..
*       REAL               RCOND, RCONDC
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SGET06 computes a test ratio to compare two values for RCOND.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] RCOND
*> \verbatim
*>          RCOND is REAL
*>          The estimate of the reciprocal of the condition number of A,
*>          as computed by SGECON.
*> \endverbatim
*>
*> \param[in] RCONDC
*> \verbatim
*>          RCONDC is REAL
*>          The reciprocal of the condition number of A, computed as
*>          ( 1/norm(A) ) / norm(inv(A)).
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
      REAL             FUNCTION SGET06( RCOND, RCONDC )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      REAL               RCOND, RCONDC
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
*     ..
*     .. Local Scalars ..
      REAL               EPS, RAT
*     ..
*     .. External Functions ..
      REAL               SLAMCH
      EXTERNAL           SLAMCH
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN
*     ..
*     .. Executable Statements ..
*
      EPS = SLAMCH( 'Epsilon' )
      IF( RCOND.GT.ZERO ) THEN
         IF( RCONDC.GT.ZERO ) THEN
            RAT = MAX( RCOND, RCONDC ) / MIN( RCOND, RCONDC ) -
     $            ( ONE-EPS )
         ELSE
            RAT = RCOND / EPS
         END IF
      ELSE
         IF( RCONDC.GT.ZERO ) THEN
            RAT = RCONDC / EPS
         ELSE
            RAT = ZERO
         END IF
      END IF
      SGET06 = RAT
      RETURN
*
*     End of SGET06
*
      END
