*> \brief \b DSLECT
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       LOGICAL          FUNCTION DSLECT( ZR, ZI )
*
*       .. Scalar Arguments ..
*       DOUBLE PRECISION   ZI, ZR
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DSLECT returns .TRUE. if the eigenvalue ZR+sqrt(-1)*ZI is to be
*> selected, and otherwise it returns .FALSE.
*> It is used by DCHK41 to test if DGEES successfully sorts eigenvalues,
*> and by DCHK43 to test if DGEESX successfully sorts eigenvalues.
*>
*> The common block /SSLCT/ controls how eigenvalues are selected.
*> If SELOPT = 0, then DSLECT return .TRUE. when ZR is less than zero,
*> and .FALSE. otherwise.
*> If SELOPT is at least 1, DSLECT returns SELVAL(SELOPT) and adds 1
*> to SELOPT, cycling back to 1 at SELMAX.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] ZR
*> \verbatim
*>          ZR is DOUBLE PRECISION
*>          The real part of a complex eigenvalue ZR + i*ZI.
*> \endverbatim
*>
*> \param[in] ZI
*> \verbatim
*>          ZI is DOUBLE PRECISION
*>          The imaginary part of a complex eigenvalue ZR + i*ZI.
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
*> \date June 2016
*
*> \ingroup double_eig
*
*  =====================================================================
      LOGICAL          FUNCTION DSLECT( ZR, ZI )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      DOUBLE PRECISION   ZI, ZR
*     ..
*
*  =====================================================================
*
*     .. Arrays in Common ..
      LOGICAL            SELVAL( 20 )
      DOUBLE PRECISION   SELWI( 20 ), SELWR( 20 )
*     ..
*     .. Scalars in Common ..
      INTEGER            SELDIM, SELOPT
*     ..
*     .. Common blocks ..
      COMMON             / SSLCT / SELOPT, SELDIM, SELVAL, SELWR, SELWI
*     ..
*     .. Local Scalars ..
      INTEGER            I
      DOUBLE PRECISION   RMIN, X
*     ..
*     .. Parameters ..
      DOUBLE PRECISION   ZERO
      PARAMETER          ( ZERO = 0.0D0 )
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAPY2
      EXTERNAL           DLAPY2
*     ..
*     .. Executable Statements ..
*
      IF( SELOPT.EQ.0 ) THEN
         DSLECT = ( ZR.LT.ZERO )
      ELSE
         RMIN = DLAPY2( ZR-SELWR( 1 ), ZI-SELWI( 1 ) )
         DSLECT = SELVAL( 1 )
         DO 10 I = 2, SELDIM
            X = DLAPY2( ZR-SELWR( I ), ZI-SELWI( I ) )
            IF( X.LE.RMIN ) THEN
               RMIN = X
               DSLECT = SELVAL( I )
            END IF
   10    CONTINUE
      END IF
      RETURN
*
*     End of DSLECT
*
      END
