*> \brief \b CLARND
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       COMPLEX FUNCTION CLARND( IDIST, ISEED )
*
*       .. Scalar Arguments ..
*       INTEGER            IDIST
*       ..
*       .. Array Arguments ..
*       INTEGER            ISEED( 4 )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CLARND returns a random complex number from a uniform or normal
*> distribution.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] IDIST
*> \verbatim
*>          IDIST is INTEGER
*>          Specifies the distribution of the random numbers:
*>          = 1:  real and imaginary parts each uniform (0,1)
*>          = 2:  real and imaginary parts each uniform (-1,1)
*>          = 3:  real and imaginary parts each normal (0,1)
*>          = 4:  uniformly distributed on the disc abs(z) <= 1
*>          = 5:  uniformly distributed on the circle abs(z) = 1
*> \endverbatim
*>
*> \param[in,out] ISEED
*> \verbatim
*>          ISEED is INTEGER array, dimension (4)
*>          On entry, the seed of the random number generator; the array
*>          elements must be between 0 and 4095, and ISEED(4) must be
*>          odd.
*>          On exit, the seed is updated.
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
*> \ingroup complex_matgen
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  This routine calls the auxiliary routine SLARAN to generate a random
*>  real number from a uniform (0,1) distribution. The Box-Muller method
*>  is used to transform numbers from a uniform to a normal distribution.
*> \endverbatim
*>
*  =====================================================================
      COMPLEX FUNCTION CLARND( IDIST, ISEED )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            IDIST
*     ..
*     .. Array Arguments ..
      INTEGER            ISEED( 4 )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE, TWO
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0, TWO = 2.0E+0 )
      REAL               TWOPI
      PARAMETER          ( TWOPI = 6.2831853071795864769252867663E+0 )
*     ..
*     .. Local Scalars ..
      REAL               T1, T2
*     ..
*     .. External Functions ..
      REAL               SLARAN
      EXTERNAL           SLARAN
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          CMPLX, EXP, LOG, SQRT
*     ..
*     .. Executable Statements ..
*
*     Generate a pair of real random numbers from a uniform (0,1)
*     distribution
*
      T1 = SLARAN( ISEED )
      T2 = SLARAN( ISEED )
*
      IF( IDIST.EQ.1 ) THEN
*
*        real and imaginary parts each uniform (0,1)
*
         CLARND = CMPLX( T1, T2 )
      ELSE IF( IDIST.EQ.2 ) THEN
*
*        real and imaginary parts each uniform (-1,1)
*
         CLARND = CMPLX( TWO*T1-ONE, TWO*T2-ONE )
      ELSE IF( IDIST.EQ.3 ) THEN
*
*        real and imaginary parts each normal (0,1)
*
         CLARND = SQRT( -TWO*LOG( T1 ) )*EXP( CMPLX( ZERO, TWOPI*T2 ) )
      ELSE IF( IDIST.EQ.4 ) THEN
*
*        uniform distribution on the unit disc abs(z) <= 1
*
         CLARND = SQRT( T1 )*EXP( CMPLX( ZERO, TWOPI*T2 ) )
      ELSE IF( IDIST.EQ.5 ) THEN
*
*        uniform distribution on the unit circle abs(z) = 1
*
         CLARND = EXP( CMPLX( ZERO, TWOPI*T2 ) )
      END IF
      RETURN
*
*     End of CLARND
*
      END
