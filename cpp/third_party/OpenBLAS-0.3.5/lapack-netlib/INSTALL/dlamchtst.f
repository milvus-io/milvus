*> \brief \b DLAMCHTST
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*      PROGRAM DLAMCHTST
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
*> \ingroup auxOTHERauxiliary
*
*  =====================================================================      PROGRAM DLAMCHTST
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
* =====================================================================
*
*     .. Local Scalars ..
      DOUBLE PRECISION   BASE, EMAX, EMIN, EPS, PREC, RMAX, RMIN, RND,
     $                   SFMIN, T
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH
      EXTERNAL           DLAMCH
*     ..
*     .. Executable Statements ..
*
      EPS   = DLAMCH( 'Epsilon' )
      SFMIN = DLAMCH( 'Safe minimum' )
      BASE  = DLAMCH( 'Base' )
      PREC  = DLAMCH( 'Precision' )
      T     = DLAMCH( 'Number of digits in mantissa' )
      RND   = DLAMCH( 'Rounding mode' )
      EMIN  = DLAMCH( 'Minimum exponent' )
      RMIN  = DLAMCH( 'Underflow threshold' )
      EMAX  = DLAMCH( 'Largest exponent' )
      RMAX  = DLAMCH( 'Overflow threshold' )
*
      WRITE( 6, * )' Epsilon                      = ', EPS
      WRITE( 6, * )' Safe minimum                 = ', SFMIN
      WRITE( 6, * )' Base                         = ', BASE
      WRITE( 6, * )' Precision                    = ', PREC
      WRITE( 6, * )' Number of digits in mantissa = ', T
      WRITE( 6, * )' Rounding mode                = ', RND
      WRITE( 6, * )' Minimum exponent             = ', EMIN
      WRITE( 6, * )' Underflow threshold          = ', RMIN
      WRITE( 6, * )' Largest exponent             = ', EMAX
      WRITE( 6, * )' Overflow threshold           = ', RMAX
      WRITE( 6, * )' Reciprocal of safe minimum   = ', 1 / SFMIN
*
      END
