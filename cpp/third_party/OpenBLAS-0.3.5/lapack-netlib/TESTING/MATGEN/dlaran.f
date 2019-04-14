*> \brief \b DLARAN
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       DOUBLE PRECISION FUNCTION DLARAN( ISEED )
*
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
*> DLARAN returns a random real number from a uniform (0,1)
*> distribution.
*> \endverbatim
*
*  Arguments:
*  ==========
*
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
*> \ingroup list_temp
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  This routine uses a multiplicative congruential method with modulus
*>  2**48 and multiplier 33952834046453 (see G.S.Fishman,
*>  'Multiplicative congruential random number generators with modulus
*>  2**b: an exhaustive analysis for b = 32 and a partial analysis for
*>  b = 48', Math. Comp. 189, pp 331-344, 1990).
*>
*>  48-bit integers are stored in 4 integer array elements with 12 bits
*>  per element. Hence the routine is portable across machines with
*>  integers of 32 bits or more.
*> \endverbatim
*>
*  =====================================================================
      DOUBLE PRECISION FUNCTION DLARAN( ISEED )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Array Arguments ..
      INTEGER            ISEED( 4 )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            M1, M2, M3, M4
      PARAMETER          ( M1 = 494, M2 = 322, M3 = 2508, M4 = 2549 )
      DOUBLE PRECISION   ONE
      PARAMETER          ( ONE = 1.0D+0 )
      INTEGER            IPW2
      DOUBLE PRECISION   R
      PARAMETER          ( IPW2 = 4096, R = ONE / IPW2 )
*     ..
*     .. Local Scalars ..
      INTEGER            IT1, IT2, IT3, IT4
      DOUBLE PRECISION   RNDOUT
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE, MOD
*     ..
*     .. Executable Statements ..
  10  CONTINUE
*
*     multiply the seed by the multiplier modulo 2**48
*
      IT4 = ISEED( 4 )*M4
      IT3 = IT4 / IPW2
      IT4 = IT4 - IPW2*IT3
      IT3 = IT3 + ISEED( 3 )*M4 + ISEED( 4 )*M3
      IT2 = IT3 / IPW2
      IT3 = IT3 - IPW2*IT2
      IT2 = IT2 + ISEED( 2 )*M4 + ISEED( 3 )*M3 + ISEED( 4 )*M2
      IT1 = IT2 / IPW2
      IT2 = IT2 - IPW2*IT1
      IT1 = IT1 + ISEED( 1 )*M4 + ISEED( 2 )*M3 + ISEED( 3 )*M2 +
     $      ISEED( 4 )*M1
      IT1 = MOD( IT1, IPW2 )
*
*     return updated seed
*
      ISEED( 1 ) = IT1
      ISEED( 2 ) = IT2
      ISEED( 3 ) = IT3
      ISEED( 4 ) = IT4
*
*     convert 48-bit integer to a real number in the interval (0,1)
*
      RNDOUT = R*( DBLE( IT1 )+R*( DBLE( IT2 )+R*( DBLE( IT3 )+R*
     $         ( DBLE( IT4 ) ) ) ) )
*
      IF (RNDOUT.EQ.1.0D+0) THEN
*        If a real number has n bits of precision, and the first
*        n bits of the 48-bit integer above happen to be all 1 (which
*        will occur about once every 2**n calls), then DLARAN will
*        be rounded to exactly 1.0.
*        Since DLARAN is not supposed to return exactly 0.0 or 1.0
*        (and some callers of DLARAN, such as CLARND, depend on that),
*        the statistically correct thing to do in this situation is
*        simply to iterate again.
*        N.B. the case DLARAN = 0.0 should not be possible.
*
         GOTO 10
      END IF
*
      DLARAN = RNDOUT
      RETURN
*
*     End of DLARAN
*
      END
