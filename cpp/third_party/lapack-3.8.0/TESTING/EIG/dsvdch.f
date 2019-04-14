*> \brief \b DSVDCH
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DSVDCH( N, S, E, SVD, TOL, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, N
*       DOUBLE PRECISION   TOL
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   E( * ), S( * ), SVD( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DSVDCH checks to see if SVD(1) ,..., SVD(N) are accurate singular
*> values of the bidiagonal matrix B with diagonal entries
*> S(1) ,..., S(N) and superdiagonal entries E(1) ,..., E(N-1)).
*> It does this by expanding each SVD(I) into an interval
*> [SVD(I) * (1-EPS) , SVD(I) * (1+EPS)], merging overlapping intervals
*> if any, and using Sturm sequences to count and verify whether each
*> resulting interval has the correct number of singular values (using
*> DSVDCT). Here EPS=TOL*MAX(N/10,1)*MAZHEP, where MACHEP is the
*> machine precision. The routine assumes the singular values are sorted
*> with SVD(1) the largest and SVD(N) smallest.  If each interval
*> contains the correct number of singular values, INFO = 0 is returned,
*> otherwise INFO is the index of the first singular value in the first
*> bad interval.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The dimension of the bidiagonal matrix B.
*> \endverbatim
*>
*> \param[in] S
*> \verbatim
*>          S is DOUBLE PRECISION array, dimension (N)
*>          The diagonal entries of the bidiagonal matrix B.
*> \endverbatim
*>
*> \param[in] E
*> \verbatim
*>          E is DOUBLE PRECISION array, dimension (N-1)
*>          The superdiagonal entries of the bidiagonal matrix B.
*> \endverbatim
*>
*> \param[in] SVD
*> \verbatim
*>          SVD is DOUBLE PRECISION array, dimension (N)
*>          The computed singular values to be checked.
*> \endverbatim
*>
*> \param[in] TOL
*> \verbatim
*>          TOL is DOUBLE PRECISION
*>          Error tolerance for checking, a multiplier of the
*>          machine precision.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          =0 if the singular values are all correct (to within
*>             1 +- TOL*MAZHEPS)
*>          >0 if the interval containing the INFO-th singular value
*>             contains the incorrect number of singular values.
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
*> \ingroup double_eig
*
*  =====================================================================
      SUBROUTINE DSVDCH( N, S, E, SVD, TOL, INFO )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, N
      DOUBLE PRECISION   TOL
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   E( * ), S( * ), SVD( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ONE
      PARAMETER          ( ONE = 1.0D0 )
      DOUBLE PRECISION   ZERO
      PARAMETER          ( ZERO = 0.0D0 )
*     ..
*     .. Local Scalars ..
      INTEGER            BPNT, COUNT, NUML, NUMU, TPNT
      DOUBLE PRECISION   EPS, LOWER, OVFL, TUPPR, UNFL, UNFLEP, UPPER
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH
      EXTERNAL           DLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           DSVDCT
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, SQRT
*     ..
*     .. Executable Statements ..
*
*     Get machine constants
*
      INFO = 0
      IF( N.LE.0 )
     $   RETURN
      UNFL = DLAMCH( 'Safe minimum' )
      OVFL = DLAMCH( 'Overflow' )
      EPS = DLAMCH( 'Epsilon' )*DLAMCH( 'Base' )
*
*     UNFLEP is chosen so that when an eigenvalue is multiplied by the
*     scale factor sqrt(OVFL)*sqrt(sqrt(UNFL))/MX in DSVDCT, it exceeds
*     sqrt(UNFL), which is the lower limit for DSVDCT.
*
      UNFLEP = ( SQRT( SQRT( UNFL ) ) / SQRT( OVFL ) )*SVD( 1 ) +
     $         UNFL / EPS
*
*     The value of EPS works best when TOL .GE. 10.
*
      EPS = TOL*MAX( N / 10, 1 )*EPS
*
*     TPNT points to singular value at right endpoint of interval
*     BPNT points to singular value at left  endpoint of interval
*
      TPNT = 1
      BPNT = 1
*
*     Begin loop over all intervals
*
   10 CONTINUE
      UPPER = ( ONE+EPS )*SVD( TPNT ) + UNFLEP
      LOWER = ( ONE-EPS )*SVD( BPNT ) - UNFLEP
      IF( LOWER.LE.UNFLEP )
     $   LOWER = -UPPER
*
*     Begin loop merging overlapping intervals
*
   20 CONTINUE
      IF( BPNT.EQ.N )
     $   GO TO 30
      TUPPR = ( ONE+EPS )*SVD( BPNT+1 ) + UNFLEP
      IF( TUPPR.LT.LOWER )
     $   GO TO 30
*
*     Merge
*
      BPNT = BPNT + 1
      LOWER = ( ONE-EPS )*SVD( BPNT ) - UNFLEP
      IF( LOWER.LE.UNFLEP )
     $   LOWER = -UPPER
      GO TO 20
   30 CONTINUE
*
*     Count singular values in interval [ LOWER, UPPER ]
*
      CALL DSVDCT( N, S, E, LOWER, NUML )
      CALL DSVDCT( N, S, E, UPPER, NUMU )
      COUNT = NUMU - NUML
      IF( LOWER.LT.ZERO )
     $   COUNT = COUNT / 2
      IF( COUNT.NE.BPNT-TPNT+1 ) THEN
*
*        Wrong number of singular values in interval
*
         INFO = TPNT
         GO TO 40
      END IF
      TPNT = BPNT + 1
      BPNT = TPNT
      IF( TPNT.LE.N )
     $   GO TO 10
   40 CONTINUE
      RETURN
*
*     End of DSVDCH
*
      END
