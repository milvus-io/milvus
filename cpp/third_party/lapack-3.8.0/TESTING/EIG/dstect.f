*> \brief \b DSTECT
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DSTECT( N, A, B, SHIFT, NUM )
*
*       .. Scalar Arguments ..
*       INTEGER            N, NUM
*       DOUBLE PRECISION   SHIFT
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   A( * ), B( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    DSTECT counts the number NUM of eigenvalues of a tridiagonal
*>    matrix T which are less than or equal to SHIFT. T has
*>    diagonal entries A(1), ... , A(N), and offdiagonal entries
*>    B(1), ..., B(N-1).
*>    See W. Kahan "Accurate Eigenvalues of a Symmetric Tridiagonal
*>    Matrix", Report CS41, Computer Science Dept., Stanford
*>    University, July 21, 1966
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The dimension of the tridiagonal matrix T.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension (N)
*>          The diagonal entries of the tridiagonal matrix T.
*> \endverbatim
*>
*> \param[in] B
*> \verbatim
*>          B is DOUBLE PRECISION array, dimension (N-1)
*>          The offdiagonal entries of the tridiagonal matrix T.
*> \endverbatim
*>
*> \param[in] SHIFT
*> \verbatim
*>          SHIFT is DOUBLE PRECISION
*>          The shift, used as described under Purpose.
*> \endverbatim
*>
*> \param[out] NUM
*> \verbatim
*>          NUM is INTEGER
*>          The number of eigenvalues of T less than or equal
*>          to SHIFT.
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
      SUBROUTINE DSTECT( N, A, B, SHIFT, NUM )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            N, NUM
      DOUBLE PRECISION   SHIFT
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   A( * ), B( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE, THREE
      PARAMETER          ( ZERO = 0.0D0, ONE = 1.0D0, THREE = 3.0D0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I
      DOUBLE PRECISION   M1, M2, MX, OVFL, SOV, SSHIFT, SSUN, SUN, TMP,
     $                   TOM, U, UNFL
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH
      EXTERNAL           DLAMCH
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, SQRT
*     ..
*     .. Executable Statements ..
*
*     Get machine constants
*
      UNFL = DLAMCH( 'Safe minimum' )
      OVFL = DLAMCH( 'Overflow' )
*
*     Find largest entry
*
      MX = ABS( A( 1 ) )
      DO 10 I = 1, N - 1
         MX = MAX( MX, ABS( A( I+1 ) ), ABS( B( I ) ) )
   10 CONTINUE
*
*     Handle easy cases, including zero matrix
*
      IF( SHIFT.GE.THREE*MX ) THEN
         NUM = N
         RETURN
      END IF
      IF( SHIFT.LT.-THREE*MX ) THEN
         NUM = 0
         RETURN
      END IF
*
*     Compute scale factors as in Kahan's report
*     At this point, MX .NE. 0 so we can divide by it
*
      SUN = SQRT( UNFL )
      SSUN = SQRT( SUN )
      SOV = SQRT( OVFL )
      TOM = SSUN*SOV
      IF( MX.LE.ONE ) THEN
         M1 = ONE / MX
         M2 = TOM
      ELSE
         M1 = ONE
         M2 = TOM / MX
      END IF
*
*     Begin counting
*
      NUM = 0
      SSHIFT = ( SHIFT*M1 )*M2
      U = ( A( 1 )*M1 )*M2 - SSHIFT
      IF( U.LE.SUN ) THEN
         IF( U.LE.ZERO ) THEN
            NUM = NUM + 1
            IF( U.GT.-SUN )
     $         U = -SUN
         ELSE
            U = SUN
         END IF
      END IF
      DO 20 I = 2, N
         TMP = ( B( I-1 )*M1 )*M2
         U = ( ( A( I )*M1 )*M2-TMP*( TMP / U ) ) - SSHIFT
         IF( U.LE.SUN ) THEN
            IF( U.LE.ZERO ) THEN
               NUM = NUM + 1
               IF( U.GT.-SUN )
     $            U = -SUN
            ELSE
               U = SUN
            END IF
         END IF
   20 CONTINUE
      RETURN
*
*     End of DSTECT
*
      END
