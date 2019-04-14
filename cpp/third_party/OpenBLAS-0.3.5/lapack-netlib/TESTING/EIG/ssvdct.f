*> \brief \b SSVDCT
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SSVDCT( N, S, E, SHIFT, NUM )
*
*       .. Scalar Arguments ..
*       INTEGER            N, NUM
*       REAL               SHIFT
*       ..
*       .. Array Arguments ..
*       REAL               E( * ), S( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SSVDCT counts the number NUM of eigenvalues of a 2*N by 2*N
*> tridiagonal matrix T which are less than or equal to SHIFT.  T is
*> formed by putting zeros on the diagonal and making the off-diagonals
*> equal to S(1), E(1), S(2), E(2), ... , E(N-1), S(N).  If SHIFT is
*> positive, NUM is equal to N plus the number of singular values of a
*> bidiagonal matrix B less than or equal to SHIFT.  Here B has diagonal
*> entries S(1), ..., S(N) and superdiagonal entries E(1), ... E(N-1).
*> If SHIFT is negative, NUM is equal to the number of singular values
*> of B greater than or equal to -SHIFT.
*>
*> See W. Kahan "Accurate Eigenvalues of a Symmetric Tridiagonal
*> Matrix", Report CS41, Computer Science Dept., Stanford University,
*> July 21, 1966
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
*>          S is REAL array, dimension (N)
*>          The diagonal entries of the bidiagonal matrix B.
*> \endverbatim
*>
*> \param[in] E
*> \verbatim
*>          E is REAL array of dimension (N-1)
*>          The superdiagonal entries of the bidiagonal matrix B.
*> \endverbatim
*>
*> \param[in] SHIFT
*> \verbatim
*>          SHIFT is REAL
*>          The shift, used as described under Purpose.
*> \endverbatim
*>
*> \param[out] NUM
*> \verbatim
*>          NUM is INTEGER
*>          The number of eigenvalues of T less than or equal to SHIFT.
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
*> \ingroup single_eig
*
*  =====================================================================
      SUBROUTINE SSVDCT( N, S, E, SHIFT, NUM )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            N, NUM
      REAL               SHIFT
*     ..
*     .. Array Arguments ..
      REAL               E( * ), S( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ONE
      PARAMETER          ( ONE = 1.0E0 )
      REAL               ZERO
      PARAMETER          ( ZERO = 0.0E0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I
      REAL               M1, M2, MX, OVFL, SOV, SSHIFT, SSUN, SUN, TMP,
     $                   TOM, U, UNFL
*     ..
*     .. External Functions ..
      REAL               SLAMCH
      EXTERNAL           SLAMCH
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, SQRT
*     ..
*     .. Executable Statements ..
*
*     Get machine constants
*
      UNFL = 2*SLAMCH( 'Safe minimum' )
      OVFL = ONE / UNFL
*
*     Find largest entry
*
      MX = ABS( S( 1 ) )
      DO 10 I = 1, N - 1
         MX = MAX( MX, ABS( S( I+1 ) ), ABS( E( I ) ) )
   10 CONTINUE
*
      IF( MX.EQ.ZERO ) THEN
         IF( SHIFT.LT.ZERO ) THEN
            NUM = 0
         ELSE
            NUM = 2*N
         END IF
         RETURN
      END IF
*
*     Compute scale factors as in Kahan's report
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
      U = ONE
      NUM = 0
      SSHIFT = ( SHIFT*M1 )*M2
      U = -SSHIFT
      IF( U.LE.SUN ) THEN
         IF( U.LE.ZERO ) THEN
            NUM = NUM + 1
            IF( U.GT.-SUN )
     $         U = -SUN
         ELSE
            U = SUN
         END IF
      END IF
      TMP = ( S( 1 )*M1 )*M2
      U = -TMP*( TMP / U ) - SSHIFT
      IF( U.LE.SUN ) THEN
         IF( U.LE.ZERO ) THEN
            NUM = NUM + 1
            IF( U.GT.-SUN )
     $         U = -SUN
         ELSE
            U = SUN
         END IF
      END IF
      DO 20 I = 1, N - 1
         TMP = ( E( I )*M1 )*M2
         U = -TMP*( TMP / U ) - SSHIFT
         IF( U.LE.SUN ) THEN
            IF( U.LE.ZERO ) THEN
               NUM = NUM + 1
               IF( U.GT.-SUN )
     $            U = -SUN
            ELSE
               U = SUN
            END IF
         END IF
         TMP = ( S( I+1 )*M1 )*M2
         U = -TMP*( TMP / U ) - SSHIFT
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
*     End of SSVDCT
*
      END
