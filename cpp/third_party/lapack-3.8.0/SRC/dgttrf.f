*> \brief \b DGTTRF
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DGTTRF + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dgttrf.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dgttrf.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dgttrf.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DGTTRF( N, DL, D, DU, DU2, IPIV, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, N
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * )
*       DOUBLE PRECISION   D( * ), DL( * ), DU( * ), DU2( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DGTTRF computes an LU factorization of a real tridiagonal matrix A
*> using elimination with partial pivoting and row interchanges.
*>
*> The factorization has the form
*>    A = L * U
*> where L is a product of permutation and unit lower bidiagonal
*> matrices and U is upper triangular with nonzeros in only the main
*> diagonal and first two superdiagonals.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix A.
*> \endverbatim
*>
*> \param[in,out] DL
*> \verbatim
*>          DL is DOUBLE PRECISION array, dimension (N-1)
*>          On entry, DL must contain the (n-1) sub-diagonal elements of
*>          A.
*>
*>          On exit, DL is overwritten by the (n-1) multipliers that
*>          define the matrix L from the LU factorization of A.
*> \endverbatim
*>
*> \param[in,out] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension (N)
*>          On entry, D must contain the diagonal elements of A.
*>
*>          On exit, D is overwritten by the n diagonal elements of the
*>          upper triangular matrix U from the LU factorization of A.
*> \endverbatim
*>
*> \param[in,out] DU
*> \verbatim
*>          DU is DOUBLE PRECISION array, dimension (N-1)
*>          On entry, DU must contain the (n-1) super-diagonal elements
*>          of A.
*>
*>          On exit, DU is overwritten by the (n-1) elements of the first
*>          super-diagonal of U.
*> \endverbatim
*>
*> \param[out] DU2
*> \verbatim
*>          DU2 is DOUBLE PRECISION array, dimension (N-2)
*>          On exit, DU2 is overwritten by the (n-2) elements of the
*>          second super-diagonal of U.
*> \endverbatim
*>
*> \param[out] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (N)
*>          The pivot indices; for 1 <= i <= n, row i of the matrix was
*>          interchanged with row IPIV(i).  IPIV(i) will always be either
*>          i or i+1; IPIV(i) = i indicates a row interchange was not
*>          required.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -k, the k-th argument had an illegal value
*>          > 0:  if INFO = k, U(k,k) is exactly zero. The factorization
*>                has been completed, but the factor U is exactly
*>                singular, and division by zero will occur if it is used
*>                to solve a system of equations.
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
*> \ingroup doubleGTcomputational
*
*  =====================================================================
      SUBROUTINE DGTTRF( N, DL, D, DU, DU2, IPIV, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, N
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * )
      DOUBLE PRECISION   D( * ), DL( * ), DU( * ), DU2( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO
      PARAMETER          ( ZERO = 0.0D+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I
      DOUBLE PRECISION   FACT, TEMP
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA
*     ..
*     .. Executable Statements ..
*
      INFO = 0
      IF( N.LT.0 ) THEN
         INFO = -1
         CALL XERBLA( 'DGTTRF', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 )
     $   RETURN
*
*     Initialize IPIV(i) = i and DU2(I) = 0
*
      DO 10 I = 1, N
         IPIV( I ) = I
   10 CONTINUE
      DO 20 I = 1, N - 2
         DU2( I ) = ZERO
   20 CONTINUE
*
      DO 30 I = 1, N - 2
         IF( ABS( D( I ) ).GE.ABS( DL( I ) ) ) THEN
*
*           No row interchange required, eliminate DL(I)
*
            IF( D( I ).NE.ZERO ) THEN
               FACT = DL( I ) / D( I )
               DL( I ) = FACT
               D( I+1 ) = D( I+1 ) - FACT*DU( I )
            END IF
         ELSE
*
*           Interchange rows I and I+1, eliminate DL(I)
*
            FACT = D( I ) / DL( I )
            D( I ) = DL( I )
            DL( I ) = FACT
            TEMP = DU( I )
            DU( I ) = D( I+1 )
            D( I+1 ) = TEMP - FACT*D( I+1 )
            DU2( I ) = DU( I+1 )
            DU( I+1 ) = -FACT*DU( I+1 )
            IPIV( I ) = I + 1
         END IF
   30 CONTINUE
      IF( N.GT.1 ) THEN
         I = N - 1
         IF( ABS( D( I ) ).GE.ABS( DL( I ) ) ) THEN
            IF( D( I ).NE.ZERO ) THEN
               FACT = DL( I ) / D( I )
               DL( I ) = FACT
               D( I+1 ) = D( I+1 ) - FACT*DU( I )
            END IF
         ELSE
            FACT = D( I ) / DL( I )
            D( I ) = DL( I )
            DL( I ) = FACT
            TEMP = DU( I )
            DU( I ) = D( I+1 )
            D( I+1 ) = TEMP - FACT*D( I+1 )
            IPIV( I ) = I + 1
         END IF
      END IF
*
*     Check for a zero on the diagonal of U.
*
      DO 40 I = 1, N
         IF( D( I ).EQ.ZERO ) THEN
            INFO = I
            GO TO 50
         END IF
   40 CONTINUE
   50 CONTINUE
*
      RETURN
*
*     End of DGTTRF
*
      END
