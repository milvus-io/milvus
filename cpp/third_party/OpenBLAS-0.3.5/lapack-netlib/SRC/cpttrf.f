*> \brief \b CPTTRF
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CPTTRF + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cpttrf.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cpttrf.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cpttrf.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CPTTRF( N, D, E, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, N
*       ..
*       .. Array Arguments ..
*       REAL               D( * )
*       COMPLEX            E( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CPTTRF computes the L*D*L**H factorization of a complex Hermitian
*> positive definite tridiagonal matrix A.  The factorization may also
*> be regarded as having the form A = U**H *D*U.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in,out] D
*> \verbatim
*>          D is REAL array, dimension (N)
*>          On entry, the n diagonal elements of the tridiagonal matrix
*>          A.  On exit, the n diagonal elements of the diagonal matrix
*>          D from the L*D*L**H factorization of A.
*> \endverbatim
*>
*> \param[in,out] E
*> \verbatim
*>          E is COMPLEX array, dimension (N-1)
*>          On entry, the (n-1) subdiagonal elements of the tridiagonal
*>          matrix A.  On exit, the (n-1) subdiagonal elements of the
*>          unit bidiagonal factor L from the L*D*L**H factorization of A.
*>          E can also be regarded as the superdiagonal of the unit
*>          bidiagonal factor U from the U**H *D*U factorization of A.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0: successful exit
*>          < 0: if INFO = -k, the k-th argument had an illegal value
*>          > 0: if INFO = k, the leading minor of order k is not
*>               positive definite; if k < N, the factorization could not
*>               be completed, while if k = N, the factorization was
*>               completed, but D(N) <= 0.
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
*> \ingroup complexPTcomputational
*
*  =====================================================================
      SUBROUTINE CPTTRF( N, D, E, INFO )
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
      REAL               D( * )
      COMPLEX            E( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO
      PARAMETER          ( ZERO = 0.0E+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, I4
      REAL               EII, EIR, F, G
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          AIMAG, CMPLX, MOD, REAL
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
      IF( N.LT.0 ) THEN
         INFO = -1
         CALL XERBLA( 'CPTTRF', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 )
     $   RETURN
*
*     Compute the L*D*L**H (or U**H *D*U) factorization of A.
*
      I4 = MOD( N-1, 4 )
      DO 10 I = 1, I4
         IF( D( I ).LE.ZERO ) THEN
            INFO = I
            GO TO 20
         END IF
         EIR = REAL( E( I ) )
         EII = AIMAG( E( I ) )
         F = EIR / D( I )
         G = EII / D( I )
         E( I ) = CMPLX( F, G )
         D( I+1 ) = D( I+1 ) - F*EIR - G*EII
   10 CONTINUE
*
      DO 110 I = I4+1, N - 4, 4
*
*        Drop out of the loop if d(i) <= 0: the matrix is not positive
*        definite.
*
         IF( D( I ).LE.ZERO ) THEN
            INFO = I
            GO TO 20
         END IF
*
*        Solve for e(i) and d(i+1).
*
         EIR = REAL( E( I ) )
         EII = AIMAG( E( I ) )
         F = EIR / D( I )
         G = EII / D( I )
         E( I ) = CMPLX( F, G )
         D( I+1 ) = D( I+1 ) - F*EIR - G*EII
*
         IF( D( I+1 ).LE.ZERO ) THEN
            INFO = I+1
            GO TO 20
         END IF
*
*        Solve for e(i+1) and d(i+2).
*
         EIR = REAL( E( I+1 ) )
         EII = AIMAG( E( I+1 ) )
         F = EIR / D( I+1 )
         G = EII / D( I+1 )
         E( I+1 ) = CMPLX( F, G )
         D( I+2 ) = D( I+2 ) - F*EIR - G*EII
*
         IF( D( I+2 ).LE.ZERO ) THEN
            INFO = I+2
            GO TO 20
         END IF
*
*        Solve for e(i+2) and d(i+3).
*
         EIR = REAL( E( I+2 ) )
         EII = AIMAG( E( I+2 ) )
         F = EIR / D( I+2 )
         G = EII / D( I+2 )
         E( I+2 ) = CMPLX( F, G )
         D( I+3 ) = D( I+3 ) - F*EIR - G*EII
*
         IF( D( I+3 ).LE.ZERO ) THEN
            INFO = I+3
            GO TO 20
         END IF
*
*        Solve for e(i+3) and d(i+4).
*
         EIR = REAL( E( I+3 ) )
         EII = AIMAG( E( I+3 ) )
         F = EIR / D( I+3 )
         G = EII / D( I+3 )
         E( I+3 ) = CMPLX( F, G )
         D( I+4 ) = D( I+4 ) - F*EIR - G*EII
  110 CONTINUE
*
*     Check d(n) for positive definiteness.
*
      IF( D( N ).LE.ZERO )
     $   INFO = N
*
   20 CONTINUE
      RETURN
*
*     End of CPTTRF
*
      END
