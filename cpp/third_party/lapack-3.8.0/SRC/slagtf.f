*> \brief \b SLAGTF computes an LU factorization of a matrix T-λI, where T is a general tridiagonal matrix, and λ a scalar, using partial pivoting with row interchanges.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLAGTF + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slagtf.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slagtf.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slagtf.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLAGTF( N, A, LAMBDA, B, C, TOL, D, IN, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, N
*       REAL               LAMBDA, TOL
*       ..
*       .. Array Arguments ..
*       INTEGER            IN( * )
*       REAL               A( * ), B( * ), C( * ), D( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLAGTF factorizes the matrix (T - lambda*I), where T is an n by n
*> tridiagonal matrix and lambda is a scalar, as
*>
*>    T - lambda*I = PLU,
*>
*> where P is a permutation matrix, L is a unit lower tridiagonal matrix
*> with at most one non-zero sub-diagonal elements per column and U is
*> an upper triangular matrix with at most two non-zero super-diagonal
*> elements per column.
*>
*> The factorization is obtained by Gaussian elimination with partial
*> pivoting and implicit row scaling.
*>
*> The parameter LAMBDA is included in the routine so that SLAGTF may
*> be used, in conjunction with SLAGTS, to obtain eigenvectors of T by
*> inverse iteration.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix T.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is REAL array, dimension (N)
*>          On entry, A must contain the diagonal elements of T.
*>
*>          On exit, A is overwritten by the n diagonal elements of the
*>          upper triangular matrix U of the factorization of T.
*> \endverbatim
*>
*> \param[in] LAMBDA
*> \verbatim
*>          LAMBDA is REAL
*>          On entry, the scalar lambda.
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is REAL array, dimension (N-1)
*>          On entry, B must contain the (n-1) super-diagonal elements of
*>          T.
*>
*>          On exit, B is overwritten by the (n-1) super-diagonal
*>          elements of the matrix U of the factorization of T.
*> \endverbatim
*>
*> \param[in,out] C
*> \verbatim
*>          C is REAL array, dimension (N-1)
*>          On entry, C must contain the (n-1) sub-diagonal elements of
*>          T.
*>
*>          On exit, C is overwritten by the (n-1) sub-diagonal elements
*>          of the matrix L of the factorization of T.
*> \endverbatim
*>
*> \param[in] TOL
*> \verbatim
*>          TOL is REAL
*>          On entry, a relative tolerance used to indicate whether or
*>          not the matrix (T - lambda*I) is nearly singular. TOL should
*>          normally be chose as approximately the largest relative error
*>          in the elements of T. For example, if the elements of T are
*>          correct to about 4 significant figures, then TOL should be
*>          set to about 5*10**(-4). If TOL is supplied as less than eps,
*>          where eps is the relative machine precision, then the value
*>          eps is used in place of TOL.
*> \endverbatim
*>
*> \param[out] D
*> \verbatim
*>          D is REAL array, dimension (N-2)
*>          On exit, D is overwritten by the (n-2) second super-diagonal
*>          elements of the matrix U of the factorization of T.
*> \endverbatim
*>
*> \param[out] IN
*> \verbatim
*>          IN is INTEGER array, dimension (N)
*>          On exit, IN contains details of the permutation matrix P. If
*>          an interchange occurred at the kth step of the elimination,
*>          then IN(k) = 1, otherwise IN(k) = 0. The element IN(n)
*>          returns the smallest positive integer j such that
*>
*>             abs( u(j,j) ).le. norm( (T - lambda*I)(j) )*TOL,
*>
*>          where norm( A(j) ) denotes the sum of the absolute values of
*>          the jth row of the matrix A. If no such j exists then IN(n)
*>          is returned as zero. If IN(n) is returned as positive, then a
*>          diagonal element of U is small, indicating that
*>          (T - lambda*I) is singular or nearly singular,
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0   : successful exit
*>          .lt. 0: if INFO = -k, the kth argument had an illegal value
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
*> \ingroup auxOTHERcomputational
*
*  =====================================================================
      SUBROUTINE SLAGTF( N, A, LAMBDA, B, C, TOL, D, IN, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, N
      REAL               LAMBDA, TOL
*     ..
*     .. Array Arguments ..
      INTEGER            IN( * )
      REAL               A( * ), B( * ), C( * ), D( * )
*     ..
*
* =====================================================================
*
*     .. Parameters ..
      REAL               ZERO
      PARAMETER          ( ZERO = 0.0E+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            K
      REAL               EPS, MULT, PIV1, PIV2, SCALE1, SCALE2, TEMP, TL
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX
*     ..
*     .. External Functions ..
      REAL               SLAMCH
      EXTERNAL           SLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA
*     ..
*     .. Executable Statements ..
*
      INFO = 0
      IF( N.LT.0 ) THEN
         INFO = -1
         CALL XERBLA( 'SLAGTF', -INFO )
         RETURN
      END IF
*
      IF( N.EQ.0 )
     $   RETURN
*
      A( 1 ) = A( 1 ) - LAMBDA
      IN( N ) = 0
      IF( N.EQ.1 ) THEN
         IF( A( 1 ).EQ.ZERO )
     $      IN( 1 ) = 1
         RETURN
      END IF
*
      EPS = SLAMCH( 'Epsilon' )
*
      TL = MAX( TOL, EPS )
      SCALE1 = ABS( A( 1 ) ) + ABS( B( 1 ) )
      DO 10 K = 1, N - 1
         A( K+1 ) = A( K+1 ) - LAMBDA
         SCALE2 = ABS( C( K ) ) + ABS( A( K+1 ) )
         IF( K.LT.( N-1 ) )
     $      SCALE2 = SCALE2 + ABS( B( K+1 ) )
         IF( A( K ).EQ.ZERO ) THEN
            PIV1 = ZERO
         ELSE
            PIV1 = ABS( A( K ) ) / SCALE1
         END IF
         IF( C( K ).EQ.ZERO ) THEN
            IN( K ) = 0
            PIV2 = ZERO
            SCALE1 = SCALE2
            IF( K.LT.( N-1 ) )
     $         D( K ) = ZERO
         ELSE
            PIV2 = ABS( C( K ) ) / SCALE2
            IF( PIV2.LE.PIV1 ) THEN
               IN( K ) = 0
               SCALE1 = SCALE2
               C( K ) = C( K ) / A( K )
               A( K+1 ) = A( K+1 ) - C( K )*B( K )
               IF( K.LT.( N-1 ) )
     $            D( K ) = ZERO
            ELSE
               IN( K ) = 1
               MULT = A( K ) / C( K )
               A( K ) = C( K )
               TEMP = A( K+1 )
               A( K+1 ) = B( K ) - MULT*TEMP
               IF( K.LT.( N-1 ) ) THEN
                  D( K ) = B( K+1 )
                  B( K+1 ) = -MULT*D( K )
               END IF
               B( K ) = TEMP
               C( K ) = MULT
            END IF
         END IF
         IF( ( MAX( PIV1, PIV2 ).LE.TL ) .AND. ( IN( N ).EQ.0 ) )
     $      IN( N ) = K
   10 CONTINUE
      IF( ( ABS( A( N ) ).LE.SCALE1*TL ) .AND. ( IN( N ).EQ.0 ) )
     $   IN( N ) = N
*
      RETURN
*
*     End of SLAGTF
*
      END
