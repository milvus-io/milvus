*> \brief \b CPTCON
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CPTCON + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cptcon.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cptcon.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cptcon.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CPTCON( N, D, E, ANORM, RCOND, RWORK, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, N
*       REAL               ANORM, RCOND
*       ..
*       .. Array Arguments ..
*       REAL               D( * ), RWORK( * )
*       COMPLEX            E( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CPTCON computes the reciprocal of the condition number (in the
*> 1-norm) of a complex Hermitian positive definite tridiagonal matrix
*> using the factorization A = L*D*L**H or A = U**H*D*U computed by
*> CPTTRF.
*>
*> Norm(inv(A)) is computed by a direct method, and the reciprocal of
*> the condition number is computed as
*>                  RCOND = 1 / (ANORM * norm(inv(A))).
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
*> \param[in] D
*> \verbatim
*>          D is REAL array, dimension (N)
*>          The n diagonal elements of the diagonal matrix D from the
*>          factorization of A, as computed by CPTTRF.
*> \endverbatim
*>
*> \param[in] E
*> \verbatim
*>          E is COMPLEX array, dimension (N-1)
*>          The (n-1) off-diagonal elements of the unit bidiagonal factor
*>          U or L from the factorization of A, as computed by CPTTRF.
*> \endverbatim
*>
*> \param[in] ANORM
*> \verbatim
*>          ANORM is REAL
*>          The 1-norm of the original matrix A.
*> \endverbatim
*>
*> \param[out] RCOND
*> \verbatim
*>          RCOND is REAL
*>          The reciprocal of the condition number of the matrix A,
*>          computed as RCOND = 1/(ANORM * AINVNM), where AINVNM is the
*>          1-norm of inv(A) computed in this routine.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is REAL array, dimension (N)
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
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
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  The method used is described in Nicholas J. Higham, "Efficient
*>  Algorithms for Computing the Condition Number of a Tridiagonal
*>  Matrix", SIAM J. Sci. Stat. Comput., Vol. 7, No. 1, January 1986.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE CPTCON( N, D, E, ANORM, RCOND, RWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, N
      REAL               ANORM, RCOND
*     ..
*     .. Array Arguments ..
      REAL               D( * ), RWORK( * )
      COMPLEX            E( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ONE, ZERO
      PARAMETER          ( ONE = 1.0E+0, ZERO = 0.0E+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, IX
      REAL               AINVNM
*     ..
*     .. External Functions ..
      INTEGER            ISAMAX
      EXTERNAL           ISAMAX
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS
*     ..
*     .. Executable Statements ..
*
*     Test the input arguments.
*
      INFO = 0
      IF( N.LT.0 ) THEN
         INFO = -1
      ELSE IF( ANORM.LT.ZERO ) THEN
         INFO = -4
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CPTCON', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      RCOND = ZERO
      IF( N.EQ.0 ) THEN
         RCOND = ONE
         RETURN
      ELSE IF( ANORM.EQ.ZERO ) THEN
         RETURN
      END IF
*
*     Check that D(1:N) is positive.
*
      DO 10 I = 1, N
         IF( D( I ).LE.ZERO )
     $      RETURN
   10 CONTINUE
*
*     Solve M(A) * x = e, where M(A) = (m(i,j)) is given by
*
*        m(i,j) =  abs(A(i,j)), i = j,
*        m(i,j) = -abs(A(i,j)), i .ne. j,
*
*     and e = [ 1, 1, ..., 1 ]**T.  Note M(A) = M(L)*D*M(L)**H.
*
*     Solve M(L) * x = e.
*
      RWORK( 1 ) = ONE
      DO 20 I = 2, N
         RWORK( I ) = ONE + RWORK( I-1 )*ABS( E( I-1 ) )
   20 CONTINUE
*
*     Solve D * M(L)**H * x = b.
*
      RWORK( N ) = RWORK( N ) / D( N )
      DO 30 I = N - 1, 1, -1
         RWORK( I ) = RWORK( I ) / D( I ) + RWORK( I+1 )*ABS( E( I ) )
   30 CONTINUE
*
*     Compute AINVNM = max(x(i)), 1<=i<=n.
*
      IX = ISAMAX( N, RWORK, 1 )
      AINVNM = ABS( RWORK( IX ) )
*
*     Compute the reciprocal condition number.
*
      IF( AINVNM.NE.ZERO )
     $   RCOND = ( ONE / AINVNM ) / ANORM
*
      RETURN
*
*     End of CPTCON
*
      END
