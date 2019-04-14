*> \brief <b> DPTSVX computes the solution to system of linear equations A * X = B for PT matrices</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DPTSVX + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dptsvx.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dptsvx.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dptsvx.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DPTSVX( FACT, N, NRHS, D, E, DF, EF, B, LDB, X, LDX,
*                          RCOND, FERR, BERR, WORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          FACT
*       INTEGER            INFO, LDB, LDX, N, NRHS
*       DOUBLE PRECISION   RCOND
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   B( LDB, * ), BERR( * ), D( * ), DF( * ),
*      $                   E( * ), EF( * ), FERR( * ), WORK( * ),
*      $                   X( LDX, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DPTSVX uses the factorization A = L*D*L**T to compute the solution
*> to a real system of linear equations A*X = B, where A is an N-by-N
*> symmetric positive definite tridiagonal matrix and X and B are
*> N-by-NRHS matrices.
*>
*> Error bounds on the solution and a condition estimate are also
*> provided.
*> \endverbatim
*
*> \par Description:
*  =================
*>
*> \verbatim
*>
*> The following steps are performed:
*>
*> 1. If FACT = 'N', the matrix A is factored as A = L*D*L**T, where L
*>    is a unit lower bidiagonal matrix and D is diagonal.  The
*>    factorization can also be regarded as having the form
*>    A = U**T*D*U.
*>
*> 2. If the leading i-by-i principal minor is not positive definite,
*>    then the routine returns with INFO = i. Otherwise, the factored
*>    form of A is used to estimate the condition number of the matrix
*>    A.  If the reciprocal of the condition number is less than machine
*>    precision, INFO = N+1 is returned as a warning, but the routine
*>    still goes on to solve for X and compute error bounds as
*>    described below.
*>
*> 3. The system of equations is solved for X using the factored form
*>    of A.
*>
*> 4. Iterative refinement is applied to improve the computed solution
*>    matrix and calculate error bounds and backward error estimates
*>    for it.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] FACT
*> \verbatim
*>          FACT is CHARACTER*1
*>          Specifies whether or not the factored form of A has been
*>          supplied on entry.
*>          = 'F':  On entry, DF and EF contain the factored form of A.
*>                  D, E, DF, and EF will not be modified.
*>          = 'N':  The matrix A will be copied to DF and EF and
*>                  factored.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] NRHS
*> \verbatim
*>          NRHS is INTEGER
*>          The number of right hand sides, i.e., the number of columns
*>          of the matrices B and X.  NRHS >= 0.
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension (N)
*>          The n diagonal elements of the tridiagonal matrix A.
*> \endverbatim
*>
*> \param[in] E
*> \verbatim
*>          E is DOUBLE PRECISION array, dimension (N-1)
*>          The (n-1) subdiagonal elements of the tridiagonal matrix A.
*> \endverbatim
*>
*> \param[in,out] DF
*> \verbatim
*>          DF is DOUBLE PRECISION array, dimension (N)
*>          If FACT = 'F', then DF is an input argument and on entry
*>          contains the n diagonal elements of the diagonal matrix D
*>          from the L*D*L**T factorization of A.
*>          If FACT = 'N', then DF is an output argument and on exit
*>          contains the n diagonal elements of the diagonal matrix D
*>          from the L*D*L**T factorization of A.
*> \endverbatim
*>
*> \param[in,out] EF
*> \verbatim
*>          EF is DOUBLE PRECISION array, dimension (N-1)
*>          If FACT = 'F', then EF is an input argument and on entry
*>          contains the (n-1) subdiagonal elements of the unit
*>          bidiagonal factor L from the L*D*L**T factorization of A.
*>          If FACT = 'N', then EF is an output argument and on exit
*>          contains the (n-1) subdiagonal elements of the unit
*>          bidiagonal factor L from the L*D*L**T factorization of A.
*> \endverbatim
*>
*> \param[in] B
*> \verbatim
*>          B is DOUBLE PRECISION array, dimension (LDB,NRHS)
*>          The N-by-NRHS right hand side matrix B.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B.  LDB >= max(1,N).
*> \endverbatim
*>
*> \param[out] X
*> \verbatim
*>          X is DOUBLE PRECISION array, dimension (LDX,NRHS)
*>          If INFO = 0 of INFO = N+1, the N-by-NRHS solution matrix X.
*> \endverbatim
*>
*> \param[in] LDX
*> \verbatim
*>          LDX is INTEGER
*>          The leading dimension of the array X.  LDX >= max(1,N).
*> \endverbatim
*>
*> \param[out] RCOND
*> \verbatim
*>          RCOND is DOUBLE PRECISION
*>          The reciprocal condition number of the matrix A.  If RCOND
*>          is less than the machine precision (in particular, if
*>          RCOND = 0), the matrix is singular to working precision.
*>          This condition is indicated by a return code of INFO > 0.
*> \endverbatim
*>
*> \param[out] FERR
*> \verbatim
*>          FERR is DOUBLE PRECISION array, dimension (NRHS)
*>          The forward error bound for each solution vector
*>          X(j) (the j-th column of the solution matrix X).
*>          If XTRUE is the true solution corresponding to X(j), FERR(j)
*>          is an estimated upper bound for the magnitude of the largest
*>          element in (X(j) - XTRUE) divided by the magnitude of the
*>          largest element in X(j).
*> \endverbatim
*>
*> \param[out] BERR
*> \verbatim
*>          BERR is DOUBLE PRECISION array, dimension (NRHS)
*>          The componentwise relative backward error of each solution
*>          vector X(j) (i.e., the smallest relative change in any
*>          element of A or B that makes X(j) an exact solution).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (2*N)
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
*>          > 0:  if INFO = i, and i is
*>                <= N:  the leading minor of order i of A is
*>                       not positive definite, so the factorization
*>                       could not be completed, and the solution has not
*>                       been computed. RCOND = 0 is returned.
*>                = N+1: U is nonsingular, but RCOND is less than machine
*>                       precision, meaning that the matrix is singular
*>                       to working precision.  Nevertheless, the
*>                       solution and error bounds are computed because
*>                       there are a number of situations where the
*>                       computed solution can be more accurate than the
*>                       value of RCOND would suggest.
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
*> \ingroup doublePTsolve
*
*  =====================================================================
      SUBROUTINE DPTSVX( FACT, N, NRHS, D, E, DF, EF, B, LDB, X, LDX,
     $                   RCOND, FERR, BERR, WORK, INFO )
*
*  -- LAPACK driver routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          FACT
      INTEGER            INFO, LDB, LDX, N, NRHS
      DOUBLE PRECISION   RCOND
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   B( LDB, * ), BERR( * ), D( * ), DF( * ),
     $                   E( * ), EF( * ), FERR( * ), WORK( * ),
     $                   X( LDX, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO
      PARAMETER          ( ZERO = 0.0D+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            NOFACT
      DOUBLE PRECISION   ANORM
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      DOUBLE PRECISION   DLAMCH, DLANST
      EXTERNAL           LSAME, DLAMCH, DLANST
*     ..
*     .. External Subroutines ..
      EXTERNAL           DCOPY, DLACPY, DPTCON, DPTRFS, DPTTRF, DPTTRS,
     $                   XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
      NOFACT = LSAME( FACT, 'N' )
      IF( .NOT.NOFACT .AND. .NOT.LSAME( FACT, 'F' ) ) THEN
         INFO = -1
      ELSE IF( N.LT.0 ) THEN
         INFO = -2
      ELSE IF( NRHS.LT.0 ) THEN
         INFO = -3
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -9
      ELSE IF( LDX.LT.MAX( 1, N ) ) THEN
         INFO = -11
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DPTSVX', -INFO )
         RETURN
      END IF
*
      IF( NOFACT ) THEN
*
*        Compute the L*D*L**T (or U**T*D*U) factorization of A.
*
         CALL DCOPY( N, D, 1, DF, 1 )
         IF( N.GT.1 )
     $      CALL DCOPY( N-1, E, 1, EF, 1 )
         CALL DPTTRF( N, DF, EF, INFO )
*
*        Return if INFO is non-zero.
*
         IF( INFO.GT.0 )THEN
            RCOND = ZERO
            RETURN
         END IF
      END IF
*
*     Compute the norm of the matrix A.
*
      ANORM = DLANST( '1', N, D, E )
*
*     Compute the reciprocal of the condition number of A.
*
      CALL DPTCON( N, DF, EF, ANORM, RCOND, WORK, INFO )
*
*     Compute the solution vectors X.
*
      CALL DLACPY( 'Full', N, NRHS, B, LDB, X, LDX )
      CALL DPTTRS( N, NRHS, DF, EF, X, LDX, INFO )
*
*     Use iterative refinement to improve the computed solutions and
*     compute error bounds and backward error estimates for them.
*
      CALL DPTRFS( N, NRHS, D, E, DF, EF, B, LDB, X, LDX, FERR, BERR,
     $             WORK, INFO )
*
*     Set INFO = N+1 if the matrix is singular to working precision.
*
      IF( RCOND.LT.DLAMCH( 'Epsilon' ) )
     $   INFO = N + 1
*
      RETURN
*
*     End of DPTSVX
*
      END
