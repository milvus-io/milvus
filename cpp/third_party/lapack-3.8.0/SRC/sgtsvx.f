*> \brief <b> SGTSVX computes the solution to system of linear equations A * X = B for GT matrices </b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SGTSVX + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/sgtsvx.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/sgtsvx.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/sgtsvx.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SGTSVX( FACT, TRANS, N, NRHS, DL, D, DU, DLF, DF, DUF,
*                          DU2, IPIV, B, LDB, X, LDX, RCOND, FERR, BERR,
*                          WORK, IWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          FACT, TRANS
*       INTEGER            INFO, LDB, LDX, N, NRHS
*       REAL               RCOND
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * ), IWORK( * )
*       REAL               B( LDB, * ), BERR( * ), D( * ), DF( * ),
*      $                   DL( * ), DLF( * ), DU( * ), DU2( * ), DUF( * ),
*      $                   FERR( * ), WORK( * ), X( LDX, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SGTSVX uses the LU factorization to compute the solution to a real
*> system of linear equations A * X = B or A**T * X = B,
*> where A is a tridiagonal matrix of order N and X and B are N-by-NRHS
*> matrices.
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
*> 1. If FACT = 'N', the LU decomposition is used to factor the matrix A
*>    as A = L * U, where L is a product of permutation and unit lower
*>    bidiagonal matrices and U is upper triangular with nonzeros in
*>    only the main diagonal and first two superdiagonals.
*>
*> 2. If some U(i,i)=0, so that U is exactly singular, then the routine
*>    returns with INFO = i. Otherwise, the factored form of A is used
*>    to estimate the condition number of the matrix A.  If the
*>    reciprocal of the condition number is less than machine precision,
*>    INFO = N+1 is returned as a warning, but the routine still goes on
*>    to solve for X and compute error bounds as described below.
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
*>          = 'F':  DLF, DF, DUF, DU2, and IPIV contain the factored
*>                  form of A; DL, D, DU, DLF, DF, DUF, DU2 and IPIV
*>                  will not be modified.
*>          = 'N':  The matrix will be copied to DLF, DF, and DUF
*>                  and factored.
*> \endverbatim
*>
*> \param[in] TRANS
*> \verbatim
*>          TRANS is CHARACTER*1
*>          Specifies the form of the system of equations:
*>          = 'N':  A * X = B     (No transpose)
*>          = 'T':  A**T * X = B  (Transpose)
*>          = 'C':  A**H * X = B  (Conjugate transpose = Transpose)
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
*>          of the matrix B.  NRHS >= 0.
*> \endverbatim
*>
*> \param[in] DL
*> \verbatim
*>          DL is REAL array, dimension (N-1)
*>          The (n-1) subdiagonal elements of A.
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is REAL array, dimension (N)
*>          The n diagonal elements of A.
*> \endverbatim
*>
*> \param[in] DU
*> \verbatim
*>          DU is REAL array, dimension (N-1)
*>          The (n-1) superdiagonal elements of A.
*> \endverbatim
*>
*> \param[in,out] DLF
*> \verbatim
*>          DLF is REAL array, dimension (N-1)
*>          If FACT = 'F', then DLF is an input argument and on entry
*>          contains the (n-1) multipliers that define the matrix L from
*>          the LU factorization of A as computed by SGTTRF.
*>
*>          If FACT = 'N', then DLF is an output argument and on exit
*>          contains the (n-1) multipliers that define the matrix L from
*>          the LU factorization of A.
*> \endverbatim
*>
*> \param[in,out] DF
*> \verbatim
*>          DF is REAL array, dimension (N)
*>          If FACT = 'F', then DF is an input argument and on entry
*>          contains the n diagonal elements of the upper triangular
*>          matrix U from the LU factorization of A.
*>
*>          If FACT = 'N', then DF is an output argument and on exit
*>          contains the n diagonal elements of the upper triangular
*>          matrix U from the LU factorization of A.
*> \endverbatim
*>
*> \param[in,out] DUF
*> \verbatim
*>          DUF is REAL array, dimension (N-1)
*>          If FACT = 'F', then DUF is an input argument and on entry
*>          contains the (n-1) elements of the first superdiagonal of U.
*>
*>          If FACT = 'N', then DUF is an output argument and on exit
*>          contains the (n-1) elements of the first superdiagonal of U.
*> \endverbatim
*>
*> \param[in,out] DU2
*> \verbatim
*>          DU2 is REAL array, dimension (N-2)
*>          If FACT = 'F', then DU2 is an input argument and on entry
*>          contains the (n-2) elements of the second superdiagonal of
*>          U.
*>
*>          If FACT = 'N', then DU2 is an output argument and on exit
*>          contains the (n-2) elements of the second superdiagonal of
*>          U.
*> \endverbatim
*>
*> \param[in,out] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (N)
*>          If FACT = 'F', then IPIV is an input argument and on entry
*>          contains the pivot indices from the LU factorization of A as
*>          computed by SGTTRF.
*>
*>          If FACT = 'N', then IPIV is an output argument and on exit
*>          contains the pivot indices from the LU factorization of A;
*>          row i of the matrix was interchanged with row IPIV(i).
*>          IPIV(i) will always be either i or i+1; IPIV(i) = i indicates
*>          a row interchange was not required.
*> \endverbatim
*>
*> \param[in] B
*> \verbatim
*>          B is REAL array, dimension (LDB,NRHS)
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
*>          X is REAL array, dimension (LDX,NRHS)
*>          If INFO = 0 or INFO = N+1, the N-by-NRHS solution matrix X.
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
*>          RCOND is REAL
*>          The estimate of the reciprocal condition number of the matrix
*>          A.  If RCOND is less than the machine precision (in
*>          particular, if RCOND = 0), the matrix is singular to working
*>          precision.  This condition is indicated by a return code of
*>          INFO > 0.
*> \endverbatim
*>
*> \param[out] FERR
*> \verbatim
*>          FERR is REAL array, dimension (NRHS)
*>          The estimated forward error bound for each solution vector
*>          X(j) (the j-th column of the solution matrix X).
*>          If XTRUE is the true solution corresponding to X(j), FERR(j)
*>          is an estimated upper bound for the magnitude of the largest
*>          element in (X(j) - XTRUE) divided by the magnitude of the
*>          largest element in X(j).  The estimate is as reliable as
*>          the estimate for RCOND, and is almost always a slight
*>          overestimate of the true error.
*> \endverbatim
*>
*> \param[out] BERR
*> \verbatim
*>          BERR is REAL array, dimension (NRHS)
*>          The componentwise relative backward error of each solution
*>          vector X(j) (i.e., the smallest relative change in
*>          any element of A or B that makes X(j) an exact solution).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (3*N)
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (N)
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
*>          > 0:  if INFO = i, and i is
*>                <= N:  U(i,i) is exactly zero.  The factorization
*>                       has not been completed unless i = N, but the
*>                       factor U is exactly singular, so the solution
*>                       and error bounds could not be computed.
*>                       RCOND = 0 is returned.
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
*> \ingroup realGTsolve
*
*  =====================================================================
      SUBROUTINE SGTSVX( FACT, TRANS, N, NRHS, DL, D, DU, DLF, DF, DUF,
     $                   DU2, IPIV, B, LDB, X, LDX, RCOND, FERR, BERR,
     $                   WORK, IWORK, INFO )
*
*  -- LAPACK driver routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          FACT, TRANS
      INTEGER            INFO, LDB, LDX, N, NRHS
      REAL               RCOND
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * ), IWORK( * )
      REAL               B( LDB, * ), BERR( * ), D( * ), DF( * ),
     $                   DL( * ), DLF( * ), DU( * ), DU2( * ), DUF( * ),
     $                   FERR( * ), WORK( * ), X( LDX, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO
      PARAMETER          ( ZERO = 0.0E+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            NOFACT, NOTRAN
      CHARACTER          NORM
      REAL               ANORM
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      REAL               SLAMCH, SLANGT
      EXTERNAL           LSAME, SLAMCH, SLANGT
*     ..
*     .. External Subroutines ..
      EXTERNAL           SCOPY, SGTCON, SGTRFS, SGTTRF, SGTTRS, SLACPY,
     $                   XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX
*     ..
*     .. Executable Statements ..
*
      INFO = 0
      NOFACT = LSAME( FACT, 'N' )
      NOTRAN = LSAME( TRANS, 'N' )
      IF( .NOT.NOFACT .AND. .NOT.LSAME( FACT, 'F' ) ) THEN
         INFO = -1
      ELSE IF( .NOT.NOTRAN .AND. .NOT.LSAME( TRANS, 'T' ) .AND. .NOT.
     $         LSAME( TRANS, 'C' ) ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( NRHS.LT.0 ) THEN
         INFO = -4
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -14
      ELSE IF( LDX.LT.MAX( 1, N ) ) THEN
         INFO = -16
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SGTSVX', -INFO )
         RETURN
      END IF
*
      IF( NOFACT ) THEN
*
*        Compute the LU factorization of A.
*
         CALL SCOPY( N, D, 1, DF, 1 )
         IF( N.GT.1 ) THEN
            CALL SCOPY( N-1, DL, 1, DLF, 1 )
            CALL SCOPY( N-1, DU, 1, DUF, 1 )
         END IF
         CALL SGTTRF( N, DLF, DF, DUF, DU2, IPIV, INFO )
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
      IF( NOTRAN ) THEN
         NORM = '1'
      ELSE
         NORM = 'I'
      END IF
      ANORM = SLANGT( NORM, N, DL, D, DU )
*
*     Compute the reciprocal of the condition number of A.
*
      CALL SGTCON( NORM, N, DLF, DF, DUF, DU2, IPIV, ANORM, RCOND, WORK,
     $             IWORK, INFO )
*
*     Compute the solution vectors X.
*
      CALL SLACPY( 'Full', N, NRHS, B, LDB, X, LDX )
      CALL SGTTRS( TRANS, N, NRHS, DLF, DF, DUF, DU2, IPIV, X, LDX,
     $             INFO )
*
*     Use iterative refinement to improve the computed solutions and
*     compute error bounds and backward error estimates for them.
*
      CALL SGTRFS( TRANS, N, NRHS, DL, D, DU, DLF, DF, DUF, DU2, IPIV,
     $             B, LDB, X, LDX, FERR, BERR, WORK, IWORK, INFO )
*
*     Set INFO = N+1 if the matrix is singular to working precision.
*
      IF( RCOND.LT.SLAMCH( 'Epsilon' ) )
     $   INFO = N + 1
*
      RETURN
*
*     End of SGTSVX
*
      END
