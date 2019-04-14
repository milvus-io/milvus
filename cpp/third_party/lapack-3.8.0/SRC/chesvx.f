*> \brief <b> CHESVX computes the solution to system of linear equations A * X = B for HE matrices</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CHESVX + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/chesvx.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/chesvx.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/chesvx.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CHESVX( FACT, UPLO, N, NRHS, A, LDA, AF, LDAF, IPIV, B,
*                          LDB, X, LDX, RCOND, FERR, BERR, WORK, LWORK,
*                          RWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          FACT, UPLO
*       INTEGER            INFO, LDA, LDAF, LDB, LDX, LWORK, N, NRHS
*       REAL               RCOND
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * )
*       REAL               BERR( * ), FERR( * ), RWORK( * )
*       COMPLEX            A( LDA, * ), AF( LDAF, * ), B( LDB, * ),
*      $                   WORK( * ), X( LDX, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CHESVX uses the diagonal pivoting factorization to compute the
*> solution to a complex system of linear equations A * X = B,
*> where A is an N-by-N Hermitian matrix and X and B are N-by-NRHS
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
*> 1. If FACT = 'N', the diagonal pivoting method is used to factor A.
*>    The form of the factorization is
*>       A = U * D * U**H,  if UPLO = 'U', or
*>       A = L * D * L**H,  if UPLO = 'L',
*>    where U (or L) is a product of permutation and unit upper (lower)
*>    triangular matrices, and D is Hermitian and block diagonal with
*>    1-by-1 and 2-by-2 diagonal blocks.
*>
*> 2. If some D(i,i)=0, so that D is exactly singular, then the routine
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
*>          = 'F':  On entry, AF and IPIV contain the factored form
*>                  of A.  A, AF and IPIV will not be modified.
*>          = 'N':  The matrix A will be copied to AF and factored.
*> \endverbatim
*>
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          = 'U':  Upper triangle of A is stored;
*>          = 'L':  Lower triangle of A is stored.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of linear equations, i.e., the order of the
*>          matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] NRHS
*> \verbatim
*>          NRHS is INTEGER
*>          The number of right hand sides, i.e., the number of columns
*>          of the matrices B and X.  NRHS >= 0.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N)
*>          The Hermitian matrix A.  If UPLO = 'U', the leading N-by-N
*>          upper triangular part of A contains the upper triangular part
*>          of the matrix A, and the strictly lower triangular part of A
*>          is not referenced.  If UPLO = 'L', the leading N-by-N lower
*>          triangular part of A contains the lower triangular part of
*>          the matrix A, and the strictly upper triangular part of A is
*>          not referenced.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] AF
*> \verbatim
*>          AF is COMPLEX array, dimension (LDAF,N)
*>          If FACT = 'F', then AF is an input argument and on entry
*>          contains the block diagonal matrix D and the multipliers used
*>          to obtain the factor U or L from the factorization
*>          A = U*D*U**H or A = L*D*L**H as computed by CHETRF.
*>
*>          If FACT = 'N', then AF is an output argument and on exit
*>          returns the block diagonal matrix D and the multipliers used
*>          to obtain the factor U or L from the factorization
*>          A = U*D*U**H or A = L*D*L**H.
*> \endverbatim
*>
*> \param[in] LDAF
*> \verbatim
*>          LDAF is INTEGER
*>          The leading dimension of the array AF.  LDAF >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (N)
*>          If FACT = 'F', then IPIV is an input argument and on entry
*>          contains details of the interchanges and the block structure
*>          of D, as determined by CHETRF.
*>          If IPIV(k) > 0, then rows and columns k and IPIV(k) were
*>          interchanged and D(k,k) is a 1-by-1 diagonal block.
*>          If UPLO = 'U' and IPIV(k) = IPIV(k-1) < 0, then rows and
*>          columns k-1 and -IPIV(k) were interchanged and D(k-1:k,k-1:k)
*>          is a 2-by-2 diagonal block.  If UPLO = 'L' and IPIV(k) =
*>          IPIV(k+1) < 0, then rows and columns k+1 and -IPIV(k) were
*>          interchanged and D(k:k+1,k:k+1) is a 2-by-2 diagonal block.
*>
*>          If FACT = 'N', then IPIV is an output argument and on exit
*>          contains details of the interchanges and the block structure
*>          of D, as determined by CHETRF.
*> \endverbatim
*>
*> \param[in] B
*> \verbatim
*>          B is COMPLEX array, dimension (LDB,NRHS)
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
*>          X is COMPLEX array, dimension (LDX,NRHS)
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
*>          WORK is COMPLEX array, dimension (MAX(1,LWORK))
*>          On exit, if INFO = 0, WORK(1) returns the optimal LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The length of WORK.  LWORK >= max(1,2*N), and for best
*>          performance, when FACT = 'N', LWORK >= max(1,2*N,N*NB), where
*>          NB is the optimal blocksize for CHETRF.
*>
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal size of the WORK array, returns
*>          this value as the first entry of the WORK array, and no error
*>          message related to LWORK is issued by XERBLA.
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
*>          = 0: successful exit
*>          < 0: if INFO = -i, the i-th argument had an illegal value
*>          > 0: if INFO = i, and i is
*>                <= N:  D(i,i) is exactly zero.  The factorization
*>                       has been completed but the factor D is exactly
*>                       singular, so the solution and error bounds could
*>                       not be computed. RCOND = 0 is returned.
*>                = N+1: D is nonsingular, but RCOND is less than machine
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
*> \date April 2012
*
*> \ingroup complexHEsolve
*
*  =====================================================================
      SUBROUTINE CHESVX( FACT, UPLO, N, NRHS, A, LDA, AF, LDAF, IPIV, B,
     $                   LDB, X, LDX, RCOND, FERR, BERR, WORK, LWORK,
     $                   RWORK, INFO )
*
*  -- LAPACK driver routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     April 2012
*
*     .. Scalar Arguments ..
      CHARACTER          FACT, UPLO
      INTEGER            INFO, LDA, LDAF, LDB, LDX, LWORK, N, NRHS
      REAL               RCOND
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * )
      REAL               BERR( * ), FERR( * ), RWORK( * )
      COMPLEX            A( LDA, * ), AF( LDAF, * ), B( LDB, * ),
     $                   WORK( * ), X( LDX, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO
      PARAMETER          ( ZERO = 0.0E+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            LQUERY, NOFACT
      INTEGER            LWKOPT, NB
      REAL               ANORM
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV
      REAL               CLANHE, SLAMCH
      EXTERNAL           ILAENV, LSAME, CLANHE, SLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           CHECON, CHERFS, CHETRF, CHETRS, CLACPY, XERBLA
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
      LQUERY = ( LWORK.EQ.-1 )
      IF( .NOT.NOFACT .AND. .NOT.LSAME( FACT, 'F' ) ) THEN
         INFO = -1
      ELSE IF( .NOT.LSAME( UPLO, 'U' ) .AND. .NOT.LSAME( UPLO, 'L' ) )
     $          THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( NRHS.LT.0 ) THEN
         INFO = -4
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -6
      ELSE IF( LDAF.LT.MAX( 1, N ) ) THEN
         INFO = -8
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -11
      ELSE IF( LDX.LT.MAX( 1, N ) ) THEN
         INFO = -13
      ELSE IF( LWORK.LT.MAX( 1, 2*N ) .AND. .NOT.LQUERY ) THEN
         INFO = -18
      END IF
*
      IF( INFO.EQ.0 ) THEN
         LWKOPT = MAX( 1, 2*N )
         IF( NOFACT ) THEN
            NB = ILAENV( 1, 'CHETRF', UPLO, N, -1, -1, -1 )
            LWKOPT = MAX( LWKOPT, N*NB )
         END IF
         WORK( 1 ) = LWKOPT
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CHESVX', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
      IF( NOFACT ) THEN
*
*        Compute the factorization A = U*D*U**H or A = L*D*L**H.
*
         CALL CLACPY( UPLO, N, N, A, LDA, AF, LDAF )
         CALL CHETRF( UPLO, N, AF, LDAF, IPIV, WORK, LWORK, INFO )
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
      ANORM = CLANHE( 'I', UPLO, N, A, LDA, RWORK )
*
*     Compute the reciprocal of the condition number of A.
*
      CALL CHECON( UPLO, N, AF, LDAF, IPIV, ANORM, RCOND, WORK, INFO )
*
*     Compute the solution vectors X.
*
      CALL CLACPY( 'Full', N, NRHS, B, LDB, X, LDX )
      CALL CHETRS( UPLO, N, NRHS, AF, LDAF, IPIV, X, LDX, INFO )
*
*     Use iterative refinement to improve the computed solutions and
*     compute error bounds and backward error estimates for them.
*
      CALL CHERFS( UPLO, N, NRHS, A, LDA, AF, LDAF, IPIV, B, LDB, X,
     $             LDX, FERR, BERR, WORK, RWORK, INFO )
*
*     Set INFO = N+1 if the matrix is singular to working precision.
*
      IF( RCOND.LT.SLAMCH( 'Epsilon' ) )
     $   INFO = N + 1
*
      WORK( 1 ) = LWKOPT
*
      RETURN
*
*     End of CHESVX
*
      END
