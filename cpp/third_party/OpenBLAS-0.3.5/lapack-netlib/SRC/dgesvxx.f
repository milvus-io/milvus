*> \brief <b> DGESVXX computes the solution to system of linear equations A * X = B for GE matrices</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DGESVXX + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dgesvxx.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dgesvxx.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dgesvxx.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DGESVXX( FACT, TRANS, N, NRHS, A, LDA, AF, LDAF, IPIV,
*                           EQUED, R, C, B, LDB, X, LDX, RCOND, RPVGRW,
*                           BERR, N_ERR_BNDS, ERR_BNDS_NORM,
*                           ERR_BNDS_COMP, NPARAMS, PARAMS, WORK, IWORK,
*                           INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          EQUED, FACT, TRANS
*       INTEGER            INFO, LDA, LDAF, LDB, LDX, N, NRHS, NPARAMS,
*      $                   N_ERR_BNDS
*       DOUBLE PRECISION   RCOND, RPVGRW
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * ), IWORK( * )
*       DOUBLE PRECISION   A( LDA, * ), AF( LDAF, * ), B( LDB, * ),
*      $                   X( LDX , * ),WORK( * )
*       DOUBLE PRECISION   R( * ), C( * ), PARAMS( * ), BERR( * ),
*      $                   ERR_BNDS_NORM( NRHS, * ),
*      $                   ERR_BNDS_COMP( NRHS, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    DGESVXX uses the LU factorization to compute the solution to a
*>    double precision system of linear equations  A * X = B,  where A is an
*>    N-by-N matrix and X and B are N-by-NRHS matrices.
*>
*>    If requested, both normwise and maximum componentwise error bounds
*>    are returned. DGESVXX will return a solution with a tiny
*>    guaranteed error (O(eps) where eps is the working machine
*>    precision) unless the matrix is very ill-conditioned, in which
*>    case a warning is returned. Relevant condition numbers also are
*>    calculated and returned.
*>
*>    DGESVXX accepts user-provided factorizations and equilibration
*>    factors; see the definitions of the FACT and EQUED options.
*>    Solving with refinement and using a factorization from a previous
*>    DGESVXX call will also produce a solution with either O(eps)
*>    errors or warnings, but we cannot make that claim for general
*>    user-provided factorizations and equilibration factors if they
*>    differ from what DGESVXX would itself produce.
*> \endverbatim
*
*> \par Description:
*  =================
*>
*> \verbatim
*>
*>    The following steps are performed:
*>
*>    1. If FACT = 'E', double precision scaling factors are computed to equilibrate
*>    the system:
*>
*>      TRANS = 'N':  diag(R)*A*diag(C)     *inv(diag(C))*X = diag(R)*B
*>      TRANS = 'T': (diag(R)*A*diag(C))**T *inv(diag(R))*X = diag(C)*B
*>      TRANS = 'C': (diag(R)*A*diag(C))**H *inv(diag(R))*X = diag(C)*B
*>
*>    Whether or not the system will be equilibrated depends on the
*>    scaling of the matrix A, but if equilibration is used, A is
*>    overwritten by diag(R)*A*diag(C) and B by diag(R)*B (if TRANS='N')
*>    or diag(C)*B (if TRANS = 'T' or 'C').
*>
*>    2. If FACT = 'N' or 'E', the LU decomposition is used to factor
*>    the matrix A (after equilibration if FACT = 'E') as
*>
*>      A = P * L * U,
*>
*>    where P is a permutation matrix, L is a unit lower triangular
*>    matrix, and U is upper triangular.
*>
*>    3. If some U(i,i)=0, so that U is exactly singular, then the
*>    routine returns with INFO = i. Otherwise, the factored form of A
*>    is used to estimate the condition number of the matrix A (see
*>    argument RCOND). If the reciprocal of the condition number is less
*>    than machine precision, the routine still goes on to solve for X
*>    and compute error bounds as described below.
*>
*>    4. The system of equations is solved for X using the factored form
*>    of A.
*>
*>    5. By default (unless PARAMS(LA_LINRX_ITREF_I) is set to zero),
*>    the routine will use iterative refinement to try to get a small
*>    error and error bounds.  Refinement calculates the residual to at
*>    least twice the working precision.
*>
*>    6. If equilibration was used, the matrix X is premultiplied by
*>    diag(C) (if TRANS = 'N') or diag(R) (if TRANS = 'T' or 'C') so
*>    that it solves the original system before equilibration.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \verbatim
*>     Some optional parameters are bundled in the PARAMS array.  These
*>     settings determine how refinement is performed, but often the
*>     defaults are acceptable.  If the defaults are acceptable, users
*>     can pass NPARAMS = 0 which prevents the source code from accessing
*>     the PARAMS argument.
*> \endverbatim
*>
*> \param[in] FACT
*> \verbatim
*>          FACT is CHARACTER*1
*>     Specifies whether or not the factored form of the matrix A is
*>     supplied on entry, and if not, whether the matrix A should be
*>     equilibrated before it is factored.
*>       = 'F':  On entry, AF and IPIV contain the factored form of A.
*>               If EQUED is not 'N', the matrix A has been
*>               equilibrated with scaling factors given by R and C.
*>               A, AF, and IPIV are not modified.
*>       = 'N':  The matrix A will be copied to AF and factored.
*>       = 'E':  The matrix A will be equilibrated if necessary, then
*>               copied to AF and factored.
*> \endverbatim
*>
*> \param[in] TRANS
*> \verbatim
*>          TRANS is CHARACTER*1
*>     Specifies the form of the system of equations:
*>       = 'N':  A * X = B     (No transpose)
*>       = 'T':  A**T * X = B  (Transpose)
*>       = 'C':  A**H * X = B  (Conjugate Transpose = Transpose)
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>     The number of linear equations, i.e., the order of the
*>     matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] NRHS
*> \verbatim
*>          NRHS is INTEGER
*>     The number of right hand sides, i.e., the number of columns
*>     of the matrices B and X.  NRHS >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension (LDA,N)
*>     On entry, the N-by-N matrix A.  If FACT = 'F' and EQUED is
*>     not 'N', then A must have been equilibrated by the scaling
*>     factors in R and/or C.  A is not modified if FACT = 'F' or
*>     'N', or if FACT = 'E' and EQUED = 'N' on exit.
*>
*>     On exit, if EQUED .ne. 'N', A is scaled as follows:
*>     EQUED = 'R':  A := diag(R) * A
*>     EQUED = 'C':  A := A * diag(C)
*>     EQUED = 'B':  A := diag(R) * A * diag(C).
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>     The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] AF
*> \verbatim
*>          AF is DOUBLE PRECISION array, dimension (LDAF,N)
*>     If FACT = 'F', then AF is an input argument and on entry
*>     contains the factors L and U from the factorization
*>     A = P*L*U as computed by DGETRF.  If EQUED .ne. 'N', then
*>     AF is the factored form of the equilibrated matrix A.
*>
*>     If FACT = 'N', then AF is an output argument and on exit
*>     returns the factors L and U from the factorization A = P*L*U
*>     of the original matrix A.
*>
*>     If FACT = 'E', then AF is an output argument and on exit
*>     returns the factors L and U from the factorization A = P*L*U
*>     of the equilibrated matrix A (see the description of A for
*>     the form of the equilibrated matrix).
*> \endverbatim
*>
*> \param[in] LDAF
*> \verbatim
*>          LDAF is INTEGER
*>     The leading dimension of the array AF.  LDAF >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (N)
*>     If FACT = 'F', then IPIV is an input argument and on entry
*>     contains the pivot indices from the factorization A = P*L*U
*>     as computed by DGETRF; row i of the matrix was interchanged
*>     with row IPIV(i).
*>
*>     If FACT = 'N', then IPIV is an output argument and on exit
*>     contains the pivot indices from the factorization A = P*L*U
*>     of the original matrix A.
*>
*>     If FACT = 'E', then IPIV is an output argument and on exit
*>     contains the pivot indices from the factorization A = P*L*U
*>     of the equilibrated matrix A.
*> \endverbatim
*>
*> \param[in,out] EQUED
*> \verbatim
*>          EQUED is CHARACTER*1
*>     Specifies the form of equilibration that was done.
*>       = 'N':  No equilibration (always true if FACT = 'N').
*>       = 'R':  Row equilibration, i.e., A has been premultiplied by
*>               diag(R).
*>       = 'C':  Column equilibration, i.e., A has been postmultiplied
*>               by diag(C).
*>       = 'B':  Both row and column equilibration, i.e., A has been
*>               replaced by diag(R) * A * diag(C).
*>     EQUED is an input argument if FACT = 'F'; otherwise, it is an
*>     output argument.
*> \endverbatim
*>
*> \param[in,out] R
*> \verbatim
*>          R is DOUBLE PRECISION array, dimension (N)
*>     The row scale factors for A.  If EQUED = 'R' or 'B', A is
*>     multiplied on the left by diag(R); if EQUED = 'N' or 'C', R
*>     is not accessed.  R is an input argument if FACT = 'F';
*>     otherwise, R is an output argument.  If FACT = 'F' and
*>     EQUED = 'R' or 'B', each element of R must be positive.
*>     If R is output, each element of R is a power of the radix.
*>     If R is input, each element of R should be a power of the radix
*>     to ensure a reliable solution and error estimates. Scaling by
*>     powers of the radix does not cause rounding errors unless the
*>     result underflows or overflows. Rounding errors during scaling
*>     lead to refining with a matrix that is not equivalent to the
*>     input matrix, producing error estimates that may not be
*>     reliable.
*> \endverbatim
*>
*> \param[in,out] C
*> \verbatim
*>          C is DOUBLE PRECISION array, dimension (N)
*>     The column scale factors for A.  If EQUED = 'C' or 'B', A is
*>     multiplied on the right by diag(C); if EQUED = 'N' or 'R', C
*>     is not accessed.  C is an input argument if FACT = 'F';
*>     otherwise, C is an output argument.  If FACT = 'F' and
*>     EQUED = 'C' or 'B', each element of C must be positive.
*>     If C is output, each element of C is a power of the radix.
*>     If C is input, each element of C should be a power of the radix
*>     to ensure a reliable solution and error estimates. Scaling by
*>     powers of the radix does not cause rounding errors unless the
*>     result underflows or overflows. Rounding errors during scaling
*>     lead to refining with a matrix that is not equivalent to the
*>     input matrix, producing error estimates that may not be
*>     reliable.
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is DOUBLE PRECISION array, dimension (LDB,NRHS)
*>     On entry, the N-by-NRHS right hand side matrix B.
*>     On exit,
*>     if EQUED = 'N', B is not modified;
*>     if TRANS = 'N' and EQUED = 'R' or 'B', B is overwritten by
*>        diag(R)*B;
*>     if TRANS = 'T' or 'C' and EQUED = 'C' or 'B', B is
*>        overwritten by diag(C)*B.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>     The leading dimension of the array B.  LDB >= max(1,N).
*> \endverbatim
*>
*> \param[out] X
*> \verbatim
*>          X is DOUBLE PRECISION array, dimension (LDX,NRHS)
*>     If INFO = 0, the N-by-NRHS solution matrix X to the original
*>     system of equations.  Note that A and B are modified on exit
*>     if EQUED .ne. 'N', and the solution to the equilibrated system is
*>     inv(diag(C))*X if TRANS = 'N' and EQUED = 'C' or 'B', or
*>     inv(diag(R))*X if TRANS = 'T' or 'C' and EQUED = 'R' or 'B'.
*> \endverbatim
*>
*> \param[in] LDX
*> \verbatim
*>          LDX is INTEGER
*>     The leading dimension of the array X.  LDX >= max(1,N).
*> \endverbatim
*>
*> \param[out] RCOND
*> \verbatim
*>          RCOND is DOUBLE PRECISION
*>     Reciprocal scaled condition number.  This is an estimate of the
*>     reciprocal Skeel condition number of the matrix A after
*>     equilibration (if done).  If this is less than the machine
*>     precision (in particular, if it is zero), the matrix is singular
*>     to working precision.  Note that the error may still be small even
*>     if this number is very small and the matrix appears ill-
*>     conditioned.
*> \endverbatim
*>
*> \param[out] RPVGRW
*> \verbatim
*>          RPVGRW is DOUBLE PRECISION
*>     Reciprocal pivot growth.  On exit, this contains the reciprocal
*>     pivot growth factor norm(A)/norm(U). The "max absolute element"
*>     norm is used.  If this is much less than 1, then the stability of
*>     the LU factorization of the (equilibrated) matrix A could be poor.
*>     This also means that the solution X, estimated condition numbers,
*>     and error bounds could be unreliable. If factorization fails with
*>     0<INFO<=N, then this contains the reciprocal pivot growth factor
*>     for the leading INFO columns of A.  In DGESVX, this quantity is
*>     returned in WORK(1).
*> \endverbatim
*>
*> \param[out] BERR
*> \verbatim
*>          BERR is DOUBLE PRECISION array, dimension (NRHS)
*>     Componentwise relative backward error.  This is the
*>     componentwise relative backward error of each solution vector X(j)
*>     (i.e., the smallest relative change in any element of A or B that
*>     makes X(j) an exact solution).
*> \endverbatim
*>
*> \param[in] N_ERR_BNDS
*> \verbatim
*>          N_ERR_BNDS is INTEGER
*>     Number of error bounds to return for each right hand side
*>     and each type (normwise or componentwise).  See ERR_BNDS_NORM and
*>     ERR_BNDS_COMP below.
*> \endverbatim
*>
*> \param[out] ERR_BNDS_NORM
*> \verbatim
*>          ERR_BNDS_NORM is DOUBLE PRECISION array, dimension (NRHS, N_ERR_BNDS)
*>     For each right-hand side, this array contains information about
*>     various error bounds and condition numbers corresponding to the
*>     normwise relative error, which is defined as follows:
*>
*>     Normwise relative error in the ith solution vector:
*>             max_j (abs(XTRUE(j,i) - X(j,i)))
*>            ------------------------------
*>                  max_j abs(X(j,i))
*>
*>     The array is indexed by the type of error information as described
*>     below. There currently are up to three pieces of information
*>     returned.
*>
*>     The first index in ERR_BNDS_NORM(i,:) corresponds to the ith
*>     right-hand side.
*>
*>     The second index in ERR_BNDS_NORM(:,err) contains the following
*>     three fields:
*>     err = 1 "Trust/don't trust" boolean. Trust the answer if the
*>              reciprocal condition number is less than the threshold
*>              sqrt(n) * dlamch('Epsilon').
*>
*>     err = 2 "Guaranteed" error bound: The estimated forward error,
*>              almost certainly within a factor of 10 of the true error
*>              so long as the next entry is greater than the threshold
*>              sqrt(n) * dlamch('Epsilon'). This error bound should only
*>              be trusted if the previous boolean is true.
*>
*>     err = 3  Reciprocal condition number: Estimated normwise
*>              reciprocal condition number.  Compared with the threshold
*>              sqrt(n) * dlamch('Epsilon') to determine if the error
*>              estimate is "guaranteed". These reciprocal condition
*>              numbers are 1 / (norm(Z^{-1},inf) * norm(Z,inf)) for some
*>              appropriately scaled matrix Z.
*>              Let Z = S*A, where S scales each row by a power of the
*>              radix so all absolute row sums of Z are approximately 1.
*>
*>     See Lapack Working Note 165 for further details and extra
*>     cautions.
*> \endverbatim
*>
*> \param[out] ERR_BNDS_COMP
*> \verbatim
*>          ERR_BNDS_COMP is DOUBLE PRECISION array, dimension (NRHS, N_ERR_BNDS)
*>     For each right-hand side, this array contains information about
*>     various error bounds and condition numbers corresponding to the
*>     componentwise relative error, which is defined as follows:
*>
*>     Componentwise relative error in the ith solution vector:
*>                    abs(XTRUE(j,i) - X(j,i))
*>             max_j ----------------------
*>                         abs(X(j,i))
*>
*>     The array is indexed by the right-hand side i (on which the
*>     componentwise relative error depends), and the type of error
*>     information as described below. There currently are up to three
*>     pieces of information returned for each right-hand side. If
*>     componentwise accuracy is not requested (PARAMS(3) = 0.0), then
*>     ERR_BNDS_COMP is not accessed.  If N_ERR_BNDS .LT. 3, then at most
*>     the first (:,N_ERR_BNDS) entries are returned.
*>
*>     The first index in ERR_BNDS_COMP(i,:) corresponds to the ith
*>     right-hand side.
*>
*>     The second index in ERR_BNDS_COMP(:,err) contains the following
*>     three fields:
*>     err = 1 "Trust/don't trust" boolean. Trust the answer if the
*>              reciprocal condition number is less than the threshold
*>              sqrt(n) * dlamch('Epsilon').
*>
*>     err = 2 "Guaranteed" error bound: The estimated forward error,
*>              almost certainly within a factor of 10 of the true error
*>              so long as the next entry is greater than the threshold
*>              sqrt(n) * dlamch('Epsilon'). This error bound should only
*>              be trusted if the previous boolean is true.
*>
*>     err = 3  Reciprocal condition number: Estimated componentwise
*>              reciprocal condition number.  Compared with the threshold
*>              sqrt(n) * dlamch('Epsilon') to determine if the error
*>              estimate is "guaranteed". These reciprocal condition
*>              numbers are 1 / (norm(Z^{-1},inf) * norm(Z,inf)) for some
*>              appropriately scaled matrix Z.
*>              Let Z = S*(A*diag(x)), where x is the solution for the
*>              current right-hand side and S scales each row of
*>              A*diag(x) by a power of the radix so all absolute row
*>              sums of Z are approximately 1.
*>
*>     See Lapack Working Note 165 for further details and extra
*>     cautions.
*> \endverbatim
*>
*> \param[in] NPARAMS
*> \verbatim
*>          NPARAMS is INTEGER
*>     Specifies the number of parameters set in PARAMS.  If .LE. 0, the
*>     PARAMS array is never referenced and default values are used.
*> \endverbatim
*>
*> \param[in,out] PARAMS
*> \verbatim
*>          PARAMS is DOUBLE PRECISION array, dimension (NPARAMS)
*>     Specifies algorithm parameters.  If an entry is .LT. 0.0, then
*>     that entry will be filled with default value used for that
*>     parameter.  Only positions up to NPARAMS are accessed; defaults
*>     are used for higher-numbered parameters.
*>
*>       PARAMS(LA_LINRX_ITREF_I = 1) : Whether to perform iterative
*>            refinement or not.
*>         Default: 1.0D+0
*>            = 0.0 : No refinement is performed, and no error bounds are
*>                    computed.
*>            = 1.0 : Use the extra-precise refinement algorithm.
*>              (other values are reserved for future use)
*>
*>       PARAMS(LA_LINRX_ITHRESH_I = 2) : Maximum number of residual
*>            computations allowed for refinement.
*>         Default: 10
*>         Aggressive: Set to 100 to permit convergence using approximate
*>                     factorizations or factorizations other than LU. If
*>                     the factorization uses a technique other than
*>                     Gaussian elimination, the guarantees in
*>                     err_bnds_norm and err_bnds_comp may no longer be
*>                     trustworthy.
*>
*>       PARAMS(LA_LINRX_CWISE_I = 3) : Flag determining if the code
*>            will attempt to find a solution with small componentwise
*>            relative error in the double-precision algorithm.  Positive
*>            is true, 0.0 is false.
*>         Default: 1.0 (attempt componentwise convergence)
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (4*N)
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
*>       = 0:  Successful exit. The solution to every right-hand side is
*>         guaranteed.
*>       < 0:  If INFO = -i, the i-th argument had an illegal value
*>       > 0 and <= N:  U(INFO,INFO) is exactly zero.  The factorization
*>         has been completed, but the factor U is exactly singular, so
*>         the solution and error bounds could not be computed. RCOND = 0
*>         is returned.
*>       = N+J: The solution corresponding to the Jth right-hand side is
*>         not guaranteed. The solutions corresponding to other right-
*>         hand sides K with K > J may not be guaranteed as well, but
*>         only the first such right-hand side is reported. If a small
*>         componentwise error is not requested (PARAMS(3) = 0.0) then
*>         the Jth right-hand side is the first with a normwise error
*>         bound that is not guaranteed (the smallest J such
*>         that ERR_BNDS_NORM(J,1) = 0.0). By default (PARAMS(3) = 1.0)
*>         the Jth right-hand side is the first with either a normwise or
*>         componentwise error bound that is not guaranteed (the smallest
*>         J such that either ERR_BNDS_NORM(J,1) = 0.0 or
*>         ERR_BNDS_COMP(J,1) = 0.0). See the definition of
*>         ERR_BNDS_NORM(:,1) and ERR_BNDS_COMP(:,1). To get information
*>         about all of the right-hand sides check ERR_BNDS_NORM or
*>         ERR_BNDS_COMP.
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
*> \ingroup doubleGEsolve
*
*  =====================================================================
      SUBROUTINE DGESVXX( FACT, TRANS, N, NRHS, A, LDA, AF, LDAF, IPIV,
     $                    EQUED, R, C, B, LDB, X, LDX, RCOND, RPVGRW,
     $                    BERR, N_ERR_BNDS, ERR_BNDS_NORM,
     $                    ERR_BNDS_COMP, NPARAMS, PARAMS, WORK, IWORK,
     $                    INFO )
*
*  -- LAPACK driver routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     April 2012
*
*     .. Scalar Arguments ..
      CHARACTER          EQUED, FACT, TRANS
      INTEGER            INFO, LDA, LDAF, LDB, LDX, N, NRHS, NPARAMS,
     $                   N_ERR_BNDS
      DOUBLE PRECISION   RCOND, RPVGRW
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * ), IWORK( * )
      DOUBLE PRECISION   A( LDA, * ), AF( LDAF, * ), B( LDB, * ),
     $                   X( LDX , * ),WORK( * )
      DOUBLE PRECISION   R( * ), C( * ), PARAMS( * ), BERR( * ),
     $                   ERR_BNDS_NORM( NRHS, * ),
     $                   ERR_BNDS_COMP( NRHS, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
      INTEGER            FINAL_NRM_ERR_I, FINAL_CMP_ERR_I, BERR_I
      INTEGER            RCOND_I, NRM_RCOND_I, NRM_ERR_I, CMP_RCOND_I
      INTEGER            CMP_ERR_I, PIV_GROWTH_I
      PARAMETER          ( FINAL_NRM_ERR_I = 1, FINAL_CMP_ERR_I = 2,
     $                   BERR_I = 3 )
      PARAMETER          ( RCOND_I = 4, NRM_RCOND_I = 5, NRM_ERR_I = 6 )
      PARAMETER          ( CMP_RCOND_I = 7, CMP_ERR_I = 8,
     $                   PIV_GROWTH_I = 9 )
*     ..
*     .. Local Scalars ..
      LOGICAL            COLEQU, EQUIL, NOFACT, NOTRAN, ROWEQU
      INTEGER            INFEQU, J
      DOUBLE PRECISION   AMAX, BIGNUM, COLCND, RCMAX, RCMIN, ROWCND,
     $                   SMLNUM
*     ..
*     .. External Functions ..
      EXTERNAL           LSAME, DLAMCH, DLA_GERPVGRW
      LOGICAL            LSAME
      DOUBLE PRECISION   DLAMCH, DLA_GERPVGRW
*     ..
*     .. External Subroutines ..
      EXTERNAL           DGEEQUB, DGETRF, DGETRS, DLACPY, DLAQGE,
     $                   XERBLA, DLASCL2, DGERFSX
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN
*     ..
*     .. Executable Statements ..
*
      INFO = 0
      NOFACT = LSAME( FACT, 'N' )
      EQUIL = LSAME( FACT, 'E' )
      NOTRAN = LSAME( TRANS, 'N' )
      SMLNUM = DLAMCH( 'Safe minimum' )
      BIGNUM = ONE / SMLNUM
      IF( NOFACT .OR. EQUIL ) THEN
         EQUED = 'N'
         ROWEQU = .FALSE.
         COLEQU = .FALSE.
      ELSE
         ROWEQU = LSAME( EQUED, 'R' ) .OR. LSAME( EQUED, 'B' )
         COLEQU = LSAME( EQUED, 'C' ) .OR. LSAME( EQUED, 'B' )
      END IF
*
*     Default is failure.  If an input parameter is wrong or
*     factorization fails, make everything look horrible.  Only the
*     pivot growth is set here, the rest is initialized in DGERFSX.
*
      RPVGRW = ZERO
*
*     Test the input parameters.  PARAMS is not tested until DGERFSX.
*
      IF( .NOT.NOFACT .AND. .NOT.EQUIL .AND. .NOT.
     $     LSAME( FACT, 'F' ) ) THEN
         INFO = -1
      ELSE IF( .NOT.NOTRAN .AND. .NOT.LSAME( TRANS, 'T' ) .AND. .NOT.
     $        LSAME( TRANS, 'C' ) ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( NRHS.LT.0 ) THEN
         INFO = -4
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -6
      ELSE IF( LDAF.LT.MAX( 1, N ) ) THEN
         INFO = -8
      ELSE IF( LSAME( FACT, 'F' ) .AND. .NOT.
     $        ( ROWEQU .OR. COLEQU .OR. LSAME( EQUED, 'N' ) ) ) THEN
         INFO = -10
      ELSE
         IF( ROWEQU ) THEN
            RCMIN = BIGNUM
            RCMAX = ZERO
            DO 10 J = 1, N
               RCMIN = MIN( RCMIN, R( J ) )
               RCMAX = MAX( RCMAX, R( J ) )
 10         CONTINUE
            IF( RCMIN.LE.ZERO ) THEN
               INFO = -11
            ELSE IF( N.GT.0 ) THEN
               ROWCND = MAX( RCMIN, SMLNUM ) / MIN( RCMAX, BIGNUM )
            ELSE
               ROWCND = ONE
            END IF
         END IF
         IF( COLEQU .AND. INFO.EQ.0 ) THEN
            RCMIN = BIGNUM
            RCMAX = ZERO
            DO 20 J = 1, N
               RCMIN = MIN( RCMIN, C( J ) )
               RCMAX = MAX( RCMAX, C( J ) )
 20         CONTINUE
            IF( RCMIN.LE.ZERO ) THEN
               INFO = -12
            ELSE IF( N.GT.0 ) THEN
               COLCND = MAX( RCMIN, SMLNUM ) / MIN( RCMAX, BIGNUM )
            ELSE
               COLCND = ONE
            END IF
         END IF
         IF( INFO.EQ.0 ) THEN
            IF( LDB.LT.MAX( 1, N ) ) THEN
               INFO = -14
            ELSE IF( LDX.LT.MAX( 1, N ) ) THEN
               INFO = -16
            END IF
         END IF
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DGESVXX', -INFO )
         RETURN
      END IF
*
      IF( EQUIL ) THEN
*
*     Compute row and column scalings to equilibrate the matrix A.
*
         CALL DGEEQUB( N, N, A, LDA, R, C, ROWCND, COLCND, AMAX,
     $        INFEQU )
         IF( INFEQU.EQ.0 ) THEN
*
*     Equilibrate the matrix.
*
            CALL DLAQGE( N, N, A, LDA, R, C, ROWCND, COLCND, AMAX,
     $           EQUED )
            ROWEQU = LSAME( EQUED, 'R' ) .OR. LSAME( EQUED, 'B' )
            COLEQU = LSAME( EQUED, 'C' ) .OR. LSAME( EQUED, 'B' )
         END IF
*
*     If the scaling factors are not applied, set them to 1.0.
*
         IF ( .NOT.ROWEQU ) THEN
            DO J = 1, N
               R( J ) = 1.0D+0
            END DO
         END IF
         IF ( .NOT.COLEQU ) THEN
            DO J = 1, N
               C( J ) = 1.0D+0
            END DO
         END IF
      END IF
*
*     Scale the right-hand side.
*
      IF( NOTRAN ) THEN
         IF( ROWEQU ) CALL DLASCL2( N, NRHS, R, B, LDB )
      ELSE
         IF( COLEQU ) CALL DLASCL2( N, NRHS, C, B, LDB )
      END IF
*
      IF( NOFACT .OR. EQUIL ) THEN
*
*        Compute the LU factorization of A.
*
         CALL DLACPY( 'Full', N, N, A, LDA, AF, LDAF )
         CALL DGETRF( N, N, AF, LDAF, IPIV, INFO )
*
*        Return if INFO is non-zero.
*
         IF( INFO.GT.0 ) THEN
*
*           Pivot in column INFO is exactly 0
*           Compute the reciprocal pivot growth factor of the
*           leading rank-deficient INFO columns of A.
*
            RPVGRW = DLA_GERPVGRW( N, INFO, A, LDA, AF, LDAF )
            RETURN
         END IF
      END IF
*
*     Compute the reciprocal pivot growth factor RPVGRW.
*
      RPVGRW = DLA_GERPVGRW( N, N, A, LDA, AF, LDAF )
*
*     Compute the solution matrix X.
*
      CALL DLACPY( 'Full', N, NRHS, B, LDB, X, LDX )
      CALL DGETRS( TRANS, N, NRHS, AF, LDAF, IPIV, X, LDX, INFO )
*
*     Use iterative refinement to improve the computed solution and
*     compute error bounds and backward error estimates for it.
*
      CALL DGERFSX( TRANS, EQUED, N, NRHS, A, LDA, AF, LDAF,
     $     IPIV, R, C, B, LDB, X, LDX, RCOND, BERR,
     $     N_ERR_BNDS, ERR_BNDS_NORM, ERR_BNDS_COMP, NPARAMS, PARAMS,
     $     WORK, IWORK, INFO )
*
*     Scale solutions.
*
      IF ( COLEQU .AND. NOTRAN ) THEN
         CALL DLASCL2 ( N, NRHS, C, X, LDX )
      ELSE IF ( ROWEQU .AND. .NOT.NOTRAN ) THEN
         CALL DLASCL2 ( N, NRHS, R, X, LDX )
      END IF
*
      RETURN
*
*     End of DGESVXX

      END
