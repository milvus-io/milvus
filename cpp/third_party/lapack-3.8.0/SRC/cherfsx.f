*> \brief \b CHERFSX
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CHERFSX + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cherfsx.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cherfsx.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cherfsx.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CHERFSX( UPLO, EQUED, N, NRHS, A, LDA, AF, LDAF, IPIV,
*                           S, B, LDB, X, LDX, RCOND, BERR, N_ERR_BNDS,
*                           ERR_BNDS_NORM, ERR_BNDS_COMP, NPARAMS, PARAMS,
*                           WORK, RWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO, EQUED
*       INTEGER            INFO, LDA, LDAF, LDB, LDX, N, NRHS, NPARAMS,
*      $                   N_ERR_BNDS
*       REAL               RCOND
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * )
*       COMPLEX            A( LDA, * ), AF( LDAF, * ), B( LDB, * ),
*      $                   X( LDX, * ), WORK( * )
*       REAL               S( * ), PARAMS( * ), BERR( * ), RWORK( * ),
*      $                   ERR_BNDS_NORM( NRHS, * ),
*      $                   ERR_BNDS_COMP( NRHS, * )
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    CHERFSX improves the computed solution to a system of linear
*>    equations when the coefficient matrix is Hermitian indefinite, and
*>    provides error bounds and backward error estimates for the
*>    solution.  In addition to normwise error bound, the code provides
*>    maximum componentwise error bound if possible.  See comments for
*>    ERR_BNDS_NORM and ERR_BNDS_COMP for details of the error bounds.
*>
*>    The original system of linear equations may have been equilibrated
*>    before calling this routine, as described by arguments EQUED and S
*>    below. In this case, the solution and error bounds returned are
*>    for the original unequilibrated system.
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
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>       = 'U':  Upper triangle of A is stored;
*>       = 'L':  Lower triangle of A is stored.
*> \endverbatim
*>
*> \param[in] EQUED
*> \verbatim
*>          EQUED is CHARACTER*1
*>     Specifies the form of equilibration that was done to A
*>     before calling this routine. This is needed to compute
*>     the solution and error bounds correctly.
*>       = 'N':  No equilibration
*>       = 'Y':  Both row and column equilibration, i.e., A has been
*>               replaced by diag(S) * A * diag(S).
*>               The right hand side B has been changed accordingly.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>     The order of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] NRHS
*> \verbatim
*>          NRHS is INTEGER
*>     The number of right hand sides, i.e., the number of columns
*>     of the matrices B and X.  NRHS >= 0.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N)
*>     The symmetric matrix A.  If UPLO = 'U', the leading N-by-N
*>     upper triangular part of A contains the upper triangular
*>     part of the matrix A, and the strictly lower triangular
*>     part of A is not referenced.  If UPLO = 'L', the leading
*>     N-by-N lower triangular part of A contains the lower
*>     triangular part of the matrix A, and the strictly upper
*>     triangular part of A is not referenced.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>     The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[in] AF
*> \verbatim
*>          AF is COMPLEX array, dimension (LDAF,N)
*>     The factored form of the matrix A.  AF contains the block
*>     diagonal matrix D and the multipliers used to obtain the
*>     factor U or L from the factorization A = U*D*U**T or A =
*>     L*D*L**T as computed by SSYTRF.
*> \endverbatim
*>
*> \param[in] LDAF
*> \verbatim
*>          LDAF is INTEGER
*>     The leading dimension of the array AF.  LDAF >= max(1,N).
*> \endverbatim
*>
*> \param[in] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (N)
*>     Details of the interchanges and the block structure of D
*>     as determined by SSYTRF.
*> \endverbatim
*>
*> \param[in,out] S
*> \verbatim
*>          S is REAL array, dimension (N)
*>     The scale factors for A.  If EQUED = 'Y', A is multiplied on
*>     the left and right by diag(S).  S is an input argument if FACT =
*>     'F'; otherwise, S is an output argument.  If FACT = 'F' and EQUED
*>     = 'Y', each element of S must be positive.  If S is output, each
*>     element of S is a power of the radix. If S is input, each element
*>     of S should be a power of the radix to ensure a reliable solution
*>     and error estimates. Scaling by powers of the radix does not cause
*>     rounding errors unless the result underflows or overflows.
*>     Rounding errors during scaling lead to refining with a matrix that
*>     is not equivalent to the input matrix, producing error estimates
*>     that may not be reliable.
*> \endverbatim
*>
*> \param[in] B
*> \verbatim
*>          B is COMPLEX array, dimension (LDB,NRHS)
*>     The right hand side matrix B.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>     The leading dimension of the array B.  LDB >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] X
*> \verbatim
*>          X is COMPLEX array, dimension (LDX,NRHS)
*>     On entry, the solution matrix X, as computed by SGETRS.
*>     On exit, the improved solution matrix X.
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
*>          RCOND is REAL
*>     Reciprocal scaled condition number.  This is an estimate of the
*>     reciprocal Skeel condition number of the matrix A after
*>     equilibration (if done).  If this is less than the machine
*>     precision (in particular, if it is zero), the matrix is singular
*>     to working precision.  Note that the error may still be small even
*>     if this number is very small and the matrix appears ill-
*>     conditioned.
*> \endverbatim
*>
*> \param[out] BERR
*> \verbatim
*>          BERR is REAL array, dimension (NRHS)
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
*>          ERR_BNDS_NORM is REAL array, dimension (NRHS, N_ERR_BNDS)
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
*>              sqrt(n) * slamch('Epsilon').
*>
*>     err = 2 "Guaranteed" error bound: The estimated forward error,
*>              almost certainly within a factor of 10 of the true error
*>              so long as the next entry is greater than the threshold
*>              sqrt(n) * slamch('Epsilon'). This error bound should only
*>              be trusted if the previous boolean is true.
*>
*>     err = 3  Reciprocal condition number: Estimated normwise
*>              reciprocal condition number.  Compared with the threshold
*>              sqrt(n) * slamch('Epsilon') to determine if the error
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
*>          ERR_BNDS_COMP is REAL array, dimension (NRHS, N_ERR_BNDS)
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
*>              sqrt(n) * slamch('Epsilon').
*>
*>     err = 2 "Guaranteed" error bound: The estimated forward error,
*>              almost certainly within a factor of 10 of the true error
*>              so long as the next entry is greater than the threshold
*>              sqrt(n) * slamch('Epsilon'). This error bound should only
*>              be trusted if the previous boolean is true.
*>
*>     err = 3  Reciprocal condition number: Estimated componentwise
*>              reciprocal condition number.  Compared with the threshold
*>              sqrt(n) * slamch('Epsilon') to determine if the error
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
*>          PARAMS is REAL array, dimension NPARAMS
*>     Specifies algorithm parameters.  If an entry is .LT. 0.0, then
*>     that entry will be filled with default value used for that
*>     parameter.  Only positions up to NPARAMS are accessed; defaults
*>     are used for higher-numbered parameters.
*>
*>       PARAMS(LA_LINRX_ITREF_I = 1) : Whether to perform iterative
*>            refinement or not.
*>         Default: 1.0
*>            = 0.0 : No refinement is performed, and no error bounds are
*>                    computed.
*>            = 1.0 : Use the double-precision refinement algorithm,
*>                    possibly with doubled-single computations if the
*>                    compilation environment does not support DOUBLE
*>                    PRECISION.
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
*>          WORK is COMPLEX array, dimension (2*N)
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is REAL array, dimension (2*N)
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
*> \ingroup complexHEcomputational
*
*  =====================================================================
      SUBROUTINE CHERFSX( UPLO, EQUED, N, NRHS, A, LDA, AF, LDAF, IPIV,
     $                    S, B, LDB, X, LDX, RCOND, BERR, N_ERR_BNDS,
     $                    ERR_BNDS_NORM, ERR_BNDS_COMP, NPARAMS, PARAMS,
     $                    WORK, RWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     April 2012
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO, EQUED
      INTEGER            INFO, LDA, LDAF, LDB, LDX, N, NRHS, NPARAMS,
     $                   N_ERR_BNDS
      REAL               RCOND
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * )
      COMPLEX            A( LDA, * ), AF( LDAF, * ), B( LDB, * ),
     $                   X( LDX, * ), WORK( * )
      REAL               S( * ), PARAMS( * ), BERR( * ), RWORK( * ),
     $                   ERR_BNDS_NORM( NRHS, * ),
     $                   ERR_BNDS_COMP( NRHS, * )
*
*  ==================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
      REAL               ITREF_DEFAULT, ITHRESH_DEFAULT,
     $                   COMPONENTWISE_DEFAULT
      REAL               RTHRESH_DEFAULT, DZTHRESH_DEFAULT
      PARAMETER          ( ITREF_DEFAULT = 1.0 )
      PARAMETER          ( ITHRESH_DEFAULT = 10.0 )
      PARAMETER          ( COMPONENTWISE_DEFAULT = 1.0 )
      PARAMETER          ( RTHRESH_DEFAULT = 0.5 )
      PARAMETER          ( DZTHRESH_DEFAULT = 0.25 )
      INTEGER            LA_LINRX_ITREF_I, LA_LINRX_ITHRESH_I,
     $                   LA_LINRX_CWISE_I
      PARAMETER          ( LA_LINRX_ITREF_I = 1,
     $                   LA_LINRX_ITHRESH_I = 2 )
      PARAMETER          ( LA_LINRX_CWISE_I = 3 )
      INTEGER            LA_LINRX_TRUST_I, LA_LINRX_ERR_I,
     $                   LA_LINRX_RCOND_I
      PARAMETER          ( LA_LINRX_TRUST_I = 1, LA_LINRX_ERR_I = 2 )
      PARAMETER          ( LA_LINRX_RCOND_I = 3 )
*     ..
*     .. Local Scalars ..
      CHARACTER(1)       NORM
      LOGICAL            RCEQU
      INTEGER            J, PREC_TYPE, REF_TYPE
      INTEGER            N_NORMS
      REAL               ANORM, RCOND_TMP
      REAL               ILLRCOND_THRESH, ERR_LBND, CWISE_WRONG
      LOGICAL            IGNORE_CWISE
      INTEGER            ITHRESH
      REAL               RTHRESH, UNSTABLE_THRESH
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA, CHECON, CLA_HERFSX_EXTENDED
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, SQRT, TRANSFER
*     ..
*     .. External Functions ..
      EXTERNAL           LSAME, ILAPREC
      EXTERNAL           SLAMCH, CLANHE, CLA_HERCOND_X, CLA_HERCOND_C
      REAL               SLAMCH, CLANHE, CLA_HERCOND_X, CLA_HERCOND_C
      LOGICAL            LSAME
      INTEGER            ILAPREC
*     ..
*     .. Executable Statements ..
*
*     Check the input parameters.
*
      INFO = 0
      REF_TYPE = INT( ITREF_DEFAULT )
      IF ( NPARAMS .GE. LA_LINRX_ITREF_I ) THEN
         IF ( PARAMS( LA_LINRX_ITREF_I ) .LT. 0.0 ) THEN
            PARAMS( LA_LINRX_ITREF_I ) = ITREF_DEFAULT
         ELSE
            REF_TYPE = PARAMS( LA_LINRX_ITREF_I )
         END IF
      END IF
*
*     Set default parameters.
*
      ILLRCOND_THRESH = REAL( N ) * SLAMCH( 'Epsilon' )
      ITHRESH = INT( ITHRESH_DEFAULT )
      RTHRESH = RTHRESH_DEFAULT
      UNSTABLE_THRESH = DZTHRESH_DEFAULT
      IGNORE_CWISE = COMPONENTWISE_DEFAULT .EQ. 0.0
*
      IF ( NPARAMS.GE.LA_LINRX_ITHRESH_I ) THEN
         IF ( PARAMS( LA_LINRX_ITHRESH_I ).LT.0.0 ) THEN
            PARAMS( LA_LINRX_ITHRESH_I ) = ITHRESH
         ELSE
            ITHRESH = INT( PARAMS( LA_LINRX_ITHRESH_I ) )
         END IF
      END IF
      IF ( NPARAMS.GE.LA_LINRX_CWISE_I ) THEN
         IF ( PARAMS(LA_LINRX_CWISE_I ).LT.0.0 ) THEN
            IF ( IGNORE_CWISE ) THEN
               PARAMS( LA_LINRX_CWISE_I ) = 0.0
            ELSE
               PARAMS( LA_LINRX_CWISE_I ) = 1.0
            END IF
         ELSE
            IGNORE_CWISE = PARAMS( LA_LINRX_CWISE_I ) .EQ. 0.0
         END IF
      END IF
      IF ( REF_TYPE .EQ. 0 .OR. N_ERR_BNDS .EQ. 0 ) THEN
         N_NORMS = 0
      ELSE IF ( IGNORE_CWISE ) THEN
         N_NORMS = 1
      ELSE
         N_NORMS = 2
      END IF
*
      RCEQU = LSAME( EQUED, 'Y' )
*
*     Test input parameters.
*
      IF (.NOT.LSAME( UPLO, 'U' ) .AND. .NOT.LSAME( UPLO, 'L' ) ) THEN
        INFO = -1
      ELSE IF( .NOT.RCEQU .AND. .NOT.LSAME( EQUED, 'N' ) ) THEN
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
        INFO = -12
      ELSE IF( LDX.LT.MAX( 1, N ) ) THEN
        INFO = -14
      END IF
      IF( INFO.NE.0 ) THEN
        CALL XERBLA( 'CHERFSX', -INFO )
        RETURN
      END IF
*
*     Quick return if possible.
*
      IF( N.EQ.0 .OR. NRHS.EQ.0 ) THEN
         RCOND = 1.0
         DO J = 1, NRHS
            BERR( J ) = 0.0
            IF ( N_ERR_BNDS .GE. 1 ) THEN
               ERR_BNDS_NORM( J, LA_LINRX_TRUST_I ) = 1.0
               ERR_BNDS_COMP( J, LA_LINRX_TRUST_I ) = 1.0
            END IF
            IF ( N_ERR_BNDS .GE. 2 ) THEN
               ERR_BNDS_NORM( J, LA_LINRX_ERR_I ) = 0.0
               ERR_BNDS_COMP( J, LA_LINRX_ERR_I ) = 0.0
            END IF
            IF ( N_ERR_BNDS .GE. 3 ) THEN
               ERR_BNDS_NORM( J, LA_LINRX_RCOND_I ) = 1.0
               ERR_BNDS_COMP( J, LA_LINRX_RCOND_I ) = 1.0
            END IF
         END DO
         RETURN
      END IF
*
*     Default to failure.
*
      RCOND = 0.0
      DO J = 1, NRHS
         BERR( J ) = 1.0
         IF ( N_ERR_BNDS .GE. 1 ) THEN
            ERR_BNDS_NORM( J, LA_LINRX_TRUST_I ) = 1.0
            ERR_BNDS_COMP( J, LA_LINRX_TRUST_I ) = 1.0
         END IF
         IF ( N_ERR_BNDS .GE. 2 ) THEN
            ERR_BNDS_NORM( J, LA_LINRX_ERR_I ) = 1.0
            ERR_BNDS_COMP( J, LA_LINRX_ERR_I ) = 1.0
         END IF
         IF ( N_ERR_BNDS .GE. 3 ) THEN
            ERR_BNDS_NORM( J, LA_LINRX_RCOND_I ) = 0.0
            ERR_BNDS_COMP( J, LA_LINRX_RCOND_I ) = 0.0
         END IF
      END DO
*
*     Compute the norm of A and the reciprocal of the condition
*     number of A.
*
      NORM = 'I'
      ANORM = CLANHE( NORM, UPLO, N, A, LDA, RWORK )
      CALL CHECON( UPLO, N, AF, LDAF, IPIV, ANORM, RCOND, WORK,
     $     INFO )
*
*     Perform refinement on each right-hand side
*
      IF ( REF_TYPE .NE. 0 ) THEN

         PREC_TYPE = ILAPREC( 'D' )

         CALL CLA_HERFSX_EXTENDED( PREC_TYPE, UPLO,  N,
     $        NRHS, A, LDA, AF, LDAF, IPIV, RCEQU, S, B,
     $        LDB, X, LDX, BERR, N_NORMS, ERR_BNDS_NORM, ERR_BNDS_COMP,
     $        WORK, RWORK, WORK(N+1),
     $        TRANSFER (RWORK(1:2*N), (/ (ZERO, ZERO) /), N), RCOND,
     $        ITHRESH, RTHRESH, UNSTABLE_THRESH, IGNORE_CWISE,
     $        INFO )
      END IF

      ERR_LBND = MAX( 10.0, SQRT( REAL( N ) ) ) * SLAMCH( 'Epsilon' )
      IF ( N_ERR_BNDS .GE. 1 .AND. N_NORMS .GE. 1 ) THEN
*
*     Compute scaled normwise condition number cond(A*C).
*
         IF ( RCEQU ) THEN
            RCOND_TMP = CLA_HERCOND_C( UPLO, N, A, LDA, AF, LDAF, IPIV,
     $           S, .TRUE., INFO, WORK, RWORK )
         ELSE
            RCOND_TMP = CLA_HERCOND_C( UPLO, N, A, LDA, AF, LDAF, IPIV,
     $           S, .FALSE., INFO, WORK, RWORK )
         END IF
         DO J = 1, NRHS
*
*     Cap the error at 1.0.
*
            IF ( N_ERR_BNDS .GE. LA_LINRX_ERR_I
     $           .AND. ERR_BNDS_NORM( J, LA_LINRX_ERR_I ) .GT. 1.0 )
     $           ERR_BNDS_NORM( J, LA_LINRX_ERR_I ) = 1.0
*
*     Threshold the error (see LAWN).
*
            IF (RCOND_TMP .LT. ILLRCOND_THRESH) THEN
               ERR_BNDS_NORM( J, LA_LINRX_ERR_I ) = 1.0
               ERR_BNDS_NORM( J, LA_LINRX_TRUST_I ) = 0.0
               IF ( INFO .LE. N ) INFO = N + J
            ELSE IF ( ERR_BNDS_NORM( J, LA_LINRX_ERR_I ) .LT. ERR_LBND )
     $              THEN
               ERR_BNDS_NORM( J, LA_LINRX_ERR_I ) = ERR_LBND
               ERR_BNDS_NORM( J, LA_LINRX_TRUST_I ) = 1.0
            END IF
*
*     Save the condition number.
*
            IF ( N_ERR_BNDS .GE. LA_LINRX_RCOND_I ) THEN
               ERR_BNDS_NORM( J, LA_LINRX_RCOND_I ) = RCOND_TMP
            END IF
         END DO
      END IF

      IF ( N_ERR_BNDS .GE. 1 .AND. N_NORMS .GE. 2 ) THEN
*
*     Compute componentwise condition number cond(A*diag(Y(:,J))) for
*     each right-hand side using the current solution as an estimate of
*     the true solution.  If the componentwise error estimate is too
*     large, then the solution is a lousy estimate of truth and the
*     estimated RCOND may be too optimistic.  To avoid misleading users,
*     the inverse condition number is set to 0.0 when the estimated
*     cwise error is at least CWISE_WRONG.
*
         CWISE_WRONG = SQRT( SLAMCH( 'Epsilon' ) )
         DO J = 1, NRHS
            IF ( ERR_BNDS_COMP( J, LA_LINRX_ERR_I ) .LT. CWISE_WRONG )
     $     THEN
               RCOND_TMP = CLA_HERCOND_X( UPLO, N, A, LDA, AF, LDAF,
     $         IPIV, X( 1, J ), INFO, WORK, RWORK )
            ELSE
               RCOND_TMP = 0.0
            END IF
*
*     Cap the error at 1.0.
*
            IF ( N_ERR_BNDS .GE. LA_LINRX_ERR_I
     $           .AND. ERR_BNDS_COMP( J, LA_LINRX_ERR_I ) .GT. 1.0 )
     $           ERR_BNDS_COMP( J, LA_LINRX_ERR_I ) = 1.0
*
*     Threshold the error (see LAWN).
*
            IF ( RCOND_TMP .LT. ILLRCOND_THRESH ) THEN
               ERR_BNDS_COMP( J, LA_LINRX_ERR_I ) = 1.0
               ERR_BNDS_COMP( J, LA_LINRX_TRUST_I ) = 0.0
               IF ( .NOT. IGNORE_CWISE
     $              .AND. INFO.LT.N + J ) INFO = N + J
            ELSE IF ( ERR_BNDS_COMP( J, LA_LINRX_ERR_I )
     $              .LT. ERR_LBND ) THEN
               ERR_BNDS_COMP( J, LA_LINRX_ERR_I ) = ERR_LBND
               ERR_BNDS_COMP( J, LA_LINRX_TRUST_I ) = 1.0
            END IF
*
*     Save the condition number.
*
            IF ( N_ERR_BNDS .GE. LA_LINRX_RCOND_I ) THEN
               ERR_BNDS_COMP( J, LA_LINRX_RCOND_I ) = RCOND_TMP
            END IF

         END DO
      END IF
*
      RETURN
*
*     End of CHERFSX
*
      END
