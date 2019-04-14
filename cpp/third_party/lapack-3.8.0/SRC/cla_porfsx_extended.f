*> \brief \b CLA_PORFSX_EXTENDED improves the computed solution to a system of linear equations for symmetric or Hermitian positive-definite matrices by performing extra-precise iterative refinement and provides error bounds and backward error estimates for the solution.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CLA_PORFSX_EXTENDED + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cla_porfsx_extended.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cla_porfsx_extended.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cla_porfsx_extended.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CLA_PORFSX_EXTENDED( PREC_TYPE, UPLO, N, NRHS, A, LDA,
*                                       AF, LDAF, COLEQU, C, B, LDB, Y,
*                                       LDY, BERR_OUT, N_NORMS,
*                                       ERR_BNDS_NORM, ERR_BNDS_COMP, RES,
*                                       AYB, DY, Y_TAIL, RCOND, ITHRESH,
*                                       RTHRESH, DZ_UB, IGNORE_CWISE,
*                                       INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, LDAF, LDB, LDY, N, NRHS, PREC_TYPE,
*      $                   N_NORMS, ITHRESH
*       CHARACTER          UPLO
*       LOGICAL            COLEQU, IGNORE_CWISE
*       REAL               RTHRESH, DZ_UB
*       ..
*       .. Array Arguments ..
*       COMPLEX            A( LDA, * ), AF( LDAF, * ), B( LDB, * ),
*      $                   Y( LDY, * ), RES( * ), DY( * ), Y_TAIL( * )
*       REAL               C( * ), AYB( * ), RCOND, BERR_OUT( * ),
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
*> CLA_PORFSX_EXTENDED improves the computed solution to a system of
*> linear equations by performing extra-precise iterative refinement
*> and provides error bounds and backward error estimates for the solution.
*> This subroutine is called by CPORFSX to perform iterative refinement.
*> In addition to normwise error bound, the code provides maximum
*> componentwise error bound if possible. See comments for ERR_BNDS_NORM
*> and ERR_BNDS_COMP for details of the error bounds. Note that this
*> subroutine is only resonsible for setting the second fields of
*> ERR_BNDS_NORM and ERR_BNDS_COMP.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] PREC_TYPE
*> \verbatim
*>          PREC_TYPE is INTEGER
*>     Specifies the intermediate precision to be used in refinement.
*>     The value is defined by ILAPREC(P) where P is a CHARACTER and
*>     P    = 'S':  Single
*>          = 'D':  Double
*>          = 'I':  Indigenous
*>          = 'X', 'E':  Extra
*> \endverbatim
*>
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>       = 'U':  Upper triangle of A is stored;
*>       = 'L':  Lower triangle of A is stored.
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
*>     The number of right-hand-sides, i.e., the number of columns of the
*>     matrix B.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N)
*>     On entry, the N-by-N matrix A.
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
*>     The triangular factor U or L from the Cholesky factorization
*>     A = U**T*U or A = L*L**T, as computed by CPOTRF.
*> \endverbatim
*>
*> \param[in] LDAF
*> \verbatim
*>          LDAF is INTEGER
*>     The leading dimension of the array AF.  LDAF >= max(1,N).
*> \endverbatim
*>
*> \param[in] COLEQU
*> \verbatim
*>          COLEQU is LOGICAL
*>     If .TRUE. then column equilibration was done to A before calling
*>     this routine. This is needed to compute the solution and error
*>     bounds correctly.
*> \endverbatim
*>
*> \param[in] C
*> \verbatim
*>          C is REAL array, dimension (N)
*>     The column scale factors for A. If COLEQU = .FALSE., C
*>     is not accessed. If C is input, each element of C should be a power
*>     of the radix to ensure a reliable solution and error estimates.
*>     Scaling by powers of the radix does not cause rounding errors unless
*>     the result underflows or overflows. Rounding errors during scaling
*>     lead to refining with a matrix that is not equivalent to the
*>     input matrix, producing error estimates that may not be
*>     reliable.
*> \endverbatim
*>
*> \param[in] B
*> \verbatim
*>          B is COMPLEX array, dimension (LDB,NRHS)
*>     The right-hand-side matrix B.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>     The leading dimension of the array B.  LDB >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] Y
*> \verbatim
*>          Y is COMPLEX array, dimension (LDY,NRHS)
*>     On entry, the solution matrix X, as computed by CPOTRS.
*>     On exit, the improved solution matrix Y.
*> \endverbatim
*>
*> \param[in] LDY
*> \verbatim
*>          LDY is INTEGER
*>     The leading dimension of the array Y.  LDY >= max(1,N).
*> \endverbatim
*>
*> \param[out] BERR_OUT
*> \verbatim
*>          BERR_OUT is REAL array, dimension (NRHS)
*>     On exit, BERR_OUT(j) contains the componentwise relative backward
*>     error for right-hand-side j from the formula
*>         max(i) ( abs(RES(i)) / ( abs(op(A_s))*abs(Y) + abs(B_s) )(i) )
*>     where abs(Z) is the componentwise absolute value of the matrix
*>     or vector Z. This is computed by CLA_LIN_BERR.
*> \endverbatim
*>
*> \param[in] N_NORMS
*> \verbatim
*>          N_NORMS is INTEGER
*>     Determines which error bounds to return (see ERR_BNDS_NORM
*>     and ERR_BNDS_COMP).
*>     If N_NORMS >= 1 return normwise error bounds.
*>     If N_NORMS >= 2 return componentwise error bounds.
*> \endverbatim
*>
*> \param[in,out] ERR_BNDS_NORM
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
*>     This subroutine is only responsible for setting the second field
*>     above.
*>     See Lapack Working Note 165 for further details and extra
*>     cautions.
*> \endverbatim
*>
*> \param[in,out] ERR_BNDS_COMP
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
*>     This subroutine is only responsible for setting the second field
*>     above.
*>     See Lapack Working Note 165 for further details and extra
*>     cautions.
*> \endverbatim
*>
*> \param[in] RES
*> \verbatim
*>          RES is COMPLEX array, dimension (N)
*>     Workspace to hold the intermediate residual.
*> \endverbatim
*>
*> \param[in] AYB
*> \verbatim
*>          AYB is REAL array, dimension (N)
*>     Workspace.
*> \endverbatim
*>
*> \param[in] DY
*> \verbatim
*>          DY is COMPLEX array, dimension (N)
*>     Workspace to hold the intermediate solution.
*> \endverbatim
*>
*> \param[in] Y_TAIL
*> \verbatim
*>          Y_TAIL is COMPLEX array, dimension (N)
*>     Workspace to hold the trailing bits of the intermediate solution.
*> \endverbatim
*>
*> \param[in] RCOND
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
*> \param[in] ITHRESH
*> \verbatim
*>          ITHRESH is INTEGER
*>     The maximum number of residual computations allowed for
*>     refinement. The default is 10. For 'aggressive' set to 100 to
*>     permit convergence using approximate factorizations or
*>     factorizations other than LU. If the factorization uses a
*>     technique other than Gaussian elimination, the guarantees in
*>     ERR_BNDS_NORM and ERR_BNDS_COMP may no longer be trustworthy.
*> \endverbatim
*>
*> \param[in] RTHRESH
*> \verbatim
*>          RTHRESH is REAL
*>     Determines when to stop refinement if the error estimate stops
*>     decreasing. Refinement will stop when the next solution no longer
*>     satisfies norm(dx_{i+1}) < RTHRESH * norm(dx_i) where norm(Z) is
*>     the infinity norm of Z. RTHRESH satisfies 0 < RTHRESH <= 1. The
*>     default value is 0.5. For 'aggressive' set to 0.9 to permit
*>     convergence on extremely ill-conditioned matrices. See LAWN 165
*>     for more details.
*> \endverbatim
*>
*> \param[in] DZ_UB
*> \verbatim
*>          DZ_UB is REAL
*>     Determines when to start considering componentwise convergence.
*>     Componentwise convergence is only considered after each component
*>     of the solution Y is stable, which we definte as the relative
*>     change in each component being less than DZ_UB. The default value
*>     is 0.25, requiring the first bit to be stable. See LAWN 165 for
*>     more details.
*> \endverbatim
*>
*> \param[in] IGNORE_CWISE
*> \verbatim
*>          IGNORE_CWISE is LOGICAL
*>     If .TRUE. then ignore componentwise convergence. Default value
*>     is .FALSE..
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>       = 0:  Successful exit.
*>       < 0:  if INFO = -i, the ith argument to CPOTRS had an illegal
*>             value
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
*> \date June 2017
*
*> \ingroup complexPOcomputational
*
*  =====================================================================
      SUBROUTINE CLA_PORFSX_EXTENDED( PREC_TYPE, UPLO, N, NRHS, A, LDA,
     $                                AF, LDAF, COLEQU, C, B, LDB, Y,
     $                                LDY, BERR_OUT, N_NORMS,
     $                                ERR_BNDS_NORM, ERR_BNDS_COMP, RES,
     $                                AYB, DY, Y_TAIL, RCOND, ITHRESH,
     $                                RTHRESH, DZ_UB, IGNORE_CWISE,
     $                                INFO )
*
*  -- LAPACK computational routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, LDAF, LDB, LDY, N, NRHS, PREC_TYPE,
     $                   N_NORMS, ITHRESH
      CHARACTER          UPLO
      LOGICAL            COLEQU, IGNORE_CWISE
      REAL               RTHRESH, DZ_UB
*     ..
*     .. Array Arguments ..
      COMPLEX            A( LDA, * ), AF( LDAF, * ), B( LDB, * ),
     $                   Y( LDY, * ), RES( * ), DY( * ), Y_TAIL( * )
      REAL               C( * ), AYB( * ), RCOND, BERR_OUT( * ),
     $                   ERR_BNDS_NORM( NRHS, * ),
     $                   ERR_BNDS_COMP( NRHS, * )
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      INTEGER            UPLO2, CNT, I, J, X_STATE, Z_STATE,
     $                   Y_PREC_STATE
      REAL               YK, DYK, YMIN, NORMY, NORMX, NORMDX, DXRAT,
     $                   DZRAT, PREVNORMDX, PREV_DZ_Z, DXRATMAX,
     $                   DZRATMAX, DX_X, DZ_Z, FINAL_DX_X, FINAL_DZ_Z,
     $                   EPS, HUGEVAL, INCR_THRESH
      LOGICAL            INCR_PREC
      COMPLEX            ZDUM
*     ..
*     .. Parameters ..
      INTEGER            UNSTABLE_STATE, WORKING_STATE, CONV_STATE,
     $                   NOPROG_STATE, BASE_RESIDUAL, EXTRA_RESIDUAL,
     $                   EXTRA_Y
      PARAMETER          ( UNSTABLE_STATE = 0, WORKING_STATE = 1,
     $                   CONV_STATE = 2, NOPROG_STATE = 3 )
      PARAMETER          ( BASE_RESIDUAL = 0, EXTRA_RESIDUAL = 1,
     $                   EXTRA_Y = 2 )
      INTEGER            FINAL_NRM_ERR_I, FINAL_CMP_ERR_I, BERR_I
      INTEGER            RCOND_I, NRM_RCOND_I, NRM_ERR_I, CMP_RCOND_I
      INTEGER            CMP_ERR_I, PIV_GROWTH_I
      PARAMETER          ( FINAL_NRM_ERR_I = 1, FINAL_CMP_ERR_I = 2,
     $                   BERR_I = 3 )
      PARAMETER          ( RCOND_I = 4, NRM_RCOND_I = 5, NRM_ERR_I = 6 )
      PARAMETER          ( CMP_RCOND_I = 7, CMP_ERR_I = 8,
     $                   PIV_GROWTH_I = 9 )
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
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           ILAUPLO
      INTEGER            ILAUPLO
*     ..
*     .. External Subroutines ..
      EXTERNAL           CAXPY, CCOPY, CPOTRS, CHEMV, BLAS_CHEMV_X,
     $                   BLAS_CHEMV2_X, CLA_HEAMV, CLA_WWADDW,
     $                   CLA_LIN_BERR, SLAMCH
      REAL               SLAMCH
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, REAL, AIMAG, MAX, MIN
*     ..
*     .. Statement Functions ..
      REAL               CABS1
*     ..
*     .. Statement Function Definitions ..
      CABS1( ZDUM ) = ABS( REAL( ZDUM ) ) + ABS( AIMAG( ZDUM ) )
*     ..
*     .. Executable Statements ..
*
      IF (INFO.NE.0) RETURN
      EPS = SLAMCH( 'Epsilon' )
      HUGEVAL = SLAMCH( 'Overflow' )
*     Force HUGEVAL to Inf
      HUGEVAL = HUGEVAL * HUGEVAL
*     Using HUGEVAL may lead to spurious underflows.
      INCR_THRESH = REAL(N) * EPS

      IF (LSAME (UPLO, 'L')) THEN
         UPLO2 = ILAUPLO( 'L' )
      ELSE
         UPLO2 = ILAUPLO( 'U' )
      ENDIF

      DO J = 1, NRHS
         Y_PREC_STATE = EXTRA_RESIDUAL
         IF (Y_PREC_STATE .EQ. EXTRA_Y) THEN
            DO I = 1, N
               Y_TAIL( I ) = 0.0
            END DO
         END IF

         DXRAT = 0.0
         DXRATMAX = 0.0
         DZRAT = 0.0
         DZRATMAX = 0.0
         FINAL_DX_X = HUGEVAL
         FINAL_DZ_Z = HUGEVAL
         PREVNORMDX = HUGEVAL
         PREV_DZ_Z = HUGEVAL
         DZ_Z = HUGEVAL
         DX_X = HUGEVAL

         X_STATE = WORKING_STATE
         Z_STATE = UNSTABLE_STATE
         INCR_PREC = .FALSE.

         DO CNT = 1, ITHRESH
*
*         Compute residual RES = B_s - op(A_s) * Y,
*             op(A) = A, A**T, or A**H depending on TRANS (and type).
*
            CALL CCOPY( N, B( 1, J ), 1, RES, 1 )
            IF (Y_PREC_STATE .EQ. BASE_RESIDUAL) THEN
               CALL CHEMV(UPLO, N, CMPLX(-1.0), A, LDA, Y(1,J), 1,
     $              CMPLX(1.0), RES, 1)
            ELSE IF (Y_PREC_STATE .EQ. EXTRA_RESIDUAL) THEN
               CALL BLAS_CHEMV_X(UPLO2, N, CMPLX(-1.0), A, LDA,
     $              Y( 1, J ), 1, CMPLX(1.0), RES, 1, PREC_TYPE)
            ELSE
               CALL BLAS_CHEMV2_X(UPLO2, N, CMPLX(-1.0), A, LDA,
     $              Y(1, J), Y_TAIL, 1, CMPLX(1.0), RES, 1, PREC_TYPE)
            END IF

!         XXX: RES is no longer needed.
            CALL CCOPY( N, RES, 1, DY, 1 )
            CALL CPOTRS( UPLO, N, 1, AF, LDAF, DY, N, INFO)
*
*         Calculate relative changes DX_X, DZ_Z and ratios DXRAT, DZRAT.
*
            NORMX = 0.0
            NORMY = 0.0
            NORMDX = 0.0
            DZ_Z = 0.0
            YMIN = HUGEVAL

            DO I = 1, N
               YK = CABS1(Y(I, J))
               DYK = CABS1(DY(I))

               IF (YK .NE. 0.0) THEN
                  DZ_Z = MAX( DZ_Z, DYK / YK )
               ELSE IF (DYK .NE. 0.0) THEN
                  DZ_Z = HUGEVAL
               END IF

               YMIN = MIN( YMIN, YK )

               NORMY = MAX( NORMY, YK )

               IF ( COLEQU ) THEN
                  NORMX = MAX(NORMX, YK * C(I))
                  NORMDX = MAX(NORMDX, DYK * C(I))
               ELSE
                  NORMX = NORMY
                  NORMDX = MAX(NORMDX, DYK)
               END IF
            END DO

            IF (NORMX .NE. 0.0) THEN
               DX_X = NORMDX / NORMX
            ELSE IF (NORMDX .EQ. 0.0) THEN
               DX_X = 0.0
            ELSE
               DX_X = HUGEVAL
            END IF

            DXRAT = NORMDX / PREVNORMDX
            DZRAT = DZ_Z / PREV_DZ_Z
*
*         Check termination criteria.
*
            IF (YMIN*RCOND .LT. INCR_THRESH*NORMY
     $           .AND. Y_PREC_STATE .LT. EXTRA_Y)
     $           INCR_PREC = .TRUE.

            IF (X_STATE .EQ. NOPROG_STATE .AND. DXRAT .LE. RTHRESH)
     $           X_STATE = WORKING_STATE
            IF (X_STATE .EQ. WORKING_STATE) THEN
               IF (DX_X .LE. EPS) THEN
                  X_STATE = CONV_STATE
               ELSE IF (DXRAT .GT. RTHRESH) THEN
                  IF (Y_PREC_STATE .NE. EXTRA_Y) THEN
                     INCR_PREC = .TRUE.
                  ELSE
                     X_STATE = NOPROG_STATE
                  END IF
               ELSE
                  IF (DXRAT .GT. DXRATMAX) DXRATMAX = DXRAT
               END IF
               IF (X_STATE .GT. WORKING_STATE) FINAL_DX_X = DX_X
            END IF

            IF (Z_STATE .EQ. UNSTABLE_STATE .AND. DZ_Z .LE. DZ_UB)
     $           Z_STATE = WORKING_STATE
            IF (Z_STATE .EQ. NOPROG_STATE .AND. DZRAT .LE. RTHRESH)
     $           Z_STATE = WORKING_STATE
            IF (Z_STATE .EQ. WORKING_STATE) THEN
               IF (DZ_Z .LE. EPS) THEN
                  Z_STATE = CONV_STATE
               ELSE IF (DZ_Z .GT. DZ_UB) THEN
                  Z_STATE = UNSTABLE_STATE
                  DZRATMAX = 0.0
                  FINAL_DZ_Z = HUGEVAL
               ELSE IF (DZRAT .GT. RTHRESH) THEN
                  IF (Y_PREC_STATE .NE. EXTRA_Y) THEN
                     INCR_PREC = .TRUE.
                  ELSE
                     Z_STATE = NOPROG_STATE
                  END IF
               ELSE
                  IF (DZRAT .GT. DZRATMAX) DZRATMAX = DZRAT
               END IF
               IF (Z_STATE .GT. WORKING_STATE) FINAL_DZ_Z = DZ_Z
            END IF

            IF ( X_STATE.NE.WORKING_STATE.AND.
     $           (IGNORE_CWISE.OR.Z_STATE.NE.WORKING_STATE) )
     $           GOTO 666

            IF (INCR_PREC) THEN
               INCR_PREC = .FALSE.
               Y_PREC_STATE = Y_PREC_STATE + 1
               DO I = 1, N
                  Y_TAIL( I ) = 0.0
               END DO
            END IF

            PREVNORMDX = NORMDX
            PREV_DZ_Z = DZ_Z
*
*           Update soluton.
*
            IF (Y_PREC_STATE .LT. EXTRA_Y) THEN
               CALL CAXPY( N, CMPLX(1.0), DY, 1, Y(1,J), 1 )
            ELSE
               CALL CLA_WWADDW(N, Y(1,J), Y_TAIL, DY)
            END IF

         END DO
*        Target of "IF (Z_STOP .AND. X_STOP)".  Sun's f77 won't EXIT.
 666     CONTINUE
*
*     Set final_* when cnt hits ithresh.
*
         IF (X_STATE .EQ. WORKING_STATE) FINAL_DX_X = DX_X
         IF (Z_STATE .EQ. WORKING_STATE) FINAL_DZ_Z = DZ_Z
*
*     Compute error bounds.
*
         IF (N_NORMS .GE. 1) THEN
            ERR_BNDS_NORM( J, LA_LINRX_ERR_I ) =
     $           FINAL_DX_X / (1 - DXRATMAX)
         END IF
         IF (N_NORMS .GE. 2) THEN
            ERR_BNDS_COMP( J, LA_LINRX_ERR_I ) =
     $           FINAL_DZ_Z / (1 - DZRATMAX)
         END IF
*
*     Compute componentwise relative backward error from formula
*         max(i) ( abs(R(i)) / ( abs(op(A_s))*abs(Y) + abs(B_s) )(i) )
*     where abs(Z) is the componentwise absolute value of the matrix
*     or vector Z.
*
*        Compute residual RES = B_s - op(A_s) * Y,
*            op(A) = A, A**T, or A**H depending on TRANS (and type).
*
         CALL CCOPY( N, B( 1, J ), 1, RES, 1 )
         CALL CHEMV(UPLO, N, CMPLX(-1.0), A, LDA, Y(1,J), 1, CMPLX(1.0),
     $        RES, 1)

         DO I = 1, N
            AYB( I ) = CABS1( B( I, J ) )
         END DO
*
*     Compute abs(op(A_s))*abs(Y) + abs(B_s).
*
         CALL CLA_HEAMV (UPLO2, N, 1.0,
     $        A, LDA, Y(1, J), 1, 1.0, AYB, 1)

         CALL CLA_LIN_BERR (N, N, 1, RES, AYB, BERR_OUT(J))
*
*     End of loop for each RHS.
*
      END DO
*
      RETURN
      END
