*> \brief <b> DSGESV computes the solution to system of linear equations A * X = B for GE matrices</b> (mixed precision with iterative refinement)
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DSGESV + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dsgesv.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dsgesv.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dsgesv.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DSGESV( N, NRHS, A, LDA, IPIV, B, LDB, X, LDX, WORK,
*                          SWORK, ITER, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, ITER, LDA, LDB, LDX, N, NRHS
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * )
*       REAL               SWORK( * )
*       DOUBLE PRECISION   A( LDA, * ), B( LDB, * ), WORK( N, * ),
*      $                   X( LDX, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DSGESV computes the solution to a real system of linear equations
*>    A * X = B,
*> where A is an N-by-N matrix and X and B are N-by-NRHS matrices.
*>
*> DSGESV first attempts to factorize the matrix in SINGLE PRECISION
*> and use this factorization within an iterative refinement procedure
*> to produce a solution with DOUBLE PRECISION normwise backward error
*> quality (see below). If the approach fails the method switches to a
*> DOUBLE PRECISION factorization and solve.
*>
*> The iterative refinement is not going to be a winning strategy if
*> the ratio SINGLE PRECISION performance over DOUBLE PRECISION
*> performance is too small. A reasonable strategy should take the
*> number of right-hand sides and the size of the matrix into account.
*> This might be done with a call to ILAENV in the future. Up to now, we
*> always try iterative refinement.
*>
*> The iterative refinement process is stopped if
*>     ITER > ITERMAX
*> or for all the RHS we have:
*>     RNRM < SQRT(N)*XNRM*ANRM*EPS*BWDMAX
*> where
*>     o ITER is the number of the current iteration in the iterative
*>       refinement process
*>     o RNRM is the infinity-norm of the residual
*>     o XNRM is the infinity-norm of the solution
*>     o ANRM is the infinity-operator-norm of the matrix A
*>     o EPS is the machine epsilon returned by DLAMCH('Epsilon')
*> The value ITERMAX and BWDMAX are fixed to 30 and 1.0D+00
*> respectively.
*> \endverbatim
*
*  Arguments:
*  ==========
*
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
*>          of the matrix B.  NRHS >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is DOUBLE PRECISION array,
*>          dimension (LDA,N)
*>          On entry, the N-by-N coefficient matrix A.
*>          On exit, if iterative refinement has been successfully used
*>          (INFO.EQ.0 and ITER.GE.0, see description below), then A is
*>          unchanged, if double precision factorization has been used
*>          (INFO.EQ.0 and ITER.LT.0, see description below), then the
*>          array A contains the factors L and U from the factorization
*>          A = P*L*U; the unit diagonal elements of L are not stored.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[out] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (N)
*>          The pivot indices that define the permutation matrix P;
*>          row i of the matrix was interchanged with row IPIV(i).
*>          Corresponds either to the single precision factorization
*>          (if INFO.EQ.0 and ITER.GE.0) or the double precision
*>          factorization (if INFO.EQ.0 and ITER.LT.0).
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
*>          If INFO = 0, the N-by-NRHS solution matrix X.
*> \endverbatim
*>
*> \param[in] LDX
*> \verbatim
*>          LDX is INTEGER
*>          The leading dimension of the array X.  LDX >= max(1,N).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (N,NRHS)
*>          This array is used to hold the residual vectors.
*> \endverbatim
*>
*> \param[out] SWORK
*> \verbatim
*>          SWORK is REAL array, dimension (N*(N+NRHS))
*>          This array is used to use the single precision matrix and the
*>          right-hand sides or solutions in single precision.
*> \endverbatim
*>
*> \param[out] ITER
*> \verbatim
*>          ITER is INTEGER
*>          < 0: iterative refinement has failed, double precision
*>               factorization has been performed
*>               -1 : the routine fell back to full precision for
*>                    implementation- or machine-specific reasons
*>               -2 : narrowing the precision induced an overflow,
*>                    the routine fell back to full precision
*>               -3 : failure of SGETRF
*>               -31: stop the iterative refinement after the 30th
*>                    iterations
*>          > 0: iterative refinement has been successfully used.
*>               Returns the number of iterations
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
*>          > 0:  if INFO = i, U(i,i) computed in DOUBLE PRECISION is
*>                exactly zero.  The factorization has been completed,
*>                but the factor U is exactly singular, so the solution
*>                could not be computed.
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
*> \date June 2016
*
*> \ingroup doubleGEsolve
*
*  =====================================================================
      SUBROUTINE DSGESV( N, NRHS, A, LDA, IPIV, B, LDB, X, LDX, WORK,
     $                   SWORK, ITER, INFO )
*
*  -- LAPACK driver routine (version 3.8.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, ITER, LDA, LDB, LDX, N, NRHS
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * )
      REAL               SWORK( * )
      DOUBLE PRECISION   A( LDA, * ), B( LDB, * ), WORK( N, * ),
     $                   X( LDX, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      LOGICAL            DOITREF
      PARAMETER          ( DOITREF = .TRUE. )
*
      INTEGER            ITERMAX
      PARAMETER          ( ITERMAX = 30 )
*
      DOUBLE PRECISION   BWDMAX
      PARAMETER          ( BWDMAX = 1.0E+00 )
*
      DOUBLE PRECISION   NEGONE, ONE
      PARAMETER          ( NEGONE = -1.0D+0, ONE = 1.0D+0 )
*
*     .. Local Scalars ..
      INTEGER            I, IITER, PTSA, PTSX
      DOUBLE PRECISION   ANRM, CTE, EPS, RNRM, XNRM
*
*     .. External Subroutines ..
      EXTERNAL           DAXPY, DGEMM, DLACPY, DLAG2S, DGETRF, DGETRS,
     $                   SGETRF, SGETRS, SLAG2D, XERBLA
*     ..
*     .. External Functions ..
      INTEGER            IDAMAX
      DOUBLE PRECISION   DLAMCH, DLANGE
      EXTERNAL           IDAMAX, DLAMCH, DLANGE
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, MAX, SQRT
*     ..
*     .. Executable Statements ..
*
      INFO = 0
      ITER = 0
*
*     Test the input parameters.
*
      IF( N.LT.0 ) THEN
         INFO = -1
      ELSE IF( NRHS.LT.0 ) THEN
         INFO = -2
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -4
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -7
      ELSE IF( LDX.LT.MAX( 1, N ) ) THEN
         INFO = -9
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DSGESV', -INFO )
         RETURN
      END IF
*
*     Quick return if (N.EQ.0).
*
      IF( N.EQ.0 )
     $   RETURN
*
*     Skip single precision iterative refinement if a priori slower
*     than double precision factorization.
*
      IF( .NOT.DOITREF ) THEN
         ITER = -1
         GO TO 40
      END IF
*
*     Compute some constants.
*
      ANRM = DLANGE( 'I', N, N, A, LDA, WORK )
      EPS = DLAMCH( 'Epsilon' )
      CTE = ANRM*EPS*SQRT( DBLE( N ) )*BWDMAX
*
*     Set the indices PTSA, PTSX for referencing SA and SX in SWORK.
*
      PTSA = 1
      PTSX = PTSA + N*N
*
*     Convert B from double precision to single precision and store the
*     result in SX.
*
      CALL DLAG2S( N, NRHS, B, LDB, SWORK( PTSX ), N, INFO )
*
      IF( INFO.NE.0 ) THEN
         ITER = -2
         GO TO 40
      END IF
*
*     Convert A from double precision to single precision and store the
*     result in SA.
*
      CALL DLAG2S( N, N, A, LDA, SWORK( PTSA ), N, INFO )
*
      IF( INFO.NE.0 ) THEN
         ITER = -2
         GO TO 40
      END IF
*
*     Compute the LU factorization of SA.
*
      CALL SGETRF( N, N, SWORK( PTSA ), N, IPIV, INFO )
*
      IF( INFO.NE.0 ) THEN
         ITER = -3
         GO TO 40
      END IF
*
*     Solve the system SA*SX = SB.
*
      CALL SGETRS( 'No transpose', N, NRHS, SWORK( PTSA ), N, IPIV,
     $             SWORK( PTSX ), N, INFO )
*
*     Convert SX back to double precision
*
      CALL SLAG2D( N, NRHS, SWORK( PTSX ), N, X, LDX, INFO )
*
*     Compute R = B - AX (R is WORK).
*
      CALL DLACPY( 'All', N, NRHS, B, LDB, WORK, N )
*
      CALL DGEMM( 'No Transpose', 'No Transpose', N, NRHS, N, NEGONE, A,
     $            LDA, X, LDX, ONE, WORK, N )
*
*     Check whether the NRHS normwise backward errors satisfy the
*     stopping criterion. If yes, set ITER=0 and return.
*
      DO I = 1, NRHS
         XNRM = ABS( X( IDAMAX( N, X( 1, I ), 1 ), I ) )
         RNRM = ABS( WORK( IDAMAX( N, WORK( 1, I ), 1 ), I ) )
         IF( RNRM.GT.XNRM*CTE )
     $      GO TO 10
      END DO
*
*     If we are here, the NRHS normwise backward errors satisfy the
*     stopping criterion. We are good to exit.
*
      ITER = 0
      RETURN
*
   10 CONTINUE
*
      DO 30 IITER = 1, ITERMAX
*
*        Convert R (in WORK) from double precision to single precision
*        and store the result in SX.
*
         CALL DLAG2S( N, NRHS, WORK, N, SWORK( PTSX ), N, INFO )
*
         IF( INFO.NE.0 ) THEN
            ITER = -2
            GO TO 40
         END IF
*
*        Solve the system SA*SX = SR.
*
         CALL SGETRS( 'No transpose', N, NRHS, SWORK( PTSA ), N, IPIV,
     $                SWORK( PTSX ), N, INFO )
*
*        Convert SX back to double precision and update the current
*        iterate.
*
         CALL SLAG2D( N, NRHS, SWORK( PTSX ), N, WORK, N, INFO )
*
         DO I = 1, NRHS
            CALL DAXPY( N, ONE, WORK( 1, I ), 1, X( 1, I ), 1 )
         END DO
*
*        Compute R = B - AX (R is WORK).
*
         CALL DLACPY( 'All', N, NRHS, B, LDB, WORK, N )
*
         CALL DGEMM( 'No Transpose', 'No Transpose', N, NRHS, N, NEGONE,
     $               A, LDA, X, LDX, ONE, WORK, N )
*
*        Check whether the NRHS normwise backward errors satisfy the
*        stopping criterion. If yes, set ITER=IITER>0 and return.
*
         DO I = 1, NRHS
            XNRM = ABS( X( IDAMAX( N, X( 1, I ), 1 ), I ) )
            RNRM = ABS( WORK( IDAMAX( N, WORK( 1, I ), 1 ), I ) )
            IF( RNRM.GT.XNRM*CTE )
     $         GO TO 20
         END DO
*
*        If we are here, the NRHS normwise backward errors satisfy the
*        stopping criterion, we are good to exit.
*
         ITER = IITER
*
         RETURN
*
   20    CONTINUE
*
   30 CONTINUE
*
*     If we are at this place of the code, this is because we have
*     performed ITER=ITERMAX iterations and never satisified the
*     stopping criterion, set up the ITER flag accordingly and follow up
*     on double precision routine.
*
      ITER = -ITERMAX - 1
*
   40 CONTINUE
*
*     Single-precision iterative refinement failed to converge to a
*     satisfactory solution, so we resort to double precision.
*
      CALL DGETRF( N, N, A, LDA, IPIV, INFO )
*
      IF( INFO.NE.0 )
     $   RETURN
*
      CALL DLACPY( 'All', N, NRHS, B, LDB, X, LDX )
      CALL DGETRS( 'No transpose', N, NRHS, A, LDA, IPIV, X, LDX,
     $             INFO )
*
      RETURN
*
*     End of DSGESV.
*
      END
