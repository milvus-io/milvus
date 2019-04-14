*> \brief <b> CPOSVX computes the solution to system of linear equations A * X = B for PO matrices</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CPOSVX + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cposvx.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cposvx.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cposvx.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CPOSVX( FACT, UPLO, N, NRHS, A, LDA, AF, LDAF, EQUED,
*                          S, B, LDB, X, LDX, RCOND, FERR, BERR, WORK,
*                          RWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          EQUED, FACT, UPLO
*       INTEGER            INFO, LDA, LDAF, LDB, LDX, N, NRHS
*       REAL               RCOND
*       ..
*       .. Array Arguments ..
*       REAL               BERR( * ), FERR( * ), RWORK( * ), S( * )
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
*> CPOSVX uses the Cholesky factorization A = U**H*U or A = L*L**H to
*> compute the solution to a complex system of linear equations
*>    A * X = B,
*> where A is an N-by-N Hermitian positive definite matrix and X and B
*> are N-by-NRHS matrices.
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
*> 1. If FACT = 'E', real scaling factors are computed to equilibrate
*>    the system:
*>       diag(S) * A * diag(S) * inv(diag(S)) * X = diag(S) * B
*>    Whether or not the system will be equilibrated depends on the
*>    scaling of the matrix A, but if equilibration is used, A is
*>    overwritten by diag(S)*A*diag(S) and B by diag(S)*B.
*>
*> 2. If FACT = 'N' or 'E', the Cholesky decomposition is used to
*>    factor the matrix A (after equilibration if FACT = 'E') as
*>       A = U**H* U,  if UPLO = 'U', or
*>       A = L * L**H,  if UPLO = 'L',
*>    where U is an upper triangular matrix and L is a lower triangular
*>    matrix.
*>
*> 3. If the leading i-by-i principal minor is not positive definite,
*>    then the routine returns with INFO = i. Otherwise, the factored
*>    form of A is used to estimate the condition number of the matrix
*>    A.  If the reciprocal of the condition number is less than machine
*>    precision, INFO = N+1 is returned as a warning, but the routine
*>    still goes on to solve for X and compute error bounds as
*>    described below.
*>
*> 4. The system of equations is solved for X using the factored form
*>    of A.
*>
*> 5. Iterative refinement is applied to improve the computed solution
*>    matrix and calculate error bounds and backward error estimates
*>    for it.
*>
*> 6. If equilibration was used, the matrix X is premultiplied by
*>    diag(S) so that it solves the original system before
*>    equilibration.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] FACT
*> \verbatim
*>          FACT is CHARACTER*1
*>          Specifies whether or not the factored form of the matrix A is
*>          supplied on entry, and if not, whether the matrix A should be
*>          equilibrated before it is factored.
*>          = 'F':  On entry, AF contains the factored form of A.
*>                  If EQUED = 'Y', the matrix A has been equilibrated
*>                  with scaling factors given by S.  A and AF will not
*>                  be modified.
*>          = 'N':  The matrix A will be copied to AF and factored.
*>          = 'E':  The matrix A will be equilibrated if necessary, then
*>                  copied to AF and factored.
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
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N)
*>          On entry, the Hermitian matrix A, except if FACT = 'F' and
*>          EQUED = 'Y', then A must contain the equilibrated matrix
*>          diag(S)*A*diag(S).  If UPLO = 'U', the leading
*>          N-by-N upper triangular part of A contains the upper
*>          triangular part of the matrix A, and the strictly lower
*>          triangular part of A is not referenced.  If UPLO = 'L', the
*>          leading N-by-N lower triangular part of A contains the lower
*>          triangular part of the matrix A, and the strictly upper
*>          triangular part of A is not referenced.  A is not modified if
*>          FACT = 'F' or 'N', or if FACT = 'E' and EQUED = 'N' on exit.
*>
*>          On exit, if FACT = 'E' and EQUED = 'Y', A is overwritten by
*>          diag(S)*A*diag(S).
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
*>          contains the triangular factor U or L from the Cholesky
*>          factorization A = U**H*U or A = L*L**H, in the same storage
*>          format as A.  If EQUED .ne. 'N', then AF is the factored form
*>          of the equilibrated matrix diag(S)*A*diag(S).
*>
*>          If FACT = 'N', then AF is an output argument and on exit
*>          returns the triangular factor U or L from the Cholesky
*>          factorization A = U**H*U or A = L*L**H of the original
*>          matrix A.
*>
*>          If FACT = 'E', then AF is an output argument and on exit
*>          returns the triangular factor U or L from the Cholesky
*>          factorization A = U**H*U or A = L*L**H of the equilibrated
*>          matrix A (see the description of A for the form of the
*>          equilibrated matrix).
*> \endverbatim
*>
*> \param[in] LDAF
*> \verbatim
*>          LDAF is INTEGER
*>          The leading dimension of the array AF.  LDAF >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] EQUED
*> \verbatim
*>          EQUED is CHARACTER*1
*>          Specifies the form of equilibration that was done.
*>          = 'N':  No equilibration (always true if FACT = 'N').
*>          = 'Y':  Equilibration was done, i.e., A has been replaced by
*>                  diag(S) * A * diag(S).
*>          EQUED is an input argument if FACT = 'F'; otherwise, it is an
*>          output argument.
*> \endverbatim
*>
*> \param[in,out] S
*> \verbatim
*>          S is REAL array, dimension (N)
*>          The scale factors for A; not accessed if EQUED = 'N'.  S is
*>          an input argument if FACT = 'F'; otherwise, S is an output
*>          argument.  If FACT = 'F' and EQUED = 'Y', each element of S
*>          must be positive.
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX array, dimension (LDB,NRHS)
*>          On entry, the N-by-NRHS righthand side matrix B.
*>          On exit, if EQUED = 'N', B is not modified; if EQUED = 'Y',
*>          B is overwritten by diag(S) * B.
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
*>          If INFO = 0 or INFO = N+1, the N-by-NRHS solution matrix X to
*>          the original system of equations.  Note that if EQUED = 'Y',
*>          A and B are modified on exit, and the solution to the
*>          equilibrated system is inv(diag(S))*X.
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
*>          A after equilibration (if done).  If RCOND is less than the
*>          machine precision (in particular, if RCOND = 0), the matrix
*>          is singular to working precision.  This condition is
*>          indicated by a return code of INFO > 0.
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
*>          WORK is COMPLEX array, dimension (2*N)
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
*> \date April 2012
*
*> \ingroup complexPOsolve
*
*  =====================================================================
      SUBROUTINE CPOSVX( FACT, UPLO, N, NRHS, A, LDA, AF, LDAF, EQUED,
     $                   S, B, LDB, X, LDX, RCOND, FERR, BERR, WORK,
     $                   RWORK, INFO )
*
*  -- LAPACK driver routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     April 2012
*
*     .. Scalar Arguments ..
      CHARACTER          EQUED, FACT, UPLO
      INTEGER            INFO, LDA, LDAF, LDB, LDX, N, NRHS
      REAL               RCOND
*     ..
*     .. Array Arguments ..
      REAL               BERR( * ), FERR( * ), RWORK( * ), S( * )
      COMPLEX            A( LDA, * ), AF( LDAF, * ), B( LDB, * ),
     $                   WORK( * ), X( LDX, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            EQUIL, NOFACT, RCEQU
      INTEGER            I, INFEQU, J
      REAL               AMAX, ANORM, BIGNUM, SCOND, SMAX, SMIN, SMLNUM
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      REAL               CLANHE, SLAMCH
      EXTERNAL           LSAME, CLANHE, SLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           CLACPY, CLAQHE, CPOCON, CPOEQU, CPORFS, CPOTRF,
     $                   CPOTRS, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN
*     ..
*     .. Executable Statements ..
*
      INFO = 0
      NOFACT = LSAME( FACT, 'N' )
      EQUIL = LSAME( FACT, 'E' )
      IF( NOFACT .OR. EQUIL ) THEN
         EQUED = 'N'
         RCEQU = .FALSE.
      ELSE
         RCEQU = LSAME( EQUED, 'Y' )
         SMLNUM = SLAMCH( 'Safe minimum' )
         BIGNUM = ONE / SMLNUM
      END IF
*
*     Test the input parameters.
*
      IF( .NOT.NOFACT .AND. .NOT.EQUIL .AND. .NOT.LSAME( FACT, 'F' ) )
     $     THEN
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
      ELSE IF( LSAME( FACT, 'F' ) .AND. .NOT.
     $         ( RCEQU .OR. LSAME( EQUED, 'N' ) ) ) THEN
         INFO = -9
      ELSE
         IF( RCEQU ) THEN
            SMIN = BIGNUM
            SMAX = ZERO
            DO 10 J = 1, N
               SMIN = MIN( SMIN, S( J ) )
               SMAX = MAX( SMAX, S( J ) )
   10       CONTINUE
            IF( SMIN.LE.ZERO ) THEN
               INFO = -10
            ELSE IF( N.GT.0 ) THEN
               SCOND = MAX( SMIN, SMLNUM ) / MIN( SMAX, BIGNUM )
            ELSE
               SCOND = ONE
            END IF
         END IF
         IF( INFO.EQ.0 ) THEN
            IF( LDB.LT.MAX( 1, N ) ) THEN
               INFO = -12
            ELSE IF( LDX.LT.MAX( 1, N ) ) THEN
               INFO = -14
            END IF
         END IF
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CPOSVX', -INFO )
         RETURN
      END IF
*
      IF( EQUIL ) THEN
*
*        Compute row and column scalings to equilibrate the matrix A.
*
         CALL CPOEQU( N, A, LDA, S, SCOND, AMAX, INFEQU )
         IF( INFEQU.EQ.0 ) THEN
*
*           Equilibrate the matrix.
*
            CALL CLAQHE( UPLO, N, A, LDA, S, SCOND, AMAX, EQUED )
            RCEQU = LSAME( EQUED, 'Y' )
         END IF
      END IF
*
*     Scale the right hand side.
*
      IF( RCEQU ) THEN
         DO 30 J = 1, NRHS
            DO 20 I = 1, N
               B( I, J ) = S( I )*B( I, J )
   20       CONTINUE
   30    CONTINUE
      END IF
*
      IF( NOFACT .OR. EQUIL ) THEN
*
*        Compute the Cholesky factorization A = U**H *U or A = L*L**H.
*
         CALL CLACPY( UPLO, N, N, A, LDA, AF, LDAF )
         CALL CPOTRF( UPLO, N, AF, LDAF, INFO )
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
      ANORM = CLANHE( '1', UPLO, N, A, LDA, RWORK )
*
*     Compute the reciprocal of the condition number of A.
*
      CALL CPOCON( UPLO, N, AF, LDAF, ANORM, RCOND, WORK, RWORK, INFO )
*
*     Compute the solution matrix X.
*
      CALL CLACPY( 'Full', N, NRHS, B, LDB, X, LDX )
      CALL CPOTRS( UPLO, N, NRHS, AF, LDAF, X, LDX, INFO )
*
*     Use iterative refinement to improve the computed solution and
*     compute error bounds and backward error estimates for it.
*
      CALL CPORFS( UPLO, N, NRHS, A, LDA, AF, LDAF, B, LDB, X, LDX,
     $             FERR, BERR, WORK, RWORK, INFO )
*
*     Transform the solution matrix X to a solution of the original
*     system.
*
      IF( RCEQU ) THEN
         DO 50 J = 1, NRHS
            DO 40 I = 1, N
               X( I, J ) = S( I )*X( I, J )
   40       CONTINUE
   50    CONTINUE
         DO 60 J = 1, NRHS
            FERR( J ) = FERR( J ) / SCOND
   60    CONTINUE
      END IF
*
*     Set INFO = N+1 if the matrix is singular to working precision.
*
      IF( RCOND.LT.SLAMCH( 'Epsilon' ) )
     $   INFO = N + 1
*
      RETURN
*
*     End of CPOSVX
*
      END
