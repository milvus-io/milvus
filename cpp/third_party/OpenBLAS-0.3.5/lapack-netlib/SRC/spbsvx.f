*> \brief <b> SPBSVX computes the solution to system of linear equations A * X = B for OTHER matrices</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SPBSVX + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/spbsvx.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/spbsvx.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/spbsvx.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SPBSVX( FACT, UPLO, N, KD, NRHS, AB, LDAB, AFB, LDAFB,
*                          EQUED, S, B, LDB, X, LDX, RCOND, FERR, BERR,
*                          WORK, IWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          EQUED, FACT, UPLO
*       INTEGER            INFO, KD, LDAB, LDAFB, LDB, LDX, N, NRHS
*       REAL               RCOND
*       ..
*       .. Array Arguments ..
*       INTEGER            IWORK( * )
*       REAL               AB( LDAB, * ), AFB( LDAFB, * ), B( LDB, * ),
*      $                   BERR( * ), FERR( * ), S( * ), WORK( * ),
*      $                   X( LDX, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SPBSVX uses the Cholesky factorization A = U**T*U or A = L*L**T to
*> compute the solution to a real system of linear equations
*>    A * X = B,
*> where A is an N-by-N symmetric positive definite band matrix and X
*> and B are N-by-NRHS matrices.
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
*>       A = U**T * U,  if UPLO = 'U', or
*>       A = L * L**T,  if UPLO = 'L',
*>    where U is an upper triangular band matrix, and L is a lower
*>    triangular band matrix.
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
*>          = 'F':  On entry, AFB contains the factored form of A.
*>                  If EQUED = 'Y', the matrix A has been equilibrated
*>                  with scaling factors given by S.  AB and AFB will not
*>                  be modified.
*>          = 'N':  The matrix A will be copied to AFB and factored.
*>          = 'E':  The matrix A will be equilibrated if necessary, then
*>                  copied to AFB and factored.
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
*> \param[in] KD
*> \verbatim
*>          KD is INTEGER
*>          The number of superdiagonals of the matrix A if UPLO = 'U',
*>          or the number of subdiagonals if UPLO = 'L'.  KD >= 0.
*> \endverbatim
*>
*> \param[in] NRHS
*> \verbatim
*>          NRHS is INTEGER
*>          The number of right-hand sides, i.e., the number of columns
*>          of the matrices B and X.  NRHS >= 0.
*> \endverbatim
*>
*> \param[in,out] AB
*> \verbatim
*>          AB is REAL array, dimension (LDAB,N)
*>          On entry, the upper or lower triangle of the symmetric band
*>          matrix A, stored in the first KD+1 rows of the array, except
*>          if FACT = 'F' and EQUED = 'Y', then A must contain the
*>          equilibrated matrix diag(S)*A*diag(S).  The j-th column of A
*>          is stored in the j-th column of the array AB as follows:
*>          if UPLO = 'U', AB(KD+1+i-j,j) = A(i,j) for max(1,j-KD)<=i<=j;
*>          if UPLO = 'L', AB(1+i-j,j)    = A(i,j) for j<=i<=min(N,j+KD).
*>          See below for further details.
*>
*>          On exit, if FACT = 'E' and EQUED = 'Y', A is overwritten by
*>          diag(S)*A*diag(S).
*> \endverbatim
*>
*> \param[in] LDAB
*> \verbatim
*>          LDAB is INTEGER
*>          The leading dimension of the array A.  LDAB >= KD+1.
*> \endverbatim
*>
*> \param[in,out] AFB
*> \verbatim
*>          AFB is REAL array, dimension (LDAFB,N)
*>          If FACT = 'F', then AFB is an input argument and on entry
*>          contains the triangular factor U or L from the Cholesky
*>          factorization A = U**T*U or A = L*L**T of the band matrix
*>          A, in the same storage format as A (see AB).  If EQUED = 'Y',
*>          then AFB is the factored form of the equilibrated matrix A.
*>
*>          If FACT = 'N', then AFB is an output argument and on exit
*>          returns the triangular factor U or L from the Cholesky
*>          factorization A = U**T*U or A = L*L**T.
*>
*>          If FACT = 'E', then AFB is an output argument and on exit
*>          returns the triangular factor U or L from the Cholesky
*>          factorization A = U**T*U or A = L*L**T of the equilibrated
*>          matrix A (see the description of A for the form of the
*>          equilibrated matrix).
*> \endverbatim
*>
*> \param[in] LDAFB
*> \verbatim
*>          LDAFB is INTEGER
*>          The leading dimension of the array AFB.  LDAFB >= KD+1.
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
*>          B is REAL array, dimension (LDB,NRHS)
*>          On entry, the N-by-NRHS right hand side matrix B.
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
*>          X is REAL array, dimension (LDX,NRHS)
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
*> \ingroup realOTHERsolve
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  The band storage scheme is illustrated by the following example, when
*>  N = 6, KD = 2, and UPLO = 'U':
*>
*>  Two-dimensional storage of the symmetric matrix A:
*>
*>     a11  a12  a13
*>          a22  a23  a24
*>               a33  a34  a35
*>                    a44  a45  a46
*>                         a55  a56
*>     (aij=conjg(aji))         a66
*>
*>  Band storage of the upper triangle of A:
*>
*>      *    *   a13  a24  a35  a46
*>      *   a12  a23  a34  a45  a56
*>     a11  a22  a33  a44  a55  a66
*>
*>  Similarly, if UPLO = 'L' the format of A is as follows:
*>
*>     a11  a22  a33  a44  a55  a66
*>     a21  a32  a43  a54  a65   *
*>     a31  a42  a53  a64   *    *
*>
*>  Array elements marked * are not used by the routine.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE SPBSVX( FACT, UPLO, N, KD, NRHS, AB, LDAB, AFB, LDAFB,
     $                   EQUED, S, B, LDB, X, LDX, RCOND, FERR, BERR,
     $                   WORK, IWORK, INFO )
*
*  -- LAPACK driver routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     April 2012
*
*     .. Scalar Arguments ..
      CHARACTER          EQUED, FACT, UPLO
      INTEGER            INFO, KD, LDAB, LDAFB, LDB, LDX, N, NRHS
      REAL               RCOND
*     ..
*     .. Array Arguments ..
      INTEGER            IWORK( * )
      REAL               AB( LDAB, * ), AFB( LDAFB, * ), B( LDB, * ),
     $                   BERR( * ), FERR( * ), S( * ), WORK( * ),
     $                   X( LDX, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            EQUIL, NOFACT, RCEQU, UPPER
      INTEGER            I, INFEQU, J, J1, J2
      REAL               AMAX, ANORM, BIGNUM, SCOND, SMAX, SMIN, SMLNUM
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      REAL               SLAMCH, SLANSB
      EXTERNAL           LSAME, SLAMCH, SLANSB
*     ..
*     .. External Subroutines ..
      EXTERNAL           SCOPY, SLACPY, SLAQSB, SPBCON, SPBEQU, SPBRFS,
     $                   SPBTRF, SPBTRS, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN
*     ..
*     .. Executable Statements ..
*
      INFO = 0
      NOFACT = LSAME( FACT, 'N' )
      EQUIL = LSAME( FACT, 'E' )
      UPPER = LSAME( UPLO, 'U' )
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
      ELSE IF( .NOT.UPPER .AND. .NOT.LSAME( UPLO, 'L' ) ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( KD.LT.0 ) THEN
         INFO = -4
      ELSE IF( NRHS.LT.0 ) THEN
         INFO = -5
      ELSE IF( LDAB.LT.KD+1 ) THEN
         INFO = -7
      ELSE IF( LDAFB.LT.KD+1 ) THEN
         INFO = -9
      ELSE IF( LSAME( FACT, 'F' ) .AND. .NOT.
     $         ( RCEQU .OR. LSAME( EQUED, 'N' ) ) ) THEN
         INFO = -10
      ELSE
         IF( RCEQU ) THEN
            SMIN = BIGNUM
            SMAX = ZERO
            DO 10 J = 1, N
               SMIN = MIN( SMIN, S( J ) )
               SMAX = MAX( SMAX, S( J ) )
   10       CONTINUE
            IF( SMIN.LE.ZERO ) THEN
               INFO = -11
            ELSE IF( N.GT.0 ) THEN
               SCOND = MAX( SMIN, SMLNUM ) / MIN( SMAX, BIGNUM )
            ELSE
               SCOND = ONE
            END IF
         END IF
         IF( INFO.EQ.0 ) THEN
            IF( LDB.LT.MAX( 1, N ) ) THEN
               INFO = -13
            ELSE IF( LDX.LT.MAX( 1, N ) ) THEN
               INFO = -15
            END IF
         END IF
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SPBSVX', -INFO )
         RETURN
      END IF
*
      IF( EQUIL ) THEN
*
*        Compute row and column scalings to equilibrate the matrix A.
*
         CALL SPBEQU( UPLO, N, KD, AB, LDAB, S, SCOND, AMAX, INFEQU )
         IF( INFEQU.EQ.0 ) THEN
*
*           Equilibrate the matrix.
*
            CALL SLAQSB( UPLO, N, KD, AB, LDAB, S, SCOND, AMAX, EQUED )
            RCEQU = LSAME( EQUED, 'Y' )
         END IF
      END IF
*
*     Scale the right-hand side.
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
*        Compute the Cholesky factorization A = U**T *U or A = L*L**T.
*
         IF( UPPER ) THEN
            DO 40 J = 1, N
               J1 = MAX( J-KD, 1 )
               CALL SCOPY( J-J1+1, AB( KD+1-J+J1, J ), 1,
     $                     AFB( KD+1-J+J1, J ), 1 )
   40       CONTINUE
         ELSE
            DO 50 J = 1, N
               J2 = MIN( J+KD, N )
               CALL SCOPY( J2-J+1, AB( 1, J ), 1, AFB( 1, J ), 1 )
   50       CONTINUE
         END IF
*
         CALL SPBTRF( UPLO, N, KD, AFB, LDAFB, INFO )
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
      ANORM = SLANSB( '1', UPLO, N, KD, AB, LDAB, WORK )
*
*     Compute the reciprocal of the condition number of A.
*
      CALL SPBCON( UPLO, N, KD, AFB, LDAFB, ANORM, RCOND, WORK, IWORK,
     $             INFO )
*
*     Compute the solution matrix X.
*
      CALL SLACPY( 'Full', N, NRHS, B, LDB, X, LDX )
      CALL SPBTRS( UPLO, N, KD, NRHS, AFB, LDAFB, X, LDX, INFO )
*
*     Use iterative refinement to improve the computed solution and
*     compute error bounds and backward error estimates for it.
*
      CALL SPBRFS( UPLO, N, KD, NRHS, AB, LDAB, AFB, LDAFB, B, LDB, X,
     $             LDX, FERR, BERR, WORK, IWORK, INFO )
*
*     Transform the solution matrix X to a solution of the original
*     system.
*
      IF( RCEQU ) THEN
         DO 70 J = 1, NRHS
            DO 60 I = 1, N
               X( I, J ) = S( I )*X( I, J )
   60       CONTINUE
   70    CONTINUE
         DO 80 J = 1, NRHS
            FERR( J ) = FERR( J ) / SCOND
   80    CONTINUE
      END IF
*
*     Set INFO = N+1 if the matrix is singular to working precision.
*
      IF( RCOND.LT.SLAMCH( 'Epsilon' ) )
     $   INFO = N + 1
*
      RETURN
*
*     End of SPBSVX
*
      END
