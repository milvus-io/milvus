*> \brief <b> SGBSVX computes the solution to system of linear equations A * X = B for GB matrices</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SGBSVX + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/sgbsvx.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/sgbsvx.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/sgbsvx.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SGBSVX( FACT, TRANS, N, KL, KU, NRHS, AB, LDAB, AFB,
*                          LDAFB, IPIV, EQUED, R, C, B, LDB, X, LDX,
*                          RCOND, FERR, BERR, WORK, IWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          EQUED, FACT, TRANS
*       INTEGER            INFO, KL, KU, LDAB, LDAFB, LDB, LDX, N, NRHS
*       REAL               RCOND
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * ), IWORK( * )
*       REAL               AB( LDAB, * ), AFB( LDAFB, * ), B( LDB, * ),
*      $                   BERR( * ), C( * ), FERR( * ), R( * ),
*      $                   WORK( * ), X( LDX, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SGBSVX uses the LU factorization to compute the solution to a real
*> system of linear equations A * X = B, A**T * X = B, or A**H * X = B,
*> where A is a band matrix of order N with KL subdiagonals and KU
*> superdiagonals, and X and B are N-by-NRHS matrices.
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
*> The following steps are performed by this subroutine:
*>
*> 1. If FACT = 'E', real scaling factors are computed to equilibrate
*>    the system:
*>       TRANS = 'N':  diag(R)*A*diag(C)     *inv(diag(C))*X = diag(R)*B
*>       TRANS = 'T': (diag(R)*A*diag(C))**T *inv(diag(R))*X = diag(C)*B
*>       TRANS = 'C': (diag(R)*A*diag(C))**H *inv(diag(R))*X = diag(C)*B
*>    Whether or not the system will be equilibrated depends on the
*>    scaling of the matrix A, but if equilibration is used, A is
*>    overwritten by diag(R)*A*diag(C) and B by diag(R)*B (if TRANS='N')
*>    or diag(C)*B (if TRANS = 'T' or 'C').
*>
*> 2. If FACT = 'N' or 'E', the LU decomposition is used to factor the
*>    matrix A (after equilibration if FACT = 'E') as
*>       A = L * U,
*>    where L is a product of permutation and unit lower triangular
*>    matrices with KL subdiagonals, and U is upper triangular with
*>    KL+KU superdiagonals.
*>
*> 3. If some U(i,i)=0, so that U is exactly singular, then the routine
*>    returns with INFO = i. Otherwise, the factored form of A is used
*>    to estimate the condition number of the matrix A.  If the
*>    reciprocal of the condition number is less than machine precision,
*>    INFO = N+1 is returned as a warning, but the routine still goes on
*>    to solve for X and compute error bounds as described below.
*>
*> 4. The system of equations is solved for X using the factored form
*>    of A.
*>
*> 5. Iterative refinement is applied to improve the computed solution
*>    matrix and calculate error bounds and backward error estimates
*>    for it.
*>
*> 6. If equilibration was used, the matrix X is premultiplied by
*>    diag(C) (if TRANS = 'N') or diag(R) (if TRANS = 'T' or 'C') so
*>    that it solves the original system before equilibration.
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
*>          = 'F':  On entry, AFB and IPIV contain the factored form of
*>                  A.  If EQUED is not 'N', the matrix A has been
*>                  equilibrated with scaling factors given by R and C.
*>                  AB, AFB, and IPIV are not modified.
*>          = 'N':  The matrix A will be copied to AFB and factored.
*>          = 'E':  The matrix A will be equilibrated if necessary, then
*>                  copied to AFB and factored.
*> \endverbatim
*>
*> \param[in] TRANS
*> \verbatim
*>          TRANS is CHARACTER*1
*>          Specifies the form of the system of equations.
*>          = 'N':  A * X = B     (No transpose)
*>          = 'T':  A**T * X = B  (Transpose)
*>          = 'C':  A**H * X = B  (Transpose)
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of linear equations, i.e., the order of the
*>          matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] KL
*> \verbatim
*>          KL is INTEGER
*>          The number of subdiagonals within the band of A.  KL >= 0.
*> \endverbatim
*>
*> \param[in] KU
*> \verbatim
*>          KU is INTEGER
*>          The number of superdiagonals within the band of A.  KU >= 0.
*> \endverbatim
*>
*> \param[in] NRHS
*> \verbatim
*>          NRHS is INTEGER
*>          The number of right hand sides, i.e., the number of columns
*>          of the matrices B and X.  NRHS >= 0.
*> \endverbatim
*>
*> \param[in,out] AB
*> \verbatim
*>          AB is REAL array, dimension (LDAB,N)
*>          On entry, the matrix A in band storage, in rows 1 to KL+KU+1.
*>          The j-th column of A is stored in the j-th column of the
*>          array AB as follows:
*>          AB(KU+1+i-j,j) = A(i,j) for max(1,j-KU)<=i<=min(N,j+kl)
*>
*>          If FACT = 'F' and EQUED is not 'N', then A must have been
*>          equilibrated by the scaling factors in R and/or C.  AB is not
*>          modified if FACT = 'F' or 'N', or if FACT = 'E' and
*>          EQUED = 'N' on exit.
*>
*>          On exit, if EQUED .ne. 'N', A is scaled as follows:
*>          EQUED = 'R':  A := diag(R) * A
*>          EQUED = 'C':  A := A * diag(C)
*>          EQUED = 'B':  A := diag(R) * A * diag(C).
*> \endverbatim
*>
*> \param[in] LDAB
*> \verbatim
*>          LDAB is INTEGER
*>          The leading dimension of the array AB.  LDAB >= KL+KU+1.
*> \endverbatim
*>
*> \param[in,out] AFB
*> \verbatim
*>          AFB is REAL array, dimension (LDAFB,N)
*>          If FACT = 'F', then AFB is an input argument and on entry
*>          contains details of the LU factorization of the band matrix
*>          A, as computed by SGBTRF.  U is stored as an upper triangular
*>          band matrix with KL+KU superdiagonals in rows 1 to KL+KU+1,
*>          and the multipliers used during the factorization are stored
*>          in rows KL+KU+2 to 2*KL+KU+1.  If EQUED .ne. 'N', then AFB is
*>          the factored form of the equilibrated matrix A.
*>
*>          If FACT = 'N', then AFB is an output argument and on exit
*>          returns details of the LU factorization of A.
*>
*>          If FACT = 'E', then AFB is an output argument and on exit
*>          returns details of the LU factorization of the equilibrated
*>          matrix A (see the description of AB for the form of the
*>          equilibrated matrix).
*> \endverbatim
*>
*> \param[in] LDAFB
*> \verbatim
*>          LDAFB is INTEGER
*>          The leading dimension of the array AFB.  LDAFB >= 2*KL+KU+1.
*> \endverbatim
*>
*> \param[in,out] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (N)
*>          If FACT = 'F', then IPIV is an input argument and on entry
*>          contains the pivot indices from the factorization A = L*U
*>          as computed by SGBTRF; row i of the matrix was interchanged
*>          with row IPIV(i).
*>
*>          If FACT = 'N', then IPIV is an output argument and on exit
*>          contains the pivot indices from the factorization A = L*U
*>          of the original matrix A.
*>
*>          If FACT = 'E', then IPIV is an output argument and on exit
*>          contains the pivot indices from the factorization A = L*U
*>          of the equilibrated matrix A.
*> \endverbatim
*>
*> \param[in,out] EQUED
*> \verbatim
*>          EQUED is CHARACTER*1
*>          Specifies the form of equilibration that was done.
*>          = 'N':  No equilibration (always true if FACT = 'N').
*>          = 'R':  Row equilibration, i.e., A has been premultiplied by
*>                  diag(R).
*>          = 'C':  Column equilibration, i.e., A has been postmultiplied
*>                  by diag(C).
*>          = 'B':  Both row and column equilibration, i.e., A has been
*>                  replaced by diag(R) * A * diag(C).
*>          EQUED is an input argument if FACT = 'F'; otherwise, it is an
*>          output argument.
*> \endverbatim
*>
*> \param[in,out] R
*> \verbatim
*>          R is REAL array, dimension (N)
*>          The row scale factors for A.  If EQUED = 'R' or 'B', A is
*>          multiplied on the left by diag(R); if EQUED = 'N' or 'C', R
*>          is not accessed.  R is an input argument if FACT = 'F';
*>          otherwise, R is an output argument.  If FACT = 'F' and
*>          EQUED = 'R' or 'B', each element of R must be positive.
*> \endverbatim
*>
*> \param[in,out] C
*> \verbatim
*>          C is REAL array, dimension (N)
*>          The column scale factors for A.  If EQUED = 'C' or 'B', A is
*>          multiplied on the right by diag(C); if EQUED = 'N' or 'R', C
*>          is not accessed.  C is an input argument if FACT = 'F';
*>          otherwise, C is an output argument.  If FACT = 'F' and
*>          EQUED = 'C' or 'B', each element of C must be positive.
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is REAL array, dimension (LDB,NRHS)
*>          On entry, the right hand side matrix B.
*>          On exit,
*>          if EQUED = 'N', B is not modified;
*>          if TRANS = 'N' and EQUED = 'R' or 'B', B is overwritten by
*>          diag(R)*B;
*>          if TRANS = 'T' or 'C' and EQUED = 'C' or 'B', B is
*>          overwritten by diag(C)*B.
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
*>          If INFO = 0 or INFO = N+1, the N-by-NRHS solution matrix X
*>          to the original system of equations.  Note that A and B are
*>          modified on exit if EQUED .ne. 'N', and the solution to the
*>          equilibrated system is inv(diag(C))*X if TRANS = 'N' and
*>          EQUED = 'C' or 'B', or inv(diag(R))*X if TRANS = 'T' or 'C'
*>          and EQUED = 'R' or 'B'.
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
*>          On exit, WORK(1) contains the reciprocal pivot growth
*>          factor norm(A)/norm(U). The "max absolute element" norm is
*>          used. If WORK(1) is much less than 1, then the stability
*>          of the LU factorization of the (equilibrated) matrix A
*>          could be poor. This also means that the solution X, condition
*>          estimator RCOND, and forward error bound FERR could be
*>          unreliable. If factorization fails with 0<INFO<=N, then
*>          WORK(1) contains the reciprocal pivot growth factor for the
*>          leading INFO columns of A.
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
*>                       has been completed, but the factor U is exactly
*>                       singular, so the solution and error bounds
*>                       could not be computed. RCOND = 0 is returned.
*>                = N+1: U is nonsingular, but RCOND is less than machine
*>                       precision, meaning that the matrix is singular
*>                       to working precision.  Nevertheless, the
*>                       solution and error bounds are computed because
*>                       there are a number of situations where the
*>                       computed solution can be more accurate than the
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
*> \ingroup realGBsolve
*
*  =====================================================================
      SUBROUTINE SGBSVX( FACT, TRANS, N, KL, KU, NRHS, AB, LDAB, AFB,
     $                   LDAFB, IPIV, EQUED, R, C, B, LDB, X, LDX,
     $                   RCOND, FERR, BERR, WORK, IWORK, INFO )
*
*  -- LAPACK driver routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     April 2012
*
*     .. Scalar Arguments ..
      CHARACTER          EQUED, FACT, TRANS
      INTEGER            INFO, KL, KU, LDAB, LDAFB, LDB, LDX, N, NRHS
      REAL               RCOND
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * ), IWORK( * )
      REAL               AB( LDAB, * ), AFB( LDAFB, * ), B( LDB, * ),
     $                   BERR( * ), C( * ), FERR( * ), R( * ),
     $                   WORK( * ), X( LDX, * )
*     ..
*
*  =====================================================================
*  Moved setting of INFO = N+1 so INFO does not subsequently get
*  overwritten.  Sven, 17 Mar 05.
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            COLEQU, EQUIL, NOFACT, NOTRAN, ROWEQU
      CHARACTER          NORM
      INTEGER            I, INFEQU, J, J1, J2
      REAL               AMAX, ANORM, BIGNUM, COLCND, RCMAX, RCMIN,
     $                   ROWCND, RPVGRW, SMLNUM
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      REAL               SLAMCH, SLANGB, SLANTB
      EXTERNAL           LSAME, SLAMCH, SLANGB, SLANTB
*     ..
*     .. External Subroutines ..
      EXTERNAL           SCOPY, SGBCON, SGBEQU, SGBRFS, SGBTRF, SGBTRS,
     $                   SLACPY, SLAQGB, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, MIN
*     ..
*     .. Executable Statements ..
*
      INFO = 0
      NOFACT = LSAME( FACT, 'N' )
      EQUIL = LSAME( FACT, 'E' )
      NOTRAN = LSAME( TRANS, 'N' )
      IF( NOFACT .OR. EQUIL ) THEN
         EQUED = 'N'
         ROWEQU = .FALSE.
         COLEQU = .FALSE.
      ELSE
         ROWEQU = LSAME( EQUED, 'R' ) .OR. LSAME( EQUED, 'B' )
         COLEQU = LSAME( EQUED, 'C' ) .OR. LSAME( EQUED, 'B' )
         SMLNUM = SLAMCH( 'Safe minimum' )
         BIGNUM = ONE / SMLNUM
      END IF
*
*     Test the input parameters.
*
      IF( .NOT.NOFACT .AND. .NOT.EQUIL .AND. .NOT.LSAME( FACT, 'F' ) )
     $     THEN
         INFO = -1
      ELSE IF( .NOT.NOTRAN .AND. .NOT.LSAME( TRANS, 'T' ) .AND. .NOT.
     $         LSAME( TRANS, 'C' ) ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( KL.LT.0 ) THEN
         INFO = -4
      ELSE IF( KU.LT.0 ) THEN
         INFO = -5
      ELSE IF( NRHS.LT.0 ) THEN
         INFO = -6
      ELSE IF( LDAB.LT.KL+KU+1 ) THEN
         INFO = -8
      ELSE IF( LDAFB.LT.2*KL+KU+1 ) THEN
         INFO = -10
      ELSE IF( LSAME( FACT, 'F' ) .AND. .NOT.
     $         ( ROWEQU .OR. COLEQU .OR. LSAME( EQUED, 'N' ) ) ) THEN
         INFO = -12
      ELSE
         IF( ROWEQU ) THEN
            RCMIN = BIGNUM
            RCMAX = ZERO
            DO 10 J = 1, N
               RCMIN = MIN( RCMIN, R( J ) )
               RCMAX = MAX( RCMAX, R( J ) )
   10       CONTINUE
            IF( RCMIN.LE.ZERO ) THEN
               INFO = -13
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
   20       CONTINUE
            IF( RCMIN.LE.ZERO ) THEN
               INFO = -14
            ELSE IF( N.GT.0 ) THEN
               COLCND = MAX( RCMIN, SMLNUM ) / MIN( RCMAX, BIGNUM )
            ELSE
               COLCND = ONE
            END IF
         END IF
         IF( INFO.EQ.0 ) THEN
            IF( LDB.LT.MAX( 1, N ) ) THEN
               INFO = -16
            ELSE IF( LDX.LT.MAX( 1, N ) ) THEN
               INFO = -18
            END IF
         END IF
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SGBSVX', -INFO )
         RETURN
      END IF
*
      IF( EQUIL ) THEN
*
*        Compute row and column scalings to equilibrate the matrix A.
*
         CALL SGBEQU( N, N, KL, KU, AB, LDAB, R, C, ROWCND, COLCND,
     $                AMAX, INFEQU )
         IF( INFEQU.EQ.0 ) THEN
*
*           Equilibrate the matrix.
*
            CALL SLAQGB( N, N, KL, KU, AB, LDAB, R, C, ROWCND, COLCND,
     $                   AMAX, EQUED )
            ROWEQU = LSAME( EQUED, 'R' ) .OR. LSAME( EQUED, 'B' )
            COLEQU = LSAME( EQUED, 'C' ) .OR. LSAME( EQUED, 'B' )
         END IF
      END IF
*
*     Scale the right hand side.
*
      IF( NOTRAN ) THEN
         IF( ROWEQU ) THEN
            DO 40 J = 1, NRHS
               DO 30 I = 1, N
                  B( I, J ) = R( I )*B( I, J )
   30          CONTINUE
   40       CONTINUE
         END IF
      ELSE IF( COLEQU ) THEN
         DO 60 J = 1, NRHS
            DO 50 I = 1, N
               B( I, J ) = C( I )*B( I, J )
   50       CONTINUE
   60    CONTINUE
      END IF
*
      IF( NOFACT .OR. EQUIL ) THEN
*
*        Compute the LU factorization of the band matrix A.
*
         DO 70 J = 1, N
            J1 = MAX( J-KU, 1 )
            J2 = MIN( J+KL, N )
            CALL SCOPY( J2-J1+1, AB( KU+1-J+J1, J ), 1,
     $                  AFB( KL+KU+1-J+J1, J ), 1 )
   70    CONTINUE
*
         CALL SGBTRF( N, N, KL, KU, AFB, LDAFB, IPIV, INFO )
*
*        Return if INFO is non-zero.
*
         IF( INFO.GT.0 ) THEN
*
*           Compute the reciprocal pivot growth factor of the
*           leading rank-deficient INFO columns of A.
*
            ANORM = ZERO
            DO 90 J = 1, INFO
               DO 80 I = MAX( KU+2-J, 1 ), MIN( N+KU+1-J, KL+KU+1 )
                  ANORM = MAX( ANORM, ABS( AB( I, J ) ) )
   80          CONTINUE
   90       CONTINUE
            RPVGRW = SLANTB( 'M', 'U', 'N', INFO, MIN( INFO-1, KL+KU ),
     $                       AFB( MAX( 1, KL+KU+2-INFO ), 1 ), LDAFB,
     $                       WORK )
            IF( RPVGRW.EQ.ZERO ) THEN
               RPVGRW = ONE
            ELSE
               RPVGRW = ANORM / RPVGRW
            END IF
            WORK( 1 ) = RPVGRW
            RCOND = ZERO
            RETURN
         END IF
      END IF
*
*     Compute the norm of the matrix A and the
*     reciprocal pivot growth factor RPVGRW.
*
      IF( NOTRAN ) THEN
         NORM = '1'
      ELSE
         NORM = 'I'
      END IF
      ANORM = SLANGB( NORM, N, KL, KU, AB, LDAB, WORK )
      RPVGRW = SLANTB( 'M', 'U', 'N', N, KL+KU, AFB, LDAFB, WORK )
      IF( RPVGRW.EQ.ZERO ) THEN
         RPVGRW = ONE
      ELSE
         RPVGRW = SLANGB( 'M', N, KL, KU, AB, LDAB, WORK ) / RPVGRW
      END IF
*
*     Compute the reciprocal of the condition number of A.
*
      CALL SGBCON( NORM, N, KL, KU, AFB, LDAFB, IPIV, ANORM, RCOND,
     $             WORK, IWORK, INFO )
*
*     Compute the solution matrix X.
*
      CALL SLACPY( 'Full', N, NRHS, B, LDB, X, LDX )
      CALL SGBTRS( TRANS, N, KL, KU, NRHS, AFB, LDAFB, IPIV, X, LDX,
     $             INFO )
*
*     Use iterative refinement to improve the computed solution and
*     compute error bounds and backward error estimates for it.
*
      CALL SGBRFS( TRANS, N, KL, KU, NRHS, AB, LDAB, AFB, LDAFB, IPIV,
     $             B, LDB, X, LDX, FERR, BERR, WORK, IWORK, INFO )
*
*     Transform the solution matrix X to a solution of the original
*     system.
*
      IF( NOTRAN ) THEN
         IF( COLEQU ) THEN
            DO 110 J = 1, NRHS
               DO 100 I = 1, N
                  X( I, J ) = C( I )*X( I, J )
  100          CONTINUE
  110       CONTINUE
            DO 120 J = 1, NRHS
               FERR( J ) = FERR( J ) / COLCND
  120       CONTINUE
         END IF
      ELSE IF( ROWEQU ) THEN
         DO 140 J = 1, NRHS
            DO 130 I = 1, N
               X( I, J ) = R( I )*X( I, J )
  130       CONTINUE
  140    CONTINUE
         DO 150 J = 1, NRHS
            FERR( J ) = FERR( J ) / ROWCND
  150    CONTINUE
      END IF
*
*     Set INFO = N+1 if the matrix is singular to working precision.
*
      IF( RCOND.LT.SLAMCH( 'Epsilon' ) )
     $   INFO = N + 1
*
      WORK( 1 ) = RPVGRW
      RETURN
*
*     End of SGBSVX
*
      END
