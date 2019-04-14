*> \brief \b ZGTRFS
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZGTRFS + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zgtrfs.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zgtrfs.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zgtrfs.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZGTRFS( TRANS, N, NRHS, DL, D, DU, DLF, DF, DUF, DU2,
*                          IPIV, B, LDB, X, LDX, FERR, BERR, WORK, RWORK,
*                          INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          TRANS
*       INTEGER            INFO, LDB, LDX, N, NRHS
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * )
*       DOUBLE PRECISION   BERR( * ), FERR( * ), RWORK( * )
*       COMPLEX*16         B( LDB, * ), D( * ), DF( * ), DL( * ),
*      $                   DLF( * ), DU( * ), DU2( * ), DUF( * ),
*      $                   WORK( * ), X( LDX, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZGTRFS improves the computed solution to a system of linear
*> equations when the coefficient matrix is tridiagonal, and provides
*> error bounds and backward error estimates for the solution.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] TRANS
*> \verbatim
*>          TRANS is CHARACTER*1
*>          Specifies the form of the system of equations:
*>          = 'N':  A * X = B     (No transpose)
*>          = 'T':  A**T * X = B  (Transpose)
*>          = 'C':  A**H * X = B  (Conjugate transpose)
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
*>          DL is COMPLEX*16 array, dimension (N-1)
*>          The (n-1) subdiagonal elements of A.
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is COMPLEX*16 array, dimension (N)
*>          The diagonal elements of A.
*> \endverbatim
*>
*> \param[in] DU
*> \verbatim
*>          DU is COMPLEX*16 array, dimension (N-1)
*>          The (n-1) superdiagonal elements of A.
*> \endverbatim
*>
*> \param[in] DLF
*> \verbatim
*>          DLF is COMPLEX*16 array, dimension (N-1)
*>          The (n-1) multipliers that define the matrix L from the
*>          LU factorization of A as computed by ZGTTRF.
*> \endverbatim
*>
*> \param[in] DF
*> \verbatim
*>          DF is COMPLEX*16 array, dimension (N)
*>          The n diagonal elements of the upper triangular matrix U from
*>          the LU factorization of A.
*> \endverbatim
*>
*> \param[in] DUF
*> \verbatim
*>          DUF is COMPLEX*16 array, dimension (N-1)
*>          The (n-1) elements of the first superdiagonal of U.
*> \endverbatim
*>
*> \param[in] DU2
*> \verbatim
*>          DU2 is COMPLEX*16 array, dimension (N-2)
*>          The (n-2) elements of the second superdiagonal of U.
*> \endverbatim
*>
*> \param[in] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (N)
*>          The pivot indices; for 1 <= i <= n, row i of the matrix was
*>          interchanged with row IPIV(i).  IPIV(i) will always be either
*>          i or i+1; IPIV(i) = i indicates a row interchange was not
*>          required.
*> \endverbatim
*>
*> \param[in] B
*> \verbatim
*>          B is COMPLEX*16 array, dimension (LDB,NRHS)
*>          The right hand side matrix B.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B.  LDB >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] X
*> \verbatim
*>          X is COMPLEX*16 array, dimension (LDX,NRHS)
*>          On entry, the solution matrix X, as computed by ZGTTRS.
*>          On exit, the improved solution matrix X.
*> \endverbatim
*>
*> \param[in] LDX
*> \verbatim
*>          LDX is INTEGER
*>          The leading dimension of the array X.  LDX >= max(1,N).
*> \endverbatim
*>
*> \param[out] FERR
*> \verbatim
*>          FERR is DOUBLE PRECISION array, dimension (NRHS)
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
*>          BERR is DOUBLE PRECISION array, dimension (NRHS)
*>          The componentwise relative backward error of each solution
*>          vector X(j) (i.e., the smallest relative change in
*>          any element of A or B that makes X(j) an exact solution).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (2*N)
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (N)
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
*> \endverbatim
*
*> \par Internal Parameters:
*  =========================
*>
*> \verbatim
*>  ITMAX is the maximum number of steps of iterative refinement.
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
*> \ingroup complex16GTcomputational
*
*  =====================================================================
      SUBROUTINE ZGTRFS( TRANS, N, NRHS, DL, D, DU, DLF, DF, DUF, DU2,
     $                   IPIV, B, LDB, X, LDX, FERR, BERR, WORK, RWORK,
     $                   INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          TRANS
      INTEGER            INFO, LDB, LDX, N, NRHS
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * )
      DOUBLE PRECISION   BERR( * ), FERR( * ), RWORK( * )
      COMPLEX*16         B( LDB, * ), D( * ), DF( * ), DL( * ),
     $                   DLF( * ), DU( * ), DU2( * ), DUF( * ),
     $                   WORK( * ), X( LDX, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            ITMAX
      PARAMETER          ( ITMAX = 5 )
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
      DOUBLE PRECISION   TWO
      PARAMETER          ( TWO = 2.0D+0 )
      DOUBLE PRECISION   THREE
      PARAMETER          ( THREE = 3.0D+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            NOTRAN
      CHARACTER          TRANSN, TRANST
      INTEGER            COUNT, I, J, KASE, NZ
      DOUBLE PRECISION   EPS, LSTRES, S, SAFE1, SAFE2, SAFMIN
      COMPLEX*16         ZDUM
*     ..
*     .. Local Arrays ..
      INTEGER            ISAVE( 3 )
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA, ZAXPY, ZCOPY, ZGTTRS, ZLACN2, ZLAGTM
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, DCMPLX, DIMAG, MAX
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      DOUBLE PRECISION   DLAMCH
      EXTERNAL           LSAME, DLAMCH
*     ..
*     .. Statement Functions ..
      DOUBLE PRECISION   CABS1
*     ..
*     .. Statement Function definitions ..
      CABS1( ZDUM ) = ABS( DBLE( ZDUM ) ) + ABS( DIMAG( ZDUM ) )
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
      NOTRAN = LSAME( TRANS, 'N' )
      IF( .NOT.NOTRAN .AND. .NOT.LSAME( TRANS, 'T' ) .AND. .NOT.
     $    LSAME( TRANS, 'C' ) ) THEN
         INFO = -1
      ELSE IF( N.LT.0 ) THEN
         INFO = -2
      ELSE IF( NRHS.LT.0 ) THEN
         INFO = -3
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -13
      ELSE IF( LDX.LT.MAX( 1, N ) ) THEN
         INFO = -15
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZGTRFS', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 .OR. NRHS.EQ.0 ) THEN
         DO 10 J = 1, NRHS
            FERR( J ) = ZERO
            BERR( J ) = ZERO
   10    CONTINUE
         RETURN
      END IF
*
      IF( NOTRAN ) THEN
         TRANSN = 'N'
         TRANST = 'C'
      ELSE
         TRANSN = 'C'
         TRANST = 'N'
      END IF
*
*     NZ = maximum number of nonzero elements in each row of A, plus 1
*
      NZ = 4
      EPS = DLAMCH( 'Epsilon' )
      SAFMIN = DLAMCH( 'Safe minimum' )
      SAFE1 = NZ*SAFMIN
      SAFE2 = SAFE1 / EPS
*
*     Do for each right hand side
*
      DO 110 J = 1, NRHS
*
         COUNT = 1
         LSTRES = THREE
   20    CONTINUE
*
*        Loop until stopping criterion is satisfied.
*
*        Compute residual R = B - op(A) * X,
*        where op(A) = A, A**T, or A**H, depending on TRANS.
*
         CALL ZCOPY( N, B( 1, J ), 1, WORK, 1 )
         CALL ZLAGTM( TRANS, N, 1, -ONE, DL, D, DU, X( 1, J ), LDX, ONE,
     $                WORK, N )
*
*        Compute abs(op(A))*abs(x) + abs(b) for use in the backward
*        error bound.
*
         IF( NOTRAN ) THEN
            IF( N.EQ.1 ) THEN
               RWORK( 1 ) = CABS1( B( 1, J ) ) +
     $                      CABS1( D( 1 ) )*CABS1( X( 1, J ) )
            ELSE
               RWORK( 1 ) = CABS1( B( 1, J ) ) +
     $                      CABS1( D( 1 ) )*CABS1( X( 1, J ) ) +
     $                      CABS1( DU( 1 ) )*CABS1( X( 2, J ) )
               DO 30 I = 2, N - 1
                  RWORK( I ) = CABS1( B( I, J ) ) +
     $                         CABS1( DL( I-1 ) )*CABS1( X( I-1, J ) ) +
     $                         CABS1( D( I ) )*CABS1( X( I, J ) ) +
     $                         CABS1( DU( I ) )*CABS1( X( I+1, J ) )
   30          CONTINUE
               RWORK( N ) = CABS1( B( N, J ) ) +
     $                      CABS1( DL( N-1 ) )*CABS1( X( N-1, J ) ) +
     $                      CABS1( D( N ) )*CABS1( X( N, J ) )
            END IF
         ELSE
            IF( N.EQ.1 ) THEN
               RWORK( 1 ) = CABS1( B( 1, J ) ) +
     $                      CABS1( D( 1 ) )*CABS1( X( 1, J ) )
            ELSE
               RWORK( 1 ) = CABS1( B( 1, J ) ) +
     $                      CABS1( D( 1 ) )*CABS1( X( 1, J ) ) +
     $                      CABS1( DL( 1 ) )*CABS1( X( 2, J ) )
               DO 40 I = 2, N - 1
                  RWORK( I ) = CABS1( B( I, J ) ) +
     $                         CABS1( DU( I-1 ) )*CABS1( X( I-1, J ) ) +
     $                         CABS1( D( I ) )*CABS1( X( I, J ) ) +
     $                         CABS1( DL( I ) )*CABS1( X( I+1, J ) )
   40          CONTINUE
               RWORK( N ) = CABS1( B( N, J ) ) +
     $                      CABS1( DU( N-1 ) )*CABS1( X( N-1, J ) ) +
     $                      CABS1( D( N ) )*CABS1( X( N, J ) )
            END IF
         END IF
*
*        Compute componentwise relative backward error from formula
*
*        max(i) ( abs(R(i)) / ( abs(op(A))*abs(X) + abs(B) )(i) )
*
*        where abs(Z) is the componentwise absolute value of the matrix
*        or vector Z.  If the i-th component of the denominator is less
*        than SAFE2, then SAFE1 is added to the i-th components of the
*        numerator and denominator before dividing.
*
         S = ZERO
         DO 50 I = 1, N
            IF( RWORK( I ).GT.SAFE2 ) THEN
               S = MAX( S, CABS1( WORK( I ) ) / RWORK( I ) )
            ELSE
               S = MAX( S, ( CABS1( WORK( I ) )+SAFE1 ) /
     $             ( RWORK( I )+SAFE1 ) )
            END IF
   50    CONTINUE
         BERR( J ) = S
*
*        Test stopping criterion. Continue iterating if
*           1) The residual BERR(J) is larger than machine epsilon, and
*           2) BERR(J) decreased by at least a factor of 2 during the
*              last iteration, and
*           3) At most ITMAX iterations tried.
*
         IF( BERR( J ).GT.EPS .AND. TWO*BERR( J ).LE.LSTRES .AND.
     $       COUNT.LE.ITMAX ) THEN
*
*           Update solution and try again.
*
            CALL ZGTTRS( TRANS, N, 1, DLF, DF, DUF, DU2, IPIV, WORK, N,
     $                   INFO )
            CALL ZAXPY( N, DCMPLX( ONE ), WORK, 1, X( 1, J ), 1 )
            LSTRES = BERR( J )
            COUNT = COUNT + 1
            GO TO 20
         END IF
*
*        Bound error from formula
*
*        norm(X - XTRUE) / norm(X) .le. FERR =
*        norm( abs(inv(op(A)))*
*           ( abs(R) + NZ*EPS*( abs(op(A))*abs(X)+abs(B) ))) / norm(X)
*
*        where
*          norm(Z) is the magnitude of the largest component of Z
*          inv(op(A)) is the inverse of op(A)
*          abs(Z) is the componentwise absolute value of the matrix or
*             vector Z
*          NZ is the maximum number of nonzeros in any row of A, plus 1
*          EPS is machine epsilon
*
*        The i-th component of abs(R)+NZ*EPS*(abs(op(A))*abs(X)+abs(B))
*        is incremented by SAFE1 if the i-th component of
*        abs(op(A))*abs(X) + abs(B) is less than SAFE2.
*
*        Use ZLACN2 to estimate the infinity-norm of the matrix
*           inv(op(A)) * diag(W),
*        where W = abs(R) + NZ*EPS*( abs(op(A))*abs(X)+abs(B) )))
*
         DO 60 I = 1, N
            IF( RWORK( I ).GT.SAFE2 ) THEN
               RWORK( I ) = CABS1( WORK( I ) ) + NZ*EPS*RWORK( I )
            ELSE
               RWORK( I ) = CABS1( WORK( I ) ) + NZ*EPS*RWORK( I ) +
     $                      SAFE1
            END IF
   60    CONTINUE
*
         KASE = 0
   70    CONTINUE
         CALL ZLACN2( N, WORK( N+1 ), WORK, FERR( J ), KASE, ISAVE )
         IF( KASE.NE.0 ) THEN
            IF( KASE.EQ.1 ) THEN
*
*              Multiply by diag(W)*inv(op(A)**H).
*
               CALL ZGTTRS( TRANST, N, 1, DLF, DF, DUF, DU2, IPIV, WORK,
     $                      N, INFO )
               DO 80 I = 1, N
                  WORK( I ) = RWORK( I )*WORK( I )
   80          CONTINUE
            ELSE
*
*              Multiply by inv(op(A))*diag(W).
*
               DO 90 I = 1, N
                  WORK( I ) = RWORK( I )*WORK( I )
   90          CONTINUE
               CALL ZGTTRS( TRANSN, N, 1, DLF, DF, DUF, DU2, IPIV, WORK,
     $                      N, INFO )
            END IF
            GO TO 70
         END IF
*
*        Normalize error.
*
         LSTRES = ZERO
         DO 100 I = 1, N
            LSTRES = MAX( LSTRES, CABS1( X( I, J ) ) )
  100    CONTINUE
         IF( LSTRES.NE.ZERO )
     $      FERR( J ) = FERR( J ) / LSTRES
*
  110 CONTINUE
*
      RETURN
*
*     End of ZGTRFS
*
      END
