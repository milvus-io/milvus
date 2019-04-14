*> \brief \b DTPRFS
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DTPRFS + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dtprfs.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dtprfs.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dtprfs.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DTPRFS( UPLO, TRANS, DIAG, N, NRHS, AP, B, LDB, X, LDX,
*                          FERR, BERR, WORK, IWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          DIAG, TRANS, UPLO
*       INTEGER            INFO, LDB, LDX, N, NRHS
*       ..
*       .. Array Arguments ..
*       INTEGER            IWORK( * )
*       DOUBLE PRECISION   AP( * ), B( LDB, * ), BERR( * ), FERR( * ),
*      $                   WORK( * ), X( LDX, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DTPRFS provides error bounds and backward error estimates for the
*> solution to a system of linear equations with a triangular packed
*> coefficient matrix.
*>
*> The solution matrix X must be computed by DTPTRS or some other
*> means before entering this routine.  DTPRFS does not do iterative
*> refinement because doing so cannot improve the backward error.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          = 'U':  A is upper triangular;
*>          = 'L':  A is lower triangular.
*> \endverbatim
*>
*> \param[in] TRANS
*> \verbatim
*>          TRANS is CHARACTER*1
*>          Specifies the form of the system of equations:
*>          = 'N':  A * X = B  (No transpose)
*>          = 'T':  A**T * X = B  (Transpose)
*>          = 'C':  A**H * X = B  (Conjugate transpose = Transpose)
*> \endverbatim
*>
*> \param[in] DIAG
*> \verbatim
*>          DIAG is CHARACTER*1
*>          = 'N':  A is non-unit triangular;
*>          = 'U':  A is unit triangular.
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
*>          of the matrices B and X.  NRHS >= 0.
*> \endverbatim
*>
*> \param[in] AP
*> \verbatim
*>          AP is DOUBLE PRECISION array, dimension (N*(N+1)/2)
*>          The upper or lower triangular matrix A, packed columnwise in
*>          a linear array.  The j-th column of A is stored in the array
*>          AP as follows:
*>          if UPLO = 'U', AP(i + (j-1)*j/2) = A(i,j) for 1<=i<=j;
*>          if UPLO = 'L', AP(i + (j-1)*(2*n-j)/2) = A(i,j) for j<=i<=n.
*>          If DIAG = 'U', the diagonal elements of A are not referenced
*>          and are assumed to be 1.
*> \endverbatim
*>
*> \param[in] B
*> \verbatim
*>          B is DOUBLE PRECISION array, dimension (LDB,NRHS)
*>          The right hand side matrix B.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B.  LDB >= max(1,N).
*> \endverbatim
*>
*> \param[in] X
*> \verbatim
*>          X is DOUBLE PRECISION array, dimension (LDX,NRHS)
*>          The solution matrix X.
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
*>          WORK is DOUBLE PRECISION array, dimension (3*N)
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
*> \ingroup doubleOTHERcomputational
*
*  =====================================================================
      SUBROUTINE DTPRFS( UPLO, TRANS, DIAG, N, NRHS, AP, B, LDB, X, LDX,
     $                   FERR, BERR, WORK, IWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          DIAG, TRANS, UPLO
      INTEGER            INFO, LDB, LDX, N, NRHS
*     ..
*     .. Array Arguments ..
      INTEGER            IWORK( * )
      DOUBLE PRECISION   AP( * ), B( LDB, * ), BERR( * ), FERR( * ),
     $                   WORK( * ), X( LDX, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO
      PARAMETER          ( ZERO = 0.0D+0 )
      DOUBLE PRECISION   ONE
      PARAMETER          ( ONE = 1.0D+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            NOTRAN, NOUNIT, UPPER
      CHARACTER          TRANST
      INTEGER            I, J, K, KASE, KC, NZ
      DOUBLE PRECISION   EPS, LSTRES, S, SAFE1, SAFE2, SAFMIN, XK
*     ..
*     .. Local Arrays ..
      INTEGER            ISAVE( 3 )
*     ..
*     .. External Subroutines ..
      EXTERNAL           DAXPY, DCOPY, DLACN2, DTPMV, DTPSV, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      DOUBLE PRECISION   DLAMCH
      EXTERNAL           LSAME, DLAMCH
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
      UPPER = LSAME( UPLO, 'U' )
      NOTRAN = LSAME( TRANS, 'N' )
      NOUNIT = LSAME( DIAG, 'N' )
*
      IF( .NOT.UPPER .AND. .NOT.LSAME( UPLO, 'L' ) ) THEN
         INFO = -1
      ELSE IF( .NOT.NOTRAN .AND. .NOT.LSAME( TRANS, 'T' ) .AND. .NOT.
     $         LSAME( TRANS, 'C' ) ) THEN
         INFO = -2
      ELSE IF( .NOT.NOUNIT .AND. .NOT.LSAME( DIAG, 'U' ) ) THEN
         INFO = -3
      ELSE IF( N.LT.0 ) THEN
         INFO = -4
      ELSE IF( NRHS.LT.0 ) THEN
         INFO = -5
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -8
      ELSE IF( LDX.LT.MAX( 1, N ) ) THEN
         INFO = -10
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DTPRFS', -INFO )
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
         TRANST = 'T'
      ELSE
         TRANST = 'N'
      END IF
*
*     NZ = maximum number of nonzero elements in each row of A, plus 1
*
      NZ = N + 1
      EPS = DLAMCH( 'Epsilon' )
      SAFMIN = DLAMCH( 'Safe minimum' )
      SAFE1 = NZ*SAFMIN
      SAFE2 = SAFE1 / EPS
*
*     Do for each right hand side
*
      DO 250 J = 1, NRHS
*
*        Compute residual R = B - op(A) * X,
*        where op(A) = A or A**T, depending on TRANS.
*
         CALL DCOPY( N, X( 1, J ), 1, WORK( N+1 ), 1 )
         CALL DTPMV( UPLO, TRANS, DIAG, N, AP, WORK( N+1 ), 1 )
         CALL DAXPY( N, -ONE, B( 1, J ), 1, WORK( N+1 ), 1 )
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
         DO 20 I = 1, N
            WORK( I ) = ABS( B( I, J ) )
   20    CONTINUE
*
         IF( NOTRAN ) THEN
*
*           Compute abs(A)*abs(X) + abs(B).
*
            IF( UPPER ) THEN
               KC = 1
               IF( NOUNIT ) THEN
                  DO 40 K = 1, N
                     XK = ABS( X( K, J ) )
                     DO 30 I = 1, K
                        WORK( I ) = WORK( I ) + ABS( AP( KC+I-1 ) )*XK
   30                CONTINUE
                     KC = KC + K
   40             CONTINUE
               ELSE
                  DO 60 K = 1, N
                     XK = ABS( X( K, J ) )
                     DO 50 I = 1, K - 1
                        WORK( I ) = WORK( I ) + ABS( AP( KC+I-1 ) )*XK
   50                CONTINUE
                     WORK( K ) = WORK( K ) + XK
                     KC = KC + K
   60             CONTINUE
               END IF
            ELSE
               KC = 1
               IF( NOUNIT ) THEN
                  DO 80 K = 1, N
                     XK = ABS( X( K, J ) )
                     DO 70 I = K, N
                        WORK( I ) = WORK( I ) + ABS( AP( KC+I-K ) )*XK
   70                CONTINUE
                     KC = KC + N - K + 1
   80             CONTINUE
               ELSE
                  DO 100 K = 1, N
                     XK = ABS( X( K, J ) )
                     DO 90 I = K + 1, N
                        WORK( I ) = WORK( I ) + ABS( AP( KC+I-K ) )*XK
   90                CONTINUE
                     WORK( K ) = WORK( K ) + XK
                     KC = KC + N - K + 1
  100             CONTINUE
               END IF
            END IF
         ELSE
*
*           Compute abs(A**T)*abs(X) + abs(B).
*
            IF( UPPER ) THEN
               KC = 1
               IF( NOUNIT ) THEN
                  DO 120 K = 1, N
                     S = ZERO
                     DO 110 I = 1, K
                        S = S + ABS( AP( KC+I-1 ) )*ABS( X( I, J ) )
  110                CONTINUE
                     WORK( K ) = WORK( K ) + S
                     KC = KC + K
  120             CONTINUE
               ELSE
                  DO 140 K = 1, N
                     S = ABS( X( K, J ) )
                     DO 130 I = 1, K - 1
                        S = S + ABS( AP( KC+I-1 ) )*ABS( X( I, J ) )
  130                CONTINUE
                     WORK( K ) = WORK( K ) + S
                     KC = KC + K
  140             CONTINUE
               END IF
            ELSE
               KC = 1
               IF( NOUNIT ) THEN
                  DO 160 K = 1, N
                     S = ZERO
                     DO 150 I = K, N
                        S = S + ABS( AP( KC+I-K ) )*ABS( X( I, J ) )
  150                CONTINUE
                     WORK( K ) = WORK( K ) + S
                     KC = KC + N - K + 1
  160             CONTINUE
               ELSE
                  DO 180 K = 1, N
                     S = ABS( X( K, J ) )
                     DO 170 I = K + 1, N
                        S = S + ABS( AP( KC+I-K ) )*ABS( X( I, J ) )
  170                CONTINUE
                     WORK( K ) = WORK( K ) + S
                     KC = KC + N - K + 1
  180             CONTINUE
               END IF
            END IF
         END IF
         S = ZERO
         DO 190 I = 1, N
            IF( WORK( I ).GT.SAFE2 ) THEN
               S = MAX( S, ABS( WORK( N+I ) ) / WORK( I ) )
            ELSE
               S = MAX( S, ( ABS( WORK( N+I ) )+SAFE1 ) /
     $             ( WORK( I )+SAFE1 ) )
            END IF
  190    CONTINUE
         BERR( J ) = S
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
*        Use DLACN2 to estimate the infinity-norm of the matrix
*           inv(op(A)) * diag(W),
*        where W = abs(R) + NZ*EPS*( abs(op(A))*abs(X)+abs(B) )))
*
         DO 200 I = 1, N
            IF( WORK( I ).GT.SAFE2 ) THEN
               WORK( I ) = ABS( WORK( N+I ) ) + NZ*EPS*WORK( I )
            ELSE
               WORK( I ) = ABS( WORK( N+I ) ) + NZ*EPS*WORK( I ) + SAFE1
            END IF
  200    CONTINUE
*
         KASE = 0
  210    CONTINUE
         CALL DLACN2( N, WORK( 2*N+1 ), WORK( N+1 ), IWORK, FERR( J ),
     $                KASE, ISAVE )
         IF( KASE.NE.0 ) THEN
            IF( KASE.EQ.1 ) THEN
*
*              Multiply by diag(W)*inv(op(A)**T).
*
               CALL DTPSV( UPLO, TRANST, DIAG, N, AP, WORK( N+1 ), 1 )
               DO 220 I = 1, N
                  WORK( N+I ) = WORK( I )*WORK( N+I )
  220          CONTINUE
            ELSE
*
*              Multiply by inv(op(A))*diag(W).
*
               DO 230 I = 1, N
                  WORK( N+I ) = WORK( I )*WORK( N+I )
  230          CONTINUE
               CALL DTPSV( UPLO, TRANS, DIAG, N, AP, WORK( N+1 ), 1 )
            END IF
            GO TO 210
         END IF
*
*        Normalize error.
*
         LSTRES = ZERO
         DO 240 I = 1, N
            LSTRES = MAX( LSTRES, ABS( X( I, J ) ) )
  240    CONTINUE
         IF( LSTRES.NE.ZERO )
     $      FERR( J ) = FERR( J ) / LSTRES
*
  250 CONTINUE
*
      RETURN
*
*     End of DTPRFS
*
      END
