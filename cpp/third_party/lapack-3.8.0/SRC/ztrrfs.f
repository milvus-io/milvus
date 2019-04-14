*> \brief \b ZTRRFS
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZTRRFS + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/ztrrfs.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/ztrrfs.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/ztrrfs.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZTRRFS( UPLO, TRANS, DIAG, N, NRHS, A, LDA, B, LDB, X,
*                          LDX, FERR, BERR, WORK, RWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          DIAG, TRANS, UPLO
*       INTEGER            INFO, LDA, LDB, LDX, N, NRHS
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   BERR( * ), FERR( * ), RWORK( * )
*       COMPLEX*16         A( LDA, * ), B( LDB, * ), WORK( * ),
*      $                   X( LDX, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZTRRFS provides error bounds and backward error estimates for the
*> solution to a system of linear equations with a triangular
*> coefficient matrix.
*>
*> The solution matrix X must be computed by ZTRTRS or some other
*> means before entering this routine.  ZTRRFS does not do iterative
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
*>          = 'N':  A * X = B     (No transpose)
*>          = 'T':  A**T * X = B  (Transpose)
*>          = 'C':  A**H * X = B  (Conjugate transpose)
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
*> \param[in] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>          The triangular matrix A.  If UPLO = 'U', the leading N-by-N
*>          upper triangular part of the array A contains the upper
*>          triangular matrix, and the strictly lower triangular part of
*>          A is not referenced.  If UPLO = 'L', the leading N-by-N lower
*>          triangular part of the array A contains the lower triangular
*>          matrix, and the strictly upper triangular part of A is not
*>          referenced.  If DIAG = 'U', the diagonal elements of A are
*>          also not referenced and are assumed to be 1.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
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
*> \param[in] X
*> \verbatim
*>          X is COMPLEX*16 array, dimension (LDX,NRHS)
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
*> \ingroup complex16OTHERcomputational
*
*  =====================================================================
      SUBROUTINE ZTRRFS( UPLO, TRANS, DIAG, N, NRHS, A, LDA, B, LDB, X,
     $                   LDX, FERR, BERR, WORK, RWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          DIAG, TRANS, UPLO
      INTEGER            INFO, LDA, LDB, LDX, N, NRHS
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   BERR( * ), FERR( * ), RWORK( * )
      COMPLEX*16         A( LDA, * ), B( LDB, * ), WORK( * ),
     $                   X( LDX, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO
      PARAMETER          ( ZERO = 0.0D+0 )
      COMPLEX*16         ONE
      PARAMETER          ( ONE = ( 1.0D+0, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            NOTRAN, NOUNIT, UPPER
      CHARACTER          TRANSN, TRANST
      INTEGER            I, J, K, KASE, NZ
      DOUBLE PRECISION   EPS, LSTRES, S, SAFE1, SAFE2, SAFMIN, XK
      COMPLEX*16         ZDUM
*     ..
*     .. Local Arrays ..
      INTEGER            ISAVE( 3 )
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA, ZAXPY, ZCOPY, ZLACN2, ZTRMV, ZTRSV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, DIMAG, MAX
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
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -7
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -9
      ELSE IF( LDX.LT.MAX( 1, N ) ) THEN
         INFO = -11
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZTRRFS', -INFO )
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
*        where op(A) = A, A**T, or A**H, depending on TRANS.
*
         CALL ZCOPY( N, X( 1, J ), 1, WORK, 1 )
         CALL ZTRMV( UPLO, TRANS, DIAG, N, A, LDA, WORK, 1 )
         CALL ZAXPY( N, -ONE, B( 1, J ), 1, WORK, 1 )
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
            RWORK( I ) = CABS1( B( I, J ) )
   20    CONTINUE
*
         IF( NOTRAN ) THEN
*
*           Compute abs(A)*abs(X) + abs(B).
*
            IF( UPPER ) THEN
               IF( NOUNIT ) THEN
                  DO 40 K = 1, N
                     XK = CABS1( X( K, J ) )
                     DO 30 I = 1, K
                        RWORK( I ) = RWORK( I ) + CABS1( A( I, K ) )*XK
   30                CONTINUE
   40             CONTINUE
               ELSE
                  DO 60 K = 1, N
                     XK = CABS1( X( K, J ) )
                     DO 50 I = 1, K - 1
                        RWORK( I ) = RWORK( I ) + CABS1( A( I, K ) )*XK
   50                CONTINUE
                     RWORK( K ) = RWORK( K ) + XK
   60             CONTINUE
               END IF
            ELSE
               IF( NOUNIT ) THEN
                  DO 80 K = 1, N
                     XK = CABS1( X( K, J ) )
                     DO 70 I = K, N
                        RWORK( I ) = RWORK( I ) + CABS1( A( I, K ) )*XK
   70                CONTINUE
   80             CONTINUE
               ELSE
                  DO 100 K = 1, N
                     XK = CABS1( X( K, J ) )
                     DO 90 I = K + 1, N
                        RWORK( I ) = RWORK( I ) + CABS1( A( I, K ) )*XK
   90                CONTINUE
                     RWORK( K ) = RWORK( K ) + XK
  100             CONTINUE
               END IF
            END IF
         ELSE
*
*           Compute abs(A**H)*abs(X) + abs(B).
*
            IF( UPPER ) THEN
               IF( NOUNIT ) THEN
                  DO 120 K = 1, N
                     S = ZERO
                     DO 110 I = 1, K
                        S = S + CABS1( A( I, K ) )*CABS1( X( I, J ) )
  110                CONTINUE
                     RWORK( K ) = RWORK( K ) + S
  120             CONTINUE
               ELSE
                  DO 140 K = 1, N
                     S = CABS1( X( K, J ) )
                     DO 130 I = 1, K - 1
                        S = S + CABS1( A( I, K ) )*CABS1( X( I, J ) )
  130                CONTINUE
                     RWORK( K ) = RWORK( K ) + S
  140             CONTINUE
               END IF
            ELSE
               IF( NOUNIT ) THEN
                  DO 160 K = 1, N
                     S = ZERO
                     DO 150 I = K, N
                        S = S + CABS1( A( I, K ) )*CABS1( X( I, J ) )
  150                CONTINUE
                     RWORK( K ) = RWORK( K ) + S
  160             CONTINUE
               ELSE
                  DO 180 K = 1, N
                     S = CABS1( X( K, J ) )
                     DO 170 I = K + 1, N
                        S = S + CABS1( A( I, K ) )*CABS1( X( I, J ) )
  170                CONTINUE
                     RWORK( K ) = RWORK( K ) + S
  180             CONTINUE
               END IF
            END IF
         END IF
         S = ZERO
         DO 190 I = 1, N
            IF( RWORK( I ).GT.SAFE2 ) THEN
               S = MAX( S, CABS1( WORK( I ) ) / RWORK( I ) )
            ELSE
               S = MAX( S, ( CABS1( WORK( I ) )+SAFE1 ) /
     $             ( RWORK( I )+SAFE1 ) )
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
*        Use ZLACN2 to estimate the infinity-norm of the matrix
*           inv(op(A)) * diag(W),
*        where W = abs(R) + NZ*EPS*( abs(op(A))*abs(X)+abs(B) )))
*
         DO 200 I = 1, N
            IF( RWORK( I ).GT.SAFE2 ) THEN
               RWORK( I ) = CABS1( WORK( I ) ) + NZ*EPS*RWORK( I )
            ELSE
               RWORK( I ) = CABS1( WORK( I ) ) + NZ*EPS*RWORK( I ) +
     $                      SAFE1
            END IF
  200    CONTINUE
*
         KASE = 0
  210    CONTINUE
         CALL ZLACN2( N, WORK( N+1 ), WORK, FERR( J ), KASE, ISAVE )
         IF( KASE.NE.0 ) THEN
            IF( KASE.EQ.1 ) THEN
*
*              Multiply by diag(W)*inv(op(A)**H).
*
               CALL ZTRSV( UPLO, TRANST, DIAG, N, A, LDA, WORK, 1 )
               DO 220 I = 1, N
                  WORK( I ) = RWORK( I )*WORK( I )
  220          CONTINUE
            ELSE
*
*              Multiply by inv(op(A))*diag(W).
*
               DO 230 I = 1, N
                  WORK( I ) = RWORK( I )*WORK( I )
  230          CONTINUE
               CALL ZTRSV( UPLO, TRANSN, DIAG, N, A, LDA, WORK, 1 )
            END IF
            GO TO 210
         END IF
*
*        Normalize error.
*
         LSTRES = ZERO
         DO 240 I = 1, N
            LSTRES = MAX( LSTRES, CABS1( X( I, J ) ) )
  240    CONTINUE
         IF( LSTRES.NE.ZERO )
     $      FERR( J ) = FERR( J ) / LSTRES
*
  250 CONTINUE
*
      RETURN
*
*     End of ZTRRFS
*
      END
