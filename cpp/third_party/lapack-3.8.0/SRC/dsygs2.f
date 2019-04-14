*> \brief \b DSYGS2 reduces a symmetric definite generalized eigenproblem to standard form, using the factorization results obtained from spotrf (unblocked algorithm).
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DSYGS2 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dsygs2.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dsygs2.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dsygs2.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DSYGS2( ITYPE, UPLO, N, A, LDA, B, LDB, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            INFO, ITYPE, LDA, LDB, N
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   A( LDA, * ), B( LDB, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DSYGS2 reduces a real symmetric-definite generalized eigenproblem
*> to standard form.
*>
*> If ITYPE = 1, the problem is A*x = lambda*B*x,
*> and A is overwritten by inv(U**T)*A*inv(U) or inv(L)*A*inv(L**T)
*>
*> If ITYPE = 2 or 3, the problem is A*B*x = lambda*x or
*> B*A*x = lambda*x, and A is overwritten by U*A*U**T or L**T *A*L.
*>
*> B must have been previously factorized as U**T *U or L*L**T by DPOTRF.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] ITYPE
*> \verbatim
*>          ITYPE is INTEGER
*>          = 1: compute inv(U**T)*A*inv(U) or inv(L)*A*inv(L**T);
*>          = 2 or 3: compute U*A*U**T or L**T *A*L.
*> \endverbatim
*>
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          Specifies whether the upper or lower triangular part of the
*>          symmetric matrix A is stored, and how B has been factorized.
*>          = 'U':  Upper triangular
*>          = 'L':  Lower triangular
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrices A and B.  N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension (LDA,N)
*>          On entry, the symmetric matrix A.  If UPLO = 'U', the leading
*>          n by n upper triangular part of A contains the upper
*>          triangular part of the matrix A, and the strictly lower
*>          triangular part of A is not referenced.  If UPLO = 'L', the
*>          leading n by n lower triangular part of A contains the lower
*>          triangular part of the matrix A, and the strictly upper
*>          triangular part of A is not referenced.
*>
*>          On exit, if INFO = 0, the transformed matrix, stored in the
*>          same format as A.
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
*>          B is DOUBLE PRECISION array, dimension (LDB,N)
*>          The triangular factor from the Cholesky factorization of B,
*>          as returned by DPOTRF.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B.  LDB >= max(1,N).
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit.
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
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
*> \ingroup doubleSYcomputational
*
*  =====================================================================
      SUBROUTINE DSYGS2( ITYPE, UPLO, N, A, LDA, B, LDB, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            INFO, ITYPE, LDA, LDB, N
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   A( LDA, * ), B( LDB, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ONE, HALF
      PARAMETER          ( ONE = 1.0D0, HALF = 0.5D0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            UPPER
      INTEGER            K
      DOUBLE PRECISION   AKK, BKK, CT
*     ..
*     .. External Subroutines ..
      EXTERNAL           DAXPY, DSCAL, DSYR2, DTRMV, DTRSV, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
      UPPER = LSAME( UPLO, 'U' )
      IF( ITYPE.LT.1 .OR. ITYPE.GT.3 ) THEN
         INFO = -1
      ELSE IF( .NOT.UPPER .AND. .NOT.LSAME( UPLO, 'L' ) ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -5
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -7
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DSYGS2', -INFO )
         RETURN
      END IF
*
      IF( ITYPE.EQ.1 ) THEN
         IF( UPPER ) THEN
*
*           Compute inv(U**T)*A*inv(U)
*
            DO 10 K = 1, N
*
*              Update the upper triangle of A(k:n,k:n)
*
               AKK = A( K, K )
               BKK = B( K, K )
               AKK = AKK / BKK**2
               A( K, K ) = AKK
               IF( K.LT.N ) THEN
                  CALL DSCAL( N-K, ONE / BKK, A( K, K+1 ), LDA )
                  CT = -HALF*AKK
                  CALL DAXPY( N-K, CT, B( K, K+1 ), LDB, A( K, K+1 ),
     $                        LDA )
                  CALL DSYR2( UPLO, N-K, -ONE, A( K, K+1 ), LDA,
     $                        B( K, K+1 ), LDB, A( K+1, K+1 ), LDA )
                  CALL DAXPY( N-K, CT, B( K, K+1 ), LDB, A( K, K+1 ),
     $                        LDA )
                  CALL DTRSV( UPLO, 'Transpose', 'Non-unit', N-K,
     $                        B( K+1, K+1 ), LDB, A( K, K+1 ), LDA )
               END IF
   10       CONTINUE
         ELSE
*
*           Compute inv(L)*A*inv(L**T)
*
            DO 20 K = 1, N
*
*              Update the lower triangle of A(k:n,k:n)
*
               AKK = A( K, K )
               BKK = B( K, K )
               AKK = AKK / BKK**2
               A( K, K ) = AKK
               IF( K.LT.N ) THEN
                  CALL DSCAL( N-K, ONE / BKK, A( K+1, K ), 1 )
                  CT = -HALF*AKK
                  CALL DAXPY( N-K, CT, B( K+1, K ), 1, A( K+1, K ), 1 )
                  CALL DSYR2( UPLO, N-K, -ONE, A( K+1, K ), 1,
     $                        B( K+1, K ), 1, A( K+1, K+1 ), LDA )
                  CALL DAXPY( N-K, CT, B( K+1, K ), 1, A( K+1, K ), 1 )
                  CALL DTRSV( UPLO, 'No transpose', 'Non-unit', N-K,
     $                        B( K+1, K+1 ), LDB, A( K+1, K ), 1 )
               END IF
   20       CONTINUE
         END IF
      ELSE
         IF( UPPER ) THEN
*
*           Compute U*A*U**T
*
            DO 30 K = 1, N
*
*              Update the upper triangle of A(1:k,1:k)
*
               AKK = A( K, K )
               BKK = B( K, K )
               CALL DTRMV( UPLO, 'No transpose', 'Non-unit', K-1, B,
     $                     LDB, A( 1, K ), 1 )
               CT = HALF*AKK
               CALL DAXPY( K-1, CT, B( 1, K ), 1, A( 1, K ), 1 )
               CALL DSYR2( UPLO, K-1, ONE, A( 1, K ), 1, B( 1, K ), 1,
     $                     A, LDA )
               CALL DAXPY( K-1, CT, B( 1, K ), 1, A( 1, K ), 1 )
               CALL DSCAL( K-1, BKK, A( 1, K ), 1 )
               A( K, K ) = AKK*BKK**2
   30       CONTINUE
         ELSE
*
*           Compute L**T *A*L
*
            DO 40 K = 1, N
*
*              Update the lower triangle of A(1:k,1:k)
*
               AKK = A( K, K )
               BKK = B( K, K )
               CALL DTRMV( UPLO, 'Transpose', 'Non-unit', K-1, B, LDB,
     $                     A( K, 1 ), LDA )
               CT = HALF*AKK
               CALL DAXPY( K-1, CT, B( K, 1 ), LDB, A( K, 1 ), LDA )
               CALL DSYR2( UPLO, K-1, ONE, A( K, 1 ), LDA, B( K, 1 ),
     $                     LDB, A, LDA )
               CALL DAXPY( K-1, CT, B( K, 1 ), LDB, A( K, 1 ), LDA )
               CALL DSCAL( K-1, BKK, A( K, 1 ), LDA )
               A( K, K ) = AKK*BKK**2
   40       CONTINUE
         END IF
      END IF
      RETURN
*
*     End of DSYGS2
*
      END
