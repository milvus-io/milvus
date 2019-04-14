*> \brief \b ZHEGS2 reduces a Hermitian definite generalized eigenproblem to standard form, using the factorization results obtained from cpotrf (unblocked algorithm).
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZHEGS2 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zhegs2.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zhegs2.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zhegs2.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZHEGS2( ITYPE, UPLO, N, A, LDA, B, LDB, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            INFO, ITYPE, LDA, LDB, N
*       ..
*       .. Array Arguments ..
*       COMPLEX*16         A( LDA, * ), B( LDB, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZHEGS2 reduces a complex Hermitian-definite generalized
*> eigenproblem to standard form.
*>
*> If ITYPE = 1, the problem is A*x = lambda*B*x,
*> and A is overwritten by inv(U**H)*A*inv(U) or inv(L)*A*inv(L**H)
*>
*> If ITYPE = 2 or 3, the problem is A*B*x = lambda*x or
*> B*A*x = lambda*x, and A is overwritten by U*A*U**H or L**H *A*L.
*>
*> B must have been previously factorized as U**H *U or L*L**H by ZPOTRF.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] ITYPE
*> \verbatim
*>          ITYPE is INTEGER
*>          = 1: compute inv(U**H)*A*inv(U) or inv(L)*A*inv(L**H);
*>          = 2 or 3: compute U*A*U**H or L**H *A*L.
*> \endverbatim
*>
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          Specifies whether the upper or lower triangular part of the
*>          Hermitian matrix A is stored, and how B has been factorized.
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
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>          On entry, the Hermitian matrix A.  If UPLO = 'U', the leading
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
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX*16 array, dimension (LDB,N)
*>          The triangular factor from the Cholesky factorization of B,
*>          as returned by ZPOTRF.
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
*> \ingroup complex16HEcomputational
*
*  =====================================================================
      SUBROUTINE ZHEGS2( ITYPE, UPLO, N, A, LDA, B, LDB, INFO )
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
      COMPLEX*16         A( LDA, * ), B( LDB, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ONE, HALF
      PARAMETER          ( ONE = 1.0D+0, HALF = 0.5D+0 )
      COMPLEX*16         CONE
      PARAMETER          ( CONE = ( 1.0D+0, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            UPPER
      INTEGER            K
      DOUBLE PRECISION   AKK, BKK
      COMPLEX*16         CT
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA, ZAXPY, ZDSCAL, ZHER2, ZLACGV, ZTRMV,
     $                   ZTRSV
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
         CALL XERBLA( 'ZHEGS2', -INFO )
         RETURN
      END IF
*
      IF( ITYPE.EQ.1 ) THEN
         IF( UPPER ) THEN
*
*           Compute inv(U**H)*A*inv(U)
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
                  CALL ZDSCAL( N-K, ONE / BKK, A( K, K+1 ), LDA )
                  CT = -HALF*AKK
                  CALL ZLACGV( N-K, A( K, K+1 ), LDA )
                  CALL ZLACGV( N-K, B( K, K+1 ), LDB )
                  CALL ZAXPY( N-K, CT, B( K, K+1 ), LDB, A( K, K+1 ),
     $                        LDA )
                  CALL ZHER2( UPLO, N-K, -CONE, A( K, K+1 ), LDA,
     $                        B( K, K+1 ), LDB, A( K+1, K+1 ), LDA )
                  CALL ZAXPY( N-K, CT, B( K, K+1 ), LDB, A( K, K+1 ),
     $                        LDA )
                  CALL ZLACGV( N-K, B( K, K+1 ), LDB )
                  CALL ZTRSV( UPLO, 'Conjugate transpose', 'Non-unit',
     $                        N-K, B( K+1, K+1 ), LDB, A( K, K+1 ),
     $                        LDA )
                  CALL ZLACGV( N-K, A( K, K+1 ), LDA )
               END IF
   10       CONTINUE
         ELSE
*
*           Compute inv(L)*A*inv(L**H)
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
                  CALL ZDSCAL( N-K, ONE / BKK, A( K+1, K ), 1 )
                  CT = -HALF*AKK
                  CALL ZAXPY( N-K, CT, B( K+1, K ), 1, A( K+1, K ), 1 )
                  CALL ZHER2( UPLO, N-K, -CONE, A( K+1, K ), 1,
     $                        B( K+1, K ), 1, A( K+1, K+1 ), LDA )
                  CALL ZAXPY( N-K, CT, B( K+1, K ), 1, A( K+1, K ), 1 )
                  CALL ZTRSV( UPLO, 'No transpose', 'Non-unit', N-K,
     $                        B( K+1, K+1 ), LDB, A( K+1, K ), 1 )
               END IF
   20       CONTINUE
         END IF
      ELSE
         IF( UPPER ) THEN
*
*           Compute U*A*U**H
*
            DO 30 K = 1, N
*
*              Update the upper triangle of A(1:k,1:k)
*
               AKK = A( K, K )
               BKK = B( K, K )
               CALL ZTRMV( UPLO, 'No transpose', 'Non-unit', K-1, B,
     $                     LDB, A( 1, K ), 1 )
               CT = HALF*AKK
               CALL ZAXPY( K-1, CT, B( 1, K ), 1, A( 1, K ), 1 )
               CALL ZHER2( UPLO, K-1, CONE, A( 1, K ), 1, B( 1, K ), 1,
     $                     A, LDA )
               CALL ZAXPY( K-1, CT, B( 1, K ), 1, A( 1, K ), 1 )
               CALL ZDSCAL( K-1, BKK, A( 1, K ), 1 )
               A( K, K ) = AKK*BKK**2
   30       CONTINUE
         ELSE
*
*           Compute L**H *A*L
*
            DO 40 K = 1, N
*
*              Update the lower triangle of A(1:k,1:k)
*
               AKK = A( K, K )
               BKK = B( K, K )
               CALL ZLACGV( K-1, A( K, 1 ), LDA )
               CALL ZTRMV( UPLO, 'Conjugate transpose', 'Non-unit', K-1,
     $                     B, LDB, A( K, 1 ), LDA )
               CT = HALF*AKK
               CALL ZLACGV( K-1, B( K, 1 ), LDB )
               CALL ZAXPY( K-1, CT, B( K, 1 ), LDB, A( K, 1 ), LDA )
               CALL ZHER2( UPLO, K-1, CONE, A( K, 1 ), LDA, B( K, 1 ),
     $                     LDB, A, LDA )
               CALL ZAXPY( K-1, CT, B( K, 1 ), LDB, A( K, 1 ), LDA )
               CALL ZLACGV( K-1, B( K, 1 ), LDB )
               CALL ZDSCAL( K-1, BKK, A( K, 1 ), LDA )
               CALL ZLACGV( K-1, A( K, 1 ), LDA )
               A( K, K ) = AKK*BKK**2
   40       CONTINUE
         END IF
      END IF
      RETURN
*
*     End of ZHEGS2
*
      END
