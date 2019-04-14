*> \brief \b ZHETRS_3
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZHETRS_3 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zhetrs_3.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zhetrs_3.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zhetrs_3.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZHETRS_3( UPLO, N, NRHS, A, LDA, E, IPIV, B, LDB,
*                            INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            INFO, LDA, LDB, N, NRHS
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * )
*       COMPLEX*16         A( LDA, * ), B( LDB, * ), E( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*> ZHETRS_3 solves a system of linear equations A * X = B with a complex
*> Hermitian matrix A using the factorization computed
*> by ZHETRF_RK or ZHETRF_BK:
*>
*>    A = P*U*D*(U**H)*(P**T) or A = P*L*D*(L**H)*(P**T),
*>
*> where U (or L) is unit upper (or lower) triangular matrix,
*> U**H (or L**H) is the conjugate of U (or L), P is a permutation
*> matrix, P**T is the transpose of P, and D is Hermitian and block
*> diagonal with 1-by-1 and 2-by-2 diagonal blocks.
*>
*> This algorithm is using Level 3 BLAS.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          Specifies whether the details of the factorization are
*>          stored as an upper or lower triangular matrix:
*>          = 'U':  Upper triangular, form is A = P*U*D*(U**H)*(P**T);
*>          = 'L':  Lower triangular, form is A = P*L*D*(L**H)*(P**T).
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
*> \param[in] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>          Diagonal of the block diagonal matrix D and factors U or L
*>          as computed by ZHETRF_RK and ZHETRF_BK:
*>            a) ONLY diagonal elements of the Hermitian block diagonal
*>               matrix D on the diagonal of A, i.e. D(k,k) = A(k,k);
*>               (superdiagonal (or subdiagonal) elements of D
*>                should be provided on entry in array E), and
*>            b) If UPLO = 'U': factor U in the superdiagonal part of A.
*>               If UPLO = 'L': factor L in the subdiagonal part of A.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[in] E
*> \verbatim
*>          E is COMPLEX*16 array, dimension (N)
*>          On entry, contains the superdiagonal (or subdiagonal)
*>          elements of the Hermitian block diagonal matrix D
*>          with 1-by-1 or 2-by-2 diagonal blocks, where
*>          If UPLO = 'U': E(i) = D(i-1,i),i=2:N, E(1) not referenced;
*>          If UPLO = 'L': E(i) = D(i+1,i),i=1:N-1, E(N) not referenced.
*>
*>          NOTE: For 1-by-1 diagonal block D(k), where
*>          1 <= k <= N, the element E(k) is not referenced in both
*>          UPLO = 'U' or UPLO = 'L' cases.
*> \endverbatim
*>
*> \param[in] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (N)
*>          Details of the interchanges and the block structure of D
*>          as determined by ZHETRF_RK or ZHETRF_BK.
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX*16 array, dimension (LDB,NRHS)
*>          On entry, the right hand side matrix B.
*>          On exit, the solution matrix X.
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
*> \date June 2017
*
*> \ingroup complex16HEcomputational
*
*> \par Contributors:
*  ==================
*>
*> \verbatim
*>
*>  June 2017,  Igor Kozachenko,
*>                  Computer Science Division,
*>                  University of California, Berkeley
*>
*>  September 2007, Sven Hammarling, Nicholas J. Higham, Craig Lucas,
*>                  School of Mathematics,
*>                  University of Manchester
*>
*> \endverbatim
*
*  =====================================================================
      SUBROUTINE ZHETRS_3( UPLO, N, NRHS, A, LDA, E, IPIV, B, LDB,
     $                     INFO )
*
*  -- LAPACK computational routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            INFO, LDA, LDB, N, NRHS
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * )
      COMPLEX*16         A( LDA, * ), B( LDB, * ), E( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX*16         ONE
      PARAMETER          ( ONE = ( 1.0D+0,0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            UPPER
      INTEGER            I, J, K, KP
      DOUBLE PRECISION   S
      COMPLEX*16         AK, AKM1, AKM1K, BK, BKM1, DENOM
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZDSCAL, ZSWAP, ZTRSM, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, DCONJG, MAX
*     ..
*     .. Executable Statements ..
*
      INFO = 0
      UPPER = LSAME( UPLO, 'U' )
      IF( .NOT.UPPER .AND. .NOT.LSAME( UPLO, 'L' ) ) THEN
         INFO = -1
      ELSE IF( N.LT.0 ) THEN
         INFO = -2
      ELSE IF( NRHS.LT.0 ) THEN
         INFO = -3
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -5
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -9
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZHETRS_3', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 .OR. NRHS.EQ.0 )
     $   RETURN
*
      IF( UPPER ) THEN
*
*        Begin Upper
*
*        Solve A*X = B, where A = U*D*U**H.
*
*        P**T * B
*
*        Interchange rows K and IPIV(K) of matrix B in the same order
*        that the formation order of IPIV(I) vector for Upper case.
*
*        (We can do the simple loop over IPIV with decrement -1,
*        since the ABS value of IPIV(I) represents the row index
*        of the interchange with row i in both 1x1 and 2x2 pivot cases)
*
         DO K = N, 1, -1
            KP = ABS( IPIV( K ) )
            IF( KP.NE.K ) THEN
               CALL ZSWAP( NRHS, B( K, 1 ), LDB, B( KP, 1 ), LDB )
            END IF
         END DO
*
*        Compute (U \P**T * B) -> B    [ (U \P**T * B) ]
*
         CALL ZTRSM( 'L', 'U', 'N', 'U', N, NRHS, ONE, A, LDA, B, LDB )
*
*        Compute D \ B -> B   [ D \ (U \P**T * B) ]
*
         I = N
         DO WHILE ( I.GE.1 )
            IF( IPIV( I ).GT.0 ) THEN
               S = DBLE( ONE ) / DBLE( A( I, I ) )
               CALL ZDSCAL( NRHS, S, B( I, 1 ), LDB )
            ELSE IF ( I.GT.1 ) THEN
               AKM1K = E( I )
               AKM1 = A( I-1, I-1 ) / AKM1K
               AK = A( I, I ) / DCONJG( AKM1K )
               DENOM = AKM1*AK - ONE
               DO J = 1, NRHS
                  BKM1 = B( I-1, J ) / AKM1K
                  BK = B( I, J ) / DCONJG( AKM1K )
                  B( I-1, J ) = ( AK*BKM1-BK ) / DENOM
                  B( I, J ) = ( AKM1*BK-BKM1 ) / DENOM
               END DO
               I = I - 1
            END IF
            I = I - 1
         END DO
*
*        Compute (U**H \ B) -> B   [ U**H \ (D \ (U \P**T * B) ) ]
*
         CALL ZTRSM( 'L', 'U', 'C', 'U', N, NRHS, ONE, A, LDA, B, LDB )
*
*        P * B  [ P * (U**H \ (D \ (U \P**T * B) )) ]
*
*        Interchange rows K and IPIV(K) of matrix B in reverse order
*        from the formation order of IPIV(I) vector for Upper case.
*
*        (We can do the simple loop over IPIV with increment 1,
*        since the ABS value of IPIV(I) represents the row index
*        of the interchange with row i in both 1x1 and 2x2 pivot cases)
*
         DO K = 1, N, 1
            KP = ABS( IPIV( K ) )
            IF( KP.NE.K ) THEN
               CALL ZSWAP( NRHS, B( K, 1 ), LDB, B( KP, 1 ), LDB )
            END IF
         END DO
*
      ELSE
*
*        Begin Lower
*
*        Solve A*X = B, where A = L*D*L**H.
*
*        P**T * B
*        Interchange rows K and IPIV(K) of matrix B in the same order
*        that the formation order of IPIV(I) vector for Lower case.
*
*        (We can do the simple loop over IPIV with increment 1,
*        since the ABS value of IPIV(I) represents the row index
*        of the interchange with row i in both 1x1 and 2x2 pivot cases)
*
         DO K = 1, N, 1
            KP = ABS( IPIV( K ) )
            IF( KP.NE.K ) THEN
               CALL ZSWAP( NRHS, B( K, 1 ), LDB, B( KP, 1 ), LDB )
            END IF
         END DO
*
*        Compute (L \P**T * B) -> B    [ (L \P**T * B) ]
*
         CALL ZTRSM( 'L', 'L', 'N', 'U', N, NRHS, ONE, A, LDA, B, LDB )
*
*        Compute D \ B -> B   [ D \ (L \P**T * B) ]
*
         I = 1
         DO WHILE ( I.LE.N )
            IF( IPIV( I ).GT.0 ) THEN
               S = DBLE( ONE ) / DBLE( A( I, I ) )
               CALL ZDSCAL( NRHS, S, B( I, 1 ), LDB )
            ELSE IF( I.LT.N ) THEN
               AKM1K = E( I )
               AKM1 = A( I, I ) / DCONJG( AKM1K )
               AK = A( I+1, I+1 ) / AKM1K
               DENOM = AKM1*AK - ONE
               DO  J = 1, NRHS
                  BKM1 = B( I, J ) / DCONJG( AKM1K )
                  BK = B( I+1, J ) / AKM1K
                  B( I, J ) = ( AK*BKM1-BK ) / DENOM
                  B( I+1, J ) = ( AKM1*BK-BKM1 ) / DENOM
               END DO
               I = I + 1
            END IF
            I = I + 1
         END DO
*
*        Compute (L**H \ B) -> B   [ L**H \ (D \ (L \P**T * B) ) ]
*
         CALL ZTRSM('L', 'L', 'C', 'U', N, NRHS, ONE, A, LDA, B, LDB )
*
*        P * B  [ P * (L**H \ (D \ (L \P**T * B) )) ]
*
*        Interchange rows K and IPIV(K) of matrix B in reverse order
*        from the formation order of IPIV(I) vector for Lower case.
*
*        (We can do the simple loop over IPIV with decrement -1,
*        since the ABS value of IPIV(I) represents the row index
*        of the interchange with row i in both 1x1 and 2x2 pivot cases)
*
         DO K = N, 1, -1
            KP = ABS( IPIV( K ) )
            IF( KP.NE.K ) THEN
               CALL ZSWAP( NRHS, B( K, 1 ), LDB, B( KP, 1 ), LDB )
            END IF
         END DO
*
*        END Lower
*
      END IF
*
      RETURN
*
*     End of ZHETRS_3
*
      END
