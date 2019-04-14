*> \brief \b  CHETRS_ROOK computes the solution to a system of linear equations A * X = B for HE matrices using factorization obtained with one of the bounded diagonal pivoting methods (max 2 interchanges)
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CHETRS_ROOK + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/chetrs_rook.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/chetrs_rook.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/chetrs_rook.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CHETRS_ROOK( UPLO, N, NRHS, A, LDA, IPIV, B, LDB, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            INFO, LDA, LDB, N, NRHS
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * )
*       COMPLEX            A( LDA, * ), B( LDB, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CHETRS_ROOK solves a system of linear equations A*X = B with a complex
*> Hermitian matrix A using the factorization A = U*D*U**H or
*> A = L*D*L**H computed by CHETRF_ROOK.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          Specifies whether the details of the factorization are stored
*>          as an upper or lower triangular matrix.
*>          = 'U':  Upper triangular, form is A = U*D*U**H;
*>          = 'L':  Lower triangular, form is A = L*D*L**H.
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
*>          A is COMPLEX array, dimension (LDA,N)
*>          The block diagonal matrix D and the multipliers used to
*>          obtain the factor U or L as computed by CHETRF_ROOK.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[in] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (N)
*>          Details of the interchanges and the block structure of D
*>          as determined by CHETRF_ROOK.
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX array, dimension (LDB,NRHS)
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
*> \date November 2013
*
*> \ingroup complexHEcomputational
*
*> \par Contributors:
*  ==================
*>
*> \verbatim
*>
*>  November 2013,  Igor Kozachenko,
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
      SUBROUTINE CHETRS_ROOK( UPLO, N, NRHS, A, LDA, IPIV, B, LDB,
     $                        INFO )
*
*  -- LAPACK computational routine (version 3.5.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2013
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            INFO, LDA, LDB, N, NRHS
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * )
      COMPLEX            A( LDA, * ), B( LDB, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX            ONE
      PARAMETER          ( ONE = ( 1.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            UPPER
      INTEGER            J, K, KP
      REAL               S
      COMPLEX            AK, AKM1, AKM1K, BK, BKM1, DENOM
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           CGEMV, CGERU, CLACGV, CSSCAL, CSWAP, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          CONJG, MAX, REAL
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
         INFO = -8
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CHETRS_ROOK', -INFO )
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
*        Solve A*X = B, where A = U*D*U**H.
*
*        First solve U*D*X = B, overwriting B with X.
*
*        K is the main loop index, decreasing from N to 1 in steps of
*        1 or 2, depending on the size of the diagonal blocks.
*
         K = N
   10    CONTINUE
*
*        If K < 1, exit from loop.
*
         IF( K.LT.1 )
     $      GO TO 30
*
         IF( IPIV( K ).GT.0 ) THEN
*
*           1 x 1 diagonal block
*
*           Interchange rows K and IPIV(K).
*
            KP = IPIV( K )
            IF( KP.NE.K )
     $         CALL CSWAP( NRHS, B( K, 1 ), LDB, B( KP, 1 ), LDB )
*
*           Multiply by inv(U(K)), where U(K) is the transformation
*           stored in column K of A.
*
            CALL CGERU( K-1, NRHS, -ONE, A( 1, K ), 1, B( K, 1 ), LDB,
     $                  B( 1, 1 ), LDB )
*
*           Multiply by the inverse of the diagonal block.
*
            S = REAL( ONE ) / REAL( A( K, K ) )
            CALL CSSCAL( NRHS, S, B( K, 1 ), LDB )
            K = K - 1
         ELSE
*
*           2 x 2 diagonal block
*
*           Interchange rows K and -IPIV(K), then K-1 and -IPIV(K-1)
*
            KP = -IPIV( K )
            IF( KP.NE.K )
     $         CALL CSWAP( NRHS, B( K, 1 ), LDB, B( KP, 1 ), LDB )
*
            KP = -IPIV( K-1)
            IF( KP.NE.K-1 )
     $         CALL CSWAP( NRHS, B( K-1, 1 ), LDB, B( KP, 1 ), LDB )
*
*           Multiply by inv(U(K)), where U(K) is the transformation
*           stored in columns K-1 and K of A.
*
            CALL CGERU( K-2, NRHS, -ONE, A( 1, K ), 1, B( K, 1 ), LDB,
     $                  B( 1, 1 ), LDB )
            CALL CGERU( K-2, NRHS, -ONE, A( 1, K-1 ), 1, B( K-1, 1 ),
     $                  LDB, B( 1, 1 ), LDB )
*
*           Multiply by the inverse of the diagonal block.
*
            AKM1K = A( K-1, K )
            AKM1 = A( K-1, K-1 ) / AKM1K
            AK = A( K, K ) / CONJG( AKM1K )
            DENOM = AKM1*AK - ONE
            DO 20 J = 1, NRHS
               BKM1 = B( K-1, J ) / AKM1K
               BK = B( K, J ) / CONJG( AKM1K )
               B( K-1, J ) = ( AK*BKM1-BK ) / DENOM
               B( K, J ) = ( AKM1*BK-BKM1 ) / DENOM
   20       CONTINUE
            K = K - 2
         END IF
*
         GO TO 10
   30    CONTINUE
*
*        Next solve U**H *X = B, overwriting B with X.
*
*        K is the main loop index, increasing from 1 to N in steps of
*        1 or 2, depending on the size of the diagonal blocks.
*
         K = 1
   40    CONTINUE
*
*        If K > N, exit from loop.
*
         IF( K.GT.N )
     $      GO TO 50
*
         IF( IPIV( K ).GT.0 ) THEN
*
*           1 x 1 diagonal block
*
*           Multiply by inv(U**H(K)), where U(K) is the transformation
*           stored in column K of A.
*
            IF( K.GT.1 ) THEN
               CALL CLACGV( NRHS, B( K, 1 ), LDB )
               CALL CGEMV( 'Conjugate transpose', K-1, NRHS, -ONE, B,
     $                     LDB, A( 1, K ), 1, ONE, B( K, 1 ), LDB )
               CALL CLACGV( NRHS, B( K, 1 ), LDB )
            END IF
*
*           Interchange rows K and IPIV(K).
*
            KP = IPIV( K )
            IF( KP.NE.K )
     $         CALL CSWAP( NRHS, B( K, 1 ), LDB, B( KP, 1 ), LDB )
            K = K + 1
         ELSE
*
*           2 x 2 diagonal block
*
*           Multiply by inv(U**H(K+1)), where U(K+1) is the transformation
*           stored in columns K and K+1 of A.
*
            IF( K.GT.1 ) THEN
               CALL CLACGV( NRHS, B( K, 1 ), LDB )
               CALL CGEMV( 'Conjugate transpose', K-1, NRHS, -ONE, B,
     $                     LDB, A( 1, K ), 1, ONE, B( K, 1 ), LDB )
               CALL CLACGV( NRHS, B( K, 1 ), LDB )
*
               CALL CLACGV( NRHS, B( K+1, 1 ), LDB )
               CALL CGEMV( 'Conjugate transpose', K-1, NRHS, -ONE, B,
     $                     LDB, A( 1, K+1 ), 1, ONE, B( K+1, 1 ), LDB )
               CALL CLACGV( NRHS, B( K+1, 1 ), LDB )
            END IF
*
*           Interchange rows K and -IPIV(K), then K+1 and -IPIV(K+1)
*
            KP = -IPIV( K )
            IF( KP.NE.K )
     $         CALL CSWAP( NRHS, B( K, 1 ), LDB, B( KP, 1 ), LDB )
*
            KP = -IPIV( K+1 )
            IF( KP.NE.K+1 )
     $         CALL CSWAP( NRHS, B( K+1, 1 ), LDB, B( KP, 1 ), LDB )
*
            K = K + 2
         END IF
*
         GO TO 40
   50    CONTINUE
*
      ELSE
*
*        Solve A*X = B, where A = L*D*L**H.
*
*        First solve L*D*X = B, overwriting B with X.
*
*        K is the main loop index, increasing from 1 to N in steps of
*        1 or 2, depending on the size of the diagonal blocks.
*
         K = 1
   60    CONTINUE
*
*        If K > N, exit from loop.
*
         IF( K.GT.N )
     $      GO TO 80
*
         IF( IPIV( K ).GT.0 ) THEN
*
*           1 x 1 diagonal block
*
*           Interchange rows K and IPIV(K).
*
            KP = IPIV( K )
            IF( KP.NE.K )
     $         CALL CSWAP( NRHS, B( K, 1 ), LDB, B( KP, 1 ), LDB )
*
*           Multiply by inv(L(K)), where L(K) is the transformation
*           stored in column K of A.
*
            IF( K.LT.N )
     $         CALL CGERU( N-K, NRHS, -ONE, A( K+1, K ), 1, B( K, 1 ),
     $                     LDB, B( K+1, 1 ), LDB )
*
*           Multiply by the inverse of the diagonal block.
*
            S = REAL( ONE ) / REAL( A( K, K ) )
            CALL CSSCAL( NRHS, S, B( K, 1 ), LDB )
            K = K + 1
         ELSE
*
*           2 x 2 diagonal block
*
*           Interchange rows K and -IPIV(K), then K+1 and -IPIV(K+1)
*
            KP = -IPIV( K )
            IF( KP.NE.K )
     $         CALL CSWAP( NRHS, B( K, 1 ), LDB, B( KP, 1 ), LDB )
*
            KP = -IPIV( K+1 )
            IF( KP.NE.K+1 )
     $         CALL CSWAP( NRHS, B( K+1, 1 ), LDB, B( KP, 1 ), LDB )
*
*           Multiply by inv(L(K)), where L(K) is the transformation
*           stored in columns K and K+1 of A.
*
            IF( K.LT.N-1 ) THEN
               CALL CGERU( N-K-1, NRHS, -ONE, A( K+2, K ), 1, B( K, 1 ),
     $                     LDB, B( K+2, 1 ), LDB )
               CALL CGERU( N-K-1, NRHS, -ONE, A( K+2, K+1 ), 1,
     $                     B( K+1, 1 ), LDB, B( K+2, 1 ), LDB )
            END IF
*
*           Multiply by the inverse of the diagonal block.
*
            AKM1K = A( K+1, K )
            AKM1 = A( K, K ) / CONJG( AKM1K )
            AK = A( K+1, K+1 ) / AKM1K
            DENOM = AKM1*AK - ONE
            DO 70 J = 1, NRHS
               BKM1 = B( K, J ) / CONJG( AKM1K )
               BK = B( K+1, J ) / AKM1K
               B( K, J ) = ( AK*BKM1-BK ) / DENOM
               B( K+1, J ) = ( AKM1*BK-BKM1 ) / DENOM
   70       CONTINUE
            K = K + 2
         END IF
*
         GO TO 60
   80    CONTINUE
*
*        Next solve L**H *X = B, overwriting B with X.
*
*        K is the main loop index, decreasing from N to 1 in steps of
*        1 or 2, depending on the size of the diagonal blocks.
*
         K = N
   90    CONTINUE
*
*        If K < 1, exit from loop.
*
         IF( K.LT.1 )
     $      GO TO 100
*
         IF( IPIV( K ).GT.0 ) THEN
*
*           1 x 1 diagonal block
*
*           Multiply by inv(L**H(K)), where L(K) is the transformation
*           stored in column K of A.
*
            IF( K.LT.N ) THEN
               CALL CLACGV( NRHS, B( K, 1 ), LDB )
               CALL CGEMV( 'Conjugate transpose', N-K, NRHS, -ONE,
     $                     B( K+1, 1 ), LDB, A( K+1, K ), 1, ONE,
     $                     B( K, 1 ), LDB )
               CALL CLACGV( NRHS, B( K, 1 ), LDB )
            END IF
*
*           Interchange rows K and IPIV(K).
*
            KP = IPIV( K )
            IF( KP.NE.K )
     $         CALL CSWAP( NRHS, B( K, 1 ), LDB, B( KP, 1 ), LDB )
            K = K - 1
         ELSE
*
*           2 x 2 diagonal block
*
*           Multiply by inv(L**H(K-1)), where L(K-1) is the transformation
*           stored in columns K-1 and K of A.
*
            IF( K.LT.N ) THEN
               CALL CLACGV( NRHS, B( K, 1 ), LDB )
               CALL CGEMV( 'Conjugate transpose', N-K, NRHS, -ONE,
     $                     B( K+1, 1 ), LDB, A( K+1, K ), 1, ONE,
     $                     B( K, 1 ), LDB )
               CALL CLACGV( NRHS, B( K, 1 ), LDB )
*
               CALL CLACGV( NRHS, B( K-1, 1 ), LDB )
               CALL CGEMV( 'Conjugate transpose', N-K, NRHS, -ONE,
     $                     B( K+1, 1 ), LDB, A( K+1, K-1 ), 1, ONE,
     $                     B( K-1, 1 ), LDB )
               CALL CLACGV( NRHS, B( K-1, 1 ), LDB )
            END IF
*
*           Interchange rows K and -IPIV(K), then K-1 and -IPIV(K-1)
*
            KP = -IPIV( K )
            IF( KP.NE.K )
     $         CALL CSWAP( NRHS, B( K, 1 ), LDB, B( KP, 1 ), LDB )
*
            KP = -IPIV( K-1 )
            IF( KP.NE.K-1 )
     $         CALL CSWAP( NRHS, B( K-1, 1 ), LDB, B( KP, 1 ), LDB )
*
            K = K - 2
         END IF
*
         GO TO 90
  100    CONTINUE
      END IF
*
      RETURN
*
*     End of CHETRS_ROOK
*
      END
