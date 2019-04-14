*> \brief \b ZHETRI_3X
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZHETRI_3X + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zhetri_3x.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zhetri_3x.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zhetri_3x.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZHETRI_3X( UPLO, N, A, LDA, E, IPIV, WORK, NB, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            INFO, LDA, N, NB
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * )
*       COMPLEX*16         A( LDA, * ),  E( * ), WORK( N+NB+1, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*> ZHETRI_3X computes the inverse of a complex Hermitian indefinite
*> matrix A using the factorization computed by ZHETRF_RK or ZHETRF_BK:
*>
*>     A = P*U*D*(U**H)*(P**T) or A = P*L*D*(L**H)*(P**T),
*>
*> where U (or L) is unit upper (or lower) triangular matrix,
*> U**H (or L**H) is the conjugate of U (or L), P is a permutation
*> matrix, P**T is the transpose of P, and D is Hermitian and block
*> diagonal with 1-by-1 and 2-by-2 diagonal blocks.
*>
*> This is the blocked version of the algorithm, calling Level 3 BLAS.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          Specifies whether the details of the factorization are
*>          stored as an upper or lower triangular matrix.
*>          = 'U':  Upper triangle of A is stored;
*>          = 'L':  Lower triangle of A is stored.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>          On entry, diagonal of the block diagonal matrix D and
*>          factors U or L as computed by ZHETRF_RK and ZHETRF_BK:
*>            a) ONLY diagonal elements of the Hermitian block diagonal
*>               matrix D on the diagonal of A, i.e. D(k,k) = A(k,k);
*>               (superdiagonal (or subdiagonal) elements of D
*>                should be provided on entry in array E), and
*>            b) If UPLO = 'U': factor U in the superdiagonal part of A.
*>               If UPLO = 'L': factor L in the subdiagonal part of A.
*>
*>          On exit, if INFO = 0, the Hermitian inverse of the original
*>          matrix.
*>             If UPLO = 'U': the upper triangular part of the inverse
*>             is formed and the part of A below the diagonal is not
*>             referenced;
*>             If UPLO = 'L': the lower triangular part of the inverse
*>             is formed and the part of A above the diagonal is not
*>             referenced.
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
*>          If UPLO = 'U': E(i) = D(i-1,i), i=2:N, E(1) not referenced;
*>          If UPLO = 'L': E(i) = D(i+1,i), i=1:N-1, E(N) not referenced.
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
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (N+NB+1,NB+3).
*> \endverbatim
*>
*> \param[in] NB
*> \verbatim
*>          NB is INTEGER
*>          Block size.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0: successful exit
*>          < 0: if INFO = -i, the i-th argument had an illegal value
*>          > 0: if INFO = i, D(i,i) = 0; the matrix is singular and its
*>               inverse could not be computed.
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
*> \verbatim
*>
*>  June 2017,  Igor Kozachenko,
*>                  Computer Science Division,
*>                  University of California, Berkeley
*>
*> \endverbatim
*
*  =====================================================================
      SUBROUTINE ZHETRI_3X( UPLO, N, A, LDA, E, IPIV, WORK, NB, INFO )
*
*  -- LAPACK computational routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            INFO, LDA, N, NB
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * )
      COMPLEX*16         A( LDA, * ), E( * ), WORK( N+NB+1, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ONE
      PARAMETER          ( ONE = 1.0D+0 )
      COMPLEX*16         CONE, CZERO
      PARAMETER          ( CONE = ( 1.0D+0, 0.0D+0 ),
     $                     CZERO = ( 0.0D+0, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            UPPER
      INTEGER            CUT, I, ICOUNT, INVD, IP, K, NNB, J, U11
      DOUBLE PRECISION   AK, AKP1, T
      COMPLEX*16         AKKP1, D, U01_I_J, U01_IP1_J, U11_I_J,
     $                   U11_IP1_J
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZGEMM, ZHESWAPR, ZTRTRI, ZTRMM, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DCONJG, DBLE, MAX
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
      UPPER = LSAME( UPLO, 'U' )
      IF( .NOT.UPPER .AND. .NOT.LSAME( UPLO, 'L' ) ) THEN
         INFO = -1
      ELSE IF( N.LT.0 ) THEN
         INFO = -2
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -4
      END IF
*
*     Quick return if possible
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZHETRI_3X', -INFO )
         RETURN
      END IF
      IF( N.EQ.0 )
     $   RETURN
*
*     Workspace got Non-diag elements of D
*
      DO K = 1, N
         WORK( K, 1 ) = E( K )
      END DO
*
*     Check that the diagonal matrix D is nonsingular.
*
      IF( UPPER ) THEN
*
*        Upper triangular storage: examine D from bottom to top
*
         DO INFO = N, 1, -1
            IF( IPIV( INFO ).GT.0 .AND. A( INFO, INFO ).EQ.CZERO )
     $         RETURN
         END DO
      ELSE
*
*        Lower triangular storage: examine D from top to bottom.
*
         DO INFO = 1, N
            IF( IPIV( INFO ).GT.0 .AND. A( INFO, INFO ).EQ.CZERO )
     $         RETURN
         END DO
      END IF
*
      INFO = 0
*
*     Splitting Workspace
*     U01 is a block ( N, NB+1 )
*     The first element of U01 is in WORK( 1, 1 )
*     U11 is a block ( NB+1, NB+1 )
*     The first element of U11 is in WORK( N+1, 1 )
*
      U11 = N
*
*     INVD is a block ( N, 2 )
*     The first element of INVD is in WORK( 1, INVD )
*
      INVD = NB + 2

      IF( UPPER ) THEN
*
*        Begin Upper
*
*        invA = P * inv(U**H) * inv(D) * inv(U) * P**T.
*
         CALL ZTRTRI( UPLO, 'U', N, A, LDA, INFO )
*
*        inv(D) and inv(D) * inv(U)
*
         K = 1
         DO WHILE( K.LE.N )
            IF( IPIV( K ).GT.0 ) THEN
*              1 x 1 diagonal NNB
               WORK( K, INVD ) = ONE / DBLE( A( K, K ) )
               WORK( K, INVD+1 ) = CZERO
            ELSE
*              2 x 2 diagonal NNB
               T = ABS( WORK( K+1, 1 ) )
               AK = DBLE( A( K, K ) ) / T
               AKP1 = DBLE( A( K+1, K+1 ) ) / T
               AKKP1 = WORK( K+1, 1 )  / T
               D = T*( AK*AKP1-CONE )
               WORK( K, INVD ) = AKP1 / D
               WORK( K+1, INVD+1 ) = AK / D
               WORK( K, INVD+1 ) = -AKKP1 / D
               WORK( K+1, INVD ) = DCONJG( WORK( K, INVD+1 ) )
               K = K + 1
            END IF
            K = K + 1
         END DO
*
*        inv(U**H) = (inv(U))**H
*
*        inv(U**H) * inv(D) * inv(U)
*
         CUT = N
         DO WHILE( CUT.GT.0 )
            NNB = NB
            IF( CUT.LE.NNB ) THEN
               NNB = CUT
            ELSE
               ICOUNT = 0
*              count negative elements,
               DO I = CUT+1-NNB, CUT
                  IF( IPIV( I ).LT.0 ) ICOUNT = ICOUNT + 1
               END DO
*              need a even number for a clear cut
               IF( MOD( ICOUNT, 2 ).EQ.1 ) NNB = NNB + 1
            END IF

            CUT = CUT - NNB
*
*           U01 Block
*
            DO I = 1, CUT
               DO J = 1, NNB
                  WORK( I, J ) = A( I, CUT+J )
               END DO
            END DO
*
*           U11 Block
*
            DO I = 1, NNB
               WORK( U11+I, I ) = CONE
               DO J = 1, I-1
                  WORK( U11+I, J ) = CZERO
                END DO
                DO J = I+1, NNB
                   WORK( U11+I, J ) = A( CUT+I, CUT+J )
                END DO
            END DO
*
*           invD * U01
*
            I = 1
            DO WHILE( I.LE.CUT )
               IF( IPIV( I ).GT.0 ) THEN
                  DO J = 1, NNB
                     WORK( I, J ) = WORK( I, INVD ) * WORK( I, J )
                  END DO
               ELSE
                  DO J = 1, NNB
                     U01_I_J = WORK( I, J )
                     U01_IP1_J = WORK( I+1, J )
                     WORK( I, J ) = WORK( I, INVD ) * U01_I_J
     $                            + WORK( I, INVD+1 ) * U01_IP1_J
                     WORK( I+1, J ) = WORK( I+1, INVD ) * U01_I_J
     $                              + WORK( I+1, INVD+1 ) * U01_IP1_J
                  END DO
                  I = I + 1
               END IF
               I = I + 1
            END DO
*
*           invD1 * U11
*
            I = 1
            DO WHILE ( I.LE.NNB )
               IF( IPIV( CUT+I ).GT.0 ) THEN
                  DO J = I, NNB
                     WORK( U11+I, J ) = WORK(CUT+I,INVD) * WORK(U11+I,J)
                  END DO
               ELSE
                  DO J = I, NNB
                     U11_I_J = WORK(U11+I,J)
                     U11_IP1_J = WORK(U11+I+1,J)
                     WORK( U11+I, J ) = WORK(CUT+I,INVD) * WORK(U11+I,J)
     $                            + WORK(CUT+I,INVD+1) * WORK(U11+I+1,J)
                     WORK( U11+I+1, J ) = WORK(CUT+I+1,INVD) * U11_I_J
     $                               + WORK(CUT+I+1,INVD+1) * U11_IP1_J
                  END DO
                  I = I + 1
               END IF
               I = I + 1
            END DO
*
*           U11**H * invD1 * U11 -> U11
*
            CALL ZTRMM( 'L', 'U', 'C', 'U', NNB, NNB,
     $                 CONE, A( CUT+1, CUT+1 ), LDA, WORK( U11+1, 1 ),
     $                 N+NB+1 )
*
            DO I = 1, NNB
               DO J = I, NNB
                  A( CUT+I, CUT+J ) = WORK( U11+I, J )
               END DO
            END DO
*
*           U01**H * invD * U01 -> A( CUT+I, CUT+J )
*
            CALL ZGEMM( 'C', 'N', NNB, NNB, CUT, CONE, A( 1, CUT+1 ),
     $                  LDA, WORK, N+NB+1, CZERO, WORK(U11+1,1),
     $                  N+NB+1 )

*
*           U11 =  U11**H * invD1 * U11 + U01**H * invD * U01
*
            DO I = 1, NNB
               DO J = I, NNB
                  A( CUT+I, CUT+J ) = A( CUT+I, CUT+J ) + WORK(U11+I,J)
               END DO
            END DO
*
*           U01 =  U00**H * invD0 * U01
*
            CALL ZTRMM( 'L', UPLO, 'C', 'U', CUT, NNB,
     $                  CONE, A, LDA, WORK, N+NB+1 )

*
*           Update U01
*
            DO I = 1, CUT
               DO J = 1, NNB
                  A( I, CUT+J ) = WORK( I, J )
               END DO
            END DO
*
*           Next Block
*
         END DO
*
*        Apply PERMUTATIONS P and P**T:
*        P * inv(U**H) * inv(D) * inv(U) * P**T.
*        Interchange rows and columns I and IPIV(I) in reverse order
*        from the formation order of IPIV vector for Upper case.
*
*        ( We can use a loop over IPIV with increment 1,
*        since the ABS value of IPIV(I) represents the row (column)
*        index of the interchange with row (column) i in both 1x1
*        and 2x2 pivot cases, i.e. we don't need separate code branches
*        for 1x1 and 2x2 pivot cases )
*
         DO I = 1, N
             IP = ABS( IPIV( I ) )
             IF( IP.NE.I ) THEN
                IF (I .LT. IP) CALL ZHESWAPR( UPLO, N, A, LDA, I ,IP )
                IF (I .GT. IP) CALL ZHESWAPR( UPLO, N, A, LDA, IP ,I )
             END IF
         END DO
*
      ELSE
*
*        Begin Lower
*
*        inv A = P * inv(L**H) * inv(D) * inv(L) * P**T.
*
         CALL ZTRTRI( UPLO, 'U', N, A, LDA, INFO )
*
*        inv(D) and inv(D) * inv(L)
*
         K = N
         DO WHILE ( K .GE. 1 )
            IF( IPIV( K ).GT.0 ) THEN
*              1 x 1 diagonal NNB
               WORK( K, INVD ) = ONE / DBLE( A( K, K ) )
               WORK( K, INVD+1 ) = CZERO
            ELSE
*              2 x 2 diagonal NNB
               T = ABS( WORK( K-1, 1 ) )
               AK = DBLE( A( K-1, K-1 ) ) / T
               AKP1 = DBLE( A( K, K ) ) / T
               AKKP1 = WORK( K-1, 1 ) / T
               D = T*( AK*AKP1-CONE )
               WORK( K-1, INVD ) = AKP1 / D
               WORK( K, INVD ) = AK / D
               WORK( K, INVD+1 ) = -AKKP1 / D
               WORK( K-1, INVD+1 ) = DCONJG( WORK( K, INVD+1 ) )
               K = K - 1
            END IF
            K = K - 1
         END DO
*
*        inv(L**H) = (inv(L))**H
*
*        inv(L**H) * inv(D) * inv(L)
*
         CUT = 0
         DO WHILE( CUT.LT.N )
            NNB = NB
            IF( (CUT + NNB).GT.N ) THEN
               NNB = N - CUT
            ELSE
               ICOUNT = 0
*              count negative elements,
               DO I = CUT + 1, CUT+NNB
                  IF ( IPIV( I ).LT.0 ) ICOUNT = ICOUNT + 1
               END DO
*              need a even number for a clear cut
               IF( MOD( ICOUNT, 2 ).EQ.1 ) NNB = NNB + 1
            END IF
*
*           L21 Block
*
            DO I = 1, N-CUT-NNB
               DO J = 1, NNB
                 WORK( I, J ) = A( CUT+NNB+I, CUT+J )
               END DO
            END DO
*
*           L11 Block
*
            DO I = 1, NNB
               WORK( U11+I, I) = CONE
               DO J = I+1, NNB
                  WORK( U11+I, J ) = CZERO
               END DO
               DO J = 1, I-1
                  WORK( U11+I, J ) = A( CUT+I, CUT+J )
               END DO
            END DO
*
*           invD*L21
*
            I = N-CUT-NNB
            DO WHILE( I.GE.1 )
               IF( IPIV( CUT+NNB+I ).GT.0 ) THEN
                  DO J = 1, NNB
                     WORK( I, J ) = WORK( CUT+NNB+I, INVD) * WORK( I, J)
                  END DO
               ELSE
                  DO J = 1, NNB
                     U01_I_J = WORK(I,J)
                     U01_IP1_J = WORK(I-1,J)
                     WORK(I,J)=WORK(CUT+NNB+I,INVD)*U01_I_J+
     $                        WORK(CUT+NNB+I,INVD+1)*U01_IP1_J
                     WORK(I-1,J)=WORK(CUT+NNB+I-1,INVD+1)*U01_I_J+
     $                        WORK(CUT+NNB+I-1,INVD)*U01_IP1_J
                  END DO
                  I = I - 1
               END IF
               I = I - 1
            END DO
*
*           invD1*L11
*
            I = NNB
            DO WHILE( I.GE.1 )
               IF( IPIV( CUT+I ).GT.0 ) THEN
                  DO J = 1, NNB
                     WORK( U11+I, J ) = WORK( CUT+I, INVD)*WORK(U11+I,J)
                  END DO

               ELSE
                  DO J = 1, NNB
                     U11_I_J = WORK( U11+I, J )
                     U11_IP1_J = WORK( U11+I-1, J )
                     WORK( U11+I, J ) = WORK(CUT+I,INVD) * WORK(U11+I,J)
     $                                + WORK(CUT+I,INVD+1) * U11_IP1_J
                     WORK( U11+I-1, J ) = WORK(CUT+I-1,INVD+1) * U11_I_J
     $                                  + WORK(CUT+I-1,INVD) * U11_IP1_J
                  END DO
                  I = I - 1
               END IF
               I = I - 1
            END DO
*
*           L11**H * invD1 * L11 -> L11
*
            CALL ZTRMM( 'L', UPLO, 'C', 'U', NNB, NNB, CONE,
     $                   A( CUT+1, CUT+1 ), LDA, WORK( U11+1, 1 ),
     $                   N+NB+1 )

*
            DO I = 1, NNB
               DO J = 1, I
                  A( CUT+I, CUT+J ) = WORK( U11+I, J )
               END DO
            END DO
*
            IF( (CUT+NNB).LT.N ) THEN
*
*              L21**H * invD2*L21 -> A( CUT+I, CUT+J )
*
               CALL ZGEMM( 'C', 'N', NNB, NNB, N-NNB-CUT, CONE,
     $                     A( CUT+NNB+1, CUT+1 ), LDA, WORK, N+NB+1,
     $                     CZERO, WORK( U11+1, 1 ), N+NB+1 )

*
*              L11 =  L11**H * invD1 * L11 + U01**H * invD * U01
*
               DO I = 1, NNB
                  DO J = 1, I
                     A( CUT+I, CUT+J ) = A( CUT+I, CUT+J )+WORK(U11+I,J)
                  END DO
               END DO
*
*              L01 =  L22**H * invD2 * L21
*
               CALL ZTRMM( 'L', UPLO, 'C', 'U', N-NNB-CUT, NNB, CONE,
     $                     A( CUT+NNB+1, CUT+NNB+1 ), LDA, WORK,
     $                     N+NB+1 )
*
*              Update L21
*
               DO I = 1, N-CUT-NNB
                  DO J = 1, NNB
                     A( CUT+NNB+I, CUT+J ) = WORK( I, J )
                  END DO
               END DO
*
            ELSE
*
*              L11 =  L11**H * invD1 * L11
*
               DO I = 1, NNB
                  DO J = 1, I
                     A( CUT+I, CUT+J ) = WORK( U11+I, J )
                  END DO
               END DO
            END IF
*
*           Next Block
*
            CUT = CUT + NNB
*
         END DO
*
*        Apply PERMUTATIONS P and P**T:
*        P * inv(L**H) * inv(D) * inv(L) * P**T.
*        Interchange rows and columns I and IPIV(I) in reverse order
*        from the formation order of IPIV vector for Lower case.
*
*        ( We can use a loop over IPIV with increment -1,
*        since the ABS value of IPIV(I) represents the row (column)
*        index of the interchange with row (column) i in both 1x1
*        and 2x2 pivot cases, i.e. we don't need separate code branches
*        for 1x1 and 2x2 pivot cases )
*
         DO I = N, 1, -1
             IP = ABS( IPIV( I ) )
             IF( IP.NE.I ) THEN
                IF (I .LT. IP) CALL ZHESWAPR( UPLO, N, A, LDA, I ,IP )
                IF (I .GT. IP) CALL ZHESWAPR( UPLO, N, A, LDA, IP ,I )
             END IF
         END DO
*
      END IF
*
      RETURN
*
*     End of ZHETRI_3X
*
      END
