*> \brief \b ZHETRI_ROOK computes the inverse of HE matrix using the factorization obtained with the bounded Bunch-Kaufman ("rook") diagonal pivoting method.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZHETRI_ROOK + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zhetri_rook.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zhetri_rook.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zhetri_rook.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZHETRI_ROOK( UPLO, N, A, LDA, IPIV, WORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            INFO, LDA, N
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * )
*       COMPLEX*16         A( LDA, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZHETRI_ROOK computes the inverse of a complex Hermitian indefinite matrix
*> A using the factorization A = U*D*U**H or A = L*D*L**H computed by
*> ZHETRF_ROOK.
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
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>          On entry, the block diagonal matrix D and the multipliers
*>          used to obtain the factor U or L as computed by ZHETRF_ROOK.
*>
*>          On exit, if INFO = 0, the (Hermitian) inverse of the original
*>          matrix.  If UPLO = 'U', the upper triangular part of the
*>          inverse is formed and the part of A below the diagonal is not
*>          referenced; if UPLO = 'L' the lower triangular part of the
*>          inverse is formed and the part of A above the diagonal is
*>          not referenced.
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
*>          as determined by ZHETRF_ROOK.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (N)
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
*> \date November 2013
*
*> \ingroup complex16HEcomputational
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
*> \endverbatim
*
*  =====================================================================
      SUBROUTINE ZHETRI_ROOK( UPLO, N, A, LDA, IPIV, WORK, INFO )
*
*  -- LAPACK computational routine (version 3.5.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2013
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            INFO, LDA, N
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * )
      COMPLEX*16         A( LDA, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ONE
      COMPLEX*16         CONE, CZERO
      PARAMETER          ( ONE = 1.0D+0, CONE = ( 1.0D+0, 0.0D+0 ),
     $                   CZERO = ( 0.0D+0, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            UPPER
      INTEGER            J, K, KP, KSTEP
      DOUBLE PRECISION   AK, AKP1, D, T
      COMPLEX*16         AKKP1, TEMP
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      COMPLEX*16         ZDOTC
      EXTERNAL           LSAME, ZDOTC
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZCOPY, ZHEMV, ZSWAP, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DCONJG, MAX, DBLE
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
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZHETRI_ROOK', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 )
     $   RETURN
*
*     Check that the diagonal matrix D is nonsingular.
*
      IF( UPPER ) THEN
*
*        Upper triangular storage: examine D from bottom to top
*
         DO 10 INFO = N, 1, -1
            IF( IPIV( INFO ).GT.0 .AND. A( INFO, INFO ).EQ.CZERO )
     $         RETURN
   10    CONTINUE
      ELSE
*
*        Lower triangular storage: examine D from top to bottom.
*
         DO 20 INFO = 1, N
            IF( IPIV( INFO ).GT.0 .AND. A( INFO, INFO ).EQ.CZERO )
     $         RETURN
   20    CONTINUE
      END IF
      INFO = 0
*
      IF( UPPER ) THEN
*
*        Compute inv(A) from the factorization A = U*D*U**H.
*
*        K is the main loop index, increasing from 1 to N in steps of
*        1 or 2, depending on the size of the diagonal blocks.
*
         K = 1
   30    CONTINUE
*
*        If K > N, exit from loop.
*
         IF( K.GT.N )
     $      GO TO 70
*
         IF( IPIV( K ).GT.0 ) THEN
*
*           1 x 1 diagonal block
*
*           Invert the diagonal block.
*
            A( K, K ) = ONE / DBLE( A( K, K ) )
*
*           Compute column K of the inverse.
*
            IF( K.GT.1 ) THEN
               CALL ZCOPY( K-1, A( 1, K ), 1, WORK, 1 )
               CALL ZHEMV( UPLO, K-1, -CONE, A, LDA, WORK, 1, CZERO,
     $                     A( 1, K ), 1 )
               A( K, K ) = A( K, K ) - DBLE( ZDOTC( K-1, WORK, 1, A( 1,
     $                     K ), 1 ) )
            END IF
            KSTEP = 1
         ELSE
*
*           2 x 2 diagonal block
*
*           Invert the diagonal block.
*
            T = ABS( A( K, K+1 ) )
            AK = DBLE( A( K, K ) ) / T
            AKP1 = DBLE( A( K+1, K+1 ) ) / T
            AKKP1 = A( K, K+1 ) / T
            D = T*( AK*AKP1-ONE )
            A( K, K ) = AKP1 / D
            A( K+1, K+1 ) = AK / D
            A( K, K+1 ) = -AKKP1 / D
*
*           Compute columns K and K+1 of the inverse.
*
            IF( K.GT.1 ) THEN
               CALL ZCOPY( K-1, A( 1, K ), 1, WORK, 1 )
               CALL ZHEMV( UPLO, K-1, -CONE, A, LDA, WORK, 1, CZERO,
     $                     A( 1, K ), 1 )
               A( K, K ) = A( K, K ) - DBLE( ZDOTC( K-1, WORK, 1, A( 1,
     $                     K ), 1 ) )
               A( K, K+1 ) = A( K, K+1 ) -
     $                       ZDOTC( K-1, A( 1, K ), 1, A( 1, K+1 ), 1 )
               CALL ZCOPY( K-1, A( 1, K+1 ), 1, WORK, 1 )
               CALL ZHEMV( UPLO, K-1, -CONE, A, LDA, WORK, 1, CZERO,
     $                     A( 1, K+1 ), 1 )
               A( K+1, K+1 ) = A( K+1, K+1 ) -
     $                         DBLE( ZDOTC( K-1, WORK, 1, A( 1, K+1 ),
     $                         1 ) )
            END IF
            KSTEP = 2
         END IF
*
         IF( KSTEP.EQ.1 ) THEN
*
*           Interchange rows and columns K and IPIV(K) in the leading
*           submatrix A(1:k,1:k)
*
            KP = IPIV( K )
            IF( KP.NE.K ) THEN
*
               IF( KP.GT.1 )
     $            CALL ZSWAP( KP-1, A( 1, K ), 1, A( 1, KP ), 1 )
*
               DO 40 J = KP + 1, K - 1
                  TEMP = DCONJG( A( J, K ) )
                  A( J, K ) = DCONJG( A( KP, J ) )
                  A( KP, J ) = TEMP
   40          CONTINUE
*
               A( KP, K ) = DCONJG( A( KP, K ) )
*
               TEMP = A( K, K )
               A( K, K ) = A( KP, KP )
               A( KP, KP ) = TEMP
            END IF
         ELSE
*
*           Interchange rows and columns K and K+1 with -IPIV(K) and
*           -IPIV(K+1) in the leading submatrix A(k+1:n,k+1:n)
*
*           (1) Interchange rows and columns K and -IPIV(K)
*
            KP = -IPIV( K )
            IF( KP.NE.K ) THEN
*
               IF( KP.GT.1 )
     $            CALL ZSWAP( KP-1, A( 1, K ), 1, A( 1, KP ), 1 )
*
               DO 50 J = KP + 1, K - 1
                  TEMP = DCONJG( A( J, K ) )
                  A( J, K ) = DCONJG( A( KP, J ) )
                  A( KP, J ) = TEMP
   50          CONTINUE
*
               A( KP, K ) = DCONJG( A( KP, K ) )
*
               TEMP = A( K, K )
               A( K, K ) = A( KP, KP )
               A( KP, KP ) = TEMP
*
               TEMP = A( K, K+1 )
               A( K, K+1 ) = A( KP, K+1 )
               A( KP, K+1 ) = TEMP
            END IF
*
*           (2) Interchange rows and columns K+1 and -IPIV(K+1)
*
            K = K + 1
            KP = -IPIV( K )
            IF( KP.NE.K ) THEN
*
               IF( KP.GT.1 )
     $            CALL ZSWAP( KP-1, A( 1, K ), 1, A( 1, KP ), 1 )
*
               DO 60 J = KP + 1, K - 1
                  TEMP = DCONJG( A( J, K ) )
                  A( J, K ) = DCONJG( A( KP, J ) )
                  A( KP, J ) = TEMP
   60          CONTINUE
*
               A( KP, K ) = DCONJG( A( KP, K ) )
*
               TEMP = A( K, K )
               A( K, K ) = A( KP, KP )
               A( KP, KP ) = TEMP
            END IF
         END IF
*
         K = K + 1
         GO TO 30
   70    CONTINUE
*
      ELSE
*
*        Compute inv(A) from the factorization A = L*D*L**H.
*
*        K is the main loop index, decreasing from N to 1 in steps of
*        1 or 2, depending on the size of the diagonal blocks.
*
         K = N
   80    CONTINUE
*
*        If K < 1, exit from loop.
*
         IF( K.LT.1 )
     $      GO TO 120
*
         IF( IPIV( K ).GT.0 ) THEN
*
*           1 x 1 diagonal block
*
*           Invert the diagonal block.
*
            A( K, K ) = ONE / DBLE( A( K, K ) )
*
*           Compute column K of the inverse.
*
            IF( K.LT.N ) THEN
               CALL ZCOPY( N-K, A( K+1, K ), 1, WORK, 1 )
               CALL ZHEMV( UPLO, N-K, -CONE, A( K+1, K+1 ), LDA, WORK,
     $                     1, CZERO, A( K+1, K ), 1 )
               A( K, K ) = A( K, K ) - DBLE( ZDOTC( N-K, WORK, 1,
     $                     A( K+1, K ), 1 ) )
            END IF
            KSTEP = 1
         ELSE
*
*           2 x 2 diagonal block
*
*           Invert the diagonal block.
*
            T = ABS( A( K, K-1 ) )
            AK = DBLE( A( K-1, K-1 ) ) / T
            AKP1 = DBLE( A( K, K ) ) / T
            AKKP1 = A( K, K-1 ) / T
            D = T*( AK*AKP1-ONE )
            A( K-1, K-1 ) = AKP1 / D
            A( K, K ) = AK / D
            A( K, K-1 ) = -AKKP1 / D
*
*           Compute columns K-1 and K of the inverse.
*
            IF( K.LT.N ) THEN
               CALL ZCOPY( N-K, A( K+1, K ), 1, WORK, 1 )
               CALL ZHEMV( UPLO, N-K, -CONE, A( K+1, K+1 ), LDA, WORK,
     $                     1, CZERO, A( K+1, K ), 1 )
               A( K, K ) = A( K, K ) - DBLE( ZDOTC( N-K, WORK, 1,
     $                     A( K+1, K ), 1 ) )
               A( K, K-1 ) = A( K, K-1 ) -
     $                       ZDOTC( N-K, A( K+1, K ), 1, A( K+1, K-1 ),
     $                       1 )
               CALL ZCOPY( N-K, A( K+1, K-1 ), 1, WORK, 1 )
               CALL ZHEMV( UPLO, N-K, -CONE, A( K+1, K+1 ), LDA, WORK,
     $                     1, CZERO, A( K+1, K-1 ), 1 )
               A( K-1, K-1 ) = A( K-1, K-1 ) -
     $                         DBLE( ZDOTC( N-K, WORK, 1, A( K+1, K-1 ),
     $                         1 ) )
            END IF
            KSTEP = 2
         END IF
*
         IF( KSTEP.EQ.1 ) THEN
*
*           Interchange rows and columns K and IPIV(K) in the trailing
*           submatrix A(k:n,k:n)
*
            KP = IPIV( K )
            IF( KP.NE.K ) THEN
*
               IF( KP.LT.N )
     $            CALL ZSWAP( N-KP, A( KP+1, K ), 1, A( KP+1, KP ), 1 )
*
               DO 90 J = K + 1, KP - 1
                  TEMP = DCONJG( A( J, K ) )
                  A( J, K ) = DCONJG( A( KP, J ) )
                  A( KP, J ) = TEMP
   90          CONTINUE
*
               A( KP, K ) = DCONJG( A( KP, K ) )
*
               TEMP = A( K, K )
               A( K, K ) = A( KP, KP )
               A( KP, KP ) = TEMP
            END IF
         ELSE
*
*           Interchange rows and columns K and K-1 with -IPIV(K) and
*           -IPIV(K-1) in the trailing submatrix A(k-1:n,k-1:n)
*
*           (1) Interchange rows and columns K and -IPIV(K)
*
            KP = -IPIV( K )
            IF( KP.NE.K ) THEN
*
               IF( KP.LT.N )
     $            CALL ZSWAP( N-KP, A( KP+1, K ), 1, A( KP+1, KP ), 1 )
*
               DO 100 J = K + 1, KP - 1
                  TEMP = DCONJG( A( J, K ) )
                  A( J, K ) = DCONJG( A( KP, J ) )
                  A( KP, J ) = TEMP
  100         CONTINUE
*
               A( KP, K ) = DCONJG( A( KP, K ) )
*
               TEMP = A( K, K )
               A( K, K ) = A( KP, KP )
               A( KP, KP ) = TEMP
*
               TEMP = A( K, K-1 )
               A( K, K-1 ) = A( KP, K-1 )
               A( KP, K-1 ) = TEMP
            END IF
*
*           (2) Interchange rows and columns K-1 and -IPIV(K-1)
*
            K = K - 1
            KP = -IPIV( K )
            IF( KP.NE.K ) THEN
*
               IF( KP.LT.N )
     $            CALL ZSWAP( N-KP, A( KP+1, K ), 1, A( KP+1, KP ), 1 )
*
               DO 110 J = K + 1, KP - 1
                  TEMP = DCONJG( A( J, K ) )
                  A( J, K ) = DCONJG( A( KP, J ) )
                  A( KP, J ) = TEMP
  110         CONTINUE
*
               A( KP, K ) = DCONJG( A( KP, K ) )
*
               TEMP = A( K, K )
               A( K, K ) = A( KP, KP )
               A( KP, KP ) = TEMP
            END IF
         END IF
*
         K = K - 1
         GO TO 80
  120    CONTINUE
      END IF
*
      RETURN
*
*     End of ZHETRI_ROOK
*
      END
