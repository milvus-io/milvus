*> \brief \b ZHBGST
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZHBGST + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zhbgst.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zhbgst.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zhbgst.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZHBGST( VECT, UPLO, N, KA, KB, AB, LDAB, BB, LDBB, X,
*                          LDX, WORK, RWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO, VECT
*       INTEGER            INFO, KA, KB, LDAB, LDBB, LDX, N
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   RWORK( * )
*       COMPLEX*16         AB( LDAB, * ), BB( LDBB, * ), WORK( * ),
*      $                   X( LDX, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZHBGST reduces a complex Hermitian-definite banded generalized
*> eigenproblem  A*x = lambda*B*x  to standard form  C*y = lambda*y,
*> such that C has the same bandwidth as A.
*>
*> B must have been previously factorized as S**H*S by ZPBSTF, using a
*> split Cholesky factorization. A is overwritten by C = X**H*A*X, where
*> X = S**(-1)*Q and Q is a unitary matrix chosen to preserve the
*> bandwidth of A.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] VECT
*> \verbatim
*>          VECT is CHARACTER*1
*>          = 'N':  do not form the transformation matrix X;
*>          = 'V':  form X.
*> \endverbatim
*>
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          = 'U':  Upper triangle of A is stored;
*>          = 'L':  Lower triangle of A is stored.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrices A and B.  N >= 0.
*> \endverbatim
*>
*> \param[in] KA
*> \verbatim
*>          KA is INTEGER
*>          The number of superdiagonals of the matrix A if UPLO = 'U',
*>          or the number of subdiagonals if UPLO = 'L'.  KA >= 0.
*> \endverbatim
*>
*> \param[in] KB
*> \verbatim
*>          KB is INTEGER
*>          The number of superdiagonals of the matrix B if UPLO = 'U',
*>          or the number of subdiagonals if UPLO = 'L'.  KA >= KB >= 0.
*> \endverbatim
*>
*> \param[in,out] AB
*> \verbatim
*>          AB is COMPLEX*16 array, dimension (LDAB,N)
*>          On entry, the upper or lower triangle of the Hermitian band
*>          matrix A, stored in the first ka+1 rows of the array.  The
*>          j-th column of A is stored in the j-th column of the array AB
*>          as follows:
*>          if UPLO = 'U', AB(ka+1+i-j,j) = A(i,j) for max(1,j-ka)<=i<=j;
*>          if UPLO = 'L', AB(1+i-j,j)    = A(i,j) for j<=i<=min(n,j+ka).
*>
*>          On exit, the transformed matrix X**H*A*X, stored in the same
*>          format as A.
*> \endverbatim
*>
*> \param[in] LDAB
*> \verbatim
*>          LDAB is INTEGER
*>          The leading dimension of the array AB.  LDAB >= KA+1.
*> \endverbatim
*>
*> \param[in] BB
*> \verbatim
*>          BB is COMPLEX*16 array, dimension (LDBB,N)
*>          The banded factor S from the split Cholesky factorization of
*>          B, as returned by ZPBSTF, stored in the first kb+1 rows of
*>          the array.
*> \endverbatim
*>
*> \param[in] LDBB
*> \verbatim
*>          LDBB is INTEGER
*>          The leading dimension of the array BB.  LDBB >= KB+1.
*> \endverbatim
*>
*> \param[out] X
*> \verbatim
*>          X is COMPLEX*16 array, dimension (LDX,N)
*>          If VECT = 'V', the n-by-n matrix X.
*>          If VECT = 'N', the array X is not referenced.
*> \endverbatim
*>
*> \param[in] LDX
*> \verbatim
*>          LDX is INTEGER
*>          The leading dimension of the array X.
*>          LDX >= max(1,N) if VECT = 'V'; LDX >= 1 otherwise.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (N)
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
*> \ingroup complex16OTHERcomputational
*
*  =====================================================================
      SUBROUTINE ZHBGST( VECT, UPLO, N, KA, KB, AB, LDAB, BB, LDBB, X,
     $                   LDX, WORK, RWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO, VECT
      INTEGER            INFO, KA, KB, LDAB, LDBB, LDX, N
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   RWORK( * )
      COMPLEX*16         AB( LDAB, * ), BB( LDBB, * ), WORK( * ),
     $                   X( LDX, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX*16         CZERO, CONE
      DOUBLE PRECISION   ONE
      PARAMETER          ( CZERO = ( 0.0D+0, 0.0D+0 ),
     $                   CONE = ( 1.0D+0, 0.0D+0 ), ONE = 1.0D+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            UPDATE, UPPER, WANTX
      INTEGER            I, I0, I1, I2, INCA, J, J1, J1T, J2, J2T, K,
     $                   KA1, KB1, KBT, L, M, NR, NRT, NX
      DOUBLE PRECISION   BII
      COMPLEX*16         RA, RA1, T
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA, ZDSCAL, ZGERC, ZGERU, ZLACGV, ZLAR2V,
     $                   ZLARGV, ZLARTG, ZLARTV, ZLASET, ZROT
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE, DCONJG, MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters
*
      WANTX = LSAME( VECT, 'V' )
      UPPER = LSAME( UPLO, 'U' )
      KA1 = KA + 1
      KB1 = KB + 1
      INFO = 0
      IF( .NOT.WANTX .AND. .NOT.LSAME( VECT, 'N' ) ) THEN
         INFO = -1
      ELSE IF( .NOT.UPPER .AND. .NOT.LSAME( UPLO, 'L' ) ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( KA.LT.0 ) THEN
         INFO = -4
      ELSE IF( KB.LT.0 .OR. KB.GT.KA ) THEN
         INFO = -5
      ELSE IF( LDAB.LT.KA+1 ) THEN
         INFO = -7
      ELSE IF( LDBB.LT.KB+1 ) THEN
         INFO = -9
      ELSE IF( LDX.LT.1 .OR. WANTX .AND. LDX.LT.MAX( 1, N ) ) THEN
         INFO = -11
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZHBGST', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 )
     $   RETURN
*
      INCA = LDAB*KA1
*
*     Initialize X to the unit matrix, if needed
*
      IF( WANTX )
     $   CALL ZLASET( 'Full', N, N, CZERO, CONE, X, LDX )
*
*     Set M to the splitting point m. It must be the same value as is
*     used in ZPBSTF. The chosen value allows the arrays WORK and RWORK
*     to be of dimension (N).
*
      M = ( N+KB ) / 2
*
*     The routine works in two phases, corresponding to the two halves
*     of the split Cholesky factorization of B as S**H*S where
*
*     S = ( U    )
*         ( M  L )
*
*     with U upper triangular of order m, and L lower triangular of
*     order n-m. S has the same bandwidth as B.
*
*     S is treated as a product of elementary matrices:
*
*     S = S(m)*S(m-1)*...*S(2)*S(1)*S(m+1)*S(m+2)*...*S(n-1)*S(n)
*
*     where S(i) is determined by the i-th row of S.
*
*     In phase 1, the index i takes the values n, n-1, ... , m+1;
*     in phase 2, it takes the values 1, 2, ... , m.
*
*     For each value of i, the current matrix A is updated by forming
*     inv(S(i))**H*A*inv(S(i)). This creates a triangular bulge outside
*     the band of A. The bulge is then pushed down toward the bottom of
*     A in phase 1, and up toward the top of A in phase 2, by applying
*     plane rotations.
*
*     There are kb*(kb+1)/2 elements in the bulge, but at most 2*kb-1
*     of them are linearly independent, so annihilating a bulge requires
*     only 2*kb-1 plane rotations. The rotations are divided into a 1st
*     set of kb-1 rotations, and a 2nd set of kb rotations.
*
*     Wherever possible, rotations are generated and applied in vector
*     operations of length NR between the indices J1 and J2 (sometimes
*     replaced by modified values NRT, J1T or J2T).
*
*     The real cosines and complex sines of the rotations are stored in
*     the arrays RWORK and WORK, those of the 1st set in elements
*     2:m-kb-1, and those of the 2nd set in elements m-kb+1:n.
*
*     The bulges are not formed explicitly; nonzero elements outside the
*     band are created only when they are required for generating new
*     rotations; they are stored in the array WORK, in positions where
*     they are later overwritten by the sines of the rotations which
*     annihilate them.
*
*     **************************** Phase 1 *****************************
*
*     The logical structure of this phase is:
*
*     UPDATE = .TRUE.
*     DO I = N, M + 1, -1
*        use S(i) to update A and create a new bulge
*        apply rotations to push all bulges KA positions downward
*     END DO
*     UPDATE = .FALSE.
*     DO I = M + KA + 1, N - 1
*        apply rotations to push all bulges KA positions downward
*     END DO
*
*     To avoid duplicating code, the two loops are merged.
*
      UPDATE = .TRUE.
      I = N + 1
   10 CONTINUE
      IF( UPDATE ) THEN
         I = I - 1
         KBT = MIN( KB, I-1 )
         I0 = I - 1
         I1 = MIN( N, I+KA )
         I2 = I - KBT + KA1
         IF( I.LT.M+1 ) THEN
            UPDATE = .FALSE.
            I = I + 1
            I0 = M
            IF( KA.EQ.0 )
     $         GO TO 480
            GO TO 10
         END IF
      ELSE
         I = I + KA
         IF( I.GT.N-1 )
     $      GO TO 480
      END IF
*
      IF( UPPER ) THEN
*
*        Transform A, working with the upper triangle
*
         IF( UPDATE ) THEN
*
*           Form  inv(S(i))**H * A * inv(S(i))
*
            BII = DBLE( BB( KB1, I ) )
            AB( KA1, I ) = ( DBLE( AB( KA1, I ) ) / BII ) / BII
            DO 20 J = I + 1, I1
               AB( I-J+KA1, J ) = AB( I-J+KA1, J ) / BII
   20       CONTINUE
            DO 30 J = MAX( 1, I-KA ), I - 1
               AB( J-I+KA1, I ) = AB( J-I+KA1, I ) / BII
   30       CONTINUE
            DO 60 K = I - KBT, I - 1
               DO 40 J = I - KBT, K
                  AB( J-K+KA1, K ) = AB( J-K+KA1, K ) -
     $                               BB( J-I+KB1, I )*
     $                               DCONJG( AB( K-I+KA1, I ) ) -
     $                               DCONJG( BB( K-I+KB1, I ) )*
     $                               AB( J-I+KA1, I ) +
     $                               DBLE( AB( KA1, I ) )*
     $                               BB( J-I+KB1, I )*
     $                               DCONJG( BB( K-I+KB1, I ) )
   40          CONTINUE
               DO 50 J = MAX( 1, I-KA ), I - KBT - 1
                  AB( J-K+KA1, K ) = AB( J-K+KA1, K ) -
     $                               DCONJG( BB( K-I+KB1, I ) )*
     $                               AB( J-I+KA1, I )
   50          CONTINUE
   60       CONTINUE
            DO 80 J = I, I1
               DO 70 K = MAX( J-KA, I-KBT ), I - 1
                  AB( K-J+KA1, J ) = AB( K-J+KA1, J ) -
     $                               BB( K-I+KB1, I )*AB( I-J+KA1, J )
   70          CONTINUE
   80       CONTINUE
*
            IF( WANTX ) THEN
*
*              post-multiply X by inv(S(i))
*
               CALL ZDSCAL( N-M, ONE / BII, X( M+1, I ), 1 )
               IF( KBT.GT.0 )
     $            CALL ZGERC( N-M, KBT, -CONE, X( M+1, I ), 1,
     $                        BB( KB1-KBT, I ), 1, X( M+1, I-KBT ),
     $                        LDX )
            END IF
*
*           store a(i,i1) in RA1 for use in next loop over K
*
            RA1 = AB( I-I1+KA1, I1 )
         END IF
*
*        Generate and apply vectors of rotations to chase all the
*        existing bulges KA positions down toward the bottom of the
*        band
*
         DO 130 K = 1, KB - 1
            IF( UPDATE ) THEN
*
*              Determine the rotations which would annihilate the bulge
*              which has in theory just been created
*
               IF( I-K+KA.LT.N .AND. I-K.GT.1 ) THEN
*
*                 generate rotation to annihilate a(i,i-k+ka+1)
*
                  CALL ZLARTG( AB( K+1, I-K+KA ), RA1,
     $                         RWORK( I-K+KA-M ), WORK( I-K+KA-M ), RA )
*
*                 create nonzero element a(i-k,i-k+ka+1) outside the
*                 band and store it in WORK(i-k)
*
                  T = -BB( KB1-K, I )*RA1
                  WORK( I-K ) = RWORK( I-K+KA-M )*T -
     $                          DCONJG( WORK( I-K+KA-M ) )*
     $                          AB( 1, I-K+KA )
                  AB( 1, I-K+KA ) = WORK( I-K+KA-M )*T +
     $                              RWORK( I-K+KA-M )*AB( 1, I-K+KA )
                  RA1 = RA
               END IF
            END IF
            J2 = I - K - 1 + MAX( 1, K-I0+2 )*KA1
            NR = ( N-J2+KA ) / KA1
            J1 = J2 + ( NR-1 )*KA1
            IF( UPDATE ) THEN
               J2T = MAX( J2, I+2*KA-K+1 )
            ELSE
               J2T = J2
            END IF
            NRT = ( N-J2T+KA ) / KA1
            DO 90 J = J2T, J1, KA1
*
*              create nonzero element a(j-ka,j+1) outside the band
*              and store it in WORK(j-m)
*
               WORK( J-M ) = WORK( J-M )*AB( 1, J+1 )
               AB( 1, J+1 ) = RWORK( J-M )*AB( 1, J+1 )
   90       CONTINUE
*
*           generate rotations in 1st set to annihilate elements which
*           have been created outside the band
*
            IF( NRT.GT.0 )
     $         CALL ZLARGV( NRT, AB( 1, J2T ), INCA, WORK( J2T-M ), KA1,
     $                      RWORK( J2T-M ), KA1 )
            IF( NR.GT.0 ) THEN
*
*              apply rotations in 1st set from the right
*
               DO 100 L = 1, KA - 1
                  CALL ZLARTV( NR, AB( KA1-L, J2 ), INCA,
     $                         AB( KA-L, J2+1 ), INCA, RWORK( J2-M ),
     $                         WORK( J2-M ), KA1 )
  100          CONTINUE
*
*              apply rotations in 1st set from both sides to diagonal
*              blocks
*
               CALL ZLAR2V( NR, AB( KA1, J2 ), AB( KA1, J2+1 ),
     $                      AB( KA, J2+1 ), INCA, RWORK( J2-M ),
     $                      WORK( J2-M ), KA1 )
*
               CALL ZLACGV( NR, WORK( J2-M ), KA1 )
            END IF
*
*           start applying rotations in 1st set from the left
*
            DO 110 L = KA - 1, KB - K + 1, -1
               NRT = ( N-J2+L ) / KA1
               IF( NRT.GT.0 )
     $            CALL ZLARTV( NRT, AB( L, J2+KA1-L ), INCA,
     $                         AB( L+1, J2+KA1-L ), INCA, RWORK( J2-M ),
     $                         WORK( J2-M ), KA1 )
  110       CONTINUE
*
            IF( WANTX ) THEN
*
*              post-multiply X by product of rotations in 1st set
*
               DO 120 J = J2, J1, KA1
                  CALL ZROT( N-M, X( M+1, J ), 1, X( M+1, J+1 ), 1,
     $                       RWORK( J-M ), DCONJG( WORK( J-M ) ) )
  120          CONTINUE
            END IF
  130    CONTINUE
*
         IF( UPDATE ) THEN
            IF( I2.LE.N .AND. KBT.GT.0 ) THEN
*
*              create nonzero element a(i-kbt,i-kbt+ka+1) outside the
*              band and store it in WORK(i-kbt)
*
               WORK( I-KBT ) = -BB( KB1-KBT, I )*RA1
            END IF
         END IF
*
         DO 170 K = KB, 1, -1
            IF( UPDATE ) THEN
               J2 = I - K - 1 + MAX( 2, K-I0+1 )*KA1
            ELSE
               J2 = I - K - 1 + MAX( 1, K-I0+1 )*KA1
            END IF
*
*           finish applying rotations in 2nd set from the left
*
            DO 140 L = KB - K, 1, -1
               NRT = ( N-J2+KA+L ) / KA1
               IF( NRT.GT.0 )
     $            CALL ZLARTV( NRT, AB( L, J2-L+1 ), INCA,
     $                         AB( L+1, J2-L+1 ), INCA, RWORK( J2-KA ),
     $                         WORK( J2-KA ), KA1 )
  140       CONTINUE
            NR = ( N-J2+KA ) / KA1
            J1 = J2 + ( NR-1 )*KA1
            DO 150 J = J1, J2, -KA1
               WORK( J ) = WORK( J-KA )
               RWORK( J ) = RWORK( J-KA )
  150       CONTINUE
            DO 160 J = J2, J1, KA1
*
*              create nonzero element a(j-ka,j+1) outside the band
*              and store it in WORK(j)
*
               WORK( J ) = WORK( J )*AB( 1, J+1 )
               AB( 1, J+1 ) = RWORK( J )*AB( 1, J+1 )
  160       CONTINUE
            IF( UPDATE ) THEN
               IF( I-K.LT.N-KA .AND. K.LE.KBT )
     $            WORK( I-K+KA ) = WORK( I-K )
            END IF
  170    CONTINUE
*
         DO 210 K = KB, 1, -1
            J2 = I - K - 1 + MAX( 1, K-I0+1 )*KA1
            NR = ( N-J2+KA ) / KA1
            J1 = J2 + ( NR-1 )*KA1
            IF( NR.GT.0 ) THEN
*
*              generate rotations in 2nd set to annihilate elements
*              which have been created outside the band
*
               CALL ZLARGV( NR, AB( 1, J2 ), INCA, WORK( J2 ), KA1,
     $                      RWORK( J2 ), KA1 )
*
*              apply rotations in 2nd set from the right
*
               DO 180 L = 1, KA - 1
                  CALL ZLARTV( NR, AB( KA1-L, J2 ), INCA,
     $                         AB( KA-L, J2+1 ), INCA, RWORK( J2 ),
     $                         WORK( J2 ), KA1 )
  180          CONTINUE
*
*              apply rotations in 2nd set from both sides to diagonal
*              blocks
*
               CALL ZLAR2V( NR, AB( KA1, J2 ), AB( KA1, J2+1 ),
     $                      AB( KA, J2+1 ), INCA, RWORK( J2 ),
     $                      WORK( J2 ), KA1 )
*
               CALL ZLACGV( NR, WORK( J2 ), KA1 )
            END IF
*
*           start applying rotations in 2nd set from the left
*
            DO 190 L = KA - 1, KB - K + 1, -1
               NRT = ( N-J2+L ) / KA1
               IF( NRT.GT.0 )
     $            CALL ZLARTV( NRT, AB( L, J2+KA1-L ), INCA,
     $                         AB( L+1, J2+KA1-L ), INCA, RWORK( J2 ),
     $                         WORK( J2 ), KA1 )
  190       CONTINUE
*
            IF( WANTX ) THEN
*
*              post-multiply X by product of rotations in 2nd set
*
               DO 200 J = J2, J1, KA1
                  CALL ZROT( N-M, X( M+1, J ), 1, X( M+1, J+1 ), 1,
     $                       RWORK( J ), DCONJG( WORK( J ) ) )
  200          CONTINUE
            END IF
  210    CONTINUE
*
         DO 230 K = 1, KB - 1
            J2 = I - K - 1 + MAX( 1, K-I0+2 )*KA1
*
*           finish applying rotations in 1st set from the left
*
            DO 220 L = KB - K, 1, -1
               NRT = ( N-J2+L ) / KA1
               IF( NRT.GT.0 )
     $            CALL ZLARTV( NRT, AB( L, J2+KA1-L ), INCA,
     $                         AB( L+1, J2+KA1-L ), INCA, RWORK( J2-M ),
     $                         WORK( J2-M ), KA1 )
  220       CONTINUE
  230    CONTINUE
*
         IF( KB.GT.1 ) THEN
            DO 240 J = N - 1, J2 + KA, -1
               RWORK( J-M ) = RWORK( J-KA-M )
               WORK( J-M ) = WORK( J-KA-M )
  240       CONTINUE
         END IF
*
      ELSE
*
*        Transform A, working with the lower triangle
*
         IF( UPDATE ) THEN
*
*           Form  inv(S(i))**H * A * inv(S(i))
*
            BII = DBLE( BB( 1, I ) )
            AB( 1, I ) = ( DBLE( AB( 1, I ) ) / BII ) / BII
            DO 250 J = I + 1, I1
               AB( J-I+1, I ) = AB( J-I+1, I ) / BII
  250       CONTINUE
            DO 260 J = MAX( 1, I-KA ), I - 1
               AB( I-J+1, J ) = AB( I-J+1, J ) / BII
  260       CONTINUE
            DO 290 K = I - KBT, I - 1
               DO 270 J = I - KBT, K
                  AB( K-J+1, J ) = AB( K-J+1, J ) -
     $                             BB( I-J+1, J )*DCONJG( AB( I-K+1,
     $                             K ) ) - DCONJG( BB( I-K+1, K ) )*
     $                             AB( I-J+1, J ) + DBLE( AB( 1, I ) )*
     $                             BB( I-J+1, J )*DCONJG( BB( I-K+1,
     $                             K ) )
  270          CONTINUE
               DO 280 J = MAX( 1, I-KA ), I - KBT - 1
                  AB( K-J+1, J ) = AB( K-J+1, J ) -
     $                             DCONJG( BB( I-K+1, K ) )*
     $                             AB( I-J+1, J )
  280          CONTINUE
  290       CONTINUE
            DO 310 J = I, I1
               DO 300 K = MAX( J-KA, I-KBT ), I - 1
                  AB( J-K+1, K ) = AB( J-K+1, K ) -
     $                             BB( I-K+1, K )*AB( J-I+1, I )
  300          CONTINUE
  310       CONTINUE
*
            IF( WANTX ) THEN
*
*              post-multiply X by inv(S(i))
*
               CALL ZDSCAL( N-M, ONE / BII, X( M+1, I ), 1 )
               IF( KBT.GT.0 )
     $            CALL ZGERU( N-M, KBT, -CONE, X( M+1, I ), 1,
     $                        BB( KBT+1, I-KBT ), LDBB-1,
     $                        X( M+1, I-KBT ), LDX )
            END IF
*
*           store a(i1,i) in RA1 for use in next loop over K
*
            RA1 = AB( I1-I+1, I )
         END IF
*
*        Generate and apply vectors of rotations to chase all the
*        existing bulges KA positions down toward the bottom of the
*        band
*
         DO 360 K = 1, KB - 1
            IF( UPDATE ) THEN
*
*              Determine the rotations which would annihilate the bulge
*              which has in theory just been created
*
               IF( I-K+KA.LT.N .AND. I-K.GT.1 ) THEN
*
*                 generate rotation to annihilate a(i-k+ka+1,i)
*
                  CALL ZLARTG( AB( KA1-K, I ), RA1, RWORK( I-K+KA-M ),
     $                         WORK( I-K+KA-M ), RA )
*
*                 create nonzero element a(i-k+ka+1,i-k) outside the
*                 band and store it in WORK(i-k)
*
                  T = -BB( K+1, I-K )*RA1
                  WORK( I-K ) = RWORK( I-K+KA-M )*T -
     $                          DCONJG( WORK( I-K+KA-M ) )*
     $                          AB( KA1, I-K )
                  AB( KA1, I-K ) = WORK( I-K+KA-M )*T +
     $                             RWORK( I-K+KA-M )*AB( KA1, I-K )
                  RA1 = RA
               END IF
            END IF
            J2 = I - K - 1 + MAX( 1, K-I0+2 )*KA1
            NR = ( N-J2+KA ) / KA1
            J1 = J2 + ( NR-1 )*KA1
            IF( UPDATE ) THEN
               J2T = MAX( J2, I+2*KA-K+1 )
            ELSE
               J2T = J2
            END IF
            NRT = ( N-J2T+KA ) / KA1
            DO 320 J = J2T, J1, KA1
*
*              create nonzero element a(j+1,j-ka) outside the band
*              and store it in WORK(j-m)
*
               WORK( J-M ) = WORK( J-M )*AB( KA1, J-KA+1 )
               AB( KA1, J-KA+1 ) = RWORK( J-M )*AB( KA1, J-KA+1 )
  320       CONTINUE
*
*           generate rotations in 1st set to annihilate elements which
*           have been created outside the band
*
            IF( NRT.GT.0 )
     $         CALL ZLARGV( NRT, AB( KA1, J2T-KA ), INCA, WORK( J2T-M ),
     $                      KA1, RWORK( J2T-M ), KA1 )
            IF( NR.GT.0 ) THEN
*
*              apply rotations in 1st set from the left
*
               DO 330 L = 1, KA - 1
                  CALL ZLARTV( NR, AB( L+1, J2-L ), INCA,
     $                         AB( L+2, J2-L ), INCA, RWORK( J2-M ),
     $                         WORK( J2-M ), KA1 )
  330          CONTINUE
*
*              apply rotations in 1st set from both sides to diagonal
*              blocks
*
               CALL ZLAR2V( NR, AB( 1, J2 ), AB( 1, J2+1 ), AB( 2, J2 ),
     $                      INCA, RWORK( J2-M ), WORK( J2-M ), KA1 )
*
               CALL ZLACGV( NR, WORK( J2-M ), KA1 )
            END IF
*
*           start applying rotations in 1st set from the right
*
            DO 340 L = KA - 1, KB - K + 1, -1
               NRT = ( N-J2+L ) / KA1
               IF( NRT.GT.0 )
     $            CALL ZLARTV( NRT, AB( KA1-L+1, J2 ), INCA,
     $                         AB( KA1-L, J2+1 ), INCA, RWORK( J2-M ),
     $                         WORK( J2-M ), KA1 )
  340       CONTINUE
*
            IF( WANTX ) THEN
*
*              post-multiply X by product of rotations in 1st set
*
               DO 350 J = J2, J1, KA1
                  CALL ZROT( N-M, X( M+1, J ), 1, X( M+1, J+1 ), 1,
     $                       RWORK( J-M ), WORK( J-M ) )
  350          CONTINUE
            END IF
  360    CONTINUE
*
         IF( UPDATE ) THEN
            IF( I2.LE.N .AND. KBT.GT.0 ) THEN
*
*              create nonzero element a(i-kbt+ka+1,i-kbt) outside the
*              band and store it in WORK(i-kbt)
*
               WORK( I-KBT ) = -BB( KBT+1, I-KBT )*RA1
            END IF
         END IF
*
         DO 400 K = KB, 1, -1
            IF( UPDATE ) THEN
               J2 = I - K - 1 + MAX( 2, K-I0+1 )*KA1
            ELSE
               J2 = I - K - 1 + MAX( 1, K-I0+1 )*KA1
            END IF
*
*           finish applying rotations in 2nd set from the right
*
            DO 370 L = KB - K, 1, -1
               NRT = ( N-J2+KA+L ) / KA1
               IF( NRT.GT.0 )
     $            CALL ZLARTV( NRT, AB( KA1-L+1, J2-KA ), INCA,
     $                         AB( KA1-L, J2-KA+1 ), INCA,
     $                         RWORK( J2-KA ), WORK( J2-KA ), KA1 )
  370       CONTINUE
            NR = ( N-J2+KA ) / KA1
            J1 = J2 + ( NR-1 )*KA1
            DO 380 J = J1, J2, -KA1
               WORK( J ) = WORK( J-KA )
               RWORK( J ) = RWORK( J-KA )
  380       CONTINUE
            DO 390 J = J2, J1, KA1
*
*              create nonzero element a(j+1,j-ka) outside the band
*              and store it in WORK(j)
*
               WORK( J ) = WORK( J )*AB( KA1, J-KA+1 )
               AB( KA1, J-KA+1 ) = RWORK( J )*AB( KA1, J-KA+1 )
  390       CONTINUE
            IF( UPDATE ) THEN
               IF( I-K.LT.N-KA .AND. K.LE.KBT )
     $            WORK( I-K+KA ) = WORK( I-K )
            END IF
  400    CONTINUE
*
         DO 440 K = KB, 1, -1
            J2 = I - K - 1 + MAX( 1, K-I0+1 )*KA1
            NR = ( N-J2+KA ) / KA1
            J1 = J2 + ( NR-1 )*KA1
            IF( NR.GT.0 ) THEN
*
*              generate rotations in 2nd set to annihilate elements
*              which have been created outside the band
*
               CALL ZLARGV( NR, AB( KA1, J2-KA ), INCA, WORK( J2 ), KA1,
     $                      RWORK( J2 ), KA1 )
*
*              apply rotations in 2nd set from the left
*
               DO 410 L = 1, KA - 1
                  CALL ZLARTV( NR, AB( L+1, J2-L ), INCA,
     $                         AB( L+2, J2-L ), INCA, RWORK( J2 ),
     $                         WORK( J2 ), KA1 )
  410          CONTINUE
*
*              apply rotations in 2nd set from both sides to diagonal
*              blocks
*
               CALL ZLAR2V( NR, AB( 1, J2 ), AB( 1, J2+1 ), AB( 2, J2 ),
     $                      INCA, RWORK( J2 ), WORK( J2 ), KA1 )
*
               CALL ZLACGV( NR, WORK( J2 ), KA1 )
            END IF
*
*           start applying rotations in 2nd set from the right
*
            DO 420 L = KA - 1, KB - K + 1, -1
               NRT = ( N-J2+L ) / KA1
               IF( NRT.GT.0 )
     $            CALL ZLARTV( NRT, AB( KA1-L+1, J2 ), INCA,
     $                         AB( KA1-L, J2+1 ), INCA, RWORK( J2 ),
     $                         WORK( J2 ), KA1 )
  420       CONTINUE
*
            IF( WANTX ) THEN
*
*              post-multiply X by product of rotations in 2nd set
*
               DO 430 J = J2, J1, KA1
                  CALL ZROT( N-M, X( M+1, J ), 1, X( M+1, J+1 ), 1,
     $                       RWORK( J ), WORK( J ) )
  430          CONTINUE
            END IF
  440    CONTINUE
*
         DO 460 K = 1, KB - 1
            J2 = I - K - 1 + MAX( 1, K-I0+2 )*KA1
*
*           finish applying rotations in 1st set from the right
*
            DO 450 L = KB - K, 1, -1
               NRT = ( N-J2+L ) / KA1
               IF( NRT.GT.0 )
     $            CALL ZLARTV( NRT, AB( KA1-L+1, J2 ), INCA,
     $                         AB( KA1-L, J2+1 ), INCA, RWORK( J2-M ),
     $                         WORK( J2-M ), KA1 )
  450       CONTINUE
  460    CONTINUE
*
         IF( KB.GT.1 ) THEN
            DO 470 J = N - 1, J2 + KA, -1
               RWORK( J-M ) = RWORK( J-KA-M )
               WORK( J-M ) = WORK( J-KA-M )
  470       CONTINUE
         END IF
*
      END IF
*
      GO TO 10
*
  480 CONTINUE
*
*     **************************** Phase 2 *****************************
*
*     The logical structure of this phase is:
*
*     UPDATE = .TRUE.
*     DO I = 1, M
*        use S(i) to update A and create a new bulge
*        apply rotations to push all bulges KA positions upward
*     END DO
*     UPDATE = .FALSE.
*     DO I = M - KA - 1, 2, -1
*        apply rotations to push all bulges KA positions upward
*     END DO
*
*     To avoid duplicating code, the two loops are merged.
*
      UPDATE = .TRUE.
      I = 0
  490 CONTINUE
      IF( UPDATE ) THEN
         I = I + 1
         KBT = MIN( KB, M-I )
         I0 = I + 1
         I1 = MAX( 1, I-KA )
         I2 = I + KBT - KA1
         IF( I.GT.M ) THEN
            UPDATE = .FALSE.
            I = I - 1
            I0 = M + 1
            IF( KA.EQ.0 )
     $         RETURN
            GO TO 490
         END IF
      ELSE
         I = I - KA
         IF( I.LT.2 )
     $      RETURN
      END IF
*
      IF( I.LT.M-KBT ) THEN
         NX = M
      ELSE
         NX = N
      END IF
*
      IF( UPPER ) THEN
*
*        Transform A, working with the upper triangle
*
         IF( UPDATE ) THEN
*
*           Form  inv(S(i))**H * A * inv(S(i))
*
            BII = DBLE( BB( KB1, I ) )
            AB( KA1, I ) = ( DBLE( AB( KA1, I ) ) / BII ) / BII
            DO 500 J = I1, I - 1
               AB( J-I+KA1, I ) = AB( J-I+KA1, I ) / BII
  500       CONTINUE
            DO 510 J = I + 1, MIN( N, I+KA )
               AB( I-J+KA1, J ) = AB( I-J+KA1, J ) / BII
  510       CONTINUE
            DO 540 K = I + 1, I + KBT
               DO 520 J = K, I + KBT
                  AB( K-J+KA1, J ) = AB( K-J+KA1, J ) -
     $                               BB( I-J+KB1, J )*
     $                               DCONJG( AB( I-K+KA1, K ) ) -
     $                               DCONJG( BB( I-K+KB1, K ) )*
     $                               AB( I-J+KA1, J ) +
     $                               DBLE( AB( KA1, I ) )*
     $                               BB( I-J+KB1, J )*
     $                               DCONJG( BB( I-K+KB1, K ) )
  520          CONTINUE
               DO 530 J = I + KBT + 1, MIN( N, I+KA )
                  AB( K-J+KA1, J ) = AB( K-J+KA1, J ) -
     $                               DCONJG( BB( I-K+KB1, K ) )*
     $                               AB( I-J+KA1, J )
  530          CONTINUE
  540       CONTINUE
            DO 560 J = I1, I
               DO 550 K = I + 1, MIN( J+KA, I+KBT )
                  AB( J-K+KA1, K ) = AB( J-K+KA1, K ) -
     $                               BB( I-K+KB1, K )*AB( J-I+KA1, I )
  550          CONTINUE
  560       CONTINUE
*
            IF( WANTX ) THEN
*
*              post-multiply X by inv(S(i))
*
               CALL ZDSCAL( NX, ONE / BII, X( 1, I ), 1 )
               IF( KBT.GT.0 )
     $            CALL ZGERU( NX, KBT, -CONE, X( 1, I ), 1,
     $                        BB( KB, I+1 ), LDBB-1, X( 1, I+1 ), LDX )
            END IF
*
*           store a(i1,i) in RA1 for use in next loop over K
*
            RA1 = AB( I1-I+KA1, I )
         END IF
*
*        Generate and apply vectors of rotations to chase all the
*        existing bulges KA positions up toward the top of the band
*
         DO 610 K = 1, KB - 1
            IF( UPDATE ) THEN
*
*              Determine the rotations which would annihilate the bulge
*              which has in theory just been created
*
               IF( I+K-KA1.GT.0 .AND. I+K.LT.M ) THEN
*
*                 generate rotation to annihilate a(i+k-ka-1,i)
*
                  CALL ZLARTG( AB( K+1, I ), RA1, RWORK( I+K-KA ),
     $                         WORK( I+K-KA ), RA )
*
*                 create nonzero element a(i+k-ka-1,i+k) outside the
*                 band and store it in WORK(m-kb+i+k)
*
                  T = -BB( KB1-K, I+K )*RA1
                  WORK( M-KB+I+K ) = RWORK( I+K-KA )*T -
     $                               DCONJG( WORK( I+K-KA ) )*
     $                               AB( 1, I+K )
                  AB( 1, I+K ) = WORK( I+K-KA )*T +
     $                           RWORK( I+K-KA )*AB( 1, I+K )
                  RA1 = RA
               END IF
            END IF
            J2 = I + K + 1 - MAX( 1, K+I0-M+1 )*KA1
            NR = ( J2+KA-1 ) / KA1
            J1 = J2 - ( NR-1 )*KA1
            IF( UPDATE ) THEN
               J2T = MIN( J2, I-2*KA+K-1 )
            ELSE
               J2T = J2
            END IF
            NRT = ( J2T+KA-1 ) / KA1
            DO 570 J = J1, J2T, KA1
*
*              create nonzero element a(j-1,j+ka) outside the band
*              and store it in WORK(j)
*
               WORK( J ) = WORK( J )*AB( 1, J+KA-1 )
               AB( 1, J+KA-1 ) = RWORK( J )*AB( 1, J+KA-1 )
  570       CONTINUE
*
*           generate rotations in 1st set to annihilate elements which
*           have been created outside the band
*
            IF( NRT.GT.0 )
     $         CALL ZLARGV( NRT, AB( 1, J1+KA ), INCA, WORK( J1 ), KA1,
     $                      RWORK( J1 ), KA1 )
            IF( NR.GT.0 ) THEN
*
*              apply rotations in 1st set from the left
*
               DO 580 L = 1, KA - 1
                  CALL ZLARTV( NR, AB( KA1-L, J1+L ), INCA,
     $                         AB( KA-L, J1+L ), INCA, RWORK( J1 ),
     $                         WORK( J1 ), KA1 )
  580          CONTINUE
*
*              apply rotations in 1st set from both sides to diagonal
*              blocks
*
               CALL ZLAR2V( NR, AB( KA1, J1 ), AB( KA1, J1-1 ),
     $                      AB( KA, J1 ), INCA, RWORK( J1 ), WORK( J1 ),
     $                      KA1 )
*
               CALL ZLACGV( NR, WORK( J1 ), KA1 )
            END IF
*
*           start applying rotations in 1st set from the right
*
            DO 590 L = KA - 1, KB - K + 1, -1
               NRT = ( J2+L-1 ) / KA1
               J1T = J2 - ( NRT-1 )*KA1
               IF( NRT.GT.0 )
     $            CALL ZLARTV( NRT, AB( L, J1T ), INCA,
     $                         AB( L+1, J1T-1 ), INCA, RWORK( J1T ),
     $                         WORK( J1T ), KA1 )
  590       CONTINUE
*
            IF( WANTX ) THEN
*
*              post-multiply X by product of rotations in 1st set
*
               DO 600 J = J1, J2, KA1
                  CALL ZROT( NX, X( 1, J ), 1, X( 1, J-1 ), 1,
     $                       RWORK( J ), WORK( J ) )
  600          CONTINUE
            END IF
  610    CONTINUE
*
         IF( UPDATE ) THEN
            IF( I2.GT.0 .AND. KBT.GT.0 ) THEN
*
*              create nonzero element a(i+kbt-ka-1,i+kbt) outside the
*              band and store it in WORK(m-kb+i+kbt)
*
               WORK( M-KB+I+KBT ) = -BB( KB1-KBT, I+KBT )*RA1
            END IF
         END IF
*
         DO 650 K = KB, 1, -1
            IF( UPDATE ) THEN
               J2 = I + K + 1 - MAX( 2, K+I0-M )*KA1
            ELSE
               J2 = I + K + 1 - MAX( 1, K+I0-M )*KA1
            END IF
*
*           finish applying rotations in 2nd set from the right
*
            DO 620 L = KB - K, 1, -1
               NRT = ( J2+KA+L-1 ) / KA1
               J1T = J2 - ( NRT-1 )*KA1
               IF( NRT.GT.0 )
     $            CALL ZLARTV( NRT, AB( L, J1T+KA ), INCA,
     $                         AB( L+1, J1T+KA-1 ), INCA,
     $                         RWORK( M-KB+J1T+KA ),
     $                         WORK( M-KB+J1T+KA ), KA1 )
  620       CONTINUE
            NR = ( J2+KA-1 ) / KA1
            J1 = J2 - ( NR-1 )*KA1
            DO 630 J = J1, J2, KA1
               WORK( M-KB+J ) = WORK( M-KB+J+KA )
               RWORK( M-KB+J ) = RWORK( M-KB+J+KA )
  630       CONTINUE
            DO 640 J = J1, J2, KA1
*
*              create nonzero element a(j-1,j+ka) outside the band
*              and store it in WORK(m-kb+j)
*
               WORK( M-KB+J ) = WORK( M-KB+J )*AB( 1, J+KA-1 )
               AB( 1, J+KA-1 ) = RWORK( M-KB+J )*AB( 1, J+KA-1 )
  640       CONTINUE
            IF( UPDATE ) THEN
               IF( I+K.GT.KA1 .AND. K.LE.KBT )
     $            WORK( M-KB+I+K-KA ) = WORK( M-KB+I+K )
            END IF
  650    CONTINUE
*
         DO 690 K = KB, 1, -1
            J2 = I + K + 1 - MAX( 1, K+I0-M )*KA1
            NR = ( J2+KA-1 ) / KA1
            J1 = J2 - ( NR-1 )*KA1
            IF( NR.GT.0 ) THEN
*
*              generate rotations in 2nd set to annihilate elements
*              which have been created outside the band
*
               CALL ZLARGV( NR, AB( 1, J1+KA ), INCA, WORK( M-KB+J1 ),
     $                      KA1, RWORK( M-KB+J1 ), KA1 )
*
*              apply rotations in 2nd set from the left
*
               DO 660 L = 1, KA - 1
                  CALL ZLARTV( NR, AB( KA1-L, J1+L ), INCA,
     $                         AB( KA-L, J1+L ), INCA, RWORK( M-KB+J1 ),
     $                         WORK( M-KB+J1 ), KA1 )
  660          CONTINUE
*
*              apply rotations in 2nd set from both sides to diagonal
*              blocks
*
               CALL ZLAR2V( NR, AB( KA1, J1 ), AB( KA1, J1-1 ),
     $                      AB( KA, J1 ), INCA, RWORK( M-KB+J1 ),
     $                      WORK( M-KB+J1 ), KA1 )
*
               CALL ZLACGV( NR, WORK( M-KB+J1 ), KA1 )
            END IF
*
*           start applying rotations in 2nd set from the right
*
            DO 670 L = KA - 1, KB - K + 1, -1
               NRT = ( J2+L-1 ) / KA1
               J1T = J2 - ( NRT-1 )*KA1
               IF( NRT.GT.0 )
     $            CALL ZLARTV( NRT, AB( L, J1T ), INCA,
     $                         AB( L+1, J1T-1 ), INCA,
     $                         RWORK( M-KB+J1T ), WORK( M-KB+J1T ),
     $                         KA1 )
  670       CONTINUE
*
            IF( WANTX ) THEN
*
*              post-multiply X by product of rotations in 2nd set
*
               DO 680 J = J1, J2, KA1
                  CALL ZROT( NX, X( 1, J ), 1, X( 1, J-1 ), 1,
     $                       RWORK( M-KB+J ), WORK( M-KB+J ) )
  680          CONTINUE
            END IF
  690    CONTINUE
*
         DO 710 K = 1, KB - 1
            J2 = I + K + 1 - MAX( 1, K+I0-M+1 )*KA1
*
*           finish applying rotations in 1st set from the right
*
            DO 700 L = KB - K, 1, -1
               NRT = ( J2+L-1 ) / KA1
               J1T = J2 - ( NRT-1 )*KA1
               IF( NRT.GT.0 )
     $            CALL ZLARTV( NRT, AB( L, J1T ), INCA,
     $                         AB( L+1, J1T-1 ), INCA, RWORK( J1T ),
     $                         WORK( J1T ), KA1 )
  700       CONTINUE
  710    CONTINUE
*
         IF( KB.GT.1 ) THEN
            DO 720 J = 2, I2 - KA
               RWORK( J ) = RWORK( J+KA )
               WORK( J ) = WORK( J+KA )
  720       CONTINUE
         END IF
*
      ELSE
*
*        Transform A, working with the lower triangle
*
         IF( UPDATE ) THEN
*
*           Form  inv(S(i))**H * A * inv(S(i))
*
            BII = DBLE( BB( 1, I ) )
            AB( 1, I ) = ( DBLE( AB( 1, I ) ) / BII ) / BII
            DO 730 J = I1, I - 1
               AB( I-J+1, J ) = AB( I-J+1, J ) / BII
  730       CONTINUE
            DO 740 J = I + 1, MIN( N, I+KA )
               AB( J-I+1, I ) = AB( J-I+1, I ) / BII
  740       CONTINUE
            DO 770 K = I + 1, I + KBT
               DO 750 J = K, I + KBT
                  AB( J-K+1, K ) = AB( J-K+1, K ) -
     $                             BB( J-I+1, I )*DCONJG( AB( K-I+1,
     $                             I ) ) - DCONJG( BB( K-I+1, I ) )*
     $                             AB( J-I+1, I ) + DBLE( AB( 1, I ) )*
     $                             BB( J-I+1, I )*DCONJG( BB( K-I+1,
     $                             I ) )
  750          CONTINUE
               DO 760 J = I + KBT + 1, MIN( N, I+KA )
                  AB( J-K+1, K ) = AB( J-K+1, K ) -
     $                             DCONJG( BB( K-I+1, I ) )*
     $                             AB( J-I+1, I )
  760          CONTINUE
  770       CONTINUE
            DO 790 J = I1, I
               DO 780 K = I + 1, MIN( J+KA, I+KBT )
                  AB( K-J+1, J ) = AB( K-J+1, J ) -
     $                             BB( K-I+1, I )*AB( I-J+1, J )
  780          CONTINUE
  790       CONTINUE
*
            IF( WANTX ) THEN
*
*              post-multiply X by inv(S(i))
*
               CALL ZDSCAL( NX, ONE / BII, X( 1, I ), 1 )
               IF( KBT.GT.0 )
     $            CALL ZGERC( NX, KBT, -CONE, X( 1, I ), 1, BB( 2, I ),
     $                        1, X( 1, I+1 ), LDX )
            END IF
*
*           store a(i,i1) in RA1 for use in next loop over K
*
            RA1 = AB( I-I1+1, I1 )
         END IF
*
*        Generate and apply vectors of rotations to chase all the
*        existing bulges KA positions up toward the top of the band
*
         DO 840 K = 1, KB - 1
            IF( UPDATE ) THEN
*
*              Determine the rotations which would annihilate the bulge
*              which has in theory just been created
*
               IF( I+K-KA1.GT.0 .AND. I+K.LT.M ) THEN
*
*                 generate rotation to annihilate a(i,i+k-ka-1)
*
                  CALL ZLARTG( AB( KA1-K, I+K-KA ), RA1,
     $                         RWORK( I+K-KA ), WORK( I+K-KA ), RA )
*
*                 create nonzero element a(i+k,i+k-ka-1) outside the
*                 band and store it in WORK(m-kb+i+k)
*
                  T = -BB( K+1, I )*RA1
                  WORK( M-KB+I+K ) = RWORK( I+K-KA )*T -
     $                               DCONJG( WORK( I+K-KA ) )*
     $                               AB( KA1, I+K-KA )
                  AB( KA1, I+K-KA ) = WORK( I+K-KA )*T +
     $                                RWORK( I+K-KA )*AB( KA1, I+K-KA )
                  RA1 = RA
               END IF
            END IF
            J2 = I + K + 1 - MAX( 1, K+I0-M+1 )*KA1
            NR = ( J2+KA-1 ) / KA1
            J1 = J2 - ( NR-1 )*KA1
            IF( UPDATE ) THEN
               J2T = MIN( J2, I-2*KA+K-1 )
            ELSE
               J2T = J2
            END IF
            NRT = ( J2T+KA-1 ) / KA1
            DO 800 J = J1, J2T, KA1
*
*              create nonzero element a(j+ka,j-1) outside the band
*              and store it in WORK(j)
*
               WORK( J ) = WORK( J )*AB( KA1, J-1 )
               AB( KA1, J-1 ) = RWORK( J )*AB( KA1, J-1 )
  800       CONTINUE
*
*           generate rotations in 1st set to annihilate elements which
*           have been created outside the band
*
            IF( NRT.GT.0 )
     $         CALL ZLARGV( NRT, AB( KA1, J1 ), INCA, WORK( J1 ), KA1,
     $                      RWORK( J1 ), KA1 )
            IF( NR.GT.0 ) THEN
*
*              apply rotations in 1st set from the right
*
               DO 810 L = 1, KA - 1
                  CALL ZLARTV( NR, AB( L+1, J1 ), INCA, AB( L+2, J1-1 ),
     $                         INCA, RWORK( J1 ), WORK( J1 ), KA1 )
  810          CONTINUE
*
*              apply rotations in 1st set from both sides to diagonal
*              blocks
*
               CALL ZLAR2V( NR, AB( 1, J1 ), AB( 1, J1-1 ),
     $                      AB( 2, J1-1 ), INCA, RWORK( J1 ),
     $                      WORK( J1 ), KA1 )
*
               CALL ZLACGV( NR, WORK( J1 ), KA1 )
            END IF
*
*           start applying rotations in 1st set from the left
*
            DO 820 L = KA - 1, KB - K + 1, -1
               NRT = ( J2+L-1 ) / KA1
               J1T = J2 - ( NRT-1 )*KA1
               IF( NRT.GT.0 )
     $            CALL ZLARTV( NRT, AB( KA1-L+1, J1T-KA1+L ), INCA,
     $                         AB( KA1-L, J1T-KA1+L ), INCA,
     $                         RWORK( J1T ), WORK( J1T ), KA1 )
  820       CONTINUE
*
            IF( WANTX ) THEN
*
*              post-multiply X by product of rotations in 1st set
*
               DO 830 J = J1, J2, KA1
                  CALL ZROT( NX, X( 1, J ), 1, X( 1, J-1 ), 1,
     $                       RWORK( J ), DCONJG( WORK( J ) ) )
  830          CONTINUE
            END IF
  840    CONTINUE
*
         IF( UPDATE ) THEN
            IF( I2.GT.0 .AND. KBT.GT.0 ) THEN
*
*              create nonzero element a(i+kbt,i+kbt-ka-1) outside the
*              band and store it in WORK(m-kb+i+kbt)
*
               WORK( M-KB+I+KBT ) = -BB( KBT+1, I )*RA1
            END IF
         END IF
*
         DO 880 K = KB, 1, -1
            IF( UPDATE ) THEN
               J2 = I + K + 1 - MAX( 2, K+I0-M )*KA1
            ELSE
               J2 = I + K + 1 - MAX( 1, K+I0-M )*KA1
            END IF
*
*           finish applying rotations in 2nd set from the left
*
            DO 850 L = KB - K, 1, -1
               NRT = ( J2+KA+L-1 ) / KA1
               J1T = J2 - ( NRT-1 )*KA1
               IF( NRT.GT.0 )
     $            CALL ZLARTV( NRT, AB( KA1-L+1, J1T+L-1 ), INCA,
     $                         AB( KA1-L, J1T+L-1 ), INCA,
     $                         RWORK( M-KB+J1T+KA ),
     $                         WORK( M-KB+J1T+KA ), KA1 )
  850       CONTINUE
            NR = ( J2+KA-1 ) / KA1
            J1 = J2 - ( NR-1 )*KA1
            DO 860 J = J1, J2, KA1
               WORK( M-KB+J ) = WORK( M-KB+J+KA )
               RWORK( M-KB+J ) = RWORK( M-KB+J+KA )
  860       CONTINUE
            DO 870 J = J1, J2, KA1
*
*              create nonzero element a(j+ka,j-1) outside the band
*              and store it in WORK(m-kb+j)
*
               WORK( M-KB+J ) = WORK( M-KB+J )*AB( KA1, J-1 )
               AB( KA1, J-1 ) = RWORK( M-KB+J )*AB( KA1, J-1 )
  870       CONTINUE
            IF( UPDATE ) THEN
               IF( I+K.GT.KA1 .AND. K.LE.KBT )
     $            WORK( M-KB+I+K-KA ) = WORK( M-KB+I+K )
            END IF
  880    CONTINUE
*
         DO 920 K = KB, 1, -1
            J2 = I + K + 1 - MAX( 1, K+I0-M )*KA1
            NR = ( J2+KA-1 ) / KA1
            J1 = J2 - ( NR-1 )*KA1
            IF( NR.GT.0 ) THEN
*
*              generate rotations in 2nd set to annihilate elements
*              which have been created outside the band
*
               CALL ZLARGV( NR, AB( KA1, J1 ), INCA, WORK( M-KB+J1 ),
     $                      KA1, RWORK( M-KB+J1 ), KA1 )
*
*              apply rotations in 2nd set from the right
*
               DO 890 L = 1, KA - 1
                  CALL ZLARTV( NR, AB( L+1, J1 ), INCA, AB( L+2, J1-1 ),
     $                         INCA, RWORK( M-KB+J1 ), WORK( M-KB+J1 ),
     $                         KA1 )
  890          CONTINUE
*
*              apply rotations in 2nd set from both sides to diagonal
*              blocks
*
               CALL ZLAR2V( NR, AB( 1, J1 ), AB( 1, J1-1 ),
     $                      AB( 2, J1-1 ), INCA, RWORK( M-KB+J1 ),
     $                      WORK( M-KB+J1 ), KA1 )
*
               CALL ZLACGV( NR, WORK( M-KB+J1 ), KA1 )
            END IF
*
*           start applying rotations in 2nd set from the left
*
            DO 900 L = KA - 1, KB - K + 1, -1
               NRT = ( J2+L-1 ) / KA1
               J1T = J2 - ( NRT-1 )*KA1
               IF( NRT.GT.0 )
     $            CALL ZLARTV( NRT, AB( KA1-L+1, J1T-KA1+L ), INCA,
     $                         AB( KA1-L, J1T-KA1+L ), INCA,
     $                         RWORK( M-KB+J1T ), WORK( M-KB+J1T ),
     $                         KA1 )
  900       CONTINUE
*
            IF( WANTX ) THEN
*
*              post-multiply X by product of rotations in 2nd set
*
               DO 910 J = J1, J2, KA1
                  CALL ZROT( NX, X( 1, J ), 1, X( 1, J-1 ), 1,
     $                       RWORK( M-KB+J ), DCONJG( WORK( M-KB+J ) ) )
  910          CONTINUE
            END IF
  920    CONTINUE
*
         DO 940 K = 1, KB - 1
            J2 = I + K + 1 - MAX( 1, K+I0-M+1 )*KA1
*
*           finish applying rotations in 1st set from the left
*
            DO 930 L = KB - K, 1, -1
               NRT = ( J2+L-1 ) / KA1
               J1T = J2 - ( NRT-1 )*KA1
               IF( NRT.GT.0 )
     $            CALL ZLARTV( NRT, AB( KA1-L+1, J1T-KA1+L ), INCA,
     $                         AB( KA1-L, J1T-KA1+L ), INCA,
     $                         RWORK( J1T ), WORK( J1T ), KA1 )
  930       CONTINUE
  940    CONTINUE
*
         IF( KB.GT.1 ) THEN
            DO 950 J = 2, I2 - KA
               RWORK( J ) = RWORK( J+KA )
               WORK( J ) = WORK( J+KA )
  950       CONTINUE
         END IF
*
      END IF
*
      GO TO 490
*
*     End of ZHBGST
*
      END
