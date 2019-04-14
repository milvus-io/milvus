*> \brief \b DTPRFB applies a real or complex "triangular-pentagonal" blocked reflector to a real or complex matrix, which is composed of two blocks.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DTPRFB + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dtprfb.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dtprfb.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dtprfb.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DTPRFB( SIDE, TRANS, DIRECT, STOREV, M, N, K, L,
*                          V, LDV, T, LDT, A, LDA, B, LDB, WORK, LDWORK )
*
*       .. Scalar Arguments ..
*       CHARACTER DIRECT, SIDE, STOREV, TRANS
*       INTEGER   K, L, LDA, LDB, LDT, LDV, LDWORK, M, N
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   A( LDA, * ), B( LDB, * ), T( LDT, * ),
*      $          V( LDV, * ), WORK( LDWORK, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DTPRFB applies a real "triangular-pentagonal" block reflector H or its
*> transpose H**T to a real matrix C, which is composed of two
*> blocks A and B, either from the left or right.
*>
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] SIDE
*> \verbatim
*>          SIDE is CHARACTER*1
*>          = 'L': apply H or H**T from the Left
*>          = 'R': apply H or H**T from the Right
*> \endverbatim
*>
*> \param[in] TRANS
*> \verbatim
*>          TRANS is CHARACTER*1
*>          = 'N': apply H (No transpose)
*>          = 'T': apply H**T (Transpose)
*> \endverbatim
*>
*> \param[in] DIRECT
*> \verbatim
*>          DIRECT is CHARACTER*1
*>          Indicates how H is formed from a product of elementary
*>          reflectors
*>          = 'F': H = H(1) H(2) . . . H(k) (Forward)
*>          = 'B': H = H(k) . . . H(2) H(1) (Backward)
*> \endverbatim
*>
*> \param[in] STOREV
*> \verbatim
*>          STOREV is CHARACTER*1
*>          Indicates how the vectors which define the elementary
*>          reflectors are stored:
*>          = 'C': Columns
*>          = 'R': Rows
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix B.
*>          M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix B.
*>          N >= 0.
*> \endverbatim
*>
*> \param[in] K
*> \verbatim
*>          K is INTEGER
*>          The order of the matrix T, i.e. the number of elementary
*>          reflectors whose product defines the block reflector.
*>          K >= 0.
*> \endverbatim
*>
*> \param[in] L
*> \verbatim
*>          L is INTEGER
*>          The order of the trapezoidal part of V.
*>          K >= L >= 0.  See Further Details.
*> \endverbatim
*>
*> \param[in] V
*> \verbatim
*>          V is DOUBLE PRECISION array, dimension
*>                                (LDV,K) if STOREV = 'C'
*>                                (LDV,M) if STOREV = 'R' and SIDE = 'L'
*>                                (LDV,N) if STOREV = 'R' and SIDE = 'R'
*>          The pentagonal matrix V, which contains the elementary reflectors
*>          H(1), H(2), ..., H(K).  See Further Details.
*> \endverbatim
*>
*> \param[in] LDV
*> \verbatim
*>          LDV is INTEGER
*>          The leading dimension of the array V.
*>          If STOREV = 'C' and SIDE = 'L', LDV >= max(1,M);
*>          if STOREV = 'C' and SIDE = 'R', LDV >= max(1,N);
*>          if STOREV = 'R', LDV >= K.
*> \endverbatim
*>
*> \param[in] T
*> \verbatim
*>          T is DOUBLE PRECISION array, dimension (LDT,K)
*>          The triangular K-by-K matrix T in the representation of the
*>          block reflector.
*> \endverbatim
*>
*> \param[in] LDT
*> \verbatim
*>          LDT is INTEGER
*>          The leading dimension of the array T.
*>          LDT >= K.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension
*>          (LDA,N) if SIDE = 'L' or (LDA,K) if SIDE = 'R'
*>          On entry, the K-by-N or M-by-K matrix A.
*>          On exit, A is overwritten by the corresponding block of
*>          H*C or H**T*C or C*H or C*H**T.  See Further Details.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.
*>          If SIDE = 'L', LDC >= max(1,K);
*>          If SIDE = 'R', LDC >= max(1,M).
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is DOUBLE PRECISION array, dimension (LDB,N)
*>          On entry, the M-by-N matrix B.
*>          On exit, B is overwritten by the corresponding block of
*>          H*C or H**T*C or C*H or C*H**T.  See Further Details.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B.
*>          LDB >= max(1,M).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension
*>          (LDWORK,N) if SIDE = 'L',
*>          (LDWORK,K) if SIDE = 'R'.
*> \endverbatim
*>
*> \param[in] LDWORK
*> \verbatim
*>          LDWORK is INTEGER
*>          The leading dimension of the array WORK.
*>          If SIDE = 'L', LDWORK >= K;
*>          if SIDE = 'R', LDWORK >= M.
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
*> \ingroup doubleOTHERauxiliary
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  The matrix C is a composite matrix formed from blocks A and B.
*>  The block B is of size M-by-N; if SIDE = 'R', A is of size M-by-K,
*>  and if SIDE = 'L', A is of size K-by-N.
*>
*>  If SIDE = 'R' and DIRECT = 'F', C = [A B].
*>
*>  If SIDE = 'L' and DIRECT = 'F', C = [A]
*>                                      [B].
*>
*>  If SIDE = 'R' and DIRECT = 'B', C = [B A].
*>
*>  If SIDE = 'L' and DIRECT = 'B', C = [B]
*>                                      [A].
*>
*>  The pentagonal matrix V is composed of a rectangular block V1 and a
*>  trapezoidal block V2.  The size of the trapezoidal block is determined by
*>  the parameter L, where 0<=L<=K.  If L=K, the V2 block of V is triangular;
*>  if L=0, there is no trapezoidal block, thus V = V1 is rectangular.
*>
*>  If DIRECT = 'F' and STOREV = 'C':  V = [V1]
*>                                         [V2]
*>     - V2 is upper trapezoidal (first L rows of K-by-K upper triangular)
*>
*>  If DIRECT = 'F' and STOREV = 'R':  V = [V1 V2]
*>
*>     - V2 is lower trapezoidal (first L columns of K-by-K lower triangular)
*>
*>  If DIRECT = 'B' and STOREV = 'C':  V = [V2]
*>                                         [V1]
*>     - V2 is lower trapezoidal (last L rows of K-by-K lower triangular)
*>
*>  If DIRECT = 'B' and STOREV = 'R':  V = [V2 V1]
*>
*>     - V2 is upper trapezoidal (last L columns of K-by-K upper triangular)
*>
*>  If STOREV = 'C' and SIDE = 'L', V is M-by-K with V2 L-by-K.
*>
*>  If STOREV = 'C' and SIDE = 'R', V is N-by-K with V2 L-by-K.
*>
*>  If STOREV = 'R' and SIDE = 'L', V is K-by-M with V2 K-by-L.
*>
*>  If STOREV = 'R' and SIDE = 'R', V is K-by-N with V2 K-by-L.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE DTPRFB( SIDE, TRANS, DIRECT, STOREV, M, N, K, L,
     $                   V, LDV, T, LDT, A, LDA, B, LDB, WORK, LDWORK )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER DIRECT, SIDE, STOREV, TRANS
      INTEGER   K, L, LDA, LDB, LDT, LDV, LDWORK, M, N
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   A( LDA, * ), B( LDB, * ), T( LDT, * ),
     $          V( LDV, * ), WORK( LDWORK, * )
*     ..
*
*  ==========================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ONE, ZERO
      PARAMETER ( ONE = 1.0, ZERO = 0.0 )
*     ..
*     .. Local Scalars ..
      INTEGER   I, J, MP, NP, KP
      LOGICAL   LEFT, FORWARD, COLUMN, RIGHT, BACKWARD, ROW
*     ..
*     .. External Functions ..
      LOGICAL   LSAME
      EXTERNAL  LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL  DGEMM, DTRMM
*     ..
*     .. Executable Statements ..
*
*     Quick return if possible
*
      IF( M.LE.0 .OR. N.LE.0 .OR. K.LE.0 .OR. L.LT.0 ) RETURN
*
      IF( LSAME( STOREV, 'C' ) ) THEN
         COLUMN = .TRUE.
         ROW = .FALSE.
      ELSE IF ( LSAME( STOREV, 'R' ) ) THEN
         COLUMN = .FALSE.
         ROW = .TRUE.
      ELSE
         COLUMN = .FALSE.
         ROW = .FALSE.
      END IF
*
      IF( LSAME( SIDE, 'L' ) ) THEN
         LEFT = .TRUE.
         RIGHT = .FALSE.
      ELSE IF( LSAME( SIDE, 'R' ) ) THEN
         LEFT = .FALSE.
         RIGHT = .TRUE.
      ELSE
         LEFT = .FALSE.
         RIGHT = .FALSE.
      END IF
*
      IF( LSAME( DIRECT, 'F' ) ) THEN
         FORWARD = .TRUE.
         BACKWARD = .FALSE.
      ELSE IF( LSAME( DIRECT, 'B' ) ) THEN
         FORWARD = .FALSE.
         BACKWARD = .TRUE.
      ELSE
         FORWARD = .FALSE.
         BACKWARD = .FALSE.
      END IF
*
* ---------------------------------------------------------------------------
*
      IF( COLUMN .AND. FORWARD .AND. LEFT  ) THEN
*
* ---------------------------------------------------------------------------
*
*        Let  W =  [ I ]    (K-by-K)
*                  [ V ]    (M-by-K)
*
*        Form  H C  or  H**T C  where  C = [ A ]  (K-by-N)
*                                          [ B ]  (M-by-N)
*
*        H = I - W T W**T          or  H**T = I - W T**T W**T
*
*        A = A -   T (A + V**T B)  or  A = A -   T**T (A + V**T B)
*        B = B - V T (A + V**T B)  or  B = B - V T**T (A + V**T B)
*
* ---------------------------------------------------------------------------
*
         MP = MIN( M-L+1, M )
         KP = MIN( L+1, K )
*
         DO J = 1, N
            DO I = 1, L
               WORK( I, J ) = B( M-L+I, J )
            END DO
         END DO
         CALL DTRMM( 'L', 'U', 'T', 'N', L, N, ONE, V( MP, 1 ), LDV,
     $               WORK, LDWORK )
         CALL DGEMM( 'T', 'N', L, N, M-L, ONE, V, LDV, B, LDB,
     $               ONE, WORK, LDWORK )
         CALL DGEMM( 'T', 'N', K-L, N, M, ONE, V( 1, KP ), LDV,
     $               B, LDB, ZERO, WORK( KP, 1 ), LDWORK )
*
         DO J = 1, N
            DO I = 1, K
               WORK( I, J ) = WORK( I, J ) + A( I, J )
            END DO
         END DO
*
         CALL DTRMM( 'L', 'U', TRANS, 'N', K, N, ONE, T, LDT,
     $               WORK, LDWORK )
*
         DO J = 1, N
            DO I = 1, K
               A( I, J ) = A( I, J ) - WORK( I, J )
            END DO
         END DO
*
         CALL DGEMM( 'N', 'N', M-L, N, K, -ONE, V, LDV, WORK, LDWORK,
     $               ONE, B, LDB )
         CALL DGEMM( 'N', 'N', L, N, K-L, -ONE, V( MP, KP ), LDV,
     $               WORK( KP, 1 ), LDWORK, ONE, B( MP, 1 ),  LDB )
         CALL DTRMM( 'L', 'U', 'N', 'N', L, N, ONE, V( MP, 1 ), LDV,
     $               WORK, LDWORK )
         DO J = 1, N
            DO I = 1, L
               B( M-L+I, J ) = B( M-L+I, J ) - WORK( I, J )
            END DO
         END DO
*
* ---------------------------------------------------------------------------
*
      ELSE IF( COLUMN .AND. FORWARD .AND. RIGHT ) THEN
*
* ---------------------------------------------------------------------------
*
*        Let  W =  [ I ]    (K-by-K)
*                  [ V ]    (N-by-K)
*
*        Form  C H or  C H**T  where  C = [ A B ] (A is M-by-K, B is M-by-N)
*
*        H = I - W T W**T          or  H**T = I - W T**T W**T
*
*        A = A - (A + B V) T      or  A = A - (A + B V) T**T
*        B = B - (A + B V) T V**T  or  B = B - (A + B V) T**T V**T
*
* ---------------------------------------------------------------------------
*
         NP = MIN( N-L+1, N )
         KP = MIN( L+1, K )
*
         DO J = 1, L
            DO I = 1, M
               WORK( I, J ) = B( I, N-L+J )
            END DO
         END DO
         CALL DTRMM( 'R', 'U', 'N', 'N', M, L, ONE, V( NP, 1 ), LDV,
     $               WORK, LDWORK )
         CALL DGEMM( 'N', 'N', M, L, N-L, ONE, B, LDB,
     $               V, LDV, ONE, WORK, LDWORK )
         CALL DGEMM( 'N', 'N', M, K-L, N, ONE, B, LDB,
     $               V( 1, KP ), LDV, ZERO, WORK( 1, KP ), LDWORK )
*
         DO J = 1, K
            DO I = 1, M
               WORK( I, J ) = WORK( I, J ) + A( I, J )
            END DO
         END DO
*
         CALL DTRMM( 'R', 'U', TRANS, 'N', M, K, ONE, T, LDT,
     $               WORK, LDWORK )
*
         DO J = 1, K
            DO I = 1, M
               A( I, J ) = A( I, J ) - WORK( I, J )
            END DO
         END DO
*
         CALL DGEMM( 'N', 'T', M, N-L, K, -ONE, WORK, LDWORK,
     $               V, LDV, ONE, B, LDB )
         CALL DGEMM( 'N', 'T', M, L, K-L, -ONE, WORK( 1, KP ), LDWORK,
     $               V( NP, KP ), LDV, ONE, B( 1, NP ), LDB )
         CALL DTRMM( 'R', 'U', 'T', 'N', M, L, ONE, V( NP, 1 ), LDV,
     $               WORK, LDWORK )
         DO J = 1, L
            DO I = 1, M
               B( I, N-L+J ) = B( I, N-L+J ) - WORK( I, J )
            END DO
         END DO
*
* ---------------------------------------------------------------------------
*
      ELSE IF( COLUMN .AND. BACKWARD .AND. LEFT ) THEN
*
* ---------------------------------------------------------------------------
*
*        Let  W =  [ V ]    (M-by-K)
*                  [ I ]    (K-by-K)
*
*        Form  H C  or  H**T C  where  C = [ B ]  (M-by-N)
*                                          [ A ]  (K-by-N)
*
*        H = I - W T W**T          or  H**T = I - W T**T W**T
*
*        A = A -   T (A + V**T B)  or  A = A -   T**T (A + V**T B)
*        B = B - V T (A + V**T B)  or  B = B - V T**T (A + V**T B)
*
* ---------------------------------------------------------------------------
*
         MP = MIN( L+1, M )
         KP = MIN( K-L+1, K )
*
         DO J = 1, N
            DO I = 1, L
               WORK( K-L+I, J ) = B( I, J )
            END DO
         END DO
*
         CALL DTRMM( 'L', 'L', 'T', 'N', L, N, ONE, V( 1, KP ), LDV,
     $               WORK( KP, 1 ), LDWORK )
         CALL DGEMM( 'T', 'N', L, N, M-L, ONE, V( MP, KP ), LDV,
     $               B( MP, 1 ), LDB, ONE, WORK( KP, 1 ), LDWORK )
         CALL DGEMM( 'T', 'N', K-L, N, M, ONE, V, LDV,
     $               B, LDB, ZERO, WORK, LDWORK )
*
         DO J = 1, N
            DO I = 1, K
               WORK( I, J ) = WORK( I, J ) + A( I, J )
            END DO
         END DO
*
         CALL DTRMM( 'L', 'L', TRANS, 'N', K, N, ONE, T, LDT,
     $               WORK, LDWORK )
*
         DO J = 1, N
            DO I = 1, K
               A( I, J ) = A( I, J ) - WORK( I, J )
            END DO
         END DO
*
         CALL DGEMM( 'N', 'N', M-L, N, K, -ONE, V( MP, 1 ), LDV,
     $               WORK, LDWORK, ONE, B( MP, 1 ), LDB )
         CALL DGEMM( 'N', 'N', L, N, K-L, -ONE, V, LDV,
     $               WORK, LDWORK, ONE, B,  LDB )
         CALL DTRMM( 'L', 'L', 'N', 'N', L, N, ONE, V( 1, KP ), LDV,
     $               WORK( KP, 1 ), LDWORK )
         DO J = 1, N
            DO I = 1, L
               B( I, J ) = B( I, J ) - WORK( K-L+I, J )
            END DO
         END DO
*
* ---------------------------------------------------------------------------
*
      ELSE IF( COLUMN .AND. BACKWARD .AND. RIGHT ) THEN
*
* ---------------------------------------------------------------------------
*
*        Let  W =  [ V ]    (N-by-K)
*                  [ I ]    (K-by-K)
*
*        Form  C H  or  C H**T  where  C = [ B A ] (B is M-by-N, A is M-by-K)
*
*        H = I - W T W**T          or  H**T = I - W T**T W**T
*
*        A = A - (A + B V) T      or  A = A - (A + B V) T**T
*        B = B - (A + B V) T V**T  or  B = B - (A + B V) T**T V**T
*
* ---------------------------------------------------------------------------
*
         NP = MIN( L+1, N )
         KP = MIN( K-L+1, K )
*
         DO J = 1, L
            DO I = 1, M
               WORK( I, K-L+J ) = B( I, J )
            END DO
         END DO
         CALL DTRMM( 'R', 'L', 'N', 'N', M, L, ONE, V( 1, KP ), LDV,
     $               WORK( 1, KP ), LDWORK )
         CALL DGEMM( 'N', 'N', M, L, N-L, ONE, B( 1, NP ), LDB,
     $               V( NP, KP ), LDV, ONE, WORK( 1, KP ), LDWORK )
         CALL DGEMM( 'N', 'N', M, K-L, N, ONE, B, LDB,
     $               V, LDV, ZERO, WORK, LDWORK )
*
         DO J = 1, K
            DO I = 1, M
               WORK( I, J ) = WORK( I, J ) + A( I, J )
            END DO
         END DO
*
         CALL DTRMM( 'R', 'L', TRANS, 'N', M, K, ONE, T, LDT,
     $               WORK, LDWORK )
*
         DO J = 1, K
            DO I = 1, M
               A( I, J ) = A( I, J ) - WORK( I, J )
            END DO
         END DO
*
         CALL DGEMM( 'N', 'T', M, N-L, K, -ONE, WORK, LDWORK,
     $               V( NP, 1 ), LDV, ONE, B( 1, NP ), LDB )
         CALL DGEMM( 'N', 'T', M, L, K-L, -ONE, WORK, LDWORK,
     $               V, LDV, ONE, B, LDB )
         CALL DTRMM( 'R', 'L', 'T', 'N', M, L, ONE, V( 1, KP ), LDV,
     $               WORK( 1, KP ), LDWORK )
         DO J = 1, L
            DO I = 1, M
               B( I, J ) = B( I, J ) - WORK( I, K-L+J )
            END DO
         END DO
*
* ---------------------------------------------------------------------------
*
      ELSE IF( ROW .AND. FORWARD .AND. LEFT ) THEN
*
* ---------------------------------------------------------------------------
*
*        Let  W =  [ I V ] ( I is K-by-K, V is K-by-M )
*
*        Form  H C  or  H**T C  where  C = [ A ]  (K-by-N)
*                                          [ B ]  (M-by-N)
*
*        H = I - W**T T W          or  H**T = I - W**T T**T W
*
*        A = A -     T (A + V B)  or  A = A -     T**T (A + V B)
*        B = B - V**T T (A + V B)  or  B = B - V**T T**T (A + V B)
*
* ---------------------------------------------------------------------------
*
         MP = MIN( M-L+1, M )
         KP = MIN( L+1, K )
*
         DO J = 1, N
            DO I = 1, L
               WORK( I, J ) = B( M-L+I, J )
            END DO
         END DO
         CALL DTRMM( 'L', 'L', 'N', 'N', L, N, ONE, V( 1, MP ), LDV,
     $               WORK, LDB )
         CALL DGEMM( 'N', 'N', L, N, M-L, ONE, V, LDV,B, LDB,
     $               ONE, WORK, LDWORK )
         CALL DGEMM( 'N', 'N', K-L, N, M, ONE, V( KP, 1 ), LDV,
     $               B, LDB, ZERO, WORK( KP, 1 ), LDWORK )
*
         DO J = 1, N
            DO I = 1, K
               WORK( I, J ) = WORK( I, J ) + A( I, J )
            END DO
         END DO
*
         CALL DTRMM( 'L', 'U', TRANS, 'N', K, N, ONE, T, LDT,
     $               WORK, LDWORK )
*
         DO J = 1, N
            DO I = 1, K
               A( I, J ) = A( I, J ) - WORK( I, J )
            END DO
         END DO
*
         CALL DGEMM( 'T', 'N', M-L, N, K, -ONE, V, LDV, WORK, LDWORK,
     $               ONE, B, LDB )
         CALL DGEMM( 'T', 'N', L, N, K-L, -ONE, V( KP, MP ), LDV,
     $               WORK( KP, 1 ), LDWORK, ONE, B( MP, 1 ), LDB )
         CALL DTRMM( 'L', 'L', 'T', 'N', L, N, ONE, V( 1, MP ), LDV,
     $               WORK, LDWORK )
         DO J = 1, N
            DO I = 1, L
               B( M-L+I, J ) = B( M-L+I, J ) - WORK( I, J )
            END DO
         END DO
*
* ---------------------------------------------------------------------------
*
      ELSE IF( ROW .AND. FORWARD .AND. RIGHT ) THEN
*
* ---------------------------------------------------------------------------
*
*        Let  W =  [ I V ] ( I is K-by-K, V is K-by-N )
*
*        Form  C H  or  C H**T  where  C = [ A B ] (A is M-by-K, B is M-by-N)
*
*        H = I - W**T T W            or  H**T = I - W**T T**T W
*
*        A = A - (A + B V**T) T      or  A = A - (A + B V**T) T**T
*        B = B - (A + B V**T) T V    or  B = B - (A + B V**T) T**T V
*
* ---------------------------------------------------------------------------
*
         NP = MIN( N-L+1, N )
         KP = MIN( L+1, K )
*
         DO J = 1, L
            DO I = 1, M
               WORK( I, J ) = B( I, N-L+J )
            END DO
         END DO
         CALL DTRMM( 'R', 'L', 'T', 'N', M, L, ONE, V( 1, NP ), LDV,
     $               WORK, LDWORK )
         CALL DGEMM( 'N', 'T', M, L, N-L, ONE, B, LDB, V, LDV,
     $               ONE, WORK, LDWORK )
         CALL DGEMM( 'N', 'T', M, K-L, N, ONE, B, LDB,
     $               V( KP, 1 ), LDV, ZERO, WORK( 1, KP ), LDWORK )
*
         DO J = 1, K
            DO I = 1, M
               WORK( I, J ) = WORK( I, J ) + A( I, J )
            END DO
         END DO
*
         CALL DTRMM( 'R', 'U', TRANS, 'N', M, K, ONE, T, LDT,
     $               WORK, LDWORK )
*
         DO J = 1, K
            DO I = 1, M
               A( I, J ) = A( I, J ) - WORK( I, J )
            END DO
         END DO
*
         CALL DGEMM( 'N', 'N', M, N-L, K, -ONE, WORK, LDWORK,
     $               V, LDV, ONE, B, LDB )
         CALL DGEMM( 'N', 'N', M, L, K-L, -ONE, WORK( 1, KP ), LDWORK,
     $               V( KP, NP ), LDV, ONE, B( 1, NP ), LDB )
         CALL DTRMM( 'R', 'L', 'N', 'N', M, L, ONE, V( 1, NP ), LDV,
     $               WORK, LDWORK )
         DO J = 1, L
            DO I = 1, M
               B( I, N-L+J ) = B( I, N-L+J ) - WORK( I, J )
            END DO
         END DO
*
* ---------------------------------------------------------------------------
*
      ELSE IF( ROW .AND. BACKWARD .AND. LEFT ) THEN
*
* ---------------------------------------------------------------------------
*
*        Let  W =  [ V I ] ( I is K-by-K, V is K-by-M )
*
*        Form  H C  or  H**T C  where  C = [ B ]  (M-by-N)
*                                          [ A ]  (K-by-N)
*
*        H = I - W**T T W          or  H**T = I - W**T T**T W
*
*        A = A -     T (A + V B)  or  A = A -     T**T (A + V B)
*        B = B - V**T T (A + V B)  or  B = B - V**T T**T (A + V B)
*
* ---------------------------------------------------------------------------
*
         MP = MIN( L+1, M )
         KP = MIN( K-L+1, K )
*
         DO J = 1, N
            DO I = 1, L
               WORK( K-L+I, J ) = B( I, J )
            END DO
         END DO
         CALL DTRMM( 'L', 'U', 'N', 'N', L, N, ONE, V( KP, 1 ), LDV,
     $               WORK( KP, 1 ), LDWORK )
         CALL DGEMM( 'N', 'N', L, N, M-L, ONE, V( KP, MP ), LDV,
     $               B( MP, 1 ), LDB, ONE, WORK( KP, 1 ), LDWORK )
         CALL DGEMM( 'N', 'N', K-L, N, M, ONE, V, LDV, B, LDB,
     $               ZERO, WORK, LDWORK )
*
         DO J = 1, N
            DO I = 1, K
               WORK( I, J ) = WORK( I, J ) + A( I, J )
            END DO
         END DO
*
         CALL DTRMM( 'L', 'L ', TRANS, 'N', K, N, ONE, T, LDT,
     $               WORK, LDWORK )
*
         DO J = 1, N
            DO I = 1, K
               A( I, J ) = A( I, J ) - WORK( I, J )
            END DO
         END DO
*
         CALL DGEMM( 'T', 'N', M-L, N, K, -ONE, V( 1, MP ), LDV,
     $               WORK, LDWORK, ONE, B( MP, 1 ), LDB )
         CALL DGEMM( 'T', 'N', L, N, K-L, -ONE, V, LDV,
     $               WORK, LDWORK, ONE, B, LDB )
         CALL DTRMM( 'L', 'U', 'T', 'N', L, N, ONE, V( KP, 1 ), LDV,
     $               WORK( KP, 1 ), LDWORK )
         DO J = 1, N
            DO I = 1, L
               B( I, J ) = B( I, J ) - WORK( K-L+I, J )
            END DO
         END DO
*
* ---------------------------------------------------------------------------
*
      ELSE IF( ROW .AND. BACKWARD .AND. RIGHT ) THEN
*
* ---------------------------------------------------------------------------
*
*        Let  W =  [ V I ] ( I is K-by-K, V is K-by-N )
*
*        Form  C H  or  C H**T  where  C = [ B A ] (A is M-by-K, B is M-by-N)
*
*        H = I - W**T T W            or  H**T = I - W**T T**T W
*
*        A = A - (A + B V**T) T      or  A = A - (A + B V**T) T**T
*        B = B - (A + B V**T) T V    or  B = B - (A + B V**T) T**T V
*
* ---------------------------------------------------------------------------
*
         NP = MIN( L+1, N )
         KP = MIN( K-L+1, K )
*
         DO J = 1, L
            DO I = 1, M
               WORK( I, K-L+J ) = B( I, J )
            END DO
         END DO
         CALL DTRMM( 'R', 'U', 'T', 'N', M, L, ONE, V( KP, 1 ), LDV,
     $               WORK( 1, KP ), LDWORK )
         CALL DGEMM( 'N', 'T', M, L, N-L, ONE, B( 1, NP ), LDB,
     $               V( KP, NP ), LDV, ONE, WORK( 1, KP ), LDWORK )
         CALL DGEMM( 'N', 'T', M, K-L, N, ONE, B, LDB, V, LDV,
     $               ZERO, WORK, LDWORK )
*
         DO J = 1, K
            DO I = 1, M
               WORK( I, J ) = WORK( I, J ) + A( I, J )
            END DO
         END DO
*
         CALL DTRMM( 'R', 'L', TRANS, 'N', M, K, ONE, T, LDT,
     $               WORK, LDWORK )
*
         DO J = 1, K
            DO I = 1, M
               A( I, J ) = A( I, J ) - WORK( I, J )
            END DO
         END DO
*
         CALL DGEMM( 'N', 'N', M, N-L, K, -ONE, WORK, LDWORK,
     $               V( 1, NP ), LDV, ONE, B( 1, NP ), LDB )
         CALL DGEMM( 'N', 'N', M, L, K-L , -ONE, WORK, LDWORK,
     $               V, LDV, ONE, B, LDB )
         CALL DTRMM( 'R', 'U', 'N', 'N', M, L, ONE, V( KP, 1 ), LDV,
     $               WORK( 1, KP ), LDWORK )
         DO J = 1, L
            DO I = 1, M
               B( I, J ) = B( I, J ) - WORK( I, K-L+J )
            END DO
         END DO
*
      END IF
*
      RETURN
*
*     End of DTPRFB
*
      END
