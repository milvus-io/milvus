*> \brief \b DTPMLQT
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DTPMQRT + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dtpmlqt.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dtpmlqt.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dtpmlqt.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DTPMLQT( SIDE, TRANS, M, N, K, L, MB, V, LDV, T, LDT,
*                           A, LDA, B, LDB, WORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER SIDE, TRANS
*       INTEGER   INFO, K, LDV, LDA, LDB, M, N, L, MB, LDT
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   V( LDV, * ), A( LDA, * ), B( LDB, * ),
*      $                   T( LDT, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DTPMQRT applies a real orthogonal matrix Q obtained from a
*> "triangular-pentagonal" real block reflector H to a general
*> real matrix C, which consists of two blocks A and B.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] SIDE
*> \verbatim
*>          SIDE is CHARACTER*1
*>          = 'L': apply Q or Q**T from the Left;
*>          = 'R': apply Q or Q**T from the Right.
*> \endverbatim
*>
*> \param[in] TRANS
*> \verbatim
*>          TRANS is CHARACTER*1
*>          = 'N':  No transpose, apply Q;
*>          = 'T':  Transpose, apply Q**T.
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix B. M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix B. N >= 0.
*> \endverbatim
*>
*> \param[in] K
*> \verbatim
*>          K is INTEGER
*>          The number of elementary reflectors whose product defines
*>          the matrix Q.
*> \endverbatim
*>
*> \param[in] L
*> \verbatim
*>          L is INTEGER
*>          The order of the trapezoidal part of V.
*>          K >= L >= 0.  See Further Details.
*> \endverbatim
*>
*> \param[in] MB
*> \verbatim
*>          MB is INTEGER
*>          The block size used for the storage of T.  K >= MB >= 1.
*>          This must be the same value of MB used to generate T
*>          in DTPLQT.
*> \endverbatim
*>
*> \param[in] V
*> \verbatim
*>          V is DOUBLE PRECISION array, dimension (LDA,K)
*>          The i-th row must contain the vector which defines the
*>          elementary reflector H(i), for i = 1,2,...,k, as returned by
*>          DTPLQT in B.  See Further Details.
*> \endverbatim
*>
*> \param[in] LDV
*> \verbatim
*>          LDV is INTEGER
*>          The leading dimension of the array V.
*>          If SIDE = 'L', LDV >= max(1,M);
*>          if SIDE = 'R', LDV >= max(1,N).
*> \endverbatim
*>
*> \param[in] T
*> \verbatim
*>          T is DOUBLE PRECISION array, dimension (LDT,K)
*>          The upper triangular factors of the block reflectors
*>          as returned by DTPLQT, stored as a MB-by-K matrix.
*> \endverbatim
*>
*> \param[in] LDT
*> \verbatim
*>          LDT is INTEGER
*>          The leading dimension of the array T.  LDT >= MB.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension
*>          (LDA,N) if SIDE = 'L' or
*>          (LDA,K) if SIDE = 'R'
*>          On entry, the K-by-N or M-by-K matrix A.
*>          On exit, A is overwritten by the corresponding block of
*>          Q*C or Q**T*C or C*Q or C*Q**T.  See Further Details.
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
*>          Q*C or Q**T*C or C*Q or C*Q**T.  See Further Details.
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
*>          WORK is DOUBLE PRECISION array. The dimension of WORK is
*>           N*MB if SIDE = 'L', or  M*MB if SIDE = 'R'.
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
*> \date November 2017
*
*> \ingroup doubleOTHERcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  The columns of the pentagonal matrix V contain the elementary reflectors
*>  H(1), H(2), ..., H(K); V is composed of a rectangular block V1 and a
*>  trapezoidal block V2:
*>
*>        V = [V1] [V2].
*>
*>
*>  The size of the trapezoidal block V2 is determined by the parameter L,
*>  where 0 <= L <= K; V2 is lower trapezoidal, consisting of the first L
*>  rows of a K-by-K upper triangular matrix.  If L=K, V2 is lower triangular;
*>  if L=0, there is no trapezoidal block, hence V = V1 is rectangular.
*>
*>  If SIDE = 'L':  C = [A]  where A is K-by-N,  B is M-by-N and V is K-by-M.
*>                      [B]
*>
*>  If SIDE = 'R':  C = [A B]  where A is M-by-K, B is M-by-N and V is K-by-N.
*>
*>  The real orthogonal matrix Q is formed from V and T.
*>
*>  If TRANS='N' and SIDE='L', C is on exit replaced with Q * C.
*>
*>  If TRANS='T' and SIDE='L', C is on exit replaced with Q**T * C.
*>
*>  If TRANS='N' and SIDE='R', C is on exit replaced with C * Q.
*>
*>  If TRANS='T' and SIDE='R', C is on exit replaced with C * Q**T.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE DTPMLQT( SIDE, TRANS, M, N, K, L, MB, V, LDV, T, LDT,
     $                    A, LDA, B, LDB, WORK, INFO )
*
*  -- LAPACK computational routine (version 3.8.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2017
*
*     .. Scalar Arguments ..
      CHARACTER SIDE, TRANS
      INTEGER   INFO, K, LDV, LDA, LDB, M, N, L, MB, LDT
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   V( LDV, * ), A( LDA, * ), B( LDB, * ),
     $                   T( LDT, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     ..
*     .. Local Scalars ..
      LOGICAL            LEFT, RIGHT, TRAN, NOTRAN
      INTEGER            I, IB, NB, LB, KF, LDAQ
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA, DLARFB, DTPRFB
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     .. Test the input arguments ..
*
      INFO   = 0
      LEFT   = LSAME( SIDE,  'L' )
      RIGHT  = LSAME( SIDE,  'R' )
      TRAN   = LSAME( TRANS, 'T' )
      NOTRAN = LSAME( TRANS, 'N' )
*
      IF ( LEFT ) THEN
         LDAQ = MAX( 1, K )
      ELSE IF ( RIGHT ) THEN
         LDAQ = MAX( 1, M )
      END IF
      IF( .NOT.LEFT .AND. .NOT.RIGHT ) THEN
         INFO = -1
      ELSE IF( .NOT.TRAN .AND. .NOT.NOTRAN ) THEN
         INFO = -2
      ELSE IF( M.LT.0 ) THEN
         INFO = -3
      ELSE IF( N.LT.0 ) THEN
         INFO = -4
      ELSE IF( K.LT.0 ) THEN
         INFO = -5
      ELSE IF( L.LT.0 .OR. L.GT.K ) THEN
         INFO = -6
      ELSE IF( MB.LT.1 .OR. (MB.GT.K .AND. K.GT.0) ) THEN
         INFO = -7
      ELSE IF( LDV.LT.K ) THEN
         INFO = -9
      ELSE IF( LDT.LT.MB ) THEN
         INFO = -11
      ELSE IF( LDA.LT.LDAQ ) THEN
         INFO = -13
      ELSE IF( LDB.LT.MAX( 1, M ) ) THEN
         INFO = -15
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DTPMLQT', -INFO )
         RETURN
      END IF
*
*     .. Quick return if possible ..
*
      IF( M.EQ.0 .OR. N.EQ.0 .OR. K.EQ.0 ) RETURN
*
      IF( LEFT .AND. NOTRAN ) THEN
*
         DO I = 1, K, MB
            IB = MIN( MB, K-I+1 )
            NB = MIN( M-L+I+IB-1, M )
            IF( I.GE.L ) THEN
               LB = 0
            ELSE
               LB = 0
            END IF
            CALL DTPRFB( 'L', 'T', 'F', 'R', NB, N, IB, LB,
     $                   V( I, 1 ), LDV, T( 1, I ), LDT,
     $                   A( I, 1 ), LDA, B, LDB, WORK, IB )
         END DO
*
      ELSE IF( RIGHT .AND. TRAN ) THEN
*
         DO I = 1, K, MB
            IB = MIN( MB, K-I+1 )
            NB = MIN( N-L+I+IB-1, N )
            IF( I.GE.L ) THEN
               LB = 0
            ELSE
               LB = NB-N+L-I+1
            END IF
            CALL DTPRFB( 'R', 'N', 'F', 'R', M, NB, IB, LB,
     $                   V( I, 1 ), LDV, T( 1, I ), LDT,
     $                   A( 1, I ), LDA, B, LDB, WORK, M )
         END DO
*
      ELSE IF( LEFT .AND. TRAN ) THEN
*
         KF = ((K-1)/MB)*MB+1
         DO I = KF, 1, -MB
            IB = MIN( MB, K-I+1 )
            NB = MIN( M-L+I+IB-1, M )
            IF( I.GE.L ) THEN
               LB = 0
            ELSE
               LB = 0
            END IF
            CALL DTPRFB( 'L', 'N', 'F', 'R', NB, N, IB, LB,
     $                   V( I, 1 ), LDV, T( 1, I ), LDT,
     $                   A( I, 1 ), LDA, B, LDB, WORK, IB )
         END DO
*
      ELSE IF( RIGHT .AND. NOTRAN ) THEN
*
         KF = ((K-1)/MB)*MB+1
         DO I = KF, 1, -MB
            IB = MIN( MB, K-I+1 )
            NB = MIN( N-L+I+IB-1, N )
            IF( I.GE.L ) THEN
               LB = 0
            ELSE
               LB = NB-N+L-I+1
            END IF
            CALL DTPRFB( 'R', 'T', 'F', 'R', M, NB, IB, LB,
     $                   V( I, 1 ), LDV, T( 1, I ), LDT,
     $                   A( 1, I ), LDA, B, LDB, WORK, M )
         END DO
*
      END IF
*
      RETURN
*
*     End of DTPMLQT
*
      END
