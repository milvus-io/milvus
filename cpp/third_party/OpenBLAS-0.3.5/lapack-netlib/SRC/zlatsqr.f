*
*  Definition:
*  ===========
*
*       SUBROUTINE ZLATSQR( M, N, MB, NB, A, LDA, T, LDT, WORK,
*                           LWORK, INFO)
*
*       .. Scalar Arguments ..
*       INTEGER           INFO, LDA, M, N, MB, NB, LDT, LWORK
*       ..
*       .. Array Arguments ..
*       COMPLEX*16        A( LDA, * ), T( LDT, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLATSQR computes a blocked Tall-Skinny QR factorization of
*> an M-by-N matrix A, where M >= N:
*> A = Q * R .
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix A.  M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix A. M >= N >= 0.
*> \endverbatim
*>
*> \param[in] MB
*> \verbatim
*>          MB is INTEGER
*>          The row block size to be used in the blocked QR.
*>          MB > N.
*> \endverbatim
*>
*> \param[in] NB
*> \verbatim
*>          NB is INTEGER
*>          The column block size to be used in the blocked QR.
*>          N >= NB >= 1.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>          On entry, the M-by-N matrix A.
*>          On exit, the elements on and above the diagonal
*>          of the array contain the N-by-N upper triangular matrix R;
*>          the elements below the diagonal represent Q by the columns
*>          of blocked V (see Further Details).
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,M).
*> \endverbatim
*>
*> \param[out] T
*> \verbatim
*>          T is COMPLEX*16 array,
*>          dimension (LDT, N * Number_of_row_blocks)
*>          where Number_of_row_blocks = CEIL((M-N)/(MB-N))
*>          The blocked upper triangular block reflectors stored in compact form
*>          as a sequence of upper triangular blocks.
*>          See Further Details below.
*> \endverbatim
*>
*> \param[in] LDT
*> \verbatim
*>          LDT is INTEGER
*>          The leading dimension of the array T.  LDT >= NB.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>         (workspace) COMPLEX*16 array, dimension (MAX(1,LWORK))
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          The dimension of the array WORK.  LWORK >= NB*N.
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal size of the WORK array, returns
*>          this value as the first entry of the WORK array, and no error
*>          message related to LWORK is issued by XERBLA.
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
*> \par Further Details:
*  =====================
*>
*> \verbatim
*> Tall-Skinny QR (TSQR) performs QR by a sequence of orthogonal transformations,
*> representing Q as a product of other orthogonal matrices
*>   Q = Q(1) * Q(2) * . . . * Q(k)
*> where each Q(i) zeros out subdiagonal entries of a block of MB rows of A:
*>   Q(1) zeros out the subdiagonal entries of rows 1:MB of A
*>   Q(2) zeros out the bottom MB-N rows of rows [1:N,MB+1:2*MB-N] of A
*>   Q(3) zeros out the bottom MB-N rows of rows [1:N,2*MB-N+1:3*MB-2*N] of A
*>   . . .
*>
*> Q(1) is computed by GEQRT, which represents Q(1) by Householder vectors
*> stored under the diagonal of rows 1:MB of A, and by upper triangular
*> block reflectors, stored in array T(1:LDT,1:N).
*> For more information see Further Details in GEQRT.
*>
*> Q(i) for i>1 is computed by TPQRT, which represents Q(i) by Householder vectors
*> stored in rows [(i-1)*(MB-N)+N+1:i*(MB-N)+N] of A, and by upper triangular
*> block reflectors, stored in array T(1:LDT,(i-1)*N+1:i*N).
*> The last Q(k) may use fewer rows.
*> For more information see Further Details in TPQRT.
*>
*> For more details of the overall algorithm, see the description of
*> Sequential TSQR in Section 2.2 of [1].
*>
*> [1] “Communication-Optimal Parallel and Sequential QR and LU Factorizations,”
*>     J. Demmel, L. Grigori, M. Hoemmen, J. Langou,
*>     SIAM J. Sci. Comput, vol. 34, no. 1, 2012
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE ZLATSQR( M, N, MB, NB, A, LDA, T, LDT, WORK,
     $                    LWORK, INFO)
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd. --
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER           INFO, LDA, M, N, MB, NB, LDT, LWORK
*     ..
*     .. Array Arguments ..
      COMPLEX*16        A( LDA, * ), WORK( * ), T(LDT, *)
*     ..
*
*  =====================================================================
*
*     ..
*     .. Local Scalars ..
      LOGICAL    LQUERY
      INTEGER    I, II, KK, CTR
*     ..
*     .. EXTERNAL FUNCTIONS ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     .. EXTERNAL SUBROUTINES ..
      EXTERNAL    ZGEQRT, ZTPQRT, XERBLA
*     .. INTRINSIC FUNCTIONS ..
      INTRINSIC          MAX, MIN, MOD
*     ..
*     .. EXECUTABLE STATEMENTS ..
*
*     TEST THE INPUT ARGUMENTS
*
      INFO = 0
*
      LQUERY = ( LWORK.EQ.-1 )
*
      IF( M.LT.0 ) THEN
        INFO = -1
      ELSE IF( N.LT.0 .OR. M.LT.N ) THEN
        INFO = -2
      ELSE IF( MB.LE.N ) THEN
        INFO = -3
      ELSE IF( NB.LT.1 .OR. ( NB.GT.N .AND. N.GT.0 )) THEN
        INFO = -4
      ELSE IF( LDA.LT.MAX( 1, M ) ) THEN
        INFO = -5
      ELSE IF( LDT.LT.NB ) THEN
        INFO = -8
      ELSE IF( LWORK.LT.(N*NB) .AND. (.NOT.LQUERY) ) THEN
        INFO = -10
      END IF
      IF( INFO.EQ.0)  THEN
        WORK(1) = NB*N
      END IF
      IF( INFO.NE.0 ) THEN
        CALL XERBLA( 'ZLATSQR', -INFO )
        RETURN
      ELSE IF (LQUERY) THEN
       RETURN
      END IF
*
*     Quick return if possible
*
      IF( MIN(M,N).EQ.0 ) THEN
          RETURN
      END IF
*
*     The QR Decomposition
*
       IF ((MB.LE.N).OR.(MB.GE.M)) THEN
         CALL ZGEQRT( M, N, NB, A, LDA, T, LDT, WORK, INFO)
         RETURN
       END IF
       KK = MOD((M-N),(MB-N))
       II=M-KK+1
*
*      Compute the QR factorization of the first block A(1:MB,1:N)
*
       CALL ZGEQRT( MB, N, NB, A(1,1), LDA, T, LDT, WORK, INFO )
       CTR = 1
*
       DO I = MB+1, II-MB+N ,  (MB-N)
*
*      Compute the QR factorization of the current block A(I:I+MB-N,1:N)
*
         CALL ZTPQRT( MB-N, N, 0, NB, A(1,1), LDA, A( I, 1 ), LDA,
     $                 T(1, CTR * N + 1),
     $                  LDT, WORK, INFO )
         CTR = CTR + 1
       END DO
*
*      Compute the QR factorization of the last block A(II:M,1:N)
*
       IF (II.LE.M) THEN
         CALL ZTPQRT( KK, N, 0, NB, A(1,1), LDA, A( II, 1 ), LDA,
     $                 T(1,CTR * N + 1), LDT,
     $                  WORK, INFO )
       END IF
*
      work( 1 ) = N*NB
      RETURN
*
*     End of ZLATSQR
*
      END
