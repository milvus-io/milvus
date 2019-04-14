C> \brief \b SGEQRF VARIANT: left-looking Level 3 BLAS version of the algorithm.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SGEQRF ( M, N, A, LDA, TAU, WORK, LWORK, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, LWORK, M, N
*       ..
*       .. Array Arguments ..
*       REAL               A( LDA, * ), TAU( * ), WORK( * )
*       ..
*
*  Purpose
*  =======
*
C>\details \b Purpose:
C>\verbatim
C>
C> SGEQRF computes a QR factorization of a real M-by-N matrix A:
C> A = Q * R.
C>
C> This is the left-looking Level 3 BLAS version of the algorithm.
C>
C>\endverbatim
*
*  Arguments:
*  ==========
*
C> \param[in] M
C> \verbatim
C>          M is INTEGER
C>          The number of rows of the matrix A.  M >= 0.
C> \endverbatim
C>
C> \param[in] N
C> \verbatim
C>          N is INTEGER
C>          The number of columns of the matrix A.  N >= 0.
C> \endverbatim
C>
C> \param[in,out] A
C> \verbatim
C>          A is REAL array, dimension (LDA,N)
C>          On entry, the M-by-N matrix A.
C>          On exit, the elements on and above the diagonal of the array
C>          contain the min(M,N)-by-N upper trapezoidal matrix R (R is
C>          upper triangular if m >= n); the elements below the diagonal,
C>          with the array TAU, represent the orthogonal matrix Q as a
C>          product of min(m,n) elementary reflectors (see Further
C>          Details).
C> \endverbatim
C>
C> \param[in] LDA
C> \verbatim
C>          LDA is INTEGER
C>          The leading dimension of the array A.  LDA >= max(1,M).
C> \endverbatim
C>
C> \param[out] TAU
C> \verbatim
C>          TAU is REAL array, dimension (min(M,N))
C>          The scalar factors of the elementary reflectors (see Further
C>          Details).
C> \endverbatim
C>
C> \param[out] WORK
C> \verbatim
C>          WORK is REAL array, dimension (MAX(1,LWORK))
C>          On exit, if INFO = 0, WORK(1) returns the optimal LWORK.
C> \endverbatim
C>
C> \param[in] LWORK
C> \verbatim
C>          LWORK is INTEGER
C> \endverbatim
C> \verbatim
C>          The dimension of the array WORK. The dimension can be divided into three parts.
C> \endverbatim
C> \verbatim
C>          1) The part for the triangular factor T. If the very last T is not bigger
C>             than any of the rest, then this part is NB x ceiling(K/NB), otherwise,
C>             NB x (K-NT), where K = min(M,N) and NT is the dimension of the very last T
C> \endverbatim
C> \verbatim
C>          2) The part for the very last T when T is bigger than any of the rest T.
C>             The size of this part is NT x NT, where NT = K - ceiling ((K-NX)/NB) x NB,
C>             where K = min(M,N), NX is calculated by
C>                   NX = MAX( 0, ILAENV( 3, 'SGEQRF', ' ', M, N, -1, -1 ) )
C> \endverbatim
C> \verbatim
C>          3) The part for dlarfb is of size max((N-M)*K, (N-M)*NB, K*NB, NB*NB)
C> \endverbatim
C> \verbatim
C>          So LWORK = part1 + part2 + part3
C> \endverbatim
C> \verbatim
C>          If LWORK = -1, then a workspace query is assumed; the routine
C>          only calculates the optimal size of the WORK array, returns
C>          this value as the first entry of the WORK array, and no error
C>          message related to LWORK is issued by XERBLA.
C> \endverbatim
C>
C> \param[out] INFO
C> \verbatim
C>          INFO is INTEGER
C>          = 0:  successful exit
C>          < 0:  if INFO = -i, the i-th argument had an illegal value
C> \endverbatim
C>
*
*  Authors:
*  ========
*
C> \author Univ. of Tennessee
C> \author Univ. of California Berkeley
C> \author Univ. of Colorado Denver
C> \author NAG Ltd.
*
C> \date December 2016
*
C> \ingroup variantsGEcomputational
*
*  Further Details
*  ===============
C>\details \b Further \b Details
C> \verbatim
C>
C>  The matrix Q is represented as a product of elementary reflectors
C>
C>     Q = H(1) H(2) . . . H(k), where k = min(m,n).
C>
C>  Each H(i) has the form
C>
C>     H(i) = I - tau * v * v'
C>
C>  where tau is a real scalar, and v is a real vector with
C>  v(1:i-1) = 0 and v(i) = 1; v(i+1:m) is stored on exit in A(i+1:m,i),
C>  and tau in TAU(i).
C>
C> \endverbatim
C>
*  =====================================================================
      SUBROUTINE SGEQRF ( M, N, A, LDA, TAU, WORK, LWORK, INFO )
*
*  -- LAPACK computational routine (version 3.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, LWORK, M, N
*     ..
*     .. Array Arguments ..
      REAL               A( LDA, * ), TAU( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      LOGICAL            LQUERY
      INTEGER            I, IB, IINFO, IWS, J, K, LWKOPT, NB,
     $                   NBMIN, NX, LBWORK, NT, LLWORK
*     ..
*     .. External Subroutines ..
      EXTERNAL           SGEQR2, SLARFB, SLARFT, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN
*     ..
*     .. External Functions ..
      INTEGER            ILAENV
      REAL               SCEIL
      EXTERNAL           ILAENV, SCEIL
*     ..
*     .. Executable Statements ..

      INFO = 0
      NBMIN = 2
      NX = 0
      IWS = N
      K = MIN( M, N )
      NB = ILAENV( 1, 'SGEQRF', ' ', M, N, -1, -1 )

      IF( NB.GT.1 .AND. NB.LT.K ) THEN
*
*        Determine when to cross over from blocked to unblocked code.
*
         NX = MAX( 0, ILAENV( 3, 'SGEQRF', ' ', M, N, -1, -1 ) )
      END IF
*
*     Get NT, the size of the very last T, which is the left-over from in-between K-NX and K to K, eg.:
*
*            NB=3     2NB=6       K=10
*            |        |           |
*      1--2--3--4--5--6--7--8--9--10
*                  |     \________/
*               K-NX=5      NT=4
*
*     So here 4 x 4 is the last T stored in the workspace
*
      NT = K-SCEIL(REAL(K-NX)/REAL(NB))*NB

*
*     optimal workspace = space for dlarfb + space for normal T's + space for the last T
*
      LLWORK = MAX (MAX((N-M)*K, (N-M)*NB), MAX(K*NB, NB*NB))
      LLWORK = SCEIL(REAL(LLWORK)/REAL(NB))

      IF ( NT.GT.NB ) THEN

          LBWORK = K-NT
*
*         Optimal workspace for dlarfb = MAX(1,N)*NT
*
          LWKOPT = (LBWORK+LLWORK)*NB
          WORK( 1 ) = (LWKOPT+NT*NT)

      ELSE

          LBWORK = SCEIL(REAL(K)/REAL(NB))*NB
          LWKOPT = (LBWORK+LLWORK-NB)*NB
          WORK( 1 ) = LWKOPT

      END IF

*
*     Test the input arguments
*
      LQUERY = ( LWORK.EQ.-1 )
      IF( M.LT.0 ) THEN
         INFO = -1
      ELSE IF( N.LT.0 ) THEN
         INFO = -2
      ELSE IF( LDA.LT.MAX( 1, M ) ) THEN
         INFO = -4
      ELSE IF( LWORK.LT.MAX( 1, N ) .AND. .NOT.LQUERY ) THEN
         INFO = -7
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SGEQRF', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( K.EQ.0 ) THEN
         WORK( 1 ) = 1
         RETURN
      END IF
*
      IF( NB.GT.1 .AND. NB.LT.K ) THEN

         IF( NX.LT.K ) THEN
*
*           Determine if workspace is large enough for blocked code.
*
            IF ( NT.LE.NB ) THEN
                IWS = (LBWORK+LLWORK-NB)*NB
            ELSE
                IWS = (LBWORK+LLWORK)*NB+NT*NT
            END IF

            IF( LWORK.LT.IWS ) THEN
*
*              Not enough workspace to use optimal NB:  reduce NB and
*              determine the minimum value of NB.
*
               IF ( NT.LE.NB ) THEN
                    NB = LWORK / (LLWORK+(LBWORK-NB))
               ELSE
                    NB = (LWORK-NT*NT)/(LBWORK+LLWORK)
               END IF

               NBMIN = MAX( 2, ILAENV( 2, 'SGEQRF', ' ', M, N, -1,
     $                 -1 ) )
            END IF
         END IF
      END IF
*
      IF( NB.GE.NBMIN .AND. NB.LT.K .AND. NX.LT.K ) THEN
*
*        Use blocked code initially
*
         DO 10 I = 1, K - NX, NB
            IB = MIN( K-I+1, NB )
*
*           Update the current column using old T's
*
            DO 20 J = 1, I - NB, NB
*
*              Apply H' to A(J:M,I:I+IB-1) from the left
*
               CALL SLARFB( 'Left', 'Transpose', 'Forward',
     $                      'Columnwise', M-J+1, IB, NB,
     $                      A( J, J ), LDA, WORK(J), LBWORK,
     $                      A( J, I ), LDA, WORK(LBWORK*NB+NT*NT+1),
     $                      IB)

20          CONTINUE
*
*           Compute the QR factorization of the current block
*           A(I:M,I:I+IB-1)
*
            CALL SGEQR2( M-I+1, IB, A( I, I ), LDA, TAU( I ),
     $                        WORK(LBWORK*NB+NT*NT+1), IINFO )

            IF( I+IB.LE.N ) THEN
*
*              Form the triangular factor of the block reflector
*              H = H(i) H(i+1) . . . H(i+ib-1)
*
               CALL SLARFT( 'Forward', 'Columnwise', M-I+1, IB,
     $                      A( I, I ), LDA, TAU( I ),
     $                      WORK(I), LBWORK )
*
            END IF
   10    CONTINUE
      ELSE
         I = 1
      END IF
*
*     Use unblocked code to factor the last or only block.
*
      IF( I.LE.K ) THEN

         IF ( I .NE. 1 )   THEN

             DO 30 J = 1, I - NB, NB
*
*                Apply H' to A(J:M,I:K) from the left
*
                 CALL SLARFB( 'Left', 'Transpose', 'Forward',
     $                       'Columnwise', M-J+1, K-I+1, NB,
     $                       A( J, J ), LDA, WORK(J), LBWORK,
     $                       A( J, I ), LDA, WORK(LBWORK*NB+NT*NT+1),
     $                       K-I+1)
30           CONTINUE

             CALL SGEQR2( M-I+1, K-I+1, A( I, I ), LDA, TAU( I ),
     $                   WORK(LBWORK*NB+NT*NT+1),IINFO )

         ELSE
*
*        Use unblocked code to factor the last or only block.
*
         CALL SGEQR2( M-I+1, N-I+1, A( I, I ), LDA, TAU( I ),
     $               WORK,IINFO )

         END IF
      END IF


*
*     Apply update to the column M+1:N when N > M
*
      IF ( M.LT.N .AND. I.NE.1) THEN
*
*         Form the last triangular factor of the block reflector
*         H = H(i) H(i+1) . . . H(i+ib-1)
*
          IF ( NT .LE. NB ) THEN
               CALL SLARFT( 'Forward', 'Columnwise', M-I+1, K-I+1,
     $                     A( I, I ), LDA, TAU( I ), WORK(I), LBWORK )
          ELSE
               CALL SLARFT( 'Forward', 'Columnwise', M-I+1, K-I+1,
     $                     A( I, I ), LDA, TAU( I ),
     $                     WORK(LBWORK*NB+1), NT )
          END IF

*
*         Apply H' to A(1:M,M+1:N) from the left
*
          DO 40 J = 1, K-NX, NB

               IB = MIN( K-J+1, NB )

               CALL SLARFB( 'Left', 'Transpose', 'Forward',
     $                     'Columnwise', M-J+1, N-M, IB,
     $                     A( J, J ), LDA, WORK(J), LBWORK,
     $                     A( J, M+1 ), LDA, WORK(LBWORK*NB+NT*NT+1),
     $                     N-M)

40       CONTINUE

         IF ( NT.LE.NB ) THEN
             CALL SLARFB( 'Left', 'Transpose', 'Forward',
     $                   'Columnwise', M-J+1, N-M, K-J+1,
     $                   A( J, J ), LDA, WORK(J), LBWORK,
     $                   A( J, M+1 ), LDA, WORK(LBWORK*NB+NT*NT+1),
     $                   N-M)
         ELSE
             CALL SLARFB( 'Left', 'Transpose', 'Forward',
     $                   'Columnwise', M-J+1, N-M, K-J+1,
     $                   A( J, J ), LDA,
     $                   WORK(LBWORK*NB+1),
     $                   NT, A( J, M+1 ), LDA, WORK(LBWORK*NB+NT*NT+1),
     $                   N-M)
         END IF

      END IF

      WORK( 1 ) = IWS
      RETURN
*
*     End of SGEQRF
*
      END
