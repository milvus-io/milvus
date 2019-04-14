*> \brief \b SQPT01
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       REAL             FUNCTION SQPT01( M, N, K, A, AF, LDA, TAU, JPVT,
*                        WORK, LWORK )
*
*       .. Scalar Arguments ..
*       INTEGER            K, LDA, LWORK, M, N
*       ..
*       .. Array Arguments ..
*       INTEGER            JPVT( * )
*       REAL               A( LDA, * ), AF( LDA, * ), TAU( * ),
*      $                   WORK( LWORK )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SQPT01 tests the QR-factorization with pivoting of a matrix A.  The
*> array AF contains the (possibly partial) QR-factorization of A, where
*> the upper triangle of AF(1:k,1:k) is a partial triangular factor,
*> the entries below the diagonal in the first k columns are the
*> Householder vectors, and the rest of AF contains a partially updated
*> matrix.
*>
*> This function returns ||A*P - Q*R||/(||norm(A)||*eps*M)
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrices A and AF.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrices A and AF.
*> \endverbatim
*>
*> \param[in] K
*> \verbatim
*>          K is INTEGER
*>          The number of columns of AF that have been reduced
*>          to upper triangular form.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is REAL array, dimension (LDA, N)
*>          The original matrix A.
*> \endverbatim
*>
*> \param[in] AF
*> \verbatim
*>          AF is REAL array, dimension (LDA,N)
*>          The (possibly partial) output of SGEQPF.  The upper triangle
*>          of AF(1:k,1:k) is a partial triangular factor, the entries
*>          below the diagonal in the first k columns are the Householder
*>          vectors, and the rest of AF contains a partially updated
*>          matrix.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the arrays A and AF.
*> \endverbatim
*>
*> \param[in] TAU
*> \verbatim
*>          TAU is REAL array, dimension (K)
*>          Details of the Householder transformations as returned by
*>          SGEQPF.
*> \endverbatim
*>
*> \param[in] JPVT
*> \verbatim
*>          JPVT is INTEGER array, dimension (N)
*>          Pivot information as returned by SGEQPF.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (LWORK)
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The length of the array WORK.  LWORK >= M*N+N.
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
*> \ingroup single_lin
*
*  =====================================================================
      REAL             FUNCTION SQPT01( M, N, K, A, AF, LDA, TAU, JPVT,
     $                 WORK, LWORK )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            K, LDA, LWORK, M, N
*     ..
*     .. Array Arguments ..
      INTEGER            JPVT( * )
      REAL               A( LDA, * ), AF( LDA, * ), TAU( * ),
     $                   WORK( LWORK )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E0, ONE = 1.0E0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, INFO, J
      REAL               NORMA
*     ..
*     .. Local Arrays ..
      REAL               RWORK( 1 )
*     ..
*     .. External Functions ..
      REAL               SLAMCH, SLANGE
      EXTERNAL           SLAMCH, SLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           SAXPY, SCOPY, SORMQR, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN, REAL
*     ..
*     .. Executable Statements ..
*
      SQPT01 = ZERO
*
*     Test if there is enough workspace
*
      IF( LWORK.LT.M*N+N ) THEN
         CALL XERBLA( 'SQPT01', 10 )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( M.LE.0 .OR. N.LE.0 )
     $   RETURN
*
      NORMA = SLANGE( 'One-norm', M, N, A, LDA, RWORK )
*
      DO 30 J = 1, K
         DO 10 I = 1, MIN( J, M )
            WORK( ( J-1 )*M+I ) = AF( I, J )
   10    CONTINUE
         DO 20 I = J + 1, M
            WORK( ( J-1 )*M+I ) = ZERO
   20    CONTINUE
   30 CONTINUE
      DO 40 J = K + 1, N
         CALL SCOPY( M, AF( 1, J ), 1, WORK( ( J-1 )*M+1 ), 1 )
   40 CONTINUE
*
      CALL SORMQR( 'Left', 'No transpose', M, N, K, AF, LDA, TAU, WORK,
     $             M, WORK( M*N+1 ), LWORK-M*N, INFO )
*
      DO 50 J = 1, N
*
*        Compare i-th column of QR and jpvt(i)-th column of A
*
         CALL SAXPY( M, -ONE, A( 1, JPVT( J ) ), 1, WORK( ( J-1 )*M+1 ),
     $               1 )
   50 CONTINUE
*
      SQPT01 = SLANGE( 'One-norm', M, N, WORK, M, RWORK ) /
     $         ( REAL( MAX( M, N ) )*SLAMCH( 'Epsilon' ) )
      IF( NORMA.NE.ZERO )
     $   SQPT01 = SQPT01 / NORMA
*
      RETURN
*
*     End of SQPT01
*
      END
