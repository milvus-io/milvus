C> \brief \b ZPOTRF VARIANT: top-looking block version of the algorithm, calling Level 3 BLAS.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZPOTRF ( UPLO, N, A, LDA, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            INFO, LDA, N
*       ..
*       .. Array Arguments ..
*       COMPLEX*16         A( LDA, * )
*       ..
*
*  Purpose
*  =======
*
C>\details \b Purpose:
C>\verbatim
C>
C> ZPOTRF computes the Cholesky factorization of a real symmetric
C> positive definite matrix A.
C>
C> The factorization has the form
C>    A = U**H * U,  if UPLO = 'U', or
C>    A = L  * L**H,  if UPLO = 'L',
C> where U is an upper triangular matrix and L is lower triangular.
C>
C> This is the top-looking block version of the algorithm, calling Level 3 BLAS.
C>
C>\endverbatim
*
*  Arguments:
*  ==========
*
C> \param[in] UPLO
C> \verbatim
C>          UPLO is CHARACTER*1
C>          = 'U':  Upper triangle of A is stored;
C>          = 'L':  Lower triangle of A is stored.
C> \endverbatim
C>
C> \param[in] N
C> \verbatim
C>          N is INTEGER
C>          The order of the matrix A.  N >= 0.
C> \endverbatim
C>
C> \param[in,out] A
C> \verbatim
C>          A is COMPLEX*16 array, dimension (LDA,N)
C>          On entry, the symmetric matrix A.  If UPLO = 'U', the leading
C>          N-by-N upper triangular part of A contains the upper
C>          triangular part of the matrix A, and the strictly lower
C>          triangular part of A is not referenced.  If UPLO = 'L', the
C>          leading N-by-N lower triangular part of A contains the lower
C>          triangular part of the matrix A, and the strictly upper
C>          triangular part of A is not referenced.
C> \endverbatim
C> \verbatim
C>          On exit, if INFO = 0, the factor U or L from the Cholesky
C>          factorization A = U**H*U or A = L*L**H.
C> \endverbatim
C>
C> \param[in] LDA
C> \verbatim
C>          LDA is INTEGER
C>          The leading dimension of the array A.  LDA >= max(1,N).
C> \endverbatim
C>
C> \param[out] INFO
C> \verbatim
C>          INFO is INTEGER
C>          = 0:  successful exit
C>          < 0:  if INFO = -i, the i-th argument had an illegal value
C>          > 0:  if INFO = i, the leading minor of order i is not
C>                positive definite, and the factorization could not be
C>                completed.
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
C> \ingroup variantsPOcomputational
*
*  =====================================================================
      SUBROUTINE ZPOTRF ( UPLO, N, A, LDA, INFO )
*
*  -- LAPACK computational routine (version 3.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            INFO, LDA, N
*     ..
*     .. Array Arguments ..
      COMPLEX*16         A( LDA, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ONE
      COMPLEX*16         CONE
      PARAMETER          ( ONE = 1.0D+0, CONE = ( 1.0D+0, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            UPPER
      INTEGER            J, JB, NB
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV
      EXTERNAL           LSAME, ILAENV
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZGEMM, ZPOTF2, ZHERK, ZTRSM, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN
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
         CALL XERBLA( 'ZPOTRF', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 )
     $   RETURN
*
*     Determine the block size for this environment.
*
      NB = ILAENV( 1, 'ZPOTRF', UPLO, N, -1, -1, -1 )
      IF( NB.LE.1 .OR. NB.GE.N ) THEN
*
*        Use unblocked code.
*
         CALL ZPOTF2( UPLO, N, A, LDA, INFO )
      ELSE
*
*        Use blocked code.
*
         IF( UPPER ) THEN
*
*           Compute the Cholesky factorization A = U'*U.
*
            DO 10 J = 1, N, NB

               JB = MIN( NB, N-J+1 )
*
*              Compute the current block.
*
               CALL ZTRSM( 'Left', 'Upper', 'Conjugate Transpose',
     $                      'Non-unit', J-1, JB, CONE, A( 1, 1 ), LDA,
     $                      A( 1, J ), LDA )

               CALL ZHERK( 'Upper', 'Conjugate Transpose', JB, J-1,
     $                      -ONE, A( 1, J ), LDA, ONE, A( J, J ), LDA )
*
*              Update and factorize the current diagonal block and test
*              for non-positive-definiteness.
*
               CALL ZPOTF2( 'Upper', JB, A( J, J ), LDA, INFO )
               IF( INFO.NE.0 )
     $            GO TO 30

   10       CONTINUE
*
         ELSE
*
*           Compute the Cholesky factorization A = L*L'.
*
            DO 20 J = 1, N, NB

               JB = MIN( NB, N-J+1 )
*
*              Compute the current block.
*
               CALL ZTRSM( 'Right', 'Lower', 'Conjugate Transpose',
     $                     'Non-unit', JB, J-1, CONE, A( 1, 1 ), LDA,
     $                     A( J, 1 ), LDA )

               CALL ZHERK( 'Lower', 'No Transpose', JB, J-1,
     $                     -ONE, A( J, 1 ), LDA,
     $                     ONE, A( J, J ), LDA )
*
*              Update and factorize the current diagonal block and test
*              for non-positive-definiteness.
*
               CALL ZPOTF2( 'Lower', JB, A( J, J ), LDA, INFO )
               IF( INFO.NE.0 )
     $            GO TO 30

   20       CONTINUE
         END IF
      END IF
      GO TO 40
*
   30 CONTINUE
      INFO = INFO + J - 1
*
   40 CONTINUE
      RETURN
*
*     End of ZPOTRF
*
      END
