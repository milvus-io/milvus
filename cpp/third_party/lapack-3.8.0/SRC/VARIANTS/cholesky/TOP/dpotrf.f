C> \brief \b DPOTRF VARIANT: top-looking block version of the algorithm, calling Level 3 BLAS.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DPOTRF ( UPLO, N, A, LDA, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            INFO, LDA, N
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   A( LDA, * )
*       ..
*
*  Purpose
*  =======
*
C>\details \b Purpose:
C>\verbatim
C>
C> DPOTRF computes the Cholesky factorization of a real symmetric
C> positive definite matrix A.
C>
C> The factorization has the form
C>    A = U**T * U,  if UPLO = 'U', or
C>    A = L  * L**T,  if UPLO = 'L',
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
C>          A is DOUBLE PRECISION array, dimension (LDA,N)
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
C>          factorization A = U**T*U or A = L*L**T.
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
      SUBROUTINE DPOTRF ( UPLO, N, A, LDA, INFO )
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
      DOUBLE PRECISION   A( LDA, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ONE
      PARAMETER          ( ONE = 1.0D+0 )
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
      EXTERNAL           DGEMM, DPOTF2, DSYRK, DTRSM, XERBLA
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
         CALL XERBLA( 'DPOTRF', -INFO )
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
      NB = ILAENV( 1, 'DPOTRF', UPLO, N, -1, -1, -1 )
      IF( NB.LE.1 .OR. NB.GE.N ) THEN
*
*        Use unblocked code.
*
         CALL DPOTF2( UPLO, N, A, LDA, INFO )
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
               CALL DTRSM( 'Left', 'Upper', 'Transpose', 'Non-unit',
     $                      J-1, JB, ONE, A( 1, 1 ), LDA,
     $                      A( 1, J ), LDA )

               CALL DSYRK( 'Upper', 'Transpose', JB, J-1, -ONE,
     $                      A( 1, J ), LDA,
     $                      ONE, A( J, J ), LDA )
*
*              Update and factorize the current diagonal block and test
*              for non-positive-definiteness.
*
               CALL DPOTF2( 'Upper', JB, A( J, J ), LDA, INFO )
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
               CALL DTRSM( 'Right', 'Lower', 'Transpose', 'Non-unit',
     $                     JB, J-1, ONE, A( 1, 1 ), LDA,
     $                     A( J, 1 ), LDA )

               CALL DSYRK( 'Lower', 'No Transpose', JB, J-1,
     $                     -ONE, A( J, 1 ), LDA,
     $                     ONE, A( J, J ), LDA )

*
*              Update and factorize the current diagonal block and test
*              for non-positive-definiteness.
*
               CALL DPOTF2( 'Lower', JB, A( J, J ), LDA, INFO )
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
*     End of DPOTRF
*
      END
