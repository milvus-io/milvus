*> \brief \b CLAUUM computes the product UUH or LHL, where U and L are upper or lower triangular matrices (blocked algorithm).
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CLAUUM + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/clauum.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/clauum.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/clauum.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CLAUUM( UPLO, N, A, LDA, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            INFO, LDA, N
*       ..
*       .. Array Arguments ..
*       COMPLEX            A( LDA, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CLAUUM computes the product U * U**H or L**H * L, where the triangular
*> factor U or L is stored in the upper or lower triangular part of
*> the array A.
*>
*> If UPLO = 'U' or 'u' then the upper triangle of the result is stored,
*> overwriting the factor U in A.
*> If UPLO = 'L' or 'l' then the lower triangle of the result is stored,
*> overwriting the factor L in A.
*>
*> This is the blocked form of the algorithm, calling Level 3 BLAS.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          Specifies whether the triangular factor stored in the array A
*>          is upper or lower triangular:
*>          = 'U':  Upper triangular
*>          = 'L':  Lower triangular
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the triangular factor U or L.  N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N)
*>          On entry, the triangular factor U or L.
*>          On exit, if UPLO = 'U', the upper triangle of A is
*>          overwritten with the upper triangle of the product U * U**H;
*>          if UPLO = 'L', the lower triangle of A is overwritten with
*>          the lower triangle of the product L**H * L.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0: successful exit
*>          < 0: if INFO = -k, the k-th argument had an illegal value
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
*> \ingroup complexOTHERauxiliary
*
*  =====================================================================
      SUBROUTINE CLAUUM( UPLO, N, A, LDA, INFO )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            INFO, LDA, N
*     ..
*     .. Array Arguments ..
      COMPLEX            A( LDA, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ONE
      PARAMETER          ( ONE = 1.0E+0 )
      COMPLEX            CONE
      PARAMETER          ( CONE = ( 1.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            UPPER
      INTEGER            I, IB, NB
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV
      EXTERNAL           LSAME, ILAENV
*     ..
*     .. External Subroutines ..
      EXTERNAL           CGEMM, CHERK, CLAUU2, CTRMM, XERBLA
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
         CALL XERBLA( 'CLAUUM', -INFO )
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
      NB = ILAENV( 1, 'CLAUUM', UPLO, N, -1, -1, -1 )
*
      IF( NB.LE.1 .OR. NB.GE.N ) THEN
*
*        Use unblocked code
*
         CALL CLAUU2( UPLO, N, A, LDA, INFO )
      ELSE
*
*        Use blocked code
*
         IF( UPPER ) THEN
*
*           Compute the product U * U**H.
*
            DO 10 I = 1, N, NB
               IB = MIN( NB, N-I+1 )
               CALL CTRMM( 'Right', 'Upper', 'Conjugate transpose',
     $                     'Non-unit', I-1, IB, CONE, A( I, I ), LDA,
     $                     A( 1, I ), LDA )
               CALL CLAUU2( 'Upper', IB, A( I, I ), LDA, INFO )
               IF( I+IB.LE.N ) THEN
                  CALL CGEMM( 'No transpose', 'Conjugate transpose',
     $                        I-1, IB, N-I-IB+1, CONE, A( 1, I+IB ),
     $                        LDA, A( I, I+IB ), LDA, CONE, A( 1, I ),
     $                        LDA )
                  CALL CHERK( 'Upper', 'No transpose', IB, N-I-IB+1,
     $                        ONE, A( I, I+IB ), LDA, ONE, A( I, I ),
     $                        LDA )
               END IF
   10       CONTINUE
         ELSE
*
*           Compute the product L**H * L.
*
            DO 20 I = 1, N, NB
               IB = MIN( NB, N-I+1 )
               CALL CTRMM( 'Left', 'Lower', 'Conjugate transpose',
     $                     'Non-unit', IB, I-1, CONE, A( I, I ), LDA,
     $                     A( I, 1 ), LDA )
               CALL CLAUU2( 'Lower', IB, A( I, I ), LDA, INFO )
               IF( I+IB.LE.N ) THEN
                  CALL CGEMM( 'Conjugate transpose', 'No transpose', IB,
     $                        I-1, N-I-IB+1, CONE, A( I+IB, I ), LDA,
     $                        A( I+IB, 1 ), LDA, CONE, A( I, 1 ), LDA )
                  CALL CHERK( 'Lower', 'Conjugate transpose', IB,
     $                        N-I-IB+1, ONE, A( I+IB, I ), LDA, ONE,
     $                        A( I, I ), LDA )
               END IF
   20       CONTINUE
         END IF
      END IF
*
      RETURN
*
*     End of CLAUUM
*
      END
