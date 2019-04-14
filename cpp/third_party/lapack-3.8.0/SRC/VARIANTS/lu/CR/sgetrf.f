C> \brief \b SGETRF VARIANT: Crout Level 3 BLAS version of the algorithm.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SGETRF ( M, N, A, LDA, IPIV, INFO)
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, M, N
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * )
*       REAL               A( LDA, * )
*       ..
*
*  Purpose
*  =======
*
C>\details \b Purpose:
C>\verbatim
C>
C> SGETRF computes an LU factorization of a general M-by-N matrix A
C> using partial pivoting with row interchanges.
C>
C> The factorization has the form
C>    A = P * L * U
C> where P is a permutation matrix, L is lower triangular with unit
C> diagonal elements (lower trapezoidal if m > n), and U is upper
C> triangular (upper trapezoidal if m < n).
C>
C> This is the Crout Level 3 BLAS version of the algorithm.
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
C>          On entry, the M-by-N matrix to be factored.
C>          On exit, the factors L and U from the factorization
C>          A = P*L*U; the unit diagonal elements of L are not stored.
C> \endverbatim
C>
C> \param[in] LDA
C> \verbatim
C>          LDA is INTEGER
C>          The leading dimension of the array A.  LDA >= max(1,M).
C> \endverbatim
C>
C> \param[out] IPIV
C> \verbatim
C>          IPIV is INTEGER array, dimension (min(M,N))
C>          The pivot indices; for 1 <= i <= min(M,N), row i of the
C>          matrix was interchanged with row IPIV(i).
C> \endverbatim
C>
C> \param[out] INFO
C> \verbatim
C>          INFO is INTEGER
C>          = 0:  successful exit
C>          < 0:  if INFO = -i, the i-th argument had an illegal value
C>          > 0:  if INFO = i, U(i,i) is exactly zero. The factorization
C>                has been completed, but the factor U is exactly
C>                singular, and division by zero will occur if it is used
C>                to solve a system of equations.
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
*  =====================================================================
      SUBROUTINE SGETRF ( M, N, A, LDA, IPIV, INFO)
*
*  -- LAPACK computational routine (version 3.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, M, N
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * )
      REAL               A( LDA, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ONE
      PARAMETER          ( ONE = 1.0E+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, IINFO, J, JB, NB
*     ..
*     .. External Subroutines ..
      EXTERNAL           SGEMM, SGETF2, SLASWP, STRSM, XERBLA
*     ..
*     .. External Functions ..
      INTEGER            ILAENV
      EXTERNAL           ILAENV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
      IF( M.LT.0 ) THEN
         INFO = -1
      ELSE IF( N.LT.0 ) THEN
         INFO = -2
      ELSE IF( LDA.LT.MAX( 1, M ) ) THEN
         INFO = -4
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SGETRF', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( M.EQ.0 .OR. N.EQ.0 )
     $   RETURN
*
*     Determine the block size for this environment.
*
      NB = ILAENV( 1, 'SGETRF', ' ', M, N, -1, -1 )
      IF( NB.LE.1 .OR. NB.GE.MIN( M, N ) ) THEN
*
*        Use unblocked code.
*
         CALL SGETF2( M, N, A, LDA, IPIV, INFO )
      ELSE
*
*        Use blocked code.
*
         DO 20 J = 1, MIN( M, N ), NB
            JB = MIN( MIN( M, N )-J+1, NB )
*
*           Update current block.
*
            CALL SGEMM( 'No transpose', 'No transpose',
     $                 M-J+1, JB, J-1, -ONE,
     $                 A( J, 1 ), LDA, A( 1, J ), LDA, ONE,
     $                 A( J, J ), LDA )

*
*           Factor diagonal and subdiagonal blocks and test for exact
*           singularity.
*
            CALL SGETF2( M-J+1, JB, A( J, J ), LDA, IPIV( J ), IINFO )
*
*           Adjust INFO and the pivot indices.
*
            IF( INFO.EQ.0 .AND. IINFO.GT.0 )
     $         INFO = IINFO + J - 1
            DO 10 I = J, MIN( M, J+JB-1 )
               IPIV( I ) = J - 1 + IPIV( I )
   10       CONTINUE
*
*           Apply interchanges to column 1:J-1
*
            CALL SLASWP( J-1, A, LDA, J, J+JB-1, IPIV, 1 )
*
            IF ( J+JB.LE.N ) THEN
*
*              Apply interchanges to column J+JB:N
*
               CALL SLASWP( N-J-JB+1, A( 1, J+JB ), LDA, J, J+JB-1,
     $                     IPIV, 1 )
*
               CALL SGEMM( 'No transpose', 'No transpose',
     $                    JB, N-J-JB+1, J-1, -ONE,
     $                    A( J, 1 ), LDA, A( 1, J+JB ), LDA, ONE,
     $                    A( J, J+JB ), LDA )
*
*              Compute block row of U.
*
               CALL STRSM( 'Left', 'Lower', 'No transpose', 'Unit',
     $                    JB, N-J-JB+1, ONE, A( J, J ), LDA,
     $                    A( J, J+JB ), LDA )
            END IF

   20    CONTINUE

      END IF
      RETURN
*
*     End of SGETRF
*
      END
