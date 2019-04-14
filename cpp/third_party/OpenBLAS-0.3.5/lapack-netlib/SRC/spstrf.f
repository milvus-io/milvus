*> \brief \b SPSTRF computes the Cholesky factorization with complete pivoting of a real symmetric positive semidefinite matrix.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SPSTRF + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/spstrf.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/spstrf.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/spstrf.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SPSTRF( UPLO, N, A, LDA, PIV, RANK, TOL, WORK, INFO )
*
*       .. Scalar Arguments ..
*       REAL               TOL
*       INTEGER            INFO, LDA, N, RANK
*       CHARACTER          UPLO
*       ..
*       .. Array Arguments ..
*       REAL               A( LDA, * ), WORK( 2*N )
*       INTEGER            PIV( N )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SPSTRF computes the Cholesky factorization with complete
*> pivoting of a real symmetric positive semidefinite matrix A.
*>
*> The factorization has the form
*>    P**T * A * P = U**T * U ,  if UPLO = 'U',
*>    P**T * A * P = L  * L**T,  if UPLO = 'L',
*> where U is an upper triangular matrix and L is lower triangular, and
*> P is stored as vector PIV.
*>
*> This algorithm does not attempt to check that A is positive
*> semidefinite. This version of the algorithm calls level 3 BLAS.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          Specifies whether the upper or lower triangular part of the
*>          symmetric matrix A is stored.
*>          = 'U':  Upper triangular
*>          = 'L':  Lower triangular
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is REAL array, dimension (LDA,N)
*>          On entry, the symmetric matrix A.  If UPLO = 'U', the leading
*>          n by n upper triangular part of A contains the upper
*>          triangular part of the matrix A, and the strictly lower
*>          triangular part of A is not referenced.  If UPLO = 'L', the
*>          leading n by n lower triangular part of A contains the lower
*>          triangular part of the matrix A, and the strictly upper
*>          triangular part of A is not referenced.
*>
*>          On exit, if INFO = 0, the factor U or L from the Cholesky
*>          factorization as above.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[out] PIV
*> \verbatim
*>          PIV is INTEGER array, dimension (N)
*>          PIV is such that the nonzero entries are P( PIV(K), K ) = 1.
*> \endverbatim
*>
*> \param[out] RANK
*> \verbatim
*>          RANK is INTEGER
*>          The rank of A given by the number of steps the algorithm
*>          completed.
*> \endverbatim
*>
*> \param[in] TOL
*> \verbatim
*>          TOL is REAL
*>          User defined tolerance. If TOL < 0, then N*U*MAX( A(K,K) )
*>          will be used. The algorithm terminates at the (K-1)st step
*>          if the pivot <= TOL.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (2*N)
*>          Work space.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          < 0: If INFO = -K, the K-th argument had an illegal value,
*>          = 0: algorithm completed successfully, and
*>          > 0: the matrix A is either rank deficient with computed rank
*>               as returned in RANK, or is not positive semidefinite. See
*>               Section 7 of LAPACK Working Note #161 for further
*>               information.
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
*> \ingroup realOTHERcomputational
*
*  =====================================================================
      SUBROUTINE SPSTRF( UPLO, N, A, LDA, PIV, RANK, TOL, WORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      REAL               TOL
      INTEGER            INFO, LDA, N, RANK
      CHARACTER          UPLO
*     ..
*     .. Array Arguments ..
      REAL               A( LDA, * ), WORK( 2*N )
      INTEGER            PIV( N )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ONE, ZERO
      PARAMETER          ( ONE = 1.0E+0, ZERO = 0.0E+0 )
*     ..
*     .. Local Scalars ..
      REAL               AJJ, SSTOP, STEMP
      INTEGER            I, ITEMP, J, JB, K, NB, PVT
      LOGICAL            UPPER
*     ..
*     .. External Functions ..
      REAL               SLAMCH
      INTEGER            ILAENV
      LOGICAL            LSAME, SISNAN
      EXTERNAL           SLAMCH, ILAENV, LSAME, SISNAN
*     ..
*     .. External Subroutines ..
      EXTERNAL           SGEMV, SPSTF2, SSCAL, SSWAP, SSYRK, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN, SQRT, MAXLOC
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
         CALL XERBLA( 'SPSTRF', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 )
     $   RETURN
*
*     Get block size
*
      NB = ILAENV( 1, 'SPOTRF', UPLO, N, -1, -1, -1 )
      IF( NB.LE.1 .OR. NB.GE.N ) THEN
*
*        Use unblocked code
*
         CALL SPSTF2( UPLO, N, A( 1, 1 ), LDA, PIV, RANK, TOL, WORK,
     $                INFO )
         GO TO 200
*
      ELSE
*
*     Initialize PIV
*
         DO 100 I = 1, N
            PIV( I ) = I
  100    CONTINUE
*
*     Compute stopping value
*
         PVT = 1
         AJJ = A( PVT, PVT )
         DO I = 2, N
            IF( A( I, I ).GT.AJJ ) THEN
               PVT = I
               AJJ = A( PVT, PVT )
            END IF
         END DO
         IF( AJJ.LE.ZERO.OR.SISNAN( AJJ ) ) THEN
            RANK = 0
            INFO = 1
            GO TO 200
         END IF
*
*     Compute stopping value if not supplied
*
         IF( TOL.LT.ZERO ) THEN
            SSTOP = N * SLAMCH( 'Epsilon' ) * AJJ
         ELSE
            SSTOP = TOL
         END IF
*
*
         IF( UPPER ) THEN
*
*           Compute the Cholesky factorization P**T * A * P = U**T * U
*
            DO 140 K = 1, N, NB
*
*              Account for last block not being NB wide
*
               JB = MIN( NB, N-K+1 )
*
*              Set relevant part of first half of WORK to zero,
*              holds dot products
*
               DO 110 I = K, N
                  WORK( I ) = 0
  110          CONTINUE
*
               DO 130 J = K, K + JB - 1
*
*              Find pivot, test for exit, else swap rows and columns
*              Update dot products, compute possible pivots which are
*              stored in the second half of WORK
*
                  DO 120 I = J, N
*
                     IF( J.GT.K ) THEN
                        WORK( I ) = WORK( I ) + A( J-1, I )**2
                     END IF
                     WORK( N+I ) = A( I, I ) - WORK( I )
*
  120             CONTINUE
*
                  IF( J.GT.1 ) THEN
                     ITEMP = MAXLOC( WORK( (N+J):(2*N) ), 1 )
                     PVT = ITEMP + J - 1
                     AJJ = WORK( N+PVT )
                     IF( AJJ.LE.SSTOP.OR.SISNAN( AJJ ) ) THEN
                        A( J, J ) = AJJ
                        GO TO 190
                     END IF
                  END IF
*
                  IF( J.NE.PVT ) THEN
*
*                    Pivot OK, so can now swap pivot rows and columns
*
                     A( PVT, PVT ) = A( J, J )
                     CALL SSWAP( J-1, A( 1, J ), 1, A( 1, PVT ), 1 )
                     IF( PVT.LT.N )
     $                  CALL SSWAP( N-PVT, A( J, PVT+1 ), LDA,
     $                              A( PVT, PVT+1 ), LDA )
                     CALL SSWAP( PVT-J-1, A( J, J+1 ), LDA,
     $                           A( J+1, PVT ), 1 )
*
*                    Swap dot products and PIV
*
                     STEMP = WORK( J )
                     WORK( J ) = WORK( PVT )
                     WORK( PVT ) = STEMP
                     ITEMP = PIV( PVT )
                     PIV( PVT ) = PIV( J )
                     PIV( J ) = ITEMP
                  END IF
*
                  AJJ = SQRT( AJJ )
                  A( J, J ) = AJJ
*
*                 Compute elements J+1:N of row J.
*
                  IF( J.LT.N ) THEN
                     CALL SGEMV( 'Trans', J-K, N-J, -ONE, A( K, J+1 ),
     $                           LDA, A( K, J ), 1, ONE, A( J, J+1 ),
     $                           LDA )
                     CALL SSCAL( N-J, ONE / AJJ, A( J, J+1 ), LDA )
                  END IF
*
  130          CONTINUE
*
*              Update trailing matrix, J already incremented
*
               IF( K+JB.LE.N ) THEN
                  CALL SSYRK( 'Upper', 'Trans', N-J+1, JB, -ONE,
     $                        A( K, J ), LDA, ONE, A( J, J ), LDA )
               END IF
*
  140       CONTINUE
*
         ELSE
*
*        Compute the Cholesky factorization P**T * A * P = L * L**T
*
            DO 180 K = 1, N, NB
*
*              Account for last block not being NB wide
*
               JB = MIN( NB, N-K+1 )
*
*              Set relevant part of first half of WORK to zero,
*              holds dot products
*
               DO 150 I = K, N
                  WORK( I ) = 0
  150          CONTINUE
*
               DO 170 J = K, K + JB - 1
*
*              Find pivot, test for exit, else swap rows and columns
*              Update dot products, compute possible pivots which are
*              stored in the second half of WORK
*
                  DO 160 I = J, N
*
                     IF( J.GT.K ) THEN
                        WORK( I ) = WORK( I ) + A( I, J-1 )**2
                     END IF
                     WORK( N+I ) = A( I, I ) - WORK( I )
*
  160             CONTINUE
*
                  IF( J.GT.1 ) THEN
                     ITEMP = MAXLOC( WORK( (N+J):(2*N) ), 1 )
                     PVT = ITEMP + J - 1
                     AJJ = WORK( N+PVT )
                     IF( AJJ.LE.SSTOP.OR.SISNAN( AJJ ) ) THEN
                        A( J, J ) = AJJ
                        GO TO 190
                     END IF
                  END IF
*
                  IF( J.NE.PVT ) THEN
*
*                    Pivot OK, so can now swap pivot rows and columns
*
                     A( PVT, PVT ) = A( J, J )
                     CALL SSWAP( J-1, A( J, 1 ), LDA, A( PVT, 1 ), LDA )
                     IF( PVT.LT.N )
     $                  CALL SSWAP( N-PVT, A( PVT+1, J ), 1,
     $                              A( PVT+1, PVT ), 1 )
                     CALL SSWAP( PVT-J-1, A( J+1, J ), 1, A( PVT, J+1 ),
     $                           LDA )
*
*                    Swap dot products and PIV
*
                     STEMP = WORK( J )
                     WORK( J ) = WORK( PVT )
                     WORK( PVT ) = STEMP
                     ITEMP = PIV( PVT )
                     PIV( PVT ) = PIV( J )
                     PIV( J ) = ITEMP
                  END IF
*
                  AJJ = SQRT( AJJ )
                  A( J, J ) = AJJ
*
*                 Compute elements J+1:N of column J.
*
                  IF( J.LT.N ) THEN
                     CALL SGEMV( 'No Trans', N-J, J-K, -ONE,
     $                           A( J+1, K ), LDA, A( J, K ), LDA, ONE,
     $                           A( J+1, J ), 1 )
                     CALL SSCAL( N-J, ONE / AJJ, A( J+1, J ), 1 )
                  END IF
*
  170          CONTINUE
*
*              Update trailing matrix, J already incremented
*
               IF( K+JB.LE.N ) THEN
                  CALL SSYRK( 'Lower', 'No Trans', N-J+1, JB, -ONE,
     $                        A( J, K ), LDA, ONE, A( J, J ), LDA )
               END IF
*
  180       CONTINUE
*
         END IF
      END IF
*
*     Ran to completion, A has full rank
*
      RANK = N
*
      GO TO 200
  190 CONTINUE
*
*     Rank is the number of steps completed.  Set INFO = 1 to signal
*     that the factorization cannot be used to solve a system.
*
      RANK = J - 1
      INFO = 1
*
  200 CONTINUE
      RETURN
*
*     End of SPSTRF
*
      END
