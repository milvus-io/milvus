*> \brief \b DPSTF2 computes the Cholesky factorization with complete pivoting of a real symmetric positive semidefinite matrix.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DPSTF2 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dpstf2.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dpstf2.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dpstf2.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DPSTF2( UPLO, N, A, LDA, PIV, RANK, TOL, WORK, INFO )
*
*       .. Scalar Arguments ..
*       DOUBLE PRECISION   TOL
*       INTEGER            INFO, LDA, N, RANK
*       CHARACTER          UPLO
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   A( LDA, * ), WORK( 2*N )
*       INTEGER            PIV( N )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DPSTF2 computes the Cholesky factorization with complete
*> pivoting of a real symmetric positive semidefinite matrix A.
*>
*> The factorization has the form
*>    P**T * A * P = U**T * U ,  if UPLO = 'U',
*>    P**T * A * P = L  * L**T,  if UPLO = 'L',
*> where U is an upper triangular matrix and L is lower triangular, and
*> P is stored as vector PIV.
*>
*> This algorithm does not attempt to check that A is positive
*> semidefinite. This version of the algorithm calls level 2 BLAS.
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
*>          A is DOUBLE PRECISION array, dimension (LDA,N)
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
*>          TOL is DOUBLE PRECISION
*>          User defined tolerance. If TOL < 0, then N*U*MAX( A( K,K ) )
*>          will be used. The algorithm terminates at the (K-1)st step
*>          if the pivot <= TOL.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (2*N)
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
*> \ingroup doubleOTHERcomputational
*
*  =====================================================================
      SUBROUTINE DPSTF2( UPLO, N, A, LDA, PIV, RANK, TOL, WORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      DOUBLE PRECISION   TOL
      INTEGER            INFO, LDA, N, RANK
      CHARACTER          UPLO
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   A( LDA, * ), WORK( 2*N )
      INTEGER            PIV( N )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ONE, ZERO
      PARAMETER          ( ONE = 1.0D+0, ZERO = 0.0D+0 )
*     ..
*     .. Local Scalars ..
      DOUBLE PRECISION   AJJ, DSTOP, DTEMP
      INTEGER            I, ITEMP, J, PVT
      LOGICAL            UPPER
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH
      LOGICAL            LSAME, DISNAN
      EXTERNAL           DLAMCH, LSAME, DISNAN
*     ..
*     .. External Subroutines ..
      EXTERNAL           DGEMV, DSCAL, DSWAP, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, SQRT, MAXLOC
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters
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
         CALL XERBLA( 'DPSTF2', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 )
     $   RETURN
*
*     Initialize PIV
*
      DO 100 I = 1, N
         PIV( I ) = I
  100 CONTINUE
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
      IF( AJJ.LE.ZERO.OR.DISNAN( AJJ ) ) THEN
         RANK = 0
         INFO = 1
         GO TO 170
      END IF
*
*     Compute stopping value if not supplied
*
      IF( TOL.LT.ZERO ) THEN
         DSTOP = N * DLAMCH( 'Epsilon' ) * AJJ
      ELSE
         DSTOP = TOL
      END IF
*
*     Set first half of WORK to zero, holds dot products
*
      DO 110 I = 1, N
         WORK( I ) = 0
  110 CONTINUE
*
      IF( UPPER ) THEN
*
*        Compute the Cholesky factorization P**T * A * P = U**T * U
*
         DO 130 J = 1, N
*
*        Find pivot, test for exit, else swap rows and columns
*        Update dot products, compute possible pivots which are
*        stored in the second half of WORK
*
            DO 120 I = J, N
*
               IF( J.GT.1 ) THEN
                  WORK( I ) = WORK( I ) + A( J-1, I )**2
               END IF
               WORK( N+I ) = A( I, I ) - WORK( I )
*
  120       CONTINUE
*
            IF( J.GT.1 ) THEN
               ITEMP = MAXLOC( WORK( (N+J):(2*N) ), 1 )
               PVT = ITEMP + J - 1
               AJJ = WORK( N+PVT )
               IF( AJJ.LE.DSTOP.OR.DISNAN( AJJ ) ) THEN
                  A( J, J ) = AJJ
                  GO TO 160
               END IF
            END IF
*
            IF( J.NE.PVT ) THEN
*
*              Pivot OK, so can now swap pivot rows and columns
*
               A( PVT, PVT ) = A( J, J )
               CALL DSWAP( J-1, A( 1, J ), 1, A( 1, PVT ), 1 )
               IF( PVT.LT.N )
     $            CALL DSWAP( N-PVT, A( J, PVT+1 ), LDA,
     $                        A( PVT, PVT+1 ), LDA )
               CALL DSWAP( PVT-J-1, A( J, J+1 ), LDA, A( J+1, PVT ), 1 )
*
*              Swap dot products and PIV
*
               DTEMP = WORK( J )
               WORK( J ) = WORK( PVT )
               WORK( PVT ) = DTEMP
               ITEMP = PIV( PVT )
               PIV( PVT ) = PIV( J )
               PIV( J ) = ITEMP
            END IF
*
            AJJ = SQRT( AJJ )
            A( J, J ) = AJJ
*
*           Compute elements J+1:N of row J
*
            IF( J.LT.N ) THEN
               CALL DGEMV( 'Trans', J-1, N-J, -ONE, A( 1, J+1 ), LDA,
     $                     A( 1, J ), 1, ONE, A( J, J+1 ), LDA )
               CALL DSCAL( N-J, ONE / AJJ, A( J, J+1 ), LDA )
            END IF
*
  130    CONTINUE
*
      ELSE
*
*        Compute the Cholesky factorization P**T * A * P = L * L**T
*
         DO 150 J = 1, N
*
*        Find pivot, test for exit, else swap rows and columns
*        Update dot products, compute possible pivots which are
*        stored in the second half of WORK
*
            DO 140 I = J, N
*
               IF( J.GT.1 ) THEN
                  WORK( I ) = WORK( I ) + A( I, J-1 )**2
               END IF
               WORK( N+I ) = A( I, I ) - WORK( I )
*
  140       CONTINUE
*
            IF( J.GT.1 ) THEN
               ITEMP = MAXLOC( WORK( (N+J):(2*N) ), 1 )
               PVT = ITEMP + J - 1
               AJJ = WORK( N+PVT )
               IF( AJJ.LE.DSTOP.OR.DISNAN( AJJ ) ) THEN
                  A( J, J ) = AJJ
                  GO TO 160
               END IF
            END IF
*
            IF( J.NE.PVT ) THEN
*
*              Pivot OK, so can now swap pivot rows and columns
*
               A( PVT, PVT ) = A( J, J )
               CALL DSWAP( J-1, A( J, 1 ), LDA, A( PVT, 1 ), LDA )
               IF( PVT.LT.N )
     $            CALL DSWAP( N-PVT, A( PVT+1, J ), 1, A( PVT+1, PVT ),
     $                        1 )
               CALL DSWAP( PVT-J-1, A( J+1, J ), 1, A( PVT, J+1 ), LDA )
*
*              Swap dot products and PIV
*
               DTEMP = WORK( J )
               WORK( J ) = WORK( PVT )
               WORK( PVT ) = DTEMP
               ITEMP = PIV( PVT )
               PIV( PVT ) = PIV( J )
               PIV( J ) = ITEMP
            END IF
*
            AJJ = SQRT( AJJ )
            A( J, J ) = AJJ
*
*           Compute elements J+1:N of column J
*
            IF( J.LT.N ) THEN
               CALL DGEMV( 'No Trans', N-J, J-1, -ONE, A( J+1, 1 ), LDA,
     $                     A( J, 1 ), LDA, ONE, A( J+1, J ), 1 )
               CALL DSCAL( N-J, ONE / AJJ, A( J+1, J ), 1 )
            END IF
*
  150    CONTINUE
*
      END IF
*
*     Ran to completion, A has full rank
*
      RANK = N
*
      GO TO 170
  160 CONTINUE
*
*     Rank is number of steps completed.  Set INFO = 1 to signal
*     that the factorization cannot be used to solve a system.
*
      RANK = J - 1
      INFO = 1
*
  170 CONTINUE
      RETURN
*
*     End of DPSTF2
*
      END
