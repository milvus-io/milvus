*> \brief \b DSYTRS_AA_2STAGE
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DSYTRS_AA_2STAGE + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dsytrs_aa_2stage.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dsytrs_aa_2stage.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dsytrs_aa_2stage.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*      SUBROUTINE DSYTRS_AA_2STAGE( UPLO, N, NRHS, A, LDA, TB, LTB, IPIV, 
*                                   IPIV2, B, LDB, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            N, NRHS, LDA, LTB, LDB, INFO
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * ), IPIV2( * )
*       DOUBLE PRECISION   A( LDA, * ), TB( * ), B( LDB, * )
*       ..
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DSYTRS_AA_2STAGE solves a system of linear equations A*X = B with a real
*> symmetric matrix A using the factorization A = U*T*U**T or
*> A = L*T*L**T computed by DSYTRF_AA_2STAGE.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          Specifies whether the details of the factorization are stored
*>          as an upper or lower triangular matrix.
*>          = 'U':  Upper triangular, form is A = U*T*U**T;
*>          = 'L':  Lower triangular, form is A = L*T*L**T.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] NRHS
*> \verbatim
*>          NRHS is INTEGER
*>          The number of right hand sides, i.e., the number of columns
*>          of the matrix B.  NRHS >= 0.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension (LDA,N)
*>          Details of factors computed by DSYTRF_AA_2STAGE.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[out] TB
*> \verbatim
*>          TB is DOUBLE PRECISION array, dimension (LTB)
*>          Details of factors computed by DSYTRF_AA_2STAGE.
*> \endverbatim
*>
*> \param[in] LTB
*> \verbatim
*>          The size of the array TB. LTB >= 4*N.
*> \endverbatim
*>
*> \param[in] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (N)
*>          Details of the interchanges as computed by
*>          DSYTRF_AA_2STAGE.
*> \endverbatim
*>
*> \param[in] IPIV2
*> \verbatim
*>          IPIV2 is INTEGER array, dimension (N)
*>          Details of the interchanges as computed by
*>          DSYTRF_AA_2STAGE.
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is DOUBLE PRECISION array, dimension (LDB,NRHS)
*>          On entry, the right hand side matrix B.
*>          On exit, the solution matrix X.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B.  LDB >= max(1,N).
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
*> \ingroup doubleSYcomputational
*
*  =====================================================================
      SUBROUTINE DSYTRS_AA_2STAGE( UPLO, N, NRHS, A, LDA, TB, LTB,
     $                             IPIV, IPIV2, B, LDB, INFO )
*
*  -- LAPACK computational routine (version 3.8.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2017
*
      IMPLICIT NONE
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            N, NRHS, LDA, LTB, LDB, INFO
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * ), IPIV2( * )
      DOUBLE PRECISION   A( LDA, * ), TB( * ), B( LDB, * )
*     ..
*
*  =====================================================================
*
      DOUBLE PRECISION   ONE
      PARAMETER          ( ONE = 1.0D+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            LDTB, NB
      LOGICAL            UPPER
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           DGBTRS, DLASWP, DTRSM, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX
*     ..
*     .. Executable Statements ..
*
      INFO = 0
      UPPER = LSAME( UPLO, 'U' )
      IF( .NOT.UPPER .AND. .NOT.LSAME( UPLO, 'L' ) ) THEN
         INFO = -1
      ELSE IF( N.LT.0 ) THEN
         INFO = -2
      ELSE IF( NRHS.LT.0 ) THEN
         INFO = -3
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -5
      ELSE IF( LTB.LT.( 4*N ) ) THEN
         INFO = -7
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -11
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DSYTRS_AA_2STAGE', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 .OR. NRHS.EQ.0 )
     $   RETURN
*
*     Read NB and compute LDTB
*
      NB = INT( TB( 1 ) )
      LDTB = LTB/N
*
      IF( UPPER ) THEN
*
*        Solve A*X = B, where A = U*T*U**T.
*
         IF( N.GT.NB ) THEN
*
*           Pivot, P**T * B
*
            CALL DLASWP( NRHS, B, LDB, NB+1, N, IPIV, 1 )
*
*           Compute (U**T \P**T * B) -> B    [ (U**T \P**T * B) ]
*
            CALL DTRSM( 'L', 'U', 'T', 'U', N-NB, NRHS, ONE, A(1, NB+1),
     $                 LDA, B(NB+1, 1), LDB)
*
         END IF
*
*        Compute T \ B -> B   [ T \ (U**T \P**T * B) ]
*
         CALL DGBTRS( 'N', N, NB, NB, NRHS, TB, LDTB, IPIV2, B, LDB,
     $               INFO)
         IF( N.GT.NB ) THEN
*
*           Compute (U \ B) -> B   [ U \ (T \ (U**T \P**T * B) ) ]
*
            CALL DTRSM( 'L', 'U', 'N', 'U', N-NB, NRHS, ONE, A(1, NB+1),
     $                  LDA, B(NB+1, 1), LDB)
*
*           Pivot, P * B  [ P * (U \ (T \ (U**T \P**T * B) )) ]
*
            CALL DLASWP( NRHS, B, LDB, NB+1, N, IPIV, -1 )
*
         END IF
*
      ELSE
*
*        Solve A*X = B, where A = L*T*L**T.
*
         IF( N.GT.NB ) THEN
*
*           Pivot, P**T * B
*
            CALL DLASWP( NRHS, B, LDB, NB+1, N, IPIV, 1 )
*
*           Compute (L \P**T * B) -> B    [ (L \P**T * B) ]
*
            CALL DTRSM( 'L', 'L', 'N', 'U', N-NB, NRHS, ONE, A(NB+1, 1),
     $                 LDA, B(NB+1, 1), LDB)
*
         END IF
*
*        Compute T \ B -> B   [ T \ (L \P**T * B) ]
*
         CALL DGBTRS( 'N', N, NB, NB, NRHS, TB, LDTB, IPIV2, B, LDB,
     $               INFO)
         IF( N.GT.NB ) THEN
*
*           Compute (L**T \ B) -> B   [ L**T \ (T \ (L \P**T * B) ) ]
*
            CALL DTRSM( 'L', 'L', 'T', 'U', N-NB, NRHS, ONE, A(NB+1, 1),
     $                  LDA, B(NB+1, 1), LDB)
*
*           Pivot, P * B  [ P * (L**T \ (T \ (L \P**T * B) )) ]
*
            CALL DLASWP( NRHS, B, LDB, NB+1, N, IPIV, -1 )
*
         END IF
      END IF
*
      RETURN
*
*     End of DSYTRS_AA_2STAGE
*
      END
