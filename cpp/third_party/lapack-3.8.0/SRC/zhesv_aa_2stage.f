*> \brief <b> ZHESV_AA_2STAGE computes the solution to system of linear equations A * X = B for HE matrices</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZHESV_AA_2STAGE + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zhesv_aa_2stage.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zhesv_aa_2stage.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zhesv_aa_2stage.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*      SUBROUTINE ZHESV_AA_2STAGE( UPLO, N, NRHS, A, LDA, TB, LTB,
*                                  IPIV, IPIV2, B, LDB, WORK, LWORK,
*                                  INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            N, NRHS, LDA, LTB, LDB, LWORK, INFO
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * ), IPIV2( * )
*       COMPLEX*16         A( LDA, * ), TB( * ), B( LDB, *), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZHESV_AA_2STAGE computes the solution to a complex system of 
*> linear equations
*>    A * X = B,
*> where A is an N-by-N Hermitian matrix and X and B are N-by-NRHS
*> matrices.
*>
*> Aasen's 2-stage algorithm is used to factor A as
*>    A = U * T * U**H,  if UPLO = 'U', or
*>    A = L * T * L**H,  if UPLO = 'L',
*> where U (or L) is a product of permutation and unit upper (lower)
*> triangular matrices, and T is Hermitian and band. The matrix T is
*> then LU-factored with partial pivoting. The factored form of A
*> is then used to solve the system of equations A * X = B.
*>
*> This is the blocked version of the algorithm, calling Level 3 BLAS.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          = 'U':  Upper triangle of A is stored;
*>          = 'L':  Lower triangle of A is stored.
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
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>          On entry, the hermitian matrix A.  If UPLO = 'U', the leading
*>          N-by-N upper triangular part of A contains the upper
*>          triangular part of the matrix A, and the strictly lower
*>          triangular part of A is not referenced.  If UPLO = 'L', the
*>          leading N-by-N lower triangular part of A contains the lower
*>          triangular part of the matrix A, and the strictly upper
*>          triangular part of A is not referenced.
*>
*>          On exit, L is stored below (or above) the subdiaonal blocks,
*>          when UPLO  is 'L' (or 'U').
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
*>          TB is COMPLEX*16 array, dimension (LTB)
*>          On exit, details of the LU factorization of the band matrix.
*> \endverbatim
*>
*> \param[in] LTB
*> \verbatim
*>          The size of the array TB. LTB >= 4*N, internally
*>          used to select NB such that LTB >= (3*NB+1)*N.
*>
*>          If LTB = -1, then a workspace query is assumed; the
*>          routine only calculates the optimal size of LTB, 
*>          returns this value as the first entry of TB, and
*>          no error message related to LTB is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (N)
*>          On exit, it contains the details of the interchanges, i.e.,
*>          the row and column k of A were interchanged with the
*>          row and column IPIV(k).
*> \endverbatim
*>
*> \param[out] IPIV2
*> \verbatim
*>          IPIV is INTEGER array, dimension (N)
*>          On exit, it contains the details of the interchanges, i.e.,
*>          the row and column k of T were interchanged with the
*>          row and column IPIV(k).
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX*16 array, dimension (LDB,NRHS)
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
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 workspace of size LWORK
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          The size of WORK. LWORK >= N, internally used to select NB
*>          such that LWORK >= N*NB.
*>
*>          If LWORK = -1, then a workspace query is assumed; the
*>          routine only calculates the optimal size of the WORK array,
*>          returns this value as the first entry of the WORK array, and
*>          no error message related to LWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
*>          > 0:  if INFO = i, band LU factorization failed on i-th column
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
*> \ingroup complex16HEsolve
*
*  =====================================================================
      SUBROUTINE ZHESV_AA_2STAGE( UPLO, N, NRHS, A, LDA, TB, LTB,
     $                            IPIV, IPIV2, B, LDB, WORK, LWORK,
     $                            INFO )
*
*  -- LAPACK driver routine (version 3.8.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2017
*
      IMPLICIT NONE
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            N, NRHS, LDA, LDB, LTB, LWORK, INFO
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * ), IPIV2( * )
      COMPLEX*16         A( LDA, * ), B( LDB, * ), TB( * ), WORK( * )
*     ..
*
*  =====================================================================
*     .. Parameters ..
      COMPLEX*16         ZERO, ONE
      PARAMETER          ( ZERO = ( 0.0D+0, 0.0D+0 ),
     $                     ONE  = ( 1.0D+0, 0.0D+0 ) )
*
*     .. Local Scalars ..
      LOGICAL            UPPER, TQUERY, WQUERY
      INTEGER            I, J, K, I1, I2, TD
      INTEGER            LDTB, LWKOPT, NB, KB, NT, IINFO
      COMPLEX            PIV
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV
      EXTERNAL           LSAME, ILAENV
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA, ZHETRF_AA_2STAGE, ZHETRS_AA_2STAGE
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
      UPPER = LSAME( UPLO, 'U' )
      WQUERY = ( LWORK.EQ.-1 )
      TQUERY = ( LTB.EQ.-1 )
      IF( .NOT.UPPER .AND. .NOT.LSAME( UPLO, 'L' ) ) THEN
         INFO = -1
      ELSE IF( N.LT.0 ) THEN
         INFO = -2
      ELSE IF( NRHS.LT.0 ) THEN
         INFO = -3
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -5
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -11
      END IF
*
      IF( INFO.EQ.0 ) THEN
         CALL ZHETRF_AA_2STAGE( UPLO, N, A, LDA, TB, -1, IPIV,
     $                          IPIV2, WORK, -1, INFO )
         LWKOPT = INT( WORK(1) )
         IF( LTB.LT.INT( TB(1) ) .AND. .NOT.TQUERY ) THEN
            INFO = -7
         ELSE IF( LWORK.LT.LWKOPT .AND. .NOT.WQUERY ) THEN
            INFO = -13
         END IF
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZHESV_AA_2STAGE', -INFO )
         RETURN
      ELSE IF( WQUERY .OR. TQUERY ) THEN
         RETURN
      END IF
*
*     Compute the factorization A = U*T*U**H or A = L*T*L**H.
*
      CALL ZHETRF_AA_2STAGE( UPLO, N, A, LDA, TB, LTB, IPIV, IPIV2,
     $                       WORK, LWORK, INFO )
      IF( INFO.EQ.0 ) THEN
*
*        Solve the system A*X = B, overwriting B with X.
*
         CALL ZHETRS_AA_2STAGE( UPLO, N, NRHS, A, LDA, TB, LTB, IPIV,
     $                          IPIV2, B, LDB, INFO )
*
      END IF
*
      WORK( 1 ) = LWKOPT
*
      RETURN
*
*     End of ZHESV_AA_2STAGE
*
      END
