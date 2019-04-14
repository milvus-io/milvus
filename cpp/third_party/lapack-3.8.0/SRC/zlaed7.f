*> \brief \b ZLAED7 used by sstedc. Computes the updated eigensystem of a diagonal matrix after modification by a rank-one symmetric matrix. Used when the original matrix is dense.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZLAED7 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zlaed7.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zlaed7.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zlaed7.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZLAED7( N, CUTPNT, QSIZ, TLVLS, CURLVL, CURPBM, D, Q,
*                          LDQ, RHO, INDXQ, QSTORE, QPTR, PRMPTR, PERM,
*                          GIVPTR, GIVCOL, GIVNUM, WORK, RWORK, IWORK,
*                          INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            CURLVL, CURPBM, CUTPNT, INFO, LDQ, N, QSIZ,
*      $                   TLVLS
*       DOUBLE PRECISION   RHO
*       ..
*       .. Array Arguments ..
*       INTEGER            GIVCOL( 2, * ), GIVPTR( * ), INDXQ( * ),
*      $                   IWORK( * ), PERM( * ), PRMPTR( * ), QPTR( * )
*       DOUBLE PRECISION   D( * ), GIVNUM( 2, * ), QSTORE( * ), RWORK( * )
*       COMPLEX*16         Q( LDQ, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZLAED7 computes the updated eigensystem of a diagonal
*> matrix after modification by a rank-one symmetric matrix. This
*> routine is used only for the eigenproblem which requires all
*> eigenvalues and optionally eigenvectors of a dense or banded
*> Hermitian matrix that has been reduced to tridiagonal form.
*>
*>   T = Q(in) ( D(in) + RHO * Z*Z**H ) Q**H(in) = Q(out) * D(out) * Q**H(out)
*>
*>   where Z = Q**Hu, u is a vector of length N with ones in the
*>   CUTPNT and CUTPNT + 1 th elements and zeros elsewhere.
*>
*>    The eigenvectors of the original matrix are stored in Q, and the
*>    eigenvalues are in D.  The algorithm consists of three stages:
*>
*>       The first stage consists of deflating the size of the problem
*>       when there are multiple eigenvalues or if there is a zero in
*>       the Z vector.  For each such occurrence the dimension of the
*>       secular equation problem is reduced by one.  This stage is
*>       performed by the routine DLAED2.
*>
*>       The second stage consists of calculating the updated
*>       eigenvalues. This is done by finding the roots of the secular
*>       equation via the routine DLAED4 (as called by SLAED3).
*>       This routine also calculates the eigenvectors of the current
*>       problem.
*>
*>       The final stage consists of computing the updated eigenvectors
*>       directly using the updated eigenvalues.  The eigenvectors for
*>       the current problem are multiplied with the eigenvectors from
*>       the overall problem.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>         The dimension of the symmetric tridiagonal matrix.  N >= 0.
*> \endverbatim
*>
*> \param[in] CUTPNT
*> \verbatim
*>          CUTPNT is INTEGER
*>         Contains the location of the last eigenvalue in the leading
*>         sub-matrix.  min(1,N) <= CUTPNT <= N.
*> \endverbatim
*>
*> \param[in] QSIZ
*> \verbatim
*>          QSIZ is INTEGER
*>         The dimension of the unitary matrix used to reduce
*>         the full matrix to tridiagonal form.  QSIZ >= N.
*> \endverbatim
*>
*> \param[in] TLVLS
*> \verbatim
*>          TLVLS is INTEGER
*>         The total number of merging levels in the overall divide and
*>         conquer tree.
*> \endverbatim
*>
*> \param[in] CURLVL
*> \verbatim
*>          CURLVL is INTEGER
*>         The current level in the overall merge routine,
*>         0 <= curlvl <= tlvls.
*> \endverbatim
*>
*> \param[in] CURPBM
*> \verbatim
*>          CURPBM is INTEGER
*>         The current problem in the current level in the overall
*>         merge routine (counting from upper left to lower right).
*> \endverbatim
*>
*> \param[in,out] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension (N)
*>         On entry, the eigenvalues of the rank-1-perturbed matrix.
*>         On exit, the eigenvalues of the repaired matrix.
*> \endverbatim
*>
*> \param[in,out] Q
*> \verbatim
*>          Q is COMPLEX*16 array, dimension (LDQ,N)
*>         On entry, the eigenvectors of the rank-1-perturbed matrix.
*>         On exit, the eigenvectors of the repaired tridiagonal matrix.
*> \endverbatim
*>
*> \param[in] LDQ
*> \verbatim
*>          LDQ is INTEGER
*>         The leading dimension of the array Q.  LDQ >= max(1,N).
*> \endverbatim
*>
*> \param[in] RHO
*> \verbatim
*>          RHO is DOUBLE PRECISION
*>         Contains the subdiagonal element used to create the rank-1
*>         modification.
*> \endverbatim
*>
*> \param[out] INDXQ
*> \verbatim
*>          INDXQ is INTEGER array, dimension (N)
*>         This contains the permutation which will reintegrate the
*>         subproblem just solved back into sorted order,
*>         ie. D( INDXQ( I = 1, N ) ) will be in ascending order.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (4*N)
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array,
*>                                 dimension (3*N+2*QSIZ*N)
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (QSIZ*N)
*> \endverbatim
*>
*> \param[in,out] QSTORE
*> \verbatim
*>          QSTORE is DOUBLE PRECISION array, dimension (N**2+1)
*>         Stores eigenvectors of submatrices encountered during
*>         divide and conquer, packed together. QPTR points to
*>         beginning of the submatrices.
*> \endverbatim
*>
*> \param[in,out] QPTR
*> \verbatim
*>          QPTR is INTEGER array, dimension (N+2)
*>         List of indices pointing to beginning of submatrices stored
*>         in QSTORE. The submatrices are numbered starting at the
*>         bottom left of the divide and conquer tree, from left to
*>         right and bottom to top.
*> \endverbatim
*>
*> \param[in] PRMPTR
*> \verbatim
*>          PRMPTR is INTEGER array, dimension (N lg N)
*>         Contains a list of pointers which indicate where in PERM a
*>         level's permutation is stored.  PRMPTR(i+1) - PRMPTR(i)
*>         indicates the size of the permutation and also the size of
*>         the full, non-deflated problem.
*> \endverbatim
*>
*> \param[in] PERM
*> \verbatim
*>          PERM is INTEGER array, dimension (N lg N)
*>         Contains the permutations (from deflation and sorting) to be
*>         applied to each eigenblock.
*> \endverbatim
*>
*> \param[in] GIVPTR
*> \verbatim
*>          GIVPTR is INTEGER array, dimension (N lg N)
*>         Contains a list of pointers which indicate where in GIVCOL a
*>         level's Givens rotations are stored.  GIVPTR(i+1) - GIVPTR(i)
*>         indicates the number of Givens rotations.
*> \endverbatim
*>
*> \param[in] GIVCOL
*> \verbatim
*>          GIVCOL is INTEGER array, dimension (2, N lg N)
*>         Each pair of numbers indicates a pair of columns to take place
*>         in a Givens rotation.
*> \endverbatim
*>
*> \param[in] GIVNUM
*> \verbatim
*>          GIVNUM is DOUBLE PRECISION array, dimension (2, N lg N)
*>         Each number indicates the S value to be used in the
*>         corresponding Givens rotation.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit.
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
*>          > 0:  if INFO = 1, an eigenvalue did not converge
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
*> \date June 2016
*
*> \ingroup complex16OTHERcomputational
*
*  =====================================================================
      SUBROUTINE ZLAED7( N, CUTPNT, QSIZ, TLVLS, CURLVL, CURPBM, D, Q,
     $                   LDQ, RHO, INDXQ, QSTORE, QPTR, PRMPTR, PERM,
     $                   GIVPTR, GIVCOL, GIVNUM, WORK, RWORK, IWORK,
     $                   INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      INTEGER            CURLVL, CURPBM, CUTPNT, INFO, LDQ, N, QSIZ,
     $                   TLVLS
      DOUBLE PRECISION   RHO
*     ..
*     .. Array Arguments ..
      INTEGER            GIVCOL( 2, * ), GIVPTR( * ), INDXQ( * ),
     $                   IWORK( * ), PERM( * ), PRMPTR( * ), QPTR( * )
      DOUBLE PRECISION   D( * ), GIVNUM( 2, * ), QSTORE( * ), RWORK( * )
      COMPLEX*16         Q( LDQ, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      INTEGER            COLTYP, CURR, I, IDLMDA, INDX,
     $                   INDXC, INDXP, IQ, IW, IZ, K, N1, N2, PTR
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLAED9, DLAEDA, DLAMRG, XERBLA, ZLACRM, ZLAED8
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
*
*     IF( ICOMPQ.LT.0 .OR. ICOMPQ.GT.1 ) THEN
*        INFO = -1
*     ELSE IF( N.LT.0 ) THEN
      IF( N.LT.0 ) THEN
         INFO = -1
      ELSE IF( MIN( 1, N ).GT.CUTPNT .OR. N.LT.CUTPNT ) THEN
         INFO = -2
      ELSE IF( QSIZ.LT.N ) THEN
         INFO = -3
      ELSE IF( LDQ.LT.MAX( 1, N ) ) THEN
         INFO = -9
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZLAED7', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 )
     $   RETURN
*
*     The following values are for bookkeeping purposes only.  They are
*     integer pointers which indicate the portion of the workspace
*     used by a particular array in DLAED2 and SLAED3.
*
      IZ = 1
      IDLMDA = IZ + N
      IW = IDLMDA + N
      IQ = IW + N
*
      INDX = 1
      INDXC = INDX + N
      COLTYP = INDXC + N
      INDXP = COLTYP + N
*
*     Form the z-vector which consists of the last row of Q_1 and the
*     first row of Q_2.
*
      PTR = 1 + 2**TLVLS
      DO 10 I = 1, CURLVL - 1
         PTR = PTR + 2**( TLVLS-I )
   10 CONTINUE
      CURR = PTR + CURPBM
      CALL DLAEDA( N, TLVLS, CURLVL, CURPBM, PRMPTR, PERM, GIVPTR,
     $             GIVCOL, GIVNUM, QSTORE, QPTR, RWORK( IZ ),
     $             RWORK( IZ+N ), INFO )
*
*     When solving the final problem, we no longer need the stored data,
*     so we will overwrite the data from this level onto the previously
*     used storage space.
*
      IF( CURLVL.EQ.TLVLS ) THEN
         QPTR( CURR ) = 1
         PRMPTR( CURR ) = 1
         GIVPTR( CURR ) = 1
      END IF
*
*     Sort and Deflate eigenvalues.
*
      CALL ZLAED8( K, N, QSIZ, Q, LDQ, D, RHO, CUTPNT, RWORK( IZ ),
     $             RWORK( IDLMDA ), WORK, QSIZ, RWORK( IW ),
     $             IWORK( INDXP ), IWORK( INDX ), INDXQ,
     $             PERM( PRMPTR( CURR ) ), GIVPTR( CURR+1 ),
     $             GIVCOL( 1, GIVPTR( CURR ) ),
     $             GIVNUM( 1, GIVPTR( CURR ) ), INFO )
      PRMPTR( CURR+1 ) = PRMPTR( CURR ) + N
      GIVPTR( CURR+1 ) = GIVPTR( CURR+1 ) + GIVPTR( CURR )
*
*     Solve Secular Equation.
*
      IF( K.NE.0 ) THEN
         CALL DLAED9( K, 1, K, N, D, RWORK( IQ ), K, RHO,
     $                RWORK( IDLMDA ), RWORK( IW ),
     $                QSTORE( QPTR( CURR ) ), K, INFO )
         CALL ZLACRM( QSIZ, K, WORK, QSIZ, QSTORE( QPTR( CURR ) ), K, Q,
     $                LDQ, RWORK( IQ ) )
         QPTR( CURR+1 ) = QPTR( CURR ) + K**2
         IF( INFO.NE.0 ) THEN
            RETURN
         END IF
*
*     Prepare the INDXQ sorting premutation.
*
         N1 = K
         N2 = N - K
         CALL DLAMRG( N1, N2, D, 1, -1, INDXQ )
      ELSE
         QPTR( CURR+1 ) = QPTR( CURR )
         DO 20 I = 1, N
            INDXQ( I ) = I
   20    CONTINUE
      END IF
*
      RETURN
*
*     End of ZLAED7
*
      END
