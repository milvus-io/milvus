*> \brief \b SLAEDA used by sstedc. Computes the Z vector determining the rank-one modification of the diagonal matrix. Used when the original matrix is dense.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLAEDA + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slaeda.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slaeda.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slaeda.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLAEDA( N, TLVLS, CURLVL, CURPBM, PRMPTR, PERM, GIVPTR,
*                          GIVCOL, GIVNUM, Q, QPTR, Z, ZTEMP, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            CURLVL, CURPBM, INFO, N, TLVLS
*       ..
*       .. Array Arguments ..
*       INTEGER            GIVCOL( 2, * ), GIVPTR( * ), PERM( * ),
*      $                   PRMPTR( * ), QPTR( * )
*       REAL               GIVNUM( 2, * ), Q( * ), Z( * ), ZTEMP( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLAEDA computes the Z vector corresponding to the merge step in the
*> CURLVLth step of the merge process with TLVLS steps for the CURPBMth
*> problem.
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
*> \param[in] PRMPTR
*> \verbatim
*>          PRMPTR is INTEGER array, dimension (N lg N)
*>         Contains a list of pointers which indicate where in PERM a
*>         level's permutation is stored.  PRMPTR(i+1) - PRMPTR(i)
*>         indicates the size of the permutation and incidentally the
*>         size of the full, non-deflated problem.
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
*>          GIVNUM is REAL array, dimension (2, N lg N)
*>         Each number indicates the S value to be used in the
*>         corresponding Givens rotation.
*> \endverbatim
*>
*> \param[in] Q
*> \verbatim
*>          Q is REAL array, dimension (N**2)
*>         Contains the square eigenblocks from previous levels, the
*>         starting positions for blocks are given by QPTR.
*> \endverbatim
*>
*> \param[in] QPTR
*> \verbatim
*>          QPTR is INTEGER array, dimension (N+2)
*>         Contains a list of pointers which indicate where in Q an
*>         eigenblock is stored.  SQRT( QPTR(i+1) - QPTR(i) ) indicates
*>         the size of the block.
*> \endverbatim
*>
*> \param[out] Z
*> \verbatim
*>          Z is REAL array, dimension (N)
*>         On output this vector contains the updating vector (the last
*>         row of the first sub-eigenvector matrix and the first row of
*>         the second sub-eigenvector matrix).
*> \endverbatim
*>
*> \param[out] ZTEMP
*> \verbatim
*>          ZTEMP is REAL array, dimension (N)
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit.
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
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
*> \ingroup auxOTHERcomputational
*
*> \par Contributors:
*  ==================
*>
*> Jeff Rutter, Computer Science Division, University of California
*> at Berkeley, USA
*
*  =====================================================================
      SUBROUTINE SLAEDA( N, TLVLS, CURLVL, CURPBM, PRMPTR, PERM, GIVPTR,
     $                   GIVCOL, GIVNUM, Q, QPTR, Z, ZTEMP, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            CURLVL, CURPBM, INFO, N, TLVLS
*     ..
*     .. Array Arguments ..
      INTEGER            GIVCOL( 2, * ), GIVPTR( * ), PERM( * ),
     $                   PRMPTR( * ), QPTR( * )
      REAL               GIVNUM( 2, * ), Q( * ), Z( * ), ZTEMP( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, HALF, ONE
      PARAMETER          ( ZERO = 0.0E0, HALF = 0.5E0, ONE = 1.0E0 )
*     ..
*     .. Local Scalars ..
      INTEGER            BSIZ1, BSIZ2, CURR, I, K, MID, PSIZ1, PSIZ2,
     $                   PTR, ZPTR1
*     ..
*     .. External Subroutines ..
      EXTERNAL           SCOPY, SGEMV, SROT, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          INT, REAL, SQRT
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
*
      IF( N.LT.0 ) THEN
         INFO = -1
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SLAEDA', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 )
     $   RETURN
*
*     Determine location of first number in second half.
*
      MID = N / 2 + 1
*
*     Gather last/first rows of appropriate eigenblocks into center of Z
*
      PTR = 1
*
*     Determine location of lowest level subproblem in the full storage
*     scheme
*
      CURR = PTR + CURPBM*2**CURLVL + 2**( CURLVL-1 ) - 1
*
*     Determine size of these matrices.  We add HALF to the value of
*     the SQRT in case the machine underestimates one of these square
*     roots.
*
      BSIZ1 = INT( HALF+SQRT( REAL( QPTR( CURR+1 )-QPTR( CURR ) ) ) )
      BSIZ2 = INT( HALF+SQRT( REAL( QPTR( CURR+2 )-QPTR( CURR+1 ) ) ) )
      DO 10 K = 1, MID - BSIZ1 - 1
         Z( K ) = ZERO
   10 CONTINUE
      CALL SCOPY( BSIZ1, Q( QPTR( CURR )+BSIZ1-1 ), BSIZ1,
     $            Z( MID-BSIZ1 ), 1 )
      CALL SCOPY( BSIZ2, Q( QPTR( CURR+1 ) ), BSIZ2, Z( MID ), 1 )
      DO 20 K = MID + BSIZ2, N
         Z( K ) = ZERO
   20 CONTINUE
*
*     Loop through remaining levels 1 -> CURLVL applying the Givens
*     rotations and permutation and then multiplying the center matrices
*     against the current Z.
*
      PTR = 2**TLVLS + 1
      DO 70 K = 1, CURLVL - 1
         CURR = PTR + CURPBM*2**( CURLVL-K ) + 2**( CURLVL-K-1 ) - 1
         PSIZ1 = PRMPTR( CURR+1 ) - PRMPTR( CURR )
         PSIZ2 = PRMPTR( CURR+2 ) - PRMPTR( CURR+1 )
         ZPTR1 = MID - PSIZ1
*
*       Apply Givens at CURR and CURR+1
*
         DO 30 I = GIVPTR( CURR ), GIVPTR( CURR+1 ) - 1
            CALL SROT( 1, Z( ZPTR1+GIVCOL( 1, I )-1 ), 1,
     $                 Z( ZPTR1+GIVCOL( 2, I )-1 ), 1, GIVNUM( 1, I ),
     $                 GIVNUM( 2, I ) )
   30    CONTINUE
         DO 40 I = GIVPTR( CURR+1 ), GIVPTR( CURR+2 ) - 1
            CALL SROT( 1, Z( MID-1+GIVCOL( 1, I ) ), 1,
     $                 Z( MID-1+GIVCOL( 2, I ) ), 1, GIVNUM( 1, I ),
     $                 GIVNUM( 2, I ) )
   40    CONTINUE
         PSIZ1 = PRMPTR( CURR+1 ) - PRMPTR( CURR )
         PSIZ2 = PRMPTR( CURR+2 ) - PRMPTR( CURR+1 )
         DO 50 I = 0, PSIZ1 - 1
            ZTEMP( I+1 ) = Z( ZPTR1+PERM( PRMPTR( CURR )+I )-1 )
   50    CONTINUE
         DO 60 I = 0, PSIZ2 - 1
            ZTEMP( PSIZ1+I+1 ) = Z( MID+PERM( PRMPTR( CURR+1 )+I )-1 )
   60    CONTINUE
*
*        Multiply Blocks at CURR and CURR+1
*
*        Determine size of these matrices.  We add HALF to the value of
*        the SQRT in case the machine underestimates one of these
*        square roots.
*
         BSIZ1 = INT( HALF+SQRT( REAL( QPTR( CURR+1 )-QPTR( CURR ) ) ) )
         BSIZ2 = INT( HALF+SQRT( REAL( QPTR( CURR+2 )-QPTR( CURR+
     $           1 ) ) ) )
         IF( BSIZ1.GT.0 ) THEN
            CALL SGEMV( 'T', BSIZ1, BSIZ1, ONE, Q( QPTR( CURR ) ),
     $                  BSIZ1, ZTEMP( 1 ), 1, ZERO, Z( ZPTR1 ), 1 )
         END IF
         CALL SCOPY( PSIZ1-BSIZ1, ZTEMP( BSIZ1+1 ), 1, Z( ZPTR1+BSIZ1 ),
     $               1 )
         IF( BSIZ2.GT.0 ) THEN
            CALL SGEMV( 'T', BSIZ2, BSIZ2, ONE, Q( QPTR( CURR+1 ) ),
     $                  BSIZ2, ZTEMP( PSIZ1+1 ), 1, ZERO, Z( MID ), 1 )
         END IF
         CALL SCOPY( PSIZ2-BSIZ2, ZTEMP( PSIZ1+BSIZ2+1 ), 1,
     $               Z( MID+BSIZ2 ), 1 )
*
         PTR = PTR + 2**( TLVLS-K )
   70 CONTINUE
*
      RETURN
*
*     End of SLAEDA
*
      END
