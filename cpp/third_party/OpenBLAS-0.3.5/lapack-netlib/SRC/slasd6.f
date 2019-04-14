*> \brief \b SLASD6 computes the SVD of an updated upper bidiagonal matrix obtained by merging two smaller ones by appending a row. Used by sbdsdc.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLASD6 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slasd6.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slasd6.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slasd6.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLASD6( ICOMPQ, NL, NR, SQRE, D, VF, VL, ALPHA, BETA,
*                          IDXQ, PERM, GIVPTR, GIVCOL, LDGCOL, GIVNUM,
*                          LDGNUM, POLES, DIFL, DIFR, Z, K, C, S, WORK,
*                          IWORK, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            GIVPTR, ICOMPQ, INFO, K, LDGCOL, LDGNUM, NL,
*      $                   NR, SQRE
*       REAL               ALPHA, BETA, C, S
*       ..
*       .. Array Arguments ..
*       INTEGER            GIVCOL( LDGCOL, * ), IDXQ( * ), IWORK( * ),
*      $                   PERM( * )
*       REAL               D( * ), DIFL( * ), DIFR( * ),
*      $                   GIVNUM( LDGNUM, * ), POLES( LDGNUM, * ),
*      $                   VF( * ), VL( * ), WORK( * ), Z( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLASD6 computes the SVD of an updated upper bidiagonal matrix B
*> obtained by merging two smaller ones by appending a row. This
*> routine is used only for the problem which requires all singular
*> values and optionally singular vector matrices in factored form.
*> B is an N-by-M matrix with N = NL + NR + 1 and M = N + SQRE.
*> A related subroutine, SLASD1, handles the case in which all singular
*> values and singular vectors of the bidiagonal matrix are desired.
*>
*> SLASD6 computes the SVD as follows:
*>
*>               ( D1(in)    0    0       0 )
*>   B = U(in) * (   Z1**T   a   Z2**T    b ) * VT(in)
*>               (   0       0   D2(in)   0 )
*>
*>     = U(out) * ( D(out) 0) * VT(out)
*>
*> where Z**T = (Z1**T a Z2**T b) = u**T VT**T, and u is a vector of dimension M
*> with ALPHA and BETA in the NL+1 and NL+2 th entries and zeros
*> elsewhere; and the entry b is empty if SQRE = 0.
*>
*> The singular values of B can be computed using D1, D2, the first
*> components of all the right singular vectors of the lower block, and
*> the last components of all the right singular vectors of the upper
*> block. These components are stored and updated in VF and VL,
*> respectively, in SLASD6. Hence U and VT are not explicitly
*> referenced.
*>
*> The singular values are stored in D. The algorithm consists of two
*> stages:
*>
*>       The first stage consists of deflating the size of the problem
*>       when there are multiple singular values or if there is a zero
*>       in the Z vector. For each such occurrence the dimension of the
*>       secular equation problem is reduced by one. This stage is
*>       performed by the routine SLASD7.
*>
*>       The second stage consists of calculating the updated
*>       singular values. This is done by finding the roots of the
*>       secular equation via the routine SLASD4 (as called by SLASD8).
*>       This routine also updates VF and VL and computes the distances
*>       between the updated singular values and the old singular
*>       values.
*>
*> SLASD6 is called from SLASDA.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] ICOMPQ
*> \verbatim
*>          ICOMPQ is INTEGER
*>         Specifies whether singular vectors are to be computed in
*>         factored form:
*>         = 0: Compute singular values only.
*>         = 1: Compute singular vectors in factored form as well.
*> \endverbatim
*>
*> \param[in] NL
*> \verbatim
*>          NL is INTEGER
*>         The row dimension of the upper block.  NL >= 1.
*> \endverbatim
*>
*> \param[in] NR
*> \verbatim
*>          NR is INTEGER
*>         The row dimension of the lower block.  NR >= 1.
*> \endverbatim
*>
*> \param[in] SQRE
*> \verbatim
*>          SQRE is INTEGER
*>         = 0: the lower block is an NR-by-NR square matrix.
*>         = 1: the lower block is an NR-by-(NR+1) rectangular matrix.
*>
*>         The bidiagonal matrix has row dimension N = NL + NR + 1,
*>         and column dimension M = N + SQRE.
*> \endverbatim
*>
*> \param[in,out] D
*> \verbatim
*>          D is REAL array, dimension (NL+NR+1).
*>         On entry D(1:NL,1:NL) contains the singular values of the
*>         upper block, and D(NL+2:N) contains the singular values
*>         of the lower block. On exit D(1:N) contains the singular
*>         values of the modified matrix.
*> \endverbatim
*>
*> \param[in,out] VF
*> \verbatim
*>          VF is REAL array, dimension (M)
*>         On entry, VF(1:NL+1) contains the first components of all
*>         right singular vectors of the upper block; and VF(NL+2:M)
*>         contains the first components of all right singular vectors
*>         of the lower block. On exit, VF contains the first components
*>         of all right singular vectors of the bidiagonal matrix.
*> \endverbatim
*>
*> \param[in,out] VL
*> \verbatim
*>          VL is REAL array, dimension (M)
*>         On entry, VL(1:NL+1) contains the  last components of all
*>         right singular vectors of the upper block; and VL(NL+2:M)
*>         contains the last components of all right singular vectors of
*>         the lower block. On exit, VL contains the last components of
*>         all right singular vectors of the bidiagonal matrix.
*> \endverbatim
*>
*> \param[in,out] ALPHA
*> \verbatim
*>          ALPHA is REAL
*>         Contains the diagonal element associated with the added row.
*> \endverbatim
*>
*> \param[in,out] BETA
*> \verbatim
*>          BETA is REAL
*>         Contains the off-diagonal element associated with the added
*>         row.
*> \endverbatim
*>
*> \param[in,out] IDXQ
*> \verbatim
*>          IDXQ is INTEGER array, dimension (N)
*>         This contains the permutation which will reintegrate the
*>         subproblem just solved back into sorted order, i.e.
*>         D( IDXQ( I = 1, N ) ) will be in ascending order.
*> \endverbatim
*>
*> \param[out] PERM
*> \verbatim
*>          PERM is INTEGER array, dimension ( N )
*>         The permutations (from deflation and sorting) to be applied
*>         to each block. Not referenced if ICOMPQ = 0.
*> \endverbatim
*>
*> \param[out] GIVPTR
*> \verbatim
*>          GIVPTR is INTEGER
*>         The number of Givens rotations which took place in this
*>         subproblem. Not referenced if ICOMPQ = 0.
*> \endverbatim
*>
*> \param[out] GIVCOL
*> \verbatim
*>          GIVCOL is INTEGER array, dimension ( LDGCOL, 2 )
*>         Each pair of numbers indicates a pair of columns to take place
*>         in a Givens rotation. Not referenced if ICOMPQ = 0.
*> \endverbatim
*>
*> \param[in] LDGCOL
*> \verbatim
*>          LDGCOL is INTEGER
*>         leading dimension of GIVCOL, must be at least N.
*> \endverbatim
*>
*> \param[out] GIVNUM
*> \verbatim
*>          GIVNUM is REAL array, dimension ( LDGNUM, 2 )
*>         Each number indicates the C or S value to be used in the
*>         corresponding Givens rotation. Not referenced if ICOMPQ = 0.
*> \endverbatim
*>
*> \param[in] LDGNUM
*> \verbatim
*>          LDGNUM is INTEGER
*>         The leading dimension of GIVNUM and POLES, must be at least N.
*> \endverbatim
*>
*> \param[out] POLES
*> \verbatim
*>          POLES is REAL array, dimension ( LDGNUM, 2 )
*>         On exit, POLES(1,*) is an array containing the new singular
*>         values obtained from solving the secular equation, and
*>         POLES(2,*) is an array containing the poles in the secular
*>         equation. Not referenced if ICOMPQ = 0.
*> \endverbatim
*>
*> \param[out] DIFL
*> \verbatim
*>          DIFL is REAL array, dimension ( N )
*>         On exit, DIFL(I) is the distance between I-th updated
*>         (undeflated) singular value and the I-th (undeflated) old
*>         singular value.
*> \endverbatim
*>
*> \param[out] DIFR
*> \verbatim
*>          DIFR is REAL array,
*>                   dimension ( LDDIFR, 2 ) if ICOMPQ = 1 and
*>                   dimension ( K ) if ICOMPQ = 0.
*>          On exit, DIFR(I,1) = D(I) - DSIGMA(I+1), DIFR(K,1) is not
*>          defined and will not be referenced.
*>
*>          If ICOMPQ = 1, DIFR(1:K,2) is an array containing the
*>          normalizing factors for the right singular vector matrix.
*>
*>         See SLASD8 for details on DIFL and DIFR.
*> \endverbatim
*>
*> \param[out] Z
*> \verbatim
*>          Z is REAL array, dimension ( M )
*>         The first elements of this array contain the components
*>         of the deflation-adjusted updating row vector.
*> \endverbatim
*>
*> \param[out] K
*> \verbatim
*>          K is INTEGER
*>         Contains the dimension of the non-deflated matrix,
*>         This is the order of the related secular equation. 1 <= K <=N.
*> \endverbatim
*>
*> \param[out] C
*> \verbatim
*>          C is REAL
*>         C contains garbage if SQRE =0 and the C-value of a Givens
*>         rotation related to the right null space if SQRE = 1.
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is REAL
*>         S contains garbage if SQRE =0 and the S-value of a Givens
*>         rotation related to the right null space if SQRE = 1.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension ( 4 * M )
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension ( 3 * N )
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit.
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
*>          > 0:  if INFO = 1, a singular value did not converge
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
*> \ingroup OTHERauxiliary
*
*> \par Contributors:
*  ==================
*>
*>     Ming Gu and Huan Ren, Computer Science Division, University of
*>     California at Berkeley, USA
*>
*  =====================================================================
      SUBROUTINE SLASD6( ICOMPQ, NL, NR, SQRE, D, VF, VL, ALPHA, BETA,
     $                   IDXQ, PERM, GIVPTR, GIVCOL, LDGCOL, GIVNUM,
     $                   LDGNUM, POLES, DIFL, DIFR, Z, K, C, S, WORK,
     $                   IWORK, INFO )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      INTEGER            GIVPTR, ICOMPQ, INFO, K, LDGCOL, LDGNUM, NL,
     $                   NR, SQRE
      REAL               ALPHA, BETA, C, S
*     ..
*     .. Array Arguments ..
      INTEGER            GIVCOL( LDGCOL, * ), IDXQ( * ), IWORK( * ),
     $                   PERM( * )
      REAL               D( * ), DIFL( * ), DIFR( * ),
     $                   GIVNUM( LDGNUM, * ), POLES( LDGNUM, * ),
     $                   VF( * ), VL( * ), WORK( * ), Z( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ONE, ZERO
      PARAMETER          ( ONE = 1.0E+0, ZERO = 0.0E+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, IDX, IDXC, IDXP, ISIGMA, IVFW, IVLW, IW, M,
     $                   N, N1, N2
      REAL               ORGNRM
*     ..
*     .. External Subroutines ..
      EXTERNAL           SCOPY, SLAMRG, SLASCL, SLASD7, SLASD8, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
      N = NL + NR + 1
      M = N + SQRE
*
      IF( ( ICOMPQ.LT.0 ) .OR. ( ICOMPQ.GT.1 ) ) THEN
         INFO = -1
      ELSE IF( NL.LT.1 ) THEN
         INFO = -2
      ELSE IF( NR.LT.1 ) THEN
         INFO = -3
      ELSE IF( ( SQRE.LT.0 ) .OR. ( SQRE.GT.1 ) ) THEN
         INFO = -4
      ELSE IF( LDGCOL.LT.N ) THEN
         INFO = -14
      ELSE IF( LDGNUM.LT.N ) THEN
         INFO = -16
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SLASD6', -INFO )
         RETURN
      END IF
*
*     The following values are for bookkeeping purposes only.  They are
*     integer pointers which indicate the portion of the workspace
*     used by a particular array in SLASD7 and SLASD8.
*
      ISIGMA = 1
      IW = ISIGMA + N
      IVFW = IW + M
      IVLW = IVFW + M
*
      IDX = 1
      IDXC = IDX + N
      IDXP = IDXC + N
*
*     Scale.
*
      ORGNRM = MAX( ABS( ALPHA ), ABS( BETA ) )
      D( NL+1 ) = ZERO
      DO 10 I = 1, N
         IF( ABS( D( I ) ).GT.ORGNRM ) THEN
            ORGNRM = ABS( D( I ) )
         END IF
   10 CONTINUE
      CALL SLASCL( 'G', 0, 0, ORGNRM, ONE, N, 1, D, N, INFO )
      ALPHA = ALPHA / ORGNRM
      BETA = BETA / ORGNRM
*
*     Sort and Deflate singular values.
*
      CALL SLASD7( ICOMPQ, NL, NR, SQRE, K, D, Z, WORK( IW ), VF,
     $             WORK( IVFW ), VL, WORK( IVLW ), ALPHA, BETA,
     $             WORK( ISIGMA ), IWORK( IDX ), IWORK( IDXP ), IDXQ,
     $             PERM, GIVPTR, GIVCOL, LDGCOL, GIVNUM, LDGNUM, C, S,
     $             INFO )
*
*     Solve Secular Equation, compute DIFL, DIFR, and update VF, VL.
*
      CALL SLASD8( ICOMPQ, K, D, Z, VF, VL, DIFL, DIFR, LDGNUM,
     $             WORK( ISIGMA ), WORK( IW ), INFO )
*
*     Report the possible convergence failure.
*
      IF( INFO.NE.0 ) THEN
         RETURN
      END IF
*
*     Save the poles if ICOMPQ = 1.
*
      IF( ICOMPQ.EQ.1 ) THEN
         CALL SCOPY( K, D, 1, POLES( 1, 1 ), 1 )
         CALL SCOPY( K, WORK( ISIGMA ), 1, POLES( 1, 2 ), 1 )
      END IF
*
*     Unscale.
*
      CALL SLASCL( 'G', 0, 0, ONE, ORGNRM, N, 1, D, N, INFO )
*
*     Prepare the IDXQ sorting permutation.
*
      N1 = K
      N2 = N - K
      CALL SLAMRG( N1, N2, D, 1, -1, IDXQ )
*
      RETURN
*
*     End of SLASD6
*
      END
