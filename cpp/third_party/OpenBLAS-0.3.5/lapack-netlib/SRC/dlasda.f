*> \brief \b DLASDA computes the singular value decomposition (SVD) of a real upper bidiagonal matrix with diagonal d and off-diagonal e. Used by sbdsdc.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DLASDA + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlasda.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlasda.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlasda.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DLASDA( ICOMPQ, SMLSIZ, N, SQRE, D, E, U, LDU, VT, K,
*                          DIFL, DIFR, Z, POLES, GIVPTR, GIVCOL, LDGCOL,
*                          PERM, GIVNUM, C, S, WORK, IWORK, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            ICOMPQ, INFO, LDGCOL, LDU, N, SMLSIZ, SQRE
*       ..
*       .. Array Arguments ..
*       INTEGER            GIVCOL( LDGCOL, * ), GIVPTR( * ), IWORK( * ),
*      $                   K( * ), PERM( LDGCOL, * )
*       DOUBLE PRECISION   C( * ), D( * ), DIFL( LDU, * ), DIFR( LDU, * ),
*      $                   E( * ), GIVNUM( LDU, * ), POLES( LDU, * ),
*      $                   S( * ), U( LDU, * ), VT( LDU, * ), WORK( * ),
*      $                   Z( LDU, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> Using a divide and conquer approach, DLASDA computes the singular
*> value decomposition (SVD) of a real upper bidiagonal N-by-M matrix
*> B with diagonal D and offdiagonal E, where M = N + SQRE. The
*> algorithm computes the singular values in the SVD B = U * S * VT.
*> The orthogonal matrices U and VT are optionally computed in
*> compact form.
*>
*> A related subroutine, DLASD0, computes the singular values and
*> the singular vectors in explicit form.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] ICOMPQ
*> \verbatim
*>          ICOMPQ is INTEGER
*>         Specifies whether singular vectors are to be computed
*>         in compact form, as follows
*>         = 0: Compute singular values only.
*>         = 1: Compute singular vectors of upper bidiagonal
*>              matrix in compact form.
*> \endverbatim
*>
*> \param[in] SMLSIZ
*> \verbatim
*>          SMLSIZ is INTEGER
*>         The maximum size of the subproblems at the bottom of the
*>         computation tree.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>         The row dimension of the upper bidiagonal matrix. This is
*>         also the dimension of the main diagonal array D.
*> \endverbatim
*>
*> \param[in] SQRE
*> \verbatim
*>          SQRE is INTEGER
*>         Specifies the column dimension of the bidiagonal matrix.
*>         = 0: The bidiagonal matrix has column dimension M = N;
*>         = 1: The bidiagonal matrix has column dimension M = N + 1.
*> \endverbatim
*>
*> \param[in,out] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension ( N )
*>         On entry D contains the main diagonal of the bidiagonal
*>         matrix. On exit D, if INFO = 0, contains its singular values.
*> \endverbatim
*>
*> \param[in] E
*> \verbatim
*>          E is DOUBLE PRECISION array, dimension ( M-1 )
*>         Contains the subdiagonal entries of the bidiagonal matrix.
*>         On exit, E has been destroyed.
*> \endverbatim
*>
*> \param[out] U
*> \verbatim
*>          U is DOUBLE PRECISION array,
*>         dimension ( LDU, SMLSIZ ) if ICOMPQ = 1, and not referenced
*>         if ICOMPQ = 0. If ICOMPQ = 1, on exit, U contains the left
*>         singular vector matrices of all subproblems at the bottom
*>         level.
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER, LDU = > N.
*>         The leading dimension of arrays U, VT, DIFL, DIFR, POLES,
*>         GIVNUM, and Z.
*> \endverbatim
*>
*> \param[out] VT
*> \verbatim
*>          VT is DOUBLE PRECISION array,
*>         dimension ( LDU, SMLSIZ+1 ) if ICOMPQ = 1, and not referenced
*>         if ICOMPQ = 0. If ICOMPQ = 1, on exit, VT**T contains the right
*>         singular vector matrices of all subproblems at the bottom
*>         level.
*> \endverbatim
*>
*> \param[out] K
*> \verbatim
*>          K is INTEGER array,
*>         dimension ( N ) if ICOMPQ = 1 and dimension 1 if ICOMPQ = 0.
*>         If ICOMPQ = 1, on exit, K(I) is the dimension of the I-th
*>         secular equation on the computation tree.
*> \endverbatim
*>
*> \param[out] DIFL
*> \verbatim
*>          DIFL is DOUBLE PRECISION array, dimension ( LDU, NLVL ),
*>         where NLVL = floor(log_2 (N/SMLSIZ))).
*> \endverbatim
*>
*> \param[out] DIFR
*> \verbatim
*>          DIFR is DOUBLE PRECISION array,
*>                  dimension ( LDU, 2 * NLVL ) if ICOMPQ = 1 and
*>                  dimension ( N ) if ICOMPQ = 0.
*>         If ICOMPQ = 1, on exit, DIFL(1:N, I) and DIFR(1:N, 2 * I - 1)
*>         record distances between singular values on the I-th
*>         level and singular values on the (I -1)-th level, and
*>         DIFR(1:N, 2 * I ) contains the normalizing factors for
*>         the right singular vector matrix. See DLASD8 for details.
*> \endverbatim
*>
*> \param[out] Z
*> \verbatim
*>          Z is DOUBLE PRECISION array,
*>                  dimension ( LDU, NLVL ) if ICOMPQ = 1 and
*>                  dimension ( N ) if ICOMPQ = 0.
*>         The first K elements of Z(1, I) contain the components of
*>         the deflation-adjusted updating row vector for subproblems
*>         on the I-th level.
*> \endverbatim
*>
*> \param[out] POLES
*> \verbatim
*>          POLES is DOUBLE PRECISION array,
*>         dimension ( LDU, 2 * NLVL ) if ICOMPQ = 1, and not referenced
*>         if ICOMPQ = 0. If ICOMPQ = 1, on exit, POLES(1, 2*I - 1) and
*>         POLES(1, 2*I) contain  the new and old singular values
*>         involved in the secular equations on the I-th level.
*> \endverbatim
*>
*> \param[out] GIVPTR
*> \verbatim
*>          GIVPTR is INTEGER array,
*>         dimension ( N ) if ICOMPQ = 1, and not referenced if
*>         ICOMPQ = 0. If ICOMPQ = 1, on exit, GIVPTR( I ) records
*>         the number of Givens rotations performed on the I-th
*>         problem on the computation tree.
*> \endverbatim
*>
*> \param[out] GIVCOL
*> \verbatim
*>          GIVCOL is INTEGER array,
*>         dimension ( LDGCOL, 2 * NLVL ) if ICOMPQ = 1, and not
*>         referenced if ICOMPQ = 0. If ICOMPQ = 1, on exit, for each I,
*>         GIVCOL(1, 2 *I - 1) and GIVCOL(1, 2 *I) record the locations
*>         of Givens rotations performed on the I-th level on the
*>         computation tree.
*> \endverbatim
*>
*> \param[in] LDGCOL
*> \verbatim
*>          LDGCOL is INTEGER, LDGCOL = > N.
*>         The leading dimension of arrays GIVCOL and PERM.
*> \endverbatim
*>
*> \param[out] PERM
*> \verbatim
*>          PERM is INTEGER array,
*>         dimension ( LDGCOL, NLVL ) if ICOMPQ = 1, and not referenced
*>         if ICOMPQ = 0. If ICOMPQ = 1, on exit, PERM(1, I) records
*>         permutations done on the I-th level of the computation tree.
*> \endverbatim
*>
*> \param[out] GIVNUM
*> \verbatim
*>          GIVNUM is DOUBLE PRECISION array,
*>         dimension ( LDU,  2 * NLVL ) if ICOMPQ = 1, and not
*>         referenced if ICOMPQ = 0. If ICOMPQ = 1, on exit, for each I,
*>         GIVNUM(1, 2 *I - 1) and GIVNUM(1, 2 *I) record the C- and S-
*>         values of Givens rotations performed on the I-th level on
*>         the computation tree.
*> \endverbatim
*>
*> \param[out] C
*> \verbatim
*>          C is DOUBLE PRECISION array,
*>         dimension ( N ) if ICOMPQ = 1, and dimension 1 if ICOMPQ = 0.
*>         If ICOMPQ = 1 and the I-th subproblem is not square, on exit,
*>         C( I ) contains the C-value of a Givens rotation related to
*>         the right null space of the I-th subproblem.
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is DOUBLE PRECISION array, dimension ( N ) if
*>         ICOMPQ = 1, and dimension 1 if ICOMPQ = 0. If ICOMPQ = 1
*>         and the I-th subproblem is not square, on exit, S( I )
*>         contains the S-value of a Givens rotation related to
*>         the right null space of the I-th subproblem.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension
*>         (6 * N + (SMLSIZ + 1)*(SMLSIZ + 1)).
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (7*N)
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
*> \date June 2017
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
      SUBROUTINE DLASDA( ICOMPQ, SMLSIZ, N, SQRE, D, E, U, LDU, VT, K,
     $                   DIFL, DIFR, Z, POLES, GIVPTR, GIVCOL, LDGCOL,
     $                   PERM, GIVNUM, C, S, WORK, IWORK, INFO )
*
*  -- LAPACK auxiliary routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      INTEGER            ICOMPQ, INFO, LDGCOL, LDU, N, SMLSIZ, SQRE
*     ..
*     .. Array Arguments ..
      INTEGER            GIVCOL( LDGCOL, * ), GIVPTR( * ), IWORK( * ),
     $                   K( * ), PERM( LDGCOL, * )
      DOUBLE PRECISION   C( * ), D( * ), DIFL( LDU, * ), DIFR( LDU, * ),
     $                   E( * ), GIVNUM( LDU, * ), POLES( LDU, * ),
     $                   S( * ), U( LDU, * ), VT( LDU, * ), WORK( * ),
     $                   Z( LDU, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, I1, IC, IDXQ, IDXQI, IM1, INODE, ITEMP, IWK,
     $                   J, LF, LL, LVL, LVL2, M, NCC, ND, NDB1, NDIML,
     $                   NDIMR, NL, NLF, NLP1, NLVL, NR, NRF, NRP1, NRU,
     $                   NWORK1, NWORK2, SMLSZP, SQREI, VF, VFI, VL, VLI
      DOUBLE PRECISION   ALPHA, BETA
*     ..
*     .. External Subroutines ..
      EXTERNAL           DCOPY, DLASD6, DLASDQ, DLASDT, DLASET, XERBLA
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
*
      IF( ( ICOMPQ.LT.0 ) .OR. ( ICOMPQ.GT.1 ) ) THEN
         INFO = -1
      ELSE IF( SMLSIZ.LT.3 ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( ( SQRE.LT.0 ) .OR. ( SQRE.GT.1 ) ) THEN
         INFO = -4
      ELSE IF( LDU.LT.( N+SQRE ) ) THEN
         INFO = -8
      ELSE IF( LDGCOL.LT.N ) THEN
         INFO = -17
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DLASDA', -INFO )
         RETURN
      END IF
*
      M = N + SQRE
*
*     If the input matrix is too small, call DLASDQ to find the SVD.
*
      IF( N.LE.SMLSIZ ) THEN
         IF( ICOMPQ.EQ.0 ) THEN
            CALL DLASDQ( 'U', SQRE, N, 0, 0, 0, D, E, VT, LDU, U, LDU,
     $                   U, LDU, WORK, INFO )
         ELSE
            CALL DLASDQ( 'U', SQRE, N, M, N, 0, D, E, VT, LDU, U, LDU,
     $                   U, LDU, WORK, INFO )
         END IF
         RETURN
      END IF
*
*     Book-keeping and  set up the computation tree.
*
      INODE = 1
      NDIML = INODE + N
      NDIMR = NDIML + N
      IDXQ = NDIMR + N
      IWK = IDXQ + N
*
      NCC = 0
      NRU = 0
*
      SMLSZP = SMLSIZ + 1
      VF = 1
      VL = VF + M
      NWORK1 = VL + M
      NWORK2 = NWORK1 + SMLSZP*SMLSZP
*
      CALL DLASDT( N, NLVL, ND, IWORK( INODE ), IWORK( NDIML ),
     $             IWORK( NDIMR ), SMLSIZ )
*
*     for the nodes on bottom level of the tree, solve
*     their subproblems by DLASDQ.
*
      NDB1 = ( ND+1 ) / 2
      DO 30 I = NDB1, ND
*
*        IC : center row of each node
*        NL : number of rows of left  subproblem
*        NR : number of rows of right subproblem
*        NLF: starting row of the left   subproblem
*        NRF: starting row of the right  subproblem
*
         I1 = I - 1
         IC = IWORK( INODE+I1 )
         NL = IWORK( NDIML+I1 )
         NLP1 = NL + 1
         NR = IWORK( NDIMR+I1 )
         NLF = IC - NL
         NRF = IC + 1
         IDXQI = IDXQ + NLF - 2
         VFI = VF + NLF - 1
         VLI = VL + NLF - 1
         SQREI = 1
         IF( ICOMPQ.EQ.0 ) THEN
            CALL DLASET( 'A', NLP1, NLP1, ZERO, ONE, WORK( NWORK1 ),
     $                   SMLSZP )
            CALL DLASDQ( 'U', SQREI, NL, NLP1, NRU, NCC, D( NLF ),
     $                   E( NLF ), WORK( NWORK1 ), SMLSZP,
     $                   WORK( NWORK2 ), NL, WORK( NWORK2 ), NL,
     $                   WORK( NWORK2 ), INFO )
            ITEMP = NWORK1 + NL*SMLSZP
            CALL DCOPY( NLP1, WORK( NWORK1 ), 1, WORK( VFI ), 1 )
            CALL DCOPY( NLP1, WORK( ITEMP ), 1, WORK( VLI ), 1 )
         ELSE
            CALL DLASET( 'A', NL, NL, ZERO, ONE, U( NLF, 1 ), LDU )
            CALL DLASET( 'A', NLP1, NLP1, ZERO, ONE, VT( NLF, 1 ), LDU )
            CALL DLASDQ( 'U', SQREI, NL, NLP1, NL, NCC, D( NLF ),
     $                   E( NLF ), VT( NLF, 1 ), LDU, U( NLF, 1 ), LDU,
     $                   U( NLF, 1 ), LDU, WORK( NWORK1 ), INFO )
            CALL DCOPY( NLP1, VT( NLF, 1 ), 1, WORK( VFI ), 1 )
            CALL DCOPY( NLP1, VT( NLF, NLP1 ), 1, WORK( VLI ), 1 )
         END IF
         IF( INFO.NE.0 ) THEN
            RETURN
         END IF
         DO 10 J = 1, NL
            IWORK( IDXQI+J ) = J
   10    CONTINUE
         IF( ( I.EQ.ND ) .AND. ( SQRE.EQ.0 ) ) THEN
            SQREI = 0
         ELSE
            SQREI = 1
         END IF
         IDXQI = IDXQI + NLP1
         VFI = VFI + NLP1
         VLI = VLI + NLP1
         NRP1 = NR + SQREI
         IF( ICOMPQ.EQ.0 ) THEN
            CALL DLASET( 'A', NRP1, NRP1, ZERO, ONE, WORK( NWORK1 ),
     $                   SMLSZP )
            CALL DLASDQ( 'U', SQREI, NR, NRP1, NRU, NCC, D( NRF ),
     $                   E( NRF ), WORK( NWORK1 ), SMLSZP,
     $                   WORK( NWORK2 ), NR, WORK( NWORK2 ), NR,
     $                   WORK( NWORK2 ), INFO )
            ITEMP = NWORK1 + ( NRP1-1 )*SMLSZP
            CALL DCOPY( NRP1, WORK( NWORK1 ), 1, WORK( VFI ), 1 )
            CALL DCOPY( NRP1, WORK( ITEMP ), 1, WORK( VLI ), 1 )
         ELSE
            CALL DLASET( 'A', NR, NR, ZERO, ONE, U( NRF, 1 ), LDU )
            CALL DLASET( 'A', NRP1, NRP1, ZERO, ONE, VT( NRF, 1 ), LDU )
            CALL DLASDQ( 'U', SQREI, NR, NRP1, NR, NCC, D( NRF ),
     $                   E( NRF ), VT( NRF, 1 ), LDU, U( NRF, 1 ), LDU,
     $                   U( NRF, 1 ), LDU, WORK( NWORK1 ), INFO )
            CALL DCOPY( NRP1, VT( NRF, 1 ), 1, WORK( VFI ), 1 )
            CALL DCOPY( NRP1, VT( NRF, NRP1 ), 1, WORK( VLI ), 1 )
         END IF
         IF( INFO.NE.0 ) THEN
            RETURN
         END IF
         DO 20 J = 1, NR
            IWORK( IDXQI+J ) = J
   20    CONTINUE
   30 CONTINUE
*
*     Now conquer each subproblem bottom-up.
*
      J = 2**NLVL
      DO 50 LVL = NLVL, 1, -1
         LVL2 = LVL*2 - 1
*
*        Find the first node LF and last node LL on
*        the current level LVL.
*
         IF( LVL.EQ.1 ) THEN
            LF = 1
            LL = 1
         ELSE
            LF = 2**( LVL-1 )
            LL = 2*LF - 1
         END IF
         DO 40 I = LF, LL
            IM1 = I - 1
            IC = IWORK( INODE+IM1 )
            NL = IWORK( NDIML+IM1 )
            NR = IWORK( NDIMR+IM1 )
            NLF = IC - NL
            NRF = IC + 1
            IF( I.EQ.LL ) THEN
               SQREI = SQRE
            ELSE
               SQREI = 1
            END IF
            VFI = VF + NLF - 1
            VLI = VL + NLF - 1
            IDXQI = IDXQ + NLF - 1
            ALPHA = D( IC )
            BETA = E( IC )
            IF( ICOMPQ.EQ.0 ) THEN
               CALL DLASD6( ICOMPQ, NL, NR, SQREI, D( NLF ),
     $                      WORK( VFI ), WORK( VLI ), ALPHA, BETA,
     $                      IWORK( IDXQI ), PERM, GIVPTR( 1 ), GIVCOL,
     $                      LDGCOL, GIVNUM, LDU, POLES, DIFL, DIFR, Z,
     $                      K( 1 ), C( 1 ), S( 1 ), WORK( NWORK1 ),
     $                      IWORK( IWK ), INFO )
            ELSE
               J = J - 1
               CALL DLASD6( ICOMPQ, NL, NR, SQREI, D( NLF ),
     $                      WORK( VFI ), WORK( VLI ), ALPHA, BETA,
     $                      IWORK( IDXQI ), PERM( NLF, LVL ),
     $                      GIVPTR( J ), GIVCOL( NLF, LVL2 ), LDGCOL,
     $                      GIVNUM( NLF, LVL2 ), LDU,
     $                      POLES( NLF, LVL2 ), DIFL( NLF, LVL ),
     $                      DIFR( NLF, LVL2 ), Z( NLF, LVL ), K( J ),
     $                      C( J ), S( J ), WORK( NWORK1 ),
     $                      IWORK( IWK ), INFO )
            END IF
            IF( INFO.NE.0 ) THEN
               RETURN
            END IF
   40    CONTINUE
   50 CONTINUE
*
      RETURN
*
*     End of DLASDA
*
      END
