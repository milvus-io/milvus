*> \brief \b DLASD0 computes the singular values of a real upper bidiagonal n-by-m matrix B with diagonal d and off-diagonal e. Used by sbdsdc.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DLASD0 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlasd0.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlasd0.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlasd0.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DLASD0( N, SQRE, D, E, U, LDU, VT, LDVT, SMLSIZ, IWORK,
*                          WORK, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDU, LDVT, N, SMLSIZ, SQRE
*       ..
*       .. Array Arguments ..
*       INTEGER            IWORK( * )
*       DOUBLE PRECISION   D( * ), E( * ), U( LDU, * ), VT( LDVT, * ),
*      $                   WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> Using a divide and conquer approach, DLASD0 computes the singular
*> value decomposition (SVD) of a real upper bidiagonal N-by-M
*> matrix B with diagonal D and offdiagonal E, where M = N + SQRE.
*> The algorithm computes orthogonal matrices U and VT such that
*> B = U * S * VT. The singular values S are overwritten on D.
*>
*> A related subroutine, DLASDA, computes only the singular values,
*> and optionally, the singular vectors in compact form.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>         On entry, the row dimension of the upper bidiagonal matrix.
*>         This is also the dimension of the main diagonal array D.
*> \endverbatim
*>
*> \param[in] SQRE
*> \verbatim
*>          SQRE is INTEGER
*>         Specifies the column dimension of the bidiagonal matrix.
*>         = 0: The bidiagonal matrix has column dimension M = N;
*>         = 1: The bidiagonal matrix has column dimension M = N+1;
*> \endverbatim
*>
*> \param[in,out] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension (N)
*>         On entry D contains the main diagonal of the bidiagonal
*>         matrix.
*>         On exit D, if INFO = 0, contains its singular values.
*> \endverbatim
*>
*> \param[in,out] E
*> \verbatim
*>          E is DOUBLE PRECISION array, dimension (M-1)
*>         Contains the subdiagonal entries of the bidiagonal matrix.
*>         On exit, E has been destroyed.
*> \endverbatim
*>
*> \param[out] U
*> \verbatim
*>          U is DOUBLE PRECISION array, dimension (LDU, N)
*>         On exit, U contains the left singular vectors.
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER
*>         On entry, leading dimension of U.
*> \endverbatim
*>
*> \param[out] VT
*> \verbatim
*>          VT is DOUBLE PRECISION array, dimension (LDVT, M)
*>         On exit, VT**T contains the right singular vectors.
*> \endverbatim
*>
*> \param[in] LDVT
*> \verbatim
*>          LDVT is INTEGER
*>         On entry, leading dimension of VT.
*> \endverbatim
*>
*> \param[in] SMLSIZ
*> \verbatim
*>          SMLSIZ is INTEGER
*>         On entry, maximum size of the subproblems at the
*>         bottom of the computation tree.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (8*N)
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (3*M**2+2*M)
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
      SUBROUTINE DLASD0( N, SQRE, D, E, U, LDU, VT, LDVT, SMLSIZ, IWORK,
     $                   WORK, INFO )
*
*  -- LAPACK auxiliary routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDU, LDVT, N, SMLSIZ, SQRE
*     ..
*     .. Array Arguments ..
      INTEGER            IWORK( * )
      DOUBLE PRECISION   D( * ), E( * ), U( LDU, * ), VT( LDVT, * ),
     $                   WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      INTEGER            I, I1, IC, IDXQ, IDXQC, IM1, INODE, ITEMP, IWK,
     $                   J, LF, LL, LVL, M, NCC, ND, NDB1, NDIML, NDIMR,
     $                   NL, NLF, NLP1, NLVL, NR, NRF, NRP1, SQREI
      DOUBLE PRECISION   ALPHA, BETA
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLASD1, DLASDQ, DLASDT, XERBLA
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
*
      IF( N.LT.0 ) THEN
         INFO = -1
      ELSE IF( ( SQRE.LT.0 ) .OR. ( SQRE.GT.1 ) ) THEN
         INFO = -2
      END IF
*
      M = N + SQRE
*
      IF( LDU.LT.N ) THEN
         INFO = -6
      ELSE IF( LDVT.LT.M ) THEN
         INFO = -8
      ELSE IF( SMLSIZ.LT.3 ) THEN
         INFO = -9
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DLASD0', -INFO )
         RETURN
      END IF
*
*     If the input matrix is too small, call DLASDQ to find the SVD.
*
      IF( N.LE.SMLSIZ ) THEN
         CALL DLASDQ( 'U', SQRE, N, M, N, 0, D, E, VT, LDVT, U, LDU, U,
     $                LDU, WORK, INFO )
         RETURN
      END IF
*
*     Set up the computation tree.
*
      INODE = 1
      NDIML = INODE + N
      NDIMR = NDIML + N
      IDXQ = NDIMR + N
      IWK = IDXQ + N
      CALL DLASDT( N, NLVL, ND, IWORK( INODE ), IWORK( NDIML ),
     $             IWORK( NDIMR ), SMLSIZ )
*
*     For the nodes on bottom level of the tree, solve
*     their subproblems by DLASDQ.
*
      NDB1 = ( ND+1 ) / 2
      NCC = 0
      DO 30 I = NDB1, ND
*
*     IC : center row of each node
*     NL : number of rows of left  subproblem
*     NR : number of rows of right subproblem
*     NLF: starting row of the left   subproblem
*     NRF: starting row of the right  subproblem
*
         I1 = I - 1
         IC = IWORK( INODE+I1 )
         NL = IWORK( NDIML+I1 )
         NLP1 = NL + 1
         NR = IWORK( NDIMR+I1 )
         NRP1 = NR + 1
         NLF = IC - NL
         NRF = IC + 1
         SQREI = 1
         CALL DLASDQ( 'U', SQREI, NL, NLP1, NL, NCC, D( NLF ), E( NLF ),
     $                VT( NLF, NLF ), LDVT, U( NLF, NLF ), LDU,
     $                U( NLF, NLF ), LDU, WORK, INFO )
         IF( INFO.NE.0 ) THEN
            RETURN
         END IF
         ITEMP = IDXQ + NLF - 2
         DO 10 J = 1, NL
            IWORK( ITEMP+J ) = J
   10    CONTINUE
         IF( I.EQ.ND ) THEN
            SQREI = SQRE
         ELSE
            SQREI = 1
         END IF
         NRP1 = NR + SQREI
         CALL DLASDQ( 'U', SQREI, NR, NRP1, NR, NCC, D( NRF ), E( NRF ),
     $                VT( NRF, NRF ), LDVT, U( NRF, NRF ), LDU,
     $                U( NRF, NRF ), LDU, WORK, INFO )
         IF( INFO.NE.0 ) THEN
            RETURN
         END IF
         ITEMP = IDXQ + IC
         DO 20 J = 1, NR
            IWORK( ITEMP+J-1 ) = J
   20    CONTINUE
   30 CONTINUE
*
*     Now conquer each subproblem bottom-up.
*
      DO 50 LVL = NLVL, 1, -1
*
*        Find the first node LF and last node LL on the
*        current level LVL.
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
            IF( ( SQRE.EQ.0 ) .AND. ( I.EQ.LL ) ) THEN
               SQREI = SQRE
            ELSE
               SQREI = 1
            END IF
            IDXQC = IDXQ + NLF - 1
            ALPHA = D( IC )
            BETA = E( IC )
            CALL DLASD1( NL, NR, SQREI, D( NLF ), ALPHA, BETA,
     $                   U( NLF, NLF ), LDU, VT( NLF, NLF ), LDVT,
     $                   IWORK( IDXQC ), IWORK( IWK ), WORK, INFO )
*
*        Report the possible convergence failure.
*
            IF( INFO.NE.0 ) THEN
               RETURN
            END IF
   40    CONTINUE
   50 CONTINUE
*
      RETURN
*
*     End of DLASD0
*
      END
