*> \brief \b ZLALSA computes the SVD of the coefficient matrix in compact form. Used by sgelsd.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZLALSA + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zlalsa.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zlalsa.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zlalsa.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZLALSA( ICOMPQ, SMLSIZ, N, NRHS, B, LDB, BX, LDBX, U,
*                          LDU, VT, K, DIFL, DIFR, Z, POLES, GIVPTR,
*                          GIVCOL, LDGCOL, PERM, GIVNUM, C, S, RWORK,
*                          IWORK, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            ICOMPQ, INFO, LDB, LDBX, LDGCOL, LDU, N, NRHS,
*      $                   SMLSIZ
*       ..
*       .. Array Arguments ..
*       INTEGER            GIVCOL( LDGCOL, * ), GIVPTR( * ), IWORK( * ),
*      $                   K( * ), PERM( LDGCOL, * )
*       DOUBLE PRECISION   C( * ), DIFL( LDU, * ), DIFR( LDU, * ),
*      $                   GIVNUM( LDU, * ), POLES( LDU, * ), RWORK( * ),
*      $                   S( * ), U( LDU, * ), VT( LDU, * ), Z( LDU, * )
*       COMPLEX*16         B( LDB, * ), BX( LDBX, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZLALSA is an itermediate step in solving the least squares problem
*> by computing the SVD of the coefficient matrix in compact form (The
*> singular vectors are computed as products of simple orthorgonal
*> matrices.).
*>
*> If ICOMPQ = 0, ZLALSA applies the inverse of the left singular vector
*> matrix of an upper bidiagonal matrix to the right hand side; and if
*> ICOMPQ = 1, ZLALSA applies the right singular vector matrix to the
*> right hand side. The singular vector matrices were generated in
*> compact form by ZLALSA.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] ICOMPQ
*> \verbatim
*>          ICOMPQ is INTEGER
*>         Specifies whether the left or the right singular vector
*>         matrix is involved.
*>         = 0: Left singular vector matrix
*>         = 1: Right singular vector matrix
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
*>         The row and column dimensions of the upper bidiagonal matrix.
*> \endverbatim
*>
*> \param[in] NRHS
*> \verbatim
*>          NRHS is INTEGER
*>         The number of columns of B and BX. NRHS must be at least 1.
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX*16 array, dimension ( LDB, NRHS )
*>         On input, B contains the right hand sides of the least
*>         squares problem in rows 1 through M.
*>         On output, B contains the solution X in rows 1 through N.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>         The leading dimension of B in the calling subprogram.
*>         LDB must be at least max(1,MAX( M, N ) ).
*> \endverbatim
*>
*> \param[out] BX
*> \verbatim
*>          BX is COMPLEX*16 array, dimension ( LDBX, NRHS )
*>         On exit, the result of applying the left or right singular
*>         vector matrix to B.
*> \endverbatim
*>
*> \param[in] LDBX
*> \verbatim
*>          LDBX is INTEGER
*>         The leading dimension of BX.
*> \endverbatim
*>
*> \param[in] U
*> \verbatim
*>          U is DOUBLE PRECISION array, dimension ( LDU, SMLSIZ ).
*>         On entry, U contains the left singular vector matrices of all
*>         subproblems at the bottom level.
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER, LDU = > N.
*>         The leading dimension of arrays U, VT, DIFL, DIFR,
*>         POLES, GIVNUM, and Z.
*> \endverbatim
*>
*> \param[in] VT
*> \verbatim
*>          VT is DOUBLE PRECISION array, dimension ( LDU, SMLSIZ+1 ).
*>         On entry, VT**H contains the right singular vector matrices of
*>         all subproblems at the bottom level.
*> \endverbatim
*>
*> \param[in] K
*> \verbatim
*>          K is INTEGER array, dimension ( N ).
*> \endverbatim
*>
*> \param[in] DIFL
*> \verbatim
*>          DIFL is DOUBLE PRECISION array, dimension ( LDU, NLVL ).
*>         where NLVL = INT(log_2 (N/(SMLSIZ+1))) + 1.
*> \endverbatim
*>
*> \param[in] DIFR
*> \verbatim
*>          DIFR is DOUBLE PRECISION array, dimension ( LDU, 2 * NLVL ).
*>         On entry, DIFL(*, I) and DIFR(*, 2 * I -1) record
*>         distances between singular values on the I-th level and
*>         singular values on the (I -1)-th level, and DIFR(*, 2 * I)
*>         record the normalizing factors of the right singular vectors
*>         matrices of subproblems on I-th level.
*> \endverbatim
*>
*> \param[in] Z
*> \verbatim
*>          Z is DOUBLE PRECISION array, dimension ( LDU, NLVL ).
*>         On entry, Z(1, I) contains the components of the deflation-
*>         adjusted updating row vector for subproblems on the I-th
*>         level.
*> \endverbatim
*>
*> \param[in] POLES
*> \verbatim
*>          POLES is DOUBLE PRECISION array, dimension ( LDU, 2 * NLVL ).
*>         On entry, POLES(*, 2 * I -1: 2 * I) contains the new and old
*>         singular values involved in the secular equations on the I-th
*>         level.
*> \endverbatim
*>
*> \param[in] GIVPTR
*> \verbatim
*>          GIVPTR is INTEGER array, dimension ( N ).
*>         On entry, GIVPTR( I ) records the number of Givens
*>         rotations performed on the I-th problem on the computation
*>         tree.
*> \endverbatim
*>
*> \param[in] GIVCOL
*> \verbatim
*>          GIVCOL is INTEGER array, dimension ( LDGCOL, 2 * NLVL ).
*>         On entry, for each I, GIVCOL(*, 2 * I - 1: 2 * I) records the
*>         locations of Givens rotations performed on the I-th level on
*>         the computation tree.
*> \endverbatim
*>
*> \param[in] LDGCOL
*> \verbatim
*>          LDGCOL is INTEGER, LDGCOL = > N.
*>         The leading dimension of arrays GIVCOL and PERM.
*> \endverbatim
*>
*> \param[in] PERM
*> \verbatim
*>          PERM is INTEGER array, dimension ( LDGCOL, NLVL ).
*>         On entry, PERM(*, I) records permutations done on the I-th
*>         level of the computation tree.
*> \endverbatim
*>
*> \param[in] GIVNUM
*> \verbatim
*>          GIVNUM is DOUBLE PRECISION array, dimension ( LDU, 2 * NLVL ).
*>         On entry, GIVNUM(*, 2 *I -1 : 2 * I) records the C- and S-
*>         values of Givens rotations performed on the I-th level on the
*>         computation tree.
*> \endverbatim
*>
*> \param[in] C
*> \verbatim
*>          C is DOUBLE PRECISION array, dimension ( N ).
*>         On entry, if the I-th subproblem is not square,
*>         C( I ) contains the C-value of a Givens rotation related to
*>         the right null space of the I-th subproblem.
*> \endverbatim
*>
*> \param[in] S
*> \verbatim
*>          S is DOUBLE PRECISION array, dimension ( N ).
*>         On entry, if the I-th subproblem is not square,
*>         S( I ) contains the S-value of a Givens rotation related to
*>         the right null space of the I-th subproblem.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension at least
*>         MAX( (SMLSZ+1)*NRHS*3, N*(1+NRHS) + 2*NRHS ).
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (3*N)
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
*> \date June 2017
*
*> \ingroup complex16OTHERcomputational
*
*> \par Contributors:
*  ==================
*>
*>     Ming Gu and Ren-Cang Li, Computer Science Division, University of
*>       California at Berkeley, USA \n
*>     Osni Marques, LBNL/NERSC, USA \n
*
*  =====================================================================
      SUBROUTINE ZLALSA( ICOMPQ, SMLSIZ, N, NRHS, B, LDB, BX, LDBX, U,
     $                   LDU, VT, K, DIFL, DIFR, Z, POLES, GIVPTR,
     $                   GIVCOL, LDGCOL, PERM, GIVNUM, C, S, RWORK,
     $                   IWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      INTEGER            ICOMPQ, INFO, LDB, LDBX, LDGCOL, LDU, N, NRHS,
     $                   SMLSIZ
*     ..
*     .. Array Arguments ..
      INTEGER            GIVCOL( LDGCOL, * ), GIVPTR( * ), IWORK( * ),
     $                   K( * ), PERM( LDGCOL, * )
      DOUBLE PRECISION   C( * ), DIFL( LDU, * ), DIFR( LDU, * ),
     $                   GIVNUM( LDU, * ), POLES( LDU, * ), RWORK( * ),
     $                   S( * ), U( LDU, * ), VT( LDU, * ), Z( LDU, * )
      COMPLEX*16         B( LDB, * ), BX( LDBX, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D0, ONE = 1.0D0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, I1, IC, IM1, INODE, J, JCOL, JIMAG, JREAL,
     $                   JROW, LF, LL, LVL, LVL2, ND, NDB1, NDIML,
     $                   NDIMR, NL, NLF, NLP1, NLVL, NR, NRF, NRP1, SQRE
*     ..
*     .. External Subroutines ..
      EXTERNAL           DGEMM, DLASDT, XERBLA, ZCOPY, ZLALS0
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE, DCMPLX, DIMAG
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
      ELSE IF( N.LT.SMLSIZ ) THEN
         INFO = -3
      ELSE IF( NRHS.LT.1 ) THEN
         INFO = -4
      ELSE IF( LDB.LT.N ) THEN
         INFO = -6
      ELSE IF( LDBX.LT.N ) THEN
         INFO = -8
      ELSE IF( LDU.LT.N ) THEN
         INFO = -10
      ELSE IF( LDGCOL.LT.N ) THEN
         INFO = -19
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZLALSA', -INFO )
         RETURN
      END IF
*
*     Book-keeping and  setting up the computation tree.
*
      INODE = 1
      NDIML = INODE + N
      NDIMR = NDIML + N
*
      CALL DLASDT( N, NLVL, ND, IWORK( INODE ), IWORK( NDIML ),
     $             IWORK( NDIMR ), SMLSIZ )
*
*     The following code applies back the left singular vector factors.
*     For applying back the right singular vector factors, go to 170.
*
      IF( ICOMPQ.EQ.1 ) THEN
         GO TO 170
      END IF
*
*     The nodes on the bottom level of the tree were solved
*     by DLASDQ. The corresponding left and right singular vector
*     matrices are in explicit form. First apply back the left
*     singular vector matrices.
*
      NDB1 = ( ND+1 ) / 2
      DO 130 I = NDB1, ND
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
         NR = IWORK( NDIMR+I1 )
         NLF = IC - NL
         NRF = IC + 1
*
*        Since B and BX are complex, the following call to DGEMM
*        is performed in two steps (real and imaginary parts).
*
*        CALL DGEMM( 'T', 'N', NL, NRHS, NL, ONE, U( NLF, 1 ), LDU,
*     $               B( NLF, 1 ), LDB, ZERO, BX( NLF, 1 ), LDBX )
*
         J = NL*NRHS*2
         DO 20 JCOL = 1, NRHS
            DO 10 JROW = NLF, NLF + NL - 1
               J = J + 1
               RWORK( J ) = DBLE( B( JROW, JCOL ) )
   10       CONTINUE
   20    CONTINUE
         CALL DGEMM( 'T', 'N', NL, NRHS, NL, ONE, U( NLF, 1 ), LDU,
     $               RWORK( 1+NL*NRHS*2 ), NL, ZERO, RWORK( 1 ), NL )
         J = NL*NRHS*2
         DO 40 JCOL = 1, NRHS
            DO 30 JROW = NLF, NLF + NL - 1
               J = J + 1
               RWORK( J ) = DIMAG( B( JROW, JCOL ) )
   30       CONTINUE
   40    CONTINUE
         CALL DGEMM( 'T', 'N', NL, NRHS, NL, ONE, U( NLF, 1 ), LDU,
     $               RWORK( 1+NL*NRHS*2 ), NL, ZERO, RWORK( 1+NL*NRHS ),
     $               NL )
         JREAL = 0
         JIMAG = NL*NRHS
         DO 60 JCOL = 1, NRHS
            DO 50 JROW = NLF, NLF + NL - 1
               JREAL = JREAL + 1
               JIMAG = JIMAG + 1
               BX( JROW, JCOL ) = DCMPLX( RWORK( JREAL ),
     $                            RWORK( JIMAG ) )
   50       CONTINUE
   60    CONTINUE
*
*        Since B and BX are complex, the following call to DGEMM
*        is performed in two steps (real and imaginary parts).
*
*        CALL DGEMM( 'T', 'N', NR, NRHS, NR, ONE, U( NRF, 1 ), LDU,
*    $               B( NRF, 1 ), LDB, ZERO, BX( NRF, 1 ), LDBX )
*
         J = NR*NRHS*2
         DO 80 JCOL = 1, NRHS
            DO 70 JROW = NRF, NRF + NR - 1
               J = J + 1
               RWORK( J ) = DBLE( B( JROW, JCOL ) )
   70       CONTINUE
   80    CONTINUE
         CALL DGEMM( 'T', 'N', NR, NRHS, NR, ONE, U( NRF, 1 ), LDU,
     $               RWORK( 1+NR*NRHS*2 ), NR, ZERO, RWORK( 1 ), NR )
         J = NR*NRHS*2
         DO 100 JCOL = 1, NRHS
            DO 90 JROW = NRF, NRF + NR - 1
               J = J + 1
               RWORK( J ) = DIMAG( B( JROW, JCOL ) )
   90       CONTINUE
  100    CONTINUE
         CALL DGEMM( 'T', 'N', NR, NRHS, NR, ONE, U( NRF, 1 ), LDU,
     $               RWORK( 1+NR*NRHS*2 ), NR, ZERO, RWORK( 1+NR*NRHS ),
     $               NR )
         JREAL = 0
         JIMAG = NR*NRHS
         DO 120 JCOL = 1, NRHS
            DO 110 JROW = NRF, NRF + NR - 1
               JREAL = JREAL + 1
               JIMAG = JIMAG + 1
               BX( JROW, JCOL ) = DCMPLX( RWORK( JREAL ),
     $                            RWORK( JIMAG ) )
  110       CONTINUE
  120    CONTINUE
*
  130 CONTINUE
*
*     Next copy the rows of B that correspond to unchanged rows
*     in the bidiagonal matrix to BX.
*
      DO 140 I = 1, ND
         IC = IWORK( INODE+I-1 )
         CALL ZCOPY( NRHS, B( IC, 1 ), LDB, BX( IC, 1 ), LDBX )
  140 CONTINUE
*
*     Finally go through the left singular vector matrices of all
*     the other subproblems bottom-up on the tree.
*
      J = 2**NLVL
      SQRE = 0
*
      DO 160 LVL = NLVL, 1, -1
         LVL2 = 2*LVL - 1
*
*        find the first node LF and last node LL on
*        the current level LVL
*
         IF( LVL.EQ.1 ) THEN
            LF = 1
            LL = 1
         ELSE
            LF = 2**( LVL-1 )
            LL = 2*LF - 1
         END IF
         DO 150 I = LF, LL
            IM1 = I - 1
            IC = IWORK( INODE+IM1 )
            NL = IWORK( NDIML+IM1 )
            NR = IWORK( NDIMR+IM1 )
            NLF = IC - NL
            NRF = IC + 1
            J = J - 1
            CALL ZLALS0( ICOMPQ, NL, NR, SQRE, NRHS, BX( NLF, 1 ), LDBX,
     $                   B( NLF, 1 ), LDB, PERM( NLF, LVL ),
     $                   GIVPTR( J ), GIVCOL( NLF, LVL2 ), LDGCOL,
     $                   GIVNUM( NLF, LVL2 ), LDU, POLES( NLF, LVL2 ),
     $                   DIFL( NLF, LVL ), DIFR( NLF, LVL2 ),
     $                   Z( NLF, LVL ), K( J ), C( J ), S( J ), RWORK,
     $                   INFO )
  150    CONTINUE
  160 CONTINUE
      GO TO 330
*
*     ICOMPQ = 1: applying back the right singular vector factors.
*
  170 CONTINUE
*
*     First now go through the right singular vector matrices of all
*     the tree nodes top-down.
*
      J = 0
      DO 190 LVL = 1, NLVL
         LVL2 = 2*LVL - 1
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
         DO 180 I = LL, LF, -1
            IM1 = I - 1
            IC = IWORK( INODE+IM1 )
            NL = IWORK( NDIML+IM1 )
            NR = IWORK( NDIMR+IM1 )
            NLF = IC - NL
            NRF = IC + 1
            IF( I.EQ.LL ) THEN
               SQRE = 0
            ELSE
               SQRE = 1
            END IF
            J = J + 1
            CALL ZLALS0( ICOMPQ, NL, NR, SQRE, NRHS, B( NLF, 1 ), LDB,
     $                   BX( NLF, 1 ), LDBX, PERM( NLF, LVL ),
     $                   GIVPTR( J ), GIVCOL( NLF, LVL2 ), LDGCOL,
     $                   GIVNUM( NLF, LVL2 ), LDU, POLES( NLF, LVL2 ),
     $                   DIFL( NLF, LVL ), DIFR( NLF, LVL2 ),
     $                   Z( NLF, LVL ), K( J ), C( J ), S( J ), RWORK,
     $                   INFO )
  180    CONTINUE
  190 CONTINUE
*
*     The nodes on the bottom level of the tree were solved
*     by DLASDQ. The corresponding right singular vector
*     matrices are in explicit form. Apply them back.
*
      NDB1 = ( ND+1 ) / 2
      DO 320 I = NDB1, ND
         I1 = I - 1
         IC = IWORK( INODE+I1 )
         NL = IWORK( NDIML+I1 )
         NR = IWORK( NDIMR+I1 )
         NLP1 = NL + 1
         IF( I.EQ.ND ) THEN
            NRP1 = NR
         ELSE
            NRP1 = NR + 1
         END IF
         NLF = IC - NL
         NRF = IC + 1
*
*        Since B and BX are complex, the following call to DGEMM is
*        performed in two steps (real and imaginary parts).
*
*        CALL DGEMM( 'T', 'N', NLP1, NRHS, NLP1, ONE, VT( NLF, 1 ), LDU,
*    $               B( NLF, 1 ), LDB, ZERO, BX( NLF, 1 ), LDBX )
*
         J = NLP1*NRHS*2
         DO 210 JCOL = 1, NRHS
            DO 200 JROW = NLF, NLF + NLP1 - 1
               J = J + 1
               RWORK( J ) = DBLE( B( JROW, JCOL ) )
  200       CONTINUE
  210    CONTINUE
         CALL DGEMM( 'T', 'N', NLP1, NRHS, NLP1, ONE, VT( NLF, 1 ), LDU,
     $               RWORK( 1+NLP1*NRHS*2 ), NLP1, ZERO, RWORK( 1 ),
     $               NLP1 )
         J = NLP1*NRHS*2
         DO 230 JCOL = 1, NRHS
            DO 220 JROW = NLF, NLF + NLP1 - 1
               J = J + 1
               RWORK( J ) = DIMAG( B( JROW, JCOL ) )
  220       CONTINUE
  230    CONTINUE
         CALL DGEMM( 'T', 'N', NLP1, NRHS, NLP1, ONE, VT( NLF, 1 ), LDU,
     $               RWORK( 1+NLP1*NRHS*2 ), NLP1, ZERO,
     $               RWORK( 1+NLP1*NRHS ), NLP1 )
         JREAL = 0
         JIMAG = NLP1*NRHS
         DO 250 JCOL = 1, NRHS
            DO 240 JROW = NLF, NLF + NLP1 - 1
               JREAL = JREAL + 1
               JIMAG = JIMAG + 1
               BX( JROW, JCOL ) = DCMPLX( RWORK( JREAL ),
     $                            RWORK( JIMAG ) )
  240       CONTINUE
  250    CONTINUE
*
*        Since B and BX are complex, the following call to DGEMM is
*        performed in two steps (real and imaginary parts).
*
*        CALL DGEMM( 'T', 'N', NRP1, NRHS, NRP1, ONE, VT( NRF, 1 ), LDU,
*    $               B( NRF, 1 ), LDB, ZERO, BX( NRF, 1 ), LDBX )
*
         J = NRP1*NRHS*2
         DO 270 JCOL = 1, NRHS
            DO 260 JROW = NRF, NRF + NRP1 - 1
               J = J + 1
               RWORK( J ) = DBLE( B( JROW, JCOL ) )
  260       CONTINUE
  270    CONTINUE
         CALL DGEMM( 'T', 'N', NRP1, NRHS, NRP1, ONE, VT( NRF, 1 ), LDU,
     $               RWORK( 1+NRP1*NRHS*2 ), NRP1, ZERO, RWORK( 1 ),
     $               NRP1 )
         J = NRP1*NRHS*2
         DO 290 JCOL = 1, NRHS
            DO 280 JROW = NRF, NRF + NRP1 - 1
               J = J + 1
               RWORK( J ) = DIMAG( B( JROW, JCOL ) )
  280       CONTINUE
  290    CONTINUE
         CALL DGEMM( 'T', 'N', NRP1, NRHS, NRP1, ONE, VT( NRF, 1 ), LDU,
     $               RWORK( 1+NRP1*NRHS*2 ), NRP1, ZERO,
     $               RWORK( 1+NRP1*NRHS ), NRP1 )
         JREAL = 0
         JIMAG = NRP1*NRHS
         DO 310 JCOL = 1, NRHS
            DO 300 JROW = NRF, NRF + NRP1 - 1
               JREAL = JREAL + 1
               JIMAG = JIMAG + 1
               BX( JROW, JCOL ) = DCMPLX( RWORK( JREAL ),
     $                            RWORK( JIMAG ) )
  300       CONTINUE
  310    CONTINUE
*
  320 CONTINUE
*
  330 CONTINUE
*
      RETURN
*
*     End of ZLALSA
*
      END
