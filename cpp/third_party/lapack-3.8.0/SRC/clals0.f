*> \brief \b CLALS0 applies back multiplying factors in solving the least squares problem using divide and conquer SVD approach. Used by sgelsd.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CLALS0 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/clals0.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/clals0.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/clals0.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CLALS0( ICOMPQ, NL, NR, SQRE, NRHS, B, LDB, BX, LDBX,
*                          PERM, GIVPTR, GIVCOL, LDGCOL, GIVNUM, LDGNUM,
*                          POLES, DIFL, DIFR, Z, K, C, S, RWORK, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            GIVPTR, ICOMPQ, INFO, K, LDB, LDBX, LDGCOL,
*      $                   LDGNUM, NL, NR, NRHS, SQRE
*       REAL               C, S
*       ..
*       .. Array Arguments ..
*       INTEGER            GIVCOL( LDGCOL, * ), PERM( * )
*       REAL               DIFL( * ), DIFR( LDGNUM, * ),
*      $                   GIVNUM( LDGNUM, * ), POLES( LDGNUM, * ),
*      $                   RWORK( * ), Z( * )
*       COMPLEX            B( LDB, * ), BX( LDBX, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CLALS0 applies back the multiplying factors of either the left or the
*> right singular vector matrix of a diagonal matrix appended by a row
*> to the right hand side matrix B in solving the least squares problem
*> using the divide-and-conquer SVD approach.
*>
*> For the left singular vector matrix, three types of orthogonal
*> matrices are involved:
*>
*> (1L) Givens rotations: the number of such rotations is GIVPTR; the
*>      pairs of columns/rows they were applied to are stored in GIVCOL;
*>      and the C- and S-values of these rotations are stored in GIVNUM.
*>
*> (2L) Permutation. The (NL+1)-st row of B is to be moved to the first
*>      row, and for J=2:N, PERM(J)-th row of B is to be moved to the
*>      J-th row.
*>
*> (3L) The left singular vector matrix of the remaining matrix.
*>
*> For the right singular vector matrix, four types of orthogonal
*> matrices are involved:
*>
*> (1R) The right singular vector matrix of the remaining matrix.
*>
*> (2R) If SQRE = 1, one extra Givens rotation to generate the right
*>      null space.
*>
*> (3R) The inverse transformation of (2L).
*>
*> (4R) The inverse transformation of (1L).
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
*>         = 0: Left singular vector matrix.
*>         = 1: Right singular vector matrix.
*> \endverbatim
*>
*> \param[in] NL
*> \verbatim
*>          NL is INTEGER
*>         The row dimension of the upper block. NL >= 1.
*> \endverbatim
*>
*> \param[in] NR
*> \verbatim
*>          NR is INTEGER
*>         The row dimension of the lower block. NR >= 1.
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
*> \param[in] NRHS
*> \verbatim
*>          NRHS is INTEGER
*>         The number of columns of B and BX. NRHS must be at least 1.
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX array, dimension ( LDB, NRHS )
*>         On input, B contains the right hand sides of the least
*>         squares problem in rows 1 through M. On output, B contains
*>         the solution X in rows 1 through N.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>         The leading dimension of B. LDB must be at least
*>         max(1,MAX( M, N ) ).
*> \endverbatim
*>
*> \param[out] BX
*> \verbatim
*>          BX is COMPLEX array, dimension ( LDBX, NRHS )
*> \endverbatim
*>
*> \param[in] LDBX
*> \verbatim
*>          LDBX is INTEGER
*>         The leading dimension of BX.
*> \endverbatim
*>
*> \param[in] PERM
*> \verbatim
*>          PERM is INTEGER array, dimension ( N )
*>         The permutations (from deflation and sorting) applied
*>         to the two blocks.
*> \endverbatim
*>
*> \param[in] GIVPTR
*> \verbatim
*>          GIVPTR is INTEGER
*>         The number of Givens rotations which took place in this
*>         subproblem.
*> \endverbatim
*>
*> \param[in] GIVCOL
*> \verbatim
*>          GIVCOL is INTEGER array, dimension ( LDGCOL, 2 )
*>         Each pair of numbers indicates a pair of rows/columns
*>         involved in a Givens rotation.
*> \endverbatim
*>
*> \param[in] LDGCOL
*> \verbatim
*>          LDGCOL is INTEGER
*>         The leading dimension of GIVCOL, must be at least N.
*> \endverbatim
*>
*> \param[in] GIVNUM
*> \verbatim
*>          GIVNUM is REAL array, dimension ( LDGNUM, 2 )
*>         Each number indicates the C or S value used in the
*>         corresponding Givens rotation.
*> \endverbatim
*>
*> \param[in] LDGNUM
*> \verbatim
*>          LDGNUM is INTEGER
*>         The leading dimension of arrays DIFR, POLES and
*>         GIVNUM, must be at least K.
*> \endverbatim
*>
*> \param[in] POLES
*> \verbatim
*>          POLES is REAL array, dimension ( LDGNUM, 2 )
*>         On entry, POLES(1:K, 1) contains the new singular
*>         values obtained from solving the secular equation, and
*>         POLES(1:K, 2) is an array containing the poles in the secular
*>         equation.
*> \endverbatim
*>
*> \param[in] DIFL
*> \verbatim
*>          DIFL is REAL array, dimension ( K ).
*>         On entry, DIFL(I) is the distance between I-th updated
*>         (undeflated) singular value and the I-th (undeflated) old
*>         singular value.
*> \endverbatim
*>
*> \param[in] DIFR
*> \verbatim
*>          DIFR is REAL array, dimension ( LDGNUM, 2 ).
*>         On entry, DIFR(I, 1) contains the distances between I-th
*>         updated (undeflated) singular value and the I+1-th
*>         (undeflated) old singular value. And DIFR(I, 2) is the
*>         normalizing factor for the I-th right singular vector.
*> \endverbatim
*>
*> \param[in] Z
*> \verbatim
*>          Z is REAL array, dimension ( K )
*>         Contain the components of the deflation-adjusted updating row
*>         vector.
*> \endverbatim
*>
*> \param[in] K
*> \verbatim
*>          K is INTEGER
*>         Contains the dimension of the non-deflated matrix,
*>         This is the order of the related secular equation. 1 <= K <=N.
*> \endverbatim
*>
*> \param[in] C
*> \verbatim
*>          C is REAL
*>         C contains garbage if SQRE =0 and the C-value of a Givens
*>         rotation related to the right null space if SQRE = 1.
*> \endverbatim
*>
*> \param[in] S
*> \verbatim
*>          S is REAL
*>         S contains garbage if SQRE =0 and the S-value of a Givens
*>         rotation related to the right null space if SQRE = 1.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is REAL array, dimension
*>         ( K*(1+NRHS) + 2*NRHS )
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
*> \ingroup complexOTHERcomputational
*
*> \par Contributors:
*  ==================
*>
*>     Ming Gu and Ren-Cang Li, Computer Science Division, University of
*>       California at Berkeley, USA \n
*>     Osni Marques, LBNL/NERSC, USA \n
*
*  =====================================================================
      SUBROUTINE CLALS0( ICOMPQ, NL, NR, SQRE, NRHS, B, LDB, BX, LDBX,
     $                   PERM, GIVPTR, GIVCOL, LDGCOL, GIVNUM, LDGNUM,
     $                   POLES, DIFL, DIFR, Z, K, C, S, RWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            GIVPTR, ICOMPQ, INFO, K, LDB, LDBX, LDGCOL,
     $                   LDGNUM, NL, NR, NRHS, SQRE
      REAL               C, S
*     ..
*     .. Array Arguments ..
      INTEGER            GIVCOL( LDGCOL, * ), PERM( * )
      REAL               DIFL( * ), DIFR( LDGNUM, * ),
     $                   GIVNUM( LDGNUM, * ), POLES( LDGNUM, * ),
     $                   RWORK( * ), Z( * )
      COMPLEX            B( LDB, * ), BX( LDBX, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ONE, ZERO, NEGONE
      PARAMETER          ( ONE = 1.0E0, ZERO = 0.0E0, NEGONE = -1.0E0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, J, JCOL, JROW, M, N, NLP1
      REAL               DIFLJ, DIFRJ, DJ, DSIGJ, DSIGJP, TEMP
*     ..
*     .. External Subroutines ..
      EXTERNAL           CCOPY, CLACPY, CLASCL, CSROT, CSSCAL, SGEMV,
     $                   XERBLA
*     ..
*     .. External Functions ..
      REAL               SLAMC3, SNRM2
      EXTERNAL           SLAMC3, SNRM2
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          AIMAG, CMPLX, MAX, REAL
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
      N = NL + NR + 1
*
      IF( ( ICOMPQ.LT.0 ) .OR. ( ICOMPQ.GT.1 ) ) THEN
         INFO = -1
      ELSE IF( NL.LT.1 ) THEN
         INFO = -2
      ELSE IF( NR.LT.1 ) THEN
         INFO = -3
      ELSE IF( ( SQRE.LT.0 ) .OR. ( SQRE.GT.1 ) ) THEN
         INFO = -4
      ELSE IF( NRHS.LT.1 ) THEN
         INFO = -5
      ELSE IF( LDB.LT.N ) THEN
         INFO = -7
      ELSE IF( LDBX.LT.N ) THEN
         INFO = -9
      ELSE IF( GIVPTR.LT.0 ) THEN
         INFO = -11
      ELSE IF( LDGCOL.LT.N ) THEN
         INFO = -13
      ELSE IF( LDGNUM.LT.N ) THEN
         INFO = -15
      ELSE IF( K.LT.1 ) THEN
         INFO = -20
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CLALS0', -INFO )
         RETURN
      END IF
*
      M = N + SQRE
      NLP1 = NL + 1
*
      IF( ICOMPQ.EQ.0 ) THEN
*
*        Apply back orthogonal transformations from the left.
*
*        Step (1L): apply back the Givens rotations performed.
*
         DO 10 I = 1, GIVPTR
            CALL CSROT( NRHS, B( GIVCOL( I, 2 ), 1 ), LDB,
     $                  B( GIVCOL( I, 1 ), 1 ), LDB, GIVNUM( I, 2 ),
     $                  GIVNUM( I, 1 ) )
   10    CONTINUE
*
*        Step (2L): permute rows of B.
*
         CALL CCOPY( NRHS, B( NLP1, 1 ), LDB, BX( 1, 1 ), LDBX )
         DO 20 I = 2, N
            CALL CCOPY( NRHS, B( PERM( I ), 1 ), LDB, BX( I, 1 ), LDBX )
   20    CONTINUE
*
*        Step (3L): apply the inverse of the left singular vector
*        matrix to BX.
*
         IF( K.EQ.1 ) THEN
            CALL CCOPY( NRHS, BX, LDBX, B, LDB )
            IF( Z( 1 ).LT.ZERO ) THEN
               CALL CSSCAL( NRHS, NEGONE, B, LDB )
            END IF
         ELSE
            DO 100 J = 1, K
               DIFLJ = DIFL( J )
               DJ = POLES( J, 1 )
               DSIGJ = -POLES( J, 2 )
               IF( J.LT.K ) THEN
                  DIFRJ = -DIFR( J, 1 )
                  DSIGJP = -POLES( J+1, 2 )
               END IF
               IF( ( Z( J ).EQ.ZERO ) .OR. ( POLES( J, 2 ).EQ.ZERO ) )
     $              THEN
                  RWORK( J ) = ZERO
               ELSE
                  RWORK( J ) = -POLES( J, 2 )*Z( J ) / DIFLJ /
     $                         ( POLES( J, 2 )+DJ )
               END IF
               DO 30 I = 1, J - 1
                  IF( ( Z( I ).EQ.ZERO ) .OR.
     $                ( POLES( I, 2 ).EQ.ZERO ) ) THEN
                     RWORK( I ) = ZERO
                  ELSE
                     RWORK( I ) = POLES( I, 2 )*Z( I ) /
     $                            ( SLAMC3( POLES( I, 2 ), DSIGJ )-
     $                            DIFLJ ) / ( POLES( I, 2 )+DJ )
                  END IF
   30          CONTINUE
               DO 40 I = J + 1, K
                  IF( ( Z( I ).EQ.ZERO ) .OR.
     $                ( POLES( I, 2 ).EQ.ZERO ) ) THEN
                     RWORK( I ) = ZERO
                  ELSE
                     RWORK( I ) = POLES( I, 2 )*Z( I ) /
     $                            ( SLAMC3( POLES( I, 2 ), DSIGJP )+
     $                            DIFRJ ) / ( POLES( I, 2 )+DJ )
                  END IF
   40          CONTINUE
               RWORK( 1 ) = NEGONE
               TEMP = SNRM2( K, RWORK, 1 )
*
*              Since B and BX are complex, the following call to SGEMV
*              is performed in two steps (real and imaginary parts).
*
*              CALL SGEMV( 'T', K, NRHS, ONE, BX, LDBX, WORK, 1, ZERO,
*    $                     B( J, 1 ), LDB )
*
               I = K + NRHS*2
               DO 60 JCOL = 1, NRHS
                  DO 50 JROW = 1, K
                     I = I + 1
                     RWORK( I ) = REAL( BX( JROW, JCOL ) )
   50             CONTINUE
   60          CONTINUE
               CALL SGEMV( 'T', K, NRHS, ONE, RWORK( 1+K+NRHS*2 ), K,
     $                     RWORK( 1 ), 1, ZERO, RWORK( 1+K ), 1 )
               I = K + NRHS*2
               DO 80 JCOL = 1, NRHS
                  DO 70 JROW = 1, K
                     I = I + 1
                     RWORK( I ) = AIMAG( BX( JROW, JCOL ) )
   70             CONTINUE
   80          CONTINUE
               CALL SGEMV( 'T', K, NRHS, ONE, RWORK( 1+K+NRHS*2 ), K,
     $                     RWORK( 1 ), 1, ZERO, RWORK( 1+K+NRHS ), 1 )
               DO 90 JCOL = 1, NRHS
                  B( J, JCOL ) = CMPLX( RWORK( JCOL+K ),
     $                           RWORK( JCOL+K+NRHS ) )
   90          CONTINUE
               CALL CLASCL( 'G', 0, 0, TEMP, ONE, 1, NRHS, B( J, 1 ),
     $                      LDB, INFO )
  100       CONTINUE
         END IF
*
*        Move the deflated rows of BX to B also.
*
         IF( K.LT.MAX( M, N ) )
     $      CALL CLACPY( 'A', N-K, NRHS, BX( K+1, 1 ), LDBX,
     $                   B( K+1, 1 ), LDB )
      ELSE
*
*        Apply back the right orthogonal transformations.
*
*        Step (1R): apply back the new right singular vector matrix
*        to B.
*
         IF( K.EQ.1 ) THEN
            CALL CCOPY( NRHS, B, LDB, BX, LDBX )
         ELSE
            DO 180 J = 1, K
               DSIGJ = POLES( J, 2 )
               IF( Z( J ).EQ.ZERO ) THEN
                  RWORK( J ) = ZERO
               ELSE
                  RWORK( J ) = -Z( J ) / DIFL( J ) /
     $                         ( DSIGJ+POLES( J, 1 ) ) / DIFR( J, 2 )
               END IF
               DO 110 I = 1, J - 1
                  IF( Z( J ).EQ.ZERO ) THEN
                     RWORK( I ) = ZERO
                  ELSE
                     RWORK( I ) = Z( J ) / ( SLAMC3( DSIGJ, -POLES( I+1,
     $                            2 ) )-DIFR( I, 1 ) ) /
     $                            ( DSIGJ+POLES( I, 1 ) ) / DIFR( I, 2 )
                  END IF
  110          CONTINUE
               DO 120 I = J + 1, K
                  IF( Z( J ).EQ.ZERO ) THEN
                     RWORK( I ) = ZERO
                  ELSE
                     RWORK( I ) = Z( J ) / ( SLAMC3( DSIGJ, -POLES( I,
     $                            2 ) )-DIFL( I ) ) /
     $                            ( DSIGJ+POLES( I, 1 ) ) / DIFR( I, 2 )
                  END IF
  120          CONTINUE
*
*              Since B and BX are complex, the following call to SGEMV
*              is performed in two steps (real and imaginary parts).
*
*              CALL SGEMV( 'T', K, NRHS, ONE, B, LDB, WORK, 1, ZERO,
*    $                     BX( J, 1 ), LDBX )
*
               I = K + NRHS*2
               DO 140 JCOL = 1, NRHS
                  DO 130 JROW = 1, K
                     I = I + 1
                     RWORK( I ) = REAL( B( JROW, JCOL ) )
  130             CONTINUE
  140          CONTINUE
               CALL SGEMV( 'T', K, NRHS, ONE, RWORK( 1+K+NRHS*2 ), K,
     $                     RWORK( 1 ), 1, ZERO, RWORK( 1+K ), 1 )
               I = K + NRHS*2
               DO 160 JCOL = 1, NRHS
                  DO 150 JROW = 1, K
                     I = I + 1
                     RWORK( I ) = AIMAG( B( JROW, JCOL ) )
  150             CONTINUE
  160          CONTINUE
               CALL SGEMV( 'T', K, NRHS, ONE, RWORK( 1+K+NRHS*2 ), K,
     $                     RWORK( 1 ), 1, ZERO, RWORK( 1+K+NRHS ), 1 )
               DO 170 JCOL = 1, NRHS
                  BX( J, JCOL ) = CMPLX( RWORK( JCOL+K ),
     $                            RWORK( JCOL+K+NRHS ) )
  170          CONTINUE
  180       CONTINUE
         END IF
*
*        Step (2R): if SQRE = 1, apply back the rotation that is
*        related to the right null space of the subproblem.
*
         IF( SQRE.EQ.1 ) THEN
            CALL CCOPY( NRHS, B( M, 1 ), LDB, BX( M, 1 ), LDBX )
            CALL CSROT( NRHS, BX( 1, 1 ), LDBX, BX( M, 1 ), LDBX, C, S )
         END IF
         IF( K.LT.MAX( M, N ) )
     $      CALL CLACPY( 'A', N-K, NRHS, B( K+1, 1 ), LDB,
     $                   BX( K+1, 1 ), LDBX )
*
*        Step (3R): permute rows of B.
*
         CALL CCOPY( NRHS, BX( 1, 1 ), LDBX, B( NLP1, 1 ), LDB )
         IF( SQRE.EQ.1 ) THEN
            CALL CCOPY( NRHS, BX( M, 1 ), LDBX, B( M, 1 ), LDB )
         END IF
         DO 190 I = 2, N
            CALL CCOPY( NRHS, BX( I, 1 ), LDBX, B( PERM( I ), 1 ), LDB )
  190    CONTINUE
*
*        Step (4R): apply back the Givens rotations performed.
*
         DO 200 I = GIVPTR, 1, -1
            CALL CSROT( NRHS, B( GIVCOL( I, 2 ), 1 ), LDB,
     $                  B( GIVCOL( I, 1 ), 1 ), LDB, GIVNUM( I, 2 ),
     $                  -GIVNUM( I, 1 ) )
  200    CONTINUE
      END IF
*
      RETURN
*
*     End of CLALS0
*
      END
