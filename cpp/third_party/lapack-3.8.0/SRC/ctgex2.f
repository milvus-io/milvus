*> \brief \b CTGEX2 swaps adjacent diagonal blocks in an upper (quasi) triangular matrix pair by an unitary equivalence transformation.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CTGEX2 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/ctgex2.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/ctgex2.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/ctgex2.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CTGEX2( WANTQ, WANTZ, N, A, LDA, B, LDB, Q, LDQ, Z,
*                          LDZ, J1, INFO )
*
*       .. Scalar Arguments ..
*       LOGICAL            WANTQ, WANTZ
*       INTEGER            INFO, J1, LDA, LDB, LDQ, LDZ, N
*       ..
*       .. Array Arguments ..
*       COMPLEX            A( LDA, * ), B( LDB, * ), Q( LDQ, * ),
*      $                   Z( LDZ, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CTGEX2 swaps adjacent diagonal 1 by 1 blocks (A11,B11) and (A22,B22)
*> in an upper triangular matrix pair (A, B) by an unitary equivalence
*> transformation.
*>
*> (A, B) must be in generalized Schur canonical form, that is, A and
*> B are both upper triangular.
*>
*> Optionally, the matrices Q and Z of generalized Schur vectors are
*> updated.
*>
*>        Q(in) * A(in) * Z(in)**H = Q(out) * A(out) * Z(out)**H
*>        Q(in) * B(in) * Z(in)**H = Q(out) * B(out) * Z(out)**H
*>
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] WANTQ
*> \verbatim
*>          WANTQ is LOGICAL
*>          .TRUE. : update the left transformation matrix Q;
*>          .FALSE.: do not update Q.
*> \endverbatim
*>
*> \param[in] WANTZ
*> \verbatim
*>          WANTZ is LOGICAL
*>          .TRUE. : update the right transformation matrix Z;
*>          .FALSE.: do not update Z.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrices A and B. N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N)
*>          On entry, the matrix A in the pair (A, B).
*>          On exit, the updated matrix A.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A. LDA >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX array, dimension (LDB,N)
*>          On entry, the matrix B in the pair (A, B).
*>          On exit, the updated matrix B.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B. LDB >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] Q
*> \verbatim
*>          Q is COMPLEX array, dimension (LDQ,N)
*>          If WANTQ = .TRUE, on entry, the unitary matrix Q. On exit,
*>          the updated matrix Q.
*>          Not referenced if WANTQ = .FALSE..
*> \endverbatim
*>
*> \param[in] LDQ
*> \verbatim
*>          LDQ is INTEGER
*>          The leading dimension of the array Q. LDQ >= 1;
*>          If WANTQ = .TRUE., LDQ >= N.
*> \endverbatim
*>
*> \param[in,out] Z
*> \verbatim
*>          Z is COMPLEX array, dimension (LDZ,N)
*>          If WANTZ = .TRUE, on entry, the unitary matrix Z. On exit,
*>          the updated matrix Z.
*>          Not referenced if WANTZ = .FALSE..
*> \endverbatim
*>
*> \param[in] LDZ
*> \verbatim
*>          LDZ is INTEGER
*>          The leading dimension of the array Z. LDZ >= 1;
*>          If WANTZ = .TRUE., LDZ >= N.
*> \endverbatim
*>
*> \param[in] J1
*> \verbatim
*>          J1 is INTEGER
*>          The index to the first block (A11, B11).
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>           =0:  Successful exit.
*>           =1:  The transformed matrix pair (A, B) would be too far
*>                from generalized Schur form; the problem is ill-
*>                conditioned.
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
*> \ingroup complexGEauxiliary
*
*> \par Further Details:
*  =====================
*>
*>  In the current code both weak and strong stability tests are
*>  performed. The user can omit the strong stability test by changing
*>  the internal logical parameter WANDS to .FALSE.. See ref. [2] for
*>  details.
*
*> \par Contributors:
*  ==================
*>
*>     Bo Kagstrom and Peter Poromaa, Department of Computing Science,
*>     Umea University, S-901 87 Umea, Sweden.
*
*> \par References:
*  ================
*>
*>  [1] B. Kagstrom; A Direct Method for Reordering Eigenvalues in the
*>      Generalized Real Schur Form of a Regular Matrix Pair (A, B), in
*>      M.S. Moonen et al (eds), Linear Algebra for Large Scale and
*>      Real-Time Applications, Kluwer Academic Publ. 1993, pp 195-218.
*> \n
*>  [2] B. Kagstrom and P. Poromaa; Computing Eigenspaces with Specified
*>      Eigenvalues of a Regular Matrix Pair (A, B) and Condition
*>      Estimation: Theory, Algorithms and Software, Report UMINF-94.04,
*>      Department of Computing Science, Umea University, S-901 87 Umea,
*>      Sweden, 1994. Also as LAPACK Working Note 87. To appear in
*>      Numerical Algorithms, 1996.
*>
*  =====================================================================
      SUBROUTINE CTGEX2( WANTQ, WANTZ, N, A, LDA, B, LDB, Q, LDQ, Z,
     $                   LDZ, J1, INFO )
*
*  -- LAPACK auxiliary routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      LOGICAL            WANTQ, WANTZ
      INTEGER            INFO, J1, LDA, LDB, LDQ, LDZ, N
*     ..
*     .. Array Arguments ..
      COMPLEX            A( LDA, * ), B( LDB, * ), Q( LDQ, * ),
     $                   Z( LDZ, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX            CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0E+0, 0.0E+0 ),
     $                   CONE = ( 1.0E+0, 0.0E+0 ) )
      REAL               TWENTY
      PARAMETER          ( TWENTY = 2.0E+1 )
      INTEGER            LDST
      PARAMETER          ( LDST = 2 )
      LOGICAL            WANDS
      PARAMETER          ( WANDS = .TRUE. )
*     ..
*     .. Local Scalars ..
      LOGICAL            STRONG, WEAK
      INTEGER            I, M
      REAL               CQ, CZ, EPS, SA, SB, SCALE, SMLNUM, SS, SUM,
     $                   THRESH, WS
      COMPLEX            CDUM, F, G, SQ, SZ
*     ..
*     .. Local Arrays ..
      COMPLEX            S( LDST, LDST ), T( LDST, LDST ), WORK( 8 )
*     ..
*     .. External Functions ..
      REAL               SLAMCH
      EXTERNAL           SLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           CLACPY, CLARTG, CLASSQ, CROT
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, CONJG, MAX, REAL, SQRT
*     ..
*     .. Executable Statements ..
*
      INFO = 0
*
*     Quick return if possible
*
      IF( N.LE.1 )
     $   RETURN
*
      M = LDST
      WEAK = .FALSE.
      STRONG = .FALSE.
*
*     Make a local copy of selected block in (A, B)
*
      CALL CLACPY( 'Full', M, M, A( J1, J1 ), LDA, S, LDST )
      CALL CLACPY( 'Full', M, M, B( J1, J1 ), LDB, T, LDST )
*
*     Compute the threshold for testing the acceptance of swapping.
*
      EPS = SLAMCH( 'P' )
      SMLNUM = SLAMCH( 'S' ) / EPS
      SCALE = REAL( CZERO )
      SUM = REAL( CONE )
      CALL CLACPY( 'Full', M, M, S, LDST, WORK, M )
      CALL CLACPY( 'Full', M, M, T, LDST, WORK( M*M+1 ), M )
      CALL CLASSQ( 2*M*M, WORK, 1, SCALE, SUM )
      SA = SCALE*SQRT( SUM )
*
*     THRES has been changed from
*        THRESH = MAX( TEN*EPS*SA, SMLNUM )
*     to
*        THRESH = MAX( TWENTY*EPS*SA, SMLNUM )
*     on 04/01/10.
*     "Bug" reported by Ondra Kamenik, confirmed by Julie Langou, fixed by
*     Jim Demmel and Guillaume Revy. See forum post 1783.
*
      THRESH = MAX( TWENTY*EPS*SA, SMLNUM )
*
*     Compute unitary QL and RQ that swap 1-by-1 and 1-by-1 blocks
*     using Givens rotations and perform the swap tentatively.
*
      F = S( 2, 2 )*T( 1, 1 ) - T( 2, 2 )*S( 1, 1 )
      G = S( 2, 2 )*T( 1, 2 ) - T( 2, 2 )*S( 1, 2 )
      SA = ABS( S( 2, 2 ) )
      SB = ABS( T( 2, 2 ) )
      CALL CLARTG( G, F, CZ, SZ, CDUM )
      SZ = -SZ
      CALL CROT( 2, S( 1, 1 ), 1, S( 1, 2 ), 1, CZ, CONJG( SZ ) )
      CALL CROT( 2, T( 1, 1 ), 1, T( 1, 2 ), 1, CZ, CONJG( SZ ) )
      IF( SA.GE.SB ) THEN
         CALL CLARTG( S( 1, 1 ), S( 2, 1 ), CQ, SQ, CDUM )
      ELSE
         CALL CLARTG( T( 1, 1 ), T( 2, 1 ), CQ, SQ, CDUM )
      END IF
      CALL CROT( 2, S( 1, 1 ), LDST, S( 2, 1 ), LDST, CQ, SQ )
      CALL CROT( 2, T( 1, 1 ), LDST, T( 2, 1 ), LDST, CQ, SQ )
*
*     Weak stability test: |S21| + |T21| <= O(EPS F-norm((S, T)))
*
      WS = ABS( S( 2, 1 ) ) + ABS( T( 2, 1 ) )
      WEAK = WS.LE.THRESH
      IF( .NOT.WEAK )
     $   GO TO 20
*
      IF( WANDS ) THEN
*
*        Strong stability test:
*           F-norm((A-QL**H*S*QR, B-QL**H*T*QR)) <= O(EPS*F-norm((A, B)))
*
         CALL CLACPY( 'Full', M, M, S, LDST, WORK, M )
         CALL CLACPY( 'Full', M, M, T, LDST, WORK( M*M+1 ), M )
         CALL CROT( 2, WORK, 1, WORK( 3 ), 1, CZ, -CONJG( SZ ) )
         CALL CROT( 2, WORK( 5 ), 1, WORK( 7 ), 1, CZ, -CONJG( SZ ) )
         CALL CROT( 2, WORK, 2, WORK( 2 ), 2, CQ, -SQ )
         CALL CROT( 2, WORK( 5 ), 2, WORK( 6 ), 2, CQ, -SQ )
         DO 10 I = 1, 2
            WORK( I ) = WORK( I ) - A( J1+I-1, J1 )
            WORK( I+2 ) = WORK( I+2 ) - A( J1+I-1, J1+1 )
            WORK( I+4 ) = WORK( I+4 ) - B( J1+I-1, J1 )
            WORK( I+6 ) = WORK( I+6 ) - B( J1+I-1, J1+1 )
   10    CONTINUE
         SCALE = REAL( CZERO )
         SUM = REAL( CONE )
         CALL CLASSQ( 2*M*M, WORK, 1, SCALE, SUM )
         SS = SCALE*SQRT( SUM )
         STRONG = SS.LE.THRESH
         IF( .NOT.STRONG )
     $      GO TO 20
      END IF
*
*     If the swap is accepted ("weakly" and "strongly"), apply the
*     equivalence transformations to the original matrix pair (A,B)
*
      CALL CROT( J1+1, A( 1, J1 ), 1, A( 1, J1+1 ), 1, CZ, CONJG( SZ ) )
      CALL CROT( J1+1, B( 1, J1 ), 1, B( 1, J1+1 ), 1, CZ, CONJG( SZ ) )
      CALL CROT( N-J1+1, A( J1, J1 ), LDA, A( J1+1, J1 ), LDA, CQ, SQ )
      CALL CROT( N-J1+1, B( J1, J1 ), LDB, B( J1+1, J1 ), LDB, CQ, SQ )
*
*     Set  N1 by N2 (2,1) blocks to 0
*
      A( J1+1, J1 ) = CZERO
      B( J1+1, J1 ) = CZERO
*
*     Accumulate transformations into Q and Z if requested.
*
      IF( WANTZ )
     $   CALL CROT( N, Z( 1, J1 ), 1, Z( 1, J1+1 ), 1, CZ, CONJG( SZ ) )
      IF( WANTQ )
     $   CALL CROT( N, Q( 1, J1 ), 1, Q( 1, J1+1 ), 1, CQ, CONJG( SQ ) )
*
*     Exit with INFO = 0 if swap was successfully performed.
*
      RETURN
*
*     Exit with INFO = 1 if swap was rejected.
*
   20 CONTINUE
      INFO = 1
      RETURN
*
*     End of CTGEX2
*
      END
