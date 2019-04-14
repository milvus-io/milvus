*> \brief \b CTGEVC
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CTGEVC + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/ctgevc.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/ctgevc.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/ctgevc.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CTGEVC( SIDE, HOWMNY, SELECT, N, S, LDS, P, LDP, VL,
*                          LDVL, VR, LDVR, MM, M, WORK, RWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          HOWMNY, SIDE
*       INTEGER            INFO, LDP, LDS, LDVL, LDVR, M, MM, N
*       ..
*       .. Array Arguments ..
*       LOGICAL            SELECT( * )
*       REAL               RWORK( * )
*       COMPLEX            P( LDP, * ), S( LDS, * ), VL( LDVL, * ),
*      $                   VR( LDVR, * ), WORK( * )
*       ..
*
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CTGEVC computes some or all of the right and/or left eigenvectors of
*> a pair of complex matrices (S,P), where S and P are upper triangular.
*> Matrix pairs of this type are produced by the generalized Schur
*> factorization of a complex matrix pair (A,B):
*>
*>    A = Q*S*Z**H,  B = Q*P*Z**H
*>
*> as computed by CGGHRD + CHGEQZ.
*>
*> The right eigenvector x and the left eigenvector y of (S,P)
*> corresponding to an eigenvalue w are defined by:
*>
*>    S*x = w*P*x,  (y**H)*S = w*(y**H)*P,
*>
*> where y**H denotes the conjugate tranpose of y.
*> The eigenvalues are not input to this routine, but are computed
*> directly from the diagonal elements of S and P.
*>
*> This routine returns the matrices X and/or Y of right and left
*> eigenvectors of (S,P), or the products Z*X and/or Q*Y,
*> where Z and Q are input matrices.
*> If Q and Z are the unitary factors from the generalized Schur
*> factorization of a matrix pair (A,B), then Z*X and Q*Y
*> are the matrices of right and left eigenvectors of (A,B).
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] SIDE
*> \verbatim
*>          SIDE is CHARACTER*1
*>          = 'R': compute right eigenvectors only;
*>          = 'L': compute left eigenvectors only;
*>          = 'B': compute both right and left eigenvectors.
*> \endverbatim
*>
*> \param[in] HOWMNY
*> \verbatim
*>          HOWMNY is CHARACTER*1
*>          = 'A': compute all right and/or left eigenvectors;
*>          = 'B': compute all right and/or left eigenvectors,
*>                 backtransformed by the matrices in VR and/or VL;
*>          = 'S': compute selected right and/or left eigenvectors,
*>                 specified by the logical array SELECT.
*> \endverbatim
*>
*> \param[in] SELECT
*> \verbatim
*>          SELECT is LOGICAL array, dimension (N)
*>          If HOWMNY='S', SELECT specifies the eigenvectors to be
*>          computed.  The eigenvector corresponding to the j-th
*>          eigenvalue is computed if SELECT(j) = .TRUE..
*>          Not referenced if HOWMNY = 'A' or 'B'.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrices S and P.  N >= 0.
*> \endverbatim
*>
*> \param[in] S
*> \verbatim
*>          S is COMPLEX array, dimension (LDS,N)
*>          The upper triangular matrix S from a generalized Schur
*>          factorization, as computed by CHGEQZ.
*> \endverbatim
*>
*> \param[in] LDS
*> \verbatim
*>          LDS is INTEGER
*>          The leading dimension of array S.  LDS >= max(1,N).
*> \endverbatim
*>
*> \param[in] P
*> \verbatim
*>          P is COMPLEX array, dimension (LDP,N)
*>          The upper triangular matrix P from a generalized Schur
*>          factorization, as computed by CHGEQZ.  P must have real
*>          diagonal elements.
*> \endverbatim
*>
*> \param[in] LDP
*> \verbatim
*>          LDP is INTEGER
*>          The leading dimension of array P.  LDP >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] VL
*> \verbatim
*>          VL is COMPLEX array, dimension (LDVL,MM)
*>          On entry, if SIDE = 'L' or 'B' and HOWMNY = 'B', VL must
*>          contain an N-by-N matrix Q (usually the unitary matrix Q
*>          of left Schur vectors returned by CHGEQZ).
*>          On exit, if SIDE = 'L' or 'B', VL contains:
*>          if HOWMNY = 'A', the matrix Y of left eigenvectors of (S,P);
*>          if HOWMNY = 'B', the matrix Q*Y;
*>          if HOWMNY = 'S', the left eigenvectors of (S,P) specified by
*>                      SELECT, stored consecutively in the columns of
*>                      VL, in the same order as their eigenvalues.
*>          Not referenced if SIDE = 'R'.
*> \endverbatim
*>
*> \param[in] LDVL
*> \verbatim
*>          LDVL is INTEGER
*>          The leading dimension of array VL.  LDVL >= 1, and if
*>          SIDE = 'L' or 'l' or 'B' or 'b', LDVL >= N.
*> \endverbatim
*>
*> \param[in,out] VR
*> \verbatim
*>          VR is COMPLEX array, dimension (LDVR,MM)
*>          On entry, if SIDE = 'R' or 'B' and HOWMNY = 'B', VR must
*>          contain an N-by-N matrix Q (usually the unitary matrix Z
*>          of right Schur vectors returned by CHGEQZ).
*>          On exit, if SIDE = 'R' or 'B', VR contains:
*>          if HOWMNY = 'A', the matrix X of right eigenvectors of (S,P);
*>          if HOWMNY = 'B', the matrix Z*X;
*>          if HOWMNY = 'S', the right eigenvectors of (S,P) specified by
*>                      SELECT, stored consecutively in the columns of
*>                      VR, in the same order as their eigenvalues.
*>          Not referenced if SIDE = 'L'.
*> \endverbatim
*>
*> \param[in] LDVR
*> \verbatim
*>          LDVR is INTEGER
*>          The leading dimension of the array VR.  LDVR >= 1, and if
*>          SIDE = 'R' or 'B', LDVR >= N.
*> \endverbatim
*>
*> \param[in] MM
*> \verbatim
*>          MM is INTEGER
*>          The number of columns in the arrays VL and/or VR. MM >= M.
*> \endverbatim
*>
*> \param[out] M
*> \verbatim
*>          M is INTEGER
*>          The number of columns in the arrays VL and/or VR actually
*>          used to store the eigenvectors.  If HOWMNY = 'A' or 'B', M
*>          is set to N.  Each selected eigenvector occupies one column.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX array, dimension (2*N)
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is REAL array, dimension (2*N)
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
*> \ingroup complexGEcomputational
*
*  =====================================================================
      SUBROUTINE CTGEVC( SIDE, HOWMNY, SELECT, N, S, LDS, P, LDP, VL,
     $                   LDVL, VR, LDVR, MM, M, WORK, RWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          HOWMNY, SIDE
      INTEGER            INFO, LDP, LDS, LDVL, LDVR, M, MM, N
*     ..
*     .. Array Arguments ..
      LOGICAL            SELECT( * )
      REAL               RWORK( * )
      COMPLEX            P( LDP, * ), S( LDS, * ), VL( LDVL, * ),
     $                   VR( LDVR, * ), WORK( * )
*     ..
*
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
      COMPLEX            CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0E+0, 0.0E+0 ),
     $                   CONE = ( 1.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            COMPL, COMPR, ILALL, ILBACK, ILBBAD, ILCOMP,
     $                   LSA, LSB
      INTEGER            I, IBEG, IEIG, IEND, IHWMNY, IM, ISIDE, ISRC,
     $                   J, JE, JR
      REAL               ACOEFA, ACOEFF, ANORM, ASCALE, BCOEFA, BIG,
     $                   BIGNUM, BNORM, BSCALE, DMIN, SAFMIN, SBETA,
     $                   SCALE, SMALL, TEMP, ULP, XMAX
      COMPLEX            BCOEFF, CA, CB, D, SALPHA, SUM, SUMA, SUMB, X
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      REAL               SLAMCH
      COMPLEX            CLADIV
      EXTERNAL           LSAME, SLAMCH, CLADIV
*     ..
*     .. External Subroutines ..
      EXTERNAL           CGEMV, SLABAD, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, AIMAG, CMPLX, CONJG, MAX, MIN, REAL
*     ..
*     .. Statement Functions ..
      REAL               ABS1
*     ..
*     .. Statement Function definitions ..
      ABS1( X ) = ABS( REAL( X ) ) + ABS( AIMAG( X ) )
*     ..
*     .. Executable Statements ..
*
*     Decode and Test the input parameters
*
      IF( LSAME( HOWMNY, 'A' ) ) THEN
         IHWMNY = 1
         ILALL = .TRUE.
         ILBACK = .FALSE.
      ELSE IF( LSAME( HOWMNY, 'S' ) ) THEN
         IHWMNY = 2
         ILALL = .FALSE.
         ILBACK = .FALSE.
      ELSE IF( LSAME( HOWMNY, 'B' ) ) THEN
         IHWMNY = 3
         ILALL = .TRUE.
         ILBACK = .TRUE.
      ELSE
         IHWMNY = -1
      END IF
*
      IF( LSAME( SIDE, 'R' ) ) THEN
         ISIDE = 1
         COMPL = .FALSE.
         COMPR = .TRUE.
      ELSE IF( LSAME( SIDE, 'L' ) ) THEN
         ISIDE = 2
         COMPL = .TRUE.
         COMPR = .FALSE.
      ELSE IF( LSAME( SIDE, 'B' ) ) THEN
         ISIDE = 3
         COMPL = .TRUE.
         COMPR = .TRUE.
      ELSE
         ISIDE = -1
      END IF
*
      INFO = 0
      IF( ISIDE.LT.0 ) THEN
         INFO = -1
      ELSE IF( IHWMNY.LT.0 ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -4
      ELSE IF( LDS.LT.MAX( 1, N ) ) THEN
         INFO = -6
      ELSE IF( LDP.LT.MAX( 1, N ) ) THEN
         INFO = -8
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CTGEVC', -INFO )
         RETURN
      END IF
*
*     Count the number of eigenvectors
*
      IF( .NOT.ILALL ) THEN
         IM = 0
         DO 10 J = 1, N
            IF( SELECT( J ) )
     $         IM = IM + 1
   10    CONTINUE
      ELSE
         IM = N
      END IF
*
*     Check diagonal of B
*
      ILBBAD = .FALSE.
      DO 20 J = 1, N
         IF( AIMAG( P( J, J ) ).NE.ZERO )
     $      ILBBAD = .TRUE.
   20 CONTINUE
*
      IF( ILBBAD ) THEN
         INFO = -7
      ELSE IF( COMPL .AND. LDVL.LT.N .OR. LDVL.LT.1 ) THEN
         INFO = -10
      ELSE IF( COMPR .AND. LDVR.LT.N .OR. LDVR.LT.1 ) THEN
         INFO = -12
      ELSE IF( MM.LT.IM ) THEN
         INFO = -13
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CTGEVC', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      M = IM
      IF( N.EQ.0 )
     $   RETURN
*
*     Machine Constants
*
      SAFMIN = SLAMCH( 'Safe minimum' )
      BIG = ONE / SAFMIN
      CALL SLABAD( SAFMIN, BIG )
      ULP = SLAMCH( 'Epsilon' )*SLAMCH( 'Base' )
      SMALL = SAFMIN*N / ULP
      BIG = ONE / SMALL
      BIGNUM = ONE / ( SAFMIN*N )
*
*     Compute the 1-norm of each column of the strictly upper triangular
*     part of A and B to check for possible overflow in the triangular
*     solver.
*
      ANORM = ABS1( S( 1, 1 ) )
      BNORM = ABS1( P( 1, 1 ) )
      RWORK( 1 ) = ZERO
      RWORK( N+1 ) = ZERO
      DO 40 J = 2, N
         RWORK( J ) = ZERO
         RWORK( N+J ) = ZERO
         DO 30 I = 1, J - 1
            RWORK( J ) = RWORK( J ) + ABS1( S( I, J ) )
            RWORK( N+J ) = RWORK( N+J ) + ABS1( P( I, J ) )
   30    CONTINUE
         ANORM = MAX( ANORM, RWORK( J )+ABS1( S( J, J ) ) )
         BNORM = MAX( BNORM, RWORK( N+J )+ABS1( P( J, J ) ) )
   40 CONTINUE
*
      ASCALE = ONE / MAX( ANORM, SAFMIN )
      BSCALE = ONE / MAX( BNORM, SAFMIN )
*
*     Left eigenvectors
*
      IF( COMPL ) THEN
         IEIG = 0
*
*        Main loop over eigenvalues
*
         DO 140 JE = 1, N
            IF( ILALL ) THEN
               ILCOMP = .TRUE.
            ELSE
               ILCOMP = SELECT( JE )
            END IF
            IF( ILCOMP ) THEN
               IEIG = IEIG + 1
*
               IF( ABS1( S( JE, JE ) ).LE.SAFMIN .AND.
     $             ABS( REAL( P( JE, JE ) ) ).LE.SAFMIN ) THEN
*
*                 Singular matrix pencil -- return unit eigenvector
*
                  DO 50 JR = 1, N
                     VL( JR, IEIG ) = CZERO
   50             CONTINUE
                  VL( IEIG, IEIG ) = CONE
                  GO TO 140
               END IF
*
*              Non-singular eigenvalue:
*              Compute coefficients  a  and  b  in
*                   H
*                 y  ( a A - b B ) = 0
*
               TEMP = ONE / MAX( ABS1( S( JE, JE ) )*ASCALE,
     $                ABS( REAL( P( JE, JE ) ) )*BSCALE, SAFMIN )
               SALPHA = ( TEMP*S( JE, JE ) )*ASCALE
               SBETA = ( TEMP*REAL( P( JE, JE ) ) )*BSCALE
               ACOEFF = SBETA*ASCALE
               BCOEFF = SALPHA*BSCALE
*
*              Scale to avoid underflow
*
               LSA = ABS( SBETA ).GE.SAFMIN .AND. ABS( ACOEFF ).LT.SMALL
               LSB = ABS1( SALPHA ).GE.SAFMIN .AND. ABS1( BCOEFF ).LT.
     $               SMALL
*
               SCALE = ONE
               IF( LSA )
     $            SCALE = ( SMALL / ABS( SBETA ) )*MIN( ANORM, BIG )
               IF( LSB )
     $            SCALE = MAX( SCALE, ( SMALL / ABS1( SALPHA ) )*
     $                    MIN( BNORM, BIG ) )
               IF( LSA .OR. LSB ) THEN
                  SCALE = MIN( SCALE, ONE /
     $                    ( SAFMIN*MAX( ONE, ABS( ACOEFF ),
     $                    ABS1( BCOEFF ) ) ) )
                  IF( LSA ) THEN
                     ACOEFF = ASCALE*( SCALE*SBETA )
                  ELSE
                     ACOEFF = SCALE*ACOEFF
                  END IF
                  IF( LSB ) THEN
                     BCOEFF = BSCALE*( SCALE*SALPHA )
                  ELSE
                     BCOEFF = SCALE*BCOEFF
                  END IF
               END IF
*
               ACOEFA = ABS( ACOEFF )
               BCOEFA = ABS1( BCOEFF )
               XMAX = ONE
               DO 60 JR = 1, N
                  WORK( JR ) = CZERO
   60          CONTINUE
               WORK( JE ) = CONE
               DMIN = MAX( ULP*ACOEFA*ANORM, ULP*BCOEFA*BNORM, SAFMIN )
*
*                                              H
*              Triangular solve of  (a A - b B)  y = 0
*
*                                      H
*              (rowwise in  (a A - b B) , or columnwise in a A - b B)
*
               DO 100 J = JE + 1, N
*
*                 Compute
*                       j-1
*                 SUM = sum  conjg( a*S(k,j) - b*P(k,j) )*x(k)
*                       k=je
*                 (Scale if necessary)
*
                  TEMP = ONE / XMAX
                  IF( ACOEFA*RWORK( J )+BCOEFA*RWORK( N+J ).GT.BIGNUM*
     $                TEMP ) THEN
                     DO 70 JR = JE, J - 1
                        WORK( JR ) = TEMP*WORK( JR )
   70                CONTINUE
                     XMAX = ONE
                  END IF
                  SUMA = CZERO
                  SUMB = CZERO
*
                  DO 80 JR = JE, J - 1
                     SUMA = SUMA + CONJG( S( JR, J ) )*WORK( JR )
                     SUMB = SUMB + CONJG( P( JR, J ) )*WORK( JR )
   80             CONTINUE
                  SUM = ACOEFF*SUMA - CONJG( BCOEFF )*SUMB
*
*                 Form x(j) = - SUM / conjg( a*S(j,j) - b*P(j,j) )
*
*                 with scaling and perturbation of the denominator
*
                  D = CONJG( ACOEFF*S( J, J )-BCOEFF*P( J, J ) )
                  IF( ABS1( D ).LE.DMIN )
     $               D = CMPLX( DMIN )
*
                  IF( ABS1( D ).LT.ONE ) THEN
                     IF( ABS1( SUM ).GE.BIGNUM*ABS1( D ) ) THEN
                        TEMP = ONE / ABS1( SUM )
                        DO 90 JR = JE, J - 1
                           WORK( JR ) = TEMP*WORK( JR )
   90                   CONTINUE
                        XMAX = TEMP*XMAX
                        SUM = TEMP*SUM
                     END IF
                  END IF
                  WORK( J ) = CLADIV( -SUM, D )
                  XMAX = MAX( XMAX, ABS1( WORK( J ) ) )
  100          CONTINUE
*
*              Back transform eigenvector if HOWMNY='B'.
*
               IF( ILBACK ) THEN
                  CALL CGEMV( 'N', N, N+1-JE, CONE, VL( 1, JE ), LDVL,
     $                        WORK( JE ), 1, CZERO, WORK( N+1 ), 1 )
                  ISRC = 2
                  IBEG = 1
               ELSE
                  ISRC = 1
                  IBEG = JE
               END IF
*
*              Copy and scale eigenvector into column of VL
*
               XMAX = ZERO
               DO 110 JR = IBEG, N
                  XMAX = MAX( XMAX, ABS1( WORK( ( ISRC-1 )*N+JR ) ) )
  110          CONTINUE
*
               IF( XMAX.GT.SAFMIN ) THEN
                  TEMP = ONE / XMAX
                  DO 120 JR = IBEG, N
                     VL( JR, IEIG ) = TEMP*WORK( ( ISRC-1 )*N+JR )
  120             CONTINUE
               ELSE
                  IBEG = N + 1
               END IF
*
               DO 130 JR = 1, IBEG - 1
                  VL( JR, IEIG ) = CZERO
  130          CONTINUE
*
            END IF
  140    CONTINUE
      END IF
*
*     Right eigenvectors
*
      IF( COMPR ) THEN
         IEIG = IM + 1
*
*        Main loop over eigenvalues
*
         DO 250 JE = N, 1, -1
            IF( ILALL ) THEN
               ILCOMP = .TRUE.
            ELSE
               ILCOMP = SELECT( JE )
            END IF
            IF( ILCOMP ) THEN
               IEIG = IEIG - 1
*
               IF( ABS1( S( JE, JE ) ).LE.SAFMIN .AND.
     $             ABS( REAL( P( JE, JE ) ) ).LE.SAFMIN ) THEN
*
*                 Singular matrix pencil -- return unit eigenvector
*
                  DO 150 JR = 1, N
                     VR( JR, IEIG ) = CZERO
  150             CONTINUE
                  VR( IEIG, IEIG ) = CONE
                  GO TO 250
               END IF
*
*              Non-singular eigenvalue:
*              Compute coefficients  a  and  b  in
*
*              ( a A - b B ) x  = 0
*
               TEMP = ONE / MAX( ABS1( S( JE, JE ) )*ASCALE,
     $                ABS( REAL( P( JE, JE ) ) )*BSCALE, SAFMIN )
               SALPHA = ( TEMP*S( JE, JE ) )*ASCALE
               SBETA = ( TEMP*REAL( P( JE, JE ) ) )*BSCALE
               ACOEFF = SBETA*ASCALE
               BCOEFF = SALPHA*BSCALE
*
*              Scale to avoid underflow
*
               LSA = ABS( SBETA ).GE.SAFMIN .AND. ABS( ACOEFF ).LT.SMALL
               LSB = ABS1( SALPHA ).GE.SAFMIN .AND. ABS1( BCOEFF ).LT.
     $               SMALL
*
               SCALE = ONE
               IF( LSA )
     $            SCALE = ( SMALL / ABS( SBETA ) )*MIN( ANORM, BIG )
               IF( LSB )
     $            SCALE = MAX( SCALE, ( SMALL / ABS1( SALPHA ) )*
     $                    MIN( BNORM, BIG ) )
               IF( LSA .OR. LSB ) THEN
                  SCALE = MIN( SCALE, ONE /
     $                    ( SAFMIN*MAX( ONE, ABS( ACOEFF ),
     $                    ABS1( BCOEFF ) ) ) )
                  IF( LSA ) THEN
                     ACOEFF = ASCALE*( SCALE*SBETA )
                  ELSE
                     ACOEFF = SCALE*ACOEFF
                  END IF
                  IF( LSB ) THEN
                     BCOEFF = BSCALE*( SCALE*SALPHA )
                  ELSE
                     BCOEFF = SCALE*BCOEFF
                  END IF
               END IF
*
               ACOEFA = ABS( ACOEFF )
               BCOEFA = ABS1( BCOEFF )
               XMAX = ONE
               DO 160 JR = 1, N
                  WORK( JR ) = CZERO
  160          CONTINUE
               WORK( JE ) = CONE
               DMIN = MAX( ULP*ACOEFA*ANORM, ULP*BCOEFA*BNORM, SAFMIN )
*
*              Triangular solve of  (a A - b B) x = 0  (columnwise)
*
*              WORK(1:j-1) contains sums w,
*              WORK(j+1:JE) contains x
*
               DO 170 JR = 1, JE - 1
                  WORK( JR ) = ACOEFF*S( JR, JE ) - BCOEFF*P( JR, JE )
  170          CONTINUE
               WORK( JE ) = CONE
*
               DO 210 J = JE - 1, 1, -1
*
*                 Form x(j) := - w(j) / d
*                 with scaling and perturbation of the denominator
*
                  D = ACOEFF*S( J, J ) - BCOEFF*P( J, J )
                  IF( ABS1( D ).LE.DMIN )
     $               D = CMPLX( DMIN )
*
                  IF( ABS1( D ).LT.ONE ) THEN
                     IF( ABS1( WORK( J ) ).GE.BIGNUM*ABS1( D ) ) THEN
                        TEMP = ONE / ABS1( WORK( J ) )
                        DO 180 JR = 1, JE
                           WORK( JR ) = TEMP*WORK( JR )
  180                   CONTINUE
                     END IF
                  END IF
*
                  WORK( J ) = CLADIV( -WORK( J ), D )
*
                  IF( J.GT.1 ) THEN
*
*                    w = w + x(j)*(a S(*,j) - b P(*,j) ) with scaling
*
                     IF( ABS1( WORK( J ) ).GT.ONE ) THEN
                        TEMP = ONE / ABS1( WORK( J ) )
                        IF( ACOEFA*RWORK( J )+BCOEFA*RWORK( N+J ).GE.
     $                      BIGNUM*TEMP ) THEN
                           DO 190 JR = 1, JE
                              WORK( JR ) = TEMP*WORK( JR )
  190                      CONTINUE
                        END IF
                     END IF
*
                     CA = ACOEFF*WORK( J )
                     CB = BCOEFF*WORK( J )
                     DO 200 JR = 1, J - 1
                        WORK( JR ) = WORK( JR ) + CA*S( JR, J ) -
     $                               CB*P( JR, J )
  200                CONTINUE
                  END IF
  210          CONTINUE
*
*              Back transform eigenvector if HOWMNY='B'.
*
               IF( ILBACK ) THEN
                  CALL CGEMV( 'N', N, JE, CONE, VR, LDVR, WORK, 1,
     $                        CZERO, WORK( N+1 ), 1 )
                  ISRC = 2
                  IEND = N
               ELSE
                  ISRC = 1
                  IEND = JE
               END IF
*
*              Copy and scale eigenvector into column of VR
*
               XMAX = ZERO
               DO 220 JR = 1, IEND
                  XMAX = MAX( XMAX, ABS1( WORK( ( ISRC-1 )*N+JR ) ) )
  220          CONTINUE
*
               IF( XMAX.GT.SAFMIN ) THEN
                  TEMP = ONE / XMAX
                  DO 230 JR = 1, IEND
                     VR( JR, IEIG ) = TEMP*WORK( ( ISRC-1 )*N+JR )
  230             CONTINUE
               ELSE
                  IEND = 0
               END IF
*
               DO 240 JR = IEND + 1, N
                  VR( JR, IEIG ) = CZERO
  240          CONTINUE
*
            END IF
  250    CONTINUE
      END IF
*
      RETURN
*
*     End of CTGEVC
*
      END
