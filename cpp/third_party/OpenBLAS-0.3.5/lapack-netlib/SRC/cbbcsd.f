*> \brief \b CBBCSD
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CBBCSD + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cbbcsd.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cbbcsd.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cbbcsd.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CBBCSD( JOBU1, JOBU2, JOBV1T, JOBV2T, TRANS, M, P, Q,
*                          THETA, PHI, U1, LDU1, U2, LDU2, V1T, LDV1T,
*                          V2T, LDV2T, B11D, B11E, B12D, B12E, B21D, B21E,
*                          B22D, B22E, RWORK, LRWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          JOBU1, JOBU2, JOBV1T, JOBV2T, TRANS
*       INTEGER            INFO, LDU1, LDU2, LDV1T, LDV2T, LRWORK, M, P, Q
*       ..
*       .. Array Arguments ..
*       REAL               B11D( * ), B11E( * ), B12D( * ), B12E( * ),
*      $                   B21D( * ), B21E( * ), B22D( * ), B22E( * ),
*      $                   PHI( * ), THETA( * ), RWORK( * )
*       COMPLEX            U1( LDU1, * ), U2( LDU2, * ), V1T( LDV1T, * ),
*      $                   V2T( LDV2T, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CBBCSD computes the CS decomposition of a unitary matrix in
*> bidiagonal-block form,
*>
*>
*>     [ B11 | B12 0  0 ]
*>     [  0  |  0 -I  0 ]
*> X = [----------------]
*>     [ B21 | B22 0  0 ]
*>     [  0  |  0  0  I ]
*>
*>                               [  C | -S  0  0 ]
*>                   [ U1 |    ] [  0 |  0 -I  0 ] [ V1 |    ]**H
*>                 = [---------] [---------------] [---------]   .
*>                   [    | U2 ] [  S |  C  0  0 ] [    | V2 ]
*>                               [  0 |  0  0  I ]
*>
*> X is M-by-M, its top-left block is P-by-Q, and Q must be no larger
*> than P, M-P, or M-Q. (If Q is not the smallest index, then X must be
*> transposed and/or permuted. This can be done in constant time using
*> the TRANS and SIGNS options. See CUNCSD for details.)
*>
*> The bidiagonal matrices B11, B12, B21, and B22 are represented
*> implicitly by angles THETA(1:Q) and PHI(1:Q-1).
*>
*> The unitary matrices U1, U2, V1T, and V2T are input/output.
*> The input matrices are pre- or post-multiplied by the appropriate
*> singular vector matrices.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] JOBU1
*> \verbatim
*>          JOBU1 is CHARACTER
*>          = 'Y':      U1 is updated;
*>          otherwise:  U1 is not updated.
*> \endverbatim
*>
*> \param[in] JOBU2
*> \verbatim
*>          JOBU2 is CHARACTER
*>          = 'Y':      U2 is updated;
*>          otherwise:  U2 is not updated.
*> \endverbatim
*>
*> \param[in] JOBV1T
*> \verbatim
*>          JOBV1T is CHARACTER
*>          = 'Y':      V1T is updated;
*>          otherwise:  V1T is not updated.
*> \endverbatim
*>
*> \param[in] JOBV2T
*> \verbatim
*>          JOBV2T is CHARACTER
*>          = 'Y':      V2T is updated;
*>          otherwise:  V2T is not updated.
*> \endverbatim
*>
*> \param[in] TRANS
*> \verbatim
*>          TRANS is CHARACTER
*>          = 'T':      X, U1, U2, V1T, and V2T are stored in row-major
*>                      order;
*>          otherwise:  X, U1, U2, V1T, and V2T are stored in column-
*>                      major order.
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows and columns in X, the unitary matrix in
*>          bidiagonal-block form.
*> \endverbatim
*>
*> \param[in] P
*> \verbatim
*>          P is INTEGER
*>          The number of rows in the top-left block of X. 0 <= P <= M.
*> \endverbatim
*>
*> \param[in] Q
*> \verbatim
*>          Q is INTEGER
*>          The number of columns in the top-left block of X.
*>          0 <= Q <= MIN(P,M-P,M-Q).
*> \endverbatim
*>
*> \param[in,out] THETA
*> \verbatim
*>          THETA is REAL array, dimension (Q)
*>          On entry, the angles THETA(1),...,THETA(Q) that, along with
*>          PHI(1), ...,PHI(Q-1), define the matrix in bidiagonal-block
*>          form. On exit, the angles whose cosines and sines define the
*>          diagonal blocks in the CS decomposition.
*> \endverbatim
*>
*> \param[in,out] PHI
*> \verbatim
*>          PHI is REAL array, dimension (Q-1)
*>          The angles PHI(1),...,PHI(Q-1) that, along with THETA(1),...,
*>          THETA(Q), define the matrix in bidiagonal-block form.
*> \endverbatim
*>
*> \param[in,out] U1
*> \verbatim
*>          U1 is COMPLEX array, dimension (LDU1,P)
*>          On entry, a P-by-P matrix. On exit, U1 is postmultiplied
*>          by the left singular vector matrix common to [ B11 ; 0 ] and
*>          [ B12 0 0 ; 0 -I 0 0 ].
*> \endverbatim
*>
*> \param[in] LDU1
*> \verbatim
*>          LDU1 is INTEGER
*>          The leading dimension of the array U1, LDU1 >= MAX(1,P).
*> \endverbatim
*>
*> \param[in,out] U2
*> \verbatim
*>          U2 is COMPLEX array, dimension (LDU2,M-P)
*>          On entry, an (M-P)-by-(M-P) matrix. On exit, U2 is
*>          postmultiplied by the left singular vector matrix common to
*>          [ B21 ; 0 ] and [ B22 0 0 ; 0 0 I ].
*> \endverbatim
*>
*> \param[in] LDU2
*> \verbatim
*>          LDU2 is INTEGER
*>          The leading dimension of the array U2, LDU2 >= MAX(1,M-P).
*> \endverbatim
*>
*> \param[in,out] V1T
*> \verbatim
*>          V1T is COMPLEX array, dimension (LDV1T,Q)
*>          On entry, a Q-by-Q matrix. On exit, V1T is premultiplied
*>          by the conjugate transpose of the right singular vector
*>          matrix common to [ B11 ; 0 ] and [ B21 ; 0 ].
*> \endverbatim
*>
*> \param[in] LDV1T
*> \verbatim
*>          LDV1T is INTEGER
*>          The leading dimension of the array V1T, LDV1T >= MAX(1,Q).
*> \endverbatim
*>
*> \param[in,out] V2T
*> \verbatim
*>          V2T is COMPLEX array, dimension (LDV2T,M-Q)
*>          On entry, an (M-Q)-by-(M-Q) matrix. On exit, V2T is
*>          premultiplied by the conjugate transpose of the right
*>          singular vector matrix common to [ B12 0 0 ; 0 -I 0 ] and
*>          [ B22 0 0 ; 0 0 I ].
*> \endverbatim
*>
*> \param[in] LDV2T
*> \verbatim
*>          LDV2T is INTEGER
*>          The leading dimension of the array V2T, LDV2T >= MAX(1,M-Q).
*> \endverbatim
*>
*> \param[out] B11D
*> \verbatim
*>          B11D is REAL array, dimension (Q)
*>          When CBBCSD converges, B11D contains the cosines of THETA(1),
*>          ..., THETA(Q). If CBBCSD fails to converge, then B11D
*>          contains the diagonal of the partially reduced top-left
*>          block.
*> \endverbatim
*>
*> \param[out] B11E
*> \verbatim
*>          B11E is REAL array, dimension (Q-1)
*>          When CBBCSD converges, B11E contains zeros. If CBBCSD fails
*>          to converge, then B11E contains the superdiagonal of the
*>          partially reduced top-left block.
*> \endverbatim
*>
*> \param[out] B12D
*> \verbatim
*>          B12D is REAL array, dimension (Q)
*>          When CBBCSD converges, B12D contains the negative sines of
*>          THETA(1), ..., THETA(Q). If CBBCSD fails to converge, then
*>          B12D contains the diagonal of the partially reduced top-right
*>          block.
*> \endverbatim
*>
*> \param[out] B12E
*> \verbatim
*>          B12E is REAL array, dimension (Q-1)
*>          When CBBCSD converges, B12E contains zeros. If CBBCSD fails
*>          to converge, then B12E contains the subdiagonal of the
*>          partially reduced top-right block.
*> \endverbatim
*>
*> \param[out] B21D
*> \verbatim
*>          B21D is REAL array, dimension (Q)
*>          When CBBCSD converges, B21D contains the negative sines of
*>          THETA(1), ..., THETA(Q). If CBBCSD fails to converge, then
*>          B21D contains the diagonal of the partially reduced bottom-left
*>          block.
*> \endverbatim
*>
*> \param[out] B21E
*> \verbatim
*>          B21E is REAL array, dimension (Q-1)
*>          When CBBCSD converges, B21E contains zeros. If CBBCSD fails
*>          to converge, then B21E contains the subdiagonal of the
*>          partially reduced bottom-left block.
*> \endverbatim
*>
*> \param[out] B22D
*> \verbatim
*>          B22D is REAL array, dimension (Q)
*>          When CBBCSD converges, B22D contains the negative sines of
*>          THETA(1), ..., THETA(Q). If CBBCSD fails to converge, then
*>          B22D contains the diagonal of the partially reduced bottom-right
*>          block.
*> \endverbatim
*>
*> \param[out] B22E
*> \verbatim
*>          B22E is REAL array, dimension (Q-1)
*>          When CBBCSD converges, B22E contains zeros. If CBBCSD fails
*>          to converge, then B22E contains the subdiagonal of the
*>          partially reduced bottom-right block.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is REAL array, dimension (MAX(1,LRWORK))
*>          On exit, if INFO = 0, RWORK(1) returns the optimal LRWORK.
*> \endverbatim
*>
*> \param[in] LRWORK
*> \verbatim
*>          LRWORK is INTEGER
*>          The dimension of the array RWORK. LRWORK >= MAX(1,8*Q).
*>
*>          If LRWORK = -1, then a workspace query is assumed; the
*>          routine only calculates the optimal size of the RWORK array,
*>          returns this value as the first entry of the work array, and
*>          no error message related to LRWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit.
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
*>          > 0:  if CBBCSD did not converge, INFO specifies the number
*>                of nonzero entries in PHI, and B11D, B11E, etc.,
*>                contain the partially reduced matrix.
*> \endverbatim
*
*> \par Internal Parameters:
*  =========================
*>
*> \verbatim
*>  TOLMUL  REAL, default = MAX(10,MIN(100,EPS**(-1/8)))
*>          TOLMUL controls the convergence criterion of the QR loop.
*>          Angles THETA(i), PHI(i) are rounded to 0 or PI/2 when they
*>          are within TOLMUL*EPS of either bound.
*> \endverbatim
*
*> \par References:
*  ================
*>
*>  [1] Brian D. Sutton. Computing the complete CS decomposition. Numer.
*>      Algorithms, 50(1):33-65, 2009.
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
*> \ingroup complexOTHERcomputational
*
*  =====================================================================
      SUBROUTINE CBBCSD( JOBU1, JOBU2, JOBV1T, JOBV2T, TRANS, M, P, Q,
     $                   THETA, PHI, U1, LDU1, U2, LDU2, V1T, LDV1T,
     $                   V2T, LDV2T, B11D, B11E, B12D, B12E, B21D, B21E,
     $                   B22D, B22E, RWORK, LRWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      CHARACTER          JOBU1, JOBU2, JOBV1T, JOBV2T, TRANS
      INTEGER            INFO, LDU1, LDU2, LDV1T, LDV2T, LRWORK, M, P, Q
*     ..
*     .. Array Arguments ..
      REAL               B11D( * ), B11E( * ), B12D( * ), B12E( * ),
     $                   B21D( * ), B21E( * ), B22D( * ), B22E( * ),
     $                   PHI( * ), THETA( * ), RWORK( * )
      COMPLEX            U1( LDU1, * ), U2( LDU2, * ), V1T( LDV1T, * ),
     $                   V2T( LDV2T, * )
*     ..
*
*  ===================================================================
*
*     .. Parameters ..
      INTEGER            MAXITR
      PARAMETER          ( MAXITR = 6 )
      REAL               HUNDRED, MEIGHTH, ONE, PIOVER2, TEN, ZERO
      PARAMETER          ( HUNDRED = 100.0E0, MEIGHTH = -0.125E0,
     $                     ONE = 1.0E0, PIOVER2 = 1.57079632679489662E0,
     $                     TEN = 10.0E0, ZERO = 0.0E0 )
      COMPLEX            NEGONECOMPLEX
      PARAMETER          ( NEGONECOMPLEX = (-1.0E0,0.0E0) )
*     ..
*     .. Local Scalars ..
      LOGICAL            COLMAJOR, LQUERY, RESTART11, RESTART12,
     $                   RESTART21, RESTART22, WANTU1, WANTU2, WANTV1T,
     $                   WANTV2T
      INTEGER            I, IMIN, IMAX, ITER, IU1CS, IU1SN, IU2CS,
     $                   IU2SN, IV1TCS, IV1TSN, IV2TCS, IV2TSN, J,
     $                   LRWORKMIN, LRWORKOPT, MAXIT, MINI
      REAL               B11BULGE, B12BULGE, B21BULGE, B22BULGE, DUMMY,
     $                   EPS, MU, NU, R, SIGMA11, SIGMA21,
     $                   TEMP, THETAMAX, THETAMIN, THRESH, TOL, TOLMUL,
     $                   UNFL, X1, X2, Y1, Y2
*
*     .. External Subroutines ..
      EXTERNAL           CLASR, CSCAL, CSWAP, SLARTGP, SLARTGS, SLAS2,
     $                   XERBLA
*     ..
*     .. External Functions ..
      REAL               SLAMCH
      LOGICAL            LSAME
      EXTERNAL           LSAME, SLAMCH
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, ATAN2, COS, MAX, MIN, SIN, SQRT
*     ..
*     .. Executable Statements ..
*
*     Test input arguments
*
      INFO = 0
      LQUERY = LRWORK .EQ. -1
      WANTU1 = LSAME( JOBU1, 'Y' )
      WANTU2 = LSAME( JOBU2, 'Y' )
      WANTV1T = LSAME( JOBV1T, 'Y' )
      WANTV2T = LSAME( JOBV2T, 'Y' )
      COLMAJOR = .NOT. LSAME( TRANS, 'T' )
*
      IF( M .LT. 0 ) THEN
         INFO = -6
      ELSE IF( P .LT. 0 .OR. P .GT. M ) THEN
         INFO = -7
      ELSE IF( Q .LT. 0 .OR. Q .GT. M ) THEN
         INFO = -8
      ELSE IF( Q .GT. P .OR. Q .GT. M-P .OR. Q .GT. M-Q ) THEN
         INFO = -8
      ELSE IF( WANTU1 .AND. LDU1 .LT. P ) THEN
         INFO = -12
      ELSE IF( WANTU2 .AND. LDU2 .LT. M-P ) THEN
         INFO = -14
      ELSE IF( WANTV1T .AND. LDV1T .LT. Q ) THEN
         INFO = -16
      ELSE IF( WANTV2T .AND. LDV2T .LT. M-Q ) THEN
         INFO = -18
      END IF
*
*     Quick return if Q = 0
*
      IF( INFO .EQ. 0 .AND. Q .EQ. 0 ) THEN
         LRWORKMIN = 1
         RWORK(1) = LRWORKMIN
         RETURN
      END IF
*
*     Compute workspace
*
      IF( INFO .EQ. 0 ) THEN
         IU1CS = 1
         IU1SN = IU1CS + Q
         IU2CS = IU1SN + Q
         IU2SN = IU2CS + Q
         IV1TCS = IU2SN + Q
         IV1TSN = IV1TCS + Q
         IV2TCS = IV1TSN + Q
         IV2TSN = IV2TCS + Q
         LRWORKOPT = IV2TSN + Q - 1
         LRWORKMIN = LRWORKOPT
         RWORK(1) = LRWORKOPT
         IF( LRWORK .LT. LRWORKMIN .AND. .NOT. LQUERY ) THEN
            INFO = -28
         END IF
      END IF
*
      IF( INFO .NE. 0 ) THEN
         CALL XERBLA( 'CBBCSD', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Get machine constants
*
      EPS = SLAMCH( 'Epsilon' )
      UNFL = SLAMCH( 'Safe minimum' )
      TOLMUL = MAX( TEN, MIN( HUNDRED, EPS**MEIGHTH ) )
      TOL = TOLMUL*EPS
      THRESH = MAX( TOL, MAXITR*Q*Q*UNFL )
*
*     Test for negligible sines or cosines
*
      DO I = 1, Q
         IF( THETA(I) .LT. THRESH ) THEN
            THETA(I) = ZERO
         ELSE IF( THETA(I) .GT. PIOVER2-THRESH ) THEN
            THETA(I) = PIOVER2
         END IF
      END DO
      DO I = 1, Q-1
         IF( PHI(I) .LT. THRESH ) THEN
            PHI(I) = ZERO
         ELSE IF( PHI(I) .GT. PIOVER2-THRESH ) THEN
            PHI(I) = PIOVER2
         END IF
      END DO
*
*     Initial deflation
*
      IMAX = Q
      DO WHILE( IMAX .GT. 1 )
         IF( PHI(IMAX-1) .NE. ZERO ) THEN
            EXIT
         END IF
         IMAX = IMAX - 1
      END DO
      IMIN = IMAX - 1
      IF  ( IMIN .GT. 1 ) THEN
         DO WHILE( PHI(IMIN-1) .NE. ZERO )
            IMIN = IMIN - 1
            IF  ( IMIN .LE. 1 ) EXIT
         END DO
      END IF
*
*     Initialize iteration counter
*
      MAXIT = MAXITR*Q*Q
      ITER = 0
*
*     Begin main iteration loop
*
      DO WHILE( IMAX .GT. 1 )
*
*        Compute the matrix entries
*
         B11D(IMIN) = COS( THETA(IMIN) )
         B21D(IMIN) = -SIN( THETA(IMIN) )
         DO I = IMIN, IMAX - 1
            B11E(I) = -SIN( THETA(I) ) * SIN( PHI(I) )
            B11D(I+1) = COS( THETA(I+1) ) * COS( PHI(I) )
            B12D(I) = SIN( THETA(I) ) * COS( PHI(I) )
            B12E(I) = COS( THETA(I+1) ) * SIN( PHI(I) )
            B21E(I) = -COS( THETA(I) ) * SIN( PHI(I) )
            B21D(I+1) = -SIN( THETA(I+1) ) * COS( PHI(I) )
            B22D(I) = COS( THETA(I) ) * COS( PHI(I) )
            B22E(I) = -SIN( THETA(I+1) ) * SIN( PHI(I) )
         END DO
         B12D(IMAX) = SIN( THETA(IMAX) )
         B22D(IMAX) = COS( THETA(IMAX) )
*
*        Abort if not converging; otherwise, increment ITER
*
         IF( ITER .GT. MAXIT ) THEN
            INFO = 0
            DO I = 1, Q
               IF( PHI(I) .NE. ZERO )
     $            INFO = INFO + 1
            END DO
            RETURN
         END IF
*
         ITER = ITER + IMAX - IMIN
*
*        Compute shifts
*
         THETAMAX = THETA(IMIN)
         THETAMIN = THETA(IMIN)
         DO I = IMIN+1, IMAX
            IF( THETA(I) > THETAMAX )
     $         THETAMAX = THETA(I)
            IF( THETA(I) < THETAMIN )
     $         THETAMIN = THETA(I)
         END DO
*
         IF( THETAMAX .GT. PIOVER2 - THRESH ) THEN
*
*           Zero on diagonals of B11 and B22; induce deflation with a
*           zero shift
*
            MU = ZERO
            NU = ONE
*
         ELSE IF( THETAMIN .LT. THRESH ) THEN
*
*           Zero on diagonals of B12 and B22; induce deflation with a
*           zero shift
*
            MU = ONE
            NU = ZERO
*
         ELSE
*
*           Compute shifts for B11 and B21 and use the lesser
*
            CALL SLAS2( B11D(IMAX-1), B11E(IMAX-1), B11D(IMAX), SIGMA11,
     $                  DUMMY )
            CALL SLAS2( B21D(IMAX-1), B21E(IMAX-1), B21D(IMAX), SIGMA21,
     $                  DUMMY )
*
            IF( SIGMA11 .LE. SIGMA21 ) THEN
               MU = SIGMA11
               NU = SQRT( ONE - MU**2 )
               IF( MU .LT. THRESH ) THEN
                  MU = ZERO
                  NU = ONE
               END IF
            ELSE
               NU = SIGMA21
               MU = SQRT( 1.0 - NU**2 )
               IF( NU .LT. THRESH ) THEN
                  MU = ONE
                  NU = ZERO
               END IF
            END IF
         END IF
*
*        Rotate to produce bulges in B11 and B21
*
         IF( MU .LE. NU ) THEN
            CALL SLARTGS( B11D(IMIN), B11E(IMIN), MU,
     $                    RWORK(IV1TCS+IMIN-1), RWORK(IV1TSN+IMIN-1) )
         ELSE
            CALL SLARTGS( B21D(IMIN), B21E(IMIN), NU,
     $                    RWORK(IV1TCS+IMIN-1), RWORK(IV1TSN+IMIN-1) )
         END IF
*
         TEMP = RWORK(IV1TCS+IMIN-1)*B11D(IMIN) +
     $          RWORK(IV1TSN+IMIN-1)*B11E(IMIN)
         B11E(IMIN) = RWORK(IV1TCS+IMIN-1)*B11E(IMIN) -
     $                RWORK(IV1TSN+IMIN-1)*B11D(IMIN)
         B11D(IMIN) = TEMP
         B11BULGE = RWORK(IV1TSN+IMIN-1)*B11D(IMIN+1)
         B11D(IMIN+1) = RWORK(IV1TCS+IMIN-1)*B11D(IMIN+1)
         TEMP = RWORK(IV1TCS+IMIN-1)*B21D(IMIN) +
     $          RWORK(IV1TSN+IMIN-1)*B21E(IMIN)
         B21E(IMIN) = RWORK(IV1TCS+IMIN-1)*B21E(IMIN) -
     $                RWORK(IV1TSN+IMIN-1)*B21D(IMIN)
         B21D(IMIN) = TEMP
         B21BULGE = RWORK(IV1TSN+IMIN-1)*B21D(IMIN+1)
         B21D(IMIN+1) = RWORK(IV1TCS+IMIN-1)*B21D(IMIN+1)
*
*        Compute THETA(IMIN)
*
         THETA( IMIN ) = ATAN2( SQRT( B21D(IMIN)**2+B21BULGE**2 ),
     $                   SQRT( B11D(IMIN)**2+B11BULGE**2 ) )
*
*        Chase the bulges in B11(IMIN+1,IMIN) and B21(IMIN+1,IMIN)
*
         IF( B11D(IMIN)**2+B11BULGE**2 .GT. THRESH**2 ) THEN
            CALL SLARTGP( B11BULGE, B11D(IMIN), RWORK(IU1SN+IMIN-1),
     $                    RWORK(IU1CS+IMIN-1), R )
         ELSE IF( MU .LE. NU ) THEN
            CALL SLARTGS( B11E( IMIN ), B11D( IMIN + 1 ), MU,
     $                    RWORK(IU1CS+IMIN-1), RWORK(IU1SN+IMIN-1) )
         ELSE
            CALL SLARTGS( B12D( IMIN ), B12E( IMIN ), NU,
     $                    RWORK(IU1CS+IMIN-1), RWORK(IU1SN+IMIN-1) )
         END IF
         IF( B21D(IMIN)**2+B21BULGE**2 .GT. THRESH**2 ) THEN
            CALL SLARTGP( B21BULGE, B21D(IMIN), RWORK(IU2SN+IMIN-1),
     $                    RWORK(IU2CS+IMIN-1), R )
         ELSE IF( NU .LT. MU ) THEN
            CALL SLARTGS( B21E( IMIN ), B21D( IMIN + 1 ), NU,
     $                    RWORK(IU2CS+IMIN-1), RWORK(IU2SN+IMIN-1) )
         ELSE
            CALL SLARTGS( B22D(IMIN), B22E(IMIN), MU,
     $                    RWORK(IU2CS+IMIN-1), RWORK(IU2SN+IMIN-1) )
         END IF
         RWORK(IU2CS+IMIN-1) = -RWORK(IU2CS+IMIN-1)
         RWORK(IU2SN+IMIN-1) = -RWORK(IU2SN+IMIN-1)
*
         TEMP = RWORK(IU1CS+IMIN-1)*B11E(IMIN) +
     $          RWORK(IU1SN+IMIN-1)*B11D(IMIN+1)
         B11D(IMIN+1) = RWORK(IU1CS+IMIN-1)*B11D(IMIN+1) -
     $                  RWORK(IU1SN+IMIN-1)*B11E(IMIN)
         B11E(IMIN) = TEMP
         IF( IMAX .GT. IMIN+1 ) THEN
            B11BULGE = RWORK(IU1SN+IMIN-1)*B11E(IMIN+1)
            B11E(IMIN+1) = RWORK(IU1CS+IMIN-1)*B11E(IMIN+1)
         END IF
         TEMP = RWORK(IU1CS+IMIN-1)*B12D(IMIN) +
     $          RWORK(IU1SN+IMIN-1)*B12E(IMIN)
         B12E(IMIN) = RWORK(IU1CS+IMIN-1)*B12E(IMIN) -
     $                RWORK(IU1SN+IMIN-1)*B12D(IMIN)
         B12D(IMIN) = TEMP
         B12BULGE = RWORK(IU1SN+IMIN-1)*B12D(IMIN+1)
         B12D(IMIN+1) = RWORK(IU1CS+IMIN-1)*B12D(IMIN+1)
         TEMP = RWORK(IU2CS+IMIN-1)*B21E(IMIN) +
     $          RWORK(IU2SN+IMIN-1)*B21D(IMIN+1)
         B21D(IMIN+1) = RWORK(IU2CS+IMIN-1)*B21D(IMIN+1) -
     $                  RWORK(IU2SN+IMIN-1)*B21E(IMIN)
         B21E(IMIN) = TEMP
         IF( IMAX .GT. IMIN+1 ) THEN
            B21BULGE = RWORK(IU2SN+IMIN-1)*B21E(IMIN+1)
            B21E(IMIN+1) = RWORK(IU2CS+IMIN-1)*B21E(IMIN+1)
         END IF
         TEMP = RWORK(IU2CS+IMIN-1)*B22D(IMIN) +
     $          RWORK(IU2SN+IMIN-1)*B22E(IMIN)
         B22E(IMIN) = RWORK(IU2CS+IMIN-1)*B22E(IMIN) -
     $                RWORK(IU2SN+IMIN-1)*B22D(IMIN)
         B22D(IMIN) = TEMP
         B22BULGE = RWORK(IU2SN+IMIN-1)*B22D(IMIN+1)
         B22D(IMIN+1) = RWORK(IU2CS+IMIN-1)*B22D(IMIN+1)
*
*        Inner loop: chase bulges from B11(IMIN,IMIN+2),
*        B12(IMIN,IMIN+1), B21(IMIN,IMIN+2), and B22(IMIN,IMIN+1) to
*        bottom-right
*
         DO I = IMIN+1, IMAX-1
*
*           Compute PHI(I-1)
*
            X1 = SIN(THETA(I-1))*B11E(I-1) + COS(THETA(I-1))*B21E(I-1)
            X2 = SIN(THETA(I-1))*B11BULGE + COS(THETA(I-1))*B21BULGE
            Y1 = SIN(THETA(I-1))*B12D(I-1) + COS(THETA(I-1))*B22D(I-1)
            Y2 = SIN(THETA(I-1))*B12BULGE + COS(THETA(I-1))*B22BULGE
*
            PHI(I-1) = ATAN2( SQRT(X1**2+X2**2), SQRT(Y1**2+Y2**2) )
*
*           Determine if there are bulges to chase or if a new direct
*           summand has been reached
*
            RESTART11 = B11E(I-1)**2 + B11BULGE**2 .LE. THRESH**2
            RESTART21 = B21E(I-1)**2 + B21BULGE**2 .LE. THRESH**2
            RESTART12 = B12D(I-1)**2 + B12BULGE**2 .LE. THRESH**2
            RESTART22 = B22D(I-1)**2 + B22BULGE**2 .LE. THRESH**2
*
*           If possible, chase bulges from B11(I-1,I+1), B12(I-1,I),
*           B21(I-1,I+1), and B22(I-1,I). If necessary, restart bulge-
*           chasing by applying the original shift again.
*
            IF( .NOT. RESTART11 .AND. .NOT. RESTART21 ) THEN
               CALL SLARTGP( X2, X1, RWORK(IV1TSN+I-1),
     $                       RWORK(IV1TCS+I-1), R )
            ELSE IF( .NOT. RESTART11 .AND. RESTART21 ) THEN
               CALL SLARTGP( B11BULGE, B11E(I-1), RWORK(IV1TSN+I-1),
     $                       RWORK(IV1TCS+I-1), R )
            ELSE IF( RESTART11 .AND. .NOT. RESTART21 ) THEN
               CALL SLARTGP( B21BULGE, B21E(I-1), RWORK(IV1TSN+I-1),
     $                       RWORK(IV1TCS+I-1), R )
            ELSE IF( MU .LE. NU ) THEN
               CALL SLARTGS( B11D(I), B11E(I), MU, RWORK(IV1TCS+I-1),
     $                       RWORK(IV1TSN+I-1) )
            ELSE
               CALL SLARTGS( B21D(I), B21E(I), NU, RWORK(IV1TCS+I-1),
     $                       RWORK(IV1TSN+I-1) )
            END IF
            RWORK(IV1TCS+I-1) = -RWORK(IV1TCS+I-1)
            RWORK(IV1TSN+I-1) = -RWORK(IV1TSN+I-1)
            IF( .NOT. RESTART12 .AND. .NOT. RESTART22 ) THEN
               CALL SLARTGP( Y2, Y1, RWORK(IV2TSN+I-1-1),
     $                       RWORK(IV2TCS+I-1-1), R )
            ELSE IF( .NOT. RESTART12 .AND. RESTART22 ) THEN
               CALL SLARTGP( B12BULGE, B12D(I-1), RWORK(IV2TSN+I-1-1),
     $                       RWORK(IV2TCS+I-1-1), R )
            ELSE IF( RESTART12 .AND. .NOT. RESTART22 ) THEN
               CALL SLARTGP( B22BULGE, B22D(I-1), RWORK(IV2TSN+I-1-1),
     $                       RWORK(IV2TCS+I-1-1), R )
            ELSE IF( NU .LT. MU ) THEN
               CALL SLARTGS( B12E(I-1), B12D(I), NU,
     $                       RWORK(IV2TCS+I-1-1), RWORK(IV2TSN+I-1-1) )
            ELSE
               CALL SLARTGS( B22E(I-1), B22D(I), MU,
     $                       RWORK(IV2TCS+I-1-1), RWORK(IV2TSN+I-1-1) )
            END IF
*
            TEMP = RWORK(IV1TCS+I-1)*B11D(I) + RWORK(IV1TSN+I-1)*B11E(I)
            B11E(I) = RWORK(IV1TCS+I-1)*B11E(I) -
     $                RWORK(IV1TSN+I-1)*B11D(I)
            B11D(I) = TEMP
            B11BULGE = RWORK(IV1TSN+I-1)*B11D(I+1)
            B11D(I+1) = RWORK(IV1TCS+I-1)*B11D(I+1)
            TEMP = RWORK(IV1TCS+I-1)*B21D(I) + RWORK(IV1TSN+I-1)*B21E(I)
            B21E(I) = RWORK(IV1TCS+I-1)*B21E(I) -
     $                RWORK(IV1TSN+I-1)*B21D(I)
            B21D(I) = TEMP
            B21BULGE = RWORK(IV1TSN+I-1)*B21D(I+1)
            B21D(I+1) = RWORK(IV1TCS+I-1)*B21D(I+1)
            TEMP = RWORK(IV2TCS+I-1-1)*B12E(I-1) +
     $             RWORK(IV2TSN+I-1-1)*B12D(I)
            B12D(I) = RWORK(IV2TCS+I-1-1)*B12D(I) -
     $                RWORK(IV2TSN+I-1-1)*B12E(I-1)
            B12E(I-1) = TEMP
            B12BULGE = RWORK(IV2TSN+I-1-1)*B12E(I)
            B12E(I) = RWORK(IV2TCS+I-1-1)*B12E(I)
            TEMP = RWORK(IV2TCS+I-1-1)*B22E(I-1) +
     $             RWORK(IV2TSN+I-1-1)*B22D(I)
            B22D(I) = RWORK(IV2TCS+I-1-1)*B22D(I) -
     $                RWORK(IV2TSN+I-1-1)*B22E(I-1)
            B22E(I-1) = TEMP
            B22BULGE = RWORK(IV2TSN+I-1-1)*B22E(I)
            B22E(I) = RWORK(IV2TCS+I-1-1)*B22E(I)
*
*           Compute THETA(I)
*
            X1 = COS(PHI(I-1))*B11D(I) + SIN(PHI(I-1))*B12E(I-1)
            X2 = COS(PHI(I-1))*B11BULGE + SIN(PHI(I-1))*B12BULGE
            Y1 = COS(PHI(I-1))*B21D(I) + SIN(PHI(I-1))*B22E(I-1)
            Y2 = COS(PHI(I-1))*B21BULGE + SIN(PHI(I-1))*B22BULGE
*
            THETA(I) = ATAN2( SQRT(Y1**2+Y2**2), SQRT(X1**2+X2**2) )
*
*           Determine if there are bulges to chase or if a new direct
*           summand has been reached
*
            RESTART11 =   B11D(I)**2 + B11BULGE**2 .LE. THRESH**2
            RESTART12 = B12E(I-1)**2 + B12BULGE**2 .LE. THRESH**2
            RESTART21 =   B21D(I)**2 + B21BULGE**2 .LE. THRESH**2
            RESTART22 = B22E(I-1)**2 + B22BULGE**2 .LE. THRESH**2
*
*           If possible, chase bulges from B11(I+1,I), B12(I+1,I-1),
*           B21(I+1,I), and B22(I+1,I-1). If necessary, restart bulge-
*           chasing by applying the original shift again.
*
            IF( .NOT. RESTART11 .AND. .NOT. RESTART12 ) THEN
               CALL SLARTGP( X2, X1, RWORK(IU1SN+I-1), RWORK(IU1CS+I-1),
     $                       R )
            ELSE IF( .NOT. RESTART11 .AND. RESTART12 ) THEN
               CALL SLARTGP( B11BULGE, B11D(I), RWORK(IU1SN+I-1),
     $                       RWORK(IU1CS+I-1), R )
            ELSE IF( RESTART11 .AND. .NOT. RESTART12 ) THEN
               CALL SLARTGP( B12BULGE, B12E(I-1), RWORK(IU1SN+I-1),
     $                       RWORK(IU1CS+I-1), R )
            ELSE IF( MU .LE. NU ) THEN
               CALL SLARTGS( B11E(I), B11D(I+1), MU, RWORK(IU1CS+I-1),
     $                       RWORK(IU1SN+I-1) )
            ELSE
               CALL SLARTGS( B12D(I), B12E(I), NU, RWORK(IU1CS+I-1),
     $                       RWORK(IU1SN+I-1) )
            END IF
            IF( .NOT. RESTART21 .AND. .NOT. RESTART22 ) THEN
               CALL SLARTGP( Y2, Y1, RWORK(IU2SN+I-1), RWORK(IU2CS+I-1),
     $                       R )
            ELSE IF( .NOT. RESTART21 .AND. RESTART22 ) THEN
               CALL SLARTGP( B21BULGE, B21D(I), RWORK(IU2SN+I-1),
     $                       RWORK(IU2CS+I-1), R )
            ELSE IF( RESTART21 .AND. .NOT. RESTART22 ) THEN
               CALL SLARTGP( B22BULGE, B22E(I-1), RWORK(IU2SN+I-1),
     $                       RWORK(IU2CS+I-1), R )
            ELSE IF( NU .LT. MU ) THEN
               CALL SLARTGS( B21E(I), B21E(I+1), NU, RWORK(IU2CS+I-1),
     $                       RWORK(IU2SN+I-1) )
            ELSE
               CALL SLARTGS( B22D(I), B22E(I), MU, RWORK(IU2CS+I-1),
     $                       RWORK(IU2SN+I-1) )
            END IF
            RWORK(IU2CS+I-1) = -RWORK(IU2CS+I-1)
            RWORK(IU2SN+I-1) = -RWORK(IU2SN+I-1)
*
            TEMP = RWORK(IU1CS+I-1)*B11E(I) + RWORK(IU1SN+I-1)*B11D(I+1)
            B11D(I+1) = RWORK(IU1CS+I-1)*B11D(I+1) -
     $                  RWORK(IU1SN+I-1)*B11E(I)
            B11E(I) = TEMP
            IF( I .LT. IMAX - 1 ) THEN
               B11BULGE = RWORK(IU1SN+I-1)*B11E(I+1)
               B11E(I+1) = RWORK(IU1CS+I-1)*B11E(I+1)
            END IF
            TEMP = RWORK(IU2CS+I-1)*B21E(I) + RWORK(IU2SN+I-1)*B21D(I+1)
            B21D(I+1) = RWORK(IU2CS+I-1)*B21D(I+1) -
     $                  RWORK(IU2SN+I-1)*B21E(I)
            B21E(I) = TEMP
            IF( I .LT. IMAX - 1 ) THEN
               B21BULGE = RWORK(IU2SN+I-1)*B21E(I+1)
               B21E(I+1) = RWORK(IU2CS+I-1)*B21E(I+1)
            END IF
            TEMP = RWORK(IU1CS+I-1)*B12D(I) + RWORK(IU1SN+I-1)*B12E(I)
            B12E(I) = RWORK(IU1CS+I-1)*B12E(I) -
     $                RWORK(IU1SN+I-1)*B12D(I)
            B12D(I) = TEMP
            B12BULGE = RWORK(IU1SN+I-1)*B12D(I+1)
            B12D(I+1) = RWORK(IU1CS+I-1)*B12D(I+1)
            TEMP = RWORK(IU2CS+I-1)*B22D(I) + RWORK(IU2SN+I-1)*B22E(I)
            B22E(I) = RWORK(IU2CS+I-1)*B22E(I) -
     $                RWORK(IU2SN+I-1)*B22D(I)
            B22D(I) = TEMP
            B22BULGE = RWORK(IU2SN+I-1)*B22D(I+1)
            B22D(I+1) = RWORK(IU2CS+I-1)*B22D(I+1)
*
         END DO
*
*        Compute PHI(IMAX-1)
*
         X1 = SIN(THETA(IMAX-1))*B11E(IMAX-1) +
     $        COS(THETA(IMAX-1))*B21E(IMAX-1)
         Y1 = SIN(THETA(IMAX-1))*B12D(IMAX-1) +
     $        COS(THETA(IMAX-1))*B22D(IMAX-1)
         Y2 = SIN(THETA(IMAX-1))*B12BULGE + COS(THETA(IMAX-1))*B22BULGE
*
         PHI(IMAX-1) = ATAN2( ABS(X1), SQRT(Y1**2+Y2**2) )
*
*        Chase bulges from B12(IMAX-1,IMAX) and B22(IMAX-1,IMAX)
*
         RESTART12 = B12D(IMAX-1)**2 + B12BULGE**2 .LE. THRESH**2
         RESTART22 = B22D(IMAX-1)**2 + B22BULGE**2 .LE. THRESH**2
*
         IF( .NOT. RESTART12 .AND. .NOT. RESTART22 ) THEN
            CALL SLARTGP( Y2, Y1, RWORK(IV2TSN+IMAX-1-1),
     $                    RWORK(IV2TCS+IMAX-1-1), R )
         ELSE IF( .NOT. RESTART12 .AND. RESTART22 ) THEN
            CALL SLARTGP( B12BULGE, B12D(IMAX-1),
     $                    RWORK(IV2TSN+IMAX-1-1),
     $                    RWORK(IV2TCS+IMAX-1-1), R )
         ELSE IF( RESTART12 .AND. .NOT. RESTART22 ) THEN
            CALL SLARTGP( B22BULGE, B22D(IMAX-1),
     $                    RWORK(IV2TSN+IMAX-1-1),
     $                    RWORK(IV2TCS+IMAX-1-1), R )
         ELSE IF( NU .LT. MU ) THEN
            CALL SLARTGS( B12E(IMAX-1), B12D(IMAX), NU,
     $                    RWORK(IV2TCS+IMAX-1-1),
     $                    RWORK(IV2TSN+IMAX-1-1) )
         ELSE
            CALL SLARTGS( B22E(IMAX-1), B22D(IMAX), MU,
     $                    RWORK(IV2TCS+IMAX-1-1),
     $                    RWORK(IV2TSN+IMAX-1-1) )
         END IF
*
         TEMP = RWORK(IV2TCS+IMAX-1-1)*B12E(IMAX-1) +
     $          RWORK(IV2TSN+IMAX-1-1)*B12D(IMAX)
         B12D(IMAX) = RWORK(IV2TCS+IMAX-1-1)*B12D(IMAX) -
     $                RWORK(IV2TSN+IMAX-1-1)*B12E(IMAX-1)
         B12E(IMAX-1) = TEMP
         TEMP = RWORK(IV2TCS+IMAX-1-1)*B22E(IMAX-1) +
     $          RWORK(IV2TSN+IMAX-1-1)*B22D(IMAX)
         B22D(IMAX) = RWORK(IV2TCS+IMAX-1-1)*B22D(IMAX) -
     $                RWORK(IV2TSN+IMAX-1-1)*B22E(IMAX-1)
         B22E(IMAX-1) = TEMP
*
*        Update singular vectors
*
         IF( WANTU1 ) THEN
            IF( COLMAJOR ) THEN
               CALL CLASR( 'R', 'V', 'F', P, IMAX-IMIN+1,
     $                     RWORK(IU1CS+IMIN-1), RWORK(IU1SN+IMIN-1),
     $                     U1(1,IMIN), LDU1 )
            ELSE
               CALL CLASR( 'L', 'V', 'F', IMAX-IMIN+1, P,
     $                     RWORK(IU1CS+IMIN-1), RWORK(IU1SN+IMIN-1),
     $                     U1(IMIN,1), LDU1 )
            END IF
         END IF
         IF( WANTU2 ) THEN
            IF( COLMAJOR ) THEN
               CALL CLASR( 'R', 'V', 'F', M-P, IMAX-IMIN+1,
     $                     RWORK(IU2CS+IMIN-1), RWORK(IU2SN+IMIN-1),
     $                     U2(1,IMIN), LDU2 )
            ELSE
               CALL CLASR( 'L', 'V', 'F', IMAX-IMIN+1, M-P,
     $                     RWORK(IU2CS+IMIN-1), RWORK(IU2SN+IMIN-1),
     $                     U2(IMIN,1), LDU2 )
            END IF
         END IF
         IF( WANTV1T ) THEN
            IF( COLMAJOR ) THEN
               CALL CLASR( 'L', 'V', 'F', IMAX-IMIN+1, Q,
     $                     RWORK(IV1TCS+IMIN-1), RWORK(IV1TSN+IMIN-1),
     $                     V1T(IMIN,1), LDV1T )
            ELSE
               CALL CLASR( 'R', 'V', 'F', Q, IMAX-IMIN+1,
     $                     RWORK(IV1TCS+IMIN-1), RWORK(IV1TSN+IMIN-1),
     $                     V1T(1,IMIN), LDV1T )
            END IF
         END IF
         IF( WANTV2T ) THEN
            IF( COLMAJOR ) THEN
               CALL CLASR( 'L', 'V', 'F', IMAX-IMIN+1, M-Q,
     $                     RWORK(IV2TCS+IMIN-1), RWORK(IV2TSN+IMIN-1),
     $                     V2T(IMIN,1), LDV2T )
            ELSE
               CALL CLASR( 'R', 'V', 'F', M-Q, IMAX-IMIN+1,
     $                     RWORK(IV2TCS+IMIN-1), RWORK(IV2TSN+IMIN-1),
     $                     V2T(1,IMIN), LDV2T )
            END IF
         END IF
*
*        Fix signs on B11(IMAX-1,IMAX) and B21(IMAX-1,IMAX)
*
         IF( B11E(IMAX-1)+B21E(IMAX-1) .GT. 0 ) THEN
            B11D(IMAX) = -B11D(IMAX)
            B21D(IMAX) = -B21D(IMAX)
            IF( WANTV1T ) THEN
               IF( COLMAJOR ) THEN
                  CALL CSCAL( Q, NEGONECOMPLEX, V1T(IMAX,1), LDV1T )
               ELSE
                  CALL CSCAL( Q, NEGONECOMPLEX, V1T(1,IMAX), 1 )
               END IF
            END IF
         END IF
*
*        Compute THETA(IMAX)
*
         X1 = COS(PHI(IMAX-1))*B11D(IMAX) +
     $        SIN(PHI(IMAX-1))*B12E(IMAX-1)
         Y1 = COS(PHI(IMAX-1))*B21D(IMAX) +
     $        SIN(PHI(IMAX-1))*B22E(IMAX-1)
*
         THETA(IMAX) = ATAN2( ABS(Y1), ABS(X1) )
*
*        Fix signs on B11(IMAX,IMAX), B12(IMAX,IMAX-1), B21(IMAX,IMAX),
*        and B22(IMAX,IMAX-1)
*
         IF( B11D(IMAX)+B12E(IMAX-1) .LT. 0 ) THEN
            B12D(IMAX) = -B12D(IMAX)
            IF( WANTU1 ) THEN
               IF( COLMAJOR ) THEN
                  CALL CSCAL( P, NEGONECOMPLEX, U1(1,IMAX), 1 )
               ELSE
                  CALL CSCAL( P, NEGONECOMPLEX, U1(IMAX,1), LDU1 )
               END IF
            END IF
         END IF
         IF( B21D(IMAX)+B22E(IMAX-1) .GT. 0 ) THEN
            B22D(IMAX) = -B22D(IMAX)
            IF( WANTU2 ) THEN
               IF( COLMAJOR ) THEN
                  CALL CSCAL( M-P, NEGONECOMPLEX, U2(1,IMAX), 1 )
               ELSE
                  CALL CSCAL( M-P, NEGONECOMPLEX, U2(IMAX,1), LDU2 )
               END IF
            END IF
         END IF
*
*        Fix signs on B12(IMAX,IMAX) and B22(IMAX,IMAX)
*
         IF( B12D(IMAX)+B22D(IMAX) .LT. 0 ) THEN
            IF( WANTV2T ) THEN
               IF( COLMAJOR ) THEN
                  CALL CSCAL( M-Q, NEGONECOMPLEX, V2T(IMAX,1), LDV2T )
               ELSE
                  CALL CSCAL( M-Q, NEGONECOMPLEX, V2T(1,IMAX), 1 )
               END IF
            END IF
         END IF
*
*        Test for negligible sines or cosines
*
         DO I = IMIN, IMAX
            IF( THETA(I) .LT. THRESH ) THEN
               THETA(I) = ZERO
            ELSE IF( THETA(I) .GT. PIOVER2-THRESH ) THEN
               THETA(I) = PIOVER2
            END IF
         END DO
         DO I = IMIN, IMAX-1
            IF( PHI(I) .LT. THRESH ) THEN
               PHI(I) = ZERO
            ELSE IF( PHI(I) .GT. PIOVER2-THRESH ) THEN
               PHI(I) = PIOVER2
            END IF
         END DO
*
*        Deflate
*
         IF (IMAX .GT. 1) THEN
            DO WHILE( PHI(IMAX-1) .EQ. ZERO )
               IMAX = IMAX - 1
               IF (IMAX .LE. 1) EXIT
            END DO
         END IF
         IF( IMIN .GT. IMAX - 1 )
     $      IMIN = IMAX - 1
         IF (IMIN .GT. 1) THEN
            DO WHILE (PHI(IMIN-1) .NE. ZERO)
                IMIN = IMIN - 1
                IF (IMIN .LE. 1) EXIT
            END DO
         END IF
*
*        Repeat main iteration loop
*
      END DO
*
*     Postprocessing: order THETA from least to greatest
*
      DO I = 1, Q
*
         MINI = I
         THETAMIN = THETA(I)
         DO J = I+1, Q
            IF( THETA(J) .LT. THETAMIN ) THEN
               MINI = J
               THETAMIN = THETA(J)
            END IF
         END DO
*
         IF( MINI .NE. I ) THEN
            THETA(MINI) = THETA(I)
            THETA(I) = THETAMIN
            IF( COLMAJOR ) THEN
               IF( WANTU1 )
     $            CALL CSWAP( P, U1(1,I), 1, U1(1,MINI), 1 )
               IF( WANTU2 )
     $            CALL CSWAP( M-P, U2(1,I), 1, U2(1,MINI), 1 )
               IF( WANTV1T )
     $            CALL CSWAP( Q, V1T(I,1), LDV1T, V1T(MINI,1), LDV1T )
               IF( WANTV2T )
     $            CALL CSWAP( M-Q, V2T(I,1), LDV2T, V2T(MINI,1),
     $               LDV2T )
            ELSE
               IF( WANTU1 )
     $            CALL CSWAP( P, U1(I,1), LDU1, U1(MINI,1), LDU1 )
               IF( WANTU2 )
     $            CALL CSWAP( M-P, U2(I,1), LDU2, U2(MINI,1), LDU2 )
               IF( WANTV1T )
     $            CALL CSWAP( Q, V1T(1,I), 1, V1T(1,MINI), 1 )
               IF( WANTV2T )
     $            CALL CSWAP( M-Q, V2T(1,I), 1, V2T(1,MINI), 1 )
            END IF
         END IF
*
      END DO
*
      RETURN
*
*     End of CBBCSD
*
      END

