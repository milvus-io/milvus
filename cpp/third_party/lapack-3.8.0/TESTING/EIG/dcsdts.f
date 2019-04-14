*> \brief \b DCSDTS
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DCSDTS( M, P, Q, X, XF, LDX, U1, LDU1, U2, LDU2, V1T,
*                          LDV1T, V2T, LDV2T, THETA, IWORK, WORK, LWORK,
*                          RWORK, RESULT )
*
*       .. Scalar Arguments ..
*       INTEGER            LDX, LDU1, LDU2, LDV1T, LDV2T, LWORK, M, P, Q
*       ..
*       .. Array Arguments ..
*       INTEGER            IWORK( * )
*       DOUBLE PRECISION   RESULT( 15 ), RWORK( * ), THETA( * )
*       DOUBLE PRECISION   U1( LDU1, * ), U2( LDU2, * ), V1T( LDV1T, * ),
*      $                   V2T( LDV2T, * ), WORK( LWORK ), X( LDX, * ),
*      $                   XF( LDX, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DCSDTS tests DORCSD, which, given an M-by-M partitioned orthogonal
*> matrix X,
*>              Q  M-Q
*>       X = [ X11 X12 ] P   ,
*>           [ X21 X22 ] M-P
*>
*> computes the CSD
*>
*>       [ U1    ]**T * [ X11 X12 ] * [ V1    ]
*>       [    U2 ]      [ X21 X22 ]   [    V2 ]
*>
*>                             [  I  0  0 |  0  0  0 ]
*>                             [  0  C  0 |  0 -S  0 ]
*>                             [  0  0  0 |  0  0 -I ]
*>                           = [---------------------] = [ D11 D12 ] ,
*>                             [  0  0  0 |  I  0  0 ]   [ D21 D22 ]
*>                             [  0  S  0 |  0  C  0 ]
*>                             [  0  0  I |  0  0  0 ]
*>
*> and also DORCSD2BY1, which, given
*>          Q
*>       [ X11 ] P   ,
*>       [ X21 ] M-P
*>
*> computes the 2-by-1 CSD
*>
*>                                     [  I  0  0 ]
*>                                     [  0  C  0 ]
*>                                     [  0  0  0 ]
*>       [ U1    ]**T * [ X11 ] * V1 = [----------] = [ D11 ] ,
*>       [    U2 ]      [ X21 ]        [  0  0  0 ]   [ D21 ]
*>                                     [  0  S  0 ]
*>                                     [  0  0  I ]
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix X.  M >= 0.
*> \endverbatim
*>
*> \param[in] P
*> \verbatim
*>          P is INTEGER
*>          The number of rows of the matrix X11.  P >= 0.
*> \endverbatim
*>
*> \param[in] Q
*> \verbatim
*>          Q is INTEGER
*>          The number of columns of the matrix X11.  Q >= 0.
*> \endverbatim
*>
*> \param[in] X
*> \verbatim
*>          X is DOUBLE PRECISION array, dimension (LDX,M)
*>          The M-by-M matrix X.
*> \endverbatim
*>
*> \param[out] XF
*> \verbatim
*>          XF is DOUBLE PRECISION array, dimension (LDX,M)
*>          Details of the CSD of X, as returned by DORCSD;
*>          see DORCSD for further details.
*> \endverbatim
*>
*> \param[in] LDX
*> \verbatim
*>          LDX is INTEGER
*>          The leading dimension of the arrays X and XF.
*>          LDX >= max( 1,M ).
*> \endverbatim
*>
*> \param[out] U1
*> \verbatim
*>          U1 is DOUBLE PRECISION array, dimension(LDU1,P)
*>          The P-by-P orthogonal matrix U1.
*> \endverbatim
*>
*> \param[in] LDU1
*> \verbatim
*>          LDU1 is INTEGER
*>          The leading dimension of the array U1. LDU >= max(1,P).
*> \endverbatim
*>
*> \param[out] U2
*> \verbatim
*>          U2 is DOUBLE PRECISION array, dimension(LDU2,M-P)
*>          The (M-P)-by-(M-P) orthogonal matrix U2.
*> \endverbatim
*>
*> \param[in] LDU2
*> \verbatim
*>          LDU2 is INTEGER
*>          The leading dimension of the array U2. LDU >= max(1,M-P).
*> \endverbatim
*>
*> \param[out] V1T
*> \verbatim
*>          V1T is DOUBLE PRECISION array, dimension(LDV1T,Q)
*>          The Q-by-Q orthogonal matrix V1T.
*> \endverbatim
*>
*> \param[in] LDV1T
*> \verbatim
*>          LDV1T is INTEGER
*>          The leading dimension of the array V1T. LDV1T >=
*>          max(1,Q).
*> \endverbatim
*>
*> \param[out] V2T
*> \verbatim
*>          V2T is DOUBLE PRECISION array, dimension(LDV2T,M-Q)
*>          The (M-Q)-by-(M-Q) orthogonal matrix V2T.
*> \endverbatim
*>
*> \param[in] LDV2T
*> \verbatim
*>          LDV2T is INTEGER
*>          The leading dimension of the array V2T. LDV2T >=
*>          max(1,M-Q).
*> \endverbatim
*>
*> \param[out] THETA
*> \verbatim
*>          THETA is DOUBLE PRECISION array, dimension MIN(P,M-P,Q,M-Q)
*>          The CS values of X; the essentially diagonal matrices C and
*>          S are constructed from THETA; see subroutine DORCSD for
*>          details.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (M)
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (LWORK)
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array
*> \endverbatim
*>
*> \param[out] RESULT
*> \verbatim
*>          RESULT is DOUBLE PRECISION array, dimension (15)
*>          The test ratios:
*>          First, the 2-by-2 CSD:
*>          RESULT(1) = norm( U1'*X11*V1 - D11 ) / ( MAX(1,P,Q)*EPS2 )
*>          RESULT(2) = norm( U1'*X12*V2 - D12 ) / ( MAX(1,P,M-Q)*EPS2 )
*>          RESULT(3) = norm( U2'*X21*V1 - D21 ) / ( MAX(1,M-P,Q)*EPS2 )
*>          RESULT(4) = norm( U2'*X22*V2 - D22 ) / ( MAX(1,M-P,M-Q)*EPS2 )
*>          RESULT(5) = norm( I - U1'*U1 ) / ( MAX(1,P)*ULP )
*>          RESULT(6) = norm( I - U2'*U2 ) / ( MAX(1,M-P)*ULP )
*>          RESULT(7) = norm( I - V1T'*V1T ) / ( MAX(1,Q)*ULP )
*>          RESULT(8) = norm( I - V2T'*V2T ) / ( MAX(1,M-Q)*ULP )
*>          RESULT(9) = 0        if THETA is in increasing order and
*>                               all angles are in [0,pi/2];
*>                    = ULPINV   otherwise.
*>          Then, the 2-by-1 CSD:
*>          RESULT(10) = norm( U1'*X11*V1 - D11 ) / ( MAX(1,P,Q)*EPS2 )
*>          RESULT(11) = norm( U2'*X21*V1 - D21 ) / ( MAX(1,M-P,Q)*EPS2 )
*>          RESULT(12) = norm( I - U1'*U1 ) / ( MAX(1,P)*ULP )
*>          RESULT(13) = norm( I - U2'*U2 ) / ( MAX(1,M-P)*ULP )
*>          RESULT(14) = norm( I - V1T'*V1T ) / ( MAX(1,Q)*ULP )
*>          RESULT(15) = 0        if THETA is in increasing order and
*>                                all angles are in [0,pi/2];
*>                     = ULPINV   otherwise.
*>          ( EPS2 = MAX( norm( I - X'*X ) / M, ULP ). )
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
*> \ingroup double_eig
*
*  =====================================================================
      SUBROUTINE DCSDTS( M, P, Q, X, XF, LDX, U1, LDU1, U2, LDU2, V1T,
     $                   LDV1T, V2T, LDV2T, THETA, IWORK, WORK, LWORK,
     $                   RWORK, RESULT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            LDX, LDU1, LDU2, LDV1T, LDV2T, LWORK, M, P, Q
*     ..
*     .. Array Arguments ..
      INTEGER            IWORK( * )
      DOUBLE PRECISION   RESULT( 15 ), RWORK( * ), THETA( * )
      DOUBLE PRECISION   U1( LDU1, * ), U2( LDU2, * ), V1T( LDV1T, * ),
     $                   V2T( LDV2T, * ), WORK( LWORK ), X( LDX, * ),
     $                   XF( LDX, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   PIOVER2, REALONE, REALZERO
      PARAMETER          ( PIOVER2 = 1.57079632679489662D0,
     $                     REALONE = 1.0D0, REALZERO = 0.0D0 )
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D0, ONE = 1.0D0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, INFO, R
      DOUBLE PRECISION   EPS2, RESID, ULP, ULPINV
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH, DLANGE, DLANSY
      EXTERNAL           DLAMCH, DLANGE, DLANSY
*     ..
*     .. External Subroutines ..
      EXTERNAL           DGEMM, DLACPY, DLASET, DORCSD, DORCSD2BY1,
     $                   DSYRK
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          COS, DBLE, MAX, MIN, SIN
*     ..
*     .. Executable Statements ..
*
      ULP = DLAMCH( 'Precision' )
      ULPINV = REALONE / ULP
*
*     The first half of the routine checks the 2-by-2 CSD
*
      CALL DLASET( 'Full', M, M, ZERO, ONE, WORK, LDX )
      CALL DSYRK( 'Upper', 'Conjugate transpose', M, M, -ONE, X, LDX,
     $            ONE, WORK, LDX )
      IF (M.GT.0) THEN
         EPS2 = MAX( ULP,
     $               DLANGE( '1', M, M, WORK, LDX, RWORK ) / DBLE( M ) )
      ELSE
         EPS2 = ULP
      END IF
      R = MIN( P, M-P, Q, M-Q )
*
*     Copy the matrix X to the array XF.
*
      CALL DLACPY( 'Full', M, M, X, LDX, XF, LDX )
*
*     Compute the CSD
*
      CALL DORCSD( 'Y', 'Y', 'Y', 'Y', 'N', 'D', M, P, Q, XF(1,1), LDX,
     $             XF(1,Q+1), LDX, XF(P+1,1), LDX, XF(P+1,Q+1), LDX,
     $             THETA, U1, LDU1, U2, LDU2, V1T, LDV1T, V2T, LDV2T,
     $             WORK, LWORK, IWORK, INFO )
*
*     Compute XF := diag(U1,U2)'*X*diag(V1,V2) - [D11 D12; D21 D22]
*
      CALL DLACPY( 'Full', M, M, X, LDX, XF, LDX )
*
      CALL DGEMM( 'No transpose', 'Conjugate transpose', P, Q, Q, ONE,
     $            XF, LDX, V1T, LDV1T, ZERO, WORK, LDX )
*
      CALL DGEMM( 'Conjugate transpose', 'No transpose', P, Q, P, ONE,
     $            U1, LDU1, WORK, LDX, ZERO, XF, LDX )
*
      DO I = 1, MIN(P,Q)-R
         XF(I,I) = XF(I,I) - ONE
      END DO
      DO I = 1, R
         XF(MIN(P,Q)-R+I,MIN(P,Q)-R+I) =
     $           XF(MIN(P,Q)-R+I,MIN(P,Q)-R+I) - COS(THETA(I))
      END DO
*
      CALL DGEMM( 'No transpose', 'Conjugate transpose', P, M-Q, M-Q,
     $            ONE, XF(1,Q+1), LDX, V2T, LDV2T, ZERO, WORK, LDX )
*
      CALL DGEMM( 'Conjugate transpose', 'No transpose', P, M-Q, P,
     $            ONE, U1, LDU1, WORK, LDX, ZERO, XF(1,Q+1), LDX )
*
      DO I = 1, MIN(P,M-Q)-R
         XF(P-I+1,M-I+1) = XF(P-I+1,M-I+1) + ONE
      END DO
      DO I = 1, R
         XF(P-(MIN(P,M-Q)-R)+1-I,M-(MIN(P,M-Q)-R)+1-I) =
     $      XF(P-(MIN(P,M-Q)-R)+1-I,M-(MIN(P,M-Q)-R)+1-I) +
     $      SIN(THETA(R-I+1))
      END DO
*
      CALL DGEMM( 'No transpose', 'Conjugate transpose', M-P, Q, Q, ONE,
     $            XF(P+1,1), LDX, V1T, LDV1T, ZERO, WORK, LDX )
*
      CALL DGEMM( 'Conjugate transpose', 'No transpose', M-P, Q, M-P,
     $            ONE, U2, LDU2, WORK, LDX, ZERO, XF(P+1,1), LDX )
*
      DO I = 1, MIN(M-P,Q)-R
         XF(M-I+1,Q-I+1) = XF(M-I+1,Q-I+1) - ONE
      END DO
      DO I = 1, R
         XF(M-(MIN(M-P,Q)-R)+1-I,Q-(MIN(M-P,Q)-R)+1-I) =
     $             XF(M-(MIN(M-P,Q)-R)+1-I,Q-(MIN(M-P,Q)-R)+1-I) -
     $             SIN(THETA(R-I+1))
      END DO
*
      CALL DGEMM( 'No transpose', 'Conjugate transpose', M-P, M-Q, M-Q,
     $            ONE, XF(P+1,Q+1), LDX, V2T, LDV2T, ZERO, WORK, LDX )
*
      CALL DGEMM( 'Conjugate transpose', 'No transpose', M-P, M-Q, M-P,
     $            ONE, U2, LDU2, WORK, LDX, ZERO, XF(P+1,Q+1), LDX )
*
      DO I = 1, MIN(M-P,M-Q)-R
         XF(P+I,Q+I) = XF(P+I,Q+I) - ONE
      END DO
      DO I = 1, R
         XF(P+(MIN(M-P,M-Q)-R)+I,Q+(MIN(M-P,M-Q)-R)+I) =
     $      XF(P+(MIN(M-P,M-Q)-R)+I,Q+(MIN(M-P,M-Q)-R)+I) -
     $      COS(THETA(I))
      END DO
*
*     Compute norm( U1'*X11*V1 - D11 ) / ( MAX(1,P,Q)*EPS2 ) .
*
      RESID = DLANGE( '1', P, Q, XF, LDX, RWORK )
      RESULT( 1 ) = ( RESID / DBLE(MAX(1,P,Q)) ) / EPS2
*
*     Compute norm( U1'*X12*V2 - D12 ) / ( MAX(1,P,M-Q)*EPS2 ) .
*
      RESID = DLANGE( '1', P, M-Q, XF(1,Q+1), LDX, RWORK )
      RESULT( 2 ) = ( RESID / DBLE(MAX(1,P,M-Q)) ) / EPS2
*
*     Compute norm( U2'*X21*V1 - D21 ) / ( MAX(1,M-P,Q)*EPS2 ) .
*
      RESID = DLANGE( '1', M-P, Q, XF(P+1,1), LDX, RWORK )
      RESULT( 3 ) = ( RESID / DBLE(MAX(1,M-P,Q)) ) / EPS2
*
*     Compute norm( U2'*X22*V2 - D22 ) / ( MAX(1,M-P,M-Q)*EPS2 ) .
*
      RESID = DLANGE( '1', M-P, M-Q, XF(P+1,Q+1), LDX, RWORK )
      RESULT( 4 ) = ( RESID / DBLE(MAX(1,M-P,M-Q)) ) / EPS2
*
*     Compute I - U1'*U1
*
      CALL DLASET( 'Full', P, P, ZERO, ONE, WORK, LDU1 )
      CALL DSYRK( 'Upper', 'Conjugate transpose', P, P, -ONE, U1, LDU1,
     $            ONE, WORK, LDU1 )
*
*     Compute norm( I - U'*U ) / ( MAX(1,P) * ULP ) .
*
      RESID = DLANSY( '1', 'Upper', P, WORK, LDU1, RWORK )
      RESULT( 5 ) = ( RESID / DBLE(MAX(1,P)) ) / ULP
*
*     Compute I - U2'*U2
*
      CALL DLASET( 'Full', M-P, M-P, ZERO, ONE, WORK, LDU2 )
      CALL DSYRK( 'Upper', 'Conjugate transpose', M-P, M-P, -ONE, U2,
     $            LDU2, ONE, WORK, LDU2 )
*
*     Compute norm( I - U2'*U2 ) / ( MAX(1,M-P) * ULP ) .
*
      RESID = DLANSY( '1', 'Upper', M-P, WORK, LDU2, RWORK )
      RESULT( 6 ) = ( RESID / DBLE(MAX(1,M-P)) ) / ULP
*
*     Compute I - V1T*V1T'
*
      CALL DLASET( 'Full', Q, Q, ZERO, ONE, WORK, LDV1T )
      CALL DSYRK( 'Upper', 'No transpose', Q, Q, -ONE, V1T, LDV1T, ONE,
     $            WORK, LDV1T )
*
*     Compute norm( I - V1T*V1T' ) / ( MAX(1,Q) * ULP ) .
*
      RESID = DLANSY( '1', 'Upper', Q, WORK, LDV1T, RWORK )
      RESULT( 7 ) = ( RESID / DBLE(MAX(1,Q)) ) / ULP
*
*     Compute I - V2T*V2T'
*
      CALL DLASET( 'Full', M-Q, M-Q, ZERO, ONE, WORK, LDV2T )
      CALL DSYRK( 'Upper', 'No transpose', M-Q, M-Q, -ONE, V2T, LDV2T,
     $            ONE, WORK, LDV2T )
*
*     Compute norm( I - V2T*V2T' ) / ( MAX(1,M-Q) * ULP ) .
*
      RESID = DLANSY( '1', 'Upper', M-Q, WORK, LDV2T, RWORK )
      RESULT( 8 ) = ( RESID / DBLE(MAX(1,M-Q)) ) / ULP
*
*     Check sorting
*
      RESULT( 9 ) = REALZERO
      DO I = 1, R
         IF( THETA(I).LT.REALZERO .OR. THETA(I).GT.PIOVER2 ) THEN
            RESULT( 9 ) = ULPINV
         END IF
         IF( I.GT.1 ) THEN
            IF ( THETA(I).LT.THETA(I-1) ) THEN
               RESULT( 9 ) = ULPINV
            END IF
         END IF
      END DO
*
*     The second half of the routine checks the 2-by-1 CSD
*
      CALL DLASET( 'Full', Q, Q, ZERO, ONE, WORK, LDX )
      CALL DSYRK( 'Upper', 'Conjugate transpose', Q, M, -ONE, X, LDX,
     $            ONE, WORK, LDX )
      IF( M.GT.0 ) THEN
         EPS2 = MAX( ULP,
     $               DLANGE( '1', Q, Q, WORK, LDX, RWORK ) / DBLE( M ) )
      ELSE
         EPS2 = ULP
      END IF
      R = MIN( P, M-P, Q, M-Q )
*
*     Copy the matrix [ X11; X21 ] to the array XF.
*
      CALL DLACPY( 'Full', M, Q, X, LDX, XF, LDX )
*
*     Compute the CSD
*
      CALL DORCSD2BY1( 'Y', 'Y', 'Y', M, P, Q, XF(1,1), LDX, XF(P+1,1),
     $                 LDX, THETA, U1, LDU1, U2, LDU2, V1T, LDV1T, WORK,
     $                 LWORK, IWORK, INFO )
*
*     Compute [X11;X21] := diag(U1,U2)'*[X11;X21]*V1 - [D11;D21]
*
      CALL DGEMM( 'No transpose', 'Conjugate transpose', P, Q, Q, ONE,
     $            X, LDX, V1T, LDV1T, ZERO, WORK, LDX )
*
      CALL DGEMM( 'Conjugate transpose', 'No transpose', P, Q, P, ONE,
     $            U1, LDU1, WORK, LDX, ZERO, X, LDX )
*
      DO I = 1, MIN(P,Q)-R
         X(I,I) = X(I,I) - ONE
      END DO
      DO I = 1, R
         X(MIN(P,Q)-R+I,MIN(P,Q)-R+I) =
     $           X(MIN(P,Q)-R+I,MIN(P,Q)-R+I) - COS(THETA(I))
      END DO
*
      CALL DGEMM( 'No transpose', 'Conjugate transpose', M-P, Q, Q, ONE,
     $            X(P+1,1), LDX, V1T, LDV1T, ZERO, WORK, LDX )
*
      CALL DGEMM( 'Conjugate transpose', 'No transpose', M-P, Q, M-P,
     $            ONE, U2, LDU2, WORK, LDX, ZERO, X(P+1,1), LDX )
*
      DO I = 1, MIN(M-P,Q)-R
         X(M-I+1,Q-I+1) = X(M-I+1,Q-I+1) - ONE
      END DO
      DO I = 1, R
         X(M-(MIN(M-P,Q)-R)+1-I,Q-(MIN(M-P,Q)-R)+1-I) =
     $             X(M-(MIN(M-P,Q)-R)+1-I,Q-(MIN(M-P,Q)-R)+1-I) -
     $             SIN(THETA(R-I+1))
      END DO
*
*     Compute norm( U1'*X11*V1 - D11 ) / ( MAX(1,P,Q)*EPS2 ) .
*
      RESID = DLANGE( '1', P, Q, X, LDX, RWORK )
      RESULT( 10 ) = ( RESID / DBLE(MAX(1,P,Q)) ) / EPS2
*
*     Compute norm( U2'*X21*V1 - D21 ) / ( MAX(1,M-P,Q)*EPS2 ) .
*
      RESID = DLANGE( '1', M-P, Q, X(P+1,1), LDX, RWORK )
      RESULT( 11 ) = ( RESID / DBLE(MAX(1,M-P,Q)) ) / EPS2
*
*     Compute I - U1'*U1
*
      CALL DLASET( 'Full', P, P, ZERO, ONE, WORK, LDU1 )
      CALL DSYRK( 'Upper', 'Conjugate transpose', P, P, -ONE, U1, LDU1,
     $            ONE, WORK, LDU1 )
*
*     Compute norm( I - U1'*U1 ) / ( MAX(1,P) * ULP ) .
*
      RESID = DLANSY( '1', 'Upper', P, WORK, LDU1, RWORK )
      RESULT( 12 ) = ( RESID / DBLE(MAX(1,P)) ) / ULP
*
*     Compute I - U2'*U2
*
      CALL DLASET( 'Full', M-P, M-P, ZERO, ONE, WORK, LDU2 )
      CALL DSYRK( 'Upper', 'Conjugate transpose', M-P, M-P, -ONE, U2,
     $            LDU2, ONE, WORK, LDU2 )
*
*     Compute norm( I - U2'*U2 ) / ( MAX(1,M-P) * ULP ) .
*
      RESID = DLANSY( '1', 'Upper', M-P, WORK, LDU2, RWORK )
      RESULT( 13 ) = ( RESID / DBLE(MAX(1,M-P)) ) / ULP
*
*     Compute I - V1T*V1T'
*
      CALL DLASET( 'Full', Q, Q, ZERO, ONE, WORK, LDV1T )
      CALL DSYRK( 'Upper', 'No transpose', Q, Q, -ONE, V1T, LDV1T, ONE,
     $            WORK, LDV1T )
*
*     Compute norm( I - V1T*V1T' ) / ( MAX(1,Q) * ULP ) .
*
      RESID = DLANSY( '1', 'Upper', Q, WORK, LDV1T, RWORK )
      RESULT( 14 ) = ( RESID / DBLE(MAX(1,Q)) ) / ULP
*
*     Check sorting
*
      RESULT( 15 ) = REALZERO
      DO I = 1, R
         IF( THETA(I).LT.REALZERO .OR. THETA(I).GT.PIOVER2 ) THEN
            RESULT( 15 ) = ULPINV
         END IF
         IF( I.GT.1 ) THEN
            IF ( THETA(I).LT.THETA(I-1) ) THEN
               RESULT( 15 ) = ULPINV
            END IF
         END IF
      END DO
*
      RETURN
*
*     End of DCSDTS
*
      END

