*> \brief \b ZLAQR5 performs a single small-bulge multi-shift QR sweep.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZLAQR5 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zlaqr5.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zlaqr5.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zlaqr5.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZLAQR5( WANTT, WANTZ, KACC22, N, KTOP, KBOT, NSHFTS, S,
*                          H, LDH, ILOZ, IHIZ, Z, LDZ, V, LDV, U, LDU, NV,
*                          WV, LDWV, NH, WH, LDWH )
*
*       .. Scalar Arguments ..
*       INTEGER            IHIZ, ILOZ, KACC22, KBOT, KTOP, LDH, LDU, LDV,
*      $                   LDWH, LDWV, LDZ, N, NH, NSHFTS, NV
*       LOGICAL            WANTT, WANTZ
*       ..
*       .. Array Arguments ..
*       COMPLEX*16         H( LDH, * ), S( * ), U( LDU, * ), V( LDV, * ),
*      $                   WH( LDWH, * ), WV( LDWV, * ), Z( LDZ, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    ZLAQR5, called by ZLAQR0, performs a
*>    single small-bulge multi-shift QR sweep.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] WANTT
*> \verbatim
*>          WANTT is LOGICAL
*>             WANTT = .true. if the triangular Schur factor
*>             is being computed.  WANTT is set to .false. otherwise.
*> \endverbatim
*>
*> \param[in] WANTZ
*> \verbatim
*>          WANTZ is LOGICAL
*>             WANTZ = .true. if the unitary Schur factor is being
*>             computed.  WANTZ is set to .false. otherwise.
*> \endverbatim
*>
*> \param[in] KACC22
*> \verbatim
*>          KACC22 is INTEGER with value 0, 1, or 2.
*>             Specifies the computation mode of far-from-diagonal
*>             orthogonal updates.
*>        = 0: ZLAQR5 does not accumulate reflections and does not
*>             use matrix-matrix multiply to update far-from-diagonal
*>             matrix entries.
*>        = 1: ZLAQR5 accumulates reflections and uses matrix-matrix
*>             multiply to update the far-from-diagonal matrix entries.
*>        = 2: ZLAQR5 accumulates reflections, uses matrix-matrix
*>             multiply to update the far-from-diagonal matrix entries,
*>             and takes advantage of 2-by-2 block structure during
*>             matrix multiplies.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>             N is the order of the Hessenberg matrix H upon which this
*>             subroutine operates.
*> \endverbatim
*>
*> \param[in] KTOP
*> \verbatim
*>          KTOP is INTEGER
*> \endverbatim
*>
*> \param[in] KBOT
*> \verbatim
*>          KBOT is INTEGER
*>             These are the first and last rows and columns of an
*>             isolated diagonal block upon which the QR sweep is to be
*>             applied. It is assumed without a check that
*>                       either KTOP = 1  or   H(KTOP,KTOP-1) = 0
*>             and
*>                       either KBOT = N  or   H(KBOT+1,KBOT) = 0.
*> \endverbatim
*>
*> \param[in] NSHFTS
*> \verbatim
*>          NSHFTS is INTEGER
*>             NSHFTS gives the number of simultaneous shifts.  NSHFTS
*>             must be positive and even.
*> \endverbatim
*>
*> \param[in,out] S
*> \verbatim
*>          S is COMPLEX*16 array, dimension (NSHFTS)
*>             S contains the shifts of origin that define the multi-
*>             shift QR sweep.  On output S may be reordered.
*> \endverbatim
*>
*> \param[in,out] H
*> \verbatim
*>          H is COMPLEX*16 array, dimension (LDH,N)
*>             On input H contains a Hessenberg matrix.  On output a
*>             multi-shift QR sweep with shifts SR(J)+i*SI(J) is applied
*>             to the isolated diagonal block in rows and columns KTOP
*>             through KBOT.
*> \endverbatim
*>
*> \param[in] LDH
*> \verbatim
*>          LDH is INTEGER
*>             LDH is the leading dimension of H just as declared in the
*>             calling procedure.  LDH.GE.MAX(1,N).
*> \endverbatim
*>
*> \param[in] ILOZ
*> \verbatim
*>          ILOZ is INTEGER
*> \endverbatim
*>
*> \param[in] IHIZ
*> \verbatim
*>          IHIZ is INTEGER
*>             Specify the rows of Z to which transformations must be
*>             applied if WANTZ is .TRUE.. 1 .LE. ILOZ .LE. IHIZ .LE. N
*> \endverbatim
*>
*> \param[in,out] Z
*> \verbatim
*>          Z is COMPLEX*16 array, dimension (LDZ,IHIZ)
*>             If WANTZ = .TRUE., then the QR Sweep unitary
*>             similarity transformation is accumulated into
*>             Z(ILOZ:IHIZ,ILOZ:IHIZ) from the right.
*>             If WANTZ = .FALSE., then Z is unreferenced.
*> \endverbatim
*>
*> \param[in] LDZ
*> \verbatim
*>          LDZ is INTEGER
*>             LDA is the leading dimension of Z just as declared in
*>             the calling procedure. LDZ.GE.N.
*> \endverbatim
*>
*> \param[out] V
*> \verbatim
*>          V is COMPLEX*16 array, dimension (LDV,NSHFTS/2)
*> \endverbatim
*>
*> \param[in] LDV
*> \verbatim
*>          LDV is INTEGER
*>             LDV is the leading dimension of V as declared in the
*>             calling procedure.  LDV.GE.3.
*> \endverbatim
*>
*> \param[out] U
*> \verbatim
*>          U is COMPLEX*16 array, dimension (LDU,3*NSHFTS-3)
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER
*>             LDU is the leading dimension of U just as declared in the
*>             in the calling subroutine.  LDU.GE.3*NSHFTS-3.
*> \endverbatim
*>
*> \param[in] NH
*> \verbatim
*>          NH is INTEGER
*>             NH is the number of columns in array WH available for
*>             workspace. NH.GE.1.
*> \endverbatim
*>
*> \param[out] WH
*> \verbatim
*>          WH is COMPLEX*16 array, dimension (LDWH,NH)
*> \endverbatim
*>
*> \param[in] LDWH
*> \verbatim
*>          LDWH is INTEGER
*>             Leading dimension of WH just as declared in the
*>             calling procedure.  LDWH.GE.3*NSHFTS-3.
*> \endverbatim
*>
*> \param[in] NV
*> \verbatim
*>          NV is INTEGER
*>             NV is the number of rows in WV agailable for workspace.
*>             NV.GE.1.
*> \endverbatim
*>
*> \param[out] WV
*> \verbatim
*>          WV is COMPLEX*16 array, dimension (LDWV,3*NSHFTS-3)
*> \endverbatim
*>
*> \param[in] LDWV
*> \verbatim
*>          LDWV is INTEGER
*>             LDWV is the leading dimension of WV as declared in the
*>             in the calling subroutine.  LDWV.GE.NV.
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
*> \ingroup complex16OTHERauxiliary
*
*> \par Contributors:
*  ==================
*>
*>       Karen Braman and Ralph Byers, Department of Mathematics,
*>       University of Kansas, USA
*
*> \par References:
*  ================
*>
*>       K. Braman, R. Byers and R. Mathias, The Multi-Shift QR
*>       Algorithm Part I: Maintaining Well Focused Shifts, and Level 3
*>       Performance, SIAM Journal of Matrix Analysis, volume 23, pages
*>       929--947, 2002.
*>
*  =====================================================================
      SUBROUTINE ZLAQR5( WANTT, WANTZ, KACC22, N, KTOP, KBOT, NSHFTS, S,
     $                   H, LDH, ILOZ, IHIZ, Z, LDZ, V, LDV, U, LDU, NV,
     $                   WV, LDWV, NH, WH, LDWH )
*
*  -- LAPACK auxiliary routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      INTEGER            IHIZ, ILOZ, KACC22, KBOT, KTOP, LDH, LDU, LDV,
     $                   LDWH, LDWV, LDZ, N, NH, NSHFTS, NV
      LOGICAL            WANTT, WANTZ
*     ..
*     .. Array Arguments ..
      COMPLEX*16         H( LDH, * ), S( * ), U( LDU, * ), V( LDV, * ),
     $                   WH( LDWH, * ), WV( LDWV, * ), Z( LDZ, * )
*     ..
*
*  ================================================================
*     .. Parameters ..
      COMPLEX*16         ZERO, ONE
      PARAMETER          ( ZERO = ( 0.0d0, 0.0d0 ),
     $                   ONE = ( 1.0d0, 0.0d0 ) )
      DOUBLE PRECISION   RZERO, RONE
      PARAMETER          ( RZERO = 0.0d0, RONE = 1.0d0 )
*     ..
*     .. Local Scalars ..
      COMPLEX*16         ALPHA, BETA, CDUM, REFSUM
      DOUBLE PRECISION   H11, H12, H21, H22, SAFMAX, SAFMIN, SCL,
     $                   SMLNUM, TST1, TST2, ULP
      INTEGER            I2, I4, INCOL, J, J2, J4, JBOT, JCOL, JLEN,
     $                   JROW, JTOP, K, K1, KDU, KMS, KNZ, KRCOL, KZS,
     $                   M, M22, MBOT, MEND, MSTART, MTOP, NBMPS, NDCOL,
     $                   NS, NU
      LOGICAL            ACCUM, BLK22, BMP22
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH
      EXTERNAL           DLAMCH
*     ..
*     .. Intrinsic Functions ..
*
      INTRINSIC          ABS, DBLE, DCONJG, DIMAG, MAX, MIN, MOD
*     ..
*     .. Local Arrays ..
      COMPLEX*16         VT( 3 )
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLABAD, ZGEMM, ZLACPY, ZLAQR1, ZLARFG, ZLASET,
     $                   ZTRMM
*     ..
*     .. Statement Functions ..
      DOUBLE PRECISION   CABS1
*     ..
*     .. Statement Function definitions ..
      CABS1( CDUM ) = ABS( DBLE( CDUM ) ) + ABS( DIMAG( CDUM ) )
*     ..
*     .. Executable Statements ..
*
*     ==== If there are no shifts, then there is nothing to do. ====
*
      IF( NSHFTS.LT.2 )
     $   RETURN
*
*     ==== If the active block is empty or 1-by-1, then there
*     .    is nothing to do. ====
*
      IF( KTOP.GE.KBOT )
     $   RETURN
*
*     ==== NSHFTS is supposed to be even, but if it is odd,
*     .    then simply reduce it by one.  ====
*
      NS = NSHFTS - MOD( NSHFTS, 2 )
*
*     ==== Machine constants for deflation ====
*
      SAFMIN = DLAMCH( 'SAFE MINIMUM' )
      SAFMAX = RONE / SAFMIN
      CALL DLABAD( SAFMIN, SAFMAX )
      ULP = DLAMCH( 'PRECISION' )
      SMLNUM = SAFMIN*( DBLE( N ) / ULP )
*
*     ==== Use accumulated reflections to update far-from-diagonal
*     .    entries ? ====
*
      ACCUM = ( KACC22.EQ.1 ) .OR. ( KACC22.EQ.2 )
*
*     ==== If so, exploit the 2-by-2 block structure? ====
*
      BLK22 = ( NS.GT.2 ) .AND. ( KACC22.EQ.2 )
*
*     ==== clear trash ====
*
      IF( KTOP+2.LE.KBOT )
     $   H( KTOP+2, KTOP ) = ZERO
*
*     ==== NBMPS = number of 2-shift bulges in the chain ====
*
      NBMPS = NS / 2
*
*     ==== KDU = width of slab ====
*
      KDU = 6*NBMPS - 3
*
*     ==== Create and chase chains of NBMPS bulges ====
*
      DO 210 INCOL = 3*( 1-NBMPS ) + KTOP - 1, KBOT - 2, 3*NBMPS - 2
         NDCOL = INCOL + KDU
         IF( ACCUM )
     $      CALL ZLASET( 'ALL', KDU, KDU, ZERO, ONE, U, LDU )
*
*        ==== Near-the-diagonal bulge chase.  The following loop
*        .    performs the near-the-diagonal part of a small bulge
*        .    multi-shift QR sweep.  Each 6*NBMPS-2 column diagonal
*        .    chunk extends from column INCOL to column NDCOL
*        .    (including both column INCOL and column NDCOL). The
*        .    following loop chases a 3*NBMPS column long chain of
*        .    NBMPS bulges 3*NBMPS-2 columns to the right.  (INCOL
*        .    may be less than KTOP and and NDCOL may be greater than
*        .    KBOT indicating phantom columns from which to chase
*        .    bulges before they are actually introduced or to which
*        .    to chase bulges beyond column KBOT.)  ====
*
         DO 140 KRCOL = INCOL, MIN( INCOL+3*NBMPS-3, KBOT-2 )
*
*           ==== Bulges number MTOP to MBOT are active double implicit
*           .    shift bulges.  There may or may not also be small
*           .    2-by-2 bulge, if there is room.  The inactive bulges
*           .    (if any) must wait until the active bulges have moved
*           .    down the diagonal to make room.  The phantom matrix
*           .    paradigm described above helps keep track.  ====
*
            MTOP = MAX( 1, ( ( KTOP-1 )-KRCOL+2 ) / 3+1 )
            MBOT = MIN( NBMPS, ( KBOT-KRCOL ) / 3 )
            M22 = MBOT + 1
            BMP22 = ( MBOT.LT.NBMPS ) .AND. ( KRCOL+3*( M22-1 ) ).EQ.
     $              ( KBOT-2 )
*
*           ==== Generate reflections to chase the chain right
*           .    one column.  (The minimum value of K is KTOP-1.) ====
*
            DO 10 M = MTOP, MBOT
               K = KRCOL + 3*( M-1 )
               IF( K.EQ.KTOP-1 ) THEN
                  CALL ZLAQR1( 3, H( KTOP, KTOP ), LDH, S( 2*M-1 ),
     $                         S( 2*M ), V( 1, M ) )
                  ALPHA = V( 1, M )
                  CALL ZLARFG( 3, ALPHA, V( 2, M ), 1, V( 1, M ) )
               ELSE
                  BETA = H( K+1, K )
                  V( 2, M ) = H( K+2, K )
                  V( 3, M ) = H( K+3, K )
                  CALL ZLARFG( 3, BETA, V( 2, M ), 1, V( 1, M ) )
*
*                 ==== A Bulge may collapse because of vigilant
*                 .    deflation or destructive underflow.  In the
*                 .    underflow case, try the two-small-subdiagonals
*                 .    trick to try to reinflate the bulge.  ====
*
                  IF( H( K+3, K ).NE.ZERO .OR. H( K+3, K+1 ).NE.
     $                ZERO .OR. H( K+3, K+2 ).EQ.ZERO ) THEN
*
*                    ==== Typical case: not collapsed (yet). ====
*
                     H( K+1, K ) = BETA
                     H( K+2, K ) = ZERO
                     H( K+3, K ) = ZERO
                  ELSE
*
*                    ==== Atypical case: collapsed.  Attempt to
*                    .    reintroduce ignoring H(K+1,K) and H(K+2,K).
*                    .    If the fill resulting from the new
*                    .    reflector is too large, then abandon it.
*                    .    Otherwise, use the new one. ====
*
                     CALL ZLAQR1( 3, H( K+1, K+1 ), LDH, S( 2*M-1 ),
     $                            S( 2*M ), VT )
                     ALPHA = VT( 1 )
                     CALL ZLARFG( 3, ALPHA, VT( 2 ), 1, VT( 1 ) )
                     REFSUM = DCONJG( VT( 1 ) )*
     $                        ( H( K+1, K )+DCONJG( VT( 2 ) )*
     $                        H( K+2, K ) )
*
                     IF( CABS1( H( K+2, K )-REFSUM*VT( 2 ) )+
     $                   CABS1( REFSUM*VT( 3 ) ).GT.ULP*
     $                   ( CABS1( H( K, K ) )+CABS1( H( K+1,
     $                   K+1 ) )+CABS1( H( K+2, K+2 ) ) ) ) THEN
*
*                       ==== Starting a new bulge here would
*                       .    create non-negligible fill.  Use
*                       .    the old one with trepidation. ====
*
                        H( K+1, K ) = BETA
                        H( K+2, K ) = ZERO
                        H( K+3, K ) = ZERO
                     ELSE
*
*                       ==== Stating a new bulge here would
*                       .    create only negligible fill.
*                       .    Replace the old reflector with
*                       .    the new one. ====
*
                        H( K+1, K ) = H( K+1, K ) - REFSUM
                        H( K+2, K ) = ZERO
                        H( K+3, K ) = ZERO
                        V( 1, M ) = VT( 1 )
                        V( 2, M ) = VT( 2 )
                        V( 3, M ) = VT( 3 )
                     END IF
                  END IF
               END IF
   10       CONTINUE
*
*           ==== Generate a 2-by-2 reflection, if needed. ====
*
            K = KRCOL + 3*( M22-1 )
            IF( BMP22 ) THEN
               IF( K.EQ.KTOP-1 ) THEN
                  CALL ZLAQR1( 2, H( K+1, K+1 ), LDH, S( 2*M22-1 ),
     $                         S( 2*M22 ), V( 1, M22 ) )
                  BETA = V( 1, M22 )
                  CALL ZLARFG( 2, BETA, V( 2, M22 ), 1, V( 1, M22 ) )
               ELSE
                  BETA = H( K+1, K )
                  V( 2, M22 ) = H( K+2, K )
                  CALL ZLARFG( 2, BETA, V( 2, M22 ), 1, V( 1, M22 ) )
                  H( K+1, K ) = BETA
                  H( K+2, K ) = ZERO
               END IF
            END IF
*
*           ==== Multiply H by reflections from the left ====
*
            IF( ACCUM ) THEN
               JBOT = MIN( NDCOL, KBOT )
            ELSE IF( WANTT ) THEN
               JBOT = N
            ELSE
               JBOT = KBOT
            END IF
            DO 30 J = MAX( KTOP, KRCOL ), JBOT
               MEND = MIN( MBOT, ( J-KRCOL+2 ) / 3 )
               DO 20 M = MTOP, MEND
                  K = KRCOL + 3*( M-1 )
                  REFSUM = DCONJG( V( 1, M ) )*
     $                     ( H( K+1, J )+DCONJG( V( 2, M ) )*
     $                     H( K+2, J )+DCONJG( V( 3, M ) )*H( K+3, J ) )
                  H( K+1, J ) = H( K+1, J ) - REFSUM
                  H( K+2, J ) = H( K+2, J ) - REFSUM*V( 2, M )
                  H( K+3, J ) = H( K+3, J ) - REFSUM*V( 3, M )
   20          CONTINUE
   30       CONTINUE
            IF( BMP22 ) THEN
               K = KRCOL + 3*( M22-1 )
               DO 40 J = MAX( K+1, KTOP ), JBOT
                  REFSUM = DCONJG( V( 1, M22 ) )*
     $                     ( H( K+1, J )+DCONJG( V( 2, M22 ) )*
     $                     H( K+2, J ) )
                  H( K+1, J ) = H( K+1, J ) - REFSUM
                  H( K+2, J ) = H( K+2, J ) - REFSUM*V( 2, M22 )
   40          CONTINUE
            END IF
*
*           ==== Multiply H by reflections from the right.
*           .    Delay filling in the last row until the
*           .    vigilant deflation check is complete. ====
*
            IF( ACCUM ) THEN
               JTOP = MAX( KTOP, INCOL )
            ELSE IF( WANTT ) THEN
               JTOP = 1
            ELSE
               JTOP = KTOP
            END IF
            DO 80 M = MTOP, MBOT
               IF( V( 1, M ).NE.ZERO ) THEN
                  K = KRCOL + 3*( M-1 )
                  DO 50 J = JTOP, MIN( KBOT, K+3 )
                     REFSUM = V( 1, M )*( H( J, K+1 )+V( 2, M )*
     $                        H( J, K+2 )+V( 3, M )*H( J, K+3 ) )
                     H( J, K+1 ) = H( J, K+1 ) - REFSUM
                     H( J, K+2 ) = H( J, K+2 ) -
     $                             REFSUM*DCONJG( V( 2, M ) )
                     H( J, K+3 ) = H( J, K+3 ) -
     $                             REFSUM*DCONJG( V( 3, M ) )
   50             CONTINUE
*
                  IF( ACCUM ) THEN
*
*                    ==== Accumulate U. (If necessary, update Z later
*                    .    with with an efficient matrix-matrix
*                    .    multiply.) ====
*
                     KMS = K - INCOL
                     DO 60 J = MAX( 1, KTOP-INCOL ), KDU
                        REFSUM = V( 1, M )*( U( J, KMS+1 )+V( 2, M )*
     $                           U( J, KMS+2 )+V( 3, M )*U( J, KMS+3 ) )
                        U( J, KMS+1 ) = U( J, KMS+1 ) - REFSUM
                        U( J, KMS+2 ) = U( J, KMS+2 ) -
     $                                  REFSUM*DCONJG( V( 2, M ) )
                        U( J, KMS+3 ) = U( J, KMS+3 ) -
     $                                  REFSUM*DCONJG( V( 3, M ) )
   60                CONTINUE
                  ELSE IF( WANTZ ) THEN
*
*                    ==== U is not accumulated, so update Z
*                    .    now by multiplying by reflections
*                    .    from the right. ====
*
                     DO 70 J = ILOZ, IHIZ
                        REFSUM = V( 1, M )*( Z( J, K+1 )+V( 2, M )*
     $                           Z( J, K+2 )+V( 3, M )*Z( J, K+3 ) )
                        Z( J, K+1 ) = Z( J, K+1 ) - REFSUM
                        Z( J, K+2 ) = Z( J, K+2 ) -
     $                                REFSUM*DCONJG( V( 2, M ) )
                        Z( J, K+3 ) = Z( J, K+3 ) -
     $                                REFSUM*DCONJG( V( 3, M ) )
   70                CONTINUE
                  END IF
               END IF
   80       CONTINUE
*
*           ==== Special case: 2-by-2 reflection (if needed) ====
*
            K = KRCOL + 3*( M22-1 )
            IF( BMP22 ) THEN
               IF ( V( 1, M22 ).NE.ZERO ) THEN
                  DO 90 J = JTOP, MIN( KBOT, K+3 )
                     REFSUM = V( 1, M22 )*( H( J, K+1 )+V( 2, M22 )*
     $                        H( J, K+2 ) )
                     H( J, K+1 ) = H( J, K+1 ) - REFSUM
                     H( J, K+2 ) = H( J, K+2 ) -
     $                             REFSUM*DCONJG( V( 2, M22 ) )
   90             CONTINUE
*
                  IF( ACCUM ) THEN
                     KMS = K - INCOL
                     DO 100 J = MAX( 1, KTOP-INCOL ), KDU
                        REFSUM = V( 1, M22 )*( U( J, KMS+1 )+
     $                           V( 2, M22 )*U( J, KMS+2 ) )
                        U( J, KMS+1 ) = U( J, KMS+1 ) - REFSUM
                        U( J, KMS+2 ) = U( J, KMS+2 ) -
     $                                  REFSUM*DCONJG( V( 2, M22 ) )
  100                CONTINUE
                  ELSE IF( WANTZ ) THEN
                     DO 110 J = ILOZ, IHIZ
                        REFSUM = V( 1, M22 )*( Z( J, K+1 )+V( 2, M22 )*
     $                           Z( J, K+2 ) )
                        Z( J, K+1 ) = Z( J, K+1 ) - REFSUM
                        Z( J, K+2 ) = Z( J, K+2 ) -
     $                                REFSUM*DCONJG( V( 2, M22 ) )
  110                CONTINUE
                  END IF
               END IF
            END IF
*
*           ==== Vigilant deflation check ====
*
            MSTART = MTOP
            IF( KRCOL+3*( MSTART-1 ).LT.KTOP )
     $         MSTART = MSTART + 1
            MEND = MBOT
            IF( BMP22 )
     $         MEND = MEND + 1
            IF( KRCOL.EQ.KBOT-2 )
     $         MEND = MEND + 1
            DO 120 M = MSTART, MEND
               K = MIN( KBOT-1, KRCOL+3*( M-1 ) )
*
*              ==== The following convergence test requires that
*              .    the tradition small-compared-to-nearby-diagonals
*              .    criterion and the Ahues & Tisseur (LAWN 122, 1997)
*              .    criteria both be satisfied.  The latter improves
*              .    accuracy in some examples. Falling back on an
*              .    alternate convergence criterion when TST1 or TST2
*              .    is zero (as done here) is traditional but probably
*              .    unnecessary. ====
*
               IF( H( K+1, K ).NE.ZERO ) THEN
                  TST1 = CABS1( H( K, K ) ) + CABS1( H( K+1, K+1 ) )
                  IF( TST1.EQ.RZERO ) THEN
                     IF( K.GE.KTOP+1 )
     $                  TST1 = TST1 + CABS1( H( K, K-1 ) )
                     IF( K.GE.KTOP+2 )
     $                  TST1 = TST1 + CABS1( H( K, K-2 ) )
                     IF( K.GE.KTOP+3 )
     $                  TST1 = TST1 + CABS1( H( K, K-3 ) )
                     IF( K.LE.KBOT-2 )
     $                  TST1 = TST1 + CABS1( H( K+2, K+1 ) )
                     IF( K.LE.KBOT-3 )
     $                  TST1 = TST1 + CABS1( H( K+3, K+1 ) )
                     IF( K.LE.KBOT-4 )
     $                  TST1 = TST1 + CABS1( H( K+4, K+1 ) )
                  END IF
                  IF( CABS1( H( K+1, K ) ).LE.MAX( SMLNUM, ULP*TST1 ) )
     $                 THEN
                     H12 = MAX( CABS1( H( K+1, K ) ),
     $                     CABS1( H( K, K+1 ) ) )
                     H21 = MIN( CABS1( H( K+1, K ) ),
     $                     CABS1( H( K, K+1 ) ) )
                     H11 = MAX( CABS1( H( K+1, K+1 ) ),
     $                     CABS1( H( K, K )-H( K+1, K+1 ) ) )
                     H22 = MIN( CABS1( H( K+1, K+1 ) ),
     $                     CABS1( H( K, K )-H( K+1, K+1 ) ) )
                     SCL = H11 + H12
                     TST2 = H22*( H11 / SCL )
*
                     IF( TST2.EQ.RZERO .OR. H21*( H12 / SCL ).LE.
     $                   MAX( SMLNUM, ULP*TST2 ) )H( K+1, K ) = ZERO
                  END IF
               END IF
  120       CONTINUE
*
*           ==== Fill in the last row of each bulge. ====
*
            MEND = MIN( NBMPS, ( KBOT-KRCOL-1 ) / 3 )
            DO 130 M = MTOP, MEND
               K = KRCOL + 3*( M-1 )
               REFSUM = V( 1, M )*V( 3, M )*H( K+4, K+3 )
               H( K+4, K+1 ) = -REFSUM
               H( K+4, K+2 ) = -REFSUM*DCONJG( V( 2, M ) )
               H( K+4, K+3 ) = H( K+4, K+3 ) -
     $                         REFSUM*DCONJG( V( 3, M ) )
  130       CONTINUE
*
*           ==== End of near-the-diagonal bulge chase. ====
*
  140    CONTINUE
*
*        ==== Use U (if accumulated) to update far-from-diagonal
*        .    entries in H.  If required, use U to update Z as
*        .    well. ====
*
         IF( ACCUM ) THEN
            IF( WANTT ) THEN
               JTOP = 1
               JBOT = N
            ELSE
               JTOP = KTOP
               JBOT = KBOT
            END IF
            IF( ( .NOT.BLK22 ) .OR. ( INCOL.LT.KTOP ) .OR.
     $          ( NDCOL.GT.KBOT ) .OR. ( NS.LE.2 ) ) THEN
*
*              ==== Updates not exploiting the 2-by-2 block
*              .    structure of U.  K1 and NU keep track of
*              .    the location and size of U in the special
*              .    cases of introducing bulges and chasing
*              .    bulges off the bottom.  In these special
*              .    cases and in case the number of shifts
*              .    is NS = 2, there is no 2-by-2 block
*              .    structure to exploit.  ====
*
               K1 = MAX( 1, KTOP-INCOL )
               NU = ( KDU-MAX( 0, NDCOL-KBOT ) ) - K1 + 1
*
*              ==== Horizontal Multiply ====
*
               DO 150 JCOL = MIN( NDCOL, KBOT ) + 1, JBOT, NH
                  JLEN = MIN( NH, JBOT-JCOL+1 )
                  CALL ZGEMM( 'C', 'N', NU, JLEN, NU, ONE, U( K1, K1 ),
     $                        LDU, H( INCOL+K1, JCOL ), LDH, ZERO, WH,
     $                        LDWH )
                  CALL ZLACPY( 'ALL', NU, JLEN, WH, LDWH,
     $                         H( INCOL+K1, JCOL ), LDH )
  150          CONTINUE
*
*              ==== Vertical multiply ====
*
               DO 160 JROW = JTOP, MAX( KTOP, INCOL ) - 1, NV
                  JLEN = MIN( NV, MAX( KTOP, INCOL )-JROW )
                  CALL ZGEMM( 'N', 'N', JLEN, NU, NU, ONE,
     $                        H( JROW, INCOL+K1 ), LDH, U( K1, K1 ),
     $                        LDU, ZERO, WV, LDWV )
                  CALL ZLACPY( 'ALL', JLEN, NU, WV, LDWV,
     $                         H( JROW, INCOL+K1 ), LDH )
  160          CONTINUE
*
*              ==== Z multiply (also vertical) ====
*
               IF( WANTZ ) THEN
                  DO 170 JROW = ILOZ, IHIZ, NV
                     JLEN = MIN( NV, IHIZ-JROW+1 )
                     CALL ZGEMM( 'N', 'N', JLEN, NU, NU, ONE,
     $                           Z( JROW, INCOL+K1 ), LDZ, U( K1, K1 ),
     $                           LDU, ZERO, WV, LDWV )
                     CALL ZLACPY( 'ALL', JLEN, NU, WV, LDWV,
     $                            Z( JROW, INCOL+K1 ), LDZ )
  170             CONTINUE
               END IF
            ELSE
*
*              ==== Updates exploiting U's 2-by-2 block structure.
*              .    (I2, I4, J2, J4 are the last rows and columns
*              .    of the blocks.) ====
*
               I2 = ( KDU+1 ) / 2
               I4 = KDU
               J2 = I4 - I2
               J4 = KDU
*
*              ==== KZS and KNZ deal with the band of zeros
*              .    along the diagonal of one of the triangular
*              .    blocks. ====
*
               KZS = ( J4-J2 ) - ( NS+1 )
               KNZ = NS + 1
*
*              ==== Horizontal multiply ====
*
               DO 180 JCOL = MIN( NDCOL, KBOT ) + 1, JBOT, NH
                  JLEN = MIN( NH, JBOT-JCOL+1 )
*
*                 ==== Copy bottom of H to top+KZS of scratch ====
*                  (The first KZS rows get multiplied by zero.) ====
*
                  CALL ZLACPY( 'ALL', KNZ, JLEN, H( INCOL+1+J2, JCOL ),
     $                         LDH, WH( KZS+1, 1 ), LDWH )
*
*                 ==== Multiply by U21**H ====
*
                  CALL ZLASET( 'ALL', KZS, JLEN, ZERO, ZERO, WH, LDWH )
                  CALL ZTRMM( 'L', 'U', 'C', 'N', KNZ, JLEN, ONE,
     $                        U( J2+1, 1+KZS ), LDU, WH( KZS+1, 1 ),
     $                        LDWH )
*
*                 ==== Multiply top of H by U11**H ====
*
                  CALL ZGEMM( 'C', 'N', I2, JLEN, J2, ONE, U, LDU,
     $                        H( INCOL+1, JCOL ), LDH, ONE, WH, LDWH )
*
*                 ==== Copy top of H to bottom of WH ====
*
                  CALL ZLACPY( 'ALL', J2, JLEN, H( INCOL+1, JCOL ), LDH,
     $                         WH( I2+1, 1 ), LDWH )
*
*                 ==== Multiply by U21**H ====
*
                  CALL ZTRMM( 'L', 'L', 'C', 'N', J2, JLEN, ONE,
     $                        U( 1, I2+1 ), LDU, WH( I2+1, 1 ), LDWH )
*
*                 ==== Multiply by U22 ====
*
                  CALL ZGEMM( 'C', 'N', I4-I2, JLEN, J4-J2, ONE,
     $                        U( J2+1, I2+1 ), LDU,
     $                        H( INCOL+1+J2, JCOL ), LDH, ONE,
     $                        WH( I2+1, 1 ), LDWH )
*
*                 ==== Copy it back ====
*
                  CALL ZLACPY( 'ALL', KDU, JLEN, WH, LDWH,
     $                         H( INCOL+1, JCOL ), LDH )
  180          CONTINUE
*
*              ==== Vertical multiply ====
*
               DO 190 JROW = JTOP, MAX( INCOL, KTOP ) - 1, NV
                  JLEN = MIN( NV, MAX( INCOL, KTOP )-JROW )
*
*                 ==== Copy right of H to scratch (the first KZS
*                 .    columns get multiplied by zero) ====
*
                  CALL ZLACPY( 'ALL', JLEN, KNZ, H( JROW, INCOL+1+J2 ),
     $                         LDH, WV( 1, 1+KZS ), LDWV )
*
*                 ==== Multiply by U21 ====
*
                  CALL ZLASET( 'ALL', JLEN, KZS, ZERO, ZERO, WV, LDWV )
                  CALL ZTRMM( 'R', 'U', 'N', 'N', JLEN, KNZ, ONE,
     $                        U( J2+1, 1+KZS ), LDU, WV( 1, 1+KZS ),
     $                        LDWV )
*
*                 ==== Multiply by U11 ====
*
                  CALL ZGEMM( 'N', 'N', JLEN, I2, J2, ONE,
     $                        H( JROW, INCOL+1 ), LDH, U, LDU, ONE, WV,
     $                        LDWV )
*
*                 ==== Copy left of H to right of scratch ====
*
                  CALL ZLACPY( 'ALL', JLEN, J2, H( JROW, INCOL+1 ), LDH,
     $                         WV( 1, 1+I2 ), LDWV )
*
*                 ==== Multiply by U21 ====
*
                  CALL ZTRMM( 'R', 'L', 'N', 'N', JLEN, I4-I2, ONE,
     $                        U( 1, I2+1 ), LDU, WV( 1, 1+I2 ), LDWV )
*
*                 ==== Multiply by U22 ====
*
                  CALL ZGEMM( 'N', 'N', JLEN, I4-I2, J4-J2, ONE,
     $                        H( JROW, INCOL+1+J2 ), LDH,
     $                        U( J2+1, I2+1 ), LDU, ONE, WV( 1, 1+I2 ),
     $                        LDWV )
*
*                 ==== Copy it back ====
*
                  CALL ZLACPY( 'ALL', JLEN, KDU, WV, LDWV,
     $                         H( JROW, INCOL+1 ), LDH )
  190          CONTINUE
*
*              ==== Multiply Z (also vertical) ====
*
               IF( WANTZ ) THEN
                  DO 200 JROW = ILOZ, IHIZ, NV
                     JLEN = MIN( NV, IHIZ-JROW+1 )
*
*                    ==== Copy right of Z to left of scratch (first
*                    .     KZS columns get multiplied by zero) ====
*
                     CALL ZLACPY( 'ALL', JLEN, KNZ,
     $                            Z( JROW, INCOL+1+J2 ), LDZ,
     $                            WV( 1, 1+KZS ), LDWV )
*
*                    ==== Multiply by U12 ====
*
                     CALL ZLASET( 'ALL', JLEN, KZS, ZERO, ZERO, WV,
     $                            LDWV )
                     CALL ZTRMM( 'R', 'U', 'N', 'N', JLEN, KNZ, ONE,
     $                           U( J2+1, 1+KZS ), LDU, WV( 1, 1+KZS ),
     $                           LDWV )
*
*                    ==== Multiply by U11 ====
*
                     CALL ZGEMM( 'N', 'N', JLEN, I2, J2, ONE,
     $                           Z( JROW, INCOL+1 ), LDZ, U, LDU, ONE,
     $                           WV, LDWV )
*
*                    ==== Copy left of Z to right of scratch ====
*
                     CALL ZLACPY( 'ALL', JLEN, J2, Z( JROW, INCOL+1 ),
     $                            LDZ, WV( 1, 1+I2 ), LDWV )
*
*                    ==== Multiply by U21 ====
*
                     CALL ZTRMM( 'R', 'L', 'N', 'N', JLEN, I4-I2, ONE,
     $                           U( 1, I2+1 ), LDU, WV( 1, 1+I2 ),
     $                           LDWV )
*
*                    ==== Multiply by U22 ====
*
                     CALL ZGEMM( 'N', 'N', JLEN, I4-I2, J4-J2, ONE,
     $                           Z( JROW, INCOL+1+J2 ), LDZ,
     $                           U( J2+1, I2+1 ), LDU, ONE,
     $                           WV( 1, 1+I2 ), LDWV )
*
*                    ==== Copy the result back to Z ====
*
                     CALL ZLACPY( 'ALL', JLEN, KDU, WV, LDWV,
     $                            Z( JROW, INCOL+1 ), LDZ )
  200             CONTINUE
               END IF
            END IF
         END IF
  210 CONTINUE
*
*     ==== End of ZLAQR5 ====
*
      END
