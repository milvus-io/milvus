! Copyright (c) 2013-2016, The OpenBLAS Project
! All rights reserved.
! Redistribution and use in source and binary forms, with or without
! modification, are permitted provided that the following conditions are
! met:
! 1. Redistributions of source code must retain the above copyright
! notice, this list of conditions and the following disclaimer.
! 2. Redistributions in binary form must reproduce the above copyright
! notice, this list of conditions and the following disclaimer in
! the documentation and/or other materials provided with the
! distribution.
! 3. Neither the name of the OpenBLAS project nor the names of
! its contributors may be used to endorse or promote products
! derived from this software without specific prior written permission.
! THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
! AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
! IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
! ARE DISCLAIMED. IN NO EVENT SHALL THE OPENBLAS PROJECT OR CONTRIBUTORS BE
! LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
! DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
! SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
! CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
! OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
! USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*> \brief \b DLAQR5 performs a single small-bulge multi-shift QR sweep.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at 
*            http://www.netlib.org/lapack/explore-html/ 
*
*> \htmlonly
*> Download DLAQR5 + dependencies 
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlaqr5.f"> 
*> [TGZ]</a> 
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlaqr5.f"> 
*> [ZIP]</a> 
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlaqr5.f"> 
*> [TXT]</a>
*> \endhtmlonly 
*
*  Definition:
*  ===========
*
*       SUBROUTINE DLAQR5( WANTT, WANTZ, KACC22, N, KTOP, KBOT, NSHFTS,
*                          SR, SI, H, LDH, ILOZ, IHIZ, Z, LDZ, V, LDV, U,
*                          LDU, NV, WV, LDWV, NH, WH, LDWH )
* 
*       .. Scalar Arguments ..
*       INTEGER            IHIZ, ILOZ, KACC22, KBOT, KTOP, LDH, LDU, LDV,
*      $                   LDWH, LDWV, LDZ, N, NH, NSHFTS, NV
*       LOGICAL            WANTT, WANTZ
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   H( LDH, * ), SI( * ), SR( * ), U( LDU, * ),
*      $                   V( LDV, * ), WH( LDWH, * ), WV( LDWV, * ),
*      $                   Z( LDZ, * )
*       ..
*  
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    DLAQR5, called by DLAQR0, performs a
*>    single small-bulge multi-shift QR sweep.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] WANTT
*> \verbatim
*>          WANTT is logical scalar
*>             WANTT = .true. if the quasi-triangular Schur factor
*>             is being computed.  WANTT is set to .false. otherwise.
*> \endverbatim
*>
*> \param[in] WANTZ
*> \verbatim
*>          WANTZ is logical scalar
*>             WANTZ = .true. if the orthogonal Schur factor is being
*>             computed.  WANTZ is set to .false. otherwise.
*> \endverbatim
*>
*> \param[in] KACC22
*> \verbatim
*>          KACC22 is integer with value 0, 1, or 2.
*>             Specifies the computation mode of far-from-diagonal
*>             orthogonal updates.
*>        = 0: DLAQR5 does not accumulate reflections and does not
*>             use matrix-matrix multiply to update far-from-diagonal
*>             matrix entries.
*>        = 1: DLAQR5 accumulates reflections and uses matrix-matrix
*>             multiply to update the far-from-diagonal matrix entries.
*>        = 2: DLAQR5 accumulates reflections, uses matrix-matrix
*>             multiply to update the far-from-diagonal matrix entries,
*>             and takes advantage of 2-by-2 block structure during
*>             matrix multiplies.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is integer scalar
*>             N is the order of the Hessenberg matrix H upon which this
*>             subroutine operates.
*> \endverbatim
*>
*> \param[in] KTOP
*> \verbatim
*>          KTOP is integer scalar
*> \endverbatim
*>
*> \param[in] KBOT
*> \verbatim
*>          KBOT is integer scalar
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
*>          NSHFTS is integer scalar
*>             NSHFTS gives the number of simultaneous shifts.  NSHFTS
*>             must be positive and even.
*> \endverbatim
*>
*> \param[in,out] SR
*> \verbatim
*>          SR is DOUBLE PRECISION array of size (NSHFTS)
*> \endverbatim
*>
*> \param[in,out] SI
*> \verbatim
*>          SI is DOUBLE PRECISION array of size (NSHFTS)
*>             SR contains the real parts and SI contains the imaginary
*>             parts of the NSHFTS shifts of origin that define the
*>             multi-shift QR sweep.  On output SR and SI may be
*>             reordered.
*> \endverbatim
*>
*> \param[in,out] H
*> \verbatim
*>          H is DOUBLE PRECISION array of size (LDH,N)
*>             On input H contains a Hessenberg matrix.  On output a
*>             multi-shift QR sweep with shifts SR(J)+i*SI(J) is applied
*>             to the isolated diagonal block in rows and columns KTOP
*>             through KBOT.
*> \endverbatim
*>
*> \param[in] LDH
*> \verbatim
*>          LDH is integer scalar
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
*>          Z is DOUBLE PRECISION array of size (LDZ,IHI)
*>             If WANTZ = .TRUE., then the QR Sweep orthogonal
*>             similarity transformation is accumulated into
*>             Z(ILOZ:IHIZ,ILO:IHI) from the right.
*>             If WANTZ = .FALSE., then Z is unreferenced.
*> \endverbatim
*>
*> \param[in] LDZ
*> \verbatim
*>          LDZ is integer scalar
*>             LDA is the leading dimension of Z just as declared in
*>             the calling procedure. LDZ.GE.N.
*> \endverbatim
*>
*> \param[out] V
*> \verbatim
*>          V is DOUBLE PRECISION array of size (LDV,NSHFTS/2)
*> \endverbatim
*>
*> \param[in] LDV
*> \verbatim
*>          LDV is integer scalar
*>             LDV is the leading dimension of V as declared in the
*>             calling procedure.  LDV.GE.3.
*> \endverbatim
*>
*> \param[out] U
*> \verbatim
*>          U is DOUBLE PRECISION array of size
*>             (LDU,3*NSHFTS-3)
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is integer scalar
*>             LDU is the leading dimension of U just as declared in the
*>             in the calling subroutine.  LDU.GE.3*NSHFTS-3.
*> \endverbatim
*>
*> \param[in] NH
*> \verbatim
*>          NH is integer scalar
*>             NH is the number of columns in array WH available for
*>             workspace. NH.GE.1.
*> \endverbatim
*>
*> \param[out] WH
*> \verbatim
*>          WH is DOUBLE PRECISION array of size (LDWH,NH)
*> \endverbatim
*>
*> \param[in] LDWH
*> \verbatim
*>          LDWH is integer scalar
*>             Leading dimension of WH just as declared in the
*>             calling procedure.  LDWH.GE.3*NSHFTS-3.
*> \endverbatim
*>
*> \param[in] NV
*> \verbatim
*>          NV is integer scalar
*>             NV is the number of rows in WV agailable for workspace.
*>             NV.GE.1.
*> \endverbatim
*>
*> \param[out] WV
*> \verbatim
*>          WV is DOUBLE PRECISION array of size
*>             (LDWV,3*NSHFTS-3)
*> \endverbatim
*>
*> \param[in] LDWV
*> \verbatim
*>          LDWV is integer scalar
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
*> \date September 2012
*
*> \ingroup doubleOTHERauxiliary
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
      SUBROUTINE DLAQR5( WANTT, WANTZ, KACC22, N, KTOP, KBOT, NSHFTS,
     $                   SR, SI, H, LDH, ILOZ, IHIZ, Z, LDZ, V, LDV, U,
     $                   LDU, NV, WV, LDWV, NH, WH, LDWH )
*
*  -- LAPACK auxiliary routine (version 3.4.2) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     September 2012
*
*     .. Scalar Arguments ..
      INTEGER            IHIZ, ILOZ, KACC22, KBOT, KTOP, LDH, LDU, LDV,
     $                   LDWH, LDWV, LDZ, N, NH, NSHFTS, NV
      LOGICAL            WANTT, WANTZ
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   H( LDH, * ), SI( * ), SR( * ), U( LDU, * ),
     $                   V( LDV, * ), WH( LDWH, * ), WV( LDWV, * ),
     $                   Z( LDZ, * )
*     ..
*
*  ================================================================
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0d0, ONE = 1.0d0 )
*     ..
*     .. Local Scalars ..
      DOUBLE PRECISION   ALPHA, BETA, H11, H12, H21, H22, REFSUM,
     $                   SAFMAX, SAFMIN, SCL, SMLNUM, SWAP, TST1, TST2,
     $                   ULP
      INTEGER            I, I2, I4, INCOL, J, J2, J4, JBOT, JCOL, JLEN,
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
      INTRINSIC          ABS, DBLE, MAX, MIN, MOD
*     ..
*     .. Local Arrays ..
      DOUBLE PRECISION   VT( 3 )
*     temp scalars
      DOUBLE PRECISION tempv1, tempv2, tempv3,
     $                 tempv4, tempv5, tempv6,
     $                 temph1, temph2, temph3,
     $                 temph4, temph5, temph6,
     $                 tempz1, tempz2, tempz3,
     $                 tempz4, tempz5, tempz6,
     $                 tempu1, tempu2, tempu3,
     $                 tempu4, tempu5, tempu6,
     $                 REFSU1
      INTEGER          JBEGIN, M1
*     ..
*     .. External Subroutines ..
      EXTERNAL           DGEMM, DLABAD, DLACPY, DLAQR1, DLARFG, DLASET,
     $                   DTRMM
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
*     ==== Shuffle shifts into pairs of real shifts and pairs
*     .    of complex conjugate shifts assuming complex
*     .    conjugate shifts are already adjacent to one
*     .    another. ====
*
      DO 10 I = 1, NSHFTS - 2, 2
         IF( SI( I ).NE.-SI( I+1 ) ) THEN
*
            SWAP = SR( I )
            SR( I ) = SR( I+1 )
            SR( I+1 ) = SR( I+2 )
            SR( I+2 ) = SWAP
*
            SWAP = SI( I )
            SI( I ) = SI( I+1 )
            SI( I+1 ) = SI( I+2 )
            SI( I+2 ) = SWAP
         END IF
   10 CONTINUE
*
*     ==== NSHFTS is supposed to be even, but if it is odd,
*     .    then simply reduce it by one.  The shuffle above
*     .    ensures that the dropped shift is real and that
*     .    the remaining shifts are paired. ====
*
      NS = NSHFTS - MOD( NSHFTS, 2 )
*
*     ==== Machine constants for deflation ====
*
      SAFMIN = DLAMCH( 'SAFE MINIMUM' )
      SAFMAX = ONE / SAFMIN
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
      DO 220 INCOL = 3*( 1-NBMPS ) + KTOP - 1, KBOT - 2, 3*NBMPS - 2
         NDCOL = INCOL + KDU
         IF( ACCUM )
     $      CALL DLASET( 'ALL', KDU, KDU, ZERO, ONE, U, LDU )
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
         DO 150 KRCOL = INCOL, MIN( INCOL+3*NBMPS-3, KBOT-2 )
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
            DO 20 M = MTOP, MBOT
               K = KRCOL + 3*( M-1 )
               IF( K.EQ.KTOP-1 ) THEN
                  CALL DLAQR1( 3, H( KTOP, KTOP ), LDH, SR( 2*M-1 ),
     $                         SI( 2*M-1 ), SR( 2*M ), SI( 2*M ),
     $                         V( 1, M ) )
                  ALPHA = V( 1, M )
                  CALL DLARFG( 3, ALPHA, V( 2, M ), 1, V( 1, M ) )
               ELSE
                  BETA = H( K+1, K )
                  V( 2, M ) = H( K+2, K )
                  V( 3, M ) = H( K+3, K )
                  CALL DLARFG( 3, BETA, V( 2, M ), 1, V( 1, M ) )
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
                     CALL DLAQR1( 3, H( K+1, K+1 ), LDH, SR( 2*M-1 ),
     $                            SI( 2*M-1 ), SR( 2*M ), SI( 2*M ),
     $                            VT )
                     ALPHA = VT( 1 )
                     CALL DLARFG( 3, ALPHA, VT( 2 ), 1, VT( 1 ) )
                     REFSUM = VT( 1 )*( H( K+1, K )+VT( 2 )*
     $                        H( K+2, K ) )
*
                     IF( ABS( H( K+2, K )-REFSUM*VT( 2 ) )+
     $                   ABS( REFSUM*VT( 3 ) ).GT.ULP*
     $                   ( ABS( H( K, K ) )+ABS( H( K+1,
     $                   K+1 ) )+ABS( H( K+2, K+2 ) ) ) ) THEN
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
   20       CONTINUE
*
*           ==== Generate a 2-by-2 reflection, if needed. ====
*
            K = KRCOL + 3*( M22-1 )
            IF( BMP22 ) THEN
               IF( K.EQ.KTOP-1 ) THEN
                  CALL DLAQR1( 2, H( K+1, K+1 ), LDH, SR( 2*M22-1 ),
     $                         SI( 2*M22-1 ), SR( 2*M22 ), SI( 2*M22 ),
     $                         V( 1, M22 ) )
                  BETA = V( 1, M22 )
                  CALL DLARFG( 2, BETA, V( 2, M22 ), 1, V( 1, M22 ) )
               ELSE
                  BETA = H( K+1, K )
                  V( 2, M22 ) = H( K+2, K )
                  CALL DLARFG( 2, BETA, V( 2, M22 ), 1, V( 1, M22 ) )
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
            DO 40 J = MAX( KTOP, KRCOL ), JBOT
               MEND = MIN( MBOT, ( J-KRCOL+2 ) / 3 )
              
               DO 30 M = MTOP, MEND

                  M1  = M -1 

                  tempv1 = V( 1, M )
                  K   = KRCOL + 2*M1
                  tempv2 = V( 2, M )
                  K   = K + M1 
                  tempv3 = V( 3, M )
                  temph1 = H( K+1, J )
                  temph2 = H( K+2, J )
                  temph3 = H( K+3, J )

                  REFSUM = tempv1*( temph1+tempv2*
     $                     temph2+tempv3*temph3 )


                  H( K+1, J ) = temph1 - REFSUM
                  H( K+2, J ) = temph2 - REFSUM*tempv2
                  H( K+3, J ) = temph3 - REFSUM*tempv3

   30          CONTINUE

   40       CONTINUE
            IF( BMP22 ) THEN
               K = KRCOL + 3*( M22-1 )
               DO 50 J = MAX( K+1, KTOP ), JBOT
                  REFSUM = V( 1, M22 )*( H( K+1, J )+V( 2, M22 )*
     $                     H( K+2, J ) )
                  H( K+1, J ) = H( K+1, J ) - REFSUM
                  H( K+2, J ) = H( K+2, J ) - REFSUM*V( 2, M22 )
   50          CONTINUE
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
            DO 90 M = MTOP, MBOT
               IF( V( 1, M ).NE.ZERO ) THEN
                  tempv1 = V( 1, M )
                  tempv2 = V( 2, M )
                  tempv3 = V( 3, M )
                  K = KRCOL + 3*( M-1 )
                  JBEGIN = JTOP

                  IF ( MOD( MIN( KBOT, K+3 )-JTOP+1, 2).GT.0 ) THEN
                     J = JBEGIN
        
                     temph1 = H( J, K+1 )
                     temph2 = H( J, K+2 )
                     temph3 = H( J, K+3 )
                     REFSUM = tempv1* ( temph1+tempv2*temph2+
     $                        tempv3*temph3 )
                     H( J, K+1 ) = temph1 - REFSUM
                     H( J, K+2 ) = temph2 - REFSUM*tempv2
                     H( J, K+3 ) = temph3 - REFSUM*tempv3

                     JBEGIN = JBEGIN + 1

                  END IF


                  DO 60 J = JBEGIN, MIN( KBOT, K+3 ), 2
        
                     temph1 = H( J, K+1 )
                     temph4 = H( J+1, K+1 )
                     temph2 = H( J, K+2 )
                     temph5 = H( J+1, K+2 )
                     temph3 = H( J, K+3 )
                     temph6 = H( J+1, K+3 )

                     REFSUM = tempv1* ( temph1+tempv2*temph2+
     $                        tempv3*temph3 )

                     REFSU1 = tempv1* ( temph4+tempv2*temph5+
     $                        tempv3*temph6 )

                     H( J, K+1 )   = temph1 - REFSUM
                     H( J+1, K+1 ) = temph4 - REFSU1
                     H( J, K+2 )   = temph2 - REFSUM*tempv2
                     H( J+1, K+2 ) = temph5 - REFSU1*tempv2
                     H( J, K+3 )   = temph3 - REFSUM*tempv3
                     H( J+1, K+3 ) = temph6 - REFSU1*tempv3

   60             CONTINUE
*
                  IF( ACCUM ) THEN
*
*                    ==== Accumulate U. (If necessary, update Z later
*                    .    with with an efficient matrix-matrix
*                    .    multiply.) ====
*
                     KMS = K - INCOL
                     JBEGIN=MAX( 1, KTOP-INCOL )                     

                     IF ( MOD(KDU-JBEGIN+1,2).GT.0 ) THEN
                        J = JBEGIN
                        tempu1 = U( J, KMS+1 )
                        tempu2 = U( J, KMS+2 )
                        tempu3 = U( J, KMS+3 )
                        REFSUM = tempv1* ( tempu1+tempv2*tempu2+
     $                           tempv3*tempu3 )
                        U( J, KMS+1 ) = tempu1 - REFSUM
                        U( J, KMS+2 ) = tempu2 - REFSUM*tempv2
                        U( J, KMS+3 ) = tempu3 - REFSUM*tempv3
                        JBEGIN = JBEGIN + 1

                     END IF


                     DO 70 J = JBEGIN, KDU , 2

                        tempu1 = U( J, KMS+1 )
                        tempu4 = U( J+1, KMS+1 )
                        tempu2 = U( J, KMS+2 )
                        tempu5 = U( J+1, KMS+2 )
                        tempu3 = U( J, KMS+3 )
                        tempu6 = U( J+1, KMS+3 )
                        REFSUM = tempv1* ( tempu1+tempv2*tempu2+
     $                           tempv3*tempu3 )

                        REFSU1 = tempv1* ( tempu4+tempv2*tempu5+
     $                           tempv3*tempu6 )

                        U( J, KMS+1 )   = tempu1 - REFSUM
                        U( J+1, KMS+1 ) = tempu4 - REFSU1
                        U( J, KMS+2 )   = tempu2 - REFSUM*tempv2
                        U( J+1, KMS+2 ) = tempu5 - REFSU1*tempv2
                        U( J, KMS+3 )   = tempu3 - REFSUM*tempv3
                        U( J+1, KMS+3 ) = tempu6 - REFSU1*tempv3

   70                CONTINUE


                  ELSE IF( WANTZ ) THEN
*
*                    ==== U is not accumulated, so update Z
*                    .    now by multiplying by reflections
*                    .    from the right. ====
*
                     JBEGIN = ILOZ

                     IF ( MOD(IHIZ-ILOZ+1,2).GT.0 ) THEN
                        J = JBEGIN

                        tempz1 = Z( J, K+1 )
                        tempz2 = Z( J, K+2 )
                        tempz3 = Z( J, K+3 )
                        REFSUM = tempv1* ( tempz1+tempv2*tempz2+
     $                           tempv3*tempz3 )
                        Z( J, K+1 ) = tempz1 - REFSUM
                        Z( J, K+2 ) = tempz2 - REFSUM*tempv2
                        Z( J, K+3 ) = tempz3 - REFSUM*tempv3

                        JBEGIN = JBEGIN + 1

                     END IF

                     DO 80 J = JBEGIN, IHIZ, 2

                        tempz1 = Z( J, K+1 )
                        tempz4 = Z( J+1, K+1 )
                        tempz2 = Z( J, K+2 )
                        tempz5 = Z( J+1, K+2 )
                        tempz3 = Z( J, K+3 )
                        tempz6 = Z( J+1, K+3 )

                        REFSUM = tempv1* ( tempz1+tempv2*tempz2+
     $                           tempv3*tempz3 )

                        REFSU1 = tempv1* ( tempz4+tempv2*tempz5+
     $                           tempv3*tempz6 )

                        Z( J, K+1 ) = tempz1 - REFSUM
                        Z( J, K+2 ) = tempz2 - REFSUM*tempv2
                        Z( J, K+3 ) = tempz3 - REFSUM*tempv3


                        Z( J+1, K+1 ) = tempz4 - REFSU1
                        Z( J+1, K+2 ) = tempz5 - REFSU1*tempv2
                        Z( J+1, K+3 ) = tempz6 - REFSU1*tempv3


   80                CONTINUE

                  END IF
               END IF
   90       CONTINUE
*
*           ==== Special case: 2-by-2 reflection (if needed) ====
*
            K = KRCOL + 3*( M22-1 )
            IF( BMP22 ) THEN
               IF ( V( 1, M22 ).NE.ZERO ) THEN
                  DO 100 J = JTOP, MIN( KBOT, K+3 )
                     REFSUM = V( 1, M22 )*( H( J, K+1 )+V( 2, M22 )*
     $                        H( J, K+2 ) )
                     H( J, K+1 ) = H( J, K+1 ) - REFSUM
                     H( J, K+2 ) = H( J, K+2 ) - REFSUM*V( 2, M22 )
  100             CONTINUE
*
                  IF( ACCUM ) THEN
                     KMS = K - INCOL
                     DO 110 J = MAX( 1, KTOP-INCOL ), KDU
                        REFSUM = V( 1, M22 )*( U( J, KMS+1 )+
     $                           V( 2, M22 )*U( J, KMS+2 ) )
                        U( J, KMS+1 ) = U( J, KMS+1 ) - REFSUM
                        U( J, KMS+2 ) = U( J, KMS+2 ) -
     $                                  REFSUM*V( 2, M22 )
  110             CONTINUE
                  ELSE IF( WANTZ ) THEN
                     DO 120 J = ILOZ, IHIZ
                        REFSUM = V( 1, M22 )*( Z( J, K+1 )+V( 2, M22 )*
     $                           Z( J, K+2 ) )
                        Z( J, K+1 ) = Z( J, K+1 ) - REFSUM
                        Z( J, K+2 ) = Z( J, K+2 ) - REFSUM*V( 2, M22 )
  120                CONTINUE
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
            DO 130 M = MSTART, MEND
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
                  TST1 = ABS( H( K, K ) ) + ABS( H( K+1, K+1 ) )
                  IF( TST1.EQ.ZERO ) THEN
                     IF( K.GE.KTOP+1 )
     $                  TST1 = TST1 + ABS( H( K, K-1 ) )
                     IF( K.GE.KTOP+2 )
     $                  TST1 = TST1 + ABS( H( K, K-2 ) )
                     IF( K.GE.KTOP+3 )
     $                  TST1 = TST1 + ABS( H( K, K-3 ) )
                     IF( K.LE.KBOT-2 )
     $                  TST1 = TST1 + ABS( H( K+2, K+1 ) )
                     IF( K.LE.KBOT-3 )
     $                  TST1 = TST1 + ABS( H( K+3, K+1 ) )
                     IF( K.LE.KBOT-4 )
     $                  TST1 = TST1 + ABS( H( K+4, K+1 ) )
                  END IF
                  IF( ABS( H( K+1, K ) ).LE.MAX( SMLNUM, ULP*TST1 ) )
     $                 THEN
                     H12 = MAX( ABS( H( K+1, K ) ), ABS( H( K, K+1 ) ) )
                     H21 = MIN( ABS( H( K+1, K ) ), ABS( H( K, K+1 ) ) )
                     H11 = MAX( ABS( H( K+1, K+1 ) ),
     $                     ABS( H( K, K )-H( K+1, K+1 ) ) )
                     H22 = MIN( ABS( H( K+1, K+1 ) ),
     $                     ABS( H( K, K )-H( K+1, K+1 ) ) )
                     SCL = H11 + H12
                     TST2 = H22*( H11 / SCL )
*
                     IF( TST2.EQ.ZERO .OR. H21*( H12 / SCL ).LE.
     $                   MAX( SMLNUM, ULP*TST2 ) )H( K+1, K ) = ZERO
                  END IF
               END IF
  130       CONTINUE
*
*           ==== Fill in the last row of each bulge. ====
*
            MEND = MIN( NBMPS, ( KBOT-KRCOL-1 ) / 3 )
            DO 140 M = MTOP, MEND
               K = KRCOL + 3*( M-1 )
               REFSUM = V( 1, M )*V( 3, M )*H( K+4, K+3 )
               H( K+4, K+1 ) = -REFSUM
               H( K+4, K+2 ) = -REFSUM*V( 2, M )
               H( K+4, K+3 ) = H( K+4, K+3 ) - REFSUM*V( 3, M )
  140       CONTINUE
*
*           ==== End of near-the-diagonal bulge chase. ====
*
  150    CONTINUE
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
               DO 160 JCOL = MIN( NDCOL, KBOT ) + 1, JBOT, NH
                  JLEN = MIN( NH, JBOT-JCOL+1 )
                  CALL DGEMM( 'C', 'N', NU, JLEN, NU, ONE, U( K1, K1 ),
     $                        LDU, H( INCOL+K1, JCOL ), LDH, ZERO, WH,
     $                        LDWH )
                  CALL DLACPY( 'ALL', NU, JLEN, WH, LDWH,
     $                         H( INCOL+K1, JCOL ), LDH )
  160          CONTINUE
*
*              ==== Vertical multiply ====
*
               DO 170 JROW = JTOP, MAX( KTOP, INCOL ) - 1, NV
                  JLEN = MIN( NV, MAX( KTOP, INCOL )-JROW )
                  CALL DGEMM( 'N', 'N', JLEN, NU, NU, ONE,
     $                        H( JROW, INCOL+K1 ), LDH, U( K1, K1 ),
     $                        LDU, ZERO, WV, LDWV )
                  CALL DLACPY( 'ALL', JLEN, NU, WV, LDWV,
     $                         H( JROW, INCOL+K1 ), LDH )
  170          CONTINUE
*
*              ==== Z multiply (also vertical) ====
*
               IF( WANTZ ) THEN
                  DO 180 JROW = ILOZ, IHIZ, NV
                     JLEN = MIN( NV, IHIZ-JROW+1 )
                     CALL DGEMM( 'N', 'N', JLEN, NU, NU, ONE,
     $                           Z( JROW, INCOL+K1 ), LDZ, U( K1, K1 ),
     $                           LDU, ZERO, WV, LDWV )
                     CALL DLACPY( 'ALL', JLEN, NU, WV, LDWV,
     $                            Z( JROW, INCOL+K1 ), LDZ )
  180             CONTINUE
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
               DO 190 JCOL = MIN( NDCOL, KBOT ) + 1, JBOT, NH
                  JLEN = MIN( NH, JBOT-JCOL+1 )
*
*                 ==== Copy bottom of H to top+KZS of scratch ====
*                  (The first KZS rows get multiplied by zero.) ====
*
                  CALL DLACPY( 'ALL', KNZ, JLEN, H( INCOL+1+J2, JCOL ),
     $                         LDH, WH( KZS+1, 1 ), LDWH )
*
*                 ==== Multiply by U21**T ====
*
                  CALL DLASET( 'ALL', KZS, JLEN, ZERO, ZERO, WH, LDWH )
                  CALL DTRMM( 'L', 'U', 'C', 'N', KNZ, JLEN, ONE,
     $                        U( J2+1, 1+KZS ), LDU, WH( KZS+1, 1 ),
     $                        LDWH )
*
*                 ==== Multiply top of H by U11**T ====
*
                  CALL DGEMM( 'C', 'N', I2, JLEN, J2, ONE, U, LDU,
     $                        H( INCOL+1, JCOL ), LDH, ONE, WH, LDWH )
*
*                 ==== Copy top of H to bottom of WH ====
*
                  CALL DLACPY( 'ALL', J2, JLEN, H( INCOL+1, JCOL ), LDH,
     $                         WH( I2+1, 1 ), LDWH )
*
*                 ==== Multiply by U21**T ====
*
                  CALL DTRMM( 'L', 'L', 'C', 'N', J2, JLEN, ONE,
     $                        U( 1, I2+1 ), LDU, WH( I2+1, 1 ), LDWH )
*
*                 ==== Multiply by U22 ====
*
                  CALL DGEMM( 'C', 'N', I4-I2, JLEN, J4-J2, ONE,
     $                        U( J2+1, I2+1 ), LDU,
     $                        H( INCOL+1+J2, JCOL ), LDH, ONE,
     $                        WH( I2+1, 1 ), LDWH )
*
*                 ==== Copy it back ====
*
                  CALL DLACPY( 'ALL', KDU, JLEN, WH, LDWH,
     $                         H( INCOL+1, JCOL ), LDH )
  190          CONTINUE
*
*              ==== Vertical multiply ====
*
               DO 200 JROW = JTOP, MAX( INCOL, KTOP ) - 1, NV
                  JLEN = MIN( NV, MAX( INCOL, KTOP )-JROW )
*
*                 ==== Copy right of H to scratch (the first KZS
*                 .    columns get multiplied by zero) ====
*
                  CALL DLACPY( 'ALL', JLEN, KNZ, H( JROW, INCOL+1+J2 ),
     $                         LDH, WV( 1, 1+KZS ), LDWV )
*
*                 ==== Multiply by U21 ====
*
                  CALL DLASET( 'ALL', JLEN, KZS, ZERO, ZERO, WV, LDWV )
                  CALL DTRMM( 'R', 'U', 'N', 'N', JLEN, KNZ, ONE,
     $                        U( J2+1, 1+KZS ), LDU, WV( 1, 1+KZS ),
     $                        LDWV )
*
*                 ==== Multiply by U11 ====
*
                  CALL DGEMM( 'N', 'N', JLEN, I2, J2, ONE,
     $                        H( JROW, INCOL+1 ), LDH, U, LDU, ONE, WV,
     $                        LDWV )
*
*                 ==== Copy left of H to right of scratch ====
*
                  CALL DLACPY( 'ALL', JLEN, J2, H( JROW, INCOL+1 ), LDH,
     $                         WV( 1, 1+I2 ), LDWV )
*
*                 ==== Multiply by U21 ====
*
                  CALL DTRMM( 'R', 'L', 'N', 'N', JLEN, I4-I2, ONE,
     $                        U( 1, I2+1 ), LDU, WV( 1, 1+I2 ), LDWV )
*
*                 ==== Multiply by U22 ====
*
                  CALL DGEMM( 'N', 'N', JLEN, I4-I2, J4-J2, ONE,
     $                        H( JROW, INCOL+1+J2 ), LDH,
     $                        U( J2+1, I2+1 ), LDU, ONE, WV( 1, 1+I2 ),
     $                        LDWV )
*
*                 ==== Copy it back ====
*
                  CALL DLACPY( 'ALL', JLEN, KDU, WV, LDWV,
     $                         H( JROW, INCOL+1 ), LDH )
  200          CONTINUE
*
*              ==== Multiply Z (also vertical) ====
*
               IF( WANTZ ) THEN
                  DO 210 JROW = ILOZ, IHIZ, NV
                     JLEN = MIN( NV, IHIZ-JROW+1 )
*
*                    ==== Copy right of Z to left of scratch (first
*                    .     KZS columns get multiplied by zero) ====
*
                     CALL DLACPY( 'ALL', JLEN, KNZ,
     $                            Z( JROW, INCOL+1+J2 ), LDZ,
     $                            WV( 1, 1+KZS ), LDWV )
*
*                    ==== Multiply by U12 ====
*
                     CALL DLASET( 'ALL', JLEN, KZS, ZERO, ZERO, WV,
     $                            LDWV )
                     CALL DTRMM( 'R', 'U', 'N', 'N', JLEN, KNZ, ONE,
     $                           U( J2+1, 1+KZS ), LDU, WV( 1, 1+KZS ),
     $                           LDWV )
*
*                    ==== Multiply by U11 ====
*
                     CALL DGEMM( 'N', 'N', JLEN, I2, J2, ONE,
     $                           Z( JROW, INCOL+1 ), LDZ, U, LDU, ONE,
     $                           WV, LDWV )
*
*                    ==== Copy left of Z to right of scratch ====
*
                     CALL DLACPY( 'ALL', JLEN, J2, Z( JROW, INCOL+1 ),
     $                            LDZ, WV( 1, 1+I2 ), LDWV )
*
*                    ==== Multiply by U21 ====
*
                     CALL DTRMM( 'R', 'L', 'N', 'N', JLEN, I4-I2, ONE,
     $                           U( 1, I2+1 ), LDU, WV( 1, 1+I2 ),
     $                           LDWV )
*
*                    ==== Multiply by U22 ====
*
                     CALL DGEMM( 'N', 'N', JLEN, I4-I2, J4-J2, ONE,
     $                           Z( JROW, INCOL+1+J2 ), LDZ,
     $                           U( J2+1, I2+1 ), LDU, ONE,
     $                           WV( 1, 1+I2 ), LDWV )
*
*                    ==== Copy the result back to Z ====
*
                     CALL DLACPY( 'ALL', JLEN, KDU, WV, LDWV,
     $                            Z( JROW, INCOL+1 ), LDZ )
  210             CONTINUE
               END IF
            END IF
         END IF
  220 CONTINUE
*
*     ==== End of DLAQR5 ====
*
      END
