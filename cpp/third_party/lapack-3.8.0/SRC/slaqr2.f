*> \brief \b SLAQR2 performs the orthogonal similarity transformation of a Hessenberg matrix to detect and deflate fully converged eigenvalues from a trailing principal submatrix (aggressive early deflation).
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLAQR2 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slaqr2.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slaqr2.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slaqr2.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLAQR2( WANTT, WANTZ, N, KTOP, KBOT, NW, H, LDH, ILOZ,
*                          IHIZ, Z, LDZ, NS, ND, SR, SI, V, LDV, NH, T,
*                          LDT, NV, WV, LDWV, WORK, LWORK )
*
*       .. Scalar Arguments ..
*       INTEGER            IHIZ, ILOZ, KBOT, KTOP, LDH, LDT, LDV, LDWV,
*      $                   LDZ, LWORK, N, ND, NH, NS, NV, NW
*       LOGICAL            WANTT, WANTZ
*       ..
*       .. Array Arguments ..
*       REAL               H( LDH, * ), SI( * ), SR( * ), T( LDT, * ),
*      $                   V( LDV, * ), WORK( * ), WV( LDWV, * ),
*      $                   Z( LDZ, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    SLAQR2 is identical to SLAQR3 except that it avoids
*>    recursion by calling SLAHQR instead of SLAQR4.
*>
*>    Aggressive early deflation:
*>
*>    This subroutine accepts as input an upper Hessenberg matrix
*>    H and performs an orthogonal similarity transformation
*>    designed to detect and deflate fully converged eigenvalues from
*>    a trailing principal submatrix.  On output H has been over-
*>    written by a new Hessenberg matrix that is a perturbation of
*>    an orthogonal similarity transformation of H.  It is to be
*>    hoped that the final version of H has many zero subdiagonal
*>    entries.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] WANTT
*> \verbatim
*>          WANTT is LOGICAL
*>          If .TRUE., then the Hessenberg matrix H is fully updated
*>          so that the quasi-triangular Schur factor may be
*>          computed (in cooperation with the calling subroutine).
*>          If .FALSE., then only enough of H is updated to preserve
*>          the eigenvalues.
*> \endverbatim
*>
*> \param[in] WANTZ
*> \verbatim
*>          WANTZ is LOGICAL
*>          If .TRUE., then the orthogonal matrix Z is updated so
*>          so that the orthogonal Schur factor may be computed
*>          (in cooperation with the calling subroutine).
*>          If .FALSE., then Z is not referenced.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix H and (if WANTZ is .TRUE.) the
*>          order of the orthogonal matrix Z.
*> \endverbatim
*>
*> \param[in] KTOP
*> \verbatim
*>          KTOP is INTEGER
*>          It is assumed that either KTOP = 1 or H(KTOP,KTOP-1)=0.
*>          KBOT and KTOP together determine an isolated block
*>          along the diagonal of the Hessenberg matrix.
*> \endverbatim
*>
*> \param[in] KBOT
*> \verbatim
*>          KBOT is INTEGER
*>          It is assumed without a check that either
*>          KBOT = N or H(KBOT+1,KBOT)=0.  KBOT and KTOP together
*>          determine an isolated block along the diagonal of the
*>          Hessenberg matrix.
*> \endverbatim
*>
*> \param[in] NW
*> \verbatim
*>          NW is INTEGER
*>          Deflation window size.  1 .LE. NW .LE. (KBOT-KTOP+1).
*> \endverbatim
*>
*> \param[in,out] H
*> \verbatim
*>          H is REAL array, dimension (LDH,N)
*>          On input the initial N-by-N section of H stores the
*>          Hessenberg matrix undergoing aggressive early deflation.
*>          On output H has been transformed by an orthogonal
*>          similarity transformation, perturbed, and the returned
*>          to Hessenberg form that (it is to be hoped) has some
*>          zero subdiagonal entries.
*> \endverbatim
*>
*> \param[in] LDH
*> \verbatim
*>          LDH is INTEGER
*>          Leading dimension of H just as declared in the calling
*>          subroutine.  N .LE. LDH
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
*>          Specify the rows of Z to which transformations must be
*>          applied if WANTZ is .TRUE.. 1 .LE. ILOZ .LE. IHIZ .LE. N.
*> \endverbatim
*>
*> \param[in,out] Z
*> \verbatim
*>          Z is REAL array, dimension (LDZ,N)
*>          IF WANTZ is .TRUE., then on output, the orthogonal
*>          similarity transformation mentioned above has been
*>          accumulated into Z(ILOZ:IHIZ,ILOZ:IHIZ) from the right.
*>          If WANTZ is .FALSE., then Z is unreferenced.
*> \endverbatim
*>
*> \param[in] LDZ
*> \verbatim
*>          LDZ is INTEGER
*>          The leading dimension of Z just as declared in the
*>          calling subroutine.  1 .LE. LDZ.
*> \endverbatim
*>
*> \param[out] NS
*> \verbatim
*>          NS is INTEGER
*>          The number of unconverged (ie approximate) eigenvalues
*>          returned in SR and SI that may be used as shifts by the
*>          calling subroutine.
*> \endverbatim
*>
*> \param[out] ND
*> \verbatim
*>          ND is INTEGER
*>          The number of converged eigenvalues uncovered by this
*>          subroutine.
*> \endverbatim
*>
*> \param[out] SR
*> \verbatim
*>          SR is REAL array, dimension (KBOT)
*> \endverbatim
*>
*> \param[out] SI
*> \verbatim
*>          SI is REAL array, dimension (KBOT)
*>          On output, the real and imaginary parts of approximate
*>          eigenvalues that may be used for shifts are stored in
*>          SR(KBOT-ND-NS+1) through SR(KBOT-ND) and
*>          SI(KBOT-ND-NS+1) through SI(KBOT-ND), respectively.
*>          The real and imaginary parts of converged eigenvalues
*>          are stored in SR(KBOT-ND+1) through SR(KBOT) and
*>          SI(KBOT-ND+1) through SI(KBOT), respectively.
*> \endverbatim
*>
*> \param[out] V
*> \verbatim
*>          V is REAL array, dimension (LDV,NW)
*>          An NW-by-NW work array.
*> \endverbatim
*>
*> \param[in] LDV
*> \verbatim
*>          LDV is INTEGER
*>          The leading dimension of V just as declared in the
*>          calling subroutine.  NW .LE. LDV
*> \endverbatim
*>
*> \param[in] NH
*> \verbatim
*>          NH is INTEGER
*>          The number of columns of T.  NH.GE.NW.
*> \endverbatim
*>
*> \param[out] T
*> \verbatim
*>          T is REAL array, dimension (LDT,NW)
*> \endverbatim
*>
*> \param[in] LDT
*> \verbatim
*>          LDT is INTEGER
*>          The leading dimension of T just as declared in the
*>          calling subroutine.  NW .LE. LDT
*> \endverbatim
*>
*> \param[in] NV
*> \verbatim
*>          NV is INTEGER
*>          The number of rows of work array WV available for
*>          workspace.  NV.GE.NW.
*> \endverbatim
*>
*> \param[out] WV
*> \verbatim
*>          WV is REAL array, dimension (LDWV,NW)
*> \endverbatim
*>
*> \param[in] LDWV
*> \verbatim
*>          LDWV is INTEGER
*>          The leading dimension of W just as declared in the
*>          calling subroutine.  NW .LE. LDV
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (LWORK)
*>          On exit, WORK(1) is set to an estimate of the optimal value
*>          of LWORK for the given values of N, NW, KTOP and KBOT.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the work array WORK.  LWORK = 2*NW
*>          suffices, but greater efficiency may result from larger
*>          values of LWORK.
*>
*>          If LWORK = -1, then a workspace query is assumed; SLAQR2
*>          only estimates the optimal workspace size for the given
*>          values of N, NW, KTOP and KBOT.  The estimate is returned
*>          in WORK(1).  No error message related to LWORK is issued
*>          by XERBLA.  Neither H nor Z are accessed.
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
*> \ingroup realOTHERauxiliary
*
*> \par Contributors:
*  ==================
*>
*>       Karen Braman and Ralph Byers, Department of Mathematics,
*>       University of Kansas, USA
*>
*  =====================================================================
      SUBROUTINE SLAQR2( WANTT, WANTZ, N, KTOP, KBOT, NW, H, LDH, ILOZ,
     $                   IHIZ, Z, LDZ, NS, ND, SR, SI, V, LDV, NH, T,
     $                   LDT, NV, WV, LDWV, WORK, LWORK )
*
*  -- LAPACK auxiliary routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      INTEGER            IHIZ, ILOZ, KBOT, KTOP, LDH, LDT, LDV, LDWV,
     $                   LDZ, LWORK, N, ND, NH, NS, NV, NW
      LOGICAL            WANTT, WANTZ
*     ..
*     .. Array Arguments ..
      REAL               H( LDH, * ), SI( * ), SR( * ), T( LDT, * ),
     $                   V( LDV, * ), WORK( * ), WV( LDWV, * ),
     $                   Z( LDZ, * )
*     ..
*
*  ================================================================
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0e0, ONE = 1.0e0 )
*     ..
*     .. Local Scalars ..
      REAL               AA, BB, BETA, CC, CS, DD, EVI, EVK, FOO, S,
     $                   SAFMAX, SAFMIN, SMLNUM, SN, TAU, ULP
      INTEGER            I, IFST, ILST, INFO, INFQR, J, JW, K, KCOL,
     $                   KEND, KLN, KROW, KWTOP, LTOP, LWK1, LWK2,
     $                   LWKOPT
      LOGICAL            BULGE, SORTED
*     ..
*     .. External Functions ..
      REAL               SLAMCH
      EXTERNAL           SLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           SCOPY, SGEHRD, SGEMM, SLABAD, SLACPY, SLAHQR,
     $                   SLANV2, SLARF, SLARFG, SLASET, SORMHR, STREXC
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, INT, MAX, MIN, REAL, SQRT
*     ..
*     .. Executable Statements ..
*
*     ==== Estimate optimal workspace. ====
*
      JW = MIN( NW, KBOT-KTOP+1 )
      IF( JW.LE.2 ) THEN
         LWKOPT = 1
      ELSE
*
*        ==== Workspace query call to SGEHRD ====
*
         CALL SGEHRD( JW, 1, JW-1, T, LDT, WORK, WORK, -1, INFO )
         LWK1 = INT( WORK( 1 ) )
*
*        ==== Workspace query call to SORMHR ====
*
         CALL SORMHR( 'R', 'N', JW, JW, 1, JW-1, T, LDT, WORK, V, LDV,
     $                WORK, -1, INFO )
         LWK2 = INT( WORK( 1 ) )
*
*        ==== Optimal workspace ====
*
         LWKOPT = JW + MAX( LWK1, LWK2 )
      END IF
*
*     ==== Quick return in case of workspace query. ====
*
      IF( LWORK.EQ.-1 ) THEN
         WORK( 1 ) = REAL( LWKOPT )
         RETURN
      END IF
*
*     ==== Nothing to do ...
*     ... for an empty active block ... ====
      NS = 0
      ND = 0
      WORK( 1 ) = ONE
      IF( KTOP.GT.KBOT )
     $   RETURN
*     ... nor for an empty deflation window. ====
      IF( NW.LT.1 )
     $   RETURN
*
*     ==== Machine constants ====
*
      SAFMIN = SLAMCH( 'SAFE MINIMUM' )
      SAFMAX = ONE / SAFMIN
      CALL SLABAD( SAFMIN, SAFMAX )
      ULP = SLAMCH( 'PRECISION' )
      SMLNUM = SAFMIN*( REAL( N ) / ULP )
*
*     ==== Setup deflation window ====
*
      JW = MIN( NW, KBOT-KTOP+1 )
      KWTOP = KBOT - JW + 1
      IF( KWTOP.EQ.KTOP ) THEN
         S = ZERO
      ELSE
         S = H( KWTOP, KWTOP-1 )
      END IF
*
      IF( KBOT.EQ.KWTOP ) THEN
*
*        ==== 1-by-1 deflation window: not much to do ====
*
         SR( KWTOP ) = H( KWTOP, KWTOP )
         SI( KWTOP ) = ZERO
         NS = 1
         ND = 0
         IF( ABS( S ).LE.MAX( SMLNUM, ULP*ABS( H( KWTOP, KWTOP ) ) ) )
     $        THEN
            NS = 0
            ND = 1
            IF( KWTOP.GT.KTOP )
     $         H( KWTOP, KWTOP-1 ) = ZERO
         END IF
         WORK( 1 ) = ONE
         RETURN
      END IF
*
*     ==== Convert to spike-triangular form.  (In case of a
*     .    rare QR failure, this routine continues to do
*     .    aggressive early deflation using that part of
*     .    the deflation window that converged using INFQR
*     .    here and there to keep track.) ====
*
      CALL SLACPY( 'U', JW, JW, H( KWTOP, KWTOP ), LDH, T, LDT )
      CALL SCOPY( JW-1, H( KWTOP+1, KWTOP ), LDH+1, T( 2, 1 ), LDT+1 )
*
      CALL SLASET( 'A', JW, JW, ZERO, ONE, V, LDV )
      CALL SLAHQR( .true., .true., JW, 1, JW, T, LDT, SR( KWTOP ),
     $             SI( KWTOP ), 1, JW, V, LDV, INFQR )
*
*     ==== STREXC needs a clean margin near the diagonal ====
*
      DO 10 J = 1, JW - 3
         T( J+2, J ) = ZERO
         T( J+3, J ) = ZERO
   10 CONTINUE
      IF( JW.GT.2 )
     $   T( JW, JW-2 ) = ZERO
*
*     ==== Deflation detection loop ====
*
      NS = JW
      ILST = INFQR + 1
   20 CONTINUE
      IF( ILST.LE.NS ) THEN
         IF( NS.EQ.1 ) THEN
            BULGE = .FALSE.
         ELSE
            BULGE = T( NS, NS-1 ).NE.ZERO
         END IF
*
*        ==== Small spike tip test for deflation ====
*
         IF( .NOT.BULGE ) THEN
*
*           ==== Real eigenvalue ====
*
            FOO = ABS( T( NS, NS ) )
            IF( FOO.EQ.ZERO )
     $         FOO = ABS( S )
            IF( ABS( S*V( 1, NS ) ).LE.MAX( SMLNUM, ULP*FOO ) ) THEN
*
*              ==== Deflatable ====
*
               NS = NS - 1
            ELSE
*
*              ==== Undeflatable.   Move it up out of the way.
*              .    (STREXC can not fail in this case.) ====
*
               IFST = NS
               CALL STREXC( 'V', JW, T, LDT, V, LDV, IFST, ILST, WORK,
     $                      INFO )
               ILST = ILST + 1
            END IF
         ELSE
*
*           ==== Complex conjugate pair ====
*
            FOO = ABS( T( NS, NS ) ) + SQRT( ABS( T( NS, NS-1 ) ) )*
     $            SQRT( ABS( T( NS-1, NS ) ) )
            IF( FOO.EQ.ZERO )
     $         FOO = ABS( S )
            IF( MAX( ABS( S*V( 1, NS ) ), ABS( S*V( 1, NS-1 ) ) ).LE.
     $          MAX( SMLNUM, ULP*FOO ) ) THEN
*
*              ==== Deflatable ====
*
               NS = NS - 2
            ELSE
*
*              ==== Undeflatable. Move them up out of the way.
*              .    Fortunately, STREXC does the right thing with
*              .    ILST in case of a rare exchange failure. ====
*
               IFST = NS
               CALL STREXC( 'V', JW, T, LDT, V, LDV, IFST, ILST, WORK,
     $                      INFO )
               ILST = ILST + 2
            END IF
         END IF
*
*        ==== End deflation detection loop ====
*
         GO TO 20
      END IF
*
*        ==== Return to Hessenberg form ====
*
      IF( NS.EQ.0 )
     $   S = ZERO
*
      IF( NS.LT.JW ) THEN
*
*        ==== sorting diagonal blocks of T improves accuracy for
*        .    graded matrices.  Bubble sort deals well with
*        .    exchange failures. ====
*
         SORTED = .false.
         I = NS + 1
   30    CONTINUE
         IF( SORTED )
     $      GO TO 50
         SORTED = .true.
*
         KEND = I - 1
         I = INFQR + 1
         IF( I.EQ.NS ) THEN
            K = I + 1
         ELSE IF( T( I+1, I ).EQ.ZERO ) THEN
            K = I + 1
         ELSE
            K = I + 2
         END IF
   40    CONTINUE
         IF( K.LE.KEND ) THEN
            IF( K.EQ.I+1 ) THEN
               EVI = ABS( T( I, I ) )
            ELSE
               EVI = ABS( T( I, I ) ) + SQRT( ABS( T( I+1, I ) ) )*
     $               SQRT( ABS( T( I, I+1 ) ) )
            END IF
*
            IF( K.EQ.KEND ) THEN
               EVK = ABS( T( K, K ) )
            ELSE IF( T( K+1, K ).EQ.ZERO ) THEN
               EVK = ABS( T( K, K ) )
            ELSE
               EVK = ABS( T( K, K ) ) + SQRT( ABS( T( K+1, K ) ) )*
     $               SQRT( ABS( T( K, K+1 ) ) )
            END IF
*
            IF( EVI.GE.EVK ) THEN
               I = K
            ELSE
               SORTED = .false.
               IFST = I
               ILST = K
               CALL STREXC( 'V', JW, T, LDT, V, LDV, IFST, ILST, WORK,
     $                      INFO )
               IF( INFO.EQ.0 ) THEN
                  I = ILST
               ELSE
                  I = K
               END IF
            END IF
            IF( I.EQ.KEND ) THEN
               K = I + 1
            ELSE IF( T( I+1, I ).EQ.ZERO ) THEN
               K = I + 1
            ELSE
               K = I + 2
            END IF
            GO TO 40
         END IF
         GO TO 30
   50    CONTINUE
      END IF
*
*     ==== Restore shift/eigenvalue array from T ====
*
      I = JW
   60 CONTINUE
      IF( I.GE.INFQR+1 ) THEN
         IF( I.EQ.INFQR+1 ) THEN
            SR( KWTOP+I-1 ) = T( I, I )
            SI( KWTOP+I-1 ) = ZERO
            I = I - 1
         ELSE IF( T( I, I-1 ).EQ.ZERO ) THEN
            SR( KWTOP+I-1 ) = T( I, I )
            SI( KWTOP+I-1 ) = ZERO
            I = I - 1
         ELSE
            AA = T( I-1, I-1 )
            CC = T( I, I-1 )
            BB = T( I-1, I )
            DD = T( I, I )
            CALL SLANV2( AA, BB, CC, DD, SR( KWTOP+I-2 ),
     $                   SI( KWTOP+I-2 ), SR( KWTOP+I-1 ),
     $                   SI( KWTOP+I-1 ), CS, SN )
            I = I - 2
         END IF
         GO TO 60
      END IF
*
      IF( NS.LT.JW .OR. S.EQ.ZERO ) THEN
         IF( NS.GT.1 .AND. S.NE.ZERO ) THEN
*
*           ==== Reflect spike back into lower triangle ====
*
            CALL SCOPY( NS, V, LDV, WORK, 1 )
            BETA = WORK( 1 )
            CALL SLARFG( NS, BETA, WORK( 2 ), 1, TAU )
            WORK( 1 ) = ONE
*
            CALL SLASET( 'L', JW-2, JW-2, ZERO, ZERO, T( 3, 1 ), LDT )
*
            CALL SLARF( 'L', NS, JW, WORK, 1, TAU, T, LDT,
     $                  WORK( JW+1 ) )
            CALL SLARF( 'R', NS, NS, WORK, 1, TAU, T, LDT,
     $                  WORK( JW+1 ) )
            CALL SLARF( 'R', JW, NS, WORK, 1, TAU, V, LDV,
     $                  WORK( JW+1 ) )
*
            CALL SGEHRD( JW, 1, NS, T, LDT, WORK, WORK( JW+1 ),
     $                   LWORK-JW, INFO )
         END IF
*
*        ==== Copy updated reduced window into place ====
*
         IF( KWTOP.GT.1 )
     $      H( KWTOP, KWTOP-1 ) = S*V( 1, 1 )
         CALL SLACPY( 'U', JW, JW, T, LDT, H( KWTOP, KWTOP ), LDH )
         CALL SCOPY( JW-1, T( 2, 1 ), LDT+1, H( KWTOP+1, KWTOP ),
     $               LDH+1 )
*
*        ==== Accumulate orthogonal matrix in order update
*        .    H and Z, if requested.  ====
*
         IF( NS.GT.1 .AND. S.NE.ZERO )
     $      CALL SORMHR( 'R', 'N', JW, NS, 1, NS, T, LDT, WORK, V, LDV,
     $                   WORK( JW+1 ), LWORK-JW, INFO )
*
*        ==== Update vertical slab in H ====
*
         IF( WANTT ) THEN
            LTOP = 1
         ELSE
            LTOP = KTOP
         END IF
         DO 70 KROW = LTOP, KWTOP - 1, NV
            KLN = MIN( NV, KWTOP-KROW )
            CALL SGEMM( 'N', 'N', KLN, JW, JW, ONE, H( KROW, KWTOP ),
     $                  LDH, V, LDV, ZERO, WV, LDWV )
            CALL SLACPY( 'A', KLN, JW, WV, LDWV, H( KROW, KWTOP ), LDH )
   70    CONTINUE
*
*        ==== Update horizontal slab in H ====
*
         IF( WANTT ) THEN
            DO 80 KCOL = KBOT + 1, N, NH
               KLN = MIN( NH, N-KCOL+1 )
               CALL SGEMM( 'C', 'N', JW, KLN, JW, ONE, V, LDV,
     $                     H( KWTOP, KCOL ), LDH, ZERO, T, LDT )
               CALL SLACPY( 'A', JW, KLN, T, LDT, H( KWTOP, KCOL ),
     $                      LDH )
   80       CONTINUE
         END IF
*
*        ==== Update vertical slab in Z ====
*
         IF( WANTZ ) THEN
            DO 90 KROW = ILOZ, IHIZ, NV
               KLN = MIN( NV, IHIZ-KROW+1 )
               CALL SGEMM( 'N', 'N', KLN, JW, JW, ONE, Z( KROW, KWTOP ),
     $                     LDZ, V, LDV, ZERO, WV, LDWV )
               CALL SLACPY( 'A', KLN, JW, WV, LDWV, Z( KROW, KWTOP ),
     $                      LDZ )
   90       CONTINUE
         END IF
      END IF
*
*     ==== Return the number of deflations ... ====
*
      ND = JW - NS
*
*     ==== ... and the number of shifts. (Subtracting
*     .    INFQR from the spike length takes care
*     .    of the case of a rare QR failure while
*     .    calculating eigenvalues of the deflation
*     .    window.)  ====
*
      NS = NS - INFQR
*
*      ==== Return optimal workspace. ====
*
      WORK( 1 ) = REAL( LWKOPT )
*
*     ==== End of SLAQR2 ====
*
      END
