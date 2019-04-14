*> \brief \b SHSEIN
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SHSEIN + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/shsein.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/shsein.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/shsein.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SHSEIN( SIDE, EIGSRC, INITV, SELECT, N, H, LDH, WR, WI,
*                          VL, LDVL, VR, LDVR, MM, M, WORK, IFAILL,
*                          IFAILR, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          EIGSRC, INITV, SIDE
*       INTEGER            INFO, LDH, LDVL, LDVR, M, MM, N
*       ..
*       .. Array Arguments ..
*       LOGICAL            SELECT( * )
*       INTEGER            IFAILL( * ), IFAILR( * )
*       REAL               H( LDH, * ), VL( LDVL, * ), VR( LDVR, * ),
*      $                   WI( * ), WORK( * ), WR( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SHSEIN uses inverse iteration to find specified right and/or left
*> eigenvectors of a real upper Hessenberg matrix H.
*>
*> The right eigenvector x and the left eigenvector y of the matrix H
*> corresponding to an eigenvalue w are defined by:
*>
*>              H * x = w * x,     y**h * H = w * y**h
*>
*> where y**h denotes the conjugate transpose of the vector y.
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
*> \param[in] EIGSRC
*> \verbatim
*>          EIGSRC is CHARACTER*1
*>          Specifies the source of eigenvalues supplied in (WR,WI):
*>          = 'Q': the eigenvalues were found using SHSEQR; thus, if
*>                 H has zero subdiagonal elements, and so is
*>                 block-triangular, then the j-th eigenvalue can be
*>                 assumed to be an eigenvalue of the block containing
*>                 the j-th row/column.  This property allows SHSEIN to
*>                 perform inverse iteration on just one diagonal block.
*>          = 'N': no assumptions are made on the correspondence
*>                 between eigenvalues and diagonal blocks.  In this
*>                 case, SHSEIN must always perform inverse iteration
*>                 using the whole matrix H.
*> \endverbatim
*>
*> \param[in] INITV
*> \verbatim
*>          INITV is CHARACTER*1
*>          = 'N': no initial vectors are supplied;
*>          = 'U': user-supplied initial vectors are stored in the arrays
*>                 VL and/or VR.
*> \endverbatim
*>
*> \param[in,out] SELECT
*> \verbatim
*>          SELECT is LOGICAL array, dimension (N)
*>          Specifies the eigenvectors to be computed. To select the
*>          real eigenvector corresponding to a real eigenvalue WR(j),
*>          SELECT(j) must be set to .TRUE.. To select the complex
*>          eigenvector corresponding to a complex eigenvalue
*>          (WR(j),WI(j)), with complex conjugate (WR(j+1),WI(j+1)),
*>          either SELECT(j) or SELECT(j+1) or both must be set to
*>          .TRUE.; then on exit SELECT(j) is .TRUE. and SELECT(j+1) is
*>          .FALSE..
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix H.  N >= 0.
*> \endverbatim
*>
*> \param[in] H
*> \verbatim
*>          H is REAL array, dimension (LDH,N)
*>          The upper Hessenberg matrix H.
*>          If a NaN is detected in H, the routine will return with INFO=-6.
*> \endverbatim
*>
*> \param[in] LDH
*> \verbatim
*>          LDH is INTEGER
*>          The leading dimension of the array H.  LDH >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] WR
*> \verbatim
*>          WR is REAL array, dimension (N)
*> \endverbatim
*>
*> \param[in] WI
*> \verbatim
*>          WI is REAL array, dimension (N)
*>
*>          On entry, the real and imaginary parts of the eigenvalues of
*>          H; a complex conjugate pair of eigenvalues must be stored in
*>          consecutive elements of WR and WI.
*>          On exit, WR may have been altered since close eigenvalues
*>          are perturbed slightly in searching for independent
*>          eigenvectors.
*> \endverbatim
*>
*> \param[in,out] VL
*> \verbatim
*>          VL is REAL array, dimension (LDVL,MM)
*>          On entry, if INITV = 'U' and SIDE = 'L' or 'B', VL must
*>          contain starting vectors for the inverse iteration for the
*>          left eigenvectors; the starting vector for each eigenvector
*>          must be in the same column(s) in which the eigenvector will
*>          be stored.
*>          On exit, if SIDE = 'L' or 'B', the left eigenvectors
*>          specified by SELECT will be stored consecutively in the
*>          columns of VL, in the same order as their eigenvalues. A
*>          complex eigenvector corresponding to a complex eigenvalue is
*>          stored in two consecutive columns, the first holding the real
*>          part and the second the imaginary part.
*>          If SIDE = 'R', VL is not referenced.
*> \endverbatim
*>
*> \param[in] LDVL
*> \verbatim
*>          LDVL is INTEGER
*>          The leading dimension of the array VL.
*>          LDVL >= max(1,N) if SIDE = 'L' or 'B'; LDVL >= 1 otherwise.
*> \endverbatim
*>
*> \param[in,out] VR
*> \verbatim
*>          VR is REAL array, dimension (LDVR,MM)
*>          On entry, if INITV = 'U' and SIDE = 'R' or 'B', VR must
*>          contain starting vectors for the inverse iteration for the
*>          right eigenvectors; the starting vector for each eigenvector
*>          must be in the same column(s) in which the eigenvector will
*>          be stored.
*>          On exit, if SIDE = 'R' or 'B', the right eigenvectors
*>          specified by SELECT will be stored consecutively in the
*>          columns of VR, in the same order as their eigenvalues. A
*>          complex eigenvector corresponding to a complex eigenvalue is
*>          stored in two consecutive columns, the first holding the real
*>          part and the second the imaginary part.
*>          If SIDE = 'L', VR is not referenced.
*> \endverbatim
*>
*> \param[in] LDVR
*> \verbatim
*>          LDVR is INTEGER
*>          The leading dimension of the array VR.
*>          LDVR >= max(1,N) if SIDE = 'R' or 'B'; LDVR >= 1 otherwise.
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
*>          The number of columns in the arrays VL and/or VR required to
*>          store the eigenvectors; each selected real eigenvector
*>          occupies one column and each selected complex eigenvector
*>          occupies two columns.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension ((N+2)*N)
*> \endverbatim
*>
*> \param[out] IFAILL
*> \verbatim
*>          IFAILL is INTEGER array, dimension (MM)
*>          If SIDE = 'L' or 'B', IFAILL(i) = j > 0 if the left
*>          eigenvector in the i-th column of VL (corresponding to the
*>          eigenvalue w(j)) failed to converge; IFAILL(i) = 0 if the
*>          eigenvector converged satisfactorily. If the i-th and (i+1)th
*>          columns of VL hold a complex eigenvector, then IFAILL(i) and
*>          IFAILL(i+1) are set to the same value.
*>          If SIDE = 'R', IFAILL is not referenced.
*> \endverbatim
*>
*> \param[out] IFAILR
*> \verbatim
*>          IFAILR is INTEGER array, dimension (MM)
*>          If SIDE = 'R' or 'B', IFAILR(i) = j > 0 if the right
*>          eigenvector in the i-th column of VR (corresponding to the
*>          eigenvalue w(j)) failed to converge; IFAILR(i) = 0 if the
*>          eigenvector converged satisfactorily. If the i-th and (i+1)th
*>          columns of VR hold a complex eigenvector, then IFAILR(i) and
*>          IFAILR(i+1) are set to the same value.
*>          If SIDE = 'L', IFAILR is not referenced.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
*>          > 0:  if INFO = i, i is the number of eigenvectors which
*>                failed to converge; see IFAILL and IFAILR for further
*>                details.
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
*> \ingroup realOTHERcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  Each eigenvector is normalized so that the element of largest
*>  magnitude has magnitude 1; here the magnitude of a complex number
*>  (x,y) is taken to be |x|+|y|.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE SHSEIN( SIDE, EIGSRC, INITV, SELECT, N, H, LDH, WR, WI,
     $                   VL, LDVL, VR, LDVR, MM, M, WORK, IFAILL,
     $                   IFAILR, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          EIGSRC, INITV, SIDE
      INTEGER            INFO, LDH, LDVL, LDVR, M, MM, N
*     ..
*     .. Array Arguments ..
      LOGICAL            SELECT( * )
      INTEGER            IFAILL( * ), IFAILR( * )
      REAL               H( LDH, * ), VL( LDVL, * ), VR( LDVR, * ),
     $                   WI( * ), WORK( * ), WR( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            BOTHV, FROMQR, LEFTV, NOINIT, PAIR, RIGHTV
      INTEGER            I, IINFO, K, KL, KLN, KR, KSI, KSR, LDWORK
      REAL               BIGNUM, EPS3, HNORM, SMLNUM, ULP, UNFL, WKI,
     $                   WKR
*     ..
*     .. External Functions ..
      LOGICAL            LSAME, SISNAN
      REAL               SLAMCH, SLANHS
      EXTERNAL           LSAME, SLAMCH, SLANHS, SISNAN
*     ..
*     .. External Subroutines ..
      EXTERNAL           SLAEIN, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX
*     ..
*     .. Executable Statements ..
*
*     Decode and test the input parameters.
*
      BOTHV = LSAME( SIDE, 'B' )
      RIGHTV = LSAME( SIDE, 'R' ) .OR. BOTHV
      LEFTV = LSAME( SIDE, 'L' ) .OR. BOTHV
*
      FROMQR = LSAME( EIGSRC, 'Q' )
*
      NOINIT = LSAME( INITV, 'N' )
*
*     Set M to the number of columns required to store the selected
*     eigenvectors, and standardize the array SELECT.
*
      M = 0
      PAIR = .FALSE.
      DO 10 K = 1, N
         IF( PAIR ) THEN
            PAIR = .FALSE.
            SELECT( K ) = .FALSE.
         ELSE
            IF( WI( K ).EQ.ZERO ) THEN
               IF( SELECT( K ) )
     $            M = M + 1
            ELSE
               PAIR = .TRUE.
               IF( SELECT( K ) .OR. SELECT( K+1 ) ) THEN
                  SELECT( K ) = .TRUE.
                  M = M + 2
               END IF
            END IF
         END IF
   10 CONTINUE
*
      INFO = 0
      IF( .NOT.RIGHTV .AND. .NOT.LEFTV ) THEN
         INFO = -1
      ELSE IF( .NOT.FROMQR .AND. .NOT.LSAME( EIGSRC, 'N' ) ) THEN
         INFO = -2
      ELSE IF( .NOT.NOINIT .AND. .NOT.LSAME( INITV, 'U' ) ) THEN
         INFO = -3
      ELSE IF( N.LT.0 ) THEN
         INFO = -5
      ELSE IF( LDH.LT.MAX( 1, N ) ) THEN
         INFO = -7
      ELSE IF( LDVL.LT.1 .OR. ( LEFTV .AND. LDVL.LT.N ) ) THEN
         INFO = -11
      ELSE IF( LDVR.LT.1 .OR. ( RIGHTV .AND. LDVR.LT.N ) ) THEN
         INFO = -13
      ELSE IF( MM.LT.M ) THEN
         INFO = -14
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SHSEIN', -INFO )
         RETURN
      END IF
*
*     Quick return if possible.
*
      IF( N.EQ.0 )
     $   RETURN
*
*     Set machine-dependent constants.
*
      UNFL = SLAMCH( 'Safe minimum' )
      ULP = SLAMCH( 'Precision' )
      SMLNUM = UNFL*( N / ULP )
      BIGNUM = ( ONE-ULP ) / SMLNUM
*
      LDWORK = N + 1
*
      KL = 1
      KLN = 0
      IF( FROMQR ) THEN
         KR = 0
      ELSE
         KR = N
      END IF
      KSR = 1
*
      DO 120 K = 1, N
         IF( SELECT( K ) ) THEN
*
*           Compute eigenvector(s) corresponding to W(K).
*
            IF( FROMQR ) THEN
*
*              If affiliation of eigenvalues is known, check whether
*              the matrix splits.
*
*              Determine KL and KR such that 1 <= KL <= K <= KR <= N
*              and H(KL,KL-1) and H(KR+1,KR) are zero (or KL = 1 or
*              KR = N).
*
*              Then inverse iteration can be performed with the
*              submatrix H(KL:N,KL:N) for a left eigenvector, and with
*              the submatrix H(1:KR,1:KR) for a right eigenvector.
*
               DO 20 I = K, KL + 1, -1
                  IF( H( I, I-1 ).EQ.ZERO )
     $               GO TO 30
   20          CONTINUE
   30          CONTINUE
               KL = I
               IF( K.GT.KR ) THEN
                  DO 40 I = K, N - 1
                     IF( H( I+1, I ).EQ.ZERO )
     $                  GO TO 50
   40             CONTINUE
   50             CONTINUE
                  KR = I
               END IF
            END IF
*
            IF( KL.NE.KLN ) THEN
               KLN = KL
*
*              Compute infinity-norm of submatrix H(KL:KR,KL:KR) if it
*              has not ben computed before.
*
               HNORM = SLANHS( 'I', KR-KL+1, H( KL, KL ), LDH, WORK )
               IF( SISNAN( HNORM ) ) THEN
                  INFO = -6
                  RETURN
               ELSE IF( HNORM.GT.ZERO ) THEN
                  EPS3 = HNORM*ULP
               ELSE
                  EPS3 = SMLNUM
               END IF
            END IF
*
*           Perturb eigenvalue if it is close to any previous
*           selected eigenvalues affiliated to the submatrix
*           H(KL:KR,KL:KR). Close roots are modified by EPS3.
*
            WKR = WR( K )
            WKI = WI( K )
   60       CONTINUE
            DO 70 I = K - 1, KL, -1
               IF( SELECT( I ) .AND. ABS( WR( I )-WKR )+
     $             ABS( WI( I )-WKI ).LT.EPS3 ) THEN
                  WKR = WKR + EPS3
                  GO TO 60
               END IF
   70       CONTINUE
            WR( K ) = WKR
*
            PAIR = WKI.NE.ZERO
            IF( PAIR ) THEN
               KSI = KSR + 1
            ELSE
               KSI = KSR
            END IF
            IF( LEFTV ) THEN
*
*              Compute left eigenvector.
*
               CALL SLAEIN( .FALSE., NOINIT, N-KL+1, H( KL, KL ), LDH,
     $                      WKR, WKI, VL( KL, KSR ), VL( KL, KSI ),
     $                      WORK, LDWORK, WORK( N*N+N+1 ), EPS3, SMLNUM,
     $                      BIGNUM, IINFO )
               IF( IINFO.GT.0 ) THEN
                  IF( PAIR ) THEN
                     INFO = INFO + 2
                  ELSE
                     INFO = INFO + 1
                  END IF
                  IFAILL( KSR ) = K
                  IFAILL( KSI ) = K
               ELSE
                  IFAILL( KSR ) = 0
                  IFAILL( KSI ) = 0
               END IF
               DO 80 I = 1, KL - 1
                  VL( I, KSR ) = ZERO
   80          CONTINUE
               IF( PAIR ) THEN
                  DO 90 I = 1, KL - 1
                     VL( I, KSI ) = ZERO
   90             CONTINUE
               END IF
            END IF
            IF( RIGHTV ) THEN
*
*              Compute right eigenvector.
*
               CALL SLAEIN( .TRUE., NOINIT, KR, H, LDH, WKR, WKI,
     $                      VR( 1, KSR ), VR( 1, KSI ), WORK, LDWORK,
     $                      WORK( N*N+N+1 ), EPS3, SMLNUM, BIGNUM,
     $                      IINFO )
               IF( IINFO.GT.0 ) THEN
                  IF( PAIR ) THEN
                     INFO = INFO + 2
                  ELSE
                     INFO = INFO + 1
                  END IF
                  IFAILR( KSR ) = K
                  IFAILR( KSI ) = K
               ELSE
                  IFAILR( KSR ) = 0
                  IFAILR( KSI ) = 0
               END IF
               DO 100 I = KR + 1, N
                  VR( I, KSR ) = ZERO
  100          CONTINUE
               IF( PAIR ) THEN
                  DO 110 I = KR + 1, N
                     VR( I, KSI ) = ZERO
  110             CONTINUE
               END IF
            END IF
*
            IF( PAIR ) THEN
               KSR = KSR + 2
            ELSE
               KSR = KSR + 1
            END IF
         END IF
  120 CONTINUE
*
      RETURN
*
*     End of SHSEIN
*
      END
