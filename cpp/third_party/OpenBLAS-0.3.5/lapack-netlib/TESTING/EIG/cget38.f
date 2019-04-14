*> \brief \b CGET38
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CGET38( RMAX, LMAX, NINFO, KNT, NIN )
*
*       .. Scalar Arguments ..
*       INTEGER            KNT, NIN
*       ..
*       .. Array Arguments ..
*       INTEGER            LMAX( 3 ), NINFO( 3 )
*       REAL               RMAX( 3 )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CGET38 tests CTRSEN, a routine for estimating condition numbers of a
*> cluster of eigenvalues and/or its associated right invariant subspace
*>
*> The test matrices are read from a file with logical unit number NIN.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[out] RMAX
*> \verbatim
*>          RMAX is REAL array, dimension (3)
*>          Values of the largest test ratios.
*>          RMAX(1) = largest residuals from CHST01 or comparing
*>                    different calls to CTRSEN
*>          RMAX(2) = largest error in reciprocal condition
*>                    numbers taking their conditioning into account
*>          RMAX(3) = largest error in reciprocal condition
*>                    numbers not taking their conditioning into
*>                    account (may be larger than RMAX(2))
*> \endverbatim
*>
*> \param[out] LMAX
*> \verbatim
*>          LMAX is INTEGER array, dimension (3)
*>          LMAX(i) is example number where largest test ratio
*>          RMAX(i) is achieved. Also:
*>          If CGEHRD returns INFO nonzero on example i, LMAX(1)=i
*>          If CHSEQR returns INFO nonzero on example i, LMAX(2)=i
*>          If CTRSEN returns INFO nonzero on example i, LMAX(3)=i
*> \endverbatim
*>
*> \param[out] NINFO
*> \verbatim
*>          NINFO is INTEGER array, dimension (3)
*>          NINFO(1) = No. of times CGEHRD returned INFO nonzero
*>          NINFO(2) = No. of times CHSEQR returned INFO nonzero
*>          NINFO(3) = No. of times CTRSEN returned INFO nonzero
*> \endverbatim
*>
*> \param[out] KNT
*> \verbatim
*>          KNT is INTEGER
*>          Total number of examples tested.
*> \endverbatim
*>
*> \param[in] NIN
*> \verbatim
*>          NIN is INTEGER
*>          Input logical unit number.
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
*> \ingroup complex_eig
*
*  =====================================================================
      SUBROUTINE CGET38( RMAX, LMAX, NINFO, KNT, NIN )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            KNT, NIN
*     ..
*     .. Array Arguments ..
      INTEGER            LMAX( 3 ), NINFO( 3 )
      REAL               RMAX( 3 )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            LDT, LWORK
      PARAMETER          ( LDT = 20, LWORK = 2*LDT*( 10+LDT ) )
      REAL               ZERO, ONE, TWO
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0, TWO = 2.0E+0 )
      REAL               EPSIN
      PARAMETER          ( EPSIN = 5.9605E-8 )
      COMPLEX            CZERO
      PARAMETER          ( CZERO = ( 0.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      INTEGER            I, INFO, ISCL, ISRT, ITMP, J, KMIN, M, N, NDIM
      REAL               BIGNUM, EPS, S, SEP, SEPIN, SEPTMP, SIN,
     $                   SMLNUM, STMP, TNRM, TOL, TOLIN, V, VMAX, VMIN,
     $                   VMUL
*     ..
*     .. Local Arrays ..
      LOGICAL            SELECT( LDT )
      INTEGER            IPNT( LDT ), ISELEC( LDT )
      REAL               RESULT( 2 ), RWORK( LDT ), VAL( 3 ),
     $                   WSRT( LDT )
      COMPLEX            Q( LDT, LDT ), QSAV( LDT, LDT ),
     $                   QTMP( LDT, LDT ), T( LDT, LDT ),
     $                   TMP( LDT, LDT ), TSAV( LDT, LDT ),
     $                   TSAV1( LDT, LDT ), TTMP( LDT, LDT ), W( LDT ),
     $                   WORK( LWORK ), WTMP( LDT )
*     ..
*     .. External Functions ..
      REAL               CLANGE, SLAMCH
      EXTERNAL           CLANGE, SLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           CGEHRD, CHSEQR, CHST01, CLACPY, CSSCAL, CTRSEN,
     $                   CUNGHR, SLABAD
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          AIMAG, MAX, REAL, SQRT
*     ..
*     .. Executable Statements ..
*
      EPS = SLAMCH( 'P' )
      SMLNUM = SLAMCH( 'S' ) / EPS
      BIGNUM = ONE / SMLNUM
      CALL SLABAD( SMLNUM, BIGNUM )
*
*     EPSIN = 2**(-24) = precision to which input data computed
*
      EPS = MAX( EPS, EPSIN )
      RMAX( 1 ) = ZERO
      RMAX( 2 ) = ZERO
      RMAX( 3 ) = ZERO
      LMAX( 1 ) = 0
      LMAX( 2 ) = 0
      LMAX( 3 ) = 0
      KNT = 0
      NINFO( 1 ) = 0
      NINFO( 2 ) = 0
      NINFO( 3 ) = 0
      VAL( 1 ) = SQRT( SMLNUM )
      VAL( 2 ) = ONE
      VAL( 3 ) = SQRT( SQRT( BIGNUM ) )
*
*     Read input data until N=0.  Assume input eigenvalues are sorted
*     lexicographically (increasing by real part, then decreasing by
*     imaginary part)
*
   10 CONTINUE
      READ( NIN, FMT = * )N, NDIM, ISRT
      IF( N.EQ.0 )
     $   RETURN
      READ( NIN, FMT = * )( ISELEC( I ), I = 1, NDIM )
      DO 20 I = 1, N
         READ( NIN, FMT = * )( TMP( I, J ), J = 1, N )
   20 CONTINUE
      READ( NIN, FMT = * )SIN, SEPIN
*
      TNRM = CLANGE( 'M', N, N, TMP, LDT, RWORK )
      DO 200 ISCL = 1, 3
*
*        Scale input matrix
*
         KNT = KNT + 1
         CALL CLACPY( 'F', N, N, TMP, LDT, T, LDT )
         VMUL = VAL( ISCL )
         DO 30 I = 1, N
            CALL CSSCAL( N, VMUL, T( 1, I ), 1 )
   30    CONTINUE
         IF( TNRM.EQ.ZERO )
     $      VMUL = ONE
         CALL CLACPY( 'F', N, N, T, LDT, TSAV, LDT )
*
*        Compute Schur form
*
         CALL CGEHRD( N, 1, N, T, LDT, WORK( 1 ), WORK( N+1 ), LWORK-N,
     $                INFO )
         IF( INFO.NE.0 ) THEN
            LMAX( 1 ) = KNT
            NINFO( 1 ) = NINFO( 1 ) + 1
            GO TO 200
         END IF
*
*        Generate unitary matrix
*
         CALL CLACPY( 'L', N, N, T, LDT, Q, LDT )
         CALL CUNGHR( N, 1, N, Q, LDT, WORK( 1 ), WORK( N+1 ), LWORK-N,
     $                INFO )
*
*        Compute Schur form
*
         DO 50 J = 1, N - 2
            DO 40 I = J + 2, N
               T( I, J ) = CZERO
   40       CONTINUE
   50    CONTINUE
         CALL CHSEQR( 'S', 'V', N, 1, N, T, LDT, W, Q, LDT, WORK, LWORK,
     $                INFO )
         IF( INFO.NE.0 ) THEN
            LMAX( 2 ) = KNT
            NINFO( 2 ) = NINFO( 2 ) + 1
            GO TO 200
         END IF
*
*        Sort, select eigenvalues
*
         DO 60 I = 1, N
            IPNT( I ) = I
            SELECT( I ) = .FALSE.
   60    CONTINUE
         IF( ISRT.EQ.0 ) THEN
            DO 70 I = 1, N
               WSRT( I ) = REAL( W( I ) )
   70       CONTINUE
         ELSE
            DO 80 I = 1, N
               WSRT( I ) = AIMAG( W( I ) )
   80       CONTINUE
         END IF
         DO 100 I = 1, N - 1
            KMIN = I
            VMIN = WSRT( I )
            DO 90 J = I + 1, N
               IF( WSRT( J ).LT.VMIN ) THEN
                  KMIN = J
                  VMIN = WSRT( J )
               END IF
   90       CONTINUE
            WSRT( KMIN ) = WSRT( I )
            WSRT( I ) = VMIN
            ITMP = IPNT( I )
            IPNT( I ) = IPNT( KMIN )
            IPNT( KMIN ) = ITMP
  100    CONTINUE
         DO 110 I = 1, NDIM
            SELECT( IPNT( ISELEC( I ) ) ) = .TRUE.
  110    CONTINUE
*
*        Compute condition numbers
*
         CALL CLACPY( 'F', N, N, Q, LDT, QSAV, LDT )
         CALL CLACPY( 'F', N, N, T, LDT, TSAV1, LDT )
         CALL CTRSEN( 'B', 'V', SELECT, N, T, LDT, Q, LDT, WTMP, M, S,
     $                SEP, WORK, LWORK, INFO )
         IF( INFO.NE.0 ) THEN
            LMAX( 3 ) = KNT
            NINFO( 3 ) = NINFO( 3 ) + 1
            GO TO 200
         END IF
         SEPTMP = SEP / VMUL
         STMP = S
*
*        Compute residuals
*
         CALL CHST01( N, 1, N, TSAV, LDT, T, LDT, Q, LDT, WORK, LWORK,
     $                RWORK, RESULT )
         VMAX = MAX( RESULT( 1 ), RESULT( 2 ) )
         IF( VMAX.GT.RMAX( 1 ) ) THEN
            RMAX( 1 ) = VMAX
            IF( NINFO( 1 ).EQ.0 )
     $         LMAX( 1 ) = KNT
         END IF
*
*        Compare condition number for eigenvalue cluster
*        taking its condition number into account
*
         V = MAX( TWO*REAL( N )*EPS*TNRM, SMLNUM )
         IF( TNRM.EQ.ZERO )
     $      V = ONE
         IF( V.GT.SEPTMP ) THEN
            TOL = ONE
         ELSE
            TOL = V / SEPTMP
         END IF
         IF( V.GT.SEPIN ) THEN
            TOLIN = ONE
         ELSE
            TOLIN = V / SEPIN
         END IF
         TOL = MAX( TOL, SMLNUM / EPS )
         TOLIN = MAX( TOLIN, SMLNUM / EPS )
         IF( EPS*( SIN-TOLIN ).GT.STMP+TOL ) THEN
            VMAX = ONE / EPS
         ELSE IF( SIN-TOLIN.GT.STMP+TOL ) THEN
            VMAX = ( SIN-TOLIN ) / ( STMP+TOL )
         ELSE IF( SIN+TOLIN.LT.EPS*( STMP-TOL ) ) THEN
            VMAX = ONE / EPS
         ELSE IF( SIN+TOLIN.LT.STMP-TOL ) THEN
            VMAX = ( STMP-TOL ) / ( SIN+TOLIN )
         ELSE
            VMAX = ONE
         END IF
         IF( VMAX.GT.RMAX( 2 ) ) THEN
            RMAX( 2 ) = VMAX
            IF( NINFO( 2 ).EQ.0 )
     $         LMAX( 2 ) = KNT
         END IF
*
*        Compare condition numbers for invariant subspace
*        taking its condition number into account
*
         IF( V.GT.SEPTMP*STMP ) THEN
            TOL = SEPTMP
         ELSE
            TOL = V / STMP
         END IF
         IF( V.GT.SEPIN*SIN ) THEN
            TOLIN = SEPIN
         ELSE
            TOLIN = V / SIN
         END IF
         TOL = MAX( TOL, SMLNUM / EPS )
         TOLIN = MAX( TOLIN, SMLNUM / EPS )
         IF( EPS*( SEPIN-TOLIN ).GT.SEPTMP+TOL ) THEN
            VMAX = ONE / EPS
         ELSE IF( SEPIN-TOLIN.GT.SEPTMP+TOL ) THEN
            VMAX = ( SEPIN-TOLIN ) / ( SEPTMP+TOL )
         ELSE IF( SEPIN+TOLIN.LT.EPS*( SEPTMP-TOL ) ) THEN
            VMAX = ONE / EPS
         ELSE IF( SEPIN+TOLIN.LT.SEPTMP-TOL ) THEN
            VMAX = ( SEPTMP-TOL ) / ( SEPIN+TOLIN )
         ELSE
            VMAX = ONE
         END IF
         IF( VMAX.GT.RMAX( 2 ) ) THEN
            RMAX( 2 ) = VMAX
            IF( NINFO( 2 ).EQ.0 )
     $         LMAX( 2 ) = KNT
         END IF
*
*        Compare condition number for eigenvalue cluster
*        without taking its condition number into account
*
         IF( SIN.LE.REAL( 2*N )*EPS .AND. STMP.LE.REAL( 2*N )*EPS ) THEN
            VMAX = ONE
         ELSE IF( EPS*SIN.GT.STMP ) THEN
            VMAX = ONE / EPS
         ELSE IF( SIN.GT.STMP ) THEN
            VMAX = SIN / STMP
         ELSE IF( SIN.LT.EPS*STMP ) THEN
            VMAX = ONE / EPS
         ELSE IF( SIN.LT.STMP ) THEN
            VMAX = STMP / SIN
         ELSE
            VMAX = ONE
         END IF
         IF( VMAX.GT.RMAX( 3 ) ) THEN
            RMAX( 3 ) = VMAX
            IF( NINFO( 3 ).EQ.0 )
     $         LMAX( 3 ) = KNT
         END IF
*
*        Compare condition numbers for invariant subspace
*        without taking its condition number into account
*
         IF( SEPIN.LE.V .AND. SEPTMP.LE.V ) THEN
            VMAX = ONE
         ELSE IF( EPS*SEPIN.GT.SEPTMP ) THEN
            VMAX = ONE / EPS
         ELSE IF( SEPIN.GT.SEPTMP ) THEN
            VMAX = SEPIN / SEPTMP
         ELSE IF( SEPIN.LT.EPS*SEPTMP ) THEN
            VMAX = ONE / EPS
         ELSE IF( SEPIN.LT.SEPTMP ) THEN
            VMAX = SEPTMP / SEPIN
         ELSE
            VMAX = ONE
         END IF
         IF( VMAX.GT.RMAX( 3 ) ) THEN
            RMAX( 3 ) = VMAX
            IF( NINFO( 3 ).EQ.0 )
     $         LMAX( 3 ) = KNT
         END IF
*
*        Compute eigenvalue condition number only and compare
*        Update Q
*
         VMAX = ZERO
         CALL CLACPY( 'F', N, N, TSAV1, LDT, TTMP, LDT )
         CALL CLACPY( 'F', N, N, QSAV, LDT, QTMP, LDT )
         SEPTMP = -ONE
         STMP = -ONE
         CALL CTRSEN( 'E', 'V', SELECT, N, TTMP, LDT, QTMP, LDT, WTMP,
     $                M, STMP, SEPTMP, WORK, LWORK, INFO )
         IF( INFO.NE.0 ) THEN
            LMAX( 3 ) = KNT
            NINFO( 3 ) = NINFO( 3 ) + 1
            GO TO 200
         END IF
         IF( S.NE.STMP )
     $      VMAX = ONE / EPS
         IF( -ONE.NE.SEPTMP )
     $      VMAX = ONE / EPS
         DO 130 I = 1, N
            DO 120 J = 1, N
               IF( TTMP( I, J ).NE.T( I, J ) )
     $            VMAX = ONE / EPS
               IF( QTMP( I, J ).NE.Q( I, J ) )
     $            VMAX = ONE / EPS
  120       CONTINUE
  130    CONTINUE
*
*        Compute invariant subspace condition number only and compare
*        Update Q
*
         CALL CLACPY( 'F', N, N, TSAV1, LDT, TTMP, LDT )
         CALL CLACPY( 'F', N, N, QSAV, LDT, QTMP, LDT )
         SEPTMP = -ONE
         STMP = -ONE
         CALL CTRSEN( 'V', 'V', SELECT, N, TTMP, LDT, QTMP, LDT, WTMP,
     $                M, STMP, SEPTMP, WORK, LWORK, INFO )
         IF( INFO.NE.0 ) THEN
            LMAX( 3 ) = KNT
            NINFO( 3 ) = NINFO( 3 ) + 1
            GO TO 200
         END IF
         IF( -ONE.NE.STMP )
     $      VMAX = ONE / EPS
         IF( SEP.NE.SEPTMP )
     $      VMAX = ONE / EPS
         DO 150 I = 1, N
            DO 140 J = 1, N
               IF( TTMP( I, J ).NE.T( I, J ) )
     $            VMAX = ONE / EPS
               IF( QTMP( I, J ).NE.Q( I, J ) )
     $            VMAX = ONE / EPS
  140       CONTINUE
  150    CONTINUE
*
*        Compute eigenvalue condition number only and compare
*        Do not update Q
*
         CALL CLACPY( 'F', N, N, TSAV1, LDT, TTMP, LDT )
         CALL CLACPY( 'F', N, N, QSAV, LDT, QTMP, LDT )
         SEPTMP = -ONE
         STMP = -ONE
         CALL CTRSEN( 'E', 'N', SELECT, N, TTMP, LDT, QTMP, LDT, WTMP,
     $                M, STMP, SEPTMP, WORK, LWORK, INFO )
         IF( INFO.NE.0 ) THEN
            LMAX( 3 ) = KNT
            NINFO( 3 ) = NINFO( 3 ) + 1
            GO TO 200
         END IF
         IF( S.NE.STMP )
     $      VMAX = ONE / EPS
         IF( -ONE.NE.SEPTMP )
     $      VMAX = ONE / EPS
         DO 170 I = 1, N
            DO 160 J = 1, N
               IF( TTMP( I, J ).NE.T( I, J ) )
     $            VMAX = ONE / EPS
               IF( QTMP( I, J ).NE.QSAV( I, J ) )
     $            VMAX = ONE / EPS
  160       CONTINUE
  170    CONTINUE
*
*        Compute invariant subspace condition number only and compare
*        Do not update Q
*
         CALL CLACPY( 'F', N, N, TSAV1, LDT, TTMP, LDT )
         CALL CLACPY( 'F', N, N, QSAV, LDT, QTMP, LDT )
         SEPTMP = -ONE
         STMP = -ONE
         CALL CTRSEN( 'V', 'N', SELECT, N, TTMP, LDT, QTMP, LDT, WTMP,
     $                M, STMP, SEPTMP, WORK, LWORK, INFO )
         IF( INFO.NE.0 ) THEN
            LMAX( 3 ) = KNT
            NINFO( 3 ) = NINFO( 3 ) + 1
            GO TO 200
         END IF
         IF( -ONE.NE.STMP )
     $      VMAX = ONE / EPS
         IF( SEP.NE.SEPTMP )
     $      VMAX = ONE / EPS
         DO 190 I = 1, N
            DO 180 J = 1, N
               IF( TTMP( I, J ).NE.T( I, J ) )
     $            VMAX = ONE / EPS
               IF( QTMP( I, J ).NE.QSAV( I, J ) )
     $            VMAX = ONE / EPS
  180       CONTINUE
  190    CONTINUE
         IF( VMAX.GT.RMAX( 1 ) ) THEN
            RMAX( 1 ) = VMAX
            IF( NINFO( 1 ).EQ.0 )
     $         LMAX( 1 ) = KNT
         END IF
  200 CONTINUE
      GO TO 10
*
*     End of CGET38
*
      END
