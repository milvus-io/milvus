*> \brief \b ZGET37
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZGET37( RMAX, LMAX, NINFO, KNT, NIN )
*
*       .. Scalar Arguments ..
*       INTEGER            KNT, NIN
*       ..
*       .. Array Arguments ..
*       INTEGER            LMAX( 3 ), NINFO( 3 )
*       DOUBLE PRECISION   RMAX( 3 )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZGET37 tests ZTRSNA, a routine for estimating condition numbers of
*> eigenvalues and/or right eigenvectors of a matrix.
*>
*> The test matrices are read from a file with logical unit number NIN.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[out] RMAX
*> \verbatim
*>          RMAX is DOUBLE PRECISION array, dimension (3)
*>          Value of the largest test ratio.
*>          RMAX(1) = largest ratio comparing different calls to ZTRSNA
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
*>          If ZGEHRD returns INFO nonzero on example i, LMAX(1)=i
*>          If ZHSEQR returns INFO nonzero on example i, LMAX(2)=i
*>          If ZTRSNA returns INFO nonzero on example i, LMAX(3)=i
*> \endverbatim
*>
*> \param[out] NINFO
*> \verbatim
*>          NINFO is INTEGER array, dimension (3)
*>          NINFO(1) = No. of times ZGEHRD returned INFO nonzero
*>          NINFO(2) = No. of times ZHSEQR returned INFO nonzero
*>          NINFO(3) = No. of times ZTRSNA returned INFO nonzero
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
*>          Input logical unit number
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
*> \ingroup complex16_eig
*
*  =====================================================================
      SUBROUTINE ZGET37( RMAX, LMAX, NINFO, KNT, NIN )
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
      DOUBLE PRECISION   RMAX( 3 )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE, TWO
      PARAMETER          ( ZERO = 0.0D0, ONE = 1.0D0, TWO = 2.0D0 )
      DOUBLE PRECISION   EPSIN
      PARAMETER          ( EPSIN = 5.9605D-8 )
      INTEGER            LDT, LWORK
      PARAMETER          ( LDT = 20, LWORK = 2*LDT*( 10+LDT ) )
*     ..
*     .. Local Scalars ..
      INTEGER            I, ICMP, INFO, ISCL, ISRT, J, KMIN, M, N
      DOUBLE PRECISION   BIGNUM, EPS, SMLNUM, TNRM, TOL, TOLIN, V,
     $                   VCMIN, VMAX, VMIN, VMUL
*     ..
*     .. Local Arrays ..
      LOGICAL            SELECT( LDT )
      INTEGER            LCMP( 3 )
      DOUBLE PRECISION   DUM( 1 ), RWORK( 2*LDT ), S( LDT ), SEP( LDT ),
     $                   SEPIN( LDT ), SEPTMP( LDT ), SIN( LDT ),
     $                   STMP( LDT ), VAL( 3 ), WIIN( LDT ),
     $                   WRIN( LDT ), WSRT( LDT )
      COMPLEX*16         CDUM( 1 ), LE( LDT, LDT ), RE( LDT, LDT ),
     $                   T( LDT, LDT ), TMP( LDT, LDT ), W( LDT ),
     $                   WORK( LWORK ), WTMP( LDT )
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH, ZLANGE
      EXTERNAL           DLAMCH, ZLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           DCOPY, DLABAD, DSCAL, ZCOPY, ZDSCAL, ZGEHRD,
     $                   ZHSEQR, ZLACPY, ZTREVC, ZTRSNA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE, DIMAG, MAX, SQRT
*     ..
*     .. Executable Statements ..
*
      EPS = DLAMCH( 'P' )
      SMLNUM = DLAMCH( 'S' ) / EPS
      BIGNUM = ONE / SMLNUM
      CALL DLABAD( SMLNUM, BIGNUM )
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
      VAL( 3 ) = SQRT( BIGNUM )
*
*     Read input data until N=0.  Assume input eigenvalues are sorted
*     lexicographically (increasing by real part if ISRT = 0,
*     increasing by imaginary part if ISRT = 1)
*
   10 CONTINUE
      READ( NIN, FMT = * )N, ISRT
      IF( N.EQ.0 )
     $   RETURN
      DO 20 I = 1, N
         READ( NIN, FMT = * )( TMP( I, J ), J = 1, N )
   20 CONTINUE
      DO 30 I = 1, N
         READ( NIN, FMT = * )WRIN( I ), WIIN( I ), SIN( I ), SEPIN( I )
   30 CONTINUE
      TNRM = ZLANGE( 'M', N, N, TMP, LDT, RWORK )
      DO 260 ISCL = 1, 3
*
*        Scale input matrix
*
         KNT = KNT + 1
         CALL ZLACPY( 'F', N, N, TMP, LDT, T, LDT )
         VMUL = VAL( ISCL )
         DO 40 I = 1, N
            CALL ZDSCAL( N, VMUL, T( 1, I ), 1 )
   40    CONTINUE
         IF( TNRM.EQ.ZERO )
     $      VMUL = ONE
*
*        Compute eigenvalues and eigenvectors
*
         CALL ZGEHRD( N, 1, N, T, LDT, WORK( 1 ), WORK( N+1 ), LWORK-N,
     $                INFO )
         IF( INFO.NE.0 ) THEN
            LMAX( 1 ) = KNT
            NINFO( 1 ) = NINFO( 1 ) + 1
            GO TO 260
         END IF
         DO 60 J = 1, N - 2
            DO 50 I = J + 2, N
               T( I, J ) = ZERO
   50       CONTINUE
   60    CONTINUE
*
*        Compute Schur form
*
         CALL ZHSEQR( 'S', 'N', N, 1, N, T, LDT, W, CDUM, 1, WORK,
     $                LWORK, INFO )
         IF( INFO.NE.0 ) THEN
            LMAX( 2 ) = KNT
            NINFO( 2 ) = NINFO( 2 ) + 1
            GO TO 260
         END IF
*
*        Compute eigenvectors
*
         DO 70 I = 1, N
            SELECT( I ) = .TRUE.
   70    CONTINUE
         CALL ZTREVC( 'B', 'A', SELECT, N, T, LDT, LE, LDT, RE, LDT, N,
     $                M, WORK, RWORK, INFO )
*
*        Compute condition numbers
*
         CALL ZTRSNA( 'B', 'A', SELECT, N, T, LDT, LE, LDT, RE, LDT, S,
     $                SEP, N, M, WORK, N, RWORK, INFO )
         IF( INFO.NE.0 ) THEN
            LMAX( 3 ) = KNT
            NINFO( 3 ) = NINFO( 3 ) + 1
            GO TO 260
         END IF
*
*        Sort eigenvalues and condition numbers lexicographically
*        to compare with inputs
*
         CALL ZCOPY( N, W, 1, WTMP, 1 )
         IF( ISRT.EQ.0 ) THEN
*
*           Sort by increasing real part
*
            DO 80 I = 1, N
               WSRT( I ) = DBLE( W( I ) )
   80       CONTINUE
         ELSE
*
*           Sort by increasing imaginary part
*
            DO 90 I = 1, N
               WSRT( I ) = DIMAG( W( I ) )
   90       CONTINUE
         END IF
         CALL DCOPY( N, S, 1, STMP, 1 )
         CALL DCOPY( N, SEP, 1, SEPTMP, 1 )
         CALL DSCAL( N, ONE / VMUL, SEPTMP, 1 )
         DO 110 I = 1, N - 1
            KMIN = I
            VMIN = WSRT( I )
            DO 100 J = I + 1, N
               IF( WSRT( J ).LT.VMIN ) THEN
                  KMIN = J
                  VMIN = WSRT( J )
               END IF
  100       CONTINUE
            WSRT( KMIN ) = WSRT( I )
            WSRT( I ) = VMIN
            VCMIN = WTMP( I )
            WTMP( I ) = W( KMIN )
            WTMP( KMIN ) = VCMIN
            VMIN = STMP( KMIN )
            STMP( KMIN ) = STMP( I )
            STMP( I ) = VMIN
            VMIN = SEPTMP( KMIN )
            SEPTMP( KMIN ) = SEPTMP( I )
            SEPTMP( I ) = VMIN
  110    CONTINUE
*
*        Compare condition numbers for eigenvalues
*        taking their condition numbers into account
*
         V = MAX( TWO*DBLE( N )*EPS*TNRM, SMLNUM )
         IF( TNRM.EQ.ZERO )
     $      V = ONE
         DO 120 I = 1, N
            IF( V.GT.SEPTMP( I ) ) THEN
               TOL = ONE
            ELSE
               TOL = V / SEPTMP( I )
            END IF
            IF( V.GT.SEPIN( I ) ) THEN
               TOLIN = ONE
            ELSE
               TOLIN = V / SEPIN( I )
            END IF
            TOL = MAX( TOL, SMLNUM / EPS )
            TOLIN = MAX( TOLIN, SMLNUM / EPS )
            IF( EPS*( SIN( I )-TOLIN ).GT.STMP( I )+TOL ) THEN
               VMAX = ONE / EPS
            ELSE IF( SIN( I )-TOLIN.GT.STMP( I )+TOL ) THEN
               VMAX = ( SIN( I )-TOLIN ) / ( STMP( I )+TOL )
            ELSE IF( SIN( I )+TOLIN.LT.EPS*( STMP( I )-TOL ) ) THEN
               VMAX = ONE / EPS
            ELSE IF( SIN( I )+TOLIN.LT.STMP( I )-TOL ) THEN
               VMAX = ( STMP( I )-TOL ) / ( SIN( I )+TOLIN )
            ELSE
               VMAX = ONE
            END IF
            IF( VMAX.GT.RMAX( 2 ) ) THEN
               RMAX( 2 ) = VMAX
               IF( NINFO( 2 ).EQ.0 )
     $            LMAX( 2 ) = KNT
            END IF
  120    CONTINUE
*
*        Compare condition numbers for eigenvectors
*        taking their condition numbers into account
*
         DO 130 I = 1, N
            IF( V.GT.SEPTMP( I )*STMP( I ) ) THEN
               TOL = SEPTMP( I )
            ELSE
               TOL = V / STMP( I )
            END IF
            IF( V.GT.SEPIN( I )*SIN( I ) ) THEN
               TOLIN = SEPIN( I )
            ELSE
               TOLIN = V / SIN( I )
            END IF
            TOL = MAX( TOL, SMLNUM / EPS )
            TOLIN = MAX( TOLIN, SMLNUM / EPS )
            IF( EPS*( SEPIN( I )-TOLIN ).GT.SEPTMP( I )+TOL ) THEN
               VMAX = ONE / EPS
            ELSE IF( SEPIN( I )-TOLIN.GT.SEPTMP( I )+TOL ) THEN
               VMAX = ( SEPIN( I )-TOLIN ) / ( SEPTMP( I )+TOL )
            ELSE IF( SEPIN( I )+TOLIN.LT.EPS*( SEPTMP( I )-TOL ) ) THEN
               VMAX = ONE / EPS
            ELSE IF( SEPIN( I )+TOLIN.LT.SEPTMP( I )-TOL ) THEN
               VMAX = ( SEPTMP( I )-TOL ) / ( SEPIN( I )+TOLIN )
            ELSE
               VMAX = ONE
            END IF
            IF( VMAX.GT.RMAX( 2 ) ) THEN
               RMAX( 2 ) = VMAX
               IF( NINFO( 2 ).EQ.0 )
     $            LMAX( 2 ) = KNT
            END IF
  130    CONTINUE
*
*        Compare condition numbers for eigenvalues
*        without taking their condition numbers into account
*
         DO 140 I = 1, N
            IF( SIN( I ).LE.DBLE( 2*N )*EPS .AND. STMP( I ).LE.
     $          DBLE( 2*N )*EPS ) THEN
               VMAX = ONE
            ELSE IF( EPS*SIN( I ).GT.STMP( I ) ) THEN
               VMAX = ONE / EPS
            ELSE IF( SIN( I ).GT.STMP( I ) ) THEN
               VMAX = SIN( I ) / STMP( I )
            ELSE IF( SIN( I ).LT.EPS*STMP( I ) ) THEN
               VMAX = ONE / EPS
            ELSE IF( SIN( I ).LT.STMP( I ) ) THEN
               VMAX = STMP( I ) / SIN( I )
            ELSE
               VMAX = ONE
            END IF
            IF( VMAX.GT.RMAX( 3 ) ) THEN
               RMAX( 3 ) = VMAX
               IF( NINFO( 3 ).EQ.0 )
     $            LMAX( 3 ) = KNT
            END IF
  140    CONTINUE
*
*        Compare condition numbers for eigenvectors
*        without taking their condition numbers into account
*
         DO 150 I = 1, N
            IF( SEPIN( I ).LE.V .AND. SEPTMP( I ).LE.V ) THEN
               VMAX = ONE
            ELSE IF( EPS*SEPIN( I ).GT.SEPTMP( I ) ) THEN
               VMAX = ONE / EPS
            ELSE IF( SEPIN( I ).GT.SEPTMP( I ) ) THEN
               VMAX = SEPIN( I ) / SEPTMP( I )
            ELSE IF( SEPIN( I ).LT.EPS*SEPTMP( I ) ) THEN
               VMAX = ONE / EPS
            ELSE IF( SEPIN( I ).LT.SEPTMP( I ) ) THEN
               VMAX = SEPTMP( I ) / SEPIN( I )
            ELSE
               VMAX = ONE
            END IF
            IF( VMAX.GT.RMAX( 3 ) ) THEN
               RMAX( 3 ) = VMAX
               IF( NINFO( 3 ).EQ.0 )
     $            LMAX( 3 ) = KNT
            END IF
  150    CONTINUE
*
*        Compute eigenvalue condition numbers only and compare
*
         VMAX = ZERO
         DUM( 1 ) = -ONE
         CALL DCOPY( N, DUM, 0, STMP, 1 )
         CALL DCOPY( N, DUM, 0, SEPTMP, 1 )
         CALL ZTRSNA( 'E', 'A', SELECT, N, T, LDT, LE, LDT, RE, LDT,
     $                STMP, SEPTMP, N, M, WORK, N, RWORK, INFO )
         IF( INFO.NE.0 ) THEN
            LMAX( 3 ) = KNT
            NINFO( 3 ) = NINFO( 3 ) + 1
            GO TO 260
         END IF
         DO 160 I = 1, N
            IF( STMP( I ).NE.S( I ) )
     $         VMAX = ONE / EPS
            IF( SEPTMP( I ).NE.DUM( 1 ) )
     $         VMAX = ONE / EPS
  160    CONTINUE
*
*        Compute eigenvector condition numbers only and compare
*
         CALL DCOPY( N, DUM, 0, STMP, 1 )
         CALL DCOPY( N, DUM, 0, SEPTMP, 1 )
         CALL ZTRSNA( 'V', 'A', SELECT, N, T, LDT, LE, LDT, RE, LDT,
     $                STMP, SEPTMP, N, M, WORK, N, RWORK, INFO )
         IF( INFO.NE.0 ) THEN
            LMAX( 3 ) = KNT
            NINFO( 3 ) = NINFO( 3 ) + 1
            GO TO 260
         END IF
         DO 170 I = 1, N
            IF( STMP( I ).NE.DUM( 1 ) )
     $         VMAX = ONE / EPS
            IF( SEPTMP( I ).NE.SEP( I ) )
     $         VMAX = ONE / EPS
  170    CONTINUE
*
*        Compute all condition numbers using SELECT and compare
*
         DO 180 I = 1, N
            SELECT( I ) = .TRUE.
  180    CONTINUE
         CALL DCOPY( N, DUM, 0, STMP, 1 )
         CALL DCOPY( N, DUM, 0, SEPTMP, 1 )
         CALL ZTRSNA( 'B', 'S', SELECT, N, T, LDT, LE, LDT, RE, LDT,
     $                STMP, SEPTMP, N, M, WORK, N, RWORK, INFO )
         IF( INFO.NE.0 ) THEN
            LMAX( 3 ) = KNT
            NINFO( 3 ) = NINFO( 3 ) + 1
            GO TO 260
         END IF
         DO 190 I = 1, N
            IF( SEPTMP( I ).NE.SEP( I ) )
     $         VMAX = ONE / EPS
            IF( STMP( I ).NE.S( I ) )
     $         VMAX = ONE / EPS
  190    CONTINUE
*
*        Compute eigenvalue condition numbers using SELECT and compare
*
         CALL DCOPY( N, DUM, 0, STMP, 1 )
         CALL DCOPY( N, DUM, 0, SEPTMP, 1 )
         CALL ZTRSNA( 'E', 'S', SELECT, N, T, LDT, LE, LDT, RE, LDT,
     $                STMP, SEPTMP, N, M, WORK, N, RWORK, INFO )
         IF( INFO.NE.0 ) THEN
            LMAX( 3 ) = KNT
            NINFO( 3 ) = NINFO( 3 ) + 1
            GO TO 260
         END IF
         DO 200 I = 1, N
            IF( STMP( I ).NE.S( I ) )
     $         VMAX = ONE / EPS
            IF( SEPTMP( I ).NE.DUM( 1 ) )
     $         VMAX = ONE / EPS
  200    CONTINUE
*
*        Compute eigenvector condition numbers using SELECT and compare
*
         CALL DCOPY( N, DUM, 0, STMP, 1 )
         CALL DCOPY( N, DUM, 0, SEPTMP, 1 )
         CALL ZTRSNA( 'V', 'S', SELECT, N, T, LDT, LE, LDT, RE, LDT,
     $                STMP, SEPTMP, N, M, WORK, N, RWORK, INFO )
         IF( INFO.NE.0 ) THEN
            LMAX( 3 ) = KNT
            NINFO( 3 ) = NINFO( 3 ) + 1
            GO TO 260
         END IF
         DO 210 I = 1, N
            IF( STMP( I ).NE.DUM( 1 ) )
     $         VMAX = ONE / EPS
            IF( SEPTMP( I ).NE.SEP( I ) )
     $         VMAX = ONE / EPS
  210    CONTINUE
         IF( VMAX.GT.RMAX( 1 ) ) THEN
            RMAX( 1 ) = VMAX
            IF( NINFO( 1 ).EQ.0 )
     $         LMAX( 1 ) = KNT
         END IF
*
*        Select second and next to last eigenvalues
*
         DO 220 I = 1, N
            SELECT( I ) = .FALSE.
  220    CONTINUE
         ICMP = 0
         IF( N.GT.1 ) THEN
            ICMP = 1
            LCMP( 1 ) = 2
            SELECT( 2 ) = .TRUE.
            CALL ZCOPY( N, RE( 1, 2 ), 1, RE( 1, 1 ), 1 )
            CALL ZCOPY( N, LE( 1, 2 ), 1, LE( 1, 1 ), 1 )
         END IF
         IF( N.GT.3 ) THEN
            ICMP = 2
            LCMP( 2 ) = N - 1
            SELECT( N-1 ) = .TRUE.
            CALL ZCOPY( N, RE( 1, N-1 ), 1, RE( 1, 2 ), 1 )
            CALL ZCOPY( N, LE( 1, N-1 ), 1, LE( 1, 2 ), 1 )
         END IF
*
*        Compute all selected condition numbers
*
         CALL DCOPY( ICMP, DUM, 0, STMP, 1 )
         CALL DCOPY( ICMP, DUM, 0, SEPTMP, 1 )
         CALL ZTRSNA( 'B', 'S', SELECT, N, T, LDT, LE, LDT, RE, LDT,
     $                STMP, SEPTMP, N, M, WORK, N, RWORK, INFO )
         IF( INFO.NE.0 ) THEN
            LMAX( 3 ) = KNT
            NINFO( 3 ) = NINFO( 3 ) + 1
            GO TO 260
         END IF
         DO 230 I = 1, ICMP
            J = LCMP( I )
            IF( SEPTMP( I ).NE.SEP( J ) )
     $         VMAX = ONE / EPS
            IF( STMP( I ).NE.S( J ) )
     $         VMAX = ONE / EPS
  230    CONTINUE
*
*        Compute selected eigenvalue condition numbers
*
         CALL DCOPY( ICMP, DUM, 0, STMP, 1 )
         CALL DCOPY( ICMP, DUM, 0, SEPTMP, 1 )
         CALL ZTRSNA( 'E', 'S', SELECT, N, T, LDT, LE, LDT, RE, LDT,
     $                STMP, SEPTMP, N, M, WORK, N, RWORK, INFO )
         IF( INFO.NE.0 ) THEN
            LMAX( 3 ) = KNT
            NINFO( 3 ) = NINFO( 3 ) + 1
            GO TO 260
         END IF
         DO 240 I = 1, ICMP
            J = LCMP( I )
            IF( STMP( I ).NE.S( J ) )
     $         VMAX = ONE / EPS
            IF( SEPTMP( I ).NE.DUM( 1 ) )
     $         VMAX = ONE / EPS
  240    CONTINUE
*
*        Compute selected eigenvector condition numbers
*
         CALL DCOPY( ICMP, DUM, 0, STMP, 1 )
         CALL DCOPY( ICMP, DUM, 0, SEPTMP, 1 )
         CALL ZTRSNA( 'V', 'S', SELECT, N, T, LDT, LE, LDT, RE, LDT,
     $                STMP, SEPTMP, N, M, WORK, N, RWORK, INFO )
         IF( INFO.NE.0 ) THEN
            LMAX( 3 ) = KNT
            NINFO( 3 ) = NINFO( 3 ) + 1
            GO TO 260
         END IF
         DO 250 I = 1, ICMP
            J = LCMP( I )
            IF( STMP( I ).NE.DUM( 1 ) )
     $         VMAX = ONE / EPS
            IF( SEPTMP( I ).NE.SEP( J ) )
     $         VMAX = ONE / EPS
  250    CONTINUE
         IF( VMAX.GT.RMAX( 1 ) ) THEN
            RMAX( 1 ) = VMAX
            IF( NINFO( 1 ).EQ.0 )
     $         LMAX( 1 ) = KNT
         END IF
  260 CONTINUE
      GO TO 10
*
*     End of ZGET37
*
      END
