*> \brief \b CCHKGK
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CCHKGK( NIN, NOUT )
*
*       .. Scalar Arguments ..
*       INTEGER            NIN, NOUT
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CCHKGK tests CGGBAK, a routine for backward balancing  of
*> a matrix pair (A, B).
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] NIN
*> \verbatim
*>          NIN is INTEGER
*>          The logical unit number for input.  NIN > 0.
*> \endverbatim
*>
*> \param[in] NOUT
*> \verbatim
*>          NOUT is INTEGER
*>          The logical unit number for output.  NOUT > 0.
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
      SUBROUTINE CCHKGK( NIN, NOUT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            NIN, NOUT
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            LDA, LDB, LDVL, LDVR
      PARAMETER          ( LDA = 50, LDB = 50, LDVL = 50, LDVR = 50 )
      INTEGER            LDE, LDF, LDWORK, LRWORK
      PARAMETER          ( LDE = 50, LDF = 50, LDWORK = 50,
     $                   LRWORK = 6*50 )
      REAL               ZERO
      PARAMETER          ( ZERO = 0.0E+0 )
      COMPLEX            CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0E+0, 0.0E+0 ),
     $                   CONE = ( 1.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      INTEGER            I, IHI, ILO, INFO, J, KNT, M, N, NINFO
      REAL               ANORM, BNORM, EPS, RMAX, VMAX
      COMPLEX            CDUM
*     ..
*     .. Local Arrays ..
      INTEGER            LMAX( 4 )
      REAL               LSCALE( LDA ), RSCALE( LDA ), RWORK( LRWORK )
      COMPLEX            A( LDA, LDA ), AF( LDA, LDA ), B( LDB, LDB ),
     $                   BF( LDB, LDB ), E( LDE, LDE ), F( LDF, LDF ),
     $                   VL( LDVL, LDVL ), VLF( LDVL, LDVL ),
     $                   VR( LDVR, LDVR ), VRF( LDVR, LDVR ),
     $                   WORK( LDWORK, LDWORK )
*     ..
*     .. External Functions ..
      REAL               CLANGE, SLAMCH
      EXTERNAL           CLANGE, SLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           CGEMM, CGGBAK, CGGBAL, CLACPY
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, AIMAG, MAX, REAL
*     ..
*     .. Statement Functions ..
      REAL               CABS1
*     ..
*     .. Statement Function definitions ..
      CABS1( CDUM ) = ABS( REAL( CDUM ) ) + ABS( AIMAG( CDUM ) )
*     ..
*     .. Executable Statements ..
*
      LMAX( 1 ) = 0
      LMAX( 2 ) = 0
      LMAX( 3 ) = 0
      LMAX( 4 ) = 0
      NINFO = 0
      KNT = 0
      RMAX = ZERO
*
      EPS = SLAMCH( 'Precision' )
*
   10 CONTINUE
      READ( NIN, FMT = * )N, M
      IF( N.EQ.0 )
     $   GO TO 100
*
      DO 20 I = 1, N
         READ( NIN, FMT = * )( A( I, J ), J = 1, N )
   20 CONTINUE
*
      DO 30 I = 1, N
         READ( NIN, FMT = * )( B( I, J ), J = 1, N )
   30 CONTINUE
*
      DO 40 I = 1, N
         READ( NIN, FMT = * )( VL( I, J ), J = 1, M )
   40 CONTINUE
*
      DO 50 I = 1, N
         READ( NIN, FMT = * )( VR( I, J ), J = 1, M )
   50 CONTINUE
*
      KNT = KNT + 1
*
      ANORM = CLANGE( 'M', N, N, A, LDA, RWORK )
      BNORM = CLANGE( 'M', N, N, B, LDB, RWORK )
*
      CALL CLACPY( 'FULL', N, N, A, LDA, AF, LDA )
      CALL CLACPY( 'FULL', N, N, B, LDB, BF, LDB )
*
      CALL CGGBAL( 'B', N, A, LDA, B, LDB, ILO, IHI, LSCALE, RSCALE,
     $             RWORK, INFO )
      IF( INFO.NE.0 ) THEN
         NINFO = NINFO + 1
         LMAX( 1 ) = KNT
      END IF
*
      CALL CLACPY( 'FULL', N, M, VL, LDVL, VLF, LDVL )
      CALL CLACPY( 'FULL', N, M, VR, LDVR, VRF, LDVR )
*
      CALL CGGBAK( 'B', 'L', N, ILO, IHI, LSCALE, RSCALE, M, VL, LDVL,
     $             INFO )
      IF( INFO.NE.0 ) THEN
         NINFO = NINFO + 1
         LMAX( 2 ) = KNT
      END IF
*
      CALL CGGBAK( 'B', 'R', N, ILO, IHI, LSCALE, RSCALE, M, VR, LDVR,
     $             INFO )
      IF( INFO.NE.0 ) THEN
         NINFO = NINFO + 1
         LMAX( 3 ) = KNT
      END IF
*
*     Test of CGGBAK
*
*     Check tilde(VL)'*A*tilde(VR) - VL'*tilde(A)*VR
*     where tilde(A) denotes the transformed matrix.
*
      CALL CGEMM( 'N', 'N', N, M, N, CONE, AF, LDA, VR, LDVR, CZERO,
     $            WORK, LDWORK )
      CALL CGEMM( 'C', 'N', M, M, N, CONE, VL, LDVL, WORK, LDWORK,
     $            CZERO, E, LDE )
*
      CALL CGEMM( 'N', 'N', N, M, N, CONE, A, LDA, VRF, LDVR, CZERO,
     $            WORK, LDWORK )
      CALL CGEMM( 'C', 'N', M, M, N, CONE, VLF, LDVL, WORK, LDWORK,
     $            CZERO, F, LDF )
*
      VMAX = ZERO
      DO 70 J = 1, M
         DO 60 I = 1, M
            VMAX = MAX( VMAX, CABS1( E( I, J )-F( I, J ) ) )
   60    CONTINUE
   70 CONTINUE
      VMAX = VMAX / ( EPS*MAX( ANORM, BNORM ) )
      IF( VMAX.GT.RMAX ) THEN
         LMAX( 4 ) = KNT
         RMAX = VMAX
      END IF
*
*     Check tilde(VL)'*B*tilde(VR) - VL'*tilde(B)*VR
*
      CALL CGEMM( 'N', 'N', N, M, N, CONE, BF, LDB, VR, LDVR, CZERO,
     $            WORK, LDWORK )
      CALL CGEMM( 'C', 'N', M, M, N, CONE, VL, LDVL, WORK, LDWORK,
     $            CZERO, E, LDE )
*
      CALL CGEMM( 'n', 'n', N, M, N, CONE, B, LDB, VRF, LDVR, CZERO,
     $            WORK, LDWORK )
      CALL CGEMM( 'C', 'N', M, M, N, CONE, VLF, LDVL, WORK, LDWORK,
     $            CZERO, F, LDF )
*
      VMAX = ZERO
      DO 90 J = 1, M
         DO 80 I = 1, M
            VMAX = MAX( VMAX, CABS1( E( I, J )-F( I, J ) ) )
   80    CONTINUE
   90 CONTINUE
      VMAX = VMAX / ( EPS*MAX( ANORM, BNORM ) )
      IF( VMAX.GT.RMAX ) THEN
         LMAX( 4 ) = KNT
         RMAX = VMAX
      END IF
*
      GO TO 10
*
  100 CONTINUE
*
      WRITE( NOUT, FMT = 9999 )
 9999 FORMAT( 1X, '.. test output of CGGBAK .. ' )
*
      WRITE( NOUT, FMT = 9998 )RMAX
 9998 FORMAT( ' value of largest test error                  =', E12.3 )
      WRITE( NOUT, FMT = 9997 )LMAX( 1 )
 9997 FORMAT( ' example number where CGGBAL info is not 0    =', I4 )
      WRITE( NOUT, FMT = 9996 )LMAX( 2 )
 9996 FORMAT( ' example number where CGGBAK(L) info is not 0 =', I4 )
      WRITE( NOUT, FMT = 9995 )LMAX( 3 )
 9995 FORMAT( ' example number where CGGBAK(R) info is not 0 =', I4 )
      WRITE( NOUT, FMT = 9994 )LMAX( 4 )
 9994 FORMAT( ' example number having largest error          =', I4 )
      WRITE( NOUT, FMT = 9992 )NINFO
 9992 FORMAT( ' number of examples where info is not 0       =', I4 )
      WRITE( NOUT, FMT = 9991 )KNT
 9991 FORMAT( ' total number of examples tested              =', I4 )
*
      RETURN
*
*     End of CCHKGK
*
      END
