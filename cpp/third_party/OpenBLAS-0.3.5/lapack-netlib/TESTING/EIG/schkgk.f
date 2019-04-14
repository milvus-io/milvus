*> \brief \b SCHKGK
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SCHKGK( NIN, NOUT )
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
*> SCHKGK tests SGGBAK, a routine for backward balancing  of
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
*> \ingroup single_eig
*
*  =====================================================================
      SUBROUTINE SCHKGK( NIN, NOUT )
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
      INTEGER            LDE, LDF, LDWORK
      PARAMETER          ( LDE = 50, LDF = 50, LDWORK = 50 )
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, IHI, ILO, INFO, J, KNT, M, N, NINFO
      REAL               ANORM, BNORM, EPS, RMAX, VMAX
*     ..
*     .. Local Arrays ..
      INTEGER            LMAX( 4 )
      REAL               A( LDA, LDA ), AF( LDA, LDA ), B( LDB, LDB ),
     $                   BF( LDB, LDB ), E( LDE, LDE ), F( LDF, LDF ),
     $                   LSCALE( LDA ), RSCALE( LDA ), VL( LDVL, LDVL ),
     $                   VLF( LDVL, LDVL ), VR( LDVR, LDVR ),
     $                   VRF( LDVR, LDVR ), WORK( LDWORK, LDWORK )
*     ..
*     .. External Functions ..
      REAL               SLAMCH, SLANGE
      EXTERNAL           SLAMCH, SLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           SGEMM, SGGBAK, SGGBAL, SLACPY
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX
*     ..
*     .. Executable Statements ..
*
*     Initialization
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
      ANORM = SLANGE( 'M', N, N, A, LDA, WORK )
      BNORM = SLANGE( 'M', N, N, B, LDB, WORK )
*
      CALL SLACPY( 'FULL', N, N, A, LDA, AF, LDA )
      CALL SLACPY( 'FULL', N, N, B, LDB, BF, LDB )
*
      CALL SGGBAL( 'B', N, A, LDA, B, LDB, ILO, IHI, LSCALE, RSCALE,
     $             WORK, INFO )
      IF( INFO.NE.0 ) THEN
         NINFO = NINFO + 1
         LMAX( 1 ) = KNT
      END IF
*
      CALL SLACPY( 'FULL', N, M, VL, LDVL, VLF, LDVL )
      CALL SLACPY( 'FULL', N, M, VR, LDVR, VRF, LDVR )
*
      CALL SGGBAK( 'B', 'L', N, ILO, IHI, LSCALE, RSCALE, M, VL, LDVL,
     $             INFO )
      IF( INFO.NE.0 ) THEN
         NINFO = NINFO + 1
         LMAX( 2 ) = KNT
      END IF
*
      CALL SGGBAK( 'B', 'R', N, ILO, IHI, LSCALE, RSCALE, M, VR, LDVR,
     $             INFO )
      IF( INFO.NE.0 ) THEN
         NINFO = NINFO + 1
         LMAX( 3 ) = KNT
      END IF
*
*     Test of SGGBAK
*
*     Check tilde(VL)'*A*tilde(VR) - VL'*tilde(A)*VR
*     where tilde(A) denotes the transformed matrix.
*
      CALL SGEMM( 'N', 'N', N, M, N, ONE, AF, LDA, VR, LDVR, ZERO, WORK,
     $            LDWORK )
      CALL SGEMM( 'T', 'N', M, M, N, ONE, VL, LDVL, WORK, LDWORK, ZERO,
     $            E, LDE )
*
      CALL SGEMM( 'N', 'N', N, M, N, ONE, A, LDA, VRF, LDVR, ZERO, WORK,
     $            LDWORK )
      CALL SGEMM( 'T', 'N', M, M, N, ONE, VLF, LDVL, WORK, LDWORK, ZERO,
     $            F, LDF )
*
      VMAX = ZERO
      DO 70 J = 1, M
         DO 60 I = 1, M
            VMAX = MAX( VMAX, ABS( E( I, J )-F( I, J ) ) )
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
      CALL SGEMM( 'N', 'N', N, M, N, ONE, BF, LDB, VR, LDVR, ZERO, WORK,
     $            LDWORK )
      CALL SGEMM( 'T', 'N', M, M, N, ONE, VL, LDVL, WORK, LDWORK, ZERO,
     $            E, LDE )
*
      CALL SGEMM( 'N', 'N', N, M, N, ONE, B, LDB, VRF, LDVR, ZERO, WORK,
     $            LDWORK )
      CALL SGEMM( 'T', 'N', M, M, N, ONE, VLF, LDVL, WORK, LDWORK, ZERO,
     $            F, LDF )
*
      VMAX = ZERO
      DO 90 J = 1, M
         DO 80 I = 1, M
            VMAX = MAX( VMAX, ABS( E( I, J )-F( I, J ) ) )
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
 9999 FORMAT( 1X, '.. test output of SGGBAK .. ' )
*
      WRITE( NOUT, FMT = 9998 )RMAX
 9998 FORMAT( ' value of largest test error                  =', E12.3 )
      WRITE( NOUT, FMT = 9997 )LMAX( 1 )
 9997 FORMAT( ' example number where SGGBAL info is not 0    =', I4 )
      WRITE( NOUT, FMT = 9996 )LMAX( 2 )
 9996 FORMAT( ' example number where SGGBAK(L) info is not 0 =', I4 )
      WRITE( NOUT, FMT = 9995 )LMAX( 3 )
 9995 FORMAT( ' example number where SGGBAK(R) info is not 0 =', I4 )
      WRITE( NOUT, FMT = 9994 )LMAX( 4 )
 9994 FORMAT( ' example number having largest error          =', I4 )
      WRITE( NOUT, FMT = 9992 )NINFO
 9992 FORMAT( ' number of examples where info is not 0       =', I4 )
      WRITE( NOUT, FMT = 9991 )KNT
 9991 FORMAT( ' total number of examples tested              =', I4 )
*
      RETURN
*
*     End of SCHKGK
*
      END
