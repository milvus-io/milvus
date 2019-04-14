*> \brief \b SCHKBK
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SCHKBK( NIN, NOUT )
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
*> SCHKBK tests SGEBAK, a routine for backward transformation of
*> the computed right or left eigenvectors if the original matrix
*> was preprocessed by balance subroutine SGEBAL.
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
      SUBROUTINE SCHKBK( NIN, NOUT )
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
* ======================================================================
*
*     .. Parameters ..
      INTEGER            LDE
      PARAMETER          ( LDE = 20 )
      REAL               ZERO
      PARAMETER          ( ZERO = 0.0E0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, IHI, ILO, INFO, J, KNT, N, NINFO
      REAL               EPS, RMAX, SAFMIN, VMAX, X
*     ..
*     .. Local Arrays ..
      INTEGER            LMAX( 2 )
      REAL               E( LDE, LDE ), EIN( LDE, LDE ), SCALE( LDE )
*     ..
*     .. External Functions ..
      REAL               SLAMCH
      EXTERNAL           SLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           SGEBAK
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX
*     ..
*     .. Executable Statements ..
*
      LMAX( 1 ) = 0
      LMAX( 2 ) = 0
      NINFO = 0
      KNT = 0
      RMAX = ZERO
      EPS = SLAMCH( 'E' )
      SAFMIN = SLAMCH( 'S' )
*
   10 CONTINUE
*
      READ( NIN, FMT = * )N, ILO, IHI
      IF( N.EQ.0 )
     $   GO TO 60
*
      READ( NIN, FMT = * )( SCALE( I ), I = 1, N )
      DO 20 I = 1, N
         READ( NIN, FMT = * )( E( I, J ), J = 1, N )
   20 CONTINUE
*
      DO 30 I = 1, N
         READ( NIN, FMT = * )( EIN( I, J ), J = 1, N )
   30 CONTINUE
*
      KNT = KNT + 1
      CALL SGEBAK( 'B', 'R', N, ILO, IHI, SCALE, N, E, LDE, INFO )
*
      IF( INFO.NE.0 ) THEN
         NINFO = NINFO + 1
         LMAX( 1 ) = KNT
      END IF
*
      VMAX = ZERO
      DO 50 I = 1, N
         DO 40 J = 1, N
            X = ABS( E( I, J )-EIN( I, J ) ) / EPS
            IF( ABS( E( I, J ) ).GT.SAFMIN )
     $         X = X / ABS( E( I, J ) )
            VMAX = MAX( VMAX, X )
   40    CONTINUE
   50 CONTINUE
*
      IF( VMAX.GT.RMAX ) THEN
         LMAX( 2 ) = KNT
         RMAX = VMAX
      END IF
*
      GO TO 10
*
   60 CONTINUE
*
      WRITE( NOUT, FMT = 9999 )
 9999 FORMAT( 1X, '.. test output of SGEBAK .. ' )
*
      WRITE( NOUT, FMT = 9998 )RMAX
 9998 FORMAT( 1X, 'value of largest test error             = ', E12.3 )
      WRITE( NOUT, FMT = 9997 )LMAX( 1 )
 9997 FORMAT( 1X, 'example number where info is not zero   = ', I4 )
      WRITE( NOUT, FMT = 9996 )LMAX( 2 )
 9996 FORMAT( 1X, 'example number having largest error     = ', I4 )
      WRITE( NOUT, FMT = 9995 )NINFO
 9995 FORMAT( 1X, 'number of examples where info is not 0  = ', I4 )
      WRITE( NOUT, FMT = 9994 )KNT
 9994 FORMAT( 1X, 'total number of examples tested         = ', I4 )
*
      RETURN
*
*     End of SCHKBK
*
      END
