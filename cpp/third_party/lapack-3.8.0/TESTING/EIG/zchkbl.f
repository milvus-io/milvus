*> \brief \b ZCHKBL
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZCHKBL( NIN, NOUT )
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
*> ZCHKBL tests ZGEBAL, a routine for balancing a general complex
*> matrix and isolating some of its eigenvalues.
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
*> \ingroup complex16_eig
*
*  =====================================================================
      SUBROUTINE ZCHKBL( NIN, NOUT )
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
      INTEGER            LDA
      PARAMETER          ( LDA = 20 )
      DOUBLE PRECISION   ZERO
      PARAMETER          ( ZERO = 0.0D+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, IHI, IHIIN, ILO, ILOIN, INFO, J, KNT, N,
     $                   NINFO
      DOUBLE PRECISION   ANORM, MEPS, RMAX, SFMIN, TEMP, VMAX
      COMPLEX*16         CDUM
*     ..
*     .. Local Arrays ..
      INTEGER            LMAX( 3 )
      DOUBLE PRECISION   DUMMY( 1 ), SCALE( LDA ), SCALIN( LDA )
      COMPLEX*16         A( LDA, LDA ), AIN( LDA, LDA )
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH, ZLANGE
      EXTERNAL           DLAMCH, ZLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZGEBAL
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, DIMAG, MAX
*     ..
*     .. Statement Functions ..
      DOUBLE PRECISION   CABS1
*     ..
*     .. Statement Function definitions ..
      CABS1( CDUM ) = ABS( DBLE( CDUM ) ) + ABS( DIMAG( CDUM ) )
*     ..
*     .. Executable Statements ..
*
      LMAX( 1 ) = 0
      LMAX( 2 ) = 0
      LMAX( 3 ) = 0
      NINFO = 0
      KNT = 0
      RMAX = ZERO
      VMAX = ZERO
      SFMIN = DLAMCH( 'S' )
      MEPS = DLAMCH( 'E' )
*
   10 CONTINUE
*
      READ( NIN, FMT = * )N
      IF( N.EQ.0 )
     $   GO TO 70
      DO 20 I = 1, N
         READ( NIN, FMT = * )( A( I, J ), J = 1, N )
   20 CONTINUE
*
      READ( NIN, FMT = * )ILOIN, IHIIN
      DO 30 I = 1, N
         READ( NIN, FMT = * )( AIN( I, J ), J = 1, N )
   30 CONTINUE
      READ( NIN, FMT = * )( SCALIN( I ), I = 1, N )
*
      ANORM = ZLANGE( 'M', N, N, A, LDA, DUMMY )
      KNT = KNT + 1
      CALL ZGEBAL( 'B', N, A, LDA, ILO, IHI, SCALE, INFO )
*
      IF( INFO.NE.0 ) THEN
         NINFO = NINFO + 1
         LMAX( 1 ) = KNT
      END IF
*
      IF( ILO.NE.ILOIN .OR. IHI.NE.IHIIN ) THEN
         NINFO = NINFO + 1
         LMAX( 2 ) = KNT
      END IF
*
      DO 50 I = 1, N
         DO 40 J = 1, N
            TEMP = MAX( CABS1( A( I, J ) ), CABS1( AIN( I, J ) ) )
            TEMP = MAX( TEMP, SFMIN )
            VMAX = MAX( VMAX, CABS1( A( I, J )-AIN( I, J ) ) / TEMP )
   40    CONTINUE
   50 CONTINUE
*
      DO 60 I = 1, N
         TEMP = MAX( SCALE( I ), SCALIN( I ) )
         TEMP = MAX( TEMP, SFMIN )
         VMAX = MAX( VMAX, ABS( SCALE( I )-SCALIN( I ) ) / TEMP )
   60 CONTINUE
*
      IF( VMAX.GT.RMAX ) THEN
         LMAX( 3 ) = KNT
         RMAX = VMAX
      END IF
*
      GO TO 10
*
   70 CONTINUE
*
      WRITE( NOUT, FMT = 9999 )
 9999 FORMAT( 1X, '.. test output of ZGEBAL .. ' )
*
      WRITE( NOUT, FMT = 9998 )RMAX
 9998 FORMAT( 1X, 'value of largest test error            = ', D12.3 )
      WRITE( NOUT, FMT = 9997 )LMAX( 1 )
 9997 FORMAT( 1X, 'example number where info is not zero  = ', I4 )
      WRITE( NOUT, FMT = 9996 )LMAX( 2 )
 9996 FORMAT( 1X, 'example number where ILO or IHI wrong  = ', I4 )
      WRITE( NOUT, FMT = 9995 )LMAX( 3 )
 9995 FORMAT( 1X, 'example number having largest error    = ', I4 )
      WRITE( NOUT, FMT = 9994 )NINFO
 9994 FORMAT( 1X, 'number of examples where info is not 0 = ', I4 )
      WRITE( NOUT, FMT = 9993 )KNT
 9993 FORMAT( 1X, 'total number of examples tested        = ', I4 )
*
      RETURN
*
*     End of ZCHKBL
*
      END
