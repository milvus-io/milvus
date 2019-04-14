*> \brief \b DSECNDTST
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*      PROGRAM DSECNDTST
*
*  Authors:
*  ========
*
*> \author Univ. of Tennessee
*> \author Univ. of California Berkeley
*> \author Univ. of Colorado Denver
*> \author NAG Ltd.
*
*> \date November 2017
*
*> \ingroup auxOTHERauxiliary
*
*  =====================================================================      PROGRAM DSECNDTST
*
*  -- LAPACK test routine (version 3.8.0) --
*
*  -- LAPACK computational routine (version 3.8.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2017
*
* =====================================================================
*
*     .. Parameters ..
      INTEGER            NMAX, ITS
      PARAMETER          ( NMAX = 1000, ITS = 50000 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, J
      DOUBLE PRECISION   ALPHA, AVG, T1, T2, TNOSEC, TOTAL
*     ..
*     .. Local Arrays ..
      DOUBLE PRECISION   X( NMAX ), Y( NMAX )
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DSECND
      EXTERNAL           DSECND
*     ..
*     .. External Subroutines ..
      EXTERNAL           MYSUB
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE
*     ..
*     .. Executable Statements ..
*
*    .. Figure TOTAL flops ..
      TOTAL = DBLE(NMAX) * DBLE(ITS) * 2.0
*
*     Initialize X and Y
*
      DO 10 I = 1, NMAX
         X( I ) = DBLE( 1 ) / DBLE( I )
         Y( I ) = DBLE( NMAX-I ) / DBLE( NMAX )
   10 CONTINUE
      ALPHA = 0.315D0
*
*     Time TOTAL SAXPY operations
*
      T1 = DSECND( )
      DO 30 J = 1, ITS
         DO 20 I = 1, NMAX
            Y( I ) = Y( I ) + ALPHA*X( I )
   20    CONTINUE
         ALPHA = -ALPHA
   30 CONTINUE
      T2 = DSECND( )
      TNOSEC = T2 - T1
      WRITE( 6, 9999 )TOTAL, TNOSEC
      IF( TNOSEC.GT.0.0 ) THEN
         WRITE( 6, 9998 )(TOTAL/1.0D6)/TNOSEC
      ELSE
         WRITE( 6, 9994 )
      END IF
*
*     Time TOTAL DAXPY operations with DSECND in the outer loop
*
      T1 = DSECND( )
      DO 50 J = 1, ITS
         DO 40 I = 1, NMAX
            Y( I ) = Y( I ) + ALPHA*X( I )
   40    CONTINUE
         ALPHA = -ALPHA
         T2 = DSECND( )
   50 CONTINUE
*
*     Compute the time used in milliseconds used by an average call
*     to DSECND.
*
      WRITE( 6, 9997 )T2 - T1
      AVG = ( ( T2-T1 ) - TNOSEC ) * 1000.0D+00/DBLE( ITS )
      IF( AVG.GT.0.0)
     $   WRITE( 6, 9996 )AVG
*
*     Compute the equivalent number of floating point operations used
*     by an average call to DSECND.
*
      IF(( AVG.GT.0.0 ).AND.( TNOSEC.GT.0.0 ))
     $   WRITE( 6, 9995 )(AVG/1000) * TOTAL / TNOSEC
*
 9999 FORMAT( ' Time for ', G10.3,' DAXPY ops = ', G10.3, ' seconds' )
 9998 FORMAT( ' DAXPY performance rate        = ', G10.3, ' mflops ' )
 9997 FORMAT( ' Including DSECND, time        = ', G10.3, ' seconds' )
 9996 FORMAT( ' Average time for DSECND       = ', G10.3,
     $      ' milliseconds' )
 9995 FORMAT( ' Equivalent floating point ops = ', G10.3, ' ops' )
 9994 FORMAT( ' *** Warning:  Time for operations was less or equal',
     $        ' than zero => timing in TESTING might be dubious' )
      CALL MYSUB(NMAX,X,Y)
      END
      SUBROUTINE MYSUB(N,X,Y)
      INTEGER N
      DOUBLE PRECISION X(N), Y(N)
      RETURN
      END
