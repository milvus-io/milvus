*> \brief \b SCHKQRT
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SCHKTSQR( THRESH, TSTERR, NM, MVAL, NN, NVAL, NNB,
*                           NBVAL, NOUT )
*
*       .. Scalar Arguments ..
*       LOGICAL            TSTERR
*       INTEGER            NM, NN, NNB, NOUT
*       REAL               THRESH
*       ..
*       .. Array Arguments ..
*       INTEGER            MVAL( * ), NBVAL( * ), NVAL( * )
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SCHKTSQR tests SGETSQR and SORMTSQR.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] THRESH
*> \verbatim
*>          THRESH is REAL
*>          The threshold value for the test ratios.  A result is
*>          included in the output file if RESULT >= THRESH.  To have
*>          every test ratio printed, use THRESH = 0.
*> \endverbatim
*>
*> \param[in] TSTERR
*> \verbatim
*>          TSTERR is LOGICAL
*>          Flag that indicates whether error exits are to be tested.
*> \endverbatim
*>
*> \param[in] NM
*> \verbatim
*>          NM is INTEGER
*>          The number of values of M contained in the vector MVAL.
*> \endverbatim
*>
*> \param[in] MVAL
*> \verbatim
*>          MVAL is INTEGER array, dimension (NM)
*>          The values of the matrix row dimension M.
*> \endverbatim
*>
*> \param[in] NN
*> \verbatim
*>          NN is INTEGER
*>          The number of values of N contained in the vector NVAL.
*> \endverbatim
*>
*> \param[in] NVAL
*> \verbatim
*>          NVAL is INTEGER array, dimension (NN)
*>          The values of the matrix column dimension N.
*> \endverbatim
*>
*> \param[in] NNB
*> \verbatim
*>          NNB is INTEGER
*>          The number of values of NB contained in the vector NBVAL.
*> \endverbatim
*>
*> \param[in] NBVAL
*> \verbatim
*>          NBVAL is INTEGER array, dimension (NBVAL)
*>          The values of the blocksize NB.
*> \endverbatim
*>
*> \param[in] NOUT
*> \verbatim
*>          NOUT is INTEGER
*>          The unit number for output.
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
*> \ingroup double_lin
*
*  =====================================================================
      SUBROUTINE SCHKTSQR( THRESH, TSTERR, NM, MVAL, NN, NVAL, NNB,
     $                     NBVAL, NOUT )
      IMPLICIT NONE
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      LOGICAL            TSTERR
      INTEGER            NM, NN, NNB, NOUT
      REAL               THRESH
*     ..
*     .. Array Arguments ..
      INTEGER            MVAL( * ), NBVAL( * ), NVAL( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            NTESTS
      PARAMETER          ( NTESTS = 6 )
*     ..
*     .. Local Scalars ..
      CHARACTER*3        PATH
      INTEGER            I, J, K, T, M, N, NB, NFAIL, NERRS, NRUN, INB,
     $                   MINMN, MB, IMB
*
*     .. Local Arrays ..
      REAL   RESULT( NTESTS )
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAERH, ALAHD, ALASUM, SERRTSQR,
     $                   STSQR01, XLAENV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC  MAX, MIN
*     ..
*     .. Scalars in Common ..
      LOGICAL            LERR, OK
      CHARACTER*32       SRNAMT
      INTEGER            INFOT, NUNIT
*     ..
*     .. Common blocks ..
      COMMON             / INFOC / INFOT, NUNIT, OK, LERR
      COMMON             / SRNAMC / SRNAMT
*     ..
*     .. Executable Statements ..
*
*     Initialize constants
*
      PATH( 1: 1 ) = 'S'
      PATH( 2: 3 ) = 'TS'
      NRUN = 0
      NFAIL = 0
      NERRS = 0
*
*     Test the error exits
*
      IF( TSTERR ) CALL SERRTSQR( PATH, NOUT )
      INFOT = 0
*
*     Do for each value of M in MVAL.
*
      DO I = 1, NM
         M = MVAL( I )
*
*        Do for each value of N in NVAL.
*
         DO J = 1, NN
            N = NVAL( J )
              IF (MIN(M,N).NE.0) THEN
              DO INB = 1, NNB
                MB = NBVAL( INB )
                  CALL XLAENV( 1, MB )
                  DO IMB = 1, NNB
                    NB = NBVAL( IMB )
                    CALL XLAENV( 2, NB )
*
*                 Test SGEQR and SGEMQR
*
                    CALL STSQR01('TS', M, N, MB, NB, RESULT )
*
*                 Print information about the tests that did not
*                 pass the threshold.
*
                    DO T = 1, NTESTS
                      IF( RESULT( T ).GE.THRESH ) THEN
                        IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                       CALL ALAHD( NOUT, PATH )
                        WRITE( NOUT, FMT = 9999 )M, N, MB, NB,
     $                       T, RESULT( T )
                        NFAIL = NFAIL + 1
                      END IF
                    END DO
                    NRUN = NRUN + NTESTS
                  END DO
              END DO
              END IF
         END DO
      END DO
*
*     Do for each value of M in MVAL.
*
      DO I = 1, NM
         M = MVAL( I )
*
*        Do for each value of N in NVAL.
*
         DO J = 1, NN
            N = NVAL( J )
            IF (MIN(M,N).NE.0) THEN
              DO INB = 1, NNB
                MB = NBVAL( INB )
                  CALL XLAENV( 1, MB )
                  DO IMB = 1, NNB
                    NB = NBVAL( IMB )
                    CALL XLAENV( 2, NB )
*
*                 Test SGEQR and SGEMQR
*
                    CALL STSQR01('SW', M, N, MB, NB, RESULT )
*
*                 Print information about the tests that did not
*                 pass the threshold.
*
                    DO T = 1, NTESTS
                      IF( RESULT( T ).GE.THRESH ) THEN
                        IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                       CALL ALAHD( NOUT, PATH )
                        WRITE( NOUT, FMT = 9998 )M, N, MB, NB,
     $                       T, RESULT( T )
                        NFAIL = NFAIL + 1
                      END IF
                    END DO
                    NRUN = NRUN + NTESTS
                  END DO
              END DO
           END IF
         END DO
      END DO
*
*     Print a summary of the results.
*
      CALL ALASUM( PATH, NOUT, NFAIL, NRUN, NERRS )
*
 9999 FORMAT( 'TS: M=', I5, ', N=', I5, ', MB=', I5,
     $      ', NB=', I5,' test(', I2, ')=', G12.5 )
 9998 FORMAT( 'SW: M=', I5, ', N=', I5, ', MB=', I5,
     $      ', NB=', I5,' test(', I2, ')=', G12.5 )
      RETURN
*
*     End of SCHKQRT
*
      END
