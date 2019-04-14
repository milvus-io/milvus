*> \brief \b DDRVRF4
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DDRVRF4( NOUT, NN, NVAL, THRESH, C1, C2, LDC, CRF, A,
*      +                    LDA, D_WORK_DLANGE )
*
*       .. Scalar Arguments ..
*       INTEGER            LDA, LDC, NN, NOUT
*       DOUBLE PRECISION   THRESH
*       ..
*       .. Array Arguments ..
*       INTEGER            NVAL( NN )
*       DOUBLE PRECISION   A( LDA, * ), C1( LDC, * ), C2( LDC, *),
*      +                   CRF( * ), D_WORK_DLANGE( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DDRVRF4 tests the LAPACK RFP routines:
*>     DSFRK
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] NOUT
*> \verbatim
*>          NOUT is INTEGER
*>                The unit number for output.
*> \endverbatim
*>
*> \param[in] NN
*> \verbatim
*>          NN is INTEGER
*>                The number of values of N contained in the vector NVAL.
*> \endverbatim
*>
*> \param[in] NVAL
*> \verbatim
*>          NVAL is INTEGER array, dimension (NN)
*>                The values of the matrix dimension N.
*> \endverbatim
*>
*> \param[in] THRESH
*> \verbatim
*>          THRESH is DOUBLE PRECISION
*>                The threshold value for the test ratios.  A result is
*>                included in the output file if RESULT >= THRESH.  To
*>                have every test ratio printed, use THRESH = 0.
*> \endverbatim
*>
*> \param[out] C1
*> \verbatim
*>          C1 is DOUBLE PRECISION array,
*>                dimension (LDC,NMAX)
*> \endverbatim
*>
*> \param[out] C2
*> \verbatim
*>          C2 is DOUBLE PRECISION array,
*>                dimension (LDC,NMAX)
*> \endverbatim
*>
*> \param[in] LDC
*> \verbatim
*>          LDC is INTEGER
*>                The leading dimension of the array A.
*>                LDA >= max(1,NMAX).
*> \endverbatim
*>
*> \param[out] CRF
*> \verbatim
*>          CRF is DOUBLE PRECISION array,
*>                dimension ((NMAX*(NMAX+1))/2).
*> \endverbatim
*>
*> \param[out] A
*> \verbatim
*>          A is DOUBLE PRECISION array,
*>                dimension (LDA,NMAX)
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>                The leading dimension of the array A.  LDA >= max(1,NMAX).
*> \endverbatim
*>
*> \param[out] D_WORK_DLANGE
*> \verbatim
*>          D_WORK_DLANGE is DOUBLE PRECISION array, dimension (NMAX)
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
      SUBROUTINE DDRVRF4( NOUT, NN, NVAL, THRESH, C1, C2, LDC, CRF, A,
     +                    LDA, D_WORK_DLANGE )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            LDA, LDC, NN, NOUT
      DOUBLE PRECISION   THRESH
*     ..
*     .. Array Arguments ..
      INTEGER            NVAL( NN )
      DOUBLE PRECISION   A( LDA, * ), C1( LDC, * ), C2( LDC, *),
     +                   CRF( * ), D_WORK_DLANGE( * )
*     ..
*
*  =====================================================================
*     ..
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE  = 1.0D+0 )
      INTEGER            NTESTS
      PARAMETER          ( NTESTS = 1 )
*     ..
*     .. Local Scalars ..
      CHARACTER          UPLO, CFORM, TRANS
      INTEGER            I, IFORM, IIK, IIN, INFO, IUPLO, J, K, N,
     +                   NFAIL, NRUN, IALPHA, ITRANS
      DOUBLE PRECISION   ALPHA, BETA, EPS, NORMA, NORMC
*     ..
*     .. Local Arrays ..
      CHARACTER          UPLOS( 2 ), FORMS( 2 ), TRANSS( 2 )
      INTEGER            ISEED( 4 ), ISEEDY( 4 )
      DOUBLE PRECISION   RESULT( NTESTS )
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH, DLARND, DLANGE
      EXTERNAL           DLAMCH, DLARND, DLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           DSYRK, DSFRK, DTFTTR, DTRTTF
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX
*     ..
*     .. Scalars in Common ..
      CHARACTER*32       SRNAMT
*     ..
*     .. Common blocks ..
      COMMON             / SRNAMC / SRNAMT
*     ..
*     .. Data statements ..
      DATA               ISEEDY / 1988, 1989, 1990, 1991 /
      DATA               UPLOS  / 'U', 'L' /
      DATA               FORMS  / 'N', 'T' /
      DATA               TRANSS / 'N', 'T' /
*     ..
*     .. Executable Statements ..
*
*     Initialize constants and the random number seed.
*
      NRUN = 0
      NFAIL = 0
      INFO = 0
      DO 10 I = 1, 4
         ISEED( I ) = ISEEDY( I )
   10 CONTINUE
      EPS = DLAMCH( 'Precision' )
*
      DO 150 IIN = 1, NN
*
         N = NVAL( IIN )
*
         DO 140 IIK = 1, NN
*
            K = NVAL( IIN )
*
            DO 130 IFORM = 1, 2
*
               CFORM = FORMS( IFORM )
*
               DO 120 IUPLO = 1, 2
*
                  UPLO = UPLOS( IUPLO )
*
                  DO 110 ITRANS = 1, 2
*
                     TRANS = TRANSS( ITRANS )
*
                     DO 100 IALPHA = 1, 4
*
                        IF ( IALPHA.EQ. 1) THEN
                           ALPHA = ZERO
                           BETA = ZERO
                        ELSE IF ( IALPHA.EQ. 2) THEN
                           ALPHA = ONE
                           BETA = ZERO
                        ELSE IF ( IALPHA.EQ. 3) THEN
                           ALPHA = ZERO
                           BETA = ONE
                        ELSE
                           ALPHA = DLARND( 2, ISEED )
                           BETA = DLARND( 2, ISEED )
                        END IF
*
*                       All the parameters are set:
*                          CFORM, UPLO, TRANS, M, N,
*                          ALPHA, and BETA
*                       READY TO TEST!
*
                        NRUN = NRUN + 1
*
                        IF ( ITRANS.EQ.1 ) THEN
*
*                          In this case we are NOTRANS, so A is N-by-K
*
                           DO J = 1, K
                              DO I = 1, N
                                 A( I, J) = DLARND( 2, ISEED )
                              END DO
                           END DO
*
                           NORMA = DLANGE( 'I', N, K, A, LDA,
     +                                      D_WORK_DLANGE )
*

                        ELSE
*
*                          In this case we are TRANS, so A is K-by-N
*
                           DO J = 1,N
                              DO I = 1, K
                                 A( I, J) = DLARND( 2, ISEED )
                              END DO
                           END DO
*
                           NORMA = DLANGE( 'I', K, N, A, LDA,
     +                                      D_WORK_DLANGE )
*
                        END IF
*
*                       Generate C1 our N--by--N symmetric matrix.
*                       Make sure C2 has the same upper/lower part,
*                       (the one that we do not touch), so
*                       copy the initial C1 in C2 in it.
*
                        DO J = 1, N
                           DO I = 1, N
                              C1( I, J) = DLARND( 2, ISEED )
                              C2(I,J) = C1(I,J)
                           END DO
                        END DO
*
*                       (See comment later on for why we use DLANGE and
*                       not DLANSY for C1.)
*
                        NORMC = DLANGE( 'I', N, N, C1, LDC,
     +                                      D_WORK_DLANGE )
*
                        SRNAMT = 'DTRTTF'
                        CALL DTRTTF( CFORM, UPLO, N, C1, LDC, CRF,
     +                               INFO )
*
*                       call dsyrk the BLAS routine -> gives C1
*
                        SRNAMT = 'DSYRK '
                        CALL DSYRK( UPLO, TRANS, N, K, ALPHA, A, LDA,
     +                              BETA, C1, LDC )
*
*                       call dsfrk the RFP routine -> gives CRF
*
                        SRNAMT = 'DSFRK '
                        CALL DSFRK( CFORM, UPLO, TRANS, N, K, ALPHA, A,
     +                              LDA, BETA, CRF )
*
*                       convert CRF in full format -> gives C2
*
                        SRNAMT = 'DTFTTR'
                        CALL DTFTTR( CFORM, UPLO, N, CRF, C2, LDC,
     +                               INFO )
*
*                       compare C1 and C2
*
                        DO J = 1, N
                           DO I = 1, N
                              C1(I,J) = C1(I,J)-C2(I,J)
                           END DO
                        END DO
*
*                       Yes, C1 is symmetric so we could call DLANSY,
*                       but we want to check the upper part that is
*                       supposed to be unchanged and the diagonal that
*                       is supposed to be real -> DLANGE
*
                        RESULT(1) = DLANGE( 'I', N, N, C1, LDC,
     +                                      D_WORK_DLANGE )
                        RESULT(1) = RESULT(1)
     +                              / MAX( ABS( ALPHA ) * NORMA
     +                                   + ABS( BETA ) , ONE )
     +                              / MAX( N , 1 ) / EPS
*
                        IF( RESULT(1).GE.THRESH ) THEN
                           IF( NFAIL.EQ.0 ) THEN
                              WRITE( NOUT, * )
                              WRITE( NOUT, FMT = 9999 )
                           END IF
                           WRITE( NOUT, FMT = 9997 ) 'DSFRK',
     +                        CFORM, UPLO, TRANS, N, K, RESULT(1)
                           NFAIL = NFAIL + 1
                        END IF
*
  100                CONTINUE
  110             CONTINUE
  120          CONTINUE
  130       CONTINUE
  140    CONTINUE
  150 CONTINUE
*
*     Print a summary of the results.
*
      IF ( NFAIL.EQ.0 ) THEN
         WRITE( NOUT, FMT = 9996 ) 'DSFRK', NRUN
      ELSE
         WRITE( NOUT, FMT = 9995 ) 'DSFRK', NFAIL, NRUN
      END IF
*
 9999 FORMAT( 1X, ' *** Error(s) or Failure(s) while testing DSFRK
     +         ***')
 9997 FORMAT( 1X, '     Failure in ',A5,', CFORM=''',A1,''',',
     + ' UPLO=''',A1,''',',' TRANS=''',A1,''',', ' N=',I3,', K =', I3,
     + ', test=',G12.5)
 9996 FORMAT( 1X, 'All tests for ',A5,' auxiliary routine passed the ',
     +        'threshold ( ',I5,' tests run)')
 9995 FORMAT( 1X, A6, ' auxiliary routine: ',I5,' out of ',I5,
     +        ' tests failed to pass the threshold')
*
      RETURN
*
*     End of DDRVRF4
*
      END
