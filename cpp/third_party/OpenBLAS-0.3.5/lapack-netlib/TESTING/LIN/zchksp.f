*> \brief \b ZCHKSP
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZCHKSP( DOTYPE, NN, NVAL, NNS, NSVAL, THRESH, TSTERR,
*                          NMAX, A, AFAC, AINV, B, X, XACT, WORK, RWORK,
*                          IWORK, NOUT )
*
*       .. Scalar Arguments ..
*       LOGICAL            TSTERR
*       INTEGER            NMAX, NN, NNS, NOUT
*       DOUBLE PRECISION   THRESH
*       ..
*       .. Array Arguments ..
*       LOGICAL            DOTYPE( * )
*       INTEGER            IWORK( * ), NSVAL( * ), NVAL( * )
*       DOUBLE PRECISION   RWORK( * )
*       COMPLEX*16         A( * ), AFAC( * ), AINV( * ), B( * ),
*      $                   WORK( * ), X( * ), XACT( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZCHKSP tests ZSPTRF, -TRI, -TRS, -RFS, and -CON
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] DOTYPE
*> \verbatim
*>          DOTYPE is LOGICAL array, dimension (NTYPES)
*>          The matrix types to be used for testing.  Matrices of type j
*>          (for 1 <= j <= NTYPES) are used for testing if DOTYPE(j) =
*>          .TRUE.; if DOTYPE(j) = .FALSE., then type j is not used.
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
*>          The values of the matrix dimension N.
*> \endverbatim
*>
*> \param[in] NNS
*> \verbatim
*>          NNS is INTEGER
*>          The number of values of NRHS contained in the vector NSVAL.
*> \endverbatim
*>
*> \param[in] NSVAL
*> \verbatim
*>          NSVAL is INTEGER array, dimension (NNS)
*>          The values of the number of right hand sides NRHS.
*> \endverbatim
*>
*> \param[in] THRESH
*> \verbatim
*>          THRESH is DOUBLE PRECISION
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
*> \param[in] NMAX
*> \verbatim
*>          NMAX is INTEGER
*>          The maximum value permitted for N, used in dimensioning the
*>          work arrays.
*> \endverbatim
*>
*> \param[out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension
*>                      (NMAX*(NMAX+1)/2)
*> \endverbatim
*>
*> \param[out] AFAC
*> \verbatim
*>          AFAC is COMPLEX*16 array, dimension
*>                      (NMAX*(NMAX+1)/2)
*> \endverbatim
*>
*> \param[out] AINV
*> \verbatim
*>          AINV is COMPLEX*16 array, dimension
*>                      (NMAX*(NMAX+1)/2)
*> \endverbatim
*>
*> \param[out] B
*> \verbatim
*>          B is COMPLEX*16 array, dimension (NMAX*NSMAX)
*>          where NSMAX is the largest entry in NSVAL.
*> \endverbatim
*>
*> \param[out] X
*> \verbatim
*>          X is COMPLEX*16 array, dimension (NMAX*NSMAX)
*> \endverbatim
*>
*> \param[out] XACT
*> \verbatim
*>          XACT is COMPLEX*16 array, dimension (NMAX*NSMAX)
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension
*>                      (NMAX*max(2,NSMAX))
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array,
*>                                 dimension (NMAX+2*NSMAX)
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (NMAX)
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
*> \ingroup complex16_lin
*
*  =====================================================================
      SUBROUTINE ZCHKSP( DOTYPE, NN, NVAL, NNS, NSVAL, THRESH, TSTERR,
     $                   NMAX, A, AFAC, AINV, B, X, XACT, WORK, RWORK,
     $                   IWORK, NOUT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      LOGICAL            TSTERR
      INTEGER            NMAX, NN, NNS, NOUT
      DOUBLE PRECISION   THRESH
*     ..
*     .. Array Arguments ..
      LOGICAL            DOTYPE( * )
      INTEGER            IWORK( * ), NSVAL( * ), NVAL( * )
      DOUBLE PRECISION   RWORK( * )
      COMPLEX*16         A( * ), AFAC( * ), AINV( * ), B( * ),
     $                   WORK( * ), X( * ), XACT( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO
      PARAMETER          ( ZERO = 0.0D+0 )
      INTEGER            NTYPES
      PARAMETER          ( NTYPES = 11 )
      INTEGER            NTESTS
      PARAMETER          ( NTESTS = 8 )
*     ..
*     .. Local Scalars ..
      LOGICAL            TRFCON, ZEROT
      CHARACTER          DIST, PACKIT, TYPE, UPLO, XTYPE
      CHARACTER*3        PATH
      INTEGER            I, I1, I2, IMAT, IN, INFO, IOFF, IRHS, IUPLO,
     $                   IZERO, J, K, KL, KU, LDA, MODE, N, NERRS,
     $                   NFAIL, NIMAT, NPP, NRHS, NRUN, NT
      DOUBLE PRECISION   ANORM, CNDNUM, RCOND, RCONDC
*     ..
*     .. Local Arrays ..
      CHARACTER          UPLOS( 2 )
      INTEGER            ISEED( 4 ), ISEEDY( 4 )
      DOUBLE PRECISION   RESULT( NTESTS )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      DOUBLE PRECISION   DGET06, ZLANSP
      EXTERNAL           LSAME, DGET06, ZLANSP
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAERH, ALAHD, ALASUM, ZCOPY, ZERRSY, ZGET04,
     $                   ZLACPY, ZLARHS, ZLATB4, ZLATMS, ZLATSP, ZPPT05,
     $                   ZSPCON, ZSPRFS, ZSPT01, ZSPT02, ZSPT03, ZSPTRF,
     $                   ZSPTRI, ZSPTRS
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN
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
*     .. Data statements ..
      DATA               ISEEDY / 1988, 1989, 1990, 1991 /
      DATA               UPLOS / 'U', 'L' /
*     ..
*     .. Executable Statements ..
*
*     Initialize constants and the random number seed.
*
      PATH( 1: 1 ) = 'Zomplex precision'
      PATH( 2: 3 ) = 'SP'
      NRUN = 0
      NFAIL = 0
      NERRS = 0
      DO 10 I = 1, 4
         ISEED( I ) = ISEEDY( I )
   10 CONTINUE
*
*     Test the error exits
*
      IF( TSTERR )
     $   CALL ZERRSY( PATH, NOUT )
      INFOT = 0
*
*     Do for each value of N in NVAL
*
      DO 170 IN = 1, NN
         N = NVAL( IN )
         LDA = MAX( N, 1 )
         XTYPE = 'N'
         NIMAT = NTYPES
         IF( N.LE.0 )
     $      NIMAT = 1
*
         DO 160 IMAT = 1, NIMAT
*
*           Do the tests only if DOTYPE( IMAT ) is true.
*
            IF( .NOT.DOTYPE( IMAT ) )
     $         GO TO 160
*
*           Skip types 3, 4, 5, or 6 if the matrix size is too small.
*
            ZEROT = IMAT.GE.3 .AND. IMAT.LE.6
            IF( ZEROT .AND. N.LT.IMAT-2 )
     $         GO TO 160
*
*           Do first for UPLO = 'U', then for UPLO = 'L'
*
            DO 150 IUPLO = 1, 2
               UPLO = UPLOS( IUPLO )
               IF( LSAME( UPLO, 'U' ) ) THEN
                  PACKIT = 'C'
               ELSE
                  PACKIT = 'R'
               END IF
*
               IF( IMAT.NE.NTYPES ) THEN
*
*                 Set up parameters with ZLATB4 and generate a test
*                 matrix with ZLATMS.
*
                  CALL ZLATB4( PATH, IMAT, N, N, TYPE, KL, KU, ANORM,
     $                         MODE, CNDNUM, DIST )
*
                  SRNAMT = 'ZLATMS'
                  CALL ZLATMS( N, N, DIST, ISEED, TYPE, RWORK, MODE,
     $                         CNDNUM, ANORM, KL, KU, PACKIT, A, LDA,
     $                         WORK, INFO )
*
*                 Check error code from ZLATMS.
*
                  IF( INFO.NE.0 ) THEN
                     CALL ALAERH( PATH, 'ZLATMS', INFO, 0, UPLO, N, N,
     $                            -1, -1, -1, IMAT, NFAIL, NERRS, NOUT )
                     GO TO 150
                  END IF
*
*                 For types 3-6, zero one or more rows and columns of
*                 the matrix to test that INFO is returned correctly.
*
                  IF( ZEROT ) THEN
                     IF( IMAT.EQ.3 ) THEN
                        IZERO = 1
                     ELSE IF( IMAT.EQ.4 ) THEN
                        IZERO = N
                     ELSE
                        IZERO = N / 2 + 1
                     END IF
*
                     IF( IMAT.LT.6 ) THEN
*
*                       Set row and column IZERO to zero.
*
                        IF( IUPLO.EQ.1 ) THEN
                           IOFF = ( IZERO-1 )*IZERO / 2
                           DO 20 I = 1, IZERO - 1
                              A( IOFF+I ) = ZERO
   20                      CONTINUE
                           IOFF = IOFF + IZERO
                           DO 30 I = IZERO, N
                              A( IOFF ) = ZERO
                              IOFF = IOFF + I
   30                      CONTINUE
                        ELSE
                           IOFF = IZERO
                           DO 40 I = 1, IZERO - 1
                              A( IOFF ) = ZERO
                              IOFF = IOFF + N - I
   40                      CONTINUE
                           IOFF = IOFF - IZERO
                           DO 50 I = IZERO, N
                              A( IOFF+I ) = ZERO
   50                      CONTINUE
                        END IF
                     ELSE
                        IF( IUPLO.EQ.1 ) THEN
*
*                          Set the first IZERO rows and columns to zero.
*
                           IOFF = 0
                           DO 70 J = 1, N
                              I2 = MIN( J, IZERO )
                              DO 60 I = 1, I2
                                 A( IOFF+I ) = ZERO
   60                         CONTINUE
                              IOFF = IOFF + J
   70                      CONTINUE
                        ELSE
*
*                          Set the last IZERO rows and columns to zero.
*
                           IOFF = 0
                           DO 90 J = 1, N
                              I1 = MAX( J, IZERO )
                              DO 80 I = I1, N
                                 A( IOFF+I ) = ZERO
   80                         CONTINUE
                              IOFF = IOFF + N - J
   90                      CONTINUE
                        END IF
                     END IF
                  ELSE
                     IZERO = 0
                  END IF
               ELSE
*
*                 Use a special block diagonal matrix to test alternate
*                 code for the 2 x 2 blocks.
*
                  CALL ZLATSP( UPLO, N, A, ISEED )
               END IF
*
*              Compute the L*D*L' or U*D*U' factorization of the matrix.
*
               NPP = N*( N+1 ) / 2
               CALL ZCOPY( NPP, A, 1, AFAC, 1 )
               SRNAMT = 'ZSPTRF'
               CALL ZSPTRF( UPLO, N, AFAC, IWORK, INFO )
*
*              Adjust the expected value of INFO to account for
*              pivoting.
*
               K = IZERO
               IF( K.GT.0 ) THEN
  100             CONTINUE
                  IF( IWORK( K ).LT.0 ) THEN
                     IF( IWORK( K ).NE.-K ) THEN
                        K = -IWORK( K )
                        GO TO 100
                     END IF
                  ELSE IF( IWORK( K ).NE.K ) THEN
                     K = IWORK( K )
                     GO TO 100
                  END IF
               END IF
*
*              Check error code from ZSPTRF.
*
               IF( INFO.NE.K )
     $            CALL ALAERH( PATH, 'ZSPTRF', INFO, K, UPLO, N, N, -1,
     $                         -1, -1, IMAT, NFAIL, NERRS, NOUT )
               IF( INFO.NE.0 ) THEN
                  TRFCON = .TRUE.
               ELSE
                  TRFCON = .FALSE.
               END IF
*
*+    TEST 1
*              Reconstruct matrix from factors and compute residual.
*
               CALL ZSPT01( UPLO, N, A, AFAC, IWORK, AINV, LDA, RWORK,
     $                      RESULT( 1 ) )
               NT = 1
*
*+    TEST 2
*              Form the inverse and compute the residual.
*
               IF( .NOT.TRFCON ) THEN
                  CALL ZCOPY( NPP, AFAC, 1, AINV, 1 )
                  SRNAMT = 'ZSPTRI'
                  CALL ZSPTRI( UPLO, N, AINV, IWORK, WORK, INFO )
*
*              Check error code from ZSPTRI.
*
                  IF( INFO.NE.0 )
     $               CALL ALAERH( PATH, 'ZSPTRI', INFO, 0, UPLO, N, N,
     $                            -1, -1, -1, IMAT, NFAIL, NERRS, NOUT )
*
                  CALL ZSPT03( UPLO, N, A, AINV, WORK, LDA, RWORK,
     $                         RCONDC, RESULT( 2 ) )
                  NT = 2
               END IF
*
*              Print information about the tests that did not pass
*              the threshold.
*
               DO 110 K = 1, NT
                  IF( RESULT( K ).GE.THRESH ) THEN
                     IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                  CALL ALAHD( NOUT, PATH )
                     WRITE( NOUT, FMT = 9999 )UPLO, N, IMAT, K,
     $                  RESULT( K )
                     NFAIL = NFAIL + 1
                  END IF
  110          CONTINUE
               NRUN = NRUN + NT
*
*              Do only the condition estimate if INFO is not 0.
*
               IF( TRFCON ) THEN
                  RCONDC = ZERO
                  GO TO 140
               END IF
*
               DO 130 IRHS = 1, NNS
                  NRHS = NSVAL( IRHS )
*
*+    TEST 3
*              Solve and compute residual for  A * X = B.
*
                  SRNAMT = 'ZLARHS'
                  CALL ZLARHS( PATH, XTYPE, UPLO, ' ', N, N, KL, KU,
     $                         NRHS, A, LDA, XACT, LDA, B, LDA, ISEED,
     $                         INFO )
                  CALL ZLACPY( 'Full', N, NRHS, B, LDA, X, LDA )
*
                  SRNAMT = 'ZSPTRS'
                  CALL ZSPTRS( UPLO, N, NRHS, AFAC, IWORK, X, LDA,
     $                         INFO )
*
*              Check error code from ZSPTRS.
*
                  IF( INFO.NE.0 )
     $               CALL ALAERH( PATH, 'ZSPTRS', INFO, 0, UPLO, N, N,
     $                            -1, -1, NRHS, IMAT, NFAIL, NERRS,
     $                            NOUT )
*
                  CALL ZLACPY( 'Full', N, NRHS, B, LDA, WORK, LDA )
                  CALL ZSPT02( UPLO, N, NRHS, A, X, LDA, WORK, LDA,
     $                         RWORK, RESULT( 3 ) )
*
*+    TEST 4
*              Check solution from generated exact solution.
*
                  CALL ZGET04( N, NRHS, X, LDA, XACT, LDA, RCONDC,
     $                         RESULT( 4 ) )
*
*+    TESTS 5, 6, and 7
*              Use iterative refinement to improve the solution.
*
                  SRNAMT = 'ZSPRFS'
                  CALL ZSPRFS( UPLO, N, NRHS, A, AFAC, IWORK, B, LDA, X,
     $                         LDA, RWORK, RWORK( NRHS+1 ), WORK,
     $                         RWORK( 2*NRHS+1 ), INFO )
*
*              Check error code from ZSPRFS.
*
                  IF( INFO.NE.0 )
     $               CALL ALAERH( PATH, 'ZSPRFS', INFO, 0, UPLO, N, N,
     $                            -1, -1, NRHS, IMAT, NFAIL, NERRS,
     $                            NOUT )
*
                  CALL ZGET04( N, NRHS, X, LDA, XACT, LDA, RCONDC,
     $                         RESULT( 5 ) )
                  CALL ZPPT05( UPLO, N, NRHS, A, B, LDA, X, LDA, XACT,
     $                         LDA, RWORK, RWORK( NRHS+1 ),
     $                         RESULT( 6 ) )
*
*                 Print information about the tests that did not pass
*                 the threshold.
*
                  DO 120 K = 3, 7
                     IF( RESULT( K ).GE.THRESH ) THEN
                        IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                     CALL ALAHD( NOUT, PATH )
                        WRITE( NOUT, FMT = 9998 )UPLO, N, NRHS, IMAT,
     $                     K, RESULT( K )
                        NFAIL = NFAIL + 1
                     END IF
  120             CONTINUE
                  NRUN = NRUN + 5
  130          CONTINUE
*
*+    TEST 8
*              Get an estimate of RCOND = 1/CNDNUM.
*
  140          CONTINUE
               ANORM = ZLANSP( '1', UPLO, N, A, RWORK )
               SRNAMT = 'ZSPCON'
               CALL ZSPCON( UPLO, N, AFAC, IWORK, ANORM, RCOND, WORK,
     $                      INFO )
*
*              Check error code from ZSPCON.
*
               IF( INFO.NE.0 )
     $            CALL ALAERH( PATH, 'ZSPCON', INFO, 0, UPLO, N, N, -1,
     $                         -1, -1, IMAT, NFAIL, NERRS, NOUT )
*
               RESULT( 8 ) = DGET06( RCOND, RCONDC )
*
*              Print the test ratio if it is .GE. THRESH.
*
               IF( RESULT( 8 ).GE.THRESH ) THEN
                  IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $               CALL ALAHD( NOUT, PATH )
                  WRITE( NOUT, FMT = 9999 )UPLO, N, IMAT, 8,
     $               RESULT( 8 )
                  NFAIL = NFAIL + 1
               END IF
               NRUN = NRUN + 1
  150       CONTINUE
  160    CONTINUE
  170 CONTINUE
*
*     Print a summary of the results.
*
      CALL ALASUM( PATH, NOUT, NFAIL, NRUN, NERRS )
*
 9999 FORMAT( ' UPLO = ''', A1, ''', N =', I5, ', type ', I2, ', test ',
     $      I2, ', ratio =', G12.5 )
 9998 FORMAT( ' UPLO = ''', A1, ''', N =', I5, ', NRHS=', I3, ', type ',
     $      I2, ', test(', I2, ') =', G12.5 )
      RETURN
*
*     End of ZCHKSP
*
      END
