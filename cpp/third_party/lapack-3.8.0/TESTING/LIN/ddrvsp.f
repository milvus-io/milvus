*> \brief \b DDRVSP
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DDRVSP( DOTYPE, NN, NVAL, NRHS, THRESH, TSTERR, NMAX,
*                          A, AFAC, AINV, B, X, XACT, WORK, RWORK, IWORK,
*                          NOUT )
*
*       .. Scalar Arguments ..
*       LOGICAL            TSTERR
*       INTEGER            NMAX, NN, NOUT, NRHS
*       DOUBLE PRECISION   THRESH
*       ..
*       .. Array Arguments ..
*       LOGICAL            DOTYPE( * )
*       INTEGER            IWORK( * ), NVAL( * )
*       DOUBLE PRECISION   A( * ), AFAC( * ), AINV( * ), B( * ),
*      $                   RWORK( * ), WORK( * ), X( * ), XACT( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DDRVSP tests the driver routines DSPSV and -SVX.
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
*> \param[in] NRHS
*> \verbatim
*>          NRHS is INTEGER
*>          The number of right hand side vectors to be generated for
*>          each linear system.
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
*>          A is DOUBLE PRECISION array, dimension
*>                      (NMAX*(NMAX+1)/2)
*> \endverbatim
*>
*> \param[out] AFAC
*> \verbatim
*>          AFAC is DOUBLE PRECISION array, dimension
*>                      (NMAX*(NMAX+1)/2)
*> \endverbatim
*>
*> \param[out] AINV
*> \verbatim
*>          AINV is DOUBLE PRECISION array, dimension
*>                      (NMAX*(NMAX+1)/2)
*> \endverbatim
*>
*> \param[out] B
*> \verbatim
*>          B is DOUBLE PRECISION array, dimension (NMAX*NRHS)
*> \endverbatim
*>
*> \param[out] X
*> \verbatim
*>          X is DOUBLE PRECISION array, dimension (NMAX*NRHS)
*> \endverbatim
*>
*> \param[out] XACT
*> \verbatim
*>          XACT is DOUBLE PRECISION array, dimension (NMAX*NRHS)
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension
*>                      (NMAX*max(2,NRHS))
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (NMAX+2*NRHS)
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (2*NMAX)
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
      SUBROUTINE DDRVSP( DOTYPE, NN, NVAL, NRHS, THRESH, TSTERR, NMAX,
     $                   A, AFAC, AINV, B, X, XACT, WORK, RWORK, IWORK,
     $                   NOUT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      LOGICAL            TSTERR
      INTEGER            NMAX, NN, NOUT, NRHS
      DOUBLE PRECISION   THRESH
*     ..
*     .. Array Arguments ..
      LOGICAL            DOTYPE( * )
      INTEGER            IWORK( * ), NVAL( * )
      DOUBLE PRECISION   A( * ), AFAC( * ), AINV( * ), B( * ),
     $                   RWORK( * ), WORK( * ), X( * ), XACT( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ONE, ZERO
      PARAMETER          ( ONE = 1.0D+0, ZERO = 0.0D+0 )
      INTEGER            NTYPES, NTESTS
      PARAMETER          ( NTYPES = 10, NTESTS = 6 )
      INTEGER            NFACT
      PARAMETER          ( NFACT = 2 )
*     ..
*     .. Local Scalars ..
      LOGICAL            ZEROT
      CHARACTER          DIST, FACT, PACKIT, TYPE, UPLO, XTYPE
      CHARACTER*3        PATH
      INTEGER            I, I1, I2, IFACT, IMAT, IN, INFO, IOFF, IUPLO,
     $                   IZERO, J, K, K1, KL, KU, LDA, LWORK, MODE, N,
     $                   NERRS, NFAIL, NIMAT, NPP, NRUN, NT
      DOUBLE PRECISION   AINVNM, ANORM, CNDNUM, RCOND, RCONDC
*     ..
*     .. Local Arrays ..
      CHARACTER          FACTS( NFACT )
      INTEGER            ISEED( 4 ), ISEEDY( 4 )
      DOUBLE PRECISION   RESULT( NTESTS )
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DGET06, DLANSP
      EXTERNAL           DGET06, DLANSP
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALADHD, ALAERH, ALASVM, DCOPY, DERRVX, DGET04,
     $                   DLACPY, DLARHS, DLASET, DLATB4, DLATMS, DPPT02,
     $                   DPPT05, DSPSV, DSPSVX, DSPT01, DSPTRF, DSPTRI
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
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN
*     ..
*     .. Data statements ..
      DATA               ISEEDY / 1988, 1989, 1990, 1991 /
      DATA               FACTS / 'F', 'N' /
*     ..
*     .. Executable Statements ..
*
*     Initialize constants and the random number seed.
*
      PATH( 1: 1 ) = 'Double precision'
      PATH( 2: 3 ) = 'SP'
      NRUN = 0
      NFAIL = 0
      NERRS = 0
      DO 10 I = 1, 4
         ISEED( I ) = ISEEDY( I )
   10 CONTINUE
      LWORK = MAX( 2*NMAX, NMAX*NRHS )
*
*     Test the error exits
*
      IF( TSTERR )
     $   CALL DERRVX( PATH, NOUT )
      INFOT = 0
*
*     Do for each value of N in NVAL
*
      DO 180 IN = 1, NN
         N = NVAL( IN )
         LDA = MAX( N, 1 )
         NPP = N*( N+1 ) / 2
         XTYPE = 'N'
         NIMAT = NTYPES
         IF( N.LE.0 )
     $      NIMAT = 1
*
         DO 170 IMAT = 1, NIMAT
*
*           Do the tests only if DOTYPE( IMAT ) is true.
*
            IF( .NOT.DOTYPE( IMAT ) )
     $         GO TO 170
*
*           Skip types 3, 4, 5, or 6 if the matrix size is too small.
*
            ZEROT = IMAT.GE.3 .AND. IMAT.LE.6
            IF( ZEROT .AND. N.LT.IMAT-2 )
     $         GO TO 170
*
*           Do first for UPLO = 'U', then for UPLO = 'L'
*
            DO 160 IUPLO = 1, 2
               IF( IUPLO.EQ.1 ) THEN
                  UPLO = 'U'
                  PACKIT = 'C'
               ELSE
                  UPLO = 'L'
                  PACKIT = 'R'
               END IF
*
*              Set up parameters with DLATB4 and generate a test matrix
*              with DLATMS.
*
               CALL DLATB4( PATH, IMAT, N, N, TYPE, KL, KU, ANORM, MODE,
     $                      CNDNUM, DIST )
*
               SRNAMT = 'DLATMS'
               CALL DLATMS( N, N, DIST, ISEED, TYPE, RWORK, MODE,
     $                      CNDNUM, ANORM, KL, KU, PACKIT, A, LDA, WORK,
     $                      INFO )
*
*              Check error code from DLATMS.
*
               IF( INFO.NE.0 ) THEN
                  CALL ALAERH( PATH, 'DLATMS', INFO, 0, UPLO, N, N, -1,
     $                         -1, -1, IMAT, NFAIL, NERRS, NOUT )
                  GO TO 160
               END IF
*
*              For types 3-6, zero one or more rows and columns of the
*              matrix to test that INFO is returned correctly.
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
*                    Set row and column IZERO to zero.
*
                     IF( IUPLO.EQ.1 ) THEN
                        IOFF = ( IZERO-1 )*IZERO / 2
                        DO 20 I = 1, IZERO - 1
                           A( IOFF+I ) = ZERO
   20                   CONTINUE
                        IOFF = IOFF + IZERO
                        DO 30 I = IZERO, N
                           A( IOFF ) = ZERO
                           IOFF = IOFF + I
   30                   CONTINUE
                     ELSE
                        IOFF = IZERO
                        DO 40 I = 1, IZERO - 1
                           A( IOFF ) = ZERO
                           IOFF = IOFF + N - I
   40                   CONTINUE
                        IOFF = IOFF - IZERO
                        DO 50 I = IZERO, N
                           A( IOFF+I ) = ZERO
   50                   CONTINUE
                     END IF
                  ELSE
                     IOFF = 0
                     IF( IUPLO.EQ.1 ) THEN
*
*                       Set the first IZERO rows and columns to zero.
*
                        DO 70 J = 1, N
                           I2 = MIN( J, IZERO )
                           DO 60 I = 1, I2
                              A( IOFF+I ) = ZERO
   60                      CONTINUE
                           IOFF = IOFF + J
   70                   CONTINUE
                     ELSE
*
*                       Set the last IZERO rows and columns to zero.
*
                        DO 90 J = 1, N
                           I1 = MAX( J, IZERO )
                           DO 80 I = I1, N
                              A( IOFF+I ) = ZERO
   80                      CONTINUE
                           IOFF = IOFF + N - J
   90                   CONTINUE
                     END IF
                  END IF
               ELSE
                  IZERO = 0
               END IF
*
               DO 150 IFACT = 1, NFACT
*
*                 Do first for FACT = 'F', then for other values.
*
                  FACT = FACTS( IFACT )
*
*                 Compute the condition number for comparison with
*                 the value returned by DSPSVX.
*
                  IF( ZEROT ) THEN
                     IF( IFACT.EQ.1 )
     $                  GO TO 150
                     RCONDC = ZERO
*
                  ELSE IF( IFACT.EQ.1 ) THEN
*
*                    Compute the 1-norm of A.
*
                     ANORM = DLANSP( '1', UPLO, N, A, RWORK )
*
*                    Factor the matrix A.
*
                     CALL DCOPY( NPP, A, 1, AFAC, 1 )
                     CALL DSPTRF( UPLO, N, AFAC, IWORK, INFO )
*
*                    Compute inv(A) and take its norm.
*
                     CALL DCOPY( NPP, AFAC, 1, AINV, 1 )
                     CALL DSPTRI( UPLO, N, AINV, IWORK, WORK, INFO )
                     AINVNM = DLANSP( '1', UPLO, N, AINV, RWORK )
*
*                    Compute the 1-norm condition number of A.
*
                     IF( ANORM.LE.ZERO .OR. AINVNM.LE.ZERO ) THEN
                        RCONDC = ONE
                     ELSE
                        RCONDC = ( ONE / ANORM ) / AINVNM
                     END IF
                  END IF
*
*                 Form an exact solution and set the right hand side.
*
                  SRNAMT = 'DLARHS'
                  CALL DLARHS( PATH, XTYPE, UPLO, ' ', N, N, KL, KU,
     $                         NRHS, A, LDA, XACT, LDA, B, LDA, ISEED,
     $                         INFO )
                  XTYPE = 'C'
*
*                 --- Test DSPSV  ---
*
                  IF( IFACT.EQ.2 ) THEN
                     CALL DCOPY( NPP, A, 1, AFAC, 1 )
                     CALL DLACPY( 'Full', N, NRHS, B, LDA, X, LDA )
*
*                    Factor the matrix and solve the system using DSPSV.
*
                     SRNAMT = 'DSPSV '
                     CALL DSPSV( UPLO, N, NRHS, AFAC, IWORK, X, LDA,
     $                           INFO )
*
*                    Adjust the expected value of INFO to account for
*                    pivoting.
*
                     K = IZERO
                     IF( K.GT.0 ) THEN
  100                   CONTINUE
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
*                    Check error code from DSPSV .
*
                     IF( INFO.NE.K ) THEN
                        CALL ALAERH( PATH, 'DSPSV ', INFO, K, UPLO, N,
     $                               N, -1, -1, NRHS, IMAT, NFAIL,
     $                               NERRS, NOUT )
                        GO TO 120
                     ELSE IF( INFO.NE.0 ) THEN
                        GO TO 120
                     END IF
*
*                    Reconstruct matrix from factors and compute
*                    residual.
*
                     CALL DSPT01( UPLO, N, A, AFAC, IWORK, AINV, LDA,
     $                            RWORK, RESULT( 1 ) )
*
*                    Compute residual of the computed solution.
*
                     CALL DLACPY( 'Full', N, NRHS, B, LDA, WORK, LDA )
                     CALL DPPT02( UPLO, N, NRHS, A, X, LDA, WORK, LDA,
     $                            RWORK, RESULT( 2 ) )
*
*                    Check solution from generated exact solution.
*
                     CALL DGET04( N, NRHS, X, LDA, XACT, LDA, RCONDC,
     $                            RESULT( 3 ) )
                     NT = 3
*
*                    Print information about the tests that did not pass
*                    the threshold.
*
                     DO 110 K = 1, NT
                        IF( RESULT( K ).GE.THRESH ) THEN
                           IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                        CALL ALADHD( NOUT, PATH )
                           WRITE( NOUT, FMT = 9999 )'DSPSV ', UPLO, N,
     $                        IMAT, K, RESULT( K )
                           NFAIL = NFAIL + 1
                        END IF
  110                CONTINUE
                     NRUN = NRUN + NT
  120                CONTINUE
                  END IF
*
*                 --- Test DSPSVX ---
*
                  IF( IFACT.EQ.2 .AND. NPP.GT.0 )
     $               CALL DLASET( 'Full', NPP, 1, ZERO, ZERO, AFAC,
     $                            NPP )
                  CALL DLASET( 'Full', N, NRHS, ZERO, ZERO, X, LDA )
*
*                 Solve the system and compute the condition number and
*                 error bounds using DSPSVX.
*
                  SRNAMT = 'DSPSVX'
                  CALL DSPSVX( FACT, UPLO, N, NRHS, A, AFAC, IWORK, B,
     $                         LDA, X, LDA, RCOND, RWORK,
     $                         RWORK( NRHS+1 ), WORK, IWORK( N+1 ),
     $                         INFO )
*
*                 Adjust the expected value of INFO to account for
*                 pivoting.
*
                  K = IZERO
                  IF( K.GT.0 ) THEN
  130                CONTINUE
                     IF( IWORK( K ).LT.0 ) THEN
                        IF( IWORK( K ).NE.-K ) THEN
                           K = -IWORK( K )
                           GO TO 130
                        END IF
                     ELSE IF( IWORK( K ).NE.K ) THEN
                        K = IWORK( K )
                        GO TO 130
                     END IF
                  END IF
*
*                 Check the error code from DSPSVX.
*
                  IF( INFO.NE.K ) THEN
                     CALL ALAERH( PATH, 'DSPSVX', INFO, K, FACT // UPLO,
     $                            N, N, -1, -1, NRHS, IMAT, NFAIL,
     $                            NERRS, NOUT )
                     GO TO 150
                  END IF
*
                  IF( INFO.EQ.0 ) THEN
                     IF( IFACT.GE.2 ) THEN
*
*                       Reconstruct matrix from factors and compute
*                       residual.
*
                        CALL DSPT01( UPLO, N, A, AFAC, IWORK, AINV, LDA,
     $                               RWORK( 2*NRHS+1 ), RESULT( 1 ) )
                        K1 = 1
                     ELSE
                        K1 = 2
                     END IF
*
*                    Compute residual of the computed solution.
*
                     CALL DLACPY( 'Full', N, NRHS, B, LDA, WORK, LDA )
                     CALL DPPT02( UPLO, N, NRHS, A, X, LDA, WORK, LDA,
     $                            RWORK( 2*NRHS+1 ), RESULT( 2 ) )
*
*                    Check solution from generated exact solution.
*
                     CALL DGET04( N, NRHS, X, LDA, XACT, LDA, RCONDC,
     $                            RESULT( 3 ) )
*
*                    Check the error bounds from iterative refinement.
*
                     CALL DPPT05( UPLO, N, NRHS, A, B, LDA, X, LDA,
     $                            XACT, LDA, RWORK, RWORK( NRHS+1 ),
     $                            RESULT( 4 ) )
                  ELSE
                     K1 = 6
                  END IF
*
*                 Compare RCOND from DSPSVX with the computed value
*                 in RCONDC.
*
                  RESULT( 6 ) = DGET06( RCOND, RCONDC )
*
*                 Print information about the tests that did not pass
*                 the threshold.
*
                  DO 140 K = K1, 6
                     IF( RESULT( K ).GE.THRESH ) THEN
                        IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                     CALL ALADHD( NOUT, PATH )
                        WRITE( NOUT, FMT = 9998 )'DSPSVX', FACT, UPLO,
     $                     N, IMAT, K, RESULT( K )
                        NFAIL = NFAIL + 1
                     END IF
  140             CONTINUE
                  NRUN = NRUN + 7 - K1
*
  150          CONTINUE
*
  160       CONTINUE
  170    CONTINUE
  180 CONTINUE
*
*     Print a summary of the results.
*
      CALL ALASVM( PATH, NOUT, NFAIL, NRUN, NERRS )
*
 9999 FORMAT( 1X, A, ', UPLO=''', A1, ''', N =', I5, ', type ', I2,
     $      ', test ', I2, ', ratio =', G12.5 )
 9998 FORMAT( 1X, A, ', FACT=''', A1, ''', UPLO=''', A1, ''', N =', I5,
     $      ', type ', I2, ', test ', I2, ', ratio =', G12.5 )
      RETURN
*
*     End of DDRVSP
*
      END
