*> \brief \b DDRVGE
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DDRVGE( DOTYPE, NN, NVAL, NRHS, THRESH, TSTERR, NMAX,
*                          A, AFAC, ASAV, B, BSAV, X, XACT, S, WORK,
*                          RWORK, IWORK, NOUT )
*
*       .. Scalar Arguments ..
*       LOGICAL            TSTERR
*       INTEGER            NMAX, NN, NOUT, NRHS
*       DOUBLE PRECISION   THRESH
*       ..
*       .. Array Arguments ..
*       LOGICAL            DOTYPE( * )
*       INTEGER            IWORK( * ), NVAL( * )
*       DOUBLE PRECISION   A( * ), AFAC( * ), ASAV( * ), B( * ),
*      $                   BSAV( * ), RWORK( * ), S( * ), WORK( * ),
*      $                   X( * ), XACT( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DDRVGE tests the driver routines DGESV and -SVX.
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
*>          The values of the matrix column dimension N.
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
*>          A is DOUBLE PRECISION array, dimension (NMAX*NMAX)
*> \endverbatim
*>
*> \param[out] AFAC
*> \verbatim
*>          AFAC is DOUBLE PRECISION array, dimension (NMAX*NMAX)
*> \endverbatim
*>
*> \param[out] ASAV
*> \verbatim
*>          ASAV is DOUBLE PRECISION array, dimension (NMAX*NMAX)
*> \endverbatim
*>
*> \param[out] B
*> \verbatim
*>          B is DOUBLE PRECISION array, dimension (NMAX*NRHS)
*> \endverbatim
*>
*> \param[out] BSAV
*> \verbatim
*>          BSAV is DOUBLE PRECISION array, dimension (NMAX*NRHS)
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
*> \param[out] S
*> \verbatim
*>          S is DOUBLE PRECISION array, dimension (2*NMAX)
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension
*>                      (NMAX*max(3,NRHS))
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (2*NRHS+NMAX)
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
      SUBROUTINE DDRVGE( DOTYPE, NN, NVAL, NRHS, THRESH, TSTERR, NMAX,
     $                   A, AFAC, ASAV, B, BSAV, X, XACT, S, WORK,
     $                   RWORK, IWORK, NOUT )
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
      DOUBLE PRECISION   A( * ), AFAC( * ), ASAV( * ), B( * ),
     $                   BSAV( * ), RWORK( * ), S( * ), WORK( * ),
     $                   X( * ), XACT( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ONE, ZERO
      PARAMETER          ( ONE = 1.0D+0, ZERO = 0.0D+0 )
      INTEGER            NTYPES
      PARAMETER          ( NTYPES = 11 )
      INTEGER            NTESTS
      PARAMETER          ( NTESTS = 7 )
      INTEGER            NTRAN
      PARAMETER          ( NTRAN = 3 )
*     ..
*     .. Local Scalars ..
      LOGICAL            EQUIL, NOFACT, PREFAC, TRFCON, ZEROT
      CHARACTER          DIST, EQUED, FACT, TRANS, TYPE, XTYPE
      CHARACTER*3        PATH
      INTEGER            I, IEQUED, IFACT, IMAT, IN, INFO, IOFF, ITRAN,
     $                   IZERO, K, K1, KL, KU, LDA, LWORK, MODE, N, NB,
     $                   NBMIN, NERRS, NFACT, NFAIL, NIMAT, NRUN, NT
      DOUBLE PRECISION   AINVNM, AMAX, ANORM, ANORMI, ANORMO, CNDNUM,
     $                   COLCND, RCOND, RCONDC, RCONDI, RCONDO, ROLDC,
     $                   ROLDI, ROLDO, ROWCND, RPVGRW
*     ..
*     .. Local Arrays ..
      CHARACTER          EQUEDS( 4 ), FACTS( 3 ), TRANSS( NTRAN )
      INTEGER            ISEED( 4 ), ISEEDY( 4 )
      DOUBLE PRECISION   RESULT( NTESTS )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      DOUBLE PRECISION   DGET06, DLAMCH, DLANGE, DLANTR
      EXTERNAL           LSAME, DGET06, DLAMCH, DLANGE, DLANTR
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALADHD, ALAERH, ALASVM, DERRVX, DGEEQU, DGESV,
     $                   DGESVX, DGET01, DGET02, DGET04, DGET07, DGETRF,
     $                   DGETRI, DLACPY, DLAQGE, DLARHS, DLASET, DLATB4,
     $                   DLATMS, XLAENV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX
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
      DATA               TRANSS / 'N', 'T', 'C' /
      DATA               FACTS / 'F', 'N', 'E' /
      DATA               EQUEDS / 'N', 'R', 'C', 'B' /
*     ..
*     .. Executable Statements ..
*
*     Initialize constants and the random number seed.
*
      PATH( 1: 1 ) = 'Double precision'
      PATH( 2: 3 ) = 'GE'
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
     $   CALL DERRVX( PATH, NOUT )
      INFOT = 0
*
*     Set the block size and minimum block size for testing.
*
      NB = 1
      NBMIN = 2
      CALL XLAENV( 1, NB )
      CALL XLAENV( 2, NBMIN )
*
*     Do for each value of N in NVAL
*
      DO 90 IN = 1, NN
         N = NVAL( IN )
         LDA = MAX( N, 1 )
         XTYPE = 'N'
         NIMAT = NTYPES
         IF( N.LE.0 )
     $      NIMAT = 1
*
         DO 80 IMAT = 1, NIMAT
*
*           Do the tests only if DOTYPE( IMAT ) is true.
*
            IF( .NOT.DOTYPE( IMAT ) )
     $         GO TO 80
*
*           Skip types 5, 6, or 7 if the matrix size is too small.
*
            ZEROT = IMAT.GE.5 .AND. IMAT.LE.7
            IF( ZEROT .AND. N.LT.IMAT-4 )
     $         GO TO 80
*
*           Set up parameters with DLATB4 and generate a test matrix
*           with DLATMS.
*
            CALL DLATB4( PATH, IMAT, N, N, TYPE, KL, KU, ANORM, MODE,
     $                   CNDNUM, DIST )
            RCONDC = ONE / CNDNUM
*
            SRNAMT = 'DLATMS'
            CALL DLATMS( N, N, DIST, ISEED, TYPE, RWORK, MODE, CNDNUM,
     $                   ANORM, KL, KU, 'No packing', A, LDA, WORK,
     $                   INFO )
*
*           Check error code from DLATMS.
*
            IF( INFO.NE.0 ) THEN
               CALL ALAERH( PATH, 'DLATMS', INFO, 0, ' ', N, N, -1, -1,
     $                      -1, IMAT, NFAIL, NERRS, NOUT )
               GO TO 80
            END IF
*
*           For types 5-7, zero one or more columns of the matrix to
*           test that INFO is returned correctly.
*
            IF( ZEROT ) THEN
               IF( IMAT.EQ.5 ) THEN
                  IZERO = 1
               ELSE IF( IMAT.EQ.6 ) THEN
                  IZERO = N
               ELSE
                  IZERO = N / 2 + 1
               END IF
               IOFF = ( IZERO-1 )*LDA
               IF( IMAT.LT.7 ) THEN
                  DO 20 I = 1, N
                     A( IOFF+I ) = ZERO
   20             CONTINUE
               ELSE
                  CALL DLASET( 'Full', N, N-IZERO+1, ZERO, ZERO,
     $                         A( IOFF+1 ), LDA )
               END IF
            ELSE
               IZERO = 0
            END IF
*
*           Save a copy of the matrix A in ASAV.
*
            CALL DLACPY( 'Full', N, N, A, LDA, ASAV, LDA )
*
            DO 70 IEQUED = 1, 4
               EQUED = EQUEDS( IEQUED )
               IF( IEQUED.EQ.1 ) THEN
                  NFACT = 3
               ELSE
                  NFACT = 1
               END IF
*
               DO 60 IFACT = 1, NFACT
                  FACT = FACTS( IFACT )
                  PREFAC = LSAME( FACT, 'F' )
                  NOFACT = LSAME( FACT, 'N' )
                  EQUIL = LSAME( FACT, 'E' )
*
                  IF( ZEROT ) THEN
                     IF( PREFAC )
     $                  GO TO 60
                     RCONDO = ZERO
                     RCONDI = ZERO
*
                  ELSE IF( .NOT.NOFACT ) THEN
*
*                    Compute the condition number for comparison with
*                    the value returned by DGESVX (FACT = 'N' reuses
*                    the condition number from the previous iteration
*                    with FACT = 'F').
*
                     CALL DLACPY( 'Full', N, N, ASAV, LDA, AFAC, LDA )
                     IF( EQUIL .OR. IEQUED.GT.1 ) THEN
*
*                       Compute row and column scale factors to
*                       equilibrate the matrix A.
*
                        CALL DGEEQU( N, N, AFAC, LDA, S, S( N+1 ),
     $                               ROWCND, COLCND, AMAX, INFO )
                        IF( INFO.EQ.0 .AND. N.GT.0 ) THEN
                           IF( LSAME( EQUED, 'R' ) ) THEN
                              ROWCND = ZERO
                              COLCND = ONE
                           ELSE IF( LSAME( EQUED, 'C' ) ) THEN
                              ROWCND = ONE
                              COLCND = ZERO
                           ELSE IF( LSAME( EQUED, 'B' ) ) THEN
                              ROWCND = ZERO
                              COLCND = ZERO
                           END IF
*
*                          Equilibrate the matrix.
*
                           CALL DLAQGE( N, N, AFAC, LDA, S, S( N+1 ),
     $                                  ROWCND, COLCND, AMAX, EQUED )
                        END IF
                     END IF
*
*                    Save the condition number of the non-equilibrated
*                    system for use in DGET04.
*
                     IF( EQUIL ) THEN
                        ROLDO = RCONDO
                        ROLDI = RCONDI
                     END IF
*
*                    Compute the 1-norm and infinity-norm of A.
*
                     ANORMO = DLANGE( '1', N, N, AFAC, LDA, RWORK )
                     ANORMI = DLANGE( 'I', N, N, AFAC, LDA, RWORK )
*
*                    Factor the matrix A.
*
                     SRNAMT = 'DGETRF'
                     CALL DGETRF( N, N, AFAC, LDA, IWORK, INFO )
*
*                    Form the inverse of A.
*
                     CALL DLACPY( 'Full', N, N, AFAC, LDA, A, LDA )
                     LWORK = NMAX*MAX( 3, NRHS )
                     SRNAMT = 'DGETRI'
                     CALL DGETRI( N, A, LDA, IWORK, WORK, LWORK, INFO )
*
*                    Compute the 1-norm condition number of A.
*
                     AINVNM = DLANGE( '1', N, N, A, LDA, RWORK )
                     IF( ANORMO.LE.ZERO .OR. AINVNM.LE.ZERO ) THEN
                        RCONDO = ONE
                     ELSE
                        RCONDO = ( ONE / ANORMO ) / AINVNM
                     END IF
*
*                    Compute the infinity-norm condition number of A.
*
                     AINVNM = DLANGE( 'I', N, N, A, LDA, RWORK )
                     IF( ANORMI.LE.ZERO .OR. AINVNM.LE.ZERO ) THEN
                        RCONDI = ONE
                     ELSE
                        RCONDI = ( ONE / ANORMI ) / AINVNM
                     END IF
                  END IF
*
                  DO 50 ITRAN = 1, NTRAN
*
*                    Do for each value of TRANS.
*
                     TRANS = TRANSS( ITRAN )
                     IF( ITRAN.EQ.1 ) THEN
                        RCONDC = RCONDO
                     ELSE
                        RCONDC = RCONDI
                     END IF
*
*                    Restore the matrix A.
*
                     CALL DLACPY( 'Full', N, N, ASAV, LDA, A, LDA )
*
*                    Form an exact solution and set the right hand side.
*
                     SRNAMT = 'DLARHS'
                     CALL DLARHS( PATH, XTYPE, 'Full', TRANS, N, N, KL,
     $                            KU, NRHS, A, LDA, XACT, LDA, B, LDA,
     $                            ISEED, INFO )
                     XTYPE = 'C'
                     CALL DLACPY( 'Full', N, NRHS, B, LDA, BSAV, LDA )
*
                     IF( NOFACT .AND. ITRAN.EQ.1 ) THEN
*
*                       --- Test DGESV  ---
*
*                       Compute the LU factorization of the matrix and
*                       solve the system.
*
                        CALL DLACPY( 'Full', N, N, A, LDA, AFAC, LDA )
                        CALL DLACPY( 'Full', N, NRHS, B, LDA, X, LDA )
*
                        SRNAMT = 'DGESV '
                        CALL DGESV( N, NRHS, AFAC, LDA, IWORK, X, LDA,
     $                              INFO )
*
*                       Check error code from DGESV .
*
                        IF( INFO.NE.IZERO )
     $                     CALL ALAERH( PATH, 'DGESV ', INFO, IZERO,
     $                                  ' ', N, N, -1, -1, NRHS, IMAT,
     $                                  NFAIL, NERRS, NOUT )
*
*                       Reconstruct matrix from factors and compute
*                       residual.
*
                        CALL DGET01( N, N, A, LDA, AFAC, LDA, IWORK,
     $                               RWORK, RESULT( 1 ) )
                        NT = 1
                        IF( IZERO.EQ.0 ) THEN
*
*                          Compute residual of the computed solution.
*
                           CALL DLACPY( 'Full', N, NRHS, B, LDA, WORK,
     $                                  LDA )
                           CALL DGET02( 'No transpose', N, N, NRHS, A,
     $                                  LDA, X, LDA, WORK, LDA, RWORK,
     $                                  RESULT( 2 ) )
*
*                          Check solution from generated exact solution.
*
                           CALL DGET04( N, NRHS, X, LDA, XACT, LDA,
     $                                  RCONDC, RESULT( 3 ) )
                           NT = 3
                        END IF
*
*                       Print information about the tests that did not
*                       pass the threshold.
*
                        DO 30 K = 1, NT
                           IF( RESULT( K ).GE.THRESH ) THEN
                              IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                           CALL ALADHD( NOUT, PATH )
                              WRITE( NOUT, FMT = 9999 )'DGESV ', N,
     $                           IMAT, K, RESULT( K )
                              NFAIL = NFAIL + 1
                           END IF
   30                   CONTINUE
                        NRUN = NRUN + NT
                     END IF
*
*                    --- Test DGESVX ---
*
                     IF( .NOT.PREFAC )
     $                  CALL DLASET( 'Full', N, N, ZERO, ZERO, AFAC,
     $                               LDA )
                     CALL DLASET( 'Full', N, NRHS, ZERO, ZERO, X, LDA )
                     IF( IEQUED.GT.1 .AND. N.GT.0 ) THEN
*
*                       Equilibrate the matrix if FACT = 'F' and
*                       EQUED = 'R', 'C', or 'B'.
*
                        CALL DLAQGE( N, N, A, LDA, S, S( N+1 ), ROWCND,
     $                               COLCND, AMAX, EQUED )
                     END IF
*
*                    Solve the system and compute the condition number
*                    and error bounds using DGESVX.
*
                     SRNAMT = 'DGESVX'
                     CALL DGESVX( FACT, TRANS, N, NRHS, A, LDA, AFAC,
     $                            LDA, IWORK, EQUED, S, S( N+1 ), B,
     $                            LDA, X, LDA, RCOND, RWORK,
     $                            RWORK( NRHS+1 ), WORK, IWORK( N+1 ),
     $                            INFO )
*
*                    Check the error code from DGESVX.
*
                     IF( INFO.NE.IZERO )
     $                  CALL ALAERH( PATH, 'DGESVX', INFO, IZERO,
     $                               FACT // TRANS, N, N, -1, -1, NRHS,
     $                               IMAT, NFAIL, NERRS, NOUT )
*
*                    Compare WORK(1) from DGESVX with the computed
*                    reciprocal pivot growth factor RPVGRW
*
                     IF( INFO.NE.0 .AND. INFO.LE.N) THEN
                        RPVGRW = DLANTR( 'M', 'U', 'N', INFO, INFO,
     $                           AFAC, LDA, WORK )
                        IF( RPVGRW.EQ.ZERO ) THEN
                           RPVGRW = ONE
                        ELSE
                           RPVGRW = DLANGE( 'M', N, INFO, A, LDA,
     $                              WORK ) / RPVGRW
                        END IF
                     ELSE
                        RPVGRW = DLANTR( 'M', 'U', 'N', N, N, AFAC, LDA,
     $                           WORK )
                        IF( RPVGRW.EQ.ZERO ) THEN
                           RPVGRW = ONE
                        ELSE
                           RPVGRW = DLANGE( 'M', N, N, A, LDA, WORK ) /
     $                              RPVGRW
                        END IF
                     END IF
                     RESULT( 7 ) = ABS( RPVGRW-WORK( 1 ) ) /
     $                             MAX( WORK( 1 ), RPVGRW ) /
     $                             DLAMCH( 'E' )
*
                     IF( .NOT.PREFAC ) THEN
*
*                       Reconstruct matrix from factors and compute
*                       residual.
*
                        CALL DGET01( N, N, A, LDA, AFAC, LDA, IWORK,
     $                               RWORK( 2*NRHS+1 ), RESULT( 1 ) )
                        K1 = 1
                     ELSE
                        K1 = 2
                     END IF
*
                     IF( INFO.EQ.0 ) THEN
                        TRFCON = .FALSE.
*
*                       Compute residual of the computed solution.
*
                        CALL DLACPY( 'Full', N, NRHS, BSAV, LDA, WORK,
     $                               LDA )
                        CALL DGET02( TRANS, N, N, NRHS, ASAV, LDA, X,
     $                               LDA, WORK, LDA, RWORK( 2*NRHS+1 ),
     $                               RESULT( 2 ) )
*
*                       Check solution from generated exact solution.
*
                        IF( NOFACT .OR. ( PREFAC .AND. LSAME( EQUED,
     $                      'N' ) ) ) THEN
                           CALL DGET04( N, NRHS, X, LDA, XACT, LDA,
     $                                  RCONDC, RESULT( 3 ) )
                        ELSE
                           IF( ITRAN.EQ.1 ) THEN
                              ROLDC = ROLDO
                           ELSE
                              ROLDC = ROLDI
                           END IF
                           CALL DGET04( N, NRHS, X, LDA, XACT, LDA,
     $                                  ROLDC, RESULT( 3 ) )
                        END IF
*
*                       Check the error bounds from iterative
*                       refinement.
*
                        CALL DGET07( TRANS, N, NRHS, ASAV, LDA, B, LDA,
     $                               X, LDA, XACT, LDA, RWORK, .TRUE.,
     $                               RWORK( NRHS+1 ), RESULT( 4 ) )
                     ELSE
                        TRFCON = .TRUE.
                     END IF
*
*                    Compare RCOND from DGESVX with the computed value
*                    in RCONDC.
*
                     RESULT( 6 ) = DGET06( RCOND, RCONDC )
*
*                    Print information about the tests that did not pass
*                    the threshold.
*
                     IF( .NOT.TRFCON ) THEN
                        DO 40 K = K1, NTESTS
                           IF( RESULT( K ).GE.THRESH ) THEN
                              IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                           CALL ALADHD( NOUT, PATH )
                              IF( PREFAC ) THEN
                                 WRITE( NOUT, FMT = 9997 )'DGESVX',
     $                              FACT, TRANS, N, EQUED, IMAT, K,
     $                              RESULT( K )
                              ELSE
                                 WRITE( NOUT, FMT = 9998 )'DGESVX',
     $                              FACT, TRANS, N, IMAT, K, RESULT( K )
                              END IF
                              NFAIL = NFAIL + 1
                           END IF
   40                   CONTINUE
                        NRUN = NRUN + NTESTS - K1 + 1
                     ELSE
                        IF( RESULT( 1 ).GE.THRESH .AND. .NOT.PREFAC )
     $                       THEN
                           IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                        CALL ALADHD( NOUT, PATH )
                           IF( PREFAC ) THEN
                              WRITE( NOUT, FMT = 9997 )'DGESVX', FACT,
     $                           TRANS, N, EQUED, IMAT, 1, RESULT( 1 )
                           ELSE
                              WRITE( NOUT, FMT = 9998 )'DGESVX', FACT,
     $                           TRANS, N, IMAT, 1, RESULT( 1 )
                           END IF
                           NFAIL = NFAIL + 1
                           NRUN = NRUN + 1
                        END IF
                        IF( RESULT( 6 ).GE.THRESH ) THEN
                           IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                        CALL ALADHD( NOUT, PATH )
                           IF( PREFAC ) THEN
                              WRITE( NOUT, FMT = 9997 )'DGESVX', FACT,
     $                           TRANS, N, EQUED, IMAT, 6, RESULT( 6 )
                           ELSE
                              WRITE( NOUT, FMT = 9998 )'DGESVX', FACT,
     $                           TRANS, N, IMAT, 6, RESULT( 6 )
                           END IF
                           NFAIL = NFAIL + 1
                           NRUN = NRUN + 1
                        END IF
                        IF( RESULT( 7 ).GE.THRESH ) THEN
                           IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                        CALL ALADHD( NOUT, PATH )
                           IF( PREFAC ) THEN
                              WRITE( NOUT, FMT = 9997 )'DGESVX', FACT,
     $                           TRANS, N, EQUED, IMAT, 7, RESULT( 7 )
                           ELSE
                              WRITE( NOUT, FMT = 9998 )'DGESVX', FACT,
     $                           TRANS, N, IMAT, 7, RESULT( 7 )
                           END IF
                           NFAIL = NFAIL + 1
                           NRUN = NRUN + 1
                        END IF
*
                     END IF
*
   50             CONTINUE
   60          CONTINUE
   70       CONTINUE
   80    CONTINUE
   90 CONTINUE
*
*     Print a summary of the results.
*
      CALL ALASVM( PATH, NOUT, NFAIL, NRUN, NERRS )
*
 9999 FORMAT( 1X, A, ', N =', I5, ', type ', I2, ', test(', I2, ') =',
     $      G12.5 )
 9998 FORMAT( 1X, A, ', FACT=''', A1, ''', TRANS=''', A1, ''', N=', I5,
     $      ', type ', I2, ', test(', I1, ')=', G12.5 )
 9997 FORMAT( 1X, A, ', FACT=''', A1, ''', TRANS=''', A1, ''', N=', I5,
     $      ', EQUED=''', A1, ''', type ', I2, ', test(', I1, ')=',
     $      G12.5 )
      RETURN
*
*     End of DDRVGE
*
      END
