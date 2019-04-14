*> \brief \b CDRVPB
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CDRVPB( DOTYPE, NN, NVAL, NRHS, THRESH, TSTERR, NMAX,
*                          A, AFAC, ASAV, B, BSAV, X, XACT, S, WORK,
*                          RWORK, NOUT )
*
*       .. Scalar Arguments ..
*       LOGICAL            TSTERR
*       INTEGER            NMAX, NN, NOUT, NRHS
*       REAL               THRESH
*       ..
*       .. Array Arguments ..
*       LOGICAL            DOTYPE( * )
*       INTEGER            NVAL( * )
*       REAL               RWORK( * ), S( * )
*       COMPLEX            A( * ), AFAC( * ), ASAV( * ), B( * ),
*      $                   BSAV( * ), WORK( * ), X( * ), XACT( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CDRVPB tests the driver routines CPBSV and -SVX.
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
*> \param[in] NMAX
*> \verbatim
*>          NMAX is INTEGER
*>          The maximum value permitted for N, used in dimensioning the
*>          work arrays.
*> \endverbatim
*>
*> \param[out] A
*> \verbatim
*>          A is COMPLEX array, dimension (NMAX*NMAX)
*> \endverbatim
*>
*> \param[out] AFAC
*> \verbatim
*>          AFAC is COMPLEX array, dimension (NMAX*NMAX)
*> \endverbatim
*>
*> \param[out] ASAV
*> \verbatim
*>          ASAV is COMPLEX array, dimension (NMAX*NMAX)
*> \endverbatim
*>
*> \param[out] B
*> \verbatim
*>          B is COMPLEX array, dimension (NMAX*NRHS)
*> \endverbatim
*>
*> \param[out] BSAV
*> \verbatim
*>          BSAV is COMPLEX array, dimension (NMAX*NRHS)
*> \endverbatim
*>
*> \param[out] X
*> \verbatim
*>          X is COMPLEX array, dimension (NMAX*NRHS)
*> \endverbatim
*>
*> \param[out] XACT
*> \verbatim
*>          XACT is COMPLEX array, dimension (NMAX*NRHS)
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is REAL array, dimension (NMAX)
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX array, dimension
*>                      (NMAX*max(3,NRHS))
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is REAL array, dimension (NMAX+2*NRHS)
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
*> \ingroup complex_lin
*
*  =====================================================================
      SUBROUTINE CDRVPB( DOTYPE, NN, NVAL, NRHS, THRESH, TSTERR, NMAX,
     $                   A, AFAC, ASAV, B, BSAV, X, XACT, S, WORK,
     $                   RWORK, NOUT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      LOGICAL            TSTERR
      INTEGER            NMAX, NN, NOUT, NRHS
      REAL               THRESH
*     ..
*     .. Array Arguments ..
      LOGICAL            DOTYPE( * )
      INTEGER            NVAL( * )
      REAL               RWORK( * ), S( * )
      COMPLEX            A( * ), AFAC( * ), ASAV( * ), B( * ),
     $                   BSAV( * ), WORK( * ), X( * ), XACT( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ONE, ZERO
      PARAMETER          ( ONE = 1.0E+0, ZERO = 0.0E+0 )
      INTEGER            NTYPES, NTESTS
      PARAMETER          ( NTYPES = 8, NTESTS = 6 )
      INTEGER            NBW
      PARAMETER          ( NBW = 4 )
*     ..
*     .. Local Scalars ..
      LOGICAL            EQUIL, NOFACT, PREFAC, ZEROT
      CHARACTER          DIST, EQUED, FACT, PACKIT, TYPE, UPLO, XTYPE
      CHARACTER*3        PATH
      INTEGER            I, I1, I2, IEQUED, IFACT, IKD, IMAT, IN, INFO,
     $                   IOFF, IUPLO, IW, IZERO, K, K1, KD, KL, KOFF,
     $                   KU, LDA, LDAB, MODE, N, NB, NBMIN, NERRS,
     $                   NFACT, NFAIL, NIMAT, NKD, NRUN, NT
      REAL               AINVNM, AMAX, ANORM, CNDNUM, RCOND, RCONDC,
     $                   ROLDC, SCOND
*     ..
*     .. Local Arrays ..
      CHARACTER          EQUEDS( 2 ), FACTS( 3 )
      INTEGER            ISEED( 4 ), ISEEDY( 4 ), KDVAL( NBW )
      REAL               RESULT( NTESTS )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      REAL               CLANGE, CLANHB, SGET06
      EXTERNAL           LSAME, CLANGE, CLANHB, SGET06
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALADHD, ALAERH, ALASVM, CCOPY, CERRVX, CGET04,
     $                   CLACPY, CLAIPD, CLAQHB, CLARHS, CLASET, CLATB4,
     $                   CLATMS, CPBEQU, CPBSV, CPBSVX, CPBT01, CPBT02,
     $                   CPBT05, CPBTRF, CPBTRS, CSWAP, XLAENV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          CMPLX, MAX, MIN
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
      DATA               FACTS / 'F', 'N', 'E' / , EQUEDS / 'N', 'Y' /
*     ..
*     .. Executable Statements ..
*
*     Initialize constants and the random number seed.
*
      PATH( 1: 1 ) = 'Complex precision'
      PATH( 2: 3 ) = 'PB'
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
     $   CALL CERRVX( PATH, NOUT )
      INFOT = 0
      KDVAL( 1 ) = 0
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
      DO 110 IN = 1, NN
         N = NVAL( IN )
         LDA = MAX( N, 1 )
         XTYPE = 'N'
*
*        Set limits on the number of loop iterations.
*
         NKD = MAX( 1, MIN( N, 4 ) )
         NIMAT = NTYPES
         IF( N.EQ.0 )
     $      NIMAT = 1
*
         KDVAL( 2 ) = N + ( N+1 ) / 4
         KDVAL( 3 ) = ( 3*N-1 ) / 4
         KDVAL( 4 ) = ( N+1 ) / 4
*
         DO 100 IKD = 1, NKD
*
*           Do for KD = 0, (5*N+1)/4, (3N-1)/4, and (N+1)/4. This order
*           makes it easier to skip redundant values for small values
*           of N.
*
            KD = KDVAL( IKD )
            LDAB = KD + 1
*
*           Do first for UPLO = 'U', then for UPLO = 'L'
*
            DO 90 IUPLO = 1, 2
               KOFF = 1
               IF( IUPLO.EQ.1 ) THEN
                  UPLO = 'U'
                  PACKIT = 'Q'
                  KOFF = MAX( 1, KD+2-N )
               ELSE
                  UPLO = 'L'
                  PACKIT = 'B'
               END IF
*
               DO 80 IMAT = 1, NIMAT
*
*                 Do the tests only if DOTYPE( IMAT ) is true.
*
                  IF( .NOT.DOTYPE( IMAT ) )
     $               GO TO 80
*
*                 Skip types 2, 3, or 4 if the matrix size is too small.
*
                  ZEROT = IMAT.GE.2 .AND. IMAT.LE.4
                  IF( ZEROT .AND. N.LT.IMAT-1 )
     $               GO TO 80
*
                  IF( .NOT.ZEROT .OR. .NOT.DOTYPE( 1 ) ) THEN
*
*                    Set up parameters with CLATB4 and generate a test
*                    matrix with CLATMS.
*
                     CALL CLATB4( PATH, IMAT, N, N, TYPE, KL, KU, ANORM,
     $                            MODE, CNDNUM, DIST )
*
                     SRNAMT = 'CLATMS'
                     CALL CLATMS( N, N, DIST, ISEED, TYPE, RWORK, MODE,
     $                            CNDNUM, ANORM, KD, KD, PACKIT,
     $                            A( KOFF ), LDAB, WORK, INFO )
*
*                    Check error code from CLATMS.
*
                     IF( INFO.NE.0 ) THEN
                        CALL ALAERH( PATH, 'CLATMS', INFO, 0, UPLO, N,
     $                               N, -1, -1, -1, IMAT, NFAIL, NERRS,
     $                               NOUT )
                        GO TO 80
                     END IF
                  ELSE IF( IZERO.GT.0 ) THEN
*
*                    Use the same matrix for types 3 and 4 as for type
*                    2 by copying back the zeroed out column,
*
                     IW = 2*LDA + 1
                     IF( IUPLO.EQ.1 ) THEN
                        IOFF = ( IZERO-1 )*LDAB + KD + 1
                        CALL CCOPY( IZERO-I1, WORK( IW ), 1,
     $                              A( IOFF-IZERO+I1 ), 1 )
                        IW = IW + IZERO - I1
                        CALL CCOPY( I2-IZERO+1, WORK( IW ), 1,
     $                              A( IOFF ), MAX( LDAB-1, 1 ) )
                     ELSE
                        IOFF = ( I1-1 )*LDAB + 1
                        CALL CCOPY( IZERO-I1, WORK( IW ), 1,
     $                              A( IOFF+IZERO-I1 ),
     $                              MAX( LDAB-1, 1 ) )
                        IOFF = ( IZERO-1 )*LDAB + 1
                        IW = IW + IZERO - I1
                        CALL CCOPY( I2-IZERO+1, WORK( IW ), 1,
     $                              A( IOFF ), 1 )
                     END IF
                  END IF
*
*                 For types 2-4, zero one row and column of the matrix
*                 to test that INFO is returned correctly.
*
                  IZERO = 0
                  IF( ZEROT ) THEN
                     IF( IMAT.EQ.2 ) THEN
                        IZERO = 1
                     ELSE IF( IMAT.EQ.3 ) THEN
                        IZERO = N
                     ELSE
                        IZERO = N / 2 + 1
                     END IF
*
*                    Save the zeroed out row and column in WORK(*,3)
*
                     IW = 2*LDA
                     DO 20 I = 1, MIN( 2*KD+1, N )
                        WORK( IW+I ) = ZERO
   20                CONTINUE
                     IW = IW + 1
                     I1 = MAX( IZERO-KD, 1 )
                     I2 = MIN( IZERO+KD, N )
*
                     IF( IUPLO.EQ.1 ) THEN
                        IOFF = ( IZERO-1 )*LDAB + KD + 1
                        CALL CSWAP( IZERO-I1, A( IOFF-IZERO+I1 ), 1,
     $                              WORK( IW ), 1 )
                        IW = IW + IZERO - I1
                        CALL CSWAP( I2-IZERO+1, A( IOFF ),
     $                              MAX( LDAB-1, 1 ), WORK( IW ), 1 )
                     ELSE
                        IOFF = ( I1-1 )*LDAB + 1
                        CALL CSWAP( IZERO-I1, A( IOFF+IZERO-I1 ),
     $                              MAX( LDAB-1, 1 ), WORK( IW ), 1 )
                        IOFF = ( IZERO-1 )*LDAB + 1
                        IW = IW + IZERO - I1
                        CALL CSWAP( I2-IZERO+1, A( IOFF ), 1,
     $                              WORK( IW ), 1 )
                     END IF
                  END IF
*
*                 Set the imaginary part of the diagonals.
*
                  IF( IUPLO.EQ.1 ) THEN
                     CALL CLAIPD( N, A( KD+1 ), LDAB, 0 )
                  ELSE
                     CALL CLAIPD( N, A( 1 ), LDAB, 0 )
                  END IF
*
*                 Save a copy of the matrix A in ASAV.
*
                  CALL CLACPY( 'Full', KD+1, N, A, LDAB, ASAV, LDAB )
*
                  DO 70 IEQUED = 1, 2
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
     $                        GO TO 60
                           RCONDC = ZERO
*
                        ELSE IF( .NOT.LSAME( FACT, 'N' ) ) THEN
*
*                          Compute the condition number for comparison
*                          with the value returned by CPBSVX (FACT =
*                          'N' reuses the condition number from the
*                          previous iteration with FACT = 'F').
*
                           CALL CLACPY( 'Full', KD+1, N, ASAV, LDAB,
     $                                  AFAC, LDAB )
                           IF( EQUIL .OR. IEQUED.GT.1 ) THEN
*
*                             Compute row and column scale factors to
*                             equilibrate the matrix A.
*
                              CALL CPBEQU( UPLO, N, KD, AFAC, LDAB, S,
     $                                     SCOND, AMAX, INFO )
                              IF( INFO.EQ.0 .AND. N.GT.0 ) THEN
                                 IF( IEQUED.GT.1 )
     $                              SCOND = ZERO
*
*                                Equilibrate the matrix.
*
                                 CALL CLAQHB( UPLO, N, KD, AFAC, LDAB,
     $                                        S, SCOND, AMAX, EQUED )
                              END IF
                           END IF
*
*                          Save the condition number of the
*                          non-equilibrated system for use in CGET04.
*
                           IF( EQUIL )
     $                        ROLDC = RCONDC
*
*                          Compute the 1-norm of A.
*
                           ANORM = CLANHB( '1', UPLO, N, KD, AFAC, LDAB,
     $                             RWORK )
*
*                          Factor the matrix A.
*
                           CALL CPBTRF( UPLO, N, KD, AFAC, LDAB, INFO )
*
*                          Form the inverse of A.
*
                           CALL CLASET( 'Full', N, N, CMPLX( ZERO ),
     $                                  CMPLX( ONE ), A, LDA )
                           SRNAMT = 'CPBTRS'
                           CALL CPBTRS( UPLO, N, KD, N, AFAC, LDAB, A,
     $                                  LDA, INFO )
*
*                          Compute the 1-norm condition number of A.
*
                           AINVNM = CLANGE( '1', N, N, A, LDA, RWORK )
                           IF( ANORM.LE.ZERO .OR. AINVNM.LE.ZERO ) THEN
                              RCONDC = ONE
                           ELSE
                              RCONDC = ( ONE / ANORM ) / AINVNM
                           END IF
                        END IF
*
*                       Restore the matrix A.
*
                        CALL CLACPY( 'Full', KD+1, N, ASAV, LDAB, A,
     $                               LDAB )
*
*                       Form an exact solution and set the right hand
*                       side.
*
                        SRNAMT = 'CLARHS'
                        CALL CLARHS( PATH, XTYPE, UPLO, ' ', N, N, KD,
     $                               KD, NRHS, A, LDAB, XACT, LDA, B,
     $                               LDA, ISEED, INFO )
                        XTYPE = 'C'
                        CALL CLACPY( 'Full', N, NRHS, B, LDA, BSAV,
     $                               LDA )
*
                        IF( NOFACT ) THEN
*
*                          --- Test CPBSV  ---
*
*                          Compute the L*L' or U'*U factorization of the
*                          matrix and solve the system.
*
                           CALL CLACPY( 'Full', KD+1, N, A, LDAB, AFAC,
     $                                  LDAB )
                           CALL CLACPY( 'Full', N, NRHS, B, LDA, X,
     $                                  LDA )
*
                           SRNAMT = 'CPBSV '
                           CALL CPBSV( UPLO, N, KD, NRHS, AFAC, LDAB, X,
     $                                 LDA, INFO )
*
*                          Check error code from CPBSV .
*
                           IF( INFO.NE.IZERO ) THEN
                              CALL ALAERH( PATH, 'CPBSV ', INFO, IZERO,
     $                                     UPLO, N, N, KD, KD, NRHS,
     $                                     IMAT, NFAIL, NERRS, NOUT )
                              GO TO 40
                           ELSE IF( INFO.NE.0 ) THEN
                              GO TO 40
                           END IF
*
*                          Reconstruct matrix from factors and compute
*                          residual.
*
                           CALL CPBT01( UPLO, N, KD, A, LDAB, AFAC,
     $                                  LDAB, RWORK, RESULT( 1 ) )
*
*                          Compute residual of the computed solution.
*
                           CALL CLACPY( 'Full', N, NRHS, B, LDA, WORK,
     $                                  LDA )
                           CALL CPBT02( UPLO, N, KD, NRHS, A, LDAB, X,
     $                                  LDA, WORK, LDA, RWORK,
     $                                  RESULT( 2 ) )
*
*                          Check solution from generated exact solution.
*
                           CALL CGET04( N, NRHS, X, LDA, XACT, LDA,
     $                                  RCONDC, RESULT( 3 ) )
                           NT = 3
*
*                          Print information about the tests that did
*                          not pass the threshold.
*
                           DO 30 K = 1, NT
                              IF( RESULT( K ).GE.THRESH ) THEN
                                 IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                              CALL ALADHD( NOUT, PATH )
                                 WRITE( NOUT, FMT = 9999 )'CPBSV ',
     $                              UPLO, N, KD, IMAT, K, RESULT( K )
                                 NFAIL = NFAIL + 1
                              END IF
   30                      CONTINUE
                           NRUN = NRUN + NT
   40                      CONTINUE
                        END IF
*
*                       --- Test CPBSVX ---
*
                        IF( .NOT.PREFAC )
     $                     CALL CLASET( 'Full', KD+1, N, CMPLX( ZERO ),
     $                                  CMPLX( ZERO ), AFAC, LDAB )
                        CALL CLASET( 'Full', N, NRHS, CMPLX( ZERO ),
     $                               CMPLX( ZERO ), X, LDA )
                        IF( IEQUED.GT.1 .AND. N.GT.0 ) THEN
*
*                          Equilibrate the matrix if FACT='F' and
*                          EQUED='Y'
*
                           CALL CLAQHB( UPLO, N, KD, A, LDAB, S, SCOND,
     $                                  AMAX, EQUED )
                        END IF
*
*                       Solve the system and compute the condition
*                       number and error bounds using CPBSVX.
*
                        SRNAMT = 'CPBSVX'
                        CALL CPBSVX( FACT, UPLO, N, KD, NRHS, A, LDAB,
     $                               AFAC, LDAB, EQUED, S, B, LDA, X,
     $                               LDA, RCOND, RWORK, RWORK( NRHS+1 ),
     $                               WORK, RWORK( 2*NRHS+1 ), INFO )
*
*                       Check the error code from CPBSVX.
*
                        IF( INFO.NE.IZERO ) THEN
                           CALL ALAERH( PATH, 'CPBSVX', INFO, IZERO,
     $                                  FACT // UPLO, N, N, KD, KD,
     $                                  NRHS, IMAT, NFAIL, NERRS, NOUT )
                           GO TO 60
                        END IF
*
                        IF( INFO.EQ.0 ) THEN
                           IF( .NOT.PREFAC ) THEN
*
*                             Reconstruct matrix from factors and
*                             compute residual.
*
                              CALL CPBT01( UPLO, N, KD, A, LDAB, AFAC,
     $                                     LDAB, RWORK( 2*NRHS+1 ),
     $                                     RESULT( 1 ) )
                              K1 = 1
                           ELSE
                              K1 = 2
                           END IF
*
*                          Compute residual of the computed solution.
*
                           CALL CLACPY( 'Full', N, NRHS, BSAV, LDA,
     $                                  WORK, LDA )
                           CALL CPBT02( UPLO, N, KD, NRHS, ASAV, LDAB,
     $                                  X, LDA, WORK, LDA,
     $                                  RWORK( 2*NRHS+1 ), RESULT( 2 ) )
*
*                          Check solution from generated exact solution.
*
                           IF( NOFACT .OR. ( PREFAC .AND. LSAME( EQUED,
     $                         'N' ) ) ) THEN
                              CALL CGET04( N, NRHS, X, LDA, XACT, LDA,
     $                                     RCONDC, RESULT( 3 ) )
                           ELSE
                              CALL CGET04( N, NRHS, X, LDA, XACT, LDA,
     $                                     ROLDC, RESULT( 3 ) )
                           END IF
*
*                          Check the error bounds from iterative
*                          refinement.
*
                           CALL CPBT05( UPLO, N, KD, NRHS, ASAV, LDAB,
     $                                  B, LDA, X, LDA, XACT, LDA,
     $                                  RWORK, RWORK( NRHS+1 ),
     $                                  RESULT( 4 ) )
                        ELSE
                           K1 = 6
                        END IF
*
*                       Compare RCOND from CPBSVX with the computed
*                       value in RCONDC.
*
                        RESULT( 6 ) = SGET06( RCOND, RCONDC )
*
*                       Print information about the tests that did not
*                       pass the threshold.
*
                        DO 50 K = K1, 6
                           IF( RESULT( K ).GE.THRESH ) THEN
                              IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                           CALL ALADHD( NOUT, PATH )
                              IF( PREFAC ) THEN
                                 WRITE( NOUT, FMT = 9997 )'CPBSVX',
     $                              FACT, UPLO, N, KD, EQUED, IMAT, K,
     $                              RESULT( K )
                              ELSE
                                 WRITE( NOUT, FMT = 9998 )'CPBSVX',
     $                              FACT, UPLO, N, KD, IMAT, K,
     $                              RESULT( K )
                              END IF
                              NFAIL = NFAIL + 1
                           END IF
   50                   CONTINUE
                        NRUN = NRUN + 7 - K1
   60                CONTINUE
   70             CONTINUE
   80          CONTINUE
   90       CONTINUE
  100    CONTINUE
  110 CONTINUE
*
*     Print a summary of the results.
*
      CALL ALASVM( PATH, NOUT, NFAIL, NRUN, NERRS )
*
 9999 FORMAT( 1X, A, ', UPLO=''', A1, ''', N =', I5, ', KD =', I5,
     $      ', type ', I1, ', test(', I1, ')=', G12.5 )
 9998 FORMAT( 1X, A, '( ''', A1, ''', ''', A1, ''', ', I5, ', ', I5,
     $      ', ... ), type ', I1, ', test(', I1, ')=', G12.5 )
 9997 FORMAT( 1X, A, '( ''', A1, ''', ''', A1, ''', ', I5, ', ', I5,
     $      ', ... ), EQUED=''', A1, ''', type ', I1, ', test(', I1,
     $      ')=', G12.5 )
      RETURN
*
*     End of CDRVPB
*
      END
