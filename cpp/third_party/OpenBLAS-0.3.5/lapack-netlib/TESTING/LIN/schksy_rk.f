*> \brief \b SCHKSY_RK
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SCHKSY_RK( DOTYPE, NN, NVAL, NNB, NBVAL, NNS, NSVAL,
*                             THRESH, TSTERR, NMAX, A, AFAC, E, AINV, B,
*                             X, XACT, WORK, RWORK, IWORK, NOUT )
*
*       .. Scalar Arguments ..
*       LOGICAL            TSTERR
*       INTEGER            NMAX, NN, NNB, NNS, NOUT
*       REAL               THRESH
*       ..
*       .. Array Arguments ..
*       LOGICAL            DOTYPE( * )
*       INTEGER            IWORK( * ), NBVAL( * ), NSVAL( * ), NVAL( * )
*       REAL               A( * ), AFAC( * ), E( * ), AINV( * ), B( * ),
*      $                   RWORK( * ), WORK( * ), X( * ), XACT( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*> SCHKSY_RK tests SSYTRF_RK, -TRI_3, -TRS_3, and -CON_3.
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
*>          A is REAL array, dimension (NMAX*NMAX)
*> \endverbatim
*>
*> \param[out] AFAC
*> \verbatim
*>          AFAC is REAL array, dimension (NMAX*NMAX)
*> \endverbatim
*>
*> \param[out] E
*> \verbatim
*>          E is REAL array, dimension (NMAX)
*> \endverbatim
*>
*> \param[out] AINV
*> \verbatim
*>          AINV is REAL array, dimension (NMAX*NMAX)
*> \endverbatim
*>
*> \param[out] B
*> \verbatim
*>          B is REAL array, dimension (NMAX*NSMAX),
*>          where NSMAX is the largest entry in NSVAL.
*> \endverbatim
*>
*> \param[out] X
*> \verbatim
*>          X is REAL array, dimension (NMAX*NSMAX),
*>          where NSMAX is the largest entry in NSVAL.
*> \endverbatim
*>
*> \param[out] XACT
*> \verbatim
*>          XACT is REAL array, dimension (NMAX*NSMAX),
*>          where NSMAX is the largest entry in NSVAL.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (NMAX*max(3,NSMAX))
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is REAL array, dimension (max(NMAX,2*NSMAX))
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
*> \date November 2017
*
*> \ingroup double_lin
*
*  =====================================================================
      SUBROUTINE SCHKSY_RK( DOTYPE, NN, NVAL, NNB, NBVAL, NNS, NSVAL,
     $                      THRESH, TSTERR, NMAX, A, AFAC, E, AINV, B,
     $                      X, XACT, WORK, RWORK, IWORK, NOUT )
*
*  -- LAPACK test routine (version 3.8.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2017
*
*     .. Scalar Arguments ..
      LOGICAL            TSTERR
      INTEGER            NMAX, NN, NNB, NNS, NOUT
      REAL               THRESH
*     ..
*     .. Array Arguments ..
      LOGICAL            DOTYPE( * )
      INTEGER            IWORK( * ), NBVAL( * ), NSVAL( * ), NVAL( * )
      REAL               A( * ), AFAC( * ), AINV( * ), B( * ), E( * ),
     $                   RWORK( * ), WORK( * ), X( * ), XACT( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
      REAL               EIGHT, SEVTEN
      PARAMETER          ( EIGHT = 8.0E+0, SEVTEN = 17.0E+0 )
      INTEGER            NTYPES
      PARAMETER          ( NTYPES = 10 )
      INTEGER            NTESTS
      PARAMETER          ( NTESTS = 7 )
*     ..
*     .. Local Scalars ..
      LOGICAL            TRFCON, ZEROT
      CHARACTER          DIST, TYPE, UPLO, XTYPE
      CHARACTER*3        PATH, MATPATH
      INTEGER            I, I1, I2, IMAT, IN, INB, INFO, IOFF, IRHS,
     $                   IUPLO, IZERO, J, K, KL, KU, LDA, LWORK,
     $                   MODE, N, NB, NERRS, NFAIL, NIMAT, NRHS, NRUN,
     $                   NT
      REAL               ALPHA, ANORM, CNDNUM, CONST, STEMP, SING_MAX,
     $                   SING_MIN, RCOND, RCONDC
*     ..
*     .. Local Arrays ..
      CHARACTER          UPLOS( 2 )
      INTEGER            ISEED( 4 ), ISEEDY( 4 )
      REAL               BLOCK( 2, 2 ), SDUMMY( 1 ), RESULT( NTESTS )
*     ..
*     .. External Functions ..
      REAL               SGET06, SLANGE, SLANSY
      EXTERNAL           SGET06, SLANGE, SLANSY
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAERH, ALAHD, ALASUM, SERRSY, SGESVD, SGET04,
     $                   SLACPY, SLARHS, SLATB4, SLATMS, SPOT02, SPOT03,
     $                   SSYCON_3, SSYT01_3, SSYTRF_RK, SSYTRI_3,
     $                   SSYTRS_3, XLAENV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN, SQRT
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
      ALPHA = ( ONE+SQRT( SEVTEN ) ) / EIGHT
*
*     Test path
*
      PATH( 1: 1 ) = 'Single precision'
      PATH( 2: 3 ) = 'SK'
*
*     Path to generate matrices
*
      MATPATH( 1: 1 ) = 'Single precision'
      MATPATH( 2: 3 ) = 'SY'
*
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
     $   CALL SERRSY( PATH, NOUT )
      INFOT = 0
*
*     Set the minimum block size for which the block routine should
*     be used, which will be later returned by ILAENV
*
      CALL XLAENV( 2, 2 )
*
*     Do for each value of N in NVAL
*
      DO 270 IN = 1, NN
         N = NVAL( IN )
         LDA = MAX( N, 1 )
         XTYPE = 'N'
         NIMAT = NTYPES
         IF( N.LE.0 )
     $      NIMAT = 1
*
         IZERO = 0
*
*        Do for each value of matrix type IMAT
*
         DO 260 IMAT = 1, NIMAT
*
*           Do the tests only if DOTYPE( IMAT ) is true.
*
            IF( .NOT.DOTYPE( IMAT ) )
     $         GO TO 260
*
*           Skip types 3, 4, 5, or 6 if the matrix size is too small.
*
            ZEROT = IMAT.GE.3 .AND. IMAT.LE.6
            IF( ZEROT .AND. N.LT.IMAT-2 )
     $         GO TO 260
*
*           Do first for UPLO = 'U', then for UPLO = 'L'
*
            DO 250 IUPLO = 1, 2
               UPLO = UPLOS( IUPLO )
*
*              Begin generate the test matrix A.
*
*              Set up parameters with SLATB4 for the matrix generator
*              based on the type of matrix to be generated.
*
               CALL SLATB4( MATPATH, IMAT, N, N, TYPE, KL, KU, ANORM,
     $                      MODE, CNDNUM, DIST )
*
*              Generate a matrix with SLATMS.
*
               SRNAMT = 'SLATMS'
               CALL SLATMS( N, N, DIST, ISEED, TYPE, RWORK, MODE,
     $                      CNDNUM, ANORM, KL, KU, UPLO, A, LDA, WORK,
     $                      INFO )
*
*              Check error code from SLATMS and handle error.
*
               IF( INFO.NE.0 ) THEN
                  CALL ALAERH( PATH, 'SLATMS', INFO, 0, UPLO, N, N, -1,
     $                         -1, -1, IMAT, NFAIL, NERRS, NOUT )
*
*                 Skip all tests for this generated matrix
*
                  GO TO 250
               END IF
*
*              For matrix types 3-6, zero one or more rows and
*              columns of the matrix to test that INFO is returned
*              correctly.
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
                        IOFF = ( IZERO-1 )*LDA
                        DO 20 I = 1, IZERO - 1
                           A( IOFF+I ) = ZERO
   20                   CONTINUE
                        IOFF = IOFF + IZERO
                        DO 30 I = IZERO, N
                           A( IOFF ) = ZERO
                           IOFF = IOFF + LDA
   30                   CONTINUE
                     ELSE
                        IOFF = IZERO
                        DO 40 I = 1, IZERO - 1
                           A( IOFF ) = ZERO
                           IOFF = IOFF + LDA
   40                   CONTINUE
                        IOFF = IOFF - IZERO
                        DO 50 I = IZERO, N
                           A( IOFF+I ) = ZERO
   50                   CONTINUE
                     END IF
                  ELSE
                     IF( IUPLO.EQ.1 ) THEN
*
*                       Set the first IZERO rows and columns to zero.
*
                        IOFF = 0
                        DO 70 J = 1, N
                           I2 = MIN( J, IZERO )
                           DO 60 I = 1, I2
                              A( IOFF+I ) = ZERO
   60                      CONTINUE
                           IOFF = IOFF + LDA
   70                   CONTINUE
                     ELSE
*
*                       Set the last IZERO rows and columns to zero.
*
                        IOFF = 0
                        DO 90 J = 1, N
                           I1 = MAX( J, IZERO )
                           DO 80 I = I1, N
                              A( IOFF+I ) = ZERO
   80                      CONTINUE
                           IOFF = IOFF + LDA
   90                   CONTINUE
                     END IF
                  END IF
               ELSE
                  IZERO = 0
               END IF
*
*              End generate the test matrix A.
*
*
*              Do for each value of NB in NBVAL
*
               DO 240 INB = 1, NNB
*
*                 Set the optimal blocksize, which will be later
*                 returned by ILAENV.
*
                  NB = NBVAL( INB )
                  CALL XLAENV( 1, NB )
*
*                 Copy the test matrix A into matrix AFAC which
*                 will be factorized in place. This is needed to
*                 preserve the test matrix A for subsequent tests.
*
                  CALL SLACPY( UPLO, N, N, A, LDA, AFAC, LDA )
*
*                 Compute the L*D*L**T or U*D*U**T factorization of the
*                 matrix. IWORK stores details of the interchanges and
*                 the block structure of D. AINV is a work array for
*                 block factorization, LWORK is the length of AINV.
*
                  LWORK = MAX( 2, NB )*LDA
                  SRNAMT = 'SSYTRF_RK'
                  CALL SSYTRF_RK( UPLO, N, AFAC, LDA, E, IWORK, AINV,
     $                            LWORK, INFO )
*
*                 Adjust the expected value of INFO to account for
*                 pivoting.
*
                  K = IZERO
                  IF( K.GT.0 ) THEN
  100                CONTINUE
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
*                 Check error code from DSYTRF_RK and handle error.
*
                  IF( INFO.NE.K)
     $               CALL ALAERH( PATH, 'SSYTRF_RK', INFO, K,
     $                            UPLO, N, N, -1, -1, NB, IMAT,
     $                            NFAIL, NERRS, NOUT )
*
*                 Set the condition estimate flag if the INFO is not 0.
*
                  IF( INFO.NE.0 ) THEN
                     TRFCON = .TRUE.
                  ELSE
                     TRFCON = .FALSE.
                  END IF
*
*+    TEST 1
*                 Reconstruct matrix from factors and compute residual.
*
                  CALL SSYT01_3( UPLO, N, A, LDA, AFAC, LDA, E, IWORK,
     $                           AINV, LDA, RWORK, RESULT( 1 ) )
                  NT = 1
*
*+    TEST 2
*                 Form the inverse and compute the residual,
*                 if the factorization was competed without INFO > 0
*                 (i.e. there is no zero rows and columns).
*                 Do it only for the first block size.
*
                  IF( INB.EQ.1 .AND. .NOT.TRFCON ) THEN
                     CALL SLACPY( UPLO, N, N, AFAC, LDA, AINV, LDA )
                     SRNAMT = 'SSYTRI_3'
*
*                    Another reason that we need to compute the invesrse
*                    is that SPOT03 produces RCONDC which is used later
*                    in TEST6 and TEST7.
*
                     LWORK = (N+NB+1)*(NB+3)
                     CALL SSYTRI_3( UPLO, N, AINV, LDA, E, IWORK, WORK,
     $                              LWORK, INFO )
*
*                    Check error code from SSYTRI_3 and handle error.
*
                     IF( INFO.NE.0 )
     $                  CALL ALAERH( PATH, 'SSYTRI_3', INFO, -1,
     $                               UPLO, N, N, -1, -1, -1, IMAT,
     $                               NFAIL, NERRS, NOUT )
*
*                    Compute the residual for a symmetric matrix times
*                    its inverse.
*
                     CALL SPOT03( UPLO, N, A, LDA, AINV, LDA, WORK, LDA,
     $                            RWORK, RCONDC, RESULT( 2 ) )
                     NT = 2
                  END IF
*
*                 Print information about the tests that did not pass
*                 the threshold.
*
                  DO 110 K = 1, NT
                     IF( RESULT( K ).GE.THRESH ) THEN
                        IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                     CALL ALAHD( NOUT, PATH )
                        WRITE( NOUT, FMT = 9999 )UPLO, N, NB, IMAT, K,
     $                     RESULT( K )
                        NFAIL = NFAIL + 1
                     END IF
  110             CONTINUE
                  NRUN = NRUN + NT
*
*+    TEST 3
*                 Compute largest element in U or L
*
                  RESULT( 3 ) = ZERO
                  STEMP = ZERO
*
                  CONST = ONE / ( ONE-ALPHA )
*
                  IF( IUPLO.EQ.1 ) THEN
*
*                 Compute largest element in U
*
                     K = N
  120                CONTINUE
                     IF( K.LE.1 )
     $                  GO TO 130
*
                     IF( IWORK( K ).GT.ZERO ) THEN
*
*                       Get max absolute value from elements
*                       in column k in in U
*
                        STEMP = SLANGE( 'M', K-1, 1,
     $                          AFAC( ( K-1 )*LDA+1 ), LDA, RWORK )
                     ELSE
*
*                       Get max absolute value from elements
*                       in columns k and k-1 in U
*
                        STEMP = SLANGE( 'M', K-2, 2,
     $                          AFAC( ( K-2 )*LDA+1 ), LDA, RWORK )
                        K = K - 1
*
                     END IF
*
*                    STEMP should be bounded by CONST
*
                     STEMP = STEMP - CONST + THRESH
                     IF( STEMP.GT.RESULT( 3 ) )
     $                  RESULT( 3 ) = STEMP
*
                     K = K - 1
*
                     GO TO 120
  130                CONTINUE
*
                  ELSE
*
*                 Compute largest element in L
*
                     K = 1
  140                CONTINUE
                     IF( K.GE.N )
     $                  GO TO 150
*
                     IF( IWORK( K ).GT.ZERO ) THEN
*
*                       Get max absolute value from elements
*                       in column k in in L
*
                        STEMP = SLANGE( 'M', N-K, 1,
     $                          AFAC( ( K-1 )*LDA+K+1 ), LDA, RWORK )
                     ELSE
*
*                       Get max absolute value from elements
*                       in columns k and k+1 in L
*
                        STEMP = SLANGE( 'M', N-K-1, 2,
     $                          AFAC( ( K-1 )*LDA+K+2 ), LDA, RWORK )
                        K = K + 1
*
                     END IF
*
*                    STEMP should be bounded by CONST
*
                     STEMP = STEMP - CONST + THRESH
                     IF( STEMP.GT.RESULT( 3 ) )
     $                  RESULT( 3 ) = STEMP
*
                     K = K + 1
*
                     GO TO 140
  150                CONTINUE
                  END IF
*
*+    TEST 4
*                 Compute largest 2-Norm (condition number)
*                 of 2-by-2 diag blocks
*
                  RESULT( 4 ) = ZERO
                  STEMP = ZERO
*
                  CONST = ( ONE+ALPHA ) / ( ONE-ALPHA )
                  CALL SLACPY( UPLO, N, N, AFAC, LDA, AINV, LDA )
*
                  IF( IUPLO.EQ.1 ) THEN
*
*                    Loop backward for UPLO = 'U'
*
                     K = N
  160                CONTINUE
                     IF( K.LE.1 )
     $                  GO TO 170
*
                     IF( IWORK( K ).LT.ZERO ) THEN
*
*                       Get the two singular values
*                       (real and non-negative) of a 2-by-2 block,
*                       store them in RWORK array
*
                        BLOCK( 1, 1 ) = AFAC( ( K-2 )*LDA+K-1 )
                        BLOCK( 1, 2 ) = E( K )
                        BLOCK( 2, 1 ) = BLOCK( 1, 2 )
                        BLOCK( 2, 2 ) = AFAC( (K-1)*LDA+K )
*
                        CALL SGESVD( 'N', 'N', 2, 2, BLOCK, 2, RWORK,
     $                               SDUMMY, 1, SDUMMY, 1,
     $                               WORK, 10, INFO )
*
                        SING_MAX = RWORK( 1 )
                        SING_MIN = RWORK( 2 )
*
                        STEMP = SING_MAX / SING_MIN
*
*                       STEMP should be bounded by CONST
*
                        STEMP = STEMP - CONST + THRESH
                        IF( STEMP.GT.RESULT( 4 ) )
     $                     RESULT( 4 ) = STEMP
                        K = K - 1
*
                     END IF
*
                     K = K - 1
*
                     GO TO 160
  170                CONTINUE
*
                  ELSE
*
*                    Loop forward for UPLO = 'L'
*
                     K = 1
  180                CONTINUE
                     IF( K.GE.N )
     $                  GO TO 190
*
                     IF( IWORK( K ).LT.ZERO ) THEN
*
*                       Get the two singular values
*                       (real and non-negative) of a 2-by-2 block,
*                       store them in RWORK array
*
                        BLOCK( 1, 1 ) = AFAC( ( K-1 )*LDA+K )
                        BLOCK( 2, 1 ) = E( K )
                        BLOCK( 1, 2 ) = BLOCK( 2, 1 )
                        BLOCK( 2, 2 ) = AFAC( K*LDA+K+1 )
*
                        CALL SGESVD( 'N', 'N', 2, 2, BLOCK, 2, RWORK,
     $                               SDUMMY, 1, SDUMMY, 1,
     $                               WORK, 10, INFO )
*
*
                        SING_MAX = RWORK( 1 )
                        SING_MIN = RWORK( 2 )
*
                        STEMP = SING_MAX / SING_MIN
*
*                       STEMP should be bounded by CONST
*
                        STEMP = STEMP - CONST + THRESH
                        IF( STEMP.GT.RESULT( 4 ) )
     $                     RESULT( 4 ) = STEMP
                        K = K + 1
*
                     END IF
*
                     K = K + 1
*
                     GO TO 180
  190                CONTINUE
                  END IF
*
*                 Print information about the tests that did not pass
*                 the threshold.
*
                  DO 200 K = 3, 4
                     IF( RESULT( K ).GE.THRESH ) THEN
                        IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                     CALL ALAHD( NOUT, PATH )
                        WRITE( NOUT, FMT = 9999 )UPLO, N, NB, IMAT, K,
     $                     RESULT( K )
                        NFAIL = NFAIL + 1
                     END IF
  200             CONTINUE
                  NRUN = NRUN + 2
*
*                 Skip the other tests if this is not the first block
*                 size.
*
                  IF( INB.GT.1 )
     $               GO TO 240
*
*                 Do only the condition estimate if INFO is not 0.
*
                  IF( TRFCON ) THEN
                     RCONDC = ZERO
                     GO TO 230
                  END IF
*
*                 Do for each value of NRHS in NSVAL.
*
                  DO 220 IRHS = 1, NNS
                     NRHS = NSVAL( IRHS )
*
*+    TEST 5 ( Using TRS_3)
*                 Solve and compute residual for  A * X = B.
*
*                    Choose a set of NRHS random solution vectors
*                    stored in XACT and set up the right hand side B
*
                     SRNAMT = 'SLARHS'
                     CALL SLARHS( MATPATH, XTYPE, UPLO, ' ', N, N,
     $                            KL, KU, NRHS, A, LDA, XACT, LDA,
     $                            B, LDA, ISEED, INFO )
                     CALL SLACPY( 'Full', N, NRHS, B, LDA, X, LDA )
*
                     SRNAMT = 'SSYTRS_3'
                     CALL SSYTRS_3( UPLO, N, NRHS, AFAC, LDA, E, IWORK,
     $                              X, LDA, INFO )
*
*                    Check error code from SSYTRS_3 and handle error.
*
                     IF( INFO.NE.0 )
     $                  CALL ALAERH( PATH, 'SSYTRS_3', INFO, 0,
     $                               UPLO, N, N, -1, -1, NRHS, IMAT,
     $                               NFAIL, NERRS, NOUT )
*
                     CALL SLACPY( 'Full', N, NRHS, B, LDA, WORK, LDA )
*
*                    Compute the residual for the solution
*
                     CALL SPOT02( UPLO, N, NRHS, A, LDA, X, LDA, WORK,
     $                            LDA, RWORK, RESULT( 5 ) )
*
*+    TEST 6
*                    Check solution from generated exact solution.
*
                     CALL SGET04( N, NRHS, X, LDA, XACT, LDA, RCONDC,
     $                            RESULT( 6 ) )
*
*                    Print information about the tests that did not pass
*                    the threshold.
*
                     DO 210 K = 5, 6
                        IF( RESULT( K ).GE.THRESH ) THEN
                           IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                        CALL ALAHD( NOUT, PATH )
                           WRITE( NOUT, FMT = 9998 )UPLO, N, NRHS,
     $                        IMAT, K, RESULT( K )
                           NFAIL = NFAIL + 1
                        END IF
  210                CONTINUE
                     NRUN = NRUN + 2
*
*                 End do for each value of NRHS in NSVAL.
*
  220             CONTINUE
*
*+    TEST 7
*                 Get an estimate of RCOND = 1/CNDNUM.
*
  230             CONTINUE
                  ANORM = SLANSY( '1', UPLO, N, A, LDA, RWORK )
                  SRNAMT = 'SSYCON_3'
                  CALL SSYCON_3( UPLO, N, AFAC, LDA, E, IWORK, ANORM,
     $                           RCOND, WORK, IWORK( N+1 ), INFO )
*
*                 Check error code from DSYCON_3 and handle error.
*
                  IF( INFO.NE.0 )
     $               CALL ALAERH( PATH, 'SSYCON_3', INFO, 0,
     $                            UPLO, N, N, -1, -1, -1, IMAT,
     $                            NFAIL, NERRS, NOUT )
*
*                 Compute the test ratio to compare to values of RCOND
*
                  RESULT( 7 ) = SGET06( RCOND, RCONDC )
*
*                 Print information about the tests that did not pass
*                 the threshold.
*
                  IF( RESULT( 7 ).GE.THRESH ) THEN
                     IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                  CALL ALAHD( NOUT, PATH )
                     WRITE( NOUT, FMT = 9997 ) UPLO, N, IMAT, 7,
     $                  RESULT( 7 )
                     NFAIL = NFAIL + 1
                  END IF
                  NRUN = NRUN + 1
  240          CONTINUE
*
  250       CONTINUE
  260    CONTINUE
  270 CONTINUE
*
*     Print a summary of the results.
*
      CALL ALASUM( PATH, NOUT, NFAIL, NRUN, NERRS )
*
 9999 FORMAT( ' UPLO = ''', A1, ''', N =', I5, ', NB =', I4, ', type ',
     $      I2, ', test ', I2, ', ratio =', G12.5 )
 9998 FORMAT( ' UPLO = ''', A1, ''', N =', I5, ', NRHS=', I3, ', type ',
     $      I2, ', test(', I2, ') =', G12.5 )
 9997 FORMAT( ' UPLO = ''', A1, ''', N =', I5, ',', 10X, ' type ', I2,
     $      ', test(', I2, ') =', G12.5 )
      RETURN
*
*     End of SCHKSY_RK
*
      END
