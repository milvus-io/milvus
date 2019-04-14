*> \brief \b CCHKGT
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CCHKGT( DOTYPE, NN, NVAL, NNS, NSVAL, THRESH, TSTERR,
*                          A, AF, B, X, XACT, WORK, RWORK, IWORK, NOUT )
*
*       .. Scalar Arguments ..
*       LOGICAL            TSTERR
*       INTEGER            NN, NNS, NOUT
*       REAL               THRESH
*       ..
*       .. Array Arguments ..
*       LOGICAL            DOTYPE( * )
*       INTEGER            IWORK( * ), NSVAL( * ), NVAL( * )
*       REAL               RWORK( * )
*       COMPLEX            A( * ), AF( * ), B( * ), WORK( * ), X( * ),
*      $                   XACT( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CCHKGT tests CGTTRF, -TRS, -RFS, and -CON
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
*> \param[out] A
*> \verbatim
*>          A is COMPLEX array, dimension (NMAX*4)
*> \endverbatim
*>
*> \param[out] AF
*> \verbatim
*>          AF is COMPLEX array, dimension (NMAX*4)
*> \endverbatim
*>
*> \param[out] B
*> \verbatim
*>          B is COMPLEX array, dimension (NMAX*NSMAX)
*>          where NSMAX is the largest entry in NSVAL.
*> \endverbatim
*>
*> \param[out] X
*> \verbatim
*>          X is COMPLEX array, dimension (NMAX*NSMAX)
*> \endverbatim
*>
*> \param[out] XACT
*> \verbatim
*>          XACT is COMPLEX array, dimension (NMAX*NSMAX)
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX array, dimension
*>                      (NMAX*max(3,NSMAX))
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is REAL array, dimension
*>                      (max(NMAX)+2*NSMAX)
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
*> \ingroup complex_lin
*
*  =====================================================================
      SUBROUTINE CCHKGT( DOTYPE, NN, NVAL, NNS, NSVAL, THRESH, TSTERR,
     $                   A, AF, B, X, XACT, WORK, RWORK, IWORK, NOUT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      LOGICAL            TSTERR
      INTEGER            NN, NNS, NOUT
      REAL               THRESH
*     ..
*     .. Array Arguments ..
      LOGICAL            DOTYPE( * )
      INTEGER            IWORK( * ), NSVAL( * ), NVAL( * )
      REAL               RWORK( * )
      COMPLEX            A( * ), AF( * ), B( * ), WORK( * ), X( * ),
     $                   XACT( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ONE, ZERO
      PARAMETER          ( ONE = 1.0E+0, ZERO = 0.0E+0 )
      INTEGER            NTYPES
      PARAMETER          ( NTYPES = 12 )
      INTEGER            NTESTS
      PARAMETER          ( NTESTS = 7 )
*     ..
*     .. Local Scalars ..
      LOGICAL            TRFCON, ZEROT
      CHARACTER          DIST, NORM, TRANS, TYPE
      CHARACTER*3        PATH
      INTEGER            I, IMAT, IN, INFO, IRHS, ITRAN, IX, IZERO, J,
     $                   K, KL, KOFF, KU, LDA, M, MODE, N, NERRS, NFAIL,
     $                   NIMAT, NRHS, NRUN
      REAL               AINVNM, ANORM, COND, RCOND, RCONDC, RCONDI,
     $                   RCONDO
*     ..
*     .. Local Arrays ..
      CHARACTER          TRANSS( 3 )
      INTEGER            ISEED( 4 ), ISEEDY( 4 )
      REAL               RESULT( NTESTS )
      COMPLEX            Z( 3 )
*     ..
*     .. External Functions ..
      REAL               CLANGT, SCASUM, SGET06
      EXTERNAL           CLANGT, SCASUM, SGET06
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAERH, ALAHD, ALASUM, CCOPY, CERRGE, CGET04,
     $                   CGTCON, CGTRFS, CGTT01, CGTT02, CGTT05, CGTTRF,
     $                   CGTTRS, CLACPY, CLAGTM, CLARNV, CLATB4, CLATMS,
     $                   CSSCAL
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX
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
      DATA               ISEEDY / 0, 0, 0, 1 / , TRANSS / 'N', 'T',
     $                   'C' /
*     ..
*     .. Executable Statements ..
*
      PATH( 1: 1 ) = 'Complex precision'
      PATH( 2: 3 ) = 'GT'
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
     $   CALL CERRGE( PATH, NOUT )
      INFOT = 0
*
      DO 110 IN = 1, NN
*
*        Do for each value of N in NVAL.
*
         N = NVAL( IN )
         M = MAX( N-1, 0 )
         LDA = MAX( 1, N )
         NIMAT = NTYPES
         IF( N.LE.0 )
     $      NIMAT = 1
*
         DO 100 IMAT = 1, NIMAT
*
*           Do the tests only if DOTYPE( IMAT ) is true.
*
            IF( .NOT.DOTYPE( IMAT ) )
     $         GO TO 100
*
*           Set up parameters with CLATB4.
*
            CALL CLATB4( PATH, IMAT, N, N, TYPE, KL, KU, ANORM, MODE,
     $                   COND, DIST )
*
            ZEROT = IMAT.GE.8 .AND. IMAT.LE.10
            IF( IMAT.LE.6 ) THEN
*
*              Types 1-6:  generate matrices of known condition number.
*
               KOFF = MAX( 2-KU, 3-MAX( 1, N ) )
               SRNAMT = 'CLATMS'
               CALL CLATMS( N, N, DIST, ISEED, TYPE, RWORK, MODE, COND,
     $                      ANORM, KL, KU, 'Z', AF( KOFF ), 3, WORK,
     $                      INFO )
*
*              Check the error code from CLATMS.
*
               IF( INFO.NE.0 ) THEN
                  CALL ALAERH( PATH, 'CLATMS', INFO, 0, ' ', N, N, KL,
     $                         KU, -1, IMAT, NFAIL, NERRS, NOUT )
                  GO TO 100
               END IF
               IZERO = 0
*
               IF( N.GT.1 ) THEN
                  CALL CCOPY( N-1, AF( 4 ), 3, A, 1 )
                  CALL CCOPY( N-1, AF( 3 ), 3, A( N+M+1 ), 1 )
               END IF
               CALL CCOPY( N, AF( 2 ), 3, A( M+1 ), 1 )
            ELSE
*
*              Types 7-12:  generate tridiagonal matrices with
*              unknown condition numbers.
*
               IF( .NOT.ZEROT .OR. .NOT.DOTYPE( 7 ) ) THEN
*
*                 Generate a matrix with elements whose real and
*                 imaginary parts are from [-1,1].
*
                  CALL CLARNV( 2, ISEED, N+2*M, A )
                  IF( ANORM.NE.ONE )
     $               CALL CSSCAL( N+2*M, ANORM, A, 1 )
               ELSE IF( IZERO.GT.0 ) THEN
*
*                 Reuse the last matrix by copying back the zeroed out
*                 elements.
*
                  IF( IZERO.EQ.1 ) THEN
                     A( N ) = Z( 2 )
                     IF( N.GT.1 )
     $                  A( 1 ) = Z( 3 )
                  ELSE IF( IZERO.EQ.N ) THEN
                     A( 3*N-2 ) = Z( 1 )
                     A( 2*N-1 ) = Z( 2 )
                  ELSE
                     A( 2*N-2+IZERO ) = Z( 1 )
                     A( N-1+IZERO ) = Z( 2 )
                     A( IZERO ) = Z( 3 )
                  END IF
               END IF
*
*              If IMAT > 7, set one column of the matrix to 0.
*
               IF( .NOT.ZEROT ) THEN
                  IZERO = 0
               ELSE IF( IMAT.EQ.8 ) THEN
                  IZERO = 1
                  Z( 2 ) = A( N )
                  A( N ) = ZERO
                  IF( N.GT.1 ) THEN
                     Z( 3 ) = A( 1 )
                     A( 1 ) = ZERO
                  END IF
               ELSE IF( IMAT.EQ.9 ) THEN
                  IZERO = N
                  Z( 1 ) = A( 3*N-2 )
                  Z( 2 ) = A( 2*N-1 )
                  A( 3*N-2 ) = ZERO
                  A( 2*N-1 ) = ZERO
               ELSE
                  IZERO = ( N+1 ) / 2
                  DO 20 I = IZERO, N - 1
                     A( 2*N-2+I ) = ZERO
                     A( N-1+I ) = ZERO
                     A( I ) = ZERO
   20             CONTINUE
                  A( 3*N-2 ) = ZERO
                  A( 2*N-1 ) = ZERO
               END IF
            END IF
*
*+    TEST 1
*           Factor A as L*U and compute the ratio
*              norm(L*U - A) / (n * norm(A) * EPS )
*
            CALL CCOPY( N+2*M, A, 1, AF, 1 )
            SRNAMT = 'CGTTRF'
            CALL CGTTRF( N, AF, AF( M+1 ), AF( N+M+1 ), AF( N+2*M+1 ),
     $                   IWORK, INFO )
*
*           Check error code from CGTTRF.
*
            IF( INFO.NE.IZERO )
     $         CALL ALAERH( PATH, 'CGTTRF', INFO, IZERO, ' ', N, N, 1,
     $                      1, -1, IMAT, NFAIL, NERRS, NOUT )
            TRFCON = INFO.NE.0
*
            CALL CGTT01( N, A, A( M+1 ), A( N+M+1 ), AF, AF( M+1 ),
     $                   AF( N+M+1 ), AF( N+2*M+1 ), IWORK, WORK, LDA,
     $                   RWORK, RESULT( 1 ) )
*
*           Print the test ratio if it is .GE. THRESH.
*
            IF( RESULT( 1 ).GE.THRESH ) THEN
               IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $            CALL ALAHD( NOUT, PATH )
               WRITE( NOUT, FMT = 9999 )N, IMAT, 1, RESULT( 1 )
               NFAIL = NFAIL + 1
            END IF
            NRUN = NRUN + 1
*
            DO 50 ITRAN = 1, 2
               TRANS = TRANSS( ITRAN )
               IF( ITRAN.EQ.1 ) THEN
                  NORM = 'O'
               ELSE
                  NORM = 'I'
               END IF
               ANORM = CLANGT( NORM, N, A, A( M+1 ), A( N+M+1 ) )
*
               IF( .NOT.TRFCON ) THEN
*
*                 Use CGTTRS to solve for one column at a time of
*                 inv(A), computing the maximum column sum as we go.
*
                  AINVNM = ZERO
                  DO 40 I = 1, N
                     DO 30 J = 1, N
                        X( J ) = ZERO
   30                CONTINUE
                     X( I ) = ONE
                     CALL CGTTRS( TRANS, N, 1, AF, AF( M+1 ),
     $                            AF( N+M+1 ), AF( N+2*M+1 ), IWORK, X,
     $                            LDA, INFO )
                     AINVNM = MAX( AINVNM, SCASUM( N, X, 1 ) )
   40             CONTINUE
*
*                 Compute RCONDC = 1 / (norm(A) * norm(inv(A))
*
                  IF( ANORM.LE.ZERO .OR. AINVNM.LE.ZERO ) THEN
                     RCONDC = ONE
                  ELSE
                     RCONDC = ( ONE / ANORM ) / AINVNM
                  END IF
                  IF( ITRAN.EQ.1 ) THEN
                     RCONDO = RCONDC
                  ELSE
                     RCONDI = RCONDC
                  END IF
               ELSE
                  RCONDC = ZERO
               END IF
*
*+    TEST 7
*              Estimate the reciprocal of the condition number of the
*              matrix.
*
               SRNAMT = 'CGTCON'
               CALL CGTCON( NORM, N, AF, AF( M+1 ), AF( N+M+1 ),
     $                      AF( N+2*M+1 ), IWORK, ANORM, RCOND, WORK,
     $                      INFO )
*
*              Check error code from CGTCON.
*
               IF( INFO.NE.0 )
     $            CALL ALAERH( PATH, 'CGTCON', INFO, 0, NORM, N, N, -1,
     $                         -1, -1, IMAT, NFAIL, NERRS, NOUT )
*
               RESULT( 7 ) = SGET06( RCOND, RCONDC )
*
*              Print the test ratio if it is .GE. THRESH.
*
               IF( RESULT( 7 ).GE.THRESH ) THEN
                  IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $               CALL ALAHD( NOUT, PATH )
                  WRITE( NOUT, FMT = 9997 )NORM, N, IMAT, 7,
     $               RESULT( 7 )
                  NFAIL = NFAIL + 1
               END IF
               NRUN = NRUN + 1
   50       CONTINUE
*
*           Skip the remaining tests if the matrix is singular.
*
            IF( TRFCON )
     $         GO TO 100
*
            DO 90 IRHS = 1, NNS
               NRHS = NSVAL( IRHS )
*
*              Generate NRHS random solution vectors.
*
               IX = 1
               DO 60 J = 1, NRHS
                  CALL CLARNV( 2, ISEED, N, XACT( IX ) )
                  IX = IX + LDA
   60          CONTINUE
*
               DO 80 ITRAN = 1, 3
                  TRANS = TRANSS( ITRAN )
                  IF( ITRAN.EQ.1 ) THEN
                     RCONDC = RCONDO
                  ELSE
                     RCONDC = RCONDI
                  END IF
*
*                 Set the right hand side.
*
                  CALL CLAGTM( TRANS, N, NRHS, ONE, A,
     $                         A( M+1 ), A( N+M+1 ), XACT, LDA,
     $                         ZERO, B, LDA )
*
*+    TEST 2
*              Solve op(A) * X = B and compute the residual.
*
                  CALL CLACPY( 'Full', N, NRHS, B, LDA, X, LDA )
                  SRNAMT = 'CGTTRS'
                  CALL CGTTRS( TRANS, N, NRHS, AF, AF( M+1 ),
     $                         AF( N+M+1 ), AF( N+2*M+1 ), IWORK, X,
     $                         LDA, INFO )
*
*              Check error code from CGTTRS.
*
                  IF( INFO.NE.0 )
     $               CALL ALAERH( PATH, 'CGTTRS', INFO, 0, TRANS, N, N,
     $                            -1, -1, NRHS, IMAT, NFAIL, NERRS,
     $                            NOUT )
*
                  CALL CLACPY( 'Full', N, NRHS, B, LDA, WORK, LDA )
                  CALL CGTT02( TRANS, N, NRHS, A, A( M+1 ), A( N+M+1 ),
     $                         X, LDA, WORK, LDA, RESULT( 2 ) )
*
*+    TEST 3
*              Check solution from generated exact solution.
*
                  CALL CGET04( N, NRHS, X, LDA, XACT, LDA, RCONDC,
     $                         RESULT( 3 ) )
*
*+    TESTS 4, 5, and 6
*              Use iterative refinement to improve the solution.
*
                  SRNAMT = 'CGTRFS'
                  CALL CGTRFS( TRANS, N, NRHS, A, A( M+1 ), A( N+M+1 ),
     $                         AF, AF( M+1 ), AF( N+M+1 ),
     $                         AF( N+2*M+1 ), IWORK, B, LDA, X, LDA,
     $                         RWORK, RWORK( NRHS+1 ), WORK,
     $                         RWORK( 2*NRHS+1 ), INFO )
*
*              Check error code from CGTRFS.
*
                  IF( INFO.NE.0 )
     $               CALL ALAERH( PATH, 'CGTRFS', INFO, 0, TRANS, N, N,
     $                            -1, -1, NRHS, IMAT, NFAIL, NERRS,
     $                            NOUT )
*
                  CALL CGET04( N, NRHS, X, LDA, XACT, LDA, RCONDC,
     $                         RESULT( 4 ) )
                  CALL CGTT05( TRANS, N, NRHS, A, A( M+1 ), A( N+M+1 ),
     $                         B, LDA, X, LDA, XACT, LDA, RWORK,
     $                         RWORK( NRHS+1 ), RESULT( 5 ) )
*
*              Print information about the tests that did not pass the
*              threshold.
*
                  DO 70 K = 2, 6
                     IF( RESULT( K ).GE.THRESH ) THEN
                        IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                     CALL ALAHD( NOUT, PATH )
                        WRITE( NOUT, FMT = 9998 )TRANS, N, NRHS, IMAT,
     $                     K, RESULT( K )
                        NFAIL = NFAIL + 1
                     END IF
   70             CONTINUE
                  NRUN = NRUN + 5
   80          CONTINUE
   90       CONTINUE
  100    CONTINUE
  110 CONTINUE
*
*     Print a summary of the results.
*
      CALL ALASUM( PATH, NOUT, NFAIL, NRUN, NERRS )
*
 9999 FORMAT( 12X, 'N =', I5, ',', 10X, ' type ', I2, ', test(', I2,
     $      ') = ', G12.5 )
 9998 FORMAT( ' TRANS=''', A1, ''', N =', I5, ', NRHS=', I3, ', type ',
     $      I2, ', test(', I2, ') = ', G12.5 )
 9997 FORMAT( ' NORM =''', A1, ''', N =', I5, ',', 10X, ' type ', I2,
     $      ', test(', I2, ') = ', G12.5 )
      RETURN
*
*     End of CCHKGT
*
      END
