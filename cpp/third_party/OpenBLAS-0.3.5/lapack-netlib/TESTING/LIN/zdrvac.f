*> \brief \b ZDRVAC
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZDRVAC( DOTYPE, NM, MVAL, NNS, NSVAL, THRESH, NMAX,
*                          A, AFAC, B, X, WORK,
*                          RWORK, SWORK, NOUT )
*
*       .. Scalar Arguments ..
*       INTEGER            NMAX, NM, NNS, NOUT
*       DOUBLE PRECISION   THRESH
*       ..
*       .. Array Arguments ..
*       LOGICAL            DOTYPE( * )
*       INTEGER            MVAL( * ), NSVAL( * )
*       DOUBLE PRECISION   RWORK( * )
*       COMPLEX            SWORK(*)
*       COMPLEX*16         A( * ), AFAC( * ), B( * ),
*      $                   WORK( * ), X( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZDRVAC tests ZCPOSV.
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
*> \param[in] NM
*> \verbatim
*>          NM is INTEGER
*>          The number of values of N contained in the vector MVAL.
*> \endverbatim
*>
*> \param[in] MVAL
*> \verbatim
*>          MVAL is INTEGER array, dimension (NM)
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
*> \param[in] NMAX
*> \verbatim
*>          NMAX is INTEGER
*>          The maximum value permitted for N, used in dimensioning the
*>          work arrays.
*> \endverbatim
*>
*> \param[out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (NMAX*NMAX)
*> \endverbatim
*>
*> \param[out] AFAC
*> \verbatim
*>          AFAC is COMPLEX*16 array, dimension (NMAX*NMAX)
*> \endverbatim
*>
*> \param[out] B
*> \verbatim
*>          B is COMPLEX*16 array, dimension (NMAX*NSMAX)
*> \endverbatim
*>
*> \param[out] X
*> \verbatim
*>          X is COMPLEX*16 array, dimension (NMAX*NSMAX)
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension
*>                      (NMAX*max(3,NSMAX))
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension
*>                      (max(2*NMAX,2*NSMAX+NWORK))
*> \endverbatim
*>
*> \param[out] SWORK
*> \verbatim
*>          SWORK is COMPLEX array, dimension
*>                      (NMAX*(NSMAX+NMAX))
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
      SUBROUTINE ZDRVAC( DOTYPE, NM, MVAL, NNS, NSVAL, THRESH, NMAX,
     $                   A, AFAC, B, X, WORK,
     $                   RWORK, SWORK, NOUT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            NMAX, NM, NNS, NOUT
      DOUBLE PRECISION   THRESH
*     ..
*     .. Array Arguments ..
      LOGICAL            DOTYPE( * )
      INTEGER            MVAL( * ), NSVAL( * )
      DOUBLE PRECISION   RWORK( * )
      COMPLEX            SWORK(*)
      COMPLEX*16         A( * ), AFAC( * ), B( * ),
     $                   WORK( * ), X( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO
      PARAMETER          ( ZERO = 0.0D+0 )
      INTEGER            NTYPES
      PARAMETER          ( NTYPES = 9 )
      INTEGER            NTESTS
      PARAMETER          ( NTESTS = 1 )
*     ..
*     .. Local Scalars ..
      LOGICAL            ZEROT
      CHARACTER          DIST, TYPE, UPLO, XTYPE
      CHARACTER*3        PATH
      INTEGER            I, IM, IMAT, INFO, IOFF, IRHS, IUPLO,
     $                   IZERO, KL, KU, LDA, MODE, N,
     $                   NERRS, NFAIL, NIMAT, NRHS, NRUN
      DOUBLE PRECISION   ANORM, CNDNUM
*     ..
*     .. Local Arrays ..
      CHARACTER          UPLOS( 2 )
      INTEGER            ISEED( 4 ), ISEEDY( 4 )
      DOUBLE PRECISION   RESULT( NTESTS )
*     ..
*     .. Local Variables ..
      INTEGER            ITER, KASE
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAERH, ZLACPY, ZLAIPD,
     $                   ZLARHS, ZLATB4, ZLATMS,
     $                   ZPOT06, ZCPOSV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE, MAX, SQRT
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
      KASE = 0
      PATH( 1: 1 ) = 'Zomplex precision'
      PATH( 2: 3 ) = 'PO'
      NRUN = 0
      NFAIL = 0
      NERRS = 0
      DO 10 I = 1, 4
         ISEED( I ) = ISEEDY( I )
   10 CONTINUE
*
      INFOT = 0
*
*     Do for each value of N in MVAL
*
      DO 120 IM = 1, NM
         N = MVAL( IM )
         LDA = MAX( N, 1 )
         NIMAT = NTYPES
         IF( N.LE.0 )
     $      NIMAT = 1
*
         DO 110 IMAT = 1, NIMAT
*
*           Do the tests only if DOTYPE( IMAT ) is true.
*
            IF( .NOT.DOTYPE( IMAT ) )
     $         GO TO 110
*
*           Skip types 3, 4, or 5 if the matrix size is too small.
*
            ZEROT = IMAT.GE.3 .AND. IMAT.LE.5
            IF( ZEROT .AND. N.LT.IMAT-2 )
     $         GO TO 110
*
*           Do first for UPLO = 'U', then for UPLO = 'L'
*
            DO 100 IUPLO = 1, 2
               UPLO = UPLOS( IUPLO )
*
*              Set up parameters with ZLATB4 and generate a test matrix
*              with ZLATMS.
*
               CALL ZLATB4( PATH, IMAT, N, N, TYPE, KL, KU, ANORM, MODE,
     $                      CNDNUM, DIST )
*
               SRNAMT = 'ZLATMS'
               CALL ZLATMS( N, N, DIST, ISEED, TYPE, RWORK, MODE,
     $                      CNDNUM, ANORM, KL, KU, UPLO, A, LDA, WORK,
     $                      INFO )
*
*              Check error code from ZLATMS.
*
               IF( INFO.NE.0 ) THEN
                  CALL ALAERH( PATH, 'ZLATMS', INFO, 0, UPLO, N, N, -1,
     $                         -1, -1, IMAT, NFAIL, NERRS, NOUT )
                  GO TO 100
               END IF
*
*              For types 3-5, zero one row and column of the matrix to
*              test that INFO is returned correctly.
*
               IF( ZEROT ) THEN
                  IF( IMAT.EQ.3 ) THEN
                     IZERO = 1
                  ELSE IF( IMAT.EQ.4 ) THEN
                     IZERO = N
                  ELSE
                     IZERO = N / 2 + 1
                  END IF
                  IOFF = ( IZERO-1 )*LDA
*
*                 Set row and column IZERO of A to 0.
*
                  IF( IUPLO.EQ.1 ) THEN
                     DO 20 I = 1, IZERO - 1
                        A( IOFF+I ) = ZERO
   20                CONTINUE
                     IOFF = IOFF + IZERO
                     DO 30 I = IZERO, N
                        A( IOFF ) = ZERO
                        IOFF = IOFF + LDA
   30                CONTINUE
                  ELSE
                     IOFF = IZERO
                     DO 40 I = 1, IZERO - 1
                        A( IOFF ) = ZERO
                        IOFF = IOFF + LDA
   40                CONTINUE
                     IOFF = IOFF - IZERO
                     DO 50 I = IZERO, N
                        A( IOFF+I ) = ZERO
   50                CONTINUE
                  END IF
               ELSE
                  IZERO = 0
               END IF
*
*              Set the imaginary part of the diagonals.
*
               CALL ZLAIPD( N, A, LDA+1, 0 )
*
               DO 60 IRHS = 1, NNS
                  NRHS = NSVAL( IRHS )
                  XTYPE = 'N'
*
*                 Form an exact solution and set the right hand side.
*
                  SRNAMT = 'ZLARHS'
                  CALL ZLARHS( PATH, XTYPE, UPLO, ' ', N, N, KL, KU,
     $                         NRHS, A, LDA, X, LDA, B, LDA,
     $                         ISEED, INFO )
*
*                 Compute the L*L' or U'*U factorization of the
*                 matrix and solve the system.
*
                  SRNAMT = 'ZCPOSV '
                  KASE = KASE + 1
*
                  CALL ZLACPY( 'All', N, N, A, LDA, AFAC, LDA)
*
                  CALL ZCPOSV( UPLO, N, NRHS, AFAC, LDA, B, LDA, X, LDA,
     $                         WORK, SWORK, RWORK, ITER, INFO )
*
                  IF (ITER.LT.0) THEN
                     CALL ZLACPY( 'All', N, N, A, LDA, AFAC, LDA )
                  ENDIF
*
*                 Check error code from ZCPOSV .
*
                  IF( INFO.NE.IZERO ) THEN
*
                     IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                  CALL ALAHD( NOUT, PATH )
                     NERRS = NERRS + 1
*
                     IF( INFO.NE.IZERO .AND. IZERO.NE.0 ) THEN
                        WRITE( NOUT, FMT = 9988 )'ZCPOSV',INFO,IZERO,N,
     $                     IMAT
                     ELSE
                        WRITE( NOUT, FMT = 9975 )'ZCPOSV',INFO,N,IMAT
                     END IF
                  END IF
*
*                 Skip the remaining test if the matrix is singular.
*
                  IF( INFO.NE.0 )
     $               GO TO 110
*
*                 Check the quality of the solution
*
                  CALL ZLACPY( 'All', N, NRHS, B, LDA, WORK, LDA )
*
                  CALL ZPOT06( UPLO, N, NRHS, A, LDA, X, LDA, WORK,
     $               LDA, RWORK, RESULT( 1 ) )
*
*                 Check if the test passes the tesing.
*                 Print information about the tests that did not
*                 pass the testing.
*
*                 If iterative refinement has been used and claimed to
*                 be successful (ITER>0), we want
*                 NORM1(B - A*X)/(NORM1(A)*NORM1(X)*EPS*SRQT(N)) < 1
*
*                 If double precision has been used (ITER<0), we want
*                 NORM1(B - A*X)/(NORM1(A)*NORM1(X)*EPS) < THRES
*                 (Cf. the linear solver testing routines)
*
                  IF ((THRESH.LE.0.0E+00)
     $               .OR.((ITER.GE.0).AND.(N.GT.0)
     $               .AND.(RESULT(1).GE.SQRT(DBLE(N))))
     $               .OR.((ITER.LT.0).AND.(RESULT(1).GE.THRESH))) THEN
*
                     IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 ) THEN
                        WRITE( NOUT, FMT = 8999 )'ZPO'
                        WRITE( NOUT, FMT = '( '' Matrix types:'' )' )
                        WRITE( NOUT, FMT = 8979 )
                        WRITE( NOUT, FMT = '( '' Test ratios:'' )' )
                        WRITE( NOUT, FMT = 8960 )1
                        WRITE( NOUT, FMT = '( '' Messages:'' )' )
                     END IF
*
                     WRITE( NOUT, FMT = 9998 )UPLO, N, NRHS, IMAT, 1,
     $                  RESULT( 1 )
*
                     NFAIL = NFAIL + 1
*
                  END IF
*
                  NRUN = NRUN + 1
*
   60          CONTINUE
  100       CONTINUE
  110    CONTINUE
  120 CONTINUE
*
*     Print a summary of the results.
*
      IF( NFAIL.GT.0 ) THEN
         WRITE( NOUT, FMT = 9996 )'ZCPOSV', NFAIL, NRUN
      ELSE
         WRITE( NOUT, FMT = 9995 )'ZCPOSV', NRUN
      END IF
      IF( NERRS.GT.0 ) THEN
         WRITE( NOUT, FMT = 9994 )NERRS
      END IF
*
 9998 FORMAT( ' UPLO=''', A1, ''', N =', I5, ', NRHS=', I3, ', type ',
     $      I2, ', test(', I2, ') =', G12.5 )
 9996 FORMAT( 1X, A6, ': ', I6, ' out of ', I6,
     $      ' tests failed to pass the threshold' )
 9995 FORMAT( /1X, 'All tests for ', A6,
     $      ' routines passed the threshold ( ', I6, ' tests run)' )
 9994 FORMAT( 6X, I6, ' error messages recorded' )
*
*     SUBNAM, INFO, INFOE, N, IMAT
*
 9988 FORMAT( ' *** ', A6, ' returned with INFO =', I5, ' instead of ',
     $      I5, / ' ==> N =', I5, ', type ',
     $      I2 )
*
*     SUBNAM, INFO, N, IMAT
*
 9975 FORMAT( ' *** Error code from ', A6, '=', I5, ' for M=', I5,
     $      ', type ', I2 )
 8999 FORMAT( / 1X, A3, ':  positive definite dense matrices' )
 8979 FORMAT( 4X, '1. Diagonal', 24X, '7. Last n/2 columns zero', / 4X,
     $      '2. Upper triangular', 16X,
     $      '8. Random, CNDNUM = sqrt(0.1/EPS)', / 4X,
     $      '3. Lower triangular', 16X, '9. Random, CNDNUM = 0.1/EPS',
     $      / 4X, '4. Random, CNDNUM = 2', 13X,
     $      '10. Scaled near underflow', / 4X, '5. First column zero',
     $      14X, '11. Scaled near overflow', / 4X,
     $      '6. Last column zero' )
 8960 FORMAT( 3X, I2, ': norm_1( B - A * X )  / ',
     $      '( norm_1(A) * norm_1(X) * EPS * SQRT(N) ) > 1 if ITERREF',
     $      / 4x, 'or norm_1( B - A * X )  / ',
     $      '( norm_1(A) * norm_1(X) * EPS ) > THRES if ZPOTRF' )

      RETURN
*
*     End of ZDRVAC
*
      END
