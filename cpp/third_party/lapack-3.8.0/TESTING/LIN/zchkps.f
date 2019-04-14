*> \brief \b ZCHKPS
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZCHKPS( DOTYPE, NN, NVAL, NNB, NBVAL, NRANK, RANKVAL,
*                          THRESH, TSTERR, NMAX, A, AFAC, PERM, PIV, WORK,
*                          RWORK, NOUT )
*
*       .. Scalar Arguments ..
*       DOUBLE PRECISION   THRESH
*       INTEGER            NMAX, NN, NNB, NOUT, NRANK
*       LOGICAL            TSTERR
*       ..
*       .. Array Arguments ..
*       COMPLEX*16         A( * ), AFAC( * ), PERM( * ), WORK( * )
*       DOUBLE PRECISION   RWORK( * )
*       INTEGER            NBVAL( * ), NVAL( * ), PIV( * ), RANKVAL( * )
*       LOGICAL            DOTYPE( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZCHKPS tests ZPSTRF.
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
*>          The values of the block size NB.
*> \endverbatim
*>
*> \param[in] NRANK
*> \verbatim
*>          NRANK is INTEGER
*>          The number of values of RANK contained in the vector RANKVAL.
*> \endverbatim
*>
*> \param[in] RANKVAL
*> \verbatim
*>          RANKVAL is INTEGER array, dimension (NBVAL)
*>          The values of the block size NB.
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
*>          A is COMPLEX*16 array, dimension (NMAX*NMAX)
*> \endverbatim
*>
*> \param[out] AFAC
*> \verbatim
*>          AFAC is COMPLEX*16 array, dimension (NMAX*NMAX)
*> \endverbatim
*>
*> \param[out] PERM
*> \verbatim
*>          PERM is COMPLEX*16 array, dimension (NMAX*NMAX)
*> \endverbatim
*>
*> \param[out] PIV
*> \verbatim
*>          PIV is INTEGER array, dimension (NMAX)
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (NMAX*3)
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (NMAX)
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
      SUBROUTINE ZCHKPS( DOTYPE, NN, NVAL, NNB, NBVAL, NRANK, RANKVAL,
     $                   THRESH, TSTERR, NMAX, A, AFAC, PERM, PIV, WORK,
     $                   RWORK, NOUT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      DOUBLE PRECISION   THRESH
      INTEGER            NMAX, NN, NNB, NOUT, NRANK
      LOGICAL            TSTERR
*     ..
*     .. Array Arguments ..
      COMPLEX*16         A( * ), AFAC( * ), PERM( * ), WORK( * )
      DOUBLE PRECISION   RWORK( * )
      INTEGER            NBVAL( * ), NVAL( * ), PIV( * ), RANKVAL( * )
      LOGICAL            DOTYPE( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ONE
      PARAMETER          ( ONE = 1.0E+0 )
      INTEGER            NTYPES
      PARAMETER          ( NTYPES = 9 )
*     ..
*     .. Local Scalars ..
      DOUBLE PRECISION   ANORM, CNDNUM, RESULT, TOL
      INTEGER            COMPRANK, I, IMAT, IN, INB, INFO, IRANK, IUPLO,
     $                   IZERO, KL, KU, LDA, MODE, N, NB, NERRS, NFAIL,
     $                   NIMAT, NRUN, RANK, RANKDIFF
      CHARACTER          DIST, TYPE, UPLO
      CHARACTER*3        PATH
*     ..
*     .. Local Arrays ..
      INTEGER            ISEED( 4 ), ISEEDY( 4 )
      CHARACTER          UPLOS( 2 )
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAERH, ALAHD, ALASUM, XLAENV, ZERRPS, ZLACPY,
     $                   ZLATB5, ZLATMT, ZPST01, ZPSTRF
*     ..
*     .. Scalars in Common ..
      INTEGER            INFOT, NUNIT
      LOGICAL            LERR, OK
      CHARACTER*32       SRNAMT
*     ..
*     .. Common blocks ..
      COMMON             / INFOC / INFOT, NUNIT, OK, LERR
      COMMON             / SRNAMC / SRNAMT
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE, MAX, CEILING
*     ..
*     .. Data statements ..
      DATA               ISEEDY / 1988, 1989, 1990, 1991 /
      DATA               UPLOS / 'U', 'L' /
*     ..
*     .. Executable Statements ..
*
*     Initialize constants and the random number seed.
*
      PATH( 1: 1 ) = 'Zomplex Precision'
      PATH( 2: 3 ) = 'PS'
      NRUN = 0
      NFAIL = 0
      NERRS = 0
      DO 100 I = 1, 4
         ISEED( I ) = ISEEDY( I )
  100 CONTINUE
*
*     Test the error exits
*
      IF( TSTERR )
     $   CALL ZERRPS( PATH, NOUT )
      INFOT = 0
*
*     Do for each value of N in NVAL
*
      DO 150 IN = 1, NN
         N = NVAL( IN )
         LDA = MAX( N, 1 )
         NIMAT = NTYPES
         IF( N.LE.0 )
     $      NIMAT = 1
*
         IZERO = 0
         DO 140 IMAT = 1, NIMAT
*
*           Do the tests only if DOTYPE( IMAT ) is true.
*
            IF( .NOT.DOTYPE( IMAT ) )
     $         GO TO 140
*
*              Do for each value of RANK in RANKVAL
*
            DO 130 IRANK = 1, NRANK
*
*              Only repeat test 3 to 5 for different ranks
*              Other tests use full rank
*
               IF( ( IMAT.LT.3 .OR. IMAT.GT.5 ) .AND. IRANK.GT.1 )
     $            GO TO 130
*
               RANK = CEILING( ( N * DBLE( RANKVAL( IRANK ) ) )
     $              / 100.E+0 )
*
*
*           Do first for UPLO = 'U', then for UPLO = 'L'
*
               DO 120 IUPLO = 1, 2
                  UPLO = UPLOS( IUPLO )
*
*              Set up parameters with ZLATB5 and generate a test matrix
*              with ZLATMT.
*
                  CALL ZLATB5( PATH, IMAT, N, TYPE, KL, KU, ANORM,
     $                         MODE, CNDNUM, DIST )
*
                  SRNAMT = 'ZLATMT'
                  CALL ZLATMT( N, N, DIST, ISEED, TYPE, RWORK, MODE,
     $                         CNDNUM, ANORM, RANK, KL, KU, UPLO, A,
     $                         LDA, WORK, INFO )
*
*              Check error code from ZLATMT.
*
                  IF( INFO.NE.0 ) THEN
                    CALL ALAERH( PATH, 'ZLATMT', INFO, 0, UPLO, N,
     $                           N, -1, -1, -1, IMAT, NFAIL, NERRS,
     $                           NOUT )
                     GO TO 120
                  END IF
*
*              Do for each value of NB in NBVAL
*
                  DO 110 INB = 1, NNB
                     NB = NBVAL( INB )
                     CALL XLAENV( 1, NB )
*
*                 Compute the pivoted L*L' or U'*U factorization
*                 of the matrix.
*
                     CALL ZLACPY( UPLO, N, N, A, LDA, AFAC, LDA )
                     SRNAMT = 'ZPSTRF'
*
*                 Use default tolerance
*
                     TOL = -ONE
                     CALL ZPSTRF( UPLO, N, AFAC, LDA, PIV, COMPRANK,
     $                            TOL, RWORK, INFO )
*
*                 Check error code from ZPSTRF.
*
                     IF( (INFO.LT.IZERO)
     $                    .OR.(INFO.NE.IZERO.AND.RANK.EQ.N)
     $                    .OR.(INFO.LE.IZERO.AND.RANK.LT.N) ) THEN
                        CALL ALAERH( PATH, 'ZPSTRF', INFO, IZERO,
     $                               UPLO, N, N, -1, -1, NB, IMAT,
     $                               NFAIL, NERRS, NOUT )
                        GO TO 110
                     END IF
*
*                 Skip the test if INFO is not 0.
*
                     IF( INFO.NE.0 )
     $                  GO TO 110
*
*                 Reconstruct matrix from factors and compute residual.
*
*                 PERM holds permuted L*L^T or U^T*U
*
                     CALL ZPST01( UPLO, N, A, LDA, AFAC, LDA, PERM, LDA,
     $                            PIV, RWORK, RESULT, COMPRANK )
*
*                 Print information about the tests that did not pass
*                 the threshold or where computed rank was not RANK.
*
                     IF( N.EQ.0 )
     $                  COMPRANK = 0
                     RANKDIFF = RANK - COMPRANK
                     IF( RESULT.GE.THRESH ) THEN
                        IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                     CALL ALAHD( NOUT, PATH )
                        WRITE( NOUT, FMT = 9999 )UPLO, N, RANK,
     $                     RANKDIFF, NB, IMAT, RESULT
                        NFAIL = NFAIL + 1
                     END IF
                     NRUN = NRUN + 1
  110             CONTINUE
*
  120          CONTINUE
  130       CONTINUE
  140    CONTINUE
  150 CONTINUE
*
*     Print a summary of the results.
*
      CALL ALASUM( PATH, NOUT, NFAIL, NRUN, NERRS )
*
 9999 FORMAT( ' UPLO = ''', A1, ''', N =', I5, ', RANK =', I3,
     $      ', Diff =', I5, ', NB =', I4, ', type ', I2, ', Ratio =',
     $      G12.5 )
      RETURN
*
*     End of ZCHKPS
*
      END
