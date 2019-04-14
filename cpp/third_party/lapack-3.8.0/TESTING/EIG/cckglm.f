*> \brief \b CCKGLM
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CCKGLM( NN, NVAL, MVAL, PVAL, NMATS, ISEED, THRESH,
*                          NMAX, A, AF, B, BF, X, WORK, RWORK, NIN, NOUT,
*                          INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, NIN, NMATS, NMAX, NN, NOUT
*       REAL               THRESH
*       ..
*       .. Array Arguments ..
*       INTEGER            ISEED( 4 ), MVAL( * ), NVAL( * ), PVAL( * )
*       REAL               RWORK( * )
*       COMPLEX            A( * ), AF( * ), B( * ), BF( * ), WORK( * ),
*      $                   X( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CCKGLM tests CGGGLM - subroutine for solving generalized linear
*>                       model problem.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] NN
*> \verbatim
*>          NN is INTEGER
*>          The number of values of N, M and P contained in the vectors
*>          NVAL, MVAL and PVAL.
*> \endverbatim
*>
*> \param[in] NVAL
*> \verbatim
*>          NVAL is INTEGER array, dimension (NN)
*>          The values of the matrix row dimension N.
*> \endverbatim
*>
*> \param[in] MVAL
*> \verbatim
*>          MVAL is INTEGER array, dimension (NN)
*>          The values of the matrix column dimension M.
*> \endverbatim
*>
*> \param[in] PVAL
*> \verbatim
*>          PVAL is INTEGER array, dimension (NN)
*>          The values of the matrix column dimension P.
*> \endverbatim
*>
*> \param[in] NMATS
*> \verbatim
*>          NMATS is INTEGER
*>          The number of matrix types to be tested for each combination
*>          of matrix dimensions.  If NMATS >= NTYPES (the maximum
*>          number of matrix types), then all the different types are
*>          generated for testing.  If NMATS < NTYPES, another input line
*>          is read to get the numbers of the matrix types to be used.
*> \endverbatim
*>
*> \param[in,out] ISEED
*> \verbatim
*>          ISEED is INTEGER array, dimension (4)
*>          On entry, the seed of the random number generator.  The array
*>          elements should be between 0 and 4095, otherwise they will be
*>          reduced mod 4096, and ISEED(4) must be odd.
*>          On exit, the next seed in the random number sequence after
*>          all the test matrices have been generated.
*> \endverbatim
*>
*> \param[in] THRESH
*> \verbatim
*>          THRESH is REAL
*>          The threshold value for the test ratios.  A result is
*>          included in the output file if RESID >= THRESH.  To have
*>          every test ratio printed, use THRESH = 0.
*> \endverbatim
*>
*> \param[in] NMAX
*> \verbatim
*>          NMAX is INTEGER
*>          The maximum value permitted for M or N, used in dimensioning
*>          the work arrays.
*> \endverbatim
*>
*> \param[out] A
*> \verbatim
*>          A is COMPLEX array, dimension (NMAX*NMAX)
*> \endverbatim
*>
*> \param[out] AF
*> \verbatim
*>          AF is COMPLEX array, dimension (NMAX*NMAX)
*> \endverbatim
*>
*> \param[out] B
*> \verbatim
*>          B is COMPLEX array, dimension (NMAX*NMAX)
*> \endverbatim
*>
*> \param[out] BF
*> \verbatim
*>          BF is COMPLEX array, dimension (NMAX*NMAX)
*> \endverbatim
*>
*> \param[out] X
*> \verbatim
*>          X is COMPLEX array, dimension (4*NMAX)
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is REAL array, dimension (NMAX)
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX array, dimension (NMAX*NMAX)
*> \endverbatim
*>
*> \param[in] NIN
*> \verbatim
*>          NIN is INTEGER
*>          The unit number for input.
*> \endverbatim
*>
*> \param[in] NOUT
*> \verbatim
*>          NOUT is INTEGER
*>          The unit number for output.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0 :  successful exit
*>          > 0 :  If CLATMS returns an error code, the absolute value
*>                 of it is returned.
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
*> \ingroup complex_eig
*
*  =====================================================================
      SUBROUTINE CCKGLM( NN, NVAL, MVAL, PVAL, NMATS, ISEED, THRESH,
     $                   NMAX, A, AF, B, BF, X, WORK, RWORK, NIN, NOUT,
     $                   INFO )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, NIN, NMATS, NMAX, NN, NOUT
      REAL               THRESH
*     ..
*     .. Array Arguments ..
      INTEGER            ISEED( 4 ), MVAL( * ), NVAL( * ), PVAL( * )
      REAL               RWORK( * )
      COMPLEX            A( * ), AF( * ), B( * ), BF( * ), WORK( * ),
     $                   X( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            NTYPES
      PARAMETER          ( NTYPES = 8 )
*     ..
*     .. Local Scalars ..
      LOGICAL            FIRSTT
      CHARACTER          DISTA, DISTB, TYPE
      CHARACTER*3        PATH
      INTEGER            I, IINFO, IK, IMAT, KLA, KLB, KUA, KUB, LDA,
     $                   LDB, LWORK, M, MODEA, MODEB, N, NFAIL, NRUN, P
      REAL               ANORM, BNORM, CNDNMA, CNDNMB, RESID
*     ..
*     .. Local Arrays ..
      LOGICAL            DOTYPE( NTYPES )
*     ..
*     .. External Functions ..
      COMPLEX            CLARND
      EXTERNAL           CLARND
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAHDG, ALAREQ, ALASUM, CGLMTS, CLATMS, SLATB9
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS
*     ..
*     .. Executable Statements ..
*
*     Initialize constants.
*
      PATH( 1: 3 ) = 'GLM'
      INFO = 0
      NRUN = 0
      NFAIL = 0
      FIRSTT = .TRUE.
      CALL ALAREQ( PATH, NMATS, DOTYPE, NTYPES, NIN, NOUT )
      LDA = NMAX
      LDB = NMAX
      LWORK = NMAX*NMAX
*
*     Check for valid input values.
*
      DO 10 IK = 1, NN
         M = MVAL( IK )
         P = PVAL( IK )
         N = NVAL( IK )
         IF( M.GT.N .OR. N.GT.M+P ) THEN
            IF( FIRSTT ) THEN
               WRITE( NOUT, FMT = * )
               FIRSTT = .FALSE.
            END IF
            WRITE( NOUT, FMT = 9997 )M, P, N
         END IF
   10 CONTINUE
      FIRSTT = .TRUE.
*
*     Do for each value of M in MVAL.
*
      DO 40 IK = 1, NN
         M = MVAL( IK )
         P = PVAL( IK )
         N = NVAL( IK )
         IF( M.GT.N .OR. N.GT.M+P )
     $      GO TO 40
*
         DO 30 IMAT = 1, NTYPES
*
*           Do the tests only if DOTYPE( IMAT ) is true.
*
            IF( .NOT.DOTYPE( IMAT ) )
     $         GO TO 30
*
*           Set up parameters with SLATB9 and generate test
*           matrices A and B with CLATMS.
*
            CALL SLATB9( PATH, IMAT, M, P, N, TYPE, KLA, KUA, KLB, KUB,
     $                   ANORM, BNORM, MODEA, MODEB, CNDNMA, CNDNMB,
     $                   DISTA, DISTB )
*
            CALL CLATMS( N, M, DISTA, ISEED, TYPE, RWORK, MODEA, CNDNMA,
     $                   ANORM, KLA, KUA, 'No packing', A, LDA, WORK,
     $                   IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUT, FMT = 9999 )IINFO
               INFO = ABS( IINFO )
               GO TO 30
            END IF
*
            CALL CLATMS( N, P, DISTB, ISEED, TYPE, RWORK, MODEB, CNDNMB,
     $                   BNORM, KLB, KUB, 'No packing', B, LDB, WORK,
     $                   IINFO )
            IF( IINFO.NE.0 ) THEN
               WRITE( NOUT, FMT = 9999 )IINFO
               INFO = ABS( IINFO )
               GO TO 30
            END IF
*
*           Generate random left hand side vector of GLM
*
            DO 20 I = 1, N
               X( I ) = CLARND( 2, ISEED )
   20       CONTINUE
*
            CALL CGLMTS( N, M, P, A, AF, LDA, B, BF, LDB, X,
     $                   X( NMAX+1 ), X( 2*NMAX+1 ), X( 3*NMAX+1 ),
     $                   WORK, LWORK, RWORK, RESID )
*
*           Print information about the tests that did not
*           pass the threshold.
*
            IF( RESID.GE.THRESH ) THEN
               IF( NFAIL.EQ.0 .AND. FIRSTT ) THEN
                  FIRSTT = .FALSE.
                  CALL ALAHDG( NOUT, PATH )
               END IF
               WRITE( NOUT, FMT = 9998 )N, M, P, IMAT, 1, RESID
               NFAIL = NFAIL + 1
            END IF
            NRUN = NRUN + 1
*
   30    CONTINUE
   40 CONTINUE
*
*     Print a summary of the results.
*
      CALL ALASUM( PATH, NOUT, NFAIL, NRUN, 0 )
*
 9999 FORMAT( ' CLATMS in CCKGLM INFO = ', I5 )
 9998 FORMAT( ' N=', I4, ' M=', I4, ', P=', I4, ', type ', I2,
     $      ', test ', I2, ', ratio=', G13.6 )
 9997 FORMAT( ' *** Invalid input  for GLM:  M = ', I6, ', P = ', I6,
     $      ', N = ', I6, ';', / '     must satisfy M <= N <= M+P  ',
     $      '(this set of values will be skipped)' )
      RETURN
*
*     End of CCKGLM
*
      END
