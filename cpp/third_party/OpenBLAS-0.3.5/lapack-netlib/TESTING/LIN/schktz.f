*> \brief \b SCHKTZ
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SCHKTZ( DOTYPE, NM, MVAL, NN, NVAL, THRESH, TSTERR, A,
*                          COPYA, S, TAU, WORK, NOUT )
*
*       .. Scalar Arguments ..
*       LOGICAL            TSTERR
*       INTEGER            NM, NN, NOUT
*       REAL               THRESH
*       ..
*       .. Array Arguments ..
*       LOGICAL            DOTYPE( * )
*       INTEGER            MVAL( * ), NVAL( * )
*       REAL               A( * ), COPYA( * ), S( * ),
*      $                   TAU( * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SCHKTZ tests STZRZF.
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
*>          The number of values of M contained in the vector MVAL.
*> \endverbatim
*>
*> \param[in] MVAL
*> \verbatim
*>          MVAL is INTEGER array, dimension (NM)
*>          The values of the matrix row dimension M.
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
*>          A is REAL array, dimension (MMAX*NMAX)
*>          where MMAX is the maximum value of M in MVAL and NMAX is the
*>          maximum value of N in NVAL.
*> \endverbatim
*>
*> \param[out] COPYA
*> \verbatim
*>          COPYA is REAL array, dimension (MMAX*NMAX)
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is REAL array, dimension
*>                      (min(MMAX,NMAX))
*> \endverbatim
*>
*> \param[out] TAU
*> \verbatim
*>          TAU is REAL array, dimension (MMAX)
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension
*>                      (MMAX*NMAX + 4*NMAX + MMAX)
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
*> \ingroup single_lin
*
*  =====================================================================
      SUBROUTINE SCHKTZ( DOTYPE, NM, MVAL, NN, NVAL, THRESH, TSTERR, A,
     $                   COPYA, S, TAU, WORK, NOUT )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      LOGICAL            TSTERR
      INTEGER            NM, NN, NOUT
      REAL               THRESH
*     ..
*     .. Array Arguments ..
      LOGICAL            DOTYPE( * )
      INTEGER            MVAL( * ), NVAL( * )
      REAL               A( * ), COPYA( * ), S( * ),
     $                   TAU( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            NTYPES
      PARAMETER          ( NTYPES = 3 )
      INTEGER            NTESTS
      PARAMETER          ( NTESTS = 3 )
      REAL               ONE, ZERO
      PARAMETER          ( ONE = 1.0E0, ZERO = 0.0E0 )
*     ..
*     .. Local Scalars ..
      CHARACTER*3        PATH
      INTEGER            I, IM, IMODE, IN, INFO, K, LDA, LWORK, M,
     $                   MNMIN, MODE, N, NERRS, NFAIL, NRUN
      REAL               EPS
*     ..
*     .. Local Arrays ..
      INTEGER            ISEED( 4 ), ISEEDY( 4 )
      REAL               RESULT( NTESTS )
*     ..
*     .. External Functions ..
      REAL               SLAMCH, SQRT12, SRZT01, SRZT02
      EXTERNAL           SLAMCH, SQRT12, SRZT01, SRZT02
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALAHD, ALASUM, SERRTZ, SGEQR2, SLACPY, SLAORD,
     $                   SLASET, SLATMS, STZRZF
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN
*     ..
*     .. Scalars in Common ..
      LOGICAL            LERR, OK
      CHARACTER*32       SRNAMT
      INTEGER            INFOT, IOUNIT
*     ..
*     .. Common blocks ..
      COMMON             / INFOC / INFOT, IOUNIT, OK, LERR
      COMMON             / SRNAMC / SRNAMT
*     ..
*     .. Data statements ..
      DATA               ISEEDY / 1988, 1989, 1990, 1991 /
*     ..
*     .. Executable Statements ..
*
*     Initialize constants and the random number seed.
*
      PATH( 1: 1 ) = 'Single precision'
      PATH( 2: 3 ) = 'TZ'
      NRUN = 0
      NFAIL = 0
      NERRS = 0
      DO 10 I = 1, 4
         ISEED( I ) = ISEEDY( I )
   10 CONTINUE
      EPS = SLAMCH( 'Epsilon' )
*
*     Test the error exits
*
      IF( TSTERR )
     $   CALL SERRTZ( PATH, NOUT )
      INFOT = 0
*
      DO 70 IM = 1, NM
*
*        Do for each value of M in MVAL.
*
         M = MVAL( IM )
         LDA = MAX( 1, M )
*
         DO 60 IN = 1, NN
*
*           Do for each value of N in NVAL for which M .LE. N.
*
            N = NVAL( IN )
            MNMIN = MIN( M, N )
            LWORK = MAX( 1, N*N+4*M+N, M*N+2*MNMIN+4*N )
*
            IF( M.LE.N ) THEN
               DO 50 IMODE = 1, NTYPES
                  IF( .NOT.DOTYPE( IMODE ) )
     $               GO TO 50
*
*                 Do for each type of singular value distribution.
*                    0:  zero matrix
*                    1:  one small singular value
*                    2:  exponential distribution
*
                  MODE = IMODE - 1
*
*                 Test STZRQF
*
*                 Generate test matrix of size m by n using
*                 singular value distribution indicated by `mode'.
*
                  IF( MODE.EQ.0 ) THEN
                     CALL SLASET( 'Full', M, N, ZERO, ZERO, A, LDA )
                     DO 30 I = 1, MNMIN
                        S( I ) = ZERO
   30                CONTINUE
                  ELSE
                     CALL SLATMS( M, N, 'Uniform', ISEED,
     $                            'Nonsymmetric', S, IMODE,
     $                            ONE / EPS, ONE, M, N, 'No packing', A,
     $                            LDA, WORK, INFO )
                     CALL SGEQR2( M, N, A, LDA, WORK, WORK( MNMIN+1 ),
     $                            INFO )
                     CALL SLASET( 'Lower', M-1, N, ZERO, ZERO, A( 2 ),
     $                            LDA )
                     CALL SLAORD( 'Decreasing', MNMIN, S, 1 )
                  END IF
*
*                 Save A and its singular values
*
                  CALL SLACPY( 'All', M, N, A, LDA, COPYA, LDA )
*
*                 Call STZRZF to reduce the upper trapezoidal matrix to
*                 upper triangular form.
*
                  SRNAMT = 'STZRZF'
                  CALL STZRZF( M, N, A, LDA, TAU, WORK, LWORK, INFO )
*
*                 Compute norm(svd(a) - svd(r))
*
                  RESULT( 1 ) = SQRT12( M, M, A, LDA, S, WORK,
     $                          LWORK )
*
*                 Compute norm( A - R*Q )
*
                  RESULT( 2 ) = SRZT01( M, N, COPYA, A, LDA, TAU, WORK,
     $                          LWORK )
*
*                 Compute norm(Q'*Q - I).
*
                  RESULT( 3 ) = SRZT02( M, N, A, LDA, TAU, WORK, LWORK )
*
*                 Print information about the tests that did not pass
*                 the threshold.
*
                  DO 40 K = 1, NTESTS
                     IF( RESULT( K ).GE.THRESH ) THEN
                        IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     $                     CALL ALAHD( NOUT, PATH )
                        WRITE( NOUT, FMT = 9999 )M, N, IMODE, K,
     $                     RESULT( K )
                        NFAIL = NFAIL + 1
                     END IF
   40             CONTINUE
                  NRUN = NRUN + 3
   50          CONTINUE
            END IF
   60    CONTINUE
   70 CONTINUE
*
*     Print a summary of the results.
*
      CALL ALASUM( PATH, NOUT, NFAIL, NRUN, NERRS )
*
 9999 FORMAT( ' M =', I5, ', N =', I5, ', type ', I2, ', test ', I2,
     $      ', ratio =', G12.5 )
*
*     End if SCHKTZ
*
      END
