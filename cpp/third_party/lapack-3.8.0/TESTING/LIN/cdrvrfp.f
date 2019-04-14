*> \brief \b CDRVRFP
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CDRVRFP( NOUT, NN, NVAL, NNS, NSVAL, NNT, NTVAL,
*      +              THRESH, A, ASAV, AFAC, AINV, B,
*      +              BSAV, XACT, X, ARF, ARFINV,
*      +              C_WORK_CLATMS, C_WORK_CPOT02,
*      +              C_WORK_CPOT03, S_WORK_CLATMS, S_WORK_CLANHE,
*      +              S_WORK_CPOT01, S_WORK_CPOT02, S_WORK_CPOT03 )
*
*       .. Scalar Arguments ..
*       INTEGER            NN, NNS, NNT, NOUT
*       REAL               THRESH
*       ..
*       .. Array Arguments ..
*       INTEGER            NVAL( NN ), NSVAL( NNS ), NTVAL( NNT )
*       COMPLEX            A( * )
*       COMPLEX            AINV( * )
*       COMPLEX            ASAV( * )
*       COMPLEX            B( * )
*       COMPLEX            BSAV( * )
*       COMPLEX            AFAC( * )
*       COMPLEX            ARF( * )
*       COMPLEX            ARFINV( * )
*       COMPLEX            XACT( * )
*       COMPLEX            X( * )
*       COMPLEX            C_WORK_CLATMS( * )
*       COMPLEX            C_WORK_CPOT02( * )
*       COMPLEX            C_WORK_CPOT03( * )
*       REAL               S_WORK_CLATMS( * )
*       REAL               S_WORK_CLANHE( * )
*       REAL               S_WORK_CPOT01( * )
*       REAL               S_WORK_CPOT02( * )
*       REAL               S_WORK_CPOT03( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CDRVRFP tests the LAPACK RFP routines:
*>     CPFTRF, CPFTRS, and CPFTRI.
*>
*> This testing routine follow the same tests as CDRVPO (test for the full
*> format Symmetric Positive Definite solver).
*>
*> The tests are performed in Full Format, conversion back and forth from
*> full format to RFP format are performed using the routines CTRTTF and
*> CTFTTR.
*>
*> First, a specific matrix A of size N is created. There is nine types of
*> different matrixes possible.
*>  1. Diagonal                        6. Random, CNDNUM = sqrt(0.1/EPS)
*>  2. Random, CNDNUM = 2              7. Random, CNDNUM = 0.1/EPS
*> *3. First row and column zero       8. Scaled near underflow
*> *4. Last row and column zero        9. Scaled near overflow
*> *5. Middle row and column zero
*> (* - tests error exits from CPFTRF, no test ratios are computed)
*> A solution XACT of size N-by-NRHS is created and the associated right
*> hand side B as well. Then CPFTRF is called to compute L (or U), the
*> Cholesky factor of A. Then L (or U) is used to solve the linear system
*> of equations AX = B. This gives X. Then L (or U) is used to compute the
*> inverse of A, AINV. The following four tests are then performed:
*> (1) norm( L*L' - A ) / ( N * norm(A) * EPS ) or
*>     norm( U'*U - A ) / ( N * norm(A) * EPS ),
*> (2) norm(B - A*X) / ( norm(A) * norm(X) * EPS ),
*> (3) norm( I - A*AINV ) / ( N * norm(A) * norm(AINV) * EPS ),
*> (4) ( norm(X-XACT) * RCOND ) / ( norm(XACT) * EPS ),
*> where EPS is the machine precision, RCOND the condition number of A, and
*> norm( . ) the 1-norm for (1,2,3) and the inf-norm for (4).
*> Errors occur when INFO parameter is not as expected. Failures occur when
*> a test ratios is greater than THRES.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] NOUT
*> \verbatim
*>          NOUT is INTEGER
*>                The unit number for output.
*> \endverbatim
*>
*> \param[in] NN
*> \verbatim
*>          NN is INTEGER
*>                The number of values of N contained in the vector NVAL.
*> \endverbatim
*>
*> \param[in] NVAL
*> \verbatim
*>          NVAL is INTEGER array, dimension (NN)
*>                The values of the matrix dimension N.
*> \endverbatim
*>
*> \param[in] NNS
*> \verbatim
*>          NNS is INTEGER
*>                The number of values of NRHS contained in the vector NSVAL.
*> \endverbatim
*>
*> \param[in] NSVAL
*> \verbatim
*>          NSVAL is INTEGER array, dimension (NNS)
*>                The values of the number of right-hand sides NRHS.
*> \endverbatim
*>
*> \param[in] NNT
*> \verbatim
*>          NNT is INTEGER
*>                The number of values of MATRIX TYPE contained in the vector NTVAL.
*> \endverbatim
*>
*> \param[in] NTVAL
*> \verbatim
*>          NTVAL is INTEGER array, dimension (NNT)
*>                The values of matrix type (between 0 and 9 for PO/PP/PF matrices).
*> \endverbatim
*>
*> \param[in] THRESH
*> \verbatim
*>          THRESH is REAL
*>                The threshold value for the test ratios.  A result is
*>                included in the output file if RESULT >= THRESH.  To have
*>                every test ratio printed, use THRESH = 0.
*> \endverbatim
*>
*> \param[out] A
*> \verbatim
*>          A is COMPLEX array, dimension (NMAX*NMAX)
*> \endverbatim
*>
*> \param[out] ASAV
*> \verbatim
*>          ASAV is COMPLEX array, dimension (NMAX*NMAX)
*> \endverbatim
*>
*> \param[out] AFAC
*> \verbatim
*>          AFAC is COMPLEX array, dimension (NMAX*NMAX)
*> \endverbatim
*>
*> \param[out] AINV
*> \verbatim
*>          AINV is COMPLEX array, dimension (NMAX*NMAX)
*> \endverbatim
*>
*> \param[out] B
*> \verbatim
*>          B is COMPLEX array, dimension (NMAX*MAXRHS)
*> \endverbatim
*>
*> \param[out] BSAV
*> \verbatim
*>          BSAV is COMPLEX array, dimension (NMAX*MAXRHS)
*> \endverbatim
*>
*> \param[out] XACT
*> \verbatim
*>          XACT is COMPLEX array, dimension (NMAX*MAXRHS)
*> \endverbatim
*>
*> \param[out] X
*> \verbatim
*>          X is COMPLEX array, dimension (NMAX*MAXRHS)
*> \endverbatim
*>
*> \param[out] ARF
*> \verbatim
*>          ARF is COMPLEX array, dimension ((NMAX*(NMAX+1))/2)
*> \endverbatim
*>
*> \param[out] ARFINV
*> \verbatim
*>          ARFINV is COMPLEX array, dimension ((NMAX*(NMAX+1))/2)
*> \endverbatim
*>
*> \param[out] C_WORK_CLATMS
*> \verbatim
*>          C_WORK_CLATMS is COMPLEX array, dimension ( 3*NMAX )
*> \endverbatim
*>
*> \param[out] C_WORK_CPOT02
*> \verbatim
*>          C_WORK_CPOT02 is COMPLEX array, dimension ( NMAX*MAXRHS )
*> \endverbatim
*>
*> \param[out] C_WORK_CPOT03
*> \verbatim
*>          C_WORK_CPOT03 is COMPLEX array, dimension ( NMAX*NMAX )
*> \endverbatim
*>
*> \param[out] S_WORK_CLATMS
*> \verbatim
*>          S_WORK_CLATMS is REAL array, dimension ( NMAX )
*> \endverbatim
*>
*> \param[out] S_WORK_CLANHE
*> \verbatim
*>          S_WORK_CLANHE is REAL array, dimension ( NMAX )
*> \endverbatim
*>
*> \param[out] S_WORK_CPOT01
*> \verbatim
*>          S_WORK_CPOT01 is REAL array, dimension ( NMAX )
*> \endverbatim
*>
*> \param[out] S_WORK_CPOT02
*> \verbatim
*>          S_WORK_CPOT02 is REAL array, dimension ( NMAX )
*> \endverbatim
*>
*> \param[out] S_WORK_CPOT03
*> \verbatim
*>          S_WORK_CPOT03 is REAL array, dimension ( NMAX )
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
      SUBROUTINE CDRVRFP( NOUT, NN, NVAL, NNS, NSVAL, NNT, NTVAL,
     +              THRESH, A, ASAV, AFAC, AINV, B,
     +              BSAV, XACT, X, ARF, ARFINV,
     +              C_WORK_CLATMS, C_WORK_CPOT02,
     +              C_WORK_CPOT03, S_WORK_CLATMS, S_WORK_CLANHE,
     +              S_WORK_CPOT01, S_WORK_CPOT02, S_WORK_CPOT03 )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            NN, NNS, NNT, NOUT
      REAL               THRESH
*     ..
*     .. Array Arguments ..
      INTEGER            NVAL( NN ), NSVAL( NNS ), NTVAL( NNT )
      COMPLEX            A( * )
      COMPLEX            AINV( * )
      COMPLEX            ASAV( * )
      COMPLEX            B( * )
      COMPLEX            BSAV( * )
      COMPLEX            AFAC( * )
      COMPLEX            ARF( * )
      COMPLEX            ARFINV( * )
      COMPLEX            XACT( * )
      COMPLEX            X( * )
      COMPLEX            C_WORK_CLATMS( * )
      COMPLEX            C_WORK_CPOT02( * )
      COMPLEX            C_WORK_CPOT03( * )
      REAL               S_WORK_CLATMS( * )
      REAL               S_WORK_CLANHE( * )
      REAL               S_WORK_CPOT01( * )
      REAL               S_WORK_CPOT02( * )
      REAL               S_WORK_CPOT03( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ONE, ZERO
      PARAMETER          ( ONE = 1.0E+0, ZERO = 0.0E+0 )
      INTEGER            NTESTS
      PARAMETER          ( NTESTS = 4 )
*     ..
*     .. Local Scalars ..
      LOGICAL            ZEROT
      INTEGER            I, INFO, IUPLO, LDA, LDB, IMAT, NERRS, NFAIL,
     +                   NRHS, NRUN, IZERO, IOFF, K, NT, N, IFORM, IIN,
     +                   IIT, IIS
      CHARACTER          DIST, CTYPE, UPLO, CFORM
      INTEGER            KL, KU, MODE
      REAL               ANORM, AINVNM, CNDNUM, RCONDC
*     ..
*     .. Local Arrays ..
      CHARACTER          UPLOS( 2 ), FORMS( 2 )
      INTEGER            ISEED( 4 ), ISEEDY( 4 )
      REAL               RESULT( NTESTS )
*     ..
*     .. External Functions ..
      REAL               CLANHE
      EXTERNAL           CLANHE
*     ..
*     .. External Subroutines ..
      EXTERNAL           ALADHD, ALAERH, ALASVM, CGET04, CTFTTR, CLACPY,
     +                   CLAIPD, CLARHS, CLATB4, CLATMS, CPFTRI, CPFTRF,
     +                   CPFTRS, CPOT01, CPOT02, CPOT03, CPOTRI, CPOTRF,
     +                   CTRTTF
*     ..
*     .. Scalars in Common ..
      CHARACTER*32       SRNAMT
*     ..
*     .. Common blocks ..
      COMMON             / SRNAMC / SRNAMT
*     ..
*     .. Data statements ..
      DATA               ISEEDY / 1988, 1989, 1990, 1991 /
      DATA               UPLOS / 'U', 'L' /
      DATA               FORMS / 'N', 'C' /
*     ..
*     .. Executable Statements ..
*
*     Initialize constants and the random number seed.
*
      NRUN = 0
      NFAIL = 0
      NERRS = 0
      DO 10 I = 1, 4
         ISEED( I ) = ISEEDY( I )
   10 CONTINUE
*
      DO 130 IIN = 1, NN
*
         N = NVAL( IIN )
         LDA = MAX( N, 1 )
         LDB = MAX( N, 1 )
*
         DO 980 IIS = 1, NNS
*
            NRHS = NSVAL( IIS )
*
            DO 120 IIT = 1, NNT
*
               IMAT = NTVAL( IIT )
*
*              If N.EQ.0, only consider the first type
*
               IF( N.EQ.0 .AND. IIT.GE.1 ) GO TO 120
*
*              Skip types 3, 4, or 5 if the matrix size is too small.
*
               IF( IMAT.EQ.4 .AND. N.LE.1 ) GO TO 120
               IF( IMAT.EQ.5 .AND. N.LE.2 ) GO TO 120
*
*              Do first for UPLO = 'U', then for UPLO = 'L'
*
               DO 110 IUPLO = 1, 2
                  UPLO = UPLOS( IUPLO )
*
*                 Do first for CFORM = 'N', then for CFORM = 'C'
*
                  DO 100 IFORM = 1, 2
                     CFORM = FORMS( IFORM )
*
*                    Set up parameters with CLATB4 and generate a test
*                    matrix with CLATMS.
*
                     CALL CLATB4( 'CPO', IMAT, N, N, CTYPE, KL, KU,
     +                            ANORM, MODE, CNDNUM, DIST )
*
                     SRNAMT = 'CLATMS'
                     CALL CLATMS( N, N, DIST, ISEED, CTYPE,
     +                            S_WORK_CLATMS,
     +                            MODE, CNDNUM, ANORM, KL, KU, UPLO, A,
     +                            LDA, C_WORK_CLATMS, INFO )
*
*                    Check error code from CLATMS.
*
                     IF( INFO.NE.0 ) THEN
                        CALL ALAERH( 'CPF', 'CLATMS', INFO, 0, UPLO, N,
     +                               N, -1, -1, -1, IIT, NFAIL, NERRS,
     +                               NOUT )
                        GO TO 100
                     END IF
*
*                    For types 3-5, zero one row and column of the matrix to
*                    test that INFO is returned correctly.
*
                     ZEROT = IMAT.GE.3 .AND. IMAT.LE.5
                     IF( ZEROT ) THEN
                        IF( IIT.EQ.3 ) THEN
                           IZERO = 1
                        ELSE IF( IIT.EQ.4 ) THEN
                           IZERO = N
                        ELSE
                           IZERO = N / 2 + 1
                        END IF
                        IOFF = ( IZERO-1 )*LDA
*
*                       Set row and column IZERO of A to 0.
*
                        IF( IUPLO.EQ.1 ) THEN
                           DO 20 I = 1, IZERO - 1
                              A( IOFF+I ) = ZERO
   20                      CONTINUE
                           IOFF = IOFF + IZERO
                           DO 30 I = IZERO, N
                              A( IOFF ) = ZERO
                              IOFF = IOFF + LDA
   30                      CONTINUE
                        ELSE
                           IOFF = IZERO
                           DO 40 I = 1, IZERO - 1
                              A( IOFF ) = ZERO
                              IOFF = IOFF + LDA
   40                      CONTINUE
                           IOFF = IOFF - IZERO
                           DO 50 I = IZERO, N
                              A( IOFF+I ) = ZERO
   50                      CONTINUE
                        END IF
                     ELSE
                        IZERO = 0
                     END IF
*
*                    Set the imaginary part of the diagonals.
*
                     CALL CLAIPD( N, A, LDA+1, 0 )
*
*                    Save a copy of the matrix A in ASAV.
*
                     CALL CLACPY( UPLO, N, N, A, LDA, ASAV, LDA )
*
*                    Compute the condition number of A (RCONDC).
*
                     IF( ZEROT ) THEN
                        RCONDC = ZERO
                     ELSE
*
*                       Compute the 1-norm of A.
*
                        ANORM = CLANHE( '1', UPLO, N, A, LDA,
     +                         S_WORK_CLANHE )
*
*                       Factor the matrix A.
*
                        CALL CPOTRF( UPLO, N, A, LDA, INFO )
*
*                       Form the inverse of A.
*
                        CALL CPOTRI( UPLO, N, A, LDA, INFO )
*
*                       Compute the 1-norm condition number of A.
*
      					IF ( N .NE. 0 ) THEN
                           AINVNM = CLANHE( '1', UPLO, N, A, LDA,
     +                           S_WORK_CLANHE )
                           RCONDC = ( ONE / ANORM ) / AINVNM
*
*                          Restore the matrix A.
*
                        CALL CLACPY( UPLO, N, N, ASAV, LDA, A, LDA )
                        END IF

*
                     END IF
*
*                    Form an exact solution and set the right hand side.
*
                     SRNAMT = 'CLARHS'
                     CALL CLARHS( 'CPO', 'N', UPLO, ' ', N, N, KL, KU,
     +                            NRHS, A, LDA, XACT, LDA, B, LDA,
     +                            ISEED, INFO )
                     CALL CLACPY( 'Full', N, NRHS, B, LDA, BSAV, LDA )
*
*                    Compute the L*L' or U'*U factorization of the
*                    matrix and solve the system.
*
                     CALL CLACPY( UPLO, N, N, A, LDA, AFAC, LDA )
                     CALL CLACPY( 'Full', N, NRHS, B, LDB, X, LDB )
*
                     SRNAMT = 'CTRTTF'
                     CALL CTRTTF( CFORM, UPLO, N, AFAC, LDA, ARF, INFO )
                     SRNAMT = 'CPFTRF'
                     CALL CPFTRF( CFORM, UPLO, N, ARF, INFO )
*
*                    Check error code from CPFTRF.
*
                     IF( INFO.NE.IZERO ) THEN
*
*                       LANGOU: there is a small hick here: IZERO should
*                       always be INFO however if INFO is ZERO, ALAERH does not
*                       complain.
*
                         CALL ALAERH( 'CPF', 'CPFSV ', INFO, IZERO,
     +                                UPLO, N, N, -1, -1, NRHS, IIT,
     +                                NFAIL, NERRS, NOUT )
                         GO TO 100
                      END IF
*
*                     Skip the tests if INFO is not 0.
*
                     IF( INFO.NE.0 ) THEN
                        GO TO 100
                     END IF
*
                     SRNAMT = 'CPFTRS'
                     CALL CPFTRS( CFORM, UPLO, N, NRHS, ARF, X, LDB,
     +                            INFO )
*
                     SRNAMT = 'CTFTTR'
                     CALL CTFTTR( CFORM, UPLO, N, ARF, AFAC, LDA, INFO )
*
*                    Reconstruct matrix from factors and compute
*                    residual.
*
                     CALL CLACPY( UPLO, N, N, AFAC, LDA, ASAV, LDA )
                     CALL CPOT01( UPLO, N, A, LDA, AFAC, LDA,
     +                             S_WORK_CPOT01, RESULT( 1 ) )
                     CALL CLACPY( UPLO, N, N, ASAV, LDA, AFAC, LDA )
*
*                    Form the inverse and compute the residual.
*
                    IF(MOD(N,2).EQ.0)THEN
                       CALL CLACPY( 'A', N+1, N/2, ARF, N+1, ARFINV,
     +                               N+1 )
                    ELSE
                       CALL CLACPY( 'A', N, (N+1)/2, ARF, N, ARFINV,
     +                               N )
                    END IF
*
                     SRNAMT = 'CPFTRI'
                     CALL CPFTRI( CFORM, UPLO, N, ARFINV , INFO )
*
                     SRNAMT = 'CTFTTR'
                     CALL CTFTTR( CFORM, UPLO, N, ARFINV, AINV, LDA,
     +                            INFO )
*
*                    Check error code from CPFTRI.
*
                     IF( INFO.NE.0 )
     +                  CALL ALAERH( 'CPO', 'CPFTRI', INFO, 0, UPLO, N,
     +                               N, -1, -1, -1, IMAT, NFAIL, NERRS,
     +                               NOUT )
*
                     CALL CPOT03( UPLO, N, A, LDA, AINV, LDA,
     +                            C_WORK_CPOT03, LDA, S_WORK_CPOT03,
     +                            RCONDC, RESULT( 2 ) )
*
*                    Compute residual of the computed solution.
*
                     CALL CLACPY( 'Full', N, NRHS, B, LDA,
     +                            C_WORK_CPOT02, LDA )
                     CALL CPOT02( UPLO, N, NRHS, A, LDA, X, LDA,
     +                            C_WORK_CPOT02, LDA, S_WORK_CPOT02,
     +                            RESULT( 3 ) )
*
*                    Check solution from generated exact solution.
*
                     CALL CGET04( N, NRHS, X, LDA, XACT, LDA, RCONDC,
     +                         RESULT( 4 ) )
                     NT = 4
*
*                    Print information about the tests that did not
*                    pass the threshold.
*
                     DO 60 K = 1, NT
                        IF( RESULT( K ).GE.THRESH ) THEN
                           IF( NFAIL.EQ.0 .AND. NERRS.EQ.0 )
     +                        CALL ALADHD( NOUT, 'CPF' )
                           WRITE( NOUT, FMT = 9999 )'CPFSV ', UPLO,
     +                            N, IIT, K, RESULT( K )
                           NFAIL = NFAIL + 1
                        END IF
   60                CONTINUE
                     NRUN = NRUN + NT
  100             CONTINUE
  110          CONTINUE
  120       CONTINUE
  980    CONTINUE
  130 CONTINUE
*
*     Print a summary of the results.
*
      CALL ALASVM( 'CPF', NOUT, NFAIL, NRUN, NERRS )
*
 9999 FORMAT( 1X, A6, ', UPLO=''', A1, ''', N =', I5, ', type ', I1,
     +      ', test(', I1, ')=', G12.5 )
*
      RETURN
*
*     End of CDRVRFP
*
      END
