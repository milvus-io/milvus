*> \brief \b DEBCHVXX
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*     SUBROUTINE DEBCHVXX( THRESH, PATH )
*
*     .. Scalar Arguments ..
*      DOUBLE PRECISION  THRESH
*      CHARACTER*3       PATH
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>  DEBCHVXX will run D**SVXX on a series of Hilbert matrices and then
*>  compare the error bounds returned by D**SVXX to see if the returned
*>  answer indeed falls within those bounds.
*>
*>  Eight test ratios will be computed.  The tests will pass if they are .LT.
*>  THRESH.  There are two cases that are determined by 1 / (SQRT( N ) * EPS).
*>  If that value is .LE. to the component wise reciprocal condition number,
*>  it uses the guaranteed case, other wise it uses the unguaranteed case.
*>
*>  Test ratios:
*>     Let Xc be X_computed and Xt be X_truth.
*>     The norm used is the infinity norm.
*>
*>     Let A be the guaranteed case and B be the unguaranteed case.
*>
*>       1. Normwise guaranteed forward error bound.
*>       A: norm ( abs( Xc - Xt ) / norm ( Xt ) .LE. ERRBND( *, nwise_i, bnd_i ) and
*>          ERRBND( *, nwise_i, bnd_i ) .LE. MAX(SQRT(N),10) * EPS.
*>          If these conditions are met, the test ratio is set to be
*>          ERRBND( *, nwise_i, bnd_i ) / MAX(SQRT(N), 10).  Otherwise it is 1/EPS.
*>       B: For this case, CGESVXX should just return 1.  If it is less than
*>          one, treat it the same as in 1A.  Otherwise it fails. (Set test
*>          ratio to ERRBND( *, nwise_i, bnd_i ) * THRESH?)
*>
*>       2. Componentwise guaranteed forward error bound.
*>       A: norm ( abs( Xc(j) - Xt(j) ) ) / norm (Xt(j)) .LE. ERRBND( *, cwise_i, bnd_i )
*>          for all j .AND. ERRBND( *, cwise_i, bnd_i ) .LE. MAX(SQRT(N), 10) * EPS.
*>          If these conditions are met, the test ratio is set to be
*>          ERRBND( *, cwise_i, bnd_i ) / MAX(SQRT(N), 10).  Otherwise it is 1/EPS.
*>       B: Same as normwise test ratio.
*>
*>       3. Backwards error.
*>       A: The test ratio is set to BERR/EPS.
*>       B: Same test ratio.
*>
*>       4. Reciprocal condition number.
*>       A: A condition number is computed with Xt and compared with the one
*>          returned from CGESVXX.  Let RCONDc be the RCOND returned by D**SVXX
*>          and RCONDt be the RCOND from the truth value.  Test ratio is set to
*>          MAX(RCONDc/RCONDt, RCONDt/RCONDc).
*>       B: Test ratio is set to 1 / (EPS * RCONDc).
*>
*>       5. Reciprocal normwise condition number.
*>       A: The test ratio is set to
*>          MAX(ERRBND( *, nwise_i, cond_i ) / NCOND, NCOND / ERRBND( *, nwise_i, cond_i )).
*>       B: Test ratio is set to 1 / (EPS * ERRBND( *, nwise_i, cond_i )).
*>
*>       6. Reciprocal componentwise condition number.
*>       A: Test ratio is set to
*>          MAX(ERRBND( *, cwise_i, cond_i ) / CCOND, CCOND / ERRBND( *, cwise_i, cond_i )).
*>       B: Test ratio is set to 1 / (EPS * ERRBND( *, cwise_i, cond_i )).
*>
*>     .. Parameters ..
*>     NMAX is determined by the largest number in the inverse of the hilbert
*>     matrix.  Precision is exhausted when the largest entry in it is greater
*>     than 2 to the power of the number of bits in the fraction of the data
*>     type used plus one, which is 24 for single precision.
*>     NMAX should be 6 for single and 11 for double.
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
      SUBROUTINE DEBCHVXX( THRESH, PATH )
      IMPLICIT NONE
*     .. Scalar Arguments ..
      DOUBLE PRECISION  THRESH
      CHARACTER*3       PATH

      INTEGER            NMAX, NPARAMS, NERRBND, NTESTS, KL, KU
      PARAMETER          (NMAX = 10, NPARAMS = 2, NERRBND = 3,
     $                    NTESTS = 6)

*     .. Local Scalars ..
      INTEGER            N, NRHS, INFO, I ,J, k, NFAIL, LDA,
     $                   N_AUX_TESTS, LDAB, LDAFB
      CHARACTER          FACT, TRANS, UPLO, EQUED
      CHARACTER*2        C2
      CHARACTER(3)       NGUAR, CGUAR
      LOGICAL            printed_guide
      DOUBLE PRECISION   NCOND, CCOND, M, NORMDIF, NORMT, RCOND,
     $                   RNORM, RINORM, SUMR, SUMRI, EPS,
     $                   BERR(NMAX), RPVGRW, ORCOND,
     $                   CWISE_ERR, NWISE_ERR, CWISE_BND, NWISE_BND,
     $                   CWISE_RCOND, NWISE_RCOND,
     $                   CONDTHRESH, ERRTHRESH

*     .. Local Arrays ..
      DOUBLE PRECISION   TSTRAT(NTESTS), RINV(NMAX), PARAMS(NPARAMS),
     $                   S(NMAX),R(NMAX),C(NMAX), DIFF(NMAX, NMAX),
     $                   ERRBND_N(NMAX*3), ERRBND_C(NMAX*3),
     $                   A(NMAX,NMAX),INVHILB(NMAX,NMAX),X(NMAX,NMAX),
     $                   AB( (NMAX-1)+(NMAX-1)+1, NMAX ),
     $                   ABCOPY( (NMAX-1)+(NMAX-1)+1, NMAX ),
     $                   AFB( 2*(NMAX-1)+(NMAX-1)+1, NMAX ),
     $                   WORK(NMAX*3*5), AF(NMAX, NMAX),B(NMAX, NMAX),
     $                   ACOPY(NMAX, NMAX)
      INTEGER            IPIV(NMAX), IWORK(3*NMAX)

*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH

*     .. External Subroutines ..
      EXTERNAL           DLAHILB, DGESVXX, DPOSVXX, DSYSVXX,
     $                   DGBSVXX, DLACPY, LSAMEN
      LOGICAL            LSAMEN

*     .. Intrinsic Functions ..
      INTRINSIC          SQRT, MAX, ABS, DBLE

*     .. Parameters ..
      INTEGER            NWISE_I, CWISE_I
      PARAMETER          (NWISE_I = 1, CWISE_I = 1)
      INTEGER            BND_I, COND_I
      PARAMETER          (BND_I = 2, COND_I = 3)

*  Create the loop to test out the Hilbert matrices

      FACT = 'E'
      UPLO = 'U'
      TRANS = 'N'
      EQUED = 'N'
      EPS = DLAMCH('Epsilon')
      NFAIL = 0
      N_AUX_TESTS = 0
      LDA = NMAX
      LDAB = (NMAX-1)+(NMAX-1)+1
      LDAFB = 2*(NMAX-1)+(NMAX-1)+1
      C2 = PATH( 2: 3 )

*     Main loop to test the different Hilbert Matrices.

      printed_guide = .false.

      DO N = 1 , NMAX
         PARAMS(1) = -1
         PARAMS(2) = -1

         KL = N-1
         KU = N-1
         NRHS = n
         M = MAX(SQRT(DBLE(N)), 10.0D+0)

*        Generate the Hilbert matrix, its inverse, and the
*        right hand side, all scaled by the LCM(1,..,2N-1).
         CALL DLAHILB(N, N, A, LDA, INVHILB, LDA, B, LDA, WORK, INFO)

*        Copy A into ACOPY.
         CALL DLACPY('ALL', N, N, A, NMAX, ACOPY, NMAX)

*        Store A in band format for GB tests
         DO J = 1, N
            DO I = 1, KL+KU+1
               AB( I, J ) = 0.0D+0
            END DO
         END DO
         DO J = 1, N
            DO I = MAX( 1, J-KU ), MIN( N, J+KL )
               AB( KU+1+I-J, J ) = A( I, J )
            END DO
         END DO

*        Copy AB into ABCOPY.
         DO J = 1, N
            DO I = 1, KL+KU+1
               ABCOPY( I, J ) = 0.0D+0
            END DO
         END DO
         CALL DLACPY('ALL', KL+KU+1, N, AB, LDAB, ABCOPY, LDAB)

*        Call D**SVXX with default PARAMS and N_ERR_BND = 3.
         IF ( LSAMEN( 2, C2, 'SY' ) ) THEN
            CALL DSYSVXX(FACT, UPLO, N, NRHS, ACOPY, LDA, AF, LDA,
     $           IPIV, EQUED, S, B, LDA, X, LDA, ORCOND,
     $           RPVGRW, BERR, NERRBND, ERRBND_N, ERRBND_C, NPARAMS,
     $           PARAMS, WORK, IWORK, INFO)
         ELSE IF ( LSAMEN( 2, C2, 'PO' ) ) THEN
            CALL DPOSVXX(FACT, UPLO, N, NRHS, ACOPY, LDA, AF, LDA,
     $           EQUED, S, B, LDA, X, LDA, ORCOND,
     $           RPVGRW, BERR, NERRBND, ERRBND_N, ERRBND_C, NPARAMS,
     $           PARAMS, WORK, IWORK, INFO)
         ELSE IF ( LSAMEN( 2, C2, 'GB' ) ) THEN
            CALL DGBSVXX(FACT, TRANS, N, KL, KU, NRHS, ABCOPY,
     $           LDAB, AFB, LDAFB, IPIV, EQUED, R, C, B,
     $           LDA, X, LDA, ORCOND, RPVGRW, BERR, NERRBND,
     $           ERRBND_N, ERRBND_C, NPARAMS, PARAMS, WORK, IWORK,
     $           INFO)
         ELSE
            CALL DGESVXX(FACT, TRANS, N, NRHS, ACOPY, LDA, AF, LDA,
     $           IPIV, EQUED, R, C, B, LDA, X, LDA, ORCOND,
     $           RPVGRW, BERR, NERRBND, ERRBND_N, ERRBND_C, NPARAMS,
     $           PARAMS, WORK, IWORK, INFO)
         END IF

         N_AUX_TESTS = N_AUX_TESTS + 1
         IF (ORCOND .LT. EPS) THEN
!        Either factorization failed or the matrix is flagged, and 1 <=
!        INFO <= N+1. We don't decide based on rcond anymore.
!            IF (INFO .EQ. 0 .OR. INFO .GT. N+1) THEN
!               NFAIL = NFAIL + 1
!               WRITE (*, FMT=8000) N, INFO, ORCOND, RCOND
!            END IF
         ELSE
!        Either everything succeeded (INFO == 0) or some solution failed
!        to converge (INFO > N+1).
            IF (INFO .GT. 0 .AND. INFO .LE. N+1) THEN
               NFAIL = NFAIL + 1
               WRITE (*, FMT=8000) C2, N, INFO, ORCOND, RCOND
            END IF
         END IF

*        Calculating the difference between D**SVXX's X and the true X.
         DO I = 1,N
            DO J =1,NRHS
               DIFF(I,J) = X(I,J) - INVHILB(I,J)
            END DO
         END DO

*        Calculating the RCOND
         RNORM = 0.0D+0
         RINORM = 0.0D+0
         IF ( LSAMEN( 2, C2, 'PO' ) .OR. LSAMEN( 2, C2, 'SY' ) ) THEN
            DO I = 1, N
               SUMR = 0.0D+0
               SUMRI = 0.0D+0
               DO J = 1, N
                  SUMR = SUMR + S(I) * ABS(A(I,J)) * S(J)
                  SUMRI = SUMRI + ABS(INVHILB(I, J)) / (S(J) * S(I))

               END DO
               RNORM = MAX(RNORM,SUMR)
               RINORM = MAX(RINORM,SUMRI)
            END DO
         ELSE IF ( LSAMEN( 2, C2, 'GE' ) .OR. LSAMEN( 2, C2, 'GB' ) )
     $           THEN
            DO I = 1, N
               SUMR = 0.0D+0
               SUMRI = 0.0D+0
               DO J = 1, N
                  SUMR = SUMR + R(I) * ABS(A(I,J)) * C(J)
                  SUMRI = SUMRI + ABS(INVHILB(I, J)) / (R(J) * C(I))
               END DO
               RNORM = MAX(RNORM,SUMR)
               RINORM = MAX(RINORM,SUMRI)
            END DO
         END IF

         RNORM = RNORM / ABS(A(1, 1))
         RCOND = 1.0D+0/(RNORM * RINORM)

*        Calculating the R for normwise rcond.
         DO I = 1, N
            RINV(I) = 0.0D+0
         END DO
         DO J = 1, N
            DO I = 1, N
               RINV(I) = RINV(I) + ABS(A(I,J))
            END DO
         END DO

*        Calculating the Normwise rcond.
         RINORM = 0.0D+0
         DO I = 1, N
            SUMRI = 0.0D+0
            DO J = 1, N
               SUMRI = SUMRI + ABS(INVHILB(I,J) * RINV(J))
            END DO
            RINORM = MAX(RINORM, SUMRI)
         END DO

!        invhilb is the inverse *unscaled* Hilbert matrix, so scale its norm
!        by 1/A(1,1) to make the scaling match A (the scaled Hilbert matrix)
         NCOND = ABS(A(1,1)) / RINORM

         CONDTHRESH = M * EPS
         ERRTHRESH = M * EPS

         DO K = 1, NRHS
            NORMT = 0.0D+0
            NORMDIF = 0.0D+0
            CWISE_ERR = 0.0D+0
            DO I = 1, N
               NORMT = MAX(ABS(INVHILB(I, K)), NORMT)
               NORMDIF = MAX(ABS(X(I,K) - INVHILB(I,K)), NORMDIF)
               IF (INVHILB(I,K) .NE. 0.0D+0) THEN
                  CWISE_ERR = MAX(ABS(X(I,K) - INVHILB(I,K))
     $                            /ABS(INVHILB(I,K)), CWISE_ERR)
               ELSE IF (X(I, K) .NE. 0.0D+0) THEN
                  CWISE_ERR = DLAMCH('OVERFLOW')
               END IF
            END DO
            IF (NORMT .NE. 0.0D+0) THEN
               NWISE_ERR = NORMDIF / NORMT
            ELSE IF (NORMDIF .NE. 0.0D+0) THEN
               NWISE_ERR = DLAMCH('OVERFLOW')
            ELSE
               NWISE_ERR = 0.0D+0
            ENDIF

            DO I = 1, N
               RINV(I) = 0.0D+0
            END DO
            DO J = 1, N
               DO I = 1, N
                  RINV(I) = RINV(I) + ABS(A(I, J) * INVHILB(J, K))
               END DO
            END DO
            RINORM = 0.0D+0
            DO I = 1, N
               SUMRI = 0.0D+0
               DO J = 1, N
                  SUMRI = SUMRI
     $                 + ABS(INVHILB(I, J) * RINV(J) / INVHILB(I, K))
               END DO
               RINORM = MAX(RINORM, SUMRI)
            END DO
!        invhilb is the inverse *unscaled* Hilbert matrix, so scale its norm
!        by 1/A(1,1) to make the scaling match A (the scaled Hilbert matrix)
            CCOND = ABS(A(1,1))/RINORM

!        Forward error bound tests
            NWISE_BND = ERRBND_N(K + (BND_I-1)*NRHS)
            CWISE_BND = ERRBND_C(K + (BND_I-1)*NRHS)
            NWISE_RCOND = ERRBND_N(K + (COND_I-1)*NRHS)
            CWISE_RCOND = ERRBND_C(K + (COND_I-1)*NRHS)
!            write (*,*) 'nwise : ', n, k, ncond, nwise_rcond,
!     $           condthresh, ncond.ge.condthresh
!            write (*,*) 'nwise2: ', k, nwise_bnd, nwise_err, errthresh
            IF (NCOND .GE. CONDTHRESH) THEN
               NGUAR = 'YES'
               IF (NWISE_BND .GT. ERRTHRESH) THEN
                  TSTRAT(1) = 1/(2.0D+0*EPS)
               ELSE
                  IF (NWISE_BND .NE. 0.0D+0) THEN
                     TSTRAT(1) = NWISE_ERR / NWISE_BND
                  ELSE IF (NWISE_ERR .NE. 0.0D+0) THEN
                     TSTRAT(1) = 1/(16.0*EPS)
                  ELSE
                     TSTRAT(1) = 0.0D+0
                  END IF
                  IF (TSTRAT(1) .GT. 1.0D+0) THEN
                     TSTRAT(1) = 1/(4.0D+0*EPS)
                  END IF
               END IF
            ELSE
               NGUAR = 'NO'
               IF (NWISE_BND .LT. 1.0D+0) THEN
                  TSTRAT(1) = 1/(8.0D+0*EPS)
               ELSE
                  TSTRAT(1) = 1.0D+0
               END IF
            END IF
!            write (*,*) 'cwise : ', n, k, ccond, cwise_rcond,
!     $           condthresh, ccond.ge.condthresh
!            write (*,*) 'cwise2: ', k, cwise_bnd, cwise_err, errthresh
            IF (CCOND .GE. CONDTHRESH) THEN
               CGUAR = 'YES'
               IF (CWISE_BND .GT. ERRTHRESH) THEN
                  TSTRAT(2) = 1/(2.0D+0*EPS)
               ELSE
                  IF (CWISE_BND .NE. 0.0D+0) THEN
                     TSTRAT(2) = CWISE_ERR / CWISE_BND
                  ELSE IF (CWISE_ERR .NE. 0.0D+0) THEN
                     TSTRAT(2) = 1/(16.0D+0*EPS)
                  ELSE
                     TSTRAT(2) = 0.0D+0
                  END IF
                  IF (TSTRAT(2) .GT. 1.0D+0) TSTRAT(2) = 1/(4.0D+0*EPS)
               END IF
            ELSE
               CGUAR = 'NO'
               IF (CWISE_BND .LT. 1.0D+0) THEN
                  TSTRAT(2) = 1/(8.0D+0*EPS)
               ELSE
                  TSTRAT(2) = 1.0D+0
               END IF
            END IF

!     Backwards error test
            TSTRAT(3) = BERR(K)/EPS

!     Condition number tests
            TSTRAT(4) = RCOND / ORCOND
            IF (RCOND .GE. CONDTHRESH .AND. TSTRAT(4) .LT. 1.0D+0)
     $         TSTRAT(4) = 1.0D+0 / TSTRAT(4)

            TSTRAT(5) = NCOND / NWISE_RCOND
            IF (NCOND .GE. CONDTHRESH .AND. TSTRAT(5) .LT. 1.0D+0)
     $         TSTRAT(5) = 1.0D+0 / TSTRAT(5)

            TSTRAT(6) = CCOND / NWISE_RCOND
            IF (CCOND .GE. CONDTHRESH .AND. TSTRAT(6) .LT. 1.0D+0)
     $         TSTRAT(6) = 1.0D+0 / TSTRAT(6)

            DO I = 1, NTESTS
               IF (TSTRAT(I) .GT. THRESH) THEN
                  IF (.NOT.PRINTED_GUIDE) THEN
                     WRITE(*,*)
                     WRITE( *, 9996) 1
                     WRITE( *, 9995) 2
                     WRITE( *, 9994) 3
                     WRITE( *, 9993) 4
                     WRITE( *, 9992) 5
                     WRITE( *, 9991) 6
                     WRITE( *, 9990) 7
                     WRITE( *, 9989) 8
                     WRITE(*,*)
                     PRINTED_GUIDE = .TRUE.
                  END IF
                  WRITE( *, 9999) C2, N, K, NGUAR, CGUAR, I, TSTRAT(I)
                  NFAIL = NFAIL + 1
               END IF
            END DO
      END DO

c$$$         WRITE(*,*)
c$$$         WRITE(*,*) 'Normwise Error Bounds'
c$$$         WRITE(*,*) 'Guaranteed error bound: ',ERRBND(NRHS,nwise_i,bnd_i)
c$$$         WRITE(*,*) 'Reciprocal condition number: ',ERRBND(NRHS,nwise_i,cond_i)
c$$$         WRITE(*,*) 'Raw error estimate: ',ERRBND(NRHS,nwise_i,rawbnd_i)
c$$$         WRITE(*,*)
c$$$         WRITE(*,*) 'Componentwise Error Bounds'
c$$$         WRITE(*,*) 'Guaranteed error bound: ',ERRBND(NRHS,cwise_i,bnd_i)
c$$$         WRITE(*,*) 'Reciprocal condition number: ',ERRBND(NRHS,cwise_i,cond_i)
c$$$         WRITE(*,*) 'Raw error estimate: ',ERRBND(NRHS,cwise_i,rawbnd_i)
c$$$         print *, 'Info: ', info
c$$$         WRITE(*,*)
*         WRITE(*,*) 'TSTRAT: ',TSTRAT

      END DO

      WRITE(*,*)
      IF( NFAIL .GT. 0 ) THEN
         WRITE(*,9998) C2, NFAIL, NTESTS*N+N_AUX_TESTS
      ELSE
         WRITE(*,9997) C2
      END IF
 9999 FORMAT( ' D', A2, 'SVXX: N =', I2, ', RHS = ', I2,
     $     ', NWISE GUAR. = ', A, ', CWISE GUAR. = ', A,
     $     ' test(',I1,') =', G12.5 )
 9998 FORMAT( ' D', A2, 'SVXX: ', I6, ' out of ', I6,
     $     ' tests failed to pass the threshold' )
 9997 FORMAT( ' D', A2, 'SVXX passed the tests of error bounds' )
*     Test ratios.
 9996 FORMAT( 3X, I2, ': Normwise guaranteed forward error', / 5X,
     $     'Guaranteed case: if norm ( abs( Xc - Xt )',
     $     ' / norm ( Xt ) .LE. ERRBND( *, nwise_i, bnd_i ), then',
     $     / 5X,
     $     'ERRBND( *, nwise_i, bnd_i ) .LE. MAX(SQRT(N), 10) * EPS')
 9995 FORMAT( 3X, I2, ': Componentwise guaranteed forward error' )
 9994 FORMAT( 3X, I2, ': Backwards error' )
 9993 FORMAT( 3X, I2, ': Reciprocal condition number' )
 9992 FORMAT( 3X, I2, ': Reciprocal normwise condition number' )
 9991 FORMAT( 3X, I2, ': Raw normwise error estimate' )
 9990 FORMAT( 3X, I2, ': Reciprocal componentwise condition number' )
 9989 FORMAT( 3X, I2, ': Raw componentwise error estimate' )

 8000 FORMAT( ' D', A2, 'SVXX: N =', I2, ', INFO = ', I3,
     $     ', ORCOND = ', G12.5, ', real RCOND = ', G12.5 )

      END
