*> \brief \b ZDRVRF2
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZDRVRF2( NOUT, NN, NVAL, A, LDA, ARF, AP, ASAV  )
*
*       .. Scalar Arguments ..
*       INTEGER            LDA, NN, NOUT
*       ..
*       .. Array Arguments ..
*       INTEGER            NVAL( NN )
*       COMPLEX*16         A( LDA, * ), ARF( * ), AP(*), ASAV( LDA, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZDRVRF2 tests the LAPACK RFP conversion routines.
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
*> \param[out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA,NMAX)
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>                The leading dimension of the array A.  LDA >= max(1,NMAX).
*> \endverbatim
*>
*> \param[out] ARF
*> \verbatim
*>          ARF is COMPLEX*16 array, dimension ((NMAX*(NMAX+1))/2).
*> \endverbatim
*>
*> \param[out] AP
*> \verbatim
*>          AP is COMPLEX*16 array, dimension ((NMAX*(NMAX+1))/2).
*> \endverbatim
*>
*> \param[out] ASAV
*> \verbatim
*>          ASAV is COMPLEX*16 array, dimension (LDA,NMAX)
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
      SUBROUTINE ZDRVRF2( NOUT, NN, NVAL, A, LDA, ARF, AP, ASAV  )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            LDA, NN, NOUT
*     ..
*     .. Array Arguments ..
      INTEGER            NVAL( NN )
      COMPLEX*16         A( LDA, * ), ARF( * ), AP(*), ASAV( LDA, * )
*     ..
*
*  =====================================================================
*     ..
*     .. Local Scalars ..
      LOGICAL            LOWER, OK1, OK2
      CHARACTER          UPLO, CFORM
      INTEGER            I, IFORM, IIN, INFO, IUPLO, J, N,
     +                   NERRS, NRUN
*     ..
*     .. Local Arrays ..
      CHARACTER          UPLOS( 2 ), FORMS( 2 )
      INTEGER            ISEED( 4 ), ISEEDY( 4 )
*     ..
*     .. External Functions ..
      COMPLEX*16         ZLARND
      EXTERNAL           ZLARND
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZTFTTR, ZTFTTP, ZTRTTF, ZTRTTP, ZTPTTR, ZTPTTF
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
      NERRS = 0
      INFO = 0
      DO 10 I = 1, 4
         ISEED( I ) = ISEEDY( I )
   10 CONTINUE
*
      DO 120 IIN = 1, NN
*
         N = NVAL( IIN )
*
*        Do first for UPLO = 'U', then for UPLO = 'L'
*
         DO 110 IUPLO = 1, 2
*
            UPLO = UPLOS( IUPLO )
            LOWER = .TRUE.
            IF ( IUPLO.EQ.1 ) LOWER = .FALSE.
*
*           Do first for CFORM = 'N', then for CFORM = 'C'
*
            DO 100 IFORM = 1, 2
*
               CFORM = FORMS( IFORM )
*
               NRUN = NRUN + 1
*
               DO J = 1, N
                  DO I = 1, N
                     A( I, J) = ZLARND( 4, ISEED )
                  END DO
               END DO
*
               SRNAMT = 'ZTRTTF'
               CALL ZTRTTF( CFORM, UPLO, N, A, LDA, ARF, INFO )
*
               SRNAMT = 'ZTFTTP'
               CALL ZTFTTP( CFORM, UPLO, N, ARF, AP, INFO )
*
               SRNAMT = 'ZTPTTR'
               CALL ZTPTTR( UPLO, N, AP, ASAV, LDA, INFO )
*
               OK1 = .TRUE.
               IF ( LOWER ) THEN
                  DO J = 1, N
                     DO I = J, N
                        IF ( A(I,J).NE.ASAV(I,J) ) THEN
                           OK1 = .FALSE.
                        END IF
                     END DO
                  END DO
               ELSE
                  DO J = 1, N
                     DO I = 1, J
                        IF ( A(I,J).NE.ASAV(I,J) ) THEN
                           OK1 = .FALSE.
                        END IF
                     END DO
                  END DO
               END IF
*
               NRUN = NRUN + 1
*
               SRNAMT = 'ZTRTTP'
               CALL ZTRTTP( UPLO, N, A, LDA, AP, INFO )
*
               SRNAMT = 'ZTPTTF'
               CALL ZTPTTF( CFORM, UPLO, N, AP, ARF, INFO )
*
               SRNAMT = 'ZTFTTR'
               CALL ZTFTTR( CFORM, UPLO, N, ARF, ASAV, LDA, INFO )
*
               OK2 = .TRUE.
               IF ( LOWER ) THEN
                  DO J = 1, N
                     DO I = J, N
                        IF ( A(I,J).NE.ASAV(I,J) ) THEN
                           OK2 = .FALSE.
                        END IF
                     END DO
                  END DO
               ELSE
                  DO J = 1, N
                     DO I = 1, J
                        IF ( A(I,J).NE.ASAV(I,J) ) THEN
                           OK2 = .FALSE.
                        END IF
                     END DO
                  END DO
               END IF
*
               IF (( .NOT.OK1 ).OR.( .NOT.OK2 )) THEN
                  IF( NERRS.EQ.0 ) THEN
                     WRITE( NOUT, * )
                     WRITE( NOUT, FMT = 9999 )
                  END IF
                  WRITE( NOUT, FMT = 9998 ) N, UPLO, CFORM
                  NERRS = NERRS + 1
               END IF
*
  100       CONTINUE
  110    CONTINUE
  120 CONTINUE
*
*     Print a summary of the results.
*
      IF ( NERRS.EQ.0 ) THEN
         WRITE( NOUT, FMT = 9997 ) NRUN
      ELSE
         WRITE( NOUT, FMT = 9996 ) NERRS, NRUN
      END IF
*
 9999 FORMAT( 1X, ' *** Error(s) while testing the RFP conversion',
     +         ' routines ***')
 9998 FORMAT( 1X, '     Error in RFP,conversion routines N=',I5,
     +        ' UPLO=''', A1, ''', FORM =''',A1,'''')
 9997 FORMAT( 1X, 'All tests for the RFP conversion routines passed (',
     +        I5,' tests run)')
 9996 FORMAT( 1X, 'RFP conversion routines:',I5,' out of ',I5,
     +        ' error message recorded')
*
      RETURN
*
*     End of ZDRVRF2
*
      END
