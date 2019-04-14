*> \brief \b ZSYCONV
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZSYCONV + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zsyconv.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zsyconv.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zsyconv.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZSYCONV( UPLO, WAY, N, A, LDA, IPIV, E, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO, WAY
*       INTEGER            INFO, LDA, N
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * )
*       COMPLEX*16         A( LDA, * ), E( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZSYCONV converts A given by ZHETRF into L and D or vice-versa.
*> Get nondiagonal elements of D (returned in workspace) and
*> apply or reverse permutation done in TRF.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          Specifies whether the details of the factorization are stored
*>          as an upper or lower triangular matrix.
*>          = 'U':  Upper triangular, form is A = U*D*U**T;
*>          = 'L':  Lower triangular, form is A = L*D*L**T.
*> \endverbatim
*>
*> \param[in] WAY
*> \verbatim
*>          WAY is CHARACTER*1
*>          = 'C': Convert
*>          = 'R': Revert
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>          The block diagonal matrix D and the multipliers used to
*>          obtain the factor U or L as computed by ZSYTRF.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[in] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (N)
*>          Details of the interchanges and the block structure of D
*>          as determined by ZSYTRF.
*> \endverbatim
*>
*> \param[out] E
*> \verbatim
*>          E is COMPLEX*16 array, dimension (N)
*>          E stores the supdiagonal/subdiagonal of the symmetric 1-by-1
*>          or 2-by-2 block diagonal matrix D in LDLT.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
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
*> \ingroup complex16SYcomputational
*
*  =====================================================================
      SUBROUTINE ZSYCONV( UPLO, WAY, N, A, LDA, IPIV, E, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO, WAY
      INTEGER            INFO, LDA, N
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * )
      COMPLEX*16         A( LDA, * ), E( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX*16         ZERO
      PARAMETER          ( ZERO = (0.0D+0,0.0D+0) )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*
*     .. External Subroutines ..
      EXTERNAL           XERBLA
*     .. Local Scalars ..
      LOGICAL            UPPER, CONVERT
      INTEGER            I, IP, J
      COMPLEX*16         TEMP
*     ..
*     .. Executable Statements ..
*
      INFO = 0
      UPPER = LSAME( UPLO, 'U' )
      CONVERT = LSAME( WAY, 'C' )
      IF( .NOT.UPPER .AND. .NOT.LSAME( UPLO, 'L' ) ) THEN
         INFO = -1
      ELSE IF( .NOT.CONVERT .AND. .NOT.LSAME( WAY, 'R' ) ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -5

      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZSYCONV', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 )
     $   RETURN
*
      IF( UPPER ) THEN
*
*        A is UPPER
*
         IF ( CONVERT ) THEN
*
*           Convert A (A is upper)
*
*           Convert VALUE
*
            I=N
            E(1)=ZERO
            DO WHILE ( I .GT. 1 )
               IF( IPIV(I) .LT. 0 ) THEN
                  E(I)=A(I-1,I)
                  E(I-1)=ZERO
                  A(I-1,I)=ZERO
                  I=I-1
               ELSE
                  E(I)=ZERO
               ENDIF
               I=I-1
            END DO
*
*           Convert PERMUTATIONS
*
            I=N
            DO WHILE ( I .GE. 1 )
               IF( IPIV(I) .GT. 0) THEN
                  IP=IPIV(I)
                  IF( I .LT. N) THEN
                     DO 12 J= I+1,N
                       TEMP=A(IP,J)
                       A(IP,J)=A(I,J)
                       A(I,J)=TEMP
 12                  CONTINUE
                  ENDIF
               ELSE
                  IP=-IPIV(I)
                  IF( I .LT. N) THEN
                     DO 13 J= I+1,N
                        TEMP=A(IP,J)
                        A(IP,J)=A(I-1,J)
                        A(I-1,J)=TEMP
 13                  CONTINUE
                  ENDIF
                  I=I-1
               ENDIF
               I=I-1
            END DO
*
         ELSE
*
*           Revert A (A is upper)
*
*           Revert PERMUTATIONS
*
            I=1
            DO WHILE ( I .LE. N )
               IF( IPIV(I) .GT. 0 ) THEN
                  IP=IPIV(I)
                  IF( I .LT. N) THEN
                  DO J= I+1,N
                    TEMP=A(IP,J)
                    A(IP,J)=A(I,J)
                    A(I,J)=TEMP
                  END DO
                  ENDIF
               ELSE
                 IP=-IPIV(I)
                 I=I+1
                 IF( I .LT. N) THEN
                    DO J= I+1,N
                       TEMP=A(IP,J)
                       A(IP,J)=A(I-1,J)
                       A(I-1,J)=TEMP
                    END DO
                 ENDIF
               ENDIF
               I=I+1
            END DO
*
*           Revert VALUE
*
            I=N
            DO WHILE ( I .GT. 1 )
               IF( IPIV(I) .LT. 0 ) THEN
                  A(I-1,I)=E(I)
                  I=I-1
               ENDIF
               I=I-1
            END DO
         END IF
*
      ELSE
*
*        A is LOWER
*
         IF ( CONVERT ) THEN
*
*           Convert A (A is lower)
*
*           Convert VALUE
*
            I=1
            E(N)=ZERO
            DO WHILE ( I .LE. N )
               IF( I.LT.N .AND. IPIV(I) .LT. 0 ) THEN
                  E(I)=A(I+1,I)
                  E(I+1)=ZERO
                  A(I+1,I)=ZERO
                  I=I+1
               ELSE
                  E(I)=ZERO
               ENDIF
               I=I+1
            END DO
*
*           Convert PERMUTATIONS
*
            I=1
            DO WHILE ( I .LE. N )
               IF( IPIV(I) .GT. 0 ) THEN
                  IP=IPIV(I)
                  IF (I .GT. 1) THEN
                     DO 22 J= 1,I-1
                        TEMP=A(IP,J)
                        A(IP,J)=A(I,J)
                        A(I,J)=TEMP
 22                  CONTINUE
                  ENDIF
               ELSE
                  IP=-IPIV(I)
                  IF (I .GT. 1) THEN
                     DO 23 J= 1,I-1
                        TEMP=A(IP,J)
                        A(IP,J)=A(I+1,J)
                        A(I+1,J)=TEMP
 23                  CONTINUE
                  ENDIF
                  I=I+1
               ENDIF
               I=I+1
            END DO
*
         ELSE
*
*           Revert A (A is lower)
*
*           Revert PERMUTATIONS
*
            I=N
            DO WHILE ( I .GE. 1 )
               IF( IPIV(I) .GT. 0 ) THEN
                  IP=IPIV(I)
                  IF (I .GT. 1) THEN
                     DO J= 1,I-1
                        TEMP=A(I,J)
                        A(I,J)=A(IP,J)
                        A(IP,J)=TEMP
                     END DO
                  ENDIF
               ELSE
                  IP=-IPIV(I)
                  I=I-1
                  IF (I .GT. 1) THEN
                     DO J= 1,I-1
                        TEMP=A(I+1,J)
                        A(I+1,J)=A(IP,J)
                        A(IP,J)=TEMP
                     END DO
                  ENDIF
               ENDIF
               I=I-1
            END DO
*
*           Revert VALUE
*
            I=1
            DO WHILE ( I .LE. N-1 )
               IF( IPIV(I) .LT. 0 ) THEN
                  A(I+1,I)=E(I)
                  I=I+1
               ENDIF
               I=I+1
            END DO
         END IF
      END IF
*
      RETURN
*
*     End of ZSYCONV
*
      END
