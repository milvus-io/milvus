*> \brief \b DLAT2S converts a double-precision triangular matrix to a single-precision triangular matrix.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DLAT2S + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dlat2s.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dlat2s.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dlat2s.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DLAT2S( UPLO, N, A, LDA, SA, LDSA, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            INFO, LDA, LDSA, N
*       ..
*       .. Array Arguments ..
*       REAL               SA( LDSA, * )
*       DOUBLE PRECISION   A( LDA, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DLAT2S converts a DOUBLE PRECISION triangular matrix, SA, to a SINGLE
*> PRECISION triangular matrix, A.
*>
*> RMAX is the overflow for the SINGLE PRECISION arithmetic
*> DLAS2S checks that all the entries of A are between -RMAX and
*> RMAX. If not the conversion is aborted and a flag is raised.
*>
*> This is an auxiliary routine so there is no argument checking.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          = 'U':  A is upper triangular;
*>          = 'L':  A is lower triangular.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of rows and columns of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension (LDA,N)
*>          On entry, the N-by-N triangular coefficient matrix A.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[out] SA
*> \verbatim
*>          SA is REAL array, dimension (LDSA,N)
*>          Only the UPLO part of SA is referenced.  On exit, if INFO=0,
*>          the N-by-N coefficient matrix SA; if INFO>0, the content of
*>          the UPLO part of SA is unspecified.
*> \endverbatim
*>
*> \param[in] LDSA
*> \verbatim
*>          LDSA is INTEGER
*>          The leading dimension of the array SA.  LDSA >= max(1,M).
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit.
*>          = 1:  an entry of the matrix A is greater than the SINGLE
*>                PRECISION overflow threshold, in this case, the content
*>                of the UPLO part of SA in exit is unspecified.
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
*> \ingroup doubleOTHERauxiliary
*
*  =====================================================================
      SUBROUTINE DLAT2S( UPLO, N, A, LDA, SA, LDSA, INFO )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            INFO, LDA, LDSA, N
*     ..
*     .. Array Arguments ..
      REAL               SA( LDSA, * )
      DOUBLE PRECISION   A( LDA, * )
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      INTEGER            I, J
      DOUBLE PRECISION   RMAX
      LOGICAL            UPPER
*     ..
*     .. External Functions ..
      REAL               SLAMCH
      LOGICAL            LSAME
      EXTERNAL           SLAMCH, LSAME
*     ..
*     .. Executable Statements ..
*
      RMAX = SLAMCH( 'O' )
      UPPER = LSAME( UPLO, 'U' )
      IF( UPPER ) THEN
         DO 20 J = 1, N
            DO 10 I = 1, J
               IF( ( A( I, J ).LT.-RMAX ) .OR. ( A( I, J ).GT.RMAX ) )
     $             THEN
                  INFO = 1
                  GO TO 50
               END IF
               SA( I, J ) = A( I, J )
   10       CONTINUE
   20    CONTINUE
      ELSE
         DO 40 J = 1, N
            DO 30 I = J, N
               IF( ( A( I, J ).LT.-RMAX ) .OR. ( A( I, J ).GT.RMAX ) )
     $             THEN
                  INFO = 1
                  GO TO 50
               END IF
               SA( I, J ) = A( I, J )
   30       CONTINUE
   40    CONTINUE
      END IF
   50 CONTINUE
*
      RETURN
*
*     End of DLAT2S
*
      END
