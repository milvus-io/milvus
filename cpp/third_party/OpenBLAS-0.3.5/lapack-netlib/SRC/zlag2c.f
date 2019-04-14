*> \brief \b ZLAG2C converts a complex double precision matrix to a complex single precision matrix.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZLAG2C + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zlag2c.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zlag2c.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zlag2c.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZLAG2C( M, N, A, LDA, SA, LDSA, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INFO, LDA, LDSA, M, N
*       ..
*       .. Array Arguments ..
*       COMPLEX            SA( LDSA, * )
*       COMPLEX*16         A( LDA, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZLAG2C converts a COMPLEX*16 matrix, SA, to a COMPLEX matrix, A.
*>
*> RMAX is the overflow for the SINGLE PRECISION arithmetic
*> ZLAG2C checks that all the entries of A are between -RMAX and
*> RMAX. If not the conversion is aborted and a flag is raised.
*>
*> This is an auxiliary routine so there is no argument checking.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of lines of the matrix A.  M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>          On entry, the M-by-N coefficient matrix A.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,M).
*> \endverbatim
*>
*> \param[out] SA
*> \verbatim
*>          SA is COMPLEX array, dimension (LDSA,N)
*>          On exit, if INFO=0, the M-by-N coefficient matrix SA; if
*>          INFO>0, the content of SA is unspecified.
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
*>                of SA in exit is unspecified.
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
*> \ingroup complex16OTHERauxiliary
*
*  =====================================================================
      SUBROUTINE ZLAG2C( M, N, A, LDA, SA, LDSA, INFO )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            INFO, LDA, LDSA, M, N
*     ..
*     .. Array Arguments ..
      COMPLEX            SA( LDSA, * )
      COMPLEX*16         A( LDA, * )
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      INTEGER            I, J
      DOUBLE PRECISION   RMAX
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE, DIMAG
*     ..
*     .. External Functions ..
      REAL               SLAMCH
      EXTERNAL           SLAMCH
*     ..
*     .. Executable Statements ..
*
      RMAX = SLAMCH( 'O' )
      DO 20 J = 1, N
         DO 10 I = 1, M
            IF( ( DBLE( A( I, J ) ).LT.-RMAX ) .OR.
     $          ( DBLE( A( I, J ) ).GT.RMAX ) .OR.
     $          ( DIMAG( A( I, J ) ).LT.-RMAX ) .OR.
     $          ( DIMAG( A( I, J ) ).GT.RMAX ) ) THEN
               INFO = 1
               GO TO 30
            END IF
            SA( I, J ) = A( I, J )
   10    CONTINUE
   20 CONTINUE
      INFO = 0
   30 CONTINUE
      RETURN
*
*     End of ZLAG2C
*
      END
