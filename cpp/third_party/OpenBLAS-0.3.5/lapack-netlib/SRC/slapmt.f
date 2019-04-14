*> \brief \b SLAPMT performs a forward or backward permutation of the columns of a matrix.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLAPMT + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slapmt.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slapmt.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slapmt.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLAPMT( FORWRD, M, N, X, LDX, K )
*
*       .. Scalar Arguments ..
*       LOGICAL            FORWRD
*       INTEGER            LDX, M, N
*       ..
*       .. Array Arguments ..
*       INTEGER            K( * )
*       REAL               X( LDX, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLAPMT rearranges the columns of the M by N matrix X as specified
*> by the permutation K(1),K(2),...,K(N) of the integers 1,...,N.
*> If FORWRD = .TRUE.,  forward permutation:
*>
*>      X(*,K(J)) is moved X(*,J) for J = 1,2,...,N.
*>
*> If FORWRD = .FALSE., backward permutation:
*>
*>      X(*,J) is moved to X(*,K(J)) for J = 1,2,...,N.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] FORWRD
*> \verbatim
*>          FORWRD is LOGICAL
*>          = .TRUE., forward permutation
*>          = .FALSE., backward permutation
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix X. M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix X. N >= 0.
*> \endverbatim
*>
*> \param[in,out] X
*> \verbatim
*>          X is REAL array, dimension (LDX,N)
*>          On entry, the M by N matrix X.
*>          On exit, X contains the permuted matrix X.
*> \endverbatim
*>
*> \param[in] LDX
*> \verbatim
*>          LDX is INTEGER
*>          The leading dimension of the array X, LDX >= MAX(1,M).
*> \endverbatim
*>
*> \param[in,out] K
*> \verbatim
*>          K is INTEGER array, dimension (N)
*>          On entry, K contains the permutation vector. K is used as
*>          internal workspace, but reset to its original value on
*>          output.
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
*> \ingroup realOTHERauxiliary
*
*  =====================================================================
      SUBROUTINE SLAPMT( FORWRD, M, N, X, LDX, K )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      LOGICAL            FORWRD
      INTEGER            LDX, M, N
*     ..
*     .. Array Arguments ..
      INTEGER            K( * )
      REAL               X( LDX, * )
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      INTEGER            I, II, J, IN
      REAL               TEMP
*     ..
*     .. Executable Statements ..
*
      IF( N.LE.1 )
     $   RETURN
*
      DO 10 I = 1, N
         K( I ) = -K( I )
   10 CONTINUE
*
      IF( FORWRD ) THEN
*
*        Forward permutation
*
         DO 60 I = 1, N
*
            IF( K( I ).GT.0 )
     $         GO TO 40
*
            J = I
            K( J ) = -K( J )
            IN = K( J )
*
   20       CONTINUE
            IF( K( IN ).GT.0 )
     $         GO TO 40
*
            DO 30 II = 1, M
               TEMP = X( II, J )
               X( II, J ) = X( II, IN )
               X( II, IN ) = TEMP
   30       CONTINUE
*
            K( IN ) = -K( IN )
            J = IN
            IN = K( IN )
            GO TO 20
*
   40       CONTINUE
*
   60    CONTINUE
*
      ELSE
*
*        Backward permutation
*
         DO 110 I = 1, N
*
            IF( K( I ).GT.0 )
     $         GO TO 100
*
            K( I ) = -K( I )
            J = K( I )
   80       CONTINUE
            IF( J.EQ.I )
     $         GO TO 100
*
            DO 90 II = 1, M
               TEMP = X( II, I )
               X( II, I ) = X( II, J )
               X( II, J ) = TEMP
   90       CONTINUE
*
            K( J ) = -K( J )
            J = K( J )
            GO TO 80
*
  100       CONTINUE

  110    CONTINUE
*
      END IF
*
      RETURN
*
*     End of SLAPMT
*
      END
