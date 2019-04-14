*> \brief \b CUNBDB5
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CUNBDB5 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/cunbdb5.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/cunbdb5.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/cunbdb5.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CUNBDB5( M1, M2, N, X1, INCX1, X2, INCX2, Q1, LDQ1, Q2,
*                           LDQ2, WORK, LWORK, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            INCX1, INCX2, INFO, LDQ1, LDQ2, LWORK, M1, M2,
*      $                   N
*       ..
*       .. Array Arguments ..
*       COMPLEX            Q1(LDQ1,*), Q2(LDQ2,*), WORK(*), X1(*), X2(*)
*       ..
*
*
*> \par Purpose:
*  =============
*>
*>\verbatim
*>
*> CUNBDB5 orthogonalizes the column vector
*>      X = [ X1 ]
*>          [ X2 ]
*> with respect to the columns of
*>      Q = [ Q1 ] .
*>          [ Q2 ]
*> The columns of Q must be orthonormal.
*>
*> If the projection is zero according to Kahan's "twice is enough"
*> criterion, then some other vector from the orthogonal complement
*> is returned. This vector is chosen in an arbitrary but deterministic
*> way.
*>
*>\endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M1
*> \verbatim
*>          M1 is INTEGER
*>           The dimension of X1 and the number of rows in Q1. 0 <= M1.
*> \endverbatim
*>
*> \param[in] M2
*> \verbatim
*>          M2 is INTEGER
*>           The dimension of X2 and the number of rows in Q2. 0 <= M2.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>           The number of columns in Q1 and Q2. 0 <= N.
*> \endverbatim
*>
*> \param[in,out] X1
*> \verbatim
*>          X1 is COMPLEX array, dimension (M1)
*>           On entry, the top part of the vector to be orthogonalized.
*>           On exit, the top part of the projected vector.
*> \endverbatim
*>
*> \param[in] INCX1
*> \verbatim
*>          INCX1 is INTEGER
*>           Increment for entries of X1.
*> \endverbatim
*>
*> \param[in,out] X2
*> \verbatim
*>          X2 is COMPLEX array, dimension (M2)
*>           On entry, the bottom part of the vector to be
*>           orthogonalized. On exit, the bottom part of the projected
*>           vector.
*> \endverbatim
*>
*> \param[in] INCX2
*> \verbatim
*>          INCX2 is INTEGER
*>           Increment for entries of X2.
*> \endverbatim
*>
*> \param[in] Q1
*> \verbatim
*>          Q1 is COMPLEX array, dimension (LDQ1, N)
*>           The top part of the orthonormal basis matrix.
*> \endverbatim
*>
*> \param[in] LDQ1
*> \verbatim
*>          LDQ1 is INTEGER
*>           The leading dimension of Q1. LDQ1 >= M1.
*> \endverbatim
*>
*> \param[in] Q2
*> \verbatim
*>          Q2 is COMPLEX array, dimension (LDQ2, N)
*>           The bottom part of the orthonormal basis matrix.
*> \endverbatim
*>
*> \param[in] LDQ2
*> \verbatim
*>          LDQ2 is INTEGER
*>           The leading dimension of Q2. LDQ2 >= M2.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX array, dimension (LWORK)
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>           The dimension of the array WORK. LWORK >= N.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>           = 0:  successful exit.
*>           < 0:  if INFO = -i, the i-th argument had an illegal value.
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
*> \date July 2012
*
*> \ingroup complexOTHERcomputational
*
*  =====================================================================
      SUBROUTINE CUNBDB5( M1, M2, N, X1, INCX1, X2, INCX2, Q1, LDQ1, Q2,
     $                    LDQ2, WORK, LWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     July 2012
*
*     .. Scalar Arguments ..
      INTEGER            INCX1, INCX2, INFO, LDQ1, LDQ2, LWORK, M1, M2,
     $                   N
*     ..
*     .. Array Arguments ..
      COMPLEX            Q1(LDQ1,*), Q2(LDQ2,*), WORK(*), X1(*), X2(*)
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX            ONE, ZERO
      PARAMETER          ( ONE = (1.0E0,0.0E0), ZERO = (0.0E0,0.0E0) )
*     ..
*     .. Local Scalars ..
      INTEGER            CHILDINFO, I, J
*     ..
*     .. External Subroutines ..
      EXTERNAL           CUNBDB6, XERBLA
*     ..
*     .. External Functions ..
      REAL               SCNRM2
      EXTERNAL           SCNRM2
*     ..
*     .. Intrinsic Function ..
      INTRINSIC          MAX
*     ..
*     .. Executable Statements ..
*
*     Test input arguments
*
      INFO = 0
      IF( M1 .LT. 0 ) THEN
         INFO = -1
      ELSE IF( M2 .LT. 0 ) THEN
         INFO = -2
      ELSE IF( N .LT. 0 ) THEN
         INFO = -3
      ELSE IF( INCX1 .LT. 1 ) THEN
         INFO = -5
      ELSE IF( INCX2 .LT. 1 ) THEN
         INFO = -7
      ELSE IF( LDQ1 .LT. MAX( 1, M1 ) ) THEN
         INFO = -9
      ELSE IF( LDQ2 .LT. MAX( 1, M2 ) ) THEN
         INFO = -11
      ELSE IF( LWORK .LT. N ) THEN
         INFO = -13
      END IF
*
      IF( INFO .NE. 0 ) THEN
         CALL XERBLA( 'CUNBDB5', -INFO )
         RETURN
      END IF
*
*     Project X onto the orthogonal complement of Q
*
      CALL CUNBDB6( M1, M2, N, X1, INCX1, X2, INCX2, Q1, LDQ1, Q2, LDQ2,
     $              WORK, LWORK, CHILDINFO )
*
*     If the projection is nonzero, then return
*
      IF( SCNRM2(M1,X1,INCX1) .NE. ZERO
     $    .OR. SCNRM2(M2,X2,INCX2) .NE. ZERO ) THEN
         RETURN
      END IF
*
*     Project each standard basis vector e_1,...,e_M1 in turn, stopping
*     when a nonzero projection is found
*
      DO I = 1, M1
         DO J = 1, M1
            X1(J) = ZERO
         END DO
         X1(I) = ONE
         DO J = 1, M2
            X2(J) = ZERO
         END DO
         CALL CUNBDB6( M1, M2, N, X1, INCX1, X2, INCX2, Q1, LDQ1, Q2,
     $                 LDQ2, WORK, LWORK, CHILDINFO )
         IF( SCNRM2(M1,X1,INCX1) .NE. ZERO
     $       .OR. SCNRM2(M2,X2,INCX2) .NE. ZERO ) THEN
            RETURN
         END IF
      END DO
*
*     Project each standard basis vector e_(M1+1),...,e_(M1+M2) in turn,
*     stopping when a nonzero projection is found
*
      DO I = 1, M2
         DO J = 1, M1
            X1(J) = ZERO
         END DO
         DO J = 1, M2
            X2(J) = ZERO
         END DO
         X2(I) = ONE
         CALL CUNBDB6( M1, M2, N, X1, INCX1, X2, INCX2, Q1, LDQ1, Q2,
     $                 LDQ2, WORK, LWORK, CHILDINFO )
         IF( SCNRM2(M1,X1,INCX1) .NE. ZERO
     $       .OR. SCNRM2(M2,X2,INCX2) .NE. ZERO ) THEN
            RETURN
         END IF
      END DO
*
      RETURN
*
*     End of CUNBDB5
*
      END

