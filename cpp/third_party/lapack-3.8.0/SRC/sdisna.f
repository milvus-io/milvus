*> \brief \b SDISNA
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SDISNA + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/sdisna.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/sdisna.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/sdisna.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SDISNA( JOB, M, N, D, SEP, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          JOB
*       INTEGER            INFO, M, N
*       ..
*       .. Array Arguments ..
*       REAL               D( * ), SEP( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SDISNA computes the reciprocal condition numbers for the eigenvectors
*> of a real symmetric or complex Hermitian matrix or for the left or
*> right singular vectors of a general m-by-n matrix. The reciprocal
*> condition number is the 'gap' between the corresponding eigenvalue or
*> singular value and the nearest other one.
*>
*> The bound on the error, measured by angle in radians, in the I-th
*> computed vector is given by
*>
*>        SLAMCH( 'E' ) * ( ANORM / SEP( I ) )
*>
*> where ANORM = 2-norm(A) = max( abs( D(j) ) ).  SEP(I) is not allowed
*> to be smaller than SLAMCH( 'E' )*ANORM in order to limit the size of
*> the error bound.
*>
*> SDISNA may also be used to compute error bounds for eigenvectors of
*> the generalized symmetric definite eigenproblem.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] JOB
*> \verbatim
*>          JOB is CHARACTER*1
*>          Specifies for which problem the reciprocal condition numbers
*>          should be computed:
*>          = 'E':  the eigenvectors of a symmetric/Hermitian matrix;
*>          = 'L':  the left singular vectors of a general matrix;
*>          = 'R':  the right singular vectors of a general matrix.
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix. M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          If JOB = 'L' or 'R', the number of columns of the matrix,
*>          in which case N >= 0. Ignored if JOB = 'E'.
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is REAL array, dimension (M) if JOB = 'E'
*>                              dimension (min(M,N)) if JOB = 'L' or 'R'
*>          The eigenvalues (if JOB = 'E') or singular values (if JOB =
*>          'L' or 'R') of the matrix, in either increasing or decreasing
*>          order. If singular values, they must be non-negative.
*> \endverbatim
*>
*> \param[out] SEP
*> \verbatim
*>          SEP is REAL array, dimension (M) if JOB = 'E'
*>                               dimension (min(M,N)) if JOB = 'L' or 'R'
*>          The reciprocal condition numbers of the vectors.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit.
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
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
*> \ingroup auxOTHERcomputational
*
*  =====================================================================
      SUBROUTINE SDISNA( JOB, M, N, D, SEP, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          JOB
      INTEGER            INFO, M, N
*     ..
*     .. Array Arguments ..
      REAL               D( * ), SEP( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO
      PARAMETER          ( ZERO = 0.0E+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            DECR, EIGEN, INCR, LEFT, RIGHT, SING
      INTEGER            I, K
      REAL               ANORM, EPS, NEWGAP, OLDGAP, SAFMIN, THRESH
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      REAL               SLAMCH
      EXTERNAL           LSAME, SLAMCH
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, MIN
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA
*     ..
*     .. Executable Statements ..
*
*     Test the input arguments
*
      INFO = 0
      EIGEN = LSAME( JOB, 'E' )
      LEFT = LSAME( JOB, 'L' )
      RIGHT = LSAME( JOB, 'R' )
      SING = LEFT .OR. RIGHT
      IF( EIGEN ) THEN
         K = M
      ELSE IF( SING ) THEN
         K = MIN( M, N )
      END IF
      IF( .NOT.EIGEN .AND. .NOT.SING ) THEN
         INFO = -1
      ELSE IF( M.LT.0 ) THEN
         INFO = -2
      ELSE IF( K.LT.0 ) THEN
         INFO = -3
      ELSE
         INCR = .TRUE.
         DECR = .TRUE.
         DO 10 I = 1, K - 1
            IF( INCR )
     $         INCR = INCR .AND. D( I ).LE.D( I+1 )
            IF( DECR )
     $         DECR = DECR .AND. D( I ).GE.D( I+1 )
   10    CONTINUE
         IF( SING .AND. K.GT.0 ) THEN
            IF( INCR )
     $         INCR = INCR .AND. ZERO.LE.D( 1 )
            IF( DECR )
     $         DECR = DECR .AND. D( K ).GE.ZERO
         END IF
         IF( .NOT.( INCR .OR. DECR ) )
     $      INFO = -4
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SDISNA', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( K.EQ.0 )
     $   RETURN
*
*     Compute reciprocal condition numbers
*
      IF( K.EQ.1 ) THEN
         SEP( 1 ) = SLAMCH( 'O' )
      ELSE
         OLDGAP = ABS( D( 2 )-D( 1 ) )
         SEP( 1 ) = OLDGAP
         DO 20 I = 2, K - 1
            NEWGAP = ABS( D( I+1 )-D( I ) )
            SEP( I ) = MIN( OLDGAP, NEWGAP )
            OLDGAP = NEWGAP
   20    CONTINUE
         SEP( K ) = OLDGAP
      END IF
      IF( SING ) THEN
         IF( ( LEFT .AND. M.GT.N ) .OR. ( RIGHT .AND. M.LT.N ) ) THEN
            IF( INCR )
     $         SEP( 1 ) = MIN( SEP( 1 ), D( 1 ) )
            IF( DECR )
     $         SEP( K ) = MIN( SEP( K ), D( K ) )
         END IF
      END IF
*
*     Ensure that reciprocal condition numbers are not less than
*     threshold, in order to limit the size of the error bound
*
      EPS = SLAMCH( 'E' )
      SAFMIN = SLAMCH( 'S' )
      ANORM = MAX( ABS( D( 1 ) ), ABS( D( K ) ) )
      IF( ANORM.EQ.ZERO ) THEN
         THRESH = EPS
      ELSE
         THRESH = MAX( EPS*ANORM, SAFMIN )
      END IF
      DO 30 I = 1, K
         SEP( I ) = MAX( SEP( I ), THRESH )
   30 CONTINUE
*
      RETURN
*
*     End of SDISNA
*
      END
