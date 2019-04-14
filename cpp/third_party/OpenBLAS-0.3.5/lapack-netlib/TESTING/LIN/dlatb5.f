*> \brief \b DLATB5
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DLATB5( PATH, IMAT, N, TYPE, KL, KU, ANORM, MODE,
*                          CNDNUM, DIST )
*
*       .. Scalar Arguments ..
*       DOUBLE PRECISION   ANORM, CNDNUM
*       INTEGER            IMAT, KL, KU, MODE, N
*       CHARACTER          DIST, TYPE
*       CHARACTER*3        PATH
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DLATB5 sets parameters for the matrix generator based on the type
*> of matrix to be generated.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] PATH
*> \verbatim
*>          PATH is CHARACTER*3
*>          The LAPACK path name.
*> \endverbatim
*>
*> \param[in] IMAT
*> \verbatim
*>          IMAT is INTEGER
*>          An integer key describing which matrix to generate for this
*>          path.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of rows and columns in the matrix to be generated.
*> \endverbatim
*>
*> \param[out] TYPE
*> \verbatim
*>          TYPE is CHARACTER*1
*>          The type of the matrix to be generated:
*>          = 'S':  symmetric matrix
*>          = 'P':  symmetric positive (semi)definite matrix
*>          = 'N':  nonsymmetric matrix
*> \endverbatim
*>
*> \param[out] KL
*> \verbatim
*>          KL is INTEGER
*>          The lower band width of the matrix to be generated.
*> \endverbatim
*>
*> \param[out] KU
*> \verbatim
*>          KU is INTEGER
*>          The upper band width of the matrix to be generated.
*> \endverbatim
*>
*> \param[out] ANORM
*> \verbatim
*>          ANORM is DOUBLE PRECISION
*>          The desired norm of the matrix to be generated.  The diagonal
*>          matrix of singular values or eigenvalues is scaled by this
*>          value.
*> \endverbatim
*>
*> \param[out] MODE
*> \verbatim
*>          MODE is INTEGER
*>          A key indicating how to choose the vector of eigenvalues.
*> \endverbatim
*>
*> \param[out] CNDNUM
*> \verbatim
*>          CNDNUM is DOUBLE PRECISION
*>          The desired condition number.
*> \endverbatim
*>
*> \param[out] DIST
*> \verbatim
*>          DIST is CHARACTER*1
*>          The type of distribution to be used by the random number
*>          generator.
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
      SUBROUTINE DLATB5( PATH, IMAT, N, TYPE, KL, KU, ANORM, MODE,
     $                   CNDNUM, DIST )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      DOUBLE PRECISION   ANORM, CNDNUM
      INTEGER            IMAT, KL, KU, MODE, N
      CHARACTER          DIST, TYPE
      CHARACTER*3        PATH
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   SHRINK, TENTH
      PARAMETER          ( SHRINK = 0.25D0, TENTH = 0.1D+0 )
      DOUBLE PRECISION   ONE
      PARAMETER          ( ONE = 1.0D+0 )
      DOUBLE PRECISION   TWO
      PARAMETER          ( TWO = 2.0D+0 )
*     ..
*     .. Local Scalars ..
      DOUBLE PRECISION   BADC1, BADC2, EPS, LARGE, SMALL
      LOGICAL            FIRST
      CHARACTER*2        C2
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DLAMCH
      EXTERNAL           DLAMCH
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, SQRT
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLABAD
*     ..
*     .. Save statement ..
      SAVE               EPS, SMALL, LARGE, BADC1, BADC2, FIRST
*     ..
*     .. Data statements ..
      DATA               FIRST / .TRUE. /
*     ..
*     .. Executable Statements ..
*
*     Set some constants for use in the subroutine.
*
      IF( FIRST ) THEN
         FIRST = .FALSE.
         EPS = DLAMCH( 'Precision' )
         BADC2 = TENTH / EPS
         BADC1 = SQRT( BADC2 )
         SMALL = DLAMCH( 'Safe minimum' )
         LARGE = ONE / SMALL
*
*        If it looks like we're on a Cray, take the square root of
*        SMALL and LARGE to avoid overflow and underflow problems.
*
         CALL DLABAD( SMALL, LARGE )
         SMALL = SHRINK*( SMALL / EPS )
         LARGE = ONE / SMALL
      END IF
*
      C2 = PATH( 2: 3 )
*
*     Set some parameters
*
      DIST = 'S'
      MODE = 3
*
*     Set TYPE, the type of matrix to be generated.
*
      TYPE = C2( 1: 1 )
*
*     Set the lower and upper bandwidths.
*
      IF( IMAT.EQ.1 ) THEN
         KL = 0
      ELSE
         KL = MAX( N-1, 0 )
      END IF
      KU = KL
*
*     Set the condition number and norm.etc
*
      IF( IMAT.EQ.3 ) THEN
         CNDNUM = 1.0D12
         MODE = 2
      ELSE IF( IMAT.EQ.4 ) THEN
         CNDNUM = 1.0D12
         MODE = 1
      ELSE IF( IMAT.EQ.5 ) THEN
         CNDNUM = 1.0D12
         MODE = 3
      ELSE IF( IMAT.EQ.6 ) THEN
         CNDNUM = BADC1
      ELSE IF( IMAT.EQ.7 ) THEN
         CNDNUM = BADC2
      ELSE
         CNDNUM = TWO
      END IF
*
      IF( IMAT.EQ.8 ) THEN
         ANORM = SMALL
      ELSE IF( IMAT.EQ.9 ) THEN
         ANORM = LARGE
      ELSE
         ANORM = ONE
      END IF
*
      IF( N.LE.1 )
     $   CNDNUM = ONE
*
      RETURN
*
*     End of DLATB5
*
      END
