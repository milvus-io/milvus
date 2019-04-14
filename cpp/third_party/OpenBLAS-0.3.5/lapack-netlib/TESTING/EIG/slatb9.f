*> \brief \b SLATB9
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLATB9( PATH, IMAT, M, P, N, TYPE, KLA, KUA,
*                          KLB, KUB, ANORM, BNORM, MODEA, MODEB,
*                          CNDNMA, CNDNMB, DISTA, DISTB )
*
*       .. Scalar Arguments ..
*       CHARACTER          DISTA, DISTB, TYPE
*       CHARACTER*3        PATH
*       INTEGER            IMAT, KLA, KUA, KLB, KUB, M, P, MODEA, MODEB, N
*       REAL               ANORM, BNORM, CNDNMA, CNDNMB
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLATB9 sets parameters for the matrix generator based on the type of
*> matrix to be generated.
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
*>          = 1:   A: diagonal, B: upper triangular
*>          = 2:   A: upper triangular, B: upper triangular
*>          = 3:   A: lower triangular, B: upper triangular
*>          Else:  A: general dense, B: general dense
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows in the matrix to be generated.
*> \endverbatim
*>
*> \param[in] P
*> \verbatim
*>          P is INTEGER
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns in the matrix to be generated.
*> \endverbatim
*>
*> \param[out] TYPE
*> \verbatim
*>          TYPE is CHARACTER*1
*>          The type of the matrix to be generated:
*>          = 'S':  symmetric matrix;
*>          = 'P':  symmetric positive (semi)definite matrix;
*>          = 'N':  nonsymmetric matrix.
*> \endverbatim
*>
*> \param[out] KLA
*> \verbatim
*>          KLA is INTEGER
*>          The lower band width of the matrix to be generated.
*> \endverbatim
*>
*> \param[out] KUA
*> \verbatim
*>          KUA is INTEGER
*>          The upper band width of the matrix to be generated.
*> \endverbatim
*>
*> \param[out] KLB
*> \verbatim
*>          KLB is INTEGER
*>          The lower band width of the matrix to be generated.
*> \endverbatim
*>
*> \param[out] KUB
*> \verbatim
*>          KUA is INTEGER
*>          The upper band width of the matrix to be generated.
*> \endverbatim
*>
*> \param[out] ANORM
*> \verbatim
*>          ANORM is REAL
*>          The desired norm of the matrix to be generated.  The diagonal
*>          matrix of singular values or eigenvalues is scaled by this
*>          value.
*> \endverbatim
*>
*> \param[out] BNORM
*> \verbatim
*>          BNORM is REAL
*>          The desired norm of the matrix to be generated.  The diagonal
*>          matrix of singular values or eigenvalues is scaled by this
*>          value.
*> \endverbatim
*>
*> \param[out] MODEA
*> \verbatim
*>          MODEA is INTEGER
*>          A key indicating how to choose the vector of eigenvalues.
*> \endverbatim
*>
*> \param[out] MODEB
*> \verbatim
*>          MODEB is INTEGER
*>          A key indicating how to choose the vector of eigenvalues.
*> \endverbatim
*>
*> \param[out] CNDNMA
*> \verbatim
*>          CNDNMA is REAL
*>          The desired condition number.
*> \endverbatim
*>
*> \param[out] CNDNMB
*> \verbatim
*>          CNDNMB is REAL
*>          The desired condition number.
*> \endverbatim
*>
*> \param[out] DISTA
*> \verbatim
*>          DISTA is CHARACTER*1
*>          The type of distribution to be used by the random number
*>          generator.
*> \endverbatim
*>
*> \param[out] DISTB
*> \verbatim
*>          DISTB is CHARACTER*1
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
*> \ingroup single_eig
*
*  =====================================================================
      SUBROUTINE SLATB9( PATH, IMAT, M, P, N, TYPE, KLA, KUA,
     $                   KLB, KUB, ANORM, BNORM, MODEA, MODEB,
     $                   CNDNMA, CNDNMB, DISTA, DISTB )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          DISTA, DISTB, TYPE
      CHARACTER*3        PATH
      INTEGER            IMAT, KLA, KUA, KLB, KUB, M, P, MODEA, MODEB, N
      REAL               ANORM, BNORM, CNDNMA, CNDNMB
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               SHRINK, TENTH
      PARAMETER          ( SHRINK = 0.25E0, TENTH = 0.1E+0 )
      REAL               ONE, TEN
      PARAMETER          ( ONE = 1.0E+0, TEN = 1.0E+1 )
*     ..
*     .. Local Scalars ..
      LOGICAL            FIRST
      REAL               BADC1, BADC2, EPS, LARGE, SMALL
*     ..
*     .. External Functions ..
      LOGICAL            LSAMEN
      REAL               SLAMCH
      EXTERNAL           LSAMEN, SLAMCH
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, SQRT
*     ..
*     .. External Subroutines ..
      EXTERNAL           SLABAD
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
         EPS = SLAMCH( 'Precision' )
         BADC2 = TENTH / EPS
         BADC1 = SQRT( BADC2 )
         SMALL = SLAMCH( 'Safe minimum' )
         LARGE = ONE / SMALL
*
*        If it looks like we're on a Cray, take the square root of
*        SMALL and LARGE to avoid overflow and underflow problems.
*
         CALL SLABAD( SMALL, LARGE )
         SMALL = SHRINK*( SMALL / EPS )
         LARGE = ONE / SMALL
      END IF
*
*     Set some parameters we don't plan to change.
*
      TYPE = 'N'
      DISTA = 'S'
      DISTB = 'S'
      MODEA = 3
      MODEB = 4
*
*     Set the lower and upper bandwidths.
*
      IF( LSAMEN( 3, PATH, 'GRQ') .OR. LSAMEN( 3, PATH, 'LSE') .OR.
     $    LSAMEN( 3, PATH, 'GSV') )THEN
*
*        A: M by N, B: P by N
*
         IF( IMAT.EQ.1 ) THEN
*
*           A: diagonal, B: upper triangular
*
            KLA = 0
            KUA = 0
            KLB = 0
            KUB = MAX( N-1,0 )
*
         ELSE IF( IMAT.EQ.2 ) THEN
*
*           A: upper triangular, B: upper triangular
*
            KLA = 0
            KUA = MAX( N-1, 0 )
            KLB = 0
            KUB = MAX( N-1, 0 )
*
         ELSE IF( IMAT.EQ.3 ) THEN
*
*           A: lower triangular, B: upper triangular
*
            KLA = MAX( M-1, 0 )
            KUA = 0
            KLB = 0
            KUB = MAX( N-1, 0 )
*
         ELSE
*
*           A: general dense, B: general dense
*
            KLA = MAX( M-1, 0 )
            KUA = MAX( N-1, 0 )
            KLB = MAX( P-1, 0 )
            KUB = MAX( N-1, 0 )
*
         END IF
*
      ELSE IF( LSAMEN( 3, PATH, 'GQR' ) .OR.
     $         LSAMEN( 3, PATH, 'GLM') )THEN
*
*        A: N by M, B: N by P
*
         IF( IMAT.EQ.1 ) THEN
*
*           A: diagonal, B: lower triangular
*
            KLA = 0
            KUA = 0
            KLB = MAX( N-1,0 )
            KUB = 0
         ELSE IF( IMAT.EQ.2 ) THEN
*
*           A: lower triangular, B: diagonal
*
            KLA = MAX( N-1, 0 )
            KUA = 0
            KLB = 0
            KUB = 0
*
         ELSE IF( IMAT.EQ.3 ) THEN
*
*           A: lower triangular, B: upper triangular
*
            KLA = MAX( N-1, 0 )
            KUA = 0
            KLB = 0
            KUB = MAX( P-1, 0 )
*
         ELSE
*
*           A: general dense, B: general dense
*
            KLA = MAX( N-1, 0 )
            KUA = MAX( M-1, 0 )
            KLB = MAX( N-1, 0 )
            KUB = MAX( P-1, 0 )
         END IF
*
      END IF
*
*     Set the condition number and norm.
*
      CNDNMA = TEN*TEN
      CNDNMB = TEN
      IF( LSAMEN( 3, PATH, 'GQR') .OR. LSAMEN( 3, PATH, 'GRQ') .OR.
     $    LSAMEN( 3, PATH, 'GSV') )THEN
         IF( IMAT.EQ.5 ) THEN
            CNDNMA = BADC1
            CNDNMB = BADC1
         ELSE IF( IMAT.EQ.6 ) THEN
            CNDNMA = BADC2
            CNDNMB = BADC2
         ELSE IF( IMAT.EQ.7 ) THEN
            CNDNMA = BADC1
            CNDNMB = BADC2
         ELSE IF( IMAT.EQ.8 ) THEN
            CNDNMA = BADC2
            CNDNMB = BADC1
         END IF
      END IF
*
      ANORM = TEN
      BNORM = TEN*TEN*TEN
      IF( LSAMEN( 3, PATH, 'GQR') .OR. LSAMEN( 3, PATH, 'GRQ') )THEN
         IF( IMAT.EQ.7 ) THEN
            ANORM = SMALL
            BNORM = LARGE
         ELSE IF( IMAT.EQ.8 ) THEN
            ANORM = LARGE
            BNORM = SMALL
         END IF
      END IF
*
      IF( N.LE.1 )THEN
         CNDNMA = ONE
         CNDNMB = ONE
      END IF
*
      RETURN
*
*     End of SLATB9
*
      END
