*> \brief \b ZLAHILB
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZLAHILB( N, NRHS, A, LDA, X, LDX, B, LDB, WORK,
*            INFO, PATH)
*
*       .. Scalar Arguments ..
*       INTEGER N, NRHS, LDA, LDX, LDB, INFO
*       .. Array Arguments ..
*       DOUBLE PRECISION WORK(N)
*       COMPLEX*16 A(LDA,N), X(LDX, NRHS), B(LDB, NRHS)
*       CHARACTER*3 PATH
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZLAHILB generates an N by N scaled Hilbert matrix in A along with
*> NRHS right-hand sides in B and solutions in X such that A*X=B.
*>
*> The Hilbert matrix is scaled by M = LCM(1, 2, ..., 2*N-1) so that all
*> entries are integers.  The right-hand sides are the first NRHS
*> columns of M * the identity matrix, and the solutions are the
*> first NRHS columns of the inverse Hilbert matrix.
*>
*> The condition number of the Hilbert matrix grows exponentially with
*> its size, roughly as O(e ** (3.5*N)).  Additionally, the inverse
*> Hilbert matrices beyond a relatively small dimension cannot be
*> generated exactly without extra precision.  Precision is exhausted
*> when the largest entry in the inverse Hilbert matrix is greater than
*> 2 to the power of the number of bits in the fraction of the data type
*> used plus one, which is 24 for single precision.
*>
*> In single, the generated solution is exact for N <= 6 and has
*> small componentwise error for 7 <= N <= 11.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The dimension of the matrix A.
*> \endverbatim
*>
*> \param[in] NRHS
*> \verbatim
*>          NRHS is INTEGER
*>          The requested number of right-hand sides.
*> \endverbatim
*>
*> \param[out] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA, N)
*>          The generated scaled Hilbert matrix.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= N.
*> \endverbatim
*>
*> \param[out] X
*> \verbatim
*>          X is COMPLEX array, dimension (LDX, NRHS)
*>          The generated exact solutions.  Currently, the first NRHS
*>          columns of the inverse Hilbert matrix.
*> \endverbatim
*>
*> \param[in] LDX
*> \verbatim
*>          LDX is INTEGER
*>          The leading dimension of the array X.  LDX >= N.
*> \endverbatim
*>
*> \param[out] B
*> \verbatim
*>          B is REAL array, dimension (LDB, NRHS)
*>          The generated right-hand sides.  Currently, the first NRHS
*>          columns of LCM(1, 2, ..., 2*N-1) * the identity matrix.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B.  LDB >= N.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (N)
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0: successful exit
*>          = 1: N is too large; the data is still generated but may not
*>               be not exact.
*>          < 0: if INFO = -i, the i-th argument had an illegal value
*> \endverbatim
*>
*> \param[in] PATH
*> \verbatim
*>          PATH is CHARACTER*3
*>          The LAPACK path name.
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
*> \date June 2017
*
*> \ingroup complex16_lin
*
*  =====================================================================
      SUBROUTINE ZLAHILB( N, NRHS, A, LDA, X, LDX, B, LDB, WORK,
     $     INFO, PATH)
*
*  -- LAPACK test routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      INTEGER N, NRHS, LDA, LDX, LDB, INFO
*     .. Array Arguments ..
      DOUBLE PRECISION WORK(N)
      COMPLEX*16 A(LDA,N), X(LDX, NRHS), B(LDB, NRHS)
      CHARACTER*3 PATH
*     ..
*
*  =====================================================================
*     .. Local Scalars ..
      INTEGER TM, TI, R
      INTEGER M
      INTEGER I, J
      COMPLEX*16 TMP
      CHARACTER*2 C2
*     ..
*     .. Parameters ..
*     NMAX_EXACT   the largest dimension where the generated data is
*                  exact.
*     NMAX_APPROX  the largest dimension where the generated data has
*                  a small componentwise relative error.
*     ??? complex uses how many bits ???
      INTEGER NMAX_EXACT, NMAX_APPROX, SIZE_D
      PARAMETER (NMAX_EXACT = 6, NMAX_APPROX = 11, SIZE_D = 8)
*
*     d's are generated from random permuation of those eight elements.
      COMPLEX*16 d1(8), d2(8), invd1(8), invd2(8)
      DATA D1 /(-1,0),(0,1),(-1,-1),(0,-1),(1,0),(-1,1),(1,1),(1,-1)/
      DATA D2 /(-1,0),(0,-1),(-1,1),(0,1),(1,0),(-1,-1),(1,-1),(1,1)/

      DATA INVD1 /(-1,0),(0,-1),(-.5,.5),(0,1),(1,0),
     $     (-.5,-.5),(.5,-.5),(.5,.5)/
      DATA INVD2 /(-1,0),(0,1),(-.5,-.5),(0,-1),(1,0),
     $     (-.5,.5),(.5,.5),(.5,-.5)/
*     ..
*     .. External Functions
      EXTERNAL ZLASET, LSAMEN
      INTRINSIC DBLE
      LOGICAL LSAMEN
*     ..
*     .. Executable Statements ..
      C2 = PATH( 2: 3 )
*
*     Test the input arguments
*
      INFO = 0
      IF (N .LT. 0 .OR. N .GT. NMAX_APPROX) THEN
         INFO = -1
      ELSE IF (NRHS .LT. 0) THEN
         INFO = -2
      ELSE IF (LDA .LT. N) THEN
         INFO = -4
      ELSE IF (LDX .LT. N) THEN
         INFO = -6
      ELSE IF (LDB .LT. N) THEN
         INFO = -8
      END IF
      IF (INFO .LT. 0) THEN
         CALL XERBLA('ZLAHILB', -INFO)
         RETURN
      END IF
      IF (N .GT. NMAX_EXACT) THEN
         INFO = 1
      END IF
*
*     Compute M = the LCM of the integers [1, 2*N-1].  The largest
*     reasonable N is small enough that integers suffice (up to N = 11).
      M = 1
      DO I = 2, (2*N-1)
         TM = M
         TI = I
         R = MOD(TM, TI)
         DO WHILE (R .NE. 0)
            TM = TI
            TI = R
            R = MOD(TM, TI)
         END DO
         M = (M / TI) * I
      END DO
*
*     Generate the scaled Hilbert matrix in A
*     If we are testing SY routines,
*        take D1_i = D2_i, else, D1_i = D2_i*
      IF ( LSAMEN( 2, C2, 'SY' ) ) THEN
         DO J = 1, N
            DO I = 1, N
               A(I, J) = D1(MOD(J,SIZE_D)+1) * (DBLE(M) / (I + J - 1))
     $              * D1(MOD(I,SIZE_D)+1)
            END DO
         END DO
      ELSE
         DO J = 1, N
            DO I = 1, N
               A(I, J) = D1(MOD(J,SIZE_D)+1) * (DBLE(M) / (I + J - 1))
     $              * D2(MOD(I,SIZE_D)+1)
            END DO
         END DO
      END IF
*
*     Generate matrix B as simply the first NRHS columns of M * the
*     identity.
      TMP = DBLE(M)
      CALL ZLASET('Full', N, NRHS, (0.0D+0,0.0D+0), TMP, B, LDB)
*
*     Generate the true solutions in X.  Because B = the first NRHS
*     columns of M*I, the true solutions are just the first NRHS columns
*     of the inverse Hilbert matrix.
      WORK(1) = N
      DO J = 2, N
         WORK(J) = (  ( (WORK(J-1)/(J-1)) * (J-1 - N) ) /(J-1)  )
     $        * (N +J -1)
      END DO

*     If we are testing SY routines,
*           take D1_i = D2_i, else, D1_i = D2_i*
      IF ( LSAMEN( 2, C2, 'SY' ) ) THEN
         DO J = 1, NRHS
            DO I = 1, N
               X(I, J) = INVD1(MOD(J,SIZE_D)+1) *
     $              ((WORK(I)*WORK(J)) / (I + J - 1))
     $              * INVD1(MOD(I,SIZE_D)+1)
            END DO
         END DO
      ELSE
         DO J = 1, NRHS
            DO I = 1, N
               X(I, J) = INVD2(MOD(J,SIZE_D)+1) *
     $              ((WORK(I)*WORK(J)) / (I + J - 1))
     $              * INVD1(MOD(I,SIZE_D)+1)
            END DO
         END DO
      END IF
      END
