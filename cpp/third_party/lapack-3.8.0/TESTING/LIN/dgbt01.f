*> \brief \b DGBT01
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE DGBT01( M, N, KL, KU, A, LDA, AFAC, LDAFAC, IPIV, WORK,
*                          RESID )
*
*       .. Scalar Arguments ..
*       INTEGER            KL, KU, LDA, LDAFAC, M, N
*       DOUBLE PRECISION   RESID
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * )
*       DOUBLE PRECISION   A( LDA, * ), AFAC( LDAFAC, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DGBT01 reconstructs a band matrix  A  from its L*U factorization and
*> computes the residual:
*>    norm(L*U - A) / ( N * norm(A) * EPS ),
*> where EPS is the machine epsilon.
*>
*> The expression L*U - A is computed one column at a time, so A and
*> AFAC are not modified.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix A.  M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] KL
*> \verbatim
*>          KL is INTEGER
*>          The number of subdiagonals within the band of A.  KL >= 0.
*> \endverbatim
*>
*> \param[in] KU
*> \verbatim
*>          KU is INTEGER
*>          The number of superdiagonals within the band of A.  KU >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension (LDA,N)
*>          The original matrix A in band storage, stored in rows 1 to
*>          KL+KU+1.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER.
*>          The leading dimension of the array A.  LDA >= max(1,KL+KU+1).
*> \endverbatim
*>
*> \param[in] AFAC
*> \verbatim
*>          AFAC is DOUBLE PRECISION array, dimension (LDAFAC,N)
*>          The factored form of the matrix A.  AFAC contains the banded
*>          factors L and U from the L*U factorization, as computed by
*>          DGBTRF.  U is stored as an upper triangular band matrix with
*>          KL+KU superdiagonals in rows 1 to KL+KU+1, and the
*>          multipliers used during the factorization are stored in rows
*>          KL+KU+2 to 2*KL+KU+1.  See DGBTRF for further details.
*> \endverbatim
*>
*> \param[in] LDAFAC
*> \verbatim
*>          LDAFAC is INTEGER
*>          The leading dimension of the array AFAC.
*>          LDAFAC >= max(1,2*KL*KU+1).
*> \endverbatim
*>
*> \param[in] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (min(M,N))
*>          The pivot indices from DGBTRF.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is DOUBLE PRECISION array, dimension (2*KL+KU+1)
*> \endverbatim
*>
*> \param[out] RESID
*> \verbatim
*>          RESID is DOUBLE PRECISION
*>          norm(L*U - A) / ( N * norm(A) * EPS )
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
      SUBROUTINE DGBT01( M, N, KL, KU, A, LDA, AFAC, LDAFAC, IPIV, WORK,
     $                   RESID )
*
*  -- LAPACK test routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            KL, KU, LDA, LDAFAC, M, N
      DOUBLE PRECISION   RESID
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * )
      DOUBLE PRECISION   A( LDA, * ), AFAC( LDAFAC, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, I1, I2, IL, IP, IW, J, JL, JU, JUA, KD, LENJ
      DOUBLE PRECISION   ANORM, EPS, T
*     ..
*     .. External Functions ..
      DOUBLE PRECISION   DASUM, DLAMCH
      EXTERNAL           DASUM, DLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           DAXPY, DCOPY
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE, MAX, MIN
*     ..
*     .. Executable Statements ..
*
*     Quick exit if M = 0 or N = 0.
*
      RESID = ZERO
      IF( M.LE.0 .OR. N.LE.0 )
     $   RETURN
*
*     Determine EPS and the norm of A.
*
      EPS = DLAMCH( 'Epsilon' )
      KD = KU + 1
      ANORM = ZERO
      DO 10 J = 1, N
         I1 = MAX( KD+1-J, 1 )
         I2 = MIN( KD+M-J, KL+KD )
         IF( I2.GE.I1 )
     $      ANORM = MAX( ANORM, DASUM( I2-I1+1, A( I1, J ), 1 ) )
   10 CONTINUE
*
*     Compute one column at a time of L*U - A.
*
      KD = KL + KU + 1
      DO 40 J = 1, N
*
*        Copy the J-th column of U to WORK.
*
         JU = MIN( KL+KU, J-1 )
         JL = MIN( KL, M-J )
         LENJ = MIN( M, J ) - J + JU + 1
         IF( LENJ.GT.0 ) THEN
            CALL DCOPY( LENJ, AFAC( KD-JU, J ), 1, WORK, 1 )
            DO 20 I = LENJ + 1, JU + JL + 1
               WORK( I ) = ZERO
   20       CONTINUE
*
*           Multiply by the unit lower triangular matrix L.  Note that L
*           is stored as a product of transformations and permutations.
*
            DO 30 I = MIN( M-1, J ), J - JU, -1
               IL = MIN( KL, M-I )
               IF( IL.GT.0 ) THEN
                  IW = I - J + JU + 1
                  T = WORK( IW )
                  CALL DAXPY( IL, T, AFAC( KD+1, I ), 1, WORK( IW+1 ),
     $                        1 )
                  IP = IPIV( I )
                  IF( I.NE.IP ) THEN
                     IP = IP - J + JU + 1
                     WORK( IW ) = WORK( IP )
                     WORK( IP ) = T
                  END IF
               END IF
   30       CONTINUE
*
*           Subtract the corresponding column of A.
*
            JUA = MIN( JU, KU )
            IF( JUA+JL+1.GT.0 )
     $         CALL DAXPY( JUA+JL+1, -ONE, A( KU+1-JUA, J ), 1,
     $                     WORK( JU+1-JUA ), 1 )
*
*           Compute the 1-norm of the column.
*
            RESID = MAX( RESID, DASUM( JU+JL+1, WORK, 1 ) )
         END IF
   40 CONTINUE
*
*     Compute norm( L*U - A ) / ( N * norm(A) * EPS )
*
      IF( ANORM.LE.ZERO ) THEN
         IF( RESID.NE.ZERO )
     $      RESID = ONE / EPS
      ELSE
         RESID = ( ( RESID / DBLE( N ) ) / ANORM ) / EPS
      END IF
*
      RETURN
*
*     End of DGBT01
*
      END
