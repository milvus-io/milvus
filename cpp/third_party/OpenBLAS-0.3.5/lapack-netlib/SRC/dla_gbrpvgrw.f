*> \brief \b DLA_GBRPVGRW computes the reciprocal pivot growth factor norm(A)/norm(U) for a general banded matrix.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DLA_GBRPVGRW + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dla_gbrpvgrw.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dla_gbrpvgrw.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dla_gbrpvgrw.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       DOUBLE PRECISION FUNCTION DLA_GBRPVGRW( N, KL, KU, NCOLS, AB,
*                                               LDAB, AFB, LDAFB )
*
*       .. Scalar Arguments ..
*       INTEGER            N, KL, KU, NCOLS, LDAB, LDAFB
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   AB( LDAB, * ), AFB( LDAFB, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DLA_GBRPVGRW computes the reciprocal pivot growth factor
*> norm(A)/norm(U). The "max absolute element" norm is used. If this is
*> much less than 1, the stability of the LU factorization of the
*> (equilibrated) matrix A could be poor. This also means that the
*> solution X, estimated condition numbers, and error bounds could be
*> unreliable.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>     The number of linear equations, i.e., the order of the
*>     matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] KL
*> \verbatim
*>          KL is INTEGER
*>     The number of subdiagonals within the band of A.  KL >= 0.
*> \endverbatim
*>
*> \param[in] KU
*> \verbatim
*>          KU is INTEGER
*>     The number of superdiagonals within the band of A.  KU >= 0.
*> \endverbatim
*>
*> \param[in] NCOLS
*> \verbatim
*>          NCOLS is INTEGER
*>     The number of columns of the matrix A.  NCOLS >= 0.
*> \endverbatim
*>
*> \param[in] AB
*> \verbatim
*>          AB is DOUBLE PRECISION array, dimension (LDAB,N)
*>     On entry, the matrix A in band storage, in rows 1 to KL+KU+1.
*>     The j-th column of A is stored in the j-th column of the
*>     array AB as follows:
*>     AB(KU+1+i-j,j) = A(i,j) for max(1,j-KU)<=i<=min(N,j+kl)
*> \endverbatim
*>
*> \param[in] LDAB
*> \verbatim
*>          LDAB is INTEGER
*>     The leading dimension of the array AB.  LDAB >= KL+KU+1.
*> \endverbatim
*>
*> \param[in] AFB
*> \verbatim
*>          AFB is DOUBLE PRECISION array, dimension (LDAFB,N)
*>     Details of the LU factorization of the band matrix A, as
*>     computed by DGBTRF.  U is stored as an upper triangular
*>     band matrix with KL+KU superdiagonals in rows 1 to KL+KU+1,
*>     and the multipliers used during the factorization are stored
*>     in rows KL+KU+2 to 2*KL+KU+1.
*> \endverbatim
*>
*> \param[in] LDAFB
*> \verbatim
*>          LDAFB is INTEGER
*>     The leading dimension of the array AFB.  LDAFB >= 2*KL+KU+1.
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
*> \ingroup doubleGBcomputational
*
*  =====================================================================
      DOUBLE PRECISION FUNCTION DLA_GBRPVGRW( N, KL, KU, NCOLS, AB,
     $                                        LDAB, AFB, LDAFB )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            N, KL, KU, NCOLS, LDAB, LDAFB
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   AB( LDAB, * ), AFB( LDAFB, * )
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      INTEGER            I, J, KD
      DOUBLE PRECISION   AMAX, UMAX, RPVGRW
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, MIN
*     ..
*     .. Executable Statements ..
*
      RPVGRW = 1.0D+0

      KD = KU + 1
      DO J = 1, NCOLS
         AMAX = 0.0D+0
         UMAX = 0.0D+0
         DO I = MAX( J-KU, 1 ), MIN( J+KL, N )
            AMAX = MAX( ABS( AB( KD+I-J, J)), AMAX )
         END DO
         DO I = MAX( J-KU, 1 ), J
            UMAX = MAX( ABS( AFB( KD+I-J, J ) ), UMAX )
         END DO
         IF ( UMAX /= 0.0D+0 ) THEN
            RPVGRW = MIN( AMAX / UMAX, RPVGRW )
         END IF
      END DO
      DLA_GBRPVGRW = RPVGRW
      END
