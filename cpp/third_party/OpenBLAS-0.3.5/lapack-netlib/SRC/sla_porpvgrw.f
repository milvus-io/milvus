*> \brief \b SLA_PORPVGRW computes the reciprocal pivot growth factor norm(A)/norm(U) for a symmetric or Hermitian positive-definite matrix.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLA_PORPVGRW + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/sla_porpvgrw.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/sla_porpvgrw.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/sla_porpvgrw.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       REAL FUNCTION SLA_PORPVGRW( UPLO, NCOLS, A, LDA, AF, LDAF, WORK )
*
*       .. Scalar Arguments ..
*       CHARACTER*1        UPLO
*       INTEGER            NCOLS, LDA, LDAF
*       ..
*       .. Array Arguments ..
*       REAL               A( LDA, * ), AF( LDAF, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>
*> SLA_PORPVGRW computes the reciprocal pivot growth factor
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
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>       = 'U':  Upper triangle of A is stored;
*>       = 'L':  Lower triangle of A is stored.
*> \endverbatim
*>
*> \param[in] NCOLS
*> \verbatim
*>          NCOLS is INTEGER
*>     The number of columns of the matrix A. NCOLS >= 0.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is REAL array, dimension (LDA,N)
*>     On entry, the N-by-N matrix A.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>     The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[in] AF
*> \verbatim
*>          AF is REAL array, dimension (LDAF,N)
*>     The triangular factor U or L from the Cholesky factorization
*>     A = U**T*U or A = L*L**T, as computed by SPOTRF.
*> \endverbatim
*>
*> \param[in] LDAF
*> \verbatim
*>          LDAF is INTEGER
*>     The leading dimension of the array AF.  LDAF >= max(1,N).
*> \endverbatim
*>
*> \param[in] WORK
*> \verbatim
*>          WORK is REAL array, dimension (2*N)
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
*> \ingroup realPOcomputational
*
*  =====================================================================
      REAL FUNCTION SLA_PORPVGRW( UPLO, NCOLS, A, LDA, AF, LDAF, WORK )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER*1        UPLO
      INTEGER            NCOLS, LDA, LDAF
*     ..
*     .. Array Arguments ..
      REAL               A( LDA, * ), AF( LDAF, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      INTEGER            I, J
      REAL               AMAX, UMAX, RPVGRW
      LOGICAL            UPPER
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, MIN
*     ..
*     .. External Functions ..
      EXTERNAL           LSAME
      LOGICAL            LSAME
*     ..
*     .. Executable Statements ..
*
      UPPER = LSAME( 'Upper', UPLO )
*
*     SPOTRF will have factored only the NCOLSxNCOLS leading minor, so
*     we restrict the growth search to that minor and use only the first
*     2*NCOLS workspace entries.
*
      RPVGRW = 1.0
      DO I = 1, 2*NCOLS
         WORK( I ) = 0.0
      END DO
*
*     Find the max magnitude entry of each column.
*
      IF ( UPPER ) THEN
         DO J = 1, NCOLS
            DO I = 1, J
               WORK( NCOLS+J ) =
     $              MAX( ABS( A( I, J ) ), WORK( NCOLS+J ) )
            END DO
         END DO
      ELSE
         DO J = 1, NCOLS
            DO I = J, NCOLS
               WORK( NCOLS+J ) =
     $              MAX( ABS( A( I, J ) ), WORK( NCOLS+J ) )
            END DO
         END DO
      END IF
*
*     Now find the max magnitude entry of each column of the factor in
*     AF.  No pivoting, so no permutations.
*
      IF ( LSAME( 'Upper', UPLO ) ) THEN
         DO J = 1, NCOLS
            DO I = 1, J
               WORK( J ) = MAX( ABS( AF( I, J ) ), WORK( J ) )
            END DO
         END DO
      ELSE
         DO J = 1, NCOLS
            DO I = J, NCOLS
               WORK( J ) = MAX( ABS( AF( I, J ) ), WORK( J ) )
            END DO
         END DO
      END IF
*
*     Compute the *inverse* of the max element growth factor.  Dividing
*     by zero would imply the largest entry of the factor's column is
*     zero.  Than can happen when either the column of A is zero or
*     massive pivots made the factor underflow to zero.  Neither counts
*     as growth in itself, so simply ignore terms with zero
*     denominators.
*
      IF ( LSAME( 'Upper', UPLO ) ) THEN
         DO I = 1, NCOLS
            UMAX = WORK( I )
            AMAX = WORK( NCOLS+I )
            IF ( UMAX /= 0.0 ) THEN
               RPVGRW = MIN( AMAX / UMAX, RPVGRW )
            END IF
         END DO
      ELSE
         DO I = 1, NCOLS
            UMAX = WORK( I )
            AMAX = WORK( NCOLS+I )
            IF ( UMAX /= 0.0 ) THEN
               RPVGRW = MIN( AMAX / UMAX, RPVGRW )
            END IF
         END DO
      END IF

      SLA_PORPVGRW = RPVGRW
      END
