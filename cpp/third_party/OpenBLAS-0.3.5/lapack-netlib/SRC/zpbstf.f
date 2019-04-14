*> \brief \b ZPBSTF
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZPBSTF + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zpbstf.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zpbstf.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zpbstf.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZPBSTF( UPLO, N, KD, AB, LDAB, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            INFO, KD, LDAB, N
*       ..
*       .. Array Arguments ..
*       COMPLEX*16         AB( LDAB, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZPBSTF computes a split Cholesky factorization of a complex
*> Hermitian positive definite band matrix A.
*>
*> This routine is designed to be used in conjunction with ZHBGST.
*>
*> The factorization has the form  A = S**H*S  where S is a band matrix
*> of the same bandwidth as A and the following structure:
*>
*>   S = ( U    )
*>       ( M  L )
*>
*> where U is upper triangular of order m = (n+kd)/2, and L is lower
*> triangular of order n-m.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          = 'U':  Upper triangle of A is stored;
*>          = 'L':  Lower triangle of A is stored.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in] KD
*> \verbatim
*>          KD is INTEGER
*>          The number of superdiagonals of the matrix A if UPLO = 'U',
*>          or the number of subdiagonals if UPLO = 'L'.  KD >= 0.
*> \endverbatim
*>
*> \param[in,out] AB
*> \verbatim
*>          AB is COMPLEX*16 array, dimension (LDAB,N)
*>          On entry, the upper or lower triangle of the Hermitian band
*>          matrix A, stored in the first kd+1 rows of the array.  The
*>          j-th column of A is stored in the j-th column of the array AB
*>          as follows:
*>          if UPLO = 'U', AB(kd+1+i-j,j) = A(i,j) for max(1,j-kd)<=i<=j;
*>          if UPLO = 'L', AB(1+i-j,j)    = A(i,j) for j<=i<=min(n,j+kd).
*>
*>          On exit, if INFO = 0, the factor S from the split Cholesky
*>          factorization A = S**H*S. See Further Details.
*> \endverbatim
*>
*> \param[in] LDAB
*> \verbatim
*>          LDAB is INTEGER
*>          The leading dimension of the array AB.  LDAB >= KD+1.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0: successful exit
*>          < 0: if INFO = -i, the i-th argument had an illegal value
*>          > 0: if INFO = i, the factorization could not be completed,
*>               because the updated element a(i,i) was negative; the
*>               matrix A is not positive definite.
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
*> \ingroup complex16OTHERcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  The band storage scheme is illustrated by the following example, when
*>  N = 7, KD = 2:
*>
*>  S = ( s11  s12  s13                     )
*>      (      s22  s23  s24                )
*>      (           s33  s34                )
*>      (                s44                )
*>      (           s53  s54  s55           )
*>      (                s64  s65  s66      )
*>      (                     s75  s76  s77 )
*>
*>  If UPLO = 'U', the array AB holds:
*>
*>  on entry:                          on exit:
*>
*>   *    *   a13  a24  a35  a46  a57   *    *   s13  s24  s53**H s64**H s75**H
*>   *   a12  a23  a34  a45  a56  a67   *   s12  s23  s34  s54**H s65**H s76**H
*>  a11  a22  a33  a44  a55  a66  a77  s11  s22  s33  s44  s55    s66    s77
*>
*>  If UPLO = 'L', the array AB holds:
*>
*>  on entry:                          on exit:
*>
*>  a11  a22  a33  a44  a55  a66  a77  s11    s22    s33    s44  s55  s66  s77
*>  a21  a32  a43  a54  a65  a76   *   s12**H s23**H s34**H s54  s65  s76   *
*>  a31  a42  a53  a64  a64   *    *   s13**H s24**H s53    s64  s75   *    *
*>
*>  Array elements marked * are not used by the routine; s12**H denotes
*>  conjg(s12); the diagonal elements of S are real.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE ZPBSTF( UPLO, N, KD, AB, LDAB, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            INFO, KD, LDAB, N
*     ..
*     .. Array Arguments ..
      COMPLEX*16         AB( LDAB, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ONE, ZERO
      PARAMETER          ( ONE = 1.0D+0, ZERO = 0.0D+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            UPPER
      INTEGER            J, KLD, KM, M
      DOUBLE PRECISION   AJJ
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA, ZDSCAL, ZHER, ZLACGV
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE, MAX, MIN, SQRT
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
      UPPER = LSAME( UPLO, 'U' )
      IF( .NOT.UPPER .AND. .NOT.LSAME( UPLO, 'L' ) ) THEN
         INFO = -1
      ELSE IF( N.LT.0 ) THEN
         INFO = -2
      ELSE IF( KD.LT.0 ) THEN
         INFO = -3
      ELSE IF( LDAB.LT.KD+1 ) THEN
         INFO = -5
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZPBSTF', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 )
     $   RETURN
*
      KLD = MAX( 1, LDAB-1 )
*
*     Set the splitting point m.
*
      M = ( N+KD ) / 2
*
      IF( UPPER ) THEN
*
*        Factorize A(m+1:n,m+1:n) as L**H*L, and update A(1:m,1:m).
*
         DO 10 J = N, M + 1, -1
*
*           Compute s(j,j) and test for non-positive-definiteness.
*
            AJJ = DBLE( AB( KD+1, J ) )
            IF( AJJ.LE.ZERO ) THEN
               AB( KD+1, J ) = AJJ
               GO TO 50
            END IF
            AJJ = SQRT( AJJ )
            AB( KD+1, J ) = AJJ
            KM = MIN( J-1, KD )
*
*           Compute elements j-km:j-1 of the j-th column and update the
*           the leading submatrix within the band.
*
            CALL ZDSCAL( KM, ONE / AJJ, AB( KD+1-KM, J ), 1 )
            CALL ZHER( 'Upper', KM, -ONE, AB( KD+1-KM, J ), 1,
     $                 AB( KD+1, J-KM ), KLD )
   10    CONTINUE
*
*        Factorize the updated submatrix A(1:m,1:m) as U**H*U.
*
         DO 20 J = 1, M
*
*           Compute s(j,j) and test for non-positive-definiteness.
*
            AJJ = DBLE( AB( KD+1, J ) )
            IF( AJJ.LE.ZERO ) THEN
               AB( KD+1, J ) = AJJ
               GO TO 50
            END IF
            AJJ = SQRT( AJJ )
            AB( KD+1, J ) = AJJ
            KM = MIN( KD, M-J )
*
*           Compute elements j+1:j+km of the j-th row and update the
*           trailing submatrix within the band.
*
            IF( KM.GT.0 ) THEN
               CALL ZDSCAL( KM, ONE / AJJ, AB( KD, J+1 ), KLD )
               CALL ZLACGV( KM, AB( KD, J+1 ), KLD )
               CALL ZHER( 'Upper', KM, -ONE, AB( KD, J+1 ), KLD,
     $                    AB( KD+1, J+1 ), KLD )
               CALL ZLACGV( KM, AB( KD, J+1 ), KLD )
            END IF
   20    CONTINUE
      ELSE
*
*        Factorize A(m+1:n,m+1:n) as L**H*L, and update A(1:m,1:m).
*
         DO 30 J = N, M + 1, -1
*
*           Compute s(j,j) and test for non-positive-definiteness.
*
            AJJ = DBLE( AB( 1, J ) )
            IF( AJJ.LE.ZERO ) THEN
               AB( 1, J ) = AJJ
               GO TO 50
            END IF
            AJJ = SQRT( AJJ )
            AB( 1, J ) = AJJ
            KM = MIN( J-1, KD )
*
*           Compute elements j-km:j-1 of the j-th row and update the
*           trailing submatrix within the band.
*
            CALL ZDSCAL( KM, ONE / AJJ, AB( KM+1, J-KM ), KLD )
            CALL ZLACGV( KM, AB( KM+1, J-KM ), KLD )
            CALL ZHER( 'Lower', KM, -ONE, AB( KM+1, J-KM ), KLD,
     $                 AB( 1, J-KM ), KLD )
            CALL ZLACGV( KM, AB( KM+1, J-KM ), KLD )
   30    CONTINUE
*
*        Factorize the updated submatrix A(1:m,1:m) as U**H*U.
*
         DO 40 J = 1, M
*
*           Compute s(j,j) and test for non-positive-definiteness.
*
            AJJ = DBLE( AB( 1, J ) )
            IF( AJJ.LE.ZERO ) THEN
               AB( 1, J ) = AJJ
               GO TO 50
            END IF
            AJJ = SQRT( AJJ )
            AB( 1, J ) = AJJ
            KM = MIN( KD, M-J )
*
*           Compute elements j+1:j+km of the j-th column and update the
*           trailing submatrix within the band.
*
            IF( KM.GT.0 ) THEN
               CALL ZDSCAL( KM, ONE / AJJ, AB( 2, J ), 1 )
               CALL ZHER( 'Lower', KM, -ONE, AB( 2, J ), 1,
     $                    AB( 1, J+1 ), KLD )
            END IF
   40    CONTINUE
      END IF
      RETURN
*
   50 CONTINUE
      INFO = J
      RETURN
*
*     End of ZPBSTF
*
      END
