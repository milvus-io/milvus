*> \brief \b CHETF2 computes the factorization of a complex Hermitian matrix, using the diagonal pivoting method (unblocked algorithm calling Level 2 BLAS).
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CHETF2 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/chetf2.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/chetf2.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/chetf2.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CHETF2( UPLO, N, A, LDA, IPIV, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            INFO, LDA, N
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * )
*       COMPLEX            A( LDA, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CHETF2 computes the factorization of a complex Hermitian matrix A
*> using the Bunch-Kaufman diagonal pivoting method:
*>
*>    A = U*D*U**H  or  A = L*D*L**H
*>
*> where U (or L) is a product of permutation and unit upper (lower)
*> triangular matrices, U**H is the conjugate transpose of U, and D is
*> Hermitian and block diagonal with 1-by-1 and 2-by-2 diagonal blocks.
*>
*> This is the unblocked version of the algorithm, calling Level 2 BLAS.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          Specifies whether the upper or lower triangular part of the
*>          Hermitian matrix A is stored:
*>          = 'U':  Upper triangular
*>          = 'L':  Lower triangular
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N)
*>          On entry, the Hermitian matrix A.  If UPLO = 'U', the leading
*>          n-by-n upper triangular part of A contains the upper
*>          triangular part of the matrix A, and the strictly lower
*>          triangular part of A is not referenced.  If UPLO = 'L', the
*>          leading n-by-n lower triangular part of A contains the lower
*>          triangular part of the matrix A, and the strictly upper
*>          triangular part of A is not referenced.
*>
*>          On exit, the block diagonal matrix D and the multipliers used
*>          to obtain the factor U or L (see below for further details).
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[out] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (N)
*>          Details of the interchanges and the block structure of D.
*>
*>          If UPLO = 'U':
*>             If IPIV(k) > 0, then rows and columns k and IPIV(k) were
*>             interchanged and D(k,k) is a 1-by-1 diagonal block.
*>
*>             If IPIV(k) = IPIV(k-1) < 0, then rows and columns
*>             k-1 and -IPIV(k) were interchanged and D(k-1:k,k-1:k)
*>             is a 2-by-2 diagonal block.
*>
*>          If UPLO = 'L':
*>             If IPIV(k) > 0, then rows and columns k and IPIV(k) were
*>             interchanged and D(k,k) is a 1-by-1 diagonal block.
*>
*>             If IPIV(k) = IPIV(k+1) < 0, then rows and columns
*>             k+1 and -IPIV(k) were interchanged and D(k:k+1,k:k+1)
*>             is a 2-by-2 diagonal block.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0: successful exit
*>          < 0: if INFO = -k, the k-th argument had an illegal value
*>          > 0: if INFO = k, D(k,k) is exactly zero.  The factorization
*>               has been completed, but the block diagonal matrix D is
*>               exactly singular, and division by zero will occur if it
*>               is used to solve a system of equations.
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
*> \ingroup complexHEcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  09-29-06 - patch from
*>    Bobby Cheng, MathWorks
*>
*>    Replace l.210 and l.392
*>         IF( MAX( ABSAKK, COLMAX ).EQ.ZERO ) THEN
*>    by
*>         IF( (MAX( ABSAKK, COLMAX ).EQ.ZERO) .OR. SISNAN(ABSAKK) ) THEN
*>
*>  01-01-96 - Based on modifications by
*>    J. Lewis, Boeing Computer Services Company
*>    A. Petitet, Computer Science Dept., Univ. of Tenn., Knoxville, USA
*>
*>  If UPLO = 'U', then A = U*D*U**H, where
*>     U = P(n)*U(n)* ... *P(k)U(k)* ...,
*>  i.e., U is a product of terms P(k)*U(k), where k decreases from n to
*>  1 in steps of 1 or 2, and D is a block diagonal matrix with 1-by-1
*>  and 2-by-2 diagonal blocks D(k).  P(k) is a permutation matrix as
*>  defined by IPIV(k), and U(k) is a unit upper triangular matrix, such
*>  that if the diagonal block D(k) is of order s (s = 1 or 2), then
*>
*>             (   I    v    0   )   k-s
*>     U(k) =  (   0    I    0   )   s
*>             (   0    0    I   )   n-k
*>                k-s   s   n-k
*>
*>  If s = 1, D(k) overwrites A(k,k), and v overwrites A(1:k-1,k).
*>  If s = 2, the upper triangle of D(k) overwrites A(k-1,k-1), A(k-1,k),
*>  and A(k,k), and v overwrites A(1:k-2,k-1:k).
*>
*>  If UPLO = 'L', then A = L*D*L**H, where
*>     L = P(1)*L(1)* ... *P(k)*L(k)* ...,
*>  i.e., L is a product of terms P(k)*L(k), where k increases from 1 to
*>  n in steps of 1 or 2, and D is a block diagonal matrix with 1-by-1
*>  and 2-by-2 diagonal blocks D(k).  P(k) is a permutation matrix as
*>  defined by IPIV(k), and L(k) is a unit lower triangular matrix, such
*>  that if the diagonal block D(k) is of order s (s = 1 or 2), then
*>
*>             (   I    0     0   )  k-1
*>     L(k) =  (   0    I     0   )  s
*>             (   0    v     I   )  n-k-s+1
*>                k-1   s  n-k-s+1
*>
*>  If s = 1, D(k) overwrites A(k,k), and v overwrites A(k+1:n,k).
*>  If s = 2, the lower triangle of D(k) overwrites A(k,k), A(k+1,k),
*>  and A(k+1,k+1), and v overwrites A(k+2:n,k:k+1).
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE CHETF2( UPLO, N, A, LDA, IPIV, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            INFO, LDA, N
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * )
      COMPLEX            A( LDA, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
      REAL               EIGHT, SEVTEN
      PARAMETER          ( EIGHT = 8.0E+0, SEVTEN = 17.0E+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            UPPER
      INTEGER            I, IMAX, J, JMAX, K, KK, KP, KSTEP
      REAL               ABSAKK, ALPHA, COLMAX, D, D11, D22, R1, ROWMAX,
     $                   TT
      COMPLEX            D12, D21, T, WK, WKM1, WKP1, ZDUM
*     ..
*     .. External Functions ..
      LOGICAL            LSAME, SISNAN
      INTEGER            ICAMAX
      REAL               SLAPY2
      EXTERNAL           LSAME, ICAMAX, SLAPY2, SISNAN
*     ..
*     .. External Subroutines ..
      EXTERNAL           CHER, CSSCAL, CSWAP, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, AIMAG, CMPLX, CONJG, MAX, REAL, SQRT
*     ..
*     .. Statement Functions ..
      REAL               CABS1
*     ..
*     .. Statement Function definitions ..
      CABS1( ZDUM ) = ABS( REAL( ZDUM ) ) + ABS( AIMAG( ZDUM ) )
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
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -4
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CHETF2', -INFO )
         RETURN
      END IF
*
*     Initialize ALPHA for use in choosing pivot block size.
*
      ALPHA = ( ONE+SQRT( SEVTEN ) ) / EIGHT
*
      IF( UPPER ) THEN
*
*        Factorize A as U*D*U**H using the upper triangle of A
*
*        K is the main loop index, decreasing from N to 1 in steps of
*        1 or 2
*
         K = N
   10    CONTINUE
*
*        If K < 1, exit from loop
*
         IF( K.LT.1 )
     $      GO TO 90
         KSTEP = 1
*
*        Determine rows and columns to be interchanged and whether
*        a 1-by-1 or 2-by-2 pivot block will be used
*
         ABSAKK = ABS( REAL( A( K, K ) ) )
*
*        IMAX is the row-index of the largest off-diagonal element in
*        column K, and COLMAX is its absolute value.
*        Determine both COLMAX and IMAX.
*
         IF( K.GT.1 ) THEN
            IMAX = ICAMAX( K-1, A( 1, K ), 1 )
            COLMAX = CABS1( A( IMAX, K ) )
         ELSE
            COLMAX = ZERO
         END IF
*
         IF( (MAX( ABSAKK, COLMAX ).EQ.ZERO) .OR. SISNAN(ABSAKK) ) THEN
*
*           Column K is or underflow, or contains a NaN:
*           set INFO and continue
*
            IF( INFO.EQ.0 )
     $         INFO = K
            KP = K
            A( K, K ) = REAL( A( K, K ) )
         ELSE
            IF( ABSAKK.GE.ALPHA*COLMAX ) THEN
*
*              no interchange, use 1-by-1 pivot block
*
               KP = K
            ELSE
*
*              JMAX is the column-index of the largest off-diagonal
*              element in row IMAX, and ROWMAX is its absolute value
*
               JMAX = IMAX + ICAMAX( K-IMAX, A( IMAX, IMAX+1 ), LDA )
               ROWMAX = CABS1( A( IMAX, JMAX ) )
               IF( IMAX.GT.1 ) THEN
                  JMAX = ICAMAX( IMAX-1, A( 1, IMAX ), 1 )
                  ROWMAX = MAX( ROWMAX, CABS1( A( JMAX, IMAX ) ) )
               END IF
*
               IF( ABSAKK.GE.ALPHA*COLMAX*( COLMAX / ROWMAX ) ) THEN
*
*                 no interchange, use 1-by-1 pivot block
*
                  KP = K
               ELSE IF( ABS( REAL( A( IMAX, IMAX ) ) ).GE.ALPHA*ROWMAX )
     $                   THEN
*
*                 interchange rows and columns K and IMAX, use 1-by-1
*                 pivot block
*
                  KP = IMAX
               ELSE
*
*                 interchange rows and columns K-1 and IMAX, use 2-by-2
*                 pivot block
*
                  KP = IMAX
                  KSTEP = 2
               END IF
            END IF
*
            KK = K - KSTEP + 1
            IF( KP.NE.KK ) THEN
*
*              Interchange rows and columns KK and KP in the leading
*              submatrix A(1:k,1:k)
*
               CALL CSWAP( KP-1, A( 1, KK ), 1, A( 1, KP ), 1 )
               DO 20 J = KP + 1, KK - 1
                  T = CONJG( A( J, KK ) )
                  A( J, KK ) = CONJG( A( KP, J ) )
                  A( KP, J ) = T
   20          CONTINUE
               A( KP, KK ) = CONJG( A( KP, KK ) )
               R1 = REAL( A( KK, KK ) )
               A( KK, KK ) = REAL( A( KP, KP ) )
               A( KP, KP ) = R1
               IF( KSTEP.EQ.2 ) THEN
                  A( K, K ) = REAL( A( K, K ) )
                  T = A( K-1, K )
                  A( K-1, K ) = A( KP, K )
                  A( KP, K ) = T
               END IF
            ELSE
               A( K, K ) = REAL( A( K, K ) )
               IF( KSTEP.EQ.2 )
     $            A( K-1, K-1 ) = REAL( A( K-1, K-1 ) )
            END IF
*
*           Update the leading submatrix
*
            IF( KSTEP.EQ.1 ) THEN
*
*              1-by-1 pivot block D(k): column k now holds
*
*              W(k) = U(k)*D(k)
*
*              where U(k) is the k-th column of U
*
*              Perform a rank-1 update of A(1:k-1,1:k-1) as
*
*              A := A - U(k)*D(k)*U(k)**H = A - W(k)*1/D(k)*W(k)**H
*
               R1 = ONE / REAL( A( K, K ) )
               CALL CHER( UPLO, K-1, -R1, A( 1, K ), 1, A, LDA )
*
*              Store U(k) in column k
*
               CALL CSSCAL( K-1, R1, A( 1, K ), 1 )
            ELSE
*
*              2-by-2 pivot block D(k): columns k and k-1 now hold
*
*              ( W(k-1) W(k) ) = ( U(k-1) U(k) )*D(k)
*
*              where U(k) and U(k-1) are the k-th and (k-1)-th columns
*              of U
*
*              Perform a rank-2 update of A(1:k-2,1:k-2) as
*
*              A := A - ( U(k-1) U(k) )*D(k)*( U(k-1) U(k) )**H
*                 = A - ( W(k-1) W(k) )*inv(D(k))*( W(k-1) W(k) )**H
*
               IF( K.GT.2 ) THEN
*
                  D = SLAPY2( REAL( A( K-1, K ) ),
     $                AIMAG( A( K-1, K ) ) )
                  D22 = REAL( A( K-1, K-1 ) ) / D
                  D11 = REAL( A( K, K ) ) / D
                  TT = ONE / ( D11*D22-ONE )
                  D12 = A( K-1, K ) / D
                  D = TT / D
*
                  DO 40 J = K - 2, 1, -1
                     WKM1 = D*( D11*A( J, K-1 )-CONJG( D12 )*A( J, K ) )
                     WK = D*( D22*A( J, K )-D12*A( J, K-1 ) )
                     DO 30 I = J, 1, -1
                        A( I, J ) = A( I, J ) - A( I, K )*CONJG( WK ) -
     $                              A( I, K-1 )*CONJG( WKM1 )
   30                CONTINUE
                     A( J, K ) = WK
                     A( J, K-1 ) = WKM1
                     A( J, J ) = CMPLX( REAL( A( J, J ) ), 0.0E+0 )
   40             CONTINUE
*
               END IF
*
            END IF
         END IF
*
*        Store details of the interchanges in IPIV
*
         IF( KSTEP.EQ.1 ) THEN
            IPIV( K ) = KP
         ELSE
            IPIV( K ) = -KP
            IPIV( K-1 ) = -KP
         END IF
*
*        Decrease K and return to the start of the main loop
*
         K = K - KSTEP
         GO TO 10
*
      ELSE
*
*        Factorize A as L*D*L**H using the lower triangle of A
*
*        K is the main loop index, increasing from 1 to N in steps of
*        1 or 2
*
         K = 1
   50    CONTINUE
*
*        If K > N, exit from loop
*
         IF( K.GT.N )
     $      GO TO 90
         KSTEP = 1
*
*        Determine rows and columns to be interchanged and whether
*        a 1-by-1 or 2-by-2 pivot block will be used
*
         ABSAKK = ABS( REAL( A( K, K ) ) )
*
*        IMAX is the row-index of the largest off-diagonal element in
*        column K, and COLMAX is its absolute value.
*        Determine both COLMAX and IMAX.
*
         IF( K.LT.N ) THEN
            IMAX = K + ICAMAX( N-K, A( K+1, K ), 1 )
            COLMAX = CABS1( A( IMAX, K ) )
         ELSE
            COLMAX = ZERO
         END IF
*
         IF( (MAX( ABSAKK, COLMAX ).EQ.ZERO) .OR. SISNAN(ABSAKK) ) THEN
*
*           Column K is zero or underflow, contains a NaN:
*           set INFO and continue
*
            IF( INFO.EQ.0 )
     $         INFO = K
            KP = K
            A( K, K ) = REAL( A( K, K ) )
         ELSE
            IF( ABSAKK.GE.ALPHA*COLMAX ) THEN
*
*              no interchange, use 1-by-1 pivot block
*
               KP = K
            ELSE
*
*              JMAX is the column-index of the largest off-diagonal
*              element in row IMAX, and ROWMAX is its absolute value
*
               JMAX = K - 1 + ICAMAX( IMAX-K, A( IMAX, K ), LDA )
               ROWMAX = CABS1( A( IMAX, JMAX ) )
               IF( IMAX.LT.N ) THEN
                  JMAX = IMAX + ICAMAX( N-IMAX, A( IMAX+1, IMAX ), 1 )
                  ROWMAX = MAX( ROWMAX, CABS1( A( JMAX, IMAX ) ) )
               END IF
*
               IF( ABSAKK.GE.ALPHA*COLMAX*( COLMAX / ROWMAX ) ) THEN
*
*                 no interchange, use 1-by-1 pivot block
*
                  KP = K
               ELSE IF( ABS( REAL( A( IMAX, IMAX ) ) ).GE.ALPHA*ROWMAX )
     $                   THEN
*
*                 interchange rows and columns K and IMAX, use 1-by-1
*                 pivot block
*
                  KP = IMAX
               ELSE
*
*                 interchange rows and columns K+1 and IMAX, use 2-by-2
*                 pivot block
*
                  KP = IMAX
                  KSTEP = 2
               END IF
            END IF
*
            KK = K + KSTEP - 1
            IF( KP.NE.KK ) THEN
*
*              Interchange rows and columns KK and KP in the trailing
*              submatrix A(k:n,k:n)
*
               IF( KP.LT.N )
     $            CALL CSWAP( N-KP, A( KP+1, KK ), 1, A( KP+1, KP ), 1 )
               DO 60 J = KK + 1, KP - 1
                  T = CONJG( A( J, KK ) )
                  A( J, KK ) = CONJG( A( KP, J ) )
                  A( KP, J ) = T
   60          CONTINUE
               A( KP, KK ) = CONJG( A( KP, KK ) )
               R1 = REAL( A( KK, KK ) )
               A( KK, KK ) = REAL( A( KP, KP ) )
               A( KP, KP ) = R1
               IF( KSTEP.EQ.2 ) THEN
                  A( K, K ) = REAL( A( K, K ) )
                  T = A( K+1, K )
                  A( K+1, K ) = A( KP, K )
                  A( KP, K ) = T
               END IF
            ELSE
               A( K, K ) = REAL( A( K, K ) )
               IF( KSTEP.EQ.2 )
     $            A( K+1, K+1 ) = REAL( A( K+1, K+1 ) )
            END IF
*
*           Update the trailing submatrix
*
            IF( KSTEP.EQ.1 ) THEN
*
*              1-by-1 pivot block D(k): column k now holds
*
*              W(k) = L(k)*D(k)
*
*              where L(k) is the k-th column of L
*
               IF( K.LT.N ) THEN
*
*                 Perform a rank-1 update of A(k+1:n,k+1:n) as
*
*                 A := A - L(k)*D(k)*L(k)**H = A - W(k)*(1/D(k))*W(k)**H
*
                  R1 = ONE / REAL( A( K, K ) )
                  CALL CHER( UPLO, N-K, -R1, A( K+1, K ), 1,
     $                       A( K+1, K+1 ), LDA )
*
*                 Store L(k) in column K
*
                  CALL CSSCAL( N-K, R1, A( K+1, K ), 1 )
               END IF
            ELSE
*
*              2-by-2 pivot block D(k)
*
               IF( K.LT.N-1 ) THEN
*
*                 Perform a rank-2 update of A(k+2:n,k+2:n) as
*
*                 A := A - ( L(k) L(k+1) )*D(k)*( L(k) L(k+1) )**H
*                    = A - ( W(k) W(k+1) )*inv(D(k))*( W(k) W(k+1) )**H
*
*                 where L(k) and L(k+1) are the k-th and (k+1)-th
*                 columns of L
*
                  D = SLAPY2( REAL( A( K+1, K ) ),
     $                        AIMAG( A( K+1, K ) ) )
                  D11 = REAL( A( K+1, K+1 ) ) / D
                  D22 = REAL( A( K, K ) ) / D
                  TT = ONE / ( D11*D22-ONE )
                  D21 = A( K+1, K ) / D
                  D =  TT / D
*
                  DO 80 J = K + 2, N
                     WK = D*( D11*A( J, K )-D21*A( J, K+1 ) )
                     WKP1 = D*( D22*A( J, K+1 )-CONJG( D21 )*A( J, K ) )
                     DO 70 I = J, N
                        A( I, J ) = A( I, J ) - A( I, K )*CONJG( WK ) -
     $                              A( I, K+1 )*CONJG( WKP1 )
   70                CONTINUE
                     A( J, K ) = WK
                     A( J, K+1 ) = WKP1
                     A( J, J ) = CMPLX( REAL( A( J, J ) ), 0.0E+0 )
   80             CONTINUE
               END IF
            END IF
         END IF
*
*        Store details of the interchanges in IPIV
*
         IF( KSTEP.EQ.1 ) THEN
            IPIV( K ) = KP
         ELSE
            IPIV( K ) = -KP
            IPIV( K+1 ) = -KP
         END IF
*
*        Increase K and return to the start of the main loop
*
         K = K + KSTEP
         GO TO 50
*
      END IF
*
   90 CONTINUE
      RETURN
*
*     End of CHETF2
*
      END
