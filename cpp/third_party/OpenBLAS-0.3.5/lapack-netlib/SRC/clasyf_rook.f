*> \brief \b CLASYF_ROOK computes a partial factorization of a complex symmetric matrix using the bounded Bunch-Kaufman ("rook") diagonal pivoting method.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CLASYF_ROOK + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/clasyf_rook.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/clasyf_rook.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/clasyf_rook.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CLASYF_ROOK( UPLO, N, NB, KB, A, LDA, IPIV, W, LDW, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            INFO, KB, LDA, LDW, N, NB
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * )
*       COMPLEX            A( LDA, * ), W( LDW, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CLASYF_ROOK computes a partial factorization of a complex symmetric
*> matrix A using the bounded Bunch-Kaufman ("rook") diagonal
*> pivoting method. The partial factorization has the form:
*>
*> A  =  ( I  U12 ) ( A11  0  ) (  I       0    )  if UPLO = 'U', or:
*>       ( 0  U22 ) (  0   D  ) ( U12**T U22**T )
*>
*> A  =  ( L11  0 ) (  D   0  ) ( L11**T L21**T )  if UPLO = 'L'
*>       ( L21  I ) (  0  A22 ) (  0       I    )
*>
*> where the order of D is at most NB. The actual order is returned in
*> the argument KB, and is either NB or NB-1, or N if N <= NB.
*>
*> CLASYF_ROOK is an auxiliary routine called by CSYTRF_ROOK. It uses
*> blocked code (calling Level 3 BLAS) to update the submatrix
*> A11 (if UPLO = 'U') or A22 (if UPLO = 'L').
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>          Specifies whether the upper or lower triangular part of the
*>          symmetric matrix A is stored:
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
*> \param[in] NB
*> \verbatim
*>          NB is INTEGER
*>          The maximum number of columns of the matrix A that should be
*>          factored.  NB should be at least 2 to allow for 2-by-2 pivot
*>          blocks.
*> \endverbatim
*>
*> \param[out] KB
*> \verbatim
*>          KB is INTEGER
*>          The number of columns of A that were actually factored.
*>          KB is either NB-1 or NB, or N if N <= NB.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N)
*>          On entry, the symmetric matrix A.  If UPLO = 'U', the leading
*>          n-by-n upper triangular part of A contains the upper
*>          triangular part of the matrix A, and the strictly lower
*>          triangular part of A is not referenced.  If UPLO = 'L', the
*>          leading n-by-n lower triangular part of A contains the lower
*>          triangular part of the matrix A, and the strictly upper
*>          triangular part of A is not referenced.
*>          On exit, A contains details of the partial factorization.
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
*>             Only the last KB elements of IPIV are set.
*>
*>             If IPIV(k) > 0, then rows and columns k and IPIV(k) were
*>             interchanged and D(k,k) is a 1-by-1 diagonal block.
*>
*>             If IPIV(k) < 0 and IPIV(k-1) < 0, then rows and
*>             columns k and -IPIV(k) were interchanged and rows and
*>             columns k-1 and -IPIV(k-1) were inerchaged,
*>             D(k-1:k,k-1:k) is a 2-by-2 diagonal block.
*>
*>          If UPLO = 'L':
*>             Only the first KB elements of IPIV are set.
*>
*>             If IPIV(k) > 0, then rows and columns k and IPIV(k)
*>             were interchanged and D(k,k) is a 1-by-1 diagonal block.
*>
*>             If IPIV(k) < 0 and IPIV(k+1) < 0, then rows and
*>             columns k and -IPIV(k) were interchanged and rows and
*>             columns k+1 and -IPIV(k+1) were inerchaged,
*>             D(k:k+1,k:k+1) is a 2-by-2 diagonal block.
*> \endverbatim
*>
*> \param[out] W
*> \verbatim
*>          W is COMPLEX array, dimension (LDW,NB)
*> \endverbatim
*>
*> \param[in] LDW
*> \verbatim
*>          LDW is INTEGER
*>          The leading dimension of the array W.  LDW >= max(1,N).
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0: successful exit
*>          > 0: if INFO = k, D(k,k) is exactly zero.  The factorization
*>               has been completed, but the block diagonal matrix D is
*>               exactly singular.
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
*> \date November 2013
*
*> \ingroup complexSYcomputational
*
*> \par Contributors:
*  ==================
*>
*> \verbatim
*>
*>  November 2013,     Igor Kozachenko,
*>                  Computer Science Division,
*>                  University of California, Berkeley
*>
*>  September 2007, Sven Hammarling, Nicholas J. Higham, Craig Lucas,
*>                  School of Mathematics,
*>                  University of Manchester
*>
*> \endverbatim
*
*  =====================================================================
      SUBROUTINE CLASYF_ROOK( UPLO, N, NB, KB, A, LDA, IPIV, W, LDW,
     $                        INFO )
*
*  -- LAPACK computational routine (version 3.5.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2013
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            INFO, KB, LDA, LDW, N, NB
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * )
      COMPLEX            A( LDA, * ), W( LDW, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
      REAL               EIGHT, SEVTEN
      PARAMETER          ( EIGHT = 8.0E+0, SEVTEN = 17.0E+0 )
      COMPLEX            CONE, CZERO
      PARAMETER          ( CONE = ( 1.0E+0, 0.0E+0 ),
     $                   CZERO = ( 0.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            DONE
      INTEGER            IMAX, ITEMP, J, JB, JJ, JMAX, JP1, JP2, K, KK,
     $                   KW, KKW, KP, KSTEP, P, II
      REAL               ABSAKK, ALPHA, COLMAX, ROWMAX, STEMP, SFMIN
      COMPLEX            D11, D12, D21, D22, R1, T, Z
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ICAMAX
      REAL               SLAMCH
      EXTERNAL           LSAME, ICAMAX, SLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           CCOPY, CGEMM, CGEMV, CSCAL, CSWAP
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, MIN, SQRT, AIMAG, REAL
*     ..
*     .. Statement Functions ..
      REAL               CABS1
*     ..
*     .. Statement Function definitions ..
      CABS1( Z ) = ABS( REAL( Z ) ) + ABS( AIMAG( Z ) )
*     ..
*     .. Executable Statements ..
*
      INFO = 0
*
*     Initialize ALPHA for use in choosing pivot block size.
*
      ALPHA = ( ONE+SQRT( SEVTEN ) ) / EIGHT
*
*     Compute machine safe minimum
*
      SFMIN = SLAMCH( 'S' )
*
      IF( LSAME( UPLO, 'U' ) ) THEN
*
*        Factorize the trailing columns of A using the upper triangle
*        of A and working backwards, and compute the matrix W = U12*D
*        for use in updating A11
*
*        K is the main loop index, decreasing from N in steps of 1 or 2
*
         K = N
   10    CONTINUE
*
*        KW is the column of W which corresponds to column K of A
*
         KW = NB + K - N
*
*        Exit from loop
*
         IF( ( K.LE.N-NB+1 .AND. NB.LT.N ) .OR. K.LT.1 )
     $      GO TO 30
*
         KSTEP = 1
         P = K
*
*        Copy column K of A to column KW of W and update it
*
         CALL CCOPY( K, A( 1, K ), 1, W( 1, KW ), 1 )
         IF( K.LT.N )
     $      CALL CGEMV( 'No transpose', K, N-K, -CONE, A( 1, K+1 ),
     $                  LDA, W( K, KW+1 ), LDW, CONE, W( 1, KW ), 1 )
*
*        Determine rows and columns to be interchanged and whether
*        a 1-by-1 or 2-by-2 pivot block will be used
*
         ABSAKK = CABS1( W( K, KW ) )
*
*        IMAX is the row-index of the largest off-diagonal element in
*        column K, and COLMAX is its absolute value.
*        Determine both COLMAX and IMAX.
*
         IF( K.GT.1 ) THEN
            IMAX = ICAMAX( K-1, W( 1, KW ), 1 )
            COLMAX = CABS1( W( IMAX, KW ) )
         ELSE
            COLMAX = ZERO
         END IF
*
         IF( MAX( ABSAKK, COLMAX ).EQ.ZERO ) THEN
*
*           Column K is zero or underflow: set INFO and continue
*
            IF( INFO.EQ.0 )
     $         INFO = K
            KP = K
            CALL CCOPY( K, W( 1, KW ), 1, A( 1, K ), 1 )
         ELSE
*
*           ============================================================
*
*           Test for interchange
*
*           Equivalent to testing for ABSAKK.GE.ALPHA*COLMAX
*           (used to handle NaN and Inf)
*
            IF( .NOT.( ABSAKK.LT.ALPHA*COLMAX ) ) THEN
*
*              no interchange, use 1-by-1 pivot block
*
               KP = K
*
            ELSE
*
               DONE = .FALSE.
*
*              Loop until pivot found
*
   12          CONTINUE
*
*                 Begin pivot search loop body
*
*
*                 Copy column IMAX to column KW-1 of W and update it
*
                  CALL CCOPY( IMAX, A( 1, IMAX ), 1, W( 1, KW-1 ), 1 )
                  CALL CCOPY( K-IMAX, A( IMAX, IMAX+1 ), LDA,
     $                        W( IMAX+1, KW-1 ), 1 )
*
                  IF( K.LT.N )
     $               CALL CGEMV( 'No transpose', K, N-K, -CONE,
     $                           A( 1, K+1 ), LDA, W( IMAX, KW+1 ), LDW,
     $                           CONE, W( 1, KW-1 ), 1 )
*
*                 JMAX is the column-index of the largest off-diagonal
*                 element in row IMAX, and ROWMAX is its absolute value.
*                 Determine both ROWMAX and JMAX.
*
                  IF( IMAX.NE.K ) THEN
                     JMAX = IMAX + ICAMAX( K-IMAX, W( IMAX+1, KW-1 ),
     $                                     1 )
                     ROWMAX = CABS1( W( JMAX, KW-1 ) )
                  ELSE
                     ROWMAX = ZERO
                  END IF
*
                  IF( IMAX.GT.1 ) THEN
                     ITEMP = ICAMAX( IMAX-1, W( 1, KW-1 ), 1 )
                     STEMP = CABS1( W( ITEMP, KW-1 ) )
                     IF( STEMP.GT.ROWMAX ) THEN
                        ROWMAX = STEMP
                        JMAX = ITEMP
                     END IF
                  END IF
*
*                 Equivalent to testing for
*                 CABS1( W( IMAX, KW-1 ) ).GE.ALPHA*ROWMAX
*                 (used to handle NaN and Inf)
*
                  IF( .NOT.(CABS1( W( IMAX, KW-1 ) ).LT.ALPHA*ROWMAX ) )
     $            THEN
*
*                    interchange rows and columns K and IMAX,
*                    use 1-by-1 pivot block
*
                     KP = IMAX
*
*                    copy column KW-1 of W to column KW of W
*
                     CALL CCOPY( K, W( 1, KW-1 ), 1, W( 1, KW ), 1 )
*
                     DONE = .TRUE.
*
*                 Equivalent to testing for ROWMAX.EQ.COLMAX,
*                 (used to handle NaN and Inf)
*
                  ELSE IF( ( P.EQ.JMAX ) .OR. ( ROWMAX.LE.COLMAX ) )
     $            THEN
*
*                    interchange rows and columns K-1 and IMAX,
*                    use 2-by-2 pivot block
*
                     KP = IMAX
                     KSTEP = 2
                     DONE = .TRUE.
                  ELSE
*
*                    Pivot not found: set params and repeat
*
                     P = IMAX
                     COLMAX = ROWMAX
                     IMAX = JMAX
*
*                    Copy updated JMAXth (next IMAXth) column to Kth of W
*
                     CALL CCOPY( K, W( 1, KW-1 ), 1, W( 1, KW ), 1 )
*
                  END IF
*
*                 End pivot search loop body
*
               IF( .NOT. DONE ) GOTO 12
*
            END IF
*
*           ============================================================
*
            KK = K - KSTEP + 1
*
*           KKW is the column of W which corresponds to column KK of A
*
            KKW = NB + KK - N
*
            IF( ( KSTEP.EQ.2 ) .AND. ( P.NE.K ) ) THEN
*
*              Copy non-updated column K to column P
*
               CALL CCOPY( K-P, A( P+1, K ), 1, A( P, P+1 ), LDA )
               CALL CCOPY( P, A( 1, K ), 1, A( 1, P ), 1 )
*
*              Interchange rows K and P in last N-K+1 columns of A
*              and last N-K+2 columns of W
*
               CALL CSWAP( N-K+1, A( K, K ), LDA, A( P, K ), LDA )
               CALL CSWAP( N-KK+1, W( K, KKW ), LDW, W( P, KKW ), LDW )
            END IF
*
*           Updated column KP is already stored in column KKW of W
*
            IF( KP.NE.KK ) THEN
*
*              Copy non-updated column KK to column KP
*
               A( KP, K ) = A( KK, K )
               CALL CCOPY( K-1-KP, A( KP+1, KK ), 1, A( KP, KP+1 ),
     $                     LDA )
               CALL CCOPY( KP, A( 1, KK ), 1, A( 1, KP ), 1 )
*
*              Interchange rows KK and KP in last N-KK+1 columns
*              of A and W
*
               CALL CSWAP( N-KK+1, A( KK, KK ), LDA, A( KP, KK ), LDA )
               CALL CSWAP( N-KK+1, W( KK, KKW ), LDW, W( KP, KKW ),
     $                     LDW )
            END IF
*
            IF( KSTEP.EQ.1 ) THEN
*
*              1-by-1 pivot block D(k): column KW of W now holds
*
*              W(k) = U(k)*D(k)
*
*              where U(k) is the k-th column of U
*
*              Store U(k) in column k of A
*
               CALL CCOPY( K, W( 1, KW ), 1, A( 1, K ), 1 )
               IF( K.GT.1 ) THEN
                  IF( CABS1( A( K, K ) ).GE.SFMIN ) THEN
                     R1 = CONE / A( K, K )
                     CALL CSCAL( K-1, R1, A( 1, K ), 1 )
                  ELSE IF( A( K, K ).NE.CZERO ) THEN
                     DO 14 II = 1, K - 1
                        A( II, K ) = A( II, K ) / A( K, K )
   14                CONTINUE
                  END IF
               END IF
*
            ELSE
*
*              2-by-2 pivot block D(k): columns KW and KW-1 of W now
*              hold
*
*              ( W(k-1) W(k) ) = ( U(k-1) U(k) )*D(k)
*
*              where U(k) and U(k-1) are the k-th and (k-1)-th columns
*              of U
*
               IF( K.GT.2 ) THEN
*
*                 Store U(k) and U(k-1) in columns k and k-1 of A
*
                  D12 = W( K-1, KW )
                  D11 = W( K, KW ) / D12
                  D22 = W( K-1, KW-1 ) / D12
                  T = CONE / ( D11*D22-CONE )
                  DO 20 J = 1, K - 2
                     A( J, K-1 ) = T*( (D11*W( J, KW-1 )-W( J, KW ) ) /
     $                             D12 )
                     A( J, K ) = T*( ( D22*W( J, KW )-W( J, KW-1 ) ) /
     $                           D12 )
   20             CONTINUE
               END IF
*
*              Copy D(k) to A
*
               A( K-1, K-1 ) = W( K-1, KW-1 )
               A( K-1, K ) = W( K-1, KW )
               A( K, K ) = W( K, KW )
            END IF
         END IF
*
*        Store details of the interchanges in IPIV
*
         IF( KSTEP.EQ.1 ) THEN
            IPIV( K ) = KP
         ELSE
            IPIV( K ) = -P
            IPIV( K-1 ) = -KP
         END IF
*
*        Decrease K and return to the start of the main loop
*
         K = K - KSTEP
         GO TO 10
*
   30    CONTINUE
*
*        Update the upper triangle of A11 (= A(1:k,1:k)) as
*
*        A11 := A11 - U12*D*U12**T = A11 - U12*W**T
*
*        computing blocks of NB columns at a time
*
         DO 50 J = ( ( K-1 ) / NB )*NB + 1, 1, -NB
            JB = MIN( NB, K-J+1 )
*
*           Update the upper triangle of the diagonal block
*
            DO 40 JJ = J, J + JB - 1
               CALL CGEMV( 'No transpose', JJ-J+1, N-K, -CONE,
     $                     A( J, K+1 ), LDA, W( JJ, KW+1 ), LDW, CONE,
     $                     A( J, JJ ), 1 )
   40       CONTINUE
*
*           Update the rectangular superdiagonal block
*
            IF( J.GE.2 )
     $         CALL CGEMM( 'No transpose', 'Transpose', J-1, JB,
     $                  N-K, -CONE, A( 1, K+1 ), LDA, W( J, KW+1 ), LDW,
     $                  CONE, A( 1, J ), LDA )
   50    CONTINUE
*
*        Put U12 in standard form by partially undoing the interchanges
*        in columns k+1:n
*
         J = K + 1
   60    CONTINUE
*
            KSTEP = 1
            JP1 = 1
            JJ = J
            JP2 = IPIV( J )
            IF( JP2.LT.0 ) THEN
               JP2 = -JP2
               J = J + 1
               JP1 = -IPIV( J )
               KSTEP = 2
            END IF
*
            J = J + 1
            IF( JP2.NE.JJ .AND. J.LE.N )
     $         CALL CSWAP( N-J+1, A( JP2, J ), LDA, A( JJ, J ), LDA )
            JJ = J - 1
            IF( JP1.NE.JJ .AND. KSTEP.EQ.2 )
     $         CALL CSWAP( N-J+1, A( JP1, J ), LDA, A( JJ, J ), LDA )
         IF( J.LE.N )
     $      GO TO 60
*
*        Set KB to the number of columns factorized
*
         KB = N - K
*
      ELSE
*
*        Factorize the leading columns of A using the lower triangle
*        of A and working forwards, and compute the matrix W = L21*D
*        for use in updating A22
*
*        K is the main loop index, increasing from 1 in steps of 1 or 2
*
         K = 1
   70   CONTINUE
*
*        Exit from loop
*
         IF( ( K.GE.NB .AND. NB.LT.N ) .OR. K.GT.N )
     $      GO TO 90
*
         KSTEP = 1
         P = K
*
*        Copy column K of A to column K of W and update it
*
         CALL CCOPY( N-K+1, A( K, K ), 1, W( K, K ), 1 )
         IF( K.GT.1 )
     $      CALL CGEMV( 'No transpose', N-K+1, K-1, -CONE, A( K, 1 ),
     $                  LDA, W( K, 1 ), LDW, CONE, W( K, K ), 1 )
*
*        Determine rows and columns to be interchanged and whether
*        a 1-by-1 or 2-by-2 pivot block will be used
*
         ABSAKK = CABS1( W( K, K ) )
*
*        IMAX is the row-index of the largest off-diagonal element in
*        column K, and COLMAX is its absolute value.
*        Determine both COLMAX and IMAX.
*
         IF( K.LT.N ) THEN
            IMAX = K + ICAMAX( N-K, W( K+1, K ), 1 )
            COLMAX = CABS1( W( IMAX, K ) )
         ELSE
            COLMAX = ZERO
         END IF
*
         IF( MAX( ABSAKK, COLMAX ).EQ.ZERO ) THEN
*
*           Column K is zero or underflow: set INFO and continue
*
            IF( INFO.EQ.0 )
     $         INFO = K
            KP = K
            CALL CCOPY( N-K+1, W( K, K ), 1, A( K, K ), 1 )
         ELSE
*
*           ============================================================
*
*           Test for interchange
*
*           Equivalent to testing for ABSAKK.GE.ALPHA*COLMAX
*           (used to handle NaN and Inf)
*
            IF( .NOT.( ABSAKK.LT.ALPHA*COLMAX ) ) THEN
*
*              no interchange, use 1-by-1 pivot block
*
               KP = K
*
            ELSE
*
               DONE = .FALSE.
*
*              Loop until pivot found
*
   72          CONTINUE
*
*                 Begin pivot search loop body
*
*
*                 Copy column IMAX to column K+1 of W and update it
*
                  CALL CCOPY( IMAX-K, A( IMAX, K ), LDA, W( K, K+1 ), 1)
                  CALL CCOPY( N-IMAX+1, A( IMAX, IMAX ), 1,
     $                        W( IMAX, K+1 ), 1 )
                  IF( K.GT.1 )
     $               CALL CGEMV( 'No transpose', N-K+1, K-1, -CONE,
     $                           A( K, 1 ), LDA, W( IMAX, 1 ), LDW,
     $                           CONE, W( K, K+1 ), 1 )
*
*                 JMAX is the column-index of the largest off-diagonal
*                 element in row IMAX, and ROWMAX is its absolute value.
*                 Determine both ROWMAX and JMAX.
*
                  IF( IMAX.NE.K ) THEN
                     JMAX = K - 1 + ICAMAX( IMAX-K, W( K, K+1 ), 1 )
                     ROWMAX = CABS1( W( JMAX, K+1 ) )
                  ELSE
                     ROWMAX = ZERO
                  END IF
*
                  IF( IMAX.LT.N ) THEN
                     ITEMP = IMAX + ICAMAX( N-IMAX, W( IMAX+1, K+1 ), 1)
                     STEMP = CABS1( W( ITEMP, K+1 ) )
                     IF( STEMP.GT.ROWMAX ) THEN
                        ROWMAX = STEMP
                        JMAX = ITEMP
                     END IF
                  END IF
*
*                 Equivalent to testing for
*                 CABS1( W( IMAX, K+1 ) ).GE.ALPHA*ROWMAX
*                 (used to handle NaN and Inf)
*
                  IF( .NOT.( CABS1( W( IMAX, K+1 ) ).LT.ALPHA*ROWMAX ) )
     $            THEN
*
*                    interchange rows and columns K and IMAX,
*                    use 1-by-1 pivot block
*
                     KP = IMAX
*
*                    copy column K+1 of W to column K of W
*
                     CALL CCOPY( N-K+1, W( K, K+1 ), 1, W( K, K ), 1 )
*
                     DONE = .TRUE.
*
*                 Equivalent to testing for ROWMAX.EQ.COLMAX,
*                 (used to handle NaN and Inf)
*
                  ELSE IF( ( P.EQ.JMAX ) .OR. ( ROWMAX.LE.COLMAX ) )
     $            THEN
*
*                    interchange rows and columns K+1 and IMAX,
*                    use 2-by-2 pivot block
*
                     KP = IMAX
                     KSTEP = 2
                     DONE = .TRUE.
                  ELSE
*
*                    Pivot not found: set params and repeat
*
                     P = IMAX
                     COLMAX = ROWMAX
                     IMAX = JMAX
*
*                    Copy updated JMAXth (next IMAXth) column to Kth of W
*
                     CALL CCOPY( N-K+1, W( K, K+1 ), 1, W( K, K ), 1 )
*
                  END IF
*
*                 End pivot search loop body
*
               IF( .NOT. DONE ) GOTO 72
*
            END IF
*
*           ============================================================
*
            KK = K + KSTEP - 1
*
            IF( ( KSTEP.EQ.2 ) .AND. ( P.NE.K ) ) THEN
*
*              Copy non-updated column K to column P
*
               CALL CCOPY( P-K, A( K, K ), 1, A( P, K ), LDA )
               CALL CCOPY( N-P+1, A( P, K ), 1, A( P, P ), 1 )
*
*              Interchange rows K and P in first K columns of A
*              and first K+1 columns of W
*
               CALL CSWAP( K, A( K, 1 ), LDA, A( P, 1 ), LDA )
               CALL CSWAP( KK, W( K, 1 ), LDW, W( P, 1 ), LDW )
            END IF
*
*           Updated column KP is already stored in column KK of W
*
            IF( KP.NE.KK ) THEN
*
*              Copy non-updated column KK to column KP
*
               A( KP, K ) = A( KK, K )
               CALL CCOPY( KP-K-1, A( K+1, KK ), 1, A( KP, K+1 ), LDA )
               CALL CCOPY( N-KP+1, A( KP, KK ), 1, A( KP, KP ), 1 )
*
*              Interchange rows KK and KP in first KK columns of A and W
*
               CALL CSWAP( KK, A( KK, 1 ), LDA, A( KP, 1 ), LDA )
               CALL CSWAP( KK, W( KK, 1 ), LDW, W( KP, 1 ), LDW )
            END IF
*
            IF( KSTEP.EQ.1 ) THEN
*
*              1-by-1 pivot block D(k): column k of W now holds
*
*              W(k) = L(k)*D(k)
*
*              where L(k) is the k-th column of L
*
*              Store L(k) in column k of A
*
               CALL CCOPY( N-K+1, W( K, K ), 1, A( K, K ), 1 )
               IF( K.LT.N ) THEN
                  IF( CABS1( A( K, K ) ).GE.SFMIN ) THEN
                     R1 = CONE / A( K, K )
                     CALL CSCAL( N-K, R1, A( K+1, K ), 1 )
                  ELSE IF( A( K, K ).NE.CZERO ) THEN
                     DO 74 II = K + 1, N
                        A( II, K ) = A( II, K ) / A( K, K )
   74                CONTINUE
                  END IF
               END IF
*
            ELSE
*
*              2-by-2 pivot block D(k): columns k and k+1 of W now hold
*
*              ( W(k) W(k+1) ) = ( L(k) L(k+1) )*D(k)
*
*              where L(k) and L(k+1) are the k-th and (k+1)-th columns
*              of L
*
               IF( K.LT.N-1 ) THEN
*
*                 Store L(k) and L(k+1) in columns k and k+1 of A
*
                  D21 = W( K+1, K )
                  D11 = W( K+1, K+1 ) / D21
                  D22 = W( K, K ) / D21
                  T = CONE / ( D11*D22-CONE )
                  DO 80 J = K + 2, N
                     A( J, K ) = T*( ( D11*W( J, K )-W( J, K+1 ) ) /
     $                           D21 )
                     A( J, K+1 ) = T*( ( D22*W( J, K+1 )-W( J, K ) ) /
     $                             D21 )
   80             CONTINUE
               END IF
*
*              Copy D(k) to A
*
               A( K, K ) = W( K, K )
               A( K+1, K ) = W( K+1, K )
               A( K+1, K+1 ) = W( K+1, K+1 )
            END IF
         END IF
*
*        Store details of the interchanges in IPIV
*
         IF( KSTEP.EQ.1 ) THEN
            IPIV( K ) = KP
         ELSE
            IPIV( K ) = -P
            IPIV( K+1 ) = -KP
         END IF
*
*        Increase K and return to the start of the main loop
*
         K = K + KSTEP
         GO TO 70
*
   90    CONTINUE
*
*        Update the lower triangle of A22 (= A(k:n,k:n)) as
*
*        A22 := A22 - L21*D*L21**T = A22 - L21*W**T
*
*        computing blocks of NB columns at a time
*
         DO 110 J = K, N, NB
            JB = MIN( NB, N-J+1 )
*
*           Update the lower triangle of the diagonal block
*
            DO 100 JJ = J, J + JB - 1
               CALL CGEMV( 'No transpose', J+JB-JJ, K-1, -CONE,
     $                     A( JJ, 1 ), LDA, W( JJ, 1 ), LDW, CONE,
     $                     A( JJ, JJ ), 1 )
  100       CONTINUE
*
*           Update the rectangular subdiagonal block
*
            IF( J+JB.LE.N )
     $         CALL CGEMM( 'No transpose', 'Transpose', N-J-JB+1, JB,
     $                    K-1, -CONE, A( J+JB, 1 ), LDA, W( J, 1 ), LDW,
     $                    CONE, A( J+JB, J ), LDA )
  110    CONTINUE
*
*        Put L21 in standard form by partially undoing the interchanges
*        in columns 1:k-1
*
         J = K - 1
  120    CONTINUE
*
            KSTEP = 1
            JP1 = 1
            JJ = J
            JP2 = IPIV( J )
            IF( JP2.LT.0 ) THEN
               JP2 = -JP2
               J = J - 1
               JP1 = -IPIV( J )
               KSTEP = 2
            END IF
*
            J = J - 1
            IF( JP2.NE.JJ .AND. J.GE.1 )
     $         CALL CSWAP( J, A( JP2, 1 ), LDA, A( JJ, 1 ), LDA )
            JJ = J + 1
            IF( JP1.NE.JJ .AND. KSTEP.EQ.2 )
     $         CALL CSWAP( J, A( JP1, 1 ), LDA, A( JJ, 1 ), LDA )
         IF( J.GE.1 )
     $      GO TO 120
*
*        Set KB to the number of columns factorized
*
         KB = K - 1
*
      END IF
      RETURN
*
*     End of CLASYF_ROOK
*
      END
