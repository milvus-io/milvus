*> \brief \b SLASYF_AA
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLASYF_AA + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slasyf_aa.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slasyf_aa.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slasyf_aa.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLASYF_AA( UPLO, J1, M, NB, A, LDA, IPIV,
*                             H, LDH, WORK )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            J1, M, NB, LDA, LDH
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * )
*       REAL               A( LDA, * ), H( LDH, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DLATRF_AA factorizes a panel of a real symmetric matrix A using
*> the Aasen's algorithm. The panel consists of a set of NB rows of A
*> when UPLO is U, or a set of NB columns when UPLO is L.
*>
*> In order to factorize the panel, the Aasen's algorithm requires the
*> last row, or column, of the previous panel. The first row, or column,
*> of A is set to be the first row, or column, of an identity matrix,
*> which is used to factorize the first panel.
*>
*> The resulting J-th row of U, or J-th column of L, is stored in the
*> (J-1)-th row, or column, of A (without the unit diagonals), while
*> the diagonal and subdiagonal of A are overwritten by those of T.
*>
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
*> \param[in] J1
*> \verbatim
*>          J1 is INTEGER
*>          The location of the first row, or column, of the panel
*>          within the submatrix of A, passed to this routine, e.g.,
*>          when called by SSYTRF_AA, for the first panel, J1 is 1,
*>          while for the remaining panels, J1 is 2.
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The dimension of the submatrix. M >= 0.
*> \endverbatim
*>
*> \param[in] NB
*> \verbatim
*>          NB is INTEGER
*>          The dimension of the panel to be facotorized.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is REAL array, dimension (LDA,M) for
*>          the first panel, while dimension (LDA,M+1) for the
*>          remaining panels.
*>
*>          On entry, A contains the last row, or column, of
*>          the previous panel, and the trailing submatrix of A
*>          to be factorized, except for the first panel, only
*>          the panel is passed.
*>
*>          On exit, the leading panel is factorized.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,M).
*> \endverbatim
*>
*> \param[out] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (M)
*>          Details of the row and column interchanges,
*>          the row and column k were interchanged with the row and
*>          column IPIV(k).
*> \endverbatim
*>
*> \param[in,out] H
*> \verbatim
*>          H is REAL workspace, dimension (LDH,NB).
*>
*> \endverbatim
*>
*> \param[in] LDH
*> \verbatim
*>          LDH is INTEGER
*>          The leading dimension of the workspace H. LDH >= max(1,M).
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL workspace, dimension (M).
*> \endverbatim
*>
*
*  Authors:
*  ========
*
*> \author Univ. of Tennessee
*> \author Univ. of California Berkeley
*> \author Univ. of Colorado Denver
*> \author NAG Ltd.
*
*> \date November 2017
*
*> \ingroup realSYcomputational
*
*  =====================================================================
      SUBROUTINE SLASYF_AA( UPLO, J1, M, NB, A, LDA, IPIV,
     $                      H, LDH, WORK )
*
*  -- LAPACK computational routine (version 3.8.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2017
*
      IMPLICIT NONE
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            M, NB, J1, LDA, LDH
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * )
      REAL               A( LDA, * ), H( LDH, * ), WORK( * )
*     ..
*
*  =====================================================================
*     .. Parameters ..
      REAL   ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
*
*     .. Local Scalars ..
      INTEGER            J, K, K1, I1, I2, MJ
      REAL               PIV, ALPHA
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ISAMAX, ILAENV
      EXTERNAL           LSAME, ILAENV, ISAMAX
*     ..
*     .. External Subroutines ..
      EXTERNAL           SAXPY, SGEMV, SSCAL, SCOPY, SSWAP, SLASET,
     $                   XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX
*     ..
*     .. Executable Statements ..
*
      J = 1
*
*     K1 is the first column of the panel to be factorized
*     i.e.,  K1 is 2 for the first block column, and 1 for the rest of the blocks
*
      K1 = (2-J1)+1
*
      IF( LSAME( UPLO, 'U' ) ) THEN
*
*        .....................................................
*        Factorize A as U**T*D*U using the upper triangle of A
*        .....................................................
*
 10      CONTINUE
         IF ( J.GT.MIN(M, NB) )
     $      GO TO 20
*
*        K is the column to be factorized
*         when being called from SSYTRF_AA,
*         > for the first block column, J1 is 1, hence J1+J-1 is J,
*         > for the rest of the columns, J1 is 2, and J1+J-1 is J+1,
*
         K = J1+J-1
         IF( J.EQ.M ) THEN
*
*            Only need to compute T(J, J)
*
             MJ = 1
         ELSE
             MJ = M-J+1
         END IF
*
*        H(J:M, J) := A(J, J:M) - H(J:M, 1:(J-1)) * L(J1:(J-1), J),
*         where H(J:M, J) has been initialized to be A(J, J:M)
*
         IF( K.GT.2 ) THEN
*
*        K is the column to be factorized
*         > for the first block column, K is J, skipping the first two
*           columns
*         > for the rest of the columns, K is J+1, skipping only the
*           first column
*
            CALL SGEMV( 'No transpose', MJ, J-K1,
     $                 -ONE, H( J, K1 ), LDH,
     $                       A( 1, J ), 1,
     $                  ONE, H( J, J ), 1 )
         END IF
*
*        Copy H(i:M, i) into WORK
*
         CALL SCOPY( MJ, H( J, J ), 1, WORK( 1 ), 1 )
*
         IF( J.GT.K1 ) THEN
*
*           Compute WORK := WORK - L(J-1, J:M) * T(J-1,J),
*            where A(J-1, J) stores T(J-1, J) and A(J-2, J:M) stores U(J-1, J:M)
*
            ALPHA = -A( K-1, J )
            CALL SAXPY( MJ, ALPHA, A( K-2, J ), LDA, WORK( 1 ), 1 )
         END IF
*
*        Set A(J, J) = T(J, J)
*
         A( K, J ) = WORK( 1 )
*
         IF( J.LT.M ) THEN
*
*           Compute WORK(2:M) = T(J, J) L(J, (J+1):M)
*            where A(J, J) stores T(J, J) and A(J-1, (J+1):M) stores U(J, (J+1):M)
*
            IF( K.GT.1 ) THEN
               ALPHA = -A( K, J )
               CALL SAXPY( M-J, ALPHA, A( K-1, J+1 ), LDA,
     $                                 WORK( 2 ), 1 )
            ENDIF
*
*           Find max(|WORK(2:M)|)
*
            I2 = ISAMAX( M-J, WORK( 2 ), 1 ) + 1
            PIV = WORK( I2 )
*
*           Apply symmetric pivot
*
            IF( (I2.NE.2) .AND. (PIV.NE.0) ) THEN
*
*              Swap WORK(I1) and WORK(I2)
*
               I1 = 2
               WORK( I2 ) = WORK( I1 )
               WORK( I1 ) = PIV
*
*              Swap A(I1, I1+1:M) with A(I1+1:M, I2)
*
               I1 = I1+J-1
               I2 = I2+J-1
               CALL SSWAP( I2-I1-1, A( J1+I1-1, I1+1 ), LDA,
     $                              A( J1+I1, I2 ), 1 )
*
*              Swap A(I1, I2+1:M) with A(I2, I2+1:M)
*
               CALL SSWAP( M-I2, A( J1+I1-1, I2+1 ), LDA,
     $                           A( J1+I2-1, I2+1 ), LDA )
*
*              Swap A(I1, I1) with A(I2,I2)
*
               PIV = A( I1+J1-1, I1 )
               A( J1+I1-1, I1 ) = A( J1+I2-1, I2 )
               A( J1+I2-1, I2 ) = PIV
*
*              Swap H(I1, 1:J1) with H(I2, 1:J1)
*
               CALL SSWAP( I1-1, H( I1, 1 ), LDH, H( I2, 1 ), LDH )
               IPIV( I1 ) = I2
*
               IF( I1.GT.(K1-1) ) THEN
*
*                 Swap L(1:I1-1, I1) with L(1:I1-1, I2),
*                  skipping the first column
*
                  CALL SSWAP( I1-K1+1, A( 1, I1 ), 1,
     $                                 A( 1, I2 ), 1 )
               END IF
            ELSE
               IPIV( J+1 ) = J+1
            ENDIF
*
*           Set A(J, J+1) = T(J, J+1)
*
            A( K, J+1 ) = WORK( 2 )
*
            IF( J.LT.NB ) THEN
*
*              Copy A(J+1:M, J+1) into H(J:M, J),
*
               CALL SCOPY( M-J, A( K+1, J+1 ), LDA,
     $                          H( J+1, J+1 ), 1 )
            END IF
*
*           Compute L(J+2, J+1) = WORK( 3:M ) / T(J, J+1),
*            where A(J, J+1) = T(J, J+1) and A(J+2:M, J) = L(J+2:M, J+1)
*
            IF( A( K, J+1 ).NE.ZERO ) THEN
               ALPHA = ONE / A( K, J+1 )
               CALL SCOPY( M-J-1, WORK( 3 ), 1, A( K, J+2 ), LDA )
               CALL SSCAL( M-J-1, ALPHA, A( K, J+2 ), LDA )
            ELSE
               CALL SLASET( 'Full', 1, M-J-1, ZERO, ZERO,
     $                      A( K, J+2 ), LDA)
            END IF
         END IF
         J = J + 1
         GO TO 10
 20      CONTINUE
*
      ELSE
*
*        .....................................................
*        Factorize A as L*D*L**T using the lower triangle of A
*        .....................................................
*
 30      CONTINUE
         IF( J.GT.MIN( M, NB ) )
     $      GO TO 40
*
*        K is the column to be factorized
*         when being called from SSYTRF_AA,
*         > for the first block column, J1 is 1, hence J1+J-1 is J,
*         > for the rest of the columns, J1 is 2, and J1+J-1 is J+1,
*
         K = J1+J-1
         IF( J.EQ.M ) THEN
*
*            Only need to compute T(J, J)
*
             MJ = 1
         ELSE
             MJ = M-J+1
         END IF
*
*        H(J:M, J) := A(J:M, J) - H(J:M, 1:(J-1)) * L(J, J1:(J-1))^T,
*         where H(J:M, J) has been initialized to be A(J:M, J)
*
         IF( K.GT.2 ) THEN
*
*        K is the column to be factorized
*         > for the first block column, K is J, skipping the first two
*           columns
*         > for the rest of the columns, K is J+1, skipping only the
*           first column
*
            CALL SGEMV( 'No transpose', MJ, J-K1,
     $                 -ONE, H( J, K1 ), LDH,
     $                       A( J, 1 ), LDA,
     $                  ONE, H( J, J ), 1 )
         END IF
*
*        Copy H(J:M, J) into WORK
*
         CALL SCOPY( MJ, H( J, J ), 1, WORK( 1 ), 1 )
*
         IF( J.GT.K1 ) THEN
*
*           Compute WORK := WORK - L(J:M, J-1) * T(J-1,J),
*            where A(J-1, J) = T(J-1, J) and A(J, J-2) = L(J, J-1)
*
            ALPHA = -A( J, K-1 )
            CALL SAXPY( MJ, ALPHA, A( J, K-2 ), 1, WORK( 1 ), 1 )
         END IF
*
*        Set A(J, J) = T(J, J)
*
         A( J, K ) = WORK( 1 )
*
         IF( J.LT.M ) THEN
*
*           Compute WORK(2:M) = T(J, J) L((J+1):M, J)
*            where A(J, J) = T(J, J) and A((J+1):M, J-1) = L((J+1):M, J)
*
            IF( K.GT.1 ) THEN
               ALPHA = -A( J, K )
               CALL SAXPY( M-J, ALPHA, A( J+1, K-1 ), 1,
     $                                 WORK( 2 ), 1 )
            ENDIF
*
*           Find max(|WORK(2:M)|)
*
            I2 = ISAMAX( M-J, WORK( 2 ), 1 ) + 1
            PIV = WORK( I2 )
*
*           Apply symmetric pivot
*
            IF( (I2.NE.2) .AND. (PIV.NE.0) ) THEN
*
*              Swap WORK(I1) and WORK(I2)
*
               I1 = 2
               WORK( I2 ) = WORK( I1 )
               WORK( I1 ) = PIV
*
*              Swap A(I1+1:M, I1) with A(I2, I1+1:M)
*
               I1 = I1+J-1
               I2 = I2+J-1
               CALL SSWAP( I2-I1-1, A( I1+1, J1+I1-1 ), 1,
     $                              A( I2, J1+I1 ), LDA )
*
*              Swap A(I2+1:M, I1) with A(I2+1:M, I2)
*
               CALL SSWAP( M-I2, A( I2+1, J1+I1-1 ), 1,
     $                           A( I2+1, J1+I2-1 ), 1 )
*
*              Swap A(I1, I1) with A(I2, I2)
*
               PIV = A( I1, J1+I1-1 )
               A( I1, J1+I1-1 ) = A( I2, J1+I2-1 )
               A( I2, J1+I2-1 ) = PIV
*
*              Swap H(I1, I1:J1) with H(I2, I2:J1)
*
               CALL SSWAP( I1-1, H( I1, 1 ), LDH, H( I2, 1 ), LDH )
               IPIV( I1 ) = I2
*
               IF( I1.GT.(K1-1) ) THEN
*
*                 Swap L(1:I1-1, I1) with L(1:I1-1, I2),
*                  skipping the first column
*
                  CALL SSWAP( I1-K1+1, A( I1, 1 ), LDA,
     $                                 A( I2, 1 ), LDA )
               END IF
            ELSE
               IPIV( J+1 ) = J+1
            ENDIF
*
*           Set A(J+1, J) = T(J+1, J)
*
            A( J+1, K ) = WORK( 2 )
*
            IF( J.LT.NB ) THEN
*
*              Copy A(J+1:M, J+1) into H(J+1:M, J),
*
               CALL SCOPY( M-J, A( J+1, K+1 ), 1,
     $                          H( J+1, J+1 ), 1 )
            END IF
*
*           Compute L(J+2, J+1) = WORK( 3:M ) / T(J, J+1),
*            where A(J, J+1) = T(J, J+1) and A(J+2:M, J) = L(J+2:M, J+1)
*
            IF( A( J+1, K ).NE.ZERO ) THEN
               ALPHA = ONE / A( J+1, K )
               CALL SCOPY( M-J-1, WORK( 3 ), 1, A( J+2, K ), 1 )
               CALL SSCAL( M-J-1, ALPHA, A( J+2, K ), 1 )
            ELSE
               CALL SLASET( 'Full', M-J-1, 1, ZERO, ZERO,
     $                      A( J+2, K ), LDA )
            END IF
         END IF
         J = J + 1
         GO TO 30
 40      CONTINUE
      END IF
      RETURN
*
*     End of SLASYF_AA
*
      END
