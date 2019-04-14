*> \brief \b ZLAQPS computes a step of QR factorization with column pivoting of a real m-by-n matrix A by using BLAS level 3.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZLAQPS + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zlaqps.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zlaqps.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zlaqps.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZLAQPS( M, N, OFFSET, NB, KB, A, LDA, JPVT, TAU, VN1,
*                          VN2, AUXV, F, LDF )
*
*       .. Scalar Arguments ..
*       INTEGER            KB, LDA, LDF, M, N, NB, OFFSET
*       ..
*       .. Array Arguments ..
*       INTEGER            JPVT( * )
*       DOUBLE PRECISION   VN1( * ), VN2( * )
*       COMPLEX*16         A( LDA, * ), AUXV( * ), F( LDF, * ), TAU( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZLAQPS computes a step of QR factorization with column pivoting
*> of a complex M-by-N matrix A by using Blas-3.  It tries to factorize
*> NB columns from A starting from the row OFFSET+1, and updates all
*> of the matrix with Blas-3 xGEMM.
*>
*> In some cases, due to catastrophic cancellations, it cannot
*> factorize NB columns.  Hence, the actual number of factorized
*> columns is returned in KB.
*>
*> Block A(1:OFFSET,1:N) is accordingly pivoted, but not factorized.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix A. M >= 0.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix A. N >= 0
*> \endverbatim
*>
*> \param[in] OFFSET
*> \verbatim
*>          OFFSET is INTEGER
*>          The number of rows of A that have been factorized in
*>          previous steps.
*> \endverbatim
*>
*> \param[in] NB
*> \verbatim
*>          NB is INTEGER
*>          The number of columns to factorize.
*> \endverbatim
*>
*> \param[out] KB
*> \verbatim
*>          KB is INTEGER
*>          The number of columns actually factorized.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>          On entry, the M-by-N matrix A.
*>          On exit, block A(OFFSET+1:M,1:KB) is the triangular
*>          factor obtained and block A(1:OFFSET,1:N) has been
*>          accordingly pivoted, but no factorized.
*>          The rest of the matrix, block A(OFFSET+1:M,KB+1:N) has
*>          been updated.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A. LDA >= max(1,M).
*> \endverbatim
*>
*> \param[in,out] JPVT
*> \verbatim
*>          JPVT is INTEGER array, dimension (N)
*>          JPVT(I) = K <==> Column K of the full matrix A has been
*>          permuted into position I in AP.
*> \endverbatim
*>
*> \param[out] TAU
*> \verbatim
*>          TAU is COMPLEX*16 array, dimension (KB)
*>          The scalar factors of the elementary reflectors.
*> \endverbatim
*>
*> \param[in,out] VN1
*> \verbatim
*>          VN1 is DOUBLE PRECISION array, dimension (N)
*>          The vector with the partial column norms.
*> \endverbatim
*>
*> \param[in,out] VN2
*> \verbatim
*>          VN2 is DOUBLE PRECISION array, dimension (N)
*>          The vector with the exact column norms.
*> \endverbatim
*>
*> \param[in,out] AUXV
*> \verbatim
*>          AUXV is COMPLEX*16 array, dimension (NB)
*>          Auxiliar vector.
*> \endverbatim
*>
*> \param[in,out] F
*> \verbatim
*>          F is COMPLEX*16 array, dimension (LDF,NB)
*>          Matrix F**H = L * Y**H * A.
*> \endverbatim
*>
*> \param[in] LDF
*> \verbatim
*>          LDF is INTEGER
*>          The leading dimension of the array F. LDF >= max(1,N).
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
*> \ingroup complex16OTHERauxiliary
*
*> \par Contributors:
*  ==================
*>
*>    G. Quintana-Orti, Depto. de Informatica, Universidad Jaime I, Spain
*>    X. Sun, Computer Science Dept., Duke University, USA
*> \n
*>  Partial column norm updating strategy modified on April 2011
*>    Z. Drmac and Z. Bujanovic, Dept. of Mathematics,
*>    University of Zagreb, Croatia.
*
*> \par References:
*  ================
*>
*> LAPACK Working Note 176
*
*> \htmlonly
*> <a href="http://www.netlib.org/lapack/lawnspdf/lawn176.pdf">[PDF]</a>
*> \endhtmlonly
*
*  =====================================================================
      SUBROUTINE ZLAQPS( M, N, OFFSET, NB, KB, A, LDA, JPVT, TAU, VN1,
     $                   VN2, AUXV, F, LDF )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            KB, LDA, LDF, M, N, NB, OFFSET
*     ..
*     .. Array Arguments ..
      INTEGER            JPVT( * )
      DOUBLE PRECISION   VN1( * ), VN2( * )
      COMPLEX*16         A( LDA, * ), AUXV( * ), F( LDF, * ), TAU( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      COMPLEX*16         CZERO, CONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0,
     $                   CZERO = ( 0.0D+0, 0.0D+0 ),
     $                   CONE = ( 1.0D+0, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      INTEGER            ITEMP, J, K, LASTRK, LSTICC, PVT, RK
      DOUBLE PRECISION   TEMP, TEMP2, TOL3Z
      COMPLEX*16         AKK
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZGEMM, ZGEMV, ZLARFG, ZSWAP
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, DCONJG, MAX, MIN, NINT, SQRT
*     ..
*     .. External Functions ..
      INTEGER            IDAMAX
      DOUBLE PRECISION   DLAMCH, DZNRM2
      EXTERNAL           IDAMAX, DLAMCH, DZNRM2
*     ..
*     .. Executable Statements ..
*
      LASTRK = MIN( M, N+OFFSET )
      LSTICC = 0
      K = 0
      TOL3Z = SQRT(DLAMCH('Epsilon'))
*
*     Beginning of while loop.
*
   10 CONTINUE
      IF( ( K.LT.NB ) .AND. ( LSTICC.EQ.0 ) ) THEN
         K = K + 1
         RK = OFFSET + K
*
*        Determine ith pivot column and swap if necessary
*
         PVT = ( K-1 ) + IDAMAX( N-K+1, VN1( K ), 1 )
         IF( PVT.NE.K ) THEN
            CALL ZSWAP( M, A( 1, PVT ), 1, A( 1, K ), 1 )
            CALL ZSWAP( K-1, F( PVT, 1 ), LDF, F( K, 1 ), LDF )
            ITEMP = JPVT( PVT )
            JPVT( PVT ) = JPVT( K )
            JPVT( K ) = ITEMP
            VN1( PVT ) = VN1( K )
            VN2( PVT ) = VN2( K )
         END IF
*
*        Apply previous Householder reflectors to column K:
*        A(RK:M,K) := A(RK:M,K) - A(RK:M,1:K-1)*F(K,1:K-1)**H.
*
         IF( K.GT.1 ) THEN
            DO 20 J = 1, K - 1
               F( K, J ) = DCONJG( F( K, J ) )
   20       CONTINUE
            CALL ZGEMV( 'No transpose', M-RK+1, K-1, -CONE, A( RK, 1 ),
     $                  LDA, F( K, 1 ), LDF, CONE, A( RK, K ), 1 )
            DO 30 J = 1, K - 1
               F( K, J ) = DCONJG( F( K, J ) )
   30       CONTINUE
         END IF
*
*        Generate elementary reflector H(k).
*
         IF( RK.LT.M ) THEN
            CALL ZLARFG( M-RK+1, A( RK, K ), A( RK+1, K ), 1, TAU( K ) )
         ELSE
            CALL ZLARFG( 1, A( RK, K ), A( RK, K ), 1, TAU( K ) )
         END IF
*
         AKK = A( RK, K )
         A( RK, K ) = CONE
*
*        Compute Kth column of F:
*
*        Compute  F(K+1:N,K) := tau(K)*A(RK:M,K+1:N)**H*A(RK:M,K).
*
         IF( K.LT.N ) THEN
            CALL ZGEMV( 'Conjugate transpose', M-RK+1, N-K, TAU( K ),
     $                  A( RK, K+1 ), LDA, A( RK, K ), 1, CZERO,
     $                  F( K+1, K ), 1 )
         END IF
*
*        Padding F(1:K,K) with zeros.
*
         DO 40 J = 1, K
            F( J, K ) = CZERO
   40    CONTINUE
*
*        Incremental updating of F:
*        F(1:N,K) := F(1:N,K) - tau(K)*F(1:N,1:K-1)*A(RK:M,1:K-1)**H
*                    *A(RK:M,K).
*
         IF( K.GT.1 ) THEN
            CALL ZGEMV( 'Conjugate transpose', M-RK+1, K-1, -TAU( K ),
     $                  A( RK, 1 ), LDA, A( RK, K ), 1, CZERO,
     $                  AUXV( 1 ), 1 )
*
            CALL ZGEMV( 'No transpose', N, K-1, CONE, F( 1, 1 ), LDF,
     $                  AUXV( 1 ), 1, CONE, F( 1, K ), 1 )
         END IF
*
*        Update the current row of A:
*        A(RK,K+1:N) := A(RK,K+1:N) - A(RK,1:K)*F(K+1:N,1:K)**H.
*
         IF( K.LT.N ) THEN
            CALL ZGEMM( 'No transpose', 'Conjugate transpose', 1, N-K,
     $                  K, -CONE, A( RK, 1 ), LDA, F( K+1, 1 ), LDF,
     $                  CONE, A( RK, K+1 ), LDA )
         END IF
*
*        Update partial column norms.
*
         IF( RK.LT.LASTRK ) THEN
            DO 50 J = K + 1, N
               IF( VN1( J ).NE.ZERO ) THEN
*
*                 NOTE: The following 4 lines follow from the analysis in
*                 Lapack Working Note 176.
*
                  TEMP = ABS( A( RK, J ) ) / VN1( J )
                  TEMP = MAX( ZERO, ( ONE+TEMP )*( ONE-TEMP ) )
                  TEMP2 = TEMP*( VN1( J ) / VN2( J ) )**2
                  IF( TEMP2 .LE. TOL3Z ) THEN
                     VN2( J ) = DBLE( LSTICC )
                     LSTICC = J
                  ELSE
                     VN1( J ) = VN1( J )*SQRT( TEMP )
                  END IF
               END IF
   50       CONTINUE
         END IF
*
         A( RK, K ) = AKK
*
*        End of while loop.
*
         GO TO 10
      END IF
      KB = K
      RK = OFFSET + KB
*
*     Apply the block reflector to the rest of the matrix:
*     A(OFFSET+KB+1:M,KB+1:N) := A(OFFSET+KB+1:M,KB+1:N) -
*                         A(OFFSET+KB+1:M,1:KB)*F(KB+1:N,1:KB)**H.
*
      IF( KB.LT.MIN( N, M-OFFSET ) ) THEN
         CALL ZGEMM( 'No transpose', 'Conjugate transpose', M-RK, N-KB,
     $               KB, -CONE, A( RK+1, 1 ), LDA, F( KB+1, 1 ), LDF,
     $               CONE, A( RK+1, KB+1 ), LDA )
      END IF
*
*     Recomputation of difficult columns.
*
   60 CONTINUE
      IF( LSTICC.GT.0 ) THEN
         ITEMP = NINT( VN2( LSTICC ) )
         VN1( LSTICC ) = DZNRM2( M-RK, A( RK+1, LSTICC ), 1 )
*
*        NOTE: The computation of VN1( LSTICC ) relies on the fact that
*        SNRM2 does not fail on vectors with norm below the value of
*        SQRT(DLAMCH('S'))
*
         VN2( LSTICC ) = VN1( LSTICC )
         LSTICC = ITEMP
         GO TO 60
      END IF
*
      RETURN
*
*     End of ZLAQPS
*
      END
