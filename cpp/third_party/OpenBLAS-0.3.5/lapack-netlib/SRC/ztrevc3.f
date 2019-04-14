*> \brief \b ZTREVC3
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZTREVC3 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/ztrevc3.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/ztrevc3.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/ztrevc3.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZTREVC3( SIDE, HOWMNY, SELECT, N, T, LDT, VL, LDVL, VR,
*      $                    LDVR, MM, M, WORK, LWORK, RWORK, LRWORK, INFO)
*
*       .. Scalar Arguments ..
*       CHARACTER          HOWMNY, SIDE
*       INTEGER            INFO, LDT, LDVL, LDVR, LWORK, M, MM, N
*       ..
*       .. Array Arguments ..
*       LOGICAL            SELECT( * )
*       DOUBLE PRECISION   RWORK( * )
*       COMPLEX*16         T( LDT, * ), VL( LDVL, * ), VR( LDVR, * ),
*      $                   WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZTREVC3 computes some or all of the right and/or left eigenvectors of
*> a complex upper triangular matrix T.
*> Matrices of this type are produced by the Schur factorization of
*> a complex general matrix:  A = Q*T*Q**H, as computed by ZHSEQR.
*>
*> The right eigenvector x and the left eigenvector y of T corresponding
*> to an eigenvalue w are defined by:
*>
*>              T*x = w*x,     (y**H)*T = w*(y**H)
*>
*> where y**H denotes the conjugate transpose of the vector y.
*> The eigenvalues are not input to this routine, but are read directly
*> from the diagonal of T.
*>
*> This routine returns the matrices X and/or Y of right and left
*> eigenvectors of T, or the products Q*X and/or Q*Y, where Q is an
*> input matrix. If Q is the unitary factor that reduces a matrix A to
*> Schur form T, then Q*X and Q*Y are the matrices of right and left
*> eigenvectors of A.
*>
*> This uses a Level 3 BLAS version of the back transformation.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] SIDE
*> \verbatim
*>          SIDE is CHARACTER*1
*>          = 'R':  compute right eigenvectors only;
*>          = 'L':  compute left eigenvectors only;
*>          = 'B':  compute both right and left eigenvectors.
*> \endverbatim
*>
*> \param[in] HOWMNY
*> \verbatim
*>          HOWMNY is CHARACTER*1
*>          = 'A':  compute all right and/or left eigenvectors;
*>          = 'B':  compute all right and/or left eigenvectors,
*>                  backtransformed using the matrices supplied in
*>                  VR and/or VL;
*>          = 'S':  compute selected right and/or left eigenvectors,
*>                  as indicated by the logical array SELECT.
*> \endverbatim
*>
*> \param[in] SELECT
*> \verbatim
*>          SELECT is LOGICAL array, dimension (N)
*>          If HOWMNY = 'S', SELECT specifies the eigenvectors to be
*>          computed.
*>          The eigenvector corresponding to the j-th eigenvalue is
*>          computed if SELECT(j) = .TRUE..
*>          Not referenced if HOWMNY = 'A' or 'B'.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix T. N >= 0.
*> \endverbatim
*>
*> \param[in,out] T
*> \verbatim
*>          T is COMPLEX*16 array, dimension (LDT,N)
*>          The upper triangular matrix T.  T is modified, but restored
*>          on exit.
*> \endverbatim
*>
*> \param[in] LDT
*> \verbatim
*>          LDT is INTEGER
*>          The leading dimension of the array T. LDT >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] VL
*> \verbatim
*>          VL is COMPLEX*16 array, dimension (LDVL,MM)
*>          On entry, if SIDE = 'L' or 'B' and HOWMNY = 'B', VL must
*>          contain an N-by-N matrix Q (usually the unitary matrix Q of
*>          Schur vectors returned by ZHSEQR).
*>          On exit, if SIDE = 'L' or 'B', VL contains:
*>          if HOWMNY = 'A', the matrix Y of left eigenvectors of T;
*>          if HOWMNY = 'B', the matrix Q*Y;
*>          if HOWMNY = 'S', the left eigenvectors of T specified by
*>                           SELECT, stored consecutively in the columns
*>                           of VL, in the same order as their
*>                           eigenvalues.
*>          Not referenced if SIDE = 'R'.
*> \endverbatim
*>
*> \param[in] LDVL
*> \verbatim
*>          LDVL is INTEGER
*>          The leading dimension of the array VL.
*>          LDVL >= 1, and if SIDE = 'L' or 'B', LDVL >= N.
*> \endverbatim
*>
*> \param[in,out] VR
*> \verbatim
*>          VR is COMPLEX*16 array, dimension (LDVR,MM)
*>          On entry, if SIDE = 'R' or 'B' and HOWMNY = 'B', VR must
*>          contain an N-by-N matrix Q (usually the unitary matrix Q of
*>          Schur vectors returned by ZHSEQR).
*>          On exit, if SIDE = 'R' or 'B', VR contains:
*>          if HOWMNY = 'A', the matrix X of right eigenvectors of T;
*>          if HOWMNY = 'B', the matrix Q*X;
*>          if HOWMNY = 'S', the right eigenvectors of T specified by
*>                           SELECT, stored consecutively in the columns
*>                           of VR, in the same order as their
*>                           eigenvalues.
*>          Not referenced if SIDE = 'L'.
*> \endverbatim
*>
*> \param[in] LDVR
*> \verbatim
*>          LDVR is INTEGER
*>          The leading dimension of the array VR.
*>          LDVR >= 1, and if SIDE = 'R' or 'B', LDVR >= N.
*> \endverbatim
*>
*> \param[in] MM
*> \verbatim
*>          MM is INTEGER
*>          The number of columns in the arrays VL and/or VR. MM >= M.
*> \endverbatim
*>
*> \param[out] M
*> \verbatim
*>          M is INTEGER
*>          The number of columns in the arrays VL and/or VR actually
*>          used to store the eigenvectors.
*>          If HOWMNY = 'A' or 'B', M is set to N.
*>          Each selected eigenvector occupies one column.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (MAX(1,LWORK))
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of array WORK. LWORK >= max(1,2*N).
*>          For optimum performance, LWORK >= N + 2*N*NB, where NB is
*>          the optimal blocksize.
*>
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal size of the WORK array, returns
*>          this value as the first entry of the WORK array, and no error
*>          message related to LWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (LRWORK)
*> \endverbatim
*>
*> \param[in] LRWORK
*> \verbatim
*>          LRWORK is INTEGER
*>          The dimension of array RWORK. LRWORK >= max(1,N).
*>
*>          If LRWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal size of the RWORK array, returns
*>          this value as the first entry of the RWORK array, and no error
*>          message related to LRWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
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
*> \date November 2017
*
*  @precisions fortran z -> c
*
*> \ingroup complex16OTHERcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  The algorithm used in this program is basically backward (forward)
*>  substitution, with scaling to make the the code robust against
*>  possible overflow.
*>
*>  Each eigenvector is normalized so that the element of largest
*>  magnitude has magnitude 1; here the magnitude of a complex number
*>  (x,y) is taken to be |x| + |y|.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE ZTREVC3( SIDE, HOWMNY, SELECT, N, T, LDT, VL, LDVL, VR,
     $                    LDVR, MM, M, WORK, LWORK, RWORK, LRWORK, INFO)
      IMPLICIT NONE
*
*  -- LAPACK computational routine (version 3.8.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2017
*
*     .. Scalar Arguments ..
      CHARACTER          HOWMNY, SIDE
      INTEGER            INFO, LDT, LDVL, LDVR, LWORK, LRWORK, M, MM, N
*     ..
*     .. Array Arguments ..
      LOGICAL            SELECT( * )
      DOUBLE PRECISION   RWORK( * )
      COMPLEX*16         T( LDT, * ), VL( LDVL, * ), VR( LDVR, * ),
     $                   WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
      COMPLEX*16         CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0D+0, 0.0D+0 ),
     $                     CONE  = ( 1.0D+0, 0.0D+0 ) )
      INTEGER            NBMIN, NBMAX
      PARAMETER          ( NBMIN = 8, NBMAX = 128 )
*     ..
*     .. Local Scalars ..
      LOGICAL            ALLV, BOTHV, LEFTV, LQUERY, OVER, RIGHTV, SOMEV
      INTEGER            I, II, IS, J, K, KI, IV, MAXWRK, NB
      DOUBLE PRECISION   OVFL, REMAX, SCALE, SMIN, SMLNUM, ULP, UNFL
      COMPLEX*16         CDUM
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV, IZAMAX
      DOUBLE PRECISION   DLAMCH, DZASUM
      EXTERNAL           LSAME, ILAENV, IZAMAX, DLAMCH, DZASUM
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA, ZCOPY, ZDSCAL, ZGEMV, ZLATRS,
     $                   ZGEMM, DLABAD, ZLASET, ZLACPY
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, DCMPLX, CONJG, AIMAG, MAX
*     ..
*     .. Statement Functions ..
      DOUBLE PRECISION   CABS1
*     ..
*     .. Statement Function definitions ..
      CABS1( CDUM ) = ABS( DBLE( CDUM ) ) + ABS( AIMAG( CDUM ) )
*     ..
*     .. Executable Statements ..
*
*     Decode and test the input parameters
*
      BOTHV  = LSAME( SIDE, 'B' )
      RIGHTV = LSAME( SIDE, 'R' ) .OR. BOTHV
      LEFTV  = LSAME( SIDE, 'L' ) .OR. BOTHV
*
      ALLV  = LSAME( HOWMNY, 'A' )
      OVER  = LSAME( HOWMNY, 'B' )
      SOMEV = LSAME( HOWMNY, 'S' )
*
*     Set M to the number of columns required to store the selected
*     eigenvectors.
*
      IF( SOMEV ) THEN
         M = 0
         DO 10 J = 1, N
            IF( SELECT( J ) )
     $         M = M + 1
   10    CONTINUE
      ELSE
         M = N
      END IF
*
      INFO = 0
      NB = ILAENV( 1, 'ZTREVC', SIDE // HOWMNY, N, -1, -1, -1 )
      MAXWRK = N + 2*N*NB
      WORK(1) = MAXWRK
      RWORK(1) = N
      LQUERY = ( LWORK.EQ.-1 .OR. LRWORK.EQ.-1 )
      IF( .NOT.RIGHTV .AND. .NOT.LEFTV ) THEN
         INFO = -1
      ELSE IF( .NOT.ALLV .AND. .NOT.OVER .AND. .NOT.SOMEV ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -4
      ELSE IF( LDT.LT.MAX( 1, N ) ) THEN
         INFO = -6
      ELSE IF( LDVL.LT.1 .OR. ( LEFTV .AND. LDVL.LT.N ) ) THEN
         INFO = -8
      ELSE IF( LDVR.LT.1 .OR. ( RIGHTV .AND. LDVR.LT.N ) ) THEN
         INFO = -10
      ELSE IF( MM.LT.M ) THEN
         INFO = -11
      ELSE IF( LWORK.LT.MAX( 1, 2*N ) .AND. .NOT.LQUERY ) THEN
         INFO = -14
      ELSE IF ( LRWORK.LT.MAX( 1, N ) .AND. .NOT.LQUERY ) THEN
         INFO = -16
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZTREVC3', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Quick return if possible.
*
      IF( N.EQ.0 )
     $   RETURN
*
*     Use blocked version of back-transformation if sufficient workspace.
*     Zero-out the workspace to avoid potential NaN propagation.
*
      IF( OVER .AND. LWORK .GE. N + 2*N*NBMIN ) THEN
         NB = (LWORK - N) / (2*N)
         NB = MIN( NB, NBMAX )
         CALL ZLASET( 'F', N, 1+2*NB, CZERO, CZERO, WORK, N )
      ELSE
         NB = 1
      END IF
*
*     Set the constants to control overflow.
*
      UNFL = DLAMCH( 'Safe minimum' )
      OVFL = ONE / UNFL
      CALL DLABAD( UNFL, OVFL )
      ULP = DLAMCH( 'Precision' )
      SMLNUM = UNFL*( N / ULP )
*
*     Store the diagonal elements of T in working array WORK.
*
      DO 20 I = 1, N
         WORK( I ) = T( I, I )
   20 CONTINUE
*
*     Compute 1-norm of each column of strictly upper triangular
*     part of T to control overflow in triangular solver.
*
      RWORK( 1 ) = ZERO
      DO 30 J = 2, N
         RWORK( J ) = DZASUM( J-1, T( 1, J ), 1 )
   30 CONTINUE
*
      IF( RIGHTV ) THEN
*
*        ============================================================
*        Compute right eigenvectors.
*
*        IV is index of column in current block.
*        Non-blocked version always uses IV=NB=1;
*        blocked     version starts with IV=NB, goes down to 1.
*        (Note the "0-th" column is used to store the original diagonal.)
         IV = NB
         IS = M
         DO 80 KI = N, 1, -1
            IF( SOMEV ) THEN
               IF( .NOT.SELECT( KI ) )
     $            GO TO 80
            END IF
            SMIN = MAX( ULP*( CABS1( T( KI, KI ) ) ), SMLNUM )
*
*           --------------------------------------------------------
*           Complex right eigenvector
*
            WORK( KI + IV*N ) = CONE
*
*           Form right-hand side.
*
            DO 40 K = 1, KI - 1
               WORK( K + IV*N ) = -T( K, KI )
   40       CONTINUE
*
*           Solve upper triangular system:
*           [ T(1:KI-1,1:KI-1) - T(KI,KI) ]*X = SCALE*WORK.
*
            DO 50 K = 1, KI - 1
               T( K, K ) = T( K, K ) - T( KI, KI )
               IF( CABS1( T( K, K ) ).LT.SMIN )
     $            T( K, K ) = SMIN
   50       CONTINUE
*
            IF( KI.GT.1 ) THEN
               CALL ZLATRS( 'Upper', 'No transpose', 'Non-unit', 'Y',
     $                      KI-1, T, LDT, WORK( 1 + IV*N ), SCALE,
     $                      RWORK, INFO )
               WORK( KI + IV*N ) = SCALE
            END IF
*
*           Copy the vector x or Q*x to VR and normalize.
*
            IF( .NOT.OVER ) THEN
*              ------------------------------
*              no back-transform: copy x to VR and normalize.
               CALL ZCOPY( KI, WORK( 1 + IV*N ), 1, VR( 1, IS ), 1 )
*
               II = IZAMAX( KI, VR( 1, IS ), 1 )
               REMAX = ONE / CABS1( VR( II, IS ) )
               CALL ZDSCAL( KI, REMAX, VR( 1, IS ), 1 )
*
               DO 60 K = KI + 1, N
                  VR( K, IS ) = CZERO
   60          CONTINUE
*
            ELSE IF( NB.EQ.1 ) THEN
*              ------------------------------
*              version 1: back-transform each vector with GEMV, Q*x.
               IF( KI.GT.1 )
     $            CALL ZGEMV( 'N', N, KI-1, CONE, VR, LDVR,
     $                        WORK( 1 + IV*N ), 1, DCMPLX( SCALE ),
     $                        VR( 1, KI ), 1 )
*
               II = IZAMAX( N, VR( 1, KI ), 1 )
               REMAX = ONE / CABS1( VR( II, KI ) )
               CALL ZDSCAL( N, REMAX, VR( 1, KI ), 1 )
*
            ELSE
*              ------------------------------
*              version 2: back-transform block of vectors with GEMM
*              zero out below vector
               DO K = KI + 1, N
                  WORK( K + IV*N ) = CZERO
               END DO
*
*              Columns IV:NB of work are valid vectors.
*              When the number of vectors stored reaches NB,
*              or if this was last vector, do the GEMM
               IF( (IV.EQ.1) .OR. (KI.EQ.1) ) THEN
                  CALL ZGEMM( 'N', 'N', N, NB-IV+1, KI+NB-IV, CONE,
     $                        VR, LDVR,
     $                        WORK( 1 + (IV)*N    ), N,
     $                        CZERO,
     $                        WORK( 1 + (NB+IV)*N ), N )
*                 normalize vectors
                  DO K = IV, NB
                     II = IZAMAX( N, WORK( 1 + (NB+K)*N ), 1 )
                     REMAX = ONE / CABS1( WORK( II + (NB+K)*N ) )
                     CALL ZDSCAL( N, REMAX, WORK( 1 + (NB+K)*N ), 1 )
                  END DO
                  CALL ZLACPY( 'F', N, NB-IV+1,
     $                         WORK( 1 + (NB+IV)*N ), N,
     $                         VR( 1, KI ), LDVR )
                  IV = NB
               ELSE
                  IV = IV - 1
               END IF
            END IF
*
*           Restore the original diagonal elements of T.
*
            DO 70 K = 1, KI - 1
               T( K, K ) = WORK( K )
   70       CONTINUE
*
            IS = IS - 1
   80    CONTINUE
      END IF
*
      IF( LEFTV ) THEN
*
*        ============================================================
*        Compute left eigenvectors.
*
*        IV is index of column in current block.
*        Non-blocked version always uses IV=1;
*        blocked     version starts with IV=1, goes up to NB.
*        (Note the "0-th" column is used to store the original diagonal.)
         IV = 1
         IS = 1
         DO 130 KI = 1, N
*
            IF( SOMEV ) THEN
               IF( .NOT.SELECT( KI ) )
     $            GO TO 130
            END IF
            SMIN = MAX( ULP*( CABS1( T( KI, KI ) ) ), SMLNUM )
*
*           --------------------------------------------------------
*           Complex left eigenvector
*
            WORK( KI + IV*N ) = CONE
*
*           Form right-hand side.
*
            DO 90 K = KI + 1, N
               WORK( K + IV*N ) = -CONJG( T( KI, K ) )
   90       CONTINUE
*
*           Solve conjugate-transposed triangular system:
*           [ T(KI+1:N,KI+1:N) - T(KI,KI) ]**H * X = SCALE*WORK.
*
            DO 100 K = KI + 1, N
               T( K, K ) = T( K, K ) - T( KI, KI )
               IF( CABS1( T( K, K ) ).LT.SMIN )
     $            T( K, K ) = SMIN
  100       CONTINUE
*
            IF( KI.LT.N ) THEN
               CALL ZLATRS( 'Upper', 'Conjugate transpose', 'Non-unit',
     $                      'Y', N-KI, T( KI+1, KI+1 ), LDT,
     $                      WORK( KI+1 + IV*N ), SCALE, RWORK, INFO )
               WORK( KI + IV*N ) = SCALE
            END IF
*
*           Copy the vector x or Q*x to VL and normalize.
*
            IF( .NOT.OVER ) THEN
*              ------------------------------
*              no back-transform: copy x to VL and normalize.
               CALL ZCOPY( N-KI+1, WORK( KI + IV*N ), 1, VL(KI,IS), 1 )
*
               II = IZAMAX( N-KI+1, VL( KI, IS ), 1 ) + KI - 1
               REMAX = ONE / CABS1( VL( II, IS ) )
               CALL ZDSCAL( N-KI+1, REMAX, VL( KI, IS ), 1 )
*
               DO 110 K = 1, KI - 1
                  VL( K, IS ) = CZERO
  110          CONTINUE
*
            ELSE IF( NB.EQ.1 ) THEN
*              ------------------------------
*              version 1: back-transform each vector with GEMV, Q*x.
               IF( KI.LT.N )
     $            CALL ZGEMV( 'N', N, N-KI, CONE, VL( 1, KI+1 ), LDVL,
     $                        WORK( KI+1 + IV*N ), 1, DCMPLX( SCALE ),
     $                        VL( 1, KI ), 1 )
*
               II = IZAMAX( N, VL( 1, KI ), 1 )
               REMAX = ONE / CABS1( VL( II, KI ) )
               CALL ZDSCAL( N, REMAX, VL( 1, KI ), 1 )
*
            ELSE
*              ------------------------------
*              version 2: back-transform block of vectors with GEMM
*              zero out above vector
*              could go from KI-NV+1 to KI-1
               DO K = 1, KI - 1
                  WORK( K + IV*N ) = CZERO
               END DO
*
*              Columns 1:IV of work are valid vectors.
*              When the number of vectors stored reaches NB,
*              or if this was last vector, do the GEMM
               IF( (IV.EQ.NB) .OR. (KI.EQ.N) ) THEN
                  CALL ZGEMM( 'N', 'N', N, IV, N-KI+IV, CONE,
     $                        VL( 1, KI-IV+1 ), LDVL,
     $                        WORK( KI-IV+1 + (1)*N ), N,
     $                        CZERO,
     $                        WORK( 1 + (NB+1)*N ), N )
*                 normalize vectors
                  DO K = 1, IV
                     II = IZAMAX( N, WORK( 1 + (NB+K)*N ), 1 )
                     REMAX = ONE / CABS1( WORK( II + (NB+K)*N ) )
                     CALL ZDSCAL( N, REMAX, WORK( 1 + (NB+K)*N ), 1 )
                  END DO
                  CALL ZLACPY( 'F', N, IV,
     $                         WORK( 1 + (NB+1)*N ), N,
     $                         VL( 1, KI-IV+1 ), LDVL )
                  IV = 1
               ELSE
                  IV = IV + 1
               END IF
            END IF
*
*           Restore the original diagonal elements of T.
*
            DO 120 K = KI + 1, N
               T( K, K ) = WORK( K )
  120       CONTINUE
*
            IS = IS + 1
  130    CONTINUE
      END IF
*
      RETURN
*
*     End of ZTREVC3
*
      END
