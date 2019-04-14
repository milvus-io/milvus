*> \brief \b ZLAHQR computes the eigenvalues and Schur factorization of an upper Hessenberg matrix, using the double-shift/single-shift QR algorithm.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZLAHQR + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zlahqr.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zlahqr.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zlahqr.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZLAHQR( WANTT, WANTZ, N, ILO, IHI, H, LDH, W, ILOZ,
*                          IHIZ, Z, LDZ, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            IHI, IHIZ, ILO, ILOZ, INFO, LDH, LDZ, N
*       LOGICAL            WANTT, WANTZ
*       ..
*       .. Array Arguments ..
*       COMPLEX*16         H( LDH, * ), W( * ), Z( LDZ, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    ZLAHQR is an auxiliary routine called by CHSEQR to update the
*>    eigenvalues and Schur decomposition already computed by CHSEQR, by
*>    dealing with the Hessenberg submatrix in rows and columns ILO to
*>    IHI.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] WANTT
*> \verbatim
*>          WANTT is LOGICAL
*>          = .TRUE. : the full Schur form T is required;
*>          = .FALSE.: only eigenvalues are required.
*> \endverbatim
*>
*> \param[in] WANTZ
*> \verbatim
*>          WANTZ is LOGICAL
*>          = .TRUE. : the matrix of Schur vectors Z is required;
*>          = .FALSE.: Schur vectors are not required.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix H.  N >= 0.
*> \endverbatim
*>
*> \param[in] ILO
*> \verbatim
*>          ILO is INTEGER
*> \endverbatim
*>
*> \param[in] IHI
*> \verbatim
*>          IHI is INTEGER
*>          It is assumed that H is already upper triangular in rows and
*>          columns IHI+1:N, and that H(ILO,ILO-1) = 0 (unless ILO = 1).
*>          ZLAHQR works primarily with the Hessenberg submatrix in rows
*>          and columns ILO to IHI, but applies transformations to all of
*>          H if WANTT is .TRUE..
*>          1 <= ILO <= max(1,IHI); IHI <= N.
*> \endverbatim
*>
*> \param[in,out] H
*> \verbatim
*>          H is COMPLEX*16 array, dimension (LDH,N)
*>          On entry, the upper Hessenberg matrix H.
*>          On exit, if INFO is zero and if WANTT is .TRUE., then H
*>          is upper triangular in rows and columns ILO:IHI.  If INFO
*>          is zero and if WANTT is .FALSE., then the contents of H
*>          are unspecified on exit.  The output state of H in case
*>          INF is positive is below under the description of INFO.
*> \endverbatim
*>
*> \param[in] LDH
*> \verbatim
*>          LDH is INTEGER
*>          The leading dimension of the array H. LDH >= max(1,N).
*> \endverbatim
*>
*> \param[out] W
*> \verbatim
*>          W is COMPLEX*16 array, dimension (N)
*>          The computed eigenvalues ILO to IHI are stored in the
*>          corresponding elements of W. If WANTT is .TRUE., the
*>          eigenvalues are stored in the same order as on the diagonal
*>          of the Schur form returned in H, with W(i) = H(i,i).
*> \endverbatim
*>
*> \param[in] ILOZ
*> \verbatim
*>          ILOZ is INTEGER
*> \endverbatim
*>
*> \param[in] IHIZ
*> \verbatim
*>          IHIZ is INTEGER
*>          Specify the rows of Z to which transformations must be
*>          applied if WANTZ is .TRUE..
*>          1 <= ILOZ <= ILO; IHI <= IHIZ <= N.
*> \endverbatim
*>
*> \param[in,out] Z
*> \verbatim
*>          Z is COMPLEX*16 array, dimension (LDZ,N)
*>          If WANTZ is .TRUE., on entry Z must contain the current
*>          matrix Z of transformations accumulated by CHSEQR, and on
*>          exit Z has been updated; transformations are applied only to
*>          the submatrix Z(ILOZ:IHIZ,ILO:IHI).
*>          If WANTZ is .FALSE., Z is not referenced.
*> \endverbatim
*>
*> \param[in] LDZ
*> \verbatim
*>          LDZ is INTEGER
*>          The leading dimension of the array Z. LDZ >= max(1,N).
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>           =   0: successful exit
*>          .GT. 0: if INFO = i, ZLAHQR failed to compute all the
*>                  eigenvalues ILO to IHI in a total of 30 iterations
*>                  per eigenvalue; elements i+1:ihi of W contain
*>                  those eigenvalues which have been successfully
*>                  computed.
*>
*>                  If INFO .GT. 0 and WANTT is .FALSE., then on exit,
*>                  the remaining unconverged eigenvalues are the
*>                  eigenvalues of the upper Hessenberg matrix
*>                  rows and columns ILO thorugh INFO of the final,
*>                  output value of H.
*>
*>                  If INFO .GT. 0 and WANTT is .TRUE., then on exit
*>          (*)       (initial value of H)*U  = U*(final value of H)
*>                  where U is an orthognal matrix.    The final
*>                  value of H is upper Hessenberg and triangular in
*>                  rows and columns INFO+1 through IHI.
*>
*>                  If INFO .GT. 0 and WANTZ is .TRUE., then on exit
*>                      (final value of Z)  = (initial value of Z)*U
*>                  where U is the orthogonal matrix in (*)
*>                  (regardless of the value of WANTT.)
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
*> \verbatim
*>
*>     02-96 Based on modifications by
*>     David Day, Sandia National Laboratory, USA
*>
*>     12-04 Further modifications by
*>     Ralph Byers, University of Kansas, USA
*>     This is a modified version of ZLAHQR from LAPACK version 3.0.
*>     It is (1) more robust against overflow and underflow and
*>     (2) adopts the more conservative Ahues & Tisseur stopping
*>     criterion (LAWN 122, 1997).
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE ZLAHQR( WANTT, WANTZ, N, ILO, IHI, H, LDH, W, ILOZ,
     $                   IHIZ, Z, LDZ, INFO )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            IHI, IHIZ, ILO, ILOZ, INFO, LDH, LDZ, N
      LOGICAL            WANTT, WANTZ
*     ..
*     .. Array Arguments ..
      COMPLEX*16         H( LDH, * ), W( * ), Z( LDZ, * )
*     ..
*
*  =========================================================
*
*     .. Parameters ..
      COMPLEX*16         ZERO, ONE
      PARAMETER          ( ZERO = ( 0.0d0, 0.0d0 ),
     $                   ONE = ( 1.0d0, 0.0d0 ) )
      DOUBLE PRECISION   RZERO, RONE, HALF
      PARAMETER          ( RZERO = 0.0d0, RONE = 1.0d0, HALF = 0.5d0 )
      DOUBLE PRECISION   DAT1
      PARAMETER          ( DAT1 = 3.0d0 / 4.0d0 )
*     ..
*     .. Local Scalars ..
      COMPLEX*16         CDUM, H11, H11S, H22, SC, SUM, T, T1, TEMP, U,
     $                   V2, X, Y
      DOUBLE PRECISION   AA, AB, BA, BB, H10, H21, RTEMP, S, SAFMAX,
     $                   SAFMIN, SMLNUM, SX, T2, TST, ULP
      INTEGER            I, I1, I2, ITS, ITMAX, J, JHI, JLO, K, L, M,
     $                   NH, NZ
*     ..
*     .. Local Arrays ..
      COMPLEX*16         V( 2 )
*     ..
*     .. External Functions ..
      COMPLEX*16         ZLADIV
      DOUBLE PRECISION   DLAMCH
      EXTERNAL           ZLADIV, DLAMCH
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLABAD, ZCOPY, ZLARFG, ZSCAL
*     ..
*     .. Statement Functions ..
      DOUBLE PRECISION   CABS1
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, DCONJG, DIMAG, MAX, MIN, SQRT
*     ..
*     .. Statement Function definitions ..
      CABS1( CDUM ) = ABS( DBLE( CDUM ) ) + ABS( DIMAG( CDUM ) )
*     ..
*     .. Executable Statements ..
*
      INFO = 0
*
*     Quick return if possible
*
      IF( N.EQ.0 )
     $   RETURN
      IF( ILO.EQ.IHI ) THEN
         W( ILO ) = H( ILO, ILO )
         RETURN
      END IF
*
*     ==== clear out the trash ====
      DO 10 J = ILO, IHI - 3
         H( J+2, J ) = ZERO
         H( J+3, J ) = ZERO
   10 CONTINUE
      IF( ILO.LE.IHI-2 )
     $   H( IHI, IHI-2 ) = ZERO
*     ==== ensure that subdiagonal entries are real ====
      IF( WANTT ) THEN
         JLO = 1
         JHI = N
      ELSE
         JLO = ILO
         JHI = IHI
      END IF
      DO 20 I = ILO + 1, IHI
         IF( DIMAG( H( I, I-1 ) ).NE.RZERO ) THEN
*           ==== The following redundant normalization
*           .    avoids problems with both gradual and
*           .    sudden underflow in ABS(H(I,I-1)) ====
            SC = H( I, I-1 ) / CABS1( H( I, I-1 ) )
            SC = DCONJG( SC ) / ABS( SC )
            H( I, I-1 ) = ABS( H( I, I-1 ) )
            CALL ZSCAL( JHI-I+1, SC, H( I, I ), LDH )
            CALL ZSCAL( MIN( JHI, I+1 )-JLO+1, DCONJG( SC ),
     $                  H( JLO, I ), 1 )
            IF( WANTZ )
     $         CALL ZSCAL( IHIZ-ILOZ+1, DCONJG( SC ), Z( ILOZ, I ), 1 )
         END IF
   20 CONTINUE
*
      NH = IHI - ILO + 1
      NZ = IHIZ - ILOZ + 1
*
*     Set machine-dependent constants for the stopping criterion.
*
      SAFMIN = DLAMCH( 'SAFE MINIMUM' )
      SAFMAX = RONE / SAFMIN
      CALL DLABAD( SAFMIN, SAFMAX )
      ULP = DLAMCH( 'PRECISION' )
      SMLNUM = SAFMIN*( DBLE( NH ) / ULP )
*
*     I1 and I2 are the indices of the first row and last column of H
*     to which transformations must be applied. If eigenvalues only are
*     being computed, I1 and I2 are set inside the main loop.
*
      IF( WANTT ) THEN
         I1 = 1
         I2 = N
      END IF
*
*     ITMAX is the total number of QR iterations allowed.
*
      ITMAX = 30 * MAX( 10, NH )
*
*     The main loop begins here. I is the loop index and decreases from
*     IHI to ILO in steps of 1. Each iteration of the loop works
*     with the active submatrix in rows and columns L to I.
*     Eigenvalues I+1 to IHI have already converged. Either L = ILO, or
*     H(L,L-1) is negligible so that the matrix splits.
*
      I = IHI
   30 CONTINUE
      IF( I.LT.ILO )
     $   GO TO 150
*
*     Perform QR iterations on rows and columns ILO to I until a
*     submatrix of order 1 splits off at the bottom because a
*     subdiagonal element has become negligible.
*
      L = ILO
      DO 130 ITS = 0, ITMAX
*
*        Look for a single small subdiagonal element.
*
         DO 40 K = I, L + 1, -1
            IF( CABS1( H( K, K-1 ) ).LE.SMLNUM )
     $         GO TO 50
            TST = CABS1( H( K-1, K-1 ) ) + CABS1( H( K, K ) )
            IF( TST.EQ.ZERO ) THEN
               IF( K-2.GE.ILO )
     $            TST = TST + ABS( DBLE( H( K-1, K-2 ) ) )
               IF( K+1.LE.IHI )
     $            TST = TST + ABS( DBLE( H( K+1, K ) ) )
            END IF
*           ==== The following is a conservative small subdiagonal
*           .    deflation criterion due to Ahues & Tisseur (LAWN 122,
*           .    1997). It has better mathematical foundation and
*           .    improves accuracy in some examples.  ====
            IF( ABS( DBLE( H( K, K-1 ) ) ).LE.ULP*TST ) THEN
               AB = MAX( CABS1( H( K, K-1 ) ), CABS1( H( K-1, K ) ) )
               BA = MIN( CABS1( H( K, K-1 ) ), CABS1( H( K-1, K ) ) )
               AA = MAX( CABS1( H( K, K ) ),
     $              CABS1( H( K-1, K-1 )-H( K, K ) ) )
               BB = MIN( CABS1( H( K, K ) ),
     $              CABS1( H( K-1, K-1 )-H( K, K ) ) )
               S = AA + AB
               IF( BA*( AB / S ).LE.MAX( SMLNUM,
     $             ULP*( BB*( AA / S ) ) ) )GO TO 50
            END IF
   40    CONTINUE
   50    CONTINUE
         L = K
         IF( L.GT.ILO ) THEN
*
*           H(L,L-1) is negligible
*
            H( L, L-1 ) = ZERO
         END IF
*
*        Exit from loop if a submatrix of order 1 has split off.
*
         IF( L.GE.I )
     $      GO TO 140
*
*        Now the active submatrix is in rows and columns L to I. If
*        eigenvalues only are being computed, only the active submatrix
*        need be transformed.
*
         IF( .NOT.WANTT ) THEN
            I1 = L
            I2 = I
         END IF
*
         IF( ITS.EQ.10 ) THEN
*
*           Exceptional shift.
*
            S = DAT1*ABS( DBLE( H( L+1, L ) ) )
            T = S + H( L, L )
         ELSE IF( ITS.EQ.20 ) THEN
*
*           Exceptional shift.
*
            S = DAT1*ABS( DBLE( H( I, I-1 ) ) )
            T = S + H( I, I )
         ELSE
*
*           Wilkinson's shift.
*
            T = H( I, I )
            U = SQRT( H( I-1, I ) )*SQRT( H( I, I-1 ) )
            S = CABS1( U )
            IF( S.NE.RZERO ) THEN
               X = HALF*( H( I-1, I-1 )-T )
               SX = CABS1( X )
               S = MAX( S, CABS1( X ) )
               Y = S*SQRT( ( X / S )**2+( U / S )**2 )
               IF( SX.GT.RZERO ) THEN
                  IF( DBLE( X / SX )*DBLE( Y )+DIMAG( X / SX )*
     $                DIMAG( Y ).LT.RZERO )Y = -Y
               END IF
               T = T - U*ZLADIV( U, ( X+Y ) )
            END IF
         END IF
*
*        Look for two consecutive small subdiagonal elements.
*
         DO 60 M = I - 1, L + 1, -1
*
*           Determine the effect of starting the single-shift QR
*           iteration at row M, and see if this would make H(M,M-1)
*           negligible.
*
            H11 = H( M, M )
            H22 = H( M+1, M+1 )
            H11S = H11 - T
            H21 = DBLE( H( M+1, M ) )
            S = CABS1( H11S ) + ABS( H21 )
            H11S = H11S / S
            H21 = H21 / S
            V( 1 ) = H11S
            V( 2 ) = H21
            H10 = DBLE( H( M, M-1 ) )
            IF( ABS( H10 )*ABS( H21 ).LE.ULP*
     $          ( CABS1( H11S )*( CABS1( H11 )+CABS1( H22 ) ) ) )
     $          GO TO 70
   60    CONTINUE
         H11 = H( L, L )
         H22 = H( L+1, L+1 )
         H11S = H11 - T
         H21 = DBLE( H( L+1, L ) )
         S = CABS1( H11S ) + ABS( H21 )
         H11S = H11S / S
         H21 = H21 / S
         V( 1 ) = H11S
         V( 2 ) = H21
   70    CONTINUE
*
*        Single-shift QR step
*
         DO 120 K = M, I - 1
*
*           The first iteration of this loop determines a reflection G
*           from the vector V and applies it from left and right to H,
*           thus creating a nonzero bulge below the subdiagonal.
*
*           Each subsequent iteration determines a reflection G to
*           restore the Hessenberg form in the (K-1)th column, and thus
*           chases the bulge one step toward the bottom of the active
*           submatrix.
*
*           V(2) is always real before the call to ZLARFG, and hence
*           after the call T2 ( = T1*V(2) ) is also real.
*
            IF( K.GT.M )
     $         CALL ZCOPY( 2, H( K, K-1 ), 1, V, 1 )
            CALL ZLARFG( 2, V( 1 ), V( 2 ), 1, T1 )
            IF( K.GT.M ) THEN
               H( K, K-1 ) = V( 1 )
               H( K+1, K-1 ) = ZERO
            END IF
            V2 = V( 2 )
            T2 = DBLE( T1*V2 )
*
*           Apply G from the left to transform the rows of the matrix
*           in columns K to I2.
*
            DO 80 J = K, I2
               SUM = DCONJG( T1 )*H( K, J ) + T2*H( K+1, J )
               H( K, J ) = H( K, J ) - SUM
               H( K+1, J ) = H( K+1, J ) - SUM*V2
   80       CONTINUE
*
*           Apply G from the right to transform the columns of the
*           matrix in rows I1 to min(K+2,I).
*
            DO 90 J = I1, MIN( K+2, I )
               SUM = T1*H( J, K ) + T2*H( J, K+1 )
               H( J, K ) = H( J, K ) - SUM
               H( J, K+1 ) = H( J, K+1 ) - SUM*DCONJG( V2 )
   90       CONTINUE
*
            IF( WANTZ ) THEN
*
*              Accumulate transformations in the matrix Z
*
               DO 100 J = ILOZ, IHIZ
                  SUM = T1*Z( J, K ) + T2*Z( J, K+1 )
                  Z( J, K ) = Z( J, K ) - SUM
                  Z( J, K+1 ) = Z( J, K+1 ) - SUM*DCONJG( V2 )
  100          CONTINUE
            END IF
*
            IF( K.EQ.M .AND. M.GT.L ) THEN
*
*              If the QR step was started at row M > L because two
*              consecutive small subdiagonals were found, then extra
*              scaling must be performed to ensure that H(M,M-1) remains
*              real.
*
               TEMP = ONE - T1
               TEMP = TEMP / ABS( TEMP )
               H( M+1, M ) = H( M+1, M )*DCONJG( TEMP )
               IF( M+2.LE.I )
     $            H( M+2, M+1 ) = H( M+2, M+1 )*TEMP
               DO 110 J = M, I
                  IF( J.NE.M+1 ) THEN
                     IF( I2.GT.J )
     $                  CALL ZSCAL( I2-J, TEMP, H( J, J+1 ), LDH )
                     CALL ZSCAL( J-I1, DCONJG( TEMP ), H( I1, J ), 1 )
                     IF( WANTZ ) THEN
                        CALL ZSCAL( NZ, DCONJG( TEMP ), Z( ILOZ, J ),
     $                              1 )
                     END IF
                  END IF
  110          CONTINUE
            END IF
  120    CONTINUE
*
*        Ensure that H(I,I-1) is real.
*
         TEMP = H( I, I-1 )
         IF( DIMAG( TEMP ).NE.RZERO ) THEN
            RTEMP = ABS( TEMP )
            H( I, I-1 ) = RTEMP
            TEMP = TEMP / RTEMP
            IF( I2.GT.I )
     $         CALL ZSCAL( I2-I, DCONJG( TEMP ), H( I, I+1 ), LDH )
            CALL ZSCAL( I-I1, TEMP, H( I1, I ), 1 )
            IF( WANTZ ) THEN
               CALL ZSCAL( NZ, TEMP, Z( ILOZ, I ), 1 )
            END IF
         END IF
*
  130 CONTINUE
*
*     Failure to converge in remaining number of iterations
*
      INFO = I
      RETURN
*
  140 CONTINUE
*
*     H(I,I-1) is negligible: one eigenvalue has converged.
*
      W( I ) = H( I, I )
*
*     return to start of the main loop with new value of I.
*
      I = L - 1
      GO TO 30
*
  150 CONTINUE
      RETURN
*
*     End of ZLAHQR
*
      END
