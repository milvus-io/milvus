*> \brief \b ZGGHD3
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZGGHD3 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zgghd3.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zgghd3.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zgghd3.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZGGHD3( COMPQ, COMPZ, N, ILO, IHI, A, LDA, B, LDB, Q,
*                          LDQ, Z, LDZ, WORK, LWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          COMPQ, COMPZ
*       INTEGER            IHI, ILO, INFO, LDA, LDB, LDQ, LDZ, N, LWORK
*       ..
*       .. Array Arguments ..
*       COMPLEX*16         A( LDA, * ), B( LDB, * ), Q( LDQ, * ),
*      $                   Z( LDZ, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZGGHD3 reduces a pair of complex matrices (A,B) to generalized upper
*> Hessenberg form using unitary transformations, where A is a
*> general matrix and B is upper triangular.  The form of the
*> generalized eigenvalue problem is
*>    A*x = lambda*B*x,
*> and B is typically made upper triangular by computing its QR
*> factorization and moving the unitary matrix Q to the left side
*> of the equation.
*>
*> This subroutine simultaneously reduces A to a Hessenberg matrix H:
*>    Q**H*A*Z = H
*> and transforms B to another upper triangular matrix T:
*>    Q**H*B*Z = T
*> in order to reduce the problem to its standard form
*>    H*y = lambda*T*y
*> where y = Z**H*x.
*>
*> The unitary matrices Q and Z are determined as products of Givens
*> rotations.  They may either be formed explicitly, or they may be
*> postmultiplied into input matrices Q1 and Z1, so that
*>      Q1 * A * Z1**H = (Q1*Q) * H * (Z1*Z)**H
*>      Q1 * B * Z1**H = (Q1*Q) * T * (Z1*Z)**H
*> If Q1 is the unitary matrix from the QR factorization of B in the
*> original equation A*x = lambda*B*x, then ZGGHD3 reduces the original
*> problem to generalized Hessenberg form.
*>
*> This is a blocked variant of CGGHRD, using matrix-matrix
*> multiplications for parts of the computation to enhance performance.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] COMPQ
*> \verbatim
*>          COMPQ is CHARACTER*1
*>          = 'N': do not compute Q;
*>          = 'I': Q is initialized to the unit matrix, and the
*>                 unitary matrix Q is returned;
*>          = 'V': Q must contain a unitary matrix Q1 on entry,
*>                 and the product Q1*Q is returned.
*> \endverbatim
*>
*> \param[in] COMPZ
*> \verbatim
*>          COMPZ is CHARACTER*1
*>          = 'N': do not compute Z;
*>          = 'I': Z is initialized to the unit matrix, and the
*>                 unitary matrix Z is returned;
*>          = 'V': Z must contain a unitary matrix Z1 on entry,
*>                 and the product Z1*Z is returned.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrices A and B.  N >= 0.
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
*>
*>          ILO and IHI mark the rows and columns of A which are to be
*>          reduced.  It is assumed that A is already upper triangular
*>          in rows and columns 1:ILO-1 and IHI+1:N.  ILO and IHI are
*>          normally set by a previous call to ZGGBAL; otherwise they
*>          should be set to 1 and N respectively.
*>          1 <= ILO <= IHI <= N, if N > 0; ILO=1 and IHI=0, if N=0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA, N)
*>          On entry, the N-by-N general matrix to be reduced.
*>          On exit, the upper triangle and the first subdiagonal of A
*>          are overwritten with the upper Hessenberg matrix H, and the
*>          rest is set to zero.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX*16 array, dimension (LDB, N)
*>          On entry, the N-by-N upper triangular matrix B.
*>          On exit, the upper triangular matrix T = Q**H B Z.  The
*>          elements below the diagonal are set to zero.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the array B.  LDB >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] Q
*> \verbatim
*>          Q is COMPLEX*16 array, dimension (LDQ, N)
*>          On entry, if COMPQ = 'V', the unitary matrix Q1, typically
*>          from the QR factorization of B.
*>          On exit, if COMPQ='I', the unitary matrix Q, and if
*>          COMPQ = 'V', the product Q1*Q.
*>          Not referenced if COMPQ='N'.
*> \endverbatim
*>
*> \param[in] LDQ
*> \verbatim
*>          LDQ is INTEGER
*>          The leading dimension of the array Q.
*>          LDQ >= N if COMPQ='V' or 'I'; LDQ >= 1 otherwise.
*> \endverbatim
*>
*> \param[in,out] Z
*> \verbatim
*>          Z is COMPLEX*16 array, dimension (LDZ, N)
*>          On entry, if COMPZ = 'V', the unitary matrix Z1.
*>          On exit, if COMPZ='I', the unitary matrix Z, and if
*>          COMPZ = 'V', the product Z1*Z.
*>          Not referenced if COMPZ='N'.
*> \endverbatim
*>
*> \param[in] LDZ
*> \verbatim
*>          LDZ is INTEGER
*>          The leading dimension of the array Z.
*>          LDZ >= N if COMPZ='V' or 'I'; LDZ >= 1 otherwise.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (LWORK)
*>          On exit, if INFO = 0, WORK(1) returns the optimal LWORK.
*> \endverbatim
*>
*> \param[in]  LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The length of the array WORK.  LWORK >= 1.
*>          For optimum performance LWORK >= 6*N*NB, where NB is the
*>          optimal blocksize.
*>
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal size of the WORK array, returns
*>          this value as the first entry of the WORK array, and no error
*>          message related to LWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit.
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
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
*> \date January 2015
*
*> \ingroup complex16OTHERcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  This routine reduces A to Hessenberg form and maintains B in
*>  using a blocked variant of Moler and Stewart's original algorithm,
*>  as described by Kagstrom, Kressner, Quintana-Orti, and Quintana-Orti
*>  (BIT 2008).
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE ZGGHD3( COMPQ, COMPZ, N, ILO, IHI, A, LDA, B, LDB, Q,
     $                   LDQ, Z, LDZ, WORK, LWORK, INFO )
*
*  -- LAPACK computational routine (version 3.8.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     January 2015
*
      IMPLICIT NONE
*
*     .. Scalar Arguments ..
      CHARACTER          COMPQ, COMPZ
      INTEGER            IHI, ILO, INFO, LDA, LDB, LDQ, LDZ, N, LWORK
*     ..
*     .. Array Arguments ..
      COMPLEX*16         A( LDA, * ), B( LDB, * ), Q( LDQ, * ),
     $                   Z( LDZ, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX*16         CONE, CZERO
      PARAMETER          ( CONE = ( 1.0D+0, 0.0D+0 ),
     $                     CZERO = ( 0.0D+0, 0.0D+0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            BLK22, INITQ, INITZ, LQUERY, WANTQ, WANTZ
      CHARACTER*1        COMPQ2, COMPZ2
      INTEGER            COLA, I, IERR, J, J0, JCOL, JJ, JROW, K,
     $                   KACC22, LEN, LWKOPT, N2NB, NB, NBLST, NBMIN,
     $                   NH, NNB, NX, PPW, PPWO, PW, TOP, TOPQ
      DOUBLE PRECISION   C
      COMPLEX*16         C1, C2, CTEMP, S, S1, S2, TEMP, TEMP1, TEMP2,
     $                   TEMP3
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV
      EXTERNAL           ILAENV, LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           ZGGHRD, ZLARTG, ZLASET, ZUNM22, ZROT, ZGEMM,
     $                   ZGEMV, ZTRMV, ZLACPY, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          DBLE, DCMPLX, DCONJG, MAX
*     ..
*     .. Executable Statements ..
*
*     Decode and test the input parameters.
*
      INFO = 0
      NB = ILAENV( 1, 'ZGGHD3', ' ', N, ILO, IHI, -1 )
      LWKOPT = MAX( 6*N*NB, 1 )
      WORK( 1 ) = DCMPLX( LWKOPT )
      INITQ = LSAME( COMPQ, 'I' )
      WANTQ = INITQ .OR. LSAME( COMPQ, 'V' )
      INITZ = LSAME( COMPZ, 'I' )
      WANTZ = INITZ .OR. LSAME( COMPZ, 'V' )
      LQUERY = ( LWORK.EQ.-1 )
*
      IF( .NOT.LSAME( COMPQ, 'N' ) .AND. .NOT.WANTQ ) THEN
         INFO = -1
      ELSE IF( .NOT.LSAME( COMPZ, 'N' ) .AND. .NOT.WANTZ ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( ILO.LT.1 ) THEN
         INFO = -4
      ELSE IF( IHI.GT.N .OR. IHI.LT.ILO-1 ) THEN
         INFO = -5
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -7
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -9
      ELSE IF( ( WANTQ .AND. LDQ.LT.N ) .OR. LDQ.LT.1 ) THEN
         INFO = -11
      ELSE IF( ( WANTZ .AND. LDZ.LT.N ) .OR. LDZ.LT.1 ) THEN
         INFO = -13
      ELSE IF( LWORK.LT.1 .AND. .NOT.LQUERY ) THEN
         INFO = -15
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZGGHD3', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Initialize Q and Z if desired.
*
      IF( INITQ )
     $   CALL ZLASET( 'All', N, N, CZERO, CONE, Q, LDQ )
      IF( INITZ )
     $   CALL ZLASET( 'All', N, N, CZERO, CONE, Z, LDZ )
*
*     Zero out lower triangle of B.
*
      IF( N.GT.1 )
     $   CALL ZLASET( 'Lower', N-1, N-1, CZERO, CZERO, B(2, 1), LDB )
*
*     Quick return if possible
*
      NH = IHI - ILO + 1
      IF( NH.LE.1 ) THEN
         WORK( 1 ) = CONE
         RETURN
      END IF
*
*     Determine the blocksize.
*
      NBMIN = ILAENV( 2, 'ZGGHD3', ' ', N, ILO, IHI, -1 )
      IF( NB.GT.1 .AND. NB.LT.NH ) THEN
*
*        Determine when to use unblocked instead of blocked code.
*
         NX = MAX( NB, ILAENV( 3, 'ZGGHD3', ' ', N, ILO, IHI, -1 ) )
         IF( NX.LT.NH ) THEN
*
*           Determine if workspace is large enough for blocked code.
*
            IF( LWORK.LT.LWKOPT ) THEN
*
*              Not enough workspace to use optimal NB:  determine the
*              minimum value of NB, and reduce NB or force use of
*              unblocked code.
*
               NBMIN = MAX( 2, ILAENV( 2, 'ZGGHD3', ' ', N, ILO, IHI,
     $                 -1 ) )
               IF( LWORK.GE.6*N*NBMIN ) THEN
                  NB = LWORK / ( 6*N )
               ELSE
                  NB = 1
               END IF
            END IF
         END IF
      END IF
*
      IF( NB.LT.NBMIN .OR. NB.GE.NH ) THEN
*
*        Use unblocked code below
*
         JCOL = ILO
*
      ELSE
*
*        Use blocked code
*
         KACC22 = ILAENV( 16, 'ZGGHD3', ' ', N, ILO, IHI, -1 )
         BLK22 = KACC22.EQ.2
         DO JCOL = ILO, IHI-2, NB
            NNB = MIN( NB, IHI-JCOL-1 )
*
*           Initialize small unitary factors that will hold the
*           accumulated Givens rotations in workspace.
*           N2NB   denotes the number of 2*NNB-by-2*NNB factors
*           NBLST  denotes the (possibly smaller) order of the last
*                  factor.
*
            N2NB = ( IHI-JCOL-1 ) / NNB - 1
            NBLST = IHI - JCOL - N2NB*NNB
            CALL ZLASET( 'All', NBLST, NBLST, CZERO, CONE, WORK, NBLST )
            PW = NBLST * NBLST + 1
            DO I = 1, N2NB
               CALL ZLASET( 'All', 2*NNB, 2*NNB, CZERO, CONE,
     $                      WORK( PW ), 2*NNB )
               PW = PW + 4*NNB*NNB
            END DO
*
*           Reduce columns JCOL:JCOL+NNB-1 of A to Hessenberg form.
*
            DO J = JCOL, JCOL+NNB-1
*
*              Reduce Jth column of A. Store cosines and sines in Jth
*              column of A and B, respectively.
*
               DO I = IHI, J+2, -1
                  TEMP = A( I-1, J )
                  CALL ZLARTG( TEMP, A( I, J ), C, S, A( I-1, J ) )
                  A( I, J ) = DCMPLX( C )
                  B( I, J ) = S
               END DO
*
*              Accumulate Givens rotations into workspace array.
*
               PPW  = ( NBLST + 1 )*( NBLST - 2 ) - J + JCOL + 1
               LEN  = 2 + J - JCOL
               JROW = J + N2NB*NNB + 2
               DO I = IHI, JROW, -1
                  CTEMP = A( I, J )
                  S = B( I, J )
                  DO JJ = PPW, PPW+LEN-1
                     TEMP = WORK( JJ + NBLST )
                     WORK( JJ + NBLST ) = CTEMP*TEMP - S*WORK( JJ )
                     WORK( JJ ) = DCONJG( S )*TEMP + CTEMP*WORK( JJ )
                  END DO
                  LEN = LEN + 1
                  PPW = PPW - NBLST - 1
               END DO
*
               PPWO = NBLST*NBLST + ( NNB+J-JCOL-1 )*2*NNB + NNB
               J0 = JROW - NNB
               DO JROW = J0, J+2, -NNB
                  PPW = PPWO
                  LEN  = 2 + J - JCOL
                  DO I = JROW+NNB-1, JROW, -1
                     CTEMP = A( I, J )
                     S = B( I, J )
                     DO JJ = PPW, PPW+LEN-1
                        TEMP = WORK( JJ + 2*NNB )
                        WORK( JJ + 2*NNB ) = CTEMP*TEMP - S*WORK( JJ )
                        WORK( JJ ) = DCONJG( S )*TEMP + CTEMP*WORK( JJ )
                     END DO
                     LEN = LEN + 1
                     PPW = PPW - 2*NNB - 1
                  END DO
                  PPWO = PPWO + 4*NNB*NNB
               END DO
*
*              TOP denotes the number of top rows in A and B that will
*              not be updated during the next steps.
*
               IF( JCOL.LE.2 ) THEN
                  TOP = 0
               ELSE
                  TOP = JCOL
               END IF
*
*              Propagate transformations through B and replace stored
*              left sines/cosines by right sines/cosines.
*
               DO JJ = N, J+1, -1
*
*                 Update JJth column of B.
*
                  DO I = MIN( JJ+1, IHI ), J+2, -1
                     CTEMP = A( I, J )
                     S = B( I, J )
                     TEMP = B( I, JJ )
                     B( I, JJ ) = CTEMP*TEMP - DCONJG( S )*B( I-1, JJ )
                     B( I-1, JJ ) = S*TEMP + CTEMP*B( I-1, JJ )
                  END DO
*
*                 Annihilate B( JJ+1, JJ ).
*
                  IF( JJ.LT.IHI ) THEN
                     TEMP = B( JJ+1, JJ+1 )
                     CALL ZLARTG( TEMP, B( JJ+1, JJ ), C, S,
     $                            B( JJ+1, JJ+1 ) )
                     B( JJ+1, JJ ) = CZERO
                     CALL ZROT( JJ-TOP, B( TOP+1, JJ+1 ), 1,
     $                          B( TOP+1, JJ ), 1, C, S )
                     A( JJ+1, J ) = DCMPLX( C )
                     B( JJ+1, J ) = -DCONJG( S )
                  END IF
               END DO
*
*              Update A by transformations from right.
*
               JJ = MOD( IHI-J-1, 3 )
               DO I = IHI-J-3, JJ+1, -3
                  CTEMP = A( J+1+I, J )
                  S = -B( J+1+I, J )
                  C1 = A( J+2+I, J )
                  S1 = -B( J+2+I, J )
                  C2 = A( J+3+I, J )
                  S2 = -B( J+3+I, J )
*
                  DO K = TOP+1, IHI
                     TEMP = A( K, J+I  )
                     TEMP1 = A( K, J+I+1 )
                     TEMP2 = A( K, J+I+2 )
                     TEMP3 = A( K, J+I+3 )
                     A( K, J+I+3 ) = C2*TEMP3 + DCONJG( S2 )*TEMP2
                     TEMP2 = -S2*TEMP3 + C2*TEMP2
                     A( K, J+I+2 ) = C1*TEMP2 + DCONJG( S1 )*TEMP1
                     TEMP1 = -S1*TEMP2 + C1*TEMP1
                     A( K, J+I+1 ) = CTEMP*TEMP1 + DCONJG( S )*TEMP
                     A( K, J+I ) = -S*TEMP1 + CTEMP*TEMP
                  END DO
               END DO
*
               IF( JJ.GT.0 ) THEN
                  DO I = JJ, 1, -1
                     C = DBLE( A( J+1+I, J ) )
                     CALL ZROT( IHI-TOP, A( TOP+1, J+I+1 ), 1,
     $                          A( TOP+1, J+I ), 1, C,
     $                          -DCONJG( B( J+1+I, J ) ) )
                  END DO
               END IF
*
*              Update (J+1)th column of A by transformations from left.
*
               IF ( J .LT. JCOL + NNB - 1 ) THEN
                  LEN  = 1 + J - JCOL
*
*                 Multiply with the trailing accumulated unitary
*                 matrix, which takes the form
*
*                        [  U11  U12  ]
*                    U = [            ],
*                        [  U21  U22  ]
*
*                 where U21 is a LEN-by-LEN matrix and U12 is lower
*                 triangular.
*
                  JROW = IHI - NBLST + 1
                  CALL ZGEMV( 'Conjugate', NBLST, LEN, CONE, WORK,
     $                        NBLST, A( JROW, J+1 ), 1, CZERO,
     $                        WORK( PW ), 1 )
                  PPW = PW + LEN
                  DO I = JROW, JROW+NBLST-LEN-1
                     WORK( PPW ) = A( I, J+1 )
                     PPW = PPW + 1
                  END DO
                  CALL ZTRMV( 'Lower', 'Conjugate', 'Non-unit',
     $                        NBLST-LEN, WORK( LEN*NBLST + 1 ), NBLST,
     $                        WORK( PW+LEN ), 1 )
                  CALL ZGEMV( 'Conjugate', LEN, NBLST-LEN, CONE,
     $                        WORK( (LEN+1)*NBLST - LEN + 1 ), NBLST,
     $                        A( JROW+NBLST-LEN, J+1 ), 1, CONE,
     $                        WORK( PW+LEN ), 1 )
                  PPW = PW
                  DO I = JROW, JROW+NBLST-1
                     A( I, J+1 ) = WORK( PPW )
                     PPW = PPW + 1
                  END DO
*
*                 Multiply with the other accumulated unitary
*                 matrices, which take the form
*
*                        [  U11  U12   0  ]
*                        [                ]
*                    U = [  U21  U22   0  ],
*                        [                ]
*                        [   0    0    I  ]
*
*                 where I denotes the (NNB-LEN)-by-(NNB-LEN) identity
*                 matrix, U21 is a LEN-by-LEN upper triangular matrix
*                 and U12 is an NNB-by-NNB lower triangular matrix.
*
                  PPWO = 1 + NBLST*NBLST
                  J0 = JROW - NNB
                  DO JROW = J0, JCOL+1, -NNB
                     PPW = PW + LEN
                     DO I = JROW, JROW+NNB-1
                        WORK( PPW ) = A( I, J+1 )
                        PPW = PPW + 1
                     END DO
                     PPW = PW
                     DO I = JROW+NNB, JROW+NNB+LEN-1
                        WORK( PPW ) = A( I, J+1 )
                        PPW = PPW + 1
                     END DO
                     CALL ZTRMV( 'Upper', 'Conjugate', 'Non-unit', LEN,
     $                           WORK( PPWO + NNB ), 2*NNB, WORK( PW ),
     $                           1 )
                     CALL ZTRMV( 'Lower', 'Conjugate', 'Non-unit', NNB,
     $                           WORK( PPWO + 2*LEN*NNB ),
     $                           2*NNB, WORK( PW + LEN ), 1 )
                     CALL ZGEMV( 'Conjugate', NNB, LEN, CONE,
     $                           WORK( PPWO ), 2*NNB, A( JROW, J+1 ), 1,
     $                           CONE, WORK( PW ), 1 )
                     CALL ZGEMV( 'Conjugate', LEN, NNB, CONE,
     $                           WORK( PPWO + 2*LEN*NNB + NNB ), 2*NNB,
     $                           A( JROW+NNB, J+1 ), 1, CONE,
     $                           WORK( PW+LEN ), 1 )
                     PPW = PW
                     DO I = JROW, JROW+LEN+NNB-1
                        A( I, J+1 ) = WORK( PPW )
                        PPW = PPW + 1
                     END DO
                     PPWO = PPWO + 4*NNB*NNB
                  END DO
               END IF
            END DO
*
*           Apply accumulated unitary matrices to A.
*
            COLA = N - JCOL - NNB + 1
            J = IHI - NBLST + 1
            CALL ZGEMM( 'Conjugate', 'No Transpose', NBLST,
     $                  COLA, NBLST, CONE, WORK, NBLST,
     $                  A( J, JCOL+NNB ), LDA, CZERO, WORK( PW ),
     $                  NBLST )
            CALL ZLACPY( 'All', NBLST, COLA, WORK( PW ), NBLST,
     $                   A( J, JCOL+NNB ), LDA )
            PPWO = NBLST*NBLST + 1
            J0 = J - NNB
            DO J = J0, JCOL+1, -NNB
               IF ( BLK22 ) THEN
*
*                 Exploit the structure of
*
*                        [  U11  U12  ]
*                    U = [            ]
*                        [  U21  U22  ],
*
*                 where all blocks are NNB-by-NNB, U21 is upper
*                 triangular and U12 is lower triangular.
*
                  CALL ZUNM22( 'Left', 'Conjugate', 2*NNB, COLA, NNB,
     $                         NNB, WORK( PPWO ), 2*NNB,
     $                         A( J, JCOL+NNB ), LDA, WORK( PW ),
     $                         LWORK-PW+1, IERR )
               ELSE
*
*                 Ignore the structure of U.
*
                  CALL ZGEMM( 'Conjugate', 'No Transpose', 2*NNB,
     $                        COLA, 2*NNB, CONE, WORK( PPWO ), 2*NNB,
     $                        A( J, JCOL+NNB ), LDA, CZERO, WORK( PW ),
     $                        2*NNB )
                  CALL ZLACPY( 'All', 2*NNB, COLA, WORK( PW ), 2*NNB,
     $                         A( J, JCOL+NNB ), LDA )
               END IF
               PPWO = PPWO + 4*NNB*NNB
            END DO
*
*           Apply accumulated unitary matrices to Q.
*
            IF( WANTQ ) THEN
               J = IHI - NBLST + 1
               IF ( INITQ ) THEN
                  TOPQ = MAX( 2, J - JCOL + 1 )
                  NH  = IHI - TOPQ + 1
               ELSE
                  TOPQ = 1
                  NH = N
               END IF
               CALL ZGEMM( 'No Transpose', 'No Transpose', NH,
     $                     NBLST, NBLST, CONE, Q( TOPQ, J ), LDQ,
     $                     WORK, NBLST, CZERO, WORK( PW ), NH )
               CALL ZLACPY( 'All', NH, NBLST, WORK( PW ), NH,
     $                      Q( TOPQ, J ), LDQ )
               PPWO = NBLST*NBLST + 1
               J0 = J - NNB
               DO J = J0, JCOL+1, -NNB
                  IF ( INITQ ) THEN
                     TOPQ = MAX( 2, J - JCOL + 1 )
                     NH  = IHI - TOPQ + 1
                  END IF
                  IF ( BLK22 ) THEN
*
*                    Exploit the structure of U.
*
                     CALL ZUNM22( 'Right', 'No Transpose', NH, 2*NNB,
     $                            NNB, NNB, WORK( PPWO ), 2*NNB,
     $                            Q( TOPQ, J ), LDQ, WORK( PW ),
     $                            LWORK-PW+1, IERR )
                  ELSE
*
*                    Ignore the structure of U.
*
                     CALL ZGEMM( 'No Transpose', 'No Transpose', NH,
     $                           2*NNB, 2*NNB, CONE, Q( TOPQ, J ), LDQ,
     $                           WORK( PPWO ), 2*NNB, CZERO, WORK( PW ),
     $                           NH )
                     CALL ZLACPY( 'All', NH, 2*NNB, WORK( PW ), NH,
     $                            Q( TOPQ, J ), LDQ )
                  END IF
                  PPWO = PPWO + 4*NNB*NNB
               END DO
            END IF
*
*           Accumulate right Givens rotations if required.
*
            IF ( WANTZ .OR. TOP.GT.0 ) THEN
*
*              Initialize small unitary factors that will hold the
*              accumulated Givens rotations in workspace.
*
               CALL ZLASET( 'All', NBLST, NBLST, CZERO, CONE, WORK,
     $                      NBLST )
               PW = NBLST * NBLST + 1
               DO I = 1, N2NB
                  CALL ZLASET( 'All', 2*NNB, 2*NNB, CZERO, CONE,
     $                         WORK( PW ), 2*NNB )
                  PW = PW + 4*NNB*NNB
               END DO
*
*              Accumulate Givens rotations into workspace array.
*
               DO J = JCOL, JCOL+NNB-1
                  PPW  = ( NBLST + 1 )*( NBLST - 2 ) - J + JCOL + 1
                  LEN  = 2 + J - JCOL
                  JROW = J + N2NB*NNB + 2
                  DO I = IHI, JROW, -1
                     CTEMP = A( I, J )
                     A( I, J ) = CZERO
                     S = B( I, J )
                     B( I, J ) = CZERO
                     DO JJ = PPW, PPW+LEN-1
                        TEMP = WORK( JJ + NBLST )
                        WORK( JJ + NBLST ) = CTEMP*TEMP -
     $                                       DCONJG( S )*WORK( JJ )
                        WORK( JJ ) = S*TEMP + CTEMP*WORK( JJ )
                     END DO
                     LEN = LEN + 1
                     PPW = PPW - NBLST - 1
                  END DO
*
                  PPWO = NBLST*NBLST + ( NNB+J-JCOL-1 )*2*NNB + NNB
                  J0 = JROW - NNB
                  DO JROW = J0, J+2, -NNB
                     PPW = PPWO
                     LEN  = 2 + J - JCOL
                     DO I = JROW+NNB-1, JROW, -1
                        CTEMP = A( I, J )
                        A( I, J ) = CZERO
                        S = B( I, J )
                        B( I, J ) = CZERO
                        DO JJ = PPW, PPW+LEN-1
                           TEMP = WORK( JJ + 2*NNB )
                           WORK( JJ + 2*NNB ) = CTEMP*TEMP -
     $                                          DCONJG( S )*WORK( JJ )
                           WORK( JJ ) = S*TEMP + CTEMP*WORK( JJ )
                        END DO
                        LEN = LEN + 1
                        PPW = PPW - 2*NNB - 1
                     END DO
                     PPWO = PPWO + 4*NNB*NNB
                  END DO
               END DO
            ELSE
*
               CALL ZLASET( 'Lower', IHI - JCOL - 1, NNB, CZERO, CZERO,
     $                      A( JCOL + 2, JCOL ), LDA )
               CALL ZLASET( 'Lower', IHI - JCOL - 1, NNB, CZERO, CZERO,
     $                      B( JCOL + 2, JCOL ), LDB )
            END IF
*
*           Apply accumulated unitary matrices to A and B.
*
            IF ( TOP.GT.0 ) THEN
               J = IHI - NBLST + 1
               CALL ZGEMM( 'No Transpose', 'No Transpose', TOP,
     $                     NBLST, NBLST, CONE, A( 1, J ), LDA,
     $                     WORK, NBLST, CZERO, WORK( PW ), TOP )
               CALL ZLACPY( 'All', TOP, NBLST, WORK( PW ), TOP,
     $                      A( 1, J ), LDA )
               PPWO = NBLST*NBLST + 1
               J0 = J - NNB
               DO J = J0, JCOL+1, -NNB
                  IF ( BLK22 ) THEN
*
*                    Exploit the structure of U.
*
                     CALL ZUNM22( 'Right', 'No Transpose', TOP, 2*NNB,
     $                            NNB, NNB, WORK( PPWO ), 2*NNB,
     $                            A( 1, J ), LDA, WORK( PW ),
     $                            LWORK-PW+1, IERR )
                  ELSE
*
*                    Ignore the structure of U.
*
                     CALL ZGEMM( 'No Transpose', 'No Transpose', TOP,
     $                           2*NNB, 2*NNB, CONE, A( 1, J ), LDA,
     $                           WORK( PPWO ), 2*NNB, CZERO,
     $                           WORK( PW ), TOP )
                     CALL ZLACPY( 'All', TOP, 2*NNB, WORK( PW ), TOP,
     $                            A( 1, J ), LDA )
                  END IF
                  PPWO = PPWO + 4*NNB*NNB
               END DO
*
               J = IHI - NBLST + 1
               CALL ZGEMM( 'No Transpose', 'No Transpose', TOP,
     $                     NBLST, NBLST, CONE, B( 1, J ), LDB,
     $                     WORK, NBLST, CZERO, WORK( PW ), TOP )
               CALL ZLACPY( 'All', TOP, NBLST, WORK( PW ), TOP,
     $                      B( 1, J ), LDB )
               PPWO = NBLST*NBLST + 1
               J0 = J - NNB
               DO J = J0, JCOL+1, -NNB
                  IF ( BLK22 ) THEN
*
*                    Exploit the structure of U.
*
                     CALL ZUNM22( 'Right', 'No Transpose', TOP, 2*NNB,
     $                            NNB, NNB, WORK( PPWO ), 2*NNB,
     $                            B( 1, J ), LDB, WORK( PW ),
     $                            LWORK-PW+1, IERR )
                  ELSE
*
*                    Ignore the structure of U.
*
                     CALL ZGEMM( 'No Transpose', 'No Transpose', TOP,
     $                           2*NNB, 2*NNB, CONE, B( 1, J ), LDB,
     $                           WORK( PPWO ), 2*NNB, CZERO,
     $                           WORK( PW ), TOP )
                     CALL ZLACPY( 'All', TOP, 2*NNB, WORK( PW ), TOP,
     $                            B( 1, J ), LDB )
                  END IF
                  PPWO = PPWO + 4*NNB*NNB
               END DO
            END IF
*
*           Apply accumulated unitary matrices to Z.
*
            IF( WANTZ ) THEN
               J = IHI - NBLST + 1
               IF ( INITQ ) THEN
                  TOPQ = MAX( 2, J - JCOL + 1 )
                  NH  = IHI - TOPQ + 1
               ELSE
                  TOPQ = 1
                  NH = N
               END IF
               CALL ZGEMM( 'No Transpose', 'No Transpose', NH,
     $                     NBLST, NBLST, CONE, Z( TOPQ, J ), LDZ,
     $                     WORK, NBLST, CZERO, WORK( PW ), NH )
               CALL ZLACPY( 'All', NH, NBLST, WORK( PW ), NH,
     $                      Z( TOPQ, J ), LDZ )
               PPWO = NBLST*NBLST + 1
               J0 = J - NNB
               DO J = J0, JCOL+1, -NNB
                     IF ( INITQ ) THEN
                     TOPQ = MAX( 2, J - JCOL + 1 )
                     NH  = IHI - TOPQ + 1
                  END IF
                  IF ( BLK22 ) THEN
*
*                    Exploit the structure of U.
*
                     CALL ZUNM22( 'Right', 'No Transpose', NH, 2*NNB,
     $                            NNB, NNB, WORK( PPWO ), 2*NNB,
     $                            Z( TOPQ, J ), LDZ, WORK( PW ),
     $                            LWORK-PW+1, IERR )
                  ELSE
*
*                    Ignore the structure of U.
*
                     CALL ZGEMM( 'No Transpose', 'No Transpose', NH,
     $                           2*NNB, 2*NNB, CONE, Z( TOPQ, J ), LDZ,
     $                           WORK( PPWO ), 2*NNB, CZERO, WORK( PW ),
     $                           NH )
                     CALL ZLACPY( 'All', NH, 2*NNB, WORK( PW ), NH,
     $                            Z( TOPQ, J ), LDZ )
                  END IF
                  PPWO = PPWO + 4*NNB*NNB
               END DO
            END IF
         END DO
      END IF
*
*     Use unblocked code to reduce the rest of the matrix
*     Avoid re-initialization of modified Q and Z.
*
      COMPQ2 = COMPQ
      COMPZ2 = COMPZ
      IF ( JCOL.NE.ILO ) THEN
         IF ( WANTQ )
     $      COMPQ2 = 'V'
         IF ( WANTZ )
     $      COMPZ2 = 'V'
      END IF
*
      IF ( JCOL.LT.IHI )
     $   CALL ZGGHRD( COMPQ2, COMPZ2, N, JCOL, IHI, A, LDA, B, LDB, Q,
     $                LDQ, Z, LDZ, IERR )
      WORK( 1 ) = DCMPLX( LWKOPT )
*
      RETURN
*
*     End of ZGGHD3
*
      END
