*> \brief \b DTGSY2 solves the generalized Sylvester equation (unblocked algorithm).
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DTGSY2 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/dtgsy2.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/dtgsy2.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/dtgsy2.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE DTGSY2( TRANS, IJOB, M, N, A, LDA, B, LDB, C, LDC, D,
*                          LDD, E, LDE, F, LDF, SCALE, RDSUM, RDSCAL,
*                          IWORK, PQ, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          TRANS
*       INTEGER            IJOB, INFO, LDA, LDB, LDC, LDD, LDE, LDF, M, N,
*      $                   PQ
*       DOUBLE PRECISION   RDSCAL, RDSUM, SCALE
*       ..
*       .. Array Arguments ..
*       INTEGER            IWORK( * )
*       DOUBLE PRECISION   A( LDA, * ), B( LDB, * ), C( LDC, * ),
*      $                   D( LDD, * ), E( LDE, * ), F( LDF, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DTGSY2 solves the generalized Sylvester equation:
*>
*>             A * R - L * B = scale * C                (1)
*>             D * R - L * E = scale * F,
*>
*> using Level 1 and 2 BLAS. where R and L are unknown M-by-N matrices,
*> (A, D), (B, E) and (C, F) are given matrix pairs of size M-by-M,
*> N-by-N and M-by-N, respectively, with real entries. (A, D) and (B, E)
*> must be in generalized Schur canonical form, i.e. A, B are upper
*> quasi triangular and D, E are upper triangular. The solution (R, L)
*> overwrites (C, F). 0 <= SCALE <= 1 is an output scaling factor
*> chosen to avoid overflow.
*>
*> In matrix notation solving equation (1) corresponds to solve
*> Z*x = scale*b, where Z is defined as
*>
*>        Z = [ kron(In, A)  -kron(B**T, Im) ]             (2)
*>            [ kron(In, D)  -kron(E**T, Im) ],
*>
*> Ik is the identity matrix of size k and X**T is the transpose of X.
*> kron(X, Y) is the Kronecker product between the matrices X and Y.
*> In the process of solving (1), we solve a number of such systems
*> where Dim(In), Dim(In) = 1 or 2.
*>
*> If TRANS = 'T', solve the transposed system Z**T*y = scale*b for y,
*> which is equivalent to solve for R and L in
*>
*>             A**T * R  + D**T * L   = scale * C           (3)
*>             R  * B**T + L  * E**T  = scale * -F
*>
*> This case is used to compute an estimate of Dif[(A, D), (B, E)] =
*> sigma_min(Z) using reverse communicaton with DLACON.
*>
*> DTGSY2 also (IJOB >= 1) contributes to the computation in DTGSYL
*> of an upper bound on the separation between to matrix pairs. Then
*> the input (A, D), (B, E) are sub-pencils of the matrix pair in
*> DTGSYL. See DTGSYL for details.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] TRANS
*> \verbatim
*>          TRANS is CHARACTER*1
*>          = 'N', solve the generalized Sylvester equation (1).
*>          = 'T': solve the 'transposed' system (3).
*> \endverbatim
*>
*> \param[in] IJOB
*> \verbatim
*>          IJOB is INTEGER
*>          Specifies what kind of functionality to be performed.
*>          = 0: solve (1) only.
*>          = 1: A contribution from this subsystem to a Frobenius
*>               norm-based estimate of the separation between two matrix
*>               pairs is computed. (look ahead strategy is used).
*>          = 2: A contribution from this subsystem to a Frobenius
*>               norm-based estimate of the separation between two matrix
*>               pairs is computed. (DGECON on sub-systems is used.)
*>          Not referenced if TRANS = 'T'.
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          On entry, M specifies the order of A and D, and the row
*>          dimension of C, F, R and L.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          On entry, N specifies the order of B and E, and the column
*>          dimension of C, F, R and L.
*> \endverbatim
*>
*> \param[in] A
*> \verbatim
*>          A is DOUBLE PRECISION array, dimension (LDA, M)
*>          On entry, A contains an upper quasi triangular matrix.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the matrix A. LDA >= max(1, M).
*> \endverbatim
*>
*> \param[in] B
*> \verbatim
*>          B is DOUBLE PRECISION array, dimension (LDB, N)
*>          On entry, B contains an upper quasi triangular matrix.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of the matrix B. LDB >= max(1, N).
*> \endverbatim
*>
*> \param[in,out] C
*> \verbatim
*>          C is DOUBLE PRECISION array, dimension (LDC, N)
*>          On entry, C contains the right-hand-side of the first matrix
*>          equation in (1).
*>          On exit, if IJOB = 0, C has been overwritten by the
*>          solution R.
*> \endverbatim
*>
*> \param[in] LDC
*> \verbatim
*>          LDC is INTEGER
*>          The leading dimension of the matrix C. LDC >= max(1, M).
*> \endverbatim
*>
*> \param[in] D
*> \verbatim
*>          D is DOUBLE PRECISION array, dimension (LDD, M)
*>          On entry, D contains an upper triangular matrix.
*> \endverbatim
*>
*> \param[in] LDD
*> \verbatim
*>          LDD is INTEGER
*>          The leading dimension of the matrix D. LDD >= max(1, M).
*> \endverbatim
*>
*> \param[in] E
*> \verbatim
*>          E is DOUBLE PRECISION array, dimension (LDE, N)
*>          On entry, E contains an upper triangular matrix.
*> \endverbatim
*>
*> \param[in] LDE
*> \verbatim
*>          LDE is INTEGER
*>          The leading dimension of the matrix E. LDE >= max(1, N).
*> \endverbatim
*>
*> \param[in,out] F
*> \verbatim
*>          F is DOUBLE PRECISION array, dimension (LDF, N)
*>          On entry, F contains the right-hand-side of the second matrix
*>          equation in (1).
*>          On exit, if IJOB = 0, F has been overwritten by the
*>          solution L.
*> \endverbatim
*>
*> \param[in] LDF
*> \verbatim
*>          LDF is INTEGER
*>          The leading dimension of the matrix F. LDF >= max(1, M).
*> \endverbatim
*>
*> \param[out] SCALE
*> \verbatim
*>          SCALE is DOUBLE PRECISION
*>          On exit, 0 <= SCALE <= 1. If 0 < SCALE < 1, the solutions
*>          R and L (C and F on entry) will hold the solutions to a
*>          slightly perturbed system but the input matrices A, B, D and
*>          E have not been changed. If SCALE = 0, R and L will hold the
*>          solutions to the homogeneous system with C = F = 0. Normally,
*>          SCALE = 1.
*> \endverbatim
*>
*> \param[in,out] RDSUM
*> \verbatim
*>          RDSUM is DOUBLE PRECISION
*>          On entry, the sum of squares of computed contributions to
*>          the Dif-estimate under computation by DTGSYL, where the
*>          scaling factor RDSCAL (see below) has been factored out.
*>          On exit, the corresponding sum of squares updated with the
*>          contributions from the current sub-system.
*>          If TRANS = 'T' RDSUM is not touched.
*>          NOTE: RDSUM only makes sense when DTGSY2 is called by DTGSYL.
*> \endverbatim
*>
*> \param[in,out] RDSCAL
*> \verbatim
*>          RDSCAL is DOUBLE PRECISION
*>          On entry, scaling factor used to prevent overflow in RDSUM.
*>          On exit, RDSCAL is updated w.r.t. the current contributions
*>          in RDSUM.
*>          If TRANS = 'T', RDSCAL is not touched.
*>          NOTE: RDSCAL only makes sense when DTGSY2 is called by
*>                DTGSYL.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (M+N+2)
*> \endverbatim
*>
*> \param[out] PQ
*> \verbatim
*>          PQ is INTEGER
*>          On exit, the number of subsystems (of size 2-by-2, 4-by-4 and
*>          8-by-8) solved by this routine.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          On exit, if INFO is set to
*>            =0: Successful exit
*>            <0: If INFO = -i, the i-th argument had an illegal value.
*>            >0: The matrix pairs (A, D) and (B, E) have common or very
*>                close eigenvalues.
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
*> \ingroup doubleSYauxiliary
*
*> \par Contributors:
*  ==================
*>
*>     Bo Kagstrom and Peter Poromaa, Department of Computing Science,
*>     Umea University, S-901 87 Umea, Sweden.
*
*  =====================================================================
      SUBROUTINE DTGSY2( TRANS, IJOB, M, N, A, LDA, B, LDB, C, LDC, D,
     $                   LDD, E, LDE, F, LDF, SCALE, RDSUM, RDSCAL,
     $                   IWORK, PQ, INFO )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          TRANS
      INTEGER            IJOB, INFO, LDA, LDB, LDC, LDD, LDE, LDF, M, N,
     $                   PQ
      DOUBLE PRECISION   RDSCAL, RDSUM, SCALE
*     ..
*     .. Array Arguments ..
      INTEGER            IWORK( * )
      DOUBLE PRECISION   A( LDA, * ), B( LDB, * ), C( LDC, * ),
     $                   D( LDD, * ), E( LDE, * ), F( LDF, * )
*     ..
*
*  =====================================================================
*  Replaced various illegal calls to DCOPY by calls to DLASET.
*  Sven Hammarling, 27/5/02.
*
*     .. Parameters ..
      INTEGER            LDZ
      PARAMETER          ( LDZ = 8 )
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            NOTRAN
      INTEGER            I, IE, IERR, II, IS, ISP1, J, JE, JJ, JS, JSP1,
     $                   K, MB, NB, P, Q, ZDIM
      DOUBLE PRECISION   ALPHA, SCALOC
*     ..
*     .. Local Arrays ..
      INTEGER            IPIV( LDZ ), JPIV( LDZ )
      DOUBLE PRECISION   RHS( LDZ ), Z( LDZ, LDZ )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           DAXPY, DCOPY, DGEMM, DGEMV, DGER, DGESC2,
     $                   DGETC2, DLASET, DLATDF, DSCAL, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX
*     ..
*     .. Executable Statements ..
*
*     Decode and test input parameters
*
      INFO = 0
      IERR = 0
      NOTRAN = LSAME( TRANS, 'N' )
      IF( .NOT.NOTRAN .AND. .NOT.LSAME( TRANS, 'T' ) ) THEN
         INFO = -1
      ELSE IF( NOTRAN ) THEN
         IF( ( IJOB.LT.0 ) .OR. ( IJOB.GT.2 ) ) THEN
            INFO = -2
         END IF
      END IF
      IF( INFO.EQ.0 ) THEN
         IF( M.LE.0 ) THEN
            INFO = -3
         ELSE IF( N.LE.0 ) THEN
            INFO = -4
         ELSE IF( LDA.LT.MAX( 1, M ) ) THEN
            INFO = -6
         ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
            INFO = -8
         ELSE IF( LDC.LT.MAX( 1, M ) ) THEN
            INFO = -10
         ELSE IF( LDD.LT.MAX( 1, M ) ) THEN
            INFO = -12
         ELSE IF( LDE.LT.MAX( 1, N ) ) THEN
            INFO = -14
         ELSE IF( LDF.LT.MAX( 1, M ) ) THEN
            INFO = -16
         END IF
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'DTGSY2', -INFO )
         RETURN
      END IF
*
*     Determine block structure of A
*
      PQ = 0
      P = 0
      I = 1
   10 CONTINUE
      IF( I.GT.M )
     $   GO TO 20
      P = P + 1
      IWORK( P ) = I
      IF( I.EQ.M )
     $   GO TO 20
      IF( A( I+1, I ).NE.ZERO ) THEN
         I = I + 2
      ELSE
         I = I + 1
      END IF
      GO TO 10
   20 CONTINUE
      IWORK( P+1 ) = M + 1
*
*     Determine block structure of B
*
      Q = P + 1
      J = 1
   30 CONTINUE
      IF( J.GT.N )
     $   GO TO 40
      Q = Q + 1
      IWORK( Q ) = J
      IF( J.EQ.N )
     $   GO TO 40
      IF( B( J+1, J ).NE.ZERO ) THEN
         J = J + 2
      ELSE
         J = J + 1
      END IF
      GO TO 30
   40 CONTINUE
      IWORK( Q+1 ) = N + 1
      PQ = P*( Q-P-1 )
*
      IF( NOTRAN ) THEN
*
*        Solve (I, J) - subsystem
*           A(I, I) * R(I, J) - L(I, J) * B(J, J) = C(I, J)
*           D(I, I) * R(I, J) - L(I, J) * E(J, J) = F(I, J)
*        for I = P, P - 1, ..., 1; J = 1, 2, ..., Q
*
         SCALE = ONE
         SCALOC = ONE
         DO 120 J = P + 2, Q
            JS = IWORK( J )
            JSP1 = JS + 1
            JE = IWORK( J+1 ) - 1
            NB = JE - JS + 1
            DO 110 I = P, 1, -1
*
               IS = IWORK( I )
               ISP1 = IS + 1
               IE = IWORK( I+1 ) - 1
               MB = IE - IS + 1
               ZDIM = MB*NB*2
*
               IF( ( MB.EQ.1 ) .AND. ( NB.EQ.1 ) ) THEN
*
*                 Build a 2-by-2 system Z * x = RHS
*
                  Z( 1, 1 ) = A( IS, IS )
                  Z( 2, 1 ) = D( IS, IS )
                  Z( 1, 2 ) = -B( JS, JS )
                  Z( 2, 2 ) = -E( JS, JS )
*
*                 Set up right hand side(s)
*
                  RHS( 1 ) = C( IS, JS )
                  RHS( 2 ) = F( IS, JS )
*
*                 Solve Z * x = RHS
*
                  CALL DGETC2( ZDIM, Z, LDZ, IPIV, JPIV, IERR )
                  IF( IERR.GT.0 )
     $               INFO = IERR
*
                  IF( IJOB.EQ.0 ) THEN
                     CALL DGESC2( ZDIM, Z, LDZ, RHS, IPIV, JPIV,
     $                            SCALOC )
                     IF( SCALOC.NE.ONE ) THEN
                        DO 50 K = 1, N
                           CALL DSCAL( M, SCALOC, C( 1, K ), 1 )
                           CALL DSCAL( M, SCALOC, F( 1, K ), 1 )
   50                   CONTINUE
                        SCALE = SCALE*SCALOC
                     END IF
                  ELSE
                     CALL DLATDF( IJOB, ZDIM, Z, LDZ, RHS, RDSUM,
     $                            RDSCAL, IPIV, JPIV )
                  END IF
*
*                 Unpack solution vector(s)
*
                  C( IS, JS ) = RHS( 1 )
                  F( IS, JS ) = RHS( 2 )
*
*                 Substitute R(I, J) and L(I, J) into remaining
*                 equation.
*
                  IF( I.GT.1 ) THEN
                     ALPHA = -RHS( 1 )
                     CALL DAXPY( IS-1, ALPHA, A( 1, IS ), 1, C( 1, JS ),
     $                           1 )
                     CALL DAXPY( IS-1, ALPHA, D( 1, IS ), 1, F( 1, JS ),
     $                           1 )
                  END IF
                  IF( J.LT.Q ) THEN
                     CALL DAXPY( N-JE, RHS( 2 ), B( JS, JE+1 ), LDB,
     $                           C( IS, JE+1 ), LDC )
                     CALL DAXPY( N-JE, RHS( 2 ), E( JS, JE+1 ), LDE,
     $                           F( IS, JE+1 ), LDF )
                  END IF
*
               ELSE IF( ( MB.EQ.1 ) .AND. ( NB.EQ.2 ) ) THEN
*
*                 Build a 4-by-4 system Z * x = RHS
*
                  Z( 1, 1 ) = A( IS, IS )
                  Z( 2, 1 ) = ZERO
                  Z( 3, 1 ) = D( IS, IS )
                  Z( 4, 1 ) = ZERO
*
                  Z( 1, 2 ) = ZERO
                  Z( 2, 2 ) = A( IS, IS )
                  Z( 3, 2 ) = ZERO
                  Z( 4, 2 ) = D( IS, IS )
*
                  Z( 1, 3 ) = -B( JS, JS )
                  Z( 2, 3 ) = -B( JS, JSP1 )
                  Z( 3, 3 ) = -E( JS, JS )
                  Z( 4, 3 ) = -E( JS, JSP1 )
*
                  Z( 1, 4 ) = -B( JSP1, JS )
                  Z( 2, 4 ) = -B( JSP1, JSP1 )
                  Z( 3, 4 ) = ZERO
                  Z( 4, 4 ) = -E( JSP1, JSP1 )
*
*                 Set up right hand side(s)
*
                  RHS( 1 ) = C( IS, JS )
                  RHS( 2 ) = C( IS, JSP1 )
                  RHS( 3 ) = F( IS, JS )
                  RHS( 4 ) = F( IS, JSP1 )
*
*                 Solve Z * x = RHS
*
                  CALL DGETC2( ZDIM, Z, LDZ, IPIV, JPIV, IERR )
                  IF( IERR.GT.0 )
     $               INFO = IERR
*
                  IF( IJOB.EQ.0 ) THEN
                     CALL DGESC2( ZDIM, Z, LDZ, RHS, IPIV, JPIV,
     $                            SCALOC )
                     IF( SCALOC.NE.ONE ) THEN
                        DO 60 K = 1, N
                           CALL DSCAL( M, SCALOC, C( 1, K ), 1 )
                           CALL DSCAL( M, SCALOC, F( 1, K ), 1 )
   60                   CONTINUE
                        SCALE = SCALE*SCALOC
                     END IF
                  ELSE
                     CALL DLATDF( IJOB, ZDIM, Z, LDZ, RHS, RDSUM,
     $                            RDSCAL, IPIV, JPIV )
                  END IF
*
*                 Unpack solution vector(s)
*
                  C( IS, JS ) = RHS( 1 )
                  C( IS, JSP1 ) = RHS( 2 )
                  F( IS, JS ) = RHS( 3 )
                  F( IS, JSP1 ) = RHS( 4 )
*
*                 Substitute R(I, J) and L(I, J) into remaining
*                 equation.
*
                  IF( I.GT.1 ) THEN
                     CALL DGER( IS-1, NB, -ONE, A( 1, IS ), 1, RHS( 1 ),
     $                          1, C( 1, JS ), LDC )
                     CALL DGER( IS-1, NB, -ONE, D( 1, IS ), 1, RHS( 1 ),
     $                          1, F( 1, JS ), LDF )
                  END IF
                  IF( J.LT.Q ) THEN
                     CALL DAXPY( N-JE, RHS( 3 ), B( JS, JE+1 ), LDB,
     $                           C( IS, JE+1 ), LDC )
                     CALL DAXPY( N-JE, RHS( 3 ), E( JS, JE+1 ), LDE,
     $                           F( IS, JE+1 ), LDF )
                     CALL DAXPY( N-JE, RHS( 4 ), B( JSP1, JE+1 ), LDB,
     $                           C( IS, JE+1 ), LDC )
                     CALL DAXPY( N-JE, RHS( 4 ), E( JSP1, JE+1 ), LDE,
     $                           F( IS, JE+1 ), LDF )
                  END IF
*
               ELSE IF( ( MB.EQ.2 ) .AND. ( NB.EQ.1 ) ) THEN
*
*                 Build a 4-by-4 system Z * x = RHS
*
                  Z( 1, 1 ) = A( IS, IS )
                  Z( 2, 1 ) = A( ISP1, IS )
                  Z( 3, 1 ) = D( IS, IS )
                  Z( 4, 1 ) = ZERO
*
                  Z( 1, 2 ) = A( IS, ISP1 )
                  Z( 2, 2 ) = A( ISP1, ISP1 )
                  Z( 3, 2 ) = D( IS, ISP1 )
                  Z( 4, 2 ) = D( ISP1, ISP1 )
*
                  Z( 1, 3 ) = -B( JS, JS )
                  Z( 2, 3 ) = ZERO
                  Z( 3, 3 ) = -E( JS, JS )
                  Z( 4, 3 ) = ZERO
*
                  Z( 1, 4 ) = ZERO
                  Z( 2, 4 ) = -B( JS, JS )
                  Z( 3, 4 ) = ZERO
                  Z( 4, 4 ) = -E( JS, JS )
*
*                 Set up right hand side(s)
*
                  RHS( 1 ) = C( IS, JS )
                  RHS( 2 ) = C( ISP1, JS )
                  RHS( 3 ) = F( IS, JS )
                  RHS( 4 ) = F( ISP1, JS )
*
*                 Solve Z * x = RHS
*
                  CALL DGETC2( ZDIM, Z, LDZ, IPIV, JPIV, IERR )
                  IF( IERR.GT.0 )
     $               INFO = IERR
                  IF( IJOB.EQ.0 ) THEN
                     CALL DGESC2( ZDIM, Z, LDZ, RHS, IPIV, JPIV,
     $                            SCALOC )
                     IF( SCALOC.NE.ONE ) THEN
                        DO 70 K = 1, N
                           CALL DSCAL( M, SCALOC, C( 1, K ), 1 )
                           CALL DSCAL( M, SCALOC, F( 1, K ), 1 )
   70                   CONTINUE
                        SCALE = SCALE*SCALOC
                     END IF
                  ELSE
                     CALL DLATDF( IJOB, ZDIM, Z, LDZ, RHS, RDSUM,
     $                            RDSCAL, IPIV, JPIV )
                  END IF
*
*                 Unpack solution vector(s)
*
                  C( IS, JS ) = RHS( 1 )
                  C( ISP1, JS ) = RHS( 2 )
                  F( IS, JS ) = RHS( 3 )
                  F( ISP1, JS ) = RHS( 4 )
*
*                 Substitute R(I, J) and L(I, J) into remaining
*                 equation.
*
                  IF( I.GT.1 ) THEN
                     CALL DGEMV( 'N', IS-1, MB, -ONE, A( 1, IS ), LDA,
     $                           RHS( 1 ), 1, ONE, C( 1, JS ), 1 )
                     CALL DGEMV( 'N', IS-1, MB, -ONE, D( 1, IS ), LDD,
     $                           RHS( 1 ), 1, ONE, F( 1, JS ), 1 )
                  END IF
                  IF( J.LT.Q ) THEN
                     CALL DGER( MB, N-JE, ONE, RHS( 3 ), 1,
     $                          B( JS, JE+1 ), LDB, C( IS, JE+1 ), LDC )
                     CALL DGER( MB, N-JE, ONE, RHS( 3 ), 1,
     $                          E( JS, JE+1 ), LDE, F( IS, JE+1 ), LDF )
                  END IF
*
               ELSE IF( ( MB.EQ.2 ) .AND. ( NB.EQ.2 ) ) THEN
*
*                 Build an 8-by-8 system Z * x = RHS
*
                  CALL DLASET( 'F', LDZ, LDZ, ZERO, ZERO, Z, LDZ )
*
                  Z( 1, 1 ) = A( IS, IS )
                  Z( 2, 1 ) = A( ISP1, IS )
                  Z( 5, 1 ) = D( IS, IS )
*
                  Z( 1, 2 ) = A( IS, ISP1 )
                  Z( 2, 2 ) = A( ISP1, ISP1 )
                  Z( 5, 2 ) = D( IS, ISP1 )
                  Z( 6, 2 ) = D( ISP1, ISP1 )
*
                  Z( 3, 3 ) = A( IS, IS )
                  Z( 4, 3 ) = A( ISP1, IS )
                  Z( 7, 3 ) = D( IS, IS )
*
                  Z( 3, 4 ) = A( IS, ISP1 )
                  Z( 4, 4 ) = A( ISP1, ISP1 )
                  Z( 7, 4 ) = D( IS, ISP1 )
                  Z( 8, 4 ) = D( ISP1, ISP1 )
*
                  Z( 1, 5 ) = -B( JS, JS )
                  Z( 3, 5 ) = -B( JS, JSP1 )
                  Z( 5, 5 ) = -E( JS, JS )
                  Z( 7, 5 ) = -E( JS, JSP1 )
*
                  Z( 2, 6 ) = -B( JS, JS )
                  Z( 4, 6 ) = -B( JS, JSP1 )
                  Z( 6, 6 ) = -E( JS, JS )
                  Z( 8, 6 ) = -E( JS, JSP1 )
*
                  Z( 1, 7 ) = -B( JSP1, JS )
                  Z( 3, 7 ) = -B( JSP1, JSP1 )
                  Z( 7, 7 ) = -E( JSP1, JSP1 )
*
                  Z( 2, 8 ) = -B( JSP1, JS )
                  Z( 4, 8 ) = -B( JSP1, JSP1 )
                  Z( 8, 8 ) = -E( JSP1, JSP1 )
*
*                 Set up right hand side(s)
*
                  K = 1
                  II = MB*NB + 1
                  DO 80 JJ = 0, NB - 1
                     CALL DCOPY( MB, C( IS, JS+JJ ), 1, RHS( K ), 1 )
                     CALL DCOPY( MB, F( IS, JS+JJ ), 1, RHS( II ), 1 )
                     K = K + MB
                     II = II + MB
   80             CONTINUE
*
*                 Solve Z * x = RHS
*
                  CALL DGETC2( ZDIM, Z, LDZ, IPIV, JPIV, IERR )
                  IF( IERR.GT.0 )
     $               INFO = IERR
                  IF( IJOB.EQ.0 ) THEN
                     CALL DGESC2( ZDIM, Z, LDZ, RHS, IPIV, JPIV,
     $                            SCALOC )
                     IF( SCALOC.NE.ONE ) THEN
                        DO 90 K = 1, N
                           CALL DSCAL( M, SCALOC, C( 1, K ), 1 )
                           CALL DSCAL( M, SCALOC, F( 1, K ), 1 )
   90                   CONTINUE
                        SCALE = SCALE*SCALOC
                     END IF
                  ELSE
                     CALL DLATDF( IJOB, ZDIM, Z, LDZ, RHS, RDSUM,
     $                            RDSCAL, IPIV, JPIV )
                  END IF
*
*                 Unpack solution vector(s)
*
                  K = 1
                  II = MB*NB + 1
                  DO 100 JJ = 0, NB - 1
                     CALL DCOPY( MB, RHS( K ), 1, C( IS, JS+JJ ), 1 )
                     CALL DCOPY( MB, RHS( II ), 1, F( IS, JS+JJ ), 1 )
                     K = K + MB
                     II = II + MB
  100             CONTINUE
*
*                 Substitute R(I, J) and L(I, J) into remaining
*                 equation.
*
                  IF( I.GT.1 ) THEN
                     CALL DGEMM( 'N', 'N', IS-1, NB, MB, -ONE,
     $                           A( 1, IS ), LDA, RHS( 1 ), MB, ONE,
     $                           C( 1, JS ), LDC )
                     CALL DGEMM( 'N', 'N', IS-1, NB, MB, -ONE,
     $                           D( 1, IS ), LDD, RHS( 1 ), MB, ONE,
     $                           F( 1, JS ), LDF )
                  END IF
                  IF( J.LT.Q ) THEN
                     K = MB*NB + 1
                     CALL DGEMM( 'N', 'N', MB, N-JE, NB, ONE, RHS( K ),
     $                           MB, B( JS, JE+1 ), LDB, ONE,
     $                           C( IS, JE+1 ), LDC )
                     CALL DGEMM( 'N', 'N', MB, N-JE, NB, ONE, RHS( K ),
     $                           MB, E( JS, JE+1 ), LDE, ONE,
     $                           F( IS, JE+1 ), LDF )
                  END IF
*
               END IF
*
  110       CONTINUE
  120    CONTINUE
      ELSE
*
*        Solve (I, J) - subsystem
*             A(I, I)**T * R(I, J) + D(I, I)**T * L(J, J)  =  C(I, J)
*             R(I, I)  * B(J, J) + L(I, J)  * E(J, J)  = -F(I, J)
*        for I = 1, 2, ..., P, J = Q, Q - 1, ..., 1
*
         SCALE = ONE
         SCALOC = ONE
         DO 200 I = 1, P
*
            IS = IWORK( I )
            ISP1 = IS + 1
            IE = IWORK ( I+1 ) - 1
            MB = IE - IS + 1
            DO 190 J = Q, P + 2, -1
*
               JS = IWORK( J )
               JSP1 = JS + 1
               JE = IWORK( J+1 ) - 1
               NB = JE - JS + 1
               ZDIM = MB*NB*2
               IF( ( MB.EQ.1 ) .AND. ( NB.EQ.1 ) ) THEN
*
*                 Build a 2-by-2 system Z**T * x = RHS
*
                  Z( 1, 1 ) = A( IS, IS )
                  Z( 2, 1 ) = -B( JS, JS )
                  Z( 1, 2 ) = D( IS, IS )
                  Z( 2, 2 ) = -E( JS, JS )
*
*                 Set up right hand side(s)
*
                  RHS( 1 ) = C( IS, JS )
                  RHS( 2 ) = F( IS, JS )
*
*                 Solve Z**T * x = RHS
*
                  CALL DGETC2( ZDIM, Z, LDZ, IPIV, JPIV, IERR )
                  IF( IERR.GT.0 )
     $               INFO = IERR
*
                  CALL DGESC2( ZDIM, Z, LDZ, RHS, IPIV, JPIV, SCALOC )
                  IF( SCALOC.NE.ONE ) THEN
                     DO 130 K = 1, N
                        CALL DSCAL( M, SCALOC, C( 1, K ), 1 )
                        CALL DSCAL( M, SCALOC, F( 1, K ), 1 )
  130                CONTINUE
                     SCALE = SCALE*SCALOC
                  END IF
*
*                 Unpack solution vector(s)
*
                  C( IS, JS ) = RHS( 1 )
                  F( IS, JS ) = RHS( 2 )
*
*                 Substitute R(I, J) and L(I, J) into remaining
*                 equation.
*
                  IF( J.GT.P+2 ) THEN
                     ALPHA = RHS( 1 )
                     CALL DAXPY( JS-1, ALPHA, B( 1, JS ), 1, F( IS, 1 ),
     $                           LDF )
                     ALPHA = RHS( 2 )
                     CALL DAXPY( JS-1, ALPHA, E( 1, JS ), 1, F( IS, 1 ),
     $                           LDF )
                  END IF
                  IF( I.LT.P ) THEN
                     ALPHA = -RHS( 1 )
                     CALL DAXPY( M-IE, ALPHA, A( IS, IE+1 ), LDA,
     $                           C( IE+1, JS ), 1 )
                     ALPHA = -RHS( 2 )
                     CALL DAXPY( M-IE, ALPHA, D( IS, IE+1 ), LDD,
     $                           C( IE+1, JS ), 1 )
                  END IF
*
               ELSE IF( ( MB.EQ.1 ) .AND. ( NB.EQ.2 ) ) THEN
*
*                 Build a 4-by-4 system Z**T * x = RHS
*
                  Z( 1, 1 ) = A( IS, IS )
                  Z( 2, 1 ) = ZERO
                  Z( 3, 1 ) = -B( JS, JS )
                  Z( 4, 1 ) = -B( JSP1, JS )
*
                  Z( 1, 2 ) = ZERO
                  Z( 2, 2 ) = A( IS, IS )
                  Z( 3, 2 ) = -B( JS, JSP1 )
                  Z( 4, 2 ) = -B( JSP1, JSP1 )
*
                  Z( 1, 3 ) = D( IS, IS )
                  Z( 2, 3 ) = ZERO
                  Z( 3, 3 ) = -E( JS, JS )
                  Z( 4, 3 ) = ZERO
*
                  Z( 1, 4 ) = ZERO
                  Z( 2, 4 ) = D( IS, IS )
                  Z( 3, 4 ) = -E( JS, JSP1 )
                  Z( 4, 4 ) = -E( JSP1, JSP1 )
*
*                 Set up right hand side(s)
*
                  RHS( 1 ) = C( IS, JS )
                  RHS( 2 ) = C( IS, JSP1 )
                  RHS( 3 ) = F( IS, JS )
                  RHS( 4 ) = F( IS, JSP1 )
*
*                 Solve Z**T * x = RHS
*
                  CALL DGETC2( ZDIM, Z, LDZ, IPIV, JPIV, IERR )
                  IF( IERR.GT.0 )
     $               INFO = IERR
                  CALL DGESC2( ZDIM, Z, LDZ, RHS, IPIV, JPIV, SCALOC )
                  IF( SCALOC.NE.ONE ) THEN
                     DO 140 K = 1, N
                        CALL DSCAL( M, SCALOC, C( 1, K ), 1 )
                        CALL DSCAL( M, SCALOC, F( 1, K ), 1 )
  140                CONTINUE
                     SCALE = SCALE*SCALOC
                  END IF
*
*                 Unpack solution vector(s)
*
                  C( IS, JS ) = RHS( 1 )
                  C( IS, JSP1 ) = RHS( 2 )
                  F( IS, JS ) = RHS( 3 )
                  F( IS, JSP1 ) = RHS( 4 )
*
*                 Substitute R(I, J) and L(I, J) into remaining
*                 equation.
*
                  IF( J.GT.P+2 ) THEN
                     CALL DAXPY( JS-1, RHS( 1 ), B( 1, JS ), 1,
     $                           F( IS, 1 ), LDF )
                     CALL DAXPY( JS-1, RHS( 2 ), B( 1, JSP1 ), 1,
     $                           F( IS, 1 ), LDF )
                     CALL DAXPY( JS-1, RHS( 3 ), E( 1, JS ), 1,
     $                           F( IS, 1 ), LDF )
                     CALL DAXPY( JS-1, RHS( 4 ), E( 1, JSP1 ), 1,
     $                           F( IS, 1 ), LDF )
                  END IF
                  IF( I.LT.P ) THEN
                     CALL DGER( M-IE, NB, -ONE, A( IS, IE+1 ), LDA,
     $                          RHS( 1 ), 1, C( IE+1, JS ), LDC )
                     CALL DGER( M-IE, NB, -ONE, D( IS, IE+1 ), LDD,
     $                          RHS( 3 ), 1, C( IE+1, JS ), LDC )
                  END IF
*
               ELSE IF( ( MB.EQ.2 ) .AND. ( NB.EQ.1 ) ) THEN
*
*                 Build a 4-by-4 system Z**T * x = RHS
*
                  Z( 1, 1 ) = A( IS, IS )
                  Z( 2, 1 ) = A( IS, ISP1 )
                  Z( 3, 1 ) = -B( JS, JS )
                  Z( 4, 1 ) = ZERO
*
                  Z( 1, 2 ) = A( ISP1, IS )
                  Z( 2, 2 ) = A( ISP1, ISP1 )
                  Z( 3, 2 ) = ZERO
                  Z( 4, 2 ) = -B( JS, JS )
*
                  Z( 1, 3 ) = D( IS, IS )
                  Z( 2, 3 ) = D( IS, ISP1 )
                  Z( 3, 3 ) = -E( JS, JS )
                  Z( 4, 3 ) = ZERO
*
                  Z( 1, 4 ) = ZERO
                  Z( 2, 4 ) = D( ISP1, ISP1 )
                  Z( 3, 4 ) = ZERO
                  Z( 4, 4 ) = -E( JS, JS )
*
*                 Set up right hand side(s)
*
                  RHS( 1 ) = C( IS, JS )
                  RHS( 2 ) = C( ISP1, JS )
                  RHS( 3 ) = F( IS, JS )
                  RHS( 4 ) = F( ISP1, JS )
*
*                 Solve Z**T * x = RHS
*
                  CALL DGETC2( ZDIM, Z, LDZ, IPIV, JPIV, IERR )
                  IF( IERR.GT.0 )
     $               INFO = IERR
*
                  CALL DGESC2( ZDIM, Z, LDZ, RHS, IPIV, JPIV, SCALOC )
                  IF( SCALOC.NE.ONE ) THEN
                     DO 150 K = 1, N
                        CALL DSCAL( M, SCALOC, C( 1, K ), 1 )
                        CALL DSCAL( M, SCALOC, F( 1, K ), 1 )
  150                CONTINUE
                     SCALE = SCALE*SCALOC
                  END IF
*
*                 Unpack solution vector(s)
*
                  C( IS, JS ) = RHS( 1 )
                  C( ISP1, JS ) = RHS( 2 )
                  F( IS, JS ) = RHS( 3 )
                  F( ISP1, JS ) = RHS( 4 )
*
*                 Substitute R(I, J) and L(I, J) into remaining
*                 equation.
*
                  IF( J.GT.P+2 ) THEN
                     CALL DGER( MB, JS-1, ONE, RHS( 1 ), 1, B( 1, JS ),
     $                          1, F( IS, 1 ), LDF )
                     CALL DGER( MB, JS-1, ONE, RHS( 3 ), 1, E( 1, JS ),
     $                          1, F( IS, 1 ), LDF )
                  END IF
                  IF( I.LT.P ) THEN
                     CALL DGEMV( 'T', MB, M-IE, -ONE, A( IS, IE+1 ),
     $                           LDA, RHS( 1 ), 1, ONE, C( IE+1, JS ),
     $                           1 )
                     CALL DGEMV( 'T', MB, M-IE, -ONE, D( IS, IE+1 ),
     $                           LDD, RHS( 3 ), 1, ONE, C( IE+1, JS ),
     $                           1 )
                  END IF
*
               ELSE IF( ( MB.EQ.2 ) .AND. ( NB.EQ.2 ) ) THEN
*
*                 Build an 8-by-8 system Z**T * x = RHS
*
                  CALL DLASET( 'F', LDZ, LDZ, ZERO, ZERO, Z, LDZ )
*
                  Z( 1, 1 ) = A( IS, IS )
                  Z( 2, 1 ) = A( IS, ISP1 )
                  Z( 5, 1 ) = -B( JS, JS )
                  Z( 7, 1 ) = -B( JSP1, JS )
*
                  Z( 1, 2 ) = A( ISP1, IS )
                  Z( 2, 2 ) = A( ISP1, ISP1 )
                  Z( 6, 2 ) = -B( JS, JS )
                  Z( 8, 2 ) = -B( JSP1, JS )
*
                  Z( 3, 3 ) = A( IS, IS )
                  Z( 4, 3 ) = A( IS, ISP1 )
                  Z( 5, 3 ) = -B( JS, JSP1 )
                  Z( 7, 3 ) = -B( JSP1, JSP1 )
*
                  Z( 3, 4 ) = A( ISP1, IS )
                  Z( 4, 4 ) = A( ISP1, ISP1 )
                  Z( 6, 4 ) = -B( JS, JSP1 )
                  Z( 8, 4 ) = -B( JSP1, JSP1 )
*
                  Z( 1, 5 ) = D( IS, IS )
                  Z( 2, 5 ) = D( IS, ISP1 )
                  Z( 5, 5 ) = -E( JS, JS )
*
                  Z( 2, 6 ) = D( ISP1, ISP1 )
                  Z( 6, 6 ) = -E( JS, JS )
*
                  Z( 3, 7 ) = D( IS, IS )
                  Z( 4, 7 ) = D( IS, ISP1 )
                  Z( 5, 7 ) = -E( JS, JSP1 )
                  Z( 7, 7 ) = -E( JSP1, JSP1 )
*
                  Z( 4, 8 ) = D( ISP1, ISP1 )
                  Z( 6, 8 ) = -E( JS, JSP1 )
                  Z( 8, 8 ) = -E( JSP1, JSP1 )
*
*                 Set up right hand side(s)
*
                  K = 1
                  II = MB*NB + 1
                  DO 160 JJ = 0, NB - 1
                     CALL DCOPY( MB, C( IS, JS+JJ ), 1, RHS( K ), 1 )
                     CALL DCOPY( MB, F( IS, JS+JJ ), 1, RHS( II ), 1 )
                     K = K + MB
                     II = II + MB
  160             CONTINUE
*
*
*                 Solve Z**T * x = RHS
*
                  CALL DGETC2( ZDIM, Z, LDZ, IPIV, JPIV, IERR )
                  IF( IERR.GT.0 )
     $               INFO = IERR
*
                  CALL DGESC2( ZDIM, Z, LDZ, RHS, IPIV, JPIV, SCALOC )
                  IF( SCALOC.NE.ONE ) THEN
                     DO 170 K = 1, N
                        CALL DSCAL( M, SCALOC, C( 1, K ), 1 )
                        CALL DSCAL( M, SCALOC, F( 1, K ), 1 )
  170                CONTINUE
                     SCALE = SCALE*SCALOC
                  END IF
*
*                 Unpack solution vector(s)
*
                  K = 1
                  II = MB*NB + 1
                  DO 180 JJ = 0, NB - 1
                     CALL DCOPY( MB, RHS( K ), 1, C( IS, JS+JJ ), 1 )
                     CALL DCOPY( MB, RHS( II ), 1, F( IS, JS+JJ ), 1 )
                     K = K + MB
                     II = II + MB
  180             CONTINUE
*
*                 Substitute R(I, J) and L(I, J) into remaining
*                 equation.
*
                  IF( J.GT.P+2 ) THEN
                     CALL DGEMM( 'N', 'T', MB, JS-1, NB, ONE,
     $                           C( IS, JS ), LDC, B( 1, JS ), LDB, ONE,
     $                           F( IS, 1 ), LDF )
                     CALL DGEMM( 'N', 'T', MB, JS-1, NB, ONE,
     $                           F( IS, JS ), LDF, E( 1, JS ), LDE, ONE,
     $                           F( IS, 1 ), LDF )
                  END IF
                  IF( I.LT.P ) THEN
                     CALL DGEMM( 'T', 'N', M-IE, NB, MB, -ONE,
     $                           A( IS, IE+1 ), LDA, C( IS, JS ), LDC,
     $                           ONE, C( IE+1, JS ), LDC )
                     CALL DGEMM( 'T', 'N', M-IE, NB, MB, -ONE,
     $                           D( IS, IE+1 ), LDD, F( IS, JS ), LDF,
     $                           ONE, C( IE+1, JS ), LDC )
                  END IF
*
               END IF
*
  190       CONTINUE
  200    CONTINUE
*
      END IF
      RETURN
*
*     End of DTGSY2
*
      END
