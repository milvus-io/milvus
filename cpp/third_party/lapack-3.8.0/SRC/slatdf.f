*> \brief \b SLATDF uses the LU factorization of the n-by-n matrix computed by sgetc2 and computes a contribution to the reciprocal Dif-estimate.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLATDF + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slatdf.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slatdf.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slatdf.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLATDF( IJOB, N, Z, LDZ, RHS, RDSUM, RDSCAL, IPIV,
*                          JPIV )
*
*       .. Scalar Arguments ..
*       INTEGER            IJOB, LDZ, N
*       REAL               RDSCAL, RDSUM
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * ), JPIV( * )
*       REAL               RHS( * ), Z( LDZ, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLATDF uses the LU factorization of the n-by-n matrix Z computed by
*> SGETC2 and computes a contribution to the reciprocal Dif-estimate
*> by solving Z * x = b for x, and choosing the r.h.s. b such that
*> the norm of x is as large as possible. On entry RHS = b holds the
*> contribution from earlier solved sub-systems, and on return RHS = x.
*>
*> The factorization of Z returned by SGETC2 has the form Z = P*L*U*Q,
*> where P and Q are permutation matrices. L is lower triangular with
*> unit diagonal elements and U is upper triangular.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] IJOB
*> \verbatim
*>          IJOB is INTEGER
*>          IJOB = 2: First compute an approximative null-vector e
*>              of Z using SGECON, e is normalized and solve for
*>              Zx = +-e - f with the sign giving the greater value
*>              of 2-norm(x). About 5 times as expensive as Default.
*>          IJOB .ne. 2: Local look ahead strategy where all entries of
*>              the r.h.s. b is chosen as either +1 or -1 (Default).
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix Z.
*> \endverbatim
*>
*> \param[in] Z
*> \verbatim
*>          Z is REAL array, dimension (LDZ, N)
*>          On entry, the LU part of the factorization of the n-by-n
*>          matrix Z computed by SGETC2:  Z = P * L * U * Q
*> \endverbatim
*>
*> \param[in] LDZ
*> \verbatim
*>          LDZ is INTEGER
*>          The leading dimension of the array Z.  LDA >= max(1, N).
*> \endverbatim
*>
*> \param[in,out] RHS
*> \verbatim
*>          RHS is REAL array, dimension N.
*>          On entry, RHS contains contributions from other subsystems.
*>          On exit, RHS contains the solution of the subsystem with
*>          entries acoording to the value of IJOB (see above).
*> \endverbatim
*>
*> \param[in,out] RDSUM
*> \verbatim
*>          RDSUM is REAL
*>          On entry, the sum of squares of computed contributions to
*>          the Dif-estimate under computation by STGSYL, where the
*>          scaling factor RDSCAL (see below) has been factored out.
*>          On exit, the corresponding sum of squares updated with the
*>          contributions from the current sub-system.
*>          If TRANS = 'T' RDSUM is not touched.
*>          NOTE: RDSUM only makes sense when STGSY2 is called by STGSYL.
*> \endverbatim
*>
*> \param[in,out] RDSCAL
*> \verbatim
*>          RDSCAL is REAL
*>          On entry, scaling factor used to prevent overflow in RDSUM.
*>          On exit, RDSCAL is updated w.r.t. the current contributions
*>          in RDSUM.
*>          If TRANS = 'T', RDSCAL is not touched.
*>          NOTE: RDSCAL only makes sense when STGSY2 is called by
*>                STGSYL.
*> \endverbatim
*>
*> \param[in] IPIV
*> \verbatim
*>          IPIV is INTEGER array, dimension (N).
*>          The pivot indices; for 1 <= i <= N, row i of the
*>          matrix has been interchanged with row IPIV(i).
*> \endverbatim
*>
*> \param[in] JPIV
*> \verbatim
*>          JPIV is INTEGER array, dimension (N).
*>          The pivot indices; for 1 <= j <= N, column j of the
*>          matrix has been interchanged with column JPIV(j).
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
*> \date June 2016
*
*> \ingroup realOTHERauxiliary
*
*> \par Further Details:
*  =====================
*>
*>  This routine is a further developed implementation of algorithm
*>  BSOLVE in [1] using complete pivoting in the LU factorization.
*
*> \par Contributors:
*  ==================
*>
*>     Bo Kagstrom and Peter Poromaa, Department of Computing Science,
*>     Umea University, S-901 87 Umea, Sweden.
*
*> \par References:
*  ================
*>
*> \verbatim
*>
*>
*>  [1] Bo Kagstrom and Lars Westin,
*>      Generalized Schur Methods with Condition Estimators for
*>      Solving the Generalized Sylvester Equation, IEEE Transactions
*>      on Automatic Control, Vol. 34, No. 7, July 1989, pp 745-751.
*>
*>  [2] Peter Poromaa,
*>      On Efficient and Robust Estimators for the Separation
*>      between two Regular Matrix Pairs with Applications in
*>      Condition Estimation. Report IMINF-95.05, Departement of
*>      Computing Science, Umea University, S-901 87 Umea, Sweden, 1995.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE SLATDF( IJOB, N, Z, LDZ, RHS, RDSUM, RDSCAL, IPIV,
     $                   JPIV )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      INTEGER            IJOB, LDZ, N
      REAL               RDSCAL, RDSUM
*     ..
*     .. Array Arguments ..
      INTEGER            IPIV( * ), JPIV( * )
      REAL               RHS( * ), Z( LDZ, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            MAXDIM
      PARAMETER          ( MAXDIM = 8 )
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
*     ..
*     .. Local Scalars ..
      INTEGER            I, INFO, J, K
      REAL               BM, BP, PMONE, SMINU, SPLUS, TEMP
*     ..
*     .. Local Arrays ..
      INTEGER            IWORK( MAXDIM )
      REAL               WORK( 4*MAXDIM ), XM( MAXDIM ), XP( MAXDIM )
*     ..
*     .. External Subroutines ..
      EXTERNAL           SAXPY, SCOPY, SGECON, SGESC2, SLASSQ, SLASWP,
     $                   SSCAL
*     ..
*     .. External Functions ..
      REAL               SASUM, SDOT
      EXTERNAL           SASUM, SDOT
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, SQRT
*     ..
*     .. Executable Statements ..
*
      IF( IJOB.NE.2 ) THEN
*
*        Apply permutations IPIV to RHS
*
         CALL SLASWP( 1, RHS, LDZ, 1, N-1, IPIV, 1 )
*
*        Solve for L-part choosing RHS either to +1 or -1.
*
         PMONE = -ONE
*
         DO 10 J = 1, N - 1
            BP = RHS( J ) + ONE
            BM = RHS( J ) - ONE
            SPLUS = ONE
*
*           Look-ahead for L-part RHS(1:N-1) = + or -1, SPLUS and
*           SMIN computed more efficiently than in BSOLVE [1].
*
            SPLUS = SPLUS + SDOT( N-J, Z( J+1, J ), 1, Z( J+1, J ), 1 )
            SMINU = SDOT( N-J, Z( J+1, J ), 1, RHS( J+1 ), 1 )
            SPLUS = SPLUS*RHS( J )
            IF( SPLUS.GT.SMINU ) THEN
               RHS( J ) = BP
            ELSE IF( SMINU.GT.SPLUS ) THEN
               RHS( J ) = BM
            ELSE
*
*              In this case the updating sums are equal and we can
*              choose RHS(J) +1 or -1. The first time this happens
*              we choose -1, thereafter +1. This is a simple way to
*              get good estimates of matrices like Byers well-known
*              example (see [1]). (Not done in BSOLVE.)
*
               RHS( J ) = RHS( J ) + PMONE
               PMONE = ONE
            END IF
*
*           Compute the remaining r.h.s.
*
            TEMP = -RHS( J )
            CALL SAXPY( N-J, TEMP, Z( J+1, J ), 1, RHS( J+1 ), 1 )
*
   10    CONTINUE
*
*        Solve for U-part, look-ahead for RHS(N) = +-1. This is not done
*        in BSOLVE and will hopefully give us a better estimate because
*        any ill-conditioning of the original matrix is transfered to U
*        and not to L. U(N, N) is an approximation to sigma_min(LU).
*
         CALL SCOPY( N-1, RHS, 1, XP, 1 )
         XP( N ) = RHS( N ) + ONE
         RHS( N ) = RHS( N ) - ONE
         SPLUS = ZERO
         SMINU = ZERO
         DO 30 I = N, 1, -1
            TEMP = ONE / Z( I, I )
            XP( I ) = XP( I )*TEMP
            RHS( I ) = RHS( I )*TEMP
            DO 20 K = I + 1, N
               XP( I ) = XP( I ) - XP( K )*( Z( I, K )*TEMP )
               RHS( I ) = RHS( I ) - RHS( K )*( Z( I, K )*TEMP )
   20       CONTINUE
            SPLUS = SPLUS + ABS( XP( I ) )
            SMINU = SMINU + ABS( RHS( I ) )
   30    CONTINUE
         IF( SPLUS.GT.SMINU )
     $      CALL SCOPY( N, XP, 1, RHS, 1 )
*
*        Apply the permutations JPIV to the computed solution (RHS)
*
         CALL SLASWP( 1, RHS, LDZ, 1, N-1, JPIV, -1 )
*
*        Compute the sum of squares
*
         CALL SLASSQ( N, RHS, 1, RDSCAL, RDSUM )
*
      ELSE
*
*        IJOB = 2, Compute approximate nullvector XM of Z
*
         CALL SGECON( 'I', N, Z, LDZ, ONE, TEMP, WORK, IWORK, INFO )
         CALL SCOPY( N, WORK( N+1 ), 1, XM, 1 )
*
*        Compute RHS
*
         CALL SLASWP( 1, XM, LDZ, 1, N-1, IPIV, -1 )
         TEMP = ONE / SQRT( SDOT( N, XM, 1, XM, 1 ) )
         CALL SSCAL( N, TEMP, XM, 1 )
         CALL SCOPY( N, XM, 1, XP, 1 )
         CALL SAXPY( N, ONE, RHS, 1, XP, 1 )
         CALL SAXPY( N, -ONE, XM, 1, RHS, 1 )
         CALL SGESC2( N, Z, LDZ, RHS, IPIV, JPIV, TEMP )
         CALL SGESC2( N, Z, LDZ, XP, IPIV, JPIV, TEMP )
         IF( SASUM( N, XP, 1 ).GT.SASUM( N, RHS, 1 ) )
     $      CALL SCOPY( N, XP, 1, RHS, 1 )
*
*        Compute the sum of squares
*
         CALL SLASSQ( N, RHS, 1, RDSCAL, RDSUM )
*
      END IF
*
      RETURN
*
*     End of SLATDF
*
      END
