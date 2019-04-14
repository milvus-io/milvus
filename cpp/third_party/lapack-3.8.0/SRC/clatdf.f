*> \brief \b CLATDF uses the LU factorization of the n-by-n matrix computed by sgetc2 and computes a contribution to the reciprocal Dif-estimate.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CLATDF + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/clatdf.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/clatdf.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/clatdf.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CLATDF( IJOB, N, Z, LDZ, RHS, RDSUM, RDSCAL, IPIV,
*                          JPIV )
*
*       .. Scalar Arguments ..
*       INTEGER            IJOB, LDZ, N
*       REAL               RDSCAL, RDSUM
*       ..
*       .. Array Arguments ..
*       INTEGER            IPIV( * ), JPIV( * )
*       COMPLEX            RHS( * ), Z( LDZ, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CLATDF computes the contribution to the reciprocal Dif-estimate
*> by solving for x in Z * x = b, where b is chosen such that the norm
*> of x is as large as possible. It is assumed that LU decomposition
*> of Z has been computed by CGETC2. On entry RHS = f holds the
*> contribution from earlier solved sub-systems, and on return RHS = x.
*>
*> The factorization of Z returned by CGETC2 has the form
*> Z = P * L * U * Q, where P and Q are permutation matrices. L is lower
*> triangular with unit diagonal elements and U is upper triangular.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] IJOB
*> \verbatim
*>          IJOB is INTEGER
*>          IJOB = 2: First compute an approximative null-vector e
*>              of Z using CGECON, e is normalized and solve for
*>              Zx = +-e - f with the sign giving the greater value of
*>              2-norm(x).  About 5 times as expensive as Default.
*>          IJOB .ne. 2: Local look ahead strategy where
*>              all entries of the r.h.s. b is chosen as either +1 or
*>              -1.  Default.
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
*>          Z is COMPLEX array, dimension (LDZ, N)
*>          On entry, the LU part of the factorization of the n-by-n
*>          matrix Z computed by CGETC2:  Z = P * L * U * Q
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
*>          RHS is COMPLEX array, dimension (N).
*>          On entry, RHS contains contributions from other subsystems.
*>          On exit, RHS contains the solution of the subsystem with
*>          entries according to the value of IJOB (see above).
*> \endverbatim
*>
*> \param[in,out] RDSUM
*> \verbatim
*>          RDSUM is REAL
*>          On entry, the sum of squares of computed contributions to
*>          the Dif-estimate under computation by CTGSYL, where the
*>          scaling factor RDSCAL (see below) has been factored out.
*>          On exit, the corresponding sum of squares updated with the
*>          contributions from the current sub-system.
*>          If TRANS = 'T' RDSUM is not touched.
*>          NOTE: RDSUM only makes sense when CTGSY2 is called by CTGSYL.
*> \endverbatim
*>
*> \param[in,out] RDSCAL
*> \verbatim
*>          RDSCAL is REAL
*>          On entry, scaling factor used to prevent overflow in RDSUM.
*>          On exit, RDSCAL is updated w.r.t. the current contributions
*>          in RDSUM.
*>          If TRANS = 'T', RDSCAL is not touched.
*>          NOTE: RDSCAL only makes sense when CTGSY2 is called by
*>          CTGSYL.
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
*> \ingroup complexOTHERauxiliary
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
*>   [1]   Bo Kagstrom and Lars Westin,
*>         Generalized Schur Methods with Condition Estimators for
*>         Solving the Generalized Sylvester Equation, IEEE Transactions
*>         on Automatic Control, Vol. 34, No. 7, July 1989, pp 745-751.
*>
*>   [2]   Peter Poromaa,
*>         On Efficient and Robust Estimators for the Separation
*>         between two Regular Matrix Pairs with Applications in
*>         Condition Estimation. Report UMINF-95.05, Department of
*>         Computing Science, Umea University, S-901 87 Umea, Sweden,
*>         1995.
*
*  =====================================================================
      SUBROUTINE CLATDF( IJOB, N, Z, LDZ, RHS, RDSUM, RDSCAL, IPIV,
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
      COMPLEX            RHS( * ), Z( LDZ, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      INTEGER            MAXDIM
      PARAMETER          ( MAXDIM = 2 )
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
      COMPLEX            CONE
      PARAMETER          ( CONE = ( 1.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      INTEGER            I, INFO, J, K
      REAL               RTEMP, SCALE, SMINU, SPLUS
      COMPLEX            BM, BP, PMONE, TEMP
*     ..
*     .. Local Arrays ..
      REAL               RWORK( MAXDIM )
      COMPLEX            WORK( 4*MAXDIM ), XM( MAXDIM ), XP( MAXDIM )
*     ..
*     .. External Subroutines ..
      EXTERNAL           CAXPY, CCOPY, CGECON, CGESC2, CLASSQ, CLASWP,
     $                   CSCAL
*     ..
*     .. External Functions ..
      REAL               SCASUM
      COMPLEX            CDOTC
      EXTERNAL           SCASUM, CDOTC
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, REAL, SQRT
*     ..
*     .. Executable Statements ..
*
      IF( IJOB.NE.2 ) THEN
*
*        Apply permutations IPIV to RHS
*
         CALL CLASWP( 1, RHS, LDZ, 1, N-1, IPIV, 1 )
*
*        Solve for L-part choosing RHS either to +1 or -1.
*
         PMONE = -CONE
         DO 10 J = 1, N - 1
            BP = RHS( J ) + CONE
            BM = RHS( J ) - CONE
            SPLUS = ONE
*
*           Lockahead for L- part RHS(1:N-1) = +-1
*           SPLUS and SMIN computed more efficiently than in BSOLVE[1].
*
            SPLUS = SPLUS + REAL( CDOTC( N-J, Z( J+1, J ), 1, Z( J+1,
     $              J ), 1 ) )
            SMINU = REAL( CDOTC( N-J, Z( J+1, J ), 1, RHS( J+1 ), 1 ) )
            SPLUS = SPLUS*REAL( RHS( J ) )
            IF( SPLUS.GT.SMINU ) THEN
               RHS( J ) = BP
            ELSE IF( SMINU.GT.SPLUS ) THEN
               RHS( J ) = BM
            ELSE
*
*              In this case the updating sums are equal and we can
*              choose RHS(J) +1 or -1. The first time this happens we
*              choose -1, thereafter +1. This is a simple way to get
*              good estimates of matrices like Byers well-known example
*              (see [1]). (Not done in BSOLVE.)
*
               RHS( J ) = RHS( J ) + PMONE
               PMONE = CONE
            END IF
*
*           Compute the remaining r.h.s.
*
            TEMP = -RHS( J )
            CALL CAXPY( N-J, TEMP, Z( J+1, J ), 1, RHS( J+1 ), 1 )
   10    CONTINUE
*
*        Solve for U- part, lockahead for RHS(N) = +-1. This is not done
*        In BSOLVE and will hopefully give us a better estimate because
*        any ill-conditioning of the original matrix is transfered to U
*        and not to L. U(N, N) is an approximation to sigma_min(LU).
*
         CALL CCOPY( N-1, RHS, 1, WORK, 1 )
         WORK( N ) = RHS( N ) + CONE
         RHS( N ) = RHS( N ) - CONE
         SPLUS = ZERO
         SMINU = ZERO
         DO 30 I = N, 1, -1
            TEMP = CONE / Z( I, I )
            WORK( I ) = WORK( I )*TEMP
            RHS( I ) = RHS( I )*TEMP
            DO 20 K = I + 1, N
               WORK( I ) = WORK( I ) - WORK( K )*( Z( I, K )*TEMP )
               RHS( I ) = RHS( I ) - RHS( K )*( Z( I, K )*TEMP )
   20       CONTINUE
            SPLUS = SPLUS + ABS( WORK( I ) )
            SMINU = SMINU + ABS( RHS( I ) )
   30    CONTINUE
         IF( SPLUS.GT.SMINU )
     $      CALL CCOPY( N, WORK, 1, RHS, 1 )
*
*        Apply the permutations JPIV to the computed solution (RHS)
*
         CALL CLASWP( 1, RHS, LDZ, 1, N-1, JPIV, -1 )
*
*        Compute the sum of squares
*
         CALL CLASSQ( N, RHS, 1, RDSCAL, RDSUM )
         RETURN
      END IF
*
*     ENTRY IJOB = 2
*
*     Compute approximate nullvector XM of Z
*
      CALL CGECON( 'I', N, Z, LDZ, ONE, RTEMP, WORK, RWORK, INFO )
      CALL CCOPY( N, WORK( N+1 ), 1, XM, 1 )
*
*     Compute RHS
*
      CALL CLASWP( 1, XM, LDZ, 1, N-1, IPIV, -1 )
      TEMP = CONE / SQRT( CDOTC( N, XM, 1, XM, 1 ) )
      CALL CSCAL( N, TEMP, XM, 1 )
      CALL CCOPY( N, XM, 1, XP, 1 )
      CALL CAXPY( N, CONE, RHS, 1, XP, 1 )
      CALL CAXPY( N, -CONE, XM, 1, RHS, 1 )
      CALL CGESC2( N, Z, LDZ, RHS, IPIV, JPIV, SCALE )
      CALL CGESC2( N, Z, LDZ, XP, IPIV, JPIV, SCALE )
      IF( SCASUM( N, XP, 1 ).GT.SCASUM( N, RHS, 1 ) )
     $   CALL CCOPY( N, XP, 1, RHS, 1 )
*
*     Compute the sum of squares
*
      CALL CLASSQ( N, RHS, 1, RDSCAL, RDSUM )
      RETURN
*
*     End of CLATDF
*
      END
