*> \brief \b ZTRSEN
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZTRSEN + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/ztrsen.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/ztrsen.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/ztrsen.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZTRSEN( JOB, COMPQ, SELECT, N, T, LDT, Q, LDQ, W, M, S,
*                          SEP, WORK, LWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          COMPQ, JOB
*       INTEGER            INFO, LDQ, LDT, LWORK, M, N
*       DOUBLE PRECISION   S, SEP
*       ..
*       .. Array Arguments ..
*       LOGICAL            SELECT( * )
*       COMPLEX*16         Q( LDQ, * ), T( LDT, * ), W( * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZTRSEN reorders the Schur factorization of a complex matrix
*> A = Q*T*Q**H, so that a selected cluster of eigenvalues appears in
*> the leading positions on the diagonal of the upper triangular matrix
*> T, and the leading columns of Q form an orthonormal basis of the
*> corresponding right invariant subspace.
*>
*> Optionally the routine computes the reciprocal condition numbers of
*> the cluster of eigenvalues and/or the invariant subspace.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] JOB
*> \verbatim
*>          JOB is CHARACTER*1
*>          Specifies whether condition numbers are required for the
*>          cluster of eigenvalues (S) or the invariant subspace (SEP):
*>          = 'N': none;
*>          = 'E': for eigenvalues only (S);
*>          = 'V': for invariant subspace only (SEP);
*>          = 'B': for both eigenvalues and invariant subspace (S and
*>                 SEP).
*> \endverbatim
*>
*> \param[in] COMPQ
*> \verbatim
*>          COMPQ is CHARACTER*1
*>          = 'V': update the matrix Q of Schur vectors;
*>          = 'N': do not update Q.
*> \endverbatim
*>
*> \param[in] SELECT
*> \verbatim
*>          SELECT is LOGICAL array, dimension (N)
*>          SELECT specifies the eigenvalues in the selected cluster. To
*>          select the j-th eigenvalue, SELECT(j) must be set to .TRUE..
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
*>          On entry, the upper triangular matrix T.
*>          On exit, T is overwritten by the reordered matrix T, with the
*>          selected eigenvalues as the leading diagonal elements.
*> \endverbatim
*>
*> \param[in] LDT
*> \verbatim
*>          LDT is INTEGER
*>          The leading dimension of the array T. LDT >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] Q
*> \verbatim
*>          Q is COMPLEX*16 array, dimension (LDQ,N)
*>          On entry, if COMPQ = 'V', the matrix Q of Schur vectors.
*>          On exit, if COMPQ = 'V', Q has been postmultiplied by the
*>          unitary transformation matrix which reorders T; the leading M
*>          columns of Q form an orthonormal basis for the specified
*>          invariant subspace.
*>          If COMPQ = 'N', Q is not referenced.
*> \endverbatim
*>
*> \param[in] LDQ
*> \verbatim
*>          LDQ is INTEGER
*>          The leading dimension of the array Q.
*>          LDQ >= 1; and if COMPQ = 'V', LDQ >= N.
*> \endverbatim
*>
*> \param[out] W
*> \verbatim
*>          W is COMPLEX*16 array, dimension (N)
*>          The reordered eigenvalues of T, in the same order as they
*>          appear on the diagonal of T.
*> \endverbatim
*>
*> \param[out] M
*> \verbatim
*>          M is INTEGER
*>          The dimension of the specified invariant subspace.
*>          0 <= M <= N.
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is DOUBLE PRECISION
*>          If JOB = 'E' or 'B', S is a lower bound on the reciprocal
*>          condition number for the selected cluster of eigenvalues.
*>          S cannot underestimate the true reciprocal condition number
*>          by more than a factor of sqrt(N). If M = 0 or N, S = 1.
*>          If JOB = 'N' or 'V', S is not referenced.
*> \endverbatim
*>
*> \param[out] SEP
*> \verbatim
*>          SEP is DOUBLE PRECISION
*>          If JOB = 'V' or 'B', SEP is the estimated reciprocal
*>          condition number of the specified invariant subspace. If
*>          M = 0 or N, SEP = norm(T).
*>          If JOB = 'N' or 'E', SEP is not referenced.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is COMPLEX*16 array, dimension (MAX(1,LWORK))
*>          On exit, if INFO = 0, WORK(1) returns the optimal LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK.
*>          If JOB = 'N', LWORK >= 1;
*>          if JOB = 'E', LWORK = max(1,M*(N-M));
*>          if JOB = 'V' or 'B', LWORK >= max(1,2*M*(N-M)).
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
*> \date December 2016
*
*> \ingroup complex16OTHERcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  ZTRSEN first collects the selected eigenvalues by computing a unitary
*>  transformation Z to move them to the top left corner of T. In other
*>  words, the selected eigenvalues are the eigenvalues of T11 in:
*>
*>          Z**H * T * Z = ( T11 T12 ) n1
*>                         (  0  T22 ) n2
*>                            n1  n2
*>
*>  where N = n1+n2. The first
*>  n1 columns of Z span the specified invariant subspace of T.
*>
*>  If T has been obtained from the Schur factorization of a matrix
*>  A = Q*T*Q**H, then the reordered Schur factorization of A is given by
*>  A = (Q*Z)*(Z**H*T*Z)*(Q*Z)**H, and the first n1 columns of Q*Z span the
*>  corresponding invariant subspace of A.
*>
*>  The reciprocal condition number of the average of the eigenvalues of
*>  T11 may be returned in S. S lies between 0 (very badly conditioned)
*>  and 1 (very well conditioned). It is computed as follows. First we
*>  compute R so that
*>
*>                         P = ( I  R ) n1
*>                             ( 0  0 ) n2
*>                               n1 n2
*>
*>  is the projector on the invariant subspace associated with T11.
*>  R is the solution of the Sylvester equation:
*>
*>                        T11*R - R*T22 = T12.
*>
*>  Let F-norm(M) denote the Frobenius-norm of M and 2-norm(M) denote
*>  the two-norm of M. Then S is computed as the lower bound
*>
*>                      (1 + F-norm(R)**2)**(-1/2)
*>
*>  on the reciprocal of 2-norm(P), the true reciprocal condition number.
*>  S cannot underestimate 1 / 2-norm(P) by more than a factor of
*>  sqrt(N).
*>
*>  An approximate error bound for the computed average of the
*>  eigenvalues of T11 is
*>
*>                         EPS * norm(T) / S
*>
*>  where EPS is the machine precision.
*>
*>  The reciprocal condition number of the right invariant subspace
*>  spanned by the first n1 columns of Z (or of Q*Z) is returned in SEP.
*>  SEP is defined as the separation of T11 and T22:
*>
*>                     sep( T11, T22 ) = sigma-min( C )
*>
*>  where sigma-min(C) is the smallest singular value of the
*>  n1*n2-by-n1*n2 matrix
*>
*>     C  = kprod( I(n2), T11 ) - kprod( transpose(T22), I(n1) )
*>
*>  I(m) is an m by m identity matrix, and kprod denotes the Kronecker
*>  product. We estimate sigma-min(C) by the reciprocal of an estimate of
*>  the 1-norm of inverse(C). The true reciprocal 1-norm of inverse(C)
*>  cannot differ from sigma-min(C) by more than a factor of sqrt(n1*n2).
*>
*>  When SEP is small, small changes in T can cause large changes in
*>  the invariant subspace. An approximate bound on the maximum angular
*>  error in the computed right invariant subspace is
*>
*>                      EPS * norm(T) / SEP
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE ZTRSEN( JOB, COMPQ, SELECT, N, T, LDT, Q, LDQ, W, M, S,
     $                   SEP, WORK, LWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          COMPQ, JOB
      INTEGER            INFO, LDQ, LDT, LWORK, M, N
      DOUBLE PRECISION   S, SEP
*     ..
*     .. Array Arguments ..
      LOGICAL            SELECT( * )
      COMPLEX*16         Q( LDQ, * ), T( LDT, * ), W( * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            LQUERY, WANTBH, WANTQ, WANTS, WANTSP
      INTEGER            IERR, K, KASE, KS, LWMIN, N1, N2, NN
      DOUBLE PRECISION   EST, RNORM, SCALE
*     ..
*     .. Local Arrays ..
      INTEGER            ISAVE( 3 )
      DOUBLE PRECISION   RWORK( 1 )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      DOUBLE PRECISION   ZLANGE
      EXTERNAL           LSAME, ZLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA, ZLACN2, ZLACPY, ZTREXC, ZTRSYL
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, SQRT
*     ..
*     .. Executable Statements ..
*
*     Decode and test the input parameters.
*
      WANTBH = LSAME( JOB, 'B' )
      WANTS = LSAME( JOB, 'E' ) .OR. WANTBH
      WANTSP = LSAME( JOB, 'V' ) .OR. WANTBH
      WANTQ = LSAME( COMPQ, 'V' )
*
*     Set M to the number of selected eigenvalues.
*
      M = 0
      DO 10 K = 1, N
         IF( SELECT( K ) )
     $      M = M + 1
   10 CONTINUE
*
      N1 = M
      N2 = N - M
      NN = N1*N2
*
      INFO = 0
      LQUERY = ( LWORK.EQ.-1 )
*
      IF( WANTSP ) THEN
         LWMIN = MAX( 1, 2*NN )
      ELSE IF( LSAME( JOB, 'N' ) ) THEN
         LWMIN = 1
      ELSE IF( LSAME( JOB, 'E' ) ) THEN
         LWMIN = MAX( 1, NN )
      END IF
*
      IF( .NOT.LSAME( JOB, 'N' ) .AND. .NOT.WANTS .AND. .NOT.WANTSP )
     $     THEN
         INFO = -1
      ELSE IF( .NOT.LSAME( COMPQ, 'N' ) .AND. .NOT.WANTQ ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -4
      ELSE IF( LDT.LT.MAX( 1, N ) ) THEN
         INFO = -6
      ELSE IF( LDQ.LT.1 .OR. ( WANTQ .AND. LDQ.LT.N ) ) THEN
         INFO = -8
      ELSE IF( LWORK.LT.LWMIN .AND. .NOT.LQUERY ) THEN
         INFO = -14
      END IF
*
      IF( INFO.EQ.0 ) THEN
         WORK( 1 ) = LWMIN
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZTRSEN', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( M.EQ.N .OR. M.EQ.0 ) THEN
         IF( WANTS )
     $      S = ONE
         IF( WANTSP )
     $      SEP = ZLANGE( '1', N, N, T, LDT, RWORK )
         GO TO 40
      END IF
*
*     Collect the selected eigenvalues at the top left corner of T.
*
      KS = 0
      DO 20 K = 1, N
         IF( SELECT( K ) ) THEN
            KS = KS + 1
*
*           Swap the K-th eigenvalue to position KS.
*
            IF( K.NE.KS )
     $         CALL ZTREXC( COMPQ, N, T, LDT, Q, LDQ, K, KS, IERR )
         END IF
   20 CONTINUE
*
      IF( WANTS ) THEN
*
*        Solve the Sylvester equation for R:
*
*           T11*R - R*T22 = scale*T12
*
         CALL ZLACPY( 'F', N1, N2, T( 1, N1+1 ), LDT, WORK, N1 )
         CALL ZTRSYL( 'N', 'N', -1, N1, N2, T, LDT, T( N1+1, N1+1 ),
     $                LDT, WORK, N1, SCALE, IERR )
*
*        Estimate the reciprocal of the condition number of the cluster
*        of eigenvalues.
*
         RNORM = ZLANGE( 'F', N1, N2, WORK, N1, RWORK )
         IF( RNORM.EQ.ZERO ) THEN
            S = ONE
         ELSE
            S = SCALE / ( SQRT( SCALE*SCALE / RNORM+RNORM )*
     $          SQRT( RNORM ) )
         END IF
      END IF
*
      IF( WANTSP ) THEN
*
*        Estimate sep(T11,T22).
*
         EST = ZERO
         KASE = 0
   30    CONTINUE
         CALL ZLACN2( NN, WORK( NN+1 ), WORK, EST, KASE, ISAVE )
         IF( KASE.NE.0 ) THEN
            IF( KASE.EQ.1 ) THEN
*
*              Solve T11*R - R*T22 = scale*X.
*
               CALL ZTRSYL( 'N', 'N', -1, N1, N2, T, LDT,
     $                      T( N1+1, N1+1 ), LDT, WORK, N1, SCALE,
     $                      IERR )
            ELSE
*
*              Solve T11**H*R - R*T22**H = scale*X.
*
               CALL ZTRSYL( 'C', 'C', -1, N1, N2, T, LDT,
     $                      T( N1+1, N1+1 ), LDT, WORK, N1, SCALE,
     $                      IERR )
            END IF
            GO TO 30
         END IF
*
         SEP = SCALE / EST
      END IF
*
   40 CONTINUE
*
*     Copy reordered eigenvalues to W.
*
      DO 50 K = 1, N
         W( K ) = T( K, K )
   50 CONTINUE
*
      WORK( 1 ) = LWMIN
*
      RETURN
*
*     End of ZTRSEN
*
      END
