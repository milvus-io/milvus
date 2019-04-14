*> \brief \b STRSEN
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download STRSEN + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/strsen.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/strsen.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/strsen.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE STRSEN( JOB, COMPQ, SELECT, N, T, LDT, Q, LDQ, WR, WI,
*                          M, S, SEP, WORK, LWORK, IWORK, LIWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          COMPQ, JOB
*       INTEGER            INFO, LDQ, LDT, LIWORK, LWORK, M, N
*       REAL               S, SEP
*       ..
*       .. Array Arguments ..
*       LOGICAL            SELECT( * )
*       INTEGER            IWORK( * )
*       REAL               Q( LDQ, * ), T( LDT, * ), WI( * ), WORK( * ),
*      $                   WR( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> STRSEN reorders the real Schur factorization of a real matrix
*> A = Q*T*Q**T, so that a selected cluster of eigenvalues appears in
*> the leading diagonal blocks of the upper quasi-triangular matrix T,
*> and the leading columns of Q form an orthonormal basis of the
*> corresponding right invariant subspace.
*>
*> Optionally the routine computes the reciprocal condition numbers of
*> the cluster of eigenvalues and/or the invariant subspace.
*>
*> T must be in Schur canonical form (as returned by SHSEQR), that is,
*> block upper triangular with 1-by-1 and 2-by-2 diagonal blocks; each
*> 2-by-2 diagonal block has its diagonal elements equal and its
*> off-diagonal elements of opposite sign.
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
*>          select a real eigenvalue w(j), SELECT(j) must be set to
*>          .TRUE.. To select a complex conjugate pair of eigenvalues
*>          w(j) and w(j+1), corresponding to a 2-by-2 diagonal block,
*>          either SELECT(j) or SELECT(j+1) or both must be set to
*>          .TRUE.; a complex conjugate pair of eigenvalues must be
*>          either both included in the cluster or both excluded.
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
*>          T is REAL array, dimension (LDT,N)
*>          On entry, the upper quasi-triangular matrix T, in Schur
*>          canonical form.
*>          On exit, T is overwritten by the reordered matrix T, again in
*>          Schur canonical form, with the selected eigenvalues in the
*>          leading diagonal blocks.
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
*>          Q is REAL array, dimension (LDQ,N)
*>          On entry, if COMPQ = 'V', the matrix Q of Schur vectors.
*>          On exit, if COMPQ = 'V', Q has been postmultiplied by the
*>          orthogonal transformation matrix which reorders T; the
*>          leading M columns of Q form an orthonormal basis for the
*>          specified invariant subspace.
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
*> \param[out] WR
*> \verbatim
*>          WR is REAL array, dimension (N)
*> \endverbatim
*>
*> \param[out] WI
*> \verbatim
*>          WI is REAL array, dimension (N)
*>
*>          The real and imaginary parts, respectively, of the reordered
*>          eigenvalues of T. The eigenvalues are stored in the same
*>          order as on the diagonal of T, with WR(i) = T(i,i) and, if
*>          T(i:i+1,i:i+1) is a 2-by-2 diagonal block, WI(i) > 0 and
*>          WI(i+1) = -WI(i). Note that if a complex eigenvalue is
*>          sufficiently ill-conditioned, then its value may differ
*>          significantly from its value before reordering.
*> \endverbatim
*>
*> \param[out] M
*> \verbatim
*>          M is INTEGER
*>          The dimension of the specified invariant subspace.
*>          0 < = M <= N.
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is REAL
*>          If JOB = 'E' or 'B', S is a lower bound on the reciprocal
*>          condition number for the selected cluster of eigenvalues.
*>          S cannot underestimate the true reciprocal condition number
*>          by more than a factor of sqrt(N). If M = 0 or N, S = 1.
*>          If JOB = 'N' or 'V', S is not referenced.
*> \endverbatim
*>
*> \param[out] SEP
*> \verbatim
*>          SEP is REAL
*>          If JOB = 'V' or 'B', SEP is the estimated reciprocal
*>          condition number of the specified invariant subspace. If
*>          M = 0 or N, SEP = norm(T).
*>          If JOB = 'N' or 'E', SEP is not referenced.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (MAX(1,LWORK))
*>          On exit, if INFO = 0, WORK(1) returns the optimal LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>          The dimension of the array WORK.
*>          If JOB = 'N', LWORK >= max(1,N);
*>          if JOB = 'E', LWORK >= max(1,M*(N-M));
*>          if JOB = 'V' or 'B', LWORK >= max(1,2*M*(N-M)).
*>
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal size of the WORK array, returns
*>          this value as the first entry of the WORK array, and no error
*>          message related to LWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (MAX(1,LIWORK))
*>          On exit, if INFO = 0, IWORK(1) returns the optimal LIWORK.
*> \endverbatim
*>
*> \param[in] LIWORK
*> \verbatim
*>          LIWORK is INTEGER
*>          The dimension of the array IWORK.
*>          If JOB = 'N' or 'E', LIWORK >= 1;
*>          if JOB = 'V' or 'B', LIWORK >= max(1,M*(N-M)).
*>
*>          If LIWORK = -1, then a workspace query is assumed; the
*>          routine only calculates the optimal size of the IWORK array,
*>          returns this value as the first entry of the IWORK array, and
*>          no error message related to LIWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0: successful exit
*>          < 0: if INFO = -i, the i-th argument had an illegal value
*>          = 1: reordering of T failed because some eigenvalues are too
*>               close to separate (the problem is very ill-conditioned);
*>               T may have been partially reordered, and WR and WI
*>               contain the eigenvalues in the same order as in T; S and
*>               SEP (if requested) are set to zero.
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
*> \date April 2012
*
*> \ingroup realOTHERcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  STRSEN first collects the selected eigenvalues by computing an
*>  orthogonal transformation Z to move them to the top left corner of T.
*>  In other words, the selected eigenvalues are the eigenvalues of T11
*>  in:
*>
*>          Z**T * T * Z = ( T11 T12 ) n1
*>                         (  0  T22 ) n2
*>                            n1  n2
*>
*>  where N = n1+n2 and Z**T means the transpose of Z. The first n1 columns
*>  of Z span the specified invariant subspace of T.
*>
*>  If T has been obtained from the real Schur factorization of a matrix
*>  A = Q*T*Q**T, then the reordered real Schur factorization of A is given
*>  by A = (Q*Z)*(Z**T*T*Z)*(Q*Z)**T, and the first n1 columns of Q*Z span
*>  the corresponding invariant subspace of A.
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
      SUBROUTINE STRSEN( JOB, COMPQ, SELECT, N, T, LDT, Q, LDQ, WR, WI,
     $                   M, S, SEP, WORK, LWORK, IWORK, LIWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     April 2012
*
*     .. Scalar Arguments ..
      CHARACTER          COMPQ, JOB
      INTEGER            INFO, LDQ, LDT, LIWORK, LWORK, M, N
      REAL               S, SEP
*     ..
*     .. Array Arguments ..
      LOGICAL            SELECT( * )
      INTEGER            IWORK( * )
      REAL               Q( LDQ, * ), T( LDT, * ), WI( * ), WORK( * ),
     $                   WR( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            LQUERY, PAIR, SWAP, WANTBH, WANTQ, WANTS,
     $                    WANTSP
      INTEGER            IERR, K, KASE, KK, KS, LIWMIN, LWMIN, N1, N2,
     $                   NN
      REAL               EST, RNORM, SCALE
*     ..
*     .. Local Arrays ..
      INTEGER            ISAVE( 3 )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      REAL               SLANGE
      EXTERNAL           LSAME, SLANGE
*     ..
*     .. External Subroutines ..
      EXTERNAL           SLACN2, SLACPY, STREXC, STRSYL, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, SQRT
*     ..
*     .. Executable Statements ..
*
*     Decode and test the input parameters
*
      WANTBH = LSAME( JOB, 'B' )
      WANTS = LSAME( JOB, 'E' ) .OR. WANTBH
      WANTSP = LSAME( JOB, 'V' ) .OR. WANTBH
      WANTQ = LSAME( COMPQ, 'V' )
*
      INFO = 0
      LQUERY = ( LWORK.EQ.-1 )
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
      ELSE
*
*        Set M to the dimension of the specified invariant subspace,
*        and test LWORK and LIWORK.
*
         M = 0
         PAIR = .FALSE.
         DO 10 K = 1, N
            IF( PAIR ) THEN
               PAIR = .FALSE.
            ELSE
               IF( K.LT.N ) THEN
                  IF( T( K+1, K ).EQ.ZERO ) THEN
                     IF( SELECT( K ) )
     $                  M = M + 1
                  ELSE
                     PAIR = .TRUE.
                     IF( SELECT( K ) .OR. SELECT( K+1 ) )
     $                  M = M + 2
                  END IF
               ELSE
                  IF( SELECT( N ) )
     $               M = M + 1
               END IF
            END IF
   10    CONTINUE
*
         N1 = M
         N2 = N - M
         NN = N1*N2
*
         IF(  WANTSP ) THEN
            LWMIN = MAX( 1, 2*NN )
            LIWMIN = MAX( 1, NN )
         ELSE IF( LSAME( JOB, 'N' ) ) THEN
            LWMIN = MAX( 1, N )
            LIWMIN = 1
         ELSE IF( LSAME( JOB, 'E' ) ) THEN
            LWMIN = MAX( 1, NN )
            LIWMIN = 1
         END IF
*
         IF( LWORK.LT.LWMIN .AND. .NOT.LQUERY ) THEN
            INFO = -15
         ELSE IF( LIWORK.LT.LIWMIN .AND. .NOT.LQUERY ) THEN
            INFO = -17
         END IF
      END IF
*
      IF( INFO.EQ.0 ) THEN
         WORK( 1 ) = LWMIN
         IWORK( 1 ) = LIWMIN
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'STRSEN', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Quick return if possible.
*
      IF( M.EQ.N .OR. M.EQ.0 ) THEN
         IF( WANTS )
     $      S = ONE
         IF( WANTSP )
     $      SEP = SLANGE( '1', N, N, T, LDT, WORK )
         GO TO 40
      END IF
*
*     Collect the selected blocks at the top-left corner of T.
*
      KS = 0
      PAIR = .FALSE.
      DO 20 K = 1, N
         IF( PAIR ) THEN
            PAIR = .FALSE.
         ELSE
            SWAP = SELECT( K )
            IF( K.LT.N ) THEN
               IF( T( K+1, K ).NE.ZERO ) THEN
                  PAIR = .TRUE.
                  SWAP = SWAP .OR. SELECT( K+1 )
               END IF
            END IF
            IF( SWAP ) THEN
               KS = KS + 1
*
*              Swap the K-th block to position KS.
*
               IERR = 0
               KK = K
               IF( K.NE.KS )
     $            CALL STREXC( COMPQ, N, T, LDT, Q, LDQ, KK, KS, WORK,
     $                         IERR )
               IF( IERR.EQ.1 .OR. IERR.EQ.2 ) THEN
*
*                 Blocks too close to swap: exit.
*
                  INFO = 1
                  IF( WANTS )
     $               S = ZERO
                  IF( WANTSP )
     $               SEP = ZERO
                  GO TO 40
               END IF
               IF( PAIR )
     $            KS = KS + 1
            END IF
         END IF
   20 CONTINUE
*
      IF( WANTS ) THEN
*
*        Solve Sylvester equation for R:
*
*           T11*R - R*T22 = scale*T12
*
         CALL SLACPY( 'F', N1, N2, T( 1, N1+1 ), LDT, WORK, N1 )
         CALL STRSYL( 'N', 'N', -1, N1, N2, T, LDT, T( N1+1, N1+1 ),
     $                LDT, WORK, N1, SCALE, IERR )
*
*        Estimate the reciprocal of the condition number of the cluster
*        of eigenvalues.
*
         RNORM = SLANGE( 'F', N1, N2, WORK, N1, WORK )
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
         CALL SLACN2( NN, WORK( NN+1 ), WORK, IWORK, EST, KASE, ISAVE )
         IF( KASE.NE.0 ) THEN
            IF( KASE.EQ.1 ) THEN
*
*              Solve  T11*R - R*T22 = scale*X.
*
               CALL STRSYL( 'N', 'N', -1, N1, N2, T, LDT,
     $                      T( N1+1, N1+1 ), LDT, WORK, N1, SCALE,
     $                      IERR )
            ELSE
*
*              Solve T11**T*R - R*T22**T = scale*X.
*
               CALL STRSYL( 'T', 'T', -1, N1, N2, T, LDT,
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
*     Store the output eigenvalues in WR and WI.
*
      DO 50 K = 1, N
         WR( K ) = T( K, K )
         WI( K ) = ZERO
   50 CONTINUE
      DO 60 K = 1, N - 1
         IF( T( K+1, K ).NE.ZERO ) THEN
            WI( K ) = SQRT( ABS( T( K, K+1 ) ) )*
     $                SQRT( ABS( T( K+1, K ) ) )
            WI( K+1 ) = -WI( K )
         END IF
   60 CONTINUE
*
      WORK( 1 ) = LWMIN
      IWORK( 1 ) = LIWMIN
*
      RETURN
*
*     End of STRSEN
*
      END
