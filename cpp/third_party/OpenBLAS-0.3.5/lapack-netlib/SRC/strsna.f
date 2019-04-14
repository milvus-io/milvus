*> \brief \b STRSNA
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download STRSNA + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/strsna.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/strsna.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/strsna.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE STRSNA( JOB, HOWMNY, SELECT, N, T, LDT, VL, LDVL, VR,
*                          LDVR, S, SEP, MM, M, WORK, LDWORK, IWORK,
*                          INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          HOWMNY, JOB
*       INTEGER            INFO, LDT, LDVL, LDVR, LDWORK, M, MM, N
*       ..
*       .. Array Arguments ..
*       LOGICAL            SELECT( * )
*       INTEGER            IWORK( * )
*       REAL               S( * ), SEP( * ), T( LDT, * ), VL( LDVL, * ),
*      $                   VR( LDVR, * ), WORK( LDWORK, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> STRSNA estimates reciprocal condition numbers for specified
*> eigenvalues and/or right eigenvectors of a real upper
*> quasi-triangular matrix T (or of any matrix Q*T*Q**T with Q
*> orthogonal).
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
*>          Specifies whether condition numbers are required for
*>          eigenvalues (S) or eigenvectors (SEP):
*>          = 'E': for eigenvalues only (S);
*>          = 'V': for eigenvectors only (SEP);
*>          = 'B': for both eigenvalues and eigenvectors (S and SEP).
*> \endverbatim
*>
*> \param[in] HOWMNY
*> \verbatim
*>          HOWMNY is CHARACTER*1
*>          = 'A': compute condition numbers for all eigenpairs;
*>          = 'S': compute condition numbers for selected eigenpairs
*>                 specified by the array SELECT.
*> \endverbatim
*>
*> \param[in] SELECT
*> \verbatim
*>          SELECT is LOGICAL array, dimension (N)
*>          If HOWMNY = 'S', SELECT specifies the eigenpairs for which
*>          condition numbers are required. To select condition numbers
*>          for the eigenpair corresponding to a real eigenvalue w(j),
*>          SELECT(j) must be set to .TRUE.. To select condition numbers
*>          corresponding to a complex conjugate pair of eigenvalues w(j)
*>          and w(j+1), either SELECT(j) or SELECT(j+1) or both, must be
*>          set to .TRUE..
*>          If HOWMNY = 'A', SELECT is not referenced.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix T. N >= 0.
*> \endverbatim
*>
*> \param[in] T
*> \verbatim
*>          T is REAL array, dimension (LDT,N)
*>          The upper quasi-triangular matrix T, in Schur canonical form.
*> \endverbatim
*>
*> \param[in] LDT
*> \verbatim
*>          LDT is INTEGER
*>          The leading dimension of the array T. LDT >= max(1,N).
*> \endverbatim
*>
*> \param[in] VL
*> \verbatim
*>          VL is REAL array, dimension (LDVL,M)
*>          If JOB = 'E' or 'B', VL must contain left eigenvectors of T
*>          (or of any Q*T*Q**T with Q orthogonal), corresponding to the
*>          eigenpairs specified by HOWMNY and SELECT. The eigenvectors
*>          must be stored in consecutive columns of VL, as returned by
*>          SHSEIN or STREVC.
*>          If JOB = 'V', VL is not referenced.
*> \endverbatim
*>
*> \param[in] LDVL
*> \verbatim
*>          LDVL is INTEGER
*>          The leading dimension of the array VL.
*>          LDVL >= 1; and if JOB = 'E' or 'B', LDVL >= N.
*> \endverbatim
*>
*> \param[in] VR
*> \verbatim
*>          VR is REAL array, dimension (LDVR,M)
*>          If JOB = 'E' or 'B', VR must contain right eigenvectors of T
*>          (or of any Q*T*Q**T with Q orthogonal), corresponding to the
*>          eigenpairs specified by HOWMNY and SELECT. The eigenvectors
*>          must be stored in consecutive columns of VR, as returned by
*>          SHSEIN or STREVC.
*>          If JOB = 'V', VR is not referenced.
*> \endverbatim
*>
*> \param[in] LDVR
*> \verbatim
*>          LDVR is INTEGER
*>          The leading dimension of the array VR.
*>          LDVR >= 1; and if JOB = 'E' or 'B', LDVR >= N.
*> \endverbatim
*>
*> \param[out] S
*> \verbatim
*>          S is REAL array, dimension (MM)
*>          If JOB = 'E' or 'B', the reciprocal condition numbers of the
*>          selected eigenvalues, stored in consecutive elements of the
*>          array. For a complex conjugate pair of eigenvalues two
*>          consecutive elements of S are set to the same value. Thus
*>          S(j), SEP(j), and the j-th columns of VL and VR all
*>          correspond to the same eigenpair (but not in general the
*>          j-th eigenpair, unless all eigenpairs are selected).
*>          If JOB = 'V', S is not referenced.
*> \endverbatim
*>
*> \param[out] SEP
*> \verbatim
*>          SEP is REAL array, dimension (MM)
*>          If JOB = 'V' or 'B', the estimated reciprocal condition
*>          numbers of the selected eigenvectors, stored in consecutive
*>          elements of the array. For a complex eigenvector two
*>          consecutive elements of SEP are set to the same value. If
*>          the eigenvalues cannot be reordered to compute SEP(j), SEP(j)
*>          is set to 0; this can only occur when the true value would be
*>          very small anyway.
*>          If JOB = 'E', SEP is not referenced.
*> \endverbatim
*>
*> \param[in] MM
*> \verbatim
*>          MM is INTEGER
*>          The number of elements in the arrays S (if JOB = 'E' or 'B')
*>           and/or SEP (if JOB = 'V' or 'B'). MM >= M.
*> \endverbatim
*>
*> \param[out] M
*> \verbatim
*>          M is INTEGER
*>          The number of elements of the arrays S and/or SEP actually
*>          used to store the estimated condition numbers.
*>          If HOWMNY = 'A', M is set to N.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (LDWORK,N+6)
*>          If JOB = 'E', WORK is not referenced.
*> \endverbatim
*>
*> \param[in] LDWORK
*> \verbatim
*>          LDWORK is INTEGER
*>          The leading dimension of the array WORK.
*>          LDWORK >= 1; and if JOB = 'V' or 'B', LDWORK >= N.
*> \endverbatim
*>
*> \param[out] IWORK
*> \verbatim
*>          IWORK is INTEGER array, dimension (2*(N-1))
*>          If JOB = 'E', IWORK is not referenced.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0: successful exit
*>          < 0: if INFO = -i, the i-th argument had an illegal value
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
*> \ingroup realOTHERcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  The reciprocal of the condition number of an eigenvalue lambda is
*>  defined as
*>
*>          S(lambda) = |v**T*u| / (norm(u)*norm(v))
*>
*>  where u and v are the right and left eigenvectors of T corresponding
*>  to lambda; v**T denotes the transpose of v, and norm(u)
*>  denotes the Euclidean norm. These reciprocal condition numbers always
*>  lie between zero (very badly conditioned) and one (very well
*>  conditioned). If n = 1, S(lambda) is defined to be 1.
*>
*>  An approximate error bound for a computed eigenvalue W(i) is given by
*>
*>                      EPS * norm(T) / S(i)
*>
*>  where EPS is the machine precision.
*>
*>  The reciprocal of the condition number of the right eigenvector u
*>  corresponding to lambda is defined as follows. Suppose
*>
*>              T = ( lambda  c  )
*>                  (   0    T22 )
*>
*>  Then the reciprocal condition number is
*>
*>          SEP( lambda, T22 ) = sigma-min( T22 - lambda*I )
*>
*>  where sigma-min denotes the smallest singular value. We approximate
*>  the smallest singular value by the reciprocal of an estimate of the
*>  one-norm of the inverse of T22 - lambda*I. If n = 1, SEP(1) is
*>  defined to be abs(T(1,1)).
*>
*>  An approximate error bound for a computed right eigenvector VR(i)
*>  is given by
*>
*>                      EPS * norm(T) / SEP(i)
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE STRSNA( JOB, HOWMNY, SELECT, N, T, LDT, VL, LDVL, VR,
     $                   LDVR, S, SEP, MM, M, WORK, LDWORK, IWORK,
     $                   INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          HOWMNY, JOB
      INTEGER            INFO, LDT, LDVL, LDVR, LDWORK, M, MM, N
*     ..
*     .. Array Arguments ..
      LOGICAL            SELECT( * )
      INTEGER            IWORK( * )
      REAL               S( * ), SEP( * ), T( LDT, * ), VL( LDVL, * ),
     $                   VR( LDVR, * ), WORK( LDWORK, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE, TWO
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0, TWO = 2.0E+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            PAIR, SOMCON, WANTBH, WANTS, WANTSP
      INTEGER            I, IERR, IFST, ILST, J, K, KASE, KS, N2, NN
      REAL               BIGNUM, COND, CS, DELTA, DUMM, EPS, EST, LNRM,
     $                   MU, PROD, PROD1, PROD2, RNRM, SCALE, SMLNUM, SN
*     ..
*     .. Local Arrays ..
      INTEGER            ISAVE( 3 )
      REAL               DUMMY( 1 )
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      REAL               SDOT, SLAMCH, SLAPY2, SNRM2
      EXTERNAL           LSAME, SDOT, SLAMCH, SLAPY2, SNRM2
*     ..
*     .. External Subroutines ..
      EXTERNAL           SLABAD, SLACN2, SLACPY, SLAQTR, STREXC, XERBLA
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
*
      SOMCON = LSAME( HOWMNY, 'S' )
*
      INFO = 0
      IF( .NOT.WANTS .AND. .NOT.WANTSP ) THEN
         INFO = -1
      ELSE IF( .NOT.LSAME( HOWMNY, 'A' ) .AND. .NOT.SOMCON ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -4
      ELSE IF( LDT.LT.MAX( 1, N ) ) THEN
         INFO = -6
      ELSE IF( LDVL.LT.1 .OR. ( WANTS .AND. LDVL.LT.N ) ) THEN
         INFO = -8
      ELSE IF( LDVR.LT.1 .OR. ( WANTS .AND. LDVR.LT.N ) ) THEN
         INFO = -10
      ELSE
*
*        Set M to the number of eigenpairs for which condition numbers
*        are required, and test MM.
*
         IF( SOMCON ) THEN
            M = 0
            PAIR = .FALSE.
            DO 10 K = 1, N
               IF( PAIR ) THEN
                  PAIR = .FALSE.
               ELSE
                  IF( K.LT.N ) THEN
                     IF( T( K+1, K ).EQ.ZERO ) THEN
                        IF( SELECT( K ) )
     $                     M = M + 1
                     ELSE
                        PAIR = .TRUE.
                        IF( SELECT( K ) .OR. SELECT( K+1 ) )
     $                     M = M + 2
                     END IF
                  ELSE
                     IF( SELECT( N ) )
     $                  M = M + 1
                  END IF
               END IF
   10       CONTINUE
         ELSE
            M = N
         END IF
*
         IF( MM.LT.M ) THEN
            INFO = -13
         ELSE IF( LDWORK.LT.1 .OR. ( WANTSP .AND. LDWORK.LT.N ) ) THEN
            INFO = -16
         END IF
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'STRSNA', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 )
     $   RETURN
*
      IF( N.EQ.1 ) THEN
         IF( SOMCON ) THEN
            IF( .NOT.SELECT( 1 ) )
     $         RETURN
         END IF
         IF( WANTS )
     $      S( 1 ) = ONE
         IF( WANTSP )
     $      SEP( 1 ) = ABS( T( 1, 1 ) )
         RETURN
      END IF
*
*     Get machine constants
*
      EPS = SLAMCH( 'P' )
      SMLNUM = SLAMCH( 'S' ) / EPS
      BIGNUM = ONE / SMLNUM
      CALL SLABAD( SMLNUM, BIGNUM )
*
      KS = 0
      PAIR = .FALSE.
      DO 60 K = 1, N
*
*        Determine whether T(k,k) begins a 1-by-1 or 2-by-2 block.
*
         IF( PAIR ) THEN
            PAIR = .FALSE.
            GO TO 60
         ELSE
            IF( K.LT.N )
     $         PAIR = T( K+1, K ).NE.ZERO
         END IF
*
*        Determine whether condition numbers are required for the k-th
*        eigenpair.
*
         IF( SOMCON ) THEN
            IF( PAIR ) THEN
               IF( .NOT.SELECT( K ) .AND. .NOT.SELECT( K+1 ) )
     $            GO TO 60
            ELSE
               IF( .NOT.SELECT( K ) )
     $            GO TO 60
            END IF
         END IF
*
         KS = KS + 1
*
         IF( WANTS ) THEN
*
*           Compute the reciprocal condition number of the k-th
*           eigenvalue.
*
            IF( .NOT.PAIR ) THEN
*
*              Real eigenvalue.
*
               PROD = SDOT( N, VR( 1, KS ), 1, VL( 1, KS ), 1 )
               RNRM = SNRM2( N, VR( 1, KS ), 1 )
               LNRM = SNRM2( N, VL( 1, KS ), 1 )
               S( KS ) = ABS( PROD ) / ( RNRM*LNRM )
            ELSE
*
*              Complex eigenvalue.
*
               PROD1 = SDOT( N, VR( 1, KS ), 1, VL( 1, KS ), 1 )
               PROD1 = PROD1 + SDOT( N, VR( 1, KS+1 ), 1, VL( 1, KS+1 ),
     $                 1 )
               PROD2 = SDOT( N, VL( 1, KS ), 1, VR( 1, KS+1 ), 1 )
               PROD2 = PROD2 - SDOT( N, VL( 1, KS+1 ), 1, VR( 1, KS ),
     $                 1 )
               RNRM = SLAPY2( SNRM2( N, VR( 1, KS ), 1 ),
     $                SNRM2( N, VR( 1, KS+1 ), 1 ) )
               LNRM = SLAPY2( SNRM2( N, VL( 1, KS ), 1 ),
     $                SNRM2( N, VL( 1, KS+1 ), 1 ) )
               COND = SLAPY2( PROD1, PROD2 ) / ( RNRM*LNRM )
               S( KS ) = COND
               S( KS+1 ) = COND
            END IF
         END IF
*
         IF( WANTSP ) THEN
*
*           Estimate the reciprocal condition number of the k-th
*           eigenvector.
*
*           Copy the matrix T to the array WORK and swap the diagonal
*           block beginning at T(k,k) to the (1,1) position.
*
            CALL SLACPY( 'Full', N, N, T, LDT, WORK, LDWORK )
            IFST = K
            ILST = 1
            CALL STREXC( 'No Q', N, WORK, LDWORK, DUMMY, 1, IFST, ILST,
     $                   WORK( 1, N+1 ), IERR )
*
            IF( IERR.EQ.1 .OR. IERR.EQ.2 ) THEN
*
*              Could not swap because blocks not well separated
*
               SCALE = ONE
               EST = BIGNUM
            ELSE
*
*              Reordering successful
*
               IF( WORK( 2, 1 ).EQ.ZERO ) THEN
*
*                 Form C = T22 - lambda*I in WORK(2:N,2:N).
*
                  DO 20 I = 2, N
                     WORK( I, I ) = WORK( I, I ) - WORK( 1, 1 )
   20             CONTINUE
                  N2 = 1
                  NN = N - 1
               ELSE
*
*                 Triangularize the 2 by 2 block by unitary
*                 transformation U = [  cs   i*ss ]
*                                    [ i*ss   cs  ].
*                 such that the (1,1) position of WORK is complex
*                 eigenvalue lambda with positive imaginary part. (2,2)
*                 position of WORK is the complex eigenvalue lambda
*                 with negative imaginary  part.
*
                  MU = SQRT( ABS( WORK( 1, 2 ) ) )*
     $                 SQRT( ABS( WORK( 2, 1 ) ) )
                  DELTA = SLAPY2( MU, WORK( 2, 1 ) )
                  CS = MU / DELTA
                  SN = -WORK( 2, 1 ) / DELTA
*
*                 Form
*
*                 C**T = WORK(2:N,2:N) + i*[rwork(1) ..... rwork(n-1) ]
*                                          [   mu                     ]
*                                          [         ..               ]
*                                          [             ..           ]
*                                          [                  mu      ]
*                 where C**T is transpose of matrix C,
*                 and RWORK is stored starting in the N+1-st column of
*                 WORK.
*
                  DO 30 J = 3, N
                     WORK( 2, J ) = CS*WORK( 2, J )
                     WORK( J, J ) = WORK( J, J ) - WORK( 1, 1 )
   30             CONTINUE
                  WORK( 2, 2 ) = ZERO
*
                  WORK( 1, N+1 ) = TWO*MU
                  DO 40 I = 2, N - 1
                     WORK( I, N+1 ) = SN*WORK( 1, I+1 )
   40             CONTINUE
                  N2 = 2
                  NN = 2*( N-1 )
               END IF
*
*              Estimate norm(inv(C**T))
*
               EST = ZERO
               KASE = 0
   50          CONTINUE
               CALL SLACN2( NN, WORK( 1, N+2 ), WORK( 1, N+4 ), IWORK,
     $                      EST, KASE, ISAVE )
               IF( KASE.NE.0 ) THEN
                  IF( KASE.EQ.1 ) THEN
                     IF( N2.EQ.1 ) THEN
*
*                       Real eigenvalue: solve C**T*x = scale*c.
*
                        CALL SLAQTR( .TRUE., .TRUE., N-1, WORK( 2, 2 ),
     $                               LDWORK, DUMMY, DUMM, SCALE,
     $                               WORK( 1, N+4 ), WORK( 1, N+6 ),
     $                               IERR )
                     ELSE
*
*                       Complex eigenvalue: solve
*                       C**T*(p+iq) = scale*(c+id) in real arithmetic.
*
                        CALL SLAQTR( .TRUE., .FALSE., N-1, WORK( 2, 2 ),
     $                               LDWORK, WORK( 1, N+1 ), MU, SCALE,
     $                               WORK( 1, N+4 ), WORK( 1, N+6 ),
     $                               IERR )
                     END IF
                  ELSE
                     IF( N2.EQ.1 ) THEN
*
*                       Real eigenvalue: solve C*x = scale*c.
*
                        CALL SLAQTR( .FALSE., .TRUE., N-1, WORK( 2, 2 ),
     $                               LDWORK, DUMMY, DUMM, SCALE,
     $                               WORK( 1, N+4 ), WORK( 1, N+6 ),
     $                               IERR )
                     ELSE
*
*                       Complex eigenvalue: solve
*                       C*(p+iq) = scale*(c+id) in real arithmetic.
*
                        CALL SLAQTR( .FALSE., .FALSE., N-1,
     $                               WORK( 2, 2 ), LDWORK,
     $                               WORK( 1, N+1 ), MU, SCALE,
     $                               WORK( 1, N+4 ), WORK( 1, N+6 ),
     $                               IERR )
*
                     END IF
                  END IF
*
                  GO TO 50
               END IF
            END IF
*
            SEP( KS ) = SCALE / MAX( EST, SMLNUM )
            IF( PAIR )
     $         SEP( KS+1 ) = SEP( KS )
         END IF
*
         IF( PAIR )
     $      KS = KS + 1
*
   60 CONTINUE
      RETURN
*
*     End of STRSNA
*
      END
