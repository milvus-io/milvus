*> \brief <b> ZGEEVX computes the eigenvalues and, optionally, the left and/or right eigenvectors for GE matrices</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZGEGV + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zgegv.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zgegv.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zgegv.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZGEGV( JOBVL, JOBVR, N, A, LDA, B, LDB, ALPHA, BETA,
*                         VL, LDVL, VR, LDVR, WORK, LWORK, RWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          JOBVL, JOBVR
*       INTEGER            INFO, LDA, LDB, LDVL, LDVR, LWORK, N
*       ..
*       .. Array Arguments ..
*       DOUBLE PRECISION   RWORK( * )
*       COMPLEX*16         A( LDA, * ), ALPHA( * ), B( LDB, * ),
*      $                   BETA( * ), VL( LDVL, * ), VR( LDVR, * ),
*      $                   WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> This routine is deprecated and has been replaced by routine ZGGEV.
*>
*> ZGEGV computes the eigenvalues and, optionally, the left and/or right
*> eigenvectors of a complex matrix pair (A,B).
*> Given two square matrices A and B,
*> the generalized nonsymmetric eigenvalue problem (GNEP) is to find the
*> eigenvalues lambda and corresponding (non-zero) eigenvectors x such
*> that
*>    A*x = lambda*B*x.
*>
*> An alternate form is to find the eigenvalues mu and corresponding
*> eigenvectors y such that
*>    mu*A*y = B*y.
*>
*> These two forms are equivalent with mu = 1/lambda and x = y if
*> neither lambda nor mu is zero.  In order to deal with the case that
*> lambda or mu is zero or small, two values alpha and beta are returned
*> for each eigenvalue, such that lambda = alpha/beta and
*> mu = beta/alpha.
*>
*> The vectors x and y in the above equations are right eigenvectors of
*> the matrix pair (A,B).  Vectors u and v satisfying
*>    u**H*A = lambda*u**H*B  or  mu*v**H*A = v**H*B
*> are left eigenvectors of (A,B).
*>
*> Note: this routine performs "full balancing" on A and B
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] JOBVL
*> \verbatim
*>          JOBVL is CHARACTER*1
*>          = 'N':  do not compute the left generalized eigenvectors;
*>          = 'V':  compute the left generalized eigenvectors (returned
*>                  in VL).
*> \endverbatim
*>
*> \param[in] JOBVR
*> \verbatim
*>          JOBVR is CHARACTER*1
*>          = 'N':  do not compute the right generalized eigenvectors;
*>          = 'V':  compute the right generalized eigenvectors (returned
*>                  in VR).
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrices A, B, VL, and VR.  N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX*16 array, dimension (LDA, N)
*>          On entry, the matrix A.
*>          If JOBVL = 'V' or JOBVR = 'V', then on exit A
*>          contains the Schur form of A from the generalized Schur
*>          factorization of the pair (A,B) after balancing.  If no
*>          eigenvectors were computed, then only the diagonal elements
*>          of the Schur form will be correct.  See ZGGHRD and ZHGEQZ
*>          for details.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of A.  LDA >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is COMPLEX*16 array, dimension (LDB, N)
*>          On entry, the matrix B.
*>          If JOBVL = 'V' or JOBVR = 'V', then on exit B contains the
*>          upper triangular matrix obtained from B in the generalized
*>          Schur factorization of the pair (A,B) after balancing.
*>          If no eigenvectors were computed, then only the diagonal
*>          elements of B will be correct.  See ZGGHRD and ZHGEQZ for
*>          details.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of B.  LDB >= max(1,N).
*> \endverbatim
*>
*> \param[out] ALPHA
*> \verbatim
*>          ALPHA is COMPLEX*16 array, dimension (N)
*>          The complex scalars alpha that define the eigenvalues of
*>          GNEP.
*> \endverbatim
*>
*> \param[out] BETA
*> \verbatim
*>          BETA is COMPLEX*16 array, dimension (N)
*>          The complex scalars beta that define the eigenvalues of GNEP.
*>
*>          Together, the quantities alpha = ALPHA(j) and beta = BETA(j)
*>          represent the j-th eigenvalue of the matrix pair (A,B), in
*>          one of the forms lambda = alpha/beta or mu = beta/alpha.
*>          Since either lambda or mu may overflow, they should not,
*>          in general, be computed.
*> \endverbatim
*>
*> \param[out] VL
*> \verbatim
*>          VL is COMPLEX*16 array, dimension (LDVL,N)
*>          If JOBVL = 'V', the left eigenvectors u(j) are stored
*>          in the columns of VL, in the same order as their eigenvalues.
*>          Each eigenvector is scaled so that its largest component has
*>          abs(real part) + abs(imag. part) = 1, except for eigenvectors
*>          corresponding to an eigenvalue with alpha = beta = 0, which
*>          are set to zero.
*>          Not referenced if JOBVL = 'N'.
*> \endverbatim
*>
*> \param[in] LDVL
*> \verbatim
*>          LDVL is INTEGER
*>          The leading dimension of the matrix VL. LDVL >= 1, and
*>          if JOBVL = 'V', LDVL >= N.
*> \endverbatim
*>
*> \param[out] VR
*> \verbatim
*>          VR is COMPLEX*16 array, dimension (LDVR,N)
*>          If JOBVR = 'V', the right eigenvectors x(j) are stored
*>          in the columns of VR, in the same order as their eigenvalues.
*>          Each eigenvector is scaled so that its largest component has
*>          abs(real part) + abs(imag. part) = 1, except for eigenvectors
*>          corresponding to an eigenvalue with alpha = beta = 0, which
*>          are set to zero.
*>          Not referenced if JOBVR = 'N'.
*> \endverbatim
*>
*> \param[in] LDVR
*> \verbatim
*>          LDVR is INTEGER
*>          The leading dimension of the matrix VR. LDVR >= 1, and
*>          if JOBVR = 'V', LDVR >= N.
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
*>          The dimension of the array WORK.  LWORK >= max(1,2*N).
*>          For good performance, LWORK must generally be larger.
*>          To compute the optimal value of LWORK, call ILAENV to get
*>          blocksizes (for ZGEQRF, ZUNMQR, and ZUNGQR.)  Then compute:
*>          NB  -- MAX of the blocksizes for ZGEQRF, ZUNMQR, and ZUNGQR;
*>          The optimal LWORK is  MAX( 2*N, N*(NB+1) ).
*>
*>          If LWORK = -1, then a workspace query is assumed; the routine
*>          only calculates the optimal size of the WORK array, returns
*>          this value as the first entry of the WORK array, and no error
*>          message related to LWORK is issued by XERBLA.
*> \endverbatim
*>
*> \param[out] RWORK
*> \verbatim
*>          RWORK is DOUBLE PRECISION array, dimension (8*N)
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
*>          =1,...,N:
*>                The QZ iteration failed.  No eigenvectors have been
*>                calculated, but ALPHA(j) and BETA(j) should be
*>                correct for j=INFO+1,...,N.
*>          > N:  errors that usually indicate LAPACK problems:
*>                =N+1: error return from ZGGBAL
*>                =N+2: error return from ZGEQRF
*>                =N+3: error return from ZUNMQR
*>                =N+4: error return from ZUNGQR
*>                =N+5: error return from ZGGHRD
*>                =N+6: error return from ZHGEQZ (other than failed
*>                                               iteration)
*>                =N+7: error return from ZTGEVC
*>                =N+8: error return from ZGGBAK (computing VL)
*>                =N+9: error return from ZGGBAK (computing VR)
*>                =N+10: error return from ZLASCL (various calls)
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
*> \ingroup complex16GEeigen
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  Balancing
*>  ---------
*>
*>  This driver calls ZGGBAL to both permute and scale rows and columns
*>  of A and B.  The permutations PL and PR are chosen so that PL*A*PR
*>  and PL*B*R will be upper triangular except for the diagonal blocks
*>  A(i:j,i:j) and B(i:j,i:j), with i and j as close together as
*>  possible.  The diagonal scaling matrices DL and DR are chosen so
*>  that the pair  DL*PL*A*PR*DR, DL*PL*B*PR*DR have elements close to
*>  one (except for the elements that start out zero.)
*>
*>  After the eigenvalues and eigenvectors of the balanced matrices
*>  have been computed, ZGGBAK transforms the eigenvectors back to what
*>  they would have been (in perfect arithmetic) if they had not been
*>  balanced.
*>
*>  Contents of A and B on Exit
*>  -------- -- - --- - -- ----
*>
*>  If any eigenvectors are computed (either JOBVL='V' or JOBVR='V' or
*>  both), then on exit the arrays A and B will contain the complex Schur
*>  form[*] of the "balanced" versions of A and B.  If no eigenvectors
*>  are computed, then only the diagonal blocks will be correct.
*>
*>  [*] In other words, upper triangular form.
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE ZGEGV( JOBVL, JOBVR, N, A, LDA, B, LDB, ALPHA, BETA,
     $                  VL, LDVL, VR, LDVR, WORK, LWORK, RWORK, INFO )
*
*  -- LAPACK driver routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          JOBVL, JOBVR
      INTEGER            INFO, LDA, LDB, LDVL, LDVR, LWORK, N
*     ..
*     .. Array Arguments ..
      DOUBLE PRECISION   RWORK( * )
      COMPLEX*16         A( LDA, * ), ALPHA( * ), B( LDB, * ),
     $                   BETA( * ), VL( LDVL, * ), VR( LDVR, * ),
     $                   WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D0, ONE = 1.0D0 )
      COMPLEX*16         CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0D0, 0.0D0 ),
     $                   CONE = ( 1.0D0, 0.0D0 ) )
*     ..
*     .. Local Scalars ..
      LOGICAL            ILIMIT, ILV, ILVL, ILVR, LQUERY
      CHARACTER          CHTEMP
      INTEGER            ICOLS, IHI, IINFO, IJOBVL, IJOBVR, ILEFT, ILO,
     $                   IN, IRIGHT, IROWS, IRWORK, ITAU, IWORK, JC, JR,
     $                   LOPT, LWKMIN, LWKOPT, NB, NB1, NB2, NB3
      DOUBLE PRECISION   ABSAI, ABSAR, ABSB, ANRM, ANRM1, ANRM2, BNRM,
     $                   BNRM1, BNRM2, EPS, SAFMAX, SAFMIN, SALFAI,
     $                   SALFAR, SBETA, SCALE, TEMP
      COMPLEX*16         X
*     ..
*     .. Local Arrays ..
      LOGICAL            LDUMMA( 1 )
*     ..
*     .. External Subroutines ..
      EXTERNAL           XERBLA, ZGEQRF, ZGGBAK, ZGGBAL, ZGGHRD, ZHGEQZ,
     $                   ZLACPY, ZLASCL, ZLASET, ZTGEVC, ZUNGQR, ZUNMQR
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV
      DOUBLE PRECISION   DLAMCH, ZLANGE
      EXTERNAL           LSAME, ILAENV, DLAMCH, ZLANGE
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, DCMPLX, DIMAG, INT, MAX
*     ..
*     .. Statement Functions ..
      DOUBLE PRECISION   ABS1
*     ..
*     .. Statement Function definitions ..
      ABS1( X ) = ABS( DBLE( X ) ) + ABS( DIMAG( X ) )
*     ..
*     .. Executable Statements ..
*
*     Decode the input arguments
*
      IF( LSAME( JOBVL, 'N' ) ) THEN
         IJOBVL = 1
         ILVL = .FALSE.
      ELSE IF( LSAME( JOBVL, 'V' ) ) THEN
         IJOBVL = 2
         ILVL = .TRUE.
      ELSE
         IJOBVL = -1
         ILVL = .FALSE.
      END IF
*
      IF( LSAME( JOBVR, 'N' ) ) THEN
         IJOBVR = 1
         ILVR = .FALSE.
      ELSE IF( LSAME( JOBVR, 'V' ) ) THEN
         IJOBVR = 2
         ILVR = .TRUE.
      ELSE
         IJOBVR = -1
         ILVR = .FALSE.
      END IF
      ILV = ILVL .OR. ILVR
*
*     Test the input arguments
*
      LWKMIN = MAX( 2*N, 1 )
      LWKOPT = LWKMIN
      WORK( 1 ) = LWKOPT
      LQUERY = ( LWORK.EQ.-1 )
      INFO = 0
      IF( IJOBVL.LE.0 ) THEN
         INFO = -1
      ELSE IF( IJOBVR.LE.0 ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( LDA.LT.MAX( 1, N ) ) THEN
         INFO = -5
      ELSE IF( LDB.LT.MAX( 1, N ) ) THEN
         INFO = -7
      ELSE IF( LDVL.LT.1 .OR. ( ILVL .AND. LDVL.LT.N ) ) THEN
         INFO = -11
      ELSE IF( LDVR.LT.1 .OR. ( ILVR .AND. LDVR.LT.N ) ) THEN
         INFO = -13
      ELSE IF( LWORK.LT.LWKMIN .AND. .NOT.LQUERY ) THEN
         INFO = -15
      END IF
*
      IF( INFO.EQ.0 ) THEN
         NB1 = ILAENV( 1, 'ZGEQRF', ' ', N, N, -1, -1 )
         NB2 = ILAENV( 1, 'ZUNMQR', ' ', N, N, N, -1 )
         NB3 = ILAENV( 1, 'ZUNGQR', ' ', N, N, N, -1 )
         NB = MAX( NB1, NB2, NB3 )
         LOPT = MAX( 2*N, N*( NB+1 ) )
         WORK( 1 ) = LOPT
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZGEGV ', -INFO )
         RETURN
      ELSE IF( LQUERY ) THEN
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.EQ.0 )
     $   RETURN
*
*     Get machine constants
*
      EPS = DLAMCH( 'E' )*DLAMCH( 'B' )
      SAFMIN = DLAMCH( 'S' )
      SAFMIN = SAFMIN + SAFMIN
      SAFMAX = ONE / SAFMIN
*
*     Scale A
*
      ANRM = ZLANGE( 'M', N, N, A, LDA, RWORK )
      ANRM1 = ANRM
      ANRM2 = ONE
      IF( ANRM.LT.ONE ) THEN
         IF( SAFMAX*ANRM.LT.ONE ) THEN
            ANRM1 = SAFMIN
            ANRM2 = SAFMAX*ANRM
         END IF
      END IF
*
      IF( ANRM.GT.ZERO ) THEN
         CALL ZLASCL( 'G', -1, -1, ANRM, ONE, N, N, A, LDA, IINFO )
         IF( IINFO.NE.0 ) THEN
            INFO = N + 10
            RETURN
         END IF
      END IF
*
*     Scale B
*
      BNRM = ZLANGE( 'M', N, N, B, LDB, RWORK )
      BNRM1 = BNRM
      BNRM2 = ONE
      IF( BNRM.LT.ONE ) THEN
         IF( SAFMAX*BNRM.LT.ONE ) THEN
            BNRM1 = SAFMIN
            BNRM2 = SAFMAX*BNRM
         END IF
      END IF
*
      IF( BNRM.GT.ZERO ) THEN
         CALL ZLASCL( 'G', -1, -1, BNRM, ONE, N, N, B, LDB, IINFO )
         IF( IINFO.NE.0 ) THEN
            INFO = N + 10
            RETURN
         END IF
      END IF
*
*     Permute the matrix to make it more nearly triangular
*     Also "balance" the matrix.
*
      ILEFT = 1
      IRIGHT = N + 1
      IRWORK = IRIGHT + N
      CALL ZGGBAL( 'P', N, A, LDA, B, LDB, ILO, IHI, RWORK( ILEFT ),
     $             RWORK( IRIGHT ), RWORK( IRWORK ), IINFO )
      IF( IINFO.NE.0 ) THEN
         INFO = N + 1
         GO TO 80
      END IF
*
*     Reduce B to triangular form, and initialize VL and/or VR
*
      IROWS = IHI + 1 - ILO
      IF( ILV ) THEN
         ICOLS = N + 1 - ILO
      ELSE
         ICOLS = IROWS
      END IF
      ITAU = 1
      IWORK = ITAU + IROWS
      CALL ZGEQRF( IROWS, ICOLS, B( ILO, ILO ), LDB, WORK( ITAU ),
     $             WORK( IWORK ), LWORK+1-IWORK, IINFO )
      IF( IINFO.GE.0 )
     $   LWKOPT = MAX( LWKOPT, INT( WORK( IWORK ) )+IWORK-1 )
      IF( IINFO.NE.0 ) THEN
         INFO = N + 2
         GO TO 80
      END IF
*
      CALL ZUNMQR( 'L', 'C', IROWS, ICOLS, IROWS, B( ILO, ILO ), LDB,
     $             WORK( ITAU ), A( ILO, ILO ), LDA, WORK( IWORK ),
     $             LWORK+1-IWORK, IINFO )
      IF( IINFO.GE.0 )
     $   LWKOPT = MAX( LWKOPT, INT( WORK( IWORK ) )+IWORK-1 )
      IF( IINFO.NE.0 ) THEN
         INFO = N + 3
         GO TO 80
      END IF
*
      IF( ILVL ) THEN
         CALL ZLASET( 'Full', N, N, CZERO, CONE, VL, LDVL )
         CALL ZLACPY( 'L', IROWS-1, IROWS-1, B( ILO+1, ILO ), LDB,
     $                VL( ILO+1, ILO ), LDVL )
         CALL ZUNGQR( IROWS, IROWS, IROWS, VL( ILO, ILO ), LDVL,
     $                WORK( ITAU ), WORK( IWORK ), LWORK+1-IWORK,
     $                IINFO )
         IF( IINFO.GE.0 )
     $      LWKOPT = MAX( LWKOPT, INT( WORK( IWORK ) )+IWORK-1 )
         IF( IINFO.NE.0 ) THEN
            INFO = N + 4
            GO TO 80
         END IF
      END IF
*
      IF( ILVR )
     $   CALL ZLASET( 'Full', N, N, CZERO, CONE, VR, LDVR )
*
*     Reduce to generalized Hessenberg form
*
      IF( ILV ) THEN
*
*        Eigenvectors requested -- work on whole matrix.
*
         CALL ZGGHRD( JOBVL, JOBVR, N, ILO, IHI, A, LDA, B, LDB, VL,
     $                LDVL, VR, LDVR, IINFO )
      ELSE
         CALL ZGGHRD( 'N', 'N', IROWS, 1, IROWS, A( ILO, ILO ), LDA,
     $                B( ILO, ILO ), LDB, VL, LDVL, VR, LDVR, IINFO )
      END IF
      IF( IINFO.NE.0 ) THEN
         INFO = N + 5
         GO TO 80
      END IF
*
*     Perform QZ algorithm
*
      IWORK = ITAU
      IF( ILV ) THEN
         CHTEMP = 'S'
      ELSE
         CHTEMP = 'E'
      END IF
      CALL ZHGEQZ( CHTEMP, JOBVL, JOBVR, N, ILO, IHI, A, LDA, B, LDB,
     $             ALPHA, BETA, VL, LDVL, VR, LDVR, WORK( IWORK ),
     $             LWORK+1-IWORK, RWORK( IRWORK ), IINFO )
      IF( IINFO.GE.0 )
     $   LWKOPT = MAX( LWKOPT, INT( WORK( IWORK ) )+IWORK-1 )
      IF( IINFO.NE.0 ) THEN
         IF( IINFO.GT.0 .AND. IINFO.LE.N ) THEN
            INFO = IINFO
         ELSE IF( IINFO.GT.N .AND. IINFO.LE.2*N ) THEN
            INFO = IINFO - N
         ELSE
            INFO = N + 6
         END IF
         GO TO 80
      END IF
*
      IF( ILV ) THEN
*
*        Compute Eigenvectors
*
         IF( ILVL ) THEN
            IF( ILVR ) THEN
               CHTEMP = 'B'
            ELSE
               CHTEMP = 'L'
            END IF
         ELSE
            CHTEMP = 'R'
         END IF
*
         CALL ZTGEVC( CHTEMP, 'B', LDUMMA, N, A, LDA, B, LDB, VL, LDVL,
     $                VR, LDVR, N, IN, WORK( IWORK ), RWORK( IRWORK ),
     $                IINFO )
         IF( IINFO.NE.0 ) THEN
            INFO = N + 7
            GO TO 80
         END IF
*
*        Undo balancing on VL and VR, rescale
*
         IF( ILVL ) THEN
            CALL ZGGBAK( 'P', 'L', N, ILO, IHI, RWORK( ILEFT ),
     $                   RWORK( IRIGHT ), N, VL, LDVL, IINFO )
            IF( IINFO.NE.0 ) THEN
               INFO = N + 8
               GO TO 80
            END IF
            DO 30 JC = 1, N
               TEMP = ZERO
               DO 10 JR = 1, N
                  TEMP = MAX( TEMP, ABS1( VL( JR, JC ) ) )
   10          CONTINUE
               IF( TEMP.LT.SAFMIN )
     $            GO TO 30
               TEMP = ONE / TEMP
               DO 20 JR = 1, N
                  VL( JR, JC ) = VL( JR, JC )*TEMP
   20          CONTINUE
   30       CONTINUE
         END IF
         IF( ILVR ) THEN
            CALL ZGGBAK( 'P', 'R', N, ILO, IHI, RWORK( ILEFT ),
     $                   RWORK( IRIGHT ), N, VR, LDVR, IINFO )
            IF( IINFO.NE.0 ) THEN
               INFO = N + 9
               GO TO 80
            END IF
            DO 60 JC = 1, N
               TEMP = ZERO
               DO 40 JR = 1, N
                  TEMP = MAX( TEMP, ABS1( VR( JR, JC ) ) )
   40          CONTINUE
               IF( TEMP.LT.SAFMIN )
     $            GO TO 60
               TEMP = ONE / TEMP
               DO 50 JR = 1, N
                  VR( JR, JC ) = VR( JR, JC )*TEMP
   50          CONTINUE
   60       CONTINUE
         END IF
*
*        End of eigenvector calculation
*
      END IF
*
*     Undo scaling in alpha, beta
*
*     Note: this does not give the alpha and beta for the unscaled
*     problem.
*
*     Un-scaling is limited to avoid underflow in alpha and beta
*     if they are significant.
*
      DO 70 JC = 1, N
         ABSAR = ABS( DBLE( ALPHA( JC ) ) )
         ABSAI = ABS( DIMAG( ALPHA( JC ) ) )
         ABSB = ABS( DBLE( BETA( JC ) ) )
         SALFAR = ANRM*DBLE( ALPHA( JC ) )
         SALFAI = ANRM*DIMAG( ALPHA( JC ) )
         SBETA = BNRM*DBLE( BETA( JC ) )
         ILIMIT = .FALSE.
         SCALE = ONE
*
*        Check for significant underflow in imaginary part of ALPHA
*
         IF( ABS( SALFAI ).LT.SAFMIN .AND. ABSAI.GE.
     $       MAX( SAFMIN, EPS*ABSAR, EPS*ABSB ) ) THEN
            ILIMIT = .TRUE.
            SCALE = ( SAFMIN / ANRM1 ) / MAX( SAFMIN, ANRM2*ABSAI )
         END IF
*
*        Check for significant underflow in real part of ALPHA
*
         IF( ABS( SALFAR ).LT.SAFMIN .AND. ABSAR.GE.
     $       MAX( SAFMIN, EPS*ABSAI, EPS*ABSB ) ) THEN
            ILIMIT = .TRUE.
            SCALE = MAX( SCALE, ( SAFMIN / ANRM1 ) /
     $              MAX( SAFMIN, ANRM2*ABSAR ) )
         END IF
*
*        Check for significant underflow in BETA
*
         IF( ABS( SBETA ).LT.SAFMIN .AND. ABSB.GE.
     $       MAX( SAFMIN, EPS*ABSAR, EPS*ABSAI ) ) THEN
            ILIMIT = .TRUE.
            SCALE = MAX( SCALE, ( SAFMIN / BNRM1 ) /
     $              MAX( SAFMIN, BNRM2*ABSB ) )
         END IF
*
*        Check for possible overflow when limiting scaling
*
         IF( ILIMIT ) THEN
            TEMP = ( SCALE*SAFMIN )*MAX( ABS( SALFAR ), ABS( SALFAI ),
     $             ABS( SBETA ) )
            IF( TEMP.GT.ONE )
     $         SCALE = SCALE / TEMP
            IF( SCALE.LT.ONE )
     $         ILIMIT = .FALSE.
         END IF
*
*        Recompute un-scaled ALPHA, BETA if necessary.
*
         IF( ILIMIT ) THEN
            SALFAR = ( SCALE*DBLE( ALPHA( JC ) ) )*ANRM
            SALFAI = ( SCALE*DIMAG( ALPHA( JC ) ) )*ANRM
            SBETA = ( SCALE*BETA( JC ) )*BNRM
         END IF
         ALPHA( JC ) = DCMPLX( SALFAR, SALFAI )
         BETA( JC ) = SBETA
   70 CONTINUE
*
   80 CONTINUE
      WORK( 1 ) = LWKOPT
*
      RETURN
*
*     End of ZGEGV
*
      END
