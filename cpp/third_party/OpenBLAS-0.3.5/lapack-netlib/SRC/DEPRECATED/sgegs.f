*> \brief <b> SGEGS computes the eigenvalues and, optionally, the left and/or right eigenvectors for GE matrices</b>
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SGEGS + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/sgegs.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/sgegs.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/sgegs.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SGEGS( JOBVSL, JOBVSR, N, A, LDA, B, LDB, ALPHAR,
*                         ALPHAI, BETA, VSL, LDVSL, VSR, LDVSR, WORK,
*                         LWORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          JOBVSL, JOBVSR
*       INTEGER            INFO, LDA, LDB, LDVSL, LDVSR, LWORK, N
*       ..
*       .. Array Arguments ..
*       REAL               A( LDA, * ), ALPHAI( * ), ALPHAR( * ),
*      $                   B( LDB, * ), BETA( * ), VSL( LDVSL, * ),
*      $                   VSR( LDVSR, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> This routine is deprecated and has been replaced by routine SGGES.
*>
*> SGEGS computes the eigenvalues, real Schur form, and, optionally,
*> left and or/right Schur vectors of a real matrix pair (A,B).
*> Given two square matrices A and B, the generalized real Schur
*> factorization has the form
*>
*>   A = Q*S*Z**T,  B = Q*T*Z**T
*>
*> where Q and Z are orthogonal matrices, T is upper triangular, and S
*> is an upper quasi-triangular matrix with 1-by-1 and 2-by-2 diagonal
*> blocks, the 2-by-2 blocks corresponding to complex conjugate pairs
*> of eigenvalues of (A,B).  The columns of Q are the left Schur vectors
*> and the columns of Z are the right Schur vectors.
*>
*> If only the eigenvalues of (A,B) are needed, the driver routine
*> SGEGV should be used instead.  See SGEGV for a description of the
*> eigenvalues of the generalized nonsymmetric eigenvalue problem
*> (GNEP).
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] JOBVSL
*> \verbatim
*>          JOBVSL is CHARACTER*1
*>          = 'N':  do not compute the left Schur vectors;
*>          = 'V':  compute the left Schur vectors (returned in VSL).
*> \endverbatim
*>
*> \param[in] JOBVSR
*> \verbatim
*>          JOBVSR is CHARACTER*1
*>          = 'N':  do not compute the right Schur vectors;
*>          = 'V':  compute the right Schur vectors (returned in VSR).
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrices A, B, VSL, and VSR.  N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is REAL array, dimension (LDA, N)
*>          On entry, the matrix A.
*>          On exit, the upper quasi-triangular matrix S from the
*>          generalized real Schur factorization.
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
*>          B is REAL array, dimension (LDB, N)
*>          On entry, the matrix B.
*>          On exit, the upper triangular matrix T from the generalized
*>          real Schur factorization.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          The leading dimension of B.  LDB >= max(1,N).
*> \endverbatim
*>
*> \param[out] ALPHAR
*> \verbatim
*>          ALPHAR is REAL array, dimension (N)
*>          The real parts of each scalar alpha defining an eigenvalue
*>          of GNEP.
*> \endverbatim
*>
*> \param[out] ALPHAI
*> \verbatim
*>          ALPHAI is REAL array, dimension (N)
*>          The imaginary parts of each scalar alpha defining an
*>          eigenvalue of GNEP.  If ALPHAI(j) is zero, then the j-th
*>          eigenvalue is real; if positive, then the j-th and (j+1)-st
*>          eigenvalues are a complex conjugate pair, with
*>          ALPHAI(j+1) = -ALPHAI(j).
*> \endverbatim
*>
*> \param[out] BETA
*> \verbatim
*>          BETA is REAL array, dimension (N)
*>          The scalars beta that define the eigenvalues of GNEP.
*>          Together, the quantities alpha = (ALPHAR(j),ALPHAI(j)) and
*>          beta = BETA(j) represent the j-th eigenvalue of the matrix
*>          pair (A,B), in one of the forms lambda = alpha/beta or
*>          mu = beta/alpha.  Since either lambda or mu may overflow,
*>          they should not, in general, be computed.
*> \endverbatim
*>
*> \param[out] VSL
*> \verbatim
*>          VSL is REAL array, dimension (LDVSL,N)
*>          If JOBVSL = 'V', the matrix of left Schur vectors Q.
*>          Not referenced if JOBVSL = 'N'.
*> \endverbatim
*>
*> \param[in] LDVSL
*> \verbatim
*>          LDVSL is INTEGER
*>          The leading dimension of the matrix VSL. LDVSL >=1, and
*>          if JOBVSL = 'V', LDVSL >= N.
*> \endverbatim
*>
*> \param[out] VSR
*> \verbatim
*>          VSR is REAL array, dimension (LDVSR,N)
*>          If JOBVSR = 'V', the matrix of right Schur vectors Z.
*>          Not referenced if JOBVSR = 'N'.
*> \endverbatim
*>
*> \param[in] LDVSR
*> \verbatim
*>          LDVSR is INTEGER
*>          The leading dimension of the matrix VSR. LDVSR >= 1, and
*>          if JOBVSR = 'V', LDVSR >= N.
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
*>          The dimension of the array WORK.  LWORK >= max(1,4*N).
*>          For good performance, LWORK must generally be larger.
*>          To compute the optimal value of LWORK, call ILAENV to get
*>          blocksizes (for SGEQRF, SORMQR, and SORGQR.)  Then compute:
*>          NB  -- MAX of the blocksizes for SGEQRF, SORMQR, and SORGQR
*>          The optimal LWORK is  2*N + N*(NB+1).
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
*>          < 0:  if INFO = -i, the i-th argument had an illegal value.
*>          = 1,...,N:
*>                The QZ iteration failed.  (A,B) are not in Schur
*>                form, but ALPHAR(j), ALPHAI(j), and BETA(j) should
*>                be correct for j=INFO+1,...,N.
*>          > N:  errors that usually indicate LAPACK problems:
*>                =N+1: error return from SGGBAL
*>                =N+2: error return from SGEQRF
*>                =N+3: error return from SORMQR
*>                =N+4: error return from SORGQR
*>                =N+5: error return from SGGHRD
*>                =N+6: error return from SHGEQZ (other than failed
*>                                                iteration)
*>                =N+7: error return from SGGBAK (computing VSL)
*>                =N+8: error return from SGGBAK (computing VSR)
*>                =N+9: error return from SLASCL (various places)
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
*> \ingroup realGEeigen
*
*  =====================================================================
      SUBROUTINE SGEGS( JOBVSL, JOBVSR, N, A, LDA, B, LDB, ALPHAR,
     $                  ALPHAI, BETA, VSL, LDVSL, VSR, LDVSR, WORK,
     $                  LWORK, INFO )
*
*  -- LAPACK driver routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          JOBVSL, JOBVSR
      INTEGER            INFO, LDA, LDB, LDVSL, LDVSR, LWORK, N
*     ..
*     .. Array Arguments ..
      REAL               A( LDA, * ), ALPHAI( * ), ALPHAR( * ),
     $                   B( LDB, * ), BETA( * ), VSL( LDVSL, * ),
     $                   VSR( LDVSR, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E0, ONE = 1.0E0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            ILASCL, ILBSCL, ILVSL, ILVSR, LQUERY
      INTEGER            ICOLS, IHI, IINFO, IJOBVL, IJOBVR, ILEFT,
     $                   ILO, IRIGHT, IROWS, ITAU, IWORK, LOPT, LWKMIN,
     $                   LWKOPT, NB, NB1, NB2, NB3
      REAL               ANRM, ANRMTO, BIGNUM, BNRM, BNRMTO, EPS,
     $                   SAFMIN, SMLNUM
*     ..
*     .. External Subroutines ..
      EXTERNAL           SGEQRF, SGGBAK, SGGBAL, SGGHRD, SHGEQZ, SLACPY,
     $                   SLASCL, SLASET, SORGQR, SORMQR, XERBLA
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      INTEGER            ILAENV
      REAL               SLAMCH, SLANGE
      EXTERNAL           ILAENV, LSAME, SLAMCH, SLANGE
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          INT, MAX
*     ..
*     .. Executable Statements ..
*
*     Decode the input arguments
*
      IF( LSAME( JOBVSL, 'N' ) ) THEN
         IJOBVL = 1
         ILVSL = .FALSE.
      ELSE IF( LSAME( JOBVSL, 'V' ) ) THEN
         IJOBVL = 2
         ILVSL = .TRUE.
      ELSE
         IJOBVL = -1
         ILVSL = .FALSE.
      END IF
*
      IF( LSAME( JOBVSR, 'N' ) ) THEN
         IJOBVR = 1
         ILVSR = .FALSE.
      ELSE IF( LSAME( JOBVSR, 'V' ) ) THEN
         IJOBVR = 2
         ILVSR = .TRUE.
      ELSE
         IJOBVR = -1
         ILVSR = .FALSE.
      END IF
*
*     Test the input arguments
*
      LWKMIN = MAX( 4*N, 1 )
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
      ELSE IF( LDVSL.LT.1 .OR. ( ILVSL .AND. LDVSL.LT.N ) ) THEN
         INFO = -12
      ELSE IF( LDVSR.LT.1 .OR. ( ILVSR .AND. LDVSR.LT.N ) ) THEN
         INFO = -14
      ELSE IF( LWORK.LT.LWKMIN .AND. .NOT.LQUERY ) THEN
         INFO = -16
      END IF
*
      IF( INFO.EQ.0 ) THEN
         NB1 = ILAENV( 1, 'SGEQRF', ' ', N, N, -1, -1 )
         NB2 = ILAENV( 1, 'SORMQR', ' ', N, N, N, -1 )
         NB3 = ILAENV( 1, 'SORGQR', ' ', N, N, N, -1 )
         NB = MAX( NB1, NB2, NB3 )
         LOPT = 2*N+N*(NB+1)
         WORK( 1 ) = LOPT
      END IF
*
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SGEGS ', -INFO )
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
      EPS = SLAMCH( 'E' )*SLAMCH( 'B' )
      SAFMIN = SLAMCH( 'S' )
      SMLNUM = N*SAFMIN / EPS
      BIGNUM = ONE / SMLNUM
*
*     Scale A if max element outside range [SMLNUM,BIGNUM]
*
      ANRM = SLANGE( 'M', N, N, A, LDA, WORK )
      ILASCL = .FALSE.
      IF( ANRM.GT.ZERO .AND. ANRM.LT.SMLNUM ) THEN
         ANRMTO = SMLNUM
         ILASCL = .TRUE.
      ELSE IF( ANRM.GT.BIGNUM ) THEN
         ANRMTO = BIGNUM
         ILASCL = .TRUE.
      END IF
*
      IF( ILASCL ) THEN
         CALL SLASCL( 'G', -1, -1, ANRM, ANRMTO, N, N, A, LDA, IINFO )
         IF( IINFO.NE.0 ) THEN
            INFO = N + 9
            RETURN
         END IF
      END IF
*
*     Scale B if max element outside range [SMLNUM,BIGNUM]
*
      BNRM = SLANGE( 'M', N, N, B, LDB, WORK )
      ILBSCL = .FALSE.
      IF( BNRM.GT.ZERO .AND. BNRM.LT.SMLNUM ) THEN
         BNRMTO = SMLNUM
         ILBSCL = .TRUE.
      ELSE IF( BNRM.GT.BIGNUM ) THEN
         BNRMTO = BIGNUM
         ILBSCL = .TRUE.
      END IF
*
      IF( ILBSCL ) THEN
         CALL SLASCL( 'G', -1, -1, BNRM, BNRMTO, N, N, B, LDB, IINFO )
         IF( IINFO.NE.0 ) THEN
            INFO = N + 9
            RETURN
         END IF
      END IF
*
*     Permute the matrix to make it more nearly triangular
*     Workspace layout:  (2*N words -- "work..." not actually used)
*        left_permutation, right_permutation, work...
*
      ILEFT = 1
      IRIGHT = N + 1
      IWORK = IRIGHT + N
      CALL SGGBAL( 'P', N, A, LDA, B, LDB, ILO, IHI, WORK( ILEFT ),
     $             WORK( IRIGHT ), WORK( IWORK ), IINFO )
      IF( IINFO.NE.0 ) THEN
         INFO = N + 1
         GO TO 10
      END IF
*
*     Reduce B to triangular form, and initialize VSL and/or VSR
*     Workspace layout:  ("work..." must have at least N words)
*        left_permutation, right_permutation, tau, work...
*
      IROWS = IHI + 1 - ILO
      ICOLS = N + 1 - ILO
      ITAU = IWORK
      IWORK = ITAU + IROWS
      CALL SGEQRF( IROWS, ICOLS, B( ILO, ILO ), LDB, WORK( ITAU ),
     $             WORK( IWORK ), LWORK+1-IWORK, IINFO )
      IF( IINFO.GE.0 )
     $   LWKOPT = MAX( LWKOPT, INT( WORK( IWORK ) )+IWORK-1 )
      IF( IINFO.NE.0 ) THEN
         INFO = N + 2
         GO TO 10
      END IF
*
      CALL SORMQR( 'L', 'T', IROWS, ICOLS, IROWS, B( ILO, ILO ), LDB,
     $             WORK( ITAU ), A( ILO, ILO ), LDA, WORK( IWORK ),
     $             LWORK+1-IWORK, IINFO )
      IF( IINFO.GE.0 )
     $   LWKOPT = MAX( LWKOPT, INT( WORK( IWORK ) )+IWORK-1 )
      IF( IINFO.NE.0 ) THEN
         INFO = N + 3
         GO TO 10
      END IF
*
      IF( ILVSL ) THEN
         CALL SLASET( 'Full', N, N, ZERO, ONE, VSL, LDVSL )
         CALL SLACPY( 'L', IROWS-1, IROWS-1, B( ILO+1, ILO ), LDB,
     $                VSL( ILO+1, ILO ), LDVSL )
         CALL SORGQR( IROWS, IROWS, IROWS, VSL( ILO, ILO ), LDVSL,
     $                WORK( ITAU ), WORK( IWORK ), LWORK+1-IWORK,
     $                IINFO )
         IF( IINFO.GE.0 )
     $      LWKOPT = MAX( LWKOPT, INT( WORK( IWORK ) )+IWORK-1 )
         IF( IINFO.NE.0 ) THEN
            INFO = N + 4
            GO TO 10
         END IF
      END IF
*
      IF( ILVSR )
     $   CALL SLASET( 'Full', N, N, ZERO, ONE, VSR, LDVSR )
*
*     Reduce to generalized Hessenberg form
*
      CALL SGGHRD( JOBVSL, JOBVSR, N, ILO, IHI, A, LDA, B, LDB, VSL,
     $             LDVSL, VSR, LDVSR, IINFO )
      IF( IINFO.NE.0 ) THEN
         INFO = N + 5
         GO TO 10
      END IF
*
*     Perform QZ algorithm, computing Schur vectors if desired
*     Workspace layout:  ("work..." must have at least 1 word)
*        left_permutation, right_permutation, work...
*
      IWORK = ITAU
      CALL SHGEQZ( 'S', JOBVSL, JOBVSR, N, ILO, IHI, A, LDA, B, LDB,
     $             ALPHAR, ALPHAI, BETA, VSL, LDVSL, VSR, LDVSR,
     $             WORK( IWORK ), LWORK+1-IWORK, IINFO )
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
         GO TO 10
      END IF
*
*     Apply permutation to VSL and VSR
*
      IF( ILVSL ) THEN
         CALL SGGBAK( 'P', 'L', N, ILO, IHI, WORK( ILEFT ),
     $                WORK( IRIGHT ), N, VSL, LDVSL, IINFO )
         IF( IINFO.NE.0 ) THEN
            INFO = N + 7
            GO TO 10
         END IF
      END IF
      IF( ILVSR ) THEN
         CALL SGGBAK( 'P', 'R', N, ILO, IHI, WORK( ILEFT ),
     $                WORK( IRIGHT ), N, VSR, LDVSR, IINFO )
         IF( IINFO.NE.0 ) THEN
            INFO = N + 8
            GO TO 10
         END IF
      END IF
*
*     Undo scaling
*
      IF( ILASCL ) THEN
         CALL SLASCL( 'H', -1, -1, ANRMTO, ANRM, N, N, A, LDA, IINFO )
         IF( IINFO.NE.0 ) THEN
            INFO = N + 9
            RETURN
         END IF
         CALL SLASCL( 'G', -1, -1, ANRMTO, ANRM, N, 1, ALPHAR, N,
     $                IINFO )
         IF( IINFO.NE.0 ) THEN
            INFO = N + 9
            RETURN
         END IF
         CALL SLASCL( 'G', -1, -1, ANRMTO, ANRM, N, 1, ALPHAI, N,
     $                IINFO )
         IF( IINFO.NE.0 ) THEN
            INFO = N + 9
            RETURN
         END IF
      END IF
*
      IF( ILBSCL ) THEN
         CALL SLASCL( 'U', -1, -1, BNRMTO, BNRM, N, N, B, LDB, IINFO )
         IF( IINFO.NE.0 ) THEN
            INFO = N + 9
            RETURN
         END IF
         CALL SLASCL( 'G', -1, -1, BNRMTO, BNRM, N, 1, BETA, N, IINFO )
         IF( IINFO.NE.0 ) THEN
            INFO = N + 9
            RETURN
         END IF
      END IF
*
   10 CONTINUE
      WORK( 1 ) = LWKOPT
*
      RETURN
*
*     End of SGEGS
*
      END
