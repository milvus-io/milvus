*> \brief \b SHSEQR
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SHSEQR + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/shseqr.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/shseqr.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/shseqr.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SHSEQR( JOB, COMPZ, N, ILO, IHI, H, LDH, WR, WI, Z,
*                          LDZ, WORK, LWORK, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER            IHI, ILO, INFO, LDH, LDZ, LWORK, N
*       CHARACTER          COMPZ, JOB
*       ..
*       .. Array Arguments ..
*       REAL               H( LDH, * ), WI( * ), WORK( * ), WR( * ),
*      $                   Z( LDZ, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    SHSEQR computes the eigenvalues of a Hessenberg matrix H
*>    and, optionally, the matrices T and Z from the Schur decomposition
*>    H = Z T Z**T, where T is an upper quasi-triangular matrix (the
*>    Schur form), and Z is the orthogonal matrix of Schur vectors.
*>
*>    Optionally Z may be postmultiplied into an input orthogonal
*>    matrix Q so that this routine can give the Schur factorization
*>    of a matrix A which has been reduced to the Hessenberg form H
*>    by the orthogonal matrix Q:  A = Q*H*Q**T = (QZ)*T*(QZ)**T.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] JOB
*> \verbatim
*>          JOB is CHARACTER*1
*>           = 'E':  compute eigenvalues only;
*>           = 'S':  compute eigenvalues and the Schur form T.
*> \endverbatim
*>
*> \param[in] COMPZ
*> \verbatim
*>          COMPZ is CHARACTER*1
*>           = 'N':  no Schur vectors are computed;
*>           = 'I':  Z is initialized to the unit matrix and the matrix Z
*>                   of Schur vectors of H is returned;
*>           = 'V':  Z must contain an orthogonal matrix Q on entry, and
*>                   the product Q*Z is returned.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>           The order of the matrix H.  N .GE. 0.
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
*>           It is assumed that H is already upper triangular in rows
*>           and columns 1:ILO-1 and IHI+1:N. ILO and IHI are normally
*>           set by a previous call to SGEBAL, and then passed to ZGEHRD
*>           when the matrix output by SGEBAL is reduced to Hessenberg
*>           form. Otherwise ILO and IHI should be set to 1 and N
*>           respectively.  If N.GT.0, then 1.LE.ILO.LE.IHI.LE.N.
*>           If N = 0, then ILO = 1 and IHI = 0.
*> \endverbatim
*>
*> \param[in,out] H
*> \verbatim
*>          H is REAL array, dimension (LDH,N)
*>           On entry, the upper Hessenberg matrix H.
*>           On exit, if INFO = 0 and JOB = 'S', then H contains the
*>           upper quasi-triangular matrix T from the Schur decomposition
*>           (the Schur form); 2-by-2 diagonal blocks (corresponding to
*>           complex conjugate pairs of eigenvalues) are returned in
*>           standard form, with H(i,i) = H(i+1,i+1) and
*>           H(i+1,i)*H(i,i+1).LT.0. If INFO = 0 and JOB = 'E', the
*>           contents of H are unspecified on exit.  (The output value of
*>           H when INFO.GT.0 is given under the description of INFO
*>           below.)
*>
*>           Unlike earlier versions of SHSEQR, this subroutine may
*>           explicitly H(i,j) = 0 for i.GT.j and j = 1, 2, ... ILO-1
*>           or j = IHI+1, IHI+2, ... N.
*> \endverbatim
*>
*> \param[in] LDH
*> \verbatim
*>          LDH is INTEGER
*>           The leading dimension of the array H. LDH .GE. max(1,N).
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
*>           The real and imaginary parts, respectively, of the computed
*>           eigenvalues. If two eigenvalues are computed as a complex
*>           conjugate pair, they are stored in consecutive elements of
*>           WR and WI, say the i-th and (i+1)th, with WI(i) .GT. 0 and
*>           WI(i+1) .LT. 0. If JOB = 'S', the eigenvalues are stored in
*>           the same order as on the diagonal of the Schur form returned
*>           in H, with WR(i) = H(i,i) and, if H(i:i+1,i:i+1) is a 2-by-2
*>           diagonal block, WI(i) = sqrt(-H(i+1,i)*H(i,i+1)) and
*>           WI(i+1) = -WI(i).
*> \endverbatim
*>
*> \param[in,out] Z
*> \verbatim
*>          Z is REAL array, dimension (LDZ,N)
*>           If COMPZ = 'N', Z is not referenced.
*>           If COMPZ = 'I', on entry Z need not be set and on exit,
*>           if INFO = 0, Z contains the orthogonal matrix Z of the Schur
*>           vectors of H.  If COMPZ = 'V', on entry Z must contain an
*>           N-by-N matrix Q, which is assumed to be equal to the unit
*>           matrix except for the submatrix Z(ILO:IHI,ILO:IHI). On exit,
*>           if INFO = 0, Z contains Q*Z.
*>           Normally Q is the orthogonal matrix generated by SORGHR
*>           after the call to SGEHRD which formed the Hessenberg matrix
*>           H. (The output value of Z when INFO.GT.0 is given under
*>           the description of INFO below.)
*> \endverbatim
*>
*> \param[in] LDZ
*> \verbatim
*>          LDZ is INTEGER
*>           The leading dimension of the array Z.  if COMPZ = 'I' or
*>           COMPZ = 'V', then LDZ.GE.MAX(1,N).  Otherwize, LDZ.GE.1.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (LWORK)
*>           On exit, if INFO = 0, WORK(1) returns an estimate of
*>           the optimal value for LWORK.
*> \endverbatim
*>
*> \param[in] LWORK
*> \verbatim
*>          LWORK is INTEGER
*>           The dimension of the array WORK.  LWORK .GE. max(1,N)
*>           is sufficient and delivers very good and sometimes
*>           optimal performance.  However, LWORK as large as 11*N
*>           may be required for optimal performance.  A workspace
*>           query is recommended to determine the optimal workspace
*>           size.
*>
*>           If LWORK = -1, then SHSEQR does a workspace query.
*>           In this case, SHSEQR checks the input parameters and
*>           estimates the optimal workspace size for the given
*>           values of N, ILO and IHI.  The estimate is returned
*>           in WORK(1).  No error message related to LWORK is
*>           issued by XERBLA.  Neither H nor Z are accessed.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>             =  0:  successful exit
*>           .LT. 0:  if INFO = -i, the i-th argument had an illegal
*>                    value
*>           .GT. 0:  if INFO = i, SHSEQR failed to compute all of
*>                the eigenvalues.  Elements 1:ilo-1 and i+1:n of WR
*>                and WI contain those eigenvalues which have been
*>                successfully computed.  (Failures are rare.)
*>
*>                If INFO .GT. 0 and JOB = 'E', then on exit, the
*>                remaining unconverged eigenvalues are the eigen-
*>                values of the upper Hessenberg matrix rows and
*>                columns ILO through INFO of the final, output
*>                value of H.
*>
*>                If INFO .GT. 0 and JOB   = 'S', then on exit
*>
*>           (*)  (initial value of H)*U  = U*(final value of H)
*>
*>                where U is an orthogonal matrix.  The final
*>                value of H is upper Hessenberg and quasi-triangular
*>                in rows and columns INFO+1 through IHI.
*>
*>                If INFO .GT. 0 and COMPZ = 'V', then on exit
*>
*>                  (final value of Z)  =  (initial value of Z)*U
*>
*>                where U is the orthogonal matrix in (*) (regard-
*>                less of the value of JOB.)
*>
*>                If INFO .GT. 0 and COMPZ = 'I', then on exit
*>                      (final value of Z)  = U
*>                where U is the orthogonal matrix in (*) (regard-
*>                less of the value of JOB.)
*>
*>                If INFO .GT. 0 and COMPZ = 'N', then Z is not
*>                accessed.
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
*> \par Contributors:
*  ==================
*>
*>       Karen Braman and Ralph Byers, Department of Mathematics,
*>       University of Kansas, USA
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>             Default values supplied by
*>             ILAENV(ISPEC,'SHSEQR',JOB(:1)//COMPZ(:1),N,ILO,IHI,LWORK).
*>             It is suggested that these defaults be adjusted in order
*>             to attain best performance in each particular
*>             computational environment.
*>
*>            ISPEC=12: The SLAHQR vs SLAQR0 crossover point.
*>                      Default: 75. (Must be at least 11.)
*>
*>            ISPEC=13: Recommended deflation window size.
*>                      This depends on ILO, IHI and NS.  NS is the
*>                      number of simultaneous shifts returned
*>                      by ILAENV(ISPEC=15).  (See ISPEC=15 below.)
*>                      The default for (IHI-ILO+1).LE.500 is NS.
*>                      The default for (IHI-ILO+1).GT.500 is 3*NS/2.
*>
*>            ISPEC=14: Nibble crossover point. (See IPARMQ for
*>                      details.)  Default: 14% of deflation window
*>                      size.
*>
*>            ISPEC=15: Number of simultaneous shifts in a multishift
*>                      QR iteration.
*>
*>                      If IHI-ILO+1 is ...
*>
*>                      greater than      ...but less    ... the
*>                      or equal to ...      than        default is
*>
*>                           1               30          NS =   2(+)
*>                          30               60          NS =   4(+)
*>                          60              150          NS =  10(+)
*>                         150              590          NS =  **
*>                         590             3000          NS =  64
*>                        3000             6000          NS = 128
*>                        6000             infinity      NS = 256
*>
*>                  (+)  By default some or all matrices of this order
*>                       are passed to the implicit double shift routine
*>                       SLAHQR and this parameter is ignored.  See
*>                       ISPEC=12 above and comments in IPARMQ for
*>                       details.
*>
*>                 (**)  The asterisks (**) indicate an ad-hoc
*>                       function of N increasing from 10 to 64.
*>
*>            ISPEC=16: Select structured matrix multiply.
*>                      If the number of simultaneous shifts (specified
*>                      by ISPEC=15) is less than 14, then the default
*>                      for ISPEC=16 is 0.  Otherwise the default for
*>                      ISPEC=16 is 2.
*> \endverbatim
*
*> \par References:
*  ================
*>
*>       K. Braman, R. Byers and R. Mathias, The Multi-Shift QR
*>       Algorithm Part I: Maintaining Well Focused Shifts, and Level 3
*>       Performance, SIAM Journal of Matrix Analysis, volume 23, pages
*>       929--947, 2002.
*> \n
*>       K. Braman, R. Byers and R. Mathias, The Multi-Shift QR
*>       Algorithm Part II: Aggressive Early Deflation, SIAM Journal
*>       of Matrix Analysis, volume 23, pages 948--973, 2002.
*
*  =====================================================================
      SUBROUTINE SHSEQR( JOB, COMPZ, N, ILO, IHI, H, LDH, WR, WI, Z,
     $                   LDZ, WORK, LWORK, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            IHI, ILO, INFO, LDH, LDZ, LWORK, N
      CHARACTER          COMPZ, JOB
*     ..
*     .. Array Arguments ..
      REAL               H( LDH, * ), WI( * ), WORK( * ), WR( * ),
     $                   Z( LDZ, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
*
*     ==== Matrices of order NTINY or smaller must be processed by
*     .    SLAHQR because of insufficient subdiagonal scratch space.
*     .    (This is a hard limit.) ====
      INTEGER            NTINY
      PARAMETER          ( NTINY = 11 )
*
*     ==== NL allocates some local workspace to help small matrices
*     .    through a rare SLAHQR failure.  NL .GT. NTINY = 11 is
*     .    required and NL .LE. NMIN = ILAENV(ISPEC=12,...) is recom-
*     .    mended.  (The default value of NMIN is 75.)  Using NL = 49
*     .    allows up to six simultaneous shifts and a 16-by-16
*     .    deflation window.  ====
      INTEGER            NL
      PARAMETER          ( NL = 49 )
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0e0, ONE = 1.0e0 )
*     ..
*     .. Local Arrays ..
      REAL               HL( NL, NL ), WORKL( NL )
*     ..
*     .. Local Scalars ..
      INTEGER            I, KBOT, NMIN
      LOGICAL            INITZ, LQUERY, WANTT, WANTZ
*     ..
*     .. External Functions ..
      INTEGER            ILAENV
      LOGICAL            LSAME
      EXTERNAL           ILAENV, LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           SLACPY, SLAHQR, SLAQR0, SLASET, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX, MIN, REAL
*     ..
*     .. Executable Statements ..
*
*     ==== Decode and check the input parameters. ====
*
      WANTT = LSAME( JOB, 'S' )
      INITZ = LSAME( COMPZ, 'I' )
      WANTZ = INITZ .OR. LSAME( COMPZ, 'V' )
      WORK( 1 ) = REAL( MAX( 1, N ) )
      LQUERY = LWORK.EQ.-1
*
      INFO = 0
      IF( .NOT.LSAME( JOB, 'E' ) .AND. .NOT.WANTT ) THEN
         INFO = -1
      ELSE IF( .NOT.LSAME( COMPZ, 'N' ) .AND. .NOT.WANTZ ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( ILO.LT.1 .OR. ILO.GT.MAX( 1, N ) ) THEN
         INFO = -4
      ELSE IF( IHI.LT.MIN( ILO, N ) .OR. IHI.GT.N ) THEN
         INFO = -5
      ELSE IF( LDH.LT.MAX( 1, N ) ) THEN
         INFO = -7
      ELSE IF( LDZ.LT.1 .OR. ( WANTZ .AND. LDZ.LT.MAX( 1, N ) ) ) THEN
         INFO = -11
      ELSE IF( LWORK.LT.MAX( 1, N ) .AND. .NOT.LQUERY ) THEN
         INFO = -13
      END IF
*
      IF( INFO.NE.0 ) THEN
*
*        ==== Quick return in case of invalid argument. ====
*
         CALL XERBLA( 'SHSEQR', -INFO )
         RETURN
*
      ELSE IF( N.EQ.0 ) THEN
*
*        ==== Quick return in case N = 0; nothing to do. ====
*
         RETURN
*
      ELSE IF( LQUERY ) THEN
*
*        ==== Quick return in case of a workspace query ====
*
         CALL SLAQR0( WANTT, WANTZ, N, ILO, IHI, H, LDH, WR, WI, ILO,
     $                IHI, Z, LDZ, WORK, LWORK, INFO )
*        ==== Ensure reported workspace size is backward-compatible with
*        .    previous LAPACK versions. ====
         WORK( 1 ) = MAX( REAL( MAX( 1, N ) ), WORK( 1 ) )
         RETURN
*
      ELSE
*
*        ==== copy eigenvalues isolated by SGEBAL ====
*
         DO 10 I = 1, ILO - 1
            WR( I ) = H( I, I )
            WI( I ) = ZERO
   10    CONTINUE
         DO 20 I = IHI + 1, N
            WR( I ) = H( I, I )
            WI( I ) = ZERO
   20    CONTINUE
*
*        ==== Initialize Z, if requested ====
*
         IF( INITZ )
     $      CALL SLASET( 'A', N, N, ZERO, ONE, Z, LDZ )
*
*        ==== Quick return if possible ====
*
         IF( ILO.EQ.IHI ) THEN
            WR( ILO ) = H( ILO, ILO )
            WI( ILO ) = ZERO
            RETURN
         END IF
*
*        ==== SLAHQR/SLAQR0 crossover point ====
*
         NMIN = ILAENV( 12, 'SHSEQR', JOB( : 1 ) // COMPZ( : 1 ), N,
     $          ILO, IHI, LWORK )
         NMIN = MAX( NTINY, NMIN )
*
*        ==== SLAQR0 for big matrices; SLAHQR for small ones ====
*
         IF( N.GT.NMIN ) THEN
            CALL SLAQR0( WANTT, WANTZ, N, ILO, IHI, H, LDH, WR, WI, ILO,
     $                   IHI, Z, LDZ, WORK, LWORK, INFO )
         ELSE
*
*           ==== Small matrix ====
*
            CALL SLAHQR( WANTT, WANTZ, N, ILO, IHI, H, LDH, WR, WI, ILO,
     $                   IHI, Z, LDZ, INFO )
*
            IF( INFO.GT.0 ) THEN
*
*              ==== A rare SLAHQR failure!  SLAQR0 sometimes succeeds
*              .    when SLAHQR fails. ====
*
               KBOT = INFO
*
               IF( N.GE.NL ) THEN
*
*                 ==== Larger matrices have enough subdiagonal scratch
*                 .    space to call SLAQR0 directly. ====
*
                  CALL SLAQR0( WANTT, WANTZ, N, ILO, KBOT, H, LDH, WR,
     $                         WI, ILO, IHI, Z, LDZ, WORK, LWORK, INFO )
*
               ELSE
*
*                 ==== Tiny matrices don't have enough subdiagonal
*                 .    scratch space to benefit from SLAQR0.  Hence,
*                 .    tiny matrices must be copied into a larger
*                 .    array before calling SLAQR0. ====
*
                  CALL SLACPY( 'A', N, N, H, LDH, HL, NL )
                  HL( N+1, N ) = ZERO
                  CALL SLASET( 'A', NL, NL-N, ZERO, ZERO, HL( 1, N+1 ),
     $                         NL )
                  CALL SLAQR0( WANTT, WANTZ, NL, ILO, KBOT, HL, NL, WR,
     $                         WI, ILO, IHI, Z, LDZ, WORKL, NL, INFO )
                  IF( WANTT .OR. INFO.NE.0 )
     $               CALL SLACPY( 'A', N, N, HL, NL, H, LDH )
               END IF
            END IF
         END IF
*
*        ==== Clear out the trash, if necessary. ====
*
         IF( ( WANTT .OR. INFO.NE.0 ) .AND. N.GT.2 )
     $      CALL SLASET( 'L', N-2, N-2, ZERO, ZERO, H( 3, 1 ), LDH )
*
*        ==== Ensure reported workspace size is backward-compatible with
*        .    previous LAPACK versions. ====
*
         WORK( 1 ) = MAX( REAL( MAX( 1, N ) ), WORK( 1 ) )
      END IF
*
*     ==== End of SHSEQR ====
*
      END
