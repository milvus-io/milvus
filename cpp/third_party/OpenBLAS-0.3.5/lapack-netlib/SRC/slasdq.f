*> \brief \b SLASDQ computes the SVD of a real bidiagonal matrix with diagonal d and off-diagonal e. Used by sbdsdc.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLASDQ + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slasdq.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slasdq.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slasdq.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLASDQ( UPLO, SQRE, N, NCVT, NRU, NCC, D, E, VT, LDVT,
*                          U, LDU, C, LDC, WORK, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          UPLO
*       INTEGER            INFO, LDC, LDU, LDVT, N, NCC, NCVT, NRU, SQRE
*       ..
*       .. Array Arguments ..
*       REAL               C( LDC, * ), D( * ), E( * ), U( LDU, * ),
*      $                   VT( LDVT, * ), WORK( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLASDQ computes the singular value decomposition (SVD) of a real
*> (upper or lower) bidiagonal matrix with diagonal D and offdiagonal
*> E, accumulating the transformations if desired. Letting B denote
*> the input bidiagonal matrix, the algorithm computes orthogonal
*> matrices Q and P such that B = Q * S * P**T (P**T denotes the transpose
*> of P). The singular values S are overwritten on D.
*>
*> The input matrix U  is changed to U  * Q  if desired.
*> The input matrix VT is changed to P**T * VT if desired.
*> The input matrix C  is changed to Q**T * C  if desired.
*>
*> See "Computing  Small Singular Values of Bidiagonal Matrices With
*> Guaranteed High Relative Accuracy," by J. Demmel and W. Kahan,
*> LAPACK Working Note #3, for a detailed description of the algorithm.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPLO
*> \verbatim
*>          UPLO is CHARACTER*1
*>        On entry, UPLO specifies whether the input bidiagonal matrix
*>        is upper or lower bidiagonal, and whether it is square are
*>        not.
*>           UPLO = 'U' or 'u'   B is upper bidiagonal.
*>           UPLO = 'L' or 'l'   B is lower bidiagonal.
*> \endverbatim
*>
*> \param[in] SQRE
*> \verbatim
*>          SQRE is INTEGER
*>        = 0: then the input matrix is N-by-N.
*>        = 1: then the input matrix is N-by-(N+1) if UPLU = 'U' and
*>             (N+1)-by-N if UPLU = 'L'.
*>
*>        The bidiagonal matrix has
*>        N = NL + NR + 1 rows and
*>        M = N + SQRE >= N columns.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>        On entry, N specifies the number of rows and columns
*>        in the matrix. N must be at least 0.
*> \endverbatim
*>
*> \param[in] NCVT
*> \verbatim
*>          NCVT is INTEGER
*>        On entry, NCVT specifies the number of columns of
*>        the matrix VT. NCVT must be at least 0.
*> \endverbatim
*>
*> \param[in] NRU
*> \verbatim
*>          NRU is INTEGER
*>        On entry, NRU specifies the number of rows of
*>        the matrix U. NRU must be at least 0.
*> \endverbatim
*>
*> \param[in] NCC
*> \verbatim
*>          NCC is INTEGER
*>        On entry, NCC specifies the number of columns of
*>        the matrix C. NCC must be at least 0.
*> \endverbatim
*>
*> \param[in,out] D
*> \verbatim
*>          D is REAL array, dimension (N)
*>        On entry, D contains the diagonal entries of the
*>        bidiagonal matrix whose SVD is desired. On normal exit,
*>        D contains the singular values in ascending order.
*> \endverbatim
*>
*> \param[in,out] E
*> \verbatim
*>          E is REAL array.
*>        dimension is (N-1) if SQRE = 0 and N if SQRE = 1.
*>        On entry, the entries of E contain the offdiagonal entries
*>        of the bidiagonal matrix whose SVD is desired. On normal
*>        exit, E will contain 0. If the algorithm does not converge,
*>        D and E will contain the diagonal and superdiagonal entries
*>        of a bidiagonal matrix orthogonally equivalent to the one
*>        given as input.
*> \endverbatim
*>
*> \param[in,out] VT
*> \verbatim
*>          VT is REAL array, dimension (LDVT, NCVT)
*>        On entry, contains a matrix which on exit has been
*>        premultiplied by P**T, dimension N-by-NCVT if SQRE = 0
*>        and (N+1)-by-NCVT if SQRE = 1 (not referenced if NCVT=0).
*> \endverbatim
*>
*> \param[in] LDVT
*> \verbatim
*>          LDVT is INTEGER
*>        On entry, LDVT specifies the leading dimension of VT as
*>        declared in the calling (sub) program. LDVT must be at
*>        least 1. If NCVT is nonzero LDVT must also be at least N.
*> \endverbatim
*>
*> \param[in,out] U
*> \verbatim
*>          U is REAL array, dimension (LDU, N)
*>        On entry, contains a  matrix which on exit has been
*>        postmultiplied by Q, dimension NRU-by-N if SQRE = 0
*>        and NRU-by-(N+1) if SQRE = 1 (not referenced if NRU=0).
*> \endverbatim
*>
*> \param[in] LDU
*> \verbatim
*>          LDU is INTEGER
*>        On entry, LDU  specifies the leading dimension of U as
*>        declared in the calling (sub) program. LDU must be at
*>        least max( 1, NRU ) .
*> \endverbatim
*>
*> \param[in,out] C
*> \verbatim
*>          C is REAL array, dimension (LDC, NCC)
*>        On entry, contains an N-by-NCC matrix which on exit
*>        has been premultiplied by Q**T  dimension N-by-NCC if SQRE = 0
*>        and (N+1)-by-NCC if SQRE = 1 (not referenced if NCC=0).
*> \endverbatim
*>
*> \param[in] LDC
*> \verbatim
*>          LDC is INTEGER
*>        On entry, LDC  specifies the leading dimension of C as
*>        declared in the calling (sub) program. LDC must be at
*>        least 1. If NCC is nonzero, LDC must also be at least N.
*> \endverbatim
*>
*> \param[out] WORK
*> \verbatim
*>          WORK is REAL array, dimension (4*N)
*>        Workspace. Only referenced if one of NCVT, NRU, or NCC is
*>        nonzero, and if N is at least 2.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>        On exit, a value of 0 indicates a successful exit.
*>        If INFO < 0, argument number -INFO is illegal.
*>        If INFO > 0, the algorithm did not converge, and INFO
*>        specifies how many superdiagonals did not converge.
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
*> \ingroup OTHERauxiliary
*
*> \par Contributors:
*  ==================
*>
*>     Ming Gu and Huan Ren, Computer Science Division, University of
*>     California at Berkeley, USA
*>
*  =====================================================================
      SUBROUTINE SLASDQ( UPLO, SQRE, N, NCVT, NRU, NCC, D, E, VT, LDVT,
     $                   U, LDU, C, LDC, WORK, INFO )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      CHARACTER          UPLO
      INTEGER            INFO, LDC, LDU, LDVT, N, NCC, NCVT, NRU, SQRE
*     ..
*     .. Array Arguments ..
      REAL               C( LDC, * ), D( * ), E( * ), U( LDU, * ),
     $                   VT( LDVT, * ), WORK( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO
      PARAMETER          ( ZERO = 0.0E+0 )
*     ..
*     .. Local Scalars ..
      LOGICAL            ROTATE
      INTEGER            I, ISUB, IUPLO, J, NP1, SQRE1
      REAL               CS, R, SMIN, SN
*     ..
*     .. External Subroutines ..
      EXTERNAL           SBDSQR, SLARTG, SLASR, SSWAP, XERBLA
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MAX
*     ..
*     .. Executable Statements ..
*
*     Test the input parameters.
*
      INFO = 0
      IUPLO = 0
      IF( LSAME( UPLO, 'U' ) )
     $   IUPLO = 1
      IF( LSAME( UPLO, 'L' ) )
     $   IUPLO = 2
      IF( IUPLO.EQ.0 ) THEN
         INFO = -1
      ELSE IF( ( SQRE.LT.0 ) .OR. ( SQRE.GT.1 ) ) THEN
         INFO = -2
      ELSE IF( N.LT.0 ) THEN
         INFO = -3
      ELSE IF( NCVT.LT.0 ) THEN
         INFO = -4
      ELSE IF( NRU.LT.0 ) THEN
         INFO = -5
      ELSE IF( NCC.LT.0 ) THEN
         INFO = -6
      ELSE IF( ( NCVT.EQ.0 .AND. LDVT.LT.1 ) .OR.
     $         ( NCVT.GT.0 .AND. LDVT.LT.MAX( 1, N ) ) ) THEN
         INFO = -10
      ELSE IF( LDU.LT.MAX( 1, NRU ) ) THEN
         INFO = -12
      ELSE IF( ( NCC.EQ.0 .AND. LDC.LT.1 ) .OR.
     $         ( NCC.GT.0 .AND. LDC.LT.MAX( 1, N ) ) ) THEN
         INFO = -14
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SLASDQ', -INFO )
         RETURN
      END IF
      IF( N.EQ.0 )
     $   RETURN
*
*     ROTATE is true if any singular vectors desired, false otherwise
*
      ROTATE = ( NCVT.GT.0 ) .OR. ( NRU.GT.0 ) .OR. ( NCC.GT.0 )
      NP1 = N + 1
      SQRE1 = SQRE
*
*     If matrix non-square upper bidiagonal, rotate to be lower
*     bidiagonal.  The rotations are on the right.
*
      IF( ( IUPLO.EQ.1 ) .AND. ( SQRE1.EQ.1 ) ) THEN
         DO 10 I = 1, N - 1
            CALL SLARTG( D( I ), E( I ), CS, SN, R )
            D( I ) = R
            E( I ) = SN*D( I+1 )
            D( I+1 ) = CS*D( I+1 )
            IF( ROTATE ) THEN
               WORK( I ) = CS
               WORK( N+I ) = SN
            END IF
   10    CONTINUE
         CALL SLARTG( D( N ), E( N ), CS, SN, R )
         D( N ) = R
         E( N ) = ZERO
         IF( ROTATE ) THEN
            WORK( N ) = CS
            WORK( N+N ) = SN
         END IF
         IUPLO = 2
         SQRE1 = 0
*
*        Update singular vectors if desired.
*
         IF( NCVT.GT.0 )
     $      CALL SLASR( 'L', 'V', 'F', NP1, NCVT, WORK( 1 ),
     $                  WORK( NP1 ), VT, LDVT )
      END IF
*
*     If matrix lower bidiagonal, rotate to be upper bidiagonal
*     by applying Givens rotations on the left.
*
      IF( IUPLO.EQ.2 ) THEN
         DO 20 I = 1, N - 1
            CALL SLARTG( D( I ), E( I ), CS, SN, R )
            D( I ) = R
            E( I ) = SN*D( I+1 )
            D( I+1 ) = CS*D( I+1 )
            IF( ROTATE ) THEN
               WORK( I ) = CS
               WORK( N+I ) = SN
            END IF
   20    CONTINUE
*
*        If matrix (N+1)-by-N lower bidiagonal, one additional
*        rotation is needed.
*
         IF( SQRE1.EQ.1 ) THEN
            CALL SLARTG( D( N ), E( N ), CS, SN, R )
            D( N ) = R
            IF( ROTATE ) THEN
               WORK( N ) = CS
               WORK( N+N ) = SN
            END IF
         END IF
*
*        Update singular vectors if desired.
*
         IF( NRU.GT.0 ) THEN
            IF( SQRE1.EQ.0 ) THEN
               CALL SLASR( 'R', 'V', 'F', NRU, N, WORK( 1 ),
     $                     WORK( NP1 ), U, LDU )
            ELSE
               CALL SLASR( 'R', 'V', 'F', NRU, NP1, WORK( 1 ),
     $                     WORK( NP1 ), U, LDU )
            END IF
         END IF
         IF( NCC.GT.0 ) THEN
            IF( SQRE1.EQ.0 ) THEN
               CALL SLASR( 'L', 'V', 'F', N, NCC, WORK( 1 ),
     $                     WORK( NP1 ), C, LDC )
            ELSE
               CALL SLASR( 'L', 'V', 'F', NP1, NCC, WORK( 1 ),
     $                     WORK( NP1 ), C, LDC )
            END IF
         END IF
      END IF
*
*     Call SBDSQR to compute the SVD of the reduced real
*     N-by-N upper bidiagonal matrix.
*
      CALL SBDSQR( 'U', N, NCVT, NRU, NCC, D, E, VT, LDVT, U, LDU, C,
     $             LDC, WORK, INFO )
*
*     Sort the singular values into ascending order (insertion sort on
*     singular values, but only one transposition per singular vector)
*
      DO 40 I = 1, N
*
*        Scan for smallest D(I).
*
         ISUB = I
         SMIN = D( I )
         DO 30 J = I + 1, N
            IF( D( J ).LT.SMIN ) THEN
               ISUB = J
               SMIN = D( J )
            END IF
   30    CONTINUE
         IF( ISUB.NE.I ) THEN
*
*           Swap singular values and vectors.
*
            D( ISUB ) = D( I )
            D( I ) = SMIN
            IF( NCVT.GT.0 )
     $         CALL SSWAP( NCVT, VT( ISUB, 1 ), LDVT, VT( I, 1 ), LDVT )
            IF( NRU.GT.0 )
     $         CALL SSWAP( NRU, U( 1, ISUB ), 1, U( 1, I ), 1 )
            IF( NCC.GT.0 )
     $         CALL SSWAP( NCC, C( ISUB, 1 ), LDC, C( I, 1 ), LDC )
         END IF
   40 CONTINUE
*
      RETURN
*
*     End of SLASDQ
*
      END
