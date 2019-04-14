*> \brief \b CLAROR
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*  Definition:
*  ===========
*
*       SUBROUTINE CLAROR( SIDE, INIT, M, N, A, LDA, ISEED, X, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          INIT, SIDE
*       INTEGER            INFO, LDA, M, N
*       ..
*       .. Array Arguments ..
*       INTEGER            ISEED( 4 )
*       COMPLEX            A( LDA, * ), X( * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*>    CLAROR pre- or post-multiplies an M by N matrix A by a random
*>    unitary matrix U, overwriting A. A may optionally be
*>    initialized to the identity matrix before multiplying by U.
*>    U is generated using the method of G.W. Stewart
*>    ( SIAM J. Numer. Anal. 17, 1980, pp. 403-409 ).
*>    (BLAS-2 version)
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] SIDE
*> \verbatim
*>          SIDE is CHARACTER*1
*>           SIDE specifies whether A is multiplied on the left or right
*>           by U.
*>       SIDE = 'L'   Multiply A on the left (premultiply) by U
*>       SIDE = 'R'   Multiply A on the right (postmultiply) by UC>       SIDE = 'C'   Multiply A on the left by U and the right by UC>       SIDE = 'T'   Multiply A on the left by U and the right by U'
*>           Not modified.
*> \endverbatim
*>
*> \param[in] INIT
*> \verbatim
*>          INIT is CHARACTER*1
*>           INIT specifies whether or not A should be initialized to
*>           the identity matrix.
*>              INIT = 'I'   Initialize A to (a section of) the
*>                           identity matrix before applying U.
*>              INIT = 'N'   No initialization.  Apply U to the
*>                           input matrix A.
*>
*>           INIT = 'I' may be used to generate square (i.e., unitary)
*>           or rectangular orthogonal matrices (orthogonality being
*>           in the sense of CDOTC):
*>
*>           For square matrices, M=N, and SIDE many be either 'L' or
*>           'R'; the rows will be orthogonal to each other, as will the
*>           columns.
*>           For rectangular matrices where M < N, SIDE = 'R' will
*>           produce a dense matrix whose rows will be orthogonal and
*>           whose columns will not, while SIDE = 'L' will produce a
*>           matrix whose rows will be orthogonal, and whose first M
*>           columns will be orthogonal, the remaining columns being
*>           zero.
*>           For matrices where M > N, just use the previous
*>           explanation, interchanging 'L' and 'R' and "rows" and
*>           "columns".
*>
*>           Not modified.
*> \endverbatim
*>
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>           Number of rows of A. Not modified.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>           Number of columns of A. Not modified.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX array, dimension ( LDA, N )
*>           Input and output array. Overwritten by U A ( if SIDE = 'L' )
*>           or by A U ( if SIDE = 'R' )
*>           or by U A U* ( if SIDE = 'C')
*>           or by U A U' ( if SIDE = 'T') on exit.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>           Leading dimension of A. Must be at least MAX ( 1, M ).
*>           Not modified.
*> \endverbatim
*>
*> \param[in,out] ISEED
*> \verbatim
*>          ISEED is INTEGER array, dimension ( 4 )
*>           On entry ISEED specifies the seed of the random number
*>           generator. The array elements should be between 0 and 4095;
*>           if not they will be reduced mod 4096.  Also, ISEED(4) must
*>           be odd.  The random number generator uses a linear
*>           congruential sequence limited to small integers, and so
*>           should produce machine independent random numbers. The
*>           values of ISEED are changed on exit, and can be used in the
*>           next call to CLAROR to continue the same random number
*>           sequence.
*>           Modified.
*> \endverbatim
*>
*> \param[out] X
*> \verbatim
*>          X is COMPLEX array, dimension ( 3*MAX( M, N ) )
*>           Workspace. Of length:
*>               2*M + N if SIDE = 'L',
*>               2*N + M if SIDE = 'R',
*>               3*N     if SIDE = 'C' or 'T'.
*>           Modified.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>           An error flag.  It is set to:
*>            0  if no error.
*>            1  if CLARND returned a bad random number (installation
*>               problem)
*>           -1  if SIDE is not L, R, C, or T.
*>           -3  if M is negative.
*>           -4  if N is negative or if SIDE is C or T and N is not equal
*>               to M.
*>           -6  if LDA is less than M.
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
*> \ingroup complex_matgen
*
*  =====================================================================
      SUBROUTINE CLAROR( SIDE, INIT, M, N, A, LDA, ISEED, X, INFO )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          INIT, SIDE
      INTEGER            INFO, LDA, M, N
*     ..
*     .. Array Arguments ..
      INTEGER            ISEED( 4 )
      COMPLEX            A( LDA, * ), X( * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE, TOOSML
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0,
     $                   TOOSML = 1.0E-20 )
      COMPLEX            CZERO, CONE
      PARAMETER          ( CZERO = ( 0.0E+0, 0.0E+0 ),
     $                   CONE = ( 1.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      INTEGER            IROW, ITYPE, IXFRM, J, JCOL, KBEG, NXFRM
      REAL               FACTOR, XABS, XNORM
      COMPLEX            CSIGN, XNORMS
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      REAL               SCNRM2
      COMPLEX            CLARND
      EXTERNAL           LSAME, SCNRM2, CLARND
*     ..
*     .. External Subroutines ..
      EXTERNAL           CGEMV, CGERC, CLACGV, CLASET, CSCAL, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, CMPLX, CONJG
*     ..
*     .. Executable Statements ..
*
      INFO = 0
      IF( N.EQ.0 .OR. M.EQ.0 )
     $   RETURN
*
      ITYPE = 0
      IF( LSAME( SIDE, 'L' ) ) THEN
         ITYPE = 1
      ELSE IF( LSAME( SIDE, 'R' ) ) THEN
         ITYPE = 2
      ELSE IF( LSAME( SIDE, 'C' ) ) THEN
         ITYPE = 3
      ELSE IF( LSAME( SIDE, 'T' ) ) THEN
         ITYPE = 4
      END IF
*
*     Check for argument errors.
*
      IF( ITYPE.EQ.0 ) THEN
         INFO = -1
      ELSE IF( M.LT.0 ) THEN
         INFO = -3
      ELSE IF( N.LT.0 .OR. ( ITYPE.EQ.3 .AND. N.NE.M ) ) THEN
         INFO = -4
      ELSE IF( LDA.LT.M ) THEN
         INFO = -6
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CLAROR', -INFO )
         RETURN
      END IF
*
      IF( ITYPE.EQ.1 ) THEN
         NXFRM = M
      ELSE
         NXFRM = N
      END IF
*
*     Initialize A to the identity matrix if desired
*
      IF( LSAME( INIT, 'I' ) )
     $   CALL CLASET( 'Full', M, N, CZERO, CONE, A, LDA )
*
*     If no rotation possible, still multiply by
*     a random complex number from the circle |x| = 1
*
*      2)      Compute Rotation by computing Householder
*              Transformations H(2), H(3), ..., H(n).  Note that the
*              order in which they are computed is irrelevant.
*
      DO 40 J = 1, NXFRM
         X( J ) = CZERO
   40 CONTINUE
*
      DO 60 IXFRM = 2, NXFRM
         KBEG = NXFRM - IXFRM + 1
*
*        Generate independent normal( 0, 1 ) random numbers
*
         DO 50 J = KBEG, NXFRM
            X( J ) = CLARND( 3, ISEED )
   50    CONTINUE
*
*        Generate a Householder transformation from the random vector X
*
         XNORM = SCNRM2( IXFRM, X( KBEG ), 1 )
         XABS = ABS( X( KBEG ) )
         IF( XABS.NE.CZERO ) THEN
            CSIGN = X( KBEG ) / XABS
         ELSE
            CSIGN = CONE
         END IF
         XNORMS = CSIGN*XNORM
         X( NXFRM+KBEG ) = -CSIGN
         FACTOR = XNORM*( XNORM+XABS )
         IF( ABS( FACTOR ).LT.TOOSML ) THEN
            INFO = 1
            CALL XERBLA( 'CLAROR', -INFO )
            RETURN
         ELSE
            FACTOR = ONE / FACTOR
         END IF
         X( KBEG ) = X( KBEG ) + XNORMS
*
*        Apply Householder transformation to A
*
         IF( ITYPE.EQ.1 .OR. ITYPE.EQ.3 .OR. ITYPE.EQ.4 ) THEN
*
*           Apply H(k) on the left of A
*
            CALL CGEMV( 'C', IXFRM, N, CONE, A( KBEG, 1 ), LDA,
     $                  X( KBEG ), 1, CZERO, X( 2*NXFRM+1 ), 1 )
            CALL CGERC( IXFRM, N, -CMPLX( FACTOR ), X( KBEG ), 1,
     $                  X( 2*NXFRM+1 ), 1, A( KBEG, 1 ), LDA )
*
         END IF
*
         IF( ITYPE.GE.2 .AND. ITYPE.LE.4 ) THEN
*
*           Apply H(k)* (or H(k)') on the right of A
*
            IF( ITYPE.EQ.4 ) THEN
               CALL CLACGV( IXFRM, X( KBEG ), 1 )
            END IF
*
            CALL CGEMV( 'N', M, IXFRM, CONE, A( 1, KBEG ), LDA,
     $                  X( KBEG ), 1, CZERO, X( 2*NXFRM+1 ), 1 )
            CALL CGERC( M, IXFRM, -CMPLX( FACTOR ), X( 2*NXFRM+1 ), 1,
     $                  X( KBEG ), 1, A( 1, KBEG ), LDA )
*
         END IF
   60 CONTINUE
*
      X( 1 ) = CLARND( 3, ISEED )
      XABS = ABS( X( 1 ) )
      IF( XABS.NE.ZERO ) THEN
         CSIGN = X( 1 ) / XABS
      ELSE
         CSIGN = CONE
      END IF
      X( 2*NXFRM ) = CSIGN
*
*     Scale the matrix A by D.
*
      IF( ITYPE.EQ.1 .OR. ITYPE.EQ.3 .OR. ITYPE.EQ.4 ) THEN
         DO 70 IROW = 1, M
            CALL CSCAL( N, CONJG( X( NXFRM+IROW ) ), A( IROW, 1 ), LDA )
   70    CONTINUE
      END IF
*
      IF( ITYPE.EQ.2 .OR. ITYPE.EQ.3 ) THEN
         DO 80 JCOL = 1, N
            CALL CSCAL( M, X( NXFRM+JCOL ), A( 1, JCOL ), 1 )
   80    CONTINUE
      END IF
*
      IF( ITYPE.EQ.4 ) THEN
         DO 90 JCOL = 1, N
            CALL CSCAL( M, CONJG( X( NXFRM+JCOL ) ), A( 1, JCOL ), 1 )
   90    CONTINUE
      END IF
      RETURN
*
*     End of CLAROR
*
      END
