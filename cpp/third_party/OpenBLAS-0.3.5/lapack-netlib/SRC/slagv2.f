*> \brief \b SLAGV2 computes the Generalized Schur factorization of a real 2-by-2 matrix pencil (A,B) where B is upper triangular.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLAGV2 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slagv2.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slagv2.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slagv2.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLAGV2( A, LDA, B, LDB, ALPHAR, ALPHAI, BETA, CSL, SNL,
*                          CSR, SNR )
*
*       .. Scalar Arguments ..
*       INTEGER            LDA, LDB
*       REAL               CSL, CSR, SNL, SNR
*       ..
*       .. Array Arguments ..
*       REAL               A( LDA, * ), ALPHAI( 2 ), ALPHAR( 2 ),
*      $                   B( LDB, * ), BETA( 2 )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLAGV2 computes the Generalized Schur factorization of a real 2-by-2
*> matrix pencil (A,B) where B is upper triangular. This routine
*> computes orthogonal (rotation) matrices given by CSL, SNL and CSR,
*> SNR such that
*>
*> 1) if the pencil (A,B) has two real eigenvalues (include 0/0 or 1/0
*>    types), then
*>
*>    [ a11 a12 ] := [  CSL  SNL ] [ a11 a12 ] [  CSR -SNR ]
*>    [  0  a22 ]    [ -SNL  CSL ] [ a21 a22 ] [  SNR  CSR ]
*>
*>    [ b11 b12 ] := [  CSL  SNL ] [ b11 b12 ] [  CSR -SNR ]
*>    [  0  b22 ]    [ -SNL  CSL ] [  0  b22 ] [  SNR  CSR ],
*>
*> 2) if the pencil (A,B) has a pair of complex conjugate eigenvalues,
*>    then
*>
*>    [ a11 a12 ] := [  CSL  SNL ] [ a11 a12 ] [  CSR -SNR ]
*>    [ a21 a22 ]    [ -SNL  CSL ] [ a21 a22 ] [  SNR  CSR ]
*>
*>    [ b11  0  ] := [  CSL  SNL ] [ b11 b12 ] [  CSR -SNR ]
*>    [  0  b22 ]    [ -SNL  CSL ] [  0  b22 ] [  SNR  CSR ]
*>
*>    where b11 >= b22 > 0.
*>
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in,out] A
*> \verbatim
*>          A is REAL array, dimension (LDA, 2)
*>          On entry, the 2 x 2 matrix A.
*>          On exit, A is overwritten by the ``A-part'' of the
*>          generalized Schur form.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          THe leading dimension of the array A.  LDA >= 2.
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is REAL array, dimension (LDB, 2)
*>          On entry, the upper triangular 2 x 2 matrix B.
*>          On exit, B is overwritten by the ``B-part'' of the
*>          generalized Schur form.
*> \endverbatim
*>
*> \param[in] LDB
*> \verbatim
*>          LDB is INTEGER
*>          THe leading dimension of the array B.  LDB >= 2.
*> \endverbatim
*>
*> \param[out] ALPHAR
*> \verbatim
*>          ALPHAR is REAL array, dimension (2)
*> \endverbatim
*>
*> \param[out] ALPHAI
*> \verbatim
*>          ALPHAI is REAL array, dimension (2)
*> \endverbatim
*>
*> \param[out] BETA
*> \verbatim
*>          BETA is REAL array, dimension (2)
*>          (ALPHAR(k)+i*ALPHAI(k))/BETA(k) are the eigenvalues of the
*>          pencil (A,B), k=1,2, i = sqrt(-1).  Note that BETA(k) may
*>          be zero.
*> \endverbatim
*>
*> \param[out] CSL
*> \verbatim
*>          CSL is REAL
*>          The cosine of the left rotation matrix.
*> \endverbatim
*>
*> \param[out] SNL
*> \verbatim
*>          SNL is REAL
*>          The sine of the left rotation matrix.
*> \endverbatim
*>
*> \param[out] CSR
*> \verbatim
*>          CSR is REAL
*>          The cosine of the right rotation matrix.
*> \endverbatim
*>
*> \param[out] SNR
*> \verbatim
*>          SNR is REAL
*>          The sine of the right rotation matrix.
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
*> \ingroup realOTHERauxiliary
*
*> \par Contributors:
*  ==================
*>
*>     Mark Fahey, Department of Mathematics, Univ. of Kentucky, USA
*
*  =====================================================================
      SUBROUTINE SLAGV2( A, LDA, B, LDB, ALPHAR, ALPHAI, BETA, CSL, SNL,
     $                   CSR, SNR )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      INTEGER            LDA, LDB
      REAL               CSL, CSR, SNL, SNR
*     ..
*     .. Array Arguments ..
      REAL               A( LDA, * ), ALPHAI( 2 ), ALPHAR( 2 ),
     $                   B( LDB, * ), BETA( 2 )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, ONE
      PARAMETER          ( ZERO = 0.0E+0, ONE = 1.0E+0 )
*     ..
*     .. Local Scalars ..
      REAL               ANORM, ASCALE, BNORM, BSCALE, H1, H2, H3, QQ,
     $                   R, RR, SAFMIN, SCALE1, SCALE2, T, ULP, WI, WR1,
     $                   WR2
*     ..
*     .. External Subroutines ..
      EXTERNAL           SLAG2, SLARTG, SLASV2, SROT
*     ..
*     .. External Functions ..
      REAL               SLAMCH, SLAPY2
      EXTERNAL           SLAMCH, SLAPY2
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX
*     ..
*     .. Executable Statements ..
*
      SAFMIN = SLAMCH( 'S' )
      ULP = SLAMCH( 'P' )
*
*     Scale A
*
      ANORM = MAX( ABS( A( 1, 1 ) )+ABS( A( 2, 1 ) ),
     $        ABS( A( 1, 2 ) )+ABS( A( 2, 2 ) ), SAFMIN )
      ASCALE = ONE / ANORM
      A( 1, 1 ) = ASCALE*A( 1, 1 )
      A( 1, 2 ) = ASCALE*A( 1, 2 )
      A( 2, 1 ) = ASCALE*A( 2, 1 )
      A( 2, 2 ) = ASCALE*A( 2, 2 )
*
*     Scale B
*
      BNORM = MAX( ABS( B( 1, 1 ) ), ABS( B( 1, 2 ) )+ABS( B( 2, 2 ) ),
     $        SAFMIN )
      BSCALE = ONE / BNORM
      B( 1, 1 ) = BSCALE*B( 1, 1 )
      B( 1, 2 ) = BSCALE*B( 1, 2 )
      B( 2, 2 ) = BSCALE*B( 2, 2 )
*
*     Check if A can be deflated
*
      IF( ABS( A( 2, 1 ) ).LE.ULP ) THEN
         CSL = ONE
         SNL = ZERO
         CSR = ONE
         SNR = ZERO
         A( 2, 1 ) = ZERO
         B( 2, 1 ) = ZERO
         WI = ZERO
*
*     Check if B is singular
*
      ELSE IF( ABS( B( 1, 1 ) ).LE.ULP ) THEN
         CALL SLARTG( A( 1, 1 ), A( 2, 1 ), CSL, SNL, R )
         CSR = ONE
         SNR = ZERO
         CALL SROT( 2, A( 1, 1 ), LDA, A( 2, 1 ), LDA, CSL, SNL )
         CALL SROT( 2, B( 1, 1 ), LDB, B( 2, 1 ), LDB, CSL, SNL )
         A( 2, 1 ) = ZERO
         B( 1, 1 ) = ZERO
         B( 2, 1 ) = ZERO
         WI = ZERO
*
      ELSE IF( ABS( B( 2, 2 ) ).LE.ULP ) THEN
         CALL SLARTG( A( 2, 2 ), A( 2, 1 ), CSR, SNR, T )
         SNR = -SNR
         CALL SROT( 2, A( 1, 1 ), 1, A( 1, 2 ), 1, CSR, SNR )
         CALL SROT( 2, B( 1, 1 ), 1, B( 1, 2 ), 1, CSR, SNR )
         CSL = ONE
         SNL = ZERO
         A( 2, 1 ) = ZERO
         B( 2, 1 ) = ZERO
         B( 2, 2 ) = ZERO
         WI = ZERO
*
      ELSE
*
*        B is nonsingular, first compute the eigenvalues of (A,B)
*
         CALL SLAG2( A, LDA, B, LDB, SAFMIN, SCALE1, SCALE2, WR1, WR2,
     $               WI )
*
         IF( WI.EQ.ZERO ) THEN
*
*           two real eigenvalues, compute s*A-w*B
*
            H1 = SCALE1*A( 1, 1 ) - WR1*B( 1, 1 )
            H2 = SCALE1*A( 1, 2 ) - WR1*B( 1, 2 )
            H3 = SCALE1*A( 2, 2 ) - WR1*B( 2, 2 )
*
            RR = SLAPY2( H1, H2 )
            QQ = SLAPY2( SCALE1*A( 2, 1 ), H3 )
*
            IF( RR.GT.QQ ) THEN
*
*              find right rotation matrix to zero 1,1 element of
*              (sA - wB)
*
               CALL SLARTG( H2, H1, CSR, SNR, T )
*
            ELSE
*
*              find right rotation matrix to zero 2,1 element of
*              (sA - wB)
*
               CALL SLARTG( H3, SCALE1*A( 2, 1 ), CSR, SNR, T )
*
            END IF
*
            SNR = -SNR
            CALL SROT( 2, A( 1, 1 ), 1, A( 1, 2 ), 1, CSR, SNR )
            CALL SROT( 2, B( 1, 1 ), 1, B( 1, 2 ), 1, CSR, SNR )
*
*           compute inf norms of A and B
*
            H1 = MAX( ABS( A( 1, 1 ) )+ABS( A( 1, 2 ) ),
     $           ABS( A( 2, 1 ) )+ABS( A( 2, 2 ) ) )
            H2 = MAX( ABS( B( 1, 1 ) )+ABS( B( 1, 2 ) ),
     $           ABS( B( 2, 1 ) )+ABS( B( 2, 2 ) ) )
*
            IF( ( SCALE1*H1 ).GE.ABS( WR1 )*H2 ) THEN
*
*              find left rotation matrix Q to zero out B(2,1)
*
               CALL SLARTG( B( 1, 1 ), B( 2, 1 ), CSL, SNL, R )
*
            ELSE
*
*              find left rotation matrix Q to zero out A(2,1)
*
               CALL SLARTG( A( 1, 1 ), A( 2, 1 ), CSL, SNL, R )
*
            END IF
*
            CALL SROT( 2, A( 1, 1 ), LDA, A( 2, 1 ), LDA, CSL, SNL )
            CALL SROT( 2, B( 1, 1 ), LDB, B( 2, 1 ), LDB, CSL, SNL )
*
            A( 2, 1 ) = ZERO
            B( 2, 1 ) = ZERO
*
         ELSE
*
*           a pair of complex conjugate eigenvalues
*           first compute the SVD of the matrix B
*
            CALL SLASV2( B( 1, 1 ), B( 1, 2 ), B( 2, 2 ), R, T, SNR,
     $                   CSR, SNL, CSL )
*
*           Form (A,B) := Q(A,B)Z**T where Q is left rotation matrix and
*           Z is right rotation matrix computed from SLASV2
*
            CALL SROT( 2, A( 1, 1 ), LDA, A( 2, 1 ), LDA, CSL, SNL )
            CALL SROT( 2, B( 1, 1 ), LDB, B( 2, 1 ), LDB, CSL, SNL )
            CALL SROT( 2, A( 1, 1 ), 1, A( 1, 2 ), 1, CSR, SNR )
            CALL SROT( 2, B( 1, 1 ), 1, B( 1, 2 ), 1, CSR, SNR )
*
            B( 2, 1 ) = ZERO
            B( 1, 2 ) = ZERO
*
         END IF
*
      END IF
*
*     Unscaling
*
      A( 1, 1 ) = ANORM*A( 1, 1 )
      A( 2, 1 ) = ANORM*A( 2, 1 )
      A( 1, 2 ) = ANORM*A( 1, 2 )
      A( 2, 2 ) = ANORM*A( 2, 2 )
      B( 1, 1 ) = BNORM*B( 1, 1 )
      B( 2, 1 ) = BNORM*B( 2, 1 )
      B( 1, 2 ) = BNORM*B( 1, 2 )
      B( 2, 2 ) = BNORM*B( 2, 2 )
*
      IF( WI.EQ.ZERO ) THEN
         ALPHAR( 1 ) = A( 1, 1 )
         ALPHAR( 2 ) = A( 2, 2 )
         ALPHAI( 1 ) = ZERO
         ALPHAI( 2 ) = ZERO
         BETA( 1 ) = B( 1, 1 )
         BETA( 2 ) = B( 2, 2 )
      ELSE
         ALPHAR( 1 ) = ANORM*WR1 / SCALE1 / BNORM
         ALPHAI( 1 ) = ANORM*WI / SCALE1 / BNORM
         ALPHAR( 2 ) = ALPHAR( 1 )
         ALPHAI( 2 ) = -ALPHAI( 1 )
         BETA( 1 ) = ONE
         BETA( 2 ) = ONE
      END IF
*
      RETURN
*
*     End of SLAGV2
*
      END
