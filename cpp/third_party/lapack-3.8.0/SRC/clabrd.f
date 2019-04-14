*> \brief \b CLABRD reduces the first nb rows and columns of a general matrix to a bidiagonal form.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CLABRD + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/clabrd.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/clabrd.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/clabrd.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CLABRD( M, N, NB, A, LDA, D, E, TAUQ, TAUP, X, LDX, Y,
*                          LDY )
*
*       .. Scalar Arguments ..
*       INTEGER            LDA, LDX, LDY, M, N, NB
*       ..
*       .. Array Arguments ..
*       REAL               D( * ), E( * )
*       COMPLEX            A( LDA, * ), TAUP( * ), TAUQ( * ), X( LDX, * ),
*      $                   Y( LDY, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CLABRD reduces the first NB rows and columns of a complex general
*> m by n matrix A to upper or lower real bidiagonal form by a unitary
*> transformation Q**H * A * P, and returns the matrices X and Y which
*> are needed to apply the transformation to the unreduced part of A.
*>
*> If m >= n, A is reduced to upper bidiagonal form; if m < n, to lower
*> bidiagonal form.
*>
*> This is an auxiliary routine called by CGEBRD
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows in the matrix A.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns in the matrix A.
*> \endverbatim
*>
*> \param[in] NB
*> \verbatim
*>          NB is INTEGER
*>          The number of leading rows and columns of A to be reduced.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is COMPLEX array, dimension (LDA,N)
*>          On entry, the m by n general matrix to be reduced.
*>          On exit, the first NB rows and columns of the matrix are
*>          overwritten; the rest of the array is unchanged.
*>          If m >= n, elements on and below the diagonal in the first NB
*>            columns, with the array TAUQ, represent the unitary
*>            matrix Q as a product of elementary reflectors; and
*>            elements above the diagonal in the first NB rows, with the
*>            array TAUP, represent the unitary matrix P as a product
*>            of elementary reflectors.
*>          If m < n, elements below the diagonal in the first NB
*>            columns, with the array TAUQ, represent the unitary
*>            matrix Q as a product of elementary reflectors, and
*>            elements on and above the diagonal in the first NB rows,
*>            with the array TAUP, represent the unitary matrix P as
*>            a product of elementary reflectors.
*>          See Further Details.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,M).
*> \endverbatim
*>
*> \param[out] D
*> \verbatim
*>          D is REAL array, dimension (NB)
*>          The diagonal elements of the first NB rows and columns of
*>          the reduced matrix.  D(i) = A(i,i).
*> \endverbatim
*>
*> \param[out] E
*> \verbatim
*>          E is REAL array, dimension (NB)
*>          The off-diagonal elements of the first NB rows and columns of
*>          the reduced matrix.
*> \endverbatim
*>
*> \param[out] TAUQ
*> \verbatim
*>          TAUQ is COMPLEX array, dimension (NB)
*>          The scalar factors of the elementary reflectors which
*>          represent the unitary matrix Q. See Further Details.
*> \endverbatim
*>
*> \param[out] TAUP
*> \verbatim
*>          TAUP is COMPLEX array, dimension (NB)
*>          The scalar factors of the elementary reflectors which
*>          represent the unitary matrix P. See Further Details.
*> \endverbatim
*>
*> \param[out] X
*> \verbatim
*>          X is COMPLEX array, dimension (LDX,NB)
*>          The m-by-nb matrix X required to update the unreduced part
*>          of A.
*> \endverbatim
*>
*> \param[in] LDX
*> \verbatim
*>          LDX is INTEGER
*>          The leading dimension of the array X. LDX >= max(1,M).
*> \endverbatim
*>
*> \param[out] Y
*> \verbatim
*>          Y is COMPLEX array, dimension (LDY,NB)
*>          The n-by-nb matrix Y required to update the unreduced part
*>          of A.
*> \endverbatim
*>
*> \param[in] LDY
*> \verbatim
*>          LDY is INTEGER
*>          The leading dimension of the array Y. LDY >= max(1,N).
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
*> \date June 2017
*
*> \ingroup complexOTHERauxiliary
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  The matrices Q and P are represented as products of elementary
*>  reflectors:
*>
*>     Q = H(1) H(2) . . . H(nb)  and  P = G(1) G(2) . . . G(nb)
*>
*>  Each H(i) and G(i) has the form:
*>
*>     H(i) = I - tauq * v * v**H  and G(i) = I - taup * u * u**H
*>
*>  where tauq and taup are complex scalars, and v and u are complex
*>  vectors.
*>
*>  If m >= n, v(1:i-1) = 0, v(i) = 1, and v(i:m) is stored on exit in
*>  A(i:m,i); u(1:i) = 0, u(i+1) = 1, and u(i+1:n) is stored on exit in
*>  A(i,i+1:n); tauq is stored in TAUQ(i) and taup in TAUP(i).
*>
*>  If m < n, v(1:i) = 0, v(i+1) = 1, and v(i+1:m) is stored on exit in
*>  A(i+2:m,i); u(1:i-1) = 0, u(i) = 1, and u(i:n) is stored on exit in
*>  A(i,i+1:n); tauq is stored in TAUQ(i) and taup in TAUP(i).
*>
*>  The elements of the vectors v and u together form the m-by-nb matrix
*>  V and the nb-by-n matrix U**H which are needed, with X and Y, to apply
*>  the transformation to the unreduced part of the matrix, using a block
*>  update of the form:  A := A - V*Y**H - X*U**H.
*>
*>  The contents of A on exit are illustrated by the following examples
*>  with nb = 2:
*>
*>  m = 6 and n = 5 (m > n):          m = 5 and n = 6 (m < n):
*>
*>    (  1   1   u1  u1  u1 )           (  1   u1  u1  u1  u1  u1 )
*>    (  v1  1   1   u2  u2 )           (  1   1   u2  u2  u2  u2 )
*>    (  v1  v2  a   a   a  )           (  v1  1   a   a   a   a  )
*>    (  v1  v2  a   a   a  )           (  v1  v2  a   a   a   a  )
*>    (  v1  v2  a   a   a  )           (  v1  v2  a   a   a   a  )
*>    (  v1  v2  a   a   a  )
*>
*>  where a denotes an element of the original matrix which is unchanged,
*>  vi denotes an element of the vector defining H(i), and ui an element
*>  of the vector defining G(i).
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE CLABRD( M, N, NB, A, LDA, D, E, TAUQ, TAUP, X, LDX, Y,
     $                   LDY )
*
*  -- LAPACK auxiliary routine (version 3.7.1) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2017
*
*     .. Scalar Arguments ..
      INTEGER            LDA, LDX, LDY, M, N, NB
*     ..
*     .. Array Arguments ..
      REAL               D( * ), E( * )
      COMPLEX            A( LDA, * ), TAUP( * ), TAUQ( * ), X( LDX, * ),
     $                   Y( LDY, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX            ZERO, ONE
      PARAMETER          ( ZERO = ( 0.0E+0, 0.0E+0 ),
     $                   ONE = ( 1.0E+0, 0.0E+0 ) )
*     ..
*     .. Local Scalars ..
      INTEGER            I
      COMPLEX            ALPHA
*     ..
*     .. External Subroutines ..
      EXTERNAL           CGEMV, CLACGV, CLARFG, CSCAL
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          MIN
*     ..
*     .. Executable Statements ..
*
*     Quick return if possible
*
      IF( M.LE.0 .OR. N.LE.0 )
     $   RETURN
*
      IF( M.GE.N ) THEN
*
*        Reduce to upper bidiagonal form
*
         DO 10 I = 1, NB
*
*           Update A(i:m,i)
*
            CALL CLACGV( I-1, Y( I, 1 ), LDY )
            CALL CGEMV( 'No transpose', M-I+1, I-1, -ONE, A( I, 1 ),
     $                  LDA, Y( I, 1 ), LDY, ONE, A( I, I ), 1 )
            CALL CLACGV( I-1, Y( I, 1 ), LDY )
            CALL CGEMV( 'No transpose', M-I+1, I-1, -ONE, X( I, 1 ),
     $                  LDX, A( 1, I ), 1, ONE, A( I, I ), 1 )
*
*           Generate reflection Q(i) to annihilate A(i+1:m,i)
*
            ALPHA = A( I, I )
            CALL CLARFG( M-I+1, ALPHA, A( MIN( I+1, M ), I ), 1,
     $                   TAUQ( I ) )
            D( I ) = ALPHA
            IF( I.LT.N ) THEN
               A( I, I ) = ONE
*
*              Compute Y(i+1:n,i)
*
               CALL CGEMV( 'Conjugate transpose', M-I+1, N-I, ONE,
     $                     A( I, I+1 ), LDA, A( I, I ), 1, ZERO,
     $                     Y( I+1, I ), 1 )
               CALL CGEMV( 'Conjugate transpose', M-I+1, I-1, ONE,
     $                     A( I, 1 ), LDA, A( I, I ), 1, ZERO,
     $                     Y( 1, I ), 1 )
               CALL CGEMV( 'No transpose', N-I, I-1, -ONE, Y( I+1, 1 ),
     $                     LDY, Y( 1, I ), 1, ONE, Y( I+1, I ), 1 )
               CALL CGEMV( 'Conjugate transpose', M-I+1, I-1, ONE,
     $                     X( I, 1 ), LDX, A( I, I ), 1, ZERO,
     $                     Y( 1, I ), 1 )
               CALL CGEMV( 'Conjugate transpose', I-1, N-I, -ONE,
     $                     A( 1, I+1 ), LDA, Y( 1, I ), 1, ONE,
     $                     Y( I+1, I ), 1 )
               CALL CSCAL( N-I, TAUQ( I ), Y( I+1, I ), 1 )
*
*              Update A(i,i+1:n)
*
               CALL CLACGV( N-I, A( I, I+1 ), LDA )
               CALL CLACGV( I, A( I, 1 ), LDA )
               CALL CGEMV( 'No transpose', N-I, I, -ONE, Y( I+1, 1 ),
     $                     LDY, A( I, 1 ), LDA, ONE, A( I, I+1 ), LDA )
               CALL CLACGV( I, A( I, 1 ), LDA )
               CALL CLACGV( I-1, X( I, 1 ), LDX )
               CALL CGEMV( 'Conjugate transpose', I-1, N-I, -ONE,
     $                     A( 1, I+1 ), LDA, X( I, 1 ), LDX, ONE,
     $                     A( I, I+1 ), LDA )
               CALL CLACGV( I-1, X( I, 1 ), LDX )
*
*              Generate reflection P(i) to annihilate A(i,i+2:n)
*
               ALPHA = A( I, I+1 )
               CALL CLARFG( N-I, ALPHA, A( I, MIN( I+2, N ) ),
     $                      LDA, TAUP( I ) )
               E( I ) = ALPHA
               A( I, I+1 ) = ONE
*
*              Compute X(i+1:m,i)
*
               CALL CGEMV( 'No transpose', M-I, N-I, ONE, A( I+1, I+1 ),
     $                     LDA, A( I, I+1 ), LDA, ZERO, X( I+1, I ), 1 )
               CALL CGEMV( 'Conjugate transpose', N-I, I, ONE,
     $                     Y( I+1, 1 ), LDY, A( I, I+1 ), LDA, ZERO,
     $                     X( 1, I ), 1 )
               CALL CGEMV( 'No transpose', M-I, I, -ONE, A( I+1, 1 ),
     $                     LDA, X( 1, I ), 1, ONE, X( I+1, I ), 1 )
               CALL CGEMV( 'No transpose', I-1, N-I, ONE, A( 1, I+1 ),
     $                     LDA, A( I, I+1 ), LDA, ZERO, X( 1, I ), 1 )
               CALL CGEMV( 'No transpose', M-I, I-1, -ONE, X( I+1, 1 ),
     $                     LDX, X( 1, I ), 1, ONE, X( I+1, I ), 1 )
               CALL CSCAL( M-I, TAUP( I ), X( I+1, I ), 1 )
               CALL CLACGV( N-I, A( I, I+1 ), LDA )
            END IF
   10    CONTINUE
      ELSE
*
*        Reduce to lower bidiagonal form
*
         DO 20 I = 1, NB
*
*           Update A(i,i:n)
*
            CALL CLACGV( N-I+1, A( I, I ), LDA )
            CALL CLACGV( I-1, A( I, 1 ), LDA )
            CALL CGEMV( 'No transpose', N-I+1, I-1, -ONE, Y( I, 1 ),
     $                  LDY, A( I, 1 ), LDA, ONE, A( I, I ), LDA )
            CALL CLACGV( I-1, A( I, 1 ), LDA )
            CALL CLACGV( I-1, X( I, 1 ), LDX )
            CALL CGEMV( 'Conjugate transpose', I-1, N-I+1, -ONE,
     $                  A( 1, I ), LDA, X( I, 1 ), LDX, ONE, A( I, I ),
     $                  LDA )
            CALL CLACGV( I-1, X( I, 1 ), LDX )
*
*           Generate reflection P(i) to annihilate A(i,i+1:n)
*
            ALPHA = A( I, I )
            CALL CLARFG( N-I+1, ALPHA, A( I, MIN( I+1, N ) ), LDA,
     $                   TAUP( I ) )
            D( I ) = ALPHA
            IF( I.LT.M ) THEN
               A( I, I ) = ONE
*
*              Compute X(i+1:m,i)
*
               CALL CGEMV( 'No transpose', M-I, N-I+1, ONE, A( I+1, I ),
     $                     LDA, A( I, I ), LDA, ZERO, X( I+1, I ), 1 )
               CALL CGEMV( 'Conjugate transpose', N-I+1, I-1, ONE,
     $                     Y( I, 1 ), LDY, A( I, I ), LDA, ZERO,
     $                     X( 1, I ), 1 )
               CALL CGEMV( 'No transpose', M-I, I-1, -ONE, A( I+1, 1 ),
     $                     LDA, X( 1, I ), 1, ONE, X( I+1, I ), 1 )
               CALL CGEMV( 'No transpose', I-1, N-I+1, ONE, A( 1, I ),
     $                     LDA, A( I, I ), LDA, ZERO, X( 1, I ), 1 )
               CALL CGEMV( 'No transpose', M-I, I-1, -ONE, X( I+1, 1 ),
     $                     LDX, X( 1, I ), 1, ONE, X( I+1, I ), 1 )
               CALL CSCAL( M-I, TAUP( I ), X( I+1, I ), 1 )
               CALL CLACGV( N-I+1, A( I, I ), LDA )
*
*              Update A(i+1:m,i)
*
               CALL CLACGV( I-1, Y( I, 1 ), LDY )
               CALL CGEMV( 'No transpose', M-I, I-1, -ONE, A( I+1, 1 ),
     $                     LDA, Y( I, 1 ), LDY, ONE, A( I+1, I ), 1 )
               CALL CLACGV( I-1, Y( I, 1 ), LDY )
               CALL CGEMV( 'No transpose', M-I, I, -ONE, X( I+1, 1 ),
     $                     LDX, A( 1, I ), 1, ONE, A( I+1, I ), 1 )
*
*              Generate reflection Q(i) to annihilate A(i+2:m,i)
*
               ALPHA = A( I+1, I )
               CALL CLARFG( M-I, ALPHA, A( MIN( I+2, M ), I ), 1,
     $                      TAUQ( I ) )
               E( I ) = ALPHA
               A( I+1, I ) = ONE
*
*              Compute Y(i+1:n,i)
*
               CALL CGEMV( 'Conjugate transpose', M-I, N-I, ONE,
     $                     A( I+1, I+1 ), LDA, A( I+1, I ), 1, ZERO,
     $                     Y( I+1, I ), 1 )
               CALL CGEMV( 'Conjugate transpose', M-I, I-1, ONE,
     $                     A( I+1, 1 ), LDA, A( I+1, I ), 1, ZERO,
     $                     Y( 1, I ), 1 )
               CALL CGEMV( 'No transpose', N-I, I-1, -ONE, Y( I+1, 1 ),
     $                     LDY, Y( 1, I ), 1, ONE, Y( I+1, I ), 1 )
               CALL CGEMV( 'Conjugate transpose', M-I, I, ONE,
     $                     X( I+1, 1 ), LDX, A( I+1, I ), 1, ZERO,
     $                     Y( 1, I ), 1 )
               CALL CGEMV( 'Conjugate transpose', I, N-I, -ONE,
     $                     A( 1, I+1 ), LDA, Y( 1, I ), 1, ONE,
     $                     Y( I+1, I ), 1 )
               CALL CSCAL( N-I, TAUQ( I ), Y( I+1, I ), 1 )
            ELSE
               CALL CLACGV( N-I+1, A( I, I ), LDA )
            END IF
   20    CONTINUE
      END IF
      RETURN
*
*     End of CLABRD
*
      END
