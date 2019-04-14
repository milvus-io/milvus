*> \brief \b SGEQRT3 recursively computes a QR factorization of a general real or complex matrix using the compact WY representation of Q.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SGEQRT3 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/sgeqrt3.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/sgeqrt3.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/sgeqrt3.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       RECURSIVE SUBROUTINE SGEQRT3( M, N, A, LDA, T, LDT, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER   INFO, LDA, M, N, LDT
*       ..
*       .. Array Arguments ..
*       REAL   A( LDA, * ), T( LDT, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SGEQRT3 recursively computes a QR factorization of a real M-by-N
*> matrix A, using the compact WY representation of Q.
*>
*> Based on the algorithm of Elmroth and Gustavson,
*> IBM J. Res. Develop. Vol 44 No. 4 July 2000.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] M
*> \verbatim
*>          M is INTEGER
*>          The number of rows of the matrix A.  M >= N.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The number of columns of the matrix A.  N >= 0.
*> \endverbatim
*>
*> \param[in,out] A
*> \verbatim
*>          A is REAL array, dimension (LDA,N)
*>          On entry, the real M-by-N matrix A.  On exit, the elements on and
*>          above the diagonal contain the N-by-N upper triangular matrix R; the
*>          elements below the diagonal are the columns of V.  See below for
*>          further details.
*> \endverbatim
*>
*> \param[in] LDA
*> \verbatim
*>          LDA is INTEGER
*>          The leading dimension of the array A.  LDA >= max(1,M).
*> \endverbatim
*>
*> \param[out] T
*> \verbatim
*>          T is REAL array, dimension (LDT,N)
*>          The N-by-N upper triangular factor of the block reflector.
*>          The elements on and above the diagonal contain the block
*>          reflector T; the elements below the diagonal are not used.
*>          See below for further details.
*> \endverbatim
*>
*> \param[in] LDT
*> \verbatim
*>          LDT is INTEGER
*>          The leading dimension of the array T.  LDT >= max(1,N).
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
*> \date June 2016
*
*> \ingroup realGEcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  The matrix V stores the elementary reflectors H(i) in the i-th column
*>  below the diagonal. For example, if M=5 and N=3, the matrix V is
*>
*>               V = (  1       )
*>                   ( v1  1    )
*>                   ( v1 v2  1 )
*>                   ( v1 v2 v3 )
*>                   ( v1 v2 v3 )
*>
*>  where the vi's represent the vectors which define H(i), which are returned
*>  in the matrix A.  The 1's along the diagonal of V are not stored in A.  The
*>  block reflector H is then given by
*>
*>               H = I - V * T * V**T
*>
*>  where V**T is the transpose of V.
*>
*>  For details of the algorithm, see Elmroth and Gustavson (cited above).
*> \endverbatim
*>
*  =====================================================================
      RECURSIVE SUBROUTINE SGEQRT3( M, N, A, LDA, T, LDT, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     June 2016
*
*     .. Scalar Arguments ..
      INTEGER   INFO, LDA, M, N, LDT
*     ..
*     .. Array Arguments ..
      REAL   A( LDA, * ), T( LDT, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL   ONE
      PARAMETER ( ONE = 1.0 )
*     ..
*     .. Local Scalars ..
      INTEGER   I, I1, J, J1, N1, N2, IINFO
*     ..
*     .. External Subroutines ..
      EXTERNAL  SLARFG, STRMM, SGEMM, XERBLA
*     ..
*     .. Executable Statements ..
*
      INFO = 0
      IF( N .LT. 0 ) THEN
         INFO = -2
      ELSE IF( M .LT. N ) THEN
         INFO = -1
      ELSE IF( LDA .LT. MAX( 1, M ) ) THEN
         INFO = -4
      ELSE IF( LDT .LT. MAX( 1, N ) ) THEN
         INFO = -6
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'SGEQRT3', -INFO )
         RETURN
      END IF
*
      IF( N.EQ.1 ) THEN
*
*        Compute Householder transform when N=1
*
         CALL SLARFG( M, A(1,1), A( MIN( 2, M ), 1 ), 1, T(1,1) )
*
      ELSE
*
*        Otherwise, split A into blocks...
*
         N1 = N/2
         N2 = N-N1
         J1 = MIN( N1+1, N )
         I1 = MIN( N+1, M )
*
*        Compute A(1:M,1:N1) <- (Y1,R1,T1), where Q1 = I - Y1 T1 Y1^H
*
         CALL SGEQRT3( M, N1, A, LDA, T, LDT, IINFO )
*
*        Compute A(1:M,J1:N) = Q1^H A(1:M,J1:N) [workspace: T(1:N1,J1:N)]
*
         DO J=1,N2
            DO I=1,N1
               T( I, J+N1 ) = A( I, J+N1 )
            END DO
         END DO
         CALL STRMM( 'L', 'L', 'T', 'U', N1, N2, ONE,
     &               A, LDA, T( 1, J1 ), LDT )
*
         CALL SGEMM( 'T', 'N', N1, N2, M-N1, ONE, A( J1, 1 ), LDA,
     &               A( J1, J1 ), LDA, ONE, T( 1, J1 ), LDT)
*
         CALL STRMM( 'L', 'U', 'T', 'N', N1, N2, ONE,
     &               T, LDT, T( 1, J1 ), LDT )
*
         CALL SGEMM( 'N', 'N', M-N1, N2, N1, -ONE, A( J1, 1 ), LDA,
     &               T( 1, J1 ), LDT, ONE, A( J1, J1 ), LDA )
*
         CALL STRMM( 'L', 'L', 'N', 'U', N1, N2, ONE,
     &               A, LDA, T( 1, J1 ), LDT )
*
         DO J=1,N2
            DO I=1,N1
               A( I, J+N1 ) = A( I, J+N1 ) - T( I, J+N1 )
            END DO
         END DO
*
*        Compute A(J1:M,J1:N) <- (Y2,R2,T2) where Q2 = I - Y2 T2 Y2^H
*
         CALL SGEQRT3( M-N1, N2, A( J1, J1 ), LDA,
     &                T( J1, J1 ), LDT, IINFO )
*
*        Compute T3 = T(1:N1,J1:N) = -T1 Y1^H Y2 T2
*
         DO I=1,N1
            DO J=1,N2
               T( I, J+N1 ) = (A( J+N1, I ))
            END DO
         END DO
*
         CALL STRMM( 'R', 'L', 'N', 'U', N1, N2, ONE,
     &               A( J1, J1 ), LDA, T( 1, J1 ), LDT )
*
         CALL SGEMM( 'T', 'N', N1, N2, M-N, ONE, A( I1, 1 ), LDA,
     &               A( I1, J1 ), LDA, ONE, T( 1, J1 ), LDT )
*
         CALL STRMM( 'L', 'U', 'N', 'N', N1, N2, -ONE, T, LDT,
     &               T( 1, J1 ), LDT )
*
         CALL STRMM( 'R', 'U', 'N', 'N', N1, N2, ONE,
     &               T( J1, J1 ), LDT, T( 1, J1 ), LDT )
*
*        Y = (Y1,Y2); R = [ R1  A(1:N1,J1:N) ];  T = [T1 T3]
*                         [  0        R2     ]       [ 0 T2]
*
      END IF
*
      RETURN
*
*     End of SGEQRT3
*
      END
