*> \brief \b ZGELQT3 recursively computes a LQ factorization of a general real or complex matrix using the compact WY representation of Q.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download DGEQRT3 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zgelqt3.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zgelqt3.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zgelqt3.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       RECURSIVE SUBROUTINE ZGELQT3( M, N, A, LDA, T, LDT, INFO )
*
*       .. Scalar Arguments ..
*       INTEGER   INFO, LDA, M, N, LDT
*       ..
*       .. Array Arguments ..
*       COMPLEX*16   A( LDA, * ), T( LDT, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> DGELQT3 recursively computes a LQ factorization of a complex M-by-N
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
*>          The number of rows of the matrix A.  M =< N.
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
*>          A is COMPLEX*16 array, dimension (LDA,N)
*>          On entry, the real M-by-N matrix A.  On exit, the elements on and
*>          below the diagonal contain the N-by-N lower triangular matrix L; the
*>          elements above the diagonal are the rows of V.  See below for
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
*>          T is COMPLEX*16 array, dimension (LDT,N)
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
*> \date November 2017
*
*> \ingroup doubleGEcomputational
*
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  The matrix V stores the elementary reflectors H(i) in the i-th row
*>  above the diagonal. For example, if M=5 and N=3, the matrix V is
*>
*>               V = (  1  v1 v1 v1 v1 )
*>                   (     1  v2 v2 v2 )
*>                   (     1  v3 v3 v3 )
*>
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
      RECURSIVE SUBROUTINE ZGELQT3( M, N, A, LDA, T, LDT, INFO )
*
*  -- LAPACK computational routine (version 3.8.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     November 2017
*
*     .. Scalar Arguments ..
      INTEGER   INFO, LDA, M, N, LDT
*     ..
*     .. Array Arguments ..
      COMPLEX*16   A( LDA, * ), T( LDT, * )
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      COMPLEX*16   ONE, ZERO
      PARAMETER ( ONE = (1.0D+00,0.0D+00) )
      PARAMETER ( ZERO = (0.0D+00,0.0D+00))
*     ..
*     .. Local Scalars ..
      INTEGER   I, I1, J, J1, M1, M2, IINFO
*     ..
*     .. External Subroutines ..
      EXTERNAL  ZLARFG, ZTRMM, ZGEMM, XERBLA
*     ..
*     .. Executable Statements ..
*
      INFO = 0
      IF( M .LT. 0 ) THEN
         INFO = -1
      ELSE IF( N .LT. M ) THEN
         INFO = -2
      ELSE IF( LDA .LT. MAX( 1, M ) ) THEN
         INFO = -4
      ELSE IF( LDT .LT. MAX( 1, M ) ) THEN
         INFO = -6
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'ZGELQT3', -INFO )
         RETURN
      END IF
*
      IF( M.EQ.1 ) THEN
*
*        Compute Householder transform when N=1
*
         CALL ZLARFG( N, A, A( 1, MIN( 2, N ) ), LDA, T )
         T(1,1)=CONJG(T(1,1))
*
      ELSE
*
*        Otherwise, split A into blocks...
*
         M1 = M/2
         M2 = M-M1
         I1 = MIN( M1+1, M )
         J1 = MIN( M+1, N )
*
*        Compute A(1:M1,1:N) <- (Y1,R1,T1), where Q1 = I - Y1 T1 Y1^H
*
         CALL ZGELQT3( M1, N, A, LDA, T, LDT, IINFO )
*
*        Compute A(J1:M,1:N) =  A(J1:M,1:N) Q1^H [workspace: T(1:N1,J1:N)]
*
         DO I=1,M2
            DO J=1,M1
               T(  I+M1, J ) = A( I+M1, J )
            END DO
         END DO
         CALL ZTRMM( 'R', 'U', 'C', 'U', M2, M1, ONE,
     &               A, LDA, T( I1, 1 ), LDT )
*
         CALL ZGEMM( 'N', 'C', M2, M1, N-M1, ONE, A( I1, I1 ), LDA,
     &               A( 1, I1 ), LDA, ONE, T( I1, 1 ), LDT)
*
         CALL ZTRMM( 'R', 'U', 'N', 'N', M2, M1, ONE,
     &               T, LDT, T( I1, 1 ), LDT )
*
         CALL ZGEMM( 'N', 'N', M2, N-M1, M1, -ONE, T( I1, 1 ), LDT,
     &                A( 1, I1 ), LDA, ONE, A( I1, I1 ), LDA )
*
         CALL ZTRMM( 'R', 'U', 'N', 'U', M2, M1 , ONE,
     &               A, LDA, T( I1, 1 ), LDT )
*
         DO I=1,M2
            DO J=1,M1
               A(  I+M1, J ) = A( I+M1, J ) - T( I+M1, J )
               T( I+M1, J )= ZERO
            END DO
         END DO
*
*        Compute A(J1:M,J1:N) <- (Y2,R2,T2) where Q2 = I - Y2 T2 Y2^H
*
         CALL ZGELQT3( M2, N-M1, A( I1, I1 ), LDA,
     &                T( I1, I1 ), LDT, IINFO )
*
*        Compute T3 = T(J1:N1,1:N) = -T1 Y1^H Y2 T2
*
         DO I=1,M2
            DO J=1,M1
               T( J, I+M1  ) = (A( J, I+M1 ))
            END DO
         END DO
*
         CALL ZTRMM( 'R', 'U', 'C', 'U', M1, M2, ONE,
     &               A( I1, I1 ), LDA, T( 1, I1 ), LDT )
*
         CALL ZGEMM( 'N', 'C', M1, M2, N-M, ONE, A( 1, J1 ), LDA,
     &               A( I1, J1 ), LDA, ONE, T( 1, I1 ), LDT )
*
         CALL ZTRMM( 'L', 'U', 'N', 'N', M1, M2, -ONE, T, LDT,
     &               T( 1, I1 ), LDT )
*
         CALL ZTRMM( 'R', 'U', 'N', 'N', M1, M2, ONE,
     &               T( I1, I1 ), LDT, T( 1, I1 ), LDT )
*
*
*
*        Y = (Y1,Y2); L = [ L1            0  ];  T = [T1 T3]
*                         [ A(1:N1,J1:N)  L2 ]       [ 0 T2]
*
      END IF
*
      RETURN
*
*     End of ZGELQT3
*
      END
