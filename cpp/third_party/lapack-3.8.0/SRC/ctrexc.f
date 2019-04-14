*> \brief \b CTREXC
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download CTREXC + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/ctrexc.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/ctrexc.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/ctrexc.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE CTREXC( COMPQ, N, T, LDT, Q, LDQ, IFST, ILST, INFO )
*
*       .. Scalar Arguments ..
*       CHARACTER          COMPQ
*       INTEGER            IFST, ILST, INFO, LDQ, LDT, N
*       ..
*       .. Array Arguments ..
*       COMPLEX            Q( LDQ, * ), T( LDT, * )
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> CTREXC reorders the Schur factorization of a complex matrix
*> A = Q*T*Q**H, so that the diagonal element of T with row index IFST
*> is moved to row ILST.
*>
*> The Schur form T is reordered by a unitary similarity transformation
*> Z**H*T*Z, and optionally the matrix Q of Schur vectors is updated by
*> postmultplying it with Z.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] COMPQ
*> \verbatim
*>          COMPQ is CHARACTER*1
*>          = 'V':  update the matrix Q of Schur vectors;
*>          = 'N':  do not update Q.
*> \endverbatim
*>
*> \param[in] N
*> \verbatim
*>          N is INTEGER
*>          The order of the matrix T. N >= 0.
*>          If N == 0 arguments ILST and IFST may be any value.
*> \endverbatim
*>
*> \param[in,out] T
*> \verbatim
*>          T is COMPLEX array, dimension (LDT,N)
*>          On entry, the upper triangular matrix T.
*>          On exit, the reordered upper triangular matrix.
*> \endverbatim
*>
*> \param[in] LDT
*> \verbatim
*>          LDT is INTEGER
*>          The leading dimension of the array T. LDT >= max(1,N).
*> \endverbatim
*>
*> \param[in,out] Q
*> \verbatim
*>          Q is COMPLEX array, dimension (LDQ,N)
*>          On entry, if COMPQ = 'V', the matrix Q of Schur vectors.
*>          On exit, if COMPQ = 'V', Q has been postmultiplied by the
*>          unitary transformation matrix Z which reorders T.
*>          If COMPQ = 'N', Q is not referenced.
*> \endverbatim
*>
*> \param[in] LDQ
*> \verbatim
*>          LDQ is INTEGER
*>          The leading dimension of the array Q.  LDQ >= 1, and if
*>          COMPQ = 'V', LDQ >= max(1,N).
*> \endverbatim
*>
*> \param[in] IFST
*> \verbatim
*>          IFST is INTEGER
*> \endverbatim
*>
*> \param[in] ILST
*> \verbatim
*>          ILST is INTEGER
*>
*>          Specify the reordering of the diagonal elements of T:
*>          The element with row index IFST is moved to row ILST by a
*>          sequence of transpositions between adjacent elements.
*>          1 <= IFST <= N; 1 <= ILST <= N.
*> \endverbatim
*>
*> \param[out] INFO
*> \verbatim
*>          INFO is INTEGER
*>          = 0:  successful exit
*>          < 0:  if INFO = -i, the i-th argument had an illegal value
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
*> \ingroup complexOTHERcomputational
*
*  =====================================================================
      SUBROUTINE CTREXC( COMPQ, N, T, LDT, Q, LDQ, IFST, ILST, INFO )
*
*  -- LAPACK computational routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      CHARACTER          COMPQ
      INTEGER            IFST, ILST, INFO, LDQ, LDT, N
*     ..
*     .. Array Arguments ..
      COMPLEX            Q( LDQ, * ), T( LDT, * )
*     ..
*
*  =====================================================================
*
*     .. Local Scalars ..
      LOGICAL            WANTQ
      INTEGER            K, M1, M2, M3
      REAL               CS
      COMPLEX            SN, T11, T22, TEMP
*     ..
*     .. External Functions ..
      LOGICAL            LSAME
      EXTERNAL           LSAME
*     ..
*     .. External Subroutines ..
      EXTERNAL           CLARTG, CROT, XERBLA
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          CONJG, MAX
*     ..
*     .. Executable Statements ..
*
*     Decode and test the input parameters.
*
      INFO = 0
      WANTQ = LSAME( COMPQ, 'V' )
      IF( .NOT.LSAME( COMPQ, 'N' ) .AND. .NOT.WANTQ ) THEN
         INFO = -1
      ELSE IF( N.LT.0 ) THEN
         INFO = -2
      ELSE IF( LDT.LT.MAX( 1, N ) ) THEN
         INFO = -4
      ELSE IF( LDQ.LT.1 .OR. ( WANTQ .AND. LDQ.LT.MAX( 1, N ) ) ) THEN
         INFO = -6
      ELSE IF(( IFST.LT.1 .OR. IFST.GT.N ).AND.( N.GT.0 )) THEN
         INFO = -7
      ELSE IF(( ILST.LT.1 .OR. ILST.GT.N ).AND.( N.GT.0 )) THEN
         INFO = -8
      END IF
      IF( INFO.NE.0 ) THEN
         CALL XERBLA( 'CTREXC', -INFO )
         RETURN
      END IF
*
*     Quick return if possible
*
      IF( N.LE.1 .OR. IFST.EQ.ILST )
     $   RETURN
*
      IF( IFST.LT.ILST ) THEN
*
*        Move the IFST-th diagonal element forward down the diagonal.
*
         M1 = 0
         M2 = -1
         M3 = 1
      ELSE
*
*        Move the IFST-th diagonal element backward up the diagonal.
*
         M1 = -1
         M2 = 0
         M3 = -1
      END IF
*
      DO 10 K = IFST + M1, ILST + M2, M3
*
*        Interchange the k-th and (k+1)-th diagonal elements.
*
         T11 = T( K, K )
         T22 = T( K+1, K+1 )
*
*        Determine the transformation to perform the interchange.
*
         CALL CLARTG( T( K, K+1 ), T22-T11, CS, SN, TEMP )
*
*        Apply transformation to the matrix T.
*
         IF( K+2.LE.N )
     $      CALL CROT( N-K-1, T( K, K+2 ), LDT, T( K+1, K+2 ), LDT, CS,
     $                 SN )
         CALL CROT( K-1, T( 1, K ), 1, T( 1, K+1 ), 1, CS, CONJG( SN ) )
*
         T( K, K ) = T22
         T( K+1, K+1 ) = T11
*
         IF( WANTQ ) THEN
*
*           Accumulate transformation in the matrix Q.
*
            CALL CROT( N, Q( 1, K ), 1, Q( 1, K+1 ), 1, CS,
     $                 CONJG( SN ) )
         END IF
*
   10 CONTINUE
*
      RETURN
*
*     End of CTREXC
*
      END
