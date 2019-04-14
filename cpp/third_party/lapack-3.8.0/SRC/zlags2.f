*> \brief \b ZLAGS2
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download ZLAGS2 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/zlags2.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/zlags2.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/zlags2.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE ZLAGS2( UPPER, A1, A2, A3, B1, B2, B3, CSU, SNU, CSV,
*                          SNV, CSQ, SNQ )
*
*       .. Scalar Arguments ..
*       LOGICAL            UPPER
*       DOUBLE PRECISION   A1, A3, B1, B3, CSQ, CSU, CSV
*       COMPLEX*16         A2, B2, SNQ, SNU, SNV
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> ZLAGS2 computes 2-by-2 unitary matrices U, V and Q, such
*> that if ( UPPER ) then
*>
*>           U**H *A*Q = U**H *( A1 A2 )*Q = ( x  0  )
*>                             ( 0  A3 )     ( x  x  )
*> and
*>           V**H*B*Q = V**H *( B1 B2 )*Q = ( x  0  )
*>                            ( 0  B3 )     ( x  x  )
*>
*> or if ( .NOT.UPPER ) then
*>
*>           U**H *A*Q = U**H *( A1 0  )*Q = ( x  x  )
*>                             ( A2 A3 )     ( 0  x  )
*> and
*>           V**H *B*Q = V**H *( B1 0  )*Q = ( x  x  )
*>                             ( B2 B3 )     ( 0  x  )
*> where
*>
*>   U = (   CSU    SNU ), V = (  CSV    SNV ),
*>       ( -SNU**H  CSU )      ( -SNV**H CSV )
*>
*>   Q = (   CSQ    SNQ )
*>       ( -SNQ**H  CSQ )
*>
*> The rows of the transformed A and B are parallel. Moreover, if the
*> input 2-by-2 matrix A is not zero, then the transformed (1,1) entry
*> of A is not zero. If the input matrices A and B are both not zero,
*> then the transformed (2,2) element of B is not zero, except when the
*> first rows of input A and B are parallel and the second rows are
*> zero.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in] UPPER
*> \verbatim
*>          UPPER is LOGICAL
*>          = .TRUE.: the input matrices A and B are upper triangular.
*>          = .FALSE.: the input matrices A and B are lower triangular.
*> \endverbatim
*>
*> \param[in] A1
*> \verbatim
*>          A1 is DOUBLE PRECISION
*> \endverbatim
*>
*> \param[in] A2
*> \verbatim
*>          A2 is COMPLEX*16
*> \endverbatim
*>
*> \param[in] A3
*> \verbatim
*>          A3 is DOUBLE PRECISION
*>          On entry, A1, A2 and A3 are elements of the input 2-by-2
*>          upper (lower) triangular matrix A.
*> \endverbatim
*>
*> \param[in] B1
*> \verbatim
*>          B1 is DOUBLE PRECISION
*> \endverbatim
*>
*> \param[in] B2
*> \verbatim
*>          B2 is COMPLEX*16
*> \endverbatim
*>
*> \param[in] B3
*> \verbatim
*>          B3 is DOUBLE PRECISION
*>          On entry, B1, B2 and B3 are elements of the input 2-by-2
*>          upper (lower) triangular matrix B.
*> \endverbatim
*>
*> \param[out] CSU
*> \verbatim
*>          CSU is DOUBLE PRECISION
*> \endverbatim
*>
*> \param[out] SNU
*> \verbatim
*>          SNU is COMPLEX*16
*>          The desired unitary matrix U.
*> \endverbatim
*>
*> \param[out] CSV
*> \verbatim
*>          CSV is DOUBLE PRECISION
*> \endverbatim
*>
*> \param[out] SNV
*> \verbatim
*>          SNV is COMPLEX*16
*>          The desired unitary matrix V.
*> \endverbatim
*>
*> \param[out] CSQ
*> \verbatim
*>          CSQ is DOUBLE PRECISION
*> \endverbatim
*>
*> \param[out] SNQ
*> \verbatim
*>          SNQ is COMPLEX*16
*>          The desired unitary matrix Q.
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
*> \ingroup complex16OTHERauxiliary
*
*  =====================================================================
      SUBROUTINE ZLAGS2( UPPER, A1, A2, A3, B1, B2, B3, CSU, SNU, CSV,
     $                   SNV, CSQ, SNQ )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      LOGICAL            UPPER
      DOUBLE PRECISION   A1, A3, B1, B3, CSQ, CSU, CSV
      COMPLEX*16         A2, B2, SNQ, SNU, SNV
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      DOUBLE PRECISION   ZERO, ONE
      PARAMETER          ( ZERO = 0.0D+0, ONE = 1.0D+0 )
*     ..
*     .. Local Scalars ..
      DOUBLE PRECISION   A, AUA11, AUA12, AUA21, AUA22, AVB12, AVB11,
     $                   AVB21, AVB22, CSL, CSR, D, FB, FC, S1, S2,
     $                   SNL, SNR, UA11R, UA22R, VB11R, VB22R
      COMPLEX*16         B, C, D1, R, T, UA11, UA12, UA21, UA22, VB11,
     $                   VB12, VB21, VB22
*     ..
*     .. External Subroutines ..
      EXTERNAL           DLASV2, ZLARTG
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, DBLE, DCMPLX, DCONJG, DIMAG
*     ..
*     .. Statement Functions ..
      DOUBLE PRECISION   ABS1
*     ..
*     .. Statement Function definitions ..
      ABS1( T ) = ABS( DBLE( T ) ) + ABS( DIMAG( T ) )
*     ..
*     .. Executable Statements ..
*
      IF( UPPER ) THEN
*
*        Input matrices A and B are upper triangular matrices
*
*        Form matrix C = A*adj(B) = ( a b )
*                                   ( 0 d )
*
         A = A1*B3
         D = A3*B1
         B = A2*B1 - A1*B2
         FB = ABS( B )
*
*        Transform complex 2-by-2 matrix C to real matrix by unitary
*        diagonal matrix diag(1,D1).
*
         D1 = ONE
         IF( FB.NE.ZERO )
     $      D1 = B / FB
*
*        The SVD of real 2 by 2 triangular C
*
*         ( CSL -SNL )*( A B )*(  CSR  SNR ) = ( R 0 )
*         ( SNL  CSL ) ( 0 D ) ( -SNR  CSR )   ( 0 T )
*
         CALL DLASV2( A, FB, D, S1, S2, SNR, CSR, SNL, CSL )
*
         IF( ABS( CSL ).GE.ABS( SNL ) .OR. ABS( CSR ).GE.ABS( SNR ) )
     $        THEN
*
*           Compute the (1,1) and (1,2) elements of U**H *A and V**H *B,
*           and (1,2) element of |U|**H *|A| and |V|**H *|B|.
*
            UA11R = CSL*A1
            UA12 = CSL*A2 + D1*SNL*A3
*
            VB11R = CSR*B1
            VB12 = CSR*B2 + D1*SNR*B3
*
            AUA12 = ABS( CSL )*ABS1( A2 ) + ABS( SNL )*ABS( A3 )
            AVB12 = ABS( CSR )*ABS1( B2 ) + ABS( SNR )*ABS( B3 )
*
*           zero (1,2) elements of U**H *A and V**H *B
*
            IF( ( ABS( UA11R )+ABS1( UA12 ) ).EQ.ZERO ) THEN
               CALL ZLARTG( -DCMPLX( VB11R ), DCONJG( VB12 ), CSQ, SNQ,
     $                      R )
            ELSE IF( ( ABS( VB11R )+ABS1( VB12 ) ).EQ.ZERO ) THEN
               CALL ZLARTG( -DCMPLX( UA11R ), DCONJG( UA12 ), CSQ, SNQ,
     $                      R )
            ELSE IF( AUA12 / ( ABS( UA11R )+ABS1( UA12 ) ).LE.AVB12 /
     $               ( ABS( VB11R )+ABS1( VB12 ) ) ) THEN
               CALL ZLARTG( -DCMPLX( UA11R ), DCONJG( UA12 ), CSQ, SNQ,
     $                      R )
            ELSE
               CALL ZLARTG( -DCMPLX( VB11R ), DCONJG( VB12 ), CSQ, SNQ,
     $                      R )
            END IF
*
            CSU = CSL
            SNU = -D1*SNL
            CSV = CSR
            SNV = -D1*SNR
*
         ELSE
*
*           Compute the (2,1) and (2,2) elements of U**H *A and V**H *B,
*           and (2,2) element of |U|**H *|A| and |V|**H *|B|.
*
            UA21 = -DCONJG( D1 )*SNL*A1
            UA22 = -DCONJG( D1 )*SNL*A2 + CSL*A3
*
            VB21 = -DCONJG( D1 )*SNR*B1
            VB22 = -DCONJG( D1 )*SNR*B2 + CSR*B3
*
            AUA22 = ABS( SNL )*ABS1( A2 ) + ABS( CSL )*ABS( A3 )
            AVB22 = ABS( SNR )*ABS1( B2 ) + ABS( CSR )*ABS( B3 )
*
*           zero (2,2) elements of U**H *A and V**H *B, and then swap.
*
            IF( ( ABS1( UA21 )+ABS1( UA22 ) ).EQ.ZERO ) THEN
               CALL ZLARTG( -DCONJG( VB21 ), DCONJG( VB22 ), CSQ, SNQ,
     $                      R )
            ELSE IF( ( ABS1( VB21 )+ABS( VB22 ) ).EQ.ZERO ) THEN
               CALL ZLARTG( -DCONJG( UA21 ), DCONJG( UA22 ), CSQ, SNQ,
     $                      R )
            ELSE IF( AUA22 / ( ABS1( UA21 )+ABS1( UA22 ) ).LE.AVB22 /
     $               ( ABS1( VB21 )+ABS1( VB22 ) ) ) THEN
               CALL ZLARTG( -DCONJG( UA21 ), DCONJG( UA22 ), CSQ, SNQ,
     $                      R )
            ELSE
               CALL ZLARTG( -DCONJG( VB21 ), DCONJG( VB22 ), CSQ, SNQ,
     $                      R )
            END IF
*
            CSU = SNL
            SNU = D1*CSL
            CSV = SNR
            SNV = D1*CSR
*
         END IF
*
      ELSE
*
*        Input matrices A and B are lower triangular matrices
*
*        Form matrix C = A*adj(B) = ( a 0 )
*                                   ( c d )
*
         A = A1*B3
         D = A3*B1
         C = A2*B3 - A3*B2
         FC = ABS( C )
*
*        Transform complex 2-by-2 matrix C to real matrix by unitary
*        diagonal matrix diag(d1,1).
*
         D1 = ONE
         IF( FC.NE.ZERO )
     $      D1 = C / FC
*
*        The SVD of real 2 by 2 triangular C
*
*         ( CSL -SNL )*( A 0 )*(  CSR  SNR ) = ( R 0 )
*         ( SNL  CSL ) ( C D ) ( -SNR  CSR )   ( 0 T )
*
         CALL DLASV2( A, FC, D, S1, S2, SNR, CSR, SNL, CSL )
*
         IF( ABS( CSR ).GE.ABS( SNR ) .OR. ABS( CSL ).GE.ABS( SNL ) )
     $        THEN
*
*           Compute the (2,1) and (2,2) elements of U**H *A and V**H *B,
*           and (2,1) element of |U|**H *|A| and |V|**H *|B|.
*
            UA21 = -D1*SNR*A1 + CSR*A2
            UA22R = CSR*A3
*
            VB21 = -D1*SNL*B1 + CSL*B2
            VB22R = CSL*B3
*
            AUA21 = ABS( SNR )*ABS( A1 ) + ABS( CSR )*ABS1( A2 )
            AVB21 = ABS( SNL )*ABS( B1 ) + ABS( CSL )*ABS1( B2 )
*
*           zero (2,1) elements of U**H *A and V**H *B.
*
            IF( ( ABS1( UA21 )+ABS( UA22R ) ).EQ.ZERO ) THEN
               CALL ZLARTG( DCMPLX( VB22R ), VB21, CSQ, SNQ, R )
            ELSE IF( ( ABS1( VB21 )+ABS( VB22R ) ).EQ.ZERO ) THEN
               CALL ZLARTG( DCMPLX( UA22R ), UA21, CSQ, SNQ, R )
            ELSE IF( AUA21 / ( ABS1( UA21 )+ABS( UA22R ) ).LE.AVB21 /
     $               ( ABS1( VB21 )+ABS( VB22R ) ) ) THEN
               CALL ZLARTG( DCMPLX( UA22R ), UA21, CSQ, SNQ, R )
            ELSE
               CALL ZLARTG( DCMPLX( VB22R ), VB21, CSQ, SNQ, R )
            END IF
*
            CSU = CSR
            SNU = -DCONJG( D1 )*SNR
            CSV = CSL
            SNV = -DCONJG( D1 )*SNL
*
         ELSE
*
*           Compute the (1,1) and (1,2) elements of U**H *A and V**H *B,
*           and (1,1) element of |U|**H *|A| and |V|**H *|B|.
*
            UA11 = CSR*A1 + DCONJG( D1 )*SNR*A2
            UA12 = DCONJG( D1 )*SNR*A3
*
            VB11 = CSL*B1 + DCONJG( D1 )*SNL*B2
            VB12 = DCONJG( D1 )*SNL*B3
*
            AUA11 = ABS( CSR )*ABS( A1 ) + ABS( SNR )*ABS1( A2 )
            AVB11 = ABS( CSL )*ABS( B1 ) + ABS( SNL )*ABS1( B2 )
*
*           zero (1,1) elements of U**H *A and V**H *B, and then swap.
*
            IF( ( ABS1( UA11 )+ABS1( UA12 ) ).EQ.ZERO ) THEN
               CALL ZLARTG( VB12, VB11, CSQ, SNQ, R )
            ELSE IF( ( ABS1( VB11 )+ABS1( VB12 ) ).EQ.ZERO ) THEN
               CALL ZLARTG( UA12, UA11, CSQ, SNQ, R )
            ELSE IF( AUA11 / ( ABS1( UA11 )+ABS1( UA12 ) ).LE.AVB11 /
     $               ( ABS1( VB11 )+ABS1( VB12 ) ) ) THEN
               CALL ZLARTG( UA12, UA11, CSQ, SNQ, R )
            ELSE
               CALL ZLARTG( VB12, VB11, CSQ, SNQ, R )
            END IF
*
            CSU = SNR
            SNU = DCONJG( D1 )*CSR
            CSV = SNL
            SNV = DCONJG( D1 )*CSL
*
         END IF
*
      END IF
*
      RETURN
*
*     End of ZLAGS2
*
      END
