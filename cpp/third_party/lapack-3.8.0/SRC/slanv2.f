*> \brief \b SLANV2 computes the Schur factorization of a real 2-by-2 nonsymmetric matrix in standard form.
*
*  =========== DOCUMENTATION ===========
*
* Online html documentation available at
*            http://www.netlib.org/lapack/explore-html/
*
*> \htmlonly
*> Download SLANV2 + dependencies
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.tgz?format=tgz&filename=/lapack/lapack_routine/slanv2.f">
*> [TGZ]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.zip?format=zip&filename=/lapack/lapack_routine/slanv2.f">
*> [ZIP]</a>
*> <a href="http://www.netlib.org/cgi-bin/netlibfiles.txt?format=txt&filename=/lapack/lapack_routine/slanv2.f">
*> [TXT]</a>
*> \endhtmlonly
*
*  Definition:
*  ===========
*
*       SUBROUTINE SLANV2( A, B, C, D, RT1R, RT1I, RT2R, RT2I, CS, SN )
*
*       .. Scalar Arguments ..
*       REAL               A, B, C, CS, D, RT1I, RT1R, RT2I, RT2R, SN
*       ..
*
*
*> \par Purpose:
*  =============
*>
*> \verbatim
*>
*> SLANV2 computes the Schur factorization of a real 2-by-2 nonsymmetric
*> matrix in standard form:
*>
*>      [ A  B ] = [ CS -SN ] [ AA  BB ] [ CS  SN ]
*>      [ C  D ]   [ SN  CS ] [ CC  DD ] [-SN  CS ]
*>
*> where either
*> 1) CC = 0 so that AA and DD are real eigenvalues of the matrix, or
*> 2) AA = DD and BB*CC < 0, so that AA + or - sqrt(BB*CC) are complex
*> conjugate eigenvalues.
*> \endverbatim
*
*  Arguments:
*  ==========
*
*> \param[in,out] A
*> \verbatim
*>          A is REAL
*> \endverbatim
*>
*> \param[in,out] B
*> \verbatim
*>          B is REAL
*> \endverbatim
*>
*> \param[in,out] C
*> \verbatim
*>          C is REAL
*> \endverbatim
*>
*> \param[in,out] D
*> \verbatim
*>          D is REAL
*>          On entry, the elements of the input matrix.
*>          On exit, they are overwritten by the elements of the
*>          standardised Schur form.
*> \endverbatim
*>
*> \param[out] RT1R
*> \verbatim
*>          RT1R is REAL
*> \endverbatim
*>
*> \param[out] RT1I
*> \verbatim
*>          RT1I is REAL
*> \endverbatim
*>
*> \param[out] RT2R
*> \verbatim
*>          RT2R is REAL
*> \endverbatim
*>
*> \param[out] RT2I
*> \verbatim
*>          RT2I is REAL
*>          The real and imaginary parts of the eigenvalues. If the
*>          eigenvalues are a complex conjugate pair, RT1I > 0.
*> \endverbatim
*>
*> \param[out] CS
*> \verbatim
*>          CS is REAL
*> \endverbatim
*>
*> \param[out] SN
*> \verbatim
*>          SN is REAL
*>          Parameters of the rotation matrix.
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
*> \par Further Details:
*  =====================
*>
*> \verbatim
*>
*>  Modified by V. Sima, Research Institute for Informatics, Bucharest,
*>  Romania, to reduce the risk of cancellation errors,
*>  when computing real eigenvalues, and to ensure, if possible, that
*>  abs(RT1R) >= abs(RT2R).
*> \endverbatim
*>
*  =====================================================================
      SUBROUTINE SLANV2( A, B, C, D, RT1R, RT1I, RT2R, RT2I, CS, SN )
*
*  -- LAPACK auxiliary routine (version 3.7.0) --
*  -- LAPACK is a software package provided by Univ. of Tennessee,    --
*  -- Univ. of California Berkeley, Univ. of Colorado Denver and NAG Ltd..--
*     December 2016
*
*     .. Scalar Arguments ..
      REAL               A, B, C, CS, D, RT1I, RT1R, RT2I, RT2R, SN
*     ..
*
*  =====================================================================
*
*     .. Parameters ..
      REAL               ZERO, HALF, ONE
      PARAMETER          ( ZERO = 0.0E+0, HALF = 0.5E+0, ONE = 1.0E+0 )
      REAL               MULTPL
      PARAMETER          ( MULTPL = 4.0E+0 )
*     ..
*     .. Local Scalars ..
      REAL               AA, BB, BCMAX, BCMIS, CC, CS1, DD, EPS, P, SAB,
     $                   SAC, SCALE, SIGMA, SN1, TAU, TEMP, Z
*     ..
*     .. External Functions ..
      REAL               SLAMCH, SLAPY2
      EXTERNAL           SLAMCH, SLAPY2
*     ..
*     .. Intrinsic Functions ..
      INTRINSIC          ABS, MAX, MIN, SIGN, SQRT
*     ..
*     .. Executable Statements ..
*
      EPS = SLAMCH( 'P' )
      IF( C.EQ.ZERO ) THEN
         CS = ONE
         SN = ZERO
         GO TO 10
*
      ELSE IF( B.EQ.ZERO ) THEN
*
*        Swap rows and columns
*
         CS = ZERO
         SN = ONE
         TEMP = D
         D = A
         A = TEMP
         B = -C
         C = ZERO
         GO TO 10
      ELSE IF( (A-D).EQ.ZERO .AND. SIGN( ONE, B ).NE.
     $   SIGN( ONE, C ) ) THEN
         CS = ONE
         SN = ZERO
         GO TO 10
      ELSE
*
         TEMP = A - D
         P = HALF*TEMP
         BCMAX = MAX( ABS( B ), ABS( C ) )
         BCMIS = MIN( ABS( B ), ABS( C ) )*SIGN( ONE, B )*SIGN( ONE, C )
         SCALE = MAX( ABS( P ), BCMAX )
         Z = ( P / SCALE )*P + ( BCMAX / SCALE )*BCMIS
*
*        If Z is of the order of the machine accuracy, postpone the
*        decision on the nature of eigenvalues
*
         IF( Z.GE.MULTPL*EPS ) THEN
*
*           Real eigenvalues. Compute A and D.
*
            Z = P + SIGN( SQRT( SCALE )*SQRT( Z ), P )
            A = D + Z
            D = D - ( BCMAX / Z )*BCMIS
*
*           Compute B and the rotation matrix
*
            TAU = SLAPY2( C, Z )
            CS = Z / TAU
            SN = C / TAU
            B = B - C
            C = ZERO
         ELSE
*
*           Complex eigenvalues, or real (almost) equal eigenvalues.
*           Make diagonal elements equal.
*
            SIGMA = B + C
            TAU = SLAPY2( SIGMA, TEMP )
            CS = SQRT( HALF*( ONE+ABS( SIGMA ) / TAU ) )
            SN = -( P / ( TAU*CS ) )*SIGN( ONE, SIGMA )
*
*           Compute [ AA  BB ] = [ A  B ] [ CS -SN ]
*                   [ CC  DD ]   [ C  D ] [ SN  CS ]
*
            AA = A*CS + B*SN
            BB = -A*SN + B*CS
            CC = C*CS + D*SN
            DD = -C*SN + D*CS
*
*           Compute [ A  B ] = [ CS  SN ] [ AA  BB ]
*                   [ C  D ]   [-SN  CS ] [ CC  DD ]
*
            A = AA*CS + CC*SN
            B = BB*CS + DD*SN
            C = -AA*SN + CC*CS
            D = -BB*SN + DD*CS
*
            TEMP = HALF*( A+D )
            A = TEMP
            D = TEMP
*
            IF( C.NE.ZERO ) THEN
               IF( B.NE.ZERO ) THEN
                  IF( SIGN( ONE, B ).EQ.SIGN( ONE, C ) ) THEN
*
*                    Real eigenvalues: reduce to upper triangular form
*
                     SAB = SQRT( ABS( B ) )
                     SAC = SQRT( ABS( C ) )
                     P = SIGN( SAB*SAC, C )
                     TAU = ONE / SQRT( ABS( B+C ) )
                     A = TEMP + P
                     D = TEMP - P
                     B = B - C
                     C = ZERO
                     CS1 = SAB*TAU
                     SN1 = SAC*TAU
                     TEMP = CS*CS1 - SN*SN1
                     SN = CS*SN1 + SN*CS1
                     CS = TEMP
                  END IF
               ELSE
                  B = -C
                  C = ZERO
                  TEMP = CS
                  CS = -SN
                  SN = TEMP
               END IF
            END IF
         END IF
*
      END IF
*
   10 CONTINUE
*
*     Store eigenvalues in (RT1R,RT1I) and (RT2R,RT2I).
*
      RT1R = A
      RT2R = D
      IF( C.EQ.ZERO ) THEN
         RT1I = ZERO
         RT2I = ZERO
      ELSE
         RT1I = SQRT( ABS( B ) )*SQRT( ABS( C ) )
         RT2I = -RT1I
      END IF
      RETURN
*
*     End of SLANV2
*
      END
