/****************************************Implementation**Details**********************************************/
/*                                                                                                           */
/*   Lets denote (a,a1i) complex which is mathematically a+a1*i                                              */
/*   Complex number multiplication: (a,a1i)*(b,b1i)                                                          */
/*   As i*i=-1 .The multiplication result will be:                                                           */
/*   (a+a1*i)(b+b1*i)=a*b+a1*i*b1*i+ a1*i*b+a*b1*i=a*b-a1*b1 + (a1*b+a*b1)*i which is (ab-a1b1,a1b+ab1)      */
/*   so let  c= ab-a1b1 , ci=a1b+ab1 then                                                                    */
/*   c=c+a*b-a1*b1    => c=a*b-( a1*b1-c)  => c= a1*b1-c then c=a*b-c two mseb                               */
/*   ci=ci+a1*b+a*b1  => ci= a1*b+ci then ci= a*b1+ci                                                        */
/*   For simd real and imaginary parts will be grouped together                                              */
/*   such (realA,realK) and (imageA ,imageK)                                                                 */
/*   Simd(0,1)=(a*b,k*b)-((ai*bi,ki*bi)-Simd(0,1))                                                           */
/*   SimdI(0,1)=SimdI(0,1)+(a*bi,k*bi)+(ai*b,ki*b)                                                           */
/*                                                                                                           */
/*                                                                                                           */
/*   for defined(NR) || defined(NC) || defined(TR) || defined(TC)                                            */
/*   (a+a1*I)(b-b1*I)=ab+a1*b1+I(a1b-ab1)                                                                    */
/*                                                                                                           */
/*   c=c+ab+a1b1  => c=a1b1+c;c=ab+c                                                                         */
/*   ci=ci+a1b-ab1 => ci=a1*b-(ab1-ci) => ci=ab1-ci; ci=a1*b-ci                                              */
/*                                                                                                           */
/*                                                                                                           */
/*   for  defined(RN) || defined(RT) || defined(CN) || defined(CT)                                           */
/*   (a-a1*I)(b+b1*I)=ab+a1*b1+I(-a1b+ab1)                                                                   */
/*                                                                                                           */
/*   c=c+ab+a1b1  => c=a1b1+c;c=ab+c                                                                         */
/*   ci=ci+a1b-ab1 => ci=a*b1-(a1b-ci) => ci=a1b-ci; ci=a*b1-ci                                              */
/*                                                                                                           */
/*                                                                                                           */
/*   for defined(RR) || defined(RC) || defined(CR) || defined(CC)                                            */
/*   (a-a1*I)(b-b1*I)=ab-a1*b1+I(-a1b-ab1)                                                                   */
/*                                                                                                           */
/*   c= a1*b1-c then c=a*b-c                                                                                 */
/*   ci = ci-a1*b -a*b1;                                                                                     */
/*   as ibm z13 only has x*z-m x*z+m  instructions implementation will  be changed a bit                     */
/*   Assuming  ci=0; and cix=cix+a1b+ab1 ;   ci=ci-cix will work                                             */
/*   cix= a*b1+cix ; cix= a1*b+cix  (two madb) ci=ci-cix (sign change if ci=0)                               */
/*   As c=0   then                                                                                           */
/*   c=a*b-c then c=a1*b1-c => c=(a1*b1-(a*b-c))  which is -1*( a*b -(a1*b1-c))                              */
/*                                                                                                           */
/*   Values will be equal to (-c) and (-ci)                                                                  */
/*   To change sign it'll be multiplied by -1*(alpha+alpha_i)                                                */
/*   This is done once:                                                                                      */
/*   lcdbr ALPHA_I,ALPHA_I                                                                                   */
/*   lcdbr ALPHA ,ALPHA                                                                                      */
/*************************************************************************************************************/

/*************************Zero vectors***************************************/
/*zero vectors for 4x4 */
.macro ZERO_ZCVEC_4x4
    vzero  %v16
    vzero  %v17
    vzero  %v18
    vzero  %v19
    vzero  %v20
    vzero  %v21
    vzero  %v22
    vzero  %v23
    vzero  %v24
    vzero  %v25
    vzero  %v26
    vzero  %v27
    vzero  %v28
    vzero  %v29
    vzero  %v30
    vzero  %v31
.endm

/*zero vectors for */
.macro ZERO_ZCVEC_2x4
    vzero  %v16
    vzero  %v17
    vzero  %v18
    vzero  %v19
    vzero  %v20
    vzero  %v21
    vzero  %v22
    vzero  %v23
.endm

/*zero vectors for */
.macro ZERO_ZCVEC_1x4
    vzero  %v16
    vzero  %v17
    vzero  %v18
    vzero  %v19
.endm

/*zero vectors for */
.macro ZERO_ZCVEC_4x2
   ZERO_ZCVEC_2x4
.endm

.macro ZERO_ZCVEC_4x1
   ZERO_ZCVEC_1x4
.endm

/*zero vectors for */
.macro ZERO_ZCVEC_2x2
    vzero  %v16
    vzero  %v17
    vzero  %v20
    vzero  %v21
.endm

/*zero vectors for */
.macro ZERO_ZCVEC_1x2
    vzero  %v16
    vzero  %v17
.endm

/*zero vectors for */
.macro ZERO_ZCVEC_2x1
    vzero  %v16
    vzero  %v17
.endm

/*zero vectors for 1x1*/
.macro ZERO_ZCVEC_1x1
    lzdr %f6
    lzdr %f7
.endm


/*
  Calculate for 4x2 inner
*/
.macro CalcComplex_4x2 vResR1, vResI1, vResR2, vResI2, vResR3, vResI3, vResR4, vResI4, vr1, vi1, vr2, vi2, vrB, viB,vrB2, viB2

  #if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
    vfmsdb \vResR1, \vi1, \viB, \vResR1
    vfmadb \vResI1, \vr1, \viB, \vResI1
    vfmsdb \vResR2, \vi2, \viB, \vResR2
    vfmadb \vResI2, \vr2, \viB, \vResI2

    vfmsdb \vResR3, \vi1, \viB2, \vResR3
    vfmadb \vResI3, \vr1, \viB2, \vResI3
    vfmsdb \vResR4, \vi2, \viB2, \vResR4
    vfmadb \vResI4, \vr2, \viB2, \vResI4

    vfmsdb \vResR1, \vr1, \vrB, \vResR1
    vfmadb \vResI1, \vi1, \vrB, \vResI1
    vfmsdb \vResR2, \vr2, \vrB, \vResR2
    vfmadb \vResI2, \vi2, \vrB, \vResI2

    vfmsdb \vResR3, \vr1, \vrB2, \vResR3
    vfmadb \vResI3, \vi1, \vrB2, \vResI3
    vfmsdb \vResR4, \vr2, \vrB2, \vResR4
    vfmadb \vResI4, \vi2, \vrB2, \vResI4

  #endif

  #if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
    vfmadb \vResR1, \vi1, \viB, \vResR1
    vfmsdb \vResI1, \vr1, \viB, \vResI1
    vfmadb \vResR2, \vi2, \viB, \vResR2
    vfmsdb \vResI2, \vr2, \viB, \vResI2

    vfmadb \vResR3, \vi1, \viB2, \vResR3
    vfmsdb \vResI3, \vr1, \viB2, \vResI3
    vfmadb \vResR4, \vi2, \viB2, \vResR4
    vfmsdb \vResI4, \vr2, \viB2, \vResI4

    vfmadb \vResR1, \vr1, \vrB, \vResR1
    vfmsdb \vResI1, \vi1, \vrB, \vResI1
    vfmadb \vResR2, \vr2, \vrB, \vResR2
    vfmsdb \vResI2, \vi2, \vrB, \vResI2

    vfmadb \vResR3, \vr1, \vrB2, \vResR3
    vfmsdb \vResI3, \vi1, \vrB2, \vResI3
    vfmadb \vResR4, \vr2, \vrB2, \vResR4
    vfmsdb \vResI4, \vi2, \vrB2, \vResI4

  #endif

  #if   defined(RN) || defined(RT) || defined(CN) || defined(CT)
    vfmadb \vResR1, \vi1, \viB, \vResR1
    vfmsdb \vResI1, \vi1, \vrB, \vResI1
    vfmadb \vResR2, \vi2, \viB, \vResR2
    vfmsdb \vResI2, \vi2, \vrB, \vResI2

    vfmadb \vResR3, \vi1, \viB2, \vResR3
    vfmsdb \vResI3, \vi1, \vrB2, \vResI3
    vfmadb \vResR4, \vi2, \viB2, \vResR4
    vfmsdb \vResI4, \vi2, \vrB2, \vResI4

    vfmadb \vResR1, \vr1, \vrB, \vResR1
    vfmsdb \vResI1, \vr1, \viB, \vResI1
    vfmadb \vResR2, \vr2, \vrB, \vResR2
    vfmsdb \vResI2, \vr2, \viB, \vResI2

    vfmadb \vResR3, \vr1, \vrB2, \vResR3
    vfmsdb \vResI3, \vr1, \viB2, \vResI3
    vfmadb \vResR4, \vr2, \vrB2, \vResR4
    vfmsdb \vResI4, \vr2, \viB2, \vResI4
  #endif
  #if   defined(RR) || defined(RC) || defined(CR) || defined(CC)

    vfmsdb \vResR1, \vr1, \vrB, \vResR1
    vfmadb \vResI1, \vi1, \vrB, \vResI1
    vfmsdb \vResR2, \vr2, \vrB, \vResR2
    vfmadb \vResI2, \vi2, \vrB, \vResI2

    vfmsdb \vResR3, \vr1, \vrB2, \vResR3
    vfmadb \vResI3, \vi1, \vrB2, \vResI3
    vfmsdb \vResR4, \vr2, \vrB2, \vResR4
    vfmadb \vResI4, \vi2, \vrB2, \vResI4

    vfmsdb \vResR1, \vi1, \viB, \vResR1
    vfmadb \vResI1, \vr1, \viB, \vResI1
    vfmsdb \vResR2, \vi2, \viB, \vResR2
    vfmadb \vResI2, \vr2, \viB, \vResI2

    vfmsdb \vResR3, \vi1, \viB2, \vResR3
    vfmadb \vResI3, \vr1, \viB2, \vResI3
    vfmsdb \vResR4, \vi2, \viB2, \vResR4
    vfmadb \vResI4, \vr2, \viB2, \vResI4


  #endif

.endm

/*
  Calculate for 2x4 inner
*/
.macro CalcComplex_2x4 vResR1, vResI1, vResR2, vResI2, vResR3, vResI3, vResR4, vResI4, vr1, vi1, vr2, vi2, vrB, viB,vrB2, viB2

  #if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
    vfmsdb \vResR1, \vi1, \viB, \vResR1
    vfmadb \vResI1, \vr1, \viB, \vResI1
    vfmsdb \vResR2, \vi2, \viB, \vResR2
    vfmadb \vResI2, \vr2, \viB, \vResI2

    vfmsdb \vResR3, \vi1, \viB2, \vResR3
    vfmadb \vResI3, \vr1, \viB2, \vResI3
    vfmsdb \vResR4, \vi2, \viB2, \vResR4
    vfmadb \vResI4, \vr2, \viB2, \vResI4

    vfmsdb \vResR1, \vr1, \vrB, \vResR1
    vfmadb \vResI1, \vi1, \vrB, \vResI1
    vfmsdb \vResR2, \vr2, \vrB, \vResR2
    vfmadb \vResI2, \vi2, \vrB, \vResI2

    vfmsdb \vResR3, \vr1, \vrB2, \vResR3
    vfmadb \vResI3, \vi1, \vrB2, \vResI3
    vfmsdb \vResR4, \vr2, \vrB2, \vResR4
    vfmadb \vResI4, \vi2, \vrB2, \vResI4

  #endif

  #if   defined(RN) || defined(RT) || defined(CN) || defined(CT)
    vfmadb \vResR1, \vi1, \viB, \vResR1
    vfmsdb \vResI1, \vr1, \viB, \vResI1
    vfmadb \vResR2, \vi2, \viB, \vResR2
    vfmsdb \vResI2, \vr2, \viB, \vResI2

    vfmadb \vResR3, \vi1, \viB2, \vResR3
    vfmsdb \vResI3, \vr1, \viB2, \vResI3
    vfmadb \vResR4, \vi2, \viB2, \vResR4
    vfmsdb \vResI4, \vr2, \viB2, \vResI4

    vfmadb \vResR1, \vr1, \vrB, \vResR1
    vfmsdb \vResI1, \vi1, \vrB, \vResI1
    vfmadb \vResR2, \vr2, \vrB, \vResR2
    vfmsdb \vResI2, \vi2, \vrB, \vResI2

    vfmadb \vResR3, \vr1, \vrB2, \vResR3
    vfmsdb \vResI3, \vi1, \vrB2, \vResI3
    vfmadb \vResR4, \vr2, \vrB2, \vResR4
    vfmsdb \vResI4, \vi2, \vrB2, \vResI4

  #endif

  #if defined(NR) || defined(NC) || defined(TR) || defined(TC)
    vfmadb \vResR1, \vi1, \viB, \vResR1
    vfmsdb \vResI1, \vi1, \vrB, \vResI1
    vfmadb \vResR2, \vi2, \viB, \vResR2
    vfmsdb \vResI2, \vi2, \vrB, \vResI2

    vfmadb \vResR3, \vi1, \viB2, \vResR3
    vfmsdb \vResI3, \vi1, \vrB2, \vResI3
    vfmadb \vResR4, \vi2, \viB2, \vResR4
    vfmsdb \vResI4, \vi2, \vrB2, \vResI4

    vfmadb \vResR1, \vr1, \vrB, \vResR1
    vfmsdb \vResI1, \vr1, \viB, \vResI1
    vfmadb \vResR2, \vr2, \vrB, \vResR2
    vfmsdb \vResI2, \vr2, \viB, \vResI2

    vfmadb \vResR3, \vr1, \vrB2, \vResR3
    vfmsdb \vResI3, \vr1, \viB2, \vResI3
    vfmadb \vResR4, \vr2, \vrB2, \vResR4
    vfmsdb \vResI4, \vr2, \viB2, \vResI4
  #endif
  #if   defined(RR) || defined(RC) || defined(CR) || defined(CC)

    vfmsdb \vResR1, \vr1, \vrB, \vResR1
    vfmadb \vResI1, \vi1, \vrB, \vResI1
    vfmsdb \vResR2, \vr2, \vrB, \vResR2
    vfmadb \vResI2, \vi2, \vrB, \vResI2

    vfmsdb \vResR3, \vr1, \vrB2, \vResR3
    vfmadb \vResI3, \vi1, \vrB2, \vResI3
    vfmsdb \vResR4, \vr2, \vrB2, \vResR4
    vfmadb \vResI4, \vi2, \vrB2, \vResI4

    vfmsdb \vResR1, \vi1, \viB, \vResR1
    vfmadb \vResI1, \vr1, \viB, \vResI1
    vfmsdb \vResR2, \vi2, \viB, \vResR2
    vfmadb \vResI2, \vr2, \viB, \vResI2

    vfmsdb \vResR3, \vi1, \viB2, \vResR3
    vfmadb \vResI3, \vr1, \viB2, \vResI3
    vfmsdb \vResR4, \vi2, \viB2, \vResR4
    vfmadb \vResI4, \vr2, \viB2, \vResI4


  #endif

.endm

/*
  Calculate for 2x2 inner
*/
.macro CalcComplex_2x2 vResR1, vResI1,vResR2, vResI2, vR1, vI1, vRB, vIB,  vRB2, vIB2
  #if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
    vfmsdb \vResR1, \vI1, \vIB, \vResR1
    vfmadb \vResI1, \vR1, \vIB, \vResI1

    vfmsdb \vResR2, \vI1, \vIB2, \vResR2
    vfmadb \vResI2, \vR1, \vIB2, \vResI2

    vfmsdb \vResR1, \vR1, \vRB, \vResR1
    vfmadb \vResI1, \vI1, \vRB, \vResI1

    vfmsdb \vResR2, \vR1, \vRB2, \vResR2
    vfmadb \vResI2, \vI1, \vRB2, \vResI2
  #endif

  #if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
    vfmadb \vResR1, \vI1, \vIB, \vResR1
    vfmsdb \vResI1, \vR1, \vIB, \vResI1

    vfmadb \vResR2, \vI1, \vIB2, \vResR2
    vfmsdb \vResI2, \vR1, \vIB2, \vResI2

    vfmadb \vResR1, \vR1, \vRB, \vResR1
    vfmsdb \vResI1, \vI1, \vRB, \vResI1

    vfmadb \vResR2, \vR1, \vRB2, \vResR2
    vfmsdb \vResI2, \vI1, \vRB2, \vResI2
  #endif

  #if   defined(RN) || defined(RT) || defined(CN) || defined(CT)
    vfmadb \vResR1, \vI1, \vIB, \vResR1
    vfmsdb \vResI1, \vI1, \vRB, \vResI1

    vfmadb \vResR2, \vI1, \vIB2, \vResR2
    vfmsdb \vResI2, \vI1, \vRB2, \vResI2

    vfmadb \vResR1, \vR1, \vRB, \vResR1
    vfmsdb \vResI1, \vR1, \vIB, \vResI1

    vfmadb \vResR2, \vR1, \vRB2, \vResR2
    vfmsdb \vResI2, \vR1, \vIB2, \vResI2
  #endif
  #if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
    vfmsdb \vResR1, \vR1, \vRB, \vResR1
    vfmadb \vResI1, \vI1, \vRB, \vResI1

    vfmsdb \vResR2, \vR1, \vRB2, \vResR2
    vfmadb \vResI2, \vI1, \vRB2, \vResI2

    vfmsdb \vResR1, \vI1, \vIB, \vResR1
    vfmadb \vResI1, \vR1, \vIB, \vResI1

    vfmsdb \vResR2, \vI1, \vIB2, \vResR2
    vfmadb \vResI2, \vR1, \vIB2, \vResI2
  #endif
.endm

/*
  Calculate for 2x1 inner
*/
.macro CalcComplex_2x1 vRealResult1, vImageResult1, vReal1, vImage1, vecRealB, vecImageB
  #if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
    vfmsdb \vRealResult1, \vImage1, \vecImageB, \vRealResult1
    vfmadb \vImageResult1, \vReal1, \vecImageB, \vImageResult1
    vfmsdb \vRealResult1, \vReal1, \vecRealB, \vRealResult1
    vfmadb \vImageResult1, \vImage1, \vecRealB, \vImageResult1
  #endif

  #if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
    vfmadb \vRealResult1, \vImage1, \vecImageB, \vRealResult1
    vfmsdb \vImageResult1, \vReal1, \vecImageB, \vImageResult1
    vfmadb \vRealResult1, \vReal1, \vecRealB, \vRealResult1
    vfmsdb \vImageResult1, \vImage1, \vecRealB, \vImageResult1
  #endif

  #if   defined(RN) || defined(RT) || defined(CN) || defined(CT)
    vfmadb \vRealResult1, \vImage1, \vecImageB, \vRealResult1
    vfmsdb \vImageResult1, \vImage1, \vecRealB, \vImageResult1
    vfmadb \vRealResult1, \vReal1, \vecRealB, \vRealResult1
    vfmsdb \vImageResult1, \vReal1, \vecImageB, \vImageResult1
  #endif
  #if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
    vfmsdb \vRealResult1, \vReal1, \vecRealB, \vRealResult1
    vfmadb \vImageResult1, \vImage1, \vecRealB, \vImageResult1
    vfmsdb \vRealResult1, \vImage1, \vecImageB, \vRealResult1
    vfmadb \vImageResult1, \vReal1, \vecImageB, \vImageResult1
  #endif
.endm

/*
  Calculate for 1x2 inner
*/
.macro CalcComplex_1x2 vRealResult1, vImageResult1, vReal1, vImage1, vecRealB, vecImageB
  #if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
    vfmsdb \vRealResult1, \vImage1, \vecImageB, \vRealResult1
    vfmadb \vImageResult1, \vReal1, \vecImageB, \vImageResult1
    vfmsdb \vRealResult1, \vReal1, \vecRealB, \vRealResult1
    vfmadb \vImageResult1, \vImage1, \vecRealB, \vImageResult1
  #endif

  #if   defined(RN) || defined(CN) || defined(RT) || defined(CT)
    vfmadb \vRealResult1, \vImage1, \vecImageB, \vRealResult1
    vfmsdb \vImageResult1, \vReal1, \vecImageB, \vImageResult1
    vfmadb \vRealResult1, \vReal1, \vecRealB, \vRealResult1
    vfmsdb \vImageResult1, \vImage1, \vecRealB, \vImageResult1
  #endif

  #if   defined(NR) || defined(TR) || defined(NC) || defined(TC)
    vfmadb \vRealResult1, \vImage1, \vecImageB, \vRealResult1
    vfmsdb \vImageResult1, \vImage1, \vecRealB, \vImageResult1
    vfmadb \vRealResult1, \vReal1, \vecRealB, \vRealResult1
    vfmsdb \vImageResult1, \vReal1, \vecImageB, \vImageResult1
  #endif
  #if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
    vfmsdb \vRealResult1, \vReal1, \vecRealB, \vRealResult1
    vfmadb \vImageResult1, \vImage1, \vecRealB, \vImageResult1
    vfmsdb \vRealResult1, \vImage1, \vecImageB, \vRealResult1
    vfmadb \vImageResult1, \vReal1, \vecImageB, \vImageResult1
  #endif
.endm


/*
  Calculate for 4x1 inner
*/
.macro CalcComplex_4x1 vRealResult1, vImageResult1, vRealResult2, vImageResult2, vReal1, vImage1, vReal2, vImage2, vecRealB, vecImageB
  #if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
    vfmsdb \vRealResult1, \vImage1, \vecImageB, \vRealResult1
    vfmadb \vImageResult1, \vReal1, \vecImageB, \vImageResult1
    vfmsdb \vRealResult2, \vImage2, \vecImageB, \vRealResult2
    vfmadb \vImageResult2, \vReal2, \vecImageB, \vImageResult2
    vfmsdb \vRealResult1, \vReal1, \vecRealB, \vRealResult1
    vfmadb \vImageResult1, \vImage1, \vecRealB, \vImageResult1
    vfmsdb \vRealResult2, \vReal2, \vecRealB, \vRealResult2
    vfmadb \vImageResult2, \vImage2, \vecRealB, \vImageResult2
  #endif

  #if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
    vfmadb \vRealResult1, \vImage1, \vecImageB, \vRealResult1
    vfmsdb \vImageResult1, \vReal1, \vecImageB, \vImageResult1
    vfmadb \vRealResult2, \vImage2, \vecImageB, \vRealResult2
    vfmsdb \vImageResult2, \vReal2, \vecImageB, \vImageResult2
    vfmadb \vRealResult1, \vReal1, \vecRealB, \vRealResult1
    vfmsdb \vImageResult1, \vImage1, \vecRealB, \vImageResult1
    vfmadb \vRealResult2, \vReal2, \vecRealB, \vRealResult2
    vfmsdb \vImageResult2, \vImage2, \vecRealB, \vImageResult2
  #endif

  #if   defined(RN) || defined(RT) || defined(CN) || defined(CT)
    vfmadb \vRealResult1, \vImage1, \vecImageB, \vRealResult1
    vfmsdb \vImageResult1, \vImage1, \vecRealB, \vImageResult1
    vfmadb \vRealResult2, \vImage2, \vecImageB, \vRealResult2
    vfmsdb \vImageResult2, \vImage2, \vecRealB, \vImageResult2
    vfmadb \vRealResult1, \vReal1, \vecRealB, \vRealResult1
    vfmsdb \vImageResult1, \vReal1, \vecImageB, \vImageResult1
    vfmadb \vRealResult2, \vReal2, \vecRealB, \vRealResult2
    vfmsdb \vImageResult2, \vReal2, \vecImageB, \vImageResult2
  #endif
  #if   defined(RR) || defined(RC) || defined(CR) || defined(CC)

    vfmsdb \vRealResult1, \vReal1, \vecRealB, \vRealResult1
    vfmadb \vImageResult1, \vImage1, \vecRealB, \vImageResult1
    vfmsdb \vRealResult2, \vReal2, \vecRealB, \vRealResult2
    vfmadb \vImageResult2, \vImage2, \vecRealB, \vImageResult2
    vfmsdb \vRealResult1, \vImage1, \vecImageB, \vRealResult1
    vfmadb \vImageResult1, \vReal1, \vecImageB, \vImageResult1
    vfmsdb \vRealResult2, \vImage2, \vecImageB, \vRealResult2
    vfmadb \vImageResult2, \vReal2, \vecImageB, \vImageResult2
  #endif

.endm

/*
  Calculate for 1x4 inner
*/
.macro CalcComplex_1x4 vRealResult1, vImageResult1, vRealResult2, vImageResult2, vReal1, vImage1, vReal2, vImage2, vecRealB, vecImageB
  #if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
    vfmsdb \vRealResult1, \vImage1, \vecImageB, \vRealResult1
    vfmadb \vImageResult1, \vReal1, \vecImageB, \vImageResult1
    vfmsdb \vRealResult2, \vImage2, \vecImageB, \vRealResult2
    vfmadb \vImageResult2, \vReal2, \vecImageB, \vImageResult2
    vfmsdb \vRealResult1, \vReal1, \vecRealB, \vRealResult1
    vfmadb \vImageResult1, \vImage1, \vecRealB, \vImageResult1
    vfmsdb \vRealResult2, \vReal2, \vecRealB, \vRealResult2
    vfmadb \vImageResult2, \vImage2, \vecRealB, \vImageResult2
  #endif

  #if   defined(RN) || defined(CN) || defined(RT) || defined(CT)
    vfmadb \vRealResult1, \vImage1, \vecImageB, \vRealResult1
    vfmsdb \vImageResult1, \vReal1, \vecImageB, \vImageResult1
    vfmadb \vRealResult2, \vImage2, \vecImageB, \vRealResult2
    vfmsdb \vImageResult2, \vReal2, \vecImageB, \vImageResult2
    vfmadb \vRealResult1, \vReal1, \vecRealB, \vRealResult1
    vfmsdb \vImageResult1, \vImage1, \vecRealB, \vImageResult1
    vfmadb \vRealResult2, \vReal2, \vecRealB, \vRealResult2
    vfmsdb \vImageResult2, \vImage2, \vecRealB, \vImageResult2
  #endif

  #if   defined(NR) || defined(TR) || defined(NC) || defined(TC)
    vfmadb \vRealResult1, \vImage1, \vecImageB, \vRealResult1
    vfmsdb \vImageResult1, \vImage1, \vecRealB, \vImageResult1
    vfmadb \vRealResult2, \vImage2, \vecImageB, \vRealResult2
    vfmsdb \vImageResult2, \vImage2, \vecRealB, \vImageResult2
    vfmadb \vRealResult1, \vReal1, \vecRealB, \vRealResult1
    vfmsdb \vImageResult1, \vReal1, \vecImageB, \vImageResult1
    vfmadb \vRealResult2, \vReal2, \vecRealB, \vRealResult2
    vfmsdb \vImageResult2, \vReal2, \vecImageB, \vImageResult2
  #endif
  #if   defined(RR) || defined(RC) || defined(CR) || defined(CC)

    vfmsdb \vRealResult1, \vReal1, \vecRealB, \vRealResult1
    vfmadb \vImageResult1, \vImage1, \vecRealB, \vImageResult1
    vfmsdb \vRealResult2, \vReal2, \vecRealB, \vRealResult2
    vfmadb \vImageResult2, \vImage2, \vecRealB, \vImageResult2
    vfmsdb \vRealResult1, \vImage1, \vecImageB, \vRealResult1
    vfmadb \vImageResult1, \vReal1, \vecImageB, \vImageResult1
    vfmsdb \vRealResult2, \vImage2, \vecImageB, \vRealResult2
    vfmadb \vImageResult2, \vReal2, \vecImageB, \vImageResult2
  #endif

.endm

.macro CalcComplex_1x1 RealResult1, ImageResult1, Real1, Image1, RealB, ImageB
  #if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
    msdbr \RealResult1, \Image1, \ImageB
    madbr \ImageResult1, \Real1, \ImageB
    msdbr \RealResult1, \Real1, \RealB
    madbr \ImageResult1, \Image1, \RealB
  #endif

  #if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
    madbr \RealResult1, \Image1, \ImageB
    msdbr \ImageResult1, \Real1, \ImageB
    madbr \RealResult1, \Real1, \RealB
    msdbr \ImageResult1, \Image1, \RealB
  #endif

  #if   defined(RN) || defined(RT) || defined(CN) || defined(CT)
    madbr \RealResult1, \Image1, \ImageB
    msdbr \ImageResult1, \Image1, \RealB
    madbr \RealResult1, \Real1, \RealB
    msdbr \ImageResult1, \Real1, \ImageB
  #endif
  #if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
    msdbr \RealResult1, \Real1, \RealB
    madbr \ImageResult1, \Image1, \RealB
    msdbr \RealResult1, \Image1, \ImageB
    madbr \ImageResult1, \Real1, \ImageB
  #endif
.endm

#define DISP(ind,stride,disp) (ind*stride+disp)
#define DISP64(ind,disp) (ind*64+disp)
#define DISP32(ind,disp) (ind*32+disp)
#define DISP16(ind,disp) (ind*16+disp)
#define USE_VLM 1

.macro ZCALC_4x4_I   PTR_A_REG,PTR_B_REG,Index,IsLast
#if defined(USE_VLM)
    vlm %v4,%v7, DISP64(\Index ,0) (\PTR_A_REG)
#else
    vl %v4 , DISP64(\Index ,0) (\PTR_A_REG)
    vl %v5 , DISP64(\Index ,16)(\PTR_A_REG)
    vl %v6 , DISP64(\Index ,32)(\PTR_A_REG)
    vl %v7 , DISP64(\Index ,48)(\PTR_A_REG)
#endif

    vlrepg %v9,  DISP64(\Index ,0)(\PTR_B_REG)
    vlrepg %v10 , DISP64(\Index ,8)(\PTR_B_REG)
    vlrepg %v11,  DISP64(\Index ,16)(\PTR_B_REG)
    vlrepg %v12 , DISP64(\Index ,24)(\PTR_B_REG)

    vpdi %v1,%v4,%v5,0
    vpdi %v5,%v4,%v5,0b101
    vpdi %v3,%v6,%v7,0
    vpdi %v7,%v6,%v7,0b101

    CalcComplex_4x2 %v16,%v17,%v18,%v19,%v20,%v21,%v22,%v23,%v1,%v5,%v3,%v7,%v9,%v10,%v11,%v12

    vlrepg %v9,  DISP64(\Index ,32)(\PTR_B_REG)
    vlrepg %v10 , DISP64(\Index ,40)(\PTR_B_REG)
    vlrepg %v11,  DISP64(\Index ,48)(\PTR_B_REG)
    vlrepg %v12 , DISP64(\Index ,56)(\PTR_B_REG)
  .if \IsLast==1
    la \PTR_A_REG, DISP64(\Index ,64)(\PTR_A_REG)
  .endif
    CalcComplex_4x2 %v24,%v25,%v26,%v27,%v28,%v29,%v30,%v31,%v1,%v5,%v3,%v7,%v9,%v10,%v11,%v12

  .if \IsLast==1
    la \PTR_B_REG, DISP64(\Index ,64)(\PTR_B_REG)
  .endif
.endm

.macro ZCALC_4x2_I   PTR_A_REG,PTR_B_REG,Index,IsLast
#if defined(USE_VLM)
    vlm %v4,%v7, DISP64(\Index ,0) (\PTR_A_REG)
#else
    vl %v4 , DISP64(\Index ,0) (\PTR_A_REG)
    vl %v5 , DISP64(\Index ,16)(\PTR_A_REG)
    vl %v6 , DISP64(\Index ,32)(\PTR_A_REG)
    vl %v7 , DISP64(\Index ,48)(\PTR_A_REG)
#endif
    vlrepg %v9,  DISP32(\Index ,0)(\PTR_B_REG)
    vlrepg %v10 , DISP32(\Index ,8)(\PTR_B_REG)
    vlrepg %v11,  DISP32(\Index ,16)(\PTR_B_REG)
    vlrepg %v12 , DISP32(\Index ,24)(\PTR_B_REG)

    vpdi %v1,%v4,%v5,0
    vpdi %v5,%v4,%v5,0b101
    vpdi %v3,%v6,%v7,0
    vpdi %v7,%v6,%v7,0b101
  .if \IsLast==1
    la \PTR_A_REG, DISP64(\Index ,64)(\PTR_A_REG)
  .endif
    CalcComplex_4x2 %v16,%v17,%v18,%v19,%v20,%v21,%v22,%v23,%v1,%v5,%v3,%v7,%v9,%v10,%v11,%v12

  .if \IsLast==1
    la \PTR_B_REG, DISP32(\Index ,32)(\PTR_B_REG)
  .endif
.endm

.macro ZCALC_2x4_I    PTR_A_REG,PTR_B_REG,Index,IsLast
#if defined(USE_VLM)
    vlm %v4,%v7, DISP64(\Index ,0) (\PTR_B_REG)
#else
    vl %v4 , DISP64(\Index ,0) (\PTR_B_REG)
    vl %v5 , DISP64(\Index ,16)(\PTR_B_REG)
    vl %v6 , DISP64(\Index ,32)(\PTR_B_REG)
    vl %v7 , DISP64(\Index ,48)(\PTR_B_REG)
#endif
    vlrepg %v9,  DISP32(\Index ,0)(\PTR_A_REG)
    vlrepg %v10 , DISP32(\Index ,8)(\PTR_A_REG)
    vlrepg %v11,  DISP32(\Index ,16)(\PTR_A_REG)
    vlrepg %v12 , DISP32(\Index ,24)(\PTR_A_REG)

    vpdi %v1,%v4,%v5,0
    vpdi %v5,%v4,%v5,0b101
    vpdi %v3,%v6,%v7,0
    vpdi %v7,%v6,%v7,0b101
  .if \IsLast==1
    la \PTR_B_REG, DISP64(\Index ,64)(\PTR_B_REG)
  .endif
    CalcComplex_2x4 %v16,%v17,%v18,%v19,%v20,%v21,%v22,%v23,%v1,%v5,%v3,%v7,%v9,%v10,%v11,%v12

  .if \IsLast==1
    la \PTR_A_REG, DISP32(\Index ,32)(\PTR_A_REG)
  .endif
.endm

.macro ZCALC_4x1_I   PTR_A_REG,PTR_B_REG,Index,IsLast
#if defined(USE_VLM)
    vlm %v4,%v7, DISP64(\Index ,0) (\PTR_A_REG)
#else
    vl %v4 , DISP64(\Index ,0) (\PTR_A_REG)
    vl %v5 , DISP64(\Index ,16)(\PTR_A_REG)
    vl %v6 , DISP64(\Index ,32)(\PTR_A_REG)
    vl %v7 , DISP64(\Index ,48)(\PTR_A_REG)
#endif
    vlrepg %v9,  DISP16(\Index ,0)(\PTR_B_REG)
    vlrepg %v10 , DISP16(\Index ,8)(\PTR_B_REG)

    vpdi %v1,%v4,%v5,0
    vpdi %v11,%v4,%v5,0b101
    vpdi %v3,%v6,%v7,0
    vpdi %v12,%v6,%v7,0b101
  .if \IsLast==1
    la \PTR_A_REG, DISP64(\Index ,64)(\PTR_A_REG)
  .endif
    CalcComplex_4x1 %v16,%v17,%v18,%v19,%v1,%v11,%v3,%v12,%v9,%v10
  .if \IsLast==1
    la \PTR_B_REG, DISP16(\Index ,16)(\PTR_B_REG)
  .endif
.endm

.macro ZCALC_1x4_I    PTR_A_REG,PTR_B_REG,Index,IsLast
#if defined(USE_VLM)
    vlm %v4,%v7, DISP64(\Index ,0) (\PTR_B_REG)
#else
    vl %v4 , DISP64(\Index ,0) (\PTR_B_REG)
    vl %v5 , DISP64(\Index ,16)(\PTR_B_REG)
    vl %v6 , DISP64(\Index ,32)(\PTR_B_REG)
    vl %v7 , DISP64(\Index ,48)(\PTR_B_REG)
#endif
    vlrepg %v9,  DISP16(\Index ,0)(\PTR_A_REG)
    vlrepg %v10 , DISP16(\Index ,8)(\PTR_A_REG)

    vpdi %v1,%v4,%v5,0
    vpdi %v11,%v4,%v5,0b101
    vpdi %v3,%v6,%v7,0
    vpdi %v12,%v6,%v7,0b101
  .if \IsLast==1
    la \PTR_B_REG, DISP64(\Index ,64)(\PTR_B_REG)
  .endif
    CalcComplex_1x4 %v16,%v17,%v18,%v19,%v1,%v11,%v3,%v12,%v9,%v10
  .if \IsLast==1
    la \PTR_A_REG, DISP16(\Index ,16)(\PTR_A_REG)
  .endif
.endm

.macro ZCALC_2x2_I   PTR_A_REG,PTR_B_REG ,Index,IsLast
    vl %v1 , DISP32(\Index ,0)(\PTR_A_REG)
    vl %v3 , DISP32(\Index ,16)(\PTR_A_REG)
    vlrepg %v9,  DISP32(\Index ,0)(\PTR_B_REG)
    vlrepg %v10 , DISP32(\Index ,8)(\PTR_B_REG)
    vlrepg %v11,  DISP32(\Index ,16)(\PTR_B_REG)
    vlrepg %v12 , DISP32(\Index ,24)(\PTR_B_REG)
    vpdi %v5,%v1,%v3,0
    vpdi %v6,%v1,%v3,0b101

  .if \IsLast==1
    la \PTR_A_REG, DISP32(\Index ,32)(\PTR_A_REG)
  .endif
    CalcComplex_2x2 %v16,%v17,%v20,%v21,%v5,%v6, %v9,%v10,%v11,%v12
  .if \IsLast==1
    la \PTR_B_REG, DISP32(\Index ,32)(\PTR_B_REG)
  .endif
.endm

.macro ZCALC_2x1_I   PTR_A_REG,PTR_B_REG ,Index,IsLast
    vl %v1 , DISP32(\Index ,0)(\PTR_A_REG)
    vl %v3 , DISP32(\Index ,16)(\PTR_A_REG)
    vlrepg %v6,  DISP16(\Index ,0)(\PTR_B_REG)
    vlrepg %v7 , DISP16(\Index ,8)(\PTR_B_REG)
    vpdi %v4,%v1,%v3,0
    vpdi %v5,%v1,%v3,0b101

  .if \IsLast==1
    la \PTR_A_REG, DISP32(\Index ,32)(\PTR_A_REG)
  .endif
    CalcComplex_2x1 %v16,%v17,%v4,%v5,%v6,%v7
  .if \IsLast==1
    la \PTR_B_REG, DISP16(\Index ,16)(\PTR_B_REG)
  .endif
.endm

.macro ZCALC_1x2_I   PTR_A_REG,PTR_B_REG ,Index,IsLast
    vl %v1 , DISP32(\Index ,0)(\PTR_B_REG)
    vl %v3 , DISP32(\Index ,16)(\PTR_B_REG)
    vlrepg %v6,  DISP16(\Index ,0)(\PTR_A_REG)
    vlrepg %v7 , DISP16(\Index ,8)(\PTR_A_REG)
    vpdi %v4,%v1,%v3,0
    vpdi %v5,%v1,%v3,0b101

  .if \IsLast==1
    la \PTR_B_REG, DISP32(\Index ,32)(\PTR_B_REG)
  .endif
    CalcComplex_1x2 %v16,%v17,%v4,%v5,%v6,%v7
  .if \IsLast==1
    la \PTR_A_REG, DISP16(\Index ,16)(\PTR_A_REG)
  .endif
.endm

.macro ZCALC_1x1_I   PTR_A_REG,PTR_B_REG ,Index,IsLast
    ld %f1 , DISP16(\Index ,0)(\PTR_A_REG)
    ld %f3 , DISP16(\Index ,8)(\PTR_A_REG)
    ld %f4 , DISP16(\Index ,0)(\PTR_B_REG)
    ld %f5 , DISP16(\Index ,8)(\PTR_B_REG)
  .if \IsLast==1
    la \PTR_A_REG, DISP16(\Index ,16)(\PTR_A_REG)
  .endif
    CalcComplex_1x1 %f6,%f7,%f1,%f3,%f4,%f5
  .if \IsLast==1
    la \PTR_B_REG, DISP16(\Index ,16)(\PTR_B_REG)
  .endif
.endm

.macro ZCALC_4x4   PTR_A_REG,PTR_B_REG
    ZCALC_4x4_I \PTR_A_REG,\PTR_B_REG,0,1
.endm
.macro ZCALC_4x2   PTR_A_REG,PTR_B_REG
    ZCALC_4x2_I \PTR_A_REG,\PTR_B_REG,0,1
.endm
.macro ZCALC_4x1   PTR_A_REG,PTR_B_REG
    ZCALC_4x1_I \PTR_A_REG,\PTR_B_REG,0,1
.endm

.macro ZCALC_4x4_4   PTR_A_REG,PTR_B_REG
    ZCALC_4x4_I \PTR_A_REG,\PTR_B_REG,0,0
    ZCALC_4x4_I \PTR_A_REG,\PTR_B_REG,1,0
    ZCALC_4x4_I \PTR_A_REG,\PTR_B_REG,2,0
    ZCALC_4x4_I \PTR_A_REG,\PTR_B_REG,3,1
.endm
.macro ZCALC_4x2_4   PTR_A_REG,PTR_B_REG
    ZCALC_4x2_I \PTR_A_REG,\PTR_B_REG,0,0
    ZCALC_4x2_I \PTR_A_REG,\PTR_B_REG,1,0
    ZCALC_4x2_I \PTR_A_REG,\PTR_B_REG,2,0
    ZCALC_4x2_I \PTR_A_REG,\PTR_B_REG,3,1
.endm
.macro ZCALC_4x1_4   PTR_A_REG,PTR_B_REG
    ZCALC_4x1_I \PTR_A_REG,\PTR_B_REG,0,0
    ZCALC_4x1_I \PTR_A_REG,\PTR_B_REG,1,0
    ZCALC_4x1_I \PTR_A_REG,\PTR_B_REG,2,0
    ZCALC_4x1_I \PTR_A_REG,\PTR_B_REG,3,1
.endm

.macro ZCALC_2x4_4   PTR_A_REG,PTR_B_REG
    ZCALC_2x4_I \PTR_A_REG,\PTR_B_REG,0,0
    ZCALC_2x4_I \PTR_A_REG,\PTR_B_REG,1,0
    ZCALC_2x4_I \PTR_A_REG,\PTR_B_REG,2,0
    ZCALC_2x4_I \PTR_A_REG,\PTR_B_REG,3,1
.endm

.macro ZCALC_2x4    PTR_A_REG,PTR_B_REG
    ZCALC_2x4_I \PTR_A_REG,\PTR_B_REG,0,1
.endm

.macro ZCALC_1x4_4   PTR_A_REG,PTR_B_REG
    ZCALC_1x4_I \PTR_A_REG,\PTR_B_REG,0,0
    ZCALC_1x4_I \PTR_A_REG,\PTR_B_REG,1,0
    ZCALC_1x4_I \PTR_A_REG,\PTR_B_REG,2,0
    ZCALC_1x4_I \PTR_A_REG,\PTR_B_REG,3,1
.endm

.macro ZCALC_1x4    PTR_A_REG,PTR_B_REG
    ZCALC_1x4_I \PTR_A_REG,\PTR_B_REG,0,1
.endm
.macro ZCALC_2x2   PTR_A_REG,PTR_B_REG
    ZCALC_2x2_I \PTR_A_REG,\PTR_B_REG,0,1
.endm

.macro ZCALC_2x2_4    PTR_A_REG,PTR_B_REG
    ZCALC_2x2_I \PTR_A_REG,\PTR_B_REG,0,0
    ZCALC_2x2_I \PTR_A_REG,\PTR_B_REG,1,0
    ZCALC_2x2_I \PTR_A_REG,\PTR_B_REG,2,0
    ZCALC_2x2_I \PTR_A_REG,\PTR_B_REG,3,1
.endm

.macro ZCALC_2x1    PTR_A_REG,PTR_B_REG
   ZCALC_2x1_I \PTR_A_REG,\PTR_B_REG,0,1
.endm

.macro ZCALC_2x1_4    PTR_A_REG,PTR_B_REG
    ZCALC_2x1_I \PTR_A_REG,\PTR_B_REG,0,0
    ZCALC_2x1_I \PTR_A_REG,\PTR_B_REG,1,0
    ZCALC_2x1_I \PTR_A_REG,\PTR_B_REG,2,0
    ZCALC_2x1_I \PTR_A_REG,\PTR_B_REG,3,1
.endm


.macro ZCALC_1x2_4    PTR_A_REG,PTR_B_REG
    ZCALC_1x2_I \PTR_A_REG,\PTR_B_REG,0,0
    ZCALC_1x2_I \PTR_A_REG,\PTR_B_REG,1,0
    ZCALC_1x2_I \PTR_A_REG,\PTR_B_REG,2,0
    ZCALC_1x2_I \PTR_A_REG,\PTR_B_REG,3,1
.endm

.macro ZCALC_1x2    PTR_A_REG,PTR_B_REG
    ZCALC_1x2_I \PTR_A_REG,\PTR_B_REG,0,1
.endm

.macro ZCALC_1x1_4    PTR_A_REG,PTR_B_REG
    ZCALC_1x1_I \PTR_A_REG,\PTR_B_REG,0,0
    ZCALC_1x1_I \PTR_A_REG,\PTR_B_REG,1,0
    ZCALC_1x1_I \PTR_A_REG,\PTR_B_REG,2,0
    ZCALC_1x1_I \PTR_A_REG,\PTR_B_REG,3,1
.endm

.macro ZCALC_1x1    PTR_A_REG,PTR_B_REG
    ZCALC_1x1_I \PTR_A_REG,\PTR_B_REG,0,1
.endm



/*****************************STORE RESULTS************************************/
.macro CalcMultAlpha_4x1 vRealResult1, vImageResult1, vRealResult2, vImageResult2, vReal1, vImage1, vReal2, vImage2, vecRealB, vecImageB
  #if defined (TRMMKERNEL)
    vfmdb \vRealResult1, \vImage1, \vecImageB
    vfmdb \vImageResult1, \vReal1, \vecImageB
    vfmdb \vRealResult2, \vImage2, \vecImageB
    vfmdb \vImageResult2, \vReal2, \vecImageB
  #else
    vfmsdb \vRealResult1, \vImage1, \vecImageB, \vRealResult1
    vfmadb \vImageResult1, \vReal1, \vecImageB, \vImageResult1
    vfmsdb \vRealResult2, \vImage2, \vecImageB, \vRealResult2
    vfmadb \vImageResult2, \vReal2, \vecImageB, \vImageResult2
#endif
    vfmsdb \vRealResult1, \vReal1, \vecRealB, \vRealResult1
    vfmadb \vImageResult1, \vImage1, \vecRealB, \vImageResult1
    vfmsdb \vRealResult2, \vReal2, \vecRealB, \vRealResult2
    vfmadb \vImageResult2, \vImage2, \vecRealB, \vImageResult2

.endm

.macro CalcMultAlpha_2x1 vRealResult1, vImageResult1, vReal1, vImage1, vecRealB, vecImageB
  #if defined (TRMMKERNEL)
    vfmdb \vRealResult1, \vImage1, \vecImageB
    vfmdb \vImageResult1, \vReal1, \vecImageB
#else
    vfmsdb \vRealResult1, \vImage1, \vecImageB, \vRealResult1
    vfmadb \vImageResult1, \vReal1, \vecImageB, \vImageResult1
#endif
    vfmsdb \vRealResult1, \vReal1, \vecRealB, \vRealResult1
    vfmadb \vImageResult1, \vImage1, \vecRealB, \vImageResult1
.endm

.macro CalcMultAlpha_1x1    RealResult1, ImageResult1, Real1, Image1, RealB, ImageB

    msdbr \RealResult1, \Image1, \ImageB
    madbr \ImageResult1, \Real1, \ImageB
    msdbr \RealResult1, \Real1, \RealB
    madbr \ImageResult1, \Image1, \RealB
.endm

.macro ZSTORE_4x4  ALPHA_VECREG,ALPHA_VECI,CIJ_REG , LDC_BYTE_ORIGINAL ,LC1,LC2
  #if !defined(TRMMKERNEL)
   vl %v1 , 0(\CIJ_REG)
    vl %v4 , 16(\CIJ_REG)
    vpdi %v3,%v1,%v4,0
    vl %v7 , 32(\CIJ_REG)
    vpdi %v4,%v1,%v4,0b101
    vl %v6 ,  48 (\CIJ_REG)
    vpdi %v1,%v7,%v6,0
    vpdi %v6,%v7,%v6,0b101
#endif
    la \LC1,0(\LDC_BYTE_ORIGINAL, \LDC_BYTE_ORIGINAL)
    CalcMultAlpha_4x1 %v3,%v4,%v1,%v6,%v16,%v17,%v18,%v19,\ALPHA_VECREG,\ALPHA_VECI
    vpdi %v16, %v3 ,%v4,0
    la \LC2,0(\LC1,\LDC_BYTE_ORIGINAL )
    vpdi %v17, %v3,%v4,0b0101
    vst %v16,0(\CIJ_REG)
    vpdi %v18, %v1 ,%v6,0
    vst %v17,16(\CIJ_REG)
    vpdi %v19, %v1 ,%v6,0b0101
    vst %v18,32(\CIJ_REG)
    vst %v19,48(\CIJ_REG)
  #if !defined(TRMMKERNEL)
    vl %v1 , 0(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vl %v4 , 16(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vpdi %v3,%v1,%v4,0
    vl %v7 , 32(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vpdi %v4,%v1,%v4,0b101
    vl %v6 ,  48 (\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vpdi %v1,%v7,%v6,0
    vpdi %v6,%v7,%v6,0b101
#endif
    CalcMultAlpha_4x1 %v3,%v4,%v1,%v6,%v20,%v21,%v22,%v23,\ALPHA_VECREG,\ALPHA_VECI
    vpdi %v16, %v3 ,%v4,0
    vpdi %v17, %v3 ,%v4,0b0101
    vst %v16,0(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vpdi %v18, %v1 ,%v6,0
    vst %v17,16(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vpdi %v19, %v1 ,%v6,0b0101
    vst %v18,32(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vst %v19,48(\CIJ_REG,\LDC_BYTE_ORIGINAL)

#if !defined(TRMMKERNEL)
    vl %v1 , 0(\CIJ_REG,\LC1)
    vl %v4 , 16(\CIJ_REG,\LC1)
    vpdi %v3,%v1,%v4,0
    vl %v7 , 32(\CIJ_REG,\LC1)
    vpdi %v4,%v1,%v4,0b101
    vl %v6 ,  48 (\CIJ_REG,\LC1)
    vpdi %v1,%v7,%v6,0
    vpdi %v6,%v7,%v6,0b101
#endif
    CalcMultAlpha_4x1 %v3,%v4,%v1,%v6,%v24,%v25,%v26,%v27,\ALPHA_VECREG,\ALPHA_VECI
    vpdi %v16, %v3 ,%v4,0
    vpdi %v17, %v3 ,%v4,0b0101
    vst %v16,0(\CIJ_REG,\LC1)
    vpdi %v18, %v1 ,%v6,0
    vst %v17,16(\CIJ_REG,\LC1)
    vpdi %v19, %v1 ,%v6,0b0101
    vst %v18,32(\CIJ_REG,\LC1)
    vst %v19,48(\CIJ_REG,\LC1)

  #if !defined(TRMMKERNEL)
    vl %v1 , 0(\CIJ_REG,\LC2)
    vl %v4 , 16(\CIJ_REG,\LC2)
    vpdi %v3,%v1,%v4,0
    vl %v7 , 32(\CIJ_REG,\LC2)
    vpdi %v4,%v1,%v4,0b101
    vl %v6 ,  48 (\CIJ_REG,\LC2)
    vpdi %v1,%v7,%v6,0
    vpdi %v6,%v7,%v6,0b101
#endif
    CalcMultAlpha_4x1 %v3,%v4,%v1,%v6,%v28,%v29,%v30,%v31,\ALPHA_VECREG,\ALPHA_VECI
    vpdi %v16, %v3 ,%v4,0
    vpdi %v17, %v3 ,%v4,0b0101
    vst %v16,0(\CIJ_REG,\LC2)
    vpdi %v18, %v1 ,%v6,0
    vst %v17,16(\CIJ_REG,\LC2)
    vpdi %v19, %v1 ,%v6,0b0101
    vst %v18,32(\CIJ_REG,\LC2)
    vst %v19,48(\CIJ_REG,\LC2)
    la \CIJ_REG,64(\CIJ_REG)
.endm

.macro ZSTORE_4x2  ALPHA_VECREG,ALPHA_VECI,CIJ_REG , LDC_BYTE_ORIGINAL
  #if !defined(TRMMKERNEL)
    vl %v1 , 0(\CIJ_REG)
    vl %v4 , 16(\CIJ_REG)
    vpdi %v3,%v1,%v4,0
    vl %v7 , 32(\CIJ_REG)
    vpdi %v4,%v1,%v4,0b101
    vl %v6 ,  48 (\CIJ_REG)
    vpdi %v1,%v7,%v6,0
    vpdi %v6,%v7,%v6,0b101
#endif
    CalcMultAlpha_4x1 %v3,%v4,%v1,%v6,%v16,%v17,%v18,%v19,\ALPHA_VECREG,\ALPHA_VECI
    vpdi %v16, %v3 ,%v4,0
    vpdi %v17, %v3,%v4,0b0101
    vst %v16,0(\CIJ_REG)
    vpdi %v18, %v1 ,%v6,0
    vst %v17,16(\CIJ_REG)
    vpdi %v19, %v1 ,%v6,0b0101
    vst %v18,32(\CIJ_REG)
    vst %v19,48(\CIJ_REG)
  #if !defined(TRMMKERNEL)
    vl %v1 , 0(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vl %v4 , 16(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vpdi %v3,%v1,%v4,0
    vl %v7 , 32(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vpdi %v4,%v1,%v4,0b101
    vl %v6 ,  48 (\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vpdi %v1,%v7,%v6,0
    vpdi %v6,%v7,%v6,0b101
#endif
    CalcMultAlpha_4x1 %v3,%v4,%v1,%v6,%v20,%v21,%v22,%v23,\ALPHA_VECREG,\ALPHA_VECI
    vpdi %v20, %v3 ,%v4,0
    vpdi %v21, %v3 ,%v4,0b0101
    vst %v20,0(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vpdi %v22, %v1 ,%v6,0
    vst %v21,16(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vpdi %v23, %v1 ,%v6,0b0101
    vst %v22,32(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vst %v23,48(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    la \CIJ_REG,64(\CIJ_REG)
.endm
.macro ZSTORE_4x1  ALPHA_VECREG,ALPHA_VECI,CIJ_REG , LDC_BYTE_ORIGINAL
  #if !defined(TRMMKERNEL)
    vl %v1 , 0(\CIJ_REG)
    vl %v4 , 16(\CIJ_REG)
    vpdi %v3,%v1,%v4,0
    vl %v7 , 32(\CIJ_REG)
    vpdi %v4,%v1,%v4,0b101
    vl %v6 ,  48 (\CIJ_REG)
    vpdi %v1,%v7,%v6,0
    vpdi %v6,%v7,%v6,0b101
#endif
    CalcMultAlpha_4x1 %v3,%v4,%v1,%v6,%v16,%v17,%v18,%v19,\ALPHA_VECREG,\ALPHA_VECI
    vpdi %v16, %v3 ,%v4,0
    vpdi %v17, %v3,%v4,0b0101
    vst %v16,0(\CIJ_REG)
    vpdi %v18, %v1 ,%v6,0
    vst %v17,16(\CIJ_REG)
    vpdi %v19, %v1 ,%v6,0b0101
    vst %v18,32(\CIJ_REG)
    vst %v19,48(\CIJ_REG)
    la \CIJ_REG,64(\CIJ_REG)
.endm
.macro ZSTORE_1x4  ALPHA_VECREG,ALPHA_VECI,CIJ_REG , LDC_BYTE_ORIGINAL,LC1,LC2
  #if !defined(TRMMKERNEL)
    vl %v1 , 0(\CIJ_REG)
    la \LC1,0(\LDC_BYTE_ORIGINAL, \LDC_BYTE_ORIGINAL)
    vl %v4 , 0(\CIJ_REG,  \LDC_BYTE_ORIGINAL)
    vpdi %v3,%v1,%v4,0
    la \LC2,0(\LC1,\LDC_BYTE_ORIGINAL )
    vl %v7 , 0(\CIJ_REG,  \LC1)
    vpdi %v4,%v1,%v4,0b101
    vl %v6 ,  0 (\CIJ_REG,\LC2)
    vpdi %v1,%v7,%v6,0
    vpdi %v6,%v7,%v6,0b101
#else
    la \LC1,0(\LDC_BYTE_ORIGINAL, \LDC_BYTE_ORIGINAL)
#endif
    CalcMultAlpha_4x1 %v3,%v4,%v1,%v6,%v16,%v17,%v18,%v19,\ALPHA_VECREG,\ALPHA_VECI
#if defined(TRMMKERNEL)
    la \LC2,0(\LC1,\LDC_BYTE_ORIGINAL )
#endif
    vpdi %v16, %v3 ,%v4,0
    vpdi %v17, %v3,%v4,0b0101
    vst %v16,0(\CIJ_REG)
    vpdi %v18, %v1 ,%v6,0
    vst %v17,0(\CIJ_REG,  \LDC_BYTE_ORIGINAL)
    vpdi %v19, %v1 ,%v6,0b0101
    vst %v18,0(\CIJ_REG,   \LC1)
    vst %v19,0(\CIJ_REG,\LC2)
    la \CIJ_REG,16(\CIJ_REG)
.endm
.macro ZSTORE_2x4  ALPHA_VECREG,ALPHA_VECI,CIJ_REG , LDC_BYTE_ORIGINAL,LC1,LC2
  #if !defined(TRMMKERNEL)
    vl %v1 , 0(\CIJ_REG)
    vl %v26 , 16(\CIJ_REG)
    la \LC1,0(\LDC_BYTE_ORIGINAL, \LDC_BYTE_ORIGINAL)
    vl %v4 , 0(\CIJ_REG,  \LDC_BYTE_ORIGINAL)
    vl %v25 , 16(\CIJ_REG,  \LDC_BYTE_ORIGINAL)
    vpdi %v3,%v1,%v4,0
    vpdi %v24,%v26,%v25,0
    la \LC2,0(\LC1,\LDC_BYTE_ORIGINAL )
    vl %v7 , 0(\CIJ_REG,  \LC1)
    vl %v28 , 16(\CIJ_REG,  \LC1)
    vpdi %v4,%v1,%v4,0b101
    vpdi %v25,%v26,%v25,0b101
    vl %v6 ,  0 (\CIJ_REG,\LC2)
    vl %v27 ,  16 (\CIJ_REG,\LC2)
    vpdi %v1,%v7,%v6,0
    vpdi %v6,%v7,%v6,0b101
    vpdi %v26,%v28,%v27,0
    vpdi %v27,%v28,%v27,0b101
#else
   la \LC1,0(\LDC_BYTE_ORIGINAL, \LDC_BYTE_ORIGINAL)
#endif
    CalcMultAlpha_4x1 %v3,%v4,%v1,%v6,%v16,%v17,%v18,%v19,\ALPHA_VECREG,\ALPHA_VECI
    CalcMultAlpha_4x1 %v24,%v25,%v26,%v27,%v20,%v21,%v22,%v23,\ALPHA_VECREG,\ALPHA_VECI
#if defined(TRMMKERNEL)
    la \LC2,0(\LC1,\LDC_BYTE_ORIGINAL )
#endif
    vpdi %v16, %v3 ,%v4,0
    vpdi %v17, %v3,%v4,0b0101
    vpdi %v20, %v24 ,%v25,0
    vpdi %v21, %v24,%v25,0b0101
    vpdi %v22, %v26 ,%v27,0
    vpdi %v23, %v26 ,%v27,0b0101
    vst %v16,0(\CIJ_REG)
    vst %v20,16(\CIJ_REG)
    vpdi %v18, %v1 ,%v6,0
    vst %v17,0(\CIJ_REG,  \LDC_BYTE_ORIGINAL)
    vst %v21,16(\CIJ_REG,  \LDC_BYTE_ORIGINAL)
    vpdi %v19, %v1 ,%v6,0b0101
    vst %v18,0(\CIJ_REG,   \LC1)
    vst %v22,16(\CIJ_REG,   \LC1)
    vst %v19,0(\CIJ_REG,\LC2)
    vst %v23,16(\CIJ_REG,\LC2)
    la \CIJ_REG,32(\CIJ_REG)

.endm

.macro ZSTORE_2x2  ALPHA_VECREG,ALPHA_VECI,CIJ_REG , LDC_BYTE_ORIGINAL
  #if !defined(TRMMKERNEL)
    vl %v1 , 0(\CIJ_REG)
    vl %v4 , 16(\CIJ_REG)
    vpdi %v3,%v1,%v4,0
    vpdi %v4,%v1,%v4,0b101
    vl %v5 , 0(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vl %v7 , 16(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vpdi %v6,%v5,%v7,0
    vpdi %v7,%v5,%v7,0b101
#endif
    CalcMultAlpha_2x1 %v3,%v4, %v16,%v17,\ALPHA_VECREG,\ALPHA_VECI
    CalcMultAlpha_2x1 %v6,%v7, %v20,%v21 ,\ALPHA_VECREG,\ALPHA_VECI
    vpdi %v16, %v3 ,%v4,0
    vpdi %v17, %v3,%v4,0b0101
    vst %v16,0(\CIJ_REG)
    vst %v17,16(\CIJ_REG)
    vpdi %v20, %v6 ,%v7,0
    vpdi %v21, %v6 ,%v7,0b0101
    vst %v20,0(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vst %v21,16(\CIJ_REG,\LDC_BYTE_ORIGINAL)

    la \CIJ_REG,32(\CIJ_REG)
.endm

.macro ZSTORE_2x1  ALPHA_VECREG,ALPHA_VECI,CIJ_REG , LDC_BYTE_ORIGINAL
  #if !defined(TRMMKERNEL)
    vl %v1 , 0(\CIJ_REG)
    vl %v4 , 16(\CIJ_REG)
    vpdi %v3,%v1,%v4,0
    vpdi %v4,%v1,%v4,0b101
#endif
    CalcMultAlpha_2x1 %v3,%v4, %v16,%v17,\ALPHA_VECREG,\ALPHA_VECI
    vpdi %v16, %v3 ,%v4,0
    vpdi %v17, %v3,%v4,0b0101
    vst %v16,0(\CIJ_REG)
    vst %v17,16(\CIJ_REG)
    la \CIJ_REG,32(\CIJ_REG)
.endm

.macro ZSTORE_1x2  ALPHA_VECREG,ALPHA_VECI,CIJ_REG , LDC_BYTE_ORIGINAL
  #if !defined(TRMMKERNEL)
    vl %v1 , 0(\CIJ_REG)
    vl %v4 , 0(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    vpdi %v3,%v1,%v4,0
    vpdi %v4,%v1,%v4,0b101
#endif
    CalcMultAlpha_2x1 %v3,%v4, %v16,%v17,\ALPHA_VECREG,\ALPHA_VECI
    vpdi %v16, %v3 ,%v4,0
    vpdi %v17, %v3,%v4,0b0101
    vst %v16,0(\CIJ_REG)
    vst %v17,0(\CIJ_REG,\LDC_BYTE_ORIGINAL)
    la \CIJ_REG,16(\CIJ_REG)
.endm

.macro ZSTORE_1x1  ALPHA_RR,ALPHA_RI ,CIJ_REG
  #if defined (TRMMKERNEL)
    lzdr %f1
    lzdr %f4
#else
    ld %f1 , 0(\CIJ_REG)
    ld %f4 , 8(\CIJ_REG )
#endif
    CalcMultAlpha_1x1 %f1,%f4, %f6,%f7,\ALPHA_RR,\ALPHA_RI
    std %f1,0(\CIJ_REG)
    std %f4,8(\CIJ_REG)
    la \CIJ_REG,16(\CIJ_REG)
.endm

/****************************TRMM POINTER REFRESH MACROSES*************************/

.macro RefreshPointers  PTR_A,PTR_B,OFF_VAL,B_VAL,C_A,C_B
  #if (defined(LEFT) &&  defined(TRANSA)) ||  (!defined(LEFT) && !defined(TRANSA))
    /*  ptrbb = bb;*/
    lgr \PTR_B,\B_VAL    /*refresh BPOINT*/

  #else
    /*  ptrba  =ptrba+ off*C_A;
    ptrbb = bb + off*C_B;*/
.if \C_B==4
  .if \C_A==4
    sllg \PTR_B, \OFF_VAL,6
    agr \PTR_A,\PTR_B /*ptrba+off*4**/
    la \PTR_B,0(\B_VAL,\PTR_B)    /*refresh BPOINT*/
  .elseif \C_A==2
    sllg \PTR_B, \OFF_VAL,5
    la \PTR_A,0(\PTR_A,\PTR_B) /*ptrba+off*2**/
    agr \PTR_B, \PTR_B
    la \PTR_B,0(\B_VAL,\PTR_B)    /*refresh BPOINT*/

  .elseif \C_A==1
    sllg \PTR_B, \OFF_VAL,4
    agr \PTR_A,\PTR_B /*ptrba+off*4**/
    sllg \PTR_B, \OFF_VAL,6
    la \PTR_B,0(\B_VAL,\PTR_B)    /*refresh BPOINT*/
  .endif

.elseif \C_B==2
  .if \C_A==4
    sllg \PTR_B, \OFF_VAL,5
    la \PTR_A,0(\PTR_A,\PTR_B) /*ptrba+off*2**/
    agr \PTR_A,\PTR_B /*ptrba+off*2**/
    la \PTR_B,0(\B_VAL,\PTR_B)    /*refresh BPOINT*/
  .elseif \C_A==2
    sllg \PTR_B, \OFF_VAL,5
    agr \PTR_A,\PTR_B /*ptrba+off*2**/
    la \PTR_B,0(\B_VAL,\PTR_B)    /*refresh BPOINT*/
  .elseif \C_A==1
    sllg \PTR_B, \OFF_VAL,4
    la \PTR_A,0(\PTR_A,\PTR_B) /*ptrba+off*1**/
    agr \PTR_B,\PTR_B /* off+off**/
    la \PTR_B,0(\B_VAL,\PTR_B)    /*refresh BPOINT*/
  .endif

.elseif \C_B==1
  .if \C_A==4
    sllg \PTR_B, \OFF_VAL,6
    agr \PTR_A,\PTR_B /*ptrba+off*4**/
    sllg \PTR_B, \OFF_VAL,4
    la \PTR_B,0(\B_VAL,\PTR_B)    /*refresh BPOINT*/
  .elseif \C_A==2
    sllg \PTR_B, \OFF_VAL,4
    la \PTR_A,0(\PTR_A,\PTR_B) /*ptrba+off*1**/
    agr \PTR_A,\PTR_B /*ptrba+off*1**/
    la \PTR_B,0(\B_VAL,\PTR_B)    /*refresh BPOINT*/

  .elseif \C_A==1
    sllg \PTR_B, \OFF_VAL,4
    agr \PTR_A,\PTR_B /*ptrba+off*1**/
    la \PTR_B,0(\B_VAL,\PTR_B)    /*refresh BPOINT*/
  .endif
.endif

  #endif
.endm

/**/
.macro RefreshTempBk TEMP_VAL,BK_VAL,OFF_VAL,INCR_A,INCR_B
  #if (defined(LEFT) && !defined(TRANSA)) ||  (!defined(LEFT) && defined(TRANSA))
        /* temp = bk-off;*/
    sgrk \TEMP_VAL,\BK_VAL,\OFF_VAL

  #elif defined(LEFT)
        /* temp = off+INCR_A; // number of values in A */
    la \TEMP_VAL,\INCR_A(\OFF_VAL)
  #else
        /* temp = off+INCR_B  // number of values in B*/
    la \TEMP_VAL,\INCR_B(\OFF_VAL)
  #endif

.endm

.macro RefreshPointersAndOFF TEMP_VAL,BK_VAL,OFF_VAL,PTR_A,C_A,C_B

  #if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
    /*temp = bk - off;*/
    sgrk \TEMP_VAL,\BK_VAL,\OFF_VAL
  #ifdef LEFT
    /*temp -= 8; // number of values in A*/
    lay \TEMP_VAL,-\C_A(\TEMP_VAL)
  #else
    /*temp -= 4; // number of values in B*/
    lay \TEMP_VAL,-\C_B(\TEMP_VAL)
  #endif
    /*ptrba += temp*C_A;
    ptrbb += temp*C_B;*/

  .if \C_A==4
    sllg \TEMP_VAL, \TEMP_VAL,6 /*temp*4*/
  .elseif \C_A==2
    sllg \TEMP_VAL, \TEMP_VAL,5 /*temp*2*/
  .elseif \C_A==1
    sllg \TEMP_VAL, \TEMP_VAL,4 /*temp*1*/
  .endif
    la \PTR_A,0(\PTR_A,\TEMP_VAL) /*ptrba+temp*C_A*/
  #endif

  #ifdef LEFT
    /*off += \c_A; // number of values in A*/
    aghi \OFF_VAL,\C_A
  #endif
.endm

