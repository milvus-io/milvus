/***************************************************************************
Copyright (c) 2013-2017, The OpenBLAS Project
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
1. Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in
the documentation and/or other materials provided with the
distribution.
3. Neither the name of the OpenBLAS project nor the names of
its contributors may be used to endorse or promote products
derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE OPENBLAS PROJECT OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/

/**************************************************************************************
* 2017/03/12 AbdelRauf (quickwritereader@gmail.com)
* 	 BLASTEST 		:  passed
* 	 CTEST			:  passed
* 	 TEST		        :  passed
**************************************************************************************/

/*********************************************************************/
/* Copyright 2009, 2010 The University of Texas at Austin.           */
/* All rights reserved.                                              */
/*                                                                   */
/* Redistribution and use in source and binary forms, with or        */
/* without modification, are permitted provided that the following   */
/* conditions are met:                                               */
/*                                                                   */
/*   1. Redistributions of source code must retain the above         */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer.                                                  */
/*                                                                   */
/*   2. Redistributions in binary form must reproduce the above      */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer in the documentation and/or other materials       */
/*      provided with the distribution.                              */
/*                                                                   */
/*    THIS  SOFTWARE IS PROVIDED  BY THE  UNIVERSITY OF  TEXAS AT    */
/*    AUSTIN  ``AS IS''  AND ANY  EXPRESS OR  IMPLIED WARRANTIES,    */
/*    INCLUDING, BUT  NOT LIMITED  TO, THE IMPLIED  WARRANTIES OF    */
/*    MERCHANTABILITY  AND FITNESS FOR  A PARTICULAR  PURPOSE ARE    */
/*    DISCLAIMED.  IN  NO EVENT SHALL THE UNIVERSITY  OF TEXAS AT    */
/*    AUSTIN OR CONTRIBUTORS BE  LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL,  SPECIAL, EXEMPLARY,  OR  CONSEQUENTIAL DAMAGES    */
/*    (INCLUDING, BUT  NOT LIMITED TO,  PROCUREMENT OF SUBSTITUTE    */
/*    GOODS  OR  SERVICES; LOSS  OF  USE,  DATA,  OR PROFITS;  OR    */
/*    BUSINESS INTERRUPTION) HOWEVER CAUSED  AND ON ANY THEORY OF    */
/*    LIABILITY, WHETHER  IN CONTRACT, STRICT  LIABILITY, OR TORT    */
/*    (INCLUDING NEGLIGENCE OR OTHERWISE)  ARISING IN ANY WAY OUT    */
/*    OF  THE  USE OF  THIS  SOFTWARE,  EVEN  IF ADVISED  OF  THE    */
/*    POSSIBILITY OF SUCH DAMAGE.                                    */
/*                                                                   */
/* The views and conclusions contained in the software and           */
/* documentation are those of the authors and should not be          */
/* interpreted as representing official policies, either expressed   */
/* or implied, of The University of Texas at Austin.                 */
/*********************************************************************/

#define ASSEMBLER
#include "common.h"

/*

 
BLASLONG bm,BLASLONG bn,BLASLONG bk,FLOAT alphar,FLOAT alphai,FLOAT* ba,FLOAT* bb,
    FLOAT* C,BLASLONG ldc, BLASLONG offset)
 ##bm=r2,bn=r3, bk=r4, alpha=f0,aplhai=f2, ba=r5,bb=r6,stack[160] ,ldc=stack[168]  
offset=stack[176]    

**********************************************************************************************/
/*Note: r0 can not be used as address disp register */ 

#define BM           %r2
#define BM_CUR       %r0
#define BN           %r3
#define BN_CUR       %r10 
#define BK           %r4 
#define LDC_BYTE     %r8 
#define ALPHA        %f0
#define ALPHA_I      %f2
#define ALPHA_VECT   %v0 
#define ALPHA_VECT_I %v2 
#define LOCAL_VAR1   %r9
#define LOCAL_VAR2   %r1
#define LOCAL_VAR3   %r11
#define A            %r5
#define B            %r6
#define CIJ          %r7
#define CIJ_LOCAL    %r12 
#define OFF          %r13
#define OFFSET       %f8
#define ALIGN_4   .align 32
#define ALIGN_2   .align 16
#define PREFETCH_INS 1

/**************************Include kernel helper macrosses**********************************/
#include "ckernelMacrosV.S"

 

/***********************************CGEMM**4x4*******************************************************/

PROLOGUE
#if defined(TRMMKERNEL) 
  std OFFSET   ,40(%r15)
  stmg %r6,%r13,48(%r15)  
#else 
  stmg %r6,%r12,48(%r15)   
#endif
std %f9, 128(%r15)
std %f10,136(%r15)
std %f11,144(%r15)
std %f12,152(%r15)

lg CIJ, 160(%r15)
lg LOCAL_VAR1, 168(%r15)
#if defined(TRMMKERNEL)
  lg OFF,176(%r15)
  ldgr OFFSET ,OFF
#endif
srlg BN_CUR,BN,2  
#if   defined(RR) || defined(RC) || defined(CR) || defined(CC) 
   lcdbr ALPHA_I,ALPHA_I 
   lcdbr ALPHA ,ALPHA  
#endif

vrepg ALPHA_VECT,ALPHA_VECT,0 /*replicate alpha which in f0*/

sllg LDC_BYTE, LOCAL_VAR1,3  /*calculate lcd stride with complex=8 x<<4 */
vrepg ALPHA_VECT_I,ALPHA_VECT_I,0 /*replicate alpha which in f0*/

vldeb ALPHA_VECT,ALPHA_VECT 
vldeb ALPHA_VECT_I,ALPHA_VECT_I   
#if defined(TRMMKERNEL) && !defined(LEFT)
   /*off = -offset;*/
   lgdr LOCAL_VAR1,OFFSET
   lcgr OFF,LOCAL_VAR1
#endif
cijle BN_CUR,0,.LX2 

ALIGN_4
.LX4_BN:
#if defined(PREFETCH_INS)
        pfd 1, 0(A)   
        pfd 1, 0(B)    
#endif
#if defined(TRMMKERNEL) && defined(LEFT)
    /*off = offset;*/
   lgdr OFF,OFFSET
#endif
srlg BM_CUR,BM,2  
lgr LOCAL_VAR3,A 
lgr CIJ_LOCAL,CIJ
cijle BM_CUR,0,.L2x4 

ALIGN_4
.L4x4_BM: /*BM start*/
#if defined(TRMMKERNEL)

    /* RefreshPointers  PTR_A,PTR_B,OFF_VAL,B_VAL,C_A,C_B */
      RefreshPointers  LOCAL_VAR3,LOCAL_VAR2,OFF,B,4,4
      RefreshTempBk LOCAL_VAR1,BK,OFF,4,4
      srl LOCAL_VAR1,2

#else
    srlg LOCAL_VAR1,BK,2  /*refresh BK*/  
    lgr LOCAL_VAR2,B      /*refresh BPOINT*/
#endif
ZERO_ZCVEC_4x4  
cijle LOCAL_VAR1,0,.L4x4_mod

ALIGN_4
.L4x4_4_BK: /*BK_CUR LOOP */ 
      ZCALC_4x4_4 LOCAL_VAR3,LOCAL_VAR2   
#if defined(PREFETCH_INS)
        pfd 1, 128(LOCAL_VAR3)   /*256-128*/
        pfd 1, 128(LOCAL_VAR2 ) 
#endif
brctg LOCAL_VAR1,.L4x4_4_BK

ALIGN_4
.L4x4_mod:
#if defined(TRMMKERNEL)
      RefreshTempBk LOCAL_VAR1,BK,OFF,4,4
      nill LOCAL_VAR1,3 
#else
    la LOCAL_VAR1,3(0,0)
    NGR LOCAL_VAR1,BK /*refresh BK*/ 
#endif
jz .L4x4_BK_Store

ALIGN_4
.L4x4_BK: /*BK_CUR LOOP */
      ZCALC_4x4 LOCAL_VAR3,LOCAL_VAR2   
brctg LOCAL_VAR1,.L4x4_BK

ALIGN_4
.L4x4_BK_Store:
/*store C and use LDC_BYTE  AND CIJ_COPY for mem storing*/
ZSTORE_4x4 ALPHA_VECT,ALPHA_VECT_I ,CIJ_LOCAL, LDC_BYTE,LOCAL_VAR1,LOCAL_VAR2
#if defined(TRMMKERNEL)
       RefreshPointersAndOFF LOCAL_VAR1,BK,OFF,LOCAL_VAR3,4,4
#endif  

brctg BM_CUR,.L4x4_BM

ALIGN_2
.L2x4:
 
tmll BM,2  
jz .L1x4
 
ALIGN_4
.L2x4_BM: /*BM start*/
#if defined(TRMMKERNEL)

     /* RefreshPointers  PTR_A,PTR_B,OFF_VAL,B_VAL,C_A,C_B */
      RefreshPointers  LOCAL_VAR3,LOCAL_VAR2,OFF,B,2,4 
      RefreshTempBk LOCAL_VAR1,BK,OFF,2,4
      srl LOCAL_VAR1,2

#else
srlg LOCAL_VAR1,BK,2  /*refresh BK*/  
lgr LOCAL_VAR2,B      /*refresh BPOINT*/
#endif
ZERO_ZCVEC_2x4  
cijle LOCAL_VAR1,0,.L2x4_mod

ALIGN_4
.L2x4_4_BK: /*BK_CUR LOOP */
      ZCALC_2x4_4 LOCAL_VAR3,LOCAL_VAR2 
#if defined(PREFETCH_INS) 
        pfd 1, 128(LOCAL_VAR2)     
#endif  
brctg LOCAL_VAR1,.L2x4_4_BK

ALIGN_4
.L2x4_mod:
#if defined(TRMMKERNEL)
      RefreshTempBk LOCAL_VAR1,BK,OFF,2,4
      nill LOCAL_VAR1,3 
#else
la LOCAL_VAR1,3(0,0)
NGR LOCAL_VAR1,BK /*refresh BK*/ 
#endif
jz .L2x4_BK_Store

ALIGN_4
.L2x4_BK: /*BK_CUR LOOP */
      ZCALC_2x4 LOCAL_VAR3,LOCAL_VAR2   
brctg LOCAL_VAR1,.L2x4_BK

ALIGN_4
.L2x4_BK_Store:
/*store C and use LDC_BYTE  AND CIJ_COPY for mem storing*/
ZSTORE_2x4 ALPHA_VECT,ALPHA_VECT_I ,CIJ_LOCAL, LDC_BYTE ,LOCAL_VAR1,LOCAL_VAR2
#if defined(TRMMKERNEL)
    RefreshPointersAndOFF LOCAL_VAR1,BK,OFF,LOCAL_VAR3,2,4
#endif  

ALIGN_4
.L1x4:
 
tmll BM,1  
jz .Lx4_INNER_END
 
ALIGN_4
.L1x4_BM: /*BM start*/
#if defined(TRMMKERNEL)

    /* RefreshPointers  PTR_A,PTR_B,OFF_VAL,B_VAL,C_A,C_B */
      RefreshPointers  LOCAL_VAR3,LOCAL_VAR2,OFF,B,1,4
      RefreshTempBk LOCAL_VAR1,BK,OFF,1,4
      srl LOCAL_VAR1,2

#else
srlg LOCAL_VAR1,BK,2  /*refresh BK*/  
lgr LOCAL_VAR2,B      /*refresh BPOINT*/
#endif
ZERO_ZCVEC_1x4  
cijle LOCAL_VAR1,0,.L1x4_mod

ALIGN_4
.L1x4_4_BK: /*BK_CUR LOOP */ 
      ZCALC_1x4_4 LOCAL_VAR3,LOCAL_VAR2   
brctg LOCAL_VAR1,.L1x4_4_BK

ALIGN_4
.L1x4_mod:
#if defined(TRMMKERNEL)
       RefreshTempBk LOCAL_VAR1,BK,OFF,1,4
      nill LOCAL_VAR1,3 
#else
la LOCAL_VAR1,3(0,0)
NGR LOCAL_VAR1,BK /*refresh BK*/
#endif 
jz .L1x4_BK_Store

ALIGN_4
.L1x4_BK: /*BK_CUR LOOP */
      ZCALC_1x4 LOCAL_VAR3,LOCAL_VAR2   
brctg LOCAL_VAR1,.L1x4_BK

ALIGN_4
.L1x4_BK_Store:
/*store C and use LDC_BYTE  AND CIJ_COPY for mem storing*/
ZSTORE_1x4 ALPHA_VECT,ALPHA_VECT_I ,CIJ_LOCAL, LDC_BYTE,LOCAL_VAR1,LOCAL_VAR2
#if defined(TRMMKERNEL)
     RefreshPointersAndOFF LOCAL_VAR1,BK,OFF,LOCAL_VAR3,1,4
#endif  
ALIGN_2
.Lx4_INNER_END:


/*add LDC_BYTE_COPY to new*/
sllg LOCAL_VAR1,LDC_BYTE,2  /*multiply*4 */
#if defined(TRMMKERNEL) && !defined(LEFT)
    aghi OFF,4
#endif
sllg LOCAL_VAR2,BK,5  /*multiply*4*sizeof(complex)  =multiply*4*8* 2**5 */
la CIJ,0(CIJ,LOCAL_VAR1)  /*refresh CIJ=CIJ+LDC_BYTE*4*/ 
la B,0(B,LOCAL_VAR2)      /*refresh B=B+Bk*4*sizeof(complex)  */

brctg BN_CUR,.LX4_BN

/*********************************X2 SECTION************************************************/
ALIGN_4
.LX2:
tmll BN,2 
jz .Lx1

ALIGN_4
.Lx2_BN:

#if defined(TRMMKERNEL) && defined(LEFT)
    /*off = offset;*/
   lgdr OFF,OFFSET
#endif

srlg BM_CUR,BM,2  
lgr LOCAL_VAR3,A
lgr CIJ_LOCAL,CIJ
cijle BM_CUR,0,.L2x2 
 
ALIGN_4
.L4x2_BM: /*BM start*/
#if defined(TRMMKERNEL)
    /* RefreshPointers  PTR_A,PTR_B,OFF_VAL,B_VAL,C_A,C_B */
      RefreshPointers  LOCAL_VAR3,LOCAL_VAR2,OFF,B,4,2
      RefreshTempBk LOCAL_VAR1,BK,OFF,4,2
      srl LOCAL_VAR1,2

#else
  srlg LOCAL_VAR1,BK,2  /*refresh BK*/  
  lgr LOCAL_VAR2,B      /*refresh BPOINT*/
#endif
ZERO_ZCVEC_4x2  
cijle LOCAL_VAR1,0,.L4x2_mod

ALIGN_4
.L4x2_4_BK: /*BK_CUR LOOP */ 
      ZCALC_4x2_4 LOCAL_VAR3,LOCAL_VAR2   
#if defined(PREFETCH_INS)   
        pfd 1, 128(LOCAL_VAR3)   
#endif
brctg LOCAL_VAR1,.L4x2_4_BK

ALIGN_4
.L4x2_mod:
#if defined(TRMMKERNEL)
      RefreshTempBk LOCAL_VAR1,BK,OFF,4,2
      nill LOCAL_VAR1,3 
#else
la LOCAL_VAR1,3(0,0)
NGR LOCAL_VAR1,BK /*refresh BK*/ 
#endif
jz .L4x2_BK_Store

ALIGN_4
.L4x2_BK: /*BK_CUR LOOP */
      ZCALC_4x2 LOCAL_VAR3,LOCAL_VAR2   
brctg LOCAL_VAR1,.L4x2_BK

ALIGN_4
.L4x2_BK_Store:
/*store C and use LDC_BYTE  AND CIJ_COPY for mem storing*/
ZSTORE_4x2 ALPHA_VECT,ALPHA_VECT_I ,CIJ_LOCAL, LDC_BYTE
#if defined(TRMMKERNEL)
     RefreshPointersAndOFF LOCAL_VAR1,BK,OFF,LOCAL_VAR3,4,2
#endif
ALIGN_4  
brctg BM_CUR,.L4x2_BM  

ALIGN_2
.L2x2:

tmll BM,2   
jz .L1x2
 
ALIGN_4
.L2x2_BM: /*BM start*/
#if defined(TRMMKERNEL)

    /* RefreshPointers  PTR_A,PTR_B,OFF_VAL,B_VAL,C_A,C_B */
      RefreshPointers  LOCAL_VAR3,LOCAL_VAR2,OFF,B,2,2
      RefreshTempBk LOCAL_VAR1,BK,OFF,2,2
      srl LOCAL_VAR1,2

#else
srlg LOCAL_VAR1,BK,2  /*refresh BK*/  
lgr LOCAL_VAR2,B      /*refresh BPOINT*/
#endif
ZERO_ZCVEC_2x2  
cijle LOCAL_VAR1,0,.L2x2_mod

ALIGN_4
.L2x2_4_BK: /*BK_CUR LOOP */
      ZCALC_2x2_4 LOCAL_VAR3,LOCAL_VAR2   
#if defined(PREFETCH_INS)
        pfd 1, 256(LOCAL_VAR3)  
        pfd 1, 256(LOCAL_VAR2)    
#endif
brctg LOCAL_VAR1,.L2x2_4_BK

ALIGN_4
.L2x2_mod:
#if defined(TRMMKERNEL)
      RefreshTempBk LOCAL_VAR1,BK,OFF,2,2
      nill LOCAL_VAR1,3 
#else
la LOCAL_VAR1,3(0,0)
NGR LOCAL_VAR1,BK /*refresh BK*/ 
#endif
jz .L2x2_BK_Store

ALIGN_4
.L2x2_BK: /*BK_CUR LOOP */
      ZCALC_2x2 LOCAL_VAR3,LOCAL_VAR2   
brctg LOCAL_VAR1,.L2x2_BK

ALIGN_4
.L2x2_BK_Store:
/*store C and use LDC_BYTE  AND CIJ_COPY for mem storing*/
ZSTORE_2x2 ALPHA_VECT,ALPHA_VECT_I ,CIJ_LOCAL, LDC_BYTE
#if defined(TRMMKERNEL)
     RefreshPointersAndOFF LOCAL_VAR1,BK,OFF,LOCAL_VAR3,2,2
#endif  

ALIGN_2
.L1x2:
 
tmll BM,1  
jz .Lx2_INNER_END
 
ALIGN_4
.L1x2_BM: /*BM start*/
#if defined(TRMMKERNEL)
    /* RefreshPointers  PTR_A,PTR_B,OFF_VAL,B_VAL,C_A,C_B */
      RefreshPointers  LOCAL_VAR3,LOCAL_VAR2,OFF,B,1,2
      RefreshTempBk LOCAL_VAR1,BK,OFF,1,2
      srl LOCAL_VAR1,2

#else
srlg LOCAL_VAR1,BK,2  /*refresh BK*/  
lgr LOCAL_VAR2,B      /*refresh BPOINT*/
#endif
ZERO_ZCVEC_1x2  
cijle LOCAL_VAR1,0,.L1x2_mod

ALIGN_4
.L1x2_4_BK: /*BK_CUR LOOP */
      ZCALC_1x2_4 LOCAL_VAR3,LOCAL_VAR2   
brctg LOCAL_VAR1,.L1x2_4_BK

ALIGN_4
.L1x2_mod:
#if defined(TRMMKERNEL)
      RefreshTempBk LOCAL_VAR1,BK,OFF,1,2
      nill LOCAL_VAR1,3 
#else
la LOCAL_VAR1,3(0,0)
NGR LOCAL_VAR1,BK /*refresh BK*/ 
#endif
jz .L1x2_BK_Store

ALIGN_4
.L1x2_BK: /*BK_CUR LOOP */
      ZCALC_1x2 LOCAL_VAR3,LOCAL_VAR2   
brctg LOCAL_VAR1,.L1x2_BK

ALIGN_4
.L1x2_BK_Store:
/*store C and use LDC_BYTE  AND CIJ_COPY for mem storing*/
ZSTORE_1x2 ALPHA_VECT,ALPHA_VECT_I ,CIJ_LOCAL, LDC_BYTE
#if defined(TRMMKERNEL)
     RefreshPointersAndOFF LOCAL_VAR1,BK,OFF,LOCAL_VAR3,1,2
#endif  
ALIGN_2
.Lx2_INNER_END: 
/*add LDC_BYTE_COPY to new*/
la LOCAL_VAR1,0(LDC_BYTE,LDC_BYTE)   /*multiply*2 */
sllg LOCAL_VAR2,BK,4  /*multiply*2*sizeof(complex)  =multiply*2*8  2^4 */
la CIJ,0(CIJ,LOCAL_VAR1)  /*refresh CIJ=CIJ+LDC_BYTE*2*/ 
#if defined(TRMMKERNEL) && !defined(LEFT)
    aghi OFF,2
#endif
la B,0(B,LOCAL_VAR2)      /*refresh B=B+Bk*2*sizeof(complex)  */

 
 

/*********************************X1 SECTION************************************************/
ALIGN_2
.Lx1:
tmll BN,1  
jz .L_FUNC_END

ALIGN_4
.Lx1_BN:

#if defined(TRMMKERNEL) && defined(LEFT)
    /*off = offset;*/
   lgdr OFF,OFFSET
#endif
srlg BM_CUR,BM,2 
lgr LOCAL_VAR3,A
lgr CIJ_LOCAL,CIJ
cijle BM_CUR,0,.L2x1

ALIGN_4
.L4x1_BM: /*BM start*/
#if defined(TRMMKERNEL)

    /* RefreshPointers  PTR_A,PTR_B,OFF_VAL,B_VAL,C_A,C_B */
      RefreshPointers  LOCAL_VAR3,LOCAL_VAR2,OFF,B,4,1
      RefreshTempBk LOCAL_VAR1,BK,OFF,4,1
      srl LOCAL_VAR1,2

#else
srlg LOCAL_VAR1,BK,2  /*refresh BK*/  
lgr LOCAL_VAR2,B      /*refresh BPOINT*/
#endif
ZERO_ZCVEC_4x1  
cijle LOCAL_VAR1,0,.L4x1_mod

ALIGN_4
.L4x1_4_BK: /*BK_CUR LOOP */ 
      ZCALC_4x1_4 LOCAL_VAR3,LOCAL_VAR2   
brctg LOCAL_VAR1,.L4x1_4_BK

ALIGN_4
.L4x1_mod:
#if defined(TRMMKERNEL)
      RefreshTempBk LOCAL_VAR1,BK,OFF,4,1
      nill LOCAL_VAR1,3 
#else
la LOCAL_VAR1,3(0,0)
NGR LOCAL_VAR1,BK /*refresh BK*/ 
#endif
jz .L4x1_BK_Store

ALIGN_4
.L4x1_BK: /*BK_CUR LOOP */
      ZCALC_4x1 LOCAL_VAR3,LOCAL_VAR2   
brctg LOCAL_VAR1,.L4x1_BK

ALIGN_4
.L4x1_BK_Store:
/*store C and use LDC_BYTE  AND CIJ_COPY for mem storing*/
ZSTORE_4x1 ALPHA_VECT,ALPHA_VECT_I ,CIJ_LOCAL, LDC_BYTE
 #if defined(TRMMKERNEL)
     RefreshPointersAndOFF LOCAL_VAR1,BK,OFF,LOCAL_VAR3,4,1
#endif
ALIGN_4
brctg BM_CUR , .L4x1_BM

ALIGN_2
.L2x1:

tmll BM,2   
jz .L1x1
 
ALIGN_4
.L2x1_BM: /*BM start*/
#if defined(TRMMKERNEL)
    /* RefreshPointers  PTR_A,PTR_B,OFF_VAL,B_VAL,C_A,C_B */
     RefreshPointers  LOCAL_VAR3,LOCAL_VAR2,OFF,B,2,1
      RefreshTempBk LOCAL_VAR1,BK,OFF,2,1
      srl LOCAL_VAR1,2

#else
srlg LOCAL_VAR1,BK,2  /*refresh BK*/  
lgr LOCAL_VAR2,B      /*refresh BPOINT*/
#endif
ZERO_ZCVEC_2x1  
cijle LOCAL_VAR1,0,.L2x1_mod

ALIGN_4
.L2x1_4_BK: /*BK_CUR LOOP */
      ZCALC_2x1_4 LOCAL_VAR3,LOCAL_VAR2   
brctg LOCAL_VAR1,.L2x1_4_BK

ALIGN_4
.L2x1_mod:
#if defined(TRMMKERNEL)
      RefreshTempBk LOCAL_VAR1,BK,OFF,2,1
      nill LOCAL_VAR1,3 
#else
la LOCAL_VAR1,3(0,0)
NGR LOCAL_VAR1,BK /*refresh BK*/ 
#endif
jz .L2x1_BK_Store

ALIGN_4
.L2x1_BK: /*BK_CUR LOOP */
      ZCALC_2x1 LOCAL_VAR3,LOCAL_VAR2   
brctg LOCAL_VAR1,.L2x1_BK

ALIGN_4
.L2x1_BK_Store:
/*store C and use LDC_BYTE  AND CIJ_COPY for mem storing*/
ZSTORE_2x1 ALPHA_VECT,ALPHA_VECT_I ,CIJ_LOCAL, LDC_BYTE
#if defined(TRMMKERNEL)
     RefreshPointersAndOFF LOCAL_VAR1,BK,OFF,LOCAL_VAR3,2,1
#endif

ALIGN_2
.L1x1:
 
tmll BM, 1   
jz .Lx1_INNER_END
 
ALIGN_4
.L1x1_BM: /*BM start*/
#if defined(TRMMKERNEL)
    /* RefreshPointers  PTR_A,PTR_B,OFF_VAL,B_VAL,C_A,C_B */
      RefreshPointers  LOCAL_VAR3,LOCAL_VAR2,OFF,B,1,1
      RefreshTempBk LOCAL_VAR1,BK,OFF,1,1
      srl LOCAL_VAR1,2

#else
srlg LOCAL_VAR1,BK,2  /*refresh BK*/  
lgr LOCAL_VAR2,B      /*refresh BPOINT*/
#endif
ZERO_ZCVEC_1x1  
cijle LOCAL_VAR1,0,.L1x1_mod

ALIGN_4
.L1x1_4_BK: /*BK_CUR LOOP */
      ZCALC_1x1_4 LOCAL_VAR3,LOCAL_VAR2   
brctg LOCAL_VAR1,.L1x1_4_BK

ALIGN_4
.L1x1_mod:
#if defined(TRMMKERNEL)
      RefreshTempBk LOCAL_VAR1,BK,OFF,1,1
      nill LOCAL_VAR1,3 
#else
la LOCAL_VAR1,3(0,0)
NGR LOCAL_VAR1,BK /*refresh BK*/ 
#endif
jz .L1x1_BK_Store

ALIGN_4
.L1x1_BK: /*BK_CUR LOOP */
      ZCALC_1x1 LOCAL_VAR3,LOCAL_VAR2   
brctg LOCAL_VAR1,.L1x1_BK

ALIGN_4
.L1x1_BK_Store:
/*store C and use  CIJ_COPY for mem storing*/
ZSTORE_1x1 ALPHA,ALPHA_I ,CIJ_LOCAL 
#if defined(TRMMKERNEL)
     RefreshPointersAndOFF LOCAL_VAR1,BK,OFF,LOCAL_VAR3,1,1
#endif
ALIGN_2
.Lx1_INNER_END: 
/*add LDC_BYTE_COPY to new*/ 
sllg LOCAL_VAR2,BK,3  /*multiply*1*sizeof(complex)  =multiply*1*8* 2^3 */
la CIJ,0(CIJ,LDC_BYTE)  /*refresh CIJ=CIJ+LDC_BYTE */ 
#if defined(TRMMKERNEL) && !defined(LEFT)
    aghi OFF,1
#endif
la B,0(B,LOCAL_VAR2)      /*refresh B=B+Bk*1*sizeof(complex)  */
 

ALIGN_2
.L_FUNC_END:
/*end*/


#if defined(TRMMKERNEL)
ld OFFSET,40(%r15)
lmg %r6,%r13,48(%r15) 
#else
lmg %r6,%r12,48(%r15) 
#endif
ld %f9, 128(%r15)
ld %f10,136(%r15)
ld %f11,144(%r15)
ld %f12,152(%r15)
br %r14
.end
















