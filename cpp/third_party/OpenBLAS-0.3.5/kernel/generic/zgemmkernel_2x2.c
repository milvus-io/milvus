#include "common.h"
/********************************
  ADD1 a*c
  ADD2 b*c
  ADD3 a*d
  ADD4 b*d
*********************************/
int CNAME(BLASLONG bm,BLASLONG bn,BLASLONG bk,FLOAT alphar,FLOAT alphai,FLOAT* ba,FLOAT* bb,FLOAT* C,BLASLONG ldc
#ifdef	TRMMKERNEL
		, BLASLONG offset
#endif
		)
{
   BLASLONG i,j,k;
   FLOAT *C0,*C1,*ptrba,*ptrbb;
   FLOAT res0,res1,res2,res3,res4,res5,res6,res7,load0,load1,load2,load3,load4,load5,load6,load7,load8,load9,load10,load11,load12,load13,load14,load15;
   for (j=0; j<bn/2; j+=1)
     {
        C0 = C;
        C1 = C0+2*ldc;
        ptrba = ba;
        for (i=0; i<bm/2; i+=1)
          {
             ptrbb = bb;
             res0 = 0;
             res1 = 0;
             res2 = 0;
             res3 = 0;
             res4 = 0;
             res5 = 0;
             res6 = 0;
             res7 = 0;
             for (k=0; k<bk/4; k+=1)
               {
#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
                  load0 = ptrba[4*0+0];
                  load1 = ptrbb[4*0+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[4*0+1];
                  res1 = res1+load2*load1;
                  load3 = ptrbb[4*0+1];
                  res0 = res0-load2*load3;
                  res1 = res1+load0*load3;
                  load4 = ptrba[4*0+2];
                  res2 = res2+load4*load1;
                  load5 = ptrba[4*0+3];
                  res3 = res3+load5*load1;
                  res2 = res2-load5*load3;
                  res3 = res3+load4*load3;
                  load6 = ptrbb[4*0+2];
                  res4 = res4+load0*load6;
                  res5 = res5+load2*load6;
                  load7 = ptrbb[4*0+3];
                  res4 = res4-load2*load7;
                  res5 = res5+load0*load7;
                  res6 = res6+load4*load6;
                  res7 = res7+load5*load6;
                  res6 = res6-load5*load7;
                  res7 = res7+load4*load7;
                  load8 = ptrba[4*1+0];
                  load9 = ptrbb[4*1+0];
                  res0 = res0+load8*load9;
                  load10 = ptrba[4*1+1];
                  res1 = res1+load10*load9;
                  load11 = ptrbb[4*1+1];
                  res0 = res0-load10*load11;
                  res1 = res1+load8*load11;
                  load12 = ptrba[4*1+2];
                  res2 = res2+load12*load9;
                  load13 = ptrba[4*1+3];
                  res3 = res3+load13*load9;
                  res2 = res2-load13*load11;
                  res3 = res3+load12*load11;
                  load14 = ptrbb[4*1+2];
                  res4 = res4+load8*load14;
                  res5 = res5+load10*load14;
                  load15 = ptrbb[4*1+3];
                  res4 = res4-load10*load15;
                  res5 = res5+load8*load15;
                  res6 = res6+load12*load14;
                  res7 = res7+load13*load14;
                  res6 = res6-load13*load15;
                  res7 = res7+load12*load15;
                  load0 = ptrba[4*2+0];
                  load1 = ptrbb[4*2+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[4*2+1];
                  res1 = res1+load2*load1;
                  load3 = ptrbb[4*2+1];
                  res0 = res0-load2*load3;
                  res1 = res1+load0*load3;
                  load4 = ptrba[4*2+2];
                  res2 = res2+load4*load1;
                  load5 = ptrba[4*2+3];
                  res3 = res3+load5*load1;
                  res2 = res2-load5*load3;
                  res3 = res3+load4*load3;
                  load6 = ptrbb[4*2+2];
                  res4 = res4+load0*load6;
                  res5 = res5+load2*load6;
                  load7 = ptrbb[4*2+3];
                  res4 = res4-load2*load7;
                  res5 = res5+load0*load7;
                  res6 = res6+load4*load6;
                  res7 = res7+load5*load6;
                  res6 = res6-load5*load7;
                  res7 = res7+load4*load7;
                  load8 = ptrba[4*3+0];
                  load9 = ptrbb[4*3+0];
                  res0 = res0+load8*load9;
                  load10 = ptrba[4*3+1];
                  res1 = res1+load10*load9;
                  load11 = ptrbb[4*3+1];
                  res0 = res0-load10*load11;
                  res1 = res1+load8*load11;
                  load12 = ptrba[4*3+2];
                  res2 = res2+load12*load9;
                  load13 = ptrba[4*3+3];
                  res3 = res3+load13*load9;
                  res2 = res2-load13*load11;
                  res3 = res3+load12*load11;
                  load14 = ptrbb[4*3+2];
                  res4 = res4+load8*load14;
                  res5 = res5+load10*load14;
                  load15 = ptrbb[4*3+3];
                  res4 = res4-load10*load15;
                  res5 = res5+load8*load15;
                  res6 = res6+load12*load14;
                  res7 = res7+load13*load14;
                  res6 = res6-load13*load15;
                  res7 = res7+load12*load15;
#endif
#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
                  load0 = ptrba[4*0+0];
                  load1 = ptrbb[4*0+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[4*0+1];
                  res1 = res1+load2*load1;
                  load3 = ptrbb[4*0+1];
                  res0 = res0+load2*load3;
                  res1 = res1-load0*load3;
                  load4 = ptrba[4*0+2];
                  res2 = res2+load4*load1;
                  load5 = ptrba[4*0+3];
                  res3 = res3+load5*load1;
                  res2 = res2+load5*load3;
                  res3 = res3-load4*load3;
                  load6 = ptrbb[4*0+2];
                  res4 = res4+load0*load6;
                  res5 = res5+load2*load6;
                  load7 = ptrbb[4*0+3];
                  res4 = res4+load2*load7;
                  res5 = res5-load0*load7;
                  res6 = res6+load4*load6;
                  res7 = res7+load5*load6;
                  res6 = res6+load5*load7;
                  res7 = res7-load4*load7;
                  load8 = ptrba[4*1+0];
                  load9 = ptrbb[4*1+0];
                  res0 = res0+load8*load9;
                  load10 = ptrba[4*1+1];
                  res1 = res1+load10*load9;
                  load11 = ptrbb[4*1+1];
                  res0 = res0+load10*load11;
                  res1 = res1-load8*load11;
                  load12 = ptrba[4*1+2];
                  res2 = res2+load12*load9;
                  load13 = ptrba[4*1+3];
                  res3 = res3+load13*load9;
                  res2 = res2+load13*load11;
                  res3 = res3-load12*load11;
                  load14 = ptrbb[4*1+2];
                  res4 = res4+load8*load14;
                  res5 = res5+load10*load14;
                  load15 = ptrbb[4*1+3];
                  res4 = res4+load10*load15;
                  res5 = res5-load8*load15;
                  res6 = res6+load12*load14;
                  res7 = res7+load13*load14;
                  res6 = res6+load13*load15;
                  res7 = res7-load12*load15;
                  load0 = ptrba[4*2+0];
                  load1 = ptrbb[4*2+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[4*2+1];
                  res1 = res1+load2*load1;
                  load3 = ptrbb[4*2+1];
                  res0 = res0+load2*load3;
                  res1 = res1-load0*load3;
                  load4 = ptrba[4*2+2];
                  res2 = res2+load4*load1;
                  load5 = ptrba[4*2+3];
                  res3 = res3+load5*load1;
                  res2 = res2+load5*load3;
                  res3 = res3-load4*load3;
                  load6 = ptrbb[4*2+2];
                  res4 = res4+load0*load6;
                  res5 = res5+load2*load6;
                  load7 = ptrbb[4*2+3];
                  res4 = res4+load2*load7;
                  res5 = res5-load0*load7;
                  res6 = res6+load4*load6;
                  res7 = res7+load5*load6;
                  res6 = res6+load5*load7;
                  res7 = res7-load4*load7;
                  load8 = ptrba[4*3+0];
                  load9 = ptrbb[4*3+0];
                  res0 = res0+load8*load9;
                  load10 = ptrba[4*3+1];
                  res1 = res1+load10*load9;
                  load11 = ptrbb[4*3+1];
                  res0 = res0+load10*load11;
                  res1 = res1-load8*load11;
                  load12 = ptrba[4*3+2];
                  res2 = res2+load12*load9;
                  load13 = ptrba[4*3+3];
                  res3 = res3+load13*load9;
                  res2 = res2+load13*load11;
                  res3 = res3-load12*load11;
                  load14 = ptrbb[4*3+2];
                  res4 = res4+load8*load14;
                  res5 = res5+load10*load14;
                  load15 = ptrbb[4*3+3];
                  res4 = res4+load10*load15;
                  res5 = res5-load8*load15;
                  res6 = res6+load12*load14;
                  res7 = res7+load13*load14;
                  res6 = res6+load13*load15;
                  res7 = res7-load12*load15;
#endif
#if   defined(RN) || defined(RT) || defined(CN) || defined(CT)
                  load0 = ptrba[4*0+0];
                  load1 = ptrbb[4*0+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[4*0+1];
                  res1 = res1-load2*load1;
                  load3 = ptrbb[4*0+1];
                  res0 = res0+load2*load3;
                  res1 = res1+load0*load3;
                  load4 = ptrba[4*0+2];
                  res2 = res2+load4*load1;
                  load5 = ptrba[4*0+3];
                  res3 = res3-load5*load1;
                  res2 = res2+load5*load3;
                  res3 = res3+load4*load3;
                  load6 = ptrbb[4*0+2];
                  res4 = res4+load0*load6;
                  res5 = res5-load2*load6;
                  load7 = ptrbb[4*0+3];
                  res4 = res4+load2*load7;
                  res5 = res5+load0*load7;
                  res6 = res6+load4*load6;
                  res7 = res7-load5*load6;
                  res6 = res6+load5*load7;
                  res7 = res7+load4*load7;
                  load8 = ptrba[4*1+0];
                  load9 = ptrbb[4*1+0];
                  res0 = res0+load8*load9;
                  load10 = ptrba[4*1+1];
                  res1 = res1-load10*load9;
                  load11 = ptrbb[4*1+1];
                  res0 = res0+load10*load11;
                  res1 = res1+load8*load11;
                  load12 = ptrba[4*1+2];
                  res2 = res2+load12*load9;
                  load13 = ptrba[4*1+3];
                  res3 = res3-load13*load9;
                  res2 = res2+load13*load11;
                  res3 = res3+load12*load11;
                  load14 = ptrbb[4*1+2];
                  res4 = res4+load8*load14;
                  res5 = res5-load10*load14;
                  load15 = ptrbb[4*1+3];
                  res4 = res4+load10*load15;
                  res5 = res5+load8*load15;
                  res6 = res6+load12*load14;
                  res7 = res7-load13*load14;
                  res6 = res6+load13*load15;
                  res7 = res7+load12*load15;
                  load0 = ptrba[4*2+0];
                  load1 = ptrbb[4*2+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[4*2+1];
                  res1 = res1-load2*load1;
                  load3 = ptrbb[4*2+1];
                  res0 = res0+load2*load3;
                  res1 = res1+load0*load3;
                  load4 = ptrba[4*2+2];
                  res2 = res2+load4*load1;
                  load5 = ptrba[4*2+3];
                  res3 = res3-load5*load1;
                  res2 = res2+load5*load3;
                  res3 = res3+load4*load3;
                  load6 = ptrbb[4*2+2];
                  res4 = res4+load0*load6;
                  res5 = res5-load2*load6;
                  load7 = ptrbb[4*2+3];
                  res4 = res4+load2*load7;
                  res5 = res5+load0*load7;
                  res6 = res6+load4*load6;
                  res7 = res7-load5*load6;
                  res6 = res6+load5*load7;
                  res7 = res7+load4*load7;
                  load8 = ptrba[4*3+0];
                  load9 = ptrbb[4*3+0];
                  res0 = res0+load8*load9;
                  load10 = ptrba[4*3+1];
                  res1 = res1-load10*load9;
                  load11 = ptrbb[4*3+1];
                  res0 = res0+load10*load11;
                  res1 = res1+load8*load11;
                  load12 = ptrba[4*3+2];
                  res2 = res2+load12*load9;
                  load13 = ptrba[4*3+3];
                  res3 = res3-load13*load9;
                  res2 = res2+load13*load11;
                  res3 = res3+load12*load11;
                  load14 = ptrbb[4*3+2];
                  res4 = res4+load8*load14;
                  res5 = res5-load10*load14;
                  load15 = ptrbb[4*3+3];
                  res4 = res4+load10*load15;
                  res5 = res5+load8*load15;
                  res6 = res6+load12*load14;
                  res7 = res7-load13*load14;
                  res6 = res6+load13*load15;
                  res7 = res7+load12*load15;
#endif
#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
                  load0 = ptrba[4*0+0];
                  load1 = ptrbb[4*0+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[4*0+1];
                  res1 = res1-load2*load1;
                  load3 = ptrbb[4*0+1];
                  res0 = res0-load2*load3;
                  res1 = res1-load0*load3;
                  load4 = ptrba[4*0+2];
                  res2 = res2+load4*load1;
                  load5 = ptrba[4*0+3];
                  res3 = res3-load5*load1;
                  res2 = res2-load5*load3;
                  res3 = res3-load4*load3;
                  load6 = ptrbb[4*0+2];
                  res4 = res4+load0*load6;
                  res5 = res5-load2*load6;
                  load7 = ptrbb[4*0+3];
                  res4 = res4-load2*load7;
                  res5 = res5-load0*load7;
                  res6 = res6+load4*load6;
                  res7 = res7-load5*load6;
                  res6 = res6-load5*load7;
                  res7 = res7-load4*load7;
                  load8 = ptrba[4*1+0];
                  load9 = ptrbb[4*1+0];
                  res0 = res0+load8*load9;
                  load10 = ptrba[4*1+1];
                  res1 = res1-load10*load9;
                  load11 = ptrbb[4*1+1];
                  res0 = res0-load10*load11;
                  res1 = res1-load8*load11;
                  load12 = ptrba[4*1+2];
                  res2 = res2+load12*load9;
                  load13 = ptrba[4*1+3];
                  res3 = res3-load13*load9;
                  res2 = res2-load13*load11;
                  res3 = res3-load12*load11;
                  load14 = ptrbb[4*1+2];
                  res4 = res4+load8*load14;
                  res5 = res5-load10*load14;
                  load15 = ptrbb[4*1+3];
                  res4 = res4-load10*load15;
                  res5 = res5-load8*load15;
                  res6 = res6+load12*load14;
                  res7 = res7-load13*load14;
                  res6 = res6-load13*load15;
                  res7 = res7-load12*load15;
                  load0 = ptrba[4*2+0];
                  load1 = ptrbb[4*2+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[4*2+1];
                  res1 = res1-load2*load1;
                  load3 = ptrbb[4*2+1];
                  res0 = res0-load2*load3;
                  res1 = res1-load0*load3;
                  load4 = ptrba[4*2+2];
                  res2 = res2+load4*load1;
                  load5 = ptrba[4*2+3];
                  res3 = res3-load5*load1;
                  res2 = res2-load5*load3;
                  res3 = res3-load4*load3;
                  load6 = ptrbb[4*2+2];
                  res4 = res4+load0*load6;
                  res5 = res5-load2*load6;
                  load7 = ptrbb[4*2+3];
                  res4 = res4-load2*load7;
                  res5 = res5-load0*load7;
                  res6 = res6+load4*load6;
                  res7 = res7-load5*load6;
                  res6 = res6-load5*load7;
                  res7 = res7-load4*load7;
                  load8 = ptrba[4*3+0];
                  load9 = ptrbb[4*3+0];
                  res0 = res0+load8*load9;
                  load10 = ptrba[4*3+1];
                  res1 = res1-load10*load9;
                  load11 = ptrbb[4*3+1];
                  res0 = res0-load10*load11;
                  res1 = res1-load8*load11;
                  load12 = ptrba[4*3+2];
                  res2 = res2+load12*load9;
                  load13 = ptrba[4*3+3];
                  res3 = res3-load13*load9;
                  res2 = res2-load13*load11;
                  res3 = res3-load12*load11;
                  load14 = ptrbb[4*3+2];
                  res4 = res4+load8*load14;
                  res5 = res5-load10*load14;
                  load15 = ptrbb[4*3+3];
                  res4 = res4-load10*load15;
                  res5 = res5-load8*load15;
                  res6 = res6+load12*load14;
                  res7 = res7-load13*load14;
                  res6 = res6-load13*load15;
                  res7 = res7-load12*load15;
#endif
                  ptrba = ptrba+16;
                  ptrbb = ptrbb+16;
               }
             for (k=0; k<(bk&3); k+=1)
               {
#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
                  load0 = ptrba[4*0+0];
                  load1 = ptrbb[4*0+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[4*0+1];
                  res1 = res1+load2*load1;
                  load3 = ptrbb[4*0+1];
                  res0 = res0-load2*load3;
                  res1 = res1+load0*load3;
                  load4 = ptrba[4*0+2];
                  res2 = res2+load4*load1;
                  load5 = ptrba[4*0+3];
                  res3 = res3+load5*load1;
                  res2 = res2-load5*load3;
                  res3 = res3+load4*load3;
                  load6 = ptrbb[4*0+2];
                  res4 = res4+load0*load6;
                  res5 = res5+load2*load6;
                  load7 = ptrbb[4*0+3];
                  res4 = res4-load2*load7;
                  res5 = res5+load0*load7;
                  res6 = res6+load4*load6;
                  res7 = res7+load5*load6;
                  res6 = res6-load5*load7;
                  res7 = res7+load4*load7;
#endif
#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
                  load0 = ptrba[4*0+0];
                  load1 = ptrbb[4*0+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[4*0+1];
                  res1 = res1+load2*load1;
                  load3 = ptrbb[4*0+1];
                  res0 = res0+load2*load3;
                  res1 = res1-load0*load3;
                  load4 = ptrba[4*0+2];
                  res2 = res2+load4*load1;
                  load5 = ptrba[4*0+3];
                  res3 = res3+load5*load1;
                  res2 = res2+load5*load3;
                  res3 = res3-load4*load3;
                  load6 = ptrbb[4*0+2];
                  res4 = res4+load0*load6;
                  res5 = res5+load2*load6;
                  load7 = ptrbb[4*0+3];
                  res4 = res4+load2*load7;
                  res5 = res5-load0*load7;
                  res6 = res6+load4*load6;
                  res7 = res7+load5*load6;
                  res6 = res6+load5*load7;
                  res7 = res7-load4*load7;
#endif
#if   defined(RN) || defined(RT) || defined(CN) || defined(CT)
                  load0 = ptrba[4*0+0];
                  load1 = ptrbb[4*0+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[4*0+1];
                  res1 = res1-load2*load1;
                  load3 = ptrbb[4*0+1];
                  res0 = res0+load2*load3;
                  res1 = res1+load0*load3;
                  load4 = ptrba[4*0+2];
                  res2 = res2+load4*load1;
                  load5 = ptrba[4*0+3];
                  res3 = res3-load5*load1;
                  res2 = res2+load5*load3;
                  res3 = res3+load4*load3;
                  load6 = ptrbb[4*0+2];
                  res4 = res4+load0*load6;
                  res5 = res5-load2*load6;
                  load7 = ptrbb[4*0+3];
                  res4 = res4+load2*load7;
                  res5 = res5+load0*load7;
                  res6 = res6+load4*load6;
                  res7 = res7-load5*load6;
                  res6 = res6+load5*load7;
                  res7 = res7+load4*load7;
#endif
#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
                  load0 = ptrba[4*0+0];
                  load1 = ptrbb[4*0+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[4*0+1];
                  res1 = res1-load2*load1;
                  load3 = ptrbb[4*0+1];
                  res0 = res0-load2*load3;
                  res1 = res1-load0*load3;
                  load4 = ptrba[4*0+2];
                  res2 = res2+load4*load1;
                  load5 = ptrba[4*0+3];
                  res3 = res3-load5*load1;
                  res2 = res2-load5*load3;
                  res3 = res3-load4*load3;
                  load6 = ptrbb[4*0+2];
                  res4 = res4+load0*load6;
                  res5 = res5-load2*load6;
                  load7 = ptrbb[4*0+3];
                  res4 = res4-load2*load7;
                  res5 = res5-load0*load7;
                  res6 = res6+load4*load6;
                  res7 = res7-load5*load6;
                  res6 = res6-load5*load7;
                  res7 = res7-load4*load7;
#endif
                  ptrba = ptrba+4;
                  ptrbb = ptrbb+4;
               }
             load0 = res0*alphar;
             C0[0] = C0[0]+load0;
             load1 = res1*alphar;
             C0[1] = C0[1]+load1;
             load0 = res1*alphai;
             C0[0] = C0[0]-load0;
             load1 = res0*alphai;
             C0[1] = C0[1]+load1;
             load2 = res2*alphar;
             C0[2] = C0[2]+load2;
             load3 = res3*alphar;
             C0[3] = C0[3]+load3;
             load2 = res3*alphai;
             C0[2] = C0[2]-load2;
             load3 = res2*alphai;
             C0[3] = C0[3]+load3;
             load4 = res4*alphar;
             C1[0] = C1[0]+load4;
             load5 = res5*alphar;
             C1[1] = C1[1]+load5;
             load4 = res5*alphai;
             C1[0] = C1[0]-load4;
             load5 = res4*alphai;
             C1[1] = C1[1]+load5;
             load6 = res6*alphar;
             C1[2] = C1[2]+load6;
             load7 = res7*alphar;
             C1[3] = C1[3]+load7;
             load6 = res7*alphai;
             C1[2] = C1[2]-load6;
             load7 = res6*alphai;
             C1[3] = C1[3]+load7;
             C0 = C0+4;
             C1 = C1+4;
          }
        for (i=0; i<(bm&1); i+=1)
          {
             ptrbb = bb;
             res0 = 0;
             res1 = 0;
             res2 = 0;
             res3 = 0;
             for (k=0; k<bk; k+=1)
               {
#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
                  load0 = ptrba[2*0+0];
                  load1 = ptrbb[4*0+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[2*0+1];
                  res1 = res1+load2*load1;
                  load3 = ptrbb[4*0+1];
                  res0 = res0-load2*load3;
                  res1 = res1+load0*load3;
                  load4 = ptrbb[4*0+2];
                  res2 = res2+load0*load4;
                  res3 = res3+load2*load4;
                  load5 = ptrbb[4*0+3];
                  res2 = res2-load2*load5;
                  res3 = res3+load0*load5;
#endif
#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
				  load0 = ptrba[2*0+0];
                  load1 = ptrbb[4*0+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[2*0+1];
                  res1 = res1+load2*load1;
                  load3 = ptrbb[4*0+1];
                  res0 = res0+load2*load3;
                  res1 = res1-load0*load3;
                  load4 = ptrbb[4*0+2];
                  res2 = res2+load0*load4;
                  res3 = res3+load2*load4;
                  load5 = ptrbb[4*0+3];
                  res2 = res2+load2*load5;
                  res3 = res3-load0*load5;
#endif
#if   defined(RN) || defined(RT) || defined(CN) || defined(CT)
                  load0 = ptrba[2*0+0];
                  load1 = ptrbb[4*0+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[2*0+1];
                  res1 = res1-load2*load1;
                  load3 = ptrbb[4*0+1];
                  res0 = res0+load2*load3;
                  res1 = res1+load0*load3;
                  load4 = ptrbb[4*0+2];
                  res2 = res2+load0*load4;
                  res3 = res3-load2*load4;
                  load5 = ptrbb[4*0+3];
                  res2 = res2+load2*load5;
                  res3 = res3+load0*load5;
#endif
#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
                  load0 = ptrba[2*0+0];
                  load1 = ptrbb[4*0+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[2*0+1];
                  res1 = res1-load2*load1;
                  load3 = ptrbb[4*0+1];
                  res0 = res0-load2*load3;
                  res1 = res1-load0*load3;
                  load4 = ptrbb[4*0+2];
                  res2 = res2+load0*load4;
                  res3 = res3-load2*load4;
                  load5 = ptrbb[4*0+3];
                  res2 = res2-load2*load5;
                  res3 = res3-load0*load5;
#endif
                  ptrba = ptrba+2;
                  ptrbb = ptrbb+4;
               }
             load0 = res0*alphar;
             C0[0] = C0[0]+load0;
             load1 = res1*alphar;
             C0[1] = C0[1]+load1;
             load0 = res1*alphai;
             C0[0] = C0[0]-load0;
             load1 = res0*alphai;
             C0[1] = C0[1]+load1;
             load2 = res2*alphar;
             C1[0] = C1[0]+load2;
             load3 = res3*alphar;
             C1[1] = C1[1]+load3;
             load2 = res3*alphai;
             C1[0] = C1[0]-load2;
             load3 = res2*alphai;
             C1[1] = C1[1]+load3;
             C0 = C0+2;
             C1 = C1+2;
          }
        k = (bk<<2);
        bb = bb+k;
        i = (ldc<<2);
        C = C+i;
     }
   for (j=0; j<(bn&1); j+=1)
     {
        C0 = C;
        ptrba = ba;
        for (i=0; i<bm/2; i+=1)
          {
             ptrbb = bb;
             res0 = 0;
             res1 = 0;
             res2 = 0;
             res3 = 0;
             for (k=0; k<bk; k+=1)
               {
#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
                  load0 = ptrba[4*0+0];
                  load1 = ptrbb[2*0+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[4*0+1];
                  res1 = res1+load2*load1;
                  load3 = ptrbb[2*0+1];
                  res0 = res0-load2*load3;
                  res1 = res1+load0*load3;
                  load4 = ptrba[4*0+2];
                  res2 = res2+load4*load1;
                  load5 = ptrba[4*0+3];
                  res3 = res3+load5*load1;
                  res2 = res2-load5*load3;
                  res3 = res3+load4*load3;
#endif
#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
                  load0 = ptrba[4*0+0];
                  load1 = ptrbb[2*0+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[4*0+1];
                  res1 = res1+load2*load1;
                  load3 = ptrbb[2*0+1];
                  res0 = res0+load2*load3;
                  res1 = res1-load0*load3;
                  load4 = ptrba[4*0+2];
                  res2 = res2+load4*load1;
                  load5 = ptrba[4*0+3];
                  res3 = res3+load5*load1;
                  res2 = res2+load5*load3;
                  res3 = res3-load4*load3;
#endif
#if   defined(RN) || defined(RT) || defined(CN) || defined(CT)
                  load0 = ptrba[4*0+0];
                  load1 = ptrbb[2*0+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[4*0+1];
                  res1 = res1-load2*load1;
                  load3 = ptrbb[2*0+1];
                  res0 = res0+load2*load3;
                  res1 = res1+load0*load3;
                  load4 = ptrba[4*0+2];
                  res2 = res2+load4*load1;
                  load5 = ptrba[4*0+3];
                  res3 = res3-load5*load1;
                  res2 = res2+load5*load3;
                  res3 = res3+load4*load3;
#endif
#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
                  load0 = ptrba[4*0+0];
                  load1 = ptrbb[2*0+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[4*0+1];
                  res1 = res1-load2*load1;
                  load3 = ptrbb[2*0+1];
                  res0 = res0-load2*load3;
                  res1 = res1-load0*load3;
                  load4 = ptrba[4*0+2];
                  res2 = res2+load4*load1;
                  load5 = ptrba[4*0+3];
                  res3 = res3-load5*load1;
                  res2 = res2-load5*load3;
                  res3 = res3-load4*load3;
#endif
                  ptrba = ptrba+4;
                  ptrbb = ptrbb+2;
               }
             load0 = res0*alphar;
             C0[0] = C0[0]+load0;
             load1 = res1*alphar;
             C0[1] = C0[1]+load1;
             load0 = res1*alphai;
             C0[0] = C0[0]-load0;
             load1 = res0*alphai;
             C0[1] = C0[1]+load1;
             load2 = res2*alphar;
             C0[2] = C0[2]+load2;
             load3 = res3*alphar;
             C0[3] = C0[3]+load3;
             load2 = res3*alphai;
             C0[2] = C0[2]-load2;
             load3 = res2*alphai;
             C0[3] = C0[3]+load3;
             C0 = C0+4;
          }
        for (i=0; i<(bm&1); i+=1)
          {
             ptrbb = bb;
             res0 = 0;
             res1 = 0;
             for (k=0; k<bk; k+=1)
               {
#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
                  load0 = ptrba[2*0+0];
                  load1 = ptrbb[2*0+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[2*0+1];
                  res1 = res1+load2*load1;
                  load3 = ptrbb[2*0+1];
                  res0 = res0-load2*load3;
                  res1 = res1+load0*load3;
#endif
#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
                  load0 = ptrba[2*0+0];
                  load1 = ptrbb[2*0+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[2*0+1];
                  res1 = res1+load2*load1;
                  load3 = ptrbb[2*0+1];
                  res0 = res0+load2*load3;
                  res1 = res1-load0*load3;
#endif
#if   defined(RN) || defined(RT) || defined(CN) || defined(CT)
                  load0 = ptrba[2*0+0];
                  load1 = ptrbb[2*0+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[2*0+1];
                  res1 = res1-load2*load1;
                  load3 = ptrbb[2*0+1];
                  res0 = res0+load2*load3;
                  res1 = res1+load0*load3;
#endif
#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
                  load0 = ptrba[2*0+0];
                  load1 = ptrbb[2*0+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[2*0+1];
                  res1 = res1-load2*load1;
                  load3 = ptrbb[2*0+1];
                  res0 = res0-load2*load3;
                  res1 = res1-load0*load3;
#endif
                  ptrba = ptrba+2;
                  ptrbb = ptrbb+2;
               }
             load0 = res0*alphar;
             C0[0] = C0[0]+load0;
             load1 = res1*alphar;
             C0[1] = C0[1]+load1;
             load0 = res1*alphai;
             C0[0] = C0[0]-load0;
             load1 = res0*alphai;
             C0[1] = C0[1]+load1;
             C0 = C0+2;
          }
        k = (bk<<1);
        bb = bb+k;
        i = (ldc<<1);
        C = C+i;
     }
   return 0;
}
