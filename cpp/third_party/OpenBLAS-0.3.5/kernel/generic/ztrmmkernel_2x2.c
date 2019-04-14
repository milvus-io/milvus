#include "common.h"
/********************************
  ADD1 a*c
  ADD2 b*c
  ADD3 a*d
  ADD4 b*d
 *********************************/
int CNAME(BLASLONG bm,BLASLONG bn,BLASLONG bk,FLOAT alphar,FLOAT alphai,FLOAT* ba,FLOAT* bb,
		FLOAT* C,BLASLONG ldc, BLASLONG offset)
{
	BLASLONG i,j,k;
	FLOAT *C0,*C1,*ptrba,*ptrbb;
	FLOAT res0,res1,res2,res3,res4,res5,res6,res7,load0,load1,load2,load3,load4,load5,load6,load7,load8,load9,load10,load11,load12,load13,load14,load15;
	BLASLONG off, temp;

#if defined(TRMMKERNEL) && !defined(LEFT)
	off = -offset;
#else
	off = 0;
#endif
	for (j=0; j<bn/2; j+=1)
	{
#if defined(TRMMKERNEL) &&  defined(LEFT)
		off = offset;
#endif
		C0 = C;
		C1 = C0+2*ldc;
		ptrba = ba;
		for (i=0; i<bm/2; i+=1)
		{
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
			ptrbb = bb;
#else
			ptrba += off*2*2;
			ptrbb = bb+off*2*2;
#endif
			res0 = 0;
			res1 = 0;
			res2 = 0;
			res3 = 0;
			res4 = 0;
			res5 = 0;
			res6 = 0;
			res7 = 0;
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
			temp = bk - off;
#elif defined(LEFT)
			temp = off + 2;
#else
			temp = off + 2;
#endif
			for (k=0; k<temp/4; k+=1)
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
			for (k=0; k<(temp&3); k+=1)
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
			load0 = res0*alphar-res1*alphai;
			load1 = res1*alphar+res0*alphai;
			C0[0] = load0;
			C0[1] = load1;

			load2 = res2*alphar-res3*alphai;
			load3 = res3*alphar+res2*alphai;
			C0[2] = load2;
			C0[3] = load3;

			load4 = res4*alphar-res5*alphai;
			load5 = res5*alphar+res4*alphai;
			C1[0] = load4;
			C1[1] = load5;

			load6 = res6*alphar-res7*alphai;
			load7 = res7*alphar+res6*alphai;
			C1[2] = load6;
			C1[3] = load7;
#if ( defined(LEFT) &&  defined(TRANSA)) || \
			(!defined(LEFT) && !defined(TRANSA))
			temp = bk - off;
#ifdef LEFT
			temp -= 2;
#else
			temp -= 2;
#endif
			ptrba += temp*2*2;
			ptrbb += temp*2*2;
#endif
#ifdef LEFT
			off += 2;
#endif

			C0 = C0+4;
			C1 = C1+4;
		}
		for (i=0; i<(bm&1); i+=1)
		{
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
			ptrbb = bb;
#else
			ptrba += off*2;
			ptrbb = bb + off*2*2;
#endif
			res0 = 0;
			res1 = 0;
			res2 = 0;
			res3 = 0;
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
			temp = bk - off;
#elif defined(LEFT)
			temp = off+1;
#else
			temp = off+2;
#endif
			for (k=0; k<temp; k+=1)
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
			load0 = res0*alphar-res1*alphai;
			load1 = res1*alphar+res0*alphai;
			C0[0] = load0;
			C0[1] = load1;

			load2 = res2*alphar-res3*alphai;
			load3 = res3*alphar+res2*alphai;
			C1[0] = load2;
			C1[1] = load3;
#if ( defined(LEFT) &&  defined(TRANSA)) || \
			(!defined(LEFT) && !defined(TRANSA))
			temp = bk - off;
#ifdef LEFT
			temp -= 1;
#else
			temp -= 2;
#endif
			ptrba += temp*2;
			ptrbb += temp*2*2;
#endif
#ifdef LEFT
			off += 1;
#endif
			C0 = C0+2;
			C1 = C1+2;
		}
#if defined(TRMMKERNEL) && !defined(LEFT)
		off += 2;
#endif
		k = (bk<<2);
		bb = bb+k;
		i = (ldc<<2);
		C = C+i;
	}
	for (j=0; j<(bn&1); j+=1)
	{
		C0 = C;
#if defined(TRMMKERNEL) &&  defined(LEFT)
		off = offset;
#endif
		ptrba = ba;
		for (i=0; i<bm/2; i+=1)
		{
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
			ptrbb = bb;
#else
			ptrba += off*2*2;
			ptrbb = bb+off*2;
#endif
			res0 = 0;
			res1 = 0;
			res2 = 0;
			res3 = 0;
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
			temp = bk - off;
#elif defined(LEFT)
			temp = off + 2;
#else
			temp = off + 1;
#endif
			for (k=0; k<temp; k+=1)
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
			load0 = res0*alphar-res1*alphai;
			load1 = res1*alphar+res0*alphai;
			C0[0] = load0;
			C0[1] = load1;

			load2 = res2*alphar-res3*alphai;
			load3 = res3*alphar+res2*alphai;
			C0[2] = load2;
			C0[3] = load3;
#if ( defined(LEFT) &&  defined(TRANSA)) || \
			(!defined(LEFT) && !defined(TRANSA))
			temp = bk-off;
#ifdef LEFT
			temp -= 2;
#else
			temp -= 1;
#endif
			ptrba += temp*2*2;
			ptrbb += temp*2;
#endif
#ifdef LEFT
			off += 2;
#endif
			C0 = C0+4;
		}
		for (i=0; i<(bm&1); i+=1)
		{
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
			ptrbb = bb;
#else
			ptrba += off*2;
			ptrbb = bb + off*2;
#endif
			res0 = 0;
			res1 = 0;
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
			temp = bk-off;
#elif defined(LEFT)
			temp = off + 1;
#else
			temp = off + 1;
#endif
			for (k=0; k<temp; k+=1)
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
			load0 = res0*alphar-res1*alphai;
			load1 = res1*alphar+res0*alphai;
			C0[0] = load0;
			C0[1] = load1;
#if ( defined(LEFT) &&  defined(TRANSA)) || \
			(!defined(LEFT) && !defined(TRANSA))
			temp = bk - off;
#ifdef LEFT
			temp -= 1;
#else
			temp -= 1;
#endif
			ptrba += temp*2;
			ptrbb += temp*2;
#endif
#ifdef LEFT
			off += 1;
#endif
			C0 = C0+2;
		}
		k = (bk<<1);
		bb = bb+k;
		i = (ldc<<1);
		C = C+i;
	}
	return 0;
}
