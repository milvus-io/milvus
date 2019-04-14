#include "common.h"
int CNAME(BLASLONG bm,BLASLONG bn,BLASLONG bk,FLOAT alpha,FLOAT* ba,FLOAT* bb,FLOAT* C,BLASLONG ldc
#ifdef TRMMKERNEL
		,BLASLONG offset
#endif
		)
{
   BLASLONG i,j,k;
   FLOAT *C0,*C1,*ptrba,*ptrbb;
   FLOAT res0,res1,res2,res3,load0,load1,load2,load3,load4,load5,load6,load7;
   BLASLONG off, temp;
#if defined(TRMMKERNEL) && !defined(LEFT)
   off = -offset;
#else
   off = 0;
#endif
   for (j=0; j<bn/2; j+=1)
     {
        C0 = C;
        C1 = C0+ldc;
#if defined(TRMMKERNEL) && defined(LEFT)
		off = offset;
#endif
        ptrba = ba;
        for (i=0; i<bm/2; i+=1)
          {
#if (defined(LEFT) &&  defined(TRANSA)) || \
			  (!defined(LEFT) && !defined(TRANSA))
             ptrbb = bb;
#else
			  ptrba += off*2;
			  ptrbb = bb + off*2;
#endif
             res0 = 0;
             res1 = 0;
             res2 = 0;
             res3 = 0;
#if (defined(LEFT) && !defined(TRANSA)) || \
			 (!defined(LEFT) && defined(TRANSA))
			 temp = bk-off;
#elif defined(LEFT)
			 temp = off+2;
#else
			 temp = off+2;
#endif
             for (k=0; k<temp/4; k+=1)
               {
                  load0 = ptrba[2*0+0];
                  load1 = ptrbb[2*0+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[2*0+1];
                  res1 = res1+load2*load1;
                  load3 = ptrbb[2*0+1];
                  res2 = res2+load0*load3;
                  res3 = res3+load2*load3;
                  load4 = ptrba[2*1+0];
                  load5 = ptrbb[2*1+0];
                  res0 = res0+load4*load5;
                  load6 = ptrba[2*1+1];
                  res1 = res1+load6*load5;
                  load7 = ptrbb[2*1+1];
                  res2 = res2+load4*load7;
                  res3 = res3+load6*load7;
                  load0 = ptrba[2*2+0];
                  load1 = ptrbb[2*2+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[2*2+1];
                  res1 = res1+load2*load1;
                  load3 = ptrbb[2*2+1];
                  res2 = res2+load0*load3;
                  res3 = res3+load2*load3;
                  load4 = ptrba[2*3+0];
                  load5 = ptrbb[2*3+0];
                  res0 = res0+load4*load5;
                  load6 = ptrba[2*3+1];
                  res1 = res1+load6*load5;
                  load7 = ptrbb[2*3+1];
                  res2 = res2+load4*load7;
                  res3 = res3+load6*load7;
                  ptrba = ptrba+8;
                  ptrbb = ptrbb+8;
               }
             for (k=0; k<(temp&3); k+=1)
               {
                  load0 = ptrba[2*0+0];
                  load1 = ptrbb[2*0+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[2*0+1];
                  res1 = res1+load2*load1;
                  load3 = ptrbb[2*0+1];
                  res2 = res2+load0*load3;
                  res3 = res3+load2*load3;
                  ptrba = ptrba+2;
                  ptrbb = ptrbb+2;
               }
             res0 = res0*alpha;
             C0[0] = res0;
             res1 = res1*alpha;
             C0[1] = res1;
             res2 = res2*alpha;
             C1[0] = res2;
             res3 = res3*alpha;
             C1[1] = res3;
#if ( defined(LEFT) && defined(TRANSA)) || \
			 (!defined(LEFT) && !defined(TRANSA))
			 temp = bk - off;
#ifdef LEFT
			 temp -= 2;
#else
			 temp -= 2;
#endif
			 ptrba += temp*2;
			 ptrbb += temp*2;
#endif
#ifdef LEFT
			 off += 2;
#endif
             C0 = C0+2;
             C1 = C1+2;
          }
        for (i=0; i<(bm&1); i+=1)
          {
#if (defined(LEFT) &&  defined(TRANSA)) ||(!defined(LEFT) && !defined(TRANSA))
             ptrbb = bb;
#else
			 ptrba += off;
			 ptrbb = bb+off*2;
#endif
             res0 = 0;
             res1 = 0;
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
			 temp = bk-off;
#elif defined(LEFT)
			 temp = off+1;
#else
			 temp = off+2;
#endif
             for (k=0; k<temp; k+=1)
               {
                  load0 = ptrba[0+0];
                  load1 = ptrbb[2*0+0];
                  res0 = res0+load0*load1;
                  load2 = ptrbb[2*0+1];
                  res1 = res1+load0*load2;
                  ptrba = ptrba+1;
                  ptrbb = ptrbb+2;
               }
             res0 = res0*alpha;
             C0[0] = res0;
             res1 = res1*alpha;
             C1[0] = res1;
#if ( defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
			 temp = bk-off;
#ifdef LEFT
			 temp -= 1;
#else
			 temp -= 2;
#endif
			 ptrba += temp;
			 ptrbb += temp*2;
#endif
#ifdef LEFT
			 off += 1;
#endif
             C0 = C0+1;
             C1 = C1+1;
          }
#if defined(TRMMKERNEL) && !defined(LEFT)
		off += 2;
#endif
        k = (bk<<1);
        bb = bb+k;
        i = (ldc<<1);
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
#if (defined(LEFT) &&  defined(TRANSA)) || \
			  (!defined(LEFT) && !defined(TRANSA))
			  ptrbb = bb;
#else
			  ptrba += off*2;
			  ptrbb = bb + off;
#endif
             res0 = 0;
             res1 = 0;
#if (defined(LEFT) && !defined(TRANSA)) || \
			 (!defined(LEFT) && defined(TRANSA))
			 temp = bk-off;
#elif defined(LEFT)
			 temp = off+2;
#else
			 temp = off+1;
#endif
			 for (k=0; k<temp; k+=1)
               {
                  load0 = ptrba[2*0+0];
                  load1 = ptrbb[0+0];
                  res0 = res0+load0*load1;
                  load2 = ptrba[2*0+1];
                  res1 = res1+load2*load1;
                  ptrba = ptrba+2;
                  ptrbb = ptrbb+1;
               }
             res0 = res0*alpha;
             C0[0] = res0;
             res1 = res1*alpha;
			 C0[1] = res1;
#if ( defined(LEFT) &&  defined(TRANSA)) || \
			 (!defined(LEFT) && !defined(TRANSA))
			 temp = bk - off;
#ifdef LEFT
			 temp -= 2;
#else
			 temp -= 1;
#endif
			 ptrba += temp*2;
			 ptrbb += temp;
#endif
#ifdef LEFT
			 off += 2;
#endif

             C0 = C0+2;
          }
        for (i=0; i<(bm&1); i+=1)
          {
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
             ptrbb = bb;
#else
			 ptrba += off;
			 ptrbb = bb+off;
#endif
             res0 = 0;
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
			 temp = bk-off;
#elif defined(LEFT)
			 temp = off + 1;
#else
			 temp = off + 1;
#endif
			 for (k=0; k<temp; k+=1)
               {
                  load0 = ptrba[0+0];
                  load1 = ptrbb[0+0];
                  res0 = res0+load0*load1;
                  ptrba = ptrba+1;
                  ptrbb = ptrbb+1;
               }
             res0 = res0*alpha;
             C0[0] = res0;
#if ( defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
			 temp = bk-off;
#ifdef LEFT
			 temp -= 1;
#else
			 temp -= 1;
#endif
			 ptrba += temp;
			 ptrbb += temp;
#endif
#ifdef LEFT
			 off += 1;
#endif
             C0 = C0+1;
          }
#if defined(TRMMKERNEL) && !defined(LEFT)
		off += 1;
#endif
        k = (bk<<0);
        bb = bb+k;
        C = C+ldc;
     }
   return 0;
}
