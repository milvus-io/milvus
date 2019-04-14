#include "common.h"
#include <stdbool.h>


static void dtrmm_kernel_4x8( BLASLONG n, FLOAT *alpha ,FLOAT *a, FLOAT *b, FLOAT *C0, FLOAT *C1, FLOAT *C2,FLOAT *C3, FLOAT *C4, FLOAT *C5,FLOAT *C6, FLOAT *C7) __attribute__ ((noinline));

static void dtrmm_kernel_4x8( BLASLONG n, FLOAT *alpha ,FLOAT *a, FLOAT *b, FLOAT *C0, FLOAT *C1, FLOAT *C2,FLOAT *C3, FLOAT *C4, FLOAT *C5,FLOAT *C6, FLOAT *C7)
{

		BLASLONG i = 0;
		BLASLONG temp1 = n * 8;

		 __asm__  __volatile__
        	(
		"	vxorpd	%%ymm4 , %%ymm4 , %%ymm4			\n\t"
		"	vxorpd	%%ymm5 , %%ymm5 , %%ymm5			\n\t"
		"	vxorpd	%%ymm6 , %%ymm6 , %%ymm6			\n\t"
		"	vxorpd	%%ymm7 , %%ymm7 , %%ymm7			\n\t"
		"	vxorpd	%%ymm8 , %%ymm8 , %%ymm8			\n\t"
		"	vxorpd	%%ymm9 , %%ymm9 , %%ymm9			\n\t"
		"	vxorpd	%%ymm10, %%ymm10, %%ymm10			\n\t"
		"	vxorpd	%%ymm11, %%ymm11, %%ymm11			\n\t"

		"	cmp $0, %1						\n\t"
		"	jz 2f							\n\t"

		"	.p2align 4						\n\t"
		"1:								\n\t"
		"	vmovups   	(%2,%0,4) , %%ymm0			\n\t"
		"	vmovups   	(%3,%0,8) , %%ymm1			\n\t"
		"	vmovups       32(%3,%0,8) , %%ymm2			\n\t"

		"	vfmadd231pd	%%ymm0 , %%ymm1 , %%ymm4		\n\t"
		"	vfmadd231pd	%%ymm0 , %%ymm2 , %%ymm8		\n\t"

		"	vpermpd         $0xb1  , %%ymm0 , %%ymm0		\n\t"
		"	vfmadd231pd	%%ymm0 , %%ymm1 , %%ymm5		\n\t"
		"	vfmadd231pd	%%ymm0 , %%ymm2 , %%ymm9		\n\t"

		"	vpermpd         $0x1b  , %%ymm0 , %%ymm0		\n\t"
		"	vfmadd231pd	%%ymm0 , %%ymm1 , %%ymm6		\n\t"
		"	vfmadd231pd	%%ymm0 , %%ymm2 , %%ymm10		\n\t"

		"	vpermpd         $0xb1  , %%ymm0 , %%ymm0		\n\t"
		"	vfmadd231pd	%%ymm0 , %%ymm1 , %%ymm7		\n\t"
		"	vfmadd231pd	%%ymm0 , %%ymm2 , %%ymm11		\n\t"

		"	addq	$8 , %0						\n\t"
		"	cmp	%0 , %1						\n\t"
		"	jne	1b						\n\t"

		"2:								\n\t"

		"	vbroadcastsd	(%4), %%ymm0				\n\t"

		"	vmulpd		%%ymm0 , %%ymm4 , %%ymm4		\n\t"
		"	vmulpd		%%ymm0 , %%ymm5 , %%ymm5		\n\t"
		"	vmulpd		%%ymm0 , %%ymm6 , %%ymm6		\n\t"
		"	vmulpd		%%ymm0 , %%ymm7 , %%ymm7		\n\t"
		"	vmulpd		%%ymm0 , %%ymm8 , %%ymm8		\n\t"
		"	vmulpd		%%ymm0 , %%ymm9 , %%ymm9		\n\t"
		"	vmulpd		%%ymm0 , %%ymm10, %%ymm10		\n\t"
		"	vmulpd		%%ymm0 , %%ymm11, %%ymm11		\n\t"

		"	vpermpd 	$0xb1  , %%ymm5 , %%ymm5		\n\t"
		"	vpermpd 	$0xb1  , %%ymm7 , %%ymm7		\n\t"

		"	vblendpd 	$0x0a  , %%ymm5 , %%ymm4 , %%ymm0	\n\t"
		"	vblendpd 	$0x05  , %%ymm5 , %%ymm4 , %%ymm1	\n\t"
		"	vblendpd 	$0x0a  , %%ymm7 , %%ymm6 , %%ymm2	\n\t"
		"	vblendpd 	$0x05  , %%ymm7 , %%ymm6 , %%ymm3	\n\t"

		"	vpermpd 	$0x1b  , %%ymm2 , %%ymm2		\n\t"
		"	vpermpd 	$0x1b  , %%ymm3 , %%ymm3		\n\t"
		"	vpermpd 	$0xb1  , %%ymm2 , %%ymm2		\n\t"
		"	vpermpd 	$0xb1  , %%ymm3 , %%ymm3		\n\t"

		"	vblendpd 	$0x03  , %%ymm0 , %%ymm2 , %%ymm4	\n\t"
		"	vblendpd 	$0x03  , %%ymm1 , %%ymm3 , %%ymm5	\n\t"
		"	vblendpd 	$0x03  , %%ymm2 , %%ymm0 , %%ymm6	\n\t"
		"	vblendpd 	$0x03  , %%ymm3 , %%ymm1 , %%ymm7	\n\t"

		"	vmovups		%%ymm4 , (%5)				\n\t"
		"	vmovups		%%ymm5 , (%6)				\n\t"
		"	vmovups		%%ymm6 , (%7)				\n\t"
		"	vmovups		%%ymm7 , (%8)				\n\t"

		"	vpermpd 	$0xb1  , %%ymm9 , %%ymm9		\n\t"
		"	vpermpd 	$0xb1  , %%ymm11, %%ymm11		\n\t"

		"	vblendpd 	$0x0a  , %%ymm9 , %%ymm8 , %%ymm0	\n\t"
		"	vblendpd 	$0x05  , %%ymm9 , %%ymm8 , %%ymm1	\n\t"
		"	vblendpd 	$0x0a  , %%ymm11, %%ymm10, %%ymm2	\n\t"
		"	vblendpd 	$0x05  , %%ymm11, %%ymm10, %%ymm3	\n\t"

		"	vpermpd 	$0x1b  , %%ymm2 , %%ymm2		\n\t"
		"	vpermpd 	$0x1b  , %%ymm3 , %%ymm3		\n\t"
		"	vpermpd 	$0xb1  , %%ymm2 , %%ymm2		\n\t"
		"	vpermpd 	$0xb1  , %%ymm3 , %%ymm3		\n\t"

		"	vblendpd 	$0x03  , %%ymm0 , %%ymm2 , %%ymm4	\n\t"
		"	vblendpd 	$0x03  , %%ymm1 , %%ymm3 , %%ymm5	\n\t"
		"	vblendpd 	$0x03  , %%ymm2 , %%ymm0 , %%ymm6	\n\t"
		"	vblendpd 	$0x03  , %%ymm3 , %%ymm1 , %%ymm7	\n\t"

		"	vmovups		%%ymm4 , (%9)				\n\t"
		"	vmovups		%%ymm5 , (%10)				\n\t"
		"	vmovups		%%ymm6 , (%11)				\n\t"
		"	vmovups		%%ymm7 , (%12)				\n\t"

	        :
        	:
		"a" (i),	 // 0
          	"r" (temp1),     // 1    
          	"S" (a),         // 2
          	"D" (b),         // 3
          	"r" (alpha),     // 4
		"r" (C0),	 // 5
		"r" (C1),	 // 6
		"r" (C2),	 // 7
		"r" (C3),	 // 8
		"r" (C4),	 // 9
		"r" (C5),	 // 10
		"r" (C6),	 // 11
		"r" (C7) 	 // 12
		: "cc",
          	"%xmm0", "%xmm1", "%xmm2", "%xmm3",
          	"%xmm4", "%xmm5", "%xmm6", "%xmm7",
          	"%xmm8", "%xmm9", "%xmm10", "%xmm11",
          	"%xmm12", "%xmm13", "%xmm14", "%xmm15",
          	"memory"
        	);




}




int CNAME(BLASLONG bm,BLASLONG bn,BLASLONG bk,FLOAT alpha,FLOAT* ba,FLOAT* bb,FLOAT* C,BLASLONG ldc ,BLASLONG offset)
{

   BLASLONG i,j,k;
   FLOAT *C0,*C1,*C2,*C3,*C4,*C5,*C6,*C7,*ptrba,*ptrbb;

   FLOAT res0_0;
   FLOAT res0_1;
   FLOAT res0_2;
   FLOAT res0_3;

   FLOAT res1_0;
   FLOAT res1_1;
   FLOAT res1_2;
   FLOAT res1_3;

   FLOAT res2_0;
   FLOAT res2_1;
   FLOAT res2_2;
   FLOAT res2_3;

   FLOAT res3_0;
   FLOAT res3_1;
   FLOAT res3_2;
   FLOAT res3_3;

   FLOAT res4_0;
   FLOAT res4_1;
/*
   FLOAT res4_2;
   FLOAT res4_3;
*/
   FLOAT res5_0;
   FLOAT res5_1;
/*
   FLOAT res5_2;
   FLOAT res5_3;
*/
   FLOAT res6_0;
   FLOAT res6_1;
/*
   FLOAT res6_2;
   FLOAT res6_3;
*/
   FLOAT res7_0;
   FLOAT res7_1;
/*
   FLOAT res7_2;
   FLOAT res7_3;
*/
   FLOAT a0;
   FLOAT a1;

   FLOAT b0;
   FLOAT b1;
   FLOAT b2;
   FLOAT b3;
   FLOAT b4;
   FLOAT b5;
   FLOAT b6;
   FLOAT b7;

   BLASLONG off, temp ;

   bool left;
   bool transposed;
   bool backwards;

#ifdef LEFT
   left = true;
#else
   left = false;
#endif

#ifdef TRANSA
   transposed = true;
#else
   transposed = false;
#endif

   backwards = left != transposed;

   if (!left) {
      off = -offset;
   }


   for (j=0; j<bn/8; j+=1) // do blocks of the Mx8 loops 
   {
        C0 = C;
        C1 = C0+ldc;
        C2 = C1+ldc;
        C3 = C2+ldc;
        C4 = C3+ldc;
        C5 = C4+ldc;
        C6 = C5+ldc;
        C7 = C6+ldc;


        if (left) {
            off = offset;
        }

        ptrba = ba;

        for (i=0; i<bm/4; i+=1) // do blocks of 4x4
	{

		ptrbb = bb;
                if (backwards)
                {
		   ptrba += off*4; // number of values in A
		   ptrbb += off*8; // number of values in B
                }
/*
		res0_0 = 0;
		res0_1 = 0;
		res0_2 = 0;
		res0_3 = 0;

		res1_0 = 0;
		res1_1 = 0;
		res1_2 = 0;
		res1_3 = 0;

		res2_0 = 0;
		res2_1 = 0;
		res2_2 = 0;
		res2_3 = 0;

		res3_0 = 0;
		res3_1 = 0;
		res3_2 = 0;
		res3_3 = 0;

		res4_0 = 0;
		res4_1 = 0;
		res4_2 = 0;
		res4_3 = 0;

		res5_0 = 0;
		res5_1 = 0;
		res5_2 = 0;
		res5_3 = 0;

		res6_0 = 0;
		res6_1 = 0;
		res6_2 = 0;
		res6_3 = 0;

		res7_0 = 0;
		res7_1 = 0;
		res7_2 = 0;
		res7_3 = 0;
*/
                temp = backwards ? bk-off :
                             left ? off + 4 : // number of values in A
                                    off + 8;  // number of values in B

	        dtrmm_kernel_4x8( temp, &alpha , ptrba, ptrbb, C0, C1, C2, C3, C4, C5, C6, C7);

		ptrba = ptrba + temp * 4;
		ptrbb = ptrbb + temp * 8;

/*
		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];
			b1 = ptrbb[1];
			b2 = ptrbb[2];
			b3 = ptrbb[3];
			b4 = ptrbb[4];
			b5 = ptrbb[5];
			b6 = ptrbb[6];
			b7 = ptrbb[7];

			a0 = ptrba[0];
			res0_0 += a0*b0;
			res1_0 += a0*b1;
			res2_0 += a0*b2;
			res3_0 += a0*b3;
			res4_0 += a0*b4;
			res5_0 += a0*b5;
			res6_0 += a0*b6;
			res7_0 += a0*b7;

			a1 = ptrba[1];
			res0_1 += a1*b0;
			res1_1 += a1*b1;
			res2_1 += a1*b2;
			res3_1 += a1*b3;
			res4_1 += a1*b4;
			res5_1 += a1*b5;
			res6_1 += a1*b6;
			res7_1 += a1*b7;

			a0 = ptrba[2];
			res0_2 += a0*b0;
			res1_2 += a0*b1;
			res2_2 += a0*b2;
			res3_2 += a0*b3;
			res4_2 += a0*b4;
			res5_2 += a0*b5;
			res6_2 += a0*b6;
			res7_2 += a0*b7;

			a1 = ptrba[3];
			res0_3 += a1*b0;
			res1_3 += a1*b1;
			res2_3 += a1*b2;
			res3_3 += a1*b3;
			res4_3 += a1*b4;
			res5_3 += a1*b5;
			res6_3 += a1*b6;
			res7_3 += a1*b7;

			ptrba = ptrba+4;
			ptrbb = ptrbb+8;
                }

		res0_0 *= alpha;
		res0_1 *= alpha;
		res0_2 *= alpha;
		res0_3 *= alpha;

		res1_0 *= alpha;
		res1_1 *= alpha;
		res1_2 *= alpha;
		res1_3 *= alpha;

		res2_0 *= alpha;
		res2_1 *= alpha;
		res2_2 *= alpha;
		res2_3 *= alpha;

		res3_0 *= alpha;
		res3_1 *= alpha;
		res3_2 *= alpha;
		res3_3 *= alpha;

		res4_0 *= alpha;
		res4_1 *= alpha;
		res4_2 *= alpha;
		res4_3 *= alpha;

		res5_0 *= alpha;
		res5_1 *= alpha;
		res5_2 *= alpha;
		res5_3 *= alpha;

		res6_0 *= alpha;
		res6_1 *= alpha;
		res6_2 *= alpha;
		res6_3 *= alpha;

		res7_0 *= alpha;
		res7_1 *= alpha;
		res7_2 *= alpha;
		res7_3 *= alpha;


		C0[0] = res0_0;
		C0[1] = res0_1;
		C0[2] = res0_2;
		C0[3] = res0_3;

		C1[0] = res1_0;
		C1[1] = res1_1;
		C1[2] = res1_2;
		C1[3] = res1_3;

		C2[0] = res2_0;
		C2[1] = res2_1;
		C2[2] = res2_2;
		C2[3] = res2_3;

		C3[0] = res3_0;
		C3[1] = res3_1;
		C3[2] = res3_2;
		C3[3] = res3_3;

		C4[0] = res4_0;
		C4[1] = res4_1;
		C4[2] = res4_2;
		C4[3] = res4_3;

		C5[0] = res5_0;
		C5[1] = res5_1;
		C5[2] = res5_2;
		C5[3] = res5_3;

		C6[0] = res6_0;
		C6[1] = res6_1;
		C6[2] = res6_2;
		C6[3] = res6_3;

		C7[0] = res7_0;
		C7[1] = res7_1;
		C7[2] = res7_2;
		C7[3] = res7_3;
*/
		if (!backwards) {
                    temp = bk-off;
                    temp = left ? temp - 4 : // number of values in A
                                  temp - 8;  // number of values in B

                    ptrba += temp*4; // number of values in A
		    ptrbb += temp*8; // number of values in B
                }
#ifdef LEFT
		off += 4; // number of values in A
#endif

		C0 = C0+4;
		C1 = C1+4;
		C2 = C2+4;
		C3 = C3+4;
		C4 = C4+4;
		C5 = C5+4;
		C6 = C6+4;
		C7 = C7+4;

	}

	if ( bm & 2 ) // do any 2x4 loop
	{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*2;
		ptrbb = bb + off*8;
#endif

		res0_0 = 0;
		res0_1 = 0;

		res1_0 = 0;
		res1_1 = 0;

		res2_0 = 0;
		res2_1 = 0;

		res3_0 = 0;
		res3_1 = 0;

		res4_0 = 0;
		res4_1 = 0;

		res5_0 = 0;
		res5_1 = 0;

		res6_0 = 0;
		res6_1 = 0;

		res7_0 = 0;
		res7_1 = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+2;	// number of values in A
#else
		temp = off+8;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];
			b1 = ptrbb[1];
			b2 = ptrbb[2];
			b3 = ptrbb[3];
			b4 = ptrbb[4];
			b5 = ptrbb[5];
			b6 = ptrbb[6];
			b7 = ptrbb[7];

			a0 = ptrba[0];
			res0_0 += a0*b0;
			res1_0 += a0*b1;
			res2_0 += a0*b2;
			res3_0 += a0*b3;
			res4_0 += a0*b4;
			res5_0 += a0*b5;
			res6_0 += a0*b6;
			res7_0 += a0*b7;

			a1 = ptrba[1];
			res0_1 += a1*b0;
			res1_1 += a1*b1;
			res2_1 += a1*b2;
			res3_1 += a1*b3;
			res4_1 += a1*b4;
			res5_1 += a1*b5;
			res6_1 += a1*b6;
			res7_1 += a1*b7;

			ptrba = ptrba+2;
			ptrbb = ptrbb+8;
                }

		res0_0 *= alpha;
		res0_1 *= alpha;

		res1_0 *= alpha;
		res1_1 *= alpha;

		res2_0 *= alpha;
		res2_1 *= alpha;

		res3_0 *= alpha;
		res3_1 *= alpha;

		res4_0 *= alpha;
		res4_1 *= alpha;

		res5_0 *= alpha;
		res5_1 *= alpha;

		res6_0 *= alpha;
		res6_1 *= alpha;

		res7_0 *= alpha;
		res7_1 *= alpha;

		C0[0] = res0_0;
		C0[1] = res0_1;

		C1[0] = res1_0;
		C1[1] = res1_1;

		C2[0] = res2_0;
		C2[1] = res2_1;

		C3[0] = res3_0;
		C3[1] = res3_1;

		C4[0] = res4_0;
		C4[1] = res4_1;

		C5[0] = res5_0;
		C5[1] = res5_1;

		C6[0] = res6_0;
		C6[1] = res6_1;

		C7[0] = res7_0;
		C7[1] = res7_1;


#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 2; // number of values in A
#else
		temp -= 8; // number of values in B
#endif
		ptrba += temp*2;
		ptrbb += temp*8;
#endif

#ifdef LEFT
		off += 2; // number of values in A
#endif

		C0 = C0+2;
		C1 = C1+2;
		C2 = C2+2;
		C3 = C3+2;
		C4 = C4+2;
		C5 = C5+2;
		C6 = C6+2;
		C7 = C7+2;

	}

	if ( bm & 1 ) // do any 1x4 loop
	{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*1;
		ptrbb = bb + off*8;
#endif

		res0_0 = 0;
		res1_0 = 0;
		res2_0 = 0;
		res3_0 = 0;
		res4_0 = 0;
		res5_0 = 0;
		res6_0 = 0;
		res7_0 = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+1;	// number of values in A
#else
		temp = off+8;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];
			b1 = ptrbb[1];
			b2 = ptrbb[2];
			b3 = ptrbb[3];
			b4 = ptrbb[4];
			b5 = ptrbb[5];
			b6 = ptrbb[6];
			b7 = ptrbb[7];

			a0 = ptrba[0];
			res0_0 += a0*b0;
			res1_0 += a0*b1;
			res2_0 += a0*b2;
			res3_0 += a0*b3;
			res4_0 += a0*b4;
			res5_0 += a0*b5;
			res6_0 += a0*b6;
			res7_0 += a0*b7;

			ptrba = ptrba+1;
			ptrbb = ptrbb+8;
                }

		res0_0 *= alpha;

		res1_0 *= alpha;

		res2_0 *= alpha;

		res3_0 *= alpha;
		res4_0 *= alpha;
		res5_0 *= alpha;
		res6_0 *= alpha;
		res7_0 *= alpha;

		C0[0] = res0_0;

		C1[0] = res1_0;

		C2[0] = res2_0;

		C3[0] = res3_0;
		C4[0] = res4_0;
		C5[0] = res5_0;
		C6[0] = res6_0;
		C7[0] = res7_0;


#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 1; // number of values in A
#else
		temp -= 8; // number of values in B
#endif
		ptrba += temp*1;
		ptrbb += temp*8;
#endif

#ifdef LEFT
		off += 1; // number of values in A
#endif

		C0 = C0+1;
		C1 = C1+1;
		C2 = C2+1;
		C3 = C3+1;
		C4 = C4+1;
		C5 = C5+1;
		C6 = C6+1;
		C7 = C7+1;

	}


#if defined(TRMMKERNEL) && !defined(LEFT)
		off += 8;
#endif

        k = (bk<<3);
        bb = bb+k;
        i = (ldc<<3);
        C = C+i;
    }



   for (j=0; j<(bn&4); j+=4) // do blocks of the Mx4 loops 
   {
        C0 = C;
        C1 = C0+ldc;
        C2 = C1+ldc;
        C3 = C2+ldc;


        if (left) {
            off = offset;
        }

        ptrba = ba;

        for (i=0; i<bm/4; i+=1) // do blocks of 4x4
	{

		ptrbb = bb;
                if (backwards)
                {
		   ptrba += off*4; // number of values in A
		   ptrbb += off*4; // number of values in B
                }

		res0_0 = 0;
		res0_1 = 0;
		res0_2 = 0;
		res0_3 = 0;

		res1_0 = 0;
		res1_1 = 0;
		res1_2 = 0;
		res1_3 = 0;

		res2_0 = 0;
		res2_1 = 0;
		res2_2 = 0;
		res2_3 = 0;

		res3_0 = 0;
		res3_1 = 0;
		res3_2 = 0;
		res3_3 = 0;

                temp = backwards ? bk-off : off + 4;
                            /* left ? off + 4 : // number of values in A
                                    off + 4;  // number of values in B */

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];
			b1 = ptrbb[1];
			b2 = ptrbb[2];
			b3 = ptrbb[3];

			a0 = ptrba[0];
			res0_0 += a0*b0;
			res1_0 += a0*b1;
			res2_0 += a0*b2;
			res3_0 += a0*b3;

			a1 = ptrba[1];
			res0_1 += a1*b0;
			res1_1 += a1*b1;
			res2_1 += a1*b2;
			res3_1 += a1*b3;

			a0 = ptrba[2];
			res0_2 += a0*b0;
			res1_2 += a0*b1;
			res2_2 += a0*b2;
			res3_2 += a0*b3;

			a1 = ptrba[3];
			res0_3 += a1*b0;
			res1_3 += a1*b1;
			res2_3 += a1*b2;
			res3_3 += a1*b3;

			ptrba = ptrba+4;
			ptrbb = ptrbb+4;
                }

		res0_0 *= alpha;
		res0_1 *= alpha;
		res0_2 *= alpha;
		res0_3 *= alpha;

		res1_0 *= alpha;
		res1_1 *= alpha;
		res1_2 *= alpha;
		res1_3 *= alpha;

		res2_0 *= alpha;
		res2_1 *= alpha;
		res2_2 *= alpha;
		res2_3 *= alpha;

		res3_0 *= alpha;
		res3_1 *= alpha;
		res3_2 *= alpha;
		res3_3 *= alpha;

		C0[0] = res0_0;
		C0[1] = res0_1;
		C0[2] = res0_2;
		C0[3] = res0_3;

		C1[0] = res1_0;
		C1[1] = res1_1;
		C1[2] = res1_2;
		C1[3] = res1_3;

		C2[0] = res2_0;
		C2[1] = res2_1;
		C2[2] = res2_2;
		C2[3] = res2_3;

		C3[0] = res3_0;
		C3[1] = res3_1;
		C3[2] = res3_2;
		C3[3] = res3_3;

		if (!backwards) {
                    temp = bk-off - 4;
                    /* temp = left ? temp - 4 : // number of values in A
                                  temp - 4;  // number of values in B */

                    ptrba += temp*4; // number of values in A
		    ptrbb += temp*4; // number of values in B
                }
#ifdef LEFT
		off += 4; // number of values in A
#endif

		C0 = C0+4;
		C1 = C1+4;
		C2 = C2+4;
		C3 = C3+4;

	}

	if ( bm & 2 ) // do any 2x4 loop
	{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*2;
		ptrbb = bb + off*4;
#endif

		res0_0 = 0;
		res0_1 = 0;

		res1_0 = 0;
		res1_1 = 0;

		res2_0 = 0;
		res2_1 = 0;

		res3_0 = 0;
		res3_1 = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+2;	// number of values in A
#else
		temp = off+4;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];
			b1 = ptrbb[1];
			b2 = ptrbb[2];
			b3 = ptrbb[3];

			a0 = ptrba[0];
			res0_0 += a0*b0;
			res1_0 += a0*b1;
			res2_0 += a0*b2;
			res3_0 += a0*b3;

			a1 = ptrba[1];
			res0_1 += a1*b0;
			res1_1 += a1*b1;
			res2_1 += a1*b2;
			res3_1 += a1*b3;

			ptrba = ptrba+2;
			ptrbb = ptrbb+4;
                }

		res0_0 *= alpha;
		res0_1 *= alpha;

		res1_0 *= alpha;
		res1_1 *= alpha;

		res2_0 *= alpha;
		res2_1 *= alpha;

		res3_0 *= alpha;
		res3_1 *= alpha;

		C0[0] = res0_0;
		C0[1] = res0_1;

		C1[0] = res1_0;
		C1[1] = res1_1;

		C2[0] = res2_0;
		C2[1] = res2_1;

		C3[0] = res3_0;
		C3[1] = res3_1;


#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 2; // number of values in A
#else
		temp -= 4; // number of values in B
#endif
		ptrba += temp*2;
		ptrbb += temp*4;
#endif

#ifdef LEFT
		off += 2; // number of values in A
#endif

		C0 = C0+2;
		C1 = C1+2;
		C2 = C2+2;
		C3 = C3+2;

	}

	if ( bm & 1 ) // do any 1x4 loop
	{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*1;
		ptrbb = bb + off*4;
#endif

		res0_0 = 0;
		res1_0 = 0;
		res2_0 = 0;
		res3_0 = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+1;	// number of values in A
#else
		temp = off+4;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];
			b1 = ptrbb[1];
			b2 = ptrbb[2];
			b3 = ptrbb[3];

			a0 = ptrba[0];
			res0_0 += a0*b0;
			res1_0 += a0*b1;
			res2_0 += a0*b2;
			res3_0 += a0*b3;

			ptrba = ptrba+1;
			ptrbb = ptrbb+4;
                }

		res0_0 *= alpha;

		res1_0 *= alpha;

		res2_0 *= alpha;

		res3_0 *= alpha;

		C0[0] = res0_0;

		C1[0] = res1_0;

		C2[0] = res2_0;

		C3[0] = res3_0;


#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 1; // number of values in A
#else
		temp -= 4; // number of values in B
#endif
		ptrba += temp*1;
		ptrbb += temp*4;
#endif

#ifdef LEFT
		off += 1; // number of values in A
#endif

		C0 = C0+1;
		C1 = C1+1;
		C2 = C2+1;
		C3 = C3+1;

	}


#if defined(TRMMKERNEL) && !defined(LEFT)
		off += 4;
#endif

        k = (bk<<2);
        bb = bb+k;
        i = (ldc<<2);
        C = C+i;
    }



   for (j=0; j<(bn&2); j+=2) // do the Mx2 loops 
   {
        C0 = C;
        C1 = C0+ldc;

#if defined(TRMMKERNEL) && defined(LEFT)
		off = offset;
#endif


        ptrba = ba;

        for (i=0; i<bm/4; i+=1) // do blocks of 4x2
	{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*4;
		ptrbb = bb + off*2;
#endif

		res0_0 = 0;
		res0_1 = 0;
		res0_2 = 0;
		res0_3 = 0;

		res1_0 = 0;
		res1_1 = 0;
		res1_2 = 0;
		res1_3 = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+4;	// number of values in A
#else
		temp = off+2;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];
			b1 = ptrbb[1];

			a0 = ptrba[0];
			res0_0 += a0*b0;
			res1_0 += a0*b1;

			a1 = ptrba[1];
			res0_1 += a1*b0;
			res1_1 += a1*b1;

			a0 = ptrba[2];
			res0_2 += a0*b0;
			res1_2 += a0*b1;

			a1 = ptrba[3];
			res0_3 += a1*b0;
			res1_3 += a1*b1;

			ptrba = ptrba+4;
			ptrbb = ptrbb+2;
                }

		res0_0 *= alpha;
		res0_1 *= alpha;
		res0_2 *= alpha;
		res0_3 *= alpha;

		res1_0 *= alpha;
		res1_1 *= alpha;
		res1_2 *= alpha;
		res1_3 *= alpha;

		C0[0] = res0_0;
		C0[1] = res0_1;
		C0[2] = res0_2;
		C0[3] = res0_3;

		C1[0] = res1_0;
		C1[1] = res1_1;
		C1[2] = res1_2;
		C1[3] = res1_3;


#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 4; // number of values in A
#else
		temp -= 2; // number of values in B
#endif
		ptrba += temp*4;
		ptrbb += temp*2;
#endif

#ifdef LEFT
		off += 4; // number of values in A
#endif

		C0 = C0+4;
		C1 = C1+4;

	}

	if ( bm & 2 ) // do any 2x2 loop
	{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*2;
		ptrbb = bb + off*2;
#endif

		res0_0 = 0;
		res0_1 = 0;

		res1_0 = 0;
		res1_1 = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+2;	// number of values in A
#else
		temp = off+2;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];
			b1 = ptrbb[1];

			a0 = ptrba[0];
			res0_0 += a0*b0;
			res1_0 += a0*b1;

			a1 = ptrba[1];
			res0_1 += a1*b0;
			res1_1 += a1*b1;

			ptrba = ptrba+2;
			ptrbb = ptrbb+2;
                }

		res0_0 *= alpha;
		res0_1 *= alpha;

		res1_0 *= alpha;
		res1_1 *= alpha;

		C0[0] = res0_0;
		C0[1] = res0_1;

		C1[0] = res1_0;
		C1[1] = res1_1;


#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 2; // number of values in A
#else
		temp -= 2; // number of values in B
#endif
		ptrba += temp*2;
		ptrbb += temp*2;
#endif

#ifdef LEFT
		off += 2; // number of values in A
#endif

		C0 = C0+2;
		C1 = C1+2;

	}

	if ( bm & 1 ) // do any 1x2 loop
	{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*1;
		ptrbb = bb + off*2;
#endif

		res0_0 = 0;

		res1_0 = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+1;	// number of values in A
#else
		temp = off+2;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];
			b1 = ptrbb[1];

			a0 = ptrba[0];
			res0_0 += a0*b0;
			res1_0 += a0*b1;

			ptrba = ptrba+1;
			ptrbb = ptrbb+2;
                }

		res0_0 *= alpha;

		res1_0 *= alpha;

		C0[0] = res0_0;

		C1[0] = res1_0;


#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 1; // number of values in A
#else
		temp -= 2; // number of values in B
#endif
		ptrba += temp*1;
		ptrbb += temp*2;
#endif

#ifdef LEFT
		off += 1; // number of values in A
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







   for (j=0; j<(bn&1); j+=1) // do the Mx1 loops
   {
        C0 = C;

#if defined(TRMMKERNEL) &&  defined(LEFT)
	off = offset;
#endif

        ptrba = ba;

        for (i=0; i<bm/4; i+=1) // do blocks of 4x1 loops
	{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*4;
		ptrbb = bb + off*1;
#endif

		res0_0 = 0;
		res0_1 = 0;
		res0_2 = 0;
		res0_3 = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+4;	// number of values in A
#else
		temp = off+1;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];

			a0 = ptrba[0];
			res0_0 += a0*b0;

			a1 = ptrba[1];
			res0_1 += a1*b0;

			a0 = ptrba[2];
			res0_2 += a0*b0;

			a1 = ptrba[3];
			res0_3 += a1*b0;

			ptrba = ptrba+4;
			ptrbb = ptrbb+1;
                }

		res0_0 *= alpha;
		res0_1 *= alpha;
		res0_2 *= alpha;
		res0_3 *= alpha;

		C0[0] = res0_0;
		C0[1] = res0_1;
		C0[2] = res0_2;
		C0[3] = res0_3;


#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 4; // number of values in A
#else
		temp -= 1; // number of values in B
#endif
		ptrba += temp*4;
		ptrbb += temp*1;
#endif

#ifdef LEFT
		off += 4; // number of values in A
#endif

		C0 = C0+4;

	}

	if ( bm & 2 ) // do any 2x1 loop
	{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*2;
		ptrbb = bb + off*1;
#endif

		res0_0 = 0;
		res0_1 = 0;



#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+2;	// number of values in A
#else
		temp = off+1;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];

			a0 = ptrba[0];
			res0_0 += a0*b0;

			a1 = ptrba[1];
			res0_1 += a1*b0;

			ptrba = ptrba+2;
			ptrbb = ptrbb+1;
                }

		res0_0 *= alpha;
		res0_1 *= alpha;

		C0[0] = res0_0;
		C0[1] = res0_1;


#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 2; // number of values in A
#else
		temp -= 1; // number of values in B
#endif
		ptrba += temp*2;
		ptrbb += temp*1;
#endif

#ifdef LEFT
		off += 2; // number of values in A
#endif

		C0 = C0+2;

	}

	if ( bm & 1 ) // do any 1x1 loop
	{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		ptrbb = bb;
#else
		ptrba += off*1;
		ptrbb = bb + off*1;
#endif

		res0_0 = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
		temp = bk-off;
#elif defined(LEFT)
		temp = off+1;	// number of values in A
#else
		temp = off+1;	// number of values in B
#endif

		for (k=0; k<temp; k++)
                {
			b0 = ptrbb[0];

			a0 = ptrba[0];
			res0_0 += a0*b0;

			ptrba = ptrba+1;
			ptrbb = ptrbb+1;
                }

		res0_0 *= alpha;

		C0[0] = res0_0;


#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
		temp = bk - off;
#ifdef LEFT
		temp -= 1; // number of values in A
#else
		temp -= 1; // number of values in B
#endif
		ptrba += temp*1;
		ptrbb += temp*1;
#endif

#ifdef LEFT
		off += 1; // number of values in A
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
