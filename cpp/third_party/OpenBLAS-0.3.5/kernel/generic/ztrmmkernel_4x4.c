#include "common.h"

#define MADD_ALPHA_N_STORE(C, res, alpha) \
	C[0] = res ## _r * alpha ## _r - res ## _i * alpha ## _i; \
	C[1] = res ## _r * alpha ## _i + res ## _i * alpha ## _r;

#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define MADD(res, op1, op2) \
	res ## _r += op1 ## _r * op2 ## _r; \
	res ## _r -= op1 ## _i * op2 ## _i; \
	res ## _i += op1 ## _r * op2 ## _i; \
	res ## _i += op1 ## _i * op2 ## _r;
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define MADD(res, op1, op2) \
	res ## _r += op1 ## _r * op2 ## _r; \
	res ## _r += op1 ## _i * op2 ## _i; \
	res ## _i -= op1 ## _r * op2 ## _i; \
	res ## _i += op1 ## _i * op2 ## _r;
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define MADD(res, op1, op2) \
	res ## _r += op1 ## _r * op2 ## _r; \
	res ## _r += op1 ## _i * op2 ## _i; \
	res ## _i += op1 ## _r * op2 ## _i; \
	res ## _i -= op1 ## _i * op2 ## _r;
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
#define MADD(res, op1, op2) \
	res ## _r += op1 ## _r * op2 ## _r; \
	res ## _r -= op1 ## _i * op2 ## _i; \
	res ## _i -= op1 ## _r * op2 ## _i; \
	res ## _i -= op1 ## _i * op2 ## _r;
#endif

int CNAME(BLASLONG bm,BLASLONG bn,BLASLONG bk,FLOAT alpha_r, FLOAT alpha_i,FLOAT* ba,FLOAT* bb,FLOAT* C,BLASLONG ldc
		, BLASLONG offset
		)
{

	BLASLONG i,j,k;
	FLOAT *C0,*C1,*C2,*C3,*ptrba,*ptrbb;
	FLOAT res00_r, res01_r, res02_r, res03_r;
	FLOAT res00_i, res01_i, res02_i, res03_i;
	FLOAT res10_r, res11_r, res12_r, res13_r;
	FLOAT res10_i, res11_i, res12_i, res13_i;
	FLOAT res20_r, res21_r, res22_r, res23_r;
	FLOAT res20_i, res21_i, res22_i, res23_i;
	FLOAT res30_r, res31_r, res32_r, res33_r;
	FLOAT res30_i, res31_i, res32_i, res33_i;
	FLOAT a0_r, a1_r;
	FLOAT a0_i, a1_i;
	FLOAT b0_r, b1_r, b2_r, b3_r;
	FLOAT b0_i, b1_i, b2_i, b3_i;
	BLASLONG off, temp;

#if defined(TRMMKERNEL) && !defined(LEFT)
	off = -offset;
#else
	off = 0;
#endif

	for (j=0; j<bn/4; j+=1) // do blocks of the Mx4 loops 
	{
		C0 = C;
		C1 = C0+2*ldc;
		C2 = C1+2*ldc;
		C3 = C2+2*ldc;


#if defined(TRMMKERNEL) && defined(LEFT)
		off = offset;
#endif

		ptrba = ba;

		for (i=0; i<bm/4; i+=1) // do blocks of 4x4
		{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
			ptrbb = bb;
#else

			ptrba += off*4*2; // number of values in A
			ptrbb = bb + off*4*2; // number of values in B
#endif

			res00_r = 0;
			res00_i = 0;
			res01_r = 0;
			res01_i = 0;
			res02_r = 0;
			res02_i = 0;
			res03_r = 0;
			res03_i = 0;

			res10_r = 0;
			res10_i = 0;
			res11_r = 0;
			res11_i = 0;
			res12_r = 0;
			res12_i = 0;
			res13_r = 0;
			res13_i = 0;

			res20_r = 0;
			res20_i = 0;
			res21_r = 0;
			res21_i = 0;
			res22_r = 0;
			res22_i = 0;
			res23_r = 0;
			res23_i = 0;

			res30_r = 0;
			res30_i = 0;
			res31_r = 0;
			res31_i = 0;
			res32_r = 0;
			res32_i = 0;
			res33_r = 0;
			res33_i = 0;

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
			temp = bk - off;
#elif defined(LEFT)
			temp = off + 4;
#else
			temp = off + 4;
#endif

			for (k=0; k<temp; k++)
			{
				b0_r = ptrbb[2*0+0]; b0_i = ptrbb[2*0+1];
				b1_r = ptrbb[2*1+0]; b1_i = ptrbb[2*1+1];
				b2_r = ptrbb[2*2+0]; b2_i = ptrbb[2*2+1];
				b3_r = ptrbb[2*3+0]; b3_i = ptrbb[2*3+1];

				a0_r = ptrba[2*0+0]; a0_i = ptrba[2*0+1];
				MADD(res00, a0, b0);
				MADD(res10, a0, b1);
				MADD(res20, a0, b2);
				MADD(res30, a0, b3);

				a1_r = ptrba[2*1+0]; a1_i = ptrba[2*1+1];
				MADD(res01, a1, b0);
				MADD(res11, a1, b1);
				MADD(res21, a1, b2);
				MADD(res31, a1, b3);

				a0_r = ptrba[2*2+0]; a0_i = ptrba[2*2+1];
				MADD(res02, a0, b0);
				MADD(res12, a0, b1);
				MADD(res22, a0, b2);
				MADD(res32, a0, b3);


				a1_r = ptrba[2*3+0]; a1_i = ptrba[2*3+1];
				MADD(res03, a1, b0);
				MADD(res13, a1, b1);
				MADD(res23, a1, b2);
				MADD(res33, a1, b3);

				ptrba = ptrba+8;
				ptrbb = ptrbb+8;
			}

			MADD_ALPHA_N_STORE(C0, res00, alpha);
			C0 = C0 + 2;
			MADD_ALPHA_N_STORE(C0, res01, alpha);
			C0 = C0 + 2;
			MADD_ALPHA_N_STORE(C0, res02, alpha);
			C0 = C0 + 2;
			MADD_ALPHA_N_STORE(C0, res03, alpha);
			C0 = C0 + 2;

			MADD_ALPHA_N_STORE(C1, res10, alpha);
			C1 = C1 + 2;
			MADD_ALPHA_N_STORE(C1, res11, alpha);
			C1 = C1 + 2;
			MADD_ALPHA_N_STORE(C1, res12, alpha);
			C1 = C1 + 2;
			MADD_ALPHA_N_STORE(C1, res13, alpha);
			C1 = C1 + 2;

			MADD_ALPHA_N_STORE(C2, res20, alpha);
			C2 = C2 + 2;
			MADD_ALPHA_N_STORE(C2, res21, alpha);
			C2 = C2 + 2;
			MADD_ALPHA_N_STORE(C2, res22, alpha);
			C2 = C2 + 2;
			MADD_ALPHA_N_STORE(C2, res23, alpha);
			C2 = C2 + 2;

			MADD_ALPHA_N_STORE(C3, res30, alpha);
			C3 = C3 + 2;
			MADD_ALPHA_N_STORE(C3, res31, alpha);
			C3 = C3 + 2;
			MADD_ALPHA_N_STORE(C3, res32, alpha);
			C3 = C3 + 2;
			MADD_ALPHA_N_STORE(C3, res33, alpha);
			C3 = C3 + 2;


#if ( defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
			temp = bk-off;
#if defined(LEFT)
			temp = temp - 4;
#else
			temp = temp - 4;
#endif
			ptrba += temp*4*2; // number of values in A
			ptrbb += temp*4*2; // number of values in B
#endif
#ifdef LEFT
			off += 4; // number of values in A
#endif


		}

		if ( bm & 2 ) // do any 2x4 loop
		{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
			ptrbb = bb;
#else
			ptrba += off*2*2;
			ptrbb = bb + off*4*2;
#endif


			res00_r = 0;
			res00_i = 0;
			res01_r = 0;
			res01_i = 0;

			res10_r = 0;
			res10_i = 0;
			res11_r = 0;
			res11_i = 0;

			res20_r = 0;
			res20_i = 0;
			res21_r = 0;
			res21_i = 0;

			res30_r = 0;
			res30_i = 0;
			res31_r = 0;
			res31_i = 0;

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
			temp = bk-off;
#elif defined(LEFT)
			temp = off+2;	// number of values in A
#else
			temp = off+4;	// number of values in B
#endif

			for (k=0; k<temp; k++)
			{
				b0_r = ptrbb[2*0+0]; b0_i = ptrbb[2*0+1];
				b1_r = ptrbb[2*1+0]; b1_i = ptrbb[2*1+1];
				b2_r = ptrbb[2*2+0]; b2_i = ptrbb[2*2+1];
				b3_r = ptrbb[2*3+0]; b3_i = ptrbb[2*3+1];

				a0_r = ptrba[2*0+0]; a0_i = ptrba[2*0+1];
				MADD(res00, a0, b0);
				MADD(res10, a0, b1);
				MADD(res20, a0, b2);
				MADD(res30, a0, b3);

				a1_r = ptrba[2*1+0]; a1_i = ptrba[2*1+1];
				MADD(res01, a1, b0);
				MADD(res11, a1, b1);
				MADD(res21, a1, b2);
				MADD(res31, a1, b3);


				ptrba = ptrba+4;
				ptrbb = ptrbb+8;
			}

			MADD_ALPHA_N_STORE(C0, res00, alpha);
			C0 = C0 + 2;
			MADD_ALPHA_N_STORE(C0, res01, alpha);
			C0 = C0 + 2;

			MADD_ALPHA_N_STORE(C1, res10, alpha);
			C1 = C1 + 2;
			MADD_ALPHA_N_STORE(C1, res11, alpha);
			C1 = C1 + 2;

			MADD_ALPHA_N_STORE(C2, res20, alpha);
			C2 = C2 + 2;
			MADD_ALPHA_N_STORE(C2, res21, alpha);
			C2 = C2 + 2;

			MADD_ALPHA_N_STORE(C3, res30, alpha);
			C3 = C3 + 2;
			MADD_ALPHA_N_STORE(C3, res31, alpha);
			C3 = C3 + 2;





#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
			temp = bk - off;
#ifdef LEFT
			temp -= 2; // number of values in A
#else
			temp -= 4; // number of values in B
#endif
			ptrba += temp*2*2;
			ptrbb += temp*4*2;
#endif

#ifdef LEFT
			off += 2; // number of values in A
#endif


		}

		if ( bm & 1 ) // do any 1x4 loop
		{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
			ptrbb = bb;
#else
			ptrba += off*1*2;
			ptrbb = bb + off*4*2;
#endif

			res00_r = 0;
			res00_i = 0;
			res10_r = 0;
			res10_i = 0;
			res20_r = 0;
			res20_i = 0;
			res30_r = 0;
			res30_i = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
			temp = bk-off;
#elif defined(LEFT)
			temp = off+1;	// number of values in A
#else
			temp = off+4;	// number of values in B
#endif

			for (k=0; k<temp; k++)
			{
				b0_r = ptrbb[2*0+0]; b0_i = ptrbb[2*0+1];
				b1_r = ptrbb[2*1+0]; b1_i = ptrbb[2*1+1];
				b2_r = ptrbb[2*2+0]; b2_i = ptrbb[2*2+1];
				b3_r = ptrbb[2*3+0]; b3_i = ptrbb[2*3+1];

				a0_r = ptrba[2*0+0]; a0_i = ptrba[2*0+1];
				MADD(res00, a0, b0);
				MADD(res10, a0, b1);
				MADD(res20, a0, b2);
				MADD(res30, a0, b3);


				ptrba = ptrba+2;
				ptrbb = ptrbb+8;
			}

			MADD_ALPHA_N_STORE(C0, res00, alpha);
			C0 = C0 + 2;

			MADD_ALPHA_N_STORE(C1, res10, alpha);
			C1 = C1 + 2;

			MADD_ALPHA_N_STORE(C2, res20, alpha);
			C2 = C2 + 2;

			MADD_ALPHA_N_STORE(C3, res30, alpha);
			C3 = C3 + 2;


#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
			temp = bk - off;
#ifdef LEFT
			temp -= 1; // number of values in A
#else
			temp -= 4; // number of values in B
#endif
			ptrba += temp*1*2;
			ptrbb += temp*4*2;
#endif

#ifdef LEFT
			off += 1; // number of values in A
#endif


		}


#if defined(TRMMKERNEL) && !defined(LEFT)
		off += 4;
#endif

		k = (bk<<3);
		bb = bb+k;
		i = (ldc<<3);
		C = C+i;
	}

	for (j=0; j<(bn&2); j+=2) // do the Mx2 loops 
	{
		C0 = C;
		C1 = C0+ldc*2;

#if defined(TRMMKERNEL) && defined(LEFT)
		off = offset;
#endif


		ptrba = ba;

		for (i=0; i<bm/4; i+=1) // do blocks of 4x2
		{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
			ptrbb = bb;
#else
			ptrba += off*4*2;
			ptrbb = bb + off*2*2;
#endif

			res00_r = 0;
			res00_i = 0;
			res01_r = 0;
			res01_i = 0;
			res02_r = 0;
			res02_i = 0;
			res03_r = 0;
			res03_i = 0;

			res10_r = 0;
			res10_i = 0;
			res11_r = 0;
			res11_i = 0;
			res12_r = 0;
			res12_i = 0;
			res13_r = 0;
			res13_i = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
			temp = bk-off;
#elif defined(LEFT)
			temp = off+4;	// number of values in A
#else
			temp = off+2;	// number of values in B
#endif

			for (k=0; k<temp; k++)
			{
				b0_r = ptrbb[2*0+0]; b0_i = ptrbb[2*0+1];
				b1_r = ptrbb[2*1+0]; b1_i = ptrbb[2*1+1];

				a0_r = ptrba[2*0+0]; a0_i = ptrba[2*0+1];
				MADD(res00, a0, b0);
				MADD(res10, a0, b1);

				a1_r = ptrba[2*1+0]; a1_i = ptrba[2*1+1];
				MADD(res01, a1, b0);
				MADD(res11, a1, b1);

				a0_r = ptrba[2*2+0]; a0_i = ptrba[2*2+1];
				MADD(res02, a0, b0);
				MADD(res12, a0, b1);

				a1_r = ptrba[2*3+0]; a1_i = ptrba[2*3+1];
				MADD(res03, a1, b0);
				MADD(res13, a1, b1);

				ptrba = ptrba+8;
				ptrbb = ptrbb+4;
			}

			MADD_ALPHA_N_STORE(C0, res00, alpha);
			C0 = C0 + 2;
			MADD_ALPHA_N_STORE(C0, res01, alpha);
			C0 = C0 + 2;
			MADD_ALPHA_N_STORE(C0, res02, alpha);
			C0 = C0 + 2;
			MADD_ALPHA_N_STORE(C0, res03, alpha);
			C0 = C0 + 2;

			MADD_ALPHA_N_STORE(C1, res10, alpha);
			C1 = C1 + 2;
			MADD_ALPHA_N_STORE(C1, res11, alpha);
			C1 = C1 + 2;
			MADD_ALPHA_N_STORE(C1, res12, alpha);
			C1 = C1 + 2;
			MADD_ALPHA_N_STORE(C1, res13, alpha);
			C1 = C1 + 2;


#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
			temp = bk - off;
#ifdef LEFT
			temp -= 4; // number of values in A
#else
			temp -= 2; // number of values in B
#endif
			ptrba += temp*4*2;
			ptrbb += temp*2*2;
#endif

#ifdef LEFT
			off += 4; // number of values in A
#endif

		}

		if ( bm & 2 ) // do any 2x2 loop
		{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
			ptrbb = bb;
#else
			ptrba += off*2*2;
			ptrbb = bb + off*2*2;
#endif

			res00_r = 0;
			res00_i = 0;
			res01_r = 0;
			res01_i = 0;

			res10_r = 0;
			res10_i = 0;
			res11_r = 0;
			res11_i = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
			temp = bk-off;
#elif defined(LEFT)
			temp = off+2;	// number of values in A
#else
			temp = off+2;	// number of values in B
#endif

			for (k=0; k<temp; k++)
			{
				b0_r = ptrbb[2*0+0]; b0_i = ptrbb[2*0+1];
				b1_r = ptrbb[2*1+0]; b1_i = ptrbb[2*1+1];

				a0_r = ptrba[2*0+0]; a0_i = ptrba[2*0+1];
				MADD(res00, a0, b0);
				MADD(res10, a0, b1);

				a1_r = ptrba[2*1+0]; a1_i = ptrba[2*1+1];
				MADD(res01, a1, b0);
				MADD(res11, a1, b1);


				ptrba = ptrba+4;
				ptrbb = ptrbb+4;
			}

			MADD_ALPHA_N_STORE(C0, res00, alpha);
			C0 = C0 + 2;
			MADD_ALPHA_N_STORE(C0, res01, alpha);
			C0 = C0 + 2;

			MADD_ALPHA_N_STORE(C1, res10, alpha);
			C1 = C1 + 2;
			MADD_ALPHA_N_STORE(C1, res11, alpha);
			C1 = C1 + 2;

#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
			temp = bk - off;
#ifdef LEFT
			temp -= 2; // number of values in A
#else
			temp -= 2; // number of values in B
#endif
			ptrba += temp*2*2;
			ptrbb += temp*2*2;
#endif

#ifdef LEFT
			off += 2; // number of values in A
#endif

		}

		if ( bm & 1 ) // do any 1x2 loop
		{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
			ptrbb = bb;
#else
			ptrba += off*1*2;
			ptrbb = bb + off*2*2;
#endif

			res00_r = 0;
			res00_i = 0;

			res10_r = 0;
			res10_i = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
			temp = bk-off;
#elif defined(LEFT)
			temp = off+1;	// number of values in A
#else
			temp = off+2;	// number of values in B
#endif

			for (k=0; k<temp; k++)
			{
				b0_r = ptrbb[2*0+0]; b0_i = ptrbb[2*0+1];
				b1_r = ptrbb[2*1+0]; b1_i = ptrbb[2*1+1];

				a0_r = ptrba[2*0+0]; a0_i = ptrba[2*0+1];
				MADD(res00, a0, b0);
				MADD(res10, a0, b1);

				ptrba = ptrba+2;
				ptrbb = ptrbb+4;
			}

			MADD_ALPHA_N_STORE(C0, res00, alpha);
			C0 = C0 + 2;

			MADD_ALPHA_N_STORE(C1, res10, alpha);
			C1 = C1 + 2;

#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
			temp = bk - off;
#ifdef LEFT
			temp -= 1; // number of values in A
#else
			temp -= 2; // number of values in B
#endif
			ptrba += temp*1*2;
			ptrbb += temp*2*2;
#endif

#ifdef LEFT
			off += 1; // number of values in A
#endif

		}


#if defined(TRMMKERNEL) && !defined(LEFT)
		off += 2;
#endif

		k = (bk<<2);
		bb = bb+k;
		i = (ldc<<2);
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
			ptrba += off*4*2;
			ptrbb = bb + off*1*2;
#endif

			res00_r = 0;
			res00_i = 0;
			res01_r = 0;
			res01_i = 0;
			res02_r = 0;
			res02_i = 0;
			res03_r = 0;
			res03_i = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
			temp = bk-off;
#elif defined(LEFT)
			temp = off+4;	// number of values in A
#else
			temp = off+1;	// number of values in B
#endif

			for (k=0; k<temp; k++)
			{
				b0_r = ptrbb[2*0+0]; b0_i = ptrbb[2*0+1];

				a0_r = ptrba[2*0+0]; a0_i = ptrba[2*0+1];
				MADD(res00, a0, b0);

				a1_r = ptrba[2*1+0]; a1_i = ptrba[2*1+1];
				MADD(res01, a1, b0);

				a0_r = ptrba[2*2+0]; a0_i = ptrba[2*2+1];
				MADD(res02, a0, b0);

				a1_r = ptrba[2*3+0]; a1_i = ptrba[2*3+1];
				MADD(res03, a1, b0);

				ptrba = ptrba+8;
				ptrbb = ptrbb+2;
			}

			MADD_ALPHA_N_STORE(C0, res00, alpha);
			C0 = C0 + 2;
			MADD_ALPHA_N_STORE(C0, res01, alpha);
			C0 = C0 + 2;
			MADD_ALPHA_N_STORE(C0, res02, alpha);
			C0 = C0 + 2;
			MADD_ALPHA_N_STORE(C0, res03, alpha);
			C0 = C0 + 2;


#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
			temp = bk - off;
#ifdef LEFT
			temp -= 4; // number of values in A
#else
			temp -= 1; // number of values in B
#endif
			ptrba += temp*4*2;
			ptrbb += temp*1*2;
#endif

#ifdef LEFT
			off += 4; // number of values in A
#endif

		}

		if ( bm & 2 ) // do any 2x1 loop
		{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
			ptrbb = bb;
#else
			ptrba += off*2*2;
			ptrbb = bb + off*1*2;
#endif

			res00_r = 0;
			res00_i = 0;
			res01_r = 0;
			res01_i = 0;

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
			temp = bk-off;
#elif defined(LEFT)
			temp = off+2;	// number of values in A
#else
			temp = off+1;	// number of values in B
#endif

			for (k=0; k<temp; k++)
			{
				b0_r = ptrbb[2*0+0]; b0_i = ptrbb[2*0+1];

				a0_r = ptrba[2*0+0]; a0_i = ptrba[2*0+1];
				MADD(res00, a0, b0);

				a1_r = ptrba[2*1+0]; a1_i = ptrba[2*1+1];
				MADD(res01, a1, b0);


				ptrba = ptrba+4;
				ptrbb = ptrbb+2;
			}

			MADD_ALPHA_N_STORE(C0, res00, alpha);
			C0 = C0 + 2;
			MADD_ALPHA_N_STORE(C0, res01, alpha);
			C0 = C0 + 2;


#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
			temp = bk - off;
#ifdef LEFT
			temp -= 2; // number of values in A
#else
			temp -= 1; // number of values in B
#endif
			ptrba += temp*2*2;
			ptrbb += temp*1*2;
#endif

#ifdef LEFT
			off += 2; // number of values in A
#endif

		}

		if ( bm & 1 ) // do any 1x1 loop
		{

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
			ptrbb = bb;
#else
			ptrba += off*1*2;
			ptrbb = bb + off*1*2;
#endif

			res00_r = 0;
			res00_i = 0;


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
			temp = bk-off;
#elif defined(LEFT)
			temp = off+1;	// number of values in A
#else
			temp = off+1;	// number of values in B
#endif

			for (k=0; k<temp; k++)
			{
				b0_r = ptrbb[2*0+0]; b0_i = ptrbb[2*0+1];

				a0_r = ptrba[2*0+0]; a0_i = ptrba[2*0+1];
				MADD(res00, a0, b0);

				ptrba = ptrba+2;
				ptrbb = ptrbb+2;
			}

			MADD_ALPHA_N_STORE(C0, res00, alpha);
			C0 = C0 + 2;

#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
			temp = bk - off;
#ifdef LEFT
			temp -= 1; // number of values in A
#else
			temp -= 1; // number of values in B
#endif
			ptrba += temp*1*2;
			ptrbb += temp*1*2;
#endif

#ifdef LEFT
			off += 1; // number of values in A
#endif

		}



#if defined(TRMMKERNEL) && !defined(LEFT)
		off += 1;
#endif

		k = (bk<<1);
		bb = bb+k;
		i = (ldc<<1);
		C = C+i;
	}
	return 0;
}
