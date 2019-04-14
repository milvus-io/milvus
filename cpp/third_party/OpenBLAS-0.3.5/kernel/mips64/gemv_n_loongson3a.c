#include "common.h"

//These are auto-tuning codes on Loongson-3A platform.

//#define prefetch(x) __builtin_prefetch(x)
//#define prefetch(x) do {_mm_prefetch((char *)(x), _MM_HINT_T0);} while(0)
#define prefetch(x) __asm__ __volatile__("ld $0, %0"::"m"(x))
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#define spec_loop_alpha1 do {Y[i] += A[LDA * j + i] * X[k]; i++;} while(0)
#define spec_loop do {Y[i] += ALPHA * A[LDA * j + i] * X[k]; i++;} while(0)
#define norm_loop_alpha1 do {Y[h] += A[LDA * j + i] * X[k]; i++; h += INCY;} while(0)
#define norm_loop do {Y[h] += ALPHA * A[LDA * j + i] * X[k]; i++; h += INCY;} while(0)

int CNAME(BLASLONG M, BLASLONG N, BLASLONG UNUSED, FLOAT ALPHA, FLOAT *A, BLASLONG LDA, FLOAT *X, BLASLONG INCX, FLOAT *Y, BLASLONG INCY, FLOAT *BUFFER)
{

	BLASLONG kx=0, ky=0;
	if(!ALPHA)
		return 0;

	//if(INCX < 0)
	//	kx = (1-N) * INCX;
	//	INCX = -INCX;
	//if(INCY < 0)
	//	ky = (1-M) * INCY;
	//	INCY = -INCY;

	BLASLONG fahead = 30;
	BLASLONG spec_unroll = 4;
	BLASLONG tMQ = M - M % spec_unroll;
	BLASLONG j = 0, k = 0;

	if(ALPHA == 1) {
		if(INCY == 1) {
			for(k=kx; likely(j < N); j++, k += INCX) {
				BLASLONG i = 0;
				for(; likely(i < tMQ);) {
					prefetch(A[LDA * j + i + fahead]);
					prefetch(Y[i + fahead]);
					/*loop_mark*/ spec_loop_alpha1;
					/*loop_mark*/ spec_loop_alpha1;
					/*loop_mark*/ spec_loop_alpha1;
					/*loop_mark*/ spec_loop_alpha1;
				}
				for(; likely(i < M);) {
					spec_loop_alpha1;
				}
			}
		} else {
			for(k=kx; likely(j < N); j++, k += INCX) {
				BLASLONG i = 0, h = ky;
				for(; likely(i < tMQ);) {
					prefetch(A[LDA * j + i + fahead]);
					prefetch(Y[h + fahead]);
					/*loop_mark*/ norm_loop_alpha1;
					/*loop_mark*/ norm_loop_alpha1;
					/*loop_mark*/ norm_loop_alpha1;
					/*loop_mark*/ norm_loop_alpha1;
				}
				for(; likely(i < M);) {
					norm_loop_alpha1;
				}
			}
		}
	} else {
		if(INCY == 1) {
			for(k=kx; likely(j < N); j++, k += INCX) {
				BLASLONG i = 0;
				for(; likely(i < tMQ);) {
					prefetch(A[LDA * j + i + fahead]);
					prefetch(Y[i + fahead]);
					/*loop_mark*/ spec_loop;
					/*loop_mark*/ spec_loop;
					/*loop_mark*/ spec_loop;
					/*loop_mark*/ spec_loop;
				}
				for(; likely(i < M);) {
					spec_loop;
				}
			}
		} else {
			for(k=kx; likely(j < N); j++, k += INCX) {
				BLASLONG i = 0, h = ky;
				for(; likely(i < tMQ);) {
					prefetch(A[LDA * j + i + fahead]);
					prefetch(Y[h + fahead]);
					/*loop_mark*/ norm_loop;
					/*loop_mark*/ norm_loop;
					/*loop_mark*/ norm_loop;
					/*loop_mark*/ norm_loop;
				}
				for(; likely(i < M);) {
					norm_loop;
				}
			}
		}
	}
	return 0;
}
