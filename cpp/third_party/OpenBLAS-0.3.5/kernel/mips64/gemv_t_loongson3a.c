#include "common.h"

//These are auto-tuning codes on Loongson-3A platform.

//#define prefetch(x) __builtin_prefetch(x)
//#define prefetch(x) do {_mm_prefetch((char *)(x), _MM_HINT_T0);} while(0)
#define prefetch(x) __asm__ __volatile__("ld $0, %0"::"m"(x))
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#define spec_loop_alpha1 do {Y[k] += A[LDA * j + i] * X[i]; i++;} while(0)
#define spec_loop do {Y[k] += ALPHA * A[LDA * j + i] * X[i]; i++;} while(0)
#define norm_loop_alpha1 do {Y[k] += A[LDA * j + i] * X[h]; i++; h += INCX;} while(0)
#define norm_loop do {Y[k] += ALPHA * A[LDA * j + i] * X[h]; i++; h += INCX;} while(0)

int CNAME(BLASLONG M, BLASLONG N, BLASLONG UNUSED, FLOAT ALPHA, FLOAT *A, BLASLONG LDA, FLOAT *X, BLASLONG INCX, FLOAT *Y, BLASLONG INCY, FLOAT *BUFFER) {

	if(!ALPHA)
		return 0;

//	if(INCX < 0)
//		INCX = -INCX;
//	if(INCY < 0)
//		INCY = -INCY;

	BLASLONG fahead = 30;
	BLASLONG spec_unroll = 3;
	BLASLONG tMQ = M - M % spec_unroll;
	BLASLONG j = 0, k = 0;

	if(ALPHA == 1) {
		if(INCX == 1) {
			for(; likely(j < N); j++, k += INCY) {
				BLASLONG i = 0;
				for(; likely(i < tMQ);) {
					prefetch(A[LDA * j + i + fahead]);
					prefetch(X[i + fahead]);
					/*loop_mark*/ spec_loop_alpha1;
					/*loop_mark*/ spec_loop_alpha1;
					/*loop_mark*/ spec_loop_alpha1;
				}
				for(; likely(i < M);) {
					spec_loop_alpha1;
				}
			}
		} else {
			for(; likely(j < N); j++, k += INCY) {
				BLASLONG i = 0, h = 0;
				for(; likely(i < tMQ);) {
					prefetch(A[LDA * j + i + fahead]);
					prefetch(X[h + fahead]);
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
		if(INCX == 1) {
			for(; likely(j < N); j++, k += INCY) {
				BLASLONG i = 0;
				for(; likely(i < tMQ);) {
					prefetch(A[LDA * j + i + fahead]);
					prefetch(X[i + fahead]);
					/*loop_mark*/ spec_loop;
					/*loop_mark*/ spec_loop;
					/*loop_mark*/ spec_loop;
				}
				for(; likely(i < M);) {
					spec_loop;
				}
			}
		} else {
			for(; likely(j < N); j++, k += INCY) {
				BLASLONG i = 0, h = 0;
				for(; likely(i < tMQ);) {
					prefetch(A[LDA * j + i + fahead]);
					prefetch(X[h + fahead]);
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
