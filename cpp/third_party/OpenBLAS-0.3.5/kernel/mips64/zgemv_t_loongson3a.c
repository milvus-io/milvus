#include "common.h"

#define prefetch(x) __asm__ __volatile__("ld $0, %0"::"m"(x))
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)

#if !defined(CONJ) && !defined(XCONJ)
#define spec_loop_alpha1	spec_loop_alpha1_0
#define spec_loop			spec_loop_0
#define norm_loop_alpha1	norm_loop_alpha1_0
#define norm_loop			norm_loop_0
#endif

#if  defined(CONJ) && !defined(XCONJ)
#define spec_loop_alpha1 	spec_loop_alpha1_1
#define spec_loop			spec_loop_1
#define norm_loop_alpha1	norm_loop_alpha1_1
#define norm_loop			norm_loop_1
#endif

#if  !defined(CONJ) && defined(XCONJ)
#define spec_loop_alpha1 	spec_loop_alpha1_2
#define spec_loop			spec_loop_2
#define norm_loop_alpha1	norm_loop_alpha1_2
#define norm_loop			norm_loop_2
#endif

#if  defined(CONJ) && defined(XCONJ)
#define spec_loop_alpha1 	spec_loop_alpha1_3
#define spec_loop			spec_loop_3
#define norm_loop_alpha1	norm_loop_alpha1_3
#define norm_loop			norm_loop_3
#endif


#define spec_loop_alpha1_0 do {Y[k] += A[jj + ii] * X[ii]; Y[k + 1] += A[jj + ii + 1] * X[ii]; Y[k + 1] += A[jj + ii] * X[ii + 1]; Y[k] -= A[jj + ii + 1] * X[ii + 1]; ii += 2;} while(0)
#define spec_loop_alpha1_1 do {Y[k] += A[jj + ii] * X[ii]; Y[k + 1] -= A[jj + ii + 1] * X[ii]; Y[k + 1] += A[jj + ii] * X[ii + 1]; Y[k] += A[jj + ii + 1] * X[ii + 1]; ii += 2;} while(0)
#define spec_loop_alpha1_2 do {Y[k] += A[jj + ii] * X[ii]; Y[k + 1] += A[jj + ii + 1] * X[ii]; Y[k + 1] -= A[jj + ii] * X[ii + 1]; Y[k] += A[jj + ii + 1] * X[ii + 1]; ii += 2;} while(0)
#define spec_loop_alpha1_3 do {Y[k] += A[jj + ii] * X[ii]; Y[k + 1] -= A[jj + ii + 1] * X[ii]; Y[k + 1] -= A[jj + ii] * X[ii + 1]; Y[k] -= A[jj + ii + 1] * X[ii + 1]; ii += 2;} while(0)

#define spec_loop_0 do {rTmp = A[jj + ii] * X[ii] - A[jj + ii + 1] * X[ii + 1]; iTmp = A[jj + ii] * X[ii + 1] + A[jj + ii + 1] * X[ii]; Y[k] += rTmp * rALPHA - iTmp * iALPHA; Y[k + 1] += rTmp * iALPHA + iTmp * rALPHA; ii += 2;} while(0)
#define spec_loop_1 do {rTmp = A[jj + ii] * X[ii] + A[jj + ii + 1] * X[ii + 1]; iTmp = A[jj + ii] * X[ii + 1] - A[jj + ii + 1] * X[ii]; Y[k] += rTmp * rALPHA - iTmp * iALPHA; Y[k + 1] += rTmp * iALPHA + iTmp * rALPHA; ii += 2;} while(0)
#define spec_loop_2 do {rTmp = A[jj + ii] * X[ii] + A[jj + ii + 1] * X[ii + 1]; iTmp = -A[jj + ii] * X[ii + 1] + A[jj + ii + 1] * X[ii]; Y[k] += rTmp * rALPHA - iTmp * iALPHA; Y[k + 1] += rTmp * iALPHA + iTmp * rALPHA; ii += 2;} while(0)
#define spec_loop_3 do {rTmp = A[jj + ii] * X[ii] - A[jj + ii + 1] * X[ii + 1]; iTmp = -A[jj + ii] * X[ii + 1] - A[jj + ii + 1] * X[ii]; Y[k] += rTmp * rALPHA - iTmp * iALPHA; Y[k + 1] += rTmp * iALPHA + iTmp * rALPHA; ii += 2;} while(0)

#define norm_loop_alpha1_0 do {Y[k] += A[jj + ii] * X[iii] - A[jj + ii + 1] * X[iii + 1]; Y[k + 1] += A[jj + ii] * X[iii + 1] + A[jj + ii + 1] * X[iii]; ii += 2; iii += INCX * 2;} while(0)
#define norm_loop_alpha1_1 do {Y[k] += A[jj + ii] * X[iii] + A[jj + ii + 1] * X[iii + 1]; Y[k + 1] += A[jj + ii] * X[iii + 1] - A[jj + ii + 1] * X[iii]; ii += 2; iii += INCX * 2;} while(0)
#define norm_loop_alpha1_2 do {Y[k] += A[jj + ii] * X[iii] + A[jj + ii + 1] * X[iii + 1]; Y[k + 1] += -A[jj + ii] * X[iii + 1] + A[jj + ii + 1] * X[iii]; ii += 2; iii += INCX * 2;} while(0)
#define norm_loop_alpha1_3 do {Y[k] += A[jj + ii] * X[iii] - A[jj + ii + 1] * X[iii + 1]; Y[k + 1] += -A[jj + ii] * X[iii + 1] - A[jj + ii + 1] * X[iii]; ii += 2; iii += INCX * 2;} while(0)

#define norm_loop_0 do {rTmp = A[jj + ii] * X[iii] - A[jj + ii + 1] * X[iii + 1]; iTmp = A[jj + ii] * X[iii + 1] + A[jj + ii + 1] * X[iii]; Y[k] += rTmp * rALPHA - iTmp * iALPHA; Y[k + 1] += rTmp * iALPHA + iTmp * rALPHA; ii += 2; iii += INCX * 2;} while(0)
#define norm_loop_1 do {rTmp = A[jj + ii] * X[iii] + A[jj + ii + 1] * X[iii + 1]; iTmp = A[jj + ii] * X[iii + 1] - A[jj + ii + 1] * X[iii]; Y[k] += rTmp * rALPHA - iTmp * iALPHA; Y[k + 1] += rTmp * iALPHA + iTmp * rALPHA; ii += 2; iii += INCX * 2;} while(0)
#define norm_loop_2 do {rTmp = A[jj + ii] * X[iii] + A[jj + ii + 1] * X[iii + 1]; iTmp = -A[jj + ii] * X[iii + 1] + A[jj + ii + 1] * X[iii]; Y[k] += rTmp * rALPHA - iTmp * iALPHA; Y[k + 1] += rTmp * iALPHA + iTmp * rALPHA; ii += 2; iii += INCX * 2;} while(0)
#define norm_loop_3 do {rTmp = A[jj + ii] * X[iii] - A[jj + ii + 1] * X[iii + 1]; iTmp = -A[jj + ii] * X[iii + 1] - A[jj + ii + 1] * X[iii]; Y[k] += rTmp * rALPHA - iTmp * iALPHA; Y[k + 1] += rTmp * iALPHA + iTmp * rALPHA; ii += 2; iii += INCX * 2;} while(0)

int CNAME(BLASLONG M, BLASLONG N, BLASLONG UNUSED, FLOAT rALPHA, FLOAT iALPHA, FLOAT *A, BLASLONG LDA, FLOAT *X, BLASLONG INCX, FLOAT *Y, BLASLONG INCY, FLOAT *BUFFER) {

	if(!rALPHA && iALPHA)
		return 0;

	BLASLONG fahead = 30;
	BLASLONG spec_unroll = 2;
	BLASLONG tMQ = M - M % spec_unroll;
	BLASLONG j = 0, k = 0, jj = 0;

	if(rALPHA == 1 && iALPHA == 0) {
		if(INCX == 1) {
			for(; likely(j < N); j++, k += INCY * 2, jj += LDA * 2) {
				BLASLONG i = 0, ii = 0;
				for(; likely(i < tMQ); i += spec_unroll) {
					prefetch(A[jj + ii + fahead]);
					prefetch(X[ii + fahead]);
					/*loop_mark*/ spec_loop_alpha1;
					/*loop_mark*/ spec_loop_alpha1;
				}
				for(; likely(i < M); i++) {
					spec_loop_alpha1;
				}
			}
		} else {
			for(; likely(j < N); j++, k += INCY * 2, jj += LDA * 2) {
				BLASLONG i = 0, ii = 0, iii = 0;
				for(; likely(i < tMQ); i += spec_unroll) {
					prefetch(A[jj + ii + fahead]);
					prefetch(X[iii + fahead]);
					/*loop_mark*/ norm_loop_alpha1;
					/*loop_mark*/ norm_loop_alpha1;
				}
				for(; likely(i < M); i++) {
					norm_loop_alpha1;
				}
			}
		}
	} else {
		FLOAT rTmp, iTmp;
		if(INCX == 1) {
			for(; likely(j < N); j++, k += INCY * 2, jj += LDA * 2) {
				BLASLONG i = 0, ii = 0;
				for(; likely(i < tMQ); i += spec_unroll) {
					prefetch(A[jj + ii + fahead]);
					prefetch(X[ii + fahead]);
					/*loop_mark*/ spec_loop;
					/*loop_mark*/ spec_loop;
				}
				for(; likely(i < M); i++) {
					spec_loop;
				}
			}
		} else {
			for(; likely(j < N); j++, k += INCY * 2, jj += LDA * 2) {
				BLASLONG i = 0, ii = 0, iii = 0;
				for(; likely(i < tMQ); i += spec_unroll) {
					prefetch(A[jj + ii + fahead]);
					prefetch(X[iii + fahead]);
					/*loop_mark*/ norm_loop;
					/*loop_mark*/ norm_loop;
				}
				for(; likely(i < M); i++) {
					norm_loop;
				}
			}
		}
	}
	return 0;
}
