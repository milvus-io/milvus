#ifndef GEMV_PARAM_H
#define GEMV_PARAM_H

#ifdef movsd
#undef movsd
#endif

#undef  movapd
#define movapd movaps

#ifdef ATHLON
#define ALIGNED_ACCESS
#define MOVUPS_A	movaps
#define MOVUPS_XL	movaps
#define MOVUPS_XS	movaps
#define MOVUPS_YL	movaps
#define MOVUPS_YS	movaps
#define PREFETCH	prefetcht0
#define PREFETCHSIZE	64 * 3
#endif

#ifdef PENTIUM4
#define ALIGNED_ACCESS
#define MOVUPS_A	movaps
#define MOVUPS_XL	movaps
#define MOVUPS_XS	movaps
#define MOVUPS_YL	movaps
#define MOVUPS_YS	movaps
#define PREFETCH	prefetcht0
#define PREFETCHSIZE	64 * 2
#endif

#ifdef CORE2
#define ALIGNED_ACCESS
#define MOVUPS_A	movaps
#define MOVUPS_XL	movaps
#define MOVUPS_XS	movaps
#define MOVUPS_YL	movaps
#define MOVUPS_YS	movaps
#define PREFETCH	prefetcht0
#define PREFETCHSIZE	64 * 4
#endif

#ifdef PENRYN
#define ALIGNED_ACCESS
#define MOVUPS_A	movaps
#define MOVUPS_XL	movaps
#define MOVUPS_XS	movaps
#define MOVUPS_YL	movaps
#define MOVUPS_YS	movaps
#define PREFETCH	prefetcht0
#define PREFETCHSIZE	64 * 4
#endif

#ifdef NEHALEM
#define MOVUPS_A	movups
#define MOVUPS_XL	movups
#define MOVUPS_XS	movups
#define MOVUPS_YL	movups
#define MOVUPS_YS	movups
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	64 * 3
#endif

#ifdef SANDYBRIDGE
#define MOVUPS_A	movups
#define MOVUPS_XL	movups
#define MOVUPS_XS	movups
#define MOVUPS_YL	movups
#define MOVUPS_YS	movups
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE	64 * 3
#endif

#ifdef OPTERON
#define PREFETCH	prefetch
#define PREFETCHW	prefetchw
#ifndef COMPLEX
#define PREFETCHSIZE	64 * 1
#else
#define PREFETCHSIZE	64 * 1
#endif
#define movsd		movlps
#endif

#if defined(BARCELONA) || defined(SHANGHAI) || defined(BOBCAT) || defined(BARCELONA_OPTIMIZATION)
#define ALIGNED_ACCESS
#define MOVUPS_A	movaps
#define MOVUPS_XL	movaps
#define MOVUPS_XS	movaps
#define MOVUPS_YL	movaps
#define MOVUPS_YS	movaps

#define PREFETCH	prefetch
#define PREFETCHW	prefetchw
#ifndef COMPLEX
#define PREFETCHSIZE	64 * 2
#else
#define PREFETCHSIZE	64 * 4
#endif
#endif

#ifdef NANO
#define ALIGNED_ACCESS
#define MOVUPS_A	movaps
#define MOVUPS_XL	movaps
#define MOVUPS_XS	movaps
#define MOVUPS_YL	movaps
#define MOVUPS_YS	movaps
#define PREFETCH	prefetcht0
#ifndef COMPLEX
#define PREFETCHSIZE	64 * 1
#else
#define PREFETCHSIZE	64 * 2
#endif
#endif

#ifndef PREOFFSET
#ifdef L1_DATA_LINESIZE
#define PREOFFSET	(L1_DATA_LINESIZE >> 1)
#else
#define PREOFFSET	32
#endif
#endif

#ifndef  GEMV_UNROLL
#define  GEMV_UNROLL 4
#endif

#ifndef ZGEMV_UNROLL
#define ZGEMV_UNROLL 4
#endif

/* #define COPY_FORCE       */   /* Always copy X or Y to the buffer */
/* #define NOCOPY_UNALIGNED */   /* Not copy if X or Y is not aligned */

#ifdef MOVUPS_A
#define MOVUPS_A1(OFF, ADDR, REGS)		MOVUPS_A	OFF(ADDR), REGS
#define MOVUPS_A2(OFF, ADDR, BASE, SCALE, REGS)	MOVUPS_A	OFF(ADDR, BASE, SCALE), REGS
#else
#define MOVUPS_A1(OFF, ADDR, REGS)		movsd	OFF(ADDR), REGS; movhps	OFF + 8(ADDR), REGS
#define MOVUPS_A2(OFF, ADDR, BASE, SCALE, REGS)	movsd	OFF(ADDR, BASE, SCALE), REGS; movhps	OFF + 8(ADDR, BASE, SCALE), REGS
#endif

#define MOVRPS_A1(OFF, ADDR, REGS)		movsd	OFF + 8(ADDR), REGS; movhps	OFF(ADDR), REGS
#define MOVRPS_A2(OFF, ADDR, BASE, SCALE, REGS)	movsd	OFF + 8(ADDR, BASE, SCALE), REGS; movhps	OFF(ADDR, BASE, SCALE), REGS

#ifdef MOVUPS_XL
#define MOVUPS_XL1(OFF, ADDR, REGS)			MOVUPS_XL	OFF(ADDR), REGS
#else
#define MOVUPS_XL1(OFF, ADDR, REGS)			movsd	OFF(ADDR), REGS; movhps	OFF + 8(ADDR), REGS
#endif

#ifdef MOVUPS_XS
#define MOVUPS_XS1(OFF, ADDR, REGS)			MOVUPS_XS	REGS, OFF(ADDR)
#else
#define MOVUPS_XS1(OFF, ADDR, REGS)			movsd	REGS, OFF(ADDR); movhps	REGS, OFF + 8(ADDR)
#endif

#ifdef MOVUPS_YL
#define MOVUPS_YL1(OFF, ADDR, REGS)			MOVUPS_YL	OFF(ADDR), REGS
#else
#define MOVUPS_YL1(OFF, ADDR, REGS)			movsd	OFF(ADDR), REGS; movhps	OFF + 8(ADDR), REGS
#endif

#ifdef MOVUPS_YS
#define MOVUPS_YS1(OFF, ADDR, REGS)			MOVUPS_YS	REGS, OFF(ADDR)
#else
#define MOVUPS_YS1(OFF, ADDR, REGS)			movsd	REGS, OFF(ADDR); movhps	REGS, OFF + 8(ADDR)
#endif



#endif
