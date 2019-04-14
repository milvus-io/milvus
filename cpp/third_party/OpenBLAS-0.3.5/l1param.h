#if defined(CORE2) || defined(PENRYN)
#define ALIGNED_ACCESS
#endif

#ifdef NEHALEM
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE (128 *  12)
#define ALIGNED_ACCESS
#endif

#ifdef SANDYBRIDGE
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#define PREFETCHSIZE (128 *  12)
#define ALIGNED_ACCESS
#endif

#ifdef ATHLON
#define PREFETCH	prefetch
#define PREFETCHW	prefetchw
#define PREFETCHSIZE (128 *  10)
#define ALIGNED_ACCESS
#define movsd	movlps
#endif

#ifdef PENTIUM3
#define PREFETCH	prefetcht0
#define PREFETCHSIZE (128 *  10)
#define ALIGNED_ACCESS
#define movsd	movlps
#endif

#ifdef PENTIUM4
#define PREFETCH	prefetcht0
#define PREFETCHSIZE (128 *  10)
#define FETCH128
#define ALIGNED_ACCESS
#define xorps	pxor
#define xorpd	pxor
#endif

#ifdef ATOM
#define ALIGNED_ACCESS
#define PREFETCH	prefetcht0
#define PREFETCHSIZE ( 64 * 12 + 32)
#endif

#ifdef OPTERON
#define PREFETCH	prefetch
#define PREFETCHW	prefetchw
#define PREFETCHSIZE (128 *   3)
#define movsd	movlps
#endif

#ifdef BARCELONA
#define PREFETCH	prefetch
#define PREFETCHW	prefetchw
#define PREFETCHSIZE (128 *   5)
#define ALIGNED_ACCESS
#endif

#ifdef SHANGHAI
#define PREFETCH	prefetch
#define PREFETCHW	prefetchw
#define PREFETCHSIZE (128 *   5)
#define ALIGNED_ACCESS
#endif

#ifdef BOBCAT
#define PREFETCH	prefetch
#define PREFETCHW	prefetchw
#define PREFETCHSIZE (128 *   5)
#define ALIGNED_ACCESS
#endif

#ifdef BULLDOZER
#define PREFETCH	prefetch
#define PREFETCHW	prefetchw
#define PREFETCHSIZE (128 *   5)
#define ALIGNED_ACCESS
#endif

#ifdef NANO
#define PREFETCH        prefetcht0
#define PREFETCHW       prefetcht0
#define PREFETCHSIZE (128 *   4)
#define ALIGNED_ACCESS
#endif

#define PREOFFSET 128


#ifdef HAVE_SSE2
#define PSHUFD1(A, B)		pshufd	A, B, B
#define PSHUFD2(A, B, C)	pshufd	A, B, C
#else
#define PSHUFD1(A, B)		shufps	A, B, B
#define PSHUFD2(A, B, C)	movaps	B, C; shufps	A, C, C
#endif

#define MOVDDUP1(OFFSET, BASE, REGS)	movddup	OFFSET(BASE), REGS

#define MOVAPS(OFFSET, BASE, REGS)	movlps	REGS, OFFSET(BASE); movhps REGS, OFFSET + SIZE(BASE)

