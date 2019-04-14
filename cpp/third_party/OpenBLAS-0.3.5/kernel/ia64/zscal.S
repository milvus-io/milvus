/*********************************************************************/
/* Copyright 2009, 2010 The University of Texas at Austin.           */
/* All rights reserved.                                              */
/*                                                                   */
/* Redistribution and use in source and binary forms, with or        */
/* without modification, are permitted provided that the following   */
/* conditions are met:                                               */
/*                                                                   */
/*   1. Redistributions of source code must retain the above         */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer.                                                  */
/*                                                                   */
/*   2. Redistributions in binary form must reproduce the above      */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer in the documentation and/or other materials       */
/*      provided with the distribution.                              */
/*                                                                   */
/*    THIS  SOFTWARE IS PROVIDED  BY THE  UNIVERSITY OF  TEXAS AT    */
/*    AUSTIN  ``AS IS''  AND ANY  EXPRESS OR  IMPLIED WARRANTIES,    */
/*    INCLUDING, BUT  NOT LIMITED  TO, THE IMPLIED  WARRANTIES OF    */
/*    MERCHANTABILITY  AND FITNESS FOR  A PARTICULAR  PURPOSE ARE    */
/*    DISCLAIMED.  IN  NO EVENT SHALL THE UNIVERSITY  OF TEXAS AT    */
/*    AUSTIN OR CONTRIBUTORS BE  LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL,  SPECIAL, EXEMPLARY,  OR  CONSEQUENTIAL DAMAGES    */
/*    (INCLUDING, BUT  NOT LIMITED TO,  PROCUREMENT OF SUBSTITUTE    */
/*    GOODS  OR  SERVICES; LOSS  OF  USE,  DATA,  OR PROFITS;  OR    */
/*    BUSINESS INTERRUPTION) HOWEVER CAUSED  AND ON ANY THEORY OF    */
/*    LIABILITY, WHETHER  IN CONTRACT, STRICT  LIABILITY, OR TORT    */
/*    (INCLUDING NEGLIGENCE OR OTHERWISE)  ARISING IN ANY WAY OUT    */
/*    OF  THE  USE OF  THIS  SOFTWARE,  EVEN  IF ADVISED  OF  THE    */
/*    POSSIBILITY OF SUCH DAMAGE.                                    */
/*                                                                   */
/* The views and conclusions contained in the software and           */
/* documentation are those of the authors and should not be          */
/* interpreted as representing official policies, either expressed   */
/* or implied, of The University of Texas at Austin.                 */
/*********************************************************************/

#define ASSEMBLER
#include "common.h"

#ifdef XDOUBLE
#define PREFETCH_SIZE ( 8 * 16)
#elif defined(DOUBLE)
#define PREFETCH_SIZE (16 * 16)
#else
#define PREFETCH_SIZE (32 * 16)
#endif

#define SP	r12

#ifdef XDOUBLE
#define N	r32
#define X1	r14
#define INCX	r15
#else
#define N	r32
#define X1	r37
#define INCX	r38
#endif

#define X2		r16
#define Y1		r17
#define INCX3		r18
#define PRE		r19
#define INCX8		r20
#define I		r29
#define J		r28

#define PR		r30
#define ARLC		r31

#define ALPHA_R		f8
#define ALPHA_I		f9

	PROLOGUE
	.prologue
	PROFCODE
	{.mmi
	adds	r22 = 16, SP
	adds	r23 = 24, SP
	mov	PR = pr
	}
	{ .mib
	cmp.ge	p7, p0 = 0, N
	shr	I = N, 3
	(p7) br.ret.sptk.many b0
	}
	;;
#ifdef XDOUBLE
	{ .mmi
	ld8	X1     = [r22]
	ld8	INCX   = [r23]
	nop	__LINE__
	}
	;;
#endif
	{ .mfi
	and	J = 7, N
 	fcmp.eq	p0, p11 = ALPHA_I, f0
	.save	ar.lc, ARLC
	mov	ARLC = ar.lc
	}
	{ .mfi
	adds	I = -1, I
	fcmp.eq	p0, p10 = ALPHA_R, f0
	shl	INCX = INCX, ZBASE_SHIFT
	}
	;;
	.body
	{ .mmi
	shladd	INCX8 = INCX, 3, r0
	shladd	X2 = INCX, 1, X1
	mov	pr.rot= 0
	}
	{ .mmi
	shladd	INCX3 = INCX, 1, INCX
	adds	PRE = PREFETCH_SIZE * SIZE, X1
	mov	Y1 = X1
	}
	;;
	{ .mmi
	cmp.gt	p8, p0 = 0, I
	cmp.ge	p9, p0 = 0, J
	mov	ar.lc = I
	}
	{ .mmi
	adds	INCX =  -1 * SIZE, INCX
	adds	INCX3 = -1 * SIZE, INCX3
	tbit.z	p0, p13 = N, 2
	}
	;;
	{ .bbb
	(p10) br.cond.dptk .L100
	(p11) br.cond.dptk .L100
	(p8)  br.cond.dpnt .L20
	}
	;;
	.align 32

.L10:
	{ .mmb
	STFD	[X1] = f0, 1 * SIZE
	STFD	[X2] = f0, 1 * SIZE
	nop.b	0
	}
	{ .mmb
	lfetch.excl.nt1	[PRE], INCX8
	nop.m	0
	}
	;;
	{ .mmb
	STFD	[X1] = f0
	add	X1 = INCX, X1
	}
	{ .mmb
	STFD	[X2] = f0
	add	X2 = INCX, X2
	}
	;;
	{ .mmb
	STFD	[X1] = f0, 1 * SIZE
	STFD	[X2] = f0, 1 * SIZE
	nop.b	0
	}
	;;
	{ .mmb
	STFD	[X1] = f0
	add	X1 = INCX3, X1
	}
	{ .mmb
	STFD	[X2] = f0
	add	X2 = INCX3, X2
	}
	;;
	{ .mmb
	STFD	[X1] = f0, 1 * SIZE
	STFD	[X2] = f0, 1 * SIZE
	nop.b	0
	}
	;;
	{ .mmb
	STFD	[X1] = f0
	add	X1 = INCX, X1
	}
	{ .mmb
	STFD	[X2] = f0
	add	X2 = INCX, X2
	}
	;;
	{ .mmb
	STFD	[X1] = f0, 1 * SIZE
	STFD	[X2] = f0, 1 * SIZE
	nop.b	0
	}
	;;
	{ .mmb
	STFD	[X1] = f0
	add	X1 = INCX3, X1
	}
	{ .mmb
	STFD	[X2] = f0
	add	X2 = INCX3, X2
	br.cloop.sptk.few .L10
	}
	;;
	.align 32

.L20:
	{ .mmi
	(p13) STFD	[X1] = f0, 1 * SIZE
	(p13) STFD	[X2] = f0, 1 * SIZE
	mov	ar.lc = ARLC
	}
	;;
	{ .mmi
	(p13) STFD	[X1] = f0
	(p13) add	X1 = INCX, X1
	tbit.z	p0, p14 = N, 1
	}
	{ .mmi
	(p13) STFD	[X2] = f0
	(p13) add	X2 = INCX, X2
	tbit.z	p0, p15 = N, 0
	}
	;;
	{ .mmb
	(p13) STFD	[X1] = f0, 1 * SIZE
	(p13) STFD	[X2] = f0, 1 * SIZE
	nop.b 0
	}
	{ .mib
	nop.m	0
	mov	pr = PR, -65474
	(p9) br.ret.sptk.many b0
	}
	;;
	{ .mmb
	(p13) STFD	[X1] = f0
	(p13) add	X1 = INCX3, X1
	}
	{ .mmb
	(p13) STFD	[X2] = f0
	(p13) add	X2 = INCX3, X2
	}
	;;
	(p14) STFD	[X1] = f0, 1 * SIZE
	;;
	{ .mmb
	(p14) STFD	[X1] = f0
	(p14) add	X1 = INCX, X1
	}
	;;
	(p14) STFD	[X1] = f0, 1 * SIZE
	;;
	{ .mmb
	(p14) STFD	[X1] = f0
	(p14) add	X1 = INCX, X1
	}
	;;
	(p15) STFD	[X1] = f0, 1 * SIZE
	;;
	{ .mib
	(p15) STFD	[X1] = f0
	mov	pr = PR, -65474
	br.ret.sptk.many b0
	}
	;;
	.align 32

.L100:
	cmp.eq	p16, p0 = r0, r0
	mov.i	ar.ec = 6
	(p8) br.cond.dpnt .L170
	;;
	.align 32

.L160:
	{ .mmf
	(p21) STFD	[X1] = f6,  1 * SIZE
	(p16) lfetch.excl.nt1	[PRE], INCX8
	(p21) FMS	f12  = ALPHA_R, f85,  f12
	}
	{ .mfb
	(p16) LDFD	f32  = [Y1], 1 * SIZE
	(p20) FMPY	f6   = ALPHA_I, f42
	}
	;;
	{ .mmf
	(p21) STFD	[X1] = f43
	(p21) add X1 = INCX, X1
	(p21) FMA	f91  = ALPHA_I, f85,  f91
	}
	{ .mfb
	(p16) LDFD	f38  = [Y1], INCX
	(p20) FMPY	f42  = ALPHA_R, f42
	}
	;;
	{ .mmf
	(p21) STFD	[X1] = f7,  1 * SIZE
	(p21) FMS	f13  = ALPHA_R, f97,  f13
	}
	{ .mfb
	(p16) LDFD	f44  = [Y1], 1 * SIZE
	(p20) FMPY	f7   = ALPHA_I, f54
	}
	;;
	{ .mmf
	(p21) STFD	[X1] = f55
	(p21) add X1 = INCX, X1
	(p21) FMA	f103 = ALPHA_I, f97,  f103
	}
	{ .mfb
	(p16) LDFD	f50  = [Y1], INCX
	(p20) FMPY	f54  = ALPHA_R, f54
	}
	;;
	{ .mmf
	(p21) STFD	[X1] = f10, 1 * SIZE
	(p21) FMS	f14  = ALPHA_R, f109, f14
	}
	{ .mfb
	(p16) LDFD	f56  = [Y1], 1 * SIZE
	(p20) FMPY	f10  = ALPHA_I, f66
	}
	;;
	{ .mmf
	(p21) STFD	[X1] = f67
	(p21) add X1 = INCX, X1
	(p21) FMA	f115 = ALPHA_I, f109, f115
	}
	{ .mfb
	(p16) LDFD	f62  = [Y1], INCX
	(p20) FMPY	f66  = ALPHA_R, f66
	}
	;;
	{ .mmf
	(p21) STFD	[X1] = f11, 1 * SIZE
	(p21) FMS	f15  = ALPHA_R, f121, f15
	}
	{ .mfb
	(p16) LDFD	f68  = [Y1], 1 * SIZE
	(p20) FMPY	f11  = ALPHA_I, f78
	}
	;;
	{ .mmf
	(p21) STFD	[X1] = f79
	(p21) add X1 = INCX, X1
	(p21) FMA	f127 = ALPHA_I, f121, f127
	}
	{ .mfb
	(p16) LDFD	f74  = [Y1], INCX
	(p20) FMPY	f78  = ALPHA_R, f78
	}
	;;
	{ .mmf
	(p21) STFD	[X1] = f12, 1 * SIZE
	(p20) FMS	f6   = ALPHA_R, f36, f6
	}
	{ .mfb
	(p16) LDFD	f80  = [Y1], 1 * SIZE
	(p20) FMPY	f12  = ALPHA_I, f90
	}
	;;
	{ .mmf
	(p21) STFD	[X1] = f91
	(p21) add X1 = INCX, X1
	(p20) FMA	f42  = ALPHA_I, f36, f42
	}
	{ .mfb
	(p16) LDFD	f86  = [Y1], INCX
	(p20) FMPY	f90  = ALPHA_R, f90
	}
	;;
	{ .mmf
	(p21) STFD	[X1] = f13, 1 * SIZE
	(p20) FMS	f7   = ALPHA_R, f48, f7
	}
	{ .mfb
	(p16) LDFD	f92  = [Y1], 1 * SIZE
	(p20) FMPY	f13  = ALPHA_I, f102
	}
	;;
	{ .mmf
	(p21) STFD	[X1] = f103
	(p21) add X1 = INCX, X1
	(p20) FMA	f54  = ALPHA_I, f48, f54
	}
	{ .mfb
	(p16) LDFD	f98  = [Y1], INCX
	(p20) FMPY	f102 = ALPHA_R, f102
	}
	;;
	{ .mmf
	(p21) STFD	[X1] = f14, 1 * SIZE
	(p20) FMS	f10  = ALPHA_R, f60, f10
	}
	{ .mfb
	(p16) LDFD	f104 = [Y1], 1 * SIZE
	(p20) FMPY	f14  = ALPHA_I, f114
	}
	;;
	{ .mmf
	(p21) STFD	[X1] = f115
	(p21) add X1 = INCX, X1
	(p20) FMA	f66  = ALPHA_I, f60, f66
	}
	{ .mfb
	(p16) LDFD	f110 = [Y1], INCX
	(p20) FMPY	f114 = ALPHA_R, f114
	}
	;;
	{ .mmf
	(p21) STFD	[X1] = f15, 1 * SIZE
	(p20) FMS	f11  = ALPHA_R, f72, f11
	}
	{ .mfb
	(p16) LDFD	f116 = [Y1], 1 * SIZE
	(p20) FMPY	f15  = ALPHA_I, f126
	}
	;;
	{ .mmf
	(p21) STFD	[X1] = f127
	(p21) add X1 = INCX, X1
	(p20) FMA	f78  = ALPHA_I, f72, f78
	}
	{ .mfb
	(p16) LDFD	f122 = [Y1], INCX
	(p20) FMPY	f126 = ALPHA_R, f126
	br.ctop.sptk.few .L160
	}
	;;
	.align 16

.L170:
	{ .mmi
	(p13) LDFD	f48 = [Y1], 1 * SIZE
	mov	ar.lc = ARLC
	}
	;;
	{ .mib
	(p13) LDFD	f49 = [Y1], INCX
	mov	pr = PR, -65474
	(p9) br.ret.sptk.many b0
	}
	;;
	(p13) LDFD	f50 = [Y1], 1 * SIZE
	tbit.z	p0, p14 = N, 1
	;;
	(p13) LDFD	f51 = [Y1], INCX
	tbit.z	p0, p15 = N, 0
	;;
	(p13) LDFD	f52 = [Y1], 1 * SIZE
	;;
	(p13) LDFD	f53 = [Y1], INCX
	;;
	(p13) LDFD	f54 = [Y1], 1 * SIZE
	(p13) FMPY	f112 = ALPHA_I, f48
	;;
	(p13) LDFD	f55 = [Y1], INCX
	(p13) FMPY	f111 = ALPHA_I, f49
	;;
	(p14) LDFD	f56 = [Y1], 1 * SIZE
	(p13) FMPY	f114 = ALPHA_I, f50
	;;
	(p14) LDFD	f57 = [Y1], INCX
	(p13) FMPY	f113 = ALPHA_I, f51
	;;
	(p14) LDFD	f58 = [Y1], 1 * SIZE
	(p13) FMPY	f116 = ALPHA_I, f52
	;;
	(p14) LDFD	f59 = [Y1], INCX
	(p13) FMPY	f115 = ALPHA_I, f53
	;;
	(p15) LDFD	f60 = [Y1], 1 * SIZE
	(p13) FMPY	f118 = ALPHA_I, f54
	;;
	(p15) LDFD	f61 = [Y1], INCX
	(p13) FMPY	f117 = ALPHA_I, f55
	;;
	(p14) FMPY	f120 = ALPHA_I, f56
	(p14) FMPY	f119 = ALPHA_I, f57
	(p14) FMPY	f122 = ALPHA_I, f58
	(p14) FMPY	f121 = ALPHA_I, f59
	(p15) FMPY	f124 = ALPHA_I, f60
	(p15) FMPY	f123 = ALPHA_I, f61
	;;
	(p13) FMS	f48  = ALPHA_R, f48, f111
	(p13) FMA	f49  = ALPHA_R, f49, f112
	(p13) FMS	f50  = ALPHA_R, f50, f113
	(p13) FMA	f51  = ALPHA_R, f51, f114

	;;
	(p13) STFD	[X1] = f48, 1 * SIZE
	(p13) FMS	f52  = ALPHA_R, f52, f115
	;;
	(p13) STFD	[X1] = f49
	(p13) add X1 = INCX, X1
	(p13) FMA	f53  = ALPHA_R, f53, f116
	;;
	(p13) STFD	[X1] = f50, 1 * SIZE
	(p13) FMS	f54  = ALPHA_R, f54, f117
	;;
	(p13) STFD	[X1] = f51
	(p13) add X1 = INCX, X1
	(p13) FMA	f55  = ALPHA_R, f55, f118
	;;
	(p13) STFD	[X1] = f52, 1 * SIZE
	(p14) FMS	f56  = ALPHA_R, f56, f119
	;;
	(p13) STFD	[X1] = f53
	(p13) add X1 = INCX, X1
	(p14) FMA	f57  = ALPHA_R, f57, f120
	;;
	(p13) STFD	[X1] = f54, 1 * SIZE
	(p14) FMS	f58  = ALPHA_R, f58, f121
	;;
	(p13) STFD	[X1] = f55
	(p13) add X1 = INCX, X1
	(p14) FMA	f59  = ALPHA_R, f59, f122
	;;
	(p14) STFD	[X1] = f56, 1 * SIZE
	(p15) FMS	f60  = ALPHA_R, f60, f123
	;;
	(p14) STFD	[X1] = f57
	(p14) add X1 = INCX, X1
	(p15) FMA	f61  = ALPHA_R, f61, f124
	;;
	(p14) STFD	[X1] = f58, 1 * SIZE
	;;
	(p14) STFD	[X1] = f59
	(p14) add X1 = INCX, X1
	;;
	(p15) STFD	[X1] = f60, 1 * SIZE
	;;
	(p15) STFD	[X1] = f61
	mov	pr = PR, -65474
	br.ret.sptk.many b0

	EPILOGUE
