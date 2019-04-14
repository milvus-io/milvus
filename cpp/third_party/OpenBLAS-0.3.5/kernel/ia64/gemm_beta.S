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

#define PREFETCHSIZE 140

#define CO1	r14
#define CO2	r15
#define CO3	r16
#define DO1	r17
#define DO2	r18
#define DO3	r19

#define I	r22
#define I_AND_15 r23
#define PRE1	r24

#define PR	r30
#define ARLC	r31

#define M	r32
#define N	r33
#define C	r34
#define LDC	r35
#define J	r36

#define BETA	f8

	PROLOGUE
	.prologue
	PROFCODE

	{ .mmi
#ifndef XDOUBLE
	adds	CO1 = 16, r12
	adds	CO2 = 24, r12
#else
	adds	CO1 = 32, r12
	adds	CO2 = 40, r12
#endif
	.save	ar.lc, ARLC
	mov	ARLC = ar.lc
	}
	{ .mfb
	cmp.ge	p6, p0 = 0, N
	fcmp.eq	p0, p15 = BETA, f0
	(p6) br.ret.sptk.many b0
	}
	;;
	.body
	{ .mmi
	ld8	C = [CO1], 8
	ld8	LDC = [CO2]
	mov	PR = pr
	}
	{ .mmi
	mov	J = N
	shr	I = M, 4
	}
	;;
	{ .mmb
	shladd LDC = LDC, BASE_SHIFT, r0
	adds	I = -1, I
	(p15) br.cond.dpnt .L100		// if (beta != 0) goto L100
	}
	;;
	.align 32

.L60:
	{ .mmi
	mov	CO1 = C
	mov	CO3 = C
	add	CO2 = 4 * SIZE, C
	}
	{ .mmi
	adds	PRE1 = PREFETCHSIZE * SIZE, C
	add	C = C, LDC
	tbit.nz	p12, p0 = M, 3
	}
	;;
	{ .mmi
	and	I_AND_15 = 15, M
	mov	ar.lc = I
	}
	{ .mib
	cmp.gt	p8, p0 = 0, I
	(p8) br.cond.dpnt .L80
	}
	;;
	.align 32

.L70:
	{ .mmi
	STFD	[CO1] = f0, 1 * SIZE
	STFD	[CO2] = f0, 1 * SIZE
	}
	{ .mmi
	lfetch.excl.nt1	[PRE1]
	nop.m 0
	adds	PRE1 = 16 * SIZE, PRE1
	}
	;;
	{ .mmi
	STFD	[CO1] = f0, 1 * SIZE
	STFD	[CO2] = f0, 1 * SIZE
	adds	CO3 = 16 * SIZE, CO3
	}
	;;
	{ .mmi
	STFD	[CO1] = f0, 1 * SIZE
	STFD	[CO2] = f0, 1 * SIZE
	}
	;;
	{ .mmi
	STFD	[CO1] = f0, 5 * SIZE
	STFD	[CO2] = f0, 5 * SIZE
	}
	;;
	{ .mmi
	STFD	[CO1] = f0, 1 * SIZE
	STFD	[CO2] = f0, 1 * SIZE
	}
	;;
	{ .mmi
	STFD	[CO1] = f0, 1 * SIZE
	STFD	[CO2] = f0, 1 * SIZE
	}
	;;
	{ .mmi
	STFD	[CO1] = f0, 1 * SIZE
	STFD	[CO2] = f0, 1 * SIZE
	}
	;;
	{ .mmb
	STFD	[CO1] = f0, 5 * SIZE
	STFD	[CO2] = f0, 5 * SIZE
	br.cloop.sptk.few .L70
	}
	;;
	.align 32

.L80:
	{ .mmi
	(p12) STFD [CO1] = f0, 1 * SIZE
	(p12) STFD [CO2] = f0, 1 * SIZE
	tbit.nz	p13, p0 = M, 2
	}
	{ .mmb
	cmp.eq	p9, p0 = 0, I_AND_15
	adds	J = -1, J
	(p9) br.cond.dptk .L99
	}
	;;
	{ .mmi
	(p12) STFD [CO1] = f0, 1 * SIZE
	(p12) STFD [CO2] = f0, 1 * SIZE
	tbit.nz	p14, p0 = M, 1
	}
	;;
	{ .mmi
	(p12) STFD [CO1] = f0, 1 * SIZE
	(p12) STFD [CO2] = f0, 1 * SIZE
	(p12) adds CO3 = 8 * SIZE, CO3
	}
	;;
	{ .mmi
	(p12) STFD [CO1] = f0, 5 * SIZE
	(p12) STFD [CO2] = f0
	(p13) adds CO3 = 4 * SIZE, CO3
	}
	;;
	{ .mmi
	(p13) STFD [CO1] = f0, 1 * SIZE
	(p14) STFD [CO3] = f0, 1 * SIZE
	}
	;;
	{ .mmi
	(p13) STFD [CO1] = f0, 1 * SIZE
	(p14) STFD [CO3] = f0, 1 * SIZE
	tbit.nz	p15, p0 = M, 0
	}
	;;
	{ .mmi
	(p13) STFD [CO1] = f0, 1 * SIZE
	(p15) STFD [CO3] = f0
	}
	;;
	{ .mmi
	(p13) STFD [CO1] = f0
	}
	;;
	.align 32

.L99:
	{ .mib
	cmp.lt	p6, p0 = 0, J
	mov ar.lc = ARLC
	}
	{ .mbb
	(p6) br.cond.dptk .L60
	br.ret.sptk.many b0
	}
	;;
	.align 32

.L100:
	{ .mmi
	mov	CO1 = C
	mov	CO3 = C
	mov	pr.rot = 0
	}
	{ .mmi
	adds	PRE1 = PREFETCHSIZE * SIZE, C
	add	CO2 = 4 * SIZE, C
	mov	DO1 = C
	}
	;;
	{ .mmi
	mov	ar.ec = 6
	}
	{ .mmi
	adds	DO2 = 4 * SIZE, C
	mov	DO3 = C
	add	C = C, LDC
	}
	;;
	{ .mmi
	and	I_AND_15 = 15, M
	cmp.eq	p16, p0 = r0, r0
	mov	ar.lc = I
	}
	{ .mib
	cmp.gt	p8, p0 = 0, I
	tbit.nz	p12, p0 = M, 3
	(p8) br.cond.dpnt .L180
	}
	;;
	.align 32

.L170:
	{ .mmf
	(p21) STFD [DO1] = f6, 1 * SIZE
	(p21) STFD [DO2] = f7, 1 * SIZE
	(p21) FMPY f6  = BETA, f85
	}
	{ .mmf
	(p16) lfetch.excl.nt1	[PRE1]
	(p16) adds CO3 = 16 * SIZE, CO3
	(p21) FMPY f7  = BETA, f91
	}
	;;
	{ .mmf
	(p21) STFD [DO1] = f10, 1 * SIZE
	(p21) STFD [DO2] = f11, 1 * SIZE
	(p21) FMPY f10 = BETA, f97
	}
	{ .mmf
	(p16) LDFD f32 = [CO1], 1 * SIZE
	(p16) LDFD f38 = [CO2], 1 * SIZE
	(p21) FMPY f11 = BETA, f103
	}
	;;
	{ .mmf
	(p21) STFD [DO1] = f12, 1 * SIZE
	(p21) STFD [DO2] = f13, 1 * SIZE
	(p21) FMPY f12 = BETA, f109
	}
	{ .mmf
	(p16) LDFD f44 = [CO1], 1 * SIZE
	(p16) LDFD f50 = [CO2], 1 * SIZE
	(p21) FMPY f13 = BETA, f115
	}
	;;
	{ .mmf
	(p21) STFD [DO1] = f14, 5 * SIZE
	(p21) STFD [DO2] = f15, 5 * SIZE
	(p21) FMPY f14 = BETA, f121
	}
	{ .mmf
	(p16) LDFD f56 = [CO1], 1 * SIZE
	(p16) LDFD f62 = [CO2], 1 * SIZE
	(p21) FMPY f15 = BETA, f127
	}
	;;
	{ .mmf
	(p21) STFD [DO1] = f6, 1 * SIZE
	(p21) STFD [DO2] = f7, 1 * SIZE
	(p20) FMPY f6  = BETA, f36
	}
	{ .mmf
	(p16) LDFD f68 = [CO1], 5 * SIZE
	(p16) LDFD f74 = [CO2], 5 * SIZE
	(p20) FMPY f7  = BETA, f42
	}
	;;
	{ .mmf
	(p21) STFD [DO1] = f10, 1 * SIZE
	(p21) STFD [DO2] = f11, 1 * SIZE
	(p20) FMPY f10 = BETA, f48
	}
	{ .mmf
	(p16) LDFD f80 = [CO1], 1 * SIZE
	(p16) LDFD f86 = [CO2], 1 * SIZE
	(p20) FMPY f11 = BETA, f54
	}
	;;
	{ .mmf
	(p21) STFD [DO1] = f12, 1 * SIZE
	(p21) STFD [DO2] = f13, 1 * SIZE
	(p20) FMPY f12 = BETA, f60
	}
	{ .mmf
	(p16) LDFD f92 = [CO1], 1 * SIZE
	(p16) LDFD f98 = [CO2], 1 * SIZE
	(p20) FMPY f13 = BETA, f66
	}
	;;
	{ .mmf
	(p21) STFD [DO1] = f14, 5 * SIZE
	(p21) STFD [DO2] = f15, 5 * SIZE
	(p20) FMPY f14 = BETA, f72
	}
	{ .mmf
	(p16) LDFD f104 = [CO1], 1 * SIZE
	(p16) LDFD f110 = [CO2], 1 * SIZE
	(p20) FMPY f15 = BETA, f78
	}
	;;
	{ .mmi
	(p16) LDFD f116 = [CO1], 5 * SIZE
	(p16) LDFD f122 = [CO2], 5 * SIZE
	adds	PRE1 = 16 * SIZE, PRE1
	}
	{ .mmb
	(p16) adds DO3 = 16 * SIZE, DO3
	nop.m 0
	br.ctop.sptk.few .L170
	}
	;;
	.align 32

.L180:
	{ .mmi
	(p12) LDFD f32 = [CO1], 1 * SIZE
	(p12) LDFD f36 = [CO2], 1 * SIZE
	tbit.nz	p13, p0 = M, 2
	}
	{ .mmb
	cmp.eq	p9, p0 = 0, I_AND_15
	adds	J = -1, J
	(p9) br.cond.dptk .L199
	}
	;;
	{ .mmi
	(p12) LDFD f33 = [CO1], 1 * SIZE
	(p12) LDFD f37 = [CO2], 1 * SIZE
	tbit.nz	p14, p0 = M, 1
	}
	;;
	{ .mmi
	(p12) LDFD f34 = [CO1], 1 * SIZE
	(p12) LDFD f38 = [CO2], 1 * SIZE
	(p12) adds CO3 = 8 * SIZE, CO3
	}
	;;
	{ .mmi
	(p12) LDFD f35 = [CO1], 5 * SIZE
	(p12) LDFD f39 = [CO2]
	(p13) adds CO3 = 4 * SIZE, CO3
	}
	;;
	{ .mmi
	(p13) LDFD f40 = [CO1], 1 * SIZE
	(p14) LDFD f44 = [CO3], 1 * SIZE
	}
	;;
	{ .mmi
	(p13) LDFD f41 = [CO1], 1 * SIZE
	(p14) LDFD f45 = [CO3], 1 * SIZE
	tbit.nz	p15, p0 = M, 0
	}
	;;
	{ .mmf
	(p13) LDFD f42 = [CO1], 1 * SIZE
	(p15) LDFD f46 = [CO3]
	(p12) FMPY f32 = BETA, f32
	}
	{ .mmf
	(p12) FMPY f36 = BETA, f36
	}
	;;
	{ .mmf
	(p13) LDFD f43 = [CO1]
	(p12) FMPY f33 = BETA, f33
	}
	{ .mmf
	(p12) FMPY f37 = BETA, f37
	}
	;;
	(p12) FMPY f34 = BETA, f34
	(p12) FMPY f38 = BETA, f38
	(p12) FMPY f35 = BETA, f35
	(p12) FMPY f39 = BETA, f39

	;;
	{ .mmf
	(p12) STFD [DO1] = f32, 1 * SIZE
	(p12) STFD [DO2] = f36, 1 * SIZE
	(p13) FMPY f40 = BETA, f40
	}
	{ .mmf
	(p12) adds DO3 = 8 * SIZE, DO3
	(p14) FMPY f44 = BETA, f44
	}
	;;
	{ .mmf
	(p12) STFD [DO1] = f33, 1 * SIZE
	(p12) STFD [DO2] = f37, 1 * SIZE
	(p13) FMPY f41 = BETA, f41
	}
	{ .mmf
	(p13) adds DO3 = 4 * SIZE, DO3
	(p14) FMPY f45 = BETA, f45
	}
	;;
	{ .mmf
	(p12) STFD [DO1] = f34, 1 * SIZE
	(p12) STFD [DO2] = f38, 1 * SIZE
	(p13) FMPY f42 = BETA, f42
	}
	{ .mmf
	(p15) FMPY f46 = BETA, f46
	}
	;;
	{ .mmf
	(p12) STFD [DO1] = f35, 5 * SIZE
	(p12) STFD [DO2] = f39
	(p13) FMPY f43 = BETA, f43
	}
	;;
	{ .mmi
	(p13) STFD [DO1] = f40, 1 * SIZE
	(p14) STFD [DO3] = f44, 1 * SIZE
	}
	;;
	{ .mmi
	(p13) STFD [DO1] = f41, 1 * SIZE
	(p14) STFD [DO3] = f45, 1 * SIZE
	}
	;;
	{ .mmi
	(p13) STFD [DO1] = f42, 1 * SIZE
	(p15) STFD [DO3] = f46
	}
	;;
	{ .mmi
	(p13) STFD [DO1] = f43
	}
	;;
	.align 32

.L199:
	{ .mib
	cmp.lt	p6, p0 = 0, J
	mov ar.lc = ARLC
	(p6) br.cond.dptk .L100
	}
	;;
	{ .mib
	mov	pr = PR, -1
	br.ret.sptk.many b0
	}
	;;
	EPILOGUE

