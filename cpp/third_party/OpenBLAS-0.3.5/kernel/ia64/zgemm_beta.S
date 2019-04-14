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

#define PREFETCHSIZE 74

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

#define BETA_R	f8
#define BETA_I	f9

	PROLOGUE
	.prologue
	PROFCODE

	{ .mmi
	adds	CO1 = 24, r12
	adds	CO2 = 32, r12
	.save	ar.lc, ARLC
	mov	ARLC = ar.lc
	}
	{ .mfb
	cmp.ge	p6, p0 = 0, N
	fcmp.eq	p0, p14 = BETA_R, f0
	(p6) br.ret.sptk.many b0
	}
	;;
	.body
	{ .mmi
	ld8	C = [CO1], 8
	ld8	LDC = [CO2]
	mov	PR = pr
	}
	{ .mfi
	mov	J = N
	fcmp.eq	p0, p15 = BETA_I, f0
	shr	I = M, 3
	}
	;;
	{ .mmb
	cmp.ge	p6, p0 = 0, M
	adds	I = -1, I
	(p6) br.ret.sptk.many b0
	}
	;;
	{ .mbb
	shladd LDC = LDC, ZBASE_SHIFT, r0
	(p14) br.cond.dpnt .L100
	(p15) br.cond.dpnt .L100
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
	tbit.nz	p12, p0 = M, 2
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
	lfetch.excl.nt1	[PRE1], 16 * SIZE
	nop.m 0
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
	tbit.nz	p13, p0 = M, 1
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
	tbit.nz	p14, p0 = M, 0
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
	}
	;;
	{ .mmi
	(p13) STFD [CO1] = f0, 1 * SIZE
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
	tbit.nz	p12, p0 = M, 2
	(p8) br.cond.dpnt .L180
	}
	;;
	.align 32

.L170:
	{ .mmf
	(p21) STFD [DO1] = f37, 1 * SIZE
	(p16) lfetch.excl.nt1	[PRE1], 16 * SIZE
	(p21) FNMA f61  = BETA_I, f67, f61
	}
	{ .mmf
	(p16) LDFD f32  = [CO1], 1 * SIZE
	(p16) adds CO2 = 16 * SIZE, CO2
	(p21) FMPY f12  = BETA_I, f85
	}
	;;
	{ .mfi
	(p21) STFD [DO1] = f43, 1 * SIZE
	(p21) FMA  f67  = BETA_R, f67, f10
	(p16) adds CO3 = 16 * SIZE, CO3
	}
	{ .mfi
	(p16) LDFD f38  = [CO1], 1 * SIZE
	(p21) FMPY f85  = BETA_R, f85
	(p16) adds DO2 = 16 * SIZE, DO2
	}
	;;
	{ .mfi
	(p21) STFD [DO1] = f49, 1 * SIZE
	(p21) FNMA f73  = BETA_I, f79, f73
	(p16) adds DO3 = 16 * SIZE, DO3
	}
	{ .mfi
	(p16) LDFD f44  = [CO1], 1 * SIZE
	(p21) FMPY f13  = BETA_I, f97
	nop.i 0
	}
	;;
	(p21) STFD [DO1] = f55, 1 * SIZE
	(p21) FMA  f79  = BETA_R, f79, f11
	(p16) LDFD f50  = [CO1], 1 * SIZE
	(p21) FMPY f97  = BETA_R, f97
	;;
	(p21) STFD [DO1] = f61, 1 * SIZE
	(p21) FNMA f85  = BETA_I, f91,  f85
	(p16) LDFD f56  = [CO1], 1 * SIZE
	(p21) FMPY f14  = BETA_I, f109
	;;
	(p21) STFD [DO1] = f67, 1 * SIZE
	(p21) FMA  f91  = BETA_R, f91,  f12
	(p16) LDFD f62  = [CO1], 1 * SIZE
	(p21) FMPY f109 = BETA_R, f109
	;;
	(p21) STFD [DO1] = f73, 1 * SIZE
	(p21) FNMA f97  = BETA_I, f103, f97
	(p16) LDFD f68  = [CO1], 1 * SIZE
	(p21) FMPY f15  = BETA_I, f121
	;;
	(p21) STFD [DO1] = f79, 1 * SIZE
	(p21) FMA  f103 = BETA_R, f103, f13
	(p16) LDFD f74  = [CO1], 1 * SIZE
	(p21) FMPY f121 = BETA_R, f121
	;;
	(p21) STFD [DO1] = f85,  1 * SIZE
	(p21) FNMA f109 = BETA_I, f115, f109
	(p16) LDFD f80  = [CO1], 1 * SIZE
	(p20) FMPY f6  = BETA_I, f36
	;;
	(p21) STFD [DO1] = f91,  1 * SIZE
	(p21) FMA  f115 = BETA_R, f115, f14
	(p16) LDFD f86  = [CO1], 1 * SIZE
	(p20) FMPY f36 = BETA_R, f36
	;;
	(p21) STFD [DO1] = f97,  1 * SIZE
	(p21) FNMA f121 = BETA_I, f127, f121
	(p16) LDFD f92  = [CO1], 1 * SIZE
	(p20) FMPY f7  = BETA_I, f48
	;;
	(p21) STFD [DO1] = f103, 1 * SIZE
	(p21) FMA  f127 = BETA_R, f127, f15
	(p16) LDFD f98  = [CO1], 1 * SIZE
	(p20) FMPY f48 = BETA_R, f48
	;;
	(p21) STFD [DO1] = f109, 1 * SIZE
	(p20) FNMA f36  = BETA_I, f42, f36
	(p16) LDFD f104 = [CO1], 1 * SIZE
	(p20) FMPY f10 = BETA_I, f60
	;;
	(p21) STFD [DO1] = f115, 1 * SIZE
	(p20) FMA  f42  = BETA_R, f42, f6
	(p16) LDFD f110 = [CO1], 1 * SIZE
	(p20) FMPY f60 = BETA_R, f60
	;;
	(p21) STFD [DO1] = f121, 1 * SIZE
	(p20) FNMA f48  = BETA_I, f54, f48
	(p16) LDFD f116 = [CO1], 1 * SIZE
	(p20) FMPY f11 = BETA_I, f72
	;;
	(p21) STFD [DO1] = f127, 1 * SIZE
	(p20) FMA  f54  = BETA_R, f54, f7
	(p16) LDFD f122 = [CO1], 1 * SIZE
	(p20) FMPY f72 = BETA_R, f72
	br.ctop.sptk.few .L170
	;;
	.align 32

.L180:
	{ .mmi
	(p12) LDFD f32 = [CO1], 1 * SIZE
	(p12) LDFD f36 = [CO2], 1 * SIZE
	tbit.nz	p13, p0 = M, 1
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
	tbit.nz	p14, p0 = M, 0
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
	}
	;;
	{ .mmf
	(p13) LDFD f42 = [CO1], 1 * SIZE
	}
	;;
	{ .mmf
	(p13) LDFD f43 = [CO1]
	}
	;;
	(p12) FMPY f80 = BETA_I, f32
	(p12) FMPY f32 = BETA_R, f32
	(p12) FMPY f81 = BETA_I, f34
	(p12) FMPY f34 = BETA_R, f34
	(p12) FMPY f82 = BETA_I, f36
	(p12) FMPY f36 = BETA_R, f36
	(p12) FMPY f83 = BETA_I, f38
	(p12) FMPY f38 = BETA_R, f38
	;;
	(p12) FNMA f32 = BETA_I, f33, f32
	(p12) FMA  f33 = BETA_R, f33, f80
	(p12) FNMA f34 = BETA_I, f35, f34
	(p12) FMA  f35 = BETA_R, f35, f81
	(p12) FNMA f36 = BETA_I, f37, f36
	(p12) FMA  f37 = BETA_R, f37, f82
	(p12) FNMA f38 = BETA_I, f39, f38
	(p12) FMA  f39 = BETA_R, f39, f83
	;;
	(p13) FMPY f84 = BETA_I, f40
	(p13) FMPY f40 = BETA_R, f40
	(p13) FMPY f85 = BETA_I, f42
	(p13) FMPY f42 = BETA_R, f42
	(p14) FMPY f86 = BETA_I, f44
	(p14) FMPY f44 = BETA_R, f44
	;;
	(p13) FNMA f40 = BETA_I, f41, f40
	(p13) FMA  f41 = BETA_R, f41, f84
	(p13) FNMA f42 = BETA_I, f43, f42
	(p13) FMA  f43 = BETA_R, f43, f85
	(p14) FNMA f44 = BETA_I, f45, f44
	(p14) FMA  f45 = BETA_R, f45, f86
	;;

	{ .mmf
	(p12) STFD [DO1] = f32, 1 * SIZE
	(p12) STFD [DO2] = f36, 1 * SIZE
	}
	{ .mmf
	(p12) adds DO3 = 8 * SIZE, DO3
	}
	;;
	{ .mmf
	(p12) STFD [DO1] = f33, 1 * SIZE
	(p12) STFD [DO2] = f37, 1 * SIZE
	}
	{ .mmf
	(p13) adds DO3 = 4 * SIZE, DO3
	}
	;;
	{ .mmf
	(p12) STFD [DO1] = f34, 1 * SIZE
	(p12) STFD [DO2] = f38, 1 * SIZE
	}
	;;
	{ .mmf
	(p12) STFD [DO1] = f35, 5 * SIZE
	(p12) STFD [DO2] = f39
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
	;;
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

