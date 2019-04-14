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

#define P	4096
#define SP	r12

#define M	r32
#define N	r33
#define A	r36
#define LDA	r37
#define X	r38
#define INCX	r39
#define Y	r34
#define INCY	r35
#define BUFFER	r11

#define MIN_M	r14
#define I	r15
#define J	r16
#define IS	r17
#define AO1	r18
#define AO2	r19
#define AO3	r20
#define AO4	r21
#define AO5	r22
#define AO6	r23
#define AO7	r24
#define AO8	r25
#define BO	r26
#define LDAP	r27

#define RPRE1	loc0
#define RPRE2	loc1
#define RPRE3	loc2
#define RPRE4	loc3
#define RPRE5	loc4
#define RPRE6	loc5
#define RPRE7	loc6
#define RPRE8	loc7

#define AO21	loc8
#define AO41	loc9
#define AO61	loc10
#define AO81	loc11

#define PREB	r8
#define WPRE	r9
#define OFFSET	PREB
#define CO	r10

#define ARLC	r29
#define PR	r30
#define ARPFS	r31

#ifdef DOUBLE
#define RPREFETCH	(16 * 3 +  8)
#else
#define RPREFETCH	(16 * 3 + 16)
#endif
#define PREFETCH	lfetch.nt1

#define ALPHA	f6

	PROLOGUE
	.prologue
	PROFCODE
	{ .mmi
	.save	ar.pfs, ARPFS
	alloc	ARPFS = ar.pfs, 8, 16, 8, 0
	setf.sig f11 = LDA
	mov	ARLC  = ar.lc
	}
	{ .mmi
	adds	r15 = 24, SP
	adds	r16 = 32, SP
	adds	r14 = 16, SP
	}
	;;
	{ .mmi
	setf.sig f10 = N
	ld8	Y      = [r14]
	mov	PR = pr
	}
	{ .mmi
	ld8	INCY   = [r15]
	adds	r8 = -8 * 16, SP
	adds	r9 = -7 * 16, SP
	}
	;;
	{ .mmi
	stf.spill  [r8] = f16, 32
	stf.spill  [r9] = f17, 32
	adds	SP = -8 * 16, SP
	}
	;;
	{ .mmf
	stf.spill  [r8] = f18, 32
	stf.spill  [r9] = f19, 32
	mov	ALPHA = f8
	}
	;;
	{ .mmi
	stf.spill  [r8] = f20, 32
	stf.spill  [r9] = f21, 32
	mov	IS = 0
	}
	;;
	{ .mmf
	stf.spill  [r8] = f22
	stf.spill  [r9] = f23
	xmpy.l f10 = f10, f11
	}
	.body
	;;
	;;
	{ .mmi
	ld8	BUFFER = [r16]
	cmp.ge	p7, p0 = r0, M
	cmp.ge	p6, p0 = r0, N
	}
	;;
	{ .mmi
	shladd	INCX = INCX, BASE_SHIFT, r0
	shladd	LDA  = LDA, BASE_SHIFT, r0
	shladd	INCY = INCY, BASE_SHIFT, r0
	}
	;;
	{ .mmi
	getf.sig LDAP = f10
	mov	r2 = P
	tbit.nz	p8, p0 = A,   BASE_SHIFT
	}
	{ .mmi
	nop	__LINE__
	nop	__LINE__
	tbit.nz	p9, p0 = LDA, BASE_SHIFT
	}
	;;
	{ .mbb
	sub	LDAP = r2, LDAP
	(p7) br.cond.dpnt .L999
	(p6) br.cond.dpnt .L999
	}
	.align 16
	;;

.LIs_loop:
	{ .mmi
	sub	MIN_M = M, IS
	(p8) LDFD f32 = [X],  INCX
	mov	pr.rot= 0
	}
	{ .mmi
	mov	AO1 = BUFFER
	adds	AO2 = 4 * SIZE, BUFFER
	}
	;;
	cmp.le	p6, p0 = r2, MIN_M
	;;
	(p6) mov MIN_M = P
	;;
	(p8) adds MIN_M = -1, MIN_M
	;;
	{ .mmi
	shladd	OFFSET = INCX, 2, INCX
	shladd	BO  = INCX, 2, X
	shr	I = MIN_M, 3
	}
	;;
	{ .mmi
	adds I = -1, I
	cmp.eq	p16, p0 = r0, r0
	mov	ar.ec= 5
	}
	;;
	{ .mmi
	(p8) STFD [AO1] = f32, 2 * SIZE
	(p8) adds	AO2 = 6 * SIZE, BUFFER
	mov	ar.lc = I
	}
	{ .mib
	cmp.gt	p6, p0 = 0, I
	tbit.nz	p13, p0 = MIN_M, 2
	(p6) br.cond.dpnt .L05
	}
	;;
	.align 16

.L01:
	(p20) STFD [AO1] = f36,  SIZE
	(p20) STFD [AO2] = f56,  SIZE
	(p16) LDFD f32 = [X],  INCX
	(p16) LDFD f52 = [BO], INCX
	;;
	(p20) STFD [AO1] = f41,  SIZE
	(p20) STFD [AO2] = f61,  SIZE
	(p16) LDFD f37 = [X],  INCX
	(p16) LDFD f57 = [BO], INCX
	;;
	(p20) STFD [AO1] = f46,  SIZE
	(p20) STFD [AO2] = f66,  SIZE
	(p16) LDFD f42 = [X],  INCX
	(p16) LDFD f62 = [BO], INCX
	;;
	(p20) STFD [AO1] = f51,  5 * SIZE
	(p20) STFD [AO2] = f71,  5 * SIZE
	(p16) LDFD f47 = [X],  OFFSET
	(p16) LDFD f67 = [BO], OFFSET
	br.ctop.sptk.few .L01
	;;
	.align 16

.L05:
	(p13) LDFD f32 = [X],  INCX
	tbit.nz	p14, p0 = MIN_M, 1
	;;
	(p13) LDFD f33 = [X],  INCX
	tbit.nz	p15, p0 = MIN_M, 0
	;;
	(p13) LDFD f34 = [X],  INCX
	;;
	(p13) LDFD f35 = [X],  INCX
	;;
	(p14) LDFD f36 = [X],  INCX
	;;
	(p13) STFD [AO1] = f32, SIZE
	(p14) LDFD f37 = [X],  INCX
	;;
	(p13) STFD [AO1] = f33, SIZE
	(p15) LDFD f38 = [X],  INCX
	;;
	(p13) STFD [AO1] = f34, SIZE
	;;
	(p13) STFD [AO1] = f35, SIZE
	;;
	(p14) STFD [AO1] = f36, SIZE
	;;
	(p14) STFD [AO1] = f37, SIZE
	;;
	(p15) STFD [AO1] = f38, SIZE
	(p9) br.cond.dpnt .L100
	;;
	.align 16

.L10:
	{ .mmi
	mov	CO  = Y
	nop	__LINE__
	shr	J   = N, 3
	}
	;;
	{ .mib
	nop	__LINE__
	cmp.eq	p6, p0 = r0, J
	(p6) br.cond.dpnt .L20
	}
	;;
	.align 16

.L11:
	{ .mfi
	mov	AO1 = A
	mov	f8  = f0
	mov	pr.rot= 0
	}
	{ .mfi
	add	AO2 = LDA, A
	mov	f10 = f0
	shr	I = MIN_M, 4
	}
	;;
	{ .mmf
	shladd	AO3 = LDA, 1, A
	shladd	AO4 = LDA, 1, AO2
	mov	f12 = f0
	}
	{ .mmf
	(p8) LDFD f32 = [AO1], SIZE
	(p8) LDFD f33 = [AO2], SIZE
	mov	f14 = f0
	}
	;;
	{ .mmf
	shladd	AO5 = LDA, 1, AO3
	shladd	AO6 = LDA, 1, AO4
	mov	f16 = f0
	}
	{ .mmf
	(p8) LDFD f34 = [AO3], SIZE
	(p8) LDFD f35 = [AO4], SIZE
	mov	f18 = f0
	}
	;;
	{ .mmf
	shladd	AO7 = LDA, 1, AO5
	shladd	AO8 = LDA, 1, AO6
	mov	f20 = f0
	}
	{ .mmf
	(p8) LDFD f36 = [AO5], SIZE
	(p8) LDFD f37 = [AO6], SIZE
	mov	f22 = f0
	}
	;;
	{ .mfi
	(p8) LDFD f38 = [AO7], SIZE
	mov	f9  = f0
	mov	ar.ec= 2
	}
	{ .mmf
	(p8) LDFD f39 = [AO8], SIZE
	mov	BO  = BUFFER
	mov	f11 = f0
	}
	;;
	{ .mmf
	(p8) LDFD f40 = [BO], 2 * SIZE
	cmp.eq	p6, p0 = 0, I
	mov	f13 = f0
	}
	{ .mmf
	shladd	A   = LDA, 3, A
	cmp.eq	p16, p0 = r0, r0
	mov	f15 = f0
	}
	;;
	{ .mmf
	add	I = I, I
	nop	__LINE__
	mov	f17 = f0
	}
	{ .mmf
	adds	RPRE1  = RPREFETCH * SIZE, AO1
	adds	RPRE2  = (RPREFETCH + 8) * SIZE, AO2
	mov	f19 = f0
	}
	;;
	{ .mmf
	adds	I = -1, I
	nop	__LINE__
	mov	f21 = f0
	}
	{ .mmf
	adds	RPRE3  = RPREFETCH * SIZE, AO3
	adds	RPRE4  = (RPREFETCH + 8) * SIZE, AO4
	mov	f23 = f0
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p8) FMPY	f8  = f40, f32
	}
	{ .mmf
	adds	RPRE5  = RPREFETCH * SIZE, AO5
	adds	RPRE6  = (RPREFETCH + 8) * SIZE, AO6
	(p8) FMPY	f10 = f40, f33
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p8) FMPY	f12 = f40, f34
	}
	{ .mmf
	adds	RPRE7  = RPREFETCH * SIZE, AO7
	adds	RPRE8  = (RPREFETCH + 8) * SIZE, AO8
	(p8) FMPY	f14 = f40, f35
	}
	;;
	{ .mfi
	nop	__LINE__
	(p8) FMPY	f16 = f40, f36
	mov	ar.lc = I
	}
	{ .mmf
	adds	WPRE = 8 * SIZE, CO
	adds	PREB  = RPREFETCH * SIZE, BO
	(p8) FMPY	f18 = f40, f37
	}
	;;
	{ .mmf
	lfetch.excl.nt1	[WPRE]
	nop	__LINE__
	(p8) FMPY	f20 = f40, f38
	}
	{ .mfb
	nop	__LINE__
	(p8) FMPY	f22 = f40, f39
	(p6) br.cond.dpnt .L15
	}
	;;
	.align 16

.L12:
	{ .mfi
	(p17) LDFPD	f95, f96 = [AO8], 2 * SIZE
	(p17) FMA	f8  = f104, f33, f8
	(p16) tbit.nz.unc p14, p15 = I, 0
	}
	{ .mfi
	(p17) LDFPD	f110, f111 = [BO], 2 * SIZE
	(p17) FMA	f9  = f105, f34, f9
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f32, f33 = [AO1], 2 * SIZE
	(p17) FMA	f10 = f104, f35, f10
	nop	__LINE__
	}
	{ .mfi
	(p14) PREFETCH [RPRE1], 16 * SIZE
	(p17) FMA	f11 = f105, f36, f11
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f34, f35 = [AO2], 2 * SIZE
	(p17) FMA	f12 = f104, f37, f12
	nop	__LINE__
	}
	{ .mfi
	(p15) PREFETCH [RPRE2], 16 * SIZE
	(p17) FMA	f13 = f105, f38, f13
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f36, f37 = [AO3], 2 * SIZE
	(p17) FMA	f14 = f104, f39, f14
	nop	__LINE__
	}
	{ .mfi
	(p14) PREFETCH [RPRE3], 16 * SIZE
	(p17) FMA	f15 = f105, f40, f15
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f38, f39 = [AO4], 2 * SIZE
	(p17) FMA	f16 = f104, f41, f16
	nop	__LINE__
	}
	{ .mfi
	(p15) PREFETCH [RPRE4], 16 * SIZE
	(p17) FMA	f17 = f105, f42, f17
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f40, f41 = [AO5], 2 * SIZE
	(p17) FMA	f18 = f104, f43, f18
	nop	__LINE__
	}
	{ .mfi
	(p14) PREFETCH [RPRE5], 16 * SIZE
	(p17) FMA	f19 = f105, f44, f19
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f42, f43 = [AO6], 2 * SIZE
	(p17) FMA	f20 = f104, f45, f20
	nop	__LINE__
	}
	{ .mfi
	(p15) PREFETCH [RPRE6], 16 * SIZE
	(p17) FMA	f21 = f105, f46, f21
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f44, f45 = [AO7], 2 * SIZE
	(p17) FMA	f22 = f104, f47, f22
	nop	__LINE__
	}
	{ .mfi
	(p14) PREFETCH [RPRE7], 16 * SIZE
	(p17) FMA	f23 = f105, f48, f23
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f46, f47 = [AO8], 2 * SIZE
	(p17) FMA	f8  = f106, f49, f8
	nop	__LINE__
	}
	{ .mfi
	(p15) PREFETCH [RPRE8], 16 * SIZE
	(p17) FMA	f9  = f107, f50, f9
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f48, f49 = [AO1], 2 * SIZE
	(p17) FMA	f10 = f106, f51, f10
	nop	__LINE__
	}
	{ .mfi
	(p14) PREFETCH [PREB], 16 * SIZE
	(p17) FMA	f11 = f107, f52, f11
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f50, f51 = [AO2], 2 * SIZE
	(p17) FMA	f12 = f106, f53, f12
	nop	__LINE__
	}
	{ .mfi
	(p16) LDFPD	f103, f104 = [BO], 2 * SIZE
	(p17) FMA	f13 = f107, f54, f13
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f52, f53 = [AO3], 2 * SIZE
	(p17) FMA	f14 = f106, f55, f14
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p17) FMA	f15 = f107, f56, f15
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f54, f55 = [AO4], 2 * SIZE
	(p17) FMA	f16 = f106, f57, f16
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p17) FMA	f17 = f107, f58, f17
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f56, f57 = [AO5], 2 * SIZE
	(p17) FMA	f18 = f106, f59, f18
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p17) FMA	f19 = f107, f60, f19
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f58, f59 = [AO6], 2 * SIZE
	(p17) FMA	f20 = f106, f61, f20
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p17) FMA	f21 = f107, f62, f21
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f60, f61 = [AO7], 2 * SIZE
	(p17) FMA	f22 = f106, f63, f22
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p17) FMA	f23 = f107, f64, f23
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f62, f63 = [AO8], 2 * SIZE
	(p17) FMA	f8  = f108, f65, f8
	nop	__LINE__
	}
	{ .mfi
	(p16) LDFPD	f105, f106 = [BO], 2 * SIZE
	(p17) FMA	f9  = f109, f66, f9
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f64, f65 = [AO1], 2 * SIZE
	(p17) FMA	f10 = f108, f67, f10
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p17) FMA	f11 = f109, f68, f11
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f66, f67 = [AO2], 2 * SIZE
	(p17) FMA	f12 = f108, f69, f12
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p17) FMA	f13 = f109, f70, f13
 	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f68, f69 = [AO3], 2 * SIZE
	(p17) FMA	f14 = f108, f71, f14
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p17) FMA	f15 = f109, f72, f15
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f70, f71 = [AO4], 2 * SIZE
	(p17) FMA	f16 = f108, f73, f16
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p17) FMA	f17 = f109, f74, f17
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f72, f73 = [AO5], 2 * SIZE
	(p17) FMA	f18 = f108, f75, f18
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p17) FMA	f19 = f109, f76, f19
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f74, f75 = [AO6], 2 * SIZE
	(p17) FMA	f20 = f108, f77, f20
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p17) FMA	f21 = f109, f78, f21
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f76, f77 = [AO7], 2 * SIZE
	(p17) FMA	f22 = f108, f79, f22
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p17) FMA	f23 = f109, f80, f23
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f107, f108 = [BO], 2 * SIZE
	(p17) FMA	f8  = f110, f81, f8
	nop	__LINE__
	}
	{ .mfi
	(p16) LDFPD	f78, f79 = [AO8], 2 * SIZE
	(p17) FMA	f9  = f111, f82, f9
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f80, f81 = [AO1], 2 * SIZE
	(p17) FMA	f10 = f110, f83, f10
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p17) FMA	f11 = f111, f84, f11
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f82, f83 = [AO2], 2 * SIZE
	(p17) FMA	f12 = f110, f85, f12
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p17) FMA	f13 = f111, f86, f13
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f84, f85 = [AO3], 2 * SIZE
	(p17) FMA	f14 = f110, f87, f14
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p17) FMA	f15 = f111, f88, f15
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f86, f87 = [AO4], 2 * SIZE
	(p17) FMA	f16 = f110, f89, f16
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p17) FMA	f17 = f111, f90, f17
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f88, f89 = [AO5], 2 * SIZE
	(p17) FMA	f18 = f110, f91, f18
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p17) FMA	f19 = f111, f92, f19
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f90, f91 = [AO6], 2 * SIZE
	(p17) FMA	f20 = f110, f93, f20
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p17) FMA	f21 = f111, f94, f21
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f92, f93 = [AO7], 2 * SIZE
	(p17) FMA	f22 = f110, f95, f22
	nop	__LINE__
	}
	{ .mfb
	adds	I = -1, I
	(p17) FMA	f23 = f111, f96, f23
	br.ctop.sptk.few .L12
	}
	;;
	.align 16

.L15:
	and	I = 15, MIN_M
	mov	pr.rot= 0
	;;
	cmp.eq	p6,  p0 = 0, I
	cmp.eq	p16, p15 = r0, r0
	;;
	adds	I = 1, I
	;;
	shr	I = I, 1
	;;
	adds	I = -1, I
	;;
	mov	ar.lc = I
	mov	ar.ec= 3
	and	I = 15, MIN_M
	(p6) br.cond.dpnt .L18
	;;
	.align 16

.L16:
	{ .mfi
	(p16) LDFPD	f104, f107 = [BO], 2 * SIZE
	(p18) FMA	f8  = f106, f34, f8
	nop	__LINE__
	}
	{ .mfi
	(p16) LDFPD	f32,  f35  = [AO1], 2 * SIZE
	(p15) FMA	f9  = f109, f37, f9
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f38,  f41  = [AO2], 2 * SIZE
	(p18) FMA	f10 = f106, f40, f10
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p15) FMA	f11 = f109, f43, f11
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f44,  f47  = [AO3], 2 * SIZE
	(p18) FMA	f12 = f106, f46, f12
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p15) FMA	f13 = f109, f49, f13
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f50,  f53  = [AO4], 2 * SIZE
	(p18) FMA	f14 = f106, f52, f14
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p15) FMA	f15 = f109, f55, f15
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f56,  f59  = [AO5], 2 * SIZE
	(p18) FMA	f16 = f106, f58, f16
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p15) FMA	f17 = f109, f61, f17
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f62,  f65  = [AO6], 2 * SIZE
	(p18) FMA	f18 = f106, f64, f18
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p15) FMA	f19 = f109, f67, f19
	(p17) adds	I = -2, I
	}
	;;
	{ .mfi
	(p16) LDFPD	f68,  f71  = [AO7], 2 * SIZE
	(p18) FMA	f20 = f106, f70, f20
	nop	__LINE__
	}
	{ .mfi
	nop	__LINE__
	(p15) FMA	f21 = f109, f73, f21
	nop	__LINE__
	}
	;;
	{ .mfi
	(p16) LDFPD	f74,  f77  = [AO8], 2 * SIZE
	(p15) FMA	f23 = f109, f79, f23
	(p17) cmp.ne.unc p15, p0 = -1, I
	}
	{ .mfb
	nop	__LINE__
	(p18) FMA	f22 = f106, f76, f22
	br.ctop.sptk.few .L16
	}
	;;

.L18:
	{ .mmf
	mov	AO1 = CO
	LDFD	f32 = [CO], INCY
	FADD	f8  = f8,  f9
	}
	;;
	{ .mmf
	LDFD	f33 = [CO], INCY
	nop	__LINE__
	FADD	f10 = f10, f11
	}
	;;
	{ .mmf
	LDFD	f34 = [CO], INCY
	nop	__LINE__
	FADD	f12 = f12, f13
	}
	;;
	{ .mmf
	LDFD	f35 = [CO], INCY
	nop	__LINE__
	FADD	f14 = f14, f15
	}
	;;
	{ .mmf
	LDFD	f36 = [CO], INCY
	nop	__LINE__
	FADD	f16 = f16, f17
	}
	;;
	{ .mmf
	LDFD	f37 = [CO], INCY
	nop	__LINE__
	FADD	f18 = f18, f19
	}
	;;
	{ .mmf
	LDFD	f38 = [CO], INCY
	nop	__LINE__
	FADD	f20 = f20, f21
	}
	;;
	{ .mmf
	LDFD	f39 = [CO], INCY
	nop	__LINE__
	FADD	f22 = f22, f23
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f32 = ALPHA, f8,  f32
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f33 = ALPHA, f10, f33
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f34 = ALPHA, f12, f34
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f35 = ALPHA, f14, f35
	}
	;;
	{ .mmf
	STFD [AO1] = f32
	add	AO1 = AO1, INCY
	FMA	f36 = ALPHA, f16, f36
	}
	;;
	{ .mmf
	STFD [AO1] = f33
	add	AO1 = AO1, INCY
	FMA	f37 = ALPHA, f18, f37
	}
	;;
	{ .mmf
	STFD [AO1] = f34
	add	AO1 = AO1, INCY
	FMA	f38 = ALPHA, f20, f38
	}
	;;
	{ .mmf
	STFD [AO1] = f35
	add	AO1 = AO1, INCY
	FMA	f39 = ALPHA, f22, f39
	}
	;;
	{ .mmi
	STFD [AO1] = f36
	add	AO1 = AO1, INCY
	adds J = -1, J
	}
	;;
	{ .mmi
	STFD [AO1] = f37
	add	AO1 = AO1, INCY
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD [AO1] = f38
	add	AO1 = AO1, INCY
	cmp4.lt p6, p0 = 0, J
	}
	;;
	{ .mib
	STFD [AO1] = f39
	add	AO1 = AO1, INCY
	(p6) br.cond.dptk .L11
	}
	;;
	.align 16

.L20:
	{ .mfi
	mov	AO1 = A
	mov	f8  = f0
	mov	pr.rot= 0
	}
	{ .mfi
	add	AO2 = LDA, A
	mov	f10 = f0
	tbit.z	p6, p0  = N, 2
	}
	;;
	{ .mfi
	shladd	AO3 = LDA, 1, A
	mov	f12 = f0
	shr	I = MIN_M, 4
	}
	{ .mfb
	shladd	AO4 = LDA, 1, AO2
	mov	f14 = f0
	(p6) br.cond.dpnt .L30
	}
	;;
	{ .mmf
	(p8) LDFD f32 = [AO1], SIZE
	(p8) LDFD f33 = [AO2], SIZE
	mov	f9  = f0
	}
	{ .mmf
	mov	BO  = BUFFER
	shladd	A   = LDA, 2, A
	mov	f11 = f0
	}
	;;
	{ .mmf
	(p8) LDFD f40 = [BO], 2 * SIZE
	cmp.eq	p6, p0 = 0, I
	mov	f13 = f0
	}
	{ .mmf
	(p8) LDFD f34 = [AO3], SIZE
	(p8) LDFD f35 = [AO4], SIZE
	mov	f15 = f0
	}
	;;
	{ .mmi
	adds	RPRE1  = RPREFETCH * SIZE, AO1
	adds	RPRE2  = (RPREFETCH + 8) * SIZE, AO2
	mov	ar.ec= 2
	}
	{ .mmi
	cmp.eq	p16, p0 = r0, r0
	add	I = I, I
	}
	;;
	{ .mmf
	adds	WPRE =  4 * SIZE, CO
	adds	PREB  = RPREFETCH * SIZE, BO
	(p8) FMPY	f8  = f40, f32
	}
	{ .mmf
	adds	RPRE3  = RPREFETCH * SIZE, AO3
	adds	I = -1, I
	(p8) FMPY	f10 = f40, f33
	}
	;;
	{ .mfi
	lfetch.excl.nt1	[WPRE]
	(p8) FMPY	f12 = f40, f34
	mov	ar.lc = I
	}
	{ .mfb
	adds	RPRE4  = (RPREFETCH + 8) * SIZE, AO4
	(p8) FMPY	f14 = f40, f35
	(p6) br.cond.dpnt .L25
	}
	;;
	.align 16

.L22:
	{ .mmf
	(p17) LDFPD	f87, f88 = [AO4], 2 * SIZE
	(p17) LDFPD	f110, f111 = [BO], 2 * SIZE
	(p17) FMA	f8  = f104, f33, f8
	}
	{ .mfi
	nop	__LINE__
	(p17) FMA	f9  = f105, f34, f9
	(p16) tbit.nz.unc p14, p15 = I, 0
	}
	;;
	{ .mmf
	(p14) PREFETCH [RPRE1], 16 * SIZE
	(p16) LDFPD	f32, f33 = [AO1], 2 * SIZE
	(p17) FMA	f10 = f104, f35, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f11 = f105, f36, f11
	}
	;;
	{ .mmf
	(p15) PREFETCH [RPRE2], 16 * SIZE
	(p16) LDFPD	f34, f35 = [AO2], 2 * SIZE
	(p17) FMA	f12 = f104, f37, f12
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f13 = f105, f38, f13
	}
	;;
	{ .mmf
	(p14) PREFETCH [RPRE3], 16 * SIZE
	(p16) LDFPD	f36, f37 = [AO3], 2 * SIZE
	(p17) FMA	f14 = f104, f39, f14
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f15 = f105, f40, f15
	}
	;;
	{ .mmf
	(p15) PREFETCH [RPRE4], 16 * SIZE
	(p16) LDFPD	f38, f39 = [AO4], 2 * SIZE
	(p17) FMA	f8  = f106, f49, f8
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f9  = f107, f50, f9
	}
	;;
	{ .mmf
	(p14) PREFETCH [PREB], 16 * SIZE
	(p16) LDFPD	f48, f49 = [AO1], 2 * SIZE
	(p17) FMA	f10 = f106, f51, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f11 = f107, f52, f11
	}
	;;
	{ .mmf
	(p16) LDFPD	f50, f51 = [AO2], 2 * SIZE
	(p16) LDFPD	f103, f104 = [BO], 2 * SIZE
	(p17) FMA	f12 = f106, f53, f12
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f13 = f107, f54, f13
	}
	;;
	{ .mmf
	(p16) LDFPD	f52, f53 = [AO3], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f14 = f106, f55, f14
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f15 = f107, f56, f15
	}
	;;
	{ .mmf
	(p16) LDFPD	f54, f55 = [AO4], 2 * SIZE
	(p16) LDFPD	f105, f106 = [BO], 2 * SIZE
	(p17) FMA	f8  = f108, f65, f8
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f9  = f109, f66, f9
	}
	;;
	{ .mmf
	(p16) LDFPD	f64, f65 = [AO1], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f10 = f108, f67, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f11 = f109, f68, f11
	}
	;;
	{ .mmf
	(p16) LDFPD	f66, f67 = [AO2], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f12 = f108, f69, f12
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f13 = f109, f70, f13
	}
	;;
	{ .mmf
	(p16) LDFPD	f68, f69 = [AO3], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f14 = f108, f71, f14
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f15 = f109, f72, f15
	}
	;;
	{ .mmf
	(p16) LDFPD	f70, f71 = [AO4], 2 * SIZE
	(p16) LDFPD	f107, f108 = [BO], 2 * SIZE
	(p17) FMA	f8  = f110, f81, f8
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f9  = f111, f82, f9
	}
	;;
	{ .mmf
	(p16) LDFPD	f80, f81 = [AO1], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f10 = f110, f83, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f11 = f111, f84, f11
	}
	;;
	{ .mmf
	(p16) LDFPD	f82, f83 = [AO2], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f12 = f110, f85, f12
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f13 = f111, f86, f13
	}
	;;
	{ .mmf
	(p16) LDFPD	f84, f85 = [AO3], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f14 = f110, f87, f14
	}
	{ .mfb
	adds	I = -1, I
	(p17) FMA	f15 = f111, f88, f15
	br.ctop.sptk.few .L22
	}
	;;
	.align 16

.L25:
	and	I = 15, MIN_M
	mov	pr.rot= 0
	;;
	cmp.eq	p6,  p0 = 0, I
	cmp.eq	p16, p15 = r0, r0
	;;
	adds	I = 1, I
	;;
	shr	I = I, 1
	;;
	adds	I = -1, I
	;;
	mov	ar.lc = I
	mov	ar.ec= 3
	and	I = 15, MIN_M
	(p6) br.cond.dpnt .L28
	;;
	.align 16

.L26:
	{ .mmf
	(p16) LDFPD	f104, f107 = [BO], 2 * SIZE
	(p16) LDFPD	f32,  f35  = [AO1], 2 * SIZE
	(p18) FMA	f8  = f106, f34, f8
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p15) FMA	f9  = f109, f37, f9
	}
	;;
	{ .mmf
	(p16) LDFPD	f38,  f41  = [AO2], 2 * SIZE
	nop	__LINE__
	(p18) FMA	f10 = f106, f40, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p15) FMA	f11 = f109, f43, f11
	}
	;;
	{ .mmf
	(p16) LDFPD	f44,  f47  = [AO3], 2 * SIZE
	nop	__LINE__
	(p18) FMA	f12 = f106, f46, f12
	}
	{ .mmf
	nop	__LINE__
	(p17) adds	I = -2, I
	(p15) FMA	f13 = f109, f49, f13
	}
	;;
	{ .mmf
	(p16) LDFPD	f50,  f53  = [AO4], 2 * SIZE
	nop	__LINE__
	(p15) FMA	f15 = f109, f55, f15
	}
	{ .mfb
	(p17) cmp.ne.unc p15, p0 = -1, I
	(p18) FMA	f14 = f106, f52, f14
	br.ctop.sptk.few .L26
	}
	;;

.L28:
	{ .mmf
	mov	AO1 = CO
	LDFD	f32 = [CO], INCY
	FADD	f8  = f8,  f9
	}
	;;
	{ .mmf
	LDFD	f33 = [CO], INCY
	nop	__LINE__
	FADD	f10 = f10, f11
	}
	;;
	{ .mmf
	LDFD	f34 = [CO], INCY
	nop	__LINE__
	FADD	f12 = f12, f13
	}
	;;
	{ .mmf
	LDFD	f35 = [CO], INCY
	nop	__LINE__
	FADD	f14 = f14, f15
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f32 = ALPHA, f8,  f32
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f33 = ALPHA, f10, f33
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f34 = ALPHA, f12, f34
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f35 = ALPHA, f14, f35
	}
	;;
	{ .mmf
	STFD [AO1] = f32
	add	AO1 = AO1, INCY
	}
	;;
	{ .mmf
	STFD [AO1] = f33
	add	AO1 = AO1, INCY
	}
	;;
	{ .mmf
	STFD [AO1] = f34
	add	AO1 = AO1, INCY
	}
	;;
	{ .mmf
	STFD [AO1] = f35
	add	AO1 = AO1, INCY
	}
	;;
	.align 16

.L30:
	{ .mfi
	mov	AO1 = A
	mov	f8  = f0
	mov	pr.rot= 0
	}
	{ .mfi
	add	AO2 = LDA, A
	mov	f10 = f0
	tbit.z	p6, p0  = N, 1
	}
	;;
	{ .mfi
	mov	BO  = BUFFER
	mov	f12 = f0
	shr	I = MIN_M, 4
	}
	{ .mfb
	adds	WPRE =  4 * SIZE, CO
	mov	f14 = f0
	(p6) br.cond.dpnt .L40
	}
	;;
	{ .mmf
	(p8) LDFD f32 = [AO1], SIZE
	(p8) LDFD f33 = [AO2], SIZE
	mov	f9  = f0
	}
	{ .mfi
	shladd	A   = LDA, 1, A
	mov	f11 = f0
	mov	ar.ec= 2
	}
	;;
	{ .mmf
	(p8) LDFD f40 = [BO], 2 * SIZE
	cmp.eq	p6, p0 = 0, I
	mov	f13 = f0
	}
	{ .mmf
	adds	RPRE1  = RPREFETCH * SIZE, AO1
	add	I = I, I
	mov	f15 = f0
	}
	;;
	{ .mmi
	cmp.eq	p16, p0 = r0, r0
	adds	RPRE2  = (RPREFETCH + 8) * SIZE, AO2
	adds	I = -1, I
	}
	;;
	{ .mfi
	lfetch.excl.nt1	[WPRE]
	(p8) FMPY	f8  = f40, f32
	mov	ar.lc = I
	}
	{ .mfb
	adds	PREB  = RPREFETCH * SIZE, BO
	(p8) FMPY	f10 = f40, f33
	(p6) br.cond.dpnt .L35
	}
	;;
	.align 16

.L32:
	{ .mmf
	(p17) LDFPD	f83, f84 = [AO2], 2 * SIZE
	(p17) LDFPD	f110, f111 = [BO], 2 * SIZE
	(p17) FMA	f8  = f104, f33, f8
	}
	{ .mfi
	nop	__LINE__
	(p17) FMA	f9  = f105, f34, f9
	(p16) tbit.nz.unc p14, p15 = I, 0
	}
	;;
	{ .mmf
	(p14) PREFETCH [RPRE1], 16 * SIZE
	(p16) LDFPD	f32, f33 = [AO1], 2 * SIZE
	(p17) FMA	f10 = f104, f35, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f11 = f105, f36, f11
	}
	;;
	{ .mmf
	(p15) PREFETCH [RPRE2], 16 * SIZE
	(p16) LDFPD	f34, f35 = [AO2], 2 * SIZE
	(p17) FMA	f8  = f106, f49, f8
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f9  = f107, f50, f9
	}
	;;
	{ .mmf
	(p14) PREFETCH [PREB], 16 * SIZE
	(p16) LDFPD	f48, f49 = [AO1], 2 * SIZE
	(p17) FMA	f10 = f106, f51, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f11 = f107, f52, f11
	}
	;;
	{ .mmf
	(p16) LDFPD	f50, f51 = [AO2], 2 * SIZE
	(p16) LDFPD	f103, f104 = [BO], 2 * SIZE
	(p17) FMA	f8  = f108, f65, f8
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f9  = f109, f66, f9
	}
	;;
	{ .mmf
	(p16) LDFPD	f105, f106 = [BO], 2 * SIZE
	(p16) LDFPD	f64, f65 = [AO1], 2 * SIZE
	(p17) FMA	f10 = f108, f67, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f11 = f109, f68, f11
	}
	;;
	{ .mmf
	(p16) LDFPD	f66, f67 = [AO2], 2 * SIZE
	(p16) LDFPD	f107, f108 = [BO], 2 * SIZE
	(p17) FMA	f8  = f110, f81, f8
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f9  = f111, f82, f9
	}
	;;
	{ .mmf
	(p16) LDFPD	f80, f81 = [AO1], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f10 = f110, f83, f10
	}
	{ .mfb
	adds	I = -1, I
	(p17) FMA	f11 = f111, f84, f11
	br.ctop.sptk.few .L32
	}
	;;
	.align 16

.L35:
	and	I = 15, MIN_M
	;;
	cmp.eq	p6,  p0 = 0, I
	(p6) br.cond.dpnt .L38
	;;
	tbit.nz	p12, p0 = MIN_M, 3
	tbit.nz	p13, p0 = MIN_M, 2
	tbit.nz	p14, p0 = MIN_M, 1
	tbit.nz	p15, p0 = MIN_M, 0
	;;
	(p12) LDFPD	f32,  f33  = [AO1], 2 * SIZE
	(p12) LDFPD	f34,  f35  = [AO2], 2 * SIZE
	(p12) LDFPD	f100, f101 = [BO], 2 * SIZE
	;;
	(p12) LDFPD	f36,  f37  = [AO1], 2 * SIZE
	(p12) LDFPD	f38,  f39  = [AO2], 2 * SIZE
	(p12) LDFPD	f102, f103 = [BO], 2 * SIZE
	;;
	(p12) LDFPD	f40,  f41  = [AO1], 2 * SIZE
	(p12) LDFPD	f42,  f43  = [AO2], 2 * SIZE
	(p12) LDFPD	f104, f105 = [BO], 2 * SIZE
	;;
	(p12) LDFPD	f44,  f45  = [AO1], 2 * SIZE
	(p12) LDFPD	f46,  f47  = [AO2], 2 * SIZE
	(p12) LDFPD	f106, f107 = [BO], 2 * SIZE
	;;
	(p13) LDFPD	f48,  f49  = [AO1], 2 * SIZE
	(p13) LDFPD	f50,  f51  = [AO2], 2 * SIZE
	(p13) LDFPD	f108, f109 = [BO], 2 * SIZE
	;;
	(p13) LDFPD	f52,  f53  = [AO1], 2 * SIZE
	(p13) LDFPD	f54,  f55  = [AO2], 2 * SIZE
	(p13) LDFPD	f110, f111 = [BO], 2 * SIZE
	;;
	(p14) LDFPD	f56,  f57  = [AO1], 2 * SIZE
	(p14) LDFPD	f58,  f59  = [AO2], 2 * SIZE
	(p14) LDFPD	f112, f113 = [BO], 2 * SIZE
	;;
	(p15) LDFD	f60        = [AO1]
	(p15) LDFD	f61        = [AO2]
	(p15) LDFD	f114       = [BO]
	;;
	(p12) FMA	f8  = f100, f32, f8
	(p12) FMA	f9  = f101, f33, f9
	(p12) FMA	f10 = f100, f34, f10
	(p12) FMA	f11 = f101, f35, f11
	;;
	(p12) FMA	f12 = f102, f36, f12
	(p12) FMA	f13 = f103, f37, f13
	(p12) FMA	f14 = f102, f38, f14
	(p12) FMA	f15 = f103, f39, f15
	;;
	(p12) FMA	f8  = f104, f40, f8
	(p12) FMA	f9  = f105, f41, f9
	(p12) FMA	f10 = f104, f42, f10
	(p12) FMA	f11 = f105, f43, f11
	;;
	(p12) FMA	f12 = f106, f44, f12
	(p12) FMA	f13 = f107, f45, f13
	(p12) FMA	f14 = f106, f46, f14
	(p12) FMA	f15 = f107, f47, f15
	;;
	(p13) FMA	f8  = f108, f48, f8
	(p13) FMA	f9  = f109, f49, f9
	(p13) FMA	f10 = f108, f50, f10
	(p13) FMA	f11 = f109, f51, f11
	;;
	(p13) FMA	f12 = f110, f52, f12
	(p13) FMA	f13 = f111, f53, f13
	(p13) FMA	f14 = f110, f54, f14
	(p13) FMA	f15 = f111, f55, f15
	;;
	(p14) FMA	f8  = f112, f56, f8
	(p14) FMA	f9  = f113, f57, f9
	(p14) FMA	f10 = f112, f58, f10
	(p14) FMA	f11 = f113, f59, f11
	;;
	(p15) FMA	f12 = f114, f60, f12
	(p15) FMA	f14 = f114, f61, f14
	;;
.L38:
	FADD	f8  = f8,  f9
	FADD	f10 = f10, f11
	FADD	f12 = f12, f13
	FADD	f14 = f14, f15
	;;
	FADD	f8  = f8,  f12
	FADD	f10 = f10, f14
	;;
	{ .mmf
	mov	AO1 = CO
	LDFD	f32 = [CO], INCY
	}
	;;
	{ .mmf
	LDFD	f33 = [CO], INCY
	nop	__LINE__
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f32 = ALPHA, f8,  f32
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f33 = ALPHA, f10, f33
	}
	;;
	{ .mmf
	STFD [AO1] = f32
	add	AO1 = AO1, INCY
	}
	;;
	{ .mmf
	STFD [AO1] = f33
	}
	;;
	.align 16

.L40:
	{ .mfi
	mov	AO1 = A
	mov	f8  = f0
	shr	I = MIN_M, 4
	}
	{ .mfi
	mov	BO  = BUFFER
	mov	f10 = f0
	tbit.z	p7, p0  = N, 0
	}
	;;
	{ .mfi
	cmp.eq	p6, p0 = 0, I
	mov	f12 = f0
	mov	pr.rot= 0
	}
	{ .mfb
	add	I = I, I
	mov	f14 = f0
	(p7) br.cond.dpnt .L99
	}
	;;
	{ .mfi
	(p8) LDFD f32 = [AO1], SIZE
	mov	f9  = f0
	mov	ar.ec= 2
	}
	{ .mmf
	(p8) LDFD f40 = [BO], 2 * SIZE
	add	A   = A, LDA
	mov	f11 = f0
	}
	;;
	{ .mmf
	adds	WPRE =  1 * SIZE, CO
	adds	PREB  = RPREFETCH * SIZE, BO
	mov	f13 = f0
	}
	{ .mmf
	cmp.eq	p16, p0 = r0, r0
	adds	I = -1, I
	mov	f15 = f0
	}
	;;
	{ .mfi
	lfetch.excl.nt1	[WPRE]
	(p8) FMPY	f8  = f40, f32
	mov	ar.lc = I
	}
	{ .mmb
	nop	__LINE__
	nop	__LINE__
	(p6) br.cond.dpnt .L45
	}
	;;
	.align 16

.L42:
	{ .mmf
	(p17) LDFPD	f81, f82   = [AO1], 2 * SIZE
	(p17) LDFPD	f110, f111 = [BO], 2 * SIZE
	(p17) FMA	f8  = f104, f33, f8
	}
	{ .mfi
	nop	__LINE__
	(p17) FMA	f9  = f105, f34, f9
	(p16) tbit.nz.unc p14, p15 = I, 0
	}
	;;
	{ .mmf
	(p16) LDFPD	f32, f33   = [AO1], 2 * SIZE
	(p16) LDFPD	f103, f104 = [BO], 2 * SIZE
	(p17) FMA	f8  = f106, f49, f8
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f9  = f107, f50, f9
	}
	;;
	{ .mmf
	(p16) LDFPD	f105, f106 = [BO], 2 * SIZE
	(p16) LDFPD	f48, f49 = [AO1], 2 * SIZE
	(p17) FMA	f8  = f108, f65, f8
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f9  = f109, f66, f9
	}
	;;
	{ .mmf
	(p16) LDFPD	f64, f65  = [AO1], 2 * SIZE
	(p16) LDFPD	f107, f108 = [BO], 2 * SIZE
	(p17) FMA	f8  = f110, f81, f8
	}
	{ .mfb
	adds	I = -1, I
	(p17) FMA	f9  = f111, f82, f9
	br.ctop.sptk.few .L42
	}
	;;
	.align 16

.L45:
	and	I = 15, MIN_M
	;;
	cmp.eq	p6,  p0 = 0, I
	(p6) br.cond.dpnt .L48
	;;
	tbit.nz	p12, p0 = MIN_M, 3
	tbit.nz	p13, p0 = MIN_M, 2
	tbit.nz	p14, p0 = MIN_M, 1
	tbit.nz	p15, p0 = MIN_M, 0
	;;
	(p12) LDFPD	f32,  f33  = [AO1], 2 * SIZE
	(p12) LDFPD	f100, f101 = [BO], 2 * SIZE
	;;
	(p12) LDFPD	f36,  f37  = [AO1], 2 * SIZE
	(p12) LDFPD	f102, f103 = [BO], 2 * SIZE
	;;
	(p12) LDFPD	f40,  f41  = [AO1], 2 * SIZE
	(p12) LDFPD	f104, f105 = [BO], 2 * SIZE
	;;
	(p12) LDFPD	f44,  f45  = [AO1], 2 * SIZE
	(p12) LDFPD	f106, f107 = [BO], 2 * SIZE
	;;
	(p13) LDFPD	f48,  f49  = [AO1], 2 * SIZE
	(p13) LDFPD	f108, f109 = [BO], 2 * SIZE
	;;
	(p13) LDFPD	f52,  f53  = [AO1], 2 * SIZE
	(p13) LDFPD	f110, f111 = [BO], 2 * SIZE
	;;
	(p14) LDFPD	f56,  f57  = [AO1], 2 * SIZE
	(p14) LDFPD	f112, f113 = [BO], 2 * SIZE
	;;
	(p15) LDFD	f60        = [AO1]
	(p15) LDFD	f114       = [BO]
	;;
	(p12) FMA	f8  = f100, f32, f8
	(p12) FMA	f9  = f101, f33, f9
	(p12) FMA	f10 = f102, f36, f10
	(p12) FMA	f11 = f103, f37, f11
	(p12) FMA	f12 = f104, f40, f12
	(p12) FMA	f13 = f105, f41, f13
	(p12) FMA	f14 = f106, f44, f14
	(p12) FMA	f15 = f107, f45, f15
	;;
	(p13) FMA	f8  = f108, f48, f8
	(p13) FMA	f9  = f109, f49, f9
	(p13) FMA	f10 = f110, f52, f10
	(p13) FMA	f11 = f111, f53, f11
	(p14) FMA	f12 = f112, f56, f12
	(p14) FMA	f13 = f113, f57, f13
	(p15) FMA	f14 = f114, f60, f14
	;;
.L48:
	{ .mmf
	LDFD	f32 = [CO]
	nop	__LINE__
	FADD	f8  = f8,  f9
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FADD	f10 = f10, f11
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FADD	f12 = f12, f13
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FADD	f14 = f14, f15
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FADD	f8  = f8,  f12
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FADD	f10 = f10, f14
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FADD	f8  = f8,  f10
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f32 = ALPHA, f8,  f32
	}
	;;
	{ .mmf
	STFD [CO] = f32
	}
	;;
	.align 16

.L99:
	adds	IS = P, IS
	shladd	A  = LDAP, BASE_SHIFT, A
	;;
	cmp.gt	p6, p0 = M, IS
	(p6) br.cond.dptk .LIs_loop
	br   .L999
	.align 4
	;;

.L100:
	shr	J   = N, 3
	mov	CO  = Y
	;;
	cmp.eq	p6, p0 = r0, J
	(p6) br.cond.dpnt .L120
	;;
	.align 16

.L111:
	{ .mfi
	mov	AO1 = A
	mov	f8  = f0
	mov	pr.rot= 0
	}
	{ .mfi
	add	AO2 = LDA, A
	mov	f10 = f0
	shr	I = MIN_M, 4
	}
	;;
	{ .mmf
	shladd	AO3 = LDA, 1, A
	shladd	AO4 = LDA, 1, AO2
	mov	f12 = f0
	}
	{ .mmf
	(p8) LDFD f32 = [AO1], SIZE
	(p8) LDFD f33 = [AO2], SIZE
	mov	f14 = f0
	}
	;;
	{ .mmf
	shladd	AO5 = LDA, 1, AO3
	shladd	AO6 = LDA, 1, AO4
	mov	f16 = f0
	}
	{ .mmf
	(p8) LDFD f34 = [AO3], SIZE
	(p8) LDFD f35 = [AO4], SIZE
	mov	f18 = f0
	}
	;;
	{ .mmf
	shladd	AO7 = LDA, 1, AO5
	shladd	AO8 = LDA, 1, AO6
	mov	f20 = f0
	}
	{ .mmf
	(p8) LDFD f36 = [AO5], SIZE
	(p8) LDFD f37 = [AO6], SIZE
	mov	f22 = f0
	}
	;;
	{ .mfi
	(p8) LDFD f38 = [AO7], SIZE
	mov	f9  = f0
	mov	ar.ec= 2
	}
	{ .mmf
	(p8) LDFD f39 = [AO8], SIZE
	mov	BO  = BUFFER
	mov	f11 = f0
	}
	;;
	{ .mmf
	(p8) LDFD f40 = [BO], 2 * SIZE
	cmp.eq	p6, p0 = 0, I
	mov	f13 = f0
	}
	{ .mmf
	shladd	A   = LDA, 3, A
	cmp.eq	p16, p0 = r0, r0
	mov	f15 = f0
	}
	;;
	{ .mmf
	add	I = I, I
	nop	__LINE__
	mov	f17 = f0
	}
	{ .mmf
	adds	RPRE1  = RPREFETCH * SIZE, AO1
	adds	RPRE2  = (RPREFETCH + 8) * SIZE, AO2
	mov	f19 = f0
	}
	;;
	{ .mmf
	adds	I = -1, I
	nop	__LINE__
	mov	f21 = f0
	}
	{ .mmf
	adds	RPRE3  = RPREFETCH * SIZE, AO3
	adds	RPRE4  = (RPREFETCH + 8) * SIZE, AO4
	mov	f23 = f0
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p8) FMPY	f8  = f40, f32
	}
	{ .mmf
	adds	RPRE5  = RPREFETCH * SIZE, AO5
	adds	RPRE6  = (RPREFETCH + 8) * SIZE, AO6
	(p8) FMPY	f10 = f40, f33
	}
	;;
	{ .mmf
	adds	AO21 = 7 * SIZE, AO2
	adds	AO41 = 7 * SIZE, AO4
	(p8) FMPY	f12 = f40, f34
	}
	{ .mmf
	adds	RPRE7  = RPREFETCH * SIZE, AO7
	adds	RPRE8  = (RPREFETCH + 8) * SIZE, AO8
	(p8) FMPY	f14 = f40, f35
	}
	;;
	{ .mfi
	nop	__LINE__
	(p8) FMPY	f16 = f40, f36
	mov	ar.lc = I
	}
	{ .mmf
	adds	WPRE = 8 * SIZE, CO
	adds	PREB  = RPREFETCH * SIZE, BO
	(p8) FMPY	f18 = f40, f37
	}
	;;
	{ .mmf
	lfetch.excl.nt1	[WPRE]
	adds	AO61 = 7 * SIZE, AO6
	(p8) FMPY	f20 = f40, f38
	}
	{ .mfb
	adds	AO81 = 7 * SIZE, AO8
	(p8) FMPY	f22 = f40, f39
	(p6) br.cond.dpnt .L115
	}
	;;
	.align 16

.L112:
	{ .mmf
	(p17) LDFPD	f80, f95 = [AO8]
	(p17) LDFPD	f110, f111 = [BO], 2 * SIZE
	(p17) FMA	f8  = f104, f33, f8
	}
	{ .mfi
	(p17) adds	AO8 = 3 * SIZE, AO8
	(p17) FMA	f9  = f105, f34, f9
	(p16) tbit.nz.unc p14, p15 = I, 0
	}
	;;
	{ .mmf
	(p14) PREFETCH [RPRE1], 16 * SIZE
	(p16) LDFPD	f32, f33 = [AO1], 2 * SIZE
	(p17) FMA	f10 = f104, f35, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f11 = f105, f36, f11
	}
	;;
	{ .mmf
	(p15) PREFETCH [RPRE2], 16 * SIZE
	(p16) LDFD	f34      = [AO2], 1 * SIZE
	(p17) FMA	f12 = f104, f37, f12
	}
	{ .mmf
	(p17) LDFD	f84      = [AO21], 8 * SIZE
	nop	__LINE__
	(p17) FMA	f13 = f105, f38, f13
	}
	;;
	{ .mmf
	(p14) PREFETCH [RPRE3], 16 * SIZE
	(p16) LDFPD	f36, f37 = [AO3], 2 * SIZE
	(p17) FMA	f14 = f104, f39, f14
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f15 = f105, f40, f15
	}
	;;
	{ .mmf
	(p15) PREFETCH [RPRE4], 16 * SIZE
	(p16) LDFD	f38      = [AO4], 1 * SIZE
	(p17) FMA	f16 = f104, f41, f16
	}
	{ .mmf
	(p17) LDFD	f88      = [AO41], 8 * SIZE
	nop	__LINE__
	(p17) FMA	f17 = f105, f42, f17
	}
	;;
	{ .mmf
	(p14) PREFETCH [RPRE5], 16 * SIZE
	(p16) LDFPD	f40, f41 = [AO5], 2 * SIZE
	(p17) FMA	f18 = f104, f43, f18
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f19 = f105, f44, f19
	}
	;;
	{ .mmf
	(p15) PREFETCH [RPRE6], 16 * SIZE
	(p16) LDFD	f42      = [AO6], 1 * SIZE
	(p17) FMA	f20 = f104, f45, f20
	}
	{ .mmf
	(p17) LDFD	f92      = [AO61], 8 * SIZE
	nop	__LINE__
	(p17) FMA	f21 = f105, f46, f21
	}
	;;
	{ .mmf
	(p14) PREFETCH [RPRE7], 16 * SIZE
	(p16) LDFPD	f44, f45 = [AO7], 2 * SIZE
	(p17) FMA	f22 = f104, f47, f22
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f23 = f105, f48, f23
	}
	;;
	{ .mmf
	(p15) PREFETCH [RPRE8], 16 * SIZE
	(p16) LDFD	f46      = [AO8], 1 * SIZE
	(p17) FMA	f8  = f106, f49, f8
	}
	{ .mmf
	(p17) LDFD	f96      = [AO81], 8 * SIZE
	nop	__LINE__
	(p17) FMA	f9  = f107, f50, f9
	}
	;;
	{ .mmf
	(p14) PREFETCH [PREB], 16 * SIZE
	(p16) LDFPD	f48, f49 = [AO1], 2 * SIZE
	(p17) FMA	f10 = f106, f51, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f11 = f107, f52, f11
	}
	;;
	{ .mmf
	(p16) LDFPD	f35, f50 = [AO2], 2 * SIZE
	(p16) LDFPD	f103, f104 = [BO], 2 * SIZE
	(p17) FMA	f12 = f106, f53, f12
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f13 = f107, f54, f13
	}
	;;
	{ .mmf
	(p16) LDFPD	f52, f53 = [AO3], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f14 = f106, f55, f14
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f15 = f107, f56, f15
	}
	;;
	{ .mmf
	(p16) LDFPD	f39, f54 = [AO4], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f16 = f106, f57, f16
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f17 = f107, f58, f17
	}
	;;
	{ .mmf
	(p16) LDFPD	f56, f57 = [AO5], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f18 = f106, f59, f18
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f19 = f107, f60, f19
	}
	;;
	{ .mmf
	(p16) LDFPD	f43, f58 = [AO6], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f20 = f106, f61, f20
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f21 = f107, f62, f21
	}
	;;
	{ .mmf
	(p16) LDFPD	f60, f61 = [AO7], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f22 = f106, f63, f22
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f23 = f107, f64, f23
	}
	;;
	{ .mmf
	(p16) LDFPD	f47, f62 = [AO8], 2 * SIZE
	(p16) LDFPD	f105, f106 = [BO], 2 * SIZE
	(p17) FMA	f8  = f108, f65, f8
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f9  = f109, f66, f9
	}
	;;
	{ .mmf
	(p16) LDFPD	f64, f65 = [AO1], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f10 = f108, f67, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f11 = f109, f68, f11
	}
	;;
	{ .mmf
	(p16) LDFPD	f51, f66 = [AO2], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f12 = f108, f69, f12
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f13 = f109, f70, f13
	}
	;;
	{ .mmf
	(p16) LDFPD	f68, f69 = [AO3], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f14 = f108, f71, f14
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f15 = f109, f72, f15
	}
	;;
	{ .mmf
	(p16) LDFPD	f55, f70 = [AO4], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f16 = f108, f73, f16
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f17 = f109, f74, f17
	}
	;;
	{ .mmf
	(p16) LDFPD	f72, f73 = [AO5], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f18 = f108, f75, f18
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f19 = f109, f76, f19
	}
	;;
	{ .mmf
	(p16) LDFPD	f59, f74 = [AO6], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f20 = f108, f77, f20
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f21 = f109, f78, f21
	}
	;;
	{ .mmf
	(p16) LDFPD	f76, f77 = [AO7], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f22 = f108, f79, f22
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f23 = f109, f80, f23
	}
	;;
	{ .mmf
	(p16) LDFPD	f63, f78 = [AO8], 2 * SIZE
	(p16) LDFPD	f107, f108 = [BO], 2 * SIZE
	(p17) FMA	f8  = f110, f81, f8
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f9  = f111, f82, f9
	}
	;;
	{ .mmf
	(p16) LDFPD	f80, f81 = [AO1], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f10 = f110, f83, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f11 = f111, f84, f11
	}
	;;
	{ .mmf
	(p16) LDFPD	f67, f82 = [AO2]
	nop	__LINE__
	(p17) FMA	f12 = f110, f85, f12
	}
	{ .mmf
	nop	__LINE__
	(p16) adds	AO2 = 3 * SIZE, AO2
	(p17) FMA	f13 = f111, f86, f13
	}
	;;
	{ .mmf
	(p16) LDFPD	f84, f85 = [AO3], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f14 = f110, f87, f14
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f15 = f111, f88, f15
	}
	;;
	{ .mmf
	(p16) LDFPD	f71, f86 = [AO4]
	nop	__LINE__
	(p17) FMA	f16 = f110, f89, f16
	}
	{ .mmf
	nop	__LINE__
	(p16) adds	AO4 = 3 * SIZE, AO4
	(p17) FMA	f17 = f111, f90, f17
	}
	;;
	{ .mmf
	(p16) LDFPD	f88, f89 = [AO5], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f18 = f110, f91, f18
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f19 = f111, f92, f19
	}
	;;
	{ .mmf
	(p16) LDFPD	f75, f90 = [AO6]
	nop	__LINE__
	(p17) FMA	f20 = f110, f93, f20
	}
	{ .mmf
	nop	__LINE__
	(p16) adds	AO6 = 3 * SIZE, AO6
	(p17) FMA	f21 = f111, f94, f21
	}
	;;
	{ .mmf
	(p16) LDFPD	f92, f93 = [AO7], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f22 = f110, f95, f22
	}
	{ .mfb
	adds	I = -1, I
	(p17) FMA	f23 = f111, f96, f23
	br.ctop.sptk.few .L112
	}
	;;
	.align 16

.L115:
	and	I = 15, MIN_M
	mov	pr.rot= 0
	;;
	cmp.eq	p6,  p0 = 0, I
	cmp.eq	p16, p15 = r0, r0
	;;
	adds	I = 1, I
	;;
	shr	I = I, 1
	;;
	adds	I = -1, I
	adds	AO21 = 1 * SIZE, AO2
	adds	AO41 = 1 * SIZE, AO4
	adds	AO61 = 1 * SIZE, AO6
	adds	AO81 = 1 * SIZE, AO8
	;;
	mov	ar.lc = I
	mov	ar.ec= 3
	and	I = 15, MIN_M
	(p6) br.cond.dpnt .L118
	;;
	.align 16

.L116:
	{ .mmf
	(p16) LDFPD	f104, f107 = [BO], 2 * SIZE
	(p16) LDFPD	f32,  f35  = [AO1], 2 * SIZE
	(p18) FMA	f8  = f106, f34, f8
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p15) FMA	f9  = f109, f37, f9
	}
	;;
	{ .mmf
	(p16) LDFD	f38  = [AO2], 2 * SIZE
	(p17) LDFD	f42  = [AO21], 2 * SIZE
	(p18) FMA	f10 = f106, f40, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p15) FMA	f11 = f109, f43, f11
	}
	;;
	{ .mmf
	(p16) LDFPD	f44,  f47  = [AO3], 2 * SIZE
	nop	__LINE__
	(p18) FMA	f12 = f106, f46, f12
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p15) FMA	f13 = f109, f49, f13
	}
	;;
	{ .mmf
	(p16) LDFD	f50  = [AO4], 2 * SIZE
	(p17) LDFD	f54  = [AO41], 2 * SIZE
	(p18) FMA	f14 = f106, f52, f14
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p15) FMA	f15 = f109, f55, f15
	}
	;;
	{ .mmf
	(p16) LDFPD	f56,  f59  = [AO5], 2 * SIZE
	nop	__LINE__
	(p18) FMA	f16 = f106, f58, f16
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p15) FMA	f17 = f109, f61, f17
	}
	;;
	{ .mmf
	(p16) LDFD	f62  = [AO6], 2 * SIZE
	(p17) LDFD	f66  = [AO61], 2 * SIZE
	(p18) FMA	f18 = f106, f64, f18
	}
	{ .mmf
	nop	__LINE__
	(p17) adds	I = -2, I
	(p15) FMA	f19 = f109, f67, f19
	}
	;;
	{ .mmf
	(p16) LDFPD	f68,  f71  = [AO7], 2 * SIZE
	nop	__LINE__
	(p18) FMA	f20 = f106, f70, f20
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p15) FMA	f21 = f109, f73, f21
	}
	;;
	{ .mmf
	(p16) LDFD	f74  = [AO8], 2 * SIZE
	(p17) LDFD	f78  = [AO81], 2 * SIZE
	(p15) FMA	f23 = f109, f79, f23
	}
	{ .mfb
	(p17) cmp.ne.unc p15, p0 = -1, I
	(p18) FMA	f22 = f106, f76, f22
	br.ctop.sptk.few .L116
	}
	;;

.L118:
	{ .mmf
	mov	AO1 = CO
	LDFD	f32 = [CO], INCY
	FADD	f8  = f8,  f9
	}
	;;
	{ .mmf
	LDFD	f33 = [CO], INCY
	nop	__LINE__
	FADD	f10 = f10, f11
	}
	;;
	{ .mmf
	LDFD	f34 = [CO], INCY
	nop	__LINE__
	FADD	f12 = f12, f13
	}
	;;
	{ .mmf
	LDFD	f35 = [CO], INCY
	nop	__LINE__
	FADD	f14 = f14, f15
	}
	;;
	{ .mmf
	LDFD	f36 = [CO], INCY
	nop	__LINE__
	FADD	f16 = f16, f17
	}
	;;
	{ .mmf
	LDFD	f37 = [CO], INCY
	nop	__LINE__
	FADD	f18 = f18, f19
	}
	;;
	{ .mmf
	LDFD	f38 = [CO], INCY
	nop	__LINE__
	FADD	f20 = f20, f21
	}
	;;
	{ .mmf
	LDFD	f39 = [CO], INCY
	nop	__LINE__
	FADD	f22 = f22, f23
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f32 = ALPHA, f8,  f32
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f33 = ALPHA, f10, f33
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f34 = ALPHA, f12, f34
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f35 = ALPHA, f14, f35
	}
	;;
	{ .mmf
	STFD [AO1] = f32
	add	AO1 = AO1, INCY
	FMA	f36 = ALPHA, f16, f36
	}
	;;
	{ .mmf
	STFD [AO1] = f33
	add	AO1 = AO1, INCY
	FMA	f37 = ALPHA, f18, f37
	}
	;;
	{ .mmf
	STFD [AO1] = f34
	add	AO1 = AO1, INCY
	FMA	f38 = ALPHA, f20, f38
	}
	;;
	{ .mmf
	STFD [AO1] = f35
	add	AO1 = AO1, INCY
	FMA	f39 = ALPHA, f22, f39
	}
	;;
	{ .mmi
	STFD [AO1] = f36
	add	AO1 = AO1, INCY
	adds J = -1, J
	}
	;;
	{ .mmi
	STFD [AO1] = f37
	add	AO1 = AO1, INCY
	nop	__LINE__
	}
	;;
	{ .mmi
	STFD [AO1] = f38
	add	AO1 = AO1, INCY
	cmp4.lt p6, p0 = 0, J
	}
	;;
	{ .mib
	STFD [AO1] = f39
	add	AO1 = AO1, INCY
	(p6) br.cond.dptk .L111
	}
	;;
	.align 16

.L120:
	{ .mfi
	mov	AO1 = A
	mov	f8  = f0
	mov	pr.rot= 0
	}
	{ .mfi
	add	AO2 = LDA, A
	mov	f10 = f0
	tbit.z	p6, p0  = N, 2
	}
	;;
	{ .mfi
	shladd	AO3 = LDA, 1, A
	mov	f12 = f0
	shr	I = MIN_M, 4
	}
	{ .mfb
	shladd	AO4 = LDA, 1, AO2
	mov	f14 = f0
	(p6) br.cond.dpnt .L130
	}
	;;
	{ .mmf
	(p8) LDFD f32 = [AO1], SIZE
	(p8) LDFD f33 = [AO2], SIZE
	mov	f9  = f0
	}
	{ .mmf
	mov	BO  = BUFFER
	shladd	A   = LDA, 2, A
	mov	f11 = f0
	}
	;;
	{ .mmf
	(p8) LDFD f40 = [BO], 2 * SIZE
	cmp.eq	p6, p0 = 0, I
	mov	f13 = f0
	}
	{ .mmf
	(p8) LDFD f34 = [AO3], SIZE
	(p8) LDFD f35 = [AO4], SIZE
	mov	f15 = f0
	}
	;;
	{ .mmi
	adds	RPRE1  = RPREFETCH * SIZE, AO1
	adds	RPRE2  = (RPREFETCH + 8) * SIZE, AO2
	mov	ar.ec= 2
	}
	{ .mmi
	cmp.eq	p16, p0 = r0, r0
	add	I = I, I
	adds	AO21 = 7 * SIZE, AO2
	}
	;;
	{ .mmf
	adds	WPRE =  4 * SIZE, CO
	adds	PREB  = RPREFETCH * SIZE, BO
	(p8) FMPY	f8  = f40, f32
	}
	{ .mmf
	adds	RPRE3  = RPREFETCH * SIZE, AO3
	adds	I = -1, I
	(p8) FMPY	f10 = f40, f33
	}
	;;
	{ .mfi
	adds	AO41 = 7 * SIZE, AO4
	(p8) FMPY	f12 = f40, f34
	mov	ar.lc = I
	}
	{ .mfb
	adds	RPRE4  = (RPREFETCH + 8) * SIZE, AO4
	(p8) FMPY	f14 = f40, f35
	(p6) br.cond.dpnt .L125
	}
	;;
	.align 16

.L122:
	{ .mmf
	(p17) LDFPD	f72, f87 = [AO4]
	(p17) LDFPD	f110, f111 = [BO], 2 * SIZE
	(p17) FMA	f8  = f104, f33, f8
	}
	{ .mfi
	(p17) adds	AO4 = 3 * SIZE, AO4
	(p17) FMA	f9  = f105, f34, f9
	(p16) tbit.nz.unc p14, p15 = I, 0
	}
	;;
	{ .mmf
	(p14) PREFETCH [RPRE1], 16 * SIZE
	(p16) LDFPD	f32, f33 = [AO1], 2 * SIZE
	(p17) FMA	f10 = f104, f35, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f11 = f105, f36, f11
	}
	;;
	{ .mmf
	(p15) PREFETCH [RPRE2], 16 * SIZE
	(p16) LDFD	f34      = [AO2], 1 * SIZE
	(p17) FMA	f12 = f104, f37, f12
	}
	{ .mmf
	(p17) LDFD	f84      = [AO21], 8 * SIZE
	nop	__LINE__
	(p17) FMA	f13 = f105, f38, f13
	}
	;;
	{ .mmf
	(p14) PREFETCH [RPRE3], 16 * SIZE
	(p16) LDFPD	f36, f37 = [AO3], 2 * SIZE
	(p17) FMA	f14 = f104, f39, f14
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f15 = f105, f40, f15
	}
	;;
	{ .mmf
	(p15) PREFETCH [RPRE4], 16 * SIZE
	(p16) LDFD	f38      = [AO4], 1 * SIZE
	(p17) FMA	f8  = f106, f49, f8
	}
	{ .mmf
	(p17) LDFD	f88      = [AO41], 8 * SIZE
	nop	__LINE__
	(p17) FMA	f9  = f107, f50, f9
	}
	;;
	{ .mmf
	(p14) PREFETCH [PREB], 16 * SIZE
	(p16) LDFPD	f48, f49 = [AO1], 2 * SIZE
	(p17) FMA	f10 = f106, f51, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f11 = f107, f52, f11
	}
	;;
	{ .mmf
	(p16) LDFPD	f35, f50 = [AO2], 2 * SIZE
	(p16) LDFPD	f103, f104 = [BO], 2 * SIZE
	(p17) FMA	f12 = f106, f53, f12
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f13 = f107, f54, f13
	}
	;;
	{ .mmf
	(p16) LDFPD	f52, f53 = [AO3], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f14 = f106, f55, f14
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f15 = f107, f56, f15
	}
	;;
	{ .mmf
	(p16) LDFPD	f39, f54 = [AO4], 2 * SIZE
	(p16) LDFPD	f105, f106 = [BO], 2 * SIZE
	(p17) FMA	f8  = f108, f65, f8
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f9  = f109, f66, f9
	}
	;;
	{ .mmf
	(p16) LDFPD	f64, f65 = [AO1], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f10 = f108, f67, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f11 = f109, f68, f11
	}
	;;
	{ .mmf
	(p16) LDFPD	f51, f66 = [AO2], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f12 = f108, f69, f12
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f13 = f109, f70, f13
	}
	;;
	{ .mmf
	(p16) LDFPD	f68, f69 = [AO3], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f14 = f108, f71, f14
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f15 = f109, f72, f15
	}
	;;
	{ .mmf
	(p16) LDFPD	f55, f70 = [AO4], 2 * SIZE
	(p16) LDFPD	f107, f108 = [BO], 2 * SIZE
	(p17) FMA	f8  = f110, f81, f8
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f9  = f111, f82, f9
	}
	;;
	{ .mmf
	(p16) LDFPD	f80, f81 = [AO1], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f10 = f110, f83, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f11 = f111, f84, f11
	}
	;;
	{ .mmf
	(p16) LDFPD	f67, f82 = [AO2]
	nop	__LINE__
	(p17) FMA	f12 = f110, f85, f12
	}
	{ .mmf
	nop	__LINE__
	(p16) adds	AO2 = 3 * SIZE, AO2
	(p17) FMA	f13 = f111, f86, f13
	}
	;;
	{ .mmf
	(p16) LDFPD	f84, f85 = [AO3], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f14 = f110, f87, f14
	}
	{ .mfb
	adds	I = -1, I
	(p17) FMA	f15 = f111, f88, f15
	br.ctop.sptk.few .L122
	}
	;;
	.align 16

.L125:
	and	I = 15, MIN_M
	mov	pr.rot= 0
	;;
	cmp.eq	p6,  p0 = 0, I
	cmp.eq	p16, p15 = r0, r0
	;;
	adds	I = 1, I
	adds	AO21 = 1 * SIZE, AO2
	adds	AO41 = 1 * SIZE, AO4
	;;
	shr	I = I, 1
	;;
	adds	I = -1, I
	;;
	mov	ar.lc = I
	mov	ar.ec= 3
	and	I = 15, MIN_M
	(p6) br.cond.dpnt .L128
	;;
	.align 16

.L126:
	{ .mmf
	(p16) LDFPD	f104, f107 = [BO], 2 * SIZE
	(p16) LDFPD	f32,  f35  = [AO1], 2 * SIZE
	(p18) FMA	f8  = f106, f34, f8
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p15) FMA	f9  = f109, f37, f9
	}
	;;
	{ .mmf
	(p17) LDFD	f42        = [AO21], 2 * SIZE
	(p16) LDFD	f38        = [AO2], 2 * SIZE
	(p18) FMA	f10 = f106, f40, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p15) FMA	f11 = f109, f43, f11
	}
	;;
	{ .mmf
	(p16) LDFPD	f44,  f47  = [AO3], 2 * SIZE
	nop	__LINE__
	(p18) FMA	f12 = f106, f46, f12
	}
	{ .mmf
	nop	__LINE__
	(p17) adds	I = -2, I
	(p15) FMA	f13 = f109, f49, f13
	}
	;;
	{ .mmf
	(p17) LDFD	f54        = [AO41], 2 * SIZE
	(p16) LDFD	f50        = [AO4], 2 * SIZE
	(p15) FMA	f15 = f109, f55, f15
	}
	{ .mfb
	(p17) cmp.ne.unc p15, p0 = -1, I
	(p18) FMA	f14 = f106, f52, f14
	br.ctop.sptk.few .L126
	}
	;;

.L128:
	{ .mmf
	mov	AO1 = CO
	LDFD	f32 = [CO], INCY
	FADD	f8  = f8,  f9
	}
	;;
	{ .mmf
	LDFD	f33 = [CO], INCY
	nop	__LINE__
	FADD	f10 = f10, f11
	}
	;;
	{ .mmf
	LDFD	f34 = [CO], INCY
	nop	__LINE__
	FADD	f12 = f12, f13
	}
	;;
	{ .mmf
	LDFD	f35 = [CO], INCY
	nop	__LINE__
	FADD	f14 = f14, f15
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f32 = ALPHA, f8,  f32
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f33 = ALPHA, f10, f33
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f34 = ALPHA, f12, f34
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f35 = ALPHA, f14, f35
	}
	;;
	{ .mmf
	STFD [AO1] = f32
	add	AO1 = AO1, INCY
	}
	;;
	{ .mmf
	STFD [AO1] = f33
	add	AO1 = AO1, INCY
	}
	;;
	{ .mmf
	STFD [AO1] = f34
	add	AO1 = AO1, INCY
	}
	;;
	{ .mmf
	STFD [AO1] = f35
	add	AO1 = AO1, INCY
	}
	;;
	.align 16

.L130:
	{ .mfi
	mov	AO1 = A
	mov	f8  = f0
	mov	pr.rot= 0
	}
	{ .mfi
	add	AO2 = LDA, A
	mov	f10 = f0
	tbit.z	p6, p0  = N, 1
	}
	;;
	{ .mfi
	mov	BO  = BUFFER
	mov	f12 = f0
	shr	I = MIN_M, 4
	}
	{ .mfb
	adds	WPRE =  4 * SIZE, CO
	mov	f14 = f0
	(p6) br.cond.dpnt .L140
	}
	;;
	{ .mmf
	(p8) LDFD f32 = [AO1], SIZE
	(p8) LDFD f33 = [AO2], SIZE
	mov	f9  = f0
	}
	{ .mfi
	shladd	A   = LDA, 1, A
	mov	f11 = f0
	mov	ar.ec= 2
	}
	;;
	{ .mmf
	(p8) LDFD f40 = [BO], 2 * SIZE
	cmp.eq	p6, p0 = 0, I
	mov	f13 = f0
	}
	{ .mmf
	adds	RPRE1  = RPREFETCH * SIZE, AO1
	add	I = I, I
	mov	f15 = f0
	}
	;;
	{ .mmi
	cmp.eq	p16, p0 = r0, r0
	adds	RPRE2  = (RPREFETCH + 8) * SIZE, AO2
	adds	I = -1, I
	}
	;;
	{ .mfi
	adds	AO21 = 7 * SIZE, AO2
	(p8) FMPY	f8  = f40, f32
	mov	ar.lc = I
	}
	{ .mfb
	adds	PREB  = RPREFETCH * SIZE, BO
	(p8) FMPY	f10 = f40, f33
	(p6) br.cond.dpnt .L135
	}
	;;
	.align 16

.L132:
	{ .mmf
	(p17) LDFPD	f68, f83 = [AO2]
	(p17) LDFPD	f110, f111 = [BO], 2 * SIZE
	(p17) FMA	f8  = f104, f33, f8
	}
	{ .mfi
	(p17) adds	AO2 = 3 * SIZE, AO2
	(p17) FMA	f9  = f105, f34, f9
	(p16) tbit.nz.unc p14, p15 = I, 0
	}
	;;
	{ .mmf
	(p14) PREFETCH [RPRE1], 16 * SIZE
	(p16) LDFPD	f32, f33 = [AO1], 2 * SIZE
	(p17) FMA	f10 = f104, f35, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f11 = f105, f36, f11
	}
	;;
	{ .mmf
	(p15) PREFETCH [RPRE2], 16 * SIZE
	(p16) LDFD	f34      = [AO2], 1 * SIZE
	(p17) FMA	f8  = f106, f49, f8
	}
	{ .mmf
	(p17) LDFD	f84      = [AO21], 8 * SIZE
	nop	__LINE__
	(p17) FMA	f9  = f107, f50, f9
	}
	;;
	{ .mmf
	(p14) PREFETCH [PREB], 16 * SIZE
	(p16) LDFPD	f48, f49 = [AO1], 2 * SIZE
	(p17) FMA	f10 = f106, f51, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f11 = f107, f52, f11
	}
	;;
	{ .mmf
	(p16) LDFPD	f35, f50 = [AO2], 2 * SIZE
	(p16) LDFPD	f103, f104 = [BO], 2 * SIZE
	(p17) FMA	f8  = f108, f65, f8
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f9  = f109, f66, f9
	}
	;;
	{ .mmf
	(p16) LDFPD	f105, f106 = [BO], 2 * SIZE
	(p16) LDFPD	f64, f65 = [AO1], 2 * SIZE
	(p17) FMA	f10 = f108, f67, f10
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f11 = f109, f68, f11
	}
	;;
	{ .mmf
	(p16) LDFPD	f51, f66 = [AO2], 2 * SIZE
	(p16) LDFPD	f107, f108 = [BO], 2 * SIZE
	(p17) FMA	f8  = f110, f81, f8
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f9  = f111, f82, f9
	}
	;;
	{ .mmf
	(p16) LDFPD	f80, f81 = [AO1], 2 * SIZE
	nop	__LINE__
	(p17) FMA	f10 = f110, f83, f10
	}
	{ .mfb
	adds	I = -1, I
	(p17) FMA	f11 = f111, f84, f11
	br.ctop.sptk.few .L132
	}
	;;
	.align 16

.L135:
	and	I = 15, MIN_M
	;;
	cmp.eq	p6,  p0 = 0, I
	(p6) br.cond.dpnt .L138
	;;
	tbit.nz	p12, p0 = MIN_M, 3
	tbit.nz	p13, p0 = MIN_M, 2
	tbit.nz	p14, p0 = MIN_M, 1
	tbit.nz	p15, p0 = MIN_M, 0
	;;
	(p12) LDFPD	f100, f101 = [BO], 2 * SIZE
	(p12) LDFPD	f32,  f33  = [AO1], 2 * SIZE
	(p12) LDFD	f34        = [AO2], 1 * SIZE
	;;
	(p12) LDFPD	f36,  f37  = [AO1], 2 * SIZE
	(p12) LDFPD	f35,  f38  = [AO2], 2 * SIZE
	;;
	(p12) LDFPD	f102, f103 = [BO],  2 * SIZE
	(p12) LDFPD	f39,  f42  = [AO2], 2 * SIZE
	;;
	(p12) LDFPD	f40,  f41  = [AO1], 2 * SIZE
	(p12) LDFPD	f43,  f46  = [AO2], 2 * SIZE
	;;
	(p12) LDFPD	f104, f105 = [BO], 2 * SIZE
	(p12) LDFPD	f44,  f45  = [AO1], 2 * SIZE
	(p12) LDFD	f47        = [AO2], 1 * SIZE
	;;
	(p12) LDFPD	f106, f107 = [BO], 2 * SIZE
	(p13) LDFD	f50        = [AO2], 1 * SIZE
	(p13) LDFPD	f48,  f49  = [AO1], 2 * SIZE
	;;
	(p13) LDFPD	f108, f109 = [BO], 2 * SIZE
	(p13) LDFPD	f51,  f54  = [AO2], 2 * SIZE
	;;
	(p13) LDFPD	f110, f111 = [BO], 2 * SIZE
	(p13) LDFPD	f52,  f53  = [AO1], 2 * SIZE
	(p13) LDFD	f55        = [AO2], 1 * SIZE
	;;
	(p14) LDFPD	f56,  f57  = [AO1], 2 * SIZE
	(p14) LDFD	f58        = [AO2], 1 * SIZE
	;;
	(p14) LDFPD	f112, f113 = [BO], 2 * SIZE
	(p15) LDFD	f60        = [AO1]
	(p14) LDFD	f59        = [AO2], 1 * SIZE
	;;
	(p15) LDFD	f61        = [AO2]
	(p15) LDFD	f114       = [BO]
	;;
	(p12) FMA	f8  = f100, f32, f8
	(p12) FMA	f9  = f101, f33, f9
	(p12) FMA	f10 = f100, f34, f10
	(p12) FMA	f11 = f101, f35, f11
	;;
	(p12) FMA	f12 = f102, f36, f12
	(p12) FMA	f13 = f103, f37, f13
	(p12) FMA	f14 = f102, f38, f14
	(p12) FMA	f15 = f103, f39, f15
	;;
	(p12) FMA	f8  = f104, f40, f8
	(p12) FMA	f9  = f105, f41, f9
	(p12) FMA	f10 = f104, f42, f10
	(p12) FMA	f11 = f105, f43, f11
	;;
	(p12) FMA	f12 = f106, f44, f12
	(p12) FMA	f13 = f107, f45, f13
	(p12) FMA	f14 = f106, f46, f14
	(p12) FMA	f15 = f107, f47, f15
	;;
	(p13) FMA	f8  = f108, f48, f8
	(p13) FMA	f9  = f109, f49, f9
	(p13) FMA	f10 = f108, f50, f10
	(p13) FMA	f11 = f109, f51, f11
	;;
	(p13) FMA	f12 = f110, f52, f12
	(p13) FMA	f13 = f111, f53, f13
	(p13) FMA	f14 = f110, f54, f14
	(p13) FMA	f15 = f111, f55, f15
	;;
	(p14) FMA	f8  = f112, f56, f8
	(p14) FMA	f9  = f113, f57, f9
	(p14) FMA	f10 = f112, f58, f10
	(p14) FMA	f11 = f113, f59, f11
	;;
	(p15) FMA	f12 = f114, f60, f12
	(p15) FMA	f14 = f114, f61, f14
	;;
.L138:
	FADD	f8  = f8,  f9
	FADD	f10 = f10, f11
	FADD	f12 = f12, f13
	FADD	f14 = f14, f15
	;;
	FADD	f8  = f8,  f12
	FADD	f10 = f10, f14
	;;
	{ .mmf
	mov	AO1 = CO
	LDFD	f32 = [CO], INCY
	}
	;;
	{ .mmf
	LDFD	f33 = [CO], INCY
	nop	__LINE__
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f32 = ALPHA, f8,  f32
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f33 = ALPHA, f10, f33
	}
	;;
	{ .mmf
	STFD [AO1] = f32
	add	AO1 = AO1, INCY
	}
	;;
	{ .mmf
	STFD [AO1] = f33
	}
	;;
	.align 16

.L140:
	{ .mfi
	mov	AO1 = A
	mov	f8  = f0
	shr	I = MIN_M, 4
	}
	{ .mfi
	mov	BO  = BUFFER
	mov	f10 = f0
	tbit.z	p7, p0  = N, 0
	}
	;;
	{ .mfi
	cmp.eq	p6, p0 = 0, I
	mov	f12 = f0
	mov	pr.rot= 0
	}
	{ .mfb
	add	I = I, I
	mov	f14 = f0
	(p7) br.cond.dpnt .L199
	}
	;;
	{ .mfi
	(p8) LDFD f32 = [AO1], SIZE
	mov	f9  = f0
	mov	ar.ec= 2
	}
	{ .mmf
	(p8) LDFD f40 = [BO], 2 * SIZE
	add	A   = A, LDA
	mov	f11 = f0
	}
	;;
	{ .mmf
	adds	WPRE =  1 * SIZE, CO
	adds	PREB  = RPREFETCH * SIZE, BO
	mov	f13 = f0
	}
	{ .mmf
	cmp.eq	p16, p0 = r0, r0
	adds	I = -1, I
	mov	f15 = f0
	}
	;;
	{ .mfi
	lfetch.excl.nt1	[WPRE]
	(p8) FMPY	f8  = f40, f32
	mov	ar.lc = I
	}
	{ .mmb
	nop	__LINE__
	nop	__LINE__
	(p6) br.cond.dpnt .L145
	}
	;;
	.align 16

.L142:
	{ .mmf
	(p17) LDFPD	f81, f82   = [AO1], 2 * SIZE
	(p17) LDFPD	f110, f111 = [BO], 2 * SIZE
	(p17) FMA	f8  = f104, f33, f8
	}
	{ .mfi
	nop	__LINE__
	(p17) FMA	f9  = f105, f34, f9
	(p16) tbit.nz.unc p14, p15 = I, 0
	}
	;;
	{ .mmf
	(p16) LDFPD	f32, f33   = [AO1], 2 * SIZE
	(p16) LDFPD	f103, f104 = [BO], 2 * SIZE
	(p17) FMA	f8  = f106, f49, f8
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f9  = f107, f50, f9
	}
	;;
	{ .mmf
	(p16) LDFPD	f105, f106 = [BO], 2 * SIZE
	(p16) LDFPD	f48, f49 = [AO1], 2 * SIZE
	(p17) FMA	f8  = f108, f65, f8
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	(p17) FMA	f9  = f109, f66, f9
	}
	;;
	{ .mmf
	(p16) LDFPD	f64, f65  = [AO1], 2 * SIZE
	(p16) LDFPD	f107, f108 = [BO], 2 * SIZE
	(p17) FMA	f8  = f110, f81, f8
	}
	{ .mfb
	adds	I = -1, I
	(p17) FMA	f9  = f111, f82, f9
	br.ctop.sptk.few .L142
	}
	;;
	.align 16

.L145:
	and	I = 15, MIN_M
	;;
	cmp.eq	p6,  p0 = 0, I
	(p6) br.cond.dpnt .L148
	;;
	tbit.nz	p12, p0 = MIN_M, 3
	tbit.nz	p13, p0 = MIN_M, 2
	tbit.nz	p14, p0 = MIN_M, 1
	tbit.nz	p15, p0 = MIN_M, 0
	;;
	(p12) LDFPD	f32,  f33  = [AO1], 2 * SIZE
	(p12) LDFPD	f100, f101 = [BO], 2 * SIZE
	;;
	(p12) LDFPD	f36,  f37  = [AO1], 2 * SIZE
	(p12) LDFPD	f102, f103 = [BO], 2 * SIZE
	;;
	(p12) LDFPD	f40,  f41  = [AO1], 2 * SIZE
	(p12) LDFPD	f104, f105 = [BO], 2 * SIZE
	;;
	(p12) LDFPD	f44,  f45  = [AO1], 2 * SIZE
	(p12) LDFPD	f106, f107 = [BO], 2 * SIZE
	;;
	(p13) LDFPD	f48,  f49  = [AO1], 2 * SIZE
	(p13) LDFPD	f108, f109 = [BO], 2 * SIZE
	;;
	(p13) LDFPD	f52,  f53  = [AO1], 2 * SIZE
	(p13) LDFPD	f110, f111 = [BO], 2 * SIZE
	;;
	(p14) LDFPD	f56,  f57  = [AO1], 2 * SIZE
	(p14) LDFPD	f112, f113 = [BO], 2 * SIZE
	;;
	(p15) LDFD	f60        = [AO1]
	(p15) LDFD	f114       = [BO]
	;;
	(p12) FMA	f8  = f100, f32, f8
	(p12) FMA	f9  = f101, f33, f9
	(p12) FMA	f10 = f102, f36, f10
	(p12) FMA	f11 = f103, f37, f11
	(p12) FMA	f12 = f104, f40, f12
	(p12) FMA	f13 = f105, f41, f13
	(p12) FMA	f14 = f106, f44, f14
	(p12) FMA	f15 = f107, f45, f15
	;;
	(p13) FMA	f8  = f108, f48, f8
	(p13) FMA	f9  = f109, f49, f9
	(p13) FMA	f10 = f110, f52, f10
	(p13) FMA	f11 = f111, f53, f11
	(p14) FMA	f12 = f112, f56, f12
	(p14) FMA	f13 = f113, f57, f13
	(p15) FMA	f14 = f114, f60, f14
	;;
.L148:
	{ .mmf
	LDFD	f32 = [CO]
	nop	__LINE__
	FADD	f8  = f8,  f9
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FADD	f10 = f10, f11
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FADD	f12 = f12, f13
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FADD	f14 = f14, f15
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FADD	f8  = f8,  f12
	}
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FADD	f10 = f10, f14
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FADD	f8  = f8,  f10
	}
	;;
	{ .mmf
	nop	__LINE__
	nop	__LINE__
	FMA	f32 = ALPHA, f8,  f32
	}
	;;
	{ .mmf
	STFD [CO] = f32
	nop	__LINE__
	nop	__LINE__
	}
	;;
	.align 16

.L199:
	adds	IS = P, IS
	shladd	A  = LDAP, BASE_SHIFT, A
	;;
	cmp.gt	p6, p0 = M, IS
	(p6) br.cond.dptk .LIs_loop
	.align 4
	;;

.L999:
	mov	r8 = r0
	adds	r9 = 1 * 16, SP
	;;
	ldf.fill  f16 = [SP], 32
	ldf.fill  f17 = [r9], 32
	mov	 ar.lc = ARLC
	;;
	ldf.fill  f18 = [SP], 32
	ldf.fill  f19 = [r9], 32
	mov pr    = PR, -1
	;;
	ldf.fill  f20 = [SP], 32
	ldf.fill  f21 = [r9], 32
	mov	ar.pfs = ARPFS
	;;
	ldf.fill  f22 = [SP], 32
	ldf.fill  f23 = [r9]
	br.ret.sptk.many b0
	;;
	EPILOGUE
