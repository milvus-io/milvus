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
#define PREFETCH_SIZE ( 8 * 16 +  4)
#elif defined(DOUBLE)
#define PREFETCH_SIZE (16 * 16 +  8)
#else
#define PREFETCH_SIZE (32 * 16 + 16)
#endif

#ifdef USE_MIN
#define CMPUNC cmp.lt.unc
#define CMP    cmp.lt
#else
#define CMPUNC cmp.gt.unc
#define CMP    cmp.gt
#endif

#define RET	r8

#define N	r32
#define DX	r33
#define INCX	r34

#define PRE1	r2

#define I	r14
#define J	r15
#define K	r16
#define TMP	r17
#define INCXM1	r18
#define INCX8	r19
#define MAX1	r20
#define DMAX1	r21
#define DATA1	r22
#define DATA2	r23
#define DATA3	r24
#define DATA4	r25
#define DATA5	r26
#define DATA6	r27
#define DATA7	r28
#define DATA8	r29

#define PR	r30
#define ARLC	r31

	PROLOGUE
	.prologue
	PROFCODE
	{ .mmi
	mov	MAX1 = -1
	mov	DMAX1 = 0
	.save	ar.lc, ARLC
	mov	ARLC = ar.lc
	}
	.body

#ifdef F_INTERFACE
	{ .mmi
	LDINT	N = [N]
	LDINT	INCX = [INCX]
	nop.i 0
	}
	;;
#ifndef USE64BITINT
	{ .mii
	nop.m 0
	sxt4	N = N
	sxt4	INCX = INCX
	}
	;;
#endif
#endif

	{ .mii
	adds	K = -1, N
	shl	INCX = INCX, ZBASE_SHIFT
	mov	PR = pr
	}
	{ .mmb
	cmp.ge	p8, p0 = 0, N
	(p8) br.cond.dptk .L999
	}
	;;
	{ .mib
	cmp.ge	p6, p0 = 0, INCX
	mov	pr.rot= 0
	(p6) br.cond.dptk .L999
	}
	;;
	{ .mmi
	LDFD	f6 = [DX], SIZE
	adds	INCXM1 = - SIZE, INCX
	mov	ar.ec= 5
	}
	;;
	{ .mmi
	LDFD	f7 = [DX], INCXM1
	mov	MAX1 = 0
	mov	I = 1
	}
	;;
	{ .mfi
	cmp.eq	p16, p0 = r0, r0
	fabs	f6 = f6
	shr	J =  K, 3
	}
	{ .mmf
	nop.m 0
	nop.m 0
	fabs	f7 = f7
	}
	;;
	{ .mmi
	cmp.ne	p8,  p0 = r0, r0
	adds	J = -1, J
	shladd	INCX8 = INCX, 3, r0
	}
	{ .mmf
	nop.m 0
	nop.m 0
	FADD	f6 = f6, f7
	}
	;;
	{ .mmi
	getf.d	DMAX1 = f6
	adds	PRE1 = PREFETCH_SIZE * SIZE, DX
	mov	ar.lc = J
	}
	{ .mib
	cmp.eq	p7 ,p0  =   -1, J
	tbit.z	p0, p13 = K, 2
	(p7) br.cond.dpnt  .L15
	}
	.align 32
	;;
.L10:
	{ .mmf
	(p16) lfetch.nt1 [PRE1], INCX8
	(p16) LDFD	f32  = [DX], SIZE
	(p19) fabs	f35  = f35
	}
	{ .mmf
	(p8 ) mov  DMAX1 = DATA1
	nop.m 0
	(p19) fabs	f40  = f40
	}
	;;
	{ .mmf
	(p20) getf.d	DATA5 = f12
	(p16) LDFD	f37  = [DX], INCXM1
	(p20) FADD	f14  = f96,  f101
	}
	{ .mmi
	(p8 ) adds  MAX1 = 0, I
	(p20) CMPUNC	p8,  p0 = DATA2,  DMAX1
	nop.i 0
	}
	;;
	{ .mmf
	(p16) LDFD	f42  = [DX], SIZE
	(p8 ) mov  DMAX1 = DATA2
	(p19) fabs	f45  = f45
	}
	{ .mmf
	nop.m 0
	nop.m 0
	(p19) fabs	f50  = f50
	}
	;;
	{ .mmf
	(p20) getf.d	DATA6 = f13
	(p16) LDFD	f47  = [DX], INCXM1
	(p20) FADD	f15  = f106, f111
	}
	{ .mmi
	(p8 ) adds  MAX1 = 1, I
	(p20) CMPUNC	p8,  p0 = DATA3,  DMAX1
	nop.i 0
	}
	;;
	{ .mmf
	(p16) LDFD	f52  = [DX], SIZE
	(p8 ) mov  DMAX1 = DATA3
	(p19) fabs	f55  = f55
	}
	{ .mmf
	nop.m 0
	nop.m 0
	(p19) fabs	f60  = f60
	}
	;;
	{ .mmf
	(p20) getf.d	DATA7 = f14
	(p16) LDFD	f57  = [DX], INCXM1
	(p19) FADD	f8   = f35,  f40
	}
	{ .mmi
	(p8 ) adds  MAX1 = 2, I
	(p20) CMPUNC	p8,  p0 = DATA4,  DMAX1
	nop.i 0
	}
	;;
	{ .mmf
	(p16) LDFD	f62 = [DX], SIZE
	(p8 ) mov  DMAX1 = DATA4
	(p19) fabs	f65  = f65
	}
	{ .mmf
	nop.m 0
	nop.m 0
	(p19) fabs	f70  = f70
	}
	;;
	{ .mmf
	(p20) getf.d	DATA8 = f15
	(p16) LDFD	f67  = [DX], INCXM1
	(p19) FADD	f9   = f45,  f50
	}
	{ .mmi
	(p8 ) adds  MAX1 = 3, I
	(p20) CMPUNC	p8,  p0 = DATA5,  DMAX1
	nop.i 0
	}
	;;
	{ .mmf
	(p16) LDFD	f72 = [DX], SIZE
	(p8 ) mov  DMAX1 = DATA5
	(p19) fabs	f75  = f75
	}
	{ .mmf
	nop.m 0
	nop.m 0
	(p19) fabs	f80  = f80
	}
	;;
	{ .mmf
	(p19) getf.d	DATA1 = f8
	(p16) LDFD	f77  = [DX], INCXM1
	(p19) FADD	f10  = f55,  f60
	}
	{ .mmi
	(p8 ) adds  MAX1 = 4, I
	(p20) CMPUNC	p8,  p0 = DATA6,  DMAX1
	nop.i 0
	}
	;;
	{ .mmf
	(p16) LDFD	f82 = [DX], SIZE
	(p8 ) mov  DMAX1 = DATA6
	(p19) fabs	f85  = f85
	}
	{ .mmf
	nop.m 0
	nop.m 0
	(p19) fabs	f90  = f90
	}
	;;
	{ .mmf
	(p19) getf.d	DATA2 = f9
	(p16) LDFD	f87  = [DX], INCXM1
	(p19) FADD	f11  = f65,  f70
	}
	{ .mmi
	(p8 ) adds  MAX1 = 5, I
	(p20) CMPUNC	p8,  p0 = DATA7,  DMAX1
	nop.i 0
	}
	;;
	{ .mmf
	(p16) LDFD	f92  = [DX], SIZE
	(p8 ) mov  DMAX1 = DATA7
	(p19) fabs	f95  = f95
	}
	{ .mmf
	mov	TMP = I
	nop.m 0
	(p19) fabs	f100 = f100
	}
	;;
	{ .mmf
	(p19) getf.d	DATA3 = f10
	(p16) LDFD	f97  = [DX], INCXM1
	(p19) FADD	f12  = f75,  f80
	}
	{ .mmi
	(p8 ) adds  MAX1 = 6, I
	(p20) CMPUNC	p8, p0 = DATA8,  DMAX1
	nop.i 0
	}
	;;
	{ .mmf
	(p16) LDFD	f102 = [DX], SIZE
	(p8 ) mov  DMAX1 = DATA8
	(p19) fabs	f105 = f105
	}
	{ .mmf
	(p20) adds I = 8, I
	nop.m 0
	(p19) fabs	f110 = f110
	}
	;;
	{ .mmi
	(p19) getf.d	DATA4 = f11
	(p16) LDFD	f107 = [DX], INCXM1
	(p8 ) adds  MAX1 = 7, TMP
	}
	{ .mfb
	(p19) CMPUNC	p8,  p0 = DATA1,  DMAX1
	(p19) FADD	f13  = f85,  f90
	br.ctop.sptk.few .L10
	}
	;;
	.align 32

.L15:
	{ .mmi
	(p13) LDFD	f32 = [DX], SIZE
	and	J =  7, K
	mov	pr = PR, -65474
	}
	;;
	{ .mmb
	(p13) LDFD	f33 = [DX], INCXM1
	cmp.eq	p8 ,p0  =   r0, J
	(p8) br.cond.dpnt  .L999
	}
	;;
	{ .mmi
	(p13) LDFD	f34 = [DX], SIZE
	;;
	(p13) LDFD	f35 = [DX], INCXM1
	nop.i 0
	}
	;;
	{ .mmi
	(p13) LDFD	f36 = [DX], SIZE
	;;
	(p13) LDFD	f37 = [DX], INCXM1
	nop.i 0
	}
	;;
	{ .mfi
	(p13) LDFD	f38 = [DX], SIZE
	(p13) fabs	f32 = f32
	tbit.z	p0, p14 = K, 1
	}
	;;
	{ .mmf
	(p13) LDFD	f39 = [DX], INCXM1
	nop.m 0
	(p13) fabs	f33 = f33
	}
	;;
	{ .mmf
	(p14) LDFD	f40 = [DX], SIZE
	nop.m 0
	(p13) fabs	f34 = f34
	}
	;;
	{ .mfi
	(p14) LDFD	f41 = [DX], INCXM1
	(p13) fabs	f35 = f35
	tbit.z	p0, p15 = K, 0
	}
	;;
	{ .mmf
	(p14) LDFD	f42 = [DX], SIZE
	nop.m 0
	(p13) fabs	f36 = f36
	}
	;;
	{ .mmf
	(p14) LDFD	f43 = [DX], INCXM1
	nop.m 0
	(p13) fabs	f37 = f37
	}
	{ .mmf
	nop.m 0
	nop.m 0
	(p13) FADD	f32 = f32, f33
	}
	;;
	{ .mmf
	(p15) LDFD	f44 = [DX], SIZE
	nop.m 0
	(p13) fabs	f38 = f38
	}
	;;
	{ .mmf
	(p15) LDFD	f45 = [DX], INCXM1
	nop.m 0
	(p13) fabs	f39 = f39
	}
	{ .mmf
	nop.m 0
	nop.m 0
	(p13) FADD	f34 = f34, f35
	}
	;;
	{ .mmf
	nop.m 0
	nop.m 0
	(p14) fabs	f40 = f40
	}
	;;
	{ .mmf
	(p13) getf.d	DATA1 = f32
	nop.m 0
	(p14) fabs	f41 = f41
	}
	{ .mmf
	nop.m 0
	nop.m 0
	(p13) FADD	f36 = f36, f37
	}
	;;
	{ .mmf
	nop.m 0
	nop.m 0
	(p14) fabs	f42 = f42
	}
	;;
	{ .mmf
	(p13) getf.d	DATA2 = f34
	nop.m 0
	(p14) fabs	f43 = f43
	}
	{ .mmf
	nop.m 0
	nop.m 0
	(p13) FADD	f38 = f38, f39
	}
	;;
	{ .mmf
	nop.m 0
	nop.m 0
	(p15) fabs	f44 = f44
	}
	;;
	{ .mmf
	(p13) getf.d	DATA3 = f36
	nop.m 0
	(p15) fabs	f45 = f45
	}
	{ .mmf
	nop.m 0
	nop.m 0
	(p14) FADD	f40 = f40, f41
	}
	;;
	{ .mmf
	(p13) getf.d	DATA4 = f38
	nop.m 0
	(p14) FADD	f42 = f42, f43
	}
	;;
	{ .mmf
	(p14) getf.d	DATA5 = f40
	nop.m 0
	(p15) FADD	f44 = f44, f45
	}
	;;
	{ .mmi
	(p14) getf.d	DATA6 = f42
	nop.m 0
	(p13) CMPUNC	p8,  p0 = DATA1, DMAX1
	}
	;;
	{ .mmi
	(p15) getf.d	DATA7 = f44
	(p8 ) adds	MAX1  = 0, I
	(p8 ) mov	DMAX1 = DATA1
	}
	;;
	{ .mmi
	(p13) CMPUNC	p8,  p0 = DATA2, DMAX1
	;;
	(p8 ) adds	MAX1  = 1, I
	(p8 ) mov	DMAX1 = DATA2
	}
	;;
	{ .mmi
	(p13) CMPUNC	p8,  p0 = DATA3, DMAX1
	;;
	(p8 ) adds	MAX1  = 2, I
	(p8 ) mov	DMAX1 = DATA3
	}
	;;
	{ .mmi
	(p13) CMPUNC	p8,  p0 = DATA4, DMAX1
	;;
	(p8 ) adds	MAX1  = 3, I
	(p8 ) mov	DMAX1 = DATA4
	}{ .mmi
	(p13) adds	I = 4, I
	nop.m 0
	nop.i 0
	}
	;;
	{ .mmi
	(p14) CMPUNC	p8,  p0 = DATA5, DMAX1
	;;
	(p8 ) adds	MAX1  = 0, I
	(p8 ) mov	DMAX1 = DATA5
	}
	;;
	{ .mmi
	(p14) CMPUNC	p8,  p0 = DATA6, DMAX1
	;;
	(p8 ) adds	MAX1  = 1, I
	(p8 ) mov	DMAX1 = DATA6
	}{ .mmi
	(p14) adds	I = 2, I
	nop.m 0
	nop.i 0
	}
	;;
	{ .mmi
	(p15) CMPUNC	p8,  p0 = DATA7, DMAX1
	;;
	(p8) adds	MAX1  = 0, I
	(p8) mov	DMAX1 = DATA7
	}
	;;
	.align 32

.L999:
	{ .mmi
	setf.d	f8 = DMAX1
	adds RET = 1, MAX1
	mov	ar.lc = ARLC
	}
	{ .mmb
	nop.m 0
	nop.m 0
	br.ret.sptk.many b0
	}
	EPILOGUE

