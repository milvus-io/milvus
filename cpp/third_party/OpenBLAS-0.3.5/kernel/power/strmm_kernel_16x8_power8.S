/***************************************************************************
Copyright (c) 2013-2016, The OpenBLAS Project
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
1. Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in
the documentation and/or other materials provided with the
distribution.
3. Neither the name of the OpenBLAS project nor the names of
its contributors may be used to endorse or promote products
derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE OPENBLAS PROJECT OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/

/**************************************************************************************
* 2016/04/02 Werner Saar (wernsaar@googlemail.com)
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
* 	 LAPACK-TEST		: OK
**************************************************************************************/

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
#include "def_vsx.h"

#ifndef __64BIT__
#define LOAD	lwz
#else
#define LOAD	ld
#endif

#ifdef __64BIT__
#define STACKSIZE 340
#define STACKSIZE 540
#define ALPHA_SP   296+200(SP)
#define FZERO	304+200(SP)
#else
#define STACKSIZE 240
#define ALPHA_SP   224(SP)
#define FZERO	232(SP)
#endif

#define	M	r3
#define	N	r4
#define	K	r5

#ifdef linux
#ifndef __64BIT__
#define A	r6
#define	B	r7
#define	C	r8
#define	LDC	r9
#define OFFSET	r10
#else
#define A	r7
#define	B	r8
#define	C	r9
#define	LDC	r10
#define OFFSET	r6
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#if !defined(__64BIT__) && defined(DOUBLE)
#define A	r8
#define	B	r9
#define	C	r10
#define	LDC	r7
#define OFFSET	r6
#else
#define A	r7
#define	B	r8
#define	C	r9
#define	LDC	r10
#define OFFSET	r6
#endif
#endif

#define alpha_r vs30
#define alpha_vr vs31

#define o0	0

#define VECSAVE r11

#define TBUFFER r13
#define o12	r14
#define o4	r15
#define K1	r16
#define o8	r17
#define L	r18
#define T1	r19
#define KK	r20
#define KKK	r21
#define	I	r22
#define J	r23
#define AO	r24
#define	BO	r25
#define	CO 	r26
#define o16	r27
#define	o32	r28
#define	o48	r29

#define PRE	r30
#define T2	r31

#include "strmm_macros_16x8_power8.S"


#ifndef NEEDPARAM

	PROLOGUE
	PROFCODE

	addi	SP, SP, -STACKSIZE
	li	r0, 0

	stfd	f14,    0(SP)
	stfd	f15,    8(SP)
	stfd	f16,   16(SP)
	stfd	f17,   24(SP)

	stfd	f18,   32(SP)
	stfd	f19,   40(SP)
	stfd	f20,   48(SP)
	stfd	f21,   56(SP)

	stfd	f22,   64(SP)
	stfd	f23,   72(SP)
	stfd	f24,   80(SP)
	stfd	f25,   88(SP)

	stfd	f26,   96(SP)
	stfd	f27,  104(SP)
	stfd	f28,  112(SP)
	stfd	f29,  120(SP)

	stfd	f30,  128(SP)
	stfd	f31,  136(SP)

#ifdef __64BIT__
	std	r31,  144(SP)
	std	r30,  152(SP)
	std	r29,  160(SP)
	std	r28,  168(SP)
	std	r27,  176(SP)
	std	r26,  184(SP)
	std	r25,  192(SP)
	std	r24,  200(SP)
	std	r23,  208(SP)
	std	r22,  216(SP)
	std	r21,  224(SP)
	std	r20,  232(SP)
	std	r19,  240(SP)
	std	r18,  248(SP)
	std	r17,  256(SP)
	std	r16,  264(SP)
	std	r15,  272(SP)
	std	r14,  280(SP)
	std	r13,  288(SP)
        addi    r11, SP, 304
#else
	stw	r31,  144(SP)
	stw	r30,  148(SP)
	stw	r29,  152(SP)
	stw	r28,  156(SP)
	stw	r27,  160(SP)
	stw	r26,  164(SP)
	stw	r25,  168(SP)
	stw	r24,  172(SP)
	stw	r23,  176(SP)
	stw	r22,  180(SP)
	stw	r21,  184(SP)
	stw	r20,  188(SP)
	stw	r19,  192(SP)
	stw	r18,  196(SP)
	stw	r17,  200(SP)
	stw	r16,  204(SP)
	stw	r15,  208(SP)
	stw	r14,  212(SP)
 	stw	r13,  216(SP)
        addi    r11, SP, 224
#endif
        stvx    v20, r11, r0
        addi    r11, r11, 16
        stvx    v21, r11, r0
        addi    r11, r11, 16
        stvx    v22, r11, r0
        addi    r11, r11, 16
        stvx    v23, r11, r0
        addi    r11, r11, 16
        stvx    v24, r11, r0
        addi    r11, r11, 16
        stvx    v25, r11, r0
        addi    r11, r11, 16
        stvx    v26, r11, r0
        addi    r11, r11, 16
        stvx    v27, r11, r0
        addi    r11, r11, 16
        stvx    v28, r11, r0
        addi    r11, r11, 16
        stvx    v29, r11, r0
        addi    r11, r11, 16
        stvx    v30, r11, r0
        addi    r11, r11, 16
        stvx    v31, r11, r0
        li      r11, 0


	// stfd	f1,  ALPHA_SP
	// stw	r0,  FZERO

#if defined(_AIX) || defined(__APPLE__)
#if !defined(__64BIT__) && defined(DOUBLE)
	lwz	LDC, FRAMESLOT(0) + STACKSIZE(SP)
#endif
#endif

	slwi	LDC, LDC, BASE_SHIFT

#if defined(TRMMKERNEL)
#if defined(linux) && defined(__64BIT__)
	ld	OFFSET,  FRAMESLOT(0) + STACKSIZE(SP)
#endif

#if defined(_AIX) || defined(__APPLE__)
#ifdef __64BIT__
	ld	OFFSET,  FRAMESLOT(0) + STACKSIZE(SP)
#else
#ifdef DOUBLE
	lwz	OFFSET,  FRAMESLOT(1) + STACKSIZE(SP)
#else
	lwz	OFFSET,  FRAMESLOT(0) + STACKSIZE(SP)
#endif
#endif
#endif
#endif

        mr      KK, OFFSET
#if defined(TRMMKERNEL) && !defined(LEFT)
        neg     KK, KK
#endif


	cmpwi	cr0, M, 0
	ble	L999_H1
	cmpwi	cr0, N, 0
	ble	L999_H1
	cmpwi	cr0, K, 0
	ble	L999_H1

	li	PRE, 256
	li	o4 , 4
	li	o8 , 8
	li	o12, 12
	li	o16, 16
	li	o32, 32
	li	o48, 48
	addi	TBUFFER, SP, 320+200

        addi    T1, SP, 300+200
        stxsspx    f1, o0 , T1
        stxsspx    f1, o4 , T1
        stxsspx    f1, o8 , T1
        stxsspx    f1, o12 , T1

	lxsspx	   alpha_r, o0,  T1
        lxvw4x     alpha_vr, o0, T1



#include "strmm_logic_16x8_power8.S"

L999:
	addi	r3, 0, 0

	lfd	f14,    0(SP)
	lfd	f15,    8(SP)
	lfd	f16,   16(SP)
	lfd	f17,   24(SP)

	lfd	f18,   32(SP)
	lfd	f19,   40(SP)
	lfd	f20,   48(SP)
	lfd	f21,   56(SP)

	lfd	f22,   64(SP)
	lfd	f23,   72(SP)
	lfd	f24,   80(SP)
	lfd	f25,   88(SP)

	lfd	f26,   96(SP)
	lfd	f27,  104(SP)
	lfd	f28,  112(SP)
	lfd	f29,  120(SP)

	lfd	f30,  128(SP)
	lfd	f31,  136(SP)

#ifdef __64BIT__
	ld	r31,  144(SP)
	ld	r30,  152(SP)
	ld	r29,  160(SP)
	ld	r28,  168(SP)
	ld	r27,  176(SP)
	ld	r26,  184(SP)
	ld	r25,  192(SP)
	ld	r24,  200(SP)
	ld	r23,  208(SP)
	ld	r22,  216(SP)
	ld	r21,  224(SP)
	ld	r20,  232(SP)
	ld	r19,  240(SP)
	ld	r18,  248(SP)
	ld	r17,  256(SP)
	ld	r16,  264(SP)
	ld	r15,  272(SP)
	ld	r14,  280(SP)
	ld	r13,  288(SP)
        addi    r11, SP, 304
#else
	lwz	r31,  144(SP)
	lwz	r30,  148(SP)
	lwz	r29,  152(SP)
	lwz	r28,  156(SP)
	lwz	r27,  160(SP)
	lwz	r26,  164(SP)
	lwz	r25,  168(SP)
	lwz	r24,  172(SP)
	lwz	r23,  176(SP)
	lwz	r22,  180(SP)
	lwz	r21,  184(SP)
	lwz	r20,  188(SP)
	lwz	r19,  192(SP)
	lwz	r18,  196(SP)
	lwz	r17,  200(SP)
	lwz	r16,  204(SP)
	lwz	r15,  208(SP)
	lwz	r14,  212(SP)
	lwz	r13,  216(SP)
        addi    r11, SP, 224
#endif
        lvx     v20, r11, r3
        addi    r11, r11, 16
        lvx     v21, r11, r3
        addi    r11, r11, 16
        lvx     v22, r11, r3
        addi    r11, r11, 16
        lvx     v23, r11, r3
        addi    r11, r11, 16
        lvx     v24, r11, r3
        addi    r11, r11, 16
        lvx     v25, r11, r3
        addi    r11, r11, 16
        lvx     v26, r11, r3
        addi    r11, r11, 16
        lvx     v27, r11, r3
        addi    r11, r11, 16
        lvx     v28, r11, r3
        addi    r11, r11, 16
        lvx     v29, r11, r3
        addi    r11, r11, 16
        lvx     v30, r11, r3
        addi    r11, r11, 16
        lvx     v31, r11, r3
        li      r11, 0

	addi	SP, SP, STACKSIZE
	blr

	EPILOGUE
#endif
