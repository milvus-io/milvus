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
#include "version.h"

#define PREFETCHSIZE 40

	PROLOGUE
	PROFCODE
	.frame	$sp, 16, $26, 0

	ldq	$24,   0($sp)
	fmov	$f19,  $f30
	ldl	$23,   8($sp)
	lda	$sp, -16($sp)
#ifndef PROFILE
	.prologue 0
#else
	.prologue 1
#endif

	nop
	sra	$16,  3,  $1
	stt	$f2,   0($sp)
	cmpeq	$21,  1,  $3

	stt	$f3,   8($sp)
	cmpeq	$23,  1, $4
	and	$16,  7,  $2
	ble	$16, $End

	and	$3,  $4,  $3
	fbeq	$f30, $End

	beq	$3,  $Sub
	ble	$1,  $Remain
	.align 4

	LD	$f10,  0*SIZE($20)
	LD	$f11,  1*SIZE($20)
	LD	$f12,  2*SIZE($20)
	LD	$f13,  3*SIZE($20)

	LD	$f18,  0*SIZE($24)
	LD	$f19,  1*SIZE($24)
	LD	$f20,  2*SIZE($24)
	LD	$f21,  3*SIZE($24)

	LD	$f14,  4*SIZE($20)
	LD	$f15,  5*SIZE($20)
	LD	$f16,  6*SIZE($20)
	LD	$f17,  7*SIZE($20)

	LD	$f22,  4*SIZE($24)
	LD	$f23,  5*SIZE($24)
	LD	$f24,  6*SIZE($24)
	LD	$f25,  7*SIZE($24)

	subq	$1,   1,  $1
	addq	$20, 8*SIZE, $20
	unop
	ble	$1,  $LoopEnd
	.align 4

$Loop:
	ldt	$f31, PREFETCHSIZE * SIZE($24)
	ldl	$31,  PREFETCHSIZE * SIZE($20)

	MUL	$f30, $f10, $f26		# ctemp1 = da * atemp1
	LD	$f10,  0*SIZE($20)
	MUL	$f30, $f11, $f27
	LD	$f11,  1*SIZE($20)

	MUL	$f30, $f12, $f28
	LD	$f12,  2*SIZE($20)
	MUL	$f30, $f13, $f29
	LD	$f13,  3*SIZE($20)

	ADD	$f18, $f26, $f0
	LD	$f18,  8*SIZE($24)
	MUL	$f30, $f14, $f26		# ctemp1 = da * atemp1
	LD	$f14,  4*SIZE($20)

	ADD	$f19, $f27, $f1
	LD	$f19,  9*SIZE($24)
	MUL	$f30, $f15, $f27
	LD	$f15,  5*SIZE($20)

	ADD	$f20, $f28, $f2
	LD	$f20, 10*SIZE($24)
	MUL	$f30, $f16, $f28
	LD	$f16,  6*SIZE($20)

	ADD	$f21, $f29, $f3
	LD	$f21, 11*SIZE($24)
	MUL	$f30, $f17, $f29
	LD	$f17, 7*SIZE($20)

	ST	$f0,   0*SIZE($24)
	ADD	$f22, $f26, $f0
	ST	$f1,   1*SIZE($24)
	ADD	$f23, $f27, $f1

	ST	$f2,   2*SIZE($24)
	ADD	$f24, $f28, $f2
	ST	$f3,   3*SIZE($24)
	ADD	$f25, $f29, $f3

	LD	$f22, 12*SIZE($24)
	LD	$f23, 13*SIZE($24)
	LD	$f24, 14*SIZE($24)
	LD	$f25, 15*SIZE($24)

	ST	$f0,  4*SIZE($24)
	ST	$f1,  5*SIZE($24)
	ST	$f2,  6*SIZE($24)
	ST	$f3,  7*SIZE($24)

	subq	$1,  1, $1
	addq	$24, 8*SIZE, $24
	addq	$20, 8*SIZE, $20
	bgt	$1, $Loop
	.align 4

$LoopEnd:
	MUL	$f30, $f10, $f26		# ctemp1 = da * atemp1
	MUL	$f30, $f11, $f27
	MUL	$f30, $f12, $f28
	MUL	$f30, $f13, $f29

	ADD	$f18, $f26, $f0
	MUL	$f30, $f14, $f26		# ctemp1 = da * atemp1
	ADD	$f19, $f27, $f1
	MUL	$f30, $f15, $f27

	ADD	$f20, $f28, $f2
	MUL	$f30, $f16, $f28
	ADD	$f21, $f29, $f3
	MUL	$f30, $f17, $f29

	ST	$f0,   0*SIZE($24)
	ADD	$f22, $f26, $f0
	ST	$f1,   1*SIZE($24)
	ADD	$f23, $f27, $f1

	ST	$f2,   2*SIZE($24)
	ADD	$f24, $f28, $f2
	ST	$f3,   3*SIZE($24)
	ADD	$f25, $f29, $f3

	ST	$f0,   4*SIZE($24)
	ST	$f1,   5*SIZE($24)
	ST	$f2,   6*SIZE($24)
	ST	$f3,   7*SIZE($24)
	addq	$24, 8*SIZE, $24
	.align 4

$Remain:
	ble	$2, $End
	.align 4

$RemainLoop:
	LD	$f10,  0*SIZE($20)
	LD	$f11,  0*SIZE($24)
	addq	$20, SIZE, $20
	addq	$24, SIZE, $24

	MUL	$f30, $f10, $f12
	subq	$2,  1,  $2
	ADD	$f11, $f12, $f13
	ST	$f13,  -1*SIZE($24)
	bgt	$2,  $RemainLoop
	.align 4

$End:
	ldt	$f2,   0($sp)
	ldt	$f3,   8($sp)
	lda	$sp,  16($sp)
	ret
	.align 4

$Sub:
	SXSUBL	$16,  SIZE, $22
	subq	$1,  1, $4
	ble	$1, $SubRemain
	.align 4

	LD	$f10,  0($20)
	SXADDQ	$21, $20, $20

	LD	$f11,  0($20)
	SXADDQ	$21, $20, $20
	LD	$f12,  0($20)
	SXADDQ	$21, $20, $20

	LD	$f13,  0($20)
	SXADDQ	$21, $20, $20
	LD	$f18,  0($24)
	SXADDQ	$23, $24, $22

	LD	$f19,  0($22)
	SXADDQ	$23, $22, $22
	LD	$f20,  0($22)
	SXADDQ	$23, $22, $22

	LD	$f21,  0($22)
	SXADDQ	$23, $22, $22
	LD	$f14,  0($20)
	SXADDQ	$21, $20, $20

	LD	$f15,  0($20)
	SXADDQ	$21, $20, $20
	LD	$f16,  0($20)
	SXADDQ	$21, $20, $20

	LD	$f17,  0($20)
	SXADDQ	$21, $20, $20
	LD	$f22,  0($22)
	SXADDQ	$23, $22, $22

	LD	$f23,  0($22)
	SXADDQ	$23, $22, $22
	LD	$f24,  0($22)
	SXADDQ	$23, $22, $22

	LD	$f25,  0($22)
	SXADDQ	$23, $22, $22
	unop
	ble	$4,  $SubLoopEnd
	.align 4

$SubLoop:
	MUL	$f30, $f10, $f26		# ctemp1 = da * atemp1
	LD	$f10,  0($20)
	unop
	SXADDQ	$21, $20, $20

	MUL	$f30, $f11, $f27
	LD	$f11,  0($20)
	unop
	SXADDQ	$21, $20, $20

	MUL	$f30, $f12, $f28
	LD	$f12,  0($20)
	unop
	SXADDQ	$21, $20, $20

	MUL	$f30, $f13, $f29
	LD	$f13,  0($20)
	unop
	SXADDQ	$21, $20, $20

	ADD	$f18, $f26, $f0
	MUL	$f30, $f14, $f26		# ctemp1 = da * atemp1
	LD	$f14,  0($20)
	SXADDQ	$21, $20, $20

	ADD	$f19, $f27, $f1
	MUL	$f30, $f15, $f27
	LD	$f15,  0($20)
	SXADDQ	$21, $20, $20

	ADD	$f20, $f28, $f2
	MUL	$f30, $f16, $f28
	LD	$f16,  0($20)
	SXADDQ	$21, $20, $20

	ADD	$f21, $f29, $f3
	MUL	$f30, $f17, $f29
	LD	$f17,  0($20)
	SXADDQ	$21, $20, $20

	ST	$f0,   0($24)
	SXADDQ	$23, $24, $24
	ADD	$f22, $f26, $f0
	unop

	ST	$f1,   0($24)
	SXADDQ	$23, $24, $24
	ADD	$f23, $f27, $f1
	unop

	ST	$f2,   0($24)
	SXADDQ	$23, $24, $24
	ADD	$f24, $f28, $f2
	unop

	ST	$f3,   0($24)
	SXADDQ	$23, $24, $24
	ADD	$f25, $f29, $f3
	unop

	LD	$f18,  0($22)
	SXADDQ	$23, $22, $22
	LD	$f19,  0($22)
	SXADDQ	$23, $22, $22

	LD	$f20,  0($22)
	SXADDQ	$23, $22, $22
	LD	$f21,  0($22)
	SXADDQ	$23, $22, $22

	LD	$f22,  0($22)
	SXADDQ	$23, $22, $22
	LD	$f23,  0($22)
	SXADDQ	$23, $22, $22

	LD	$f24,  0($22)
	SXADDQ	$23, $22, $22
	LD	$f25,  0($22)
	SXADDQ	$23, $22, $22

	ST	$f0,  0($24)
	SXADDQ	$23, $24, $24
	ST	$f1,  0($24)
	SXADDQ	$23, $24, $24
	ST	$f2,  0($24)
	SXADDQ	$23, $24, $24
	ST	$f3,  0($24)
	SXADDQ	$23, $24, $24

	subq	$4,   1,  $4
	bgt	$4, $SubLoop
	.align 4

$SubLoopEnd:
	MUL	$f30, $f10, $f26		# ctemp1 = da * atemp1
	MUL	$f30, $f11, $f27
	MUL	$f30, $f12, $f28
	MUL	$f30, $f13, $f29

	ADD	$f18, $f26, $f0
	MUL	$f30, $f14, $f26		# ctemp1 = da * atemp1
	ADD	$f19, $f27, $f1
	MUL	$f30, $f15, $f27

	ADD	$f20, $f28, $f2
	MUL	$f30, $f16, $f28
	ADD	$f21, $f29, $f3
	MUL	$f30, $f17, $f29

	ST	$f0,   0($24)
	SXADDQ	$23, $24, $24
	ST	$f1,   0($24)
	SXADDQ	$23, $24, $24

	ST	$f2,   0($24)
	SXADDQ	$23, $24, $24
	ST	$f3,   0($24)
	SXADDQ	$23, $24, $24

	ADD	$f22, $f26, $f0
	ADD	$f23, $f27, $f1
	ADD	$f24, $f28, $f2
	ADD	$f25, $f29, $f3

	ST	$f0,   0($24)
	SXADDQ	$23, $24, $24
	ST	$f1,   0($24)
	SXADDQ	$23, $24, $24

	ST	$f2,   0($24)
	SXADDQ	$23, $24, $24
	ST	$f3,   0($24)
	SXADDQ	$23, $24, $24
	.align 4

$SubRemain:
	ble	$2, $SubEnd
	.align 4

$SubRemainLoop:
	LD	$f10,  0($20)
	LD	$f11,  0($24)
	SXADDQ	$21, $20, $20

	MUL	$f30, $f10, $f12
	subq	$2,  1,  $2
	ADD	$f11, $f12, $f13
	ST	$f13,  0($24)
	SXADDQ	$23, $24, $24

	bgt	$2,  $SubRemainLoop
	.align 4

$SubEnd:
	ldt	$f2,   0($sp)
	ldt	$f3,   8($sp)
	lda	$sp,  16($sp)
	ret
	EPILOGUE
