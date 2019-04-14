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

	.set noat
	.set noreorder

#define ASSEMBLER

#include "common.h"
#include "version.h"

	.globl NAME
	.ent   NAME

NAME:
#ifdef PROFILE
	ldgp	$gp, 0($27)
	lda	$28, _mcount
	jsr	$28, ($28), _mcount
#endif

#ifndef C_INTERFACE
	ldl	$16,   0($16)		# n
	mov	$18,  $20		# Store Address
	ldl	$19,   0($19)		# incx
	nop

	LD	$f1,   0($17)		# alpha
#else
	mov	$18,  $20		# Store Address
	fmov	$f17, $f1		# alpha
#endif

#ifndef PROFILE
	.prologue 0
#else
	.prologue 1
#endif

	sra	$16,  1, $21		# 4-unrolling
	ble	$16,  $End

	lda	$23,  -1($19)
	ble	$19,  $End

	bgt	$23,  $INC_NOT_1
	.align 4

	ble	$21,  $Sub
	lda	$21,  -1($21)
	LD	$f10,   0*SIZE($18)
	LD	$f11,   1*SIZE($18)

	LD	$f12,   2*SIZE($18)
	LD	$f13,   3*SIZE($18)
	lda	$18,    4*SIZE($18)
	ble	$21,  $MainRemain
	.align 4

$MainLoop:
	MUL	$f10, $f1,  $f20
	LD	$f10,   0*SIZE($18)
	MUL	$f11, $f1,  $f21
	LD	$f11,   1*SIZE($18)

	MUL	$f12, $f1,  $f22
	LD	$f12,   2*SIZE($18)
	MUL	$f13, $f1,  $f23
	LD	$f13,   3*SIZE($18)

	lda	$18,    4*SIZE($18)
	lda	$21,   -1($21)

	ST	$f20,   0*SIZE($20)
	ST	$f21,   1*SIZE($20)
	ST	$f22,   2*SIZE($20)
	ST	$f23,   3*SIZE($20)
	lda	$20,    4*SIZE($20)

	bgt	$21, $MainLoop
	.align 4

$MainRemain:
	MUL	$f10, $f1,  $f20
	MUL	$f11, $f1,  $f21
	MUL	$f12, $f1,  $f22
	MUL	$f13, $f1,  $f23

	ST	$f20,   0*SIZE($20)
	ST	$f21,   1*SIZE($20)
	ST	$f22,   2*SIZE($20)
	ST	$f23,   3*SIZE($20)
	lda	$20,    4*SIZE($20)
	.align 4

$Sub:
	blbc	$16, $End
	LD	$f10,      0*SIZE($18)
	LD	$f11,      1*SIZE($18)
	MUL	$f10, $f1,  $f20
	MUL	$f11, $f1,  $f21
	ST	$f20,      0*SIZE($20)
	ST	$f21,      1*SIZE($20)
	.align 4

$End:
	ret
	.align 4

$INC_NOT_1:
	addl	$19, $19, $19
	ble	$21,  $INC_Sub
	lda	$21,  -1($21)

	LD	$f10,   0*SIZE($18)
	LD	$f11,   1*SIZE($18)
	SXADDQ	$19, $18, $18

	LD	$f12,   0*SIZE($18)
	LD	$f13,   1*SIZE($18)
	SXADDQ	$19, $18, $18
	ble	$21,  $INC_MainRemain
	.align 4

$INC_MainLoop:
	MUL	$f10, $f1,  $f20
	LD	$f10,   0*SIZE($18)
	MUL	$f11, $f1,  $f21
	LD	$f11,   1*SIZE($18)

	SXADDQ	$19, $18, $18

	MUL	$f12, $f1,  $f22
	LD	$f12,   0*SIZE($18)
	MUL	$f13, $f1,  $f23
	LD	$f13,   1*SIZE($18)

	SXADDQ	$19, $18, $18

	ST	$f20,   0*SIZE($20)
	lda	$21,   -1($21)
	ST	$f21,   1*SIZE($20)
	SXADDQ	$19, $20, $20

	ST	$f22,   0*SIZE($20)
	ST	$f23,   1*SIZE($20)
	SXADDQ	$19, $20, $20
	unop
	bgt	$21, $INC_MainLoop
	.align 4

$INC_MainRemain:
	MUL	$f10, $f1,  $f20
	MUL	$f11, $f1,  $f21
	MUL	$f12, $f1,  $f22
	MUL	$f13, $f1,  $f23

	ST	$f20,   0*SIZE($20)
	ST	$f21,   1*SIZE($20)
	SXADDQ	$19, $20, $20

	ST	$f22,   0*SIZE($20)
	ST	$f23,   1*SIZE($20)
	SXADDQ	$19, $20, $20
	.align 4

$INC_Sub:
	blbc	$16, $INC_End

	LD	$f10,   0*SIZE($18)
	LD	$f11,   1*SIZE($18)
	MUL	$f10, $f1,  $f20
	MUL	$f11, $f1,  $f21

	ST	$f20,   0*SIZE($20)
	ST	$f21,   1*SIZE($20)
	.align 4

$INC_End:
	ret
	.end NAME
	.ident VERSION
