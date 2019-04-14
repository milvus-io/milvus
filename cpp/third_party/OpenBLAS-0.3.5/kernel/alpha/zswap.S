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

	PROLOGUE
	PROFCODE
	.frame	$sp, 0, $26, 0

	mov	$21, $17
	ldl	$18,   0($sp)
	ldq	$19,   8($sp)
	ldl	$20,  16($sp)
#ifndef PROFILE
	.prologue 0
#else
	.prologue 1
#endif

	ble	$16,  $SubEnd		# if n <= 0 goto $End

	cmpeq	$18, 1, $1
	addq	$18, $18, $18
	cmpeq	$20, 1, $2
	addq	$20, $20, $20

	sra	$16, 2, $21
	and	$1,  $2, $1
	and	$16, 3, $22
	beq	$1,  $Sub

	ble	$21, $MainRemain
	.align 4

$MainLoop:
	LD	$f10,   0*SIZE($19)
	LD	$f11,   1*SIZE($19)
	LD	$f12,   2*SIZE($19)
	LD	$f13,   3*SIZE($19)
	LD	$f14,   4*SIZE($19)
	LD	$f15,   5*SIZE($19)
	LD	$f16,   6*SIZE($19)
	LD	$f17,   7*SIZE($19)

	LD	$f20,   0*SIZE($17)
	LD	$f21,   1*SIZE($17)
	LD	$f22,   2*SIZE($17)
	LD	$f23,   3*SIZE($17)
	LD	$f24,   4*SIZE($17)
	LD	$f25,   5*SIZE($17)
	LD	$f26,   6*SIZE($17)
	LD	$f27,   7*SIZE($17)

	lds	$f31,  16*SIZE($17)
	unop
	lds	$f31,  16*SIZE($19)
	subl	$21, 1, $21

	ST	$f10,   0*SIZE($17)
	ST	$f11,   1*SIZE($17)
	ST	$f12,   2*SIZE($17)
	ST	$f13,   3*SIZE($17)
	ST	$f14,   4*SIZE($17)
	ST	$f15,   5*SIZE($17)
	ST	$f16,   6*SIZE($17)
	ST	$f17,   7*SIZE($17)

	ST	$f20,   0*SIZE($19)
	ST	$f21,   1*SIZE($19)
	ST	$f22,   2*SIZE($19)
	ST	$f23,   3*SIZE($19)
	ST	$f24,   4*SIZE($19)
	ST	$f25,   5*SIZE($19)
	ST	$f26,   6*SIZE($19)
	ST	$f27,   7*SIZE($19)

	lda	$17,   8*SIZE($17)
	lda	$19,   8*SIZE($19)
	bgt	$21, $MainLoop
	.align 4

$MainRemain:
	ble	$22, $MainEnd
	.align 4

$MainRemainLoop:
	LD	$f10,   0*SIZE($19)
	LD	$f11,   1*SIZE($19)
	LD	$f20,   0*SIZE($17)
	LD	$f21,   1*SIZE($17)

	lda	$17,    2*SIZE($17)
	lda	$19,    2*SIZE($19)
	subl	$22, 1, $22
	ST	$f10,  -2*SIZE($17)
	ST	$f11,  -1*SIZE($17)
	ST	$f20,  -2*SIZE($19)
	ST	$f21,  -1*SIZE($19)
	bgt	$22, $MainRemainLoop
	.align 4

$MainEnd:
	clr	$0
	ret
	.align 4

$Sub:
	mov	$17, $23
	mov	$19, $24
	ble	$21, $SubRemain
	.align 4

$SubLoop:
	LD	$f10,   0*SIZE($19)
	LD	$f11,   1*SIZE($19)
	SXADDQ	$20, $19, $19

	LD	$f12,   0*SIZE($19)
	LD	$f13,   1*SIZE($19)
	SXADDQ	$20, $19, $19

	LD	$f14,   0*SIZE($19)
	LD	$f15,   1*SIZE($19)
	SXADDQ	$20, $19, $19

	LD	$f16,   0*SIZE($19)
	LD	$f17,   1*SIZE($19)
	SXADDQ	$20, $19, $19

	LD	$f20,   0*SIZE($17)
	LD	$f21,   1*SIZE($17)
	SXADDQ	$18, $17, $17

	LD	$f22,   0*SIZE($17)
	LD	$f23,   1*SIZE($17)
	SXADDQ	$18, $17, $17

	LD	$f24,   0*SIZE($17)
	LD	$f25,   1*SIZE($17)
	SXADDQ	$18, $17, $17

	LD	$f26,   0*SIZE($17)
	LD	$f27,   1*SIZE($17)
	SXADDQ	$18, $17, $17

	ST	$f10,   0*SIZE($23)
	ST	$f11,   1*SIZE($23)
	SXADDQ	$18, $23, $23

	ST	$f12,   0*SIZE($23)
	ST	$f13,   1*SIZE($23)
	SXADDQ	$18, $23, $23

	ST	$f14,   0*SIZE($23)
	ST	$f15,   1*SIZE($23)
	SXADDQ	$18, $23, $23

	ST	$f16,   0*SIZE($23)
	ST	$f17,   1*SIZE($23)
	SXADDQ	$18, $23, $23

	ST	$f20,   0*SIZE($24)
	ST	$f21,   1*SIZE($24)
	SXADDQ	$20, $24, $24

	ST	$f22,   0*SIZE($24)
	ST	$f23,   1*SIZE($24)
	SXADDQ	$20, $24, $24

	ST	$f24,   0*SIZE($24)
	ST	$f25,   1*SIZE($24)
	SXADDQ	$20, $24, $24

	ST	$f26,   0*SIZE($24)
	ST	$f27,   1*SIZE($24)
	SXADDQ	$20, $24, $24

	subl	$21, 1, $21
	bgt	$21, $SubLoop
	.align 4

$SubRemain:
	ble	$22, $SubEnd
	.align 4

$SubRemainLoop:
	LD	$f10,   0*SIZE($19)
	LD	$f11,   1*SIZE($19)
	LD	$f20,   0*SIZE($17)
	LD	$f21,   1*SIZE($17)

	subl	$22, 1, $22

	ST	$f10,   0*SIZE($17)
	ST	$f11,   1*SIZE($17)
	ST	$f20,   0*SIZE($19)
	ST	$f21,   1*SIZE($19)

	SXADDQ	$18, $17, $17
	SXADDQ	$20, $19, $19
	bgt	$22, $SubRemainLoop
	.align 4

$SubEnd:
	clr	$0
	ret
	EPILOGUE
