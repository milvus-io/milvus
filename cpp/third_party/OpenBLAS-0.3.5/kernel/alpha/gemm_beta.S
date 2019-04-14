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

	.set noat
	.set noreorder
.text
	.align 5
	.globl	CNAME
	.ent	CNAME
CNAME:
	.frame	$sp, 0, $26, 0

#ifdef PROFILE
	ldgp	$gp, 0($27)
	lda	$28, _mcount
	jsr	$28, ($28), _mcount
#endif

	ldq	$18,   16($sp)
	ble	$16, $End
	ldl	$19,   24($sp)
	ble	$17, $End
#ifndef PROFILE
	.prologue 0
#else
	.prologue 1
#endif

	fbeq	$f19, $BETA_EQ_ZERO		# if (beta == ZERO)
	.align 4

$BETA_NE_ZERO:
	sra	$16, 3,  $2		# i = (m >> 3)
 	mov	$18, $1			# c_offset = c
	lda	$17,  -1($17)		# j --
	ble	$2,$L52
	.align 4

$L51:
	lds	$f31,  64($1)
	lda	$2,    -1($2)

	LD	$f14, 0*SIZE($1)
	LD	$f15, 1*SIZE($1)
	LD	$f16, 2*SIZE($1)
	LD	$f17, 3*SIZE($1)
	LD	$f18, 4*SIZE($1)
	LD	$f11, 5*SIZE($1)
	LD	$f21, 6*SIZE($1)
	LD	$f22, 7*SIZE($1)

	MUL	$f19, $f14, $f23
	MUL	$f19, $f15, $f24
	MUL	$f19, $f16, $f25
	MUL	$f19, $f17, $f26
	MUL	$f19, $f18, $f27
	MUL	$f19, $f11, $f28
	MUL	$f19, $f21, $f29
	MUL	$f19, $f22, $f30

	ST	$f23, 0*SIZE($1)
	ST	$f24, 1*SIZE($1)
	ST	$f25, 2*SIZE($1)
	ST	$f26, 3*SIZE($1)
	ST	$f27, 4*SIZE($1)
	ST	$f28, 5*SIZE($1)
	ST	$f29, 6*SIZE($1)
	ST	$f30, 7*SIZE($1)

	lda	$1,8*SIZE($1)
	bgt	$2,$L51
	.align 4

$L52:
	and	$16, 7,  $2
	ble	$2,$L54
	.align	4

$L53:
	LD	$f12,   0($1)
	lda	$2,    -1($2)
	MUL	$f19, $f12, $f23
	ST	$f23,   0($1)
	lda	$1,  SIZE($1)
	bgt	$2,$L53
	.align 4

$L54:
	SXADDQ	$19, $18, $18		# c += ldc
	bgt	$17,$BETA_NE_ZERO
	clr	$0
	ret
	.align 4

$BETA_EQ_ZERO:
	sra	$16, 3,  $2		# i = (m >> 3)
	lda	$4,  8*SIZE($18)
 	mov	$18, $1			# c_offset = c
	lda	$17,  -1($17)		# j --
	ble	$2,$L42
	.align 4

$L41:
	ST	$f31, 0*SIZE($1)
	ST	$f31, 1*SIZE($1)
	ST	$f31, 2*SIZE($1)
	ST	$f31, 3*SIZE($1)
	ST	$f31, 4*SIZE($1)
	ST	$f31, 5*SIZE($1)
	ST	$f31, 6*SIZE($1)
	ST	$f31, 7*SIZE($1)
	lda	$2,    -1($2)

	lda	$4,   8*SIZE($4)
	lda	$1,  8*SIZE($1)
	bgt	$2,$L41
	.align 4

$L42:
	and	$16, 7,  $2
	ble	$2,$L44
	.align	4

$L43:
	lda	$2,    -1($2)
	ST	$f31,   0($1)
	lda	$1,  SIZE($1)
	bgt	$2, $L43
	.align 4

$L44:
	SXADDQ	$19, $18, $18		# c += ldc
	bgt	$17,$BETA_EQ_ZERO
	clr	$0
	.align 4

$End:
	ret
	.ident	VERSION
	.end CNAME
