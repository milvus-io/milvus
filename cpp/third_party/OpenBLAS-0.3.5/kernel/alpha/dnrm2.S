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

#define PREFETCH_SIZE 80

#define N	$16
#define X	$17
#define INCX	$18
#define XX	$19

#define I	$0

#define a0	$f0
#define a1	$f1
#define a2	$f10
#define a3	$f11
#define t0	$f12
#define t1	$f13
#define t2	$f14
#define t3	$f15

#define x0	$f16
#define x1	$f17
#define x2	$f18
#define x3	$f19
#define x4	$f20
#define x5	$f21
#define x6	$f22
#define x7	$f23

	PROLOGUE

#if defined(EV4) || defined(EV5)
        .frame $30,16,$26,0
        .mask 0x4000000,-16
        ldah	$29,    0($27)		!gpdisp!1
        lda	$29,    0($29)		!gpdisp!1

	lda	$sp,  -16($sp)
	ldq	$27, sqrt($29)		!literal!2
	stq	$26,    0($sp)

	PROFCODE
	.prologue 1
#else
	PROFCODE
#endif

	fclr	a0
	SXADDQ	INCX, 0, INCX
	fclr	a1
	ble	N, $L999

	fclr	a2
	cmpeq	INCX, SIZE, $0
	fclr	a3
	beq	$0,   $L20

	fclr	t0
	sra	N, 4, I
	fclr	t1
	ble	I, $L15

	fclr	t2
	LD	x0,  0 * SIZE(X)
	fclr	t3
	LD	x1,  1 * SIZE(X)

	LD	x2,  2 * SIZE(X)
	LD	x3,  3 * SIZE(X)
	LD	x4,  4 * SIZE(X)
	LD	x5,  5 * SIZE(X)
	LD	x6,  6 * SIZE(X)
	LD	x7,  7 * SIZE(X)

	lda	I, -1(I)
	ble	I, $L12
	.align 4

$L11:
	addt	a0, t0, a0
	ldl	$31, (PREFETCH_SIZE) * SIZE(X)
	mult	x0, x0, t0
	LD	x0,  8 * SIZE(X)

	addt	a1, t1, a1
	mov	X, XX
	mult	x1, x1, t1
	LD	x1,  9 * SIZE(X)

	addt	a2, t2, a2
	unop
	mult	x2, x2, t2
	LD	x2, 10 * SIZE(X)

	addt	a3, t3, a3
	unop
	mult	x3, x3, t3
	LD	x3, 11 * SIZE(X)

	addt	a0, t0, a0
	unop
	mult	x4, x4, t0
	LD	x4, 12 * SIZE(X)

	addt	a1, t1, a1
	unop
	mult	x5, x5, t1
	LD	x5, 13 * SIZE(X)

	addt	a2, t2, a2
	unop
	mult	x6, x6, t2
	LD	x6, 14 * SIZE(X)

	addt	a3, t3, a3
	unop
	mult	x7, x7, t3
	LD	x7, 15 * SIZE(X)

	addt	a0, t0, a0
	unop
	mult	x0, x0, t0
	LD	x0, 16 * SIZE(X)

	addt	a1, t1, a1
	lda	X,  16 * SIZE(X)
	mult	x1, x1, t1
	LD	x1, 17 * SIZE(XX)

	addt	a2, t2, a2
	unop
	mult	x2, x2, t2
	LD	x2, 18 * SIZE(XX)

	addt	a3, t3, a3
	unop
	mult	x3, x3, t3
	LD	x3, 19 * SIZE(XX)

	addt	a0, t0, a0
	unop
	mult	x4, x4, t0
	LD	x4, 20 * SIZE(XX)

	addt	a1, t1, a1
	lda	I, -1(I)
	mult	x5, x5, t1
	LD	x5, 21 * SIZE(XX)

	addt	a2, t2, a2
	unop
	mult	x6, x6, t2
	LD	x6, 22 * SIZE(XX)

	addt	a3, t3, a3
	mult	x7, x7, t3
	LD	x7, 23 * SIZE(XX)
	bgt	I, $L11
	.align 4

$L12:
	addt	a0, t0, a0
	mov	X, XX
	mult	x0, x0, t0
	LD	x0,  8 * SIZE(X)

	addt	a1, t1, a1
	unop
	mult	x1, x1, t1
	LD	x1,  9 * SIZE(X)

	addt	a2, t2, a2
	unop
	mult	x2, x2, t2
	LD	x2, 10 * SIZE(X)

	addt	a3, t3, a3
	unop
	mult	x3, x3, t3
	LD	x3, 11 * SIZE(X)

	addt	a0, t0, a0
	unop
	mult	x4, x4, t0
	LD	x4, 12 * SIZE(XX)

	addt	a1, t1, a1
	unop
	mult	x5, x5, t1
	LD	x5, 13 * SIZE(XX)

	addt	a2, t2, a2
	unop
	mult	x6, x6, t2
	LD	x6, 14 * SIZE(XX)

	addt	a3, t3, a3
	lda	X,  16 * SIZE(X)
	mult	x7, x7, t3
	LD	x7, 15 * SIZE(XX)

	addt	a0, t0, a0
	mult	x0, x0, t0
	addt	a1, t1, a1
	mult	x1, x1, t1

	addt	a2, t2, a2
	mult	x2, x2, t2
	addt	a3, t3, a3
	mult	x3, x3, t3

	addt	a0, t0, a0
	mult	x4, x4, t0
	addt	a1, t1, a1
	mult	x5, x5, t1

	addt	a2, t2, a2
	mult	x6, x6, t2
	addt	a3, t3, a3
	mult	x7, x7, t3

	addt	a1, t1, a1
	addt	a2, t2, a2
	addt	a3, t3, a3
	.align 4

$L15:
	and	N, 15, I
	ble	I, $L998
	.align 4

$L16:
	LD	x0,  0 * SIZE(X)
	lda	X,   1 * SIZE(X)

	addt	a0, t0, a0
	mult	x0, x0, t0

	lda	I, -1(I)
	bgt	I, $L16
	bsr	$31, $L998
	.align 4

$L20:
	fclr	t0
	sra	N, 3, I
	fclr	t1
	ble	I, $L25

	fclr	t2
	fclr	t3

	LD	x0,  0 * SIZE(X)
	addq	X, INCX, X
	LD	x1,  0 * SIZE(X)
	addq	X, INCX, X
	LD	x2,  0 * SIZE(X)
	addq	X, INCX, X
	LD	x3,  0 * SIZE(X)
	addq	X, INCX, X

	LD	x4,  0 * SIZE(X)
	addq	X, INCX, X
	LD	x5,  0 * SIZE(X)
	addq	X, INCX, X
	LD	x6,  0 * SIZE(X)
	addq	X, INCX, X

	lda	I, -1(I)
	ble	I, $L22
	.align 4

$L21:
	addt	a0, t0, a0
	LD	x7,  0 * SIZE(X)
	mult	x0, x0, t0
	addq	X, INCX, X

	addt	a1, t1, a1
	LD	x0,  0 * SIZE(X)
	mult	x1, x1, t1
	addq	X, INCX, X

	addt	a2, t2, a2
	LD	x1,  0 * SIZE(X)
	mult	x2, x2, t2
	addq	X, INCX, X

	addt	a3, t3, a3
	LD	x2,  0 * SIZE(X)
	mult	x3, x3, t3
	addq	X, INCX, X

	addt	a0, t0, a0
	LD	x3,  0 * SIZE(X)
	mult	x4, x4, t0
	addq	X, INCX, X

	addt	a1, t1, a1
	LD	x4,  0 * SIZE(X)
	mult	x5, x5, t1
	addq	X, INCX, X

	addt	a2, t2, a2
	LD	x5,  0 * SIZE(X)
	mult	x6, x6, t2
	addq	X, INCX, X

	addt	a3, t3, a3
	LD	x6,  0 * SIZE(X)
	mult	x7, x7, t3
	addq	X, INCX, X

	lda	I, -1(I)
	bgt	I, $L21
	.align 4

$L22:
	addt	a0, t0, a0
	LD	x7,  0 * SIZE(X)
	mult	x0, x0, t0
	addq	X, INCX, X

	addt	a1, t1, a1
	unop
	mult	x1, x1, t1
	unop

	addt	a2, t2, a2
	mult	x2, x2, t2
	addt	a3, t3, a3
	mult	x3, x3, t3

	addt	a0, t0, a0
	mult	x4, x4, t0
	addt	a1, t1, a1
	mult	x5, x5, t1

	addt	a2, t2, a2
	mult	x6, x6, t2
	addt	a3, t3, a3
	mult	x7, x7, t3

	addt	a1, t1, a1
	addt	a2, t2, a2
	addt	a3, t3, a3
	.align 4

$L25:
	and	N, 7, I
	ble	I, $L998
	.align 4

$L26:
	LD	x0,  0 * SIZE(X)
	addq	X, INCX, X

	addt	a0, t0, a0
	mult	x0, x0, t0

	lda	I, -1(I)
	bgt	I, $L26
	.align 4


$L998:
	addt	a0, t0, a0

	addt	a0, a1, a0
	addt	a2, a3, a2

#if defined(EV4) || defined(EV5)
	addt	a0, a2, $f16
	jsr	$26, ($27), sqrt	!lituse_jsr!2

        ldah	$29,   0($26)		!gpdisp!3
        lda	$29,   0($29)		!gpdisp!3
#else
	addt	a0, a2, a0
	sqrtt	a0, a0
#endif
	.align 4

$L999:
#if defined(EV4) || defined(EV5)
	ldq	$26,   0($sp)
	lda	$sp,  16($sp)
#endif
	ret
	EPILOGUE
