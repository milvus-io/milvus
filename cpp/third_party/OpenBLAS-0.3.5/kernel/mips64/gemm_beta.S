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

#define M	$4
#define	N	$5
#define C	$6
#define LDC	$7

#define I	$2
#define J	$3

#define CO1	$8

#define a1	$f0
#define a2	$f1
#define a3	$f2
#define a4	$f3
#define b1	$f4
#define b2	$f5
#define b3	$f6
#define b4	$f7

#define FZERO	$f8
#define ALPHA	$f15

	PROLOGUE

	LDARG	C,     0($sp)
	MTC	$0,  FZERO
	LDARG	LDC,   8($sp)

	dsll	LDC, LDC, BASE_SHIFT

	move	J, N
	blez	J, .L999
	nop
	.align 3

.L10:
	move	CO1, C
	dsra	I,  M, 3

	blez	I, .L15
	daddu	C,   C, LDC

	LD	a1,  0 * SIZE(CO1)
	LD	a2,  1 * SIZE(CO1)
	LD	a3,  2 * SIZE(CO1)
	LD	a4,  3 * SIZE(CO1)

	MUL	b1, ALPHA, a1
	LD	a1,  4 * SIZE(CO1)

	MUL	b2, ALPHA, a2
	daddiu	I, I, -1

	blez	I, .L13
	LD	a2,  5 * SIZE(CO1)
	.align 3

.L12:
	MUL	b3, ALPHA, a3
	LD	a3,  6 * SIZE(CO1)

	ST	b1,  0 * SIZE(CO1)

	MUL	b4, ALPHA, a4
	LD	a4,  7 * SIZE(CO1)

	ST	b2,  1 * SIZE(CO1)

	MUL	b1, ALPHA, a1
	LD	a1,  8 * SIZE(CO1)

	ST	b3,  2 * SIZE(CO1)

	MUL	b2, ALPHA, a2
	LD	a2,  9 * SIZE(CO1)

	ST	b4,  3 * SIZE(CO1)

	MUL	b3, ALPHA, a3
	LD	a3, 10 * SIZE(CO1)

	ST	b1,  4 * SIZE(CO1)

	MUL	b4, ALPHA, a4
	LD	a4, 11 * SIZE(CO1)

	ST	b2,  5 * SIZE(CO1)

	MUL	b1, ALPHA, a1
	LD	a1, 12 * SIZE(CO1)

	ST	b3,  6 * SIZE(CO1)

	MUL	b2, ALPHA, a2
	LD	a2, 13 * SIZE(CO1)

	ST	b4,  7 * SIZE(CO1)
	daddiu	I, I, -1

	bgtz	I, .L12
	daddiu	CO1, CO1, 8 * SIZE
	.align 3

.L13:
	MUL	b3, ALPHA, a3
	LD	a3,  6 * SIZE(CO1)

	ST	b1,  0 * SIZE(CO1)

	MUL	b4, ALPHA, a4
	LD	a4,  7 * SIZE(CO1)

	ST	b2,  1 * SIZE(CO1)

	MUL	b1, ALPHA, a1

	ST	b3,  2 * SIZE(CO1)

	MUL	b2, ALPHA, a2

	ST	b4,  3 * SIZE(CO1)

	MUL	b3, ALPHA, a3

	ST	b1,  4 * SIZE(CO1)

	MUL	b4, ALPHA, a4

	ST	b2,  5 * SIZE(CO1)
	ST	b3,  6 * SIZE(CO1)
	ST	b4,  7 * SIZE(CO1)

	daddiu	CO1, CO1, 8 * SIZE
	.align 3

.L15:
	andi	I,  M, 7
	daddiu	J, J, -1

	blez	I, .L18
	NOP
	.align	3

.L16:
	LD	a1,  0 * SIZE(CO1)
	daddiu	I, I, -1

	MUL	b1, ALPHA, a1
	daddiu	CO1, CO1, 1 * SIZE

	bgtz	I, .L16
	ST	b1, -1 * SIZE(CO1)
	.align 3

.L18:
	bgtz	J, .L10
	NOP
	.align 3

.L999:
	j	$31
	NOP

	EPILOGUE
