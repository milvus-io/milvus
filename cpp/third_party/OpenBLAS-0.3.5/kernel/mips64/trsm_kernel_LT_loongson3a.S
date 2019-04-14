#define REALNAME ASMNAME

#define ASSEMBLER
#include "common.h"


#define M	$4
#define	N	$5
#define	K	$6
#define A	$8
#define B	$9
#define C	$10
#define LDC	$11

#define AO	$12
#define BO	$13

#define I	$2
#define J	$3
#define L	$7

#define CO1	$14
#define CO2	$15
#define CO3	$16
#define CO4	$17

#define OFFSET	$22
#define KK	$23
#define TEMP	$24
#define AORIG	$25

#define a1	$f0
#define a2	$f1
#define a3	$f2
#define a4	$f3
#define a5	$f4
#define a6	$f5
#define a7	$f6
#define	a8	$f7

#define b1	$f8
#define b2	$f9
#define b3	$f10
#define b4	$f11
#define b5	$f12
#define b6	$f13
#define b7	$f14
#define b8	$f15

#define t11	$f16
#define t21	$f17
#define t31	$f18
#define t41	$f19

#define t12	$f20
#define t22	$f21
#define t32	$f22
#define t42	$f23

#define t13	$f24
#define t23	$f25
#define t33	$f26
#define t43	$f27

#define t14	$f28
#define t24	$f29
#define t34	$f30
#define t44	$f31

#define ALPHA	$f15

	PROLOGUE

	daddiu	$sp, $sp, -144

	SDARG	$16,   0($sp)
	SDARG	$17,   8($sp)
	SDARG	$18,  16($sp)
	SDARG	$19,  24($sp)
	SDARG	$20,  32($sp)
	SDARG	$21,  40($sp)
	sdc1	$f24, 48($sp)
	sdc1	$f25, 56($sp)
	sdc1	$f26, 64($sp)
	sdc1	$f27, 72($sp)
	sdc1	$f28, 80($sp)

	SDARG	$22,  88($sp)
	SDARG	$23,  96($sp)
	SDARG	$24, 104($sp)
	SDARG	$25, 112($sp)

#ifndef __64BIT__
	sdc1	$f20,112($sp)
	sdc1	$f21,120($sp)
	sdc1	$f22,128($sp)
	sdc1	$f23,136($sp)
#endif
											#	LT compute from left to right, top to bottom
	LDARG	OFFSET, 144($sp)
	dsll	LDC, LDC, BASE_SHIFT			#	ldc

	dsra	J,  N, 2						#	j = nc/4
	blez	J, .L30
	nop

.L10:										#	nr=4
	daddiu	J, J, -1
	move	CO1, C
	daddu	CO2, C,   LDC
	daddu	CO3, CO2, LDC
	daddu	CO4, CO3, LDC

	MTC	$0,  t11							#	clear result registers
	MOV	t21, t11
	MOV	t31, t11
	MOV	t41, t11
	MOV	t12, t11
	MOV	t22, t11
	MOV	t32, t11
	MOV	t42, t11

	dsra	I,  M, 2						#	i = mc/4
	move	KK, OFFSET						#	kk is the length of the rectangular data part of panel Ai
	move	AO, A							#	reset A
	daddu	C,  CO4, LDC					#	fixed pointer C, the write back address
	blez	I, .L20
	nop


.L11:										#	mr=4
	LD	a1,  0 * SIZE(AO)					#	this part compute the rectangular data part of Ai
	LD	a2,  1 * SIZE(AO)					#	mr*KK with nr*KK
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)					#	get 4a

	LD	b1,  0 * SIZE(B)					#	get 4b
	LD	b2,  1 * SIZE(B)
	LD	b3,  2 * SIZE(B)
	LD	b4,  3 * SIZE(B)

	MOV	t13, t11							#	clear result registers
	MOV	t23, t11
	MOV	t33, t11
	MOV	t43, t11
	MOV	t14, t11
	MOV	t24, t11
	MOV	t34, t11
	MOV	t44, t11

	dsra	L,  KK, 2						#	L = kk/4
	blez	L, .L15
	move	BO,  B							#


	.align	3
.L12:
	LD	a5,  4 * SIZE(AO)
	LD	a6,  5 * SIZE(AO)
	LD	a7,  6 * SIZE(AO)
	LD	a8,  7 * SIZE(AO)

	LD	b5,  4 * SIZE(BO)
	LD	b6,  5 * SIZE(BO)
	LD	b7,  6 * SIZE(BO)
	LD	b8,  7 * SIZE(BO)

	MADD	t11, t11, a1, b1				#	1st compute
	MADD	t21, t21, a2, b1
	MADD	t31, t31, a3, b1
	MADD	t41, t41, a4, b1

	MADD	t12, t12, a1, b2
	MADD	t22, t22, a2, b2
	MADD	t32, t32, a3, b2
	MADD	t42, t42, a4, b2

	MADD	t13, t13, a1, b3
	MADD	t23, t23, a2, b3
	MADD	t33, t33, a3, b3
	MADD	t43, t43, a4, b3

	MADD	t14, t14, a1, b4
	MADD	t24, t24, a2, b4
	MADD	t34, t34, a3, b4
	MADD	t44, t44, a4, b4

	LD	a1,   8 * SIZE(AO)
	LD	a2,   9 * SIZE(AO)
	LD	a3,  10 * SIZE(AO)
	LD	a4,  11 * SIZE(AO)

	LD	b1,   8 * SIZE(BO)
	LD	b2,   9 * SIZE(BO)
	LD	b3,  10 * SIZE(BO)
	LD	b4,  11 * SIZE(BO)

	MADD	t11, t11, a5, b5				#	2ed compute
	MADD	t21, t21, a6, b5
	MADD	t31, t31, a7, b5
	MADD	t41, t41, a8, b5

	MADD	t12, t12, a5, b6
	MADD	t22, t22, a6, b6
	MADD	t32, t32, a7, b6
	MADD	t42, t42, a8, b6

	MADD	t13, t13, a5, b7
	MADD	t23, t23, a6, b7
	MADD	t33, t33, a7, b7
	MADD	t43, t43, a8, b7

	MADD	t14, t14, a5, b8
	MADD	t24, t24, a6, b8
	MADD	t34, t34, a7, b8
	MADD	t44, t44, a8, b8

	LD	a5,  12 * SIZE(AO)
	LD	a6,  13 * SIZE(AO)
	LD	a7,  14 * SIZE(AO)
	LD	a8,  15 * SIZE(AO)

	LD	b5,  12 * SIZE(BO)
	LD	b6,  13 * SIZE(BO)
	LD	b7,  14 * SIZE(BO)
	LD	b8,  15 * SIZE(BO)

	MADD	t11, t11, a1, b1				#	3rd compute
	MADD	t21, t21, a2, b1
	MADD	t31, t31, a3, b1
	MADD	t41, t41, a4, b1

	MADD	t12, t12, a1, b2
	MADD	t22, t22, a2, b2
	MADD	t32, t32, a3, b2
	MADD	t42, t42, a4, b2

	MADD	t13, t13, a1, b3
	MADD	t23, t23, a2, b3
	MADD	t33, t33, a3, b3
	MADD	t43, t43, a4, b3

	MADD	t14, t14, a1, b4
	MADD	t24, t24, a2, b4
	MADD	t34, t34, a3, b4
	MADD	t44, t44, a4, b4

	daddiu	AO, AO, 16 * SIZE				#	AO += 4mr*4kr
	daddiu	BO, BO, 16 * SIZE				#	BO += 4nr*4kr

	LD	a1,  0 * SIZE(AO)					#	next
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	MADD	t11, t11, a5, b5				#	4th compute
	MADD	t21, t21, a6, b5
	MADD	t31, t31, a7, b5
	MADD	t41, t41, a8, b5

	MADD	t12, t12, a5, b6
	MADD	t22, t22, a6, b6
	MADD	t32, t32, a7, b6
	MADD	t42, t42, a8, b6

	MADD	t13, t13, a5, b7
	MADD	t23, t23, a6, b7
	MADD	t33, t33, a7, b7
	MADD	t43, t43, a8, b7

	MADD	t14, t14, a5, b8
	MADD	t24, t24, a6, b8
	MADD	t34, t34, a7, b8
	MADD	t44, t44, a8, b8

	daddiu	L, L, -1
	bgtz	L, .L12
	nop


	.align 3
.L15:
	andi	L, KK,  3						#	the remainder part: KK-KK/4
	blez	L, .L18
	nop

	.align	3
.L16:
	MADD	t11, t11, a1, b1
	MADD	t21, t21, a2, b1
	MADD	t31, t31, a3, b1
	MADD	t41, t41, a4, b1

	MADD	t12, t12, a1, b2
	MADD	t22, t22, a2, b2
	MADD	t32, t32, a3, b2
	MADD	t42, t42, a4, b2

	MADD	t13, t13, a1, b3
	MADD	t23, t23, a2, b3
	MADD	t33, t33, a3, b3
	MADD	t43, t43, a4, b3

	MADD	t14, t14, a1, b4
	MADD	t24, t24, a2, b4
	MADD	t34, t34, a3, b4
	MADD	t44, t44, a4, b4

	daddiu	AO, AO,  4 * SIZE				#	AO += 4mr
	daddiu	BO, BO,  4 * SIZE				#	BO += 4nr

	LD	a1,  0 * SIZE(AO)					#	next
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L16
	nop


.L18:										#	deal with the triangular data part of panel Ai
	LD	b1,  0 * SIZE(BO)					#	triangular_part*X + rectangular_part = B
	LD	b2,  1 * SIZE(BO)					#	triangular_part*X = B - rectangular_part
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	SUB	t11, b1, t11
	SUB	t12, b2, t12
	SUB	t13, b3, t13
	SUB	t14, b4, t14

	LD	b5,  4 * SIZE(BO)					#	sb store in row major
	LD	b6,  5 * SIZE(BO)
	LD	b7,  6 * SIZE(BO)
	LD	b8,  7 * SIZE(BO)

	SUB	t21, b5, t21
	SUB	t22, b6, t22
	SUB	t23, b7, t23
	SUB	t24, b8, t24

	LD	b1,  8 * SIZE(BO)
	LD	b2,  9 * SIZE(BO)
	LD	b3, 10 * SIZE(BO)
	LD	b4, 11 * SIZE(BO)

	SUB	t31, b1, t31
	SUB	t32, b2, t32
	SUB	t33, b3, t33
	SUB	t34, b4, t34

	LD	b5, 12 * SIZE(BO)
	LD	b6, 13 * SIZE(BO)
	LD	b7, 14 * SIZE(BO)
	LD	b8, 15 * SIZE(BO)

	SUB	t41, b5, t41
	SUB	t42, b6, t42
	SUB	t43, b7, t43
	SUB	t44, b8, t44


	LD	a1,  0 * SIZE(AO)					#	sa stores in col major
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)
	MUL	t11, a1, t11
	MUL	t12, a1, t12
	MUL	t13, a1, t13
	MUL	t14, a1, t14
	NMSUB	t21, t21, a2, t11
	NMSUB	t22, t22, a2, t12
	NMSUB	t23, t23, a2, t13
	NMSUB	t24, t24, a2, t14
	NMSUB	t31, t31, a3, t11
	NMSUB	t32, t32, a3, t12
	NMSUB	t33, t33, a3, t13
	NMSUB	t34, t34, a3, t14
	NMSUB	t41, t41, a4, t11
	NMSUB	t42, t42, a4, t12
	NMSUB	t43, t43, a4, t13
	NMSUB	t44, t44, a4, t14


	LD	a5,  5 * SIZE(AO)
	LD	a6,  6 * SIZE(AO)
	LD	a7,  7 * SIZE(AO)
	MUL	t21, a5, t21
	MUL	t22, a5, t22
	MUL	t23, a5, t23
	MUL	t24, a5, t24
	NMSUB	t31, t31, a6, t21
	NMSUB	t32, t32, a6, t22
	NMSUB	t33, t33, a6, t23
	NMSUB	t34, t34, a6, t24
	NMSUB	t41, t41, a7, t21
	NMSUB	t42, t42, a7, t22
	NMSUB	t43, t43, a7, t23
	NMSUB	t44, t44, a7, t24


	LD	a8, 10 * SIZE(AO)
	LD	a1, 11 * SIZE(AO)
	MUL	t31, a8, t31
	MUL	t32, a8, t32
	MUL	t33, a8, t33
	MUL	t34, a8, t34
	NMSUB	t41, t41, a1, t31
	NMSUB	t42, t42, a1, t32
	NMSUB	t43, t43, a1, t33
	NMSUB	t44, t44, a1, t34


	LD	a2, 15 * SIZE(AO)
	MUL	t41, a2, t41
	MUL	t42, a2, t42
	MUL	t43, a2, t43
	MUL	t44, a2, t44

	ST	t11,  0 * SIZE(BO)					#	update packed B
	ST	t12,  1 * SIZE(BO)
	ST	t13,  2 * SIZE(BO)
	ST	t14,  3 * SIZE(BO)
	ST	t21,  4 * SIZE(BO)
	ST	t22,  5 * SIZE(BO)
	ST	t23,  6 * SIZE(BO)
	ST	t24,  7 * SIZE(BO)
	ST	t31,  8 * SIZE(BO)
	ST	t32,  9 * SIZE(BO)
	ST	t33, 10 * SIZE(BO)
	ST	t34, 11 * SIZE(BO)
	ST	t41, 12 * SIZE(BO)
	ST	t42, 13 * SIZE(BO)
	ST	t43, 14 * SIZE(BO)
	ST	t44, 15 * SIZE(BO)

	ST	t11,  0 * SIZE(CO1)					#	write back
	ST	t21,  1 * SIZE(CO1)
	ST	t31,  2 * SIZE(CO1)
	ST	t41,  3 * SIZE(CO1)
	ST	t12,  0 * SIZE(CO2)
	ST	t22,  1 * SIZE(CO2)
	ST	t32,  2 * SIZE(CO2)
	ST	t42,  3 * SIZE(CO2)
	ST	t13,  0 * SIZE(CO3)
	ST	t23,  1 * SIZE(CO3)
	ST	t33,  2 * SIZE(CO3)
	ST	t43,  3 * SIZE(CO3)
	ST	t14,  0 * SIZE(CO4)
	ST	t24,  1 * SIZE(CO4)
	ST	t34,  2 * SIZE(CO4)
	ST	t44,  3 * SIZE(CO4)

	daddiu	CO1, CO1, 4 * SIZE				#	fixed pointers
	daddiu	CO2, CO2, 4 * SIZE
	daddiu	CO3, CO3, 4 * SIZE
	daddiu	CO4, CO4, 4 * SIZE

	dsubu	TEMP, K, KK
	dsll	L,    TEMP, 2 + BASE_SHIFT
	dsll	TEMP, TEMP, 2 + BASE_SHIFT
	daddu	AO, AO, L						#	mov AO to the end of panel Ai
	daddu	BO, BO, TEMP					#	mov BO to the end of panel Bj

	daddiu	KK, KK, 4						#	the length of rectangular data part increases by 4
	daddiu	I, I, -1

	MTC	$0,  a1
	MOV	t11, a1
	MOV	t21, a1
	MOV	t31, a1
	MOV	t41, a1
	MOV	t12, a1
	MOV	t22, a1
	MOV	t32, a1
	MOV	t42, a1
	bgtz	I, .L11
	nop


	.align 3
.L20:
	andi	I,  M, 2						#	mr=2,nr=4
	blez	I, .L50
	nop

	MOV	t13, t11
	MOV	t23, t11
	MOV	t33, t11
	MOV	t43, t11
	MOV	t14, t11
	MOV	t24, t11
	MOV	t34, t11
	MOV	t44, t11

	LD	a1,  0 * SIZE(AO)					#	this part compute the rectangular data part of Ai
	LD	a2,  1 * SIZE(AO)					#	mr*KK with nr*KK

	LD	b1,  0 * SIZE(B)					#	get 4b
	LD	b2,  1 * SIZE(B)
	LD	b3,  2 * SIZE(B)
	LD	b4,  3 * SIZE(B)

	dsra	L,  KK, 2
	blez	L, .L25
	move	BO,  B


	.align	3
.L22:
	LD	a5,  2 * SIZE(AO)
	LD	a6,  3 * SIZE(AO)
	LD	b5,  4 * SIZE(BO)
	LD	b6,  5 * SIZE(BO)
	LD	b7,  6 * SIZE(BO)
	LD	b8,  7 * SIZE(BO)

	MADD	t11, t11, a1, b1				#	1st compute
	MADD	t21, t21, a2, b1
	MADD	t12, t12, a1, b2
	MADD	t22, t22, a2, b2
	MADD	t13, t13, a1, b3
	MADD	t23, t23, a2, b3
	MADD	t14, t14, a1, b4
	MADD	t24, t24, a2, b4

	LD	a3,   4 * SIZE(AO)
	LD	a4,   5 * SIZE(AO)
	LD	b1,   8 * SIZE(BO)
	LD	b2,   9 * SIZE(BO)
	LD	b3,  10 * SIZE(BO)
	LD	b4,  11 * SIZE(BO)

	MADD	t11, t11, a5, b5				#	2ed compute
	MADD	t21, t21, a6, b5
	MADD	t12, t12, a5, b6
	MADD	t22, t22, a6, b6
	MADD	t13, t13, a5, b7
	MADD	t23, t23, a6, b7
	MADD	t14, t14, a5, b8
	MADD	t24, t24, a6, b8

	LD	a7,   6 * SIZE(AO)
	LD	a8,   7 * SIZE(AO)
	LD	b5,  12 * SIZE(BO)
	LD	b6,  13 * SIZE(BO)
	LD	b7,  14 * SIZE(BO)
	LD	b8,  15 * SIZE(BO)

	MADD	t11, t11, a3, b1				#	3rd compute
	MADD	t21, t21, a4, b1
	MADD	t12, t12, a3, b2
	MADD	t22, t22, a4, b2
	MADD	t13, t13, a3, b3
	MADD	t23, t23, a4, b3
	MADD	t14, t14, a3, b4
	MADD	t24, t24, a4, b4

	daddiu	AO, AO,  8 * SIZE				#	AO += 2mr*4kr
	daddiu	BO, BO, 16 * SIZE				#	BO += 4nr*4kr

	LD	a1,  0 * SIZE(AO)					#	next
	LD	a2,  1 * SIZE(AO)
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	MADD	t11, t11, a7, b5				#	4th compute
	MADD	t21, t21, a8, b5
	MADD	t12, t12, a7, b6
	MADD	t22, t22, a8, b6
	MADD	t13, t13, a7, b7
	MADD	t23, t23, a8, b7
	MADD	t14, t14, a7, b8
	MADD	t24, t24, a8, b8

	daddiu	L, L, -1
	bgtz	L, .L22
	nop


	.align 3
.L25:
	andi	L, KK,  3
	blez	L, .L28
	nop

	.align	3
.L26:
	MADD	t11, t11, a1, b1				#	3rd compute
	MADD	t21, t21, a2, b1
	MADD	t12, t12, a1, b2
	MADD	t22, t22, a2, b2
	MADD	t13, t13, a1, b3
	MADD	t23, t23, a2, b3
	MADD	t14, t14, a1, b4
	MADD	t24, t24, a2, b4

	daddiu	AO, AO,  2 * SIZE				#	AO += 2mr
	daddiu	BO, BO,  4 * SIZE				#	BO += 4nr

	LD	a1,  0 * SIZE(AO)					#	next
	LD	a2,  1 * SIZE(AO)
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L26
	nop


.L28:										#	deal with the triangular part
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)
	LD	b5,  4 * SIZE(BO)
	LD	b6,  5 * SIZE(BO)
	LD	b7,  6 * SIZE(BO)
	LD	b8,  7 * SIZE(BO)

	SUB	t11, b1, t11
	SUB	t12, b2, t12
	SUB	t13, b3, t13
	SUB	t14, b4, t14
	SUB	t21, b5, t21
	SUB	t22, b6, t22
	SUB	t23, b7, t23
	SUB	t24, b8, t24


	LD	b1,  0 * SIZE(AO)				#	computes the triangular_part
	LD	b2,  1 * SIZE(AO)
	MUL	t11, b1, t11
	MUL	t12, b1, t12
	MUL	t13, b1, t13
	MUL	t14, b1, t14
	NMSUB	t21, t21, b2, t11
	NMSUB	t22, t22, b2, t12
	NMSUB	t23, t23, b2, t13
	NMSUB	t24, t24, b2, t14

	LD	b3,  3 * SIZE(AO)
	MUL	t21, b3, t21
	MUL	t22, b3, t22
	MUL	t23, b3, t23
	MUL	t24, b3, t24


	ST	t11,  0 * SIZE(BO)
	ST	t12,  1 * SIZE(BO)
	ST	t13,  2 * SIZE(BO)
	ST	t14,  3 * SIZE(BO)
	ST	t21,  4 * SIZE(BO)
	ST	t22,  5 * SIZE(BO)
	ST	t23,  6 * SIZE(BO)
	ST	t24,  7 * SIZE(BO)

	ST	t11,  0 * SIZE(CO1)
	ST	t21,  1 * SIZE(CO1)
	ST	t12,  0 * SIZE(CO2)
	ST	t22,  1 * SIZE(CO2)
	ST	t13,  0 * SIZE(CO3)
	ST	t23,  1 * SIZE(CO3)
	ST	t14,  0 * SIZE(CO4)
	ST	t24,  1 * SIZE(CO4)

	daddiu	CO1, CO1, 2 * SIZE
	daddiu	CO2, CO2, 2 * SIZE
	daddiu	CO3, CO3, 2 * SIZE
	daddiu	CO4, CO4, 2 * SIZE


	dsubu	TEMP, K, KK
	dsll	L,    TEMP, 1 + BASE_SHIFT
	dsll	TEMP, TEMP, 2 + BASE_SHIFT
	daddu	AO, AO, L							#	mov AO to the end of Ai
	daddu	BO, BO, TEMP						#	mov BO to the end of Bj

	daddiu	KK, KK, 2							#	the length of rectangular data part increases by 2
	MTC	$0,  a1
	MOV	t11, a1
	MOV	t21, a1
	MOV	t31, a1
	MOV	t41, a1
	MOV	t12, a1
	MOV	t22, a1
	MOV	t32, a1
	MOV	t42, a1


	.align 3
.L50:
	andi	I,  M, 1						#	mr=1,nr=4
	blez	I, .L29
	nop

	MOV	t13, t11
	MOV	t23, t11
	MOV	t33, t11
	MOV	t43, t11
	MOV	t14, t11
	MOV	t24, t11
	MOV	t34, t11
	MOV	t44, t11

	LD	a1,  0 * SIZE(AO)					#	this part compute the rectangular data part of Ai

	LD	b1,  0 * SIZE(B)					#	get 4b
	LD	b2,  1 * SIZE(B)
	LD	b3,  2 * SIZE(B)
	LD	b4,  3 * SIZE(B)

	dsra	L,  KK, 2
	blez	L, .L55
	move	BO,  B


	.align	3
.L52:
	LD	a5,  1 * SIZE(AO)
	LD	b5,  4 * SIZE(BO)
	LD	b6,  5 * SIZE(BO)
	LD	b7,  6 * SIZE(BO)
	LD	b8,  7 * SIZE(BO)

	MADD	t11, t11, a1, b1				#	1st compute
	MADD	t12, t12, a1, b2
	MADD	t13, t13, a1, b3
	MADD	t14, t14, a1, b4

	LD	a3,   2 * SIZE(AO)
	LD	b1,   8 * SIZE(BO)
	LD	b2,   9 * SIZE(BO)
	LD	b3,  10 * SIZE(BO)
	LD	b4,  11 * SIZE(BO)

	MADD	t11, t11, a5, b5				#	2ed compute
	MADD	t12, t12, a5, b6
	MADD	t13, t13, a5, b7
	MADD	t14, t14, a5, b8

	LD	a7,   3 * SIZE(AO)
	LD	b5,  12 * SIZE(BO)
	LD	b6,  13 * SIZE(BO)
	LD	b7,  14 * SIZE(BO)
	LD	b8,  15 * SIZE(BO)

	MADD	t11, t11, a3, b1				#	3rd compute
	MADD	t12, t12, a3, b2
	MADD	t13, t13, a3, b3
	MADD	t14, t14, a3, b4

	daddiu	AO, AO,  4 * SIZE				#	AO += mr*4kr
	daddiu	BO, BO, 16 * SIZE				#	BO += 4nr*4kr

	LD	a1,  0 * SIZE(AO)					#	next
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	MADD	t11, t11, a7, b5				#	4th compute
	MADD	t12, t12, a7, b6
	MADD	t13, t13, a7, b7
	MADD	t14, t14, a7, b8

	daddiu	L, L, -1
	bgtz	L, .L52
	nop


	.align 3
.L55:
	andi	L, KK,  3
	blez	L, .L58
	nop

	.align	3
.L56:
	MADD	t11, t11, a1, b1				#	3rd compute
	MADD	t12, t12, a1, b2
	MADD	t13, t13, a1, b3
	MADD	t14, t14, a1, b4

	daddiu	AO, AO,  1 * SIZE				#	AO += 2mr
	daddiu	BO, BO,  4 * SIZE				#	BO += 4nr

	LD	a1,  0 * SIZE(AO)					#	next
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L56
	nop


.L58:										#	deal with the triangular part
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	SUB	t11, b1, t11
	SUB	t12, b2, t12
	SUB	t13, b3, t13
	SUB	t14, b4, t14


	LD	b1,  0 * SIZE(AO)				#	computes the triangular_part
	MUL	t11, b1, t11
	MUL	t12, b1, t12
	MUL	t13, b1, t13
	MUL	t14, b1, t14

	ST	t11,  0 * SIZE(BO)
	ST	t12,  1 * SIZE(BO)
	ST	t13,  2 * SIZE(BO)
	ST	t14,  3 * SIZE(BO)

	ST	t11,  0 * SIZE(CO1)
	ST	t12,  0 * SIZE(CO2)
	ST	t13,  0 * SIZE(CO3)
	ST	t14,  0 * SIZE(CO4)

	daddiu	CO1, CO1, 1 * SIZE
	daddiu	CO2, CO2, 1 * SIZE
	daddiu	CO3, CO3, 1 * SIZE
	daddiu	CO4, CO4, 1 * SIZE


	dsubu	TEMP, K, KK
	dsll	L,    TEMP, BASE_SHIFT				#	mr=1
	dsll	TEMP, TEMP, 2 + BASE_SHIFT
	daddu	AO, AO, L							#	mov AO to the end of Ai
	daddu	BO, BO, TEMP						#	mov BO to the end of Bj

	daddiu	KK, KK, 1							#	the length of rectangular data part increases by 2

	.align 3
.L29:
	move	B,  BO								#	fixed panel Bj
	bgtz	J, .L10
	nop


	.align 3
.L30:
	andi	J,  N, 2							#	nr=2
	blez	J, .L70
	nop

	move	CO1, C
	daddu	CO2, C,   LDC

	MTC	$0,  t11								#	clear result regusters
	MOV	t21, t11
	MOV	t31, t11
	MOV	t41, t11

	move	KK, OFFSET
	move	AO, A								#	reset A
	daddu	C,  CO2, LDC						#	fixed

	dsra	I,  M, 2							#	I = mc/4
	blez	I, .L40
	nop

.L31:
	MOV	t12, t11
	MOV	t22, t11
	MOV	t32, t11
	MOV	t42, t11
	LD	a1,  0 * SIZE(AO)					#	this part compute the rectangular data part of Ai
	LD	a2,  1 * SIZE(AO)					#	mr*KK with nr*KK
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)					#	get 4a

	LD	b1,  0 * SIZE(B)					#	get 4b
	LD	b2,  1 * SIZE(B)

	dsra	L,  KK, 2						#	L=kk/4
	blez	L, .L35
	move	BO,  B							#	reset B


	.align	3
.L32:
	LD	a5,  4 * SIZE(AO)
	LD	a6,  5 * SIZE(AO)
	LD	a7,  6 * SIZE(AO)
	LD	a8,  7 * SIZE(AO)
	LD	b5,  2 * SIZE(BO)
	LD	b6,  3 * SIZE(BO)

	MADD	t11, t11, a1, b1				#	1st compute
	MADD	t21, t21, a2, b1
	MADD	t31, t31, a3, b1
	MADD	t41, t41, a4, b1
	MADD	t12, t12, a1, b2
	MADD	t22, t22, a2, b2
	MADD	t32, t32, a3, b2
	MADD	t42, t42, a4, b2

	LD	a1,   8 * SIZE(AO)
	LD	a2,   9 * SIZE(AO)
	LD	a3,  10 * SIZE(AO)
	LD	a4,  11 * SIZE(AO)
	LD	b3,   4 * SIZE(BO)
	LD	b4,   5 * SIZE(BO)

	MADD	t11, t11, a5, b5				#	2ed compute
	MADD	t21, t21, a6, b5
	MADD	t31, t31, a7, b5
	MADD	t41, t41, a8, b5
	MADD	t12, t12, a5, b6
	MADD	t22, t22, a6, b6
	MADD	t32, t32, a7, b6
	MADD	t42, t42, a8, b6

	LD	a5,  12 * SIZE(AO)
	LD	a6,  13 * SIZE(AO)
	LD	a7,  14 * SIZE(AO)
	LD	a8,  15 * SIZE(AO)
	LD	b7,  6 * SIZE(BO)
	LD	b8,  7 * SIZE(BO)

	MADD	t11, t11, a1, b3				#	3rd compute
	MADD	t21, t21, a2, b3
	MADD	t31, t31, a3, b3
	MADD	t41, t41, a4, b3
	MADD	t12, t12, a1, b4
	MADD	t22, t22, a2, b4
	MADD	t32, t32, a3, b4
	MADD	t42, t42, a4, b4

	daddiu	AO, AO, 16 * SIZE				#	AO += 4mr*4kr
	daddiu	BO, BO,  8 * SIZE				#	BO += 2nr*4kr

	LD	a1,  0 * SIZE(AO)					#	next
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	MADD	t11, t11, a5, b7				#	4th compute
	MADD	t21, t21, a6, b7
	MADD	t31, t31, a7, b7
	MADD	t41, t41, a8, b7
	MADD	t12, t12, a5, b8
	MADD	t22, t22, a6, b8
	MADD	t32, t32, a7, b8
	MADD	t42, t42, a8, b8

	daddiu	L, L, -1
	bgtz	L, .L32
	nop


	.align 3

.L35:
	andi	L, KK,  3
	blez	L, .L38
	nop

	.align	3
.L36:
	MADD	t11, t11, a1, b1				#	3rd compute
	MADD	t21, t21, a2, b1
	MADD	t31, t31, a3, b1
	MADD	t41, t41, a4, b1

	MADD	t12, t12, a1, b2
	MADD	t22, t22, a2, b2
	MADD	t32, t32, a3, b2
	MADD	t42, t42, a4, b2

	daddiu	AO, AO,  4 * SIZE				#	AO += 4mr
	daddiu	BO, BO,  2 * SIZE				#	BO += 2nr

	LD	a1,  0 * SIZE(AO)					#	next
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L36
	nop


.L38:										#
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)
	LD	b5,  4 * SIZE(BO)
	LD	b6,  5 * SIZE(BO)
	LD	b7,  6 * SIZE(BO)
	LD	b8,  7 * SIZE(BO)

	SUB	t11, b1, t11
	SUB	t12, b2, t12
	SUB	t21, b3, t21
	SUB	t22, b4, t22
	SUB	t31, b5, t31
	SUB	t32, b6, t32
	SUB	t41, b7, t41
	SUB	t42, b8, t42

	LD	a1,  0 * SIZE(AO)					#	sa stores in col major
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)
	MUL	t11, a1, t11
	MUL	t12, a1, t12
	NMSUB	t21, t21, a2, t11
	NMSUB	t22, t22, a2, t12
	NMSUB	t31, t31, a3, t11
	NMSUB	t32, t32, a3, t12
	NMSUB	t41, t41, a4, t11
	NMSUB	t42, t42, a4, t12


	LD	a5,  5 * SIZE(AO)
	LD	a6,  6 * SIZE(AO)
	LD	a7,  7 * SIZE(AO)
	MUL	t21, a5, t21
	MUL	t22, a5, t22
	NMSUB	t31, t31, a6, t21
	NMSUB	t32, t32, a6, t22
	NMSUB	t41, t41, a7, t21
	NMSUB	t42, t42, a7, t22


	LD	a8, 10 * SIZE(AO)
	LD	a1, 11 * SIZE(AO)
	MUL	t31, a8, t31
	MUL	t32, a8, t32
	NMSUB	t41, t41, a1, t31
	NMSUB	t42, t42, a1, t32


	LD	a2, 15 * SIZE(AO)
	MUL	t41, a2, t41
	MUL	t42, a2, t42

	ST	t11,  0 * SIZE(BO)
	ST	t12,  1 * SIZE(BO)
	ST	t21,  2 * SIZE(BO)
	ST	t22,  3 * SIZE(BO)
	ST	t31,  4 * SIZE(BO)
	ST	t32,  5 * SIZE(BO)
	ST	t41,  6 * SIZE(BO)
	ST	t42,  7 * SIZE(BO)

	ST	t11,  0 * SIZE(CO1)
	ST	t21,  1 * SIZE(CO1)
	ST	t31,  2 * SIZE(CO1)
	ST	t41,  3 * SIZE(CO1)
	ST	t12,  0 * SIZE(CO2)
	ST	t22,  1 * SIZE(CO2)
	ST	t32,  2 * SIZE(CO2)
	ST	t42,  3 * SIZE(CO2)

	daddiu	CO1, CO1, 4 * SIZE
	daddiu	CO2, CO2, 4 * SIZE

	dsubu	TEMP, K, KK
	dsll	L,    TEMP, 2 + BASE_SHIFT
	dsll	TEMP, TEMP, 1 + BASE_SHIFT
	daddu	AO, AO, L						#	move AO to the end of Ai
	daddu	BO, BO, TEMP

	daddiu	KK, KK, 4						#

	MTC	$0,  a1
	MOV	t11, a1
	MOV	t21, a1
	MOV	t31, a1
	MOV	t41, a1

	daddiu	I, I, -1
	bgtz	I, .L31
	nop


	.align 3
.L40:
	andi	I,  M, 2
	blez	I, .L60
	nop

	MOV	t12, t11							#	clear result registers
	MOV	t22, t21
	MOV	t32, t31
	MOV	t42, t41

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	b1,  0 * SIZE(B)
	LD	b2,  1 * SIZE(B)

	dsra	L,  KK, 2
	blez	L, .L45
	move	BO,  B							#	reset B


	.align	3
.L42:
	LD	a5,  2 * SIZE(AO)
	LD	a6,  3 * SIZE(AO)
	LD	b5,  2 * SIZE(BO)
	LD	b6,  3 * SIZE(BO)

	MADD	t11, t11, a1, b1				#	1st compute
	MADD	t21, t21, a2, b1
	MADD	t12, t12, a1, b2
	MADD	t22, t22, a2, b2

	LD	a3,  4 * SIZE(AO)
	LD	a4,  5 * SIZE(AO)
	LD	b3,  4 * SIZE(BO)
	LD	b4,  5 * SIZE(BO)

	MADD	t11, t11, a5, b5				#	2ed compute
	MADD	t21, t21, a6, b5
	MADD	t12, t12, a5, b6
	MADD	t22, t22, a6, b6

	LD	a7,  6 * SIZE(AO)
	LD	a8,  7 * SIZE(AO)
	LD	b7,  6 * SIZE(BO)
	LD	b8,  7 * SIZE(BO)

	MADD	t11, t11, a3, b3				#	3rd compute
	MADD	t21, t21, a4, b3
	MADD	t12, t12, a3, b4
	MADD	t22, t22, a4, b4

	daddiu	AO, AO,  8 * SIZE				#	AO += 2mr*4kr
	daddiu	BO, BO,  8 * SIZE				#	BO += 2nr*4kr

	LD	a1,  0 * SIZE(AO)					#	next
	LD	a2,  1 * SIZE(AO)
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	MADD	t11, t11, a7, b7				#	4th compute
	MADD	t21, t21, a8, b7
	MADD	t12, t12, a7, b8
	MADD	t22, t22, a8, b8

	daddiu	L, L, -1
	bgtz	L, .L42
	nop

	.align 3

.L45:
	andi	L, KK,  3
	blez	L, .L48
	nop

	.align	3
.L46:
	MADD	t11, t11, a1, b1				#	3rd compute
	MADD	t21, t21, a2, b1
	MADD	t12, t12, a1, b2
	MADD	t22, t22, a2, b2

	daddiu	AO, AO,  2 * SIZE				#	AO += 2mr
	daddiu	BO, BO,  2 * SIZE				#	BO += 2nr

	LD	a1,  0 * SIZE(AO)					#	next
	LD	a2,  1 * SIZE(AO)
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L46
	nop

.L48:
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	SUB	t11, b1, t11
	SUB	t12, b2, t12
	SUB	t21, b3, t21
	SUB	t22, b4, t22

	LD	b1,  0 * SIZE(AO)				#	computes the triangular_part
	LD	b2,  1 * SIZE(AO)
	MUL	t11, b1, t11
	MUL	t12, b1, t12
	NMSUB	t21, t21, b2, t11
	NMSUB	t22, t22, b2, t12

	LD	b3,  3 * SIZE(AO)
	MUL	t21, b3, t21
	MUL	t22, b3, t22

	ST	t11,  0 * SIZE(BO)
	ST	t12,  1 * SIZE(BO)
	ST	t21,  2 * SIZE(BO)
	ST	t22,  3 * SIZE(BO)

	ST	t11,  0 * SIZE(CO1)
	ST	t21,  1 * SIZE(CO1)
	ST	t12,  0 * SIZE(CO2)
	ST	t22,  1 * SIZE(CO2)

	daddiu	CO1, CO1, 2 * SIZE
	daddiu	CO2, CO2, 2 * SIZE

	dsubu	TEMP, K, KK
	dsll	L,    TEMP, 1 + BASE_SHIFT
	dsll	TEMP, TEMP, 1 + BASE_SHIFT
	daddu	AO, AO, L
	daddu	BO, BO, TEMP

	daddiu	KK, KK, 2
	MTC	$0,  a1
	MOV	t11, a1
	MOV	t21, a1
	MOV	t31, a1
	MOV	t41, a1


	.align 3
.L60:
	andi	I,  M, 1						#	mr=1
	blez	I, .L49
	nop

	MOV	t12, t11							#	clear result registers
	MOV	t22, t21
	MOV	t32, t31
	MOV	t42, t41

	LD	a1,  0 * SIZE(AO)
	LD	b1,  0 * SIZE(B)
	LD	b2,  1 * SIZE(B)

	dsra	L,  KK, 2
	blez	L, .L65
	move	BO,  B							#	reset B


	.align	3
.L62:
	LD	a5,  1 * SIZE(AO)
	LD	b5,  2 * SIZE(BO)
	LD	b6,  3 * SIZE(BO)

	MADD	t11, t11, a1, b1				#	1st compute
	MADD	t12, t12, a1, b2

	LD	a3,  2 * SIZE(AO)
	LD	b3,  4 * SIZE(BO)
	LD	b4,  5 * SIZE(BO)

	MADD	t11, t11, a5, b5				#	2ed compute
	MADD	t12, t12, a5, b6

	LD	a7,  3 * SIZE(AO)
	LD	b7,  6 * SIZE(BO)
	LD	b8,  7 * SIZE(BO)

	MADD	t11, t11, a3, b3				#	3rd compute
	MADD	t12, t12, a3, b4

	daddiu	AO, AO,  4 * SIZE				#	AO += mr*4kr
	daddiu	BO, BO,  8 * SIZE				#	BO += 2nr*4kr

	LD	a1,  0 * SIZE(AO)					#	next
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	MADD	t11, t11, a7, b7				#	4th compute
	MADD	t12, t12, a7, b8

	daddiu	L, L, -1
	bgtz	L, .L62
	nop

	.align 3

.L65:
	andi	L, KK,  3
	blez	L, .L68
	nop

	.align	3
.L66:
	MADD	t11, t11, a1, b1				#	3rd compute
	MADD	t12, t12, a1, b2

	daddiu	AO, AO,  1 * SIZE				#	AO += 1mr
	daddiu	BO, BO,  2 * SIZE				#	BO += 2nr

	LD	a1,  0 * SIZE(AO)					#	next
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L66
	nop

.L68:
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	SUB	t11, b1, t11
	SUB	t12, b2, t12

	LD	b1,  0 * SIZE(AO)				#	computes the triangular_part
	MUL	t11, b1, t11
	MUL	t12, b1, t12

	ST	t11,  0 * SIZE(BO)
	ST	t12,  1 * SIZE(BO)

	ST	t11,  0 * SIZE(CO1)
	ST	t12,  0 * SIZE(CO2)

	daddiu	CO1, CO1, 1 * SIZE
	daddiu	CO2, CO2, 1 * SIZE

	dsubu	TEMP, K, KK
	dsll	L,    TEMP, BASE_SHIFT		#	mr=1
	dsll	TEMP, TEMP, 1 + BASE_SHIFT
	daddu	AO, AO, L
	daddu	BO, BO, TEMP

	daddiu	KK, KK, 1

	.align 3
.L49:
	move	B,  BO

	.align 3

.L70:
	andi	J,  N, 1					#	nr=1
	blez	J, .L999					#	END
	nop

	move	CO1, C

	move	KK, OFFSET
	move	AO, A

	dsra	I,  M, 2
	blez	I, .L80
	nop

.L71:
	MTC	$0,  t11								#	clear result regusters
	MOV	t21, t11
	MOV	t31, t11
	MOV	t41, t11

	LD	a1,  0 * SIZE(AO)					#	this part compute the rectangular data part of Ai
	LD	a2,  1 * SIZE(AO)					#	mr*KK with nr*KK
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)					#	get 4a

	LD	b1,  0 * SIZE(B)					#	get 4b

	dsra	L,  KK, 2
	blez	L, .L75
	move	BO,  B							#	reset B

	.align	3
.L72:
	LD	a5,  4 * SIZE(AO)
	LD	a6,  5 * SIZE(AO)
	LD	a7,  6 * SIZE(AO)
	LD	a8,  7 * SIZE(AO)

	LD	b5,  1 * SIZE(BO)

	MADD	t11, t11, a1, b1				#	1st compute
	MADD	t21, t21, a2, b1
	MADD	t31, t31, a3, b1
	MADD	t41, t41, a4, b1

	LD	a1,   8 * SIZE(AO)
	LD	a2,   9 * SIZE(AO)
	LD	a3,  10 * SIZE(AO)
	LD	a4,  11 * SIZE(AO)

	LD	b3,  2 * SIZE(BO)

	MADD	t11, t11, a5, b5				#	2ed compute
	MADD	t21, t21, a6, b5
	MADD	t31, t31, a7, b5
	MADD	t41, t41, a8, b5

	LD	a5,  12 * SIZE(AO)
	LD	a6,  13 * SIZE(AO)
	LD	a7,  14 * SIZE(AO)
	LD	a8,  15 * SIZE(AO)

	LD	b7,  3 * SIZE(BO)

	MADD	t11, t11, a1, b3				#	3rd compute
	MADD	t21, t21, a2, b3
	MADD	t31, t31, a3, b3
	MADD	t41, t41, a4, b3

	daddiu	AO, AO, 16 * SIZE				#	AO += 4mr*4kr
	daddiu	BO, BO,  4 * SIZE				#	BO += 1nr*4kr

	LD	a1,  0 * SIZE(AO)					#	next
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)

	MADD	t11, t11, a5, b7				#	4th compute
	MADD	t21, t21, a6, b7
	MADD	t31, t31, a7, b7
	MADD	t41, t41, a8, b7

	daddiu	L, L, -1
	bgtz	L, .L72
	nop

	.align 3

.L75:
	andi	L, KK,  3
	blez	L, .L78
	nop

	.align	3
.L76:
	MADD	t11, t11, a1, b1				#	3rd compute
	MADD	t21, t21, a2, b1
	MADD	t31, t31, a3, b1
	MADD	t41, t41, a4, b1

	daddiu	AO, AO,  4 * SIZE				#	AO += 4mr
	daddiu	BO, BO,  1 * SIZE				#	BO += 1nr

	LD	a1,  0 * SIZE(AO)					#	next
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L76
	nop

.L78:
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	SUB	t11, b1, t11
	SUB	t21, b2, t21
	SUB	t31, b3, t31
	SUB	t41, b4, t41

	LD	a1,  0 * SIZE(AO)					#	sa stores in col major
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)
	MUL	t11, a1, t11
	NMSUB	t21, t21, a2, t11
	NMSUB	t31, t31, a3, t11
	NMSUB	t41, t41, a4, t11

	LD	a5,  5 * SIZE(AO)
	LD	a6,  6 * SIZE(AO)
	LD	a7,  7 * SIZE(AO)
	MUL	t21, a5, t21
	NMSUB	t31, t31, a6, t21
	NMSUB	t41, t41, a7, t21

	LD	a8, 10 * SIZE(AO)
	LD	a1, 11 * SIZE(AO)
	MUL	t31, a8, t31
	NMSUB	t41, t41, a1, t31

	LD	a2, 15 * SIZE(AO)
	MUL	t41, a2, t41


	ST	t11,  0 * SIZE(BO)
	ST	t21,  1 * SIZE(BO)
	ST	t31,  2 * SIZE(BO)
	ST	t41,  3 * SIZE(BO)

	ST	t11,  0 * SIZE(CO1)
	ST	t21,  1 * SIZE(CO1)
	ST	t31,  2 * SIZE(CO1)
	ST	t41,  3 * SIZE(CO1)

	daddiu	CO1, CO1, 4 * SIZE

	dsubu	TEMP, K, KK
	dsll	L,    TEMP, 2 + BASE_SHIFT
	dsll	TEMP, TEMP, 0 + BASE_SHIFT
	daddu	AO, AO, L
	daddu	BO, BO, TEMP

	daddiu	KK, KK, 4
	daddiu	I, I, -1
	bgtz	I, .L71
	nop


	.align 3

.L80:
	andi	I,  M, 2
	blez	I, .L90
	NOP

	MTC	$0,  t11
	MOV	t21, t11							#	clear result registers

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	b1,  0 * SIZE(B)

	dsra	L,  KK, 2
	blez	L, .L85
	move	BO,  B

	.align	3
.L82:
	LD	a5,  2 * SIZE(AO)
	LD	a6,  3 * SIZE(AO)

	LD	b5,  1 * SIZE(BO)

	MADD	t11, t11, a1, b1				#	1st compute
	MADD	t21, t21, a2, b1

	LD	a3,  4 * SIZE(AO)
	LD	a4,  5 * SIZE(AO)

	LD	b3,  2 * SIZE(BO)

	MADD	t11, t11, a5, b5				#	2ed compute
	MADD	t21, t21, a6, b5

	LD	a7,  6 * SIZE(AO)
	LD	a8,  7 * SIZE(AO)

	LD	b7,  3 * SIZE(BO)

	MADD	t11, t11, a3, b3				#	3rd compute
	MADD	t21, t21, a4, b3

	daddiu	AO, AO,  8 * SIZE				#	AO += 2mr*4kr
	daddiu	BO, BO,  4 * SIZE				#	BO += 1nr*4kr

	LD	a1,  0 * SIZE(AO)					#	next
	LD	a2,  1 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)

	MADD	t11, t11, a7, b7				#	4th compute
	MADD	t21, t21, a8, b7

	daddiu	L, L, -1
	bgtz	L, .L82
	nop

	.align 3

.L85:
	andi	L, KK,  3
	blez	L, .L88
	nop

	.align	3
.L86:
	MADD	t11, t11, a1, b1				#	3rd compute
	MADD	t21, t21, a2, b1

	daddiu	AO, AO,  2 * SIZE				#	AO += 2mr
	daddiu	BO, BO,  1 * SIZE				#	BO += 1nr

	LD	a1,  0 * SIZE(AO)					#	next
	LD	a2,  1 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L86
	nop


.L88:
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	SUB	t11, b1, t11
	SUB	t21, b2, t21

	LD	b1,  0 * SIZE(AO)				#	computes the triangular_part
	LD	b2,  1 * SIZE(AO)
	MUL	t11, b1, t11
	NMSUB	t21, t21, b2, t11

	LD	b3,  3 * SIZE(AO)
	MUL	t21, b3, t21

	ST	t11,  0 * SIZE(BO)
	ST	t21,  1 * SIZE(BO)

	ST	t11,  0 * SIZE(CO1)
	ST	t21,  1 * SIZE(CO1)


	daddiu	CO1, CO1, 2 * SIZE

	dsubu	TEMP, K, KK
	dsll	L,    TEMP, 1 + BASE_SHIFT
	dsll	TEMP, TEMP, 0 + BASE_SHIFT
	daddu	AO, AO, L
	daddu	BO, BO, TEMP

	daddiu	KK, KK, 2


	.align 3
.L90:
	andi	I,  M, 1					#	mr=1
	blez	I, .L89
	NOP

	MTC	$0,  t11

	LD	a1,  0 * SIZE(AO)
	LD	b1,  0 * SIZE(B)

	dsra	L,  KK, 2
	blez	L, .L95
	move	BO,  B

	.align	3
.L92:
	LD	a5,  1 * SIZE(AO)
	LD	b5,  1 * SIZE(BO)

	MADD	t11, t11, a1, b1				#	1st compute

	LD	a3,  2 * SIZE(AO)
	LD	b3,  2 * SIZE(BO)

	MADD	t11, t11, a5, b5				#	2ed compute

	LD	a7,  3 * SIZE(AO)
	LD	b7,  3 * SIZE(BO)

	MADD	t11, t11, a3, b3				#	3rd compute

	daddiu	AO, AO,  4 * SIZE				#	AO += 2mr*4kr
	daddiu	BO, BO,  4 * SIZE				#	BO += 1nr*4kr

	LD	a1,  0 * SIZE(AO)					#	next
	LD	b1,  0 * SIZE(BO)

	MADD	t11, t11, a7, b7				#	4th compute

	daddiu	L, L, -1
	bgtz	L, .L92
	nop

	.align 3
.L95:
	andi	L, KK,  3
	blez	L, .L98
	nop

	.align	3
.L96:
	MADD	t11, t11, a1, b1				#	3rd compute

	daddiu	AO, AO,  1 * SIZE				#	AO += 2mr
	daddiu	BO, BO,  1 * SIZE				#	BO += 1nr

	LD	a1,  0 * SIZE(AO)					#	next
	LD	b1,  0 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L96
	nop


.L98:
	LD	b1,  0 * SIZE(BO)

	SUB	t11, b1, t11

	LD	b1,  0 * SIZE(AO)				#	computes the triangular_part
	MUL	t11, b1, t11

	ST	t11,  0 * SIZE(BO)

	ST	t11,  0 * SIZE(CO1)


	daddiu	CO1, CO1, 1 * SIZE

	dsubu	TEMP, K, KK
	dsll	L,    TEMP,  BASE_SHIFT
	dsll	TEMP, TEMP,  BASE_SHIFT
	daddu	AO, AO, L
	daddu	BO, BO, TEMP

	daddiu	KK, KK, 1


	.align 3
.L89:
	move	B,  BO


	.align 3

.L999:
	LDARG	$16,   0($sp)
	LDARG	$17,   8($sp)
	LDARG	$18,  16($sp)
	LDARG	$19,  24($sp)
	LDARG	$20,  32($sp)
	LDARG	$21,  40($sp)
	ldc1	$f24, 48($sp)
	ldc1	$f25, 56($sp)
	ldc1	$f26, 64($sp)
	ldc1	$f27, 72($sp)
	ldc1	$f28, 80($sp)

	LDARG	$22,  88($sp)
	LDARG	$23,  96($sp)
	LDARG	$24, 104($sp)
	LDARG	$25, 112($sp)

#ifndef __64BIT__
	ldc1	$f20,112($sp)
	ldc1	$f21,120($sp)
	ldc1	$f22,128($sp)
	ldc1	$f23,136($sp)
#endif

	j	$31
	daddiu	$sp, $sp, 144

	EPILOGUE
