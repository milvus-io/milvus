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
#define a3	$f26
#define a4	$f27

#define a5	$f28
#define a6	$f29
#define	a7	$f30
#define	a8	$f31

#define b1	$f2
#define b2	$f3
#define b3	$f4
#define b4	$f5

#define b5	$f6
#define b6	$f7
#define b7	$f8
#define b8	$f9

#define t11	$f10
#define t21	$f11
#define t31	$f12
#define	t41	$f13

#define t12	$f14
#define	t22	$f15
#define t32	$f16
#define	t42	$f17

#define	t13	$f18
#define	t23	$f19
#define	t33	$f20
#define	t43	$f21

#define	t14	$f22
#define	t24	$f23
#define	t34	$f24
#define t44	$f25

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

											#	RN compute from top to bottom left to right
	.align	3
	LDARG	OFFSET, 144($sp)				#	get the last parameter
	dsll	LDC, LDC, BASE_SHIFT			#	LDC * data_Byte

	neg	KK, OFFSET							#	for RN OFFSET always 0

	dsra	J,  N, 2						#	J = NC/4
	blez	J, .L30
	NOP

.L10:
	daddiu	J, J, -1

	move	CO1, C
	daddu	CO2, C,   LDC
	daddu	CO3, CO2, LDC
	daddu	CO4, CO3, LDC

	move	AO, A							#	A is the retangular matrix and B is the trigular matrix
	daddu	C,  CO4, LDC					#	Fixed pointer C

	dsra	I,  M, 2						#	I=MC/4
	blez	I, .L20
	NOP

	.align 3
.L11:
	MTC	$0,  t11							#	clear results registers
	MOV	t21, t11
	MOV	t31, t11
	MOV	t41, t11

	MOV	t12, t11
	MOV	t22, t11
	MOV	t32, t11
	MOV	t42, t11

	MOV	t13, t11
	MOV	t23, t11
	MOV	t33, t11
	MOV	t43, t11

	MOV	t14, t11
	MOV	t24, t11
	MOV	t34, t11
	MOV	t44, t11

	LD	a1,  0 * SIZE(AO)					#	AO point to the beginning address of sa
	LD	a2,  1 * SIZE(AO)					#	get 4 a
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  0 * SIZE(B)					#	B point to the beginning address of every panel Bj
	LD	b2,  1 * SIZE(B)					#	get 4 b
	LD	b3,  2 * SIZE(B)
	LD	b4,  3 * SIZE(B)

	dsra	L,  KK, 2						#	L=KK/4, KK is the length of the retangular data part of Bj
	blez	L, .L15
	move	BO,  B							#	reset B

.L12:
	LD	a5,  4 * SIZE(AO)
	LD	a6,  5 * SIZE(AO)
	LD	a7,  6 * SIZE(AO)
	LD	a8,  7 * SIZE(AO)

	LD	b5,  4 * SIZE(BO)
	LD	b6,  5 * SIZE(BO)
	LD	b7,  6 * SIZE(BO)
	LD	b8,  7 * SIZE(BO)

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
	MADD	t44, t44, a4, b4			#	fisrt

	LD	a1,  8 * SIZE(AO)
	LD	a2,  9 * SIZE(AO)
	LD	a3,  10 * SIZE(AO)
	LD	a4,  11 * SIZE(AO)

	LD	b1,  8 * SIZE(BO)
	LD	b2,  9 * SIZE(BO)
	LD	b3,  10 * SIZE(BO)
	LD	b4,  11 * SIZE(BO)

	MADD	t11, t11, a5, b5
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
	MADD	t44, t44, a8, b8			#	second

	LD	a5,  12 * SIZE(AO)
	LD	a6,  13 * SIZE(AO)
	LD	a7,  14 * SIZE(AO)
	LD	a8,  15 * SIZE(AO)

	LD	b5,  12 * SIZE(BO)
	LD	b6,  13 * SIZE(BO)
	LD	b7,  14 * SIZE(BO)
	LD	b8,  15 * SIZE(BO)

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
	MADD	t44, t44, a4, b4			#	third

	daddiu	AO, AO, 16 * SIZE			#	AO += 4mr*4kr
	daddiu	BO, BO, 16 * SIZE			#	BP += 4nr*4kr

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	MADD	t11, t11, a5, b5
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
	MADD	t44, t44, a8, b8			#	fouth

	daddiu	L, L, -1
	bgtz	L, .L12
	NOP


.L15:
	andi	L, KK, 3					#	deal with kc remainder part
	blez	L, .L18
	NOP

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

	daddiu	AO, AO, 4 * SIZE			#	AO += 4mr
	daddiu	BO, BO, 4 * SIZE			#	BP += 4nr

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L16
	NOP


	.align 3
.L18:									#	.L18 always deal with the trigular data part
 	LD	b1,  0 * SIZE(AO)				#	for RN & RT A is the result matrix
	LD	b2,  1 * SIZE(AO)				#	Fixed results
	LD	b3,  2 * SIZE(AO)
	LD	b4,  3 * SIZE(AO)				#	sa stored as col major

	SUB	t11, b1, t11
	SUB	t21, b2, t21
	SUB	t31, b3, t31
	SUB	t41, b4, t41

 	LD	b5,  4 * SIZE(AO)
	LD	b6,  5 * SIZE(AO)
	LD	b7,  6 * SIZE(AO)
	LD	b8,  7 * SIZE(AO)

	SUB	t12, b5, t12
	SUB	t22, b6, t22
	SUB	t32, b7, t32
	SUB	t42, b8, t42

 	LD	b1,  8 * SIZE(AO)
	LD	b2,  9 * SIZE(AO)
	LD	b3, 10 * SIZE(AO)
	LD	b4, 11 * SIZE(AO)

	SUB	t13, b1, t13
	SUB	t23, b2, t23
	SUB	t33, b3, t33
	SUB	t43, b4, t43

 	LD	b5, 12 * SIZE(AO)
	LD	b6, 13 * SIZE(AO)
	LD	b7, 14 * SIZE(AO)
	LD	b8, 15 * SIZE(AO)

	SUB	t14, b5, t14
	SUB	t24, b6, t24
	SUB	t34, b7, t34
	SUB	t44, b8, t44



	LD	b1,  0 * SIZE(BO)				#	BO point to the beginning of the trigular data part of Bj
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)
	MUL	t11, b1, t11
	MUL	t21, b1, t21
	MUL	t31, b1, t31
	MUL	t41, b1, t41
	NMSUB	t12, t12, b2, t11
	NMSUB	t22, t22, b2, t21
	NMSUB	t32, t32, b2, t31
	NMSUB	t42, t42, b2, t41
	NMSUB	t13, t13, b3, t11
	NMSUB	t23, t23, b3, t21
	NMSUB	t33, t33, b3, t31
	NMSUB	t43, t43, b3, t41
	NMSUB	t14, t14, b4, t11
	NMSUB	t24, t24, b4, t21
	NMSUB	t34, t34, b4, t31
	NMSUB	t44, t44, b4, t41


	LD	b5,  5 * SIZE(BO)
	LD	b6,  6 * SIZE(BO)
	LD	b7,  7 * SIZE(BO)
	MUL	t12, b5, t12
	MUL	t22, b5, t22
	MUL	t32, b5, t32
	MUL	t42, b5, t42
	NMSUB	t13, t13, b6, t12
	NMSUB	t23, t23, b6, t22
	NMSUB	t33, t33, b6, t32
	NMSUB	t43, t43, b6, t42
	NMSUB	t14, t14, b7, t12
	NMSUB	t24, t24, b7, t22
	NMSUB	t34, t34, b7, t32
	NMSUB	t44, t44, b7, t42



	LD	b8,  10 * SIZE(BO)
	LD	b1,  11 * SIZE(BO)
	MUL	t13, b8, t13
	MUL	t23, b8, t23
	MUL	t33, b8, t33
	MUL	t43, b8, t43
	NMSUB	t14, t14, b1, t13
	NMSUB	t24, t24, b1, t23
	NMSUB	t34, t34, b1, t33
	NMSUB	t44, t44, b1, t43



	LD	b2,  15 * SIZE(BO)
	MUL	t14, b2, t14
	MUL	t24, b2, t24
	MUL	t34, b2, t34
	MUL	t44, b2, t44



	ST	t11,  0 * SIZE(AO)				#	update packed blockA for follow-up compute
	ST	t21,  1 * SIZE(AO)
	ST	t31,  2 * SIZE(AO)
	ST	t41,  3 * SIZE(AO)

	ST	t12,  4 * SIZE(AO)
	ST	t22,  5 * SIZE(AO)
	ST	t32,  6 * SIZE(AO)
	ST	t42,  7 * SIZE(AO)

	ST	t13,  8 * SIZE(AO)
	ST	t23,  9 * SIZE(AO)
	ST	t33, 10 * SIZE(AO)
	ST	t43, 11 * SIZE(AO)

	ST	t14, 12 * SIZE(AO)
	ST	t24, 13 * SIZE(AO)
	ST	t34, 14 * SIZE(AO)
	ST	t44, 15 * SIZE(AO)


	ST	t11,  0 * SIZE(CO1)				#	write back results
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

	daddiu	CO1, CO1, 4 * SIZE			#	fixed address
	daddiu	CO2, CO2, 4 * SIZE
	daddiu	CO3, CO3, 4 * SIZE
	daddiu	CO4, CO4, 4 * SIZE


	dsubu	TEMP, K, KK					#	temp = kc - retangular data length of every panel
	dsll	L,    TEMP, 2 + BASE_SHIFT
	dsll	TEMP, TEMP, 2 + BASE_SHIFT
	daddu	AO, AO, L					#	move AO to the end of this panel. also the beginning of next panel
	daddu	BO, BO, TEMP				#	move BO to the end of this panel

	daddiu	I, I, -1
	bgtz	I, .L11
	NOP

	.align 3
.L20:
	andi	I,  M, 2					#	mr=2
	blez	I, .L50
	nop

	MTC	$0,  t11							#	clear results registers
	MOV	t21, t11
	MOV	t31, t11
	MOV	t41, t11

	MOV	t12, t11
	MOV	t22, t11
	MOV	t32, t11
	MOV	t42, t11

	MOV	t13, t11
	MOV	t23, t11
	MOV	t33, t11
	MOV	t43, t11

	MOV	t14, t11
	MOV	t24, t11
	MOV	t34, t11
	MOV	t44, t11

	LD	a1,  0 * SIZE(AO)					#	AO point to the beginning address of sa
	LD	a2,  1 * SIZE(AO)					#	get 4 a

	LD	b1,  0 * SIZE(B)					#	B point to the beginning address of every panel Bj
	LD	b2,  1 * SIZE(B)					#	get 4 b
	LD	b3,  2 * SIZE(B)
	LD	b4,  3 * SIZE(B)

	dsra	L,  KK, 2						#	L=KK/4, KK is the length of the retangular data part of Bj
	blez	L, .L25
	move	BO,  B							#	reset B

.L22:
	LD	a5,  2 * SIZE(AO)
	LD	a6,  3 * SIZE(AO)

	LD	b5,  4 * SIZE(BO)
	LD	b6,  5 * SIZE(BO)
	LD	b7,  6 * SIZE(BO)
	LD	b8,  7 * SIZE(BO)

	MADD	t11, t11, a1, b1
	MADD	t21, t21, a2, b1

	MADD	t12, t12, a1, b2
	MADD	t22, t22, a2, b2

	MADD	t13, t13, a1, b3
	MADD	t23, t23, a2, b3

	MADD	t14, t14, a1, b4
	MADD	t24, t24, a2, b4

	LD	a3,  4 * SIZE(AO)
	LD	a4,  5 * SIZE(AO)

	LD	b1,  8 * SIZE(BO)
	LD	b2,  9 * SIZE(BO)
	LD	b3,  10 * SIZE(BO)
	LD	b4,  11 * SIZE(BO)

	MADD	t11, t11, a5, b5
	MADD	t21, t21, a6, b5

	MADD	t12, t12, a5, b6
	MADD	t22, t22, a6, b6

	MADD	t13, t13, a5, b7
	MADD	t23, t23, a6, b7

	MADD	t14, t14, a5, b8
	MADD	t24, t24, a6, b8

	LD	a7,  6 * SIZE(AO)
	LD	a8,  7 * SIZE(AO)

	LD	b5,  12 * SIZE(BO)
	LD	b6,  13 * SIZE(BO)
	LD	b7,  14 * SIZE(BO)
	LD	b8,  15 * SIZE(BO)

	MADD	t11, t11, a3, b1
	MADD	t21, t21, a4, b1

	MADD	t12, t12, a3, b2
	MADD	t22, t22, a4, b2

	MADD	t13, t13, a3, b3
	MADD	t23, t23, a4, b3

	MADD	t14, t14, a3, b4
	MADD	t24, t24, a4, b4

	daddiu	AO, AO, 8 * SIZE			#	AO += 2mr*4kr
	daddiu	BO, BO, 16 * SIZE			#	BP += 4nr*4kr

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	MADD	t11, t11, a7, b5
	MADD	t21, t21, a8, b5

	MADD	t12, t12, a7, b6
	MADD	t22, t22, a8, b6

	MADD	t13, t13, a7, b7
	MADD	t23, t23, a8, b7

	MADD	t14, t14, a7, b8
	MADD	t24, t24, a8, b8

	daddiu	L, L, -1
	bgtz	L, .L22
	NOP


.L25:
	andi	L, KK, 3					#	deal with kc remainder part
	blez	L, .L28
	NOP

	.align	3
.L26:
	MADD	t11, t11, a1, b1
	MADD	t21, t21, a2, b1

	MADD	t12, t12, a1, b2
	MADD	t22, t22, a2, b2

	MADD	t13, t13, a1, b3
	MADD	t23, t23, a2, b3

	MADD	t14, t14, a1, b4
	MADD	t24, t24, a2, b4

	daddiu	AO, AO, 2 * SIZE			#	AO += 2mr
	daddiu	BO, BO, 4 * SIZE			#	BP += 4nr

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L26
	NOP


	.align 3
.L28:									#	.L18 always deal with the trigular data part
 	LD	b1,  0 * SIZE(AO)				#	for RN & RT A is the result matrix
	LD	b2,  1 * SIZE(AO)				#	Fixed results

	SUB	t11, b1, t11
	SUB	t21, b2, t21

 	LD	b5,  2 * SIZE(AO)
	LD	b6,  3 * SIZE(AO)

	SUB	t12, b5, t12
	SUB	t22, b6, t22

	LD	b3,  4 * SIZE(AO)
	LD	b4,  5 * SIZE(AO)

	SUB	t13, b3, t13
	SUB	t23, b4, t23

	LD	b7,  6 * SIZE(AO)
	LD	b8,  7 * SIZE(AO)

	SUB	t14, b7, t14
	SUB	t24, b8, t24



	LD	b1,  0 * SIZE(BO)				#	BO point to the beginning of the trigular data part of Bj
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)
	MUL	t11, b1, t11
	MUL	t21, b1, t21
	NMSUB	t12, t12, b2, t11
	NMSUB	t22, t22, b2, t21
	NMSUB	t13, t13, b3, t11
	NMSUB	t23, t23, b3, t21
	NMSUB	t14, t14, b4, t11
	NMSUB	t24, t24, b4, t21


	LD	b5,  5 * SIZE(BO)
	LD	b6,  6 * SIZE(BO)
	LD	b7,  7 * SIZE(BO)
	MUL	t12, b5, t12
	MUL	t22, b5, t22
	NMSUB	t13, t13, b6, t12
	NMSUB	t23, t23, b6, t22
	NMSUB	t14, t14, b7, t12
	NMSUB	t24, t24, b7, t22



	LD	b8,  10 * SIZE(BO)
	LD	b1,  11 * SIZE(BO)
	MUL	t13, b8, t13
	MUL	t23, b8, t23
	NMSUB	t14, t14, b1, t13
	NMSUB	t24, t24, b1, t23



	LD	b2,  15 * SIZE(BO)
	MUL	t14, b2, t14
	MUL	t24, b2, t24



	ST	t11,  0 * SIZE(AO)				#	update packed blockA for follow-up compute
	ST	t21,  1 * SIZE(AO)

	ST	t12,  2 * SIZE(AO)
	ST	t22,  3 * SIZE(AO)

	ST	t13,  4 * SIZE(AO)
	ST	t23,  5 * SIZE(AO)

	ST	t14,  6 * SIZE(AO)
	ST	t24,  7 * SIZE(AO)


	ST	t11,  0 * SIZE(CO1)				#	write back results
	ST	t21,  1 * SIZE(CO1)

	ST	t12,  0 * SIZE(CO2)
	ST	t22,  1 * SIZE(CO2)

	ST	t13,  0 * SIZE(CO3)
	ST	t23,  1 * SIZE(CO3)

	ST	t14,  0 * SIZE(CO4)
	ST	t24,  1 * SIZE(CO4)

	daddiu	CO1, CO1, 2 * SIZE			#	fixed address
	daddiu	CO2, CO2, 2 * SIZE			#	mr=2
	daddiu	CO3, CO3, 2 * SIZE
	daddiu	CO4, CO4, 2 * SIZE


	dsubu	TEMP, K, KK					#	temp = kc - retangular data length of every panel
	dsll	L,    TEMP, 1 + BASE_SHIFT	#	mr=2
	dsll	TEMP, TEMP, 2 + BASE_SHIFT
	daddu	AO, AO, L					#	move AO to the end of this panel. also the beginning of next panel
	daddu	BO, BO, TEMP				#	move BO to the end of this panel

	.align 3
.L50:
	andi	I,  M, 1					#	mr=1
	blez	I, .L29
	nop

	MTC	$0,  t11							#	clear results registers
	MOV	t21, t11
	MOV	t31, t11
	MOV	t41, t11

	MOV	t12, t11
	MOV	t22, t11
	MOV	t32, t11
	MOV	t42, t11

	MOV	t13, t11
	MOV	t23, t11
	MOV	t33, t11
	MOV	t43, t11

	MOV	t14, t11
	MOV	t24, t11
	MOV	t34, t11
	MOV	t44, t11

	LD	a1,  0 * SIZE(AO)					#	AO point to the beginning address of sa

	LD	b1,  0 * SIZE(B)					#	B point to the beginning address of every panel Bj
	LD	b2,  1 * SIZE(B)					#	get 4 b
	LD	b3,  2 * SIZE(B)
	LD	b4,  3 * SIZE(B)

	dsra	L,  KK, 2						#	L=KK/4, KK is the length of the retangular data part of Bj
	blez	L, .L55
	move	BO,  B							#	reset B

.L52:
	LD	a5,  1 * SIZE(AO)

	LD	b5,  4 * SIZE(BO)
	LD	b6,  5 * SIZE(BO)
	LD	b7,  6 * SIZE(BO)
	LD	b8,  7 * SIZE(BO)

	MADD	t11, t11, a1, b1
	MADD	t12, t12, a1, b2
	MADD	t13, t13, a1, b3
	MADD	t14, t14, a1, b4

	LD	a3,  2 * SIZE(AO)

	LD	b1,  8 * SIZE(BO)
	LD	b2,  9 * SIZE(BO)
	LD	b3,  10 * SIZE(BO)
	LD	b4,  11 * SIZE(BO)

	MADD	t11, t11, a5, b5
	MADD	t12, t12, a5, b6
	MADD	t13, t13, a5, b7
	MADD	t14, t14, a5, b8

	LD	a7,  3 * SIZE(AO)

	LD	b5,  12 * SIZE(BO)
	LD	b6,  13 * SIZE(BO)
	LD	b7,  14 * SIZE(BO)
	LD	b8,  15 * SIZE(BO)

	MADD	t11, t11, a3, b1
	MADD	t12, t12, a3, b2
	MADD	t13, t13, a3, b3
	MADD	t14, t14, a3, b4

	daddiu	AO, AO, 4 * SIZE			#	AO += 1mr*4kr
	daddiu	BO, BO, 16 * SIZE			#	BP += 4nr*4kr

	LD	a1,  0 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	MADD	t11, t11, a7, b5
	MADD	t12, t12, a7, b6
	MADD	t13, t13, a7, b7
	MADD	t14, t14, a7, b8

	daddiu	L, L, -1
	bgtz	L, .L52
	NOP


.L55:
	andi	L, KK, 3					#	deal with kc remainder part
	blez	L, .L58
	NOP

	.align	3
.L56:
	MADD	t11, t11, a1, b1
	MADD	t12, t12, a1, b2
	MADD	t13, t13, a1, b3
	MADD	t14, t14, a1, b4

	daddiu	AO, AO, 1 * SIZE			#	AO += 1mr
	daddiu	BO, BO, 4 * SIZE			#	BP += 4nr

	LD	a1,  0 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L56
	NOP


	.align 3
.L58:									#	.L18 always deal with the trigular data part
 	LD	b1,  0 * SIZE(AO)				#	for RN & RT A is the result matrix
 	LD	b5,  1 * SIZE(AO)
	LD	b3,  2 * SIZE(AO)
	LD	b7,  3 * SIZE(AO)

	SUB	t11, b1, t11
	SUB	t12, b5, t12
	SUB	t13, b3, t13
	SUB	t14, b7, t14



	LD	b1,  0 * SIZE(BO)				#	BO point to the beginning of the trigular data part of Bj
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)
	MUL	t11, b1, t11
	NMSUB	t12, t12, b2, t11
	NMSUB	t13, t13, b3, t11
	NMSUB	t14, t14, b4, t11


	LD	b5,  5 * SIZE(BO)
	LD	b6,  6 * SIZE(BO)
	LD	b7,  7 * SIZE(BO)
	MUL	t12, b5, t12
	NMSUB	t13, t13, b6, t12
	NMSUB	t14, t14, b7, t12


	LD	b8,  10 * SIZE(BO)
	LD	b1,  11 * SIZE(BO)
	MUL	t13, b8, t13
	NMSUB	t14, t14, b1, t13


	LD	b2,  15 * SIZE(BO)
	MUL	t14, b2, t14



	ST	t11,  0 * SIZE(AO)				#	update packed blockA for follow-up compute
	ST	t12,  1 * SIZE(AO)
	ST	t13,  2 * SIZE(AO)
	ST	t14,  3 * SIZE(AO)


	ST	t11,  0 * SIZE(CO1)				#	write back results
	ST	t12,  0 * SIZE(CO2)
	ST	t13,  0 * SIZE(CO3)
	ST	t14,  0 * SIZE(CO4)

	daddiu	CO1, CO1, 1 * SIZE			#	fixed address
	daddiu	CO2, CO2, 1 * SIZE			#	mr=2
	daddiu	CO3, CO3, 1 * SIZE
	daddiu	CO4, CO4, 1 * SIZE


	dsubu	TEMP, K, KK					#	temp = kc - retangular data length of every panel
	dsll	L,    TEMP, BASE_SHIFT	#	mr=2
	dsll	TEMP, TEMP, 2 + BASE_SHIFT
	daddu	AO, AO, L					#	move AO to the end of this panel. also the beginning of next panel
	daddu	BO, BO, TEMP				#	move BO to the end of this panel


	.align 3
.L29:
	move	B,  BO						#	change to next panel of Bj
	daddiu	KK, KK,  4					#	rectangular data length increase by 4
	bgtz	J, .L10
	NOP


	.align 3

.L30:
	andi	J,  N, 2
	blez	J, .L70
	nop

	move	CO1, C
	daddu	CO2, C,   LDC

	move	AO, A							#	A is the retangular matrix and B is the trigular matrix
	daddu	C,  CO2, LDC					#	Fixed pointer C

	dsra	I,  M, 2						#	I=MC/4
	blez	I, .L40
	NOP

	.align 3
.L31:
	MTC	$0,  t11							#	clear results registers
	MOV	t21, t11
	MOV	t31, t11
	MOV	t41, t11

	MOV	t12, t11
	MOV	t22, t11
	MOV	t32, t11
	MOV	t42, t11

	LD	a1,  0 * SIZE(AO)					#	AO point to the beginning address of sa
	LD	a2,  1 * SIZE(AO)					#	get 4 a
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  0 * SIZE(B)					#	B point to the beginning address of every panel Bj
	LD	b2,  1 * SIZE(B)					#	get 4 b

	dsra	L,  KK, 2						#	L=KK/4, KK is the length of the retangular data part of Bj
	blez	L, .L35
	move	BO,  B							#	reset B

.L32:
	LD	a5,  4 * SIZE(AO)
	LD	a6,  5 * SIZE(AO)
	LD	a7,  6 * SIZE(AO)
	LD	a8,  7 * SIZE(AO)

	LD	b5,  2 * SIZE(BO)
	LD	b6,  3 * SIZE(BO)

	MADD	t11, t11, a1, b1
	MADD	t21, t21, a2, b1
	MADD	t31, t31, a3, b1
	MADD	t41, t41, a4, b1

	MADD	t12, t12, a1, b2
	MADD	t22, t22, a2, b2
	MADD	t32, t32, a3, b2
	MADD	t42, t42, a4, b2

	LD	a1,  8 * SIZE(AO)
	LD	a2,  9 * SIZE(AO)
	LD	a3,  10 * SIZE(AO)
	LD	a4,  11 * SIZE(AO)

	LD	b3,  4 * SIZE(BO)
	LD	b4,  5 * SIZE(BO)

	MADD	t11, t11, a5, b5
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

	MADD	t11, t11, a1, b3
	MADD	t21, t21, a2, b3
	MADD	t31, t31, a3, b3
	MADD	t41, t41, a4, b3

	MADD	t12, t12, a1, b4
	MADD	t22, t22, a2, b4
	MADD	t32, t32, a3, b4
	MADD	t42, t42, a4, b4

	daddiu	AO, AO, 16 * SIZE			#	AO += 4mr*4kr
	daddiu	BO, BO,  8 * SIZE			#	BP += 2nr*4kr

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	MADD	t11, t11, a5, b7
	MADD	t21, t21, a6, b7
	MADD	t31, t31, a7, b7
	MADD	t41, t41, a8, b7

	MADD	t12, t12, a5, b8
	MADD	t22, t22, a6, b8
	MADD	t32, t32, a7, b8
	MADD	t42, t42, a8, b8

	daddiu	L, L, -1
	bgtz	L, .L32
	NOP


.L35:
	andi	L, KK, 3					#	deal with kc remainder part
	blez	L, .L38
	NOP

	.align	3
.L36:
	MADD	t11, t11, a1, b1
	MADD	t21, t21, a2, b1
	MADD	t31, t31, a3, b1
	MADD	t41, t41, a4, b1

	MADD	t12, t12, a1, b2
	MADD	t22, t22, a2, b2
	MADD	t32, t32, a3, b2
	MADD	t42, t42, a4, b2

	daddiu	AO, AO, 4 * SIZE			#	AO += 4mr
	daddiu	BO, BO, 2 * SIZE			#	BP += 2nr

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L36
	NOP


	.align 3
.L38:									#	.L38 always deal with the trigular data part
 	LD	b1,  0 * SIZE(AO)				#	for RN & RT A is the result matrix
	LD	b2,  1 * SIZE(AO)				#	Fixed results
	LD	b3,  2 * SIZE(AO)
	LD	b4,  3 * SIZE(AO)				#	sa stored as col major

	SUB	t11, b1, t11
	SUB	t21, b2, t21
	SUB	t31, b3, t31
	SUB	t41, b4, t41

 	LD	b5,  4 * SIZE(AO)
	LD	b6,  5 * SIZE(AO)
	LD	b7,  6 * SIZE(AO)
	LD	b8,  7 * SIZE(AO)

	SUB	t12, b5, t12
	SUB	t22, b6, t22
	SUB	t32, b7, t32
	SUB	t42, b8, t42


	LD	b1,  0 * SIZE(BO)				#	BO point to the beginning of the trigular data part of Bj
	LD	b2,  1 * SIZE(BO)
	MUL	t11, b1, t11
	MUL	t21, b1, t21
	MUL	t31, b1, t31
	MUL	t41, b1, t41
	NMSUB	t12, t12, b2, t11
	NMSUB	t22, t22, b2, t21
	NMSUB	t32, t32, b2, t31
	NMSUB	t42, t42, b2, t41

	LD	b5,  3 * SIZE(BO)
	MUL	t12, b5, t12
	MUL	t22, b5, t22
	MUL	t32, b5, t32
	MUL	t42, b5, t42


	ST	t11,  0 * SIZE(AO)				#	update packed blockA for follow-up compute
	ST	t21,  1 * SIZE(AO)
	ST	t31,  2 * SIZE(AO)
	ST	t41,  3 * SIZE(AO)

	ST	t12,  4 * SIZE(AO)
	ST	t22,  5 * SIZE(AO)
	ST	t32,  6 * SIZE(AO)
	ST	t42,  7 * SIZE(AO)

	ST	t11,  0 * SIZE(CO1)				#	write back results
	ST	t21,  1 * SIZE(CO1)
	ST	t31,  2 * SIZE(CO1)
	ST	t41,  3 * SIZE(CO1)

	ST	t12,  0 * SIZE(CO2)
	ST	t22,  1 * SIZE(CO2)
	ST	t32,  2 * SIZE(CO2)
	ST	t42,  3 * SIZE(CO2)

	daddiu	CO1, CO1, 4 * SIZE			#	fixed address
	daddiu	CO2, CO2, 4 * SIZE

	dsubu	TEMP, K, KK					#	temp = kc - retangular data length of every panel
	dsll	L,    TEMP, 2 + BASE_SHIFT
	dsll	TEMP, TEMP, 1 + BASE_SHIFT	#	nr=2
	daddu	AO, AO, L					#	move AO to the end of this panel. also the beginning of next panel
	daddu	BO, BO, TEMP				#	move BO to the end of this panel

	daddiu	I, I, -1
	bgtz	I, .L31
	NOP

	.align 3
.L40:
	andi	I, M,2
	blez	I,.L60
	nop

	MTC	$0,  t11							#	clear results registers
	MOV	t21, t11

	MOV	t12, t11
	MOV	t22, t11

	LD	a1,  0 * SIZE(AO)					#	AO point to the beginning address of sa
	LD	a2,  1 * SIZE(AO)					#	get 4 a

	LD	b1,  0 * SIZE(B)					#	B point to the beginning address of every panel Bj
	LD	b2,  1 * SIZE(B)					#	get 4 b

	dsra	L,  KK, 2						#	L=KK/4, KK is the length of the retangular data part of Bj
	blez	L, .L45
	move	BO,  B							#	reset B

.L42:
	LD	a5,  2 * SIZE(AO)
	LD	a6,  3 * SIZE(AO)
	LD	b5,  2 * SIZE(BO)
	LD	b6,  3 * SIZE(BO)

	MADD	t11, t11, a1, b1
	MADD	t21, t21, a2, b1
	MADD	t12, t12, a1, b2
	MADD	t22, t22, a2, b2

	LD	a3,  4 * SIZE(AO)
	LD	a4,  5 * SIZE(AO)
	LD	b3,  4 * SIZE(BO)
	LD	b4,  5 * SIZE(BO)

	MADD	t11, t11, a5, b5
	MADD	t21, t21, a6, b5
	MADD	t12, t12, a5, b6
	MADD	t22, t22, a6, b6

	LD	a7,  6 * SIZE(AO)
	LD	a8,  7 * SIZE(AO)
	LD	b7,  6 * SIZE(BO)
	LD	b8,  7 * SIZE(BO)

	MADD	t11, t11, a3, b3
	MADD	t21, t21, a4, b3
	MADD	t12, t12, a3, b4
	MADD	t22, t22, a4, b4

	daddiu	AO, AO,  8 * SIZE			#	AO += 2mr*4kr
	daddiu	BO, BO,  8 * SIZE			#	BP += 2nr*4kr

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	MADD	t11, t11, a7, b7
	MADD	t21, t21, a8, b7
	MADD	t12, t12, a7, b8
	MADD	t22, t22, a8, b8

	daddiu	L, L, -1
	bgtz	L, .L42
	NOP


.L45:
	andi	L, KK, 3					#	deal with kc remainder part
	blez	L, .L48
	NOP

	.align	3
.L46:
	MADD	t11, t11, a1, b1
	MADD	t21, t21, a2, b1
	MADD	t12, t12, a1, b2
	MADD	t22, t22, a2, b2

	daddiu	AO, AO, 2 * SIZE			#	AO += 2mr
	daddiu	BO, BO, 2 * SIZE			#	BP += 2nr

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L46
	NOP


	.align 3
.L48:									#	.L48 always deal with the trigular data part
 	LD	b1,  0 * SIZE(AO)				#	for RN & RT A is the result matrix
	LD	b2,  1 * SIZE(AO)				#	Fixed results

	SUB	t11, b1, t11
	SUB	t21, b2, t21

 	LD	b5,  2 * SIZE(AO)
	LD	b6,  3 * SIZE(AO)

	SUB	t12, b5, t12
	SUB	t22, b6, t22


	LD	b1,  0 * SIZE(BO)				#	BO point to the beginning of the trigular data part of Bj
	LD	b2,  1 * SIZE(BO)
	MUL	t11, b1, t11
	MUL	t21, b1, t21
	NMSUB	t12, t12, b2, t11
	NMSUB	t22, t22, b2, t21

	LD	b5,  3 * SIZE(BO)
	MUL	t12, b5, t12
	MUL	t22, b5, t22


	ST	t11,  0 * SIZE(AO)				#	update packed blockA for follow-up compute
	ST	t21,  1 * SIZE(AO)
	ST	t12,  2 * SIZE(AO)
	ST	t22,  3 * SIZE(AO)

	ST	t11,  0 * SIZE(CO1)				#	write back results
	ST	t21,  1 * SIZE(CO1)
	ST	t12,  0 * SIZE(CO2)
	ST	t22,  1 * SIZE(CO2)

	daddiu	CO1, CO1, 2 * SIZE			#	fixed address
	daddiu	CO2, CO2, 2 * SIZE

	dsubu	TEMP, K, KK					#	temp = kc - retangular data length of every panel
	dsll	L,    TEMP, 1 + BASE_SHIFT
	dsll	TEMP, TEMP, 1 + BASE_SHIFT	#	nr=2
	daddu	AO, AO, L					#	move AO to the end of this panel. also the beginning of next panel
	daddu	BO, BO, TEMP				#	move BO to the end of this panel


	.align 3
.L60:
	andi	I,M,1						#	nr=2 mr=1
	blez	I,.L39
	nop

	MTC	$0,  t11							#	clear results registers
	MOV	t12, t11

	LD	a1,  0 * SIZE(AO)					#	AO point to the beginning address of sa

	LD	b1,  0 * SIZE(B)					#	B point to the beginning address of every panel Bj
	LD	b2,  1 * SIZE(B)					#	get 4 b

	dsra	L,  KK, 2						#	L=KK/4, KK is the length of the retangular data part of Bj
	blez	L, .L65
	move	BO,  B							#	reset B

.L62:
	LD	a5,  1 * SIZE(AO)
	LD	b5,  2 * SIZE(BO)
	LD	b6,  3 * SIZE(BO)

	MADD	t11, t11, a1, b1
	MADD	t12, t12, a1, b2

	LD	a3,  2 * SIZE(AO)
	LD	b3,  4 * SIZE(BO)
	LD	b4,  5 * SIZE(BO)

	MADD	t11, t11, a5, b5
	MADD	t12, t12, a5, b6

	LD	a7,  3 * SIZE(AO)
	LD	b7,  6 * SIZE(BO)
	LD	b8,  7 * SIZE(BO)

	MADD	t11, t11, a3, b3
	MADD	t12, t12, a3, b4

	daddiu	AO, AO,  4 * SIZE			#	AO += 1mr*4kr
	daddiu	BO, BO,  8 * SIZE			#	BP += 2nr*4kr

	LD	a1,  0 * SIZE(AO)
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	MADD	t11, t11, a7, b7
	MADD	t12, t12, a7, b8

	daddiu	L, L, -1
	bgtz	L, .L62
	NOP


.L65:
	andi	L, KK, 3					#	deal with kc remainder part
	blez	L, .L68
	NOP

	.align	3
.L66:
	MADD	t11, t11, a1, b1
	MADD	t12, t12, a1, b2

	daddiu	AO, AO, 1 * SIZE			#	AO += mr
	daddiu	BO, BO, 2 * SIZE			#	BP += 2nr

	LD	a1,  0 * SIZE(AO)
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L66
	NOP


	.align 3
.L68:									#	.L48 always deal with the trigular data part
 	LD	b1,  0 * SIZE(AO)				#	for RN & RT A is the result matrix
	LD	b5,  1 * SIZE(AO)				#	Fixed results

	SUB	t11, b1, t11
	SUB	t12, b5, t12


	LD	b1,  0 * SIZE(BO)				#	BO point to the beginning of the trigular data part of Bj
	LD	b2,  1 * SIZE(BO)
	MUL	t11, b1, t11
	NMSUB	t12, t12, b2, t11

	LD	b5,  3 * SIZE(BO)
	MUL	t12, b5, t12


	ST	t11,  0 * SIZE(AO)				#	update packed blockA for follow-up compute
	ST	t12,  1 * SIZE(AO)

	ST	t11,  0 * SIZE(CO1)				#	write back results
	ST	t12,  0 * SIZE(CO2)

	daddiu	CO1, CO1, 1 * SIZE			#	fixed address
	daddiu	CO2, CO2, 1 * SIZE

	dsubu	TEMP, K, KK					#	temp = kc - retangular data length of every panel
	dsll	L,    TEMP, BASE_SHIFT		#	mr=1
	dsll	TEMP, TEMP, 1 + BASE_SHIFT	#	nr=2
	daddu	AO, AO, L					#	move AO to the end of this panel. also the beginning of next panel
	daddu	BO, BO, TEMP				#	move BO to the end of this panel


	.align 3
.L39:
	move	B,  BO						#	change to next panel of Bj
	daddiu	KK, KK,  2					#	rectangular data length increase by 4



	.align 3

.L70:
	andi	J,  N, 1					#	nr=1
	blez	J, .L999
	NOP

	move	CO1, C
	move	AO, A

	daddu	C,  CO1, LDC

	dsra	I,  M, 2					#	I=MC/4
	blez	I, .L80
	NOP

	.align 3
.L71:
	MTC	$0,  t11							#	clear results registers
	MOV	t21, t11
	MOV	t31, t11
	MOV	t41, t11

	LD	a1,  0 * SIZE(AO)					#	AO point to the beginning address of sa
	LD	a2,  1 * SIZE(AO)					#	get 4 a
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  0 * SIZE(B)					#	B point to the beginning address of every panel Bj

	dsra	L,  KK, 2						#	L=KK/4, KK is the length of the retangular data part of Bj
	blez	L, .L75
	move	BO,  B							#	reset B

.L72:
	LD	a5,  4 * SIZE(AO)
	LD	a6,  5 * SIZE(AO)
	LD	a7,  6 * SIZE(AO)
	LD	a8,  7 * SIZE(AO)

	LD	b5,  1 * SIZE(BO)

	MADD	t11, t11, a1, b1
	MADD	t21, t21, a2, b1
	MADD	t31, t31, a3, b1
	MADD	t41, t41, a4, b1

	LD	a1,  8 * SIZE(AO)
	LD	a2,  9 * SIZE(AO)
	LD	a3,  10 * SIZE(AO)
	LD	a4,  11 * SIZE(AO)

	LD	b3,  2 * SIZE(BO)

	MADD	t11, t11, a5, b5
	MADD	t21, t21, a6, b5
	MADD	t31, t31, a7, b5
	MADD	t41, t41, a8, b5

	LD	a5,  12 * SIZE(AO)
	LD	a6,  13 * SIZE(AO)
	LD	a7,  14 * SIZE(AO)
	LD	a8,  15 * SIZE(AO)

	LD	b7,  3 * SIZE(BO)

	MADD	t11, t11, a1, b3
	MADD	t21, t21, a2, b3
	MADD	t31, t31, a3, b3
	MADD	t41, t41, a4, b3

	daddiu	AO, AO, 16 * SIZE			#	AO += 4mr*4kr
	daddiu	BO, BO,  4 * SIZE			#	BP += 1nr*4kr

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)

	MADD	t11, t11, a5, b7
	MADD	t21, t21, a6, b7
	MADD	t31, t31, a7, b7
	MADD	t41, t41, a8, b7

	daddiu	L, L, -1
	bgtz	L, .L72
	NOP


.L75:
	andi	L, KK, 3					#	deal with kc remainder part
	blez	L, .L78
	NOP

	.align	3
.L76:
	MADD	t11, t11, a1, b1
	MADD	t21, t21, a2, b1
	MADD	t31, t31, a3, b1
	MADD	t41, t41, a4, b1

	daddiu	AO, AO, 4 * SIZE			#	AO += 4mr
	daddiu	BO, BO, 1 * SIZE			#	BP += 1nr

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L76
	NOP


	.align 3
.L78:									#	.L78 always deal with the trigular data part
 	LD	b1,  0 * SIZE(AO)				#	for RN & RT A is the result matrix
	LD	b2,  1 * SIZE(AO)				#	Fixed results
	LD	b3,  2 * SIZE(AO)
	LD	b4,  3 * SIZE(AO)				#	sa stored as col major

	SUB	t11, b1, t11
	SUB	t21, b2, t21
	SUB	t31, b3, t31
	SUB	t41, b4, t41


	LD	b1,  0 * SIZE(BO)				#	BO point to the beginning of the trigular data part of Bj
	MUL	t11, b1, t11
	MUL	t21, b1, t21
	MUL	t31, b1, t31
	MUL	t41, b1, t41


	ST	t11,  0 * SIZE(AO)				#	update packed blockA for follow-up compute
	ST	t21,  1 * SIZE(AO)
	ST	t31,  2 * SIZE(AO)
	ST	t41,  3 * SIZE(AO)

	ST	t11,  0 * SIZE(CO1)				#	write back results
	ST	t21,  1 * SIZE(CO1)
	ST	t31,  2 * SIZE(CO1)
	ST	t41,  3 * SIZE(CO1)


	daddiu	CO1, CO1, 4 * SIZE			#	fixed address

	dsubu	TEMP, K, KK					#	temp = kc - retangular data length of every panel
	dsll	L,    TEMP, 2 + BASE_SHIFT
	dsll	TEMP, TEMP, BASE_SHIFT		#	nr=1
	daddu	AO, AO, L					#	move AO to the end of this panel. also the beginning of next panel
	daddu	BO, BO, TEMP				#	move BO to the end of this panel

	daddiu	I, I, -1
	bgtz	I, .L71
	NOP


	.align 3
.L80:
	andi	I, M, 2						#	mr=2
	blez	I, .L90
	nop

	MTC	$0,  t11							#	clear results registers
	MOV	t21, t11

	LD	a1,  0 * SIZE(AO)					#	AO point to the beginning address of sa
	LD	a2,  1 * SIZE(AO)					#	get 4 a

	LD	b1,  0 * SIZE(B)					#	B point to the beginning address of every panel Bj

	dsra	L,  KK, 2						#	L=KK/4, KK is the length of the retangular data part of Bj
	blez	L, .L85
	move	BO,  B							#	reset B

.L82:
	LD	a5,  2 * SIZE(AO)
	LD	a6,  3 * SIZE(AO)

	LD	b5,  1 * SIZE(BO)

	MADD	t11, t11, a1, b1
	MADD	t21, t21, a2, b1

	LD	a3,  4 * SIZE(AO)
	LD	a4,  5 * SIZE(AO)

	LD	b3,  2 * SIZE(BO)

	MADD	t11, t11, a5, b5
	MADD	t21, t21, a6, b5

	LD	a7,  6 * SIZE(AO)
	LD	a8,  7 * SIZE(AO)

	LD	b7,  3 * SIZE(BO)

	MADD	t11, t11, a3, b3
	MADD	t21, t21, a4, b3

	daddiu	AO, AO,  8 * SIZE			#	AO += 2mr*4kr
	daddiu	BO, BO,  4 * SIZE			#	BP += 1nr*4kr

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)

	MADD	t11, t11, a7, b7
	MADD	t21, t21, a8, b7

	daddiu	L, L, -1
	bgtz	L, .L82
	NOP


.L85:
	andi	L, KK, 3					#	deal with kc remainder part
	blez	L, .L88
	NOP

	.align	3
.L86:
	MADD	t11, t11, a1, b1
	MADD	t21, t21, a2, b1

	daddiu	AO, AO, 2 * SIZE			#	AO += 2mr
	daddiu	BO, BO, 1 * SIZE			#	BP += 1nr

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L86
	NOP


	.align 3
.L88:									#	.L88 always deal with the trigular data part
 	LD	b1,  0 * SIZE(AO)				#	for RN & RT A is the result matrix
	LD	b2,  1 * SIZE(AO)				#	Fixed results

	SUB	t11, b1, t11
	SUB	t21, b2, t21


	LD	b1,  0 * SIZE(BO)				#	BO point to the beginning of the trigular data part of Bj
	MUL	t11, b1, t11
	MUL	t21, b1, t21


	ST	t11,  0 * SIZE(AO)				#	update packed blockA for follow-up compute
	ST	t21,  1 * SIZE(AO)

	ST	t11,  0 * SIZE(CO1)				#	write back results
	ST	t21,  1 * SIZE(CO1)


	daddiu	CO1, CO1, 2 * SIZE			#	fixed address

	dsubu	TEMP, K, KK					#	temp = kc - retangular data length of every panel
	dsll	L,    TEMP, 1 + BASE_SHIFT
	dsll	TEMP, TEMP, BASE_SHIFT		#	nr=1
	daddu	AO, AO, L					#	move AO to the end of this panel. also the beginning of next panel
	daddu	BO, BO, TEMP				#	move BO to the end of this panel


	.align 3
.L90:
	andi	I, M, 1						#	mr=1
	blez	I, .L79
	nop

	MTC	$0,  t11							#	clear results registers

	LD	a1,  0 * SIZE(AO)					#	AO point to the beginning address of sa
	LD	b1,  0 * SIZE(B)					#	B point to the beginning address of every panel Bj

	dsra	L,  KK, 2						#	L=KK/4, KK is the length of the retangular data part of Bj
	blez	L, .L95
	move	BO,  B							#	reset B

.L92:
	LD	a5,  1 * SIZE(AO)
	LD	b5,  1 * SIZE(BO)

	MADD	t11, t11, a1, b1

	LD	a3,  2 * SIZE(AO)
	LD	b3,  2 * SIZE(BO)

	MADD	t11, t11, a5, b5

	LD	a7,  3 * SIZE(AO)
	LD	b7,  3 * SIZE(BO)

	MADD	t11, t11, a3, b3

	daddiu	AO, AO,  4 * SIZE			#	AO += 1mr*4kr
	daddiu	BO, BO,  4 * SIZE			#	BP += 1nr*4kr

	LD	a1,  0 * SIZE(AO)
	LD	b1,  0 * SIZE(BO)

	MADD	t11, t11, a7, b7

	daddiu	L, L, -1
	bgtz	L, .L92
	NOP


.L95:
	andi	L, KK, 3					#	deal with kc remainder part
	blez	L, .L98
	NOP

	.align	3
.L96:
	MADD	t11, t11, a1, b1

	daddiu	AO, AO, 1 * SIZE			#	AO += 2mr
	daddiu	BO, BO, 1 * SIZE			#	BP += 1nr

	LD	a1,  0 * SIZE(AO)
	LD	b1,  0 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L96
	NOP


	.align 3
.L98:									#	.L98 always deal with the trigular data part
 	LD	b1,  0 * SIZE(AO)				#	for RN & RT A is the result matrix

	SUB	t11, b1, t11


	LD	b1,  0 * SIZE(BO)				#	BO point to the beginning of the trigular data part of Bj
	MUL	t11, b1, t11


	ST	t11,  0 * SIZE(AO)				#	update packed blockA for follow-up compute

	ST	t11,  0 * SIZE(CO1)				#	write back results


	daddiu	CO1, CO1, 1 * SIZE			#	fixed address

	dsubu	TEMP, K, KK					#	temp = kc - retangular data length of every panel
	dsll	L,    TEMP, BASE_SHIFT
	dsll	TEMP, TEMP, BASE_SHIFT		#	nr=1
	daddu	AO, AO, L					#	move AO to the end of this panel. also the beginning of next panel
	daddu	BO, BO, TEMP				#	move BO to the end of this panel


	.align 3
.L79:
	move	B,  BO
	daddiu	KK, KK, 1


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
