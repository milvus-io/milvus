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


	.align	3								#	RT compute from right to left
	LDARG	OFFSET, 144($sp)				#	get the last parameter
	dsll	LDC, LDC, BASE_SHIFT			#	LDC * data_Byte

	mult	N, K
	mflo	TEMP

	dsll	TEMP, TEMP, BASE_SHIFT			#	B Representative triangle matrix!!!
	daddu	B, B, TEMP						#	B point to the end of sb
											#	Be carefull B has no effeck of mc!!
	mult	N, LDC
	mflo	TEMP
	daddu	C, C, TEMP						#	C point to the last colum of blockB

	dsubu	KK, K, OFFSET					#	KC-KK is the length of rectangular data part of Bj

	andi	J,  N, 1
	blez	J, .L30
	nop

	dsll	TEMP, K, BASE_SHIFT
	dsubu	B, B, TEMP						#	move B to the beginning address of Bj

	dsubu	C, C, LDC

	move	CO1, C

	move	AORIG, A

	dsra	I,  M, 2
	blez	I, .L80
	NOP

.L31:										#	mr=4,nr=1
	dsll	L,    KK, 2 + BASE_SHIFT		#	mr=4
	dsll	TEMP, KK, BASE_SHIFT			#	nr=1
	daddu	AO, AORIG, L
	daddu	BO, B, TEMP						#	BO point to the retangular data part,also reset BO
	dsubu	TEMP, K, KK						#	temp = the length of rectangular data part

	MTC	$0,  t11							#	clear 4 results registers
	MOV	t21, t11
	MOV	t31, t11
	MOV	t41, t11

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)

	dsra	L,  TEMP, 2						#	L=(KC-offset)/4
	blez	L, .L35
	NOP

	.align	3

.L32:
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
	daddiu	BO, BO,  4 * SIZE			#	BO += 1nr*4kr

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
	bgtz	L, .L32
	NOP


	.align 3

.L35:
	andi	L, TEMP, 3
	blez	L, .L38
	NOP
	.align	3

.L36:
	MADD	t11, t11, a1, b1
	MADD	t21, t21, a2, b1
	MADD	t31, t31, a3, b1
	MADD	t41, t41, a4, b1

	daddiu	AO, AO, 4 * SIZE			#	AO += 4mr
	daddiu	BO, BO, 1 * SIZE			#	BO += 2nr

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L36
	NOP


	.align
.L38:
	daddiu	TEMP, KK,  -1				#	deal with the triangular data part
	dsll	L,    TEMP, 2 + BASE_SHIFT
	dsll	TEMP, TEMP, BASE_SHIFT		#	nr=1
	daddu	AO, AORIG, L
	daddu	BO, B, TEMP					#	BO point to the trigular data part

 	LD	b1,  0 * SIZE(AO)				#	fixed results
	LD	b2,  1 * SIZE(AO)
	LD	b3,  2 * SIZE(AO)
	LD	b4,  3 * SIZE(AO)

	SUB	t11, b1, t11
	SUB	t21, b2, t21
	SUB	t31, b3, t31
	SUB	t41, b4, t41


	LD	b2,   0 * SIZE(BO)
	MUL	t11, b2, t11
	MUL	t21, b2, t21
	MUL	t31, b2, t31
	MUL	t41, b2, t41


	ST	t11,  0 * SIZE(AO)				# updata packed A
	ST	t21,  1 * SIZE(AO)
	ST	t31,  2 * SIZE(AO)
	ST	t41,  3 * SIZE(AO)

	ST	t11,  0 * SIZE(CO1)				#	write back
	ST	t21,  1 * SIZE(CO1)
	ST	t31,  2 * SIZE(CO1)
	ST	t41,  3 * SIZE(CO1)


	daddiu	CO1, CO1, 4 * SIZE			#	fixed pointer

	dsll	TEMP, K, 2 + BASE_SHIFT
	daddu	AORIG, AORIG, TEMP			# move to next panel Ai

	daddiu	I, I, -1
	bgtz	I, .L31
	NOP


	.align 3
.L80:
	andi	I, M, 2
	blez	I, .L90
	nop

	dsll	L,    KK, 1 + BASE_SHIFT		#	mr=2
	dsll	TEMP, KK, BASE_SHIFT			#	nr=1
	daddu	AO, AORIG, L
	daddu	BO, B, TEMP						#	BO point to the retangular data part,also reset BO
	dsubu	TEMP, K, KK						#	temp = the length of rectangular data part

	MTC	$0,  t11							#	clear 4 results registers
	MOV	t21, t11

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)

	dsra	L,  TEMP, 2						#	L=(KC-offset)/4
	blez	L, .L85
	NOP

	.align	3

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

	daddiu	AO, AO, 16 * SIZE			#	AO += 4mr*4kr
	daddiu	BO, BO,  4 * SIZE			#	BO += 1nr*4kr

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)

	MADD	t11, t11, a7, b7
	MADD	t21, t21, a8, b7

	daddiu	L, L, -1
	bgtz	L, .L82
	NOP


	.align 3

.L85:
	andi	L, TEMP, 3
	blez	L, .L88
	NOP
	.align	3

.L86:
	MADD	t11, t11, a1, b1
	MADD	t21, t21, a2, b1

	daddiu	AO, AO, 2 * SIZE			#	AO += 2mr
	daddiu	BO, BO, 1 * SIZE			#	BO += 1nr

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L86
	NOP


	.align
.L88:
	daddiu	TEMP, KK,  -1				#	deal with the triangular data part
	dsll	L,    TEMP, 1 + BASE_SHIFT
	dsll	TEMP, TEMP, BASE_SHIFT		#	nr=1
	daddu	AO, AORIG, L
	daddu	BO, B, TEMP					#	BO point to the trigular data part

 	LD	b1,  0 * SIZE(AO)				#	fixed results
	LD	b2,  1 * SIZE(AO)

	SUB	t11, b1, t11
	SUB	t21, b2, t21


	LD	b2,   0 * SIZE(BO)
	MUL	t11, b2, t11
	MUL	t21, b2, t21


	ST	t11,  0 * SIZE(AO)				# updata packed A
	ST	t21,  1 * SIZE(AO)

	ST	t11,  0 * SIZE(CO1)				#	write back
	ST	t21,  1 * SIZE(CO1)


	daddiu	CO1, CO1, 2 * SIZE			#	fixed pointer

	dsll	TEMP, K, 1 + BASE_SHIFT
	daddu	AORIG, AORIG, TEMP			# move to next panel Ai


	.align 3
.L90:
	andi	I, M, 1
	blez	I, .L39
	nop

	dsll	L,    KK, BASE_SHIFT			#	mr=1
	dsll	TEMP, KK, BASE_SHIFT			#	nr=1
	daddu	AO, AORIG, L
	daddu	BO, B, TEMP						#	BO point to the retangular data part,also reset BO
	dsubu	TEMP, K, KK						#	temp = the length of rectangular data part

	MTC	$0,  t11							#	clear 4 results registers

	LD	a1,  0 * SIZE(AO)
	LD	b1,  0 * SIZE(BO)

	dsra	L,  TEMP, 2						#	L=(KC-offset)/4
	blez	L, .L95
	NOP

	.align	3

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
	daddiu	BO, BO,  4 * SIZE			#	BO += 1nr*4kr

	LD	a1,  0 * SIZE(AO)
	LD	b1,  0 * SIZE(BO)

	MADD	t11, t11, a7, b7

	daddiu	L, L, -1
	bgtz	L, .L92
	NOP


	.align 3

.L95:
	andi	L, TEMP, 3
	blez	L, .L98
	NOP
	.align	3

.L96:
	MADD	t11, t11, a1, b1

	daddiu	AO, AO, 1 * SIZE			#	AO += 2mr
	daddiu	BO, BO, 1 * SIZE			#	BO += 1nr

	LD	a1,  0 * SIZE(AO)
	LD	b1,  0 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L96
	NOP


	.align
.L98:
	daddiu	TEMP, KK,  -1				#	deal with the triangular data part
	dsll	L,    TEMP, BASE_SHIFT
	dsll	TEMP, TEMP, BASE_SHIFT		#	nr=1
	daddu	AO, AORIG, L
	daddu	BO, B, TEMP					#	BO point to the trigular data part

 	LD	b1,  0 * SIZE(AO)				#	fixed results

	SUB	t11, b1, t11


	LD	b2,   0 * SIZE(BO)
	MUL	t11, b2, t11


	ST	t11,  0 * SIZE(AO)				# updata packed A

	ST	t11,  0 * SIZE(CO1)				#	write back


	daddiu	CO1, CO1, 1 * SIZE			#	fixed pointer

	dsll	TEMP, K, BASE_SHIFT
	daddu	AORIG, AORIG, TEMP			# move to next panel Ai


.L39:
	daddiu	KK, KK, -1						#	rectangular data length increased by 1


	.align 3
.L30:										#	nr=2
	andi	J,  N, 2
	blez	J, .L50
	nop

	dsll	TEMP, K, 1 + BASE_SHIFT			#	Kc*2nr	move B to the beginning address of Bj
	dsubu	B, B, TEMP

	dsll	TEMP, LDC, 1					#	C
	dsubu	C, C, TEMP

	move	CO1, C
	daddu	CO2, C,   LDC

	move	AORIG, A

	dsra	I,  M, 2
	blez	I, .L60
	NOP

.L51:										#	mr=4,nr=2
	dsll	L,    KK, 2 + BASE_SHIFT		#	mr=4
	dsll	TEMP, KK, 1 + BASE_SHIFT		#	nr=2
	daddu	AO, AORIG, L
	daddu	BO, B, TEMP						#	BO point to the retangular data part,also reset BO
	dsubu	TEMP, K, KK						#	temp = the length of rectangular data part

	MTC	$0,  t11							#	clear 8 results registers
	MOV	t21, t11
	MOV	t31, t11
	MOV	t41, t11
	MOV	t12, t11
	MOV	t22, t11
	MOV	t32, t11
	MOV	t42, t11

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	dsra	L,  TEMP, 2						#	L=(KC-offset)/4
	blez	L, .L55
	NOP

	.align	3

.L52:
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
	daddiu	BO, BO,  8 * SIZE			#	BO += 2nr*4kr

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
	bgtz	L, .L52
	NOP


	.align 3

.L55:
	andi	L, TEMP, 3
	blez	L, .L58
	NOP
	.align	3

.L56:
	MADD	t11, t11, a1, b1
	MADD	t21, t21, a2, b1
	MADD	t31, t31, a3, b1
	MADD	t41, t41, a4, b1

	MADD	t12, t12, a1, b2
	MADD	t22, t22, a2, b2
	MADD	t32, t32, a3, b2
	MADD	t42, t42, a4, b2

	daddiu	AO, AO, 4 * SIZE			#	AO += 4mr
	daddiu	BO, BO, 2 * SIZE			#	BO += 2nr

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L56
	NOP


	.align
.L58:
	daddiu	TEMP, KK,  -2				#	deal with the triangular data part
	dsll	L,    TEMP, 2 + BASE_SHIFT
	dsll	TEMP, TEMP, 1 + BASE_SHIFT	#	nr=2
	daddu	AO, AORIG, L
	daddu	BO, B, TEMP					#	BO point to the trigular data part

 	LD	b1,  0 * SIZE(AO)				#	fixed results
	LD	b2,  1 * SIZE(AO)
	LD	b3,  2 * SIZE(AO)
	LD	b4,  3 * SIZE(AO)

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


	LD	b8,   3 * SIZE(BO)
	LD	b1,   2 * SIZE(BO)
	MUL	t12, b8, t12
	MUL	t22, b8, t22
	MUL	t32, b8, t32
	MUL	t42, b8, t42
	NMSUB	t11, t11, b1, t12
	NMSUB	t21, t21, b1, t22
	NMSUB	t31, t31, b1, t32
	NMSUB	t41, t41, b1, t42


	LD	b2,   0 * SIZE(BO)
	MUL	t11, b2, t11
	MUL	t21, b2, t21
	MUL	t31, b2, t31
	MUL	t41, b2, t41


	ST	t11,  0 * SIZE(AO)				# updata packed A
	ST	t21,  1 * SIZE(AO)
	ST	t31,  2 * SIZE(AO)
	ST	t41,  3 * SIZE(AO)

	ST	t12,  4 * SIZE(AO)
	ST	t22,  5 * SIZE(AO)
	ST	t32,  6 * SIZE(AO)
	ST	t42,  7 * SIZE(AO)

	ST	t11,  0 * SIZE(CO1)				#	write back
	ST	t21,  1 * SIZE(CO1)
	ST	t31,  2 * SIZE(CO1)
	ST	t41,  3 * SIZE(CO1)

	ST	t12,  0 * SIZE(CO2)
	ST	t22,  1 * SIZE(CO2)
	ST	t32,  2 * SIZE(CO2)
	ST	t42,  3 * SIZE(CO2)

	daddiu	CO1, CO1, 4 * SIZE			#	fixed pointer
	daddiu	CO2, CO2, 4 * SIZE

	dsll	TEMP, K, 2 + BASE_SHIFT
	daddu	AORIG, AORIG, TEMP			# move to next panel Ai

	daddiu	I, I, -1
	bgtz	I, .L51
	NOP



	.align 3
.L60:
	andi	I,  M, 2						#	mr=2
	blez	I, .L70
	nop

	dsll	L,    KK, 1 + BASE_SHIFT		#	mr=2
	dsll	TEMP, KK, 1 + BASE_SHIFT		#	nr=2
	daddu	AO, AORIG, L
	daddu	BO, B, TEMP						#	BO point to the retangular data part,also reset BO
	dsubu	TEMP, K, KK						#	temp = the length of rectangular data part

	MTC	$0,  t11							#	clear 8 results registers
	MOV	t21, t11
	MOV	t12, t11
	MOV	t22, t11

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	dsra	L,  TEMP, 2						#	L=(KC-offset)/4
	blez	L, .L65
	NOP

	.align	3

.L62:
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
	daddiu	BO, BO,  8 * SIZE			#	BO += 2nr*4kr

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	MADD	t11, t11, a7, b7
	MADD	t21, t21, a8, b7

	MADD	t12, t12, a7, b8
	MADD	t22, t22, a8, b8

	daddiu	L, L, -1
	bgtz	L, .L62
	NOP


	.align 3

.L65:
	andi	L, TEMP, 3
	blez	L, .L68
	NOP
	.align	3

.L66:
	MADD	t11, t11, a1, b1
	MADD	t21, t21, a2, b1

	MADD	t12, t12, a1, b2
	MADD	t22, t22, a2, b2

	daddiu	AO, AO, 2 * SIZE			#	AO += 2mr
	daddiu	BO, BO, 2 * SIZE			#	BO += 2nr

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L66
	NOP


	.align
.L68:
	daddiu	TEMP, KK,  -2				#	deal with the triangular data part
	dsll	L,    TEMP, 1 + BASE_SHIFT
	dsll	TEMP, TEMP, 1 + BASE_SHIFT	#	nr=2
	daddu	AO, AORIG, L
	daddu	BO, B, TEMP					#	BO point to the trigular data part

 	LD	b1,  0 * SIZE(AO)				#	fixed results
	LD	b2,  1 * SIZE(AO)
	LD	b3,  2 * SIZE(AO)
	LD	b4,  3 * SIZE(AO)

	SUB	t11, b1, t11
	SUB	t21, b2, t21
	SUB	t12, b3, t12
	SUB	t22, b4, t22


	LD	b8,   3 * SIZE(BO)
	LD	b7,   2 * SIZE(BO)
	MUL	t12, b8, t12
	MUL	t22, b8, t22
	NMSUB	t11, t11, b7, t12
	NMSUB	t21, t21, b7, t22


	LD	b6,   0 * SIZE(BO)
	MUL	t11, b6, t11
	MUL	t21, b6, t21


	ST	t11,  0 * SIZE(AO)				# updata packed A
	ST	t21,  1 * SIZE(AO)
	ST	t12,  2 * SIZE(AO)
	ST	t22,  3 * SIZE(AO)

	ST	t11,  0 * SIZE(CO1)				#	write back
	ST	t21,  1 * SIZE(CO1)

	ST	t12,  0 * SIZE(CO2)
	ST	t22,  1 * SIZE(CO2)

	daddiu	CO1, CO1, 2 * SIZE			#	fixed pointer
	daddiu	CO2, CO2, 2 * SIZE

	dsll	TEMP, K, 1 + BASE_SHIFT		#	mr=2
	daddu	AORIG, AORIG, TEMP			# 	move to next panel Ai



	.align 3
.L70:
	andi	I, M, 1							#	mr=1
	blez	I, .L59
	nop

	dsll	L,    KK, BASE_SHIFT			#	mr=1
	dsll	TEMP, KK, 1 + BASE_SHIFT		#	nr=2
	daddu	AO, AORIG, L
	daddu	BO, B, TEMP						#	BO point to the retangular data part,also reset BO
	dsubu	TEMP, K, KK						#	temp = the length of rectangular data part

	MTC	$0,  t11							#	clear 8 results registers
	MOV	t12, t11

	LD	a1,  0 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	dsra	L,  TEMP, 2						#	L=(KC-offset)/4
	blez	L, .L75
	NOP

	.align	3

.L72:
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
	daddiu	BO, BO,  8 * SIZE			#	BO += 2nr*4kr

	LD	a1,  0 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	MADD	t11, t11, a7, b7
	MADD	t12, t12, a7, b8

	daddiu	L, L, -1
	bgtz	L, .L72
	NOP


	.align 3

.L75:
	andi	L, TEMP, 3
	blez	L, .L78
	NOP
	.align	3

.L76:
	MADD	t11, t11, a1, b1
	MADD	t12, t12, a1, b2

	daddiu	AO, AO, 1 * SIZE			#	AO += 1mr
	daddiu	BO, BO, 2 * SIZE			#	BO += 2nr

	LD	a1,  0 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L76
	NOP


	.align
.L78:
	daddiu	TEMP, KK,  -2				#	deal with the triangular data part
	dsll	L,    TEMP, BASE_SHIFT
	dsll	TEMP, TEMP, 1 + BASE_SHIFT	#	nr=2
	daddu	AO, AORIG, L
	daddu	BO, B, TEMP					#	BO point to the trigular data part

 	LD	b1,  0 * SIZE(AO)				#	fixed results
	LD	b2,  1 * SIZE(AO)

	SUB	t11, b1, t11
	SUB	t12, b2, t12


	LD	b8,   3 * SIZE(BO)
	LD	b7,   2 * SIZE(BO)
	MUL	t12, b8, t12
	NMSUB	t11, t11, b7, t12


	LD	b6,   0 * SIZE(BO)
	MUL	t11, b6, t11


	ST	t11,  0 * SIZE(AO)				# updata packed A
	ST	t12,  1 * SIZE(AO)

	ST	t11,  0 * SIZE(CO1)				#	write back
	ST	t12,  0 * SIZE(CO2)

	daddiu	CO1, CO1, 1 * SIZE			#	fixed pointer
	daddiu	CO2, CO2, 1 * SIZE

	dsll	TEMP, K, BASE_SHIFT		#	mr=2
	daddu	AORIG, AORIG, TEMP			# 	move to next panel Ai


.L59:
	daddiu	KK, KK, -2						#	rectangular data length increased by 2



	.align 3
.L50:
	dsra	J,  N, 2						#	J = NC/4
	blez	J, .L999
	NOP

.L10:
	dsll	TEMP, K, 2 + BASE_SHIFT
	dsubu	B, B, TEMP						#	move B to the beginning address of Bj

	dsll	TEMP, LDC, 2
	dsubu	C, C, TEMP						#	move C to the beginning address of Cj

	daddiu	J, J, -1

	move	CO1, C
	daddu	CO2, C,   LDC
	daddu	CO3, CO2, LDC
	daddu	CO4, CO3, LDC

	move	AORIG, A						#	reset A

	dsra	I,  M, 2						#	I=MC/4
	blez	I, .L20
	NOP

	.align 3
.L11:
	dsll	L,    KK, 2 + BASE_SHIFT		#	mr=4
	dsll	TEMP, KK, 2 + BASE_SHIFT		#	nr=4
	daddu	AO, AORIG, L
	daddu	BO, B, TEMP						#	BO point to the retangular data part,also reset BO
	dsubu	TEMP, K, KK						#	temp = the length of rectangular data part

	MTC	$0,  t11							#	clear 16 results registers
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

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	dsra	L,  TEMP, 2						#	L=(KC-offset)/4
	blez	L, .L15
	NOP

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
	daddiu	BO, BO, 16 * SIZE			#	BO += 4nr*4kr

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


	.align 3

.L15:
	andi	L, TEMP, 3
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
	MADD	t44, t44, a4, b4			#	third

	daddiu	AO, AO, 4 * SIZE			#	AO += 4mr
	daddiu	BO, BO, 4 * SIZE			#	BO += 4nr

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


	.align
.L18:
	daddiu	TEMP, KK, -4				#	deal with the triangular data part
	dsll	L,    TEMP, 2 + BASE_SHIFT
	dsll	TEMP, TEMP, 2 + BASE_SHIFT
	daddu	AO, AORIG, L
	daddu	BO, B, TEMP					#	BO point to the trigular data part

 	LD	b1,  0 * SIZE(AO)				#	fixed results
	LD	b2,  1 * SIZE(AO)
	LD	b3,  2 * SIZE(AO)
	LD	b4,  3 * SIZE(AO)

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


	LD	b1, 15 * SIZE(BO)
	LD	b2, 14 * SIZE(BO)
	LD	b3, 13 * SIZE(BO)
	LD	b4, 12 * SIZE(BO)
	MUL	t14, b1, t14
	MUL	t24, b1, t24
	MUL	t34, b1, t34
	MUL	t44, b1, t44
	NMSUB	t13, t13, b2, t14
	NMSUB	t23, t23, b2, t24
	NMSUB	t33, t33, b2, t34
	NMSUB	t43, t43, b2, t44
	NMSUB	t12, t12, b3, t14
	NMSUB	t22, t22, b3, t24
	NMSUB	t32, t32, b3, t34
	NMSUB	t42, t42, b3, t44
	NMSUB	t11, t11, b4, t14
	NMSUB	t21, t21, b4, t24
	NMSUB	t31, t31, b4, t34
	NMSUB	t41, t41, b4, t44


	LD	b5,  10 * SIZE(BO)
	LD	b6,   9 * SIZE(BO)
	LD	b7,   8 * SIZE(BO)
	MUL	t13, b5, t13
	MUL	t23, b5, t23
	MUL	t33, b5, t33
	MUL	t43, b5, t43
	NMSUB	t12, t12, b6, t13
	NMSUB	t22, t22, b6, t23
	NMSUB	t32, t32, b6, t33
	NMSUB	t42, t42, b6, t43
	NMSUB	t11, t11, b7, t13
	NMSUB	t21, t21, b7, t23
	NMSUB	t31, t31, b7, t33
	NMSUB	t41, t41, b7, t43


	LD	b8,   5 * SIZE(BO)
	LD	b1,   4 * SIZE(BO)
	MUL	t12, b8, t12
	MUL	t22, b8, t22
	MUL	t32, b8, t32
	MUL	t42, b8, t42
	NMSUB	t11, t11, b1, t12
	NMSUB	t21, t21, b1, t22
	NMSUB	t31, t31, b1, t32
	NMSUB	t41, t41, b1, t42


	LD	b2,   0 * SIZE(BO)
	MUL	t11, b2, t11
	MUL	t21, b2, t21
	MUL	t31, b2, t31
	MUL	t41, b2, t41


	ST	t11,  0 * SIZE(AO)				# updata packed A
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

	ST	t11,  0 * SIZE(CO1)				#	write back
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

	daddiu	CO1, CO1, 4 * SIZE			#	fixed pointer
	daddiu	CO2, CO2, 4 * SIZE
	daddiu	CO3, CO3, 4 * SIZE
	daddiu	CO4, CO4, 4 * SIZE

	dsll	TEMP, K, 2 + BASE_SHIFT
	daddu	AORIG, AORIG, TEMP			# move to next panel Ai

	daddiu	I, I, -1
	bgtz	I, .L11
	NOP

	.align 3
.L20:
	andi	I,  M, 2						#	mr=2
	blez	I, .L40
	NOP

	dsll	L,    KK, 1 + BASE_SHIFT		#	mr=2
	dsll	TEMP, KK, 2 + BASE_SHIFT		#	nr=4
	daddu	AO, AORIG, L
	daddu	BO, B, TEMP						#	BO point to the retangular data part,also reset BO
	dsubu	TEMP, K, KK						#	temp = the length of rectangular data part

	MTC	$0,  t11							#	clear 8 results registers
	MOV	t21, t11
	MOV	t12, t11
	MOV	t22, t11
	MOV	t13, t11
	MOV	t23, t11
	MOV	t14, t11
	MOV	t24, t11

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	dsra	L,  TEMP, 2						#	L=(KC-offset)/4
	blez	L, .L25
	NOP

	.align	3

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
	daddiu	BO, BO, 16 * SIZE			#	BO += 4nr*4kr

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


	.align 3

.L25:
	andi	L, TEMP, 3
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
	daddiu	BO, BO, 4 * SIZE			#	BO += 4nr

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L26
	NOP


	.align
.L28:
	daddiu	TEMP, KK, -4				#	deal with the triangular data part
	dsll	L,    TEMP, 1 + BASE_SHIFT	#	mr=2
	dsll	TEMP, TEMP, 2 + BASE_SHIFT
	daddu	AO, AORIG, L
	daddu	BO, B, TEMP					#	BO point to the trigular data part

 	LD	b1,  0 * SIZE(AO)				#	fixed results
	LD	b2,  1 * SIZE(AO)

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


	LD	b1, 15 * SIZE(BO)
	LD	b2, 14 * SIZE(BO)
	LD	b3, 13 * SIZE(BO)
	LD	b4, 12 * SIZE(BO)
	MUL	t14, b1, t14
	MUL	t24, b1, t24
	NMSUB	t13, t13, b2, t14
	NMSUB	t23, t23, b2, t24
	NMSUB	t12, t12, b3, t14
	NMSUB	t22, t22, b3, t24
	NMSUB	t11, t11, b4, t14
	NMSUB	t21, t21, b4, t24


	LD	b5,  10 * SIZE(BO)
	LD	b6,   9 * SIZE(BO)
	LD	b7,   8 * SIZE(BO)
	MUL	t13, b5, t13
	MUL	t23, b5, t23
	NMSUB	t12, t12, b6, t13
	NMSUB	t22, t22, b6, t23
	NMSUB	t11, t11, b7, t13
	NMSUB	t21, t21, b7, t23


	LD	b8,   5 * SIZE(BO)
	LD	b1,   4 * SIZE(BO)
	MUL	t12, b8, t12
	MUL	t22, b8, t22
	NMSUB	t11, t11, b1, t12
	NMSUB	t21, t21, b1, t22


	LD	b2,   0 * SIZE(BO)
	MUL	t11, b2, t11
	MUL	t21, b2, t21


	ST	t11,  0 * SIZE(AO)				# updata packed A
	ST	t21,  1 * SIZE(AO)

	ST	t12,  2 * SIZE(AO)
	ST	t22,  3 * SIZE(AO)

	ST	t13,  4 * SIZE(AO)
	ST	t23,  5 * SIZE(AO)

	ST	t14,  6 * SIZE(AO)
	ST	t24,  7 * SIZE(AO)

	ST	t11,  0 * SIZE(CO1)				#	write back
	ST	t21,  1 * SIZE(CO1)

	ST	t12,  0 * SIZE(CO2)
	ST	t22,  1 * SIZE(CO2)

	ST	t13,  0 * SIZE(CO3)
	ST	t23,  1 * SIZE(CO3)

	ST	t14,  0 * SIZE(CO4)
	ST	t24,  1 * SIZE(CO4)

	daddiu	CO1, CO1, 2 * SIZE			#	fixed pointer
	daddiu	CO2, CO2, 2 * SIZE
	daddiu	CO3, CO3, 2 * SIZE
	daddiu	CO4, CO4, 2 * SIZE

	dsll	TEMP, K, 1 + BASE_SHIFT		#	mr=2
	daddu	AORIG, AORIG, TEMP			# 	move to next panel Ai


	.align 3
.L40:
	andi	I,  M, 1
	blez	I, .L29
	NOP

	dsll	L,    KK, BASE_SHIFT			#	mr=1
	dsll	TEMP, KK, 2 + BASE_SHIFT		#	nr=4
	daddu	AO, AORIG, L
	daddu	BO, B, TEMP						#	BO point to the retangular data part,also reset BO
	dsubu	TEMP, K, KK						#	temp = the length of rectangular data part

	MTC	$0,  t11							#	clear 4 results registers
	MOV	t12, t11
	MOV	t13, t11
	MOV	t14, t11

	LD	a1,  0 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	dsra	L,  TEMP, 2						#	L=(KC-offset)/4
	blez	L, .L45
	NOP

	.align	3

.L42:
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
	daddiu	BO, BO, 16 * SIZE			#	BO += 4nr*4kr

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
	bgtz	L, .L42
	NOP


	.align 3

.L45:
	andi	L, TEMP, 3
	blez	L, .L48
	NOP
	.align	3

.L46:
	MADD	t11, t11, a1, b1
	MADD	t12, t12, a1, b2
	MADD	t13, t13, a1, b3
	MADD	t14, t14, a1, b4

	daddiu	AO, AO, 1 * SIZE			#	AO += 2mr
	daddiu	BO, BO, 4 * SIZE			#	BO += 4nr

	LD	a1,  0 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	daddiu	L, L, -1
	bgtz	L, .L46
	NOP


	.align
.L48:
	daddiu	TEMP, KK, -4				#	deal with the triangular data part
	dsll	L,    TEMP, BASE_SHIFT		#	mr=1
	dsll	TEMP, TEMP, 2 + BASE_SHIFT
	daddu	AO, AORIG, L
	daddu	BO, B, TEMP					#	BO point to the trigular data part

 	LD	b1,  0 * SIZE(AO)				#	fixed results
 	LD	b5,  1 * SIZE(AO)
	LD	b3,  2 * SIZE(AO)
	LD	b7,  3 * SIZE(AO)

	SUB	t11, b1, t11
	SUB	t12, b5, t12
	SUB	t13, b3, t13
	SUB	t14, b7, t14


	LD	b1, 15 * SIZE(BO)
	LD	b2, 14 * SIZE(BO)
	LD	b3, 13 * SIZE(BO)
	LD	b4, 12 * SIZE(BO)
	MUL	t14, b1, t14
	NMSUB	t13, t13, b2, t14
	NMSUB	t12, t12, b3, t14
	NMSUB	t11, t11, b4, t14


	LD	b5,  10 * SIZE(BO)
	LD	b6,   9 * SIZE(BO)
	LD	b7,   8 * SIZE(BO)
	MUL	t13, b5, t13
	NMSUB	t12, t12, b6, t13
	NMSUB	t11, t11, b7, t13


	LD	b8,   5 * SIZE(BO)
	LD	b1,   4 * SIZE(BO)
	MUL	t12, b8, t12
	NMSUB	t11, t11, b1, t12


	LD	b2,   0 * SIZE(BO)
	MUL	t11, b2, t11


	ST	t11,  0 * SIZE(AO)				# updata packed A
	ST	t12,  1 * SIZE(AO)
	ST	t13,  2 * SIZE(AO)
	ST	t14,  3 * SIZE(AO)

	ST	t11,  0 * SIZE(CO1)				#	write back
	ST	t12,  0 * SIZE(CO2)
	ST	t13,  0 * SIZE(CO3)
	ST	t14,  0 * SIZE(CO4)

	daddiu	CO1, CO1, 1 * SIZE			#	fixed pointer
	daddiu	CO2, CO2, 1 * SIZE
	daddiu	CO3, CO3, 1 * SIZE
	daddiu	CO4, CO4, 1 * SIZE

	dsll	TEMP, K, BASE_SHIFT			#	mr=2
	daddu	AORIG, AORIG, TEMP			# 	move to next panel Ai


.L29:
	daddiu	KK, KK, -4					#	rectangular data part increased by 4
	bgtz	J, .L10
	NOP



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
