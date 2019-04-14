##define REALNAME gemm
#define ASSEMBLER
#include "common.h"

#define FETCH	ld
#define	STACKSIZE	192
#define gsLQC1(base,fq,ft,offset) .word(0x32<<26|base<<21|ft<<16|0x1<<15|offset<<6|0x1<<5|fq)
#define gsSQC1(base,fq,ft,offset) .word(0x3A<<26|base<<21|ft<<16|0x1<<15|offset<<6|0x1<<5|fq)


#####	Parameter registers	####
#define M	$4
#define	N	$5
#define	K	$6
#define A	$9
#define B	$10
#define C	$11
#define LDC	$8

####	Pointer A, B, C	####
#define AO	$12
#define BO	$13

#define CO1	$14
#define CO2	$15

#define PREA	$18
#define PREB	$19

####	Used registers	####
#define A1	$f0
#define A2	$f1
#define A3	$f2
#define A4	$f3
#define A5	$f4
#define	A6	$f5
#define	A7	$f6
#define	A8	$f7

#define B1	$f8
#define B2	$f9
#define B3	$f10
#define B4	$f11
#define B5	$f12
#define	B6	$f13
#define	B7	$f14
#define	B8	$f15

#define C11	$f16
#define C12	$f17
#define C21	$f18
#define C22	$f19
#define C31	$f20
#define C32	$f21
#define C41	$f22
#define C42	$f23
#define C13	$f24
#define C14	$f25
#define C23	$f26
#define C24	$f27
#define C33	$f28
#define C34	$f29
#define C43	$f30
#define C44	$f31

#define I	$2
#define J	$3
#define L	$7

####	Alpha register	####
#define ALPHA	$f15

#define F31 31
#define F30 30
#define F29 29
#define F28 28
#define F27 27
#define F26 26
#define F25 25
#define F24 24
#define F23 23
#define F22 22
#define F21 21
#define F20 20
#define F19 19
#define F18 18
#define F17 17
#define F16 16
#define F15 15
#define F14 14
#define F13 13
#define F12 12
#define F11 11
#define F10 10
#define F9 9
#define F8 8
#define F7 7
#define F6 6
#define F5 5
#define F4 4
#define F3 3
#define F2 2
#define F1 1
#define F0 0

#define	R12	12
#define	R13	13

#define R14	14
#define R15	15
#define	R16	16
#define	R17	17

#if defined(TRMMKERNEL)
#define	OFFSET	$23
#define	KK		$24
#define TEMP	$25
#endif


	PROLOGUE

	LDARG	LDC,   0($sp)
	daddiu	$sp,$sp,-STACKSIZE

	sd	$16,   0($sp)
	sd	$17,   8($sp)
	sd	$18,  16($sp)
	sd	$19,  24($sp)
	sd	$20,  32($sp)
	sd	$21,  40($sp)
	sd	$22,  48($sp)

	ST	$f24, 56($sp)
	ST	$f25, 64($sp)
	ST	$f26, 72($sp)
	ST	$f27, 80($sp)
	ST	$f28, 88($sp)

#if defined(TRMMKERNEL)
	sd	$23,  96($sp)
	sd	$24, 104($sp)
	sd	$25, 112($sp)

	LDARG	OFFSET, STACKSIZE+8($sp)
#endif

#ifndef __64BIT__
	ST	$f20,120($sp)
	ST	$f21,128($sp)
	ST	$f22,136($sp)
	ST	$f23,144($sp)
#endif

	.align	4
.L2:
	dsra	J, N, 1				#	NR=2
	ST		$f15, 152($sp)

#if defined(TRMMKERNEL) && !defined(LEFT)
	neg	KK, OFFSET
#endif

	dsll	LDC, LDC, ZBASE_SHIFT#	LDC*SIZE
	blez	J, .L1
	ST		$f16, 160($sp)

.L24:
#if defined(TRMMKERNEL) &&  defined(LEFT)
	move	KK, OFFSET
#endif

	dsra	I, M, 2				#	MR=8
	move	AO, A				#	Reset A

	dsll	PREA, K, 1 + ZBASE_SHIFT
	move	CO1, C

	daddu	CO2, C,   LDC
	daddu	PREA, AO, PREA

	blez	I, .L22
	daddu	C,   CO2, LDC

	.align	4
.L241:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	BO,  B
#else
	dsll	L, KK, 2 + ZBASE_SHIFT
	dsll	TEMP, KK, 1 + ZBASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, B,  TEMP
#endif
	MTC		$0, C11				#	CLEAR REAULTS REGISTERS
	MOV		C12, C11

	dsll	PREB, K, ZBASE_SHIFT
	MOV		C21, C11
	MOV		C22, C11

	gsLQC1(R13, F9, F8, 0)		#	B1 B2
	MOV		C31, C11
	MOV		C32, C11

	gsLQC1(R12, F1, F0, 0)		#	A1 A2
	MOV		C41, C11
	MOV		C42, C11

	gsLQC1(R12, F3, F2, 1)		#	A3 A4
	MOV		C13, C11
	MOV		C14, C11

	MOV		C23, C11
	MOV		C24, C11

	MOV		C33, C11
	MOV		C34, C11

	MOV		C43, C11
	MOV		C44, C11

	PLU		B3,	B1, B1
	PLU		B4, B2, B2
	daddu	PREB, BO, PREB

	FETCH	$0, 0 * SIZE(CO1)
	FETCH	$0, 8 * SIZE(CO1)
	FETCH	$0, 0 * SIZE(CO2)
	FETCH	$0, 8 * SIZE(CO2)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, K, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 4
#else
	daddiu	TEMP, KK, 2
#endif
	dsra	L,  TEMP, 2
	blez	L, .L242
	NOP

#else

	move	BO, B				#	Reset	B
	dsra	L, K, 2				#	UnRoll	K=64
	MTC		$0, C11				#	CLEAR REAULTS REGISTERS
	MOV		C12, C11

	dsll	PREB, K, ZBASE_SHIFT
	MOV		C21, C11
	MOV		C22, C11

	gsLQC1(R13, F9, F8, 0)		#	B1 B2
	MOV		C31, C11
	MOV		C32, C11

	gsLQC1(R12, F1, F0, 0)		#	A1 A2
	MOV		C41, C11
	MOV		C42, C11

	gsLQC1(R12, F3, F2, 1)		#	A3 A4
	MOV		C13, C11
	MOV		C14, C11

	FETCH	$0, 0 * SIZE(CO1)
	MOV		C23, C11
	MOV		C24, C11

	FETCH	$0, 0 * SIZE(CO2)
	MOV		C33, C11
	MOV		C34, C11

	MOV		C43, C11
	MOV		C44, C11
	daddu	PREB, BO, PREB

	PLU		B3,	B1, B1
	PLU		B4, B2, B2

	FETCH	$0, 8 * SIZE(CO1)
	blez	L, .L242
	FETCH	$0, 8 * SIZE(CO2)
#endif

.L2410:
	daddiu	L, L, -1
	gsLQC1(R13, F13, F12, 1)	#	B3 B4
	MADPS	C11, C11, A1, B1
	MADPS	C21, C21, A2, B1

	gsLQC1(R12, F5, F4, 2)		#	A5 A6
	MADPS	C12, C12, A1, B2
	MADPS	C22, C22, A2, B2

	gsLQC1(R12, F7, F6, 3)		#	A7 A8
	MADPS	C31, C31, A3, B1
	MADPS	C41, C41, A4, B1

	FETCH	$0, 0 * SIZE(PREB)
	MADPS	C32, C32, A3, B2
	MADPS	C42, C42, A4, B2

	FETCH	$0, 0 * SIZE(PREA)
	MADPS	C13, C13, A1, B3
	MADPS	C23, C23, A2, B3

	MADPS	C33, C33, A3, B3
	MADPS	C43, C43, A4, B3

	MADPS	C14, C14, A1, B4
	MADPS	C24, C24, A2, B4

	PLU		B7,	B5, B5
	PLU		B8, B6, B6
	daddu	PREB, PREB, 8 * SIZE

	MADPS	C34, C34, A3, B4
	MADPS	C44, C44, A4, B4

	gsLQC1(R13, F9, F8, 2)		#	B1 B2
	MADPS	C11, C11, A5, B5
	MADPS	C21, C21, A6, B5

	gsLQC1(R12, F1, F0, 4)		#	A1 A2
	MADPS	C12, C12, A5, B6
	MADPS	C22, C22, A6, B6

	gsLQC1(R12, F3, F2, 5)		#	A3 A4
	MADPS	C31, C31, A7, B5
	MADPS	C41, C41, A8, B5

	FETCH	$0, 8 * SIZE(PREA)
	MADPS	C32, C32, A7, B6
	MADPS	C42, C42, A8, B6

	MADPS	C13, C13, A5, B7
	MADPS	C23, C23, A6, B7

	MADPS	C33, C33, A7, B7
	MADPS	C43, C43, A8, B7

	MADPS	C14, C14, A5, B8
	MADPS	C24, C24, A6, B8

	PLU		B3,	B1, B1
	PLU		B4, B2, B2

	MADPS	C34, C34, A7, B8
	MADPS	C44, C44, A8, B8

	gsLQC1(R13, F13, F12, 3)	#	B3 B4
	MADPS	C11, C11, A1, B1
	MADPS	C21, C21, A2, B1

	gsLQC1(R12, F5, F4, 6)		#	A5 A6
	MADPS	C12, C12, A1, B2
	MADPS	C22, C22, A2, B2

	gsLQC1(R12, F7, F6, 7)		#	A7 A8
	MADPS	C31, C31, A3, B1
	MADPS	C41, C41, A4, B1
	daddiu	BO, BO, 4 * 4 * SIZE	#	4KR*4NR

	FETCH	$0, 16 * SIZE(PREA)
	MADPS	C32, C32, A3, B2
	MADPS	C42, C42, A4, B2
	daddiu	AO, AO, 8 * 4 * SIZE 	#	4KR*8MR

	MADPS	C13, C13, A1, B3
	MADPS	C23, C23, A2, B3

	MADPS	C33, C33, A3, B3
	MADPS	C43, C43, A4, B3

	MADPS	C14, C14, A1, B4
	MADPS	C24, C24, A2, B4

	PLU		B7,	B5, B5
	PLU		B8, B6, B6

	MADPS	C34, C34, A3, B4
	MADPS	C44, C44, A4, B4

	gsLQC1(R13, F9, F8, 0)		#	B1 B2
	MADPS	C11, C11, A5, B5
	MADPS	C21, C21, A6, B5

	gsLQC1(R12, F1, F0, 0)		#	A1 A2
	MADPS	C12, C12, A5, B6
	MADPS	C22, C22, A6, B6

	gsLQC1(R12, F3, F2, 1)		#	A3 A4
	MADPS	C31, C31, A7, B5
	MADPS	C41, C41, A8, B5

	FETCH	$0, 24 * SIZE(PREA)
	MADPS	C32, C32, A7, B6
	MADPS	C42, C42, A8, B6

	MADPS	C13, C13, A5, B7
	MADPS	C23, C23, A6, B7
	daddu	PREA, PREA, 32 * SIZE

	MADPS	C33, C33, A7, B7
	MADPS	C43, C43, A8, B7

	MADPS	C14, C14, A5, B8
	MADPS	C24, C24, A6, B8

	PLU		B3,	B1, B1
	PLU		B4, B2, B2

	MADPS	C34, C34, A7, B8
	bgtz	L, .L2410
	MADPS	C44, C44, A8, B8


	.align	4
.L242:
#ifndef	TRMMKERNEL
	andi	L, K, 2
#else
	andi	L, TEMP, 2
#endif
	blez	L, .L247
	NOP

	gsLQC1(R13, F13, F12, 1)	#	B3 B4
	MADPS	C11, C11, A1, B1
	MADPS	C21, C21, A2, B1

	gsLQC1(R12, F5, F4, 2)		#	A5 A6
	MADPS	C12, C12, A1, B2
	MADPS	C22, C22, A2, B2

	gsLQC1(R12, F7, F6, 3)		#	A7 A8
	MADPS	C31, C31, A3, B1
	MADPS	C41, C41, A4, B1
	daddiu	BO, BO, 2 * 4 * SIZE	#	4KR*4NR

	MADPS	C32, C32, A3, B2
	MADPS	C42, C42, A4, B2
	daddiu	AO, AO, 4 * 4 * SIZE

	MADPS	C13, C13, A1, B3
	MADPS	C23, C23, A2, B3

	MADPS	C33, C33, A3, B3
	MADPS	C43, C43, A4, B3

	MADPS	C14, C14, A1, B4
	MADPS	C24, C24, A2, B4

	PLU		B7,	B5, B5
	PLU		B8, B6, B6

	MADPS	C34, C34, A3, B4
	MADPS	C44, C44, A4, B4

	gsLQC1(R13, F9, F8, 0)		#	B1 B2
	MADPS	C11, C11, A5, B5
	MADPS	C21, C21, A6, B5

	gsLQC1(R12, F1, F0, 0)		#	A1 A2
	MADPS	C12, C12, A5, B6
	MADPS	C22, C22, A6, B6

	gsLQC1(R12, F3, F2, 1)		#	A3 A4
	MADPS	C31, C31, A7, B5
	MADPS	C41, C41, A8, B5

	MADPS	C32, C32, A7, B6
	MADPS	C42, C42, A8, B6

	MADPS	C13, C13, A5, B7
	MADPS	C23, C23, A6, B7

	MADPS	C33, C33, A7, B7
	MADPS	C43, C43, A8, B7

	MADPS	C14, C14, A5, B8
	MADPS	C24, C24, A6, B8

	PLU		B3,	B1, B1
	PLU		B4, B2, B2

	MADPS	C34, C34, A7, B8
	MADPS	C44, C44, A8, B8

	.align	4
.L247:
#ifndef	TRMMKERNEL
	andi	L, K, 1
#else
	andi	L, TEMP, 1
#endif
	blez	L, .L240
	NOP

	MADPS	C11, C11, A1, B1
	MADPS	C21, C21, A2, B1

	MADPS	C12, C12, A1, B2
	MADPS	C22, C22, A2, B2

	MADPS	C31, C31, A3, B1
	MADPS	C41, C41, A4, B1
	daddiu	BO, BO, 1 * 4 * SIZE	#	4KR*4NR

	MADPS	C32, C32, A3, B2
	MADPS	C42, C42, A4, B2
	daddiu	AO, AO, 2 * 4 * SIZE

	MADPS	C13, C13, A1, B3
	MADPS	C23, C23, A2, B3

	MADPS	C33, C33, A3, B3
	MADPS	C43, C43, A4, B3

	MADPS	C14, C14, A1, B4
	MADPS	C24, C24, A2, B4

	MADPS	C34, C34, A3, B4
	MADPS	C44, C44, A4, B4


	.align	4
.L240:							#	Write Back
#ifndef TRMMKERNEL
	daddiu	I, I, -1
	CVTU	A1, C11
	CVTU	A2, C21

	CVTU	A3, C31
	CVTU	A4, C41

	CVTU	A5, C13
	CVTU	A6, C23

	CVTU	A7, C33
	CVTU	A8, C43

	CVTU	B1, C12
	CVTU	B2, C22

	CVTU	B3, C32
	CVTU	B4, C42

	CVTU	B5, C14
	CVTU	B6, C24

	CVTU	B7, C34
	CVTU	B8, C44

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	/*	(a + bi) * (c + di) */
	SUB		C11, C11, A1		#	ac'+'bd
	SUB		C21, C21, A2
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	SUB		C31, C31, A3
	LD		A1, 152($sp)		#	load alpha_r
	SUB		C41, C41, A4
	LD		A2, 160($sp)		#	load alpha_i
#	LD		A2, 0 * SIZE(A)		#	load alpha_i
	ADD		C13, A5, C13		#	ad'+'cb
	ADD		C23, A6, C23
	ADD		C33, A7, C33
	ADD		C43, A8, C43
	SUB		C12, C12, B1
	SUB		C22, C22, B2
	SUB		C32, C32, B3
	SUB		C42, C42, B4
	ADD		C14, B5, C14
	ADD		C24, B6, C24
	ADD		C34, B7, C34
	ADD		C44, B8, C44

	LD		B1, 0 * SIZE(CO1)
	LD		B3, 2 * SIZE(CO1)
	LD		B5, 4 * SIZE(CO1)
	LD		B7, 6 * SIZE(CO1)
	LD		B2, 1 * SIZE(CO1)
	LD		B4, 3 * SIZE(CO1)
	LD		B6, 5 * SIZE(CO1)
	LD		B8, 7 * SIZE(CO1)

	MADD	B1, B1, C11, A1		#	A1 = alpha_r
	MADD	B3, B3, C21, A1
	MADD	B5, B5, C31, A1
	MADD	B7, B7, C41, A1
	MADD	B2, B2, C13, A1
	MADD	B4, B4, C23, A1
	MADD	B6, B6, C33, A1
	MADD	B8, B8, C43, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	NMSUB	B5, B5, C33, A2
	NMSUB	B7, B7, C43, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2
	MADD	B6, B6, C31, A2
	MADD	B8, B8, C41, A2

	LD		C13, 0 * SIZE(CO2)
	LD		C23, 2 * SIZE(CO2)
	LD		C33, 4 * SIZE(CO2)
	LD		C43, 6 * SIZE(CO2)
	LD		C11, 1 * SIZE(CO2)
	LD		C21, 3 * SIZE(CO2)
	LD		C31, 5 * SIZE(CO2)
	LD		C41, 7 * SIZE(CO2)

	MADD	C13, C13, C12, A1
	MADD	C23, C23, C22, A1

	MADD	C33, C33, C32, A1
	ST		B1, 0 * SIZE(CO1)

	MADD	C43, C43, C42, A1
	ST		B3, 2 * SIZE(CO1)

	MADD	C11, C11, C14, A1
	ST		B5, 4 * SIZE(CO1)

	MADD	C21, C21, C24, A1
	ST		B7, 6 * SIZE(CO1)

	MADD	C31, C31, C34, A1
	ST		B2, 1 * SIZE(CO1)

	MADD	C41, C41, C44, A1
	ST		B4, 3 * SIZE(CO1)

	NMSUB	C13, C13, C14, A2
	ST		B6, 5 * SIZE(CO1)

	NMSUB	C23, C23, C24, A2
	ST		B8, 7 * SIZE(CO1)

	NMSUB	C33, C33, C34, A2
	NMSUB	C43, C43, C44, A2

	MADD	C11, C11, C12, A2
	MADD	C21, C21, C22, A2

	MADD	C31, C31, C32, A2
	MADD	C41, C41, C42, A2

	ST		C13, 0 * SIZE(CO2)
	ST		C23, 2 * SIZE(CO2)
	ST		C33, 4 * SIZE(CO2)
	ST		C43, 6 * SIZE(CO2)
	ST		C11, 1 * SIZE(CO2)
	ST		C21, 3 * SIZE(CO2)
	ST		C31, 5 * SIZE(CO2)
	ST		C41, 7 * SIZE(CO2)
#endif

#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
	/*	(a + bi) * (c - di) */
	ADD		C11, A1, C11		#	ac'+'bd
	ADD		C21, A2, C21
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	ADD		C31, A3, C31
	LD		A1, 152($sp)		#	load alpha_r
	ADD		C41, A4, C41
	LD		A2, 160($sp)		#	load alpha_i
#	LD		A2, 0 * SIZE(A)		#	load alpha_r
	SUB		C13, A5, C13		#	ad'+'cb
	SUB		C23, A6, C23
	SUB		C33, A7, C33
	SUB		C43, A8, C43
	ADD		C12, B1, C12
	ADD		C22, B2, C22
	ADD		C32, B3, C32
	ADD		C42, B4, C42
	SUB		C14, B5, C14
	SUB		C24, B6, C24
	SUB		C34, B7, C34
	SUB		C44, B8, C44

	LD		B1, 0 * SIZE(CO1)
	LD		B3, 2 * SIZE(CO1)
	LD		B5, 4 * SIZE(CO1)
	LD		B7, 6 * SIZE(CO1)
	LD		B2, 1 * SIZE(CO1)
	LD		B4, 3 * SIZE(CO1)
	LD		B6, 5 * SIZE(CO1)
	LD		B8, 7 * SIZE(CO1)

	MADD	B1, B1, C11, A1		#	A1 = alpha_r
	MADD	B3, B3, C21, A1
	MADD	B5, B5, C31, A1
	MADD	B7, B7, C41, A1
	MADD	B2, B2, C13, A1
	MADD	B4, B4, C23, A1
	MADD	B6, B6, C33, A1
	MADD	B8, B8, C43, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	NMSUB	B5, B5, C33, A2
	NMSUB	B7, B7, C43, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2
	MADD	B6, B6, C31, A2
	MADD	B8, B8, C41, A2

	LD		C13, 0 * SIZE(CO2)
	LD		C23, 2 * SIZE(CO2)
	LD		C33, 4 * SIZE(CO2)
	LD		C43, 6 * SIZE(CO2)
	LD		C11, 1 * SIZE(CO2)
	LD		C21, 3 * SIZE(CO2)
	LD		C31, 5 * SIZE(CO2)
	LD		C41, 7 * SIZE(CO2)

	MADD	C13, C13, C12, A1
	MADD	C23, C23, C22, A1

	MADD	C33, C33, C32, A1
	ST		B1, 0 * SIZE(CO1)

	MADD	C43, C43, C42, A1
	ST		B3, 2 * SIZE(CO1)

	MADD	C11, C11, C14, A1
	ST		B5, 4 * SIZE(CO1)

	MADD	C21, C21, C24, A1
	ST		B7, 6 * SIZE(CO1)

	MADD	C31, C31, C34, A1
	ST		B2, 1 * SIZE(CO1)

	MADD	C41, C41, C44, A1
	ST		B4, 3 * SIZE(CO1)

	NMSUB	C13, C13, C14, A2
	ST		B6, 5 * SIZE(CO1)

	NMSUB	C23, C23, C24, A2
	ST		B8, 7 * SIZE(CO1)

	NMSUB	C33, C33, C34, A2
	NMSUB	C43, C43, C44, A2

	MADD	C11, C11, C12, A2
	MADD	C21, C21, C22, A2

	MADD	C31, C31, C32, A2
	MADD	C41, C41, C42, A2

	ST		C13, 0 * SIZE(CO2)
	ST		C23, 2 * SIZE(CO2)
	ST		C33, 4 * SIZE(CO2)
	ST		C43, 6 * SIZE(CO2)
	ST		C11, 1 * SIZE(CO2)
	ST		C21, 3 * SIZE(CO2)
	ST		C31, 5 * SIZE(CO2)
	ST		C41, 7 * SIZE(CO2)

#endif

#if	  defined(RN) || defined(RT) || defined(CN) || defined(CT)
	/*	(a - bi) * (c + di) */
	ADD		C11, A1, C11		#	ac'+'bd
	ADD		C21, A2, C21
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	ADD		C31, A3, C31
	LD		A1, 152($sp)		#	load alpha_r
#	LD		A2, 0 * SIZE(A)		#	load alpha_r
	ADD		C41, A4, C41
	LD		A2, 160($sp)		#	load alpha_i
	SUB		C13, C13, A5		#	ad'+'cb
	SUB		C23, C23, A6
	SUB		C33, C33, A7
	SUB		C43, C43, A8
	ADD		C12, B1, C12
	ADD		C22, B2, C22
	ADD		C32, B3, C32
	ADD		C42, B4, C42
	SUB		C14, C14, B5
	SUB		C24, C24, B6
	SUB		C34, C34, B7
	SUB		C44, C44, B8

	LD		B1, 0 * SIZE(CO1)
	LD		B3, 2 * SIZE(CO1)
	LD		B5, 4 * SIZE(CO1)
	LD		B7, 6 * SIZE(CO1)
	LD		B2, 1 * SIZE(CO1)
	LD		B4, 3 * SIZE(CO1)
	LD		B6, 5 * SIZE(CO1)
	LD		B8, 7 * SIZE(CO1)

	MADD	B1, B1, C11, A1		#	A1 = alpha_r
	MADD	B3, B3, C21, A1
	MADD	B5, B5, C31, A1
	MADD	B7, B7, C41, A1
	MADD	B2, B2, C13, A1
	MADD	B4, B4, C23, A1
	MADD	B6, B6, C33, A1
	MADD	B8, B8, C43, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	NMSUB	B5, B5, C33, A2
	NMSUB	B7, B7, C43, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2
	MADD	B6, B6, C31, A2
	MADD	B8, B8, C41, A2

	LD		C13, 0 * SIZE(CO2)
	LD		C23, 2 * SIZE(CO2)
	LD		C33, 4 * SIZE(CO2)
	LD		C43, 6 * SIZE(CO2)
	LD		C11, 1 * SIZE(CO2)
	LD		C21, 3 * SIZE(CO2)
	LD		C31, 5 * SIZE(CO2)
	LD		C41, 7 * SIZE(CO2)

	MADD	C13, C13, C12, A1
	MADD	C23, C23, C22, A1

	MADD	C33, C33, C32, A1
	ST		B1, 0 * SIZE(CO1)

	MADD	C43, C43, C42, A1
	ST		B3, 2 * SIZE(CO1)

	MADD	C11, C11, C14, A1
	ST		B5, 4 * SIZE(CO1)

	MADD	C21, C21, C24, A1
	ST		B7, 6 * SIZE(CO1)

	MADD	C31, C31, C34, A1
	ST		B2, 1 * SIZE(CO1)

	MADD	C41, C41, C44, A1
	ST		B4, 3 * SIZE(CO1)

	NMSUB	C13, C13, C14, A2
	ST		B6, 5 * SIZE(CO1)

	NMSUB	C23, C23, C24, A2
	ST		B8, 7 * SIZE(CO1)

	NMSUB	C33, C33, C34, A2
	NMSUB	C43, C43, C44, A2

	MADD	C11, C11, C12, A2
	MADD	C21, C21, C22, A2

	MADD	C31, C31, C32, A2
	MADD	C41, C41, C42, A2

	ST		C13, 0 * SIZE(CO2)
	ST		C23, 2 * SIZE(CO2)
	ST		C33, 4 * SIZE(CO2)
	ST		C43, 6 * SIZE(CO2)
	ST		C11, 1 * SIZE(CO2)
	ST		C21, 3 * SIZE(CO2)
	ST		C31, 5 * SIZE(CO2)
	ST		C41, 7 * SIZE(CO2)

#endif

#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
	/*	(a - bi) * (c - di) */
	SUB		C11, C11, A1		#	ac'+'bd
	SUB		C21, C21, A2
	SUB		C31, C31, A3
	LD		A1, 152($sp)		#	load alpha_r
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	SUB		C41, C41, A4
	LD		A2, 160($sp)
#	LD		A2, 0 * SIZE(A)		#	load alpha_i

	ADD		C13, A5, C13		#	ad'+'cb
	ADD		C23, A6, C23
	ADD		C33, A7, C33
	ADD		C43, A8, C43
	SUB		C12, C12, B1
	SUB		C22, C22, B2
	SUB		C32, C32, B3
	SUB		C42, C42, B4
	ADD		C14, B5, C14
	ADD		C24, B6, C24
	ADD		C34, B7, C34
	ADD		C44, B8, C44
	NEG		C13, C13
	NEG		C23, C23
	NEG		C33, C33
	NEG		C43, C43
	NEG		C14, C14
	NEG		C24, C24
	NEG		C34, C34
	NEG		C44, C44

	LD		B1, 0 * SIZE(CO1)
	LD		B3, 2 * SIZE(CO1)
	LD		B5, 4 * SIZE(CO1)
	LD		B7, 6 * SIZE(CO1)
	LD		B2, 1 * SIZE(CO1)
	LD		B4, 3 * SIZE(CO1)
	LD		B6, 5 * SIZE(CO1)
	LD		B8, 7 * SIZE(CO1)

	MADD	B1, B1, C11, A1		#	A1 = alpha_r
	MADD	B3, B3, C21, A1
	MADD	B5, B5, C31, A1
	MADD	B7, B7, C41, A1
	MADD	B2, B2, C13, A1
	MADD	B4, B4, C23, A1
	MADD	B6, B6, C33, A1
	MADD	B8, B8, C43, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	NMSUB	B5, B5, C33, A2
	NMSUB	B7, B7, C43, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2
	MADD	B6, B6, C31, A2
	MADD	B8, B8, C41, A2

	LD		C13, 0 * SIZE(CO2)
	LD		C43, 6 * SIZE(CO2)
	LD		C23, 2 * SIZE(CO2)
	LD		C33, 4 * SIZE(CO2)
	LD		C11, 1 * SIZE(CO2)
	LD		C21, 3 * SIZE(CO2)
	LD		C31, 5 * SIZE(CO2)
	LD		C41, 7 * SIZE(CO2)

	MADD	C13, C13, C12, A1
	ST		B1, 0 * SIZE(CO1)

	MADD	C23, C23, C22, A1
	ST		B3, 2 * SIZE(CO1)

	MADD	C33, C33, C32, A1
	ST		B5, 4 * SIZE(CO1)

	MADD	C43, C43, C42, A1
	ST		B7, 6 * SIZE(CO1)

	MADD	C11, C11, C14, A1
	ST		B2, 1 * SIZE(CO1)

	MADD	C21, C21, C24, A1
	ST		B4, 3 * SIZE(CO1)

	MADD	C31, C31, C34, A1
	ST		B6, 5 * SIZE(CO1)

	MADD	C41, C41, C44, A1
	ST		B8, 7 * SIZE(CO1)

	NMSUB	C13, C13, C14, A2
	NMSUB	C23, C23, C24, A2
	NMSUB	C33, C33, C34, A2
	NMSUB	C43, C43, C44, A2

	MADD	C11, C11, C12, A2
	MADD	C21, C21, C22, A2
	MADD	C31, C31, C32, A2
	MADD	C41, C41, C42, A2

	ST		C13, 0 * SIZE(CO2)
	ST		C23, 2 * SIZE(CO2)
	ST		C33, 4 * SIZE(CO2)
	ST		C43, 6 * SIZE(CO2)
	ST		C11, 1 * SIZE(CO2)
	ST		C21, 3 * SIZE(CO2)
	ST		C31, 5 * SIZE(CO2)
	ST		C41, 7 * SIZE(CO2)

#endif

#else
	daddiu	I, I, -1
	CVTU	A1, C11
	CVTU	A2, C21

	CVTU	A3, C31
	CVTU	A4, C41

	CVTU	A5, C13
	CVTU	A6, C23

	CVTU	A7, C33
	CVTU	A8, C43

	CVTU	B1, C12
	CVTU	B2, C22

	CVTU	B3, C32
	CVTU	B4, C42

	CVTU	B5, C14
	CVTU	B6, C24

	CVTU	B7, C34
	CVTU	B8, C44

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	/*	(a + bi) * (c + di) */
	SUB		C11, C11, A1		#	ac'+'bd
	SUB		C21, C21, A2
	SUB		C31, C31, A3
	LD		A1, 152($sp)		#	load alpha_r
	SUB		C41, C41, A4
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i
	ADD		C13, A5, C13		#	ad'+'cb
	ADD		C23, A6, C23
#	LD		A2, 0 * SIZE(A)		#	load alpha_i
	ADD		C33, A7, C33
	ADD		C43, A8, C43
	SUB		C12, C12, B1
	SUB		C22, C22, B2
	SUB		C32, C32, B3
	SUB		C42, C42, B4
	ADD		C14, B5, C14
	ADD		C24, B6, C24
	ADD		C34, B7, C34
	ADD		C44, B8, C44

	MUL		B1, C11, A1		#	A1 = alpha_r
	MUL		B3, C21, A1
	MUL		B5, C31, A1
	MUL		B7, C41, A1
	MUL		B2, C13, A1
	MUL		B4, C23, A1
	MUL		B6, C33, A1
	MUL		B8, C43, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	NMSUB	B5, B5, C33, A2
	NMSUB	B7, B7, C43, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2
	MADD	B6, B6, C31, A2
	MADD	B8, B8, C41, A2

	ST		B1, 0 * SIZE(CO1)
	MUL		C13, C12, A1
	MUL		C23, C22, A1

	ST		B3, 2 * SIZE(CO1)
	MUL		C33, C32, A1
	MUL		C43, C42, A1

	ST		B5, 4 * SIZE(CO1)
	MUL		C11, C14, A1
	MUL		C21, C24, A1

	ST		B7, 6 * SIZE(CO1)
	MUL		C31, C34, A1
	MUL		C41, C44, A1

	ST		B2, 1 * SIZE(CO1)
	NMSUB	C13, C13, C14, A2
	NMSUB	C23, C23, C24, A2

	ST		B4, 3 * SIZE(CO1)
	NMSUB	C33, C33, C34, A2
	NMSUB	C43, C43, C44, A2

	ST		B6, 5 * SIZE(CO1)
	MADD	C11, C11, C12, A2
	MADD	C21, C21, C22, A2

	ST		B8, 7 * SIZE(CO1)
	MADD	C31, C31, C32, A2
	MADD	C41, C41, C42, A2

	ST		C13, 0 * SIZE(CO2)
	ST		C23, 2 * SIZE(CO2)
	ST		C33, 4 * SIZE(CO2)
	ST		C43, 6 * SIZE(CO2)
	ST		C11, 1 * SIZE(CO2)
	ST		C21, 3 * SIZE(CO2)
	ST		C31, 5 * SIZE(CO2)
	ST		C41, 7 * SIZE(CO2)
#endif

#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
	/*	(a + bi) * (c - di) */
	ADD		C11, A1, C11		#	ac'+'bd
	ADD		C21, A2, C21
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	ADD		C31, A3, C31
	LD		A1, 152($sp)		#	load alpha_r
	ADD		C41, A4, C41
	LD		A2, 160($sp)		#	load alpha_i
#	LD		A2, 0 * SIZE(A)		#	load alpha_r
	SUB		C13, A5, C13		#	ad'+'cb
	SUB		C23, A6, C23
	SUB		C33, A7, C33
	SUB		C43, A8, C43
	ADD		C12, B1, C12
	ADD		C22, B2, C22
	ADD		C32, B3, C32
	ADD		C42, B4, C42
	SUB		C14, B5, C14
	SUB		C24, B6, C24
	SUB		C34, B7, C34
	SUB		C44, B8, C44

	MUL		B1, C11, A1		#	A1 = alpha_r
	MUL		B3, C21, A1
	MUL		B5, C31, A1
	MUL	 	B7, C41, A1
	MUL		B2, C13, A1
	MUL		B4, C23, A1
	MUL		B6, C33, A1
	MUL		B8, C43, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	NMSUB	B5, B5, C33, A2
	NMSUB	B7, B7, C43, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2
	MADD	B6, B6, C31, A2
	MADD	B8, B8, C41, A2

	MUL		C13, C12, A1
	MUL		C23, C22, A1

	ST		B1, 0 * SIZE(CO1)
	MUL		C33, C32, A1
	MUL		C43, C42, A1

	ST		B3, 2 * SIZE(CO1)
	MUL		C11, C14, A1
	MUL		C21, C24, A1

	ST		B5, 4 * SIZE(CO1)
	MUL 	C31, C34, A1
	MUL 	C41, C44, A1

	ST		B7, 6 * SIZE(CO1)
	NMSUB	C13, C13, C14, A2
	NMSUB	C23, C23, C24, A2

	ST		B2, 1 * SIZE(CO1)
	NMSUB	C33, C33, C34, A2
	NMSUB	C43, C43, C44, A2

	ST		B4, 3 * SIZE(CO1)
	MADD	C11, C11, C12, A2
	MADD	C21, C21, C22, A2

	ST		B6, 5 * SIZE(CO1)
	MADD	C31, C31, C32, A2
	MADD	C41, C41, C42, A2

	ST		B8, 7 * SIZE(CO1)
	ST		C13, 0 * SIZE(CO2)
	ST		C23, 2 * SIZE(CO2)
	ST		C33, 4 * SIZE(CO2)
	ST		C43, 6 * SIZE(CO2)
	ST		C11, 1 * SIZE(CO2)
	ST		C21, 3 * SIZE(CO2)
	ST		C31, 5 * SIZE(CO2)
	ST		C41, 7 * SIZE(CO2)

#endif

#if	  defined(RN) || defined(RT) || defined(CN) || defined(CT)
	/*	(a - bi) * (c + di) */
	ADD		C11, A1, C11		#	ac'+'bd
	ADD		C21, A2, C21
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	ADD		C31, A3, C31
	LD		A1, 152($sp)		#	load alpha_r
#	LD		A2, 0 * SIZE(A)		#	load alpha_r
	ADD		C41, A4, C41
	LD		A2, 160($sp)		#	load alpha_i
	SUB		C13, C13, A5		#	ad'+'cb
	SUB		C23, C23, A6
	SUB		C33, C33, A7
	SUB		C43, C43, A8
	ADD		C12, B1, C12
	ADD		C22, B2, C22
	ADD		C32, B3, C32
	ADD		C42, B4, C42
	SUB		C14, C14, B5
	SUB		C24, C24, B6

	SUB		C34, C34, B7
	SUB		C44, C44, B8

	MUL	B1, C11, A1		#	A1 = alpha_r
	MUL	B3, C21, A1
	MUL	B5, C31, A1
	MUL	B7, C41, A1
	MUL	B2, C13, A1
	MUL	B4, C23, A1
	MUL	B6, C33, A1
	MUL	B8, C43, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	NMSUB	B5, B5, C33, A2
	NMSUB	B7, B7, C43, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2
	MADD	B6, B6, C31, A2
	MADD	B8, B8, C41, A2

	MUL	C13, C12, A1
	MUL	C23, C22, A1

	ST		B1, 0 * SIZE(CO1)
	MUL	C33, C32, A1
	MUL	C43, C42, A1

	ST		B3, 2 * SIZE(CO1)
	MUL	C11, C14, A1
	MUL	C21, C24, A1

	ST		B5, 4 * SIZE(CO1)
	MUL	C31, C34, A1
	MUL	C41, C44, A1

	ST		B7, 6 * SIZE(CO1)
	NMSUB	C13, C13, C14, A2
	NMSUB	C23, C23, C24, A2

	ST		B2, 1 * SIZE(CO1)
	NMSUB	C33, C33, C34, A2
	NMSUB	C43, C43, C44, A2

	ST		B4, 3 * SIZE(CO1)
	MADD	C11, C11, C12, A2
	MADD	C21, C21, C22, A2

	ST		B6, 5 * SIZE(CO1)
	MADD	C31, C31, C32, A2
	MADD	C41, C41, C42, A2

	ST		B8, 7 * SIZE(CO1)
	ST		C13, 0 * SIZE(CO2)
	ST		C23, 2 * SIZE(CO2)
	ST		C33, 4 * SIZE(CO2)
	ST		C43, 6 * SIZE(CO2)
	ST		C11, 1 * SIZE(CO2)
	ST		C21, 3 * SIZE(CO2)
	ST		C31, 5 * SIZE(CO2)
	ST		C41, 7 * SIZE(CO2)

#endif

#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
	/*	(a - bi) * (c - di) */
	SUB		C11, C11, A1		#	ac'+'bd
	SUB		C21, C21, A2
	SUB		C31, C31, A3
	LD		A1, 152($sp)		#	load alpha_r
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	SUB		C41, C41, A4
	LD		A2, 160($sp)
#	LD		A2, 0 * SIZE(A)		#	load alpha_i

	ADD		C13, A5, C13		#	ad'+'cb
	ADD		C23, A6, C23
	ADD		C33, A7, C33
	ADD		C43, A8, C43
	SUB		C12, C12, B1
	SUB		C22, C22, B2
	SUB		C32, C32, B3
	SUB		C42, C42, B4
	ADD		C14, B5, C14
	ADD		C24, B6, C24
	ADD		C34, B7, C34
	ADD		C44, B8, C44

	NEG		C13, C13
	NEG		C23, C23
	NEG		C33, C33
	NEG		C43, C43
	NEG		C14, C14
	NEG		C24, C24
	NEG		C34, C34
	NEG		C44, C44

	MUL		B1, C11, A1		#	A1 = alpha_r
	MUL		B3, C21, A1
	MUL		B5, C31, A1
	MUL		B7, C41, A1
	MUL		B2, C13, A1
	MUL		B4, C23, A1
	MUL		B6, C33, A1
	MUL		B8, C43, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	NMSUB	B5, B5, C33, A2
	NMSUB	B7, B7, C43, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2
	MADD	B6, B6, C31, A2
	MADD	B8, B8, C41, A2

	ST		B1, 0 * SIZE(CO1)
	MUL		C13, C12, A1
	MUL		C23, C22, A1

	ST		B3, 2 * SIZE(CO1)
	MUL		C33, C32, A1
	MUL		C43, C42, A1

	ST		B5, 4 * SIZE(CO1)
	MUL		C11, C14, A1
	MUL		C21, C24, A1

	ST		B7, 6 * SIZE(CO1)
	MUL		C31, C34, A1
	MUL		C41, C44, A1

	ST		B2, 1 * SIZE(CO1)
	NMSUB	C13, C13, C14, A2
	NMSUB	C23, C23, C24, A2

	ST		B4, 3 * SIZE(CO1)
	NMSUB	C33, C33, C34, A2
	NMSUB	C43, C43, C44, A2

	ST		B6, 5 * SIZE(CO1)
	MADD	C11, C11, C12, A2
	MADD	C21, C21, C22, A2

	ST		B8, 7 * SIZE(CO1)
	MADD	C31, C31, C32, A2
	MADD	C41, C41, C42, A2

	ST		C13, 0 * SIZE(CO2)
	ST		C23, 2 * SIZE(CO2)
	ST		C33, 4 * SIZE(CO2)
	ST		C43, 6 * SIZE(CO2)
	ST		C11, 1 * SIZE(CO2)
	ST		C21, 3 * SIZE(CO2)
	ST		C31, 5 * SIZE(CO2)
	ST		C41, 7 * SIZE(CO2)
#endif


#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP, K, KK
#ifdef LEFT
	daddiu	TEMP, TEMP, -4
#else
	daddiu	TEMP, TEMP, -2
#endif

	dsll	L, TEMP, 2 + ZBASE_SHIFT
	dsll	TEMP, TEMP, 1 + ZBASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, BO, TEMP
#endif

#ifdef LEFT
	daddiu	KK, KK, 4
#endif

#endif
	daddiu	CO1, CO1, 8 * SIZE
	bgtz	I, .L241
	daddiu	CO2, CO2, 8 * SIZE

	.align	4
.L22:
	andi	I, M, 2				#	MR=4
	blez	I, .L21
	NOP

	.align	4
.L221:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	BO,  B
#else
	dsll	TEMP, KK, 1 + ZBASE_SHIFT		#	NR=2

	daddu	AO, AO, TEMP
	daddu	BO, B,  TEMP
#endif
	MTC		$0, C11				#	CLEAR REAULTS REGISTERS
	MOV		C12, C11

	MOV		C21, C11
	MOV		C22, C11
	gsLQC1(R13, F9, F8, 0)		#	B1 B2

	gsLQC1(R12, F1, F0, 0)		#	A1 A2
	MOV		C13, C11
	MOV		C14, C11

	MOV		C23, C11
	FETCH	$0, 0 * SIZE(CO1)

	FETCH	$0, 8 * SIZE(CO1)
	MOV		C24, C11

	FETCH	$0, 0 * SIZE(CO2)
	FETCH	$0, 8 * SIZE(CO2)

	PLU		B3,	B1, B1
	PLU		B4, B2, B2
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, K, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 2							#	MR=2
#else
	daddiu	TEMP, KK, 2							#	NR=2
#endif
	dsra	L,  TEMP, 2
	blez	L, .L222
	NOP

#else
	move	BO, B				#	Reset	B
	dsra	L, K, 2				#	UnRoll	K=64

	MTC		$0, C11				#	CLEAR REAULTS REGISTERS
	MOV		C12, C11

	MOV		C21, C11
	MOV		C22, C11
	gsLQC1(R13, F9, F8, 0)		#	B1 B2

	gsLQC1(R12, F1, F0, 0)		#	A1 A2
	MOV		C13, C11
	MOV		C14, C11

	MOV		C23, C11
	FETCH	$0, 0 * SIZE(CO1)

	FETCH	$0, 8 * SIZE(CO1)
	MOV		C24, C11

	FETCH	$0, 0 * SIZE(CO2)
	FETCH	$0, 8 * SIZE(CO2)

	PLU		B3,	B1, B1
	blez	L, .L222
	PLU		B4, B2, B2
#endif

.L2210:
	daddiu	L, L, -1
	gsLQC1(R13, F13, F12, 1)	#	B3 B4
	MADPS	C11, C11, A1, B1
	MADPS	C21, C21, A2, B1

	gsLQC1(R12, F3, F2, 1)		#	A3 A4
	MADPS	C12, C12, A1, B2
	MADPS	C22, C22, A2, B2

	MADPS	C13, C13, A1, B3
	MADPS	C23, C23, A2, B3

	MADPS	C14, C14, A1, B4
	MADPS	C24, C24, A2, B4

	gsLQC1(R12, F5, F4, 2)		#	A5 A6
	PLU		B7,	B5, B5
	PLU		B8, B6, B6

	gsLQC1(R13, F9, F8, 2)		#	B1 B2
	MADPS	C11, C11, A3, B5
	MADPS	C21, C21, A4, B5

	MADPS	C12, C12, A3, B6
	MADPS	C22, C22, A4, B6

	MADPS	C13, C13, A3, B7
	MADPS	C23, C23, A4, B7

	MADPS	C14, C14, A3, B8
	MADPS	C24, C24, A4, B8

	gsLQC1(R12, F7, F6, 3)		#	A7 A8
	PLU		B3,	B1, B1
	PLU		B4, B2, B2

	gsLQC1(R13, F13, F12, 3)	#	B3 B4
	MADPS	C11, C11, A5, B1
	MADPS	C21, C21, A6, B1

	MADPS	C12, C12, A5, B2
	MADPS	C22, C22, A6, B2
	daddiu	BO, BO, 4 * 4 * SIZE	#	4KR*4NR

	daddiu	AO, AO, 4 * 4 * SIZE 	#	4KR*8MR
	MADPS	C13, C13, A5, B3
	MADPS	C23, C23, A6, B3

	MADPS	C14, C14, A5, B4
	MADPS	C24, C24, A6, B4

	gsLQC1(R12, F1, F0, 0)		#	A1 A2
	PLU		B7,	B5, B5
	PLU		B8, B6, B6

	gsLQC1(R13, F9, F8, 0)		#	B1 B2
	MADPS	C11, C11, A7, B5
	MADPS	C21, C21, A8, B5

	MADPS	C12, C12, A7, B6
	MADPS	C22, C22, A8, B6

	MADPS	C13, C13, A7, B7
	MADPS	C23, C23, A8, B7

	MADPS	C14, C14, A7, B8
	MADPS	C24, C24, A8, B8

	PLU		B3,	B1, B1
	bgtz	L, .L2210
	PLU		B4, B2, B2


	.align	4
.L222:
#ifndef TRMMKERNEL
	andi	L, K, 2
#else
	andi	L, TEMP, 2
#endif
	blez	L, .L227
	NOP

	gsLQC1(R13, F13, F12, 1)	#	B3 B4
	MADPS	C11, C11, A1, B1
	MADPS	C21, C21, A2, B1

	gsLQC1(R12, F3, F2, 1)		#	A3 A4
	MADPS	C12, C12, A1, B2
	MADPS	C22, C22, A2, B2

	MADPS	C13, C13, A1, B3
	MADPS	C23, C23, A2, B3

	MADPS	C14, C14, A1, B4
	MADPS	C24, C24, A2, B4

	PLU		B7,	B5, B5
	PLU		B8, B6, B6
	daddiu	BO, BO, 2 * 4 * SIZE

	daddiu	AO, AO, 2 * 4 * SIZE
	MADPS	C11, C11, A3, B5
	MADPS	C21, C21, A4, B5
	gsLQC1(R13, F9, F8, 0)		#	A1 A2

	MADPS	C12, C12, A3, B6
	MADPS	C22, C22, A4, B6
	gsLQC1(R12, F1, F0, 0)		#	A1 A2

	MADPS	C13, C13, A3, B7
	MADPS	C23, C23, A4, B7

	MADPS	C14, C14, A3, B8
	MADPS	C24, C24, A4, B8

	PLU		B3,	B1, B1
	PLU		B4, B2, B2


	.align	4
.L227:
#ifndef TRMMKERNEL
	andi	L, K, 1
#else
	andi	L, TEMP, 1
#endif
	blez	L, .L220
	NOP

	MADPS	C11, C11, A1, B1
	MADPS	C21, C21, A2, B1
	daddiu	BO, BO, 4 * SIZE
	daddiu	AO, AO, 4 * SIZE

	MADPS	C12, C12, A1, B2
	MADPS	C22, C22, A2, B2

	MADPS	C13, C13, A1, B3
	MADPS	C23, C23, A2, B3

	MADPS	C14, C14, A1, B4
	MADPS	C24, C24, A2, B4

	.align	4
.L220:							#	Write Back
#ifndef TRMMKERNEL
	daddiu	I, I, -1
	CVTU	A1, C11
	CVTU	A2, C21

	CVTU	A3, C13
	CVTU	A4, C23

	CVTU	A5, C12
	CVTU	A6, C22

	CVTU	A7, C14
	CVTU	A8, C24


#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	/*	(a + bi) * (c + di) */
	SUB		C11, C11, A1		#	ac'+'bd
	SUB		C21, C21, A2
	ADD		C13, A3, C13		#	ad'+'cb
	ADD		C23, A4, C23
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A1, 152($sp)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i
#	LD		A2, 0 * SIZE(A)		#	load alpha_i
	SUB		C12, C12, A5
	SUB		C22, C22, A6
	ADD		C14, A7, C14
	ADD		C24, A8, C24

	LD		B1, 0 * SIZE(CO1)
	LD		B3, 2 * SIZE(CO1)
	LD		B2, 1 * SIZE(CO1)
	LD		B4, 3 * SIZE(CO1)

	MADD	B1, B1, C11, A1		#	A1 = alpha_r
	MADD	B3, B3, C21, A1
	MADD	B2, B2, C13, A1
	MADD	B4, B4, C23, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2

	LD		B5, 0 * SIZE(CO2)
	LD		B7, 2 * SIZE(CO2)
	LD		B6, 1 * SIZE(CO2)
	LD		B8, 3 * SIZE(CO2)

	MADD	B5, B5, C12, A1
	MADD	B7, B7, C22, A1

	ST		B1, 0 * SIZE(CO1)
	ST		B3, 2 * SIZE(CO1)

	MADD	B6, B6, C14, A1
	MADD	B8, B8, C24, A1

	ST		B2, 1 * SIZE(CO1)
	ST		B4, 3 * SIZE(CO1)

	NMSUB	B5, B5, C14, A2
	NMSUB	B7, B7, C24, A2

	MADD	B6, B6, C12, A2
	MADD	B8, B8, C22, A2

	ST		B5, 0 * SIZE(CO2)
	ST		B7, 2 * SIZE(CO2)
	ST		B6, 1 * SIZE(CO2)
	ST		B8, 3 * SIZE(CO2)
#endif

#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
	/*	(a + bi) * (c - di) */
	ADD		C11, A1, C11		#	ac'+'bd
	ADD		C21, A2, C21
	SUB		C13, A3, C13		#	ad'+'cb
	SUB		C23, A4, C23
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A1, 152($sp)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i
#	LD		A2, 0 * SIZE(A)		#	load alpha_r
	ADD		C12, A5, C12
	ADD		C22, A6, C22
	SUB		C14, A7, C14
	SUB		C24, A8, C24

	LD		B1, 0 * SIZE(CO1)
	LD		B3, 2 * SIZE(CO1)
	LD		B2, 1 * SIZE(CO1)
	LD		B4, 3 * SIZE(CO1)

	MADD	B1, B1, C11, A1		#	A1 = alpha_r
	MADD	B3, B3, C21, A1
	MADD	B2, B2, C13, A1
	MADD	B4, B4, C23, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2

	LD		B5, 0 * SIZE(CO2)
	LD		B7, 2 * SIZE(CO2)
	LD		B6, 1 * SIZE(CO2)
	LD		B8, 3 * SIZE(CO2)

	MADD	B5, B5, C12, A1
	MADD	B7, B7, C22, A1

	ST		B1, 0 * SIZE(CO1)
	ST		B3, 2 * SIZE(CO1)

	MADD	B6, B6, C14, A1
	MADD	B8, B8, C24, A1

	ST		B2, 1 * SIZE(CO1)
	ST		B4, 3 * SIZE(CO1)

	NMSUB	B5, B5, C14, A2
	NMSUB	B7, B7, C24, A2

	MADD	B6, B6, C12, A2
	MADD	B8, B8, C22, A2

	ST		B5, 0 * SIZE(CO2)
	ST		B7, 2 * SIZE(CO2)
	ST		B6, 1 * SIZE(CO2)
	ST		B8, 3 * SIZE(CO2)

#endif

#if	  defined(RN) || defined(RT) || defined(CN) || defined(CT)
	/*	(a - bi) * (c + di) */
	ADD		C11, A1, C11		#	ac'+'bd
	ADD		C21, A2, C21
	SUB		C13, C13, A3		#	ad'+'cb
	SUB		C23, C23, A4
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A1, 152($sp)		#	load alpha_r
#	LD		A2, 0 * SIZE(A)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i
	ADD		C12, A5, C12
	ADD		C22, A6, C22
	SUB		C14, C14, A7
	SUB		C24, C24, A8

	LD		B1, 0 * SIZE(CO1)
	LD		B3, 2 * SIZE(CO1)
	LD		B2, 1 * SIZE(CO1)
	LD		B4, 3 * SIZE(CO1)

	MADD	B1, B1, C11, A1		#	A1 = alpha_r
	MADD	B3, B3, C21, A1
	MADD	B2, B2, C13, A1
	MADD	B4, B4, C23, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2

	LD		B5, 0 * SIZE(CO2)
	LD		B7, 2 * SIZE(CO2)
	LD		B6, 1 * SIZE(CO2)
	LD		B8, 3 * SIZE(CO2)

	MADD	B5, B5, C12, A1
	MADD	B7, B7, C22, A1

	ST		B1, 0 * SIZE(CO1)
	ST		B3, 2 * SIZE(CO1)

	MADD	B6, B6, C14, A1
	MADD	B8, B8, C24, A1

	ST		B2, 1 * SIZE(CO1)
	ST		B4, 3 * SIZE(CO1)

	NMSUB	B5, B5, C14, A2
	NMSUB	B7, B7, C24, A2

	MADD	B6, B6, C12, A2
	MADD	B8, B8, C22, A2

	ST		B5, 0 * SIZE(CO2)
	ST		B7, 2 * SIZE(CO2)
	ST		B6, 1 * SIZE(CO2)
	ST		B8, 3 * SIZE(CO2)

#endif

#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
	/*	(a - bi) * (c - di) */
	SUB		C11, C11, A1		#	ac'+'bd
	SUB		C21, C21, A2
	ADD		C13, A3, C13		#	ad'+'cb
	ADD		C23, A4, C23
	LD		A1, 152($sp)		#	load alpha_r
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A2, 160($sp)
#	LD		A2, 0 * SIZE(A)		#	load alpha_i
	SUB		C12, C12, A5
	SUB		C22, C22, A6
	ADD		C14, A7, C14
	ADD		C24, A8, C24
	NEG		C13, C13
	NEG		C23, C23
	NEG		C14, C14
	NEG		C24, C24


	LD		B1, 0 * SIZE(CO1)
	LD		B3, 2 * SIZE(CO1)
	LD		B2, 1 * SIZE(CO1)
	LD		B4, 3 * SIZE(CO1)

	MADD	B1, B1, C11, A1		#	A1 = alpha_r
	MADD	B3, B3, C21, A1
	MADD	B2, B2, C13, A1
	MADD	B4, B4, C23, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2

	LD		B5, 0 * SIZE(CO2)
	LD		B7, 2 * SIZE(CO2)
	LD		B6, 1 * SIZE(CO2)
	LD		B8, 3 * SIZE(CO2)

	MADD	B5, B5, C12, A1
	MADD	B7, B7, C22, A1

	ST		B1, 0 * SIZE(CO1)
	ST		B3, 2 * SIZE(CO1)

	MADD	B6, B6, C14, A1
	MADD	B8, B8, C24, A1

	ST		B2, 1 * SIZE(CO1)
	ST		B4, 3 * SIZE(CO1)

	NMSUB	B5, B5, C14, A2
	NMSUB	B7, B7, C24, A2

	MADD	B6, B6, C12, A2
	MADD	B8, B8, C22, A2

	ST		B5, 0 * SIZE(CO2)
	ST		B7, 2 * SIZE(CO2)
	ST		B6, 1 * SIZE(CO2)
	ST		B8, 3 * SIZE(CO2)
#endif

#else
	daddiu	I, I, -1
	CVTU	A1, C11
	CVTU	A2, C21

	CVTU	A3, C13
	CVTU	A4, C23

	CVTU	A5, C12
	CVTU	A6, C22

	CVTU	A7, C14
	CVTU	A8, C24


#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	/*	(a + bi) * (c + di) */
	SUB		C11, C11, A1		#	ac'+'bd
	SUB		C21, C21, A2
	ADD		C13, A3, C13		#	ad'+'cb
	ADD		C23, A4, C23
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A1, 152($sp)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i
#	LD		A2, 0 * SIZE(A)		#	load alpha_i
	SUB		C12, C12, A5
	SUB		C22, C22, A6
	ADD		C14, A7, C14
	ADD		C24, A8, C24

	MUL	B1, C11, A1		#	A1 = alpha_r
	MUL	B3, C21, A1
	MUL	B2, C13, A1
	MUL	B4, C23, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2


	MUL	B5, C12, A1
	MUL	B7, C22, A1

	ST		B1, 0 * SIZE(CO1)
	ST		B3, 2 * SIZE(CO1)

	MUL	B6, C14, A1
	MUL	B8, C24, A1

	ST		B2, 1 * SIZE(CO1)
	ST		B4, 3 * SIZE(CO1)

	NMSUB	B5, B5, C14, A2
	NMSUB	B7, B7, C24, A2

	MADD	B6, B6, C12, A2
	MADD	B8, B8, C22, A2

	ST		B5, 0 * SIZE(CO2)
	ST		B7, 2 * SIZE(CO2)
	ST		B6, 1 * SIZE(CO2)
	ST		B8, 3 * SIZE(CO2)
#endif

#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
	/*	(a + bi) * (c - di) */
	ADD		C11, A1, C11		#	ac'+'bd
	ADD		C21, A2, C21
	SUB		C13, A3, C13		#	ad'+'cb
	SUB		C23, A4, C23
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A1, 152($sp)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i
#	LD		A2, 0 * SIZE(A)		#	load alpha_r
	ADD		C12, A5, C12
	ADD		C22, A6, C22
	SUB		C14, A7, C14
	SUB		C24, A8, C24

	MUL	B1, C11, A1		#	A1 = alpha_r
	MUL	B3, C21, A1
	MUL	B2, C13, A1
	MUL	B4, C23, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2

	MUL	B5, C12, A1
	MUL	B7, C22, A1

	ST		B1, 0 * SIZE(CO1)
	ST		B3, 2 * SIZE(CO1)

	MUL	B6, C14, A1
	MUL	B8, C24, A1

	ST		B2, 1 * SIZE(CO1)
	ST		B4, 3 * SIZE(CO1)

	NMSUB	B5, B5, C14, A2
	NMSUB	B7, B7, C24, A2

	MADD	B6, B6, C12, A2
	MADD	B8, B8, C22, A2

	ST		B5, 0 * SIZE(CO2)
	ST		B7, 2 * SIZE(CO2)
	ST		B6, 1 * SIZE(CO2)
	ST		B8, 3 * SIZE(CO2)

#endif

#if	  defined(RN) || defined(RT) || defined(CN) || defined(CT)
	/*	(a - bi) * (c + di) */
	ADD		C11, A1, C11		#	ac'+'bd
	ADD		C21, A2, C21
	SUB		C13, C13, A3		#	ad'+'cb
	SUB		C23, C23, A4
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A1, 152($sp)		#	load alpha_r
#	LD		A2, 0 * SIZE(A)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i
	ADD		C12, A5, C12
	ADD		C22, A6, C22
	SUB		C14, C14, A7
	SUB		C24, C24, A8

	MUL	B1, C11, A1		#	A1 = alpha_r
	MUL	B3, C21, A1
	MUL	B2, C13, A1
	MUL	B4, C23, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2

	MUL	B5, C12, A1
	MUL	B7, C22, A1

	ST		B1, 0 * SIZE(CO1)
	ST		B3, 2 * SIZE(CO1)

	MUL	B6, C14, A1
	MUL	B8, C24, A1

	ST		B2, 1 * SIZE(CO1)
	ST		B4, 3 * SIZE(CO1)

	NMSUB	B5, B5, C14, A2
	NMSUB	B7, B7, C24, A2

	MADD	B6, B6, C12, A2
	MADD	B8, B8, C22, A2

	ST		B5, 0 * SIZE(CO2)
	ST		B7, 2 * SIZE(CO2)
	ST		B6, 1 * SIZE(CO2)
	ST		B8, 3 * SIZE(CO2)

#endif

#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
	/*	(a - bi) * (c - di) */
	SUB		C11, C11, A1		#	ac'+'bd
	SUB		C21, C21, A2
	ADD		C13, A3, C13		#	ad'+'cb
	ADD		C23, A4, C23
	LD		A1, 152($sp)		#	load alpha_r
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A2, 160($sp)
#	LD		A2, 0 * SIZE(A)		#	load alpha_i
	SUB		C12, C12, A5
	SUB		C22, C22, A6
	ADD		C14, A7, C14
	ADD		C24, A8, C24
	NEG		C13, C13
	NEG		C23, C23
	NEG		C14, C14
	NEG		C24, C24

	MUL	B1, C11, A1		#	A1 = alpha_r
	MUL	B3, C21, A1
	MUL	B2, C13, A1
	MUL	B4, C23, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2

	MUL	B5, C12, A1
	MUL	B7, C22, A1

	ST		B1, 0 * SIZE(CO1)
	ST		B3, 2 * SIZE(CO1)

	MUL	B6, C14, A1
	MUL	B8, C24, A1

	ST		B2, 1 * SIZE(CO1)
	ST		B4, 3 * SIZE(CO1)

	NMSUB	B5, B5, C14, A2
	NMSUB	B7, B7, C24, A2

	MADD	B6, B6, C12, A2
	MADD	B8, B8, C22, A2

	ST		B5, 0 * SIZE(CO2)
	ST		B7, 2 * SIZE(CO2)
	ST		B6, 1 * SIZE(CO2)
	ST		B8, 3 * SIZE(CO2)
#endif

#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP, K, KK
#ifdef LEFT
	daddiu	TEMP, TEMP, -2
#else
	daddiu	TEMP, TEMP, -2
#endif
	dsll	TEMP, TEMP, 1 + ZBASE_SHIFT

	daddu	AO, AO, TEMP
	daddu	BO, BO, TEMP
#endif

#ifdef LEFT
	daddiu	KK, KK, 2
#endif

#endif
	daddiu	CO1, CO1, 4 * SIZE
	daddiu	CO2, CO2, 4 * SIZE


	.align	4
.L21:
	andi	I, M, 1
	blez	I, .L20
	NOP

	.align	4
.L211:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	BO,  B
#else
	dsll	L,    KK, ZBASE_SHIFT			#	MR=1
	dsll	TEMP, KK, 1 + ZBASE_SHIFT		#	NR=2

	daddu	AO, AO, L
	daddu	BO, B,  TEMP
#endif
	MTC		$0, C11				#	CLEAR REAULTS REGISTERS
	MOV		C12, C11
	gsLQC1(R13, F9, F8, 0)		#	B1 B2

	gsLQC1(R12, F1, F0, 0)		#	A1 A2
	MOV		C13, C11
	MOV		C14, C11

	FETCH	$0, 0 * SIZE(CO1)
	FETCH	$0, 0 * SIZE(CO2)

	PLU		B3,	B1, B1
	PLU		B4, B2, B2
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, K, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 1							#	MR=1
#else
	daddiu	TEMP, KK, 2							#	NR=2
#endif
	dsra	L,  TEMP, 2
	blez	L, .L212
	NOP

#else
	move	BO, B				#	Reset	B
	dsra	L, K, 2				#	UnRoll	K=64

	MTC		$0, C11				#	CLEAR REAULTS REGISTERS
	MOV		C12, C11
	gsLQC1(R13, F9, F8, 0)		#	B1 B2

	gsLQC1(R12, F1, F0, 0)		#	A1 A2
	MOV		C13, C11
	MOV		C14, C11

	FETCH	$0, 0 * SIZE(CO1)
	FETCH	$0, 0 * SIZE(CO2)

	PLU		B3,	B1, B1
	blez	L, .L212
	PLU		B4, B2, B2
#endif

.L2110:
	daddiu	L, L, -1
	gsLQC1(R13, F13, F12, 1)	#	B3 B4
	MADPS	C11, C11, A1, B1
	MADPS	C12, C12, A1, B2

	MADPS	C13, C13, A1, B3
	MADPS	C14, C14, A1, B4

	PLU		B7,	B5, B5
	PLU		B8, B6, B6

	gsLQC1(R13, F9, F8, 2)		#	B1 B2
	MADPS	C11, C11, A2, B5
	MADPS	C12, C12, A2, B6

	gsLQC1(R12, F3, F2, 1)		#	A3 A4
	MADPS	C13, C13, A2, B7
	MADPS	C14, C14, A2, B8

	PLU		B3,	B1, B1
	PLU		B4, B2, B2

	gsLQC1(R13, F13, F12, 3)	#	B3 B4
	MADPS	C11, C11, A3, B1
	MADPS	C12, C12, A3, B2
	daddiu	BO, BO, 4 * 4 * SIZE	#	4KR*4NR

	daddiu	AO, AO, 2 * 4 * SIZE 	#	4KR*8MR
	MADPS	C13, C13, A3, B3
	MADPS	C14, C14, A3, B4

	PLU		B7,	B5, B5
	PLU		B8, B6, B6

	gsLQC1(R13, F9, F8, 0)		#	B1 B2
	MADPS	C11, C11, A4, B5
	MADPS	C12, C12, A4, B6

	gsLQC1(R12, F1, F0, 0)		#	A1 A2
	MADPS	C13, C13, A4, B7
	MADPS	C14, C14, A4, B8

	PLU		B3,	B1, B1
	bgtz	L, .L2110
	PLU		B4, B2, B2


	.align	4
.L212:
#ifndef TRMMKERNEL
	andi	L, K, 2
#else
	andi	L, TEMP, 2
#endif
	blez	L, .L217
	NOP

	gsLQC1(R13, F13, F12, 1)	#	B3 B4
	MADPS	C11, C11, A1, B1
	MADPS	C12, C12, A1, B2

	MADPS	C13, C13, A1, B3
	MADPS	C14, C14, A1, B4

	PLU		B7,	B5, B5
	PLU		B8, B6, B6
	daddiu	BO, BO, 2 * 4 * SIZE

	MADPS	C11, C11, A2, B5
	MADPS	C12, C12, A2, B6
	daddiu	AO, AO, 4 * SIZE

	MADPS	C13, C13, A2, B7
	MADPS	C14, C14, A2, B8

	gsLQC1(R12, F1, F0, 0)		#	A5 A6
	gsLQC1(R13, F9, F8, 0)		#	B1 B2
	PLU		B3,	B1, B1
	PLU		B4, B2, B2


	.align	4
.L217:
#ifndef TRMMKERNEL
	andi	L, K, 1
#else
	andi	L, TEMP, 1
#endif
	blez	L, .L210
	NOP

	MADPS	C11, C11, A1, B1
	daddiu	BO, BO, 4 * SIZE
	MADPS	C12, C12, A1, B2
	daddiu	AO, AO, 2 * SIZE

	MADPS	C13, C13, A1, B3
	MADPS	C14, C14, A1, B4

	.align	4
.L210:							#	Write Back
#ifndef TRMMKERNEL
	daddiu	I, I, -1
	CVTU	A1, C11
	CVTU	A3, C13
	CVTU	A5, C12
	CVTU	A7, C14

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	/*	(a + bi) * (c + di) */
	SUB		C11, C11, A1		#	ac'+'bd
	ADD		C13, A3, C13		#	ad'+'cb
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A4, 152($sp)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i
#	LD		A2, 0 * SIZE(A)		#	load alpha_i
	SUB		C12, C12, A5
	ADD		C14, A7, C14

	LD		B1, 0 * SIZE(CO1)
	LD		B2, 1 * SIZE(CO1)

	MADD	B1, B1, C11, A4		#	A1 = alpha_r
	MADD	B2, B2, C13, A4
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	MADD	B2, B2, C11, A2

	LD		B5, 0 * SIZE(CO2)
	LD		B6, 1 * SIZE(CO2)

	MADD	B5, B5, C12, A4
	ST		B1, 0 * SIZE(CO1)
	MADD	B6, B6, C14, A4
	ST		B2, 1 * SIZE(CO1)

	NMSUB	B5, B5, C14, A2
	MADD	B6, B6, C12, A2

	ST		B5, 0 * SIZE(CO2)
	ST		B6, 1 * SIZE(CO2)
#endif

#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
	/*	(a + bi) * (c - di) */
	ADD		C11, A1, C11		#	ac'+'bd
	SUB		C13, A3, C13		#	ad'+'cb
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A4, 152($sp)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i
#	LD		A2, 0 * SIZE(A)		#	load alpha_r
	ADD		C12, A5, C12
	SUB		C14, A7, C14

	LD		B1, 0 * SIZE(CO1)
	LD		B2, 1 * SIZE(CO1)

	MADD	B1, B1, C11, A4		#	A1 = alpha_r
	MADD	B2, B2, C13, A4
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	MADD	B2, B2, C11, A2

	LD		B5, 0 * SIZE(CO2)
	LD		B6, 1 * SIZE(CO2)

	MADD	B5, B5, C12, A4
	ST		B1, 0 * SIZE(CO1)
	MADD	B6, B6, C14, A4
	ST		B2, 1 * SIZE(CO1)

	NMSUB	B5, B5, C14, A2
	MADD	B6, B6, C12, A2

	ST		B5, 0 * SIZE(CO2)
	ST		B6, 1 * SIZE(CO2)

#endif

#if	  defined(RN) || defined(RT) || defined(CN) || defined(CT)
	/*	(a - bi) * (c + di) */
	ADD		C11, A1, C11		#	ac'+'bd
	SUB		C13, C13, A3		#	ad'+'cb
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A4, 152($sp)		#	load alpha_r
#	LD		A2, 0 * SIZE(A)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i
	ADD		C12, A5, C12
	SUB		C14, C14, A7

	LD		B1, 0 * SIZE(CO1)
	LD		B2, 1 * SIZE(CO1)

	MADD	B1, B1, C11, A4		#	A1 = alpha_r
	MADD	B2, B2, C13, A4
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	MADD	B2, B2, C11, A2

	LD		B5, 0 * SIZE(CO2)
	LD		B6, 1 * SIZE(CO2)

	MADD	B5, B5, C12, A4
	ST		B1, 0 * SIZE(CO1)
	MADD	B6, B6, C14, A4
	ST		B2, 1 * SIZE(CO1)

	NMSUB	B5, B5, C14, A2
	MADD	B6, B6, C12, A2

	ST		B5, 0 * SIZE(CO2)
	ST		B6, 1 * SIZE(CO2)
#endif

#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
	/*	(a - bi) * (c - di) */
	SUB		C11, C11, A1		#	ac'+'bd
	ADD		C13, A3, C13		#	ad'+'cb
	LD		A4, 152($sp)		#	load alpha_r
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A2, 160($sp)
#	LD		A2, 0 * SIZE(A)		#	load alpha_i
	SUB		C12, C12, A5
	ADD		C14, A7, C14
	NEG		C13, C13
	LD		B1, 0 * SIZE(CO1)
	LD		B2, 1 * SIZE(CO1)
	NEG		C14, C14

	MADD	B1, B1, C11, A4		#	A1 = alpha_r
	MADD	B2, B2, C13, A4
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	MADD	B2, B2, C11, A2

	LD		B5, 0 * SIZE(CO2)
	LD		B6, 1 * SIZE(CO2)

	MADD	B5, B5, C12, A4
	ST		B1, 0 * SIZE(CO1)
	MADD	B6, B6, C14, A4
	ST		B2, 1 * SIZE(CO1)

	NMSUB	B5, B5, C14, A2
	MADD	B6, B6, C12, A2

	ST		B5, 0 * SIZE(CO2)
	ST		B6, 1 * SIZE(CO2)
#endif

#else
	daddiu	I, I, -1
	CVTU	A1, C11
	CVTU	A3, C13
	CVTU	A5, C12
	CVTU	A7, C14

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	/*	(a + bi) * (c + di) */
	SUB		C11, C11, A1		#	ac'+'bd
	ADD		C13, A3, C13		#	ad'+'cb
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A4, 152($sp)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i
#	LD		A2, 0 * SIZE(A)		#	load alpha_i
	SUB		C12, C12, A5
	ADD		C14, A7, C14

	MUL B1, C11, A4		#	A1 = alpha_r
	MUL B2, C13, A4
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	MADD	B2, B2, C11, A2

	MUL B5, C12, A4
	ST		B1, 0 * SIZE(CO1)
	MUL B6, C14, A4
	ST		B2, 1 * SIZE(CO1)

	NMSUB	B5, B5, C14, A2
	MADD	B6, B6, C12, A2

	ST		B5, 0 * SIZE(CO2)
	ST		B6, 1 * SIZE(CO2)
#endif

#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
	/*	(a + bi) * (c - di) */
	ADD		C11, A1, C11		#	ac'+'bd
	SUB		C13, A3, C13		#	ad'+'cb
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A4, 152($sp)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i
#	LD		A2, 0 * SIZE(A)		#	load alpha_r
	ADD		C12, A5, C12
	SUB		C14, A7, C14

	MUL B1, C11, A4		#	A1 = alpha_r
	MUL B2, C13, A4
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	MADD	B2, B2, C11, A2

	MUL B5, C12, A4
	ST		B1, 0 * SIZE(CO1)
	MUL B6, C14, A4
	ST		B2, 1 * SIZE(CO1)

	NMSUB	B5, B5, C14, A2
	MADD	B6, B6, C12, A2

	ST		B5, 0 * SIZE(CO2)
	ST		B6, 1 * SIZE(CO2)

#endif

#if	  defined(RN) || defined(RT) || defined(CN) || defined(CT)
	/*	(a - bi) * (c + di) */
	ADD		C11, A1, C11		#	ac'+'bd
	SUB		C13, C13, A3		#	ad'+'cb
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A4, 152($sp)		#	load alpha_r
#	LD		A2, 0 * SIZE(A)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i
	ADD		C12, A5, C12
	SUB		C14, C14, A7

	MUL B1, C11, A4		#	A1 = alpha_r
	MUL B2, C13, A4
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	MADD	B2, B2, C11, A2

	MUL B5, C12, A4
	ST		B1, 0 * SIZE(CO1)
	MUL B6, C14, A4
	ST		B2, 1 * SIZE(CO1)

	NMSUB	B5, B5, C14, A2
	MADD	B6, B6, C12, A2

	ST		B5, 0 * SIZE(CO2)
	ST		B6, 1 * SIZE(CO2)
#endif

#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
	/*	(a - bi) * (c - di) */
	SUB		C11, C11, A1		#	ac'+'bd
	ADD		C13, A3, C13		#	ad'+'cb
	LD		A4, 152($sp)		#	load alpha_r
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A2, 160($sp)
#	LD		A2, 0 * SIZE(A)		#	load alpha_i
	SUB		C12, C12, A5
	ADD		C14, A7, C14
	NEG		C13, C13
	NEG		C14, C14

	MUL B1, C11, A4		#	A1 = alpha_r
	MUL B2, C13, A4
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	MADD	B2, B2, C11, A2

	MUL B5, C12, A4
	ST		B1, 0 * SIZE(CO1)
	MUL B6, C14, A4
	ST		B2, 1 * SIZE(CO1)

	NMSUB	B5, B5, C14, A2
	MADD	B6, B6, C12, A2

	ST		B5, 0 * SIZE(CO2)
	ST		B6, 1 * SIZE(CO2)
#endif


#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP, K, KK
#ifdef LEFT
	daddiu	TEMP, TEMP, -1
#else
	daddiu	TEMP, TEMP, -2
#endif
	dsll	L,    TEMP, ZBASE_SHIFT
	dsll	TEMP, TEMP, 1 + ZBASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, BO, TEMP
#endif

#ifdef LEFT
	daddiu	KK, KK, 1
#endif

#endif
	daddiu	CO1, CO1, 2 * SIZE
	daddiu	CO2, CO2, 2 * SIZE


	.align	4
.L20:
	daddiu	J, J, -1
	move	B, BO

#if defined(TRMMKERNEL) && !defined(LEFT)
	daddiu	KK, KK, 2
#endif

	bgtz	J, .L24
	NOP


	.align	4
.L1:
	andi	J, N, 1
	blez	J, .L999
	NOP

.L14:
	dsra	I, M, 2				#	MR=8
	move	AO, A				#	Reset A

#if defined(TRMMKERNEL) &&  defined(LEFT)
	move	KK, OFFSET
#endif

	move	CO1, C
	blez	I, .L12
	daddu	C,   CO1, LDC

	.align	4
.L141:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	BO,  B
#else
	dsll	L,    KK, 2 + ZBASE_SHIFT
	dsll	TEMP, KK, ZBASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, B,  TEMP
#endif
	MTC		$0, C11				#	CLEAR REAULTS REGISTERS
	MOV		C21, C11
	gsLQC1(R13, F9, F8, 0)		#	B1 B2

	gsLQC1(R12, F1, F0, 0)		#	A1 A2
	MOV		C31, C11
	MOV		C41, C11

	gsLQC1(R12, F3, F2, 1)		#	A3 A4
	MOV		C13, C11
	MOV		C23, C11

	FETCH	$0, 0 * SIZE(CO1)
	MOV		C33, C11
	MOV		C43, C11

	FETCH	$0, 8 * SIZE(CO1)
	PLU		B3,	B1, B1
	PLU		B4, B2, B2
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, K, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 4						#	define Mr=4
#else
	daddiu	TEMP, KK, 1						#	define	NR=1
#endif
	dsra	L,  TEMP, 2
	blez	L, .L142
	NOP

#else
	move	BO, B				#	Reset	B
	dsra	L, K, 2				#	UnRoll	K=64

	MTC		$0, C11				#	CLEAR REAULTS REGISTERS
	MOV		C21, C11
	gsLQC1(R13, F9, F8, 0)		#	B1 B2

	gsLQC1(R12, F1, F0, 0)		#	A1 A2
	MOV		C31, C11
	MOV		C41, C11

	gsLQC1(R12, F3, F2, 1)		#	A3 A4
	MOV		C13, C11
	MOV		C23, C11

	FETCH	$0, 0 * SIZE(CO1)
	MOV		C33, C11
	MOV		C43, C11

	FETCH	$0, 8 * SIZE(CO1)
	PLU		B3,	B1, B1
	blez	L, .L142
	PLU		B4, B2, B2
#endif

.L1410:
	daddiu	L, L, -1
	MADPS	C11, C11, A1, B1
	MADPS	C21, C21, A2, B1
	gsLQC1(R12, F5, F4, 2)		#	A5 A6

	gsLQC1(R12, F7, F6, 3)		#	A7 A8
	MADPS	C31, C31, A3, B1
	MADPS	C41, C41, A4, B1

	MADPS	C13, C13, A1, B3
	MADPS	C23, C23, A2, B3

	MADPS	C33, C33, A3, B3
	MADPS	C43, C43, A4, B3
	gsLQC1(R13, F13, F12, 1)	#	B3 B4

	gsLQC1(R12, F1, F0, 4)		#	A1 A2
	MADPS	C11, C11, A5, B2
	MADPS	C21, C21, A6, B2

	gsLQC1(R12, F3, F2, 5)		#	A3 A4
	MADPS	C31, C31, A7, B2
	MADPS	C41, C41, A8, B2
	daddiu	BO, BO, 2 * 4 * SIZE	#	4KR*4NR

	MADPS	C13, C13, A5, B4
	MADPS	C23, C23, A6, B4

	MADPS	C33, C33, A7, B4
	MADPS	C43, C43, A8, B4

	PLU		B7,	B5, B5
	PLU		B8, B6, B6

	MADPS	C11, C11, A1, B5
	MADPS	C21, C21, A2, B5
	gsLQC1(R12, F5, F4, 6)		#	A5 A6

	gsLQC1(R12, F7, F6, 7)		#	A7 A8
	MADPS	C31, C31, A3, B5
	MADPS	C41, C41, A4, B5

	daddiu	AO, AO, 8 * 4 * SIZE 	#	4KR*8MR
	MADPS	C13, C13, A1, B7
	MADPS	C23, C23, A2, B7

	MADPS	C33, C33, A3, B7
	MADPS	C43, C43, A4, B7
	gsLQC1(R13, F9, F8, 0)		#	B1 B2

	gsLQC1(R12, F1, F0, 0)		#	A1 A2
	MADPS	C11, C11, A5, B6
	MADPS	C21, C21, A6, B6

	gsLQC1(R12, F3, F2, 1)		#	A3 A4
	MADPS	C31, C31, A7, B6
	MADPS	C41, C41, A8, B6

	MADPS	C13, C13, A5, B8
	MADPS	C23, C23, A6, B8

	MADPS	C33, C33, A7, B8
	MADPS	C43, C43, A8, B8

	PLU		B3,	B1, B1
	bgtz	L, .L1410
	PLU		B4, B2, B2


	.align	4
.L142:
#ifndef TRMMKERNEL
	andi	L, K, 2
#else
	andi	L, TEMP, 2
#endif
	blez	L, .L147
	NOP

	MADPS	C11, C11, A1, B1
	MADPS	C21, C21, A2, B1
	gsLQC1(R12, F5, F4, 2)		#	A5 A6

	gsLQC1(R12, F7, F6, 3)		#	A7 A8
	MADPS	C31, C31, A3, B1
	MADPS	C41, C41, A4, B1
	daddiu	AO, AO, 4 * 4 * SIZE 	#	4KR*8MR

	MADPS	C13, C13, A1, B3
	MADPS	C23, C23, A2, B3

	MADPS	C33, C33, A3, B3
	MADPS	C43, C43, A4, B3
	gsLQC1(R13, F13, F8, 1)	#	B3 B4

	gsLQC1(R12, F1, F0, 0)		#	A1 A2
	MADPS	C11, C11, A5, B2
	MADPS	C21, C21, A6, B2

	gsLQC1(R12, F3, F2, 1)		#	A3 A4
	MADPS	C31, C31, A7, B2
	MADPS	C41, C41, A8, B2
	daddiu	BO, BO, 4 * SIZE	#	4KR*4NR

	MADPS	C13, C13, A5, B4
	MADPS	C23, C23, A6, B4

	MADPS	C33, C33, A7, B4
	MADPS	C43, C43, A8, B4
	PLU		B3,	B1, B1


	.align	4
.L147:
#ifndef TRMMKERNEL
	andi	L, K, 1
#else
	andi	L, TEMP, 1
#endif
	blez	L, .L140
	NOP

	MADPS	C11, C11, A1, B1
	MADPS	C21, C21, A2, B1
	daddiu	BO, BO, 2 * SIZE

	MADPS	C31, C31, A3, B1
	MADPS	C41, C41, A4, B1
	daddiu	AO, AO, 2 * 4 * SIZE

	MADPS	C13, C13, A1, B3
	MADPS	C23, C23, A2, B3

	MADPS	C33, C33, A3, B3
	MADPS	C43, C43, A4, B3


	.align	4
.L140:							#	Write Back
#ifndef TRMMKERNEL
	daddiu	I, I, -1
	CVTU	A1, C11
	CVTU	A2, C21

	CVTU	A3, C31
	CVTU	A4, C41

	CVTU	A5, C13
	CVTU	A6, C23

	CVTU	A7, C33
	CVTU	A8, C43

	CVTU	B1, C12
	CVTU	B2, C22

	CVTU	B3, C32
	CVTU	B4, C42

	CVTU	B5, C14
	CVTU	B6, C24

	CVTU	B7, C34
	CVTU	B8, C44

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	/*	(a + bi) * (c + di) */
	SUB		C11, C11, A1		#	ac'+'bd
	SUB		C21, C21, A2
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	SUB		C31, C31, A3
	LD		A1, 152($sp)		#	load alpha_r
	SUB		C41, C41, A4
	LD		A2, 160($sp)		#	load alpha_i
#	LD		A2, 0 * SIZE(A)		#	load alpha_i
	ADD		C13, A5, C13		#	ad'+'cb
	ADD		C23, A6, C23
	ADD		C33, A7, C33
	ADD		C43, A8, C43

	LD		B1, 0 * SIZE(CO1)
	LD		B3, 2 * SIZE(CO1)
	LD		B5, 4 * SIZE(CO1)
	LD		B7, 6 * SIZE(CO1)
	LD		B2, 1 * SIZE(CO1)
	LD		B4, 3 * SIZE(CO1)
	LD		B6, 5 * SIZE(CO1)
	LD		B8, 7 * SIZE(CO1)

	MADD	B1, B1, C11, A1		#	A1 = alpha_r
	MADD	B3, B3, C21, A1
	MADD	B5, B5, C31, A1
	MADD	B7, B7, C41, A1
	MADD	B2, B2, C13, A1
	MADD	B4, B4, C23, A1
	MADD	B6, B6, C33, A1
	MADD	B8, B8, C43, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	NMSUB	B5, B5, C33, A2
	NMSUB	B7, B7, C43, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2
	MADD	B6, B6, C31, A2
	MADD	B8, B8, C41, A2

	ST		B1, 0 * SIZE(CO1)
	ST		B3, 2 * SIZE(CO1)
	ST		B5, 4 * SIZE(CO1)
	ST		B7, 6 * SIZE(CO1)
	ST		B2, 1 * SIZE(CO1)
	ST		B4, 3 * SIZE(CO1)
	ST		B6, 5 * SIZE(CO1)
	ST		B8, 7 * SIZE(CO1)
#endif

#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
	/*	(a + bi) * (c - di) */
	ADD		C11, A1, C11		#	ac'+'bd
	ADD		C21, A2, C21
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	ADD		C31, A3, C31
	LD		A1, 152($sp)		#	load alpha_r
	ADD		C41, A4, C41
	LD		A2, 160($sp)		#	load alpha_i
#	LD		A2, 0 * SIZE(A)		#	load alpha_r
	SUB		C13, A5, C13		#	ad'+'cb
	SUB		C23, A6, C23
	SUB		C33, A7, C33
	SUB		C43, A8, C43

	LD		B1, 0 * SIZE(CO1)
	LD		B3, 2 * SIZE(CO1)
	LD		B5, 4 * SIZE(CO1)
	LD		B7, 6 * SIZE(CO1)
	LD		B2, 1 * SIZE(CO1)
	LD		B4, 3 * SIZE(CO1)
	LD		B6, 5 * SIZE(CO1)
	LD		B8, 7 * SIZE(CO1)

	MADD	B1, B1, C11, A1		#	A1 = alpha_r
	MADD	B3, B3, C21, A1
	MADD	B5, B5, C31, A1
	MADD	B7, B7, C41, A1
	MADD	B2, B2, C13, A1
	MADD	B4, B4, C23, A1
	MADD	B6, B6, C33, A1
	MADD	B8, B8, C43, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	NMSUB	B5, B5, C33, A2
	NMSUB	B7, B7, C43, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2
	MADD	B6, B6, C31, A2
	MADD	B8, B8, C41, A2

	ST		B1, 0 * SIZE(CO1)
	ST		B3, 2 * SIZE(CO1)
	ST		B5, 4 * SIZE(CO1)
	ST		B7, 6 * SIZE(CO1)
	ST		B2, 1 * SIZE(CO1)
	ST		B4, 3 * SIZE(CO1)
	ST		B6, 5 * SIZE(CO1)
	ST		B8, 7 * SIZE(CO1)
#endif

#if	  defined(RN) || defined(RT) || defined(CN) || defined(CT)
	/*	(a - bi) * (c + di) */
	ADD		C11, A1, C11		#	ac'+'bd
	ADD		C21, A2, C21
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	ADD		C31, A3, C31
	LD		A1, 152($sp)		#	load alpha_r
#	LD		A2, 0 * SIZE(A)		#	load alpha_r
	ADD		C41, A4, C41
	LD		A2, 160($sp)		#	load alpha_i
	SUB		C13, C13, A5		#	ad'+'cb
	SUB		C23, C23, A6
	SUB		C33, C33, A7
	SUB		C43, C43, A8

	LD		B1, 0 * SIZE(CO1)
	LD		B3, 2 * SIZE(CO1)
	LD		B5, 4 * SIZE(CO1)
	LD		B7, 6 * SIZE(CO1)
	LD		B2, 1 * SIZE(CO1)
	LD		B4, 3 * SIZE(CO1)
	LD		B6, 5 * SIZE(CO1)
	LD		B8, 7 * SIZE(CO1)

	MADD	B1, B1, C11, A1		#	A1 = alpha_r
	MADD	B3, B3, C21, A1
	MADD	B5, B5, C31, A1
	MADD	B7, B7, C41, A1
	MADD	B2, B2, C13, A1
	MADD	B4, B4, C23, A1
	MADD	B6, B6, C33, A1
	MADD	B8, B8, C43, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	NMSUB	B5, B5, C33, A2
	NMSUB	B7, B7, C43, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2
	MADD	B6, B6, C31, A2
	MADD	B8, B8, C41, A2

	ST		B1, 0 * SIZE(CO1)
	ST		B3, 2 * SIZE(CO1)
	ST		B5, 4 * SIZE(CO1)
	ST		B7, 6 * SIZE(CO1)
	ST		B2, 1 * SIZE(CO1)
	ST		B4, 3 * SIZE(CO1)
	ST		B6, 5 * SIZE(CO1)
	ST		B8, 7 * SIZE(CO1)
#endif

#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
	/*	(a - bi) * (c - di) */
	SUB		C11, C11, A1		#	AC'+'BD
	SUB		C21, C21, A2
	SUB		C31, C31, A3
	LD		A1, 152($sp)		#	LOAD ALPHA_R
#	LD		A1, 0 * SIZE(A)		#	LOAD ALPHA_R
	SUB		C41, C41, A4
	LD		A2, 160($sp)
#	LD		A2, 0 * SIZE(A)		#	LOAD ALPHA_I

	ADD		C13, A5, C13		#	AD'+'CB
	ADD		C23, A6, C23
	ADD		C33, A7, C33
	ADD		C43, A8, C43
	NEG		C13, C13		#	AD'+'CB
	NEG		C23, C23
	NEG		C33, C33
	NEG		C43, C43


	LD		B1, 0 * SIZE(CO1)
	LD		B3, 2 * SIZE(CO1)
	LD		B5, 4 * SIZE(CO1)
	LD		B7, 6 * SIZE(CO1)
	LD		B2, 1 * SIZE(CO1)
	LD		B4, 3 * SIZE(CO1)
	LD		B6, 5 * SIZE(CO1)
	LD		B8, 7 * SIZE(CO1)

	MADD	B1, B1, C11, A1		#	A1 = ALPHA_R
	MADD	B3, B3, C21, A1
	MADD	B5, B5, C31, A1
	MADD	B7, B7, C41, A1
	MADD	B2, B2, C13, A1
	MADD	B4, B4, C23, A1
	MADD	B6, B6, C33, A1
	MADD	B8, B8, C43, A1
	NMSUB	B1, B1, C13, A2		#	A2 = ALPHA_I
	NMSUB	B3, B3, C23, A2
	NMSUB	B5, B5, C33, A2
	NMSUB	B7, B7, C43, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2
	MADD	B6, B6, C31, A2
	MADD	B8, B8, C41, A2

	ST		B1, 0 * SIZE(CO1)
	ST		B3, 2 * SIZE(CO1)
	ST		B5, 4 * SIZE(CO1)
	ST		B7, 6 * SIZE(CO1)
	ST		B2, 1 * SIZE(CO1)
	ST		B4, 3 * SIZE(CO1)
	ST		B6, 5 * SIZE(CO1)
	ST		B8, 7 * SIZE(CO1)
#endif

#else
	daddiu	I, I, -1
	CVTU	A1, C11
	CVTU	A2, C21

	CVTU	A3, C31
	CVTU	A4, C41

	CVTU	A5, C13
	CVTU	A6, C23

	CVTU	A7, C33
	CVTU	A8, C43

	CVTU	B1, C12
	CVTU	B2, C22

	CVTU	B3, C32
	CVTU	B4, C42

	CVTU	B5, C14
	CVTU	B6, C24

	CVTU	B7, C34
	CVTU	B8, C44

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	/*	(a + bi) * (c + di) */
	SUB		C11, C11, A1		#	ac'+'bd
	SUB		C21, C21, A2
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	SUB		C31, C31, A3
	LD		A1, 152($sp)		#	load alpha_r
	SUB		C41, C41, A4
	LD		A2, 160($sp)		#	load alpha_i
#	LD		A2, 0 * SIZE(A)		#	load alpha_i
	ADD		C13, A5, C13		#	ad'+'cb
	ADD		C23, A6, C23
	ADD		C33, A7, C33
	ADD		C43, A8, C43

	MUL B1, C11, A1		#	A1 = alpha_r
	MUL B3, C21, A1
	MUL B5, C31, A1
	MUL B7, C41, A1
	MUL B2, C13, A1
	MUL B4, C23, A1
	MUL B6, C33, A1
	MUL B8, C43, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	NMSUB	B5, B5, C33, A2
	NMSUB	B7, B7, C43, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2
	MADD	B6, B6, C31, A2
	MADD	B8, B8, C41, A2

	ST		B1, 0 * SIZE(CO1)
	ST		B3, 2 * SIZE(CO1)
	ST		B5, 4 * SIZE(CO1)
	ST		B7, 6 * SIZE(CO1)
	ST		B2, 1 * SIZE(CO1)
	ST		B4, 3 * SIZE(CO1)
	ST		B6, 5 * SIZE(CO1)
	ST		B8, 7 * SIZE(CO1)
#endif

#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
	/*	(a + bi) * (c - di) */
	ADD		C11, A1, C11		#	ac'+'bd
	ADD		C21, A2, C21
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	ADD		C31, A3, C31
	LD		A1, 152($sp)		#	load alpha_r
	ADD		C41, A4, C41
	LD		A2, 160($sp)		#	load alpha_i
#	LD		A2, 0 * SIZE(A)		#	load alpha_r
	SUB		C13, A5, C13		#	ad'+'cb
	SUB		C23, A6, C23
	SUB		C33, A7, C33
	SUB		C43, A8, C43

	MUL B1, C11, A1		#	A1 = alpha_r
	MUL B3, C21, A1
	MUL B5, C31, A1
	MUL B7, C41, A1
	MUL B2, C13, A1
	MUL B4, C23, A1
	MUL B6, C33, A1
	MUL B8, C43, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	NMSUB	B5, B5, C33, A2
	NMSUB	B7, B7, C43, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2
	MADD	B6, B6, C31, A2
	MADD	B8, B8, C41, A2

	ST		B1, 0 * SIZE(CO1)
	ST		B3, 2 * SIZE(CO1)
	ST		B5, 4 * SIZE(CO1)
	ST		B7, 6 * SIZE(CO1)
	ST		B2, 1 * SIZE(CO1)
	ST		B4, 3 * SIZE(CO1)
	ST		B6, 5 * SIZE(CO1)
	ST		B8, 7 * SIZE(CO1)
#endif

#if	  defined(RN) || defined(RT) || defined(CN) || defined(CT)
	/*	(a - bi) * (c + di) */
	ADD		C11, A1, C11		#	ac'+'bd
	ADD		C21, A2, C21
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	ADD		C31, A3, C31
	LD		A1, 152($sp)		#	load alpha_r
#	LD		A2, 0 * SIZE(A)		#	load alpha_r
	ADD		C41, A4, C41
	LD		A2, 160($sp)		#	load alpha_i
	SUB		C13, C13, A5		#	ad'+'cb
	SUB		C23, C23, A6
	SUB		C33, C33, A7
	SUB		C43, C43, A8

	MUL B1, C11, A1		#	A1 = alpha_r
	MUL B3, C21, A1
	MUL B5, C31, A1
	MUL B7, C41, A1
	MUL B2, C13, A1
	MUL B4, C23, A1
	MUL B6, C33, A1
	MUL B8, C43, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	NMSUB	B5, B5, C33, A2
	NMSUB	B7, B7, C43, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2
	MADD	B6, B6, C31, A2
	MADD	B8, B8, C41, A2

	ST		B1, 0 * SIZE(CO1)
	ST		B3, 2 * SIZE(CO1)
	ST		B5, 4 * SIZE(CO1)
	ST		B7, 6 * SIZE(CO1)
	ST		B2, 1 * SIZE(CO1)
	ST		B4, 3 * SIZE(CO1)
	ST		B6, 5 * SIZE(CO1)
	ST		B8, 7 * SIZE(CO1)
#endif

#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
	/*	(a - bi) * (c - di) */
	SUB		C11, C11, A1		#	AC'+'BD
	SUB		C21, C21, A2
	SUB		C31, C31, A3
	LD		A1, 152($sp)		#	LOAD ALPHA_R
#	LD		A1, 0 * SIZE(A)		#	LOAD ALPHA_R
	SUB		C41, C41, A4
	LD		A2, 160($sp)
#	LD		A2, 0 * SIZE(A)		#	LOAD ALPHA_I

	ADD		C13, A5, C13		#	AD'+'CB
	ADD		C23, A6, C23
	ADD		C33, A7, C33
	ADD		C43, A8, C43
	NEG		C13, C13		#	AD'+'CB
	NEG		C23, C23
	NEG		C33, C33
	NEG		C43, C43

	MUL B1, C11, A1		#	A1 = ALPHA_R
	MUL B3, C21, A1
	MUL B5, C31, A1
	MUL B7, C41, A1
	MUL B2, C13, A1
	MUL B4, C23, A1
	MUL B6, C33, A1
	MUL B8, C43, A1
	NMSUB	B1, B1, C13, A2		#	A2 = ALPHA_I
	NMSUB	B3, B3, C23, A2
	NMSUB	B5, B5, C33, A2
	NMSUB	B7, B7, C43, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2
	MADD	B6, B6, C31, A2
	MADD	B8, B8, C41, A2

	ST		B1, 0 * SIZE(CO1)
	ST		B3, 2 * SIZE(CO1)
	ST		B5, 4 * SIZE(CO1)
	ST		B7, 6 * SIZE(CO1)
	ST		B2, 1 * SIZE(CO1)
	ST		B4, 3 * SIZE(CO1)
	ST		B6, 5 * SIZE(CO1)
	ST		B8, 7 * SIZE(CO1)
#endif


#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP, K, KK
#ifdef LEFT
	daddiu	TEMP, TEMP, -4
#else
	daddiu	TEMP, TEMP, -1
#endif

	dsll	L,    TEMP, 2 + ZBASE_SHIFT
	dsll	TEMP, TEMP, ZBASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, BO, TEMP
#endif

#ifdef LEFT
	daddiu	KK, KK, 4
#endif

#endif
	bgtz	I, .L141
	daddiu	CO1, CO1, 8 * SIZE

	.align	4
.L12:
	andi	I, M, 2				#	MR=4
	blez	I, .L11
	NOP

	.align	4
.L121:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	BO,  B
#else
	dsll	L, KK, 1 + ZBASE_SHIFT
	dsll	TEMP, KK,  ZBASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, B,  TEMP
#endif

	MTC		$0, C11				#	CLEAR REAULTS REGISTERS
	MOV		C21, C11
	gsLQC1(R13, F9, F8, 0)		#	B1 B2

	gsLQC1(R12, F1, F0, 0)		#	A1 A2
	MOV		C13, C11
	MOV		C23, C11

	FETCH	$0, 0 * SIZE(CO1)
	FETCH	$0, 8 * SIZE(CO1)

	PLU		B3,	B1, B1
	PLU		B4, B2, B2
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, K, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 2
#else
	daddiu	TEMP, KK, 1
#endif
	dsra	L,  TEMP, 2
	blez	L, .L122
	NOP

#else
	move	BO, B				#	Reset	B
	dsra	L, K, 2				#	UnRoll	K=64

	MTC		$0, C11				#	CLEAR REAULTS REGISTERS
	MOV		C21, C11
	gsLQC1(R13, F9, F8, 0)		#	B1 B2

	gsLQC1(R12, F1, F0, 0)		#	A1 A2
	MOV		C13, C11
	MOV		C23, C11

	FETCH	$0, 0 * SIZE(CO1)
	FETCH	$0, 8 * SIZE(CO1)

	PLU		B3,	B1, B1
	blez	L, .L122
	PLU		B4, B2, B2
#endif

.L1210:
	daddiu	L, L, -1
	gsLQC1(R13, F13, F12, 1)	#	B3 B4
	MADPS	C11, C11, A1, B1
	MADPS	C21, C21, A2, B1

	gsLQC1(R12, F3, F2, 1)		#	A3 A4
	MADPS	C13, C13, A1, B3
	MADPS	C23, C23, A2, B3

	gsLQC1(R12, F5, F4, 2)		#	A5 A6
	PLU		B7,	B5, B5
	PLU		B8, B6, B6
	daddiu	BO, BO, 2 * 4 * SIZE	#	4KR*4NR

	MADPS	C11, C11, A3, B2
	MADPS	C21, C21, A4, B2

	gsLQC1(R12, F7, F6, 3)		#	A7 A8
	MADPS	C13, C13, A3, B4
	MADPS	C23, C23, A4, B4

	MADPS	C11, C11, A5, B5
	MADPS	C21, C21, A6, B5
	daddiu	AO, AO, 4 * 4 * SIZE 	#	4KR*8MR

	gsLQC1(R13, F9, F8, 0)		#	B1 B2
	MADPS	C13, C13, A5, B7
	MADPS	C23, C23, A6, B7

	gsLQC1(R12, F1, F0, 0)		#	A1 A2
	MADPS	C11, C11, A7, B6
	MADPS	C21, C21, A8, B6

	MADPS	C13, C13, A7, B8
	MADPS	C23, C23, A8, B8

	PLU		B3,	B1, B1
	bgtz	L, .L1210
	PLU		B4, B2, B2


	.align	4
.L122:
#ifndef	TRMMKERNEL
	andi	L, K, 2
#else
	andi	L, TEMP, 2
#endif
	blez	L, .L127
	NOP

	MADPS	C11, C11, A1, B1
	MADPS	C21, C21, A2, B1

	gsLQC1(R12, F3, F2, 1)		#	A3 A4
	MADPS	C13, C13, A1, B3
	MADPS	C23, C23, A2, B3

	PLU		B7,	B5, B5
	daddiu	BO, BO, 1 * 4 * SIZE

	daddiu	AO, AO, 2 * 4 * SIZE
	MADPS	C11, C11, A3, B2
	MADPS	C21, C21, A4, B2

	MADPS	C13, C13, A3, B4
	MADPS	C23, C23, A4, B4

	gsLQC1(R13, F9, F8, 0)
	gsLQC1(R12, F1, F0, 0)
	PLU		B3, B1, B1

	.align	4
.L127:
#ifndef	TRMMKERNEL
	andi	L, K, 1
#else
	andi	L, TEMP, 1
#endif
	blez	L, .L120
	NOP

	MADPS	C11, C11, A1, B1
	MADPS	C21, C21, A2, B1
	daddiu	BO, BO, 2 * SIZE
	daddiu	AO, AO, 4 * SIZE

	MADPS	C13, C13, A1, B3
	MADPS	C23, C23, A2, B3

	.align	4
.L120:							#	Write Back
#ifndef	TRMMKERNEL
	daddiu	I, I, -1
	CVTU	A1, C11
	CVTU	A2, C21

	CVTU	A3, C13
	CVTU	A4, C23


#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	/*	(a + bi) * (c + di) */
	SUB		C11, C11, A1		#	ac'+'bd
	SUB		C21, C21, A2
	ADD		C13, A3, C13		#	ad'+'cb
	ADD		C23, A4, C23
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A1, 152($sp)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i
#	LD		A2, 0 * SIZE(A)		#	load alpha_i

	LD		B1, 0 * SIZE(CO1)
	LD		B3, 2 * SIZE(CO1)
	LD		B2, 1 * SIZE(CO1)
	LD		B4, 3 * SIZE(CO1)

	MADD	B1, B1, C11, A1		#	A1 = alpha_r
	MADD	B3, B3, C21, A1
	MADD	B2, B2, C13, A1
	MADD	B4, B4, C23, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2

	ST		B1, 0 * SIZE(CO1)
	ST		B3, 2 * SIZE(CO1)

	ST		B2, 1 * SIZE(CO1)
	ST		B4, 3 * SIZE(CO1)
#endif

#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
	/*	(a + bi) * (c - di) */
	ADD		C11, A1, C11		#	ac'+'bd
	ADD		C21, A2, C21
	SUB		C13, A3, C13		#	ad'+'cb
	SUB		C23, A4, C23
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A1, 152($sp)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i
#	LD		A2, 0 * SIZE(A)		#	load alpha_r

	LD		B1, 0 * SIZE(CO1)
	LD		B3, 2 * SIZE(CO1)
	LD		B2, 1 * SIZE(CO1)
	LD		B4, 3 * SIZE(CO1)

	MADD	B1, B1, C11, A1		#	A1 = alpha_r
	MADD	B3, B3, C21, A1
	MADD	B2, B2, C13, A1
	MADD	B4, B4, C23, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2

	ST		B1, 0 * SIZE(CO1)
	ST		B3, 2 * SIZE(CO1)
	ST		B2, 1 * SIZE(CO1)
	ST		B4, 3 * SIZE(CO1)
#endif

#if	  defined(RN) || defined(RT) || defined(CN) || defined(CT)
	/*	(a - bi) * (c + di) */
	ADD		C11, A1, C11		#	ac'+'bd
	ADD		C21, A2, C21
	SUB		C13, C13, A3		#	ad'+'cb
	SUB		C23, C23, A4
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A1, 152($sp)		#	load alpha_r
#	LD		A2, 0 * SIZE(A)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i

	LD		B1, 0 * SIZE(CO1)
	LD		B3, 2 * SIZE(CO1)
	LD		B2, 1 * SIZE(CO1)
	LD		B4, 3 * SIZE(CO1)

	MADD	B1, B1, C11, A1		#	A1 = alpha_r
	MADD	B3, B3, C21, A1
	MADD	B2, B2, C13, A1
	MADD	B4, B4, C23, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2

	ST		B1, 0 * SIZE(CO1)
	ST		B3, 2 * SIZE(CO1)
	ST		B2, 1 * SIZE(CO1)
	ST		B4, 3 * SIZE(CO1)
#endif

#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
	/*	(a - bi) * (c - di) */
	SUB		C11, C11, A1		#	ac'+'bd
	SUB		C21, C21, A2
	ADD		C13, A3, C13		#	ad'+'cb
	ADD		C23, A4, C23
	LD		A1, 152($sp)		#	load alpha_r
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A2, 160($sp)
#	LD		A2, 0 * SIZE(A)		#	load alpha_i
	NEG		C13, C13		#	ad'+'cb
	NEG		C23, C23

	LD		B1, 0 * SIZE(CO1)
	LD		B3, 2 * SIZE(CO1)
	LD		B2, 1 * SIZE(CO1)
	LD		B4, 3 * SIZE(CO1)

	MADD	B1, B1, C11, A1		#	A1 = alpha_r
	MADD	B3, B3, C21, A1
	MADD	B2, B2, C13, A1
	MADD	B4, B4, C23, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2

	ST		B1, 0 * SIZE(CO1)
	ST		B3, 2 * SIZE(CO1)
	ST		B2, 1 * SIZE(CO1)
	ST		B4, 3 * SIZE(CO1)
#endif

#else
	daddiu	I, I, -1
	CVTU	A1, C11
	CVTU	A2, C21

	CVTU	A3, C13
	CVTU	A4, C23


#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	/*	(a + bi) * (c + di) */
	SUB		C11, C11, A1		#	ac'+'bd
	SUB		C21, C21, A2
	ADD		C13, A3, C13		#	ad'+'cb
	ADD		C23, A4, C23
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A1, 152($sp)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i
#	LD		A2, 0 * SIZE(A)		#	load alpha_i

	MUL B1, C11, A1		#	A1 = alpha_r
	MUL B3, C21, A1
	MUL B2, C13, A1
	MUL B4, C23, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2

	ST		B1, 0 * SIZE(CO1)
	ST		B3, 2 * SIZE(CO1)

	ST		B2, 1 * SIZE(CO1)
	ST		B4, 3 * SIZE(CO1)
#endif

#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
	/*	(a + bi) * (c - di) */
	ADD		C11, A1, C11		#	ac'+'bd
	ADD		C21, A2, C21
	SUB		C13, A3, C13		#	ad'+'cb
	SUB		C23, A4, C23
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A1, 152($sp)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i
#	LD		A2, 0 * SIZE(A)		#	load alpha_r

	MUL B1, C11, A1		#	A1 = alpha_r
	MUL B3, C21, A1
	MUL B2, C13, A1
	MUL B4, C23, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2

	ST		B1, 0 * SIZE(CO1)
	ST		B3, 2 * SIZE(CO1)
	ST		B2, 1 * SIZE(CO1)
	ST		B4, 3 * SIZE(CO1)
#endif

#if	  defined(RN) || defined(RT) || defined(CN) || defined(CT)
	/*	(a - bi) * (c + di) */
	ADD		C11, A1, C11		#	ac'+'bd
	ADD		C21, A2, C21
	SUB		C13, C13, A3		#	ad'+'cb
	SUB		C23, C23, A4
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A1, 152($sp)		#	load alpha_r
#	LD		A2, 0 * SIZE(A)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i

	MUL B1, C11, A1		#	A1 = alpha_r
	MUL B3, C21, A1
	MUL B2, C13, A1
	MUL B4, C23, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2

	ST		B1, 0 * SIZE(CO1)
	ST		B3, 2 * SIZE(CO1)
	ST		B2, 1 * SIZE(CO1)
	ST		B4, 3 * SIZE(CO1)
#endif

#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
	/*	(a - bi) * (c - di) */
	SUB		C11, C11, A1		#	ac'+'bd
	SUB		C21, C21, A2
	ADD		C13, A3, C13		#	ad'+'cb
	ADD		C23, A4, C23
	LD		A1, 152($sp)		#	load alpha_r
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A2, 160($sp)
#	LD		A2, 0 * SIZE(A)		#	load alpha_i
	NEG		C13, C13		#	ad'+'cb
	NEG		C23, C23

	MUL B1, C11, A1		#	A1 = alpha_r
	MUL B3, C21, A1
	MUL B2, C13, A1
	MUL B4, C23, A1
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	NMSUB	B3, B3, C23, A2
	MADD	B2, B2, C11, A2
	MADD	B4, B4, C21, A2

	ST		B1, 0 * SIZE(CO1)
	ST		B3, 2 * SIZE(CO1)
	ST		B2, 1 * SIZE(CO1)
	ST		B4, 3 * SIZE(CO1)
#endif
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP, K, KK
#ifdef LEFT
	daddiu	TEMP, TEMP, -2
#else
	daddiu	TEMP, TEMP, -1
#endif
	dsll	L, TEMP, 1 + ZBASE_SHIFT
	dsll	TEMP, TEMP, ZBASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, BO, TEMP
#endif

#ifdef LEFT
	daddiu	KK, KK, 2
#endif

#endif
	daddiu	CO1, CO1, 4 * SIZE
	daddiu	CO2, CO2, 4 * SIZE


	.align	4
.L11:
	andi	I, M, 1
	blez	I, .L10
	NOP

	.align	4
.L111:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	BO,  B
#else
	dsll	TEMP, KK,  ZBASE_SHIFT

	daddu	AO, AO, TEMP
	daddu	BO, B,  TEMP
#endif
	MTC		$0, C11				#	CLEAR REAULTS REGISTERS
	gsLQC1(R13, F9, F8, 0)		#	B1 B2

	gsLQC1(R12, F1, F0, 0)		#	A1 A2
	MOV		C13, C11

	FETCH	$0, 0 * SIZE(CO1)

	PLU		B3,	B1, B1
	PLU		B4, B2, B2
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, K, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 1
#else
	daddiu	TEMP, KK, 1
#endif
	dsra	L,  TEMP, 2
	blez	L, .L112
	NOP

#else
	move	BO, B				#	Reset	B
	dsra	L, K, 2				#	UnRoll	K=64

	MTC		$0, C11				#	CLEAR REAULTS REGISTERS
	gsLQC1(R13, F9, F8, 0)		#	B1 B2

	gsLQC1(R12, F1, F0, 0)		#	A1 A2
	MOV		C13, C11

	FETCH	$0, 0 * SIZE(CO1)

	PLU		B3,	B1, B1
	blez	L, .L112
	PLU		B4, B2, B2
#endif

.L1110:
	daddiu	L, L, -1
	gsLQC1(R13, F13, F12, 1)	#	B3 B4
	MADPS	C11, C11, A1, B1

	gsLQC1(R12, F3, F2, 1)		#	A3 A4
	MADPS	C13, C13, A1, B3
	daddiu	BO, BO, 2 * 4 * SIZE	#	4KR*4NR

	PLU		B7,	B5, B5
	PLU		B8, B6, B6
	daddiu	AO, AO, 2 * 4 * SIZE 	#	4KR*8MR

	MADPS	C11, C11, A2, B2
	MADPS	C13, C13, A2, B4

	MADPS	C11, C11, A3, B5
	MADPS	C13, C13, A3, B7

	gsLQC1(R13, F9, F8, 0)		#	B1 B2
	MADPS	C11, C11, A4, B6

	gsLQC1(R12, F1, F0, 0)		#	A1 A2
	MADPS	C13, C13, A4, B8

	PLU		B3,	B1, B1
	bgtz	L, .L1110
	PLU		B4, B2, B2


	.align	4
.L112:
#ifndef	TRMMKERNEL
	andi	L, K, 2
#else
	andi	L, TEMP, 2
#endif
	blez	L, .L117
	NOP

	MADPS	C11, C11, A1, B1
	MADPS	C13, C13, A1, B3
	daddiu	BO, BO, 4 * SIZE
	daddiu	AO, AO, 4 * SIZE

	MADPS	C11, C11, A2, B2
	MADPS	C13, C13, A2, B4

	gsLQC1(R13, F9, F8, 0)
	gsLQC1(R12, F1, F0, 0)
	PLU		B3,	B1, B1


	.align	4
.L117:
#ifndef	TRMMKERNEL
	andi	L, K, 1
#else
	andi	L, TEMP, 1
#endif
	blez	L, .L110
	NOP

	daddiu	BO, BO, 2 * SIZE
	daddiu	AO, AO, 2 * SIZE

	MADPS	C11, C11, A1, B1
	MADPS	C13, C13, A1, B3


	.align	4
.L110:							#	Write Back
#ifndef	TRMMKERNEL
	daddiu	I, I, -1
	CVTU	A1, C11
	CVTU	A3, C13

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	/*	(a + bi) * (c + di) */
	SUB		C11, C11, A1		#	ac'+'bd
	ADD		C13, A3, C13		#	ad'+'cb
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A4, 152($sp)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i
#	LD		A2, 0 * SIZE(A)		#	load alpha_i

	LD		B1, 0 * SIZE(CO1)
	LD		B2, 1 * SIZE(CO1)

	MADD	B1, B1, C11, A4		#	A1 = alpha_r
	MADD	B2, B2, C13, A4
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	MADD	B2, B2, C11, A2

	ST		B1, 0 * SIZE(CO1)
	ST		B2, 1 * SIZE(CO1)
#endif

#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
	/*	(a + bi) * (c - di) */
	ADD		C11, A1, C11		#	ac'+'bd
	SUB		C13, A3, C13		#	ad'+'cb
	LD		A4, 152($sp)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i

	LD		B1, 0 * SIZE(CO1)
	LD		B2, 1 * SIZE(CO1)

	MADD	B1, B1, C11, A4		#	A1 = alpha_r
	MADD	B2, B2, C13, A4
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	MADD	B2, B2, C11, A2

	ST		B1, 0 * SIZE(CO1)
	ST		B2, 1 * SIZE(CO1)
#endif

#if	  defined(RN) || defined(RT) || defined(CN) || defined(CT)
	/*	(a - bi) * (c + di) */
	ADD		C11, A1, C11		#	ac'+'bd
	SUB		C13, C13, A3		#	ad'+'cb
	LD		A4, 152($sp)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i

	LD		B1, 0 * SIZE(CO1)
	LD		B2, 1 * SIZE(CO1)

	MADD	B1, B1, C11, A4		#	A1 = alpha_r
	MADD	B2, B2, C13, A4
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	MADD	B2, B2, C11, A2

	ST		B1, 0 * SIZE(CO1)
	ST		B2, 1 * SIZE(CO1)
#endif

#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
	/*	(a - bi) * (c - di) */
	SUB		C11, C11, A1		#	ac'+'bd
	ADD		C13, A3, C13		#	ad'+'cb
	NEG		C13, C13
	LD		A4, 152($sp)		#	load alpha_r
	LD		A2, 160($sp)

	LD		B1, 0 * SIZE(CO1)
	LD		B2, 1 * SIZE(CO1)

	MADD	B1, B1, C11, A4		#	A1 = alpha_r
	MADD	B2, B2, C13, A4
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	MADD	B2, B2, C11, A2

	ST		B1, 0 * SIZE(CO1)
	ST		B2, 1 * SIZE(CO1)
#endif

#else
	daddiu	I, I, -1
	CVTU	A1, C11
	CVTU	A3, C13

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	/*	(a + bi) * (c + di) */
	SUB		C11, C11, A1		#	ac'+'bd
	ADD		C13, A3, C13		#	ad'+'cb
#	LD		A1, 0 * SIZE(A)		#	load alpha_r
	LD		A4, 152($sp)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i
#	LD		A2, 0 * SIZE(A)		#	load alpha_i

	MUL B1, C11, A4		#	A1 = alpha_r
	MUL B2, C13, A4
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	MADD	B2, B2, C11, A2

	ST		B1, 0 * SIZE(CO1)
	ST		B2, 1 * SIZE(CO1)
#endif

#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
	/*	(a + bi) * (c - di) */
	ADD		C11, A1, C11		#	ac'+'bd
	SUB		C13, A3, C13		#	ad'+'cb
	LD		A4, 152($sp)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i

	MUL B1, C11, A4		#	A1 = alpha_r
	MUL B2, C13, A4
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	MADD	B2, B2, C11, A2

	ST		B1, 0 * SIZE(CO1)
	ST		B2, 1 * SIZE(CO1)
#endif

#if	  defined(RN) || defined(RT) || defined(CN) || defined(CT)
	/*	(a - bi) * (c + di) */
	ADD		C11, A1, C11		#	ac'+'bd
	SUB		C13, C13, A3		#	ad'+'cb
	LD		A4, 152($sp)		#	load alpha_r
	LD		A2, 160($sp)		#	load alpha_i

	MUL B1, C11, A4		#	A1 = alpha_r
	MUL B2, C13, A4
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	MADD	B2, B2, C11, A2

	ST		B1, 0 * SIZE(CO1)
	ST		B2, 1 * SIZE(CO1)
#endif

#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
	/*	(a - bi) * (c - di) */
	SUB		C11, C11, A1		#	ac'+'bd
	ADD		C13, A3, C13		#	ad'+'cb
	NEG		C13, C13
	LD		A4, 152($sp)		#	load alpha_r
	LD		A2, 160($sp)

	MUL B1, C11, A4		#	A1 = alpha_r
	MUL B2, C13, A4
	NMSUB	B1, B1, C13, A2		#	A2 = alpha_i
	MADD	B2, B2, C11, A2

	ST		B1, 0 * SIZE(CO1)
	ST		B2, 1 * SIZE(CO1)
#endif


#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP, K, KK
#ifdef LEFT
	daddiu	TEMP, TEMP, -1
#else
	daddiu	TEMP, TEMP, -1
#endif

	dsll	TEMP, TEMP, ZBASE_SHIFT

	daddu	AO, AO, TEMP
	daddu	BO, BO, TEMP
#endif

#ifdef LEFT
	daddiu	KK, KK, 1
#endif

#endif
	daddiu	CO1, CO1, 2 * SIZE
	daddiu	CO2, CO2, 2 * SIZE


	.align	4
.L10:
	move	B, BO
#if defined(TRMMKERNEL) && !defined(LEFT)
	daddiu	KK, KK, 1
#endif

.L999:
	ld	$16,   0($sp)
	ld	$17,   8($sp)
	ld	$18,  16($sp)
	ld	$19,  24($sp)
	ld	$20,  32($sp)
	ld	$21,  40($sp)
	ld	$22,  48($sp)

	LD	$f24, 56($sp)
	LD	$f25, 64($sp)
	LD	$f26, 72($sp)
	LD	$f27, 80($sp)
	LD	$f28, 88($sp)

#if defined(TRMMKERNEL)
	ld	$23,  96($sp)
	ld	$24, 104($sp)
	ld	$25, 112($sp)
#endif

#ifndef __64BIT__
	LD	$f20,120($sp)
	LD	$f21,128($sp)
	LD	$f22,136($sp)
	LD	$f23,144($sp)
#endif

	daddiu	$sp,$sp,STACKSIZE
	j	$31
	nop

	EPILOGUE
