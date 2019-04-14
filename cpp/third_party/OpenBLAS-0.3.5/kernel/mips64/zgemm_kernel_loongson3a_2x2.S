#define ASSEMBLER
#include "common.h"

#define FETCH	ld
#define gsLQC1(base,fq,ft,offset) .word(0x32<<26|base<<21|ft<<16|0x1<<15|offset<<6|0x1<<5|fq)
#define gsSQC1(base,fq,ft,offset) .word(0x3A<<26|base<<21|ft<<16|0x1<<15|offset<<6|0x1<<5|fq)

#define STACKSIZE	160
#define M	$4
#define	N	$5
#define	K	$6
#define A	$9
#define B	$10
#define C	$11
#define LDC	$8

#define AO	$12
#define BO	$13

#define	R12	12
#define	R13	13

#define I	$2
#define J	$3
#define L	$7

#define CO1	$14
#define CO2	$15
#define PREA	$16
#define PREB	$17

#if defined(TRMMKERNEL)
#define OFFSET	$18
#define KK	$19
#define TEMP	$20
#endif

#define a1	$f0
#define a2	$f1
#define a3	$f2
#define a4	$f3

#define b1	$f4
#define b2	$f5
#define b3	$f6
#define b4	$f7

#define a5	$f8
#define a6	$f9
#define a7	$f10
#define a8	$f11

#define b5	$f12
#define b6	$f13
#define b7	$f15
#define b8	$f16

#define c11	$f14
#define c12	$f17
#define c13	$f18
#define c14	$f19
#define c21	$f20
#define c22	$f21
#define c23	$f22
#define c24	$f23
#define c31	$f24
#define c32	$f25
#define c33	$f26
#define c34	$f27
#define c41	$f28
#define c42	$f29
#define c43	$f30
#define c44	$f31

#define F0	0
#define F1	1
#define F2	2
#define F3	3
#define F4	4
#define F5	5
#define	F6	6
#define	F7	7
#define	F8	8
#define	F9	9
#define	F10	10
#define	F11	11
#define	F12	12
#define	F13	13
#define	F14	14
#define	F15	15
#define	F16	16
#define	F17	17
#define	F18	18
#define	F19	19
#define	F20	20
#define	F21	21
#define	F22	22
#define	F23	23
#define	F24	24
#define	F25	25
#define	F26	26
#define	F27	27
#define	F28	28
#define	F29	29
#define	F30	30
#define	F31	31

#define ALPHA_R	$f15
#define ALPHA_I	$f16

#################################
##	MADD1	a*c
##	MADD2	b*c
##	MADD3	a*d
##	MADD4	d*b
##################################
#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define MADD1	  MADD
#define MADD2	  MADD
#define MADD3	  MADD
#define MADD4	  NMSUB
#endif

#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define MADD1	  MADD
#define MADD2	  MADD
#define MADD3	  NMSUB
#define MADD4	  MADD
#endif

#if   defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define MADD1	  MADD
#define MADD2	  NMSUB
#define MADD3	  MADD
#define MADD4	  MADD
#endif

#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
#define MADD1	  MADD
#define MADD2	  NMSUB
#define MADD3	  NMSUB
#define MADD4	  NMSUB
#endif

	PROLOGUE

	LDARG	LDC,   0($sp)
	daddiu	$sp, $sp, -STACKSIZE

	SDARG	$16,   0($sp)
	SDARG	$17,   8($sp)
	sdc1	$f24, 16($sp)
	sdc1	$f25, 24($sp)
	sdc1	$f26, 32($sp)
	sdc1	$f27, 40($sp)
	sdc1	$f28, 48($sp)
	sdc1	$f29, 56($sp)

#if defined(TRMMKERNEL)
	SDARG	$18,  64($sp)
	SDARG	$19,  72($sp)
	SDARG	$20,  80($sp)

	LDARG	OFFSET, STACKSIZE + 8($sp)
#endif

#ifndef __64BIT__
	sdc1	$f20, 88($sp)
	sdc1	$f21, 96($sp)
	sdc1	$f22,104($sp)
	sdc1	$f23,112($sp)
#endif

	dsra	J,  N, 1							#	J=N/2
	ST	ALPHA_R, 128($sp)						#	store alpha_r & alpha_i
#if defined(TRMMKERNEL) && !defined(LEFT)
	neg	KK, OFFSET
#endif

	dsll	LDC, LDC, ZBASE_SHIFT				#	LDC*SIZE*COMPSIZE
	blez	J, .L20
	ST	ALPHA_I, 136($sp)


	.align	5
.L10:
#if defined(TRMMKERNEL) &&  defined(LEFT)
	move	KK, OFFSET
#endif

	daddiu	J, J, -1
	dsra	I,  M, 1							#	I=M/2

	dsll	PREB, K, 1 + ZBASE_SHIFT				#	PREA=K*2*2^4
	dsll	PREA, K, 1 + ZBASE_SHIFT				#	PREA=K*2*2^4

	move	CO1, C								#	Fix pointer Cx
	daddu	CO2, C,   LDC

	move	AO, A								#	Reset AO
	blez	I, .L30
	daddu	PREA, PREA, A						#	PREA=A+panel size

.L11:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	BO,  B
#else
	dsll	L,    KK, 1 + ZBASE_SHIFT			#	MR=NR=2
	dsll	TEMP, KK, 1 + ZBASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, B,  TEMP
#endif
	MTC	$0,  c11								#	Clear results regs
	MOV	c12, c11
	gsLQC1(R12, F1, F0, 0)						#	R:a1	I:a2

	MOV	c13, c11
	MOV	c14, c11
	gsLQC1(R13, F5, F4, 0)						#	R:b1	I:b2

	MOV	c21, c11
	MOV	c22, c11
	gsLQC1(R12, F3, F2, 1)						#	R:a3	I:a4

	MOV	c23, c11
	MOV	c24, c11
	gsLQC1(R13, F7, F6, 1)						#	R:b2	I:b3

	FETCH	$0, 0 * SIZE(CO2)
	MOV	c31, c11
	MOV	c32, c11

	FETCH	$0, 0 * SIZE(CO1)
	MOV	c33, c11
	MOV	c34, c11

	FETCH	$0, 4 * SIZE(CO2)
	MOV	c41, c11
	MOV	c42, c11

	FETCH	$0, 4 * SIZE(CO1)
	MOV	c43, c11
	MOV	c44, c11

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, K, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 2
#else
	daddiu	TEMP, KK, 2
#endif
	dsra	L,  TEMP, 2
	daddu	PREB, PREB, B						#	PREA=A+panel size
	blez	L, .L15
	NOP

#else

	dsra	L,  K, 2							#	Unroll K 4 times
	move	BO,  B

	MTC	$0,  c11								#	Clear results regs
	MOV	c12, c11
	gsLQC1(R12, F1, F0, 0)						#	R:a1	I:a2

	MOV	c13, c11
	MOV	c14, c11
	gsLQC1(R13, F5, F4, 0)						#	R:b1	I:b2

	MOV	c21, c11
	MOV	c22, c11
	gsLQC1(R12, F3, F2, 1)						#	R:a3	I:a4

	MOV	c23, c11
	MOV	c24, c11
	gsLQC1(R13, F7, F6, 1)						#	R:b2	I:b3

	FETCH	$0, 0 * SIZE(CO2)
	MOV	c31, c11
	MOV	c32, c11

	FETCH	$0, 0 * SIZE(CO1)
	MOV	c33, c11
	MOV	c34, c11

	FETCH	$0, 4 * SIZE(CO2)
	MOV	c41, c11
	MOV	c42, c11

	FETCH	$0, 4 * SIZE(CO1)
	MOV	c43, c11

	daddu	PREB, PREB, B						#	PREA=A+panel size
	blez	L, .L15
	MOV	c44, c11
#endif

	.align	5

.L12:
	gsLQC1(R12, F9, F8, 2)						#	Unroll K=1
	gsLQC1(R13, F13, F12, 2)
	MADD1	c11, c11, a1, b1 					#	axc		A1xB1
	MADD3	c13, c13, a1, b2					#	axd

	gsLQC1(R12, F11, F10, 3)
	gsLQC1(R13, F16, F15, 3)
	MADD2	c12, c12, a2, b1					#	bxc
	MADD4	c14, c14, a2, b2					#	bxd

	MADD1	c21, c21, a3, b1 					#	A2xB1
	MADD3	c23, c23, a3, b2

	MADD2	c22, c22, a4, b1
	MADD4	c24, c24, a4, b2

	FETCH	$0, 4 * SIZE(PREA)
	FETCH	$0, 4 * SIZE(PREB)
	MADD1	c31, c31, a1, b3 					#	A1xB2
	MADD3	c33, c33, a1, b4

	MADD2	c32, c32, a2, b3
	MADD4	c34, c34, a2, b4

	MADD1	c41, c41, a3, b3 					#	A2xB2
	MADD3	c43, c43, a3, b4
	MADD2	c42, c42, a4, b3
	MADD4	c44, c44, a4, b4

	gsLQC1(R12, F1, F0, 4)						#	unroll	k=2
	gsLQC1(R13, F5, F4, 4)
	MADD1	c11, c11, a5, b5 					#	axc		A1xB1
	MADD3	c13, c13, a5, b6					#	axd

	MADD2	c12, c12, a6, b5					#	bxc
	MADD4	c14, c14, a6, b6					#	bxd

	gsLQC1(R12, F3, F2, 5)
	gsLQC1(R13, F7, F6, 5)
	MADD1	c21, c21, a7, b5 					#	A2xB1
	MADD3	c23, c23, a7, b6

	MADD2	c22, c22, a8, b5
	MADD4	c24, c24, a8, b6

	FETCH	$0, 8 * SIZE(PREA)
	FETCH	$0, 8 * SIZE(PREB)
	MADD1	c31, c31, a5, b7 					#	A1xB2
	MADD3	c33, c33, a5, b8

	MADD2	c32, c32, a6, b7
	MADD4	c34, c34, a6, b8

	MADD1	c41, c41, a7, b7 					#	A2xB2
	MADD3	c43, c43, a7, b8
	MADD2	c42, c42, a8, b7
	MADD4	c44, c44, a8, b8

	gsLQC1(R12, F9, F8, 6)						#	Unroll	K=3
	gsLQC1(R13, F13, F12, 6)
	MADD1	c11, c11, a1, b1 					#	axc		A1xB1
	MADD3	c13, c13, a1, b2					#	axd

	gsLQC1(R13, F16, F15, 7)
	gsLQC1(R12, F11, F10, 7)
	MADD2	c12, c12, a2, b1					#	bxc
	MADD4	c14, c14, a2, b2					#	bxd

	MADD1	c21, c21, a3, b1 					#	A2xB1
	MADD3	c23, c23, a3, b2
	daddiu	AO, AO, 16 * SIZE 					#	2mr*4kr*cmpx
	daddiu	BO, BO, 16 * SIZE  					#	2nr*4kr*cmpx

	MADD2	c22, c22, a4, b1
	MADD4	c24, c24, a4, b2

	FETCH	$0, 12 * SIZE(PREA)
	MADD1	c31, c31, a1, b3 					#	A1xB2
	MADD3	c33, c33, a1, b4
	daddiu	L, L, -1

	FETCH	$0, 12 * SIZE(PREB)
	MADD2	c32, c32, a2, b3
	MADD4	c34, c34, a2, b4

	MADD1	c41, c41, a3, b3 					#	A2xB2
	MADD3	c43, c43, a3, b4
	daddu	PREA, PREA, 16 * SIZE
	daddu	PREB, PREB, 16 * SIZE

	MADD2	c42, c42, a4, b3
	MADD4	c44, c44, a4, b4

	gsLQC1(R12, F1, F0, 0)						#	R:a1	I:a2	Unroll K=4
	gsLQC1(R13, F5, F4, 0)						#	R:b1	I:b2
	MADD1	c11, c11, a5, b5 					#	axc		A1xB1
	MADD3	c13, c13, a5, b6					#	axd

	gsLQC1(R12, F3, F2, 1)						#	R:a3	I:a4
	gsLQC1(R13, F7, F6, 1)						#	R:b2	I:b3
	MADD2	c12, c12, a6, b5					#	bxc
	MADD4	c14, c14, a6, b6					#	bxd

	MADD1	c21, c21, a7, b5 					#	A2xB1
	MADD3	c23, c23, a7, b6

	MADD2	c22, c22, a8, b5
	MADD4	c24, c24, a8, b6

	FETCH	$0, 0 * SIZE(PREA)
	FETCH	$0, 0 * SIZE(PREB)
	MADD1	c31, c31, a5, b7 					#	A1xB2
	MADD3	c33, c33, a5, b8

	MADD2	c32, c32, a6, b7
	MADD4	c34, c34, a6, b8

	MADD1	c41, c41, a7, b7 					#	A2xB2
	MADD3	c43, c43, a7, b8

	MADD2	c42, c42, a8, b7
	bgtz	L, .L12
	MADD4	c44, c44, a8, b8

	.align	5

.L15:
#ifndef	TRMMKERNEL
	andi	L,  K, 3
	LD	ALPHA_R, 128($sp)
#else
	andi	L,  TEMP, 3
	LD	ALPHA_R, 128($sp)
#endif
	blez	L, .L18
	LD	ALPHA_I, 136($sp)

	.align	5

.L16:
	daddiu	BO, BO, 4 * SIZE					#	2nr*1kr*cmpx
	daddiu	AO, AO, 4 * SIZE					#	2mr*1kr*cmpx
	MADD1	c11, c11, a1, b1 					#	axc		A1xB1
	MADD3	c13, c13, a1, b2					#	axd

	daddiu	PREA, PREA, 4 * SIZE
	daddiu	PREB, PREB, 4 * SIZE
	MADD2	c12, c12, a2, b1					#	bxc
	MADD4	c14, c14, a2, b2					#	bxd

	MADD1	c21, c21, a3, b1 					#	A2xB1
	MADD3	c23, c23, a3, b2

	MADD2	c22, c22, a4, b1
	MADD4	c24, c24, a4, b2

	FETCH	$0, 0 * SIZE(PREA)
	MADD1	c31, c31, a1, b3 					#	A1xB2
	MADD3	c33, c33, a1, b4
	daddiu	L, L, -1

	MADD2	c32, c32, a2, b3
	MADD4	c34, c34, a2, b4

	FETCH	$0, 0 * SIZE(PREB)
	MADD1	c41, c41, a3, b3 					#	A2xB2
	MADD3	c43, c43, a3, b4

	MADD2	c42, c42, a4, b3
	MADD4	c44, c44, a4, b4

	gsLQC1(R12, F1, F0, 0)						#	R:a1	I:a2	Unroll K=4
	gsLQC1(R13, F5, F4, 0)						#	R:b1	I:b2
	gsLQC1(R12, F3, F2, 1)						#	R:a3	I:a4
	gsLQC1(R13, F7, F6, 1)						#	R:b2	I:b3
	bgtz	L, .L16
	NOP

.L18:
#ifndef TRMMKERNEL
 	ADD	c11, c14, c11
	LD	a1,  0 * SIZE(CO1)
	ADD	c12, c13, c12
	LD	a2,  1 * SIZE(CO1)
	ADD	c21, c24, c21
	LD	b1,  2 * SIZE(CO1)
	ADD	c22, c23, c22
	LD	b2,  3 * SIZE(CO1)

	ADD	c31, c34, c31
	LD	a3,  0 * SIZE(CO2)
	ADD	c32, c33, c32
	LD	a4,  1 * SIZE(CO2)
	ADD	c41, c44, c41
	LD	b3,  2 * SIZE(CO2)
	ADD	c42, c43, c42
	LD	b4,  3 * SIZE(CO2)

	daddiu	I, I, -1
	MADD	a1, a1, ALPHA_R, c11
	MADD	a2, a2, ALPHA_R, c12
	MADD	b1, b1, ALPHA_R, c21
	MADD	b2, b2, ALPHA_R, c22

	NMSUB	a1, a1, ALPHA_I, c12
	MADD	a2, a2, ALPHA_I, c11
	NMSUB	b1, b1, ALPHA_I, c22
	MADD	b2, b2, ALPHA_I, c21

	MADD	a3, a3, ALPHA_R, c31
	MADD	a4, a4, ALPHA_R, c32
	ST	a1, 0 * SIZE(CO1)
	MADD	b3, b3, ALPHA_R, c41
	MADD	b4, b4, ALPHA_R, c42
	ST	a2, 1 * SIZE(CO1)

	NMSUB	a3, a3, ALPHA_I, c32
	MADD	a4, a4, ALPHA_I, c31
	ST	b1, 2 * SIZE(CO1)

	NMSUB	b3, b3, ALPHA_I, c42
	MADD	b4, b4, ALPHA_I, c41
	ST	b2, 3 * SIZE(CO1)

	ST	a3, 0 * SIZE(CO2)
	ST	a4, 1 * SIZE(CO2)
	ST	b3, 2 * SIZE(CO2)
	ST	b4, 3 * SIZE(CO2)

#else
 	ADD	c11, c14, c11
	ADD	c12, c13, c12
	ADD	c21, c24, c21
	ADD	c22, c23, c22

	ADD	c31, c34, c31
	ADD	c32, c33, c32
	ADD	c41, c44, c41
	ADD	c42, c43, c42

	daddiu	I, I, -1
	MUL a1, ALPHA_R, c11
	MUL a2, ALPHA_R, c12
	MUL	b1, ALPHA_R, c21
	MUL	b2, ALPHA_R, c22

	NMSUB	a1, a1, ALPHA_I, c12
	MADD	a2, a2, ALPHA_I, c11
	NMSUB	b1, b1, ALPHA_I, c22
	MADD	b2, b2, ALPHA_I, c21

	MUL	a3, ALPHA_R, c31
	MUL	a4, ALPHA_R, c32
	MUL	b3, ALPHA_R, c41
	MUL	b4, ALPHA_R, c42

	NMSUB	a3, a3, ALPHA_I, c32
	MADD	a4, a4, ALPHA_I, c31
	NMSUB	b3, b3, ALPHA_I, c42
	MADD	b4, b4, ALPHA_I, c41

	ST	a1, 0 * SIZE(CO1)
	ST	a2, 1 * SIZE(CO1)
	ST	b1, 2 * SIZE(CO1)
	ST	b2, 3 * SIZE(CO1)

	ST	a3, 0 * SIZE(CO2)
	ST	a4, 1 * SIZE(CO2)
	ST	b3, 2 * SIZE(CO2)
	ST	b4, 3 * SIZE(CO2)

#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP, K, KK
#ifdef LEFT
	daddiu	TEMP, TEMP, -2
#else
	daddiu	TEMP, TEMP, -2
#endif

	dsll	L,    TEMP, 1 + ZBASE_SHIFT
	dsll	TEMP, TEMP, 1 + ZBASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, BO, TEMP
#endif

#ifdef LEFT
	daddiu	KK, KK, 2
#endif
#endif

	dsll	PREB, K, 1 + ZBASE_SHIFT				#	PREA=K*2*2^4
	daddiu	CO1,CO1, 4 * SIZE
	bgtz	I, .L11
	daddiu	CO2,CO2, 4 * SIZE

	.align	5
.L30:
	andi	I, M, 1
	daddu	C,   C, LDC						#	Change C to next panel

	daddu	PREB, PREB, B						#	PREA=A+panel size
	blez	I, .L19
	daddu	C,   C, LDC						#	Change C to next panel

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	BO,  B
#else
	dsll	L,    KK, ZBASE_SHIFT			#	MR=1
	dsll	TEMP, KK, 1 + ZBASE_SHIFT		#	NR=2

	daddu	AO, AO, L
	daddu	BO, B,  TEMP
#endif

	gsLQC1(R12, F1, F0, 0)						#	R:a1	I:a2
	MTC	$0,  c11								#	Clear results regs
	MOV	c12, c11

	gsLQC1(R13, F5, F4, 0)						#	R:b1	I:b2
	MOV	c13, c11
	MOV	c14, c11

	gsLQC1(R13, F7, F6, 1)						#	R:b2	I:b3
	MOV	c31, c11
	MOV	c32, c11

	FETCH	$0, 0 * SIZE(PREB)
	MOV	c33, c11
	MOV	c34, c11

	FETCH	$0, 0 * SIZE(CO1)
	FETCH	$0, 0 * SIZE(CO2)
	FETCH	$0, 4 * SIZE(CO1)
	FETCH	$0, 4 * SIZE(CO2)

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, K, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 1							#	MR=1
#else
	daddiu	TEMP, KK, 2							#	NR=2
#endif
	dsra	L,  TEMP, 2
	blez	L, .L35
	NOP

#else

	gsLQC1(R12, F1, F0, 0)						#	R:a1	I:a2
	dsra	L,  K, 2							#	Unroll K 4 times
	move	BO,  B

	gsLQC1(R13, F5, F4, 0)						#	R:b1	I:b2
	MTC	$0,  c11								#	Clear results regs
	MOV	c12, c11

	gsLQC1(R13, F7, F6, 1)						#	R:b2	I:b3
	MOV	c13, c11
	MOV	c14, c11

	FETCH	$0, 0 * SIZE(PREB)
	MOV	c31, c11
	MOV	c32, c11

	FETCH	$0, 0 * SIZE(CO1)
	FETCH	$0, 0 * SIZE(CO2)
	FETCH	$0, 4 * SIZE(CO1)
	FETCH	$0, 4 * SIZE(CO2)

	MOV	c33, c11
	blez	L, .L35
	MOV	c34, c11
#endif

	.align	5

.L32:
	gsLQC1(R12, F3, F2, 1)						#	R:a3	I:a4
	gsLQC1(R13, F13, F12, 2)
	MADD1	c11, c11, a1, b1 					#	axc		A1xB1
	MADD3	c13, c13, a1, b2					#	axd

	gsLQC1(R13, F16, F15, 3)
	MADD2	c12, c12, a2, b1					#	bxc
	MADD4	c14, c14, a2, b2					#	bxd
	NOP

	MADD1	c31, c31, a1, b3 					#	A1xB2
	MADD3	c33, c33, a1, b4

	FETCH	$0, 4 * SIZE(PREB)
	MADD2	c32, c32, a2, b3
	MADD4	c34, c34, a2, b4
	NOP

	gsLQC1(R12, F9, F8, 2)						#	Unroll K=1
	gsLQC1(R13, F5, F4, 4)
	MADD1	c11, c11, a3, b5 					#	axc		A1xB1
	MADD3	c13, c13, a3, b6					#	axd

	gsLQC1(R13, F7, F6, 5)
	MADD2	c12, c12, a4, b5					#	bxc
	MADD4	c14, c14, a4, b6					#	bxd
	NOP

	MADD1	c31, c31, a3, b7 					#	A1xB2
	MADD3	c33, c33, a3, b8

	FETCH	$0, 8 * SIZE(PREB)
	MADD2	c32, c32, a4, b7
	MADD4	c34, c34, a4, b8
	daddiu	L, L, -1

	gsLQC1(R12, F11, F10, 3)
	gsLQC1(R13, F13, F12, 6)
	MADD1	c11, c11, a5, b1 					#	axc		A1xB1
	MADD3	c13, c13, a5, b2					#	axd

	gsLQC1(R13, F16, F15, 7)
	MADD2	c12, c12, a6, b1					#	bxc
	MADD4	c14, c14, a6, b2					#	bxd
	daddiu	AO, AO, 8 * SIZE 					#	2mr*4kr*cmpx

	MADD1	c31, c31, a5, b3 					#	A1xB2
	MADD3	c33, c33, a5, b4

	FETCH	$0, 12 * SIZE(PREB)
	MADD2	c32, c32, a6, b3
	MADD4	c34, c34, a6, b4
	daddiu	BO, BO, 16 * SIZE  					#	2nr*4kr*cmpx

	gsLQC1(R12, F1, F0, 0)						#	R:a1	I:a2	Unroll K=4
	gsLQC1(R13, F5, F4, 0)						#	R:b1	I:b2
	MADD1	c11, c11, a7, b5 					#	axc		A1xB1
	MADD3	c13, c13, a7, b6					#	axd

	gsLQC1(R13, F7, F6, 1)						#	R:b2	I:b3
	MADD2	c12, c12, a8, b5					#	bxc
	MADD4	c14, c14, a8, b6					#	bxd
	daddiu	PREB, PREB, 16 * SIZE

	MADD1	c31, c31, a7, b7 					#	A1xB2
	MADD3	c33, c33, a7, b8

	FETCH	$0, 0 * SIZE(PREB)
	MADD2	c32, c32, a8, b7
	bgtz	L, .L32
	MADD4	c34, c34, a8, b8


.L35:
#ifndef TRMMKERNEL
	andi	L,  K, 3
	LD	ALPHA_R, 128($sp)
#else
	andi	L,  TEMP, 3
	LD	ALPHA_R, 128($sp)
#endif
	blez	L, .L38
	LD	ALPHA_I, 136($sp)
	.align	5

.L36:
	daddiu	L, L, -1
	MADD1	c11, c11, a1, b1 					#	axc		A1xB1
	MADD3	c13, c13, a1, b2					#	axd

	daddiu	BO, BO, 4 * SIZE 					#	2nr*1kr*cmpx
	MADD2	c12, c12, a2, b1					#	bxc
	MADD4	c14, c14, a2, b2					#	bxd

	daddiu	AO, AO, 2 * SIZE					#	2mr*1kr*cmpx
	MADD1	c31, c31, a1, b3 					#	A1xB2
	MADD3	c33, c33, a1, b4

	daddiu	PREB, PREB, 4 * SIZE
	MADD2	c32, c32, a2, b3
	MADD4	c34, c34, a2, b4
	gsLQC1(R12, F1, F0, 0)						#	R:a1	I:a2	Unroll K=4

	gsLQC1(R13, F5, F4, 0)						#	R:b1	I:b2
	NOP

	bgtz	L, .L36
	gsLQC1(R13, F7, F6, 1)						#	R:b2	I:b3

.L38:
#ifndef	TRMMKERNEL
 	ADD	c11, c14, c11
	LD	a1,  0 * SIZE(CO1)
	ADD	c12, c13, c12
	LD	a2,  1 * SIZE(CO1)

	ADD	c31, c34, c31
	LD	a3,  0 * SIZE(CO2)
	ADD	c32, c33, c32
	LD	a4,  1 * SIZE(CO2)

	MADD	a1, a1, ALPHA_R, c11
	MADD	a2, a2, ALPHA_R, c12

	MADD	a3, a3, ALPHA_R, c31
	MADD	a4, a4, ALPHA_R, c32

	NMSUB	a1, a1, ALPHA_I, c12
	MADD	a2, a2, ALPHA_I, c11

	NMSUB	a3, a3, ALPHA_I, c32
	MADD	a4, a4, ALPHA_I, c31

	ST	a1, 0 * SIZE(CO1)
	ST	a2, 1 * SIZE(CO1)

	ST	a3, 0 * SIZE(CO2)
	ST	a4, 1 * SIZE(CO2)

	daddiu	CO1,CO1, 2 * SIZE
	daddiu	CO2,CO2, 2 * SIZE
#else
 	ADD	c11, c14, c11
	ADD	c12, c13, c12

	ADD	c31, c34, c31
	ADD	c32, c33, c32

	MUL	a1, ALPHA_R, c11
	MUL	a2, ALPHA_R, c12
	MUL	a3, ALPHA_R, c31
	MUL	a4, ALPHA_R, c32

	NMSUB	a1, a1, ALPHA_I, c12
	MADD	a2, a2, ALPHA_I, c11

	NMSUB	a3, a3, ALPHA_I, c32
	MADD	a4, a4, ALPHA_I, c31

	ST	a1, 0 * SIZE(CO1)
	ST	a2, 1 * SIZE(CO1)

	ST	a3, 0 * SIZE(CO2)
	ST	a4, 1 * SIZE(CO2)

	daddiu	CO1,CO1, 2 * SIZE
	daddiu	CO2,CO2, 2 * SIZE

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

	.align 5

.L19:
#if defined(TRMMKERNEL) && !defined(LEFT)
	daddiu	KK, KK, 2
#endif

	bgtz	J, .L10
	move	B, BO

	.align 5

.L20:
	andi	J,  N, 1
	blez	J, .L999
	dsll	PREA, K, 1+ZBASE_SHIFT				#	PREA=K*2*2^4

	dsra	I,  M, 1							#	I=M/2
	move	CO1, C

#if defined(TRMMKERNEL) &&  defined(LEFT)
	move	KK, OFFSET
#endif

	move	AO, A								#	Reset AO
	blez	I, .L29
	daddu	PREA, PREA, A

.L21:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	BO,  B
#else
	dsll	L,    KK, 1 + ZBASE_SHIFT
	dsll	TEMP, KK, ZBASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, B,  TEMP
#endif
	gsLQC1(R12, F1, F0, 0)						#	R:a1	I:a2
	MTC	$0,  c11								#	Clear results regs
	MOV	c12, c11

	gsLQC1(R13, F5, F4, 0)						#	R:b1	I:b2
	MOV	c13, c11
	MOV	c14, c11

	gsLQC1(R12, F3, F2, 1)						#	R:a3	I:a4
	MOV	c21, c11
	MOV	c22, c11

	FETCH	$0, 0 * SIZE(PREA)
	MOV	c23, c11
	MOV	c24, c11

	FETCH	$0, 0 * SIZE(CO1)
	FETCH	$0, 4 * SIZE(CO1)

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, K, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 2						#	define Mr=2
#else
	daddiu	TEMP, KK, 1						#	define	NR=1
#endif
	dsra	L,  TEMP, 2
	blez	L, .L25
	NOP

#else
	dsra	L,  K, 2							#	Unroll K 4 times
	move	BO,  B

	gsLQC1(R12, F1, F0, 0)						#	R:a1	I:a2
	MTC	$0,  c11								#	Clear results regs
	MOV	c12, c11

	gsLQC1(R13, F5, F4, 0)						#	R:b1	I:b2
	MOV	c13, c11
	MOV	c14, c11

	gsLQC1(R12, F3, F2, 1)						#	R:a3	I:a4
	MOV	c21, c11
	MOV	c22, c11

	FETCH	$0, 0 * SIZE(PREA)
	MOV	c23, c11
	MOV	c24, c11

	FETCH	$0, 0 * SIZE(CO1)
	FETCH	$0, 4 * SIZE(CO1)

	blez	L, .L25
	NOP
#endif

	.align	5

.L22:
	gsLQC1(R12, F9, F8, 2)						#	Unroll K=1
	MADD1	c11, c11, a1, b1 					#	axc		A1xB1
	MADD3	c13, c13, a1, b2					#	axd

	gsLQC1(R13, F7, F6, 1)						#	R:b2	I:b3
	MADD2	c12, c12, a2, b1					#	bxc
	MADD4	c14, c14, a2, b2					#	bxd

	gsLQC1(R12, F11, F10, 3)
	MADD1	c21, c21, a3, b1 					#	A2xB1
	MADD3	c23, c23, a3, b2

	FETCH	$0, 4 * SIZE(PREA)
	MADD2	c22, c22, a4, b1
	MADD4	c24, c24, a4, b2

	gsLQC1(R12, F1, F0, 4)						#	Unroll	K=2
	MADD1	c11, c11, a5, b3 					#	axc		A1xB1
	MADD3	c13, c13, a5, b4					#	axd

	gsLQC1(R13, F13, F12, 2)
	MADD2	c12, c12, a6, b3					#	bxc
	MADD4	c14, c14, a6, b4					#	bxd

	gsLQC1(R12, F3, F2, 5)
	MADD1	c21, c21, a7, b3 					#	A2xB1
	MADD3	c23, c23, a7, b4

	FETCH	$0, 8 * SIZE(PREA)
	MADD2	c22, c22, a8, b3
	MADD4	c24, c24, a8, b4
	daddiu	L, L, -1

	gsLQC1(R12, F9, F8, 6)						#	Unroll	K=3
	MADD1	c11, c11, a1, b5 					#	axc		A1xB1
	MADD3	c13, c13, a1, b6					#	axd

	gsLQC1(R13, F16, F15, 3)
	MADD2	c12, c12, a2, b5					#	bxc
	MADD4	c14, c14, a2, b6					#	bxd

	gsLQC1(R12, F11, F10, 7)
	MADD1	c21, c21, a3, b5 					#	A2xB1
	MADD3	c23, c23, a3, b6
	daddiu	BO, BO,  8 * SIZE  					#	1nr*4kr*cmpx

	FETCH	$0, 12 * SIZE(PREA)
	MADD2	c22, c22, a4, b5
	MADD4	c24, c24, a4, b6
	daddiu	AO, AO, 16 * SIZE 					#	2mr*4kr*cmpx

	gsLQC1(R12, F1, F0, 0)						#	R:a1	I:a2	Unroll K=4
	MADD1	c11, c11, a5, b7 					#	axc		A1xB1
	MADD3	c13, c13, a5, b8					#	axd
	daddiu	PREA, PREA, 16 * SIZE

	gsLQC1(R13, F5, F4, 0)						#	R:b1	I:b2
	MADD2	c12, c12, a6, b7					#	bxc
	MADD4	c14, c14, a6, b8					#	bxd

	gsLQC1(R12, F3, F2, 1)						#	R:a3	I:a4
	MADD1	c21, c21, a7, b7 					#	A2xB1
	MADD3	c23, c23, a7, b8

	FETCH	$0, 0 * SIZE(PREA)
	MADD2	c22, c22, a8, b7
	bgtz	L, .L22
	MADD4	c24, c24, a8, b8


.L25:
#ifndef TRMMKERNEL
	andi	L,  K, 3
	LD	ALPHA_R, 128($sp)
#else
	andi	L, TEMP, 3
	LD	ALPHA_R, 128($sp)
#endif
	blez	L, .L28
	LD	ALPHA_I, 136($sp)
	.align	3

.L26:
	daddiu	L, L, -1
	MADD1	c11, c11, a1, b1 					#	axc		A1xB1
	MADD3	c13, c13, a1, b2					#	axd

	daddiu	BO, BO, 2 * SIZE 					#	2nr*1kr*cmpx
	MADD2	c12, c12, a2, b1					#	bxc
	MADD4	c14, c14, a2, b2					#	bxd

	daddiu	AO, AO, 4 * SIZE 					#	2mr*1kr*cmpx
	MADD1	c21, c21, a3, b1 					#	A2xB1
	MADD3	c23, c23, a3, b2

	daddiu	PREA, PREA, 4 * SIZE 				#	2mr*1kr*cmpx
	MADD2	c22, c22, a4, b1
	MADD4	c24, c24, a4, b2

	gsLQC1(R12, F1, F0, 0)						#	R:a1	I:a2	Unroll K=4
	gsLQC1(R12, F3, F2, 1)						#	R:a3	I:a4
	gsLQC1(R13, F5, F4, 0)						#	R:b1	I:b2

	bgtz	L, .L26
	FETCH	$0, 0 * SIZE(PREA)

.L28:
#ifndef	TRMMKERNEL
 	ADD	c11, c14, c11
	LD	a1,  0 * SIZE(CO1)
	ADD	c12, c13, c12
	LD	a2,  1 * SIZE(CO1)
	ADD	c21, c24, c21
	LD	b1,  2 * SIZE(CO1)
	ADD	c22, c23, c22
	LD	b2,  3 * SIZE(CO1)

	daddiu	I, I, -1
	MADD	a1, a1, ALPHA_R, c11
	MADD	a2, a2, ALPHA_R, c12
	MADD	b1, b1, ALPHA_R, c21
	MADD	b2, b2, ALPHA_R, c22

	NMSUB	a1, a1, ALPHA_I, c12
	MADD	a2, a2, ALPHA_I, c11
	NMSUB	b1, b1, ALPHA_I, c22
	MADD	b2, b2, ALPHA_I, c21

	ST	a1, 0 * SIZE(CO1)
	ST	a2, 1 * SIZE(CO1)
	ST	b1, 2 * SIZE(CO1)
	ST	b2, 3 * SIZE(CO1)

#else
 	ADD	c11, c14, c11
	ADD	c12, c13, c12
	ADD	c21, c24, c21
	ADD	c22, c23, c22

	daddiu	I, I, -1
	MUL	a1, ALPHA_R, c11
	MUL	a2, ALPHA_R, c12
	MUL	b1, ALPHA_R, c21
	MUL	b2, ALPHA_R, c22

	NMSUB	a1, a1, ALPHA_I, c12
	MADD	a2, a2, ALPHA_I, c11
	NMSUB	b1, b1, ALPHA_I, c22
	MADD	b2, b2, ALPHA_I, c21

	ST	a1, 0 * SIZE(CO1)
	ST	a2, 1 * SIZE(CO1)
	ST	b1, 2 * SIZE(CO1)
	ST	b2, 3 * SIZE(CO1)

#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP, K, KK
#ifdef LEFT
	daddiu	TEMP, TEMP, -2
#else
	daddiu	TEMP, TEMP, -1
#endif

	dsll	L,    TEMP, 1 + ZBASE_SHIFT
	dsll	TEMP, TEMP, ZBASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, BO, TEMP
#endif

#ifdef LEFT
	daddiu	KK, KK, 2
#endif
#endif
	daddiu	CO1,CO1, 4 * SIZE
	bgtz	I, .L21
	NOP

.L29:
	andi	I, M, 1
	blez	I, .L999
	NOP

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	BO,  B
#else
	dsll	TEMP, KK,  ZBASE_SHIFT

	daddu	AO, AO, TEMP
	daddu	BO, B,  TEMP
#endif

	gsLQC1(R12, F1, F0, 0)						#	R:a1	I:a2
	MTC	$0,  c11								#	Clear results regs
	MOV	c12, c11

	gsLQC1(R13, F5, F4, 0)						#	R:b1	I:b2
	MOV	c13, c11
	MOV	c14, c11

	FETCH	$0, 0 * SIZE(PREA)
	FETCH	$0, 4 * SIZE(PREA)

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, K, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 1
#else
	daddiu	TEMP, KK, 1
#endif
	dsra	L,  TEMP, 2
	blez	L, .L45
	NOP

#else
	dsra	L,  K, 2							#	Unroll K 4 times
	move	BO,  B

	gsLQC1(R12, F1, F0, 0)						#	R:a1	I:a2
	MTC	$0,  c11								#	Clear results regs
	MOV	c12, c11

	gsLQC1(R13, F5, F4, 0)						#	R:b1	I:b2
	MOV	c13, c11
	MOV	c14, c11

	FETCH	$0, 0 * SIZE(PREA)
	FETCH	$0, 4 * SIZE(PREA)
	blez	L, .L45
	NOP
#endif

	.align	3

.L42:
	gsLQC1(R12, F3, F2, 1)						#	R:a3	I:a4
	MADD1	c11, c11, a1, b1 					#	axc		A1xB1
	MADD3	c13, c13, a1, b2					#	axd

	gsLQC1(R13, F7, F6, 1)						#	R:b2	I:b3
	MADD2	c12, c12, a2, b1					#	bxc
	MADD4	c14, c14, a2, b2					#	bxd

	gsLQC1(R12, F9, F8, 2)						#	Unroll K=1
	MADD1	c11, c11, a3, b3 					#	axc		A1xB1
	MADD3	c13, c13, a3, b4					#	axd

	gsLQC1(R13, F13, F12, 2)
	MADD2	c12, c12, a4, b3					#	bxc
	MADD4	c14, c14, a4, b4					#	bxd
	daddiu	L, L, -1

	gsLQC1(R12, F11, F10, 3)
	MADD1	c11, c11, a5, b5 					#	axc		A1xB1
	MADD3	c13, c13, a5, b6					#	axd
	daddiu	AO, AO, 8 * SIZE 					#	2mr*4kr*cmpx

	gsLQC1(R13, F16, F15, 3)
	MADD2	c12, c12, a6, b5					#	bxc
	MADD4	c14, c14, a6, b6					#	bxd
	daddiu	BO, BO, 8 * SIZE  					#	2nr*4kr*cmpx

	gsLQC1(R12, F1, F0, 0)						#	R:a1	I:a2	Unroll K=4
	MADD1	c11, c11, a7, b7 					#	axc		A1xB1
	MADD3	c13, c13, a7, b8					#	axd

	gsLQC1(R13, F5, F4, 0)						#	R:b1	I:b2
	MADD2	c12, c12, a8, b7					#	bxc
	bgtz	L, .L42
	MADD4	c14, c14, a8, b8					#	bxd


	.align 5

.L45:
#ifndef	TRMMKERNEL
	andi	L,  K, 3
	LD	ALPHA_R, 128($sp)
#else
	andi	L,  TEMP, 3
	LD	ALPHA_R, 128($sp)
#endif
	blez	L, .L48
	LD	ALPHA_I, 136($sp)

.L46:
	daddiu	L, L, -1
	daddiu	BO, BO, 1 * SIZE * COMPSIZE			#	2nr*1kr*cmpx
	daddiu	AO, AO, 1 * SIZE * COMPSIZE			#	2mr*1kr*cmpx

	MADD1	c11, c11, a1, b1 					#	axc		A1xB1
	MADD3	c13, c13, a1, b2					#	axd
	MADD2	c12, c12, a2, b1					#	bxc
	MADD4	c14, c14, a2, b2					#	bxd

	gsLQC1(R12, F1, F0, 0)						#	R:a1	I:a2	Unroll K=4
	gsLQC1(R13, F5, F4, 0)						#	R:b1	I:b2

	bgtz	L, .L46
	NOP

.L48:
#ifndef	TRMMKERNEL
 	ADD	c11, c14, c11
	ADD	c12, c13, c12

	LD	a1,  0 * SIZE(CO1)
	LD	a2,  1 * SIZE(CO1)

	MADD	a1, a1, ALPHA_R, c11
	MADD	a2, a2, ALPHA_R, c12

	NMSUB	a1, a1, ALPHA_I, c12
	MADD	a2, a2, ALPHA_I, c11

	ST	a1, 0 * SIZE(CO1)
	ST	a2, 1 * SIZE(CO1)

#else
	ADD	c11, c14, c11
	ADD	c12, c13, c12

	MUL	a1, ALPHA_R, c11
	MUL	a2, ALPHA_R, c12

	NMSUB	a1, a1, ALPHA_I, c12
	MADD	a2, a2, ALPHA_I, c11

	ST	a1, 0 * SIZE(CO1)
	ST	a2, 1 * SIZE(CO1)

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

	daddiu	CO1,CO1, 2 * SIZE
#endif



	.align 5

.L999:
	LDARG	$16,   0($sp)
	LDARG	$17,   8($sp)
	ldc1	$f24, 16($sp)
	ldc1	$f25, 24($sp)
	ldc1	$f26, 32($sp)
	ldc1	$f27, 40($sp)
	ldc1	$f28, 48($sp)
	ldc1	$f29, 56($sp)

#if defined(TRMMKERNEL)
	LDARG	$18,  64($sp)
	LDARG	$19,  72($sp)
	LDARG	$20,  80($sp)
#endif

#ifndef __64BIT__
	ldc1	$f20, 88($sp)
	ldc1	$f21, 96($sp)
	ldc1	$f22,104($sp)
	ldc1	$f23,112($sp)
#endif

	j	$31
	daddiu	$sp, $sp, STACKSIZE

	EPILOGUE
