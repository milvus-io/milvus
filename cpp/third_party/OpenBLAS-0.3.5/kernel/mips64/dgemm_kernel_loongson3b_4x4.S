#define REALNAME ASMNAME
#define ASSEMBLER
#include "common.h"

#define FETCH	ld
#define gsLQC1(base,fq,ft,offset) .word(0x32<<26|base<<21|ft<<16|0x1<<15|offset<<6|0x1<<5|fq)
#define gsSQC1(base,fq,ft,offset) .word(0x3A<<26|base<<21|ft<<16|0x1<<15|offset<<6|0x1<<5|fq)

#define M	$4
#define	N	$5
#define	K	$6
#define A	$8
#define B	$9
#define C	$10
#define LDC	$11

#define AO	$12
#define BO	$13

#define CO1	$14
#define CO2	$15
#define CO3	$16
#define CO4	$17

#define KCO	$18
#define MCO	$19
#define NCO	$20

#define SPANB	$21
#define PREB	$23
#define PREA	$24
#define SPANA	$25

#define ALPHA	$f15

#if defined(TRMMKERNEL)
#define	OFFSET	$2
#define	KK	$3
#define	TEMP	$7
#endif

#define R8	8
#define	R9	9
#define R14	14
#define R15	15
#define R16	16
#define R17 17

#define	t11	$f30
#define	t21	$f31
#define	t31	$f28
#define	t41	$f29

#define	t12	$f26
#define	t22	$f27
#define	t32	$f24
#define	t42	$f25

#define	t13	$f22
#define	t23	$f23
#define	t33	$f20
#define	t43	$f21

#define	t14	$f18
#define	t24	$f19
#define	t34	$f16
#define	t44	$f17

#define	c11	$f0
#define	c21	$f1
#define	c31	$f2
#define	c41	$f3

#define	c12	$f4
#define	c22	$f5
#define	c32	$f6
#define	c42	$f7

#define	c13	$f8
#define	c23	$f9
#define	c33	$f10
#define c43	$f11

#define	c14	$f12
#define	c24	$f13
#define	c34	$f14
#define	c44	$f0

#define	a0	$f0
#define	a1	$f1
#define	a2	$f2
#define	a3	$f3
#define	a4	$f4
#define	a5	$f5
#define	a6	$f6
#define	a7	$f7
#define	b0	$f8
#define	b1	$f9
#define	b2	$f10
#define b3	$f11
#define	b4	$f12
#define	b5	$f13
#define	b6	$f14
#define	b7	$f15

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

	PROLOGUE

	daddiu	$sp, $sp, -160
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
	sd	$23,  96($sp)
	sd	$24, 104($sp)
	sd	$25, 112($sp)
	ST	$f20,120($sp)
	ST	$f21,128($sp)
	ST	$f22,136($sp)
	ST	$f23,144($sp)


	.align	5
.L0_N4:									#  Loop N
	ST	ALPHA,152($sp)					#  Backup	ALPHA
	move	MCO,M						#  Backup	M

	move	NCO,N						#  Backup	N
	move	KCO,K						#  Backup	K

	move	AO,A						#  Backup	A_addr
	dsra	N,NCO,2						#  N=NCO/2

	dsll	LDC,LDC,BASE_SHIFT			#  LDC*8Byte
	dsll	SPANB,KCO,2+BASE_SHIFT		#  SPANB=KC*4nr*8Byte=KC*2^5

#if defined(TRMMKERNEL)
	LDARG	OFFSET,160($sp)				#	OFFSET is relate to the data part
#endif

#if defined(TRMMKERNEL) && !defined(LEFT)
	neg		KK,OFFSET
#endif

	move	BO,B						#  Backup	B_addr
	beq		N,$0,.L0_N2					#  N=0,NCO<4
	dsll	SPANA,KCO,1+BASE_SHIFT		#  SPANA = KCO*2mr*8Byte

.L0_N4_Lb:								#	mr=4,nr=4
	move	CO1,C
	dsra	M,MCO,2						#  M=MCO/2

	move	A,AO						#  Reset A
	daddu	CO2,C,LDC

	daddu	PREB,BO,SPANB				#  PreB point next panelB
	daddu	CO3,CO2,LDC

	daddu	PREA,AO,SPANA
	daddu	CO4,CO3,LDC

#if defined(TRMMKERNEL) && defined(LEFT)
	move	KK,OFFSET
#endif
	beqz	M,.L14_M2
	daddu	C,CO4,LDC					#	move C to next panel Cj

.L10:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	B,BO						#	(SIDE=L and UPLO=L) or (SIZE=R and UPLO=U)
#else
	dsll	K,KK,2 + BASE_SHIFT			#  KK is the length that needs to span to the data part
	dsll	TEMP,KK,2 + BASE_SHIFT

	daddu	A,A,K						#  move A B to data part
	daddu	B,BO,TEMP
#endif

	MTC		$0,t11						# 	GEMM part	NR=4,MR=4
	LD	a0,0(A)

	MOV	t21,t11
	MOV	t31,t11
	LD	a1,1*SIZE(A)

	MOV	t41,t11
	MOV	t12,t11
	LD	b0,0(B)

	MOV	t22,t11
	MOV	t32,t11
	LD	b1,1*SIZE(B)

	MOV	t42,t11
	LD	a2,2*SIZE(A)

	MOV	t13,t11
	MOV	t23,t11
	LD	b2,2*SIZE(B)

	MOV	t33,t11
	MOV	t43,t11
	LD	a3,3*SIZE(A)

	MOV	t14,t11
	MOV	t24,t11
	LD	b3,3*SIZE(B)

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP,KCO,KK					#  temp is the length of the data part
#elif defined(LEFT)
	daddiu	TEMP, KK, 4					#	S=L,U=L
#else
	daddiu	TEMP, KK, 4					#	S=R,U=U,for this two situation KK is the length of the data part
#endif
	dsra	K,TEMP,2					#  K=KCO/2
	MOV	t34,t11
	beqz	K,.L15
	MOV	t44,t11

#else
	move	B,BO						#	Reset B
	MTC		$0,t11						# 	GEMM part	NR=4,MR=4
	LD	a0,0(A)

	MOV	t21,t11
	MOV	t31,t11
	LD	a1,1*SIZE(A)

	MOV	t41,t11
	MOV	t12,t11
	LD	b0,0(B)

	MOV	t22,t11
	MOV	t32,t11
	LD	b1,1*SIZE(B)

	MOV	t42,t11
	dsra	K,KCO,2						#  K=KCO/2
	LD	a2,2*SIZE(A)

	MOV	t13,t11
	MOV	t23,t11
	LD	b2,2*SIZE(B)

	MOV	t33,t11
	MOV	t43,t11
	LD	a3,3*SIZE(A)

	MOV	t14,t11
	MOV	t24,t11
	LD	b3,3*SIZE(B)

	MOV	t34,t11
	beqz	K,.L15
	MOV	t44,t11							#	clear 16 results registers
#endif

	.align	5
.L11:									#  kr=4
	MADD	t11,t11,a0,b0
	MADD	t21,t21,a1,b0
	LD	a4,4*SIZE(A)

	MADD	t12,t12,a0,b1
	MADD	t22,t22,a1,b1
	LD	a5,5*SIZE(A)

	MADD	t31,t31,a2,b0
	MADD	t41,t41,a3,b0
	LD	b4,4*SIZE(B)

	MADD	t32,t32,a2,b1
	MADD	t42,t42,a3,b1
	LD	b5,5*SIZE(B)
	FETCH		$0,(PREB)

	MADD	t13,t13,a0,b2
	MADD	t23,t23,a1,b2
	LD	a6,6*SIZE(A)

	MADD	t14,t14,a0,b3
	MADD	t24,t24,a1,b3
	LD	b6,6*SIZE(B)
	FETCH		$0,(PREA)

	MADD	t33,t33,a2,b2
	MADD	t43,t43,a3,b2
	LD	a7,7*SIZE(A)

	MADD	t34,t34,a2,b3
	MADD	t44,t44,a3,b3
	LD	b7,7*SIZE(B)

.L12:
	MADD	t11,t11,a4,b4
	MADD	t21,t21,a5,b4
	LD	a0,8*SIZE(A)

	MADD	t12,t12,a4,b5
	MADD	t22,t22,a5,b5
	LD	a1,9*SIZE(A)

	MADD	t31,t31,a6,b4
	MADD	t41,t41,a7,b4
	LD	b0,8*SIZE(B)

	MADD	t32,t32,a6,b5
	MADD	t42,t42,a7,b5
	LD	b1,9*SIZE(B)

	FETCH		$0,4*SIZE(PREB)
	MADD	t13,t13,a4,b6
	MADD	t23,t23,a5,b6
	LD	a2,10*SIZE(A)

	MADD	t14,t14,a4,b7
	MADD	t24,t24,a5,b7
	LD	b2,10*SIZE(B)

	FETCH		$0,4*SIZE(PREA)
	MADD	t33,t33,a6,b6
	MADD	t43,t43,a7,b6
	LD	a3,11*SIZE(A)

	MADD	t34,t34,a6,b7
	MADD	t44,t44,a7,b7
	LD	b3,11*SIZE(B)

.L13:
	MADD	t11,t11,a0,b0
	MADD	t21,t21,a1,b0
	LD	a4,12*SIZE(A)

	MADD	t12,t12,a0,b1
	MADD	t22,t22,a1,b1
	LD	a5,13*SIZE(A)

	MADD	t31,t31,a2,b0
	MADD	t41,t41,a3,b0
	LD	b4,12*SIZE(B)

	FETCH		$0,8*SIZE(PREA)
	MADD	t32,t32,a2,b1
	MADD	t42,t42,a3,b1
	LD	b5,13*SIZE(B)

	FETCH		$0,8*SIZE(PREB)
	MADD	t13,t13,a0,b2
	MADD	t23,t23,a1,b2
	LD	a6,14*SIZE(A)

	MADD	t14,t14,a0,b3
	MADD	t24,t24,a1,b3
	daddu	A,A,16*SIZE					#  4mr*4kr
	LD	b6,14*SIZE(B)

	MADD	t33,t33,a2,b2
	MADD	t43,t43,a3,b2
	daddu	B,B,16*SIZE					#	4nr*4kr
	LD	a7,-1*SIZE(A)

	MADD	t34,t34,a2,b3
	MADD	t44,t44,a3,b3
	LD	b7,-1*SIZE(B)

.L14:
	MADD	t11,t11,a4,b4
	MADD	t21,t21,a5,b4
	LD	a0,0(A)

	MADD	t12,t12,a4,b5
	MADD	t22,t22,a5,b5
	LD	a1,1*SIZE(A)

	MADD	t31,t31,a6,b4
	MADD	t41,t41,a7,b4
	daddiu	K,K,-1
	LD	b0,0(B)

	MADD	t32,t32,a6,b5
	MADD	t42,t42,a7,b5
	daddu	PREA,PREA,16*SIZE
	LD	b1,1*SIZE(B)

	FETCH		$0,12*SIZE(PREB)
	MADD	t13,t13,a4,b6
	MADD	t23,t23,a5,b6
	LD	a2,2*SIZE(A)

	FETCH		$0,-4*SIZE(PREA)
	MADD	t14,t14,a4,b7
	MADD	t24,t24,a5,b7
	LD	b2,2*SIZE(B)

	MADD	t33,t33,a6,b6
	MADD	t43,t43,a7,b6
	daddu	PREB,PREB,16*SIZE
	LD	a3,3*SIZE(A)

	MADD	t34,t34,a6,b7
	MADD	t44,t44,a7,b7
	bnez 	K,.L11
	LD	b3,3*SIZE(B)


.L15:									#  kr=2
#ifndef TRMMKERNEL
	andi	K,KCO,2
#else
	andi	K,TEMP, 2
#endif
	beqz	K,.L18
	nop

.L16:
	MADD	t11,t11,a0,b0
	MADD	t21,t21,a1,b0
	LD	a4,4*SIZE(A)

	MADD	t12,t12,a0,b1
	MADD	t22,t22,a1,b1
	LD	a5,5*SIZE(A)

	MADD	t31,t31,a2,b0
	MADD	t41,t41,a3,b0
	LD	b4,4*SIZE(B)

	FETCH		$0,0(PREA)
	MADD	t32,t32,a2,b1
	MADD	t42,t42,a3,b1
	LD	b5,5*SIZE(B)

	FETCH		$0,0(PREB)
	MADD	t13,t13,a0,b2
	MADD	t23,t23,a1,b2
	LD	a6,6*SIZE(A)

	MADD	t14,t14,a0,b3
	MADD	t24,t24,a1,b3
	daddu	A,A,8*SIZE					#	4mr*2kr
	LD	b6,6*SIZE(B)

	MADD	t33,t33,a2,b2
	MADD	t43,t43,a3,b2
	daddu	B,B,8*SIZE					#	4nr*2kr
	LD	a7,-1*SIZE(A)

	MADD	t34,t34,a2,b3
	MADD	t44,t44,a3,b3
	LD	b7,-1*SIZE(B)

.L17:
	MADD	t11,t11,a4,b4
	MADD	t21,t21,a5,b4
	LD	a0,0*SIZE(A)

	MADD	t12,t12,a4,b5
	MADD	t22,t22,a5,b5
	LD	a1,1*SIZE(A)

	MADD	t31,t31,a6,b4
	MADD	t41,t41,a7,b4
	LD	b0,0*SIZE(B)

	MADD	t32,t32,a6,b5
	MADD	t42,t42,a7,b5
	LD	b1,1*SIZE(B)

	FETCH		$0,4*SIZE(PREB)
	MADD	t13,t13,a4,b6
	MADD	t23,t23,a5,b6
	LD	a2,2*SIZE(A)

	FETCH		$0,4*SIZE(PREA)
	MADD	t14,t14,a4,b7
	MADD	t24,t24,a5,b7
	LD	b2,2*SIZE(B)

	MADD	t33,t33,a6,b6
	MADD	t43,t43,a7,b6
	daddu	PREA,PREA,8*SIZE
	LD	a3,3*SIZE(A)

	MADD	t34,t34,a6,b7
	MADD	t44,t44,a7,b7
	daddu	PREB,PREB,8*SIZE
	LD	b3,3*SIZE(B)


.L18:									#	kr=1
#ifndef TRMMKERNEL
	andi	K,KCO,1
#else
	andi	K,TEMP,1
#endif
	beqz	K,.L19
	LD	ALPHA,152($sp)					#  Get ALPHA

	FETCH		$0,0(PREB)
	MADD	t11,t11,a0,b0
	MADD	t21,t21,a1,b0
	daddu	A,A,4*SIZE					#  	4mr*kr

	MADD	t12,t12,a0,b1
	MADD	t22,t22,a1,b1
	daddu	B,B,4*SIZE					#	4nr*kr

	FETCH		$0,0(PREA)
	MADD	t31,t31,a2,b0
	MADD	t41,t41,a3,b0
	daddu	PREB,PREB,4*SIZE

	MADD	t32,t32,a2,b1
	MADD	t42,t42,a3,b1
	daddu	PREA,PREA,4*SIZE

	MADD	t13,t13,a0,b2
	MADD	t23,t23,a1,b2

	MADD	t14,t14,a0,b3
	MADD	t24,t24,a1,b3

	MADD	t33,t33,a2,b2
	MADD	t43,t43,a3,b2

	MADD	t34,t34,a2,b3
	MADD	t44,t44,a3,b3

.L19:									#  Write Back to C
#ifndef TRMMKERNEL
	LD	c11,0(CO1)						#  GEMM write part
	LD	c21,1*SIZE(CO1)					#  get 16 C
	LD	c31,2*SIZE(CO1)
	LD	c41,3*SIZE(CO1)

	LD	c12,0(CO2)
	MADD	t11,c11,t11,ALPHA
	LD	c22,1*SIZE(CO2)
	MADD	t21,c21,t21,ALPHA
	LD	c32,2*SIZE(CO2)
	MADD	t31,c31,t31,ALPHA
	LD	c42,3*SIZE(CO2)
	MADD	t41,c41,t41,ALPHA

	LD	c13,0(CO3)
	MADD	t12,c12,t12,ALPHA
	LD	c23,1*SIZE(CO3)
	MADD	t22,c22,t22,ALPHA
	LD	c33,2*SIZE(CO3)
	MADD	t32,c32,t32,ALPHA
	LD	c43,3*SIZE(CO3)
	MADD	t42,c42,t42,ALPHA

	LD	c14,0(CO4)
	MADD	t13,c13,t13,ALPHA
	LD	c24,1*SIZE(CO4)
	MADD	t23,c23,t23,ALPHA
	LD	c34,2*SIZE(CO4)
	MADD	t33,c33,t33,ALPHA
	LD	c44,3*SIZE(CO4)
	MADD	t43,c43,t43,ALPHA

	ST	t11,0(CO1)
	MADD	t14,c14,t14,ALPHA
	ST	t21,1*SIZE(CO1)
	MADD	t24,c24,t24,ALPHA
	ST	t31,2*SIZE(CO1)
	MADD	t34,c34,t34,ALPHA
	ST	t41,3*SIZE(CO1)
	MADD	t44,c44,t44,ALPHA
	daddiu	M,M,-1						#  M--

	ST	t12,0(CO2)
	ST	t22,1*SIZE(CO2)
	ST	t32,2*SIZE(CO2)
	ST	t42,3*SIZE(CO2)

	ST	t13,0(CO3)
	ST	t23,1*SIZE(CO3)
	ST	t33,2*SIZE(CO3)
	ST	t43,3*SIZE(CO3)

	FETCH	$0,4*SIZE(CO1)
	FETCH	$0,4*SIZE(CO2)
	FETCH	$0,4*SIZE(CO3)
	FETCH	$0,4*SIZE(CO4)

	FETCH	$0,8*SIZE(CO1)
	FETCH	$0,8*SIZE(CO2)
	FETCH	$0,8*SIZE(CO3)
	FETCH	$0,8*SIZE(CO4)

	ST	t14,0(CO4)
	daddu	CO1,CO1,4*SIZE				#  COi += 4
	ST	t24,1*SIZE(CO4)
	daddu	CO2,CO2,4*SIZE
	ST	t34,2*SIZE(CO4)
	daddu	CO3,CO3,4*SIZE
	ST	t44,3*SIZE(CO4)
	daddu	PREB,BO,SPANB

	bnez	M,.L10
	daddu	CO4,CO4,4*SIZE

#else
	MUL	t11, ALPHA, t11					#	TRMM write back part
	MUL	t21, ALPHA, t21
	MUL	t31, ALPHA, t31
	MUL	t41, ALPHA, t41

	ST	t11, 0 * SIZE(CO1)
	MUL	t12, ALPHA, t12
	ST	t21, 1 * SIZE(CO1)
	MUL	t22, ALPHA, t22
	ST	t31, 2 * SIZE(CO1)
	MUL	t32, ALPHA, t32
	ST	t41, 3 * SIZE(CO1)
	MUL	t42, ALPHA, t42

	ST	t12, 0 * SIZE(CO2)
	MUL	t13, ALPHA, t13
	ST	t22, 1 * SIZE(CO2)
	MUL	t23, ALPHA, t23
	ST	t32, 2 * SIZE(CO2)
	MUL	t33, ALPHA, t33
	ST	t42, 3 * SIZE(CO2)
	MUL	t43, ALPHA, t43

	ST	t13, 0 * SIZE(CO3)
	MUL	t14, ALPHA, t14
	ST	t23, 1 * SIZE(CO3)
	MUL	t24, ALPHA, t24
	ST	t33, 2 * SIZE(CO3)
	MUL	t34, ALPHA, t34
	ST	t43, 3 * SIZE(CO3)
	MUL	t44, ALPHA, t44

	ST	t14, 0 * SIZE(CO4)
	daddiu	M,M,-1						#  M--
	ST	t24, 1 * SIZE(CO4)
	ST	t34, 2 * SIZE(CO4)
	ST	t44, 3 * SIZE(CO4)
	daddiu	CO1,CO1, 4 * SIZE
	daddiu	CO2,CO2, 4 * SIZE
	daddiu	CO3,CO3, 4 * SIZE
	daddiu	CO4,CO4, 4 * SIZE

	FETCH	$0,4*SIZE(CO1)
	FETCH	$0,4*SIZE(CO2)
	FETCH	$0,4*SIZE(CO3)
	FETCH	$0,4*SIZE(CO4)

	FETCH	$0,0(CO1)
	FETCH	$0,0(CO2)
	FETCH	$0,0(CO3)
	FETCH	$0,0(CO4)

#if ( defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP,KCO,KK
#ifdef LEFT
	daddiu	TEMP,TEMP, -4
#else
	daddiu	TEMP,TEMP, -4
#endif
	dsll	K,TEMP,2 + BASE_SHIFT
	dsll	TEMP,TEMP,2 + BASE_SHIFT
	daddu	A,A,K						# 	mov A to the end of panel Ai
	daddu	B,B,TEMP					# 	mov B to the end of panel Bj
#endif

#ifdef LEFT
	daddiu	KK, KK,4
#endif
	bnez	M,.L10
	nop
#endif


	.align 3
.L14_M2:
	andi	M, MCO, 2					#	nr=4,mr=2
	beqz	M,.L14_M1
	nop

.L20:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	B,BO						#	Reset B
#else
	dsll	K,KK,1 + BASE_SHIFT			#	mr=2
	dsll	TEMP,KK,2 + BASE_SHIFT		#	nr=4
	daddu	A,A,K
	daddu	B,BO,TEMP
#endif

	LD	a0,0*SIZE(A)
	MTC		$0,t11
	LD	a1,1*SIZE(A)

	MOV	t21,t11
	LD	b0,0*SIZE(B)
	MOV	t12,t11
	LD	b1,1*SIZE(B)

	MOV	t22,t11
	LD	b2,2*SIZE(B)

	MOV	t13,t11
	MOV	t23,t11
	LD	b3,3*SIZE(B)

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP,KCO,KK
#elif defined(LEFT)
	daddiu	TEMP,KK,2					#	left part,controlled by mr, mr=2
#else
	daddiu	TEMP,KK,4					#  	right part,controlled by nr,nr=4
#endif
	dsra	K,TEMP,2
	MOV	t14,t11
	beqz	K,.L25
	MOV	t24,t11							#	clear 2*4=8 results registers

#else
	move	B,BO						#	Reset B
	LD	a0,0*SIZE(A)
	MTC		$0,t11
	LD	a1,1*SIZE(A)

	MOV	t21,t11
	LD	b0,0*SIZE(B)
	MOV	t12,t11
	LD	b1,1*SIZE(B)

	MOV	t22,t11
	dsra	K,KCO,2
	LD	b2,2*SIZE(B)

	MOV	t13,t11
	MOV	t23,t11
	LD	b3,3*SIZE(B)

	MOV	t14,t11
	beqz	K,.L25
	MOV	t24,t11

#endif

.L21:									#  nr=4,mr=2,kr=4
	MADD	t11,t11,a0,b0
	LD	a4,2*SIZE(A)
	MADD	t21,t21,a1,b0
	LD	a5,3*SIZE(A)

	MADD	t12,t12,a0,b1
	LD	b4,4*SIZE(B)
	MADD	t22,t22,a1,b1
	LD	b5,5*SIZE(B)

	MADD	t13,t13,a0,b2
	LD	b6,6*SIZE(B)
	MADD	t23,t23,a1,b2
	LD	b7,7*SIZE(B)

	MADD	t14,t14,a0,b3
	MADD	t24,t24,a1,b3

	MADD	t11,t11,a4,b4
	LD	a2,4*SIZE(A)
	MADD	t21,t21,a5,b4
	LD	a3,5*SIZE(A)

	MADD	t12,t12,a4,b5
	LD	b0,8*SIZE(B)
	MADD	t22,t22,a5,b5
	LD	b1,9*SIZE(B)

	MADD	t13,t13,a4,b6
	LD	b2,10*SIZE(B)
	MADD	t23,t23,a5,b6
	LD	b3,11*SIZE(B)

	MADD	t14,t14,a4,b7
	MADD	t24,t24,a5,b7
	daddiu	K,K,-1

	MADD	t11,t11,a2,b0
	LD	a6,6*SIZE(A)
	MADD	t21,t21,a3,b0
	LD	a7,7*SIZE(A)

	MADD	t12,t12,a2,b1
	LD	b4,12*SIZE(B)
	MADD	t22,t22,a3,b1
	LD	b5,13*SIZE(B)

	MADD	t13,t13,a2,b2
	LD	b6,14*SIZE(B)
	MADD	t23,t23,a3,b2
	LD	b7,15*SIZE(B)

	MADD	t14,t14,a2,b3
	MADD	t24,t24,a3,b3
	daddu	A,A,8*SIZE					#  2mr*4kr
	daddu	B,B,16*SIZE					#	4nr*4kr

	MADD	t11,t11,a6,b4
	LD	a0,0*SIZE(A)
	MADD	t21,t21,a7,b4
	LD	a1,1*SIZE(A)

	MADD	t12,t12,a6,b5
	LD	b0,0*SIZE(B)
	MADD	t22,t22,a7,b5
	LD	b1,1*SIZE(B)

	MADD	t13,t13,a6,b6
	LD	b2,2*SIZE(B)
	MADD	t23,t23,a7,b6
	LD	b3,3*SIZE(B)

	MADD	t14,t14,a6,b7
	bnez 	K,.L21
	MADD	t24,t24,a7,b7


.L25:
#ifndef TRMMKERNEL
	andi	K,KCO,2						#	kr=2
#else
	andi	K,TEMP,2
#endif
	beqz	K,.L28
	nop

.L26:
	MADD	t11,t11,a0,b0
	LD	a4,2*SIZE(A)
	MADD	t21,t21,a1,b0
	LD	a5,3*SIZE(A)

	MADD	t12,t12,a0,b1
	LD	b4,4*SIZE(B)
	MADD	t22,t22,a1,b1
	LD	b5,5*SIZE(B)

	MADD	t13,t13,a0,b2
	LD	b6,6*SIZE(B)
	MADD	t23,t23,a1,b2
	LD	b7,7*SIZE(B)

	MADD	t14,t14,a0,b3
	MADD	t24,t24,a1,b3
	daddu	A,A,4*SIZE					#  	2mr*2kr
	daddu	B,B,8*SIZE					#	4nr*2kr

.L27:
	MADD	t11,t11,a4,b4
	LD	a0,0*SIZE(A)
	MADD	t21,t21,a5,b4
	LD	a1,1*SIZE(A)

	MADD	t12,t12,a4,b5
	LD	b0,0*SIZE(B)
	MADD	t22,t22,a5,b5
	LD	b1,1*SIZE(B)

	MADD	t13,t13,a4,b6
	LD	b2,2*SIZE(B)
	MADD	t23,t23,a5,b6
	LD	b3,3*SIZE(B)

	MADD	t14,t14,a4,b7
	MADD	t24,t24,a5,b7


.L28:									#	kr=1
#ifndef TRMMKERNEL
	andi	K,KCO,1
#else
	andi	K,TEMP,1
#endif
	beqz	K,.L29
	LD	ALPHA,152($sp)					#  Get ALPHA

	MADD	t11,t11,a0,b0
	MADD	t21,t21,a1,b0
	daddu	A,A,2*SIZE					#  2mr*kr
	daddu	B,B,4*SIZE					#  4nr*kr

	MADD	t12,t12,a0,b1
	MADD	t22,t22,a1,b1

	MADD	t13,t13,a0,b2
	MADD	t23,t23,a1,b2

	MADD	t14,t14,a0,b3
	MADD	t24,t24,a1,b3

.L29:									#  Write Back to C
#ifndef TRMMKERNEL
	LD	c11,0(CO1)						#	GEMM write back part
	LD	c21,1*SIZE(CO1)

	LD	c12,0(CO2)
	LD	c22,1*SIZE(CO2)

	LD	c13,0(CO3)
	MADD	t11,c11,t11,ALPHA
	LD	c23,1*SIZE(CO3)
	MADD	t21,c21,t21,ALPHA

	LD	c14,0(CO4)
	MADD	t12,c12,t12,ALPHA
	LD	c24,1*SIZE(CO4)
	MADD	t22,c22,t22,ALPHA

	ST	t11,0(CO1)
	MADD	t13,c13,t13,ALPHA
	ST	t21,1*SIZE(CO1)
	MADD	t23,c23,t23,ALPHA

	ST	t12,0(CO2)
	MADD	t14,c14,t14,ALPHA
	ST	t22,1*SIZE(CO2)
	MADD	t24,c24,t24,ALPHA

	ST	t13,0(CO3)
	daddu	CO1,CO1,2*SIZE				#  COi += 2
	ST	t23,1*SIZE(CO3)
	daddu	CO2,CO2,2*SIZE

	ST	t14,0(CO4)
	daddu	CO3,CO3,2*SIZE
	ST	t24,1*SIZE(CO4)
	daddu	CO4,CO4,2*SIZE

	FETCH	$0,0(CO1)
	FETCH	$0,0(CO2)
	FETCH	$0,0(CO3)
	FETCH	$0,0(CO4)

#else
	MUL	t11, ALPHA, t11					#	TRMM write back part
	MUL	t21, ALPHA, t21

	ST	t11, 0 * SIZE(CO1)
	MUL	t12, ALPHA, t12
	ST	t21, 1 * SIZE(CO1)
	MUL	t22, ALPHA, t22

	ST	t12, 0 * SIZE(CO2)
	MUL	t13, ALPHA, t13
	ST	t22, 1 * SIZE(CO2)
	MUL	t23, ALPHA, t23

	ST	t13, 0 * SIZE(CO3)
	MUL	t14, ALPHA, t14
	ST	t23, 1 * SIZE(CO3)
	MUL	t24, ALPHA, t24

	ST	t14, 0 * SIZE(CO4)
	ST	t24, 1 * SIZE(CO4)

	daddiu	CO1,CO1, 2 * SIZE
	daddiu	CO2,CO2, 2 * SIZE
	daddiu	CO3,CO3, 2 * SIZE
	daddiu	CO4,CO4, 2 * SIZE

	FETCH	$0,0(CO1)
	FETCH	$0,0(CO2)
	FETCH	$0,0(CO3)
	FETCH	$0,0(CO4)

#if ( defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP,KCO,KK
#ifdef LEFT
	daddiu	TEMP,TEMP,-2
#else
	daddiu	TEMP,TEMP,-4
#endif
	dsll	K,TEMP,1 + BASE_SHIFT
	dsll	TEMP,TEMP,2 + BASE_SHIFT

	daddu	A,A,K						#	move A to next panel Ai
	daddu	B,B,TEMP					#	move B to next panel Bj
#endif

#ifdef LEFT
	daddiu	KK, KK, 2
#endif
#endif


	.align 3
.L14_M1:
	andi	M,MCO,1						#	mr=1
	beqz	M,.L0_N4_Loop				#  	M = 0, finishing one panel Bj
	nop

.L30:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	B,BO						#	Reset B
#else
	dsll	K,KK, BASE_SHIFT
	dsll	TEMP,KK,2 + BASE_SHIFT

	daddu	A,A,K
	daddu	B,BO,TEMP
#endif

	LD	a0, 0 * SIZE(A)					#	a0

	MTC		$0,t11
	LD	b0,0*SIZE(B)

	MOV	t12,t11
	LD	b1,1*SIZE(B)

	MOV	t13,t11
	LD	b2,2*SIZE(B)

	MOV	t14,t11
	LD	b3,3*SIZE(B)

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, KCO, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 1
#else
	daddiu	TEMP, KK, 4
#endif
	dsra	K,TEMP, 2
	nop
	beqz	K,.L35
	nop

#else
	move	B,BO						#	Reset B, GEMM part
	dsra	K,KCO,2						#  	K=KCO/2
	LD	a0, 0 * SIZE(A)					#	a0

	MTC		$0,t11
	LD	b0,0*SIZE(B)

	MOV	t12,t11
	LD	b1,1*SIZE(B)

	MOV	t13,t11
	LD	b2,2*SIZE(B)

	MOV	t14,t11
	beqz	K,.L35
	LD	b3,3*SIZE(B)

#endif

.L31:									#	nr=4,mr=1,kr=4
	LD	a1,	1*SIZE(A)					#	load a1
	MADD	t11,t11,a0,b0

	LD	b4,4*SIZE(B)
	LD	b5,5*SIZE(B)
	MADD	t12,t12,a0,b1

	LD	b6,6*SIZE(B)
	LD	b7,7*SIZE(B)
	MADD	t13,t13,a0,b2
	MADD	t14,t14,a0,b3

	LD	a2,	2*SIZE(A)					#	a2
	MADD	t11,t11,a1,b4

	LD	b0,8*SIZE(B)
	LD	b1,9*SIZE(B)
	MADD	t12,t12,a1,b5

	LD	b2,10*SIZE(B)
	LD	b3,11*SIZE(B)
	MADD	t13,t13,a1,b6
	MADD	t14,t14,a1,b7

	LD	a3,	3*SIZE(A)					#	a3
	MADD	t11,t11,a2,b0
	daddiu	K,K,-1

	LD	b4,12*SIZE(B)
	LD	b5,13*SIZE(B)
	MADD	t12,t12,a2,b1
	daddu	A,A,4*SIZE					#	1mr*4kr

	LD	b6,14*SIZE(B)
	LD	b7,15*SIZE(B)
	MADD	t13,t13,a2,b2
	MADD	t14,t14,a2,b3

	LD	a0,	0*SIZE(A)					#	a0
	daddu	B,B,16*SIZE					#	4nr*4kr
	MADD	t11,t11,a3,b4

	LD	b0,0*SIZE(B)
	MADD	t12,t12,a3,b5
	LD	b1,1*SIZE(B)
	MADD	t13,t13,a3,b6

	LD	b2,2*SIZE(B)
	MADD	t14,t14,a3,b7
	bnez 	K,.L31
	LD	b3,3*SIZE(B)


.L35:									#  kr=2
#ifndef TRMMKERNEL
	andi	K,KCO,2
#else
	andi	K,TEMP,2
#endif
	beqz	K,.L38
	nop

.L36:
	LD	a1,1*SIZE(A)					#	load a1
	MADD	t11,t11,a0,b0

	LD	b4,4*SIZE(B)
	LD	b5,5*SIZE(B)
	MADD	t12,t12,a0,b1
	daddu	A,A,2*SIZE					#  	mr*2kr

	LD	b6,6*SIZE(B)
	MADD	t13,t13,a0,b2

	LD	b7,7*SIZE(B)
	MADD	t14,t14,a0,b3
	daddu	B,B,8*SIZE					#	4nr*2kr


.L37:
	LD	a0,0(A)
	MADD	t11,t11,a1,b4

	LD	b0,0*SIZE(B)
	LD	b1,1*SIZE(B)
	MADD	t12,t12,a1,b5

	LD	b2,2*SIZE(B)
	LD	b3,3*SIZE(B)
	MADD	t13,t13,a1,b6
	MADD	t14,t14,a1,b7


.L38:									#  	kr=1
#ifndef TRMMKERNEL
	andi	K,KCO,1
#else
	andi	K,TEMP,1
#endif
	beqz	K,.L39
	LD	ALPHA,152($sp)					#  Get ALPHA

	MADD	t11,t11,a0,b0
	MADD	t12,t12,a0,b1
	daddu	A,A,1*SIZE
	daddu	B,B,4*SIZE

	MADD	t13,t13,a0,b2
	MADD	t14,t14,a0,b3

.L39:									#  Write Back
#ifndef TRMMKERNEL
	LD	c11,0(CO1)
	LD	c12,0(CO2)
	LD	c13,0(CO3)
	LD	c14,0(CO4)

	MADD	t11,c11,t11,ALPHA
	MADD	t12,c12,t12,ALPHA
	MADD	t13,c13,t13,ALPHA
	MADD	t14,c14,t14,ALPHA

	ST	t11,0(CO1)
	ST	t12,0(CO2)
	ST	t13,0(CO3)
	ST	t14,0(CO4)
#else
	MUL	t11, ALPHA, t11
	MUL	t12, ALPHA, t12
	MUL	t13, ALPHA, t13
	MUL	t14, ALPHA, t14

	ST	t11,  0 * SIZE(CO1)
	ST	t12,  0 * SIZE(CO2)
	ST	t13,  0 * SIZE(CO3)
	ST	t14,  0 * SIZE(CO4)

#if ( defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP, KCO, KK
#ifdef LEFT
	daddiu	TEMP, TEMP, -1
#else
	daddiu	TEMP, TEMP, -4
#endif

	dsll	K,TEMP, BASE_SHIFT
	dsll	TEMP,TEMP, 2 + BASE_SHIFT

	daddu	A,A,K
	daddu	B,B,TEMP
#endif

#ifdef LEFT
	daddiu	KK, KK, 1
#endif
#endif


	.align	3
.L0_N4_Loop:								#	mc finished
	daddiu	N,N,-1							#  N--
#if defined(TRMMKERNEL) && !defined(LEFT)
	daddiu	KK, KK,4
#endif
	bnez	N,.L0_N4_Lb
	move	BO,B							#  Set BO point to next panel Bj

	.align	5
.L0_N2:
	andi	N,NCO,2							#  	nr = 2
	beqz	N,.L0_N1
	nop

.L0_N2_Lb:
	move	CO1,C
	daddu	CO2,C,LDC

	dsra	M,MCO,2
	move	A,AO							#  Reset A

	daddu	PREA,AO,SPANA
	daddu	C,CO2,LDC

#if defined(TRMMKERNEL) &&  defined(LEFT)
	move	KK, OFFSET
#endif
	beqz	M,.L12_M2
	nop

.L40:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	B,BO							#	Reset B
#else
	dsll	K,KK, 2 + BASE_SHIFT
	dsll	TEMP, KK,1 + BASE_SHIFT

	daddu	A,A,K
	daddu	B,BO,TEMP
#endif
	LD	a0,0*SIZE(A)
	MTC		$0,t11							#  	gemm part
	LD	a1,1*SIZE(A)

	MOV	t21,t11
	LD	b0,0*SIZE(B)
	MOV	t31,t11
	LD	b1,1*SIZE(B)

	MOV	t41,t11
	LD	a2,2*SIZE(A)
	LD	a3,3*SIZE(A)

	MOV	t12,t11
	MOV	t22,t11

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP,KCO,KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 4
#else
	daddiu	TEMP, KK, 2
#endif
	dsra	K,TEMP,2
	MOV	t32,t11
	beqz	K,.L45
	MOV	t42,t11

#else
	move	B,BO							#	Reset B
	LD	a0,0*SIZE(A)
	MTC		$0,t11							#  	gemm part
	LD	a1,1*SIZE(A)

	MOV	t21,t11
	LD	b0,0*SIZE(B)
	MOV	t31,t11
	LD	b1,1*SIZE(B)

	MOV	t41,t11
	LD	a2,2*SIZE(A)
	dsra	K,KCO,2							#	K=KCO/2
	LD	a3,3*SIZE(A)

	MOV	t12,t11
	MOV	t22,t11

	MOV	t32,t11
	beqz	K,.L45
	MOV	t42,t11

#endif

.L41:										#  	nr=2,mr=kr=4
	MADD	t11,t11,a0,b0
	LD	a4,4*SIZE(A)
	MADD	t21,t21,a1,b0
	LD	a5,5*SIZE(A)

	MADD	t12,t12,a0,b1
	LD	b4,2*SIZE(B)
	MADD	t22,t22,a1,b1
	LD	b5,3*SIZE(B)

	MADD	t31,t31,a2,b0
	LD	a6,6*SIZE(A)
	MADD	t41,t41,a3,b0
	LD	a7,7*SIZE(A)

	FETCH		$0,(PREA)
	MADD	t32,t32,a2,b1
	MADD	t42,t42,a3,b1

.L42:
	MADD	t11,t11,a4,b4
	LD	a0,8*SIZE(A)
	MADD	t21,t21,a5,b4
	LD	a1,9*SIZE(A)

	MADD	t12,t12,a4,b5
	LD	b2,4*SIZE(B)
	MADD	t22,t22,a5,b5
	LD	b3,5*SIZE(B)

	MADD	t31,t31,a6,b4
	LD	a2,10*SIZE(A)
	MADD	t41,t41,a7,b4
	LD	a3,11*SIZE(A)

	FETCH		$0,4*SIZE(PREA)
	MADD	t32,t32,a6,b5
	MADD	t42,t42,a7,b5

.L43:
	MADD	t11,t11,a0,b2
	LD	a4,12*SIZE(A)
	MADD	t21,t21,a1,b2
	LD	a5,13*SIZE(A)

	MADD	t12,t12,a0,b3
	LD	b6,6*SIZE(B)
	MADD	t22,t22,a1,b3
	LD	b7,7*SIZE(B)

	MADD	t31,t31,a2,b2
	LD	a6,14*SIZE(A)
	MADD	t41,t41,a3,b2
	LD	a7,15*SIZE(A)

	FETCH		$0,8*SIZE(PREA)
	MADD	t32,t32,a2,b3
	MADD	t42,t42,a3,b3

	daddu	A,A,16*SIZE						#	4mr*4kr
	daddu	B,B,8*SIZE						#	2nr*4kr

.L44:
	MADD	t11,t11,a4,b6
	LD	a0,0*SIZE(A)
	MADD	t21,t21,a5,b6
	LD	a1,1*SIZE(A)


	MADD	t12,t12,a4,b7
	LD	b0,0*SIZE(B)
	MADD	t22,t22,a5,b7
	LD	b1,1*SIZE(B)

	daddiu	K,K,-1
	daddu	PREA,PREA,16*SIZE

	MADD	t31,t31,a6,b6
	LD	a2,2*SIZE(A)
	MADD	t41,t41,a7,b6
	LD	a3,3*SIZE(A)

	FETCH		$0,-4*SIZE(PREA)
	MADD	t32,t32,a6,b7
	bnez 	K,.L41
	MADD	t42,t42,a7,b7


.L45:										#  	kr=2
#ifndef TRMMKERNEL
	andi	K,KCO,2
#else
	andi	K,TEMP,2
#endif
	beqz	K,.L48
	nop

.L46:
	MADD	t11,t11,a0,b0
	LD	a4,4*SIZE(A)
	MADD	t21,t21,a1,b0
	LD	a5,5*SIZE(A)

	MADD	t12,t12,a0,b1
	LD	b4,2*SIZE(B)
	MADD	t22,t22,a1,b1
	LD	b5,3*SIZE(B)

	MADD	t31,t31,a2,b0
	LD	a6,6*SIZE(A)
	MADD	t41,t41,a3,b0
	LD	a7,7*SIZE(A)

	FETCH		$0,0(PREA)
	MADD	t32,t32,a2,b1
	daddu	B,B,4*SIZE						#  B+=2(nr)*2(kr)*8Byte=32

	MADD	t42,t42,a3,b1
	daddu	A,A,8*SIZE						#  A+=4(mr)*2(kr)*8Byte=8*SIZE

.L47:
	MADD	t11,t11,a4,b4
	LD	a0,0*SIZE(A)
	MADD	t21,t21,a5,b4
	LD	a1,1*SIZE(A)

	MADD	t12,t12,a4,b5
	LD	b0,0*SIZE(B)
	MADD	t22,t22,a5,b5
	LD	b1,1*SIZE(B)

	MADD	t31,t31,a6,b4
	LD	a2,2*SIZE(A)
	MADD	t41,t41,a7,b4
	LD	a3,3*SIZE(A)

	FETCH		$0,4*SIZE(PREA)
	MADD	t32,t32,a6,b5
	MADD	t42,t42,a7,b5
	daddu	PREA,PREA,8*SIZE



.L48:										#	 kr=1
#ifndef TRMMKERNEL
	andi	K,KCO,1
#else
	andi	K,TEMP,1
#endif
	beqz	K,.L49
	LD	ALPHA,152($sp)						#  Get ALPHA

	FETCH		$0,0(PREA)
	MADD	t11,t11,a0,b0
	MADD	t21,t21,a1,b0
	daddu	A,A,4*SIZE						#  A+=4(mr)*1(kr)*8Byte=32

	MADD	t12,t12,a0,b1
	MADD	t22,t22,a1,b1
	daddu	B,B,2*SIZE
	daddu	PREA,PREA,4*SIZE

	MADD	t31,t31,a2,b0
	MADD	t41,t41,a3,b0

	MADD	t32,t32,a2,b1
	MADD	t42,t42,a3,b1

.L49:										#  Write Back
#ifndef TRMMKERNEL
	LD	c11,0(CO1)							#  gemm write back part Fetch 16 C
	LD	c21,1*SIZE(CO1)
	LD	c31,2*SIZE(CO1)
	LD	c41,3*SIZE(CO1)

	LD	c12,0(CO2)
	MADD	t11,c11,t11,ALPHA
	LD	c22,1*SIZE(CO2)
	MADD	t21,c21,t21,ALPHA
	LD	c32,2*SIZE(CO2)
	MADD	t31,c31,t31,ALPHA
	LD	c42,3*SIZE(CO2)
	MADD	t41,c41,t41,ALPHA

	ST	t11,0(CO1)
	MADD	t12,c12,t12,ALPHA
	ST	t21,1*SIZE(CO1)
	MADD	t22,c22,t22,ALPHA
	ST	t31,2*SIZE(CO1)
	MADD	t32,c32,t32,ALPHA
	ST	t41,3*SIZE(CO1)
	MADD	t42,c42,t42,ALPHA
	daddiu	M,M,-1

	ST	t12,0(CO2)
	ST	t22,1*SIZE(CO2)
	ST	t32,2*SIZE(CO2)
	ST	t42,3*SIZE(CO2)

	FETCH	$0,4*SIZE(CO1)
	FETCH	$0,4*SIZE(CO2)
	FETCH	$0,8*SIZE(CO1)
	FETCH	$0,8*SIZE(CO2)

	daddu	CO1,CO1,4*SIZE
	bnez	M,.L40
	daddu	CO2,CO2,4*SIZE

#else
	MUL	t11, ALPHA, t11
	MUL	t21, ALPHA, t21
	MUL	t31, ALPHA, t31
	MUL	t41, ALPHA, t41

	MUL	t12, ALPHA, t12
	ST	t11, 0 * SIZE(CO1)
	MUL	t22, ALPHA, t22
	ST	t21, 1 * SIZE(CO1)
	MUL	t32, ALPHA, t32
	ST	t31, 2 * SIZE(CO1)
	MUL	t42, ALPHA, t42
	ST	t41, 3 * SIZE(CO1)

	ST	t12, 0 * SIZE(CO2)
	daddiu	M,M,-1
	ST	t22, 1 * SIZE(CO2)
	ST	t32, 2 * SIZE(CO2)
	ST	t42, 3 * SIZE(CO2)

	daddiu	CO1,CO1, 4*SIZE
	daddiu	CO2,CO2, 4*SIZE

	FETCH	$0,0(CO1)
	FETCH	$0,0(CO2)
	FETCH	$0,4(CO1)
	FETCH	$0,4(CO2)

#if ( defined(LEFT) &&  defined(TRANSA)) ||(!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP, KCO, KK
#ifdef LEFT
	daddiu	TEMP, TEMP, -4
#else
	daddiu	TEMP, TEMP, -2
#endif
	dsll	K,TEMP, 2 + BASE_SHIFT
	dsll	TEMP, TEMP, 1 + BASE_SHIFT

	daddu	A,A,K
	daddu	B,B,TEMP
#endif

#ifdef LEFT
	daddiu	KK, KK, 4
#endif
	bnez	M,.L40
	nop
#endif


	.align 3
.L12_M2:
	andi	M,MCO,2						#  	mr = 2
	beqz	M,.L12_M1
	nop

.L50:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	B,BO
#else
	dsll	K,    KK, 1 + BASE_SHIFT	#mr=2
	dsll	TEMP, KK, 1 + BASE_SHIFT	#nr=2

	daddu	A, A, K
	daddu	B, BO,  TEMP
#endif
	LD	a0,0*SIZE(A)
	LD	a1,1*SIZE(A)

	MTC		$0,t11
	LD	b0,0*SIZE(B)
	MOV	t21,t11
	LD	b1,1*SIZE(B)

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, KCO, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 2
#else
	daddiu	TEMP, KK, 2
#endif
	dsra	K,TEMP,2
	MOV	t12,t11
	beqz	K,.L55
	MOV	t22,t11

#else
	move	B,BO
	LD	a0,0*SIZE(A)
	dsra	K,KCO,2						#  K=KCO/2
	LD	a1,1*SIZE(A)

	MTC		$0,t11
	LD	b0,0*SIZE(B)
	MOV	t21,t11
	LD	b1,1*SIZE(B)

	MOV	t12,t11
	beqz	K,.L55
	MOV	t22,t11

#endif

.L51:									#  nr=2 mr=2,kr=4
	MADD	t11,t11,a0,b0
	LD	a4,2*SIZE(A)
	MADD	t21,t21,a1,b0
	LD	b4,2*SIZE(B)

	MADD	t12,t12,a0,b1
	LD	a5,3*SIZE(A)
	MADD	t22,t22,a1,b1
	LD	b5,3*SIZE(B)

	MADD	t11,t11,a4,b4
	LD	a2,4*SIZE(A)
	MADD	t21,t21,a5,b4
	LD	b2,4*SIZE(B)

	MADD	t12,t12,a4,b5
	LD	a3,5*SIZE(A)
	MADD	t22,t22,a5,b5
	daddiu	K,K,-1
	LD	b3,5*SIZE(B)

	MADD	t11,t11,a2,b2
	LD	a6,6*SIZE(A)
	MADD	t21,t21,a3,b2
	daddu	A,A,8*SIZE					#  A+=2(mr)*4(kr)*8Byte=8*SIZE
	LD	b6,6*SIZE(B)

	MADD	t12,t12,a2,b3
	daddu	B,B,8*SIZE					#  B+=2(nr)*4(kr)*8Byte=16*SIZE
	LD	a7,-1*SIZE(A)
	MADD	t22,t22,a3,b3
	LD	b7,-1*SIZE(B)

	MADD	t11,t11,a6,b6
	LD	a0,0*SIZE(A)
	MADD	t21,t21,a7,b6
	LD	b0,0*SIZE(B)

	MADD	t12,t12,a6,b7
	LD	a1,1*SIZE(A)

	MADD	t22,t22,a7,b7
	bnez 	K,.L51
	LD	b1,1*SIZE(B)


.L55:									#  	kr=2
#ifndef TRMMKERNEL
	andi	K,KCO,2
#else
	andi	K,TEMP,2
#endif
	beqz	K,.L58
	nop

.L56:
	MADD	t11,t11,a0,b0
	LD	a4,2*SIZE(A)
	MADD	t21,t21,a1,b0
	daddu	A,A,4*SIZE					#  A+=2(mr)*2(kr)*8Byte=32
	LD	b4,2*SIZE(B)

	MADD	t12,t12,a0,b1
	daddu	B,B,4*SIZE					#	2nr*2kr
	LD	a5,-1*SIZE(A)
	MADD	t22,t22,a1,b1
	LD	b5,-1*SIZE(B)

.L57:
	MADD	t11,t11,a4,b4
	LD	a0,0*SIZE(A)
	MADD	t21,t21,a5,b4
	LD	b0,0*SIZE(B)

	MADD	t12,t12,a4,b5
	LD	a1,1*SIZE(A)
	MADD	t22,t22,a5,b5
	LD	b1,1*SIZE(B)

.L58:									#  kr=1
#ifndef TRMMKERNEL
	andi	K,KCO,1
#else
	andi	K,TEMP, 1
#endif
	beqz	K,.L59
	LD	ALPHA,152($sp)					#  Get ALPHA

	MADD	t11,t11,a0,b0
	MADD	t21,t21,a1,b0
	daddu	A,A,2*SIZE					#  	A+=2(mr)*1(kr)*8Byte=16
	daddu	B,B,2*SIZE					#	2nr*kr

	MADD	t12,t12,a0,b1
	MADD	t22,t22,a1,b1


.L59:									#  Write Back
#ifndef TRMMKERNEL
	LD	c11,0(CO1)						#  write gemm part back Fetch 16 C
	LD	c21,1*SIZE(CO1)
	LD	c12,0(CO2)
	LD	c22,1*SIZE(CO2)

	MADD	t11,c11,t11,ALPHA
	MADD	t21,c21,t21,ALPHA
	MADD	t12,c12,t12,ALPHA
	MADD	t22,c22,t22,ALPHA

	ST	t11,0(CO1)
	ST	t21,1*SIZE(CO1)
	ST	t12,0(CO2)
	ST	t22,1*SIZE(CO2)

	daddu	CO1,CO1,2*SIZE
	daddu	CO2,CO2,2*SIZE

	FETCH	$0,0(CO1)
	FETCH	$0,0(CO2)
#else
	daddiu	M, M, -1
	daddiu	CO1,CO1, 2 * SIZE
	daddiu	CO2,CO2, 2 * SIZE
	MUL	t11, ALPHA, t11
	MUL	t21, ALPHA, t21
	MUL	t12, ALPHA, t12
	MUL	t22, ALPHA, t22

	ST	t11, -2 * SIZE(CO1)
	ST	t21, -1 * SIZE(CO1)
	ST	t12, -2 * SIZE(CO2)
	ST	t22, -1 * SIZE(CO2)

	FETCH	$0,0(CO1)
	FETCH	$0,0(CO2)

#if ( defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP, KCO, KK
#ifdef LEFT
	daddiu	TEMP, TEMP, -2
#else
	daddiu	TEMP, TEMP, -2
#endif

	dsll	K,    TEMP, 1 + BASE_SHIFT
	dsll	TEMP, TEMP, 1 + BASE_SHIFT

	daddu	A, A, K
	daddu	B, B, TEMP
#endif

#ifdef LEFT
	daddiu	KK, KK, 2
#endif
#endif


	.align 3
.L12_M1:
	andi	M,MCO,1					#  	mr = 1
	beqz	M,.L0_N2_Loop
	nop

.L60:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	B,BO					#	Reset B
#else
	dsll	K,    KK, 0 + BASE_SHIFT
	dsll	TEMP, KK, 1 + BASE_SHIFT

	daddu	A, A, K
	daddu	B, BO,  TEMP
#endif
	LD	a0,0*SIZE(A)

	MTC		$0,t11
	MOV	t21,t11
	LD	b0,0*SIZE(B)

	MOV	t12,t11
	LD	b1,1*SIZE(B)

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, KCO, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 1
#else
	daddiu	TEMP, KK, 2
#endif
	dsra	K,TEMP,2
	MOV	t22,t11
	beqz	K,.L65
	nop

#else
	dsra	K,KCO,2
	move	B,BO					#  Reset B
	LD	a0,0*SIZE(A)

	MTC		$0,t11
	MOV	t21,t11
	LD	b0,0*SIZE(B)

	MOV	t12,t11
	LD	b1,1*SIZE(B)
	beqz	K,.L65
	MOV	t22,t11

#endif

.L61:								#	nr=2,mr=1,kr=4
	LD	a4,	1*SIZE(A)				#	a2
	LD	b4, 2*SIZE(B)
	MADD	t11,t11,a0,b0

	LD	b5,3*SIZE(B)
	MADD	t12,t12,a0,b1

	LD	a2,	2*SIZE(A)				#	a3
	LD	b2,4*SIZE(B)
	MADD	t11,t11,a4,b4

	LD	b3,5*SIZE(B)
	MADD	t12,t12,a4,b5

	LD	a6,	3*SIZE(A)				#	a4
	daddiu	K,K,-1
	LD	b6,6*SIZE(B)
	MADD	t11,t11,a2,b2

	LD	b7,7*SIZE(B)
	MADD	t12,t12,a2,b3
	daddu	A,A,4*SIZE				#  A+=1(mr)*4(kr)*8Byte=32

	LD	a0,	0*SIZE(A)
	daddu	B,B,8*SIZE				#  B+=2(nr)*4(kr)*8Byte=8*SIZE

	LD	b0,0*SIZE(B)
	MADD	t11,t11,a6,b6

	LD	b1,1*SIZE(B)
	bnez 	K,.L61
	MADD	t12,t12,a6,b7



.L65:								#  kr=2
#ifndef TRMMKERNEL
	andi	K,KCO,2
#else
	andi	K,TEMP,2
#endif
	beqz	K,.L68
	nop

.L66:
	LD	a4,	1*SIZE(A)				#	a1
	MADD	t11,t11,a0,b0
	LD	b4,2*SIZE(B)
	daddu	A,A,2*SIZE				#  A+=1(mr)*2(kr)*8Byte=16

	LD	b5,3*SIZE(B)
	MADD	t12,t12,a0,b1
	daddu	B,B,4*SIZE

.L67:
	LD	a0,0(A)						#	a0
	LD	b0,0*SIZE(B)
	MADD	t11,t11,a4,b4

	LD	b1,1*SIZE(B)
	MADD	t12,t12,a4,b5


.L68:								#   kr=1
#ifndef TRMMKERNEL
	andi	K,KCO,1
#else
	andi	K,TEMP,1
#endif
	beqz	K,.L69
	LD	ALPHA,152($sp)				#  Get ALPHA

	MADD	t11,t11,a0,b0
	MADD	t12,t12,a0,b1
	daddu	A,A,1*SIZE				#  A+=1(mr)*1(kr)*8Byte=16
	daddu	B,B,2*SIZE


.L69:								#  Write Back
#ifndef TRMMKERNEL
	LD	c11,0(CO1)					#  Fetch 16 C
	LD	c12,0(CO2)

	MADD	t11,c11,t11,ALPHA
	MADD	t12,c12,t12,ALPHA

	ST	t11,0(CO1)
	ST	t12,0(CO2)

	daddu	CO1,CO1,1*SIZE
	daddu	CO2,CO2,1*SIZE

#else
	MUL	t11, ALPHA, t11
	MUL	t12, ALPHA, t12

	ST	t11,  0 * SIZE(CO1)
	ST	t12,  0 * SIZE(CO2)

	daddu	CO1,CO1,1*SIZE
	daddu	CO2,CO2,1*SIZE

#if ( defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP, KCO, KK
#ifdef LEFT
	daddiu	TEMP, TEMP, -1
#else
	daddiu	TEMP, TEMP, -2
#endif

	dsll	K,    TEMP, 0 + BASE_SHIFT
	dsll	TEMP, TEMP, 1 + BASE_SHIFT

	daddu	A, A, K
	daddu	B, B, TEMP
#endif

#ifdef LEFT
	daddiu	KK, KK, 1
#endif
#endif

.L0_N2_Loop:
#if defined(TRMMKERNEL) && !defined(LEFT)
	daddiu	KK, KK, 2
#endif
	move	BO, B


	.align	5
.L0_N1:
	andi	N,NCO,1					#  nr = 1
	beqz	N,.L999
	nop

	move	CO1,C
	dsra	M,MCO,2

	move	A,AO					#  Reset A
	daddu	PREA,AO,SPANA
#if defined(TRMMKERNEL) &&  defined(LEFT)
	move	KK, OFFSET
#endif

	beqz	M,.L11_M2
	daddu	C,CO1,LDC

.L70:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	B, BO					#	Reset B
#else
	dsll	K,    KK, 2 + BASE_SHIFT
	dsll	TEMP, KK, 0 + BASE_SHIFT

	daddu	A, A, K
	daddu	B, BO,  TEMP
#endif
	LD	b0,	0*SIZE(B)

	MTC		$0,t11
	LD	a0,0*SIZE(A)
	MOV	t21,t11
	LD	a1,1*SIZE(A)

	MOV	t31,t11
	LD	a2,2*SIZE(A)
	MOV	t41,t11
	LD	a3,3*SIZE(A)


#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, KCO, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 4
#else
	daddiu	TEMP, KK, 1
#endif
	dsra	K,TEMP,2
	beqz	K,.L75
	nop
#else
	move	B, BO					#	Reset B
	dsra	K,KCO,2
	LD	b0,	0*SIZE(B)

	MTC		$0,t11
	LD	a0,0*SIZE(A)
	MOV	t21,t11
	LD	a1,1*SIZE(A)

	MOV	t31,t11
	LD	a2,2*SIZE(A)
	MOV	t41,t11
	beqz	K,.L75
	LD	a3,3*SIZE(A)

#endif

.L71:								#  nr=1,mr=kr=4
	LD	b4,	1*SIZE(B)				#	b1
	MADD	t11,t11,a0,b0

	LD	a4,	4*SIZE(A)
	MADD	t21,t21,a1,b0

	LD	a5,	5*SIZE(A)
	FETCH		$0,(PREA)

	LD	a6,6*SIZE(A)
	MADD	t31,t31,a2,b0

	LD	a7,7*SIZE(A)
	MADD	t41,t41,a3,b0

.L72:
	LD	b2,	2*SIZE(B)				#	b2
	MADD	t11,t11,a4,b4

	LD	a0,8*SIZE(A)
	MADD	t21,t21,a5,b4

	LD	a1,9*SIZE(A)
	FETCH		$0,4*SIZE(PREA)

	LD	a2,10*SIZE(A)
	MADD	t31,t31,a6,b4

	LD	a3,11*SIZE(A)
	MADD	t41,t41,a7,b4

.L73:
	LD	b6,	3*SIZE(B)
	MADD	t11,t11,a0,b2

	LD	a4,12*SIZE(A)
	daddu	B,B,4*SIZE				#  B+=1(nr)*4(kr)*8Byte=32

	LD	a5,13*SIZE(A)
	MADD	t21,t21,a1,b2

	LD	a6,14*SIZE(A)
	FETCH		$0,8*SIZE(PREA)
	MADD	t31,t31,a2,b2

	LD	a7,15*SIZE(A)
	MADD	t41,t41,a3,b2
	daddu	A,A,16*SIZE				#  A+=4(mr)*4(kr)*8Byte=16*SIZE

.L74:
	LD	b0,	0*SIZE(B)
	MADD	t11,t11,a4,b6

	LD	a0,0*SIZE(A)
	daddu	PREA,PREA,16*SIZE

	LD	a1,1*SIZE(A)
	MADD	t21,t21,a5,b6

	LD	a2,2*SIZE(A)
	daddiu	K,K,-1
	MADD	t31,t31,a6,b6

	LD	a3,3*SIZE(A)
	MADD	t41,t41,a7,b6
	bnez 	K,.L71
	FETCH		$0,-32(PREA)


.L75:								#  kr=2
#ifndef TRMMKERNEL
	andi	K,KCO,2
#else
	andi	K,TEMP,2
#endif
	beqz	K,.L78
	nop

.L76:
	LD	b4,	1*SIZE(B)
	MADD	t11,t11,a0,b0

	LD	a4,4*SIZE(A)
	daddu	B,B,2*SIZE				#  B+=1(nr)*2(kr)*8Byte=32

	LD	a5,5*SIZE(A)
	MADD	t21,t21,a1,b0
	FETCH		$0,0(PREA)

	LD	a6,6*SIZE(A)
	MADD	t31,t31,a2,b0

	LD	a7,7*SIZE(A)
	MADD	t41,t41,a3,b0
	daddu	A,A,8*SIZE				#  A+=4(mr)*2(kr)*8Byte=8*SIZE

.L77:
	LD	b0,0(B)
	MADD	t11,t11,a4,b4

	LD	a0,0*SIZE(A)
	MADD	t21,t21,a5,b4
	FETCH		$0,4*SIZE(PREA)

	LD	a1,1*SIZE(A)
	MADD	t31,t31,a6,b4

	LD	a2,2*SIZE(A)
	MADD	t41,t41,a7,b4

	LD	a3,3*SIZE(A)
	daddu	PREA,PREA,8*SIZE



.L78:								#   kr=1
#ifndef TRMMKERNEL
	andi	K,KCO,1
#else
	andi	K,TEMP,1
#endif
	beqz	K,.L79
	LD	ALPHA,152($sp)				#  Get ALPHA

	FETCH		$0,0(PREA)
	MADD	t11,t11,a0,b0
	MADD	t21,t21,a1,b0
	daddu	A,A,4*SIZE				#  A+=4(mr)*1(kr)*8Byte=32

	MADD	t31,t31,a2,b0
	MADD	t41,t41,a3,b0
	daddu	B,B,1*SIZE
	daddu	PREA,PREA,4*SIZE


.L79:								#  Write Back
#ifndef TRMMKERNEL
	LD	c11,0(CO1)					#  Fetch 16 C
	LD	c21,1*SIZE(CO1)
	LD	c31,2*SIZE(CO1)
	LD	c41,3*SIZE(CO1)

	MADD	t11,c11,t11,ALPHA
	MADD	t21,c21,t21,ALPHA
	MADD	t31,c31,t31,ALPHA
	MADD	t41,c41,t41,ALPHA

	ST	t11,0(CO1)
	ST	t21,1*SIZE(CO1)
	ST	t31,2*SIZE(CO1)
	ST	t41,3*SIZE(CO1)
	daddiu	M,M,-1					#  M--

	FETCH	$0,4*SIZE(CO1)
	FETCH	$0,8*SIZE(CO1)

	bnez	M,.L70					#  M!=0
	daddu	CO1,CO1,4*SIZE			#  COx += 4*8Byte
#else
	daddiu	M,M,-1					#  M--
	MUL	t11, ALPHA, t11
	MUL	t21, ALPHA, t21
	MUL	t31, ALPHA, t31
	MUL	t41, ALPHA, t41

	ST	t11,0(CO1)
	ST	t21,1*SIZE(CO1)
	ST	t31,2*SIZE(CO1)
	ST	t41,3*SIZE(CO1)

	FETCH	$0,4*SIZE(CO1)
	FETCH	$0,8*SIZE(CO1)

	daddu	CO1,CO1,4*SIZE
#if ( defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP, KCO, KK
#ifdef LEFT
	daddiu	TEMP, TEMP, -4
#else
	daddiu	TEMP, TEMP, -1
#endif

	dsll	K,    TEMP, 2 + BASE_SHIFT
	dsll	TEMP, TEMP, 0 + BASE_SHIFT

	daddu	A, A,K
	daddu	B, B, TEMP
#endif

#ifdef LEFT
	daddiu	KK, KK, 4
#endif
	bnez	M,.L70
	nop
#endif


	.align 3
.L11_M2:
	andi	M,MCO,2					#  mr = 2
	beqz	M,.L11_M1
	nop

.L80:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	B, BO
#else
	dsll	K,    KK, 1 + BASE_SHIFT
	dsll	TEMP, KK, 0 + BASE_SHIFT

	daddu	A, A, K
	daddu	B, BO,  TEMP
#endif
	LD	b0,	0*SIZE(B)

	MTC		$0,t11
	MOV		t21,t11
	LD	a0,0*SIZE(A)
	LD	a1,1*SIZE(A)

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, KCO, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 2
#else
	daddiu	TEMP, KK, 1
#endif
	dsra	K,TEMP,2				#  K=KCO/2
	beqz	K,.L85
	nop
#else
	move	B, BO
	dsra	K,KCO,2
	LD	b0,	0*SIZE(B)

	MTC		$0,t11
	MOV		t21,t11
	LD	a0,0*SIZE(A)

	beqz	K,.L85
	LD	a1,1*SIZE(A)

#endif

.L81:								#  nr=1,mr=2,kr=4
	LD	b4,	1*SIZE(B)
	LD	a4,2*SIZE(A)
	MADD	t11,t11,a0,b0
	LD	a5,3*SIZE(A)
	MADD	t21,t21,a1,b0

	LD	b2,	2*SIZE(B)
	LD	a2,4*SIZE(A)
	MADD	t11,t11,a4,b4
	LD	a3,5*SIZE(A)
	MADD	t21,t21,a5,b4

	LD	b6,	3*SIZE(B)
	LD	a6,6*SIZE(A)
	MADD	t11,t11,a2,b2
	LD	a7,7*SIZE(A)
	MADD	t21,t21,a3,b2

	daddu	A,A,8*SIZE				#  A+=2(mr)*4(kr)*8Byte=8*SIZE
	daddu	B,B,4*SIZE				#  B+=1(nr)*4(kr)*8Byte=32

	LD	b0,	0*SIZE(B)
	daddiu	K,K,-1

	LD	a0,0*SIZE(A)
	MADD	t11,t11,a6,b6

	LD	a1,1*SIZE(A)
	bnez 	K,.L81
	MADD	t21,t21,a7,b6

.L85:								#  kr=2
#ifndef TRMMKERNEL
	andi	K,KCO,2
#else
	andi	K,TEMP,2
#endif
	beqz	K,.L88
	nop

.L86:
	LD	b4,	1*SIZE(B)
	LD	a4,2*SIZE(A)
	MADD	t11,t11,a0,b0
	LD	a5,3*SIZE(A)
	MADD	t21,t21,a1,b0

	daddu	A,A,4*SIZE				#  A+=2(mr)*2(kr)*8Byte=32
	daddu	B,B,2*SIZE				#  B+=1(nr)*2(kr)*8Byte=16

	LD	b0,0(B)
	LD	a0,0*SIZE(A)
	MADD	t11,t11,a4,b4
	LD	a1,1*SIZE(A)
	MADD	t21,t21,a5,b4



.L88:								#  kr=1
#ifndef TRMMKERNEL
	andi	K,KCO,1
#else
	andi	K,TEMP,1
#endif
	beqz	K,.L89
	LD	ALPHA,152($sp)				#  Get ALPHA

	MADD	t11,t11,a0,b0
	MADD	t21,t21,a1,b0
	daddu	A,A,2*SIZE				#  A+=2(mr)*1(kr)*8Byte=16
	daddu	B,B,1*SIZE


.L89:								#  Write Back
#ifndef TRMMKERNEL
	LD	c11,0(CO1)					#  Fetch 16 C
	LD	c21,1*SIZE(CO1)

	MADD	t11,c11,t11,ALPHA
	MADD	t21,c21,t21,ALPHA

	ST	t11,0(CO1)
	ST	t21,1*SIZE(CO1)

	FETCH	$0,2*SIZE(CO1)

	daddu	CO1,CO1,2*SIZE			#  COx += 2*8Byte

#else
	daddu	CO1,CO1,2*SIZE			#  COx += 2*8Byte
	MUL	t11, ALPHA, t11
	MUL	t21, ALPHA, t21

	FETCH	$0,0(CO1)
	ST	t11, -2 * SIZE(CO1)
	ST	t21, -1 * SIZE(CO1)
#if ( defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP, KCO, KK
#ifdef LEFT
	daddiu	TEMP, TEMP, -2
#else
	daddiu	TEMP, TEMP, -1
#endif

	dsll	K,    TEMP, 1 + BASE_SHIFT
	dsll	TEMP, TEMP, 0 + BASE_SHIFT

	daddu	A, A, K
	daddu	B, B, TEMP
#endif

#ifdef LEFT
	daddiu	KK, KK, 2
#endif
#endif


	.align 3
.L11_M1:
	andi		M,MCO,1				#   mr = 1
	beqz	M,.L999
	nop

.L90:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	B,  BO
#else
	dsll	K,    KK, 0 + BASE_SHIFT
	dsll	TEMP, KK, 0 + BASE_SHIFT

	daddu	A, A, K
	daddu	B, BO,  TEMP
#endif
	LD	a0,	0*SIZE(A)
	LD	b0,	0*SIZE(B)
	MTC		$0,t11

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, KCO, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 1
#else
	daddiu	TEMP, KK, 1
#endif
	dsra	K,  TEMP, 2
	beqz	K,.L95
	nop

#else
	move	B,  BO
	LD	a0,	0*SIZE(A)
	LD	b0,	0*SIZE(B)
	dsra	K,KCO,2
	beqz	K,.L95
	MTC		$0,t11
#endif

.L91:								#  nr=mr=1,kr=4
	LD	a4,	1*SIZE(A)
	LD	b4,	1*SIZE(B)
	MADD	t11,t11,a0,b0

	LD	a2,	2*SIZE(A)
	LD	b2,	2*SIZE(B)
	MADD	t11,t11,a4,b4

	LD	a6,	3*SIZE(A)
	LD	b6,	3*SIZE(B)
	MADD	t11,t11,a2,b2

	daddu	A,A,4*SIZE				#  A+=1(mr)*4(kr)*8Byte=32
	daddu	B,B,4*SIZE				#  B+=1(nr)*4(kr)*8Byte=32

	LD	a0,	0*SIZE(A)
	LD	b0,	0*SIZE(B)
	MADD	t11,t11,a6,b6

	daddiu	K,K,-1
	bnez 	K,.L91
	nop

.L95:								#  kr=2
#ifndef TRMMKERNEL
	andi	K,KCO,2
#else
	andi	K,TEMP,2
#endif
	beqz	K,.L98
	nop

.L96:
	LD	a4,	1*SIZE(A)
	LD	b4,	1*SIZE(B)
	MADD	t11,t11,a0,b0
	daddu	B,B,2*SIZE				#  B+=1(nr)*2(kr)*8Byte=16
	daddu	A,A,2*SIZE				#  A+=1(mr)*2(kr)*8Byte=32

	LD	b0,0(B)
	LD	a0,0(A)
	MADD	t11,t11,a4,b4

.L98:								#  kr=1
#ifndef TRMMKERNEL
	andi		K,KCO,1
#else
	andi	K,TEMP,1
#endif
	beqz	K,.L99
	LD	ALPHA,152($sp)				#  Get ALPHA

	MADD	t11,t11,a0,b0


.L99:								#  Write Back
#ifndef TRMMKERNEL
	LD	c11,0(CO1)					#  Fetch 16 C
	MADD	t11,c11,t11,ALPHA
	ST	t11,0(CO1)

#else
	MUL	t11, ALPHA, t11

	ST	t11,  0 * SIZE(CO1)
#endif


.L999:							#  End
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
	ld	$23,  96($sp)
	ld	$24, 104($sp)
	ld	$25, 112($sp)
	LD	$f20,120($sp)
	LD	$f21,128($sp)
	LD	$f22,136($sp)
	LD	$f23,144($sp)

	j	$31
	daddiu	$sp, $sp, 160

	EPILOGUE
