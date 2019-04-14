	srawi.		J,	N,	2
	ble		DSTRM_LT_L4_END


DSTRM_LT_L4_BEGIN:

	mr		CO,	C
	mr		AO,	A
	slwi		T1,	LDC	,	2
	add		C,	C,	T1

	mr		KK,	OFFSET
	srawi.		I,	M,	4
	ble		DSTRM_LT_L4x16_END


DSTRM_LT_L4x16_BEGIN:

	mr		BO,	B

	li              L,      -128
	
 	mr              T1,     CO
        add             T2,     T1,     LDC
        add             T3,     T2,     LDC
        add             T4,     T3,     LDC

        and             T1,     T1,     L
        and             T2,     T2,     L
        and             T3,     T3,     L
        and             T4,     T4,     L

        dcbt            T1,     r0
        dcbt            T2,     r0
        dcbt            T3,     r0
        dcbt            T4,     r0

        addi            T1, T1, 128
        addi            T2, T2, 128
        addi            T3, T3, 128
        addi            T4, T4, 128

        dcbt            T1,     r0
        dcbt            T2,     r0
        dcbt            T3,     r0
        dcbt            T4,     r0


DSTRM_LT_L4x16_LOOP_START:


	INIT_16x4


	addic.		L,	KK,	0
	ble-		DSTRM_LT_L4x16_SAVE
	mtctr		L

DSTRM_LT_L4x16_LOOP:

	dcbt		AO,	PRE
	dcbt		BO,	PRE
	KERNEL_16x4
	bdz-		DSTRM_LT_L4x16_SAVE

	dcbt		AO,	PRE
	KERNEL_16x4
	bdz-		DSTRM_LT_L4x16_SAVE

	dcbt		AO,	PRE
	KERNEL_16x4
	bdz-		DSTRM_LT_L4x16_SAVE

	dcbt		AO,	PRE
	KERNEL_16x4
	bdnz+		DSTRM_LT_L4x16_LOOP


DSTRM_LT_L4x16_SAVE:

	SOLVE_LT_16x4

	addi		CO,	CO,	16*SIZE

	sub		T3,	K,	KK
	sub		T4,	K,	KK
	slwi		T3,	T3,	4+BASE_SHIFT
	slwi		T4,	T4,	2+BASE_SHIFT
	add		AO,	AO,	T3
	add		BO,	BO,	T4
	addi		KK,	KK,	16

	addic.		I,	I,	-1
	bgt		DSTRM_LT_L4x16_BEGIN

DSTRM_LT_L4x16_END:


DSTRM_LT_L4x8_BEGIN:

	andi.		T2,	M,	15
	ble		DSTRM_LT_L4x1_END

	andi.		T1,	M,	8
	ble		DSTRM_LT_L4x8_END

	mr		BO,	B


DSTRM_LT_L4x8_LOOP_START:


	INIT_8x4


	addic.		L,	KK,	0
	ble		DSTRM_LT_L4x8_SAVE

DSTRM_LT_L4x8_LOOP:


	KERNEL_8x4

	addic.		L,	L,	-1
	bgt		DSTRM_LT_L4x8_LOOP


DSTRM_LT_L4x8_SAVE:

	SOLVE_LT_8x4

	addi		CO,	CO,	8*SIZE

	sub		T3,	K,	KK
	sub		T4,	K,	KK
	slwi		T3,	T3,	3+BASE_SHIFT
	slwi		T4,	T4,	2+BASE_SHIFT
	add		AO,	AO,	T3
	add		BO,	BO,	T4
	addi		KK,	KK,	8

DSTRM_LT_L4x8_END:


DSTRM_LT_L4x4_BEGIN:

	andi.		T1,	M,	4
	ble		DSTRM_LT_L4x4_END

	mr		BO,	B


DSTRM_LT_L4x4_LOOP_START:


	INIT_4x4


	addic.		L,	KK,	0
	ble		DSTRM_LT_L4x4_SAVE

DSTRM_LT_L4x4_LOOP:


	KERNEL_4x4

	addic.		L,	L,	-1
	bgt		DSTRM_LT_L4x4_LOOP


DSTRM_LT_L4x4_SAVE:

	SOLVE_LT_4x4

	addi		CO,	CO,	4*SIZE

	sub		T3,	K,	KK
	sub		T4,	K,	KK
	slwi		T3,	T3,	2+BASE_SHIFT
	slwi		T4,	T4,	2+BASE_SHIFT
	add		AO,	AO,	T3
	add		BO,	BO,	T4
	addi		KK,	KK,	4

DSTRM_LT_L4x4_END:


DSTRM_LT_L4x2_BEGIN:

	andi.		T1,	M,	2
	ble		DSTRM_LT_L4x2_END

	mr		BO,	B


DSTRM_LT_L4x2_LOOP_START:


	INIT_2x4


	addic.		L,	KK,	0
	ble		DSTRM_LT_L4x2_SAVE

DSTRM_LT_L4x2_LOOP:


	KERNEL_2x4

	addic.		L,	L,	-1
	bgt		DSTRM_LT_L4x2_LOOP


DSTRM_LT_L4x2_SAVE:

	SOLVE_LT_2x4

	addi		CO,	CO,	2*SIZE

	sub		T3,	K,	KK
	sub		T4,	K,	KK
	slwi		T3,	T3,	1+BASE_SHIFT
	slwi		T4,	T4,	2+BASE_SHIFT
	add		AO,	AO,	T3
	add		BO,	BO,	T4
	addi		KK,	KK,	2

DSTRM_LT_L4x2_END:


DSTRM_LT_L4x1_BEGIN:

	andi.		T1,	M,	1
	ble		DSTRM_LT_L4x1_END

	mr		BO,	B


DSTRM_LT_L4x1_LOOP_START:


	INIT_1x4


	addic.		L,	KK,	0
	ble		DSTRM_LT_L4x1_SAVE

DSTRM_LT_L4x1_LOOP:


	KERNEL_1x4

	addic.		L,	L,	-1
	bgt		DSTRM_LT_L4x1_LOOP


DSTRM_LT_L4x1_SAVE:

	SOLVE_LT_1x4

	addi		CO,	CO,	1*SIZE

	sub		T3,	K,	KK
	sub		T4,	K,	KK
	slwi		T3,	T3,	0+BASE_SHIFT
	slwi		T4,	T4,	2+BASE_SHIFT
	add		AO,	AO,	T3
	add		BO,	BO,	T4
	addi		KK,	KK,	1

DSTRM_LT_L4x1_END:

	slwi		T1,	K,	2+BASE_SHIFT
	add		B,	B,	T1

	addic.		J,	J,	-1
	bgt		DSTRM_LT_L4_BEGIN

	andi.		T2,	N,	3
	ble		L999

DSTRM_LT_L4_END:

	b		DSTRM_LT_L2_BEGIN

L999_H1:

	b		L999


DSTRM_LT_L2_BEGIN:

	andi.		T1,	N,	2
	ble		DSTRM_LT_L2_END

	mr		CO,	C
	mr		AO,	A
	slwi		T1,	LDC	,	1
	add		C,	C,	T1

	mr		KK,	OFFSET
	srawi.		I,	M,	4
	ble		DSTRM_LT_L2x16_END


DSTRM_LT_L2x16_BEGIN:

	mr		BO,	B


DSTRM_LT_L2x16_LOOP_START:


	INIT_16x2


	addic.		L,	KK,	0
	ble		DSTRM_LT_L2x16_SAVE

DSTRM_LT_L2x16_LOOP:


	KERNEL_16x2

	addic.		L,	L,	-1
	bgt		DSTRM_LT_L2x16_LOOP


DSTRM_LT_L2x16_SAVE:

	SOLVE_LT_16x2

	addi		CO,	CO,	16*SIZE

	sub		T3,	K,	KK
	sub		T4,	K,	KK
	slwi		T3,	T3,	4+BASE_SHIFT
	slwi		T4,	T4,	1+BASE_SHIFT
	add		AO,	AO,	T3
	add		BO,	BO,	T4
	addi		KK,	KK,	16

	addic.		I,	I,	-1
	bgt		DSTRM_LT_L2x16_BEGIN

DSTRM_LT_L2x16_END:


DSTRM_LT_L2x8_BEGIN:

	andi.		T2,	M,	15
	ble		DSTRM_LT_L2x1_END

	andi.		T1,	M,	8
	ble		DSTRM_LT_L2x8_END

	mr		BO,	B


DSTRM_LT_L2x8_LOOP_START:


	INIT_8x2


	addic.		L,	KK,	0
	ble		DSTRM_LT_L2x8_SAVE

DSTRM_LT_L2x8_LOOP:


	KERNEL_8x2

	addic.		L,	L,	-1
	bgt		DSTRM_LT_L2x8_LOOP


DSTRM_LT_L2x8_SAVE:

	SOLVE_LT_8x2

	addi		CO,	CO,	8*SIZE

	sub		T3,	K,	KK
	sub		T4,	K,	KK
	slwi		T3,	T3,	3+BASE_SHIFT
	slwi		T4,	T4,	1+BASE_SHIFT
	add		AO,	AO,	T3
	add		BO,	BO,	T4
	addi		KK,	KK,	8

DSTRM_LT_L2x8_END:


DSTRM_LT_L2x4_BEGIN:

	andi.		T1,	M,	4
	ble		DSTRM_LT_L2x4_END

	mr		BO,	B


DSTRM_LT_L2x4_LOOP_START:


	INIT_4x2


	addic.		L,	KK,	0
	ble		DSTRM_LT_L2x4_SAVE

DSTRM_LT_L2x4_LOOP:


	KERNEL_4x2

	addic.		L,	L,	-1
	bgt		DSTRM_LT_L2x4_LOOP


DSTRM_LT_L2x4_SAVE:

	SOLVE_LT_4x2

	addi		CO,	CO,	4*SIZE

	sub		T3,	K,	KK
	sub		T4,	K,	KK
	slwi		T3,	T3,	2+BASE_SHIFT
	slwi		T4,	T4,	1+BASE_SHIFT
	add		AO,	AO,	T3
	add		BO,	BO,	T4
	addi		KK,	KK,	4

DSTRM_LT_L2x4_END:


DSTRM_LT_L2x2_BEGIN:

	andi.		T1,	M,	2
	ble		DSTRM_LT_L2x2_END

	mr		BO,	B


DSTRM_LT_L2x2_LOOP_START:


	INIT_2x2


	addic.		L,	KK,	0
	ble		DSTRM_LT_L2x2_SAVE

DSTRM_LT_L2x2_LOOP:


	KERNEL_2x2

	addic.		L,	L,	-1
	bgt		DSTRM_LT_L2x2_LOOP


DSTRM_LT_L2x2_SAVE:

	SOLVE_LT_2x2

	addi		CO,	CO,	2*SIZE

	sub		T3,	K,	KK
	sub		T4,	K,	KK
	slwi		T3,	T3,	1+BASE_SHIFT
	slwi		T4,	T4,	1+BASE_SHIFT
	add		AO,	AO,	T3
	add		BO,	BO,	T4
	addi		KK,	KK,	2

DSTRM_LT_L2x2_END:


DSTRM_LT_L2x1_BEGIN:

	andi.		T1,	M,	1
	ble		DSTRM_LT_L2x1_END

	mr		BO,	B


DSTRM_LT_L2x1_LOOP_START:


	INIT_1x2


	addic.		L,	KK,	0
	ble		DSTRM_LT_L2x1_SAVE

DSTRM_LT_L2x1_LOOP:


	KERNEL_1x2

	addic.		L,	L,	-1
	bgt		DSTRM_LT_L2x1_LOOP


DSTRM_LT_L2x1_SAVE:

	SOLVE_LT_1x2

	addi		CO,	CO,	1*SIZE

	sub		T3,	K,	KK
	sub		T4,	K,	KK
	slwi		T3,	T3,	0+BASE_SHIFT
	slwi		T4,	T4,	1+BASE_SHIFT
	add		AO,	AO,	T3
	add		BO,	BO,	T4
	addi		KK,	KK,	1

DSTRM_LT_L2x1_END:

	slwi		T1,	K,	1+BASE_SHIFT
	add		B,	B,	T1

DSTRM_LT_L2_END:

DSTRM_LT_L1_BEGIN:

	andi.		T1,	N,	1
	ble		DSTRM_LT_L1_END

	mr		CO,	C
	mr		AO,	A

	mr		KK,	OFFSET
	srawi.		I,	M,	4
	ble		DSTRM_LT_L1x16_END


DSTRM_LT_L1x16_BEGIN:

	mr		BO,	B


DSTRM_LT_L1x16_LOOP_START:


	INIT_16x1


	addic.		L,	KK,	0
	ble		DSTRM_LT_L1x16_SAVE

DSTRM_LT_L1x16_LOOP:


	KERNEL_16x1

	addic.		L,	L,	-1
	bgt		DSTRM_LT_L1x16_LOOP


DSTRM_LT_L1x16_SAVE:

	SOLVE_LT_16x1

	addi		CO,	CO,	16*SIZE

	sub		T3,	K,	KK
	sub		T4,	K,	KK
	slwi		T3,	T3,	4+BASE_SHIFT
	slwi		T4,	T4,	0+BASE_SHIFT
	add		AO,	AO,	T3
	add		BO,	BO,	T4
	addi		KK,	KK,	16

	addic.		I,	I,	-1
	bgt		DSTRM_LT_L1x16_BEGIN

DSTRM_LT_L1x16_END:


DSTRM_LT_L1x8_BEGIN:

	andi.		T1,	M,	8
	ble		DSTRM_LT_L1x8_END

	mr		BO,	B


DSTRM_LT_L1x8_LOOP_START:


	INIT_8x1


	addic.		L,	KK,	0
	ble		DSTRM_LT_L1x8_SAVE

DSTRM_LT_L1x8_LOOP:


	KERNEL_8x1

	addic.		L,	L,	-1
	bgt		DSTRM_LT_L1x8_LOOP


DSTRM_LT_L1x8_SAVE:

	SOLVE_LT_8x1

	addi		CO,	CO,	8*SIZE

	sub		T3,	K,	KK
	sub		T4,	K,	KK
	slwi		T3,	T3,	3+BASE_SHIFT
	slwi		T4,	T4,	0+BASE_SHIFT
	add		AO,	AO,	T3
	add		BO,	BO,	T4
	addi		KK,	KK,	8

DSTRM_LT_L1x8_END:


DSTRM_LT_L1x4_BEGIN:

	andi.		T1,	M,	4
	ble		DSTRM_LT_L1x4_END

	mr		BO,	B


DSTRM_LT_L1x4_LOOP_START:


	INIT_4x1


	addic.		L,	KK,	0
	ble		DSTRM_LT_L1x4_SAVE

DSTRM_LT_L1x4_LOOP:


	KERNEL_4x1

	addic.		L,	L,	-1
	bgt		DSTRM_LT_L1x4_LOOP


DSTRM_LT_L1x4_SAVE:

	SOLVE_LT_4x1

	addi		CO,	CO,	4*SIZE

	sub		T3,	K,	KK
	sub		T4,	K,	KK
	slwi		T3,	T3,	2+BASE_SHIFT
	slwi		T4,	T4,	0+BASE_SHIFT
	add		AO,	AO,	T3
	add		BO,	BO,	T4
	addi		KK,	KK,	4

DSTRM_LT_L1x4_END:


DSTRM_LT_L1x2_BEGIN:

	andi.		T1,	M,	2
	ble		DSTRM_LT_L1x2_END

	mr		BO,	B


DSTRM_LT_L1x2_LOOP_START:


	INIT_2x1


	addic.		L,	KK,	0
	ble		DSTRM_LT_L1x2_SAVE

DSTRM_LT_L1x2_LOOP:


	KERNEL_2x1

	addic.		L,	L,	-1
	bgt		DSTRM_LT_L1x2_LOOP


DSTRM_LT_L1x2_SAVE:

	SOLVE_LT_2x1

	addi		CO,	CO,	2*SIZE

	sub		T3,	K,	KK
	sub		T4,	K,	KK
	slwi		T3,	T3,	1+BASE_SHIFT
	slwi		T4,	T4,	0+BASE_SHIFT
	add		AO,	AO,	T3
	add		BO,	BO,	T4
	addi		KK,	KK,	2

DSTRM_LT_L1x2_END:


DSTRM_LT_L1x1_BEGIN:

	andi.		T1,	M,	1
	ble		DSTRM_LT_L1x1_END

	mr		BO,	B


DSTRM_LT_L1x1_LOOP_START:


	INIT_1x1


	addic.		L,	KK,	0
	ble		DSTRM_LT_L1x1_SAVE

DSTRM_LT_L1x1_LOOP:


	KERNEL_1x1

	addic.		L,	L,	-1
	bgt		DSTRM_LT_L1x1_LOOP


DSTRM_LT_L1x1_SAVE:

	SOLVE_LT_1x1

	addi		CO,	CO,	1*SIZE

	sub		T3,	K,	KK
	sub		T4,	K,	KK
	slwi		T3,	T3,	0+BASE_SHIFT
	slwi		T4,	T4,	0+BASE_SHIFT
	add		AO,	AO,	T3
	add		BO,	BO,	T4
	addi		KK,	KK,	1

DSTRM_LT_L1x1_END:

DSTRM_LT_L1_END:
