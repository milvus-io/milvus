
.macro INIT_16x4


	xxlxor		vs0,	vs0,	vs0

	xvmovdp		vs32,	vs0
	xvmovdp		vs33,	vs0
	xvmovdp		vs34,	vs0
	xvmovdp		vs35,	vs0
	xvmovdp		vs36,	vs0
	xvmovdp		vs37,	vs0
	xvmovdp		vs38,	vs0
	xvmovdp		vs39,	vs0
	xvmovdp		vs40,	vs0
	xvmovdp		vs41,	vs0
	xvmovdp		vs42,	vs0
	xvmovdp		vs43,	vs0
	xvmovdp		vs44,	vs0
	xvmovdp		vs45,	vs0
	xvmovdp		vs46,	vs0
	xvmovdp		vs47,	vs0
	xvmovdp		vs48,	vs0
	xvmovdp		vs49,	vs0
	xvmovdp		vs50,	vs0
	xvmovdp		vs51,	vs0
	xvmovdp		vs52,	vs0
	xvmovdp		vs53,	vs0
	xvmovdp		vs54,	vs0
	xvmovdp		vs55,	vs0
	xvmovdp		vs56,	vs0
	xvmovdp		vs57,	vs0
	xvmovdp		vs58,	vs0
	xvmovdp		vs59,	vs0
	xvmovdp		vs60,	vs0
	xvmovdp		vs61,	vs0
	xvmovdp		vs62,	vs0
	xvmovdp		vs63,	vs0

.endm


.macro KERNEL_16x4


	lxvd2x		vs0,	o0,	AO

	lxvdsx		vs16,	o0,	BO
	lxvdsx		vs17,	o8,	BO
	lxvdsx		vs18,	o16,	BO
	lxvdsx		vs19,	o24,	BO

	lxvd2x		vs1,	o16,	AO
	lxvd2x		vs2,	o32,	AO
	lxvd2x		vs3,	o48,	AO

	addi		BO,	BO,	32
	addi		AO,	AO,	64

	lxvd2x		vs4,	o0,	AO
	lxvd2x		vs5,	o16,	AO
	lxvd2x		vs6,	o32,	AO
	lxvd2x		vs7,	o48,	AO

	addi		AO,	AO,	64

	xvmaddadp	vs32,	vs0,	vs16
	xvmaddadp	vs33,	vs0,	vs17
	xvmaddadp	vs34,	vs0,	vs18
	xvmaddadp	vs35,	vs0,	vs19
	xvmaddadp	vs36,	vs1,	vs16
	xvmaddadp	vs37,	vs1,	vs17
	xvmaddadp	vs38,	vs1,	vs18
	xvmaddadp	vs39,	vs1,	vs19
	xvmaddadp	vs40,	vs2,	vs16
	xvmaddadp	vs41,	vs2,	vs17
	xvmaddadp	vs42,	vs2,	vs18
	xvmaddadp	vs43,	vs2,	vs19
	xvmaddadp	vs44,	vs3,	vs16
	xvmaddadp	vs45,	vs3,	vs17
	xvmaddadp	vs46,	vs3,	vs18
	xvmaddadp	vs47,	vs3,	vs19
	xvmaddadp	vs48,	vs4,	vs16
	xvmaddadp	vs49,	vs4,	vs17
	xvmaddadp	vs50,	vs4,	vs18
	xvmaddadp	vs51,	vs4,	vs19
	xvmaddadp	vs52,	vs5,	vs16
	xvmaddadp	vs53,	vs5,	vs17
	xvmaddadp	vs54,	vs5,	vs18
	xvmaddadp	vs55,	vs5,	vs19
	xvmaddadp	vs56,	vs6,	vs16
	xvmaddadp	vs57,	vs6,	vs17
	xvmaddadp	vs58,	vs6,	vs18
	xvmaddadp	vs59,	vs6,	vs19
	xvmaddadp	vs60,	vs7,	vs16
	xvmaddadp	vs61,	vs7,	vs17
	xvmaddadp	vs62,	vs7,	vs18
	xvmaddadp	vs63,	vs7,	vs19


.endm


.macro INIT_8x4


	xxlxor		vs0,	vs0,	vs0

	xvmovdp		vs32,	vs0
	xvmovdp		vs33,	vs0
	xvmovdp		vs34,	vs0
	xvmovdp		vs35,	vs0
	xvmovdp		vs36,	vs0
	xvmovdp		vs37,	vs0
	xvmovdp		vs38,	vs0
	xvmovdp		vs39,	vs0
	xvmovdp		vs40,	vs0
	xvmovdp		vs41,	vs0
	xvmovdp		vs42,	vs0
	xvmovdp		vs43,	vs0
	xvmovdp		vs44,	vs0
	xvmovdp		vs45,	vs0
	xvmovdp		vs46,	vs0
	xvmovdp		vs47,	vs0

.endm


.macro KERNEL_8x4


	lxvd2x		vs0,	o0,	AO
	lxvd2x		vs1,	o16,	AO
	lxvd2x		vs2,	o32,	AO
	lxvd2x		vs3,	o48,	AO

	addi		AO,	AO,	64

	lxvdsx		vs16,	o0,	BO
	lxvdsx		vs17,	o8,	BO
	lxvdsx		vs18,	o16,	BO
	lxvdsx		vs19,	o24,	BO

	addi		BO,	BO,	32

	xvmaddadp	vs32,	vs0,	vs16
	xvmaddadp	vs33,	vs0,	vs17
	xvmaddadp	vs34,	vs0,	vs18
	xvmaddadp	vs35,	vs0,	vs19
	xvmaddadp	vs36,	vs1,	vs16
	xvmaddadp	vs37,	vs1,	vs17
	xvmaddadp	vs38,	vs1,	vs18
	xvmaddadp	vs39,	vs1,	vs19
	xvmaddadp	vs40,	vs2,	vs16
	xvmaddadp	vs41,	vs2,	vs17
	xvmaddadp	vs42,	vs2,	vs18
	xvmaddadp	vs43,	vs2,	vs19
	xvmaddadp	vs44,	vs3,	vs16
	xvmaddadp	vs45,	vs3,	vs17
	xvmaddadp	vs46,	vs3,	vs18
	xvmaddadp	vs47,	vs3,	vs19


.endm


.macro INIT_4x4


	xxlxor		vs0,	vs0,	vs0

	xvmovdp		vs32,	vs0
	xvmovdp		vs33,	vs0
	xvmovdp		vs34,	vs0
	xvmovdp		vs35,	vs0
	xvmovdp		vs36,	vs0
	xvmovdp		vs37,	vs0
	xvmovdp		vs38,	vs0
	xvmovdp		vs39,	vs0

.endm


.macro KERNEL_4x4


	lxvd2x		vs0,	o0,	AO
	lxvd2x		vs1,	o16,	AO

	addi		AO,	AO,	32

	lxvdsx		vs16,	o0,	BO
	lxvdsx		vs17,	o8,	BO
	lxvdsx		vs18,	o16,	BO
	lxvdsx		vs19,	o24,	BO

	addi		BO,	BO,	32

	xvmaddadp	vs32,	vs0,	vs16
	xvmaddadp	vs33,	vs0,	vs17
	xvmaddadp	vs34,	vs0,	vs18
	xvmaddadp	vs35,	vs0,	vs19
	xvmaddadp	vs36,	vs1,	vs16
	xvmaddadp	vs37,	vs1,	vs17
	xvmaddadp	vs38,	vs1,	vs18
	xvmaddadp	vs39,	vs1,	vs19


.endm


.macro INIT_2x4


	xxlxor		vs0,	vs0,	vs0

	xvmovdp		vs32,	vs0
	xvmovdp		vs33,	vs0
	xvmovdp		vs34,	vs0
	xvmovdp		vs35,	vs0

.endm


.macro KERNEL_2x4


	lxvd2x		vs0,	o0,	AO

	addi		AO,	AO,	16

	lxvdsx		vs16,	o0,	BO
	lxvdsx		vs17,	o8,	BO
	lxvdsx		vs18,	o16,	BO
	lxvdsx		vs19,	o24,	BO

	addi		BO,	BO,	32

	xvmaddadp	vs32,	vs0,	vs16
	xvmaddadp	vs33,	vs0,	vs17
	xvmaddadp	vs34,	vs0,	vs18
	xvmaddadp	vs35,	vs0,	vs19


.endm


.macro INIT_1x4


	xxlxor		vs0,	vs0,	vs0

	xvmovdp		vs32,	vs0
	xvmovdp		vs33,	vs0
	xvmovdp		vs34,	vs0
	xvmovdp		vs35,	vs0

.endm


.macro KERNEL_1x4


	lxvdsx		vs0,	o0,	AO

	addi		AO,	AO,	8

	lxvdsx		vs16,	o0,	BO
	lxvdsx		vs17,	o8,	BO
	lxvdsx		vs18,	o16,	BO
	lxvdsx		vs19,	o24,	BO

	addi		BO,	BO,	32

	xvmaddadp	vs32,	vs0,	vs16
	xvmaddadp	vs33,	vs0,	vs17
	xvmaddadp	vs34,	vs0,	vs18
	xvmaddadp	vs35,	vs0,	vs19


.endm


/*##########################################################################################
			 SOLVE_LT 16x4
##########################################################################################*/

.macro SOLVE_LT_16x4

//############### LOAD B #######################

	mr		T1,	BO	
	mr		T4,	BO	

	xxpermdi	vs0,	vs32,	vs33,	0
	xxpermdi	vs1,	vs34,	vs35,	0
	xxpermdi	vs2,	vs32,	vs33,	3
	xxpermdi	vs3,	vs34,	vs35,	3

	lxvd2x		vs32,	o0,	T1
	lxvd2x		vs33,	o16,	T1
	lxvd2x		vs34,	o32,	T1
	lxvd2x		vs35,	o48,	T1

	addi		T1,	T1,	64

	xxpermdi	vs4,	vs36,	vs37,	0
	xxpermdi	vs5,	vs38,	vs39,	0
	xxpermdi	vs6,	vs36,	vs37,	3
	xxpermdi	vs7,	vs38,	vs39,	3

	lxvd2x		vs36,	o0,	T1
	lxvd2x		vs37,	o16,	T1
	lxvd2x		vs38,	o32,	T1
	lxvd2x		vs39,	o48,	T1

	addi		T1,	T1,	64

	xxpermdi	vs8,	vs40,	vs41,	0
	xxpermdi	vs9,	vs42,	vs43,	0
	xxpermdi	vs10,	vs40,	vs41,	3
	xxpermdi	vs11,	vs42,	vs43,	3

	lxvd2x		vs40,	o0,	T1
	lxvd2x		vs41,	o16,	T1
	lxvd2x		vs42,	o32,	T1
	lxvd2x		vs43,	o48,	T1

	addi		T1,	T1,	64

	xxpermdi	vs12,	vs44,	vs45,	0
	xxpermdi	vs13,	vs46,	vs47,	0
	xxpermdi	vs14,	vs44,	vs45,	3
	xxpermdi	vs15,	vs46,	vs47,	3

	lxvd2x		vs44,	o0,	T1
	lxvd2x		vs45,	o16,	T1
	lxvd2x		vs46,	o32,	T1
	lxvd2x		vs47,	o48,	T1

	addi		T1,	T1,	64

	xxpermdi	vs16,	vs48,	vs49,	0
	xxpermdi	vs17,	vs50,	vs51,	0
	xxpermdi	vs18,	vs48,	vs49,	3
	xxpermdi	vs19,	vs50,	vs51,	3

	lxvd2x		vs48,	o0,	T1
	lxvd2x		vs49,	o16,	T1
	lxvd2x		vs50,	o32,	T1
	lxvd2x		vs51,	o48,	T1

	addi		T1,	T1,	64

	xxpermdi	vs20,	vs52,	vs53,	0
	xxpermdi	vs21,	vs54,	vs55,	0
	xxpermdi	vs22,	vs52,	vs53,	3
	xxpermdi	vs23,	vs54,	vs55,	3

	lxvd2x		vs52,	o0,	T1
	lxvd2x		vs53,	o16,	T1
	lxvd2x		vs54,	o32,	T1
	lxvd2x		vs55,	o48,	T1

	addi		T1,	T1,	64

	xxpermdi	vs24,	vs56,	vs57,	0
	xxpermdi	vs25,	vs58,	vs59,	0
	xxpermdi	vs26,	vs56,	vs57,	3
	xxpermdi	vs27,	vs58,	vs59,	3

	lxvd2x		vs56,	o0,	T1
	lxvd2x		vs57,	o16,	T1
	lxvd2x		vs58,	o32,	T1
	lxvd2x		vs59,	o48,	T1

	addi		T1,	T1,	64

	xxpermdi	vs28,	vs60,	vs61,	0
	xxpermdi	vs29,	vs62,	vs63,	0
	xxpermdi	vs30,	vs60,	vs61,	3
	xxpermdi	vs31,	vs62,	vs63,	3



	lxvd2x		vs60,	o0,	T1
	lxvd2x		vs61,	o16,	T1
	lxvd2x		vs62,	o32,	T1
	lxvd2x		vs63,	o48,	T1

//############### OFFSET 0 #######################

	dcbt		AO,	PRE
	mr		T1,	AO	

	xvsubdp		vs32,	vs32,	vs0
	xvsubdp		vs33,	vs33,	vs1
	xvsubdp		vs34,	vs34,	vs2
	xvsubdp		vs35,	vs35,	vs3

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	xvsubdp		vs36,	vs36,	vs4
	xvsubdp		vs37,	vs37,	vs5
	xvsubdp		vs38,	vs38,	vs6
	xvsubdp		vs39,	vs39,	vs7

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1
	lxvdsx		vs6,	o16,	T1
	lxvdsx		vs7,	o24,	T1

	addi		T1,	T1,	32

	xvsubdp		vs40,	vs40,	vs8
	xvsubdp		vs41,	vs41,	vs9
	xvsubdp		vs42,	vs42,	vs10
	xvsubdp		vs43,	vs43,	vs11

	lxvdsx		vs8,	o0,	T1
	lxvdsx		vs9,	o8,	T1
	lxvdsx		vs10,	o16,	T1
	lxvdsx		vs11,	o24,	T1

	addi		T1,	T1,	32

	xvsubdp		vs44,	vs44,	vs12
	xvsubdp		vs45,	vs45,	vs13
	xvsubdp		vs46,	vs46,	vs14
	xvsubdp		vs47,	vs47,	vs15

	lxvdsx		vs12,	o0,	T1
	lxvdsx		vs13,	o8,	T1
	lxvdsx		vs14,	o16,	T1
	lxvdsx		vs15,	o24,	T1

	addi		T1,	T1,	32

	xvsubdp		vs48,	vs48,	vs16
	xvsubdp		vs49,	vs49,	vs17
	xvsubdp		vs50,	vs50,	vs18
	xvsubdp		vs51,	vs51,	vs19

	xvsubdp		vs52,	vs52,	vs20
	xvsubdp		vs53,	vs53,	vs21
	xvsubdp		vs54,	vs54,	vs22
	xvsubdp		vs55,	vs55,	vs23

	xvsubdp		vs56,	vs56,	vs24
	xvsubdp		vs57,	vs57,	vs25
	xvsubdp		vs58,	vs58,	vs26
	xvsubdp		vs59,	vs59,	vs27

	xvsubdp		vs60,	vs60,	vs28
	xvsubdp		vs61,	vs61,	vs29
	xvsubdp		vs62,	vs62,	vs30
	xvsubdp		vs63,	vs63,	vs31

//############### OFFSET 1 #######################

	addi		T1,	T1,	1*SIZE

	xvmuldp		vs32,	vs32,	vs0
	xvmuldp		vs33,	vs33,	vs0

	xvnmsubadp	vs34,	vs32,	vs1
	xvnmsubadp	vs35,	vs33,	vs1
	xvnmsubadp	vs36,	vs32,	vs2
	dcbt		T1,	PRE
	xvnmsubadp	vs37,	vs33,	vs2
	xvnmsubadp	vs38,	vs32,	vs3
	xvnmsubadp	vs39,	vs33,	vs3

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	xvnmsubadp	vs40,	vs32,	vs4
	xvnmsubadp	vs41,	vs33,	vs4
	xvnmsubadp	vs42,	vs32,	vs5
	xvnmsubadp	vs43,	vs33,	vs5
	xvnmsubadp	vs44,	vs32,	vs6
	xvnmsubadp	vs45,	vs33,	vs6
	xvnmsubadp	vs46,	vs32,	vs7
	xvnmsubadp	vs47,	vs33,	vs7

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1
	lxvdsx		vs6,	o16,	T1
	lxvdsx		vs7,	o24,	T1

	addi		T1,	T1,	32

	xvnmsubadp	vs48,	vs32,	vs8
	xvnmsubadp	vs49,	vs33,	vs8
	xvnmsubadp	vs50,	vs32,	vs9
	xvnmsubadp	vs51,	vs33,	vs9
	xvnmsubadp	vs52,	vs32,	vs10
	xvnmsubadp	vs53,	vs33,	vs10
	xvnmsubadp	vs54,	vs32,	vs11
	xvnmsubadp	vs55,	vs33,	vs11

	lxvdsx		vs8,	o0,	T1
	lxvdsx		vs9,	o8,	T1
	lxvdsx		vs10,	o16,	T1
	lxvdsx		vs11,	o24,	T1

	addi		T1,	T1,	32

	xvnmsubadp	vs56,	vs32,	vs12
	xvnmsubadp	vs57,	vs33,	vs12
	xvnmsubadp	vs58,	vs32,	vs13
	xvnmsubadp	vs59,	vs33,	vs13
	xvnmsubadp	vs60,	vs32,	vs14
	xvnmsubadp	vs61,	vs33,	vs14
	xvnmsubadp	vs62,	vs32,	vs15
	xvnmsubadp	vs63,	vs33,	vs15


	lxvdsx		vs12,	o0,	T1
	lxvdsx		vs13,	o8,	T1
	lxvdsx		vs14,	o16,	T1

	addi		T1,	T1,	24

//############### OFFSET 2 #######################

	xvmuldp		vs34,	vs34,	vs0
	xvmuldp		vs35,	vs35,	vs0

	addi		T1,	T1,	2*SIZE

	xvnmsubadp	vs36,	vs34,	vs1
	xvnmsubadp	vs37,	vs35,	vs1
	xvnmsubadp	vs38,	vs34,	vs2
	dcbt		T1,	PRE
	xvnmsubadp	vs39,	vs35,	vs2
	xvnmsubadp	vs40,	vs34,	vs3
	xvnmsubadp	vs41,	vs35,	vs3

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	xvnmsubadp	vs42,	vs34,	vs4
	xvnmsubadp	vs43,	vs35,	vs4
	xvnmsubadp	vs44,	vs34,	vs5
	xvnmsubadp	vs45,	vs35,	vs5
	xvnmsubadp	vs46,	vs34,	vs6
	xvnmsubadp	vs47,	vs35,	vs6
	xvnmsubadp	vs48,	vs34,	vs7
	xvnmsubadp	vs49,	vs35,	vs7

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1
	lxvdsx		vs6,	o16,	T1
	lxvdsx		vs7,	o24,	T1

	addi		T1,	T1,	32

	xvnmsubadp	vs50,	vs34,	vs8
	xvnmsubadp	vs51,	vs35,	vs8
	xvnmsubadp	vs52,	vs34,	vs9
	xvnmsubadp	vs53,	vs35,	vs9
	xvnmsubadp	vs54,	vs34,	vs10
	xvnmsubadp	vs55,	vs35,	vs10
	xvnmsubadp	vs56,	vs34,	vs11
	xvnmsubadp	vs57,	vs35,	vs11

	lxvdsx		vs8,	o0,	T1
	lxvdsx		vs9,	o8,	T1
	lxvdsx		vs10,	o16,	T1
	lxvdsx		vs11,	o24,	T1

	addi		T1,	T1,	32


	xvnmsubadp	vs58,	vs34,	vs12
	xvnmsubadp	vs59,	vs35,	vs12
	xvnmsubadp	vs60,	vs34,	vs13
	xvnmsubadp	vs61,	vs35,	vs13
	xvnmsubadp	vs62,	vs34,	vs14
	xvnmsubadp	vs63,	vs35,	vs14

	lxvdsx		vs12,	o0,	T1
	lxvdsx		vs13,	o8,	T1

	addi		T1,	T1,	16

//############### OFFSET 3 #######################
	xvmuldp		vs36,	vs36,	vs0
	xvmuldp		vs37,	vs37,	vs0

	addi		T1,	T1,	3*SIZE

	xvnmsubadp	vs38,	vs36,	vs1
	xvnmsubadp	vs39,	vs37,	vs1
	xvnmsubadp	vs40,	vs36,	vs2
	dcbt		T1,	PRE
	xvnmsubadp	vs41,	vs37,	vs2
	xvnmsubadp	vs42,	vs36,	vs3
	xvnmsubadp	vs43,	vs37,	vs3

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	xvnmsubadp	vs44,	vs36,	vs4
	xvnmsubadp	vs45,	vs37,	vs4
	xvnmsubadp	vs46,	vs36,	vs5
	xvnmsubadp	vs47,	vs37,	vs5
	xvnmsubadp	vs48,	vs36,	vs6
	xvnmsubadp	vs49,	vs37,	vs6
	xvnmsubadp	vs50,	vs36,	vs7
	xvnmsubadp	vs51,	vs37,	vs7

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1
	lxvdsx		vs6,	o16,	T1
	lxvdsx		vs7,	o24,	T1

	addi		T1,	T1,	32

	xvnmsubadp	vs52,	vs36,	vs8
	xvnmsubadp	vs53,	vs37,	vs8
	xvnmsubadp	vs54,	vs36,	vs9
	xvnmsubadp	vs55,	vs37,	vs9
	xvnmsubadp	vs56,	vs36,	vs10
	xvnmsubadp	vs57,	vs37,	vs10
	xvnmsubadp	vs58,	vs36,	vs11
	xvnmsubadp	vs59,	vs37,	vs11

	lxvdsx		vs8,	o0,	T1
	lxvdsx		vs9,	o8,	T1
	lxvdsx		vs10,	o16,	T1
	lxvdsx		vs11,	o24,	T1

	addi		T1,	T1,	32

	xvnmsubadp	vs60,	vs36,	vs12
	xvnmsubadp	vs61,	vs37,	vs12
	xvnmsubadp	vs62,	vs36,	vs13
	xvnmsubadp	vs63,	vs37,	vs13

	lxvdsx		vs12,	o0,	T1

	stxvd2x		vs32,	o0,	T4
	stxvd2x		vs33,	o16,	T4
	stxvd2x		vs34,	o32,	T4
	stxvd2x		vs35,	o48,	T4

	addi		T4,	T4,	64

	addi		T1,	T1,	8

//############### OFFSET 4 #######################
	xvmuldp		vs38,	vs38,	vs0
	xvmuldp		vs39,	vs39,	vs0

	addi		T1,	T1,	4*SIZE

	xvnmsubadp	vs40,	vs38,	vs1
	xvnmsubadp	vs41,	vs39,	vs1
	xvnmsubadp	vs42,	vs38,	vs2
	dcbt		T1,	PRE
	xvnmsubadp	vs43,	vs39,	vs2
	xvnmsubadp	vs44,	vs38,	vs3
	xvnmsubadp	vs45,	vs39,	vs3

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	xvnmsubadp	vs46,	vs38,	vs4
	xvnmsubadp	vs47,	vs39,	vs4
	xvnmsubadp	vs48,	vs38,	vs5
	xvnmsubadp	vs49,	vs39,	vs5
	xvnmsubadp	vs50,	vs38,	vs6
	xvnmsubadp	vs51,	vs39,	vs6
	xvnmsubadp	vs52,	vs38,	vs7
	xvnmsubadp	vs53,	vs39,	vs7

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1
	lxvdsx		vs6,	o16,	T1
	lxvdsx		vs7,	o24,	T1

	addi		T1,	T1,	32


	xvnmsubadp	vs54,	vs38,	vs8
	xvnmsubadp	vs55,	vs39,	vs8
	xvnmsubadp	vs56,	vs38,	vs9
	xvnmsubadp	vs57,	vs39,	vs9
	xvnmsubadp	vs58,	vs38,	vs10
	xvnmsubadp	vs59,	vs39,	vs10
	xvnmsubadp	vs60,	vs38,	vs11
	xvnmsubadp	vs61,	vs39,	vs11

	lxvdsx		vs8,	o0,	T1
	lxvdsx		vs9,	o8,	T1
	lxvdsx		vs10,	o16,	T1
	lxvdsx		vs11,	o24,	T1

	addi		T1,	T1,	32

	xvnmsubadp	vs62,	vs38,	vs12
	xvnmsubadp	vs63,	vs39,	vs12


//############### OFFSET 5 #######################
	xvmuldp		vs40,	vs40,	vs0
	xvmuldp		vs41,	vs41,	vs0

	addi		T1,	T1,	5*SIZE

	xvnmsubadp	vs42,	vs40,	vs1
	xvnmsubadp	vs43,	vs41,	vs1
	xvnmsubadp	vs44,	vs40,	vs2
	dcbt		T1,	PRE
	xvnmsubadp	vs45,	vs41,	vs2
	xvnmsubadp	vs46,	vs40,	vs3
	xvnmsubadp	vs47,	vs41,	vs3

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	xvnmsubadp	vs48,	vs40,	vs4
	xvnmsubadp	vs49,	vs41,	vs4
	xvnmsubadp	vs50,	vs40,	vs5
	xvnmsubadp	vs51,	vs41,	vs5
	xvnmsubadp	vs52,	vs40,	vs6
	xvnmsubadp	vs53,	vs41,	vs6
	xvnmsubadp	vs54,	vs40,	vs7
	xvnmsubadp	vs55,	vs41,	vs7

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1
	lxvdsx		vs6,	o16,	T1
	lxvdsx		vs7,	o24,	T1

	addi		T1,	T1,	32

	xvnmsubadp	vs56,	vs40,	vs8
	xvnmsubadp	vs57,	vs41,	vs8
	xvnmsubadp	vs58,	vs40,	vs9
	xvnmsubadp	vs59,	vs41,	vs9
	xvnmsubadp	vs60,	vs40,	vs10
	xvnmsubadp	vs61,	vs41,	vs10
	xvnmsubadp	vs62,	vs40,	vs11
	xvnmsubadp	vs63,	vs41,	vs11


	lxvdsx		vs8,	o0,	T1
	lxvdsx		vs9,	o8,	T1
	lxvdsx		vs10,	o16,	T1

	addi		T1,	T1,	24

//############### OFFSET 6 #######################
	xvmuldp		vs42,	vs42,	vs0
	xvmuldp		vs43,	vs43,	vs0

	addi		T1,	T1,	6*SIZE

	xvnmsubadp	vs44,	vs42,	vs1
	xvnmsubadp	vs45,	vs43,	vs1
	xvnmsubadp	vs46,	vs42,	vs2
	dcbt		T1,	PRE
	xvnmsubadp	vs47,	vs43,	vs2
	xvnmsubadp	vs48,	vs42,	vs3
	xvnmsubadp	vs49,	vs43,	vs3

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	xvnmsubadp	vs50,	vs42,	vs4
	xvnmsubadp	vs51,	vs43,	vs4
	xvnmsubadp	vs52,	vs42,	vs5
	xvnmsubadp	vs53,	vs43,	vs5
	xvnmsubadp	vs54,	vs42,	vs6
	xvnmsubadp	vs55,	vs43,	vs6
	xvnmsubadp	vs56,	vs42,	vs7
	xvnmsubadp	vs57,	vs43,	vs7

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1
	lxvdsx		vs6,	o16,	T1
	lxvdsx		vs7,	o24,	T1

	addi		T1,	T1,	32

	xvnmsubadp	vs58,	vs42,	vs8
	xvnmsubadp	vs59,	vs43,	vs8
	xvnmsubadp	vs60,	vs42,	vs9
	xvnmsubadp	vs61,	vs43,	vs9
	xvnmsubadp	vs62,	vs42,	vs10
	xvnmsubadp	vs63,	vs43,	vs10

	lxvdsx		vs8,	o0,	T1
	lxvdsx		vs9,	o8,	T1

	addi		T1,	T1,	16

	stxvd2x		vs36,	o0,	T4
	stxvd2x		vs37,	o16,	T4
	stxvd2x		vs38,	o32,	T4
	stxvd2x		vs39,	o48,	T4

	addi		T4,	T4,	64

//############### OFFSET 7 #######################
	xvmuldp		vs44,	vs44,	vs0
	xvmuldp		vs45,	vs45,	vs0

	addi		T1,	T1,	7*SIZE

	xvnmsubadp	vs46,	vs44,	vs1
	xvnmsubadp	vs47,	vs45,	vs1
	xvnmsubadp	vs48,	vs44,	vs2
	dcbt		T1,	PRE
	xvnmsubadp	vs49,	vs45,	vs2
	xvnmsubadp	vs50,	vs44,	vs3
	xvnmsubadp	vs51,	vs45,	vs3

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	xvnmsubadp	vs52,	vs44,	vs4
	xvnmsubadp	vs53,	vs45,	vs4
	xvnmsubadp	vs54,	vs44,	vs5
	xvnmsubadp	vs55,	vs45,	vs5
	xvnmsubadp	vs56,	vs44,	vs6
	xvnmsubadp	vs57,	vs45,	vs6
	xvnmsubadp	vs58,	vs44,	vs7
	xvnmsubadp	vs59,	vs45,	vs7

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1
	lxvdsx		vs6,	o16,	T1
	lxvdsx		vs7,	o24,	T1

	addi		T1,	T1,	32

	xvnmsubadp	vs60,	vs44,	vs8
	xvnmsubadp	vs61,	vs45,	vs8
	xvnmsubadp	vs62,	vs44,	vs9
	xvnmsubadp	vs63,	vs45,	vs9

	lxvdsx		vs8,	o0,	T1

	addi		T1,	T1,	8

//############### OFFSET 8 #######################
	xvmuldp		vs46,	vs46,	vs0
	xvmuldp		vs47,	vs47,	vs0

	addi		T1,	T1,	8*SIZE

	xvnmsubadp	vs48,	vs46,	vs1
	xvnmsubadp	vs49,	vs47,	vs1
	xvnmsubadp	vs50,	vs46,	vs2
	dcbt		T1,	PRE
	xvnmsubadp	vs51,	vs47,	vs2
	xvnmsubadp	vs52,	vs46,	vs3
	xvnmsubadp	vs53,	vs47,	vs3

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	xvnmsubadp	vs54,	vs46,	vs4
	xvnmsubadp	vs55,	vs47,	vs4
	xvnmsubadp	vs56,	vs46,	vs5
	xvnmsubadp	vs57,	vs47,	vs5
	xvnmsubadp	vs58,	vs46,	vs6
	xvnmsubadp	vs59,	vs47,	vs6
	xvnmsubadp	vs60,	vs46,	vs7
	xvnmsubadp	vs61,	vs47,	vs7

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1
	lxvdsx		vs6,	o16,	T1
	lxvdsx		vs7,	o24,	T1

	addi		T1,	T1,	32

	stxvd2x		vs40,	o0,	T4
	stxvd2x		vs41,	o16,	T4
	stxvd2x		vs42,	o32,	T4
	stxvd2x		vs43,	o48,	T4

	addi		T4,	T4,	64

	xvnmsubadp	vs62,	vs46,	vs8
	xvnmsubadp	vs63,	vs47,	vs8


//############### OFFSET 9 #######################
	xvmuldp		vs48,	vs48,	vs0
	xvmuldp		vs49,	vs49,	vs0

	addi		T1,	T1,	9*SIZE

	xvnmsubadp	vs50,	vs48,	vs1
	xvnmsubadp	vs51,	vs49,	vs1
	xvnmsubadp	vs52,	vs48,	vs2
	dcbt		T1,	PRE
	xvnmsubadp	vs53,	vs49,	vs2
	xvnmsubadp	vs54,	vs48,	vs3
	xvnmsubadp	vs55,	vs49,	vs3

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	xvnmsubadp	vs56,	vs48,	vs4
	xvnmsubadp	vs57,	vs49,	vs4
	xvnmsubadp	vs58,	vs48,	vs5
	xvnmsubadp	vs59,	vs49,	vs5
	xvnmsubadp	vs60,	vs48,	vs6
	xvnmsubadp	vs61,	vs49,	vs6
	xvnmsubadp	vs62,	vs48,	vs7
	xvnmsubadp	vs63,	vs49,	vs7

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1
	lxvdsx		vs6,	o16,	T1

	addi		T1,	T1,	24

//############### OFFSET 10 #######################
	xvmuldp		vs50,	vs50,	vs0
	xvmuldp		vs51,	vs51,	vs0

	addi		T1,	T1,	10*SIZE

	xvnmsubadp	vs52,	vs50,	vs1
	xvnmsubadp	vs53,	vs51,	vs1
	xvnmsubadp	vs54,	vs50,	vs2
	dcbt		T1,	PRE
	xvnmsubadp	vs55,	vs51,	vs2
	xvnmsubadp	vs56,	vs50,	vs3
	xvnmsubadp	vs57,	vs51,	vs3

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	xvnmsubadp	vs58,	vs50,	vs4
	xvnmsubadp	vs59,	vs51,	vs4
	xvnmsubadp	vs60,	vs50,	vs5
	xvnmsubadp	vs61,	vs51,	vs5
	xvnmsubadp	vs62,	vs50,	vs6
	xvnmsubadp	vs63,	vs51,	vs6

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1

	addi		T1,	T1,	16

	stxvd2x		vs44,	o0,	T4
	stxvd2x		vs45,	o16,	T4
	stxvd2x		vs46,	o32,	T4
	stxvd2x		vs47,	o48,	T4

	addi		T4,	T4,	64

//############### OFFSET 11 #######################
	xvmuldp		vs52,	vs52,	vs0
	xvmuldp		vs53,	vs53,	vs0

	addi		T1,	T1,	11*SIZE

	xvnmsubadp	vs54,	vs52,	vs1
	xvnmsubadp	vs55,	vs53,	vs1
	xvnmsubadp	vs56,	vs52,	vs2
	dcbt		T1,	PRE
	xvnmsubadp	vs57,	vs53,	vs2
	xvnmsubadp	vs58,	vs52,	vs3
	xvnmsubadp	vs59,	vs53,	vs3

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	xvnmsubadp	vs60,	vs52,	vs4
	xvnmsubadp	vs61,	vs53,	vs4
	xvnmsubadp	vs62,	vs52,	vs5
	xvnmsubadp	vs63,	vs53,	vs5

	lxvdsx		vs4,	o0,	T1

	addi		T1,	T1,	8

//############### OFFSET 12 #######################
	xvmuldp		vs54,	vs54,	vs0
	xvmuldp		vs55,	vs55,	vs0

	addi		T1,	T1,	12*SIZE

	xvnmsubadp	vs56,	vs54,	vs1
	xvnmsubadp	vs57,	vs55,	vs1
	xvnmsubadp	vs58,	vs54,	vs2
	dcbt		T1,	PRE
	xvnmsubadp	vs59,	vs55,	vs2
	xvnmsubadp	vs60,	vs54,	vs3
	xvnmsubadp	vs61,	vs55,	vs3

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	stxvd2x		vs48,	o0,	T4
	stxvd2x		vs49,	o16,	T4
	stxvd2x		vs50,	o32,	T4
	stxvd2x		vs51,	o48,	T4

	addi		T4,	T4,	64

	xvnmsubadp	vs62,	vs54,	vs4
	xvnmsubadp	vs63,	vs55,	vs4


//############### OFFSET 13 #######################
	xvmuldp		vs56,	vs56,	vs0
	xvmuldp		vs57,	vs57,	vs0

	addi		T1,	T1,	13*SIZE

	xvnmsubadp	vs58,	vs56,	vs1
	xvnmsubadp	vs59,	vs57,	vs1
	xvnmsubadp	vs60,	vs56,	vs2
	xvnmsubadp	vs61,	vs57,	vs2
	xvnmsubadp	vs62,	vs56,	vs3
	xvnmsubadp	vs63,	vs57,	vs3

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1

	addi		T1,	T1,	24

//############### OFFSET 14 #######################
	xvmuldp		vs58,	vs58,	vs0
	xvmuldp		vs59,	vs59,	vs0

	addi		T1,	T1,	14*SIZE

	xvnmsubadp	vs60,	vs58,	vs1
	xvnmsubadp	vs61,	vs59,	vs1
	xvnmsubadp	vs62,	vs58,	vs2
	xvnmsubadp	vs63,	vs59,	vs2


	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1

	addi		T1,	T1,	16

	stxvd2x		vs52,	o0,	T4
	stxvd2x		vs53,	o16,	T4
	stxvd2x		vs54,	o32,	T4
	stxvd2x		vs55,	o48,	T4

	addi		T4,	T4,	64
//############### OFFSET 15 #######################
	xvmuldp		vs60,	vs60,	vs0
	xvmuldp		vs61,	vs61,	vs0

	addi		T1,	T1,	15*SIZE

	xvnmsubadp	vs62,	vs60,	vs1
	xvnmsubadp	vs63,	vs61,	vs1

	lxvdsx		vs0,	o0,	T1

	addi		T1,	T1,	8

	xvmuldp		vs62,	vs62,	vs0
	xvmuldp		vs63,	vs63,	vs0


//############### SAVE B #######################



	stxvd2x		vs56,	o0,	T4
	stxvd2x		vs57,	o16,	T4
	stxvd2x		vs58,	o32,	T4
	stxvd2x		vs59,	o48,	T4

	addi		T4,	T4,	64

	stxvd2x		vs60,	o0,	T4
	stxvd2x		vs61,	o16,	T4
	stxvd2x		vs62,	o32,	T4
	stxvd2x		vs63,	o48,	T4

//############### SAVE C #######################


	mr		T1,	CO
	add		T2,	CO,	LDC


	stxsdx		vs32,	o0,	T1
	xxswapd		vs32,	vs32
	stxsdx		vs34,	o8,	T1
	xxswapd		vs34,	vs34
	stxsdx		vs36,	o16,	T1
	xxswapd		vs36,	vs36
	stxsdx		vs38,	o24,	T1
	xxswapd		vs38,	vs38

	addi		T1,	T1,	32

	stxsdx		vs40,	o0,	T1
	xxswapd		vs40,	vs40
	stxsdx		vs42,	o8,	T1
	xxswapd		vs42,	vs42
	stxsdx		vs44,	o16,	T1
	xxswapd		vs44,	vs44
	stxsdx		vs46,	o24,	T1
	xxswapd		vs46,	vs46

	addi		T1,	T1,	32

	stxsdx		vs48,	o0,	T1
	xxswapd		vs48,	vs48
	stxsdx		vs50,	o8,	T1
	xxswapd		vs50,	vs50
	stxsdx		vs52,	o16,	T1
	xxswapd		vs52,	vs52
	stxsdx		vs54,	o24,	T1
	xxswapd		vs54,	vs54

	addi		T1,	T1,	32

	stxsdx		vs56,	o0,	T1
	xxswapd		vs56,	vs56
	stxsdx		vs58,	o8,	T1
	xxswapd		vs58,	vs58
	stxsdx		vs60,	o16,	T1
	xxswapd		vs60,	vs60
	stxsdx		vs62,	o24,	T1
	xxswapd		vs62,	vs62

	stxsdx		vs32,	o0,	T2
	stxsdx		vs34,	o8,	T2
	stxsdx		vs36,	o16,	T2
	stxsdx		vs38,	o24,	T2

	addi		T2,	T2,	32

	stxsdx		vs40,	o0,	T2
	stxsdx		vs42,	o8,	T2
	stxsdx		vs44,	o16,	T2
	stxsdx		vs46,	o24,	T2

	addi		T2,	T2,	32

	stxsdx		vs48,	o0,	T2
	stxsdx		vs50,	o8,	T2
	stxsdx		vs52,	o16,	T2
	stxsdx		vs54,	o24,	T2

	addi		T2,	T2,	32

	stxsdx		vs56,	o0,	T2
	stxsdx		vs58,	o8,	T2
	stxsdx		vs60,	o16,	T2
	stxsdx		vs62,	o24,	T2

	mr		T1,	CO
	add		T2,	CO,	LDC


	add		T1,	T2,	LDC
	add		T2,	T1,	LDC


	stxsdx		vs33,	o0,	T1
	xxswapd		vs33,	vs33
	stxsdx		vs35,	o8,	T1
	xxswapd		vs35,	vs35
	stxsdx		vs37,	o16,	T1
	xxswapd		vs37,	vs37
	stxsdx		vs39,	o24,	T1
	xxswapd		vs39,	vs39

	addi		T1,	T1,	32

	stxsdx		vs41,	o0,	T1
	xxswapd		vs41,	vs41
	stxsdx		vs43,	o8,	T1
	xxswapd		vs43,	vs43
	stxsdx		vs45,	o16,	T1
	xxswapd		vs45,	vs45
	stxsdx		vs47,	o24,	T1
	xxswapd		vs47,	vs47

	addi		T1,	T1,	32

	stxsdx		vs49,	o0,	T1
	xxswapd		vs49,	vs49
	stxsdx		vs51,	o8,	T1
	xxswapd		vs51,	vs51
	stxsdx		vs53,	o16,	T1
	xxswapd		vs53,	vs53
	stxsdx		vs55,	o24,	T1
	xxswapd		vs55,	vs55

	addi		T1,	T1,	32

	stxsdx		vs57,	o0,	T1
	xxswapd		vs57,	vs57
	stxsdx		vs59,	o8,	T1
	xxswapd		vs59,	vs59
	stxsdx		vs61,	o16,	T1
	xxswapd		vs61,	vs61
	stxsdx		vs63,	o24,	T1
	xxswapd		vs63,	vs63

	stxsdx		vs33,	o0,	T2
	stxsdx		vs35,	o8,	T2
	stxsdx		vs37,	o16,	T2
	stxsdx		vs39,	o24,	T2

	addi		T2,	T2,	32

	stxsdx		vs41,	o0,	T2
	stxsdx		vs43,	o8,	T2
	stxsdx		vs45,	o16,	T2
	stxsdx		vs47,	o24,	T2

	addi		T2,	T2,	32

	stxsdx		vs49,	o0,	T2
	stxsdx		vs51,	o8,	T2
	stxsdx		vs53,	o16,	T2
	stxsdx		vs55,	o24,	T2

	addi		T2,	T2,	32

	stxsdx		vs57,	o0,	T2
	stxsdx		vs59,	o8,	T2
	stxsdx		vs61,	o16,	T2
	stxsdx		vs63,	o24,	T2

.endm


/*##########################################################################################
			 SOLVE_LT 8x4
##########################################################################################*/

.macro SOLVE_LT_8x4

	xxpermdi	vs0,	vs32,	vs33,	0
	xxpermdi	vs1,	vs34,	vs35,	0
	xxpermdi	vs2,	vs32,	vs33,	3
	xxpermdi	vs3,	vs34,	vs35,	3

	xxpermdi	vs4,	vs36,	vs37,	0
	xxpermdi	vs5,	vs38,	vs39,	0
	xxpermdi	vs6,	vs36,	vs37,	3
	xxpermdi	vs7,	vs38,	vs39,	3

	xxpermdi	vs8,	vs40,	vs41,	0
	xxpermdi	vs9,	vs42,	vs43,	0
	xxpermdi	vs10,	vs40,	vs41,	3
	xxpermdi	vs11,	vs42,	vs43,	3

	xxpermdi	vs12,	vs44,	vs45,	0
	xxpermdi	vs13,	vs46,	vs47,	0
	xxpermdi	vs14,	vs44,	vs45,	3
	xxpermdi	vs15,	vs46,	vs47,	3


//############### LOAD B #######################


	mr		T1,	BO	

	lxvd2x		vs32,	o0,	T1
	lxvd2x		vs33,	o16,	T1
	lxvd2x		vs34,	o32,	T1
	lxvd2x		vs35,	o48,	T1

	addi		T1,	T1,	64

	lxvd2x		vs36,	o0,	T1
	lxvd2x		vs37,	o16,	T1
	lxvd2x		vs38,	o32,	T1
	lxvd2x		vs39,	o48,	T1

	addi		T1,	T1,	64

	lxvd2x		vs40,	o0,	T1
	lxvd2x		vs41,	o16,	T1
	lxvd2x		vs42,	o32,	T1
	lxvd2x		vs43,	o48,	T1

	addi		T1,	T1,	64

	lxvd2x		vs44,	o0,	T1
	lxvd2x		vs45,	o16,	T1
	lxvd2x		vs46,	o32,	T1
	lxvd2x		vs47,	o48,	T1

	xvsubdp		vs32,	vs32,	vs0
	xvsubdp		vs33,	vs33,	vs1
	xvsubdp		vs34,	vs34,	vs2
	xvsubdp		vs35,	vs35,	vs3
	xvsubdp		vs36,	vs36,	vs4
	xvsubdp		vs37,	vs37,	vs5
	xvsubdp		vs38,	vs38,	vs6
	xvsubdp		vs39,	vs39,	vs7
	xvsubdp		vs40,	vs40,	vs8
	xvsubdp		vs41,	vs41,	vs9
	xvsubdp		vs42,	vs42,	vs10
	xvsubdp		vs43,	vs43,	vs11
	xvsubdp		vs44,	vs44,	vs12
	xvsubdp		vs45,	vs45,	vs13
	xvsubdp		vs46,	vs46,	vs14
	xvsubdp		vs47,	vs47,	vs15

	mr		T1,	AO	


//############### OFFSET 0 #######################

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1
	lxvdsx		vs6,	o16,	T1
	lxvdsx		vs7,	o24,	T1

	addi		T1,	T1,	32

	xvmuldp		vs32,	vs32,	vs0
	xvmuldp		vs33,	vs33,	vs0

	xvnmsubadp	vs34,	vs32,	vs1
	xvnmsubadp	vs35,	vs33,	vs1
	xvnmsubadp	vs36,	vs32,	vs2
	xvnmsubadp	vs37,	vs33,	vs2
	xvnmsubadp	vs38,	vs32,	vs3
	xvnmsubadp	vs39,	vs33,	vs3
	xvnmsubadp	vs40,	vs32,	vs4
	xvnmsubadp	vs41,	vs33,	vs4
	xvnmsubadp	vs42,	vs32,	vs5
	xvnmsubadp	vs43,	vs33,	vs5
	xvnmsubadp	vs44,	vs32,	vs6
	xvnmsubadp	vs45,	vs33,	vs6
	xvnmsubadp	vs46,	vs32,	vs7
	xvnmsubadp	vs47,	vs33,	vs7

//############### OFFSET 1 #######################

	addi		T1,	T1,	1*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1
	lxvdsx		vs6,	o16,	T1

	addi		T1,	T1,	24

	xvmuldp		vs34,	vs34,	vs0
	xvmuldp		vs35,	vs35,	vs0

	xvnmsubadp	vs36,	vs34,	vs1
	xvnmsubadp	vs37,	vs35,	vs1
	xvnmsubadp	vs38,	vs34,	vs2
	xvnmsubadp	vs39,	vs35,	vs2
	xvnmsubadp	vs40,	vs34,	vs3
	xvnmsubadp	vs41,	vs35,	vs3
	xvnmsubadp	vs42,	vs34,	vs4
	xvnmsubadp	vs43,	vs35,	vs4
	xvnmsubadp	vs44,	vs34,	vs5
	xvnmsubadp	vs45,	vs35,	vs5
	xvnmsubadp	vs46,	vs34,	vs6
	xvnmsubadp	vs47,	vs35,	vs6

//############### OFFSET 2 #######################

	addi		T1,	T1,	2*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1

	addi		T1,	T1,	16

	xvmuldp		vs36,	vs36,	vs0
	xvmuldp		vs37,	vs37,	vs0

	xvnmsubadp	vs38,	vs36,	vs1
	xvnmsubadp	vs39,	vs37,	vs1
	xvnmsubadp	vs40,	vs36,	vs2
	xvnmsubadp	vs41,	vs37,	vs2
	xvnmsubadp	vs42,	vs36,	vs3
	xvnmsubadp	vs43,	vs37,	vs3
	xvnmsubadp	vs44,	vs36,	vs4
	xvnmsubadp	vs45,	vs37,	vs4
	xvnmsubadp	vs46,	vs36,	vs5
	xvnmsubadp	vs47,	vs37,	vs5

//############### OFFSET 3 #######################

	addi		T1,	T1,	3*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs4,	o0,	T1

	addi		T1,	T1,	8

	xvmuldp		vs38,	vs38,	vs0
	xvmuldp		vs39,	vs39,	vs0

	xvnmsubadp	vs40,	vs38,	vs1
	xvnmsubadp	vs41,	vs39,	vs1
	xvnmsubadp	vs42,	vs38,	vs2
	xvnmsubadp	vs43,	vs39,	vs2
	xvnmsubadp	vs44,	vs38,	vs3
	xvnmsubadp	vs45,	vs39,	vs3
	xvnmsubadp	vs46,	vs38,	vs4
	xvnmsubadp	vs47,	vs39,	vs4

//############### OFFSET 4 #######################

	addi		T1,	T1,	4*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	xvmuldp		vs40,	vs40,	vs0
	xvmuldp		vs41,	vs41,	vs0

	xvnmsubadp	vs42,	vs40,	vs1
	xvnmsubadp	vs43,	vs41,	vs1
	xvnmsubadp	vs44,	vs40,	vs2
	xvnmsubadp	vs45,	vs41,	vs2
	xvnmsubadp	vs46,	vs40,	vs3
	xvnmsubadp	vs47,	vs41,	vs3

//############### OFFSET 5 #######################

	addi		T1,	T1,	5*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1

	addi		T1,	T1,	24

	xvmuldp		vs42,	vs42,	vs0
	xvmuldp		vs43,	vs43,	vs0

	xvnmsubadp	vs44,	vs42,	vs1
	xvnmsubadp	vs45,	vs43,	vs1
	xvnmsubadp	vs46,	vs42,	vs2
	xvnmsubadp	vs47,	vs43,	vs2

//############### OFFSET 6 #######################

	addi		T1,	T1,	6*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1

	addi		T1,	T1,	16

	xvmuldp		vs44,	vs44,	vs0
	xvmuldp		vs45,	vs45,	vs0

	xvnmsubadp	vs46,	vs44,	vs1
	xvnmsubadp	vs47,	vs45,	vs1

//############### OFFSET 7 #######################

	addi		T1,	T1,	7*SIZE

	lxvdsx		vs0,	o0,	T1

	addi		T1,	T1,	8

	xvmuldp		vs46,	vs46,	vs0
	xvmuldp		vs47,	vs47,	vs0


//############### SAVE B #######################


	mr		T1,	BO	


	stxvd2x		vs32,	o0,	T1
	stxvd2x		vs33,	o16,	T1
	stxvd2x		vs34,	o32,	T1
	stxvd2x		vs35,	o48,	T1

	addi		T1,	T1,	64

	stxvd2x		vs36,	o0,	T1
	stxvd2x		vs37,	o16,	T1
	stxvd2x		vs38,	o32,	T1
	stxvd2x		vs39,	o48,	T1

	addi		T1,	T1,	64

	stxvd2x		vs40,	o0,	T1
	stxvd2x		vs41,	o16,	T1
	stxvd2x		vs42,	o32,	T1
	stxvd2x		vs43,	o48,	T1

	addi		T1,	T1,	64

	stxvd2x		vs44,	o0,	T1
	stxvd2x		vs45,	o16,	T1
	stxvd2x		vs46,	o32,	T1
	stxvd2x		vs47,	o48,	T1

//############### SAVE C #######################


	mr		T1,	CO
	add		T2,	CO,	LDC


	stxsdx		vs32,	o0,	T1
	xxswapd		vs32,	vs32
	stxsdx		vs34,	o8,	T1
	xxswapd		vs34,	vs34
	stxsdx		vs36,	o16,	T1
	xxswapd		vs36,	vs36
	stxsdx		vs38,	o24,	T1
	xxswapd		vs38,	vs38

	addi		T1,	T1,	32

	stxsdx		vs40,	o0,	T1
	xxswapd		vs40,	vs40
	stxsdx		vs42,	o8,	T1
	xxswapd		vs42,	vs42
	stxsdx		vs44,	o16,	T1
	xxswapd		vs44,	vs44
	stxsdx		vs46,	o24,	T1
	xxswapd		vs46,	vs46

	stxsdx		vs32,	o0,	T2
	stxsdx		vs34,	o8,	T2
	stxsdx		vs36,	o16,	T2
	stxsdx		vs38,	o24,	T2

	addi		T2,	T2,	32

	stxsdx		vs40,	o0,	T2
	stxsdx		vs42,	o8,	T2
	stxsdx		vs44,	o16,	T2
	stxsdx		vs46,	o24,	T2

	mr		T1,	CO
	add		T2,	CO,	LDC


	add		T1,	T2,	LDC
	add		T2,	T1,	LDC


	stxsdx		vs33,	o0,	T1
	xxswapd		vs33,	vs33
	stxsdx		vs35,	o8,	T1
	xxswapd		vs35,	vs35
	stxsdx		vs37,	o16,	T1
	xxswapd		vs37,	vs37
	stxsdx		vs39,	o24,	T1
	xxswapd		vs39,	vs39

	addi		T1,	T1,	32

	stxsdx		vs41,	o0,	T1
	xxswapd		vs41,	vs41
	stxsdx		vs43,	o8,	T1
	xxswapd		vs43,	vs43
	stxsdx		vs45,	o16,	T1
	xxswapd		vs45,	vs45
	stxsdx		vs47,	o24,	T1
	xxswapd		vs47,	vs47

	stxsdx		vs33,	o0,	T2
	stxsdx		vs35,	o8,	T2
	stxsdx		vs37,	o16,	T2
	stxsdx		vs39,	o24,	T2

	addi		T2,	T2,	32

	stxsdx		vs41,	o0,	T2
	stxsdx		vs43,	o8,	T2
	stxsdx		vs45,	o16,	T2
	stxsdx		vs47,	o24,	T2

.endm


/*##########################################################################################
			 SOLVE_LT 4x4
##########################################################################################*/

.macro SOLVE_LT_4x4

	xxpermdi	vs0,	vs32,	vs33,	0
	xxpermdi	vs1,	vs34,	vs35,	0
	xxpermdi	vs2,	vs32,	vs33,	3
	xxpermdi	vs3,	vs34,	vs35,	3

	xxpermdi	vs4,	vs36,	vs37,	0
	xxpermdi	vs5,	vs38,	vs39,	0
	xxpermdi	vs6,	vs36,	vs37,	3
	xxpermdi	vs7,	vs38,	vs39,	3


//############### LOAD B #######################


	mr		T1,	BO	

	lxvd2x		vs32,	o0,	T1
	lxvd2x		vs33,	o16,	T1
	lxvd2x		vs34,	o32,	T1
	lxvd2x		vs35,	o48,	T1

	addi		T1,	T1,	64

	lxvd2x		vs36,	o0,	T1
	lxvd2x		vs37,	o16,	T1
	lxvd2x		vs38,	o32,	T1
	lxvd2x		vs39,	o48,	T1

	xvsubdp		vs32,	vs32,	vs0
	xvsubdp		vs33,	vs33,	vs1
	xvsubdp		vs34,	vs34,	vs2
	xvsubdp		vs35,	vs35,	vs3
	xvsubdp		vs36,	vs36,	vs4
	xvsubdp		vs37,	vs37,	vs5
	xvsubdp		vs38,	vs38,	vs6
	xvsubdp		vs39,	vs39,	vs7

	mr		T1,	AO	


//############### OFFSET 0 #######################

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	xvmuldp		vs32,	vs32,	vs0
	xvmuldp		vs33,	vs33,	vs0

	xvnmsubadp	vs34,	vs32,	vs1
	xvnmsubadp	vs35,	vs33,	vs1
	xvnmsubadp	vs36,	vs32,	vs2
	xvnmsubadp	vs37,	vs33,	vs2
	xvnmsubadp	vs38,	vs32,	vs3
	xvnmsubadp	vs39,	vs33,	vs3

//############### OFFSET 1 #######################

	addi		T1,	T1,	1*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1

	addi		T1,	T1,	24

	xvmuldp		vs34,	vs34,	vs0
	xvmuldp		vs35,	vs35,	vs0

	xvnmsubadp	vs36,	vs34,	vs1
	xvnmsubadp	vs37,	vs35,	vs1
	xvnmsubadp	vs38,	vs34,	vs2
	xvnmsubadp	vs39,	vs35,	vs2

//############### OFFSET 2 #######################

	addi		T1,	T1,	2*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1

	addi		T1,	T1,	16

	xvmuldp		vs36,	vs36,	vs0
	xvmuldp		vs37,	vs37,	vs0

	xvnmsubadp	vs38,	vs36,	vs1
	xvnmsubadp	vs39,	vs37,	vs1

//############### OFFSET 3 #######################

	addi		T1,	T1,	3*SIZE

	lxvdsx		vs0,	o0,	T1

	addi		T1,	T1,	8

	xvmuldp		vs38,	vs38,	vs0
	xvmuldp		vs39,	vs39,	vs0


//############### SAVE B #######################


	mr		T1,	BO	


	stxvd2x		vs32,	o0,	T1
	stxvd2x		vs33,	o16,	T1
	stxvd2x		vs34,	o32,	T1
	stxvd2x		vs35,	o48,	T1

	addi		T1,	T1,	64

	stxvd2x		vs36,	o0,	T1
	stxvd2x		vs37,	o16,	T1
	stxvd2x		vs38,	o32,	T1
	stxvd2x		vs39,	o48,	T1

//############### SAVE C #######################


	mr		T1,	CO
	add		T2,	CO,	LDC


	stxsdx		vs32,	o0,	T1
	xxswapd		vs32,	vs32
	stxsdx		vs34,	o8,	T1
	xxswapd		vs34,	vs34
	stxsdx		vs36,	o16,	T1
	xxswapd		vs36,	vs36
	stxsdx		vs38,	o24,	T1
	xxswapd		vs38,	vs38

	stxsdx		vs32,	o0,	T2
	stxsdx		vs34,	o8,	T2
	stxsdx		vs36,	o16,	T2
	stxsdx		vs38,	o24,	T2

	mr		T1,	CO
	add		T2,	CO,	LDC


	add		T1,	T2,	LDC
	add		T2,	T1,	LDC


	stxsdx		vs33,	o0,	T1
	xxswapd		vs33,	vs33
	stxsdx		vs35,	o8,	T1
	xxswapd		vs35,	vs35
	stxsdx		vs37,	o16,	T1
	xxswapd		vs37,	vs37
	stxsdx		vs39,	o24,	T1
	xxswapd		vs39,	vs39

	stxsdx		vs33,	o0,	T2
	stxsdx		vs35,	o8,	T2
	stxsdx		vs37,	o16,	T2
	stxsdx		vs39,	o24,	T2

.endm


/*##########################################################################################
			 SOLVE_LT 2x4
##########################################################################################*/

.macro SOLVE_LT_2x4

	xxpermdi	vs0,	vs32,	vs33,	0
	xxpermdi	vs1,	vs34,	vs35,	0
	xxpermdi	vs2,	vs32,	vs33,	3
	xxpermdi	vs3,	vs34,	vs35,	3


//############### LOAD B #######################


	mr		T1,	BO	

	lxvd2x		vs32,	o0,	T1
	lxvd2x		vs33,	o16,	T1
	lxvd2x		vs34,	o32,	T1
	lxvd2x		vs35,	o48,	T1

	xvsubdp		vs32,	vs32,	vs0
	xvsubdp		vs33,	vs33,	vs1
	xvsubdp		vs34,	vs34,	vs2
	xvsubdp		vs35,	vs35,	vs3

	mr		T1,	AO	


//############### OFFSET 0 #######################

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1

	addi		T1,	T1,	16

	xvmuldp		vs32,	vs32,	vs0
	xvmuldp		vs33,	vs33,	vs0

	xvnmsubadp	vs34,	vs32,	vs1
	xvnmsubadp	vs35,	vs33,	vs1

//############### OFFSET 1 #######################

	addi		T1,	T1,	1*SIZE

	lxvdsx		vs0,	o0,	T1

	addi		T1,	T1,	8

	xvmuldp		vs34,	vs34,	vs0
	xvmuldp		vs35,	vs35,	vs0


//############### SAVE B #######################


	mr		T1,	BO	


	stxvd2x		vs32,	o0,	T1
	stxvd2x		vs33,	o16,	T1
	stxvd2x		vs34,	o32,	T1
	stxvd2x		vs35,	o48,	T1

//############### SAVE C #######################


	mr		T1,	CO
	add		T2,	CO,	LDC


	stxsdx		vs32,	o0,	T1
	xxswapd		vs32,	vs32
	stxsdx		vs34,	o8,	T1
	xxswapd		vs34,	vs34

	stxsdx		vs32,	o0,	T2
	stxsdx		vs34,	o8,	T2

	mr		T1,	CO
	add		T2,	CO,	LDC


	add		T1,	T2,	LDC
	add		T2,	T1,	LDC


	stxsdx		vs33,	o0,	T1
	xxswapd		vs33,	vs33
	stxsdx		vs35,	o8,	T1
	xxswapd		vs35,	vs35

	stxsdx		vs33,	o0,	T2
	stxsdx		vs35,	o8,	T2

.endm


/*##########################################################################################
			 SOLVE_LT 1x4
##########################################################################################*/

.macro SOLVE_LT_1x4

	xxpermdi	vs0,	vs32,	vs33,	0
	xxpermdi	vs1,	vs34,	vs35,	0

//############### LOAD B #######################


	mr		T1,	BO	

	lxvd2x		vs32,	o0,	T1
	lxvd2x		vs33,	o16,	T1

	xvsubdp		vs32,	vs32,	vs0
	xvsubdp		vs33,	vs33,	vs1

	mr		T1,	AO	


//############### OFFSET 0 #######################

	lxvdsx		vs0,	o0,	T1

	addi		T1,	T1,	8

	xvmuldp		vs32,	vs32,	vs0
	xvmuldp		vs33,	vs33,	vs0


//############### SAVE B #######################


	mr		T1,	BO	


	stxvd2x		vs32,	o0,	T1
	stxvd2x		vs33,	o16,	T1

//############### SAVE C #######################


	mr		T1,	CO
	add		T2,	CO,	LDC


	stxsdx		vs32,	o0,	T1
	xxswapd		vs32,	vs32

	stxsdx		vs32,	o0,	T2

	mr		T1,	CO
	add		T2,	CO,	LDC


	add		T1,	T2,	LDC
	add		T2,	T1,	LDC


	stxsdx		vs33,	o0,	T1
	xxswapd		vs33,	vs33

	stxsdx		vs33,	o0,	T2

.endm


.macro INIT_16x2


	xxlxor		vs0,	vs0,	vs0

	xvmovdp		vs32,	vs0
	xvmovdp		vs33,	vs0
	xvmovdp		vs34,	vs0
	xvmovdp		vs35,	vs0
	xvmovdp		vs36,	vs0
	xvmovdp		vs37,	vs0
	xvmovdp		vs38,	vs0
	xvmovdp		vs39,	vs0
	xvmovdp		vs40,	vs0
	xvmovdp		vs41,	vs0
	xvmovdp		vs42,	vs0
	xvmovdp		vs43,	vs0
	xvmovdp		vs44,	vs0
	xvmovdp		vs45,	vs0
	xvmovdp		vs46,	vs0
	xvmovdp		vs47,	vs0

.endm


.macro KERNEL_16x2


	lxvd2x		vs0,	o0,	AO
	lxvd2x		vs1,	o16,	AO
	lxvd2x		vs2,	o32,	AO
	lxvd2x		vs3,	o48,	AO

	addi		AO,	AO,	64

	lxvd2x		vs4,	o0,	AO
	lxvd2x		vs5,	o16,	AO
	lxvd2x		vs6,	o32,	AO
	lxvd2x		vs7,	o48,	AO

	addi		AO,	AO,	64

	lxvdsx		vs16,	o0,	BO
	lxvdsx		vs17,	o8,	BO

	addi		BO,	BO,	16

	xvmaddadp	vs32,	vs0,	vs16
	xvmaddadp	vs33,	vs0,	vs17
	xvmaddadp	vs34,	vs1,	vs16
	xvmaddadp	vs35,	vs1,	vs17
	xvmaddadp	vs36,	vs2,	vs16
	xvmaddadp	vs37,	vs2,	vs17
	xvmaddadp	vs38,	vs3,	vs16
	xvmaddadp	vs39,	vs3,	vs17
	xvmaddadp	vs40,	vs4,	vs16
	xvmaddadp	vs41,	vs4,	vs17
	xvmaddadp	vs42,	vs5,	vs16
	xvmaddadp	vs43,	vs5,	vs17
	xvmaddadp	vs44,	vs6,	vs16
	xvmaddadp	vs45,	vs6,	vs17
	xvmaddadp	vs46,	vs7,	vs16
	xvmaddadp	vs47,	vs7,	vs17


.endm


.macro INIT_8x2


	xxlxor		vs0,	vs0,	vs0

	xvmovdp		vs32,	vs0
	xvmovdp		vs33,	vs0
	xvmovdp		vs34,	vs0
	xvmovdp		vs35,	vs0
	xvmovdp		vs36,	vs0
	xvmovdp		vs37,	vs0
	xvmovdp		vs38,	vs0
	xvmovdp		vs39,	vs0

.endm


.macro KERNEL_8x2


	lxvd2x		vs0,	o0,	AO
	lxvd2x		vs1,	o16,	AO
	lxvd2x		vs2,	o32,	AO
	lxvd2x		vs3,	o48,	AO

	addi		AO,	AO,	64

	lxvdsx		vs16,	o0,	BO
	lxvdsx		vs17,	o8,	BO

	addi		BO,	BO,	16

	xvmaddadp	vs32,	vs0,	vs16
	xvmaddadp	vs33,	vs0,	vs17
	xvmaddadp	vs34,	vs1,	vs16
	xvmaddadp	vs35,	vs1,	vs17
	xvmaddadp	vs36,	vs2,	vs16
	xvmaddadp	vs37,	vs2,	vs17
	xvmaddadp	vs38,	vs3,	vs16
	xvmaddadp	vs39,	vs3,	vs17


.endm


.macro INIT_4x2


	xxlxor		vs0,	vs0,	vs0

	xvmovdp		vs32,	vs0
	xvmovdp		vs33,	vs0
	xvmovdp		vs34,	vs0
	xvmovdp		vs35,	vs0

.endm


.macro KERNEL_4x2


	lxvd2x		vs0,	o0,	AO
	lxvd2x		vs1,	o16,	AO

	addi		AO,	AO,	32

	lxvdsx		vs16,	o0,	BO
	lxvdsx		vs17,	o8,	BO

	addi		BO,	BO,	16

	xvmaddadp	vs32,	vs0,	vs16
	xvmaddadp	vs33,	vs0,	vs17
	xvmaddadp	vs34,	vs1,	vs16
	xvmaddadp	vs35,	vs1,	vs17


.endm


.macro INIT_2x2


	xxlxor		vs0,	vs0,	vs0

	xvmovdp		vs32,	vs0
	xvmovdp		vs33,	vs0

.endm


.macro KERNEL_2x2


	lxvd2x		vs0,	o0,	AO

	addi		AO,	AO,	16

	lxvdsx		vs16,	o0,	BO
	lxvdsx		vs17,	o8,	BO

	addi		BO,	BO,	16

	xvmaddadp	vs32,	vs0,	vs16
	xvmaddadp	vs33,	vs0,	vs17


.endm


.macro INIT_1x2


	xxlxor		vs0,	vs0,	vs0

	xvmovdp		vs32,	vs0
	xvmovdp		vs33,	vs0

.endm


.macro KERNEL_1x2


	lxvdsx		vs0,	o0,	AO

	addi		AO,	AO,	8

	lxvdsx		vs16,	o0,	BO
	lxvdsx		vs17,	o8,	BO

	addi		BO,	BO,	16

	xvmaddadp	vs32,	vs0,	vs16
	xvmaddadp	vs33,	vs0,	vs17


.endm


/*##########################################################################################
			 SOLVE_LT 16x2
##########################################################################################*/

.macro SOLVE_LT_16x2

	xxpermdi	vs0,	vs32,	vs33,	0
	xxpermdi	vs1,	vs32,	vs33,	3

	xxpermdi	vs2,	vs34,	vs35,	0
	xxpermdi	vs3,	vs34,	vs35,	3

	xxpermdi	vs4,	vs36,	vs37,	0
	xxpermdi	vs5,	vs36,	vs37,	3

	xxpermdi	vs6,	vs38,	vs39,	0
	xxpermdi	vs7,	vs38,	vs39,	3

	xxpermdi	vs8,	vs40,	vs41,	0
	xxpermdi	vs9,	vs40,	vs41,	3

	xxpermdi	vs10,	vs42,	vs43,	0
	xxpermdi	vs11,	vs42,	vs43,	3

	xxpermdi	vs12,	vs44,	vs45,	0
	xxpermdi	vs13,	vs44,	vs45,	3

	xxpermdi	vs14,	vs46,	vs47,	0
	xxpermdi	vs15,	vs46,	vs47,	3


//############### LOAD B #######################


	mr		T1,	BO	

	lxvd2x		vs32,	o0,	T1
	lxvd2x		vs33,	o16,	T1
	lxvd2x		vs34,	o32,	T1
	lxvd2x		vs35,	o48,	T1

	addi		T1,	T1,	64

	lxvd2x		vs36,	o0,	T1
	lxvd2x		vs37,	o16,	T1
	lxvd2x		vs38,	o32,	T1
	lxvd2x		vs39,	o48,	T1

	addi		T1,	T1,	64

	lxvd2x		vs40,	o0,	T1
	lxvd2x		vs41,	o16,	T1
	lxvd2x		vs42,	o32,	T1
	lxvd2x		vs43,	o48,	T1

	addi		T1,	T1,	64

	lxvd2x		vs44,	o0,	T1
	lxvd2x		vs45,	o16,	T1
	lxvd2x		vs46,	o32,	T1
	lxvd2x		vs47,	o48,	T1

	xvsubdp		vs32,	vs32,	vs0
	xvsubdp		vs33,	vs33,	vs1
	xvsubdp		vs34,	vs34,	vs2
	xvsubdp		vs35,	vs35,	vs3
	xvsubdp		vs36,	vs36,	vs4
	xvsubdp		vs37,	vs37,	vs5
	xvsubdp		vs38,	vs38,	vs6
	xvsubdp		vs39,	vs39,	vs7
	xvsubdp		vs40,	vs40,	vs8
	xvsubdp		vs41,	vs41,	vs9
	xvsubdp		vs42,	vs42,	vs10
	xvsubdp		vs43,	vs43,	vs11
	xvsubdp		vs44,	vs44,	vs12
	xvsubdp		vs45,	vs45,	vs13
	xvsubdp		vs46,	vs46,	vs14
	xvsubdp		vs47,	vs47,	vs15

	mr		T1,	AO	


//############### OFFSET 0 #######################

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1
	lxvdsx		vs6,	o16,	T1
	lxvdsx		vs7,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs8,	o0,	T1
	lxvdsx		vs9,	o8,	T1
	lxvdsx		vs10,	o16,	T1
	lxvdsx		vs11,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs12,	o0,	T1
	lxvdsx		vs13,	o8,	T1
	lxvdsx		vs14,	o16,	T1
	lxvdsx		vs15,	o24,	T1

	addi		T1,	T1,	32

	xvmuldp		vs32,	vs32,	vs0
	xvnmsubadp	vs33,	vs32,	vs1
	xvnmsubadp	vs34,	vs32,	vs2
	xvnmsubadp	vs35,	vs32,	vs3
	xvnmsubadp	vs36,	vs32,	vs4
	xvnmsubadp	vs37,	vs32,	vs5
	xvnmsubadp	vs38,	vs32,	vs6
	xvnmsubadp	vs39,	vs32,	vs7
	xvnmsubadp	vs40,	vs32,	vs8
	xvnmsubadp	vs41,	vs32,	vs9
	xvnmsubadp	vs42,	vs32,	vs10
	xvnmsubadp	vs43,	vs32,	vs11
	xvnmsubadp	vs44,	vs32,	vs12
	xvnmsubadp	vs45,	vs32,	vs13
	xvnmsubadp	vs46,	vs32,	vs14
	xvnmsubadp	vs47,	vs32,	vs15

//############### OFFSET 1 #######################

	addi		T1,	T1,	1*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1
	lxvdsx		vs6,	o16,	T1
	lxvdsx		vs7,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs8,	o0,	T1
	lxvdsx		vs9,	o8,	T1
	lxvdsx		vs10,	o16,	T1
	lxvdsx		vs11,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs12,	o0,	T1
	lxvdsx		vs13,	o8,	T1
	lxvdsx		vs14,	o16,	T1

	addi		T1,	T1,	24

	xvmuldp		vs33,	vs33,	vs0
	xvnmsubadp	vs34,	vs33,	vs1
	xvnmsubadp	vs35,	vs33,	vs2
	xvnmsubadp	vs36,	vs33,	vs3
	xvnmsubadp	vs37,	vs33,	vs4
	xvnmsubadp	vs38,	vs33,	vs5
	xvnmsubadp	vs39,	vs33,	vs6
	xvnmsubadp	vs40,	vs33,	vs7
	xvnmsubadp	vs41,	vs33,	vs8
	xvnmsubadp	vs42,	vs33,	vs9
	xvnmsubadp	vs43,	vs33,	vs10
	xvnmsubadp	vs44,	vs33,	vs11
	xvnmsubadp	vs45,	vs33,	vs12
	xvnmsubadp	vs46,	vs33,	vs13
	xvnmsubadp	vs47,	vs33,	vs14

//############### OFFSET 2 #######################

	addi		T1,	T1,	2*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1
	lxvdsx		vs6,	o16,	T1
	lxvdsx		vs7,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs8,	o0,	T1
	lxvdsx		vs9,	o8,	T1
	lxvdsx		vs10,	o16,	T1
	lxvdsx		vs11,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs12,	o0,	T1
	lxvdsx		vs13,	o8,	T1

	addi		T1,	T1,	16

	xvmuldp		vs34,	vs34,	vs0
	xvnmsubadp	vs35,	vs34,	vs1
	xvnmsubadp	vs36,	vs34,	vs2
	xvnmsubadp	vs37,	vs34,	vs3
	xvnmsubadp	vs38,	vs34,	vs4
	xvnmsubadp	vs39,	vs34,	vs5
	xvnmsubadp	vs40,	vs34,	vs6
	xvnmsubadp	vs41,	vs34,	vs7
	xvnmsubadp	vs42,	vs34,	vs8
	xvnmsubadp	vs43,	vs34,	vs9
	xvnmsubadp	vs44,	vs34,	vs10
	xvnmsubadp	vs45,	vs34,	vs11
	xvnmsubadp	vs46,	vs34,	vs12
	xvnmsubadp	vs47,	vs34,	vs13

//############### OFFSET 3 #######################

	addi		T1,	T1,	3*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1
	lxvdsx		vs6,	o16,	T1
	lxvdsx		vs7,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs8,	o0,	T1
	lxvdsx		vs9,	o8,	T1
	lxvdsx		vs10,	o16,	T1
	lxvdsx		vs11,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs12,	o0,	T1

	addi		T1,	T1,	8

	xvmuldp		vs35,	vs35,	vs0
	xvnmsubadp	vs36,	vs35,	vs1
	xvnmsubadp	vs37,	vs35,	vs2
	xvnmsubadp	vs38,	vs35,	vs3
	xvnmsubadp	vs39,	vs35,	vs4
	xvnmsubadp	vs40,	vs35,	vs5
	xvnmsubadp	vs41,	vs35,	vs6
	xvnmsubadp	vs42,	vs35,	vs7
	xvnmsubadp	vs43,	vs35,	vs8
	xvnmsubadp	vs44,	vs35,	vs9
	xvnmsubadp	vs45,	vs35,	vs10
	xvnmsubadp	vs46,	vs35,	vs11
	xvnmsubadp	vs47,	vs35,	vs12

//############### OFFSET 4 #######################

	addi		T1,	T1,	4*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1
	lxvdsx		vs6,	o16,	T1
	lxvdsx		vs7,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs8,	o0,	T1
	lxvdsx		vs9,	o8,	T1
	lxvdsx		vs10,	o16,	T1
	lxvdsx		vs11,	o24,	T1

	addi		T1,	T1,	32

	xvmuldp		vs36,	vs36,	vs0
	xvnmsubadp	vs37,	vs36,	vs1
	xvnmsubadp	vs38,	vs36,	vs2
	xvnmsubadp	vs39,	vs36,	vs3
	xvnmsubadp	vs40,	vs36,	vs4
	xvnmsubadp	vs41,	vs36,	vs5
	xvnmsubadp	vs42,	vs36,	vs6
	xvnmsubadp	vs43,	vs36,	vs7
	xvnmsubadp	vs44,	vs36,	vs8
	xvnmsubadp	vs45,	vs36,	vs9
	xvnmsubadp	vs46,	vs36,	vs10
	xvnmsubadp	vs47,	vs36,	vs11

//############### OFFSET 5 #######################

	addi		T1,	T1,	5*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1
	lxvdsx		vs6,	o16,	T1
	lxvdsx		vs7,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs8,	o0,	T1
	lxvdsx		vs9,	o8,	T1
	lxvdsx		vs10,	o16,	T1

	addi		T1,	T1,	24

	xvmuldp		vs37,	vs37,	vs0
	xvnmsubadp	vs38,	vs37,	vs1
	xvnmsubadp	vs39,	vs37,	vs2
	xvnmsubadp	vs40,	vs37,	vs3
	xvnmsubadp	vs41,	vs37,	vs4
	xvnmsubadp	vs42,	vs37,	vs5
	xvnmsubadp	vs43,	vs37,	vs6
	xvnmsubadp	vs44,	vs37,	vs7
	xvnmsubadp	vs45,	vs37,	vs8
	xvnmsubadp	vs46,	vs37,	vs9
	xvnmsubadp	vs47,	vs37,	vs10

//############### OFFSET 6 #######################

	addi		T1,	T1,	6*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1
	lxvdsx		vs6,	o16,	T1
	lxvdsx		vs7,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs8,	o0,	T1
	lxvdsx		vs9,	o8,	T1

	addi		T1,	T1,	16

	xvmuldp		vs38,	vs38,	vs0
	xvnmsubadp	vs39,	vs38,	vs1
	xvnmsubadp	vs40,	vs38,	vs2
	xvnmsubadp	vs41,	vs38,	vs3
	xvnmsubadp	vs42,	vs38,	vs4
	xvnmsubadp	vs43,	vs38,	vs5
	xvnmsubadp	vs44,	vs38,	vs6
	xvnmsubadp	vs45,	vs38,	vs7
	xvnmsubadp	vs46,	vs38,	vs8
	xvnmsubadp	vs47,	vs38,	vs9

//############### OFFSET 7 #######################

	addi		T1,	T1,	7*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1
	lxvdsx		vs6,	o16,	T1
	lxvdsx		vs7,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs8,	o0,	T1

	addi		T1,	T1,	8

	xvmuldp		vs39,	vs39,	vs0
	xvnmsubadp	vs40,	vs39,	vs1
	xvnmsubadp	vs41,	vs39,	vs2
	xvnmsubadp	vs42,	vs39,	vs3
	xvnmsubadp	vs43,	vs39,	vs4
	xvnmsubadp	vs44,	vs39,	vs5
	xvnmsubadp	vs45,	vs39,	vs6
	xvnmsubadp	vs46,	vs39,	vs7
	xvnmsubadp	vs47,	vs39,	vs8

//############### OFFSET 8 #######################

	addi		T1,	T1,	8*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1
	lxvdsx		vs6,	o16,	T1
	lxvdsx		vs7,	o24,	T1

	addi		T1,	T1,	32

	xvmuldp		vs40,	vs40,	vs0
	xvnmsubadp	vs41,	vs40,	vs1
	xvnmsubadp	vs42,	vs40,	vs2
	xvnmsubadp	vs43,	vs40,	vs3
	xvnmsubadp	vs44,	vs40,	vs4
	xvnmsubadp	vs45,	vs40,	vs5
	xvnmsubadp	vs46,	vs40,	vs6
	xvnmsubadp	vs47,	vs40,	vs7

//############### OFFSET 9 #######################

	addi		T1,	T1,	9*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1
	lxvdsx		vs6,	o16,	T1

	addi		T1,	T1,	24

	xvmuldp		vs41,	vs41,	vs0
	xvnmsubadp	vs42,	vs41,	vs1
	xvnmsubadp	vs43,	vs41,	vs2
	xvnmsubadp	vs44,	vs41,	vs3
	xvnmsubadp	vs45,	vs41,	vs4
	xvnmsubadp	vs46,	vs41,	vs5
	xvnmsubadp	vs47,	vs41,	vs6

//############### OFFSET 10 #######################

	addi		T1,	T1,	10*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1

	addi		T1,	T1,	16

	xvmuldp		vs42,	vs42,	vs0
	xvnmsubadp	vs43,	vs42,	vs1
	xvnmsubadp	vs44,	vs42,	vs2
	xvnmsubadp	vs45,	vs42,	vs3
	xvnmsubadp	vs46,	vs42,	vs4
	xvnmsubadp	vs47,	vs42,	vs5

//############### OFFSET 11 #######################

	addi		T1,	T1,	11*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs4,	o0,	T1

	addi		T1,	T1,	8

	xvmuldp		vs43,	vs43,	vs0
	xvnmsubadp	vs44,	vs43,	vs1
	xvnmsubadp	vs45,	vs43,	vs2
	xvnmsubadp	vs46,	vs43,	vs3
	xvnmsubadp	vs47,	vs43,	vs4

//############### OFFSET 12 #######################

	addi		T1,	T1,	12*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	xvmuldp		vs44,	vs44,	vs0
	xvnmsubadp	vs45,	vs44,	vs1
	xvnmsubadp	vs46,	vs44,	vs2
	xvnmsubadp	vs47,	vs44,	vs3

//############### OFFSET 13 #######################

	addi		T1,	T1,	13*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1

	addi		T1,	T1,	24

	xvmuldp		vs45,	vs45,	vs0
	xvnmsubadp	vs46,	vs45,	vs1
	xvnmsubadp	vs47,	vs45,	vs2

//############### OFFSET 14 #######################

	addi		T1,	T1,	14*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1

	addi		T1,	T1,	16

	xvmuldp		vs46,	vs46,	vs0
	xvnmsubadp	vs47,	vs46,	vs1

//############### OFFSET 15 #######################

	addi		T1,	T1,	15*SIZE

	lxvdsx		vs0,	o0,	T1

	addi		T1,	T1,	8

	xvmuldp		vs47,	vs47,	vs0

//############### SAVE B #######################


	mr		T1,	BO	


	stxvd2x		vs32,	o0,	T1
	stxvd2x		vs33,	o16,	T1
	stxvd2x		vs34,	o32,	T1
	stxvd2x		vs35,	o48,	T1

	addi		T1,	T1,	64

	stxvd2x		vs36,	o0,	T1
	stxvd2x		vs37,	o16,	T1
	stxvd2x		vs38,	o32,	T1
	stxvd2x		vs39,	o48,	T1

	addi		T1,	T1,	64

	stxvd2x		vs40,	o0,	T1
	stxvd2x		vs41,	o16,	T1
	stxvd2x		vs42,	o32,	T1
	stxvd2x		vs43,	o48,	T1

	addi		T1,	T1,	64

	stxvd2x		vs44,	o0,	T1
	stxvd2x		vs45,	o16,	T1
	stxvd2x		vs46,	o32,	T1
	stxvd2x		vs47,	o48,	T1

//############### SAVE C #######################


	mr		T1,	CO
	add		T2,	CO,	LDC


	stxsdx		vs32,	o0,	T1
	xxswapd		vs32,	vs32
	stxsdx		vs33,	o8,	T1
	xxswapd		vs33,	vs33
	stxsdx		vs34,	o16,	T1
	xxswapd		vs34,	vs34
	stxsdx		vs35,	o24,	T1
	xxswapd		vs35,	vs35

	addi		T1,	T1,	32

	stxsdx		vs36,	o0,	T1
	xxswapd		vs36,	vs36
	stxsdx		vs37,	o8,	T1
	xxswapd		vs37,	vs37
	stxsdx		vs38,	o16,	T1
	xxswapd		vs38,	vs38
	stxsdx		vs39,	o24,	T1
	xxswapd		vs39,	vs39

	addi		T1,	T1,	32

	stxsdx		vs40,	o0,	T1
	xxswapd		vs40,	vs40
	stxsdx		vs41,	o8,	T1
	xxswapd		vs41,	vs41
	stxsdx		vs42,	o16,	T1
	xxswapd		vs42,	vs42
	stxsdx		vs43,	o24,	T1
	xxswapd		vs43,	vs43

	addi		T1,	T1,	32

	stxsdx		vs44,	o0,	T1
	xxswapd		vs44,	vs44
	stxsdx		vs45,	o8,	T1
	xxswapd		vs45,	vs45
	stxsdx		vs46,	o16,	T1
	xxswapd		vs46,	vs46
	stxsdx		vs47,	o24,	T1
	xxswapd		vs47,	vs47

	stxsdx		vs32,	o0,	T2
	stxsdx		vs33,	o8,	T2
	stxsdx		vs34,	o16,	T2
	stxsdx		vs35,	o24,	T2

	addi		T2,	T2,	32

	stxsdx		vs36,	o0,	T2
	stxsdx		vs37,	o8,	T2
	stxsdx		vs38,	o16,	T2
	stxsdx		vs39,	o24,	T2

	addi		T2,	T2,	32

	stxsdx		vs40,	o0,	T2
	stxsdx		vs41,	o8,	T2
	stxsdx		vs42,	o16,	T2
	stxsdx		vs43,	o24,	T2

	addi		T2,	T2,	32

	stxsdx		vs44,	o0,	T2
	stxsdx		vs45,	o8,	T2
	stxsdx		vs46,	o16,	T2
	stxsdx		vs47,	o24,	T2

.endm


/*##########################################################################################
			 SOLVE_LT 8x2
##########################################################################################*/

.macro SOLVE_LT_8x2

	xxpermdi	vs0,	vs32,	vs33,	0
	xxpermdi	vs1,	vs32,	vs33,	3

	xxpermdi	vs2,	vs34,	vs35,	0
	xxpermdi	vs3,	vs34,	vs35,	3

	xxpermdi	vs4,	vs36,	vs37,	0
	xxpermdi	vs5,	vs36,	vs37,	3

	xxpermdi	vs6,	vs38,	vs39,	0
	xxpermdi	vs7,	vs38,	vs39,	3


//############### LOAD B #######################


	mr		T1,	BO	

	lxvd2x		vs32,	o0,	T1
	lxvd2x		vs33,	o16,	T1
	lxvd2x		vs34,	o32,	T1
	lxvd2x		vs35,	o48,	T1

	addi		T1,	T1,	64

	lxvd2x		vs36,	o0,	T1
	lxvd2x		vs37,	o16,	T1
	lxvd2x		vs38,	o32,	T1
	lxvd2x		vs39,	o48,	T1

	xvsubdp		vs32,	vs32,	vs0
	xvsubdp		vs33,	vs33,	vs1
	xvsubdp		vs34,	vs34,	vs2
	xvsubdp		vs35,	vs35,	vs3
	xvsubdp		vs36,	vs36,	vs4
	xvsubdp		vs37,	vs37,	vs5
	xvsubdp		vs38,	vs38,	vs6
	xvsubdp		vs39,	vs39,	vs7

	mr		T1,	AO	


//############### OFFSET 0 #######################

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1
	lxvdsx		vs6,	o16,	T1
	lxvdsx		vs7,	o24,	T1

	addi		T1,	T1,	32

	xvmuldp		vs32,	vs32,	vs0
	xvnmsubadp	vs33,	vs32,	vs1
	xvnmsubadp	vs34,	vs32,	vs2
	xvnmsubadp	vs35,	vs32,	vs3
	xvnmsubadp	vs36,	vs32,	vs4
	xvnmsubadp	vs37,	vs32,	vs5
	xvnmsubadp	vs38,	vs32,	vs6
	xvnmsubadp	vs39,	vs32,	vs7

//############### OFFSET 1 #######################

	addi		T1,	T1,	1*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1
	lxvdsx		vs6,	o16,	T1

	addi		T1,	T1,	24

	xvmuldp		vs33,	vs33,	vs0
	xvnmsubadp	vs34,	vs33,	vs1
	xvnmsubadp	vs35,	vs33,	vs2
	xvnmsubadp	vs36,	vs33,	vs3
	xvnmsubadp	vs37,	vs33,	vs4
	xvnmsubadp	vs38,	vs33,	vs5
	xvnmsubadp	vs39,	vs33,	vs6

//############### OFFSET 2 #######################

	addi		T1,	T1,	2*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs4,	o0,	T1
	lxvdsx		vs5,	o8,	T1

	addi		T1,	T1,	16

	xvmuldp		vs34,	vs34,	vs0
	xvnmsubadp	vs35,	vs34,	vs1
	xvnmsubadp	vs36,	vs34,	vs2
	xvnmsubadp	vs37,	vs34,	vs3
	xvnmsubadp	vs38,	vs34,	vs4
	xvnmsubadp	vs39,	vs34,	vs5

//############### OFFSET 3 #######################

	addi		T1,	T1,	3*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxvdsx		vs4,	o0,	T1

	addi		T1,	T1,	8

	xvmuldp		vs35,	vs35,	vs0
	xvnmsubadp	vs36,	vs35,	vs1
	xvnmsubadp	vs37,	vs35,	vs2
	xvnmsubadp	vs38,	vs35,	vs3
	xvnmsubadp	vs39,	vs35,	vs4

//############### OFFSET 4 #######################

	addi		T1,	T1,	4*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	xvmuldp		vs36,	vs36,	vs0
	xvnmsubadp	vs37,	vs36,	vs1
	xvnmsubadp	vs38,	vs36,	vs2
	xvnmsubadp	vs39,	vs36,	vs3

//############### OFFSET 5 #######################

	addi		T1,	T1,	5*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1

	addi		T1,	T1,	24

	xvmuldp		vs37,	vs37,	vs0
	xvnmsubadp	vs38,	vs37,	vs1
	xvnmsubadp	vs39,	vs37,	vs2

//############### OFFSET 6 #######################

	addi		T1,	T1,	6*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1

	addi		T1,	T1,	16

	xvmuldp		vs38,	vs38,	vs0
	xvnmsubadp	vs39,	vs38,	vs1

//############### OFFSET 7 #######################

	addi		T1,	T1,	7*SIZE

	lxvdsx		vs0,	o0,	T1

	addi		T1,	T1,	8

	xvmuldp		vs39,	vs39,	vs0

//############### SAVE B #######################


	mr		T1,	BO	


	stxvd2x		vs32,	o0,	T1
	stxvd2x		vs33,	o16,	T1
	stxvd2x		vs34,	o32,	T1
	stxvd2x		vs35,	o48,	T1

	addi		T1,	T1,	64

	stxvd2x		vs36,	o0,	T1
	stxvd2x		vs37,	o16,	T1
	stxvd2x		vs38,	o32,	T1
	stxvd2x		vs39,	o48,	T1

//############### SAVE C #######################


	mr		T1,	CO
	add		T2,	CO,	LDC


	stxsdx		vs32,	o0,	T1
	xxswapd		vs32,	vs32
	stxsdx		vs33,	o8,	T1
	xxswapd		vs33,	vs33
	stxsdx		vs34,	o16,	T1
	xxswapd		vs34,	vs34
	stxsdx		vs35,	o24,	T1
	xxswapd		vs35,	vs35

	addi		T1,	T1,	32

	stxsdx		vs36,	o0,	T1
	xxswapd		vs36,	vs36
	stxsdx		vs37,	o8,	T1
	xxswapd		vs37,	vs37
	stxsdx		vs38,	o16,	T1
	xxswapd		vs38,	vs38
	stxsdx		vs39,	o24,	T1
	xxswapd		vs39,	vs39

	stxsdx		vs32,	o0,	T2
	stxsdx		vs33,	o8,	T2
	stxsdx		vs34,	o16,	T2
	stxsdx		vs35,	o24,	T2

	addi		T2,	T2,	32

	stxsdx		vs36,	o0,	T2
	stxsdx		vs37,	o8,	T2
	stxsdx		vs38,	o16,	T2
	stxsdx		vs39,	o24,	T2

.endm


/*##########################################################################################
			 SOLVE_LT 4x2
##########################################################################################*/

.macro SOLVE_LT_4x2

	xxpermdi	vs0,	vs32,	vs33,	0
	xxpermdi	vs1,	vs32,	vs33,	3

	xxpermdi	vs2,	vs34,	vs35,	0
	xxpermdi	vs3,	vs34,	vs35,	3


//############### LOAD B #######################


	mr		T1,	BO	

	lxvd2x		vs32,	o0,	T1
	lxvd2x		vs33,	o16,	T1
	lxvd2x		vs34,	o32,	T1
	lxvd2x		vs35,	o48,	T1

	xvsubdp		vs32,	vs32,	vs0
	xvsubdp		vs33,	vs33,	vs1
	xvsubdp		vs34,	vs34,	vs2
	xvsubdp		vs35,	vs35,	vs3

	mr		T1,	AO	


//############### OFFSET 0 #######################

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1
	lxvdsx		vs3,	o24,	T1

	addi		T1,	T1,	32

	xvmuldp		vs32,	vs32,	vs0
	xvnmsubadp	vs33,	vs32,	vs1
	xvnmsubadp	vs34,	vs32,	vs2
	xvnmsubadp	vs35,	vs32,	vs3

//############### OFFSET 1 #######################

	addi		T1,	T1,	1*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1
	lxvdsx		vs2,	o16,	T1

	addi		T1,	T1,	24

	xvmuldp		vs33,	vs33,	vs0
	xvnmsubadp	vs34,	vs33,	vs1
	xvnmsubadp	vs35,	vs33,	vs2

//############### OFFSET 2 #######################

	addi		T1,	T1,	2*SIZE

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1

	addi		T1,	T1,	16

	xvmuldp		vs34,	vs34,	vs0
	xvnmsubadp	vs35,	vs34,	vs1

//############### OFFSET 3 #######################

	addi		T1,	T1,	3*SIZE

	lxvdsx		vs0,	o0,	T1

	addi		T1,	T1,	8

	xvmuldp		vs35,	vs35,	vs0

//############### SAVE B #######################


	mr		T1,	BO	


	stxvd2x		vs32,	o0,	T1
	stxvd2x		vs33,	o16,	T1
	stxvd2x		vs34,	o32,	T1
	stxvd2x		vs35,	o48,	T1

//############### SAVE C #######################


	mr		T1,	CO
	add		T2,	CO,	LDC


	stxsdx		vs32,	o0,	T1
	xxswapd		vs32,	vs32
	stxsdx		vs33,	o8,	T1
	xxswapd		vs33,	vs33
	stxsdx		vs34,	o16,	T1
	xxswapd		vs34,	vs34
	stxsdx		vs35,	o24,	T1
	xxswapd		vs35,	vs35

	stxsdx		vs32,	o0,	T2
	stxsdx		vs33,	o8,	T2
	stxsdx		vs34,	o16,	T2
	stxsdx		vs35,	o24,	T2

.endm


/*##########################################################################################
			 SOLVE_LT 2x2
##########################################################################################*/

.macro SOLVE_LT_2x2

	xxpermdi	vs0,	vs32,	vs33,	0
	xxpermdi	vs1,	vs32,	vs33,	3


//############### LOAD B #######################


	mr		T1,	BO	

	lxvd2x		vs32,	o0,	T1
	lxvd2x		vs33,	o16,	T1

	xvsubdp		vs32,	vs32,	vs0
	xvsubdp		vs33,	vs33,	vs1

	mr		T1,	AO	


//############### OFFSET 0 #######################

	lxvdsx		vs0,	o0,	T1
	lxvdsx		vs1,	o8,	T1

	addi		T1,	T1,	16

	xvmuldp		vs32,	vs32,	vs0
	xvnmsubadp	vs33,	vs32,	vs1

//############### OFFSET 1 #######################

	addi		T1,	T1,	1*SIZE

	lxvdsx		vs0,	o0,	T1

	addi		T1,	T1,	8

	xvmuldp		vs33,	vs33,	vs0

//############### SAVE B #######################


	mr		T1,	BO	


	stxvd2x		vs32,	o0,	T1
	stxvd2x		vs33,	o16,	T1

//############### SAVE C #######################


	mr		T1,	CO
	add		T2,	CO,	LDC


	stxsdx		vs32,	o0,	T1
	xxswapd		vs32,	vs32
	stxsdx		vs33,	o8,	T1
	xxswapd		vs33,	vs33

	stxsdx		vs32,	o0,	T2
	stxsdx		vs33,	o8,	T2

.endm


/*##########################################################################################
			 SOLVE_LT 1x2
##########################################################################################*/

.macro SOLVE_LT_1x2

	xxpermdi	vs0,	vs32,	vs33,	0

//############### LOAD B #######################


	mr		T1,	BO	

	lxvd2x		vs32,	o0,	T1

	xvsubdp		vs32,	vs32,	vs0

	mr		T1,	AO	


//############### OFFSET 0 #######################

	lxvdsx		vs0,	o0,	T1

	addi		T1,	T1,	8

	xvmuldp		vs32,	vs32,	vs0

//############### SAVE B #######################


	mr		T1,	BO	


	stxvd2x		vs32,	o0,	T1

//############### SAVE C #######################


	mr		T1,	CO
	add		T2,	CO,	LDC


	stxsdx		vs32,	o0,	T1
	xxswapd		vs32,	vs32

	stxsdx		vs32,	o0,	T2

.endm


.macro INIT_16x1


	xxlxor		vs0,	vs0,	vs0

	xvmovdp		vs32,	vs0
	xvmovdp		vs33,	vs0
	xvmovdp		vs34,	vs0
	xvmovdp		vs35,	vs0
	xvmovdp		vs36,	vs0
	xvmovdp		vs37,	vs0
	xvmovdp		vs38,	vs0
	xvmovdp		vs39,	vs0
	xvmovdp		vs40,	vs0
	xvmovdp		vs41,	vs0
	xvmovdp		vs42,	vs0
	xvmovdp		vs43,	vs0
	xvmovdp		vs44,	vs0
	xvmovdp		vs45,	vs0
	xvmovdp		vs46,	vs0
	xvmovdp		vs47,	vs0

.endm


.macro KERNEL_16x1


	lxvdsx		vs0,	o0,	AO
	lxvdsx		vs1,	o8,	AO
	lxvdsx		vs2,	o16,	AO
	lxvdsx		vs3,	o24,	AO

	addi		AO,	AO,	32

	lxvdsx		vs4,	o0,	AO
	lxvdsx		vs5,	o8,	AO
	lxvdsx		vs6,	o16,	AO
	lxvdsx		vs7,	o24,	AO

	addi		AO,	AO,	32

	lxvdsx		vs8,	o0,	AO
	lxvdsx		vs9,	o8,	AO
	lxvdsx		vs10,	o16,	AO
	lxvdsx		vs11,	o24,	AO

	addi		AO,	AO,	32

	lxvdsx		vs12,	o0,	AO
	lxvdsx		vs13,	o8,	AO
	lxvdsx		vs14,	o16,	AO
	lxvdsx		vs15,	o24,	AO

	addi		AO,	AO,	32

	lxvdsx		vs16,	o0,	BO

	addi		BO,	BO,	8

	xvmaddadp	vs32,	vs0,	vs16
	xvmaddadp	vs33,	vs1,	vs16
	xvmaddadp	vs34,	vs2,	vs16
	xvmaddadp	vs35,	vs3,	vs16
	xvmaddadp	vs36,	vs4,	vs16
	xvmaddadp	vs37,	vs5,	vs16
	xvmaddadp	vs38,	vs6,	vs16
	xvmaddadp	vs39,	vs7,	vs16
	xvmaddadp	vs40,	vs8,	vs16
	xvmaddadp	vs41,	vs9,	vs16
	xvmaddadp	vs42,	vs10,	vs16
	xvmaddadp	vs43,	vs11,	vs16
	xvmaddadp	vs44,	vs12,	vs16
	xvmaddadp	vs45,	vs13,	vs16
	xvmaddadp	vs46,	vs14,	vs16
	xvmaddadp	vs47,	vs15,	vs16


.endm


.macro INIT_8x1


	xxlxor		vs0,	vs0,	vs0

	xvmovdp		vs32,	vs0
	xvmovdp		vs33,	vs0
	xvmovdp		vs34,	vs0
	xvmovdp		vs35,	vs0
	xvmovdp		vs36,	vs0
	xvmovdp		vs37,	vs0
	xvmovdp		vs38,	vs0
	xvmovdp		vs39,	vs0

.endm


.macro KERNEL_8x1


	lxvdsx		vs0,	o0,	AO
	lxvdsx		vs1,	o8,	AO
	lxvdsx		vs2,	o16,	AO
	lxvdsx		vs3,	o24,	AO

	addi		AO,	AO,	32

	lxvdsx		vs4,	o0,	AO
	lxvdsx		vs5,	o8,	AO
	lxvdsx		vs6,	o16,	AO
	lxvdsx		vs7,	o24,	AO

	addi		AO,	AO,	32

	lxvdsx		vs16,	o0,	BO

	addi		BO,	BO,	8

	xvmaddadp	vs32,	vs0,	vs16
	xvmaddadp	vs33,	vs1,	vs16
	xvmaddadp	vs34,	vs2,	vs16
	xvmaddadp	vs35,	vs3,	vs16
	xvmaddadp	vs36,	vs4,	vs16
	xvmaddadp	vs37,	vs5,	vs16
	xvmaddadp	vs38,	vs6,	vs16
	xvmaddadp	vs39,	vs7,	vs16


.endm


.macro INIT_4x1


	xxlxor		vs0,	vs0,	vs0

	xvmovdp		vs32,	vs0
	xvmovdp		vs33,	vs0
	xvmovdp		vs34,	vs0
	xvmovdp		vs35,	vs0

.endm


.macro KERNEL_4x1


	lxvdsx		vs0,	o0,	AO
	lxvdsx		vs1,	o8,	AO
	lxvdsx		vs2,	o16,	AO
	lxvdsx		vs3,	o24,	AO

	addi		AO,	AO,	32

	lxvdsx		vs16,	o0,	BO

	addi		BO,	BO,	8

	xvmaddadp	vs32,	vs0,	vs16
	xvmaddadp	vs33,	vs1,	vs16
	xvmaddadp	vs34,	vs2,	vs16
	xvmaddadp	vs35,	vs3,	vs16


.endm


.macro INIT_2x1


	xxlxor		vs0,	vs0,	vs0

	xvmovdp		vs32,	vs0
	xvmovdp		vs33,	vs0

.endm


.macro KERNEL_2x1


	lxvdsx		vs0,	o0,	AO
	lxvdsx		vs1,	o8,	AO

	addi		AO,	AO,	16

	lxvdsx		vs16,	o0,	BO

	addi		BO,	BO,	8

	xvmaddadp	vs32,	vs0,	vs16
	xvmaddadp	vs33,	vs1,	vs16


.endm


.macro INIT_1x1


	xxlxor		vs0,	vs0,	vs0

	xvmovdp		vs32,	vs0

.endm


.macro KERNEL_1x1


	lxvdsx		vs0,	o0,	AO

	addi		AO,	AO,	8

	lxvdsx		vs16,	o0,	BO

	addi		BO,	BO,	8

	xvmaddadp	vs32,	vs0,	vs16


.endm


/*##########################################################################################
			 SOLVE_LT 16x1
##########################################################################################*/

.macro SOLVE_LT_16x1

	xxswapd		vs0,	vs32
	xxswapd		vs1,	vs33
	xxswapd		vs2,	vs34
	xxswapd		vs3,	vs35
	xxswapd		vs4,	vs36
	xxswapd		vs5,	vs37
	xxswapd		vs6,	vs38
	xxswapd		vs7,	vs39
	xxswapd		vs8,	vs40
	xxswapd		vs9,	vs41
	xxswapd		vs10,	vs42
	xxswapd		vs11,	vs43
	xxswapd		vs12,	vs44
	xxswapd		vs13,	vs45
	xxswapd		vs14,	vs46
	xxswapd		vs15,	vs47

//############### LOAD B #######################


	mr		T1,	BO	

	lxsdx		vs32,	o0,	T1
	lxsdx		vs33,	o8,	T1
	lxsdx		vs34,	o16,	T1
	lxsdx		vs35,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs36,	o0,	T1
	lxsdx		vs37,	o8,	T1
	lxsdx		vs38,	o16,	T1
	lxsdx		vs39,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs40,	o0,	T1
	lxsdx		vs41,	o8,	T1
	lxsdx		vs42,	o16,	T1
	lxsdx		vs43,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs44,	o0,	T1
	lxsdx		vs45,	o8,	T1
	lxsdx		vs46,	o16,	T1
	lxsdx		vs47,	o24,	T1

	xssubdp		vs32,	vs32,	vs0
	xssubdp		vs33,	vs33,	vs1
	xssubdp		vs34,	vs34,	vs2
	xssubdp		vs35,	vs35,	vs3
	xssubdp		vs36,	vs36,	vs4
	xssubdp		vs37,	vs37,	vs5
	xssubdp		vs38,	vs38,	vs6
	xssubdp		vs39,	vs39,	vs7
	xssubdp		vs40,	vs40,	vs8
	xssubdp		vs41,	vs41,	vs9
	xssubdp		vs42,	vs42,	vs10
	xssubdp		vs43,	vs43,	vs11
	xssubdp		vs44,	vs44,	vs12
	xssubdp		vs45,	vs45,	vs13
	xssubdp		vs46,	vs46,	vs14
	xssubdp		vs47,	vs47,	vs15

	mr		T1,	AO	


//############### OFFSET 0 #######################

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1
	lxsdx		vs2,	o16,	T1
	lxsdx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs4,	o0,	T1
	lxsdx		vs5,	o8,	T1
	lxsdx		vs6,	o16,	T1
	lxsdx		vs7,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs8,	o0,	T1
	lxsdx		vs9,	o8,	T1
	lxsdx		vs10,	o16,	T1
	lxsdx		vs11,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs12,	o0,	T1
	lxsdx		vs13,	o8,	T1
	lxsdx		vs14,	o16,	T1
	lxsdx		vs15,	o24,	T1

	addi		T1,	T1,	32

	xsmuldp		vs32,	vs32,	vs0
	xsnmsubadp	vs33,	vs32,	vs1
	xsnmsubadp	vs34,	vs32,	vs2
	xsnmsubadp	vs35,	vs32,	vs3
	xsnmsubadp	vs36,	vs32,	vs4
	xsnmsubadp	vs37,	vs32,	vs5
	xsnmsubadp	vs38,	vs32,	vs6
	xsnmsubadp	vs39,	vs32,	vs7
	xsnmsubadp	vs40,	vs32,	vs8
	xsnmsubadp	vs41,	vs32,	vs9
	xsnmsubadp	vs42,	vs32,	vs10
	xsnmsubadp	vs43,	vs32,	vs11
	xsnmsubadp	vs44,	vs32,	vs12
	xsnmsubadp	vs45,	vs32,	vs13
	xsnmsubadp	vs46,	vs32,	vs14
	xsnmsubadp	vs47,	vs32,	vs15

//############### OFFSET 1 #######################

	addi		T1,	T1,	1*SIZE

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1
	lxsdx		vs2,	o16,	T1
	lxsdx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs4,	o0,	T1
	lxsdx		vs5,	o8,	T1
	lxsdx		vs6,	o16,	T1
	lxsdx		vs7,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs8,	o0,	T1
	lxsdx		vs9,	o8,	T1
	lxsdx		vs10,	o16,	T1
	lxsdx		vs11,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs12,	o0,	T1
	lxsdx		vs13,	o8,	T1
	lxsdx		vs14,	o16,	T1

	addi		T1,	T1,	24

	xsmuldp		vs33,	vs33,	vs0
	xsnmsubadp	vs34,	vs33,	vs1
	xsnmsubadp	vs35,	vs33,	vs2
	xsnmsubadp	vs36,	vs33,	vs3
	xsnmsubadp	vs37,	vs33,	vs4
	xsnmsubadp	vs38,	vs33,	vs5
	xsnmsubadp	vs39,	vs33,	vs6
	xsnmsubadp	vs40,	vs33,	vs7
	xsnmsubadp	vs41,	vs33,	vs8
	xsnmsubadp	vs42,	vs33,	vs9
	xsnmsubadp	vs43,	vs33,	vs10
	xsnmsubadp	vs44,	vs33,	vs11
	xsnmsubadp	vs45,	vs33,	vs12
	xsnmsubadp	vs46,	vs33,	vs13
	xsnmsubadp	vs47,	vs33,	vs14

//############### OFFSET 2 #######################

	addi		T1,	T1,	2*SIZE

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1
	lxsdx		vs2,	o16,	T1
	lxsdx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs4,	o0,	T1
	lxsdx		vs5,	o8,	T1
	lxsdx		vs6,	o16,	T1
	lxsdx		vs7,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs8,	o0,	T1
	lxsdx		vs9,	o8,	T1
	lxsdx		vs10,	o16,	T1
	lxsdx		vs11,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs12,	o0,	T1
	lxsdx		vs13,	o8,	T1

	addi		T1,	T1,	16

	xsmuldp		vs34,	vs34,	vs0
	xsnmsubadp	vs35,	vs34,	vs1
	xsnmsubadp	vs36,	vs34,	vs2
	xsnmsubadp	vs37,	vs34,	vs3
	xsnmsubadp	vs38,	vs34,	vs4
	xsnmsubadp	vs39,	vs34,	vs5
	xsnmsubadp	vs40,	vs34,	vs6
	xsnmsubadp	vs41,	vs34,	vs7
	xsnmsubadp	vs42,	vs34,	vs8
	xsnmsubadp	vs43,	vs34,	vs9
	xsnmsubadp	vs44,	vs34,	vs10
	xsnmsubadp	vs45,	vs34,	vs11
	xsnmsubadp	vs46,	vs34,	vs12
	xsnmsubadp	vs47,	vs34,	vs13

//############### OFFSET 3 #######################

	addi		T1,	T1,	3*SIZE

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1
	lxsdx		vs2,	o16,	T1
	lxsdx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs4,	o0,	T1
	lxsdx		vs5,	o8,	T1
	lxsdx		vs6,	o16,	T1
	lxsdx		vs7,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs8,	o0,	T1
	lxsdx		vs9,	o8,	T1
	lxsdx		vs10,	o16,	T1
	lxsdx		vs11,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs12,	o0,	T1

	addi		T1,	T1,	8

	xsmuldp		vs35,	vs35,	vs0
	xsnmsubadp	vs36,	vs35,	vs1
	xsnmsubadp	vs37,	vs35,	vs2
	xsnmsubadp	vs38,	vs35,	vs3
	xsnmsubadp	vs39,	vs35,	vs4
	xsnmsubadp	vs40,	vs35,	vs5
	xsnmsubadp	vs41,	vs35,	vs6
	xsnmsubadp	vs42,	vs35,	vs7
	xsnmsubadp	vs43,	vs35,	vs8
	xsnmsubadp	vs44,	vs35,	vs9
	xsnmsubadp	vs45,	vs35,	vs10
	xsnmsubadp	vs46,	vs35,	vs11
	xsnmsubadp	vs47,	vs35,	vs12

//############### OFFSET 4 #######################

	addi		T1,	T1,	4*SIZE

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1
	lxsdx		vs2,	o16,	T1
	lxsdx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs4,	o0,	T1
	lxsdx		vs5,	o8,	T1
	lxsdx		vs6,	o16,	T1
	lxsdx		vs7,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs8,	o0,	T1
	lxsdx		vs9,	o8,	T1
	lxsdx		vs10,	o16,	T1
	lxsdx		vs11,	o24,	T1

	addi		T1,	T1,	32

	xsmuldp		vs36,	vs36,	vs0
	xsnmsubadp	vs37,	vs36,	vs1
	xsnmsubadp	vs38,	vs36,	vs2
	xsnmsubadp	vs39,	vs36,	vs3
	xsnmsubadp	vs40,	vs36,	vs4
	xsnmsubadp	vs41,	vs36,	vs5
	xsnmsubadp	vs42,	vs36,	vs6
	xsnmsubadp	vs43,	vs36,	vs7
	xsnmsubadp	vs44,	vs36,	vs8
	xsnmsubadp	vs45,	vs36,	vs9
	xsnmsubadp	vs46,	vs36,	vs10
	xsnmsubadp	vs47,	vs36,	vs11

//############### OFFSET 5 #######################

	addi		T1,	T1,	5*SIZE

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1
	lxsdx		vs2,	o16,	T1
	lxsdx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs4,	o0,	T1
	lxsdx		vs5,	o8,	T1
	lxsdx		vs6,	o16,	T1
	lxsdx		vs7,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs8,	o0,	T1
	lxsdx		vs9,	o8,	T1
	lxsdx		vs10,	o16,	T1

	addi		T1,	T1,	24

	xsmuldp		vs37,	vs37,	vs0
	xsnmsubadp	vs38,	vs37,	vs1
	xsnmsubadp	vs39,	vs37,	vs2
	xsnmsubadp	vs40,	vs37,	vs3
	xsnmsubadp	vs41,	vs37,	vs4
	xsnmsubadp	vs42,	vs37,	vs5
	xsnmsubadp	vs43,	vs37,	vs6
	xsnmsubadp	vs44,	vs37,	vs7
	xsnmsubadp	vs45,	vs37,	vs8
	xsnmsubadp	vs46,	vs37,	vs9
	xsnmsubadp	vs47,	vs37,	vs10

//############### OFFSET 6 #######################

	addi		T1,	T1,	6*SIZE

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1
	lxsdx		vs2,	o16,	T1
	lxsdx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs4,	o0,	T1
	lxsdx		vs5,	o8,	T1
	lxsdx		vs6,	o16,	T1
	lxsdx		vs7,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs8,	o0,	T1
	lxsdx		vs9,	o8,	T1

	addi		T1,	T1,	16

	xsmuldp		vs38,	vs38,	vs0
	xsnmsubadp	vs39,	vs38,	vs1
	xsnmsubadp	vs40,	vs38,	vs2
	xsnmsubadp	vs41,	vs38,	vs3
	xsnmsubadp	vs42,	vs38,	vs4
	xsnmsubadp	vs43,	vs38,	vs5
	xsnmsubadp	vs44,	vs38,	vs6
	xsnmsubadp	vs45,	vs38,	vs7
	xsnmsubadp	vs46,	vs38,	vs8
	xsnmsubadp	vs47,	vs38,	vs9

//############### OFFSET 7 #######################

	addi		T1,	T1,	7*SIZE

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1
	lxsdx		vs2,	o16,	T1
	lxsdx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs4,	o0,	T1
	lxsdx		vs5,	o8,	T1
	lxsdx		vs6,	o16,	T1
	lxsdx		vs7,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs8,	o0,	T1

	addi		T1,	T1,	8

	xsmuldp		vs39,	vs39,	vs0
	xsnmsubadp	vs40,	vs39,	vs1
	xsnmsubadp	vs41,	vs39,	vs2
	xsnmsubadp	vs42,	vs39,	vs3
	xsnmsubadp	vs43,	vs39,	vs4
	xsnmsubadp	vs44,	vs39,	vs5
	xsnmsubadp	vs45,	vs39,	vs6
	xsnmsubadp	vs46,	vs39,	vs7
	xsnmsubadp	vs47,	vs39,	vs8

//############### OFFSET 8 #######################

	addi		T1,	T1,	8*SIZE

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1
	lxsdx		vs2,	o16,	T1
	lxsdx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs4,	o0,	T1
	lxsdx		vs5,	o8,	T1
	lxsdx		vs6,	o16,	T1
	lxsdx		vs7,	o24,	T1

	addi		T1,	T1,	32

	xsmuldp		vs40,	vs40,	vs0
	xsnmsubadp	vs41,	vs40,	vs1
	xsnmsubadp	vs42,	vs40,	vs2
	xsnmsubadp	vs43,	vs40,	vs3
	xsnmsubadp	vs44,	vs40,	vs4
	xsnmsubadp	vs45,	vs40,	vs5
	xsnmsubadp	vs46,	vs40,	vs6
	xsnmsubadp	vs47,	vs40,	vs7

//############### OFFSET 9 #######################

	addi		T1,	T1,	9*SIZE

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1
	lxsdx		vs2,	o16,	T1
	lxsdx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs4,	o0,	T1
	lxsdx		vs5,	o8,	T1
	lxsdx		vs6,	o16,	T1

	addi		T1,	T1,	24

	xsmuldp		vs41,	vs41,	vs0
	xsnmsubadp	vs42,	vs41,	vs1
	xsnmsubadp	vs43,	vs41,	vs2
	xsnmsubadp	vs44,	vs41,	vs3
	xsnmsubadp	vs45,	vs41,	vs4
	xsnmsubadp	vs46,	vs41,	vs5
	xsnmsubadp	vs47,	vs41,	vs6

//############### OFFSET 10 #######################

	addi		T1,	T1,	10*SIZE

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1
	lxsdx		vs2,	o16,	T1
	lxsdx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs4,	o0,	T1
	lxsdx		vs5,	o8,	T1

	addi		T1,	T1,	16

	xsmuldp		vs42,	vs42,	vs0
	xsnmsubadp	vs43,	vs42,	vs1
	xsnmsubadp	vs44,	vs42,	vs2
	xsnmsubadp	vs45,	vs42,	vs3
	xsnmsubadp	vs46,	vs42,	vs4
	xsnmsubadp	vs47,	vs42,	vs5

//############### OFFSET 11 #######################

	addi		T1,	T1,	11*SIZE

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1
	lxsdx		vs2,	o16,	T1
	lxsdx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs4,	o0,	T1

	addi		T1,	T1,	8

	xsmuldp		vs43,	vs43,	vs0
	xsnmsubadp	vs44,	vs43,	vs1
	xsnmsubadp	vs45,	vs43,	vs2
	xsnmsubadp	vs46,	vs43,	vs3
	xsnmsubadp	vs47,	vs43,	vs4

//############### OFFSET 12 #######################

	addi		T1,	T1,	12*SIZE

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1
	lxsdx		vs2,	o16,	T1
	lxsdx		vs3,	o24,	T1

	addi		T1,	T1,	32

	xsmuldp		vs44,	vs44,	vs0
	xsnmsubadp	vs45,	vs44,	vs1
	xsnmsubadp	vs46,	vs44,	vs2
	xsnmsubadp	vs47,	vs44,	vs3

//############### OFFSET 13 #######################

	addi		T1,	T1,	13*SIZE

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1
	lxsdx		vs2,	o16,	T1

	addi		T1,	T1,	24

	xsmuldp		vs45,	vs45,	vs0
	xsnmsubadp	vs46,	vs45,	vs1
	xsnmsubadp	vs47,	vs45,	vs2

//############### OFFSET 14 #######################

	addi		T1,	T1,	14*SIZE

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1

	addi		T1,	T1,	16

	xsmuldp		vs46,	vs46,	vs0
	xsnmsubadp	vs47,	vs46,	vs1

//############### OFFSET 15 #######################

	addi		T1,	T1,	15*SIZE

	lxsdx		vs0,	o0,	T1

	addi		T1,	T1,	8

	xsmuldp		vs47,	vs47,	vs0

//############### SAVE B #######################


	mr		T1,	BO	


	stxsdx		vs32,	o0,	T1
	stxsdx		vs33,	o8,	T1
	stxsdx		vs34,	o16,	T1
	stxsdx		vs35,	o24,	T1

	addi		T1,	T1,	32

	stxsdx		vs36,	o0,	T1
	stxsdx		vs37,	o8,	T1
	stxsdx		vs38,	o16,	T1
	stxsdx		vs39,	o24,	T1

	addi		T1,	T1,	32

	stxsdx		vs40,	o0,	T1
	stxsdx		vs41,	o8,	T1
	stxsdx		vs42,	o16,	T1
	stxsdx		vs43,	o24,	T1

	addi		T1,	T1,	32

	stxsdx		vs44,	o0,	T1
	stxsdx		vs45,	o8,	T1
	stxsdx		vs46,	o16,	T1
	stxsdx		vs47,	o24,	T1

//############### SAVE C #######################


	mr		T1,	CO

	stxsdx		vs32,	o0,	T1
	stxsdx		vs33,	o8,	T1
	stxsdx		vs34,	o16,	T1
	stxsdx		vs35,	o24,	T1

	addi		T1,	T1,	32

	stxsdx		vs36,	o0,	T1
	stxsdx		vs37,	o8,	T1
	stxsdx		vs38,	o16,	T1
	stxsdx		vs39,	o24,	T1

	addi		T1,	T1,	32

	stxsdx		vs40,	o0,	T1
	stxsdx		vs41,	o8,	T1
	stxsdx		vs42,	o16,	T1
	stxsdx		vs43,	o24,	T1

	addi		T1,	T1,	32

	stxsdx		vs44,	o0,	T1
	stxsdx		vs45,	o8,	T1
	stxsdx		vs46,	o16,	T1
	stxsdx		vs47,	o24,	T1

.endm


/*##########################################################################################
			 SOLVE_LT 8x1
##########################################################################################*/

.macro SOLVE_LT_8x1

	xxswapd		vs0,	vs32
	xxswapd		vs1,	vs33
	xxswapd		vs2,	vs34
	xxswapd		vs3,	vs35
	xxswapd		vs4,	vs36
	xxswapd		vs5,	vs37
	xxswapd		vs6,	vs38
	xxswapd		vs7,	vs39

//############### LOAD B #######################


	mr		T1,	BO	

	lxsdx		vs32,	o0,	T1
	lxsdx		vs33,	o8,	T1
	lxsdx		vs34,	o16,	T1
	lxsdx		vs35,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs36,	o0,	T1
	lxsdx		vs37,	o8,	T1
	lxsdx		vs38,	o16,	T1
	lxsdx		vs39,	o24,	T1

	xssubdp		vs32,	vs32,	vs0
	xssubdp		vs33,	vs33,	vs1
	xssubdp		vs34,	vs34,	vs2
	xssubdp		vs35,	vs35,	vs3
	xssubdp		vs36,	vs36,	vs4
	xssubdp		vs37,	vs37,	vs5
	xssubdp		vs38,	vs38,	vs6
	xssubdp		vs39,	vs39,	vs7

	mr		T1,	AO	


//############### OFFSET 0 #######################

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1
	lxsdx		vs2,	o16,	T1
	lxsdx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs4,	o0,	T1
	lxsdx		vs5,	o8,	T1
	lxsdx		vs6,	o16,	T1
	lxsdx		vs7,	o24,	T1

	addi		T1,	T1,	32

	xsmuldp		vs32,	vs32,	vs0
	xsnmsubadp	vs33,	vs32,	vs1
	xsnmsubadp	vs34,	vs32,	vs2
	xsnmsubadp	vs35,	vs32,	vs3
	xsnmsubadp	vs36,	vs32,	vs4
	xsnmsubadp	vs37,	vs32,	vs5
	xsnmsubadp	vs38,	vs32,	vs6
	xsnmsubadp	vs39,	vs32,	vs7

//############### OFFSET 1 #######################

	addi		T1,	T1,	1*SIZE

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1
	lxsdx		vs2,	o16,	T1
	lxsdx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs4,	o0,	T1
	lxsdx		vs5,	o8,	T1
	lxsdx		vs6,	o16,	T1

	addi		T1,	T1,	24

	xsmuldp		vs33,	vs33,	vs0
	xsnmsubadp	vs34,	vs33,	vs1
	xsnmsubadp	vs35,	vs33,	vs2
	xsnmsubadp	vs36,	vs33,	vs3
	xsnmsubadp	vs37,	vs33,	vs4
	xsnmsubadp	vs38,	vs33,	vs5
	xsnmsubadp	vs39,	vs33,	vs6

//############### OFFSET 2 #######################

	addi		T1,	T1,	2*SIZE

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1
	lxsdx		vs2,	o16,	T1
	lxsdx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs4,	o0,	T1
	lxsdx		vs5,	o8,	T1

	addi		T1,	T1,	16

	xsmuldp		vs34,	vs34,	vs0
	xsnmsubadp	vs35,	vs34,	vs1
	xsnmsubadp	vs36,	vs34,	vs2
	xsnmsubadp	vs37,	vs34,	vs3
	xsnmsubadp	vs38,	vs34,	vs4
	xsnmsubadp	vs39,	vs34,	vs5

//############### OFFSET 3 #######################

	addi		T1,	T1,	3*SIZE

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1
	lxsdx		vs2,	o16,	T1
	lxsdx		vs3,	o24,	T1

	addi		T1,	T1,	32

	lxsdx		vs4,	o0,	T1

	addi		T1,	T1,	8

	xsmuldp		vs35,	vs35,	vs0
	xsnmsubadp	vs36,	vs35,	vs1
	xsnmsubadp	vs37,	vs35,	vs2
	xsnmsubadp	vs38,	vs35,	vs3
	xsnmsubadp	vs39,	vs35,	vs4

//############### OFFSET 4 #######################

	addi		T1,	T1,	4*SIZE

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1
	lxsdx		vs2,	o16,	T1
	lxsdx		vs3,	o24,	T1

	addi		T1,	T1,	32

	xsmuldp		vs36,	vs36,	vs0
	xsnmsubadp	vs37,	vs36,	vs1
	xsnmsubadp	vs38,	vs36,	vs2
	xsnmsubadp	vs39,	vs36,	vs3

//############### OFFSET 5 #######################

	addi		T1,	T1,	5*SIZE

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1
	lxsdx		vs2,	o16,	T1

	addi		T1,	T1,	24

	xsmuldp		vs37,	vs37,	vs0
	xsnmsubadp	vs38,	vs37,	vs1
	xsnmsubadp	vs39,	vs37,	vs2

//############### OFFSET 6 #######################

	addi		T1,	T1,	6*SIZE

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1

	addi		T1,	T1,	16

	xsmuldp		vs38,	vs38,	vs0
	xsnmsubadp	vs39,	vs38,	vs1

//############### OFFSET 7 #######################

	addi		T1,	T1,	7*SIZE

	lxsdx		vs0,	o0,	T1

	addi		T1,	T1,	8

	xsmuldp		vs39,	vs39,	vs0

//############### SAVE B #######################


	mr		T1,	BO	


	stxsdx		vs32,	o0,	T1
	stxsdx		vs33,	o8,	T1
	stxsdx		vs34,	o16,	T1
	stxsdx		vs35,	o24,	T1

	addi		T1,	T1,	32

	stxsdx		vs36,	o0,	T1
	stxsdx		vs37,	o8,	T1
	stxsdx		vs38,	o16,	T1
	stxsdx		vs39,	o24,	T1

//############### SAVE C #######################


	mr		T1,	CO

	stxsdx		vs32,	o0,	T1
	stxsdx		vs33,	o8,	T1
	stxsdx		vs34,	o16,	T1
	stxsdx		vs35,	o24,	T1

	addi		T1,	T1,	32

	stxsdx		vs36,	o0,	T1
	stxsdx		vs37,	o8,	T1
	stxsdx		vs38,	o16,	T1
	stxsdx		vs39,	o24,	T1

.endm


/*##########################################################################################
			 SOLVE_LT 4x1
##########################################################################################*/

.macro SOLVE_LT_4x1

	xxswapd		vs0,	vs32
	xxswapd		vs1,	vs33
	xxswapd		vs2,	vs34
	xxswapd		vs3,	vs35

//############### LOAD B #######################


	mr		T1,	BO	

	lxsdx		vs32,	o0,	T1
	lxsdx		vs33,	o8,	T1
	lxsdx		vs34,	o16,	T1
	lxsdx		vs35,	o24,	T1

	xssubdp		vs32,	vs32,	vs0
	xssubdp		vs33,	vs33,	vs1
	xssubdp		vs34,	vs34,	vs2
	xssubdp		vs35,	vs35,	vs3

	mr		T1,	AO	


//############### OFFSET 0 #######################

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1
	lxsdx		vs2,	o16,	T1
	lxsdx		vs3,	o24,	T1

	addi		T1,	T1,	32

	xsmuldp		vs32,	vs32,	vs0
	xsnmsubadp	vs33,	vs32,	vs1
	xsnmsubadp	vs34,	vs32,	vs2
	xsnmsubadp	vs35,	vs32,	vs3

//############### OFFSET 1 #######################

	addi		T1,	T1,	1*SIZE

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1
	lxsdx		vs2,	o16,	T1

	addi		T1,	T1,	24

	xsmuldp		vs33,	vs33,	vs0
	xsnmsubadp	vs34,	vs33,	vs1
	xsnmsubadp	vs35,	vs33,	vs2

//############### OFFSET 2 #######################

	addi		T1,	T1,	2*SIZE

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1

	addi		T1,	T1,	16

	xsmuldp		vs34,	vs34,	vs0
	xsnmsubadp	vs35,	vs34,	vs1

//############### OFFSET 3 #######################

	addi		T1,	T1,	3*SIZE

	lxsdx		vs0,	o0,	T1

	addi		T1,	T1,	8

	xsmuldp		vs35,	vs35,	vs0

//############### SAVE B #######################


	mr		T1,	BO	


	stxsdx		vs32,	o0,	T1
	stxsdx		vs33,	o8,	T1
	stxsdx		vs34,	o16,	T1
	stxsdx		vs35,	o24,	T1

//############### SAVE C #######################


	mr		T1,	CO

	stxsdx		vs32,	o0,	T1
	stxsdx		vs33,	o8,	T1
	stxsdx		vs34,	o16,	T1
	stxsdx		vs35,	o24,	T1

.endm


/*##########################################################################################
			 SOLVE_LT 2x1
##########################################################################################*/

.macro SOLVE_LT_2x1

	xxswapd		vs0,	vs32
	xxswapd		vs1,	vs33

//############### LOAD B #######################


	mr		T1,	BO	

	lxsdx		vs32,	o0,	T1
	lxsdx		vs33,	o8,	T1

	xssubdp		vs32,	vs32,	vs0
	xssubdp		vs33,	vs33,	vs1

	mr		T1,	AO	


//############### OFFSET 0 #######################

	lxsdx		vs0,	o0,	T1
	lxsdx		vs1,	o8,	T1

	addi		T1,	T1,	16

	xsmuldp		vs32,	vs32,	vs0
	xsnmsubadp	vs33,	vs32,	vs1

//############### OFFSET 1 #######################

	addi		T1,	T1,	1*SIZE

	lxsdx		vs0,	o0,	T1

	addi		T1,	T1,	8

	xsmuldp		vs33,	vs33,	vs0

//############### SAVE B #######################


	mr		T1,	BO	


	stxsdx		vs32,	o0,	T1
	stxsdx		vs33,	o8,	T1

//############### SAVE C #######################


	mr		T1,	CO

	stxsdx		vs32,	o0,	T1
	stxsdx		vs33,	o8,	T1

.endm


/*##########################################################################################
			 SOLVE_LT 1x1
##########################################################################################*/

.macro SOLVE_LT_1x1

	xxswapd		vs0,	vs32

//############### LOAD B #######################


	mr		T1,	BO	

	lxsdx		vs32,	o0,	T1

	xssubdp		vs32,	vs32,	vs0

	mr		T1,	AO	


//############### OFFSET 0 #######################

	lxsdx		vs0,	o0,	T1

	addi		T1,	T1,	8

	xsmuldp		vs32,	vs32,	vs0

//############### SAVE B #######################


	mr		T1,	BO	


	stxsdx		vs32,	o0,	T1

//############### SAVE C #######################


	mr		T1,	CO

	stxsdx		vs32,	o0,	T1

.endm

