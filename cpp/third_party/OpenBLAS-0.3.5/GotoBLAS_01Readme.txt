              Optimized GotoBLAS2 libraries version 1.13

                       By Kazushige Goto <kgoto@tacc.utexas.edu>

# This is the last update and done on 5th Feb. 2010.

0. License

   See 00TACC_Research_License.txt.

1. Supported OS

     Linux
     FreeBSD(Also it may work on NetBSD)
     OSX
     Soralis
     Windows 2k, XP, Server 2003 and 2008(both 32bit and 64bit)
     AIX
     Tru64 UNIX

2. Supported Architecture

    X86  : Pentium3 Katmai
                    Coppermine
           Athlon  (not well optimized, though)
           PentiumM Banias, Yonah
           Pentium4 Northwood
                    Nocona (Prescott)
	   Core 2   Woodcrest
	   Core 2   Penryn
           Nehalem-EP  Corei{3,5,7}
	   Atom
           AMD Opteron
	   AMD Barlcelona, Shanghai, Istanbul
	   VIA NANO

   X86_64: Pentium4 Nocona
	   Core 2   Woodcrest
	   Core 2   Penryn
           Nehalem
	   Atom
           AMD Opteron
	   AMD Barlcelona, Shanghai, Istanbul
	   VIA NANO

   IA64  : Itanium2

   Alpha : EV4, EV5, EV6

   POWER : POWER4
           PPC970/PPC970FX
           PPC970MP
	   CELL (PPU only)
           POWER5
           PPC440   (QCDOC)
           PPC440FP2(BG/L)
	   POWERPC G4(PPC7450)
           POWER6

   SPARC : SPARC IV
           SPARC VI, VII (Fujitsu chip)

   MIPS64/32: Sicortex

3. Supported compiler

   C compiler       : GNU CC
                      Cygwin, MinGW
                      Other commercial compiler(especially for x86/x86_64)

   Fortran Compiler : GNU G77, GFORTRAN
		      G95
		      Open64
                      Compaq
                      F2C
                      IBM
                      Intel
                      PathScale
                      PGI
                      SUN
                      Fujitsu

4. Suported precision

 Now x86/x86_64 version support 80bit FP precision in addition to
normal double presicion and single precision. Currently only
gfortran supports 80bit FP with "REAL*10".


5. How to build library?

 Please see 02QuickInstall.txt or just type "make".

