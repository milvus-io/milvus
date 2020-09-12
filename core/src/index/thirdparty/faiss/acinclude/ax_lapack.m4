# ===========================================================================
#        https://www.gnu.org/software/autoconf-archive/ax_lapack.html
# ===========================================================================
#
# SYNOPSIS
#
#   AX_LAPACK([ACTION-IF-FOUND[, ACTION-IF-NOT-FOUND]])
#
# DESCRIPTION
#
#   This macro looks for a library that implements the LAPACK linear-algebra
#   interface (see http://www.netlib.org/lapack/). On success, it sets the
#   LAPACK_LIBS output variable to hold the requisite library linkages.
#
#   To link with LAPACK, you should link with:
#
#     $LAPACK_LIBS $BLAS_LIBS $LIBS $FLIBS
#
#   in that order. BLAS_LIBS is the output variable of the AX_BLAS macro,
#   called automatically. FLIBS is the output variable of the
#   AC_F77_LIBRARY_LDFLAGS macro (called if necessary by AX_BLAS), and is
#   sometimes necessary in order to link with F77 libraries. Users will also
#   need to use AC_F77_DUMMY_MAIN (see the autoconf manual), for the same
#   reason.
#
#   The user may also use --with-lapack=<lib> in order to use some specific
#   LAPACK library <lib>. In order to link successfully, however, be aware
#   that you will probably need to use the same Fortran compiler (which can
#   be set via the F77 env. var.) as was used to compile the LAPACK and BLAS
#   libraries.
#
#   ACTION-IF-FOUND is a list of shell commands to run if a LAPACK library
#   is found, and ACTION-IF-NOT-FOUND is a list of commands to run it if it
#   is not found. If ACTION-IF-FOUND is not specified, the default action
#   will define HAVE_LAPACK.
#
# LICENSE
#
#   Copyright (c) 2009 Steven G. Johnson <stevenj@alum.mit.edu>
#
#   This program is free software: you can redistribute it and/or modify it
#   under the terms of the GNU General Public License as published by the
#   Free Software Foundation, either version 3 of the License, or (at your
#   option) any later version.
#
#   This program is distributed in the hope that it will be useful, but
#   WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General
#   Public License for more details.
#
#   You should have received a copy of the GNU General Public License along
#   with this program. If not, see <https://www.gnu.org/licenses/>.
#
#   As a special exception, the respective Autoconf Macro's copyright owner
#   gives unlimited permission to copy, distribute and modify the configure
#   scripts that are the output of Autoconf when processing the Macro. You
#   need not follow the terms of the GNU General Public License when using
#   or distributing such scripts, even though portions of the text of the
#   Macro appear in them. The GNU General Public License (GPL) does govern
#   all other use of the material that constitutes the Autoconf Macro.
#
#   This special exception to the GPL applies to versions of the Autoconf
#   Macro released by the Autoconf Archive. When you make and distribute a
#   modified version of the Autoconf Macro, you may extend this special
#   exception to the GPL to apply to your modified version as well.

#serial 8

AU_ALIAS([ACX_LAPACK], [AX_LAPACK])
AC_DEFUN([AX_LAPACK], [
AC_REQUIRE([AX_BLAS])
ax_lapack_ok=no

AC_ARG_WITH(lapack,
        [AS_HELP_STRING([--with-lapack=<lib>], [use LAPACK library <lib>])])
case $with_lapack in
        yes | "") ;;
        no) ax_lapack_ok=disable ;;
        -* | */* | *.a | *.so | *.so.* | *.o) LAPACK_LIBS="$with_lapack" ;;
        *) LAPACK_LIBS="-l$with_lapack" ;;
esac

# Get fortran linker name of LAPACK function to check for.
# AC_F77_FUNC(cheev)
cheev=cheev_

# We cannot use LAPACK if BLAS is not found
if test "x$ax_blas_ok" != xyes; then
        ax_lapack_ok=noblas
        LAPACK_LIBS=""
fi

# First, check LAPACK_LIBS environment variable
if test "x$LAPACK_LIBS" != x; then
        save_LIBS="$LIBS"; LIBS="$LAPACK_LIBS $BLAS_LIBS $LIBS $FLIBS"
        AC_MSG_CHECKING([for $cheev in $LAPACK_LIBS])
        AC_TRY_LINK_FUNC($cheev, [ax_lapack_ok=yes], [LAPACK_LIBS=""])
        AC_MSG_RESULT($ax_lapack_ok)
        LIBS="$save_LIBS"
        if test $ax_lapack_ok = no; then
                LAPACK_LIBS=""
        fi
fi

# LAPACK linked to by default?  (is sometimes included in BLAS lib)
if test $ax_lapack_ok = no; then
        save_LIBS="$LIBS"; LIBS="$LIBS $BLAS_LIBS $FLIBS"
        AC_CHECK_FUNC($cheev, [ax_lapack_ok=yes])
        LIBS="$save_LIBS"
fi

# Generic LAPACK library?
for lapack in lapack lapack_rs6k; do
        if test $ax_lapack_ok = no; then
                save_LIBS="$LIBS"; LIBS="$BLAS_LIBS $LIBS"
                AC_CHECK_LIB($lapack, $cheev,
                    [ax_lapack_ok=yes; LAPACK_LIBS="-l$lapack"], [], [$FLIBS])
                LIBS="$save_LIBS"
        fi
done

AC_SUBST(LAPACK_LIBS)

# Finally, execute ACTION-IF-FOUND/ACTION-IF-NOT-FOUND:
if test x"$ax_lapack_ok" = xyes; then
        ifelse([$1],,AC_DEFINE(HAVE_LAPACK,1,[Define if you have LAPACK library.]),[$1])
        :
else
        ax_lapack_ok=no
        $2
fi
])dnl AX_LAPACK
