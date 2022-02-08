AC_DEFUN([FA_PROG_SWIG], [

AC_ARG_WITH(swig,
[AS_HELP_STRING([--with-swig=<bin>], [use SWIG binary <bin>])])
case $with_swig in
 "") AC_CHECK_PROG(SWIG, swig, swig);;
  *) SWIG="$with_swig"
esac

AC_SUBST(SWIG)
])
