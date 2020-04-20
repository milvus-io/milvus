AC_DEFUN([FA_NUMPY], [
AC_REQUIRE([FA_PYTHON])

AC_MSG_CHECKING([for numpy headers path])

fa_numpy_headers=`$PYTHON -c "import numpy; print(numpy.get_include())"`

if test $? == 0; then
  if test x$fa_numpy_headers != x; then
    AC_MSG_RESULT($fa_numpy_headers)
    AC_SUBST(NUMPY_INCLUDE, $fa_numpy_headers)
  else
    AC_MSG_RESULT([not found])
    AC_MSG_WARN([You won't be able to build the python interface.])
  fi
else
  AC_MSG_RESULT([not found])
  AC_MSG_WARN([You won't be able to build the python interface.])
fi
])dnl
