AC_DEFUN([FA_PYTHON], [

AC_ARG_WITH(python,
  [AS_HELP_STRING([--with-python=<bin>], [use Python binary <bin>])])
case $with_python in
  "") PYTHON_BIN=python ;;
  *) PYTHON_BIN="$with_python"
esac

AC_CHECK_PROG(PYTHON, $PYTHON_BIN, $PYTHON_BIN)
fa_python_bin=$PYTHON

AC_MSG_CHECKING([for Python C flags])
fa_python_cflags=`$PYTHON -c "
import sysconfig
paths = [['-I' + sysconfig.get_path(p) for p in ['include', 'platinclude']]]
print(' '.join(paths))"`
AC_MSG_RESULT($fa_python_cflags)
AC_SUBST(PYTHON_CFLAGS, "$PYTHON_CFLAGS $fa_python_cflags")

])dnl FA_PYTHON
