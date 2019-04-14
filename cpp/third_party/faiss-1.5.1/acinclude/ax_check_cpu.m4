
# serial 1

AC_DEFUN([AX_CPU_ARCH], [

AC_MSG_CHECKING([for cpu arch])
AC_CANONICAL_TARGET

case $target in
  amd64-* | x86_64-*)
    ARCH_CPUFLAGS="-msse4 -mpopcnt"
    ARCH_CXXFLAGS="-m64"
    ;;
  aarch64*-*)
dnl This is an arch for Nvidia Xavier a proper detection would be nice.
    ARCH_CPUFLAGS="-march=armv8.2-a"
    ;;
  *) ;;
esac
AC_MSG_RESULT([$target CPUFLAGS+=$ARCH_CPUFLAGS CXXFLAGS+=$ARCH_CXXFLAGS])

AC_SUBST(ARCH_CPUFLAGS)
AC_SUBST(ARCH_CXXFLAGS)

])dnl
